#!/usr/bin/env python3
"""落地门禁：main CI 红灯熔断 + policy-ack trailer 核验。

约定见 ``docs/landing-and-acceptance.md``（2026-10-18 owner 拍板生效）。
只依赖 ``gh`` 与 ``git``，无第三方 Python 包。

用法::

    python scripts/landing_gate.py ci                    # 合并前必跑；红=1，无结论=2
    python scripts/landing_gate.py ci --override "原因 / ack 人"   # 双 ack 豁免（留痕）
    python scripts/landing_gate.py trailers <base>..<head>          # [policy] 提交须带 Policy-ack
    python scripts/landing_gate.py dist-scan [--dist DIR]           # 可选相位：#266 产物扫描
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = "jeffkit/plaita"
#: 视为「未通过」的 CI 结论（gh JSON 为小写）。
BAD_CONCLUSIONS = {"failure", "timed_out", "startup_failure", "cancelled"}


def _run(cmd: list[str]) -> tuple[int, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"[landing-gate] 命令失败（rc={proc.returncode}）：{' '.join(cmd)}", file=sys.stderr)
        print(proc.stderr.strip(), file=sys.stderr)
    return proc.returncode, proc.stdout


def cmd_ci(override: str | None) -> int:
    """检查 origin/main 上 ci.yml 最近一次已完成 run 的结论。"""
    rc, out = _run([
        "gh", "run", "list", "-R", REPO, "--branch", "main", "--workflow", "ci.yml",
        "--limit", "10", "--json", "conclusion,status,headSha,displayTitle,createdAt",
    ])
    if rc != 0:
        print("[landing-gate] 无法读取 CI 状态（gh 失败）——按红处理（fail-closed）", file=sys.stderr)
        return 1
    import json

    runs = json.loads(out or "[]")
    completed = [r for r in runs if r.get("status") == "completed"]
    if not completed:
        print("[landing-gate] main 上 ci.yml 无已完成 run（全部排队中？）——无结论，不放开")
        return 2
    latest = completed[0]

    # 「被更新推送取代」的 cancelled 不是红：并发推送会取消旧 head 的 run，
    # 新 head 的 run 才是权威判决。等待其完成（上限 10 分钟），按其结论判定。
    newest = runs[0]
    if latest["conclusion"] == "cancelled" and newest["headSha"] != latest["headSha"]:
        print(
            f"[landing-gate] run {latest['headSha'][:7]} 被更新推送 {newest['headSha'][:7]} "
            f"取代（{newest['status']}）——等待新 run 完成后判决"
        )
        deadline = time.time() + 600
        while time.time() < deadline:
            time.sleep(20)
            rc, out = _run([
                "gh", "run", "list", "-R", REPO, "--branch", "main", "--workflow", "ci.yml",
                "--limit", "10", "--json", "conclusion,status,headSha,displayTitle,createdAt",
            ])
            if rc != 0:
                continue
            runs = json.loads(out or "[]")
            head = runs[0]["headSha"] if runs else None
            done = next(
                (r for r in runs if r.get("headSha") == head and r.get("status") == "completed"),
                None,
            )
            if done is None:
                continue
            latest = done
            print(
                f"[landing-gate] 新 head run 完成：{done['createdAt']} {done['headSha'][:7]} "
                f"conclusion={done['conclusion']}  |  {done.get('displayTitle', '')[:60]}"
            )
            break
        else:
            print("[landing-gate] 等待超时（新 run 10 分钟未完成）——无结论，不放开，稍后重试")
            return 2

    print(
        f"[landing-gate] main 最近完成 run：{latest['createdAt']} {latest['headSha'][:7]} "
        f"conclusion={latest['conclusion']}  |  {latest['displayTitle'][:60]}"
    )
    if latest["conclusion"] == "success":
        print("[landing-gate] ✅ main CI 绿，允许落地")
        return 0
    if override:
        print(f"[landing-gate] ⚠️ OVERRIDE（红灯豁免，留痕）：{override}")
        return 0
    print(
        f"[landing-gate] ❌ main CI 红（conclusion={latest['conclusion']}）——落地熔断，"
        "先修 main 或走双 ack 豁免（--override）",
        file=sys.stderr,
    )
    return 1


def cmd_trailers(rev_range: str) -> int:
    """``rev_range`` 内含 ``[policy]`` 标记的提交必须带 ``Policy-ack`` trailer。"""
    rc, out = _run(["git", "log", "--format=%H%x1f%B%x1e", rev_range])
    if rc != 0:
        return 1
    missing: list[str] = []
    found = 0
    for entry in out.split("\x1e"):
        entry = entry.strip("\n")
        if not entry.strip():
            continue
        sha, body = entry.split("\x1f", 1)
        if "[policy]" not in body.lower():
            continue
        found += 1
        if not re.search(r"^Policy-ack:\s*\S+", body, re.MULTILINE | re.IGNORECASE):
            missing.append(sha[:7])
    if found == 0:
        print(f"[landing-gate] 区间 {rev_range} 无 [policy] 提交——trailer 核验通过")
        return 0
    if missing:
        print(
            f"[landing-gate] ❌ {len(missing)} 个 [policy] 提交缺 Policy-ack trailer："
            f"{' '.join(missing)}",
            file=sys.stderr,
        )
        return 1
    print(f"[landing-gate] ✅ {found} 个 [policy] 提交均带 Policy-ack")
    return 0


def cmd_dist_scan(dist: str | None) -> int:
    """可选相位：#266 浏览器管理密钥通道的构建产物扫描。

    不并入 ``ci``：Node 与前端产物并非所有落地环境都具备。显式请求时
    fail-closed——node 或产物目录缺失按「无结论」返回 2，不静默放行。
    """
    node = shutil.which("node")
    if node is None:
        print("[landing-gate] node 不可用——dist 扫描无结论", file=sys.stderr)
        return 2
    root = Path(__file__).resolve().parents[1]
    scanner = root / "plaita-console/frontend/scripts/check-admin-key.mjs"
    target = Path(dist) if dist else Path("plaita-console/frontend/dist")
    if not target.is_absolute():
        target = root / target
    if not target.is_dir():
        print(
            f"[landing-gate] 构建产物目录不存在: {target}——先构建前端再跑本相位",
            file=sys.stderr,
        )
        return 2
    proc = subprocess.run([node, str(scanner), "--dist", str(target)])
    if proc.returncode != 0:
        print("[landing-gate] ❌ #266 构建产物扫描红", file=sys.stderr)
        return 1
    print("[landing-gate] ✅ #266 构建产物扫描绿")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="落地门禁（docs/landing-and-acceptance.md）")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_ci = sub.add_parser("ci", help="main CI 红灯熔断")
    p_ci.add_argument("--override", metavar="TEXT", help="双 ack 豁免（原因 / ack 人），原样留痕")
    p_tr = sub.add_parser("trailers", help="[policy] 提交的 Policy-ack 核验")
    p_tr.add_argument("range", metavar="BASE..HEAD")
    p_ds = sub.add_parser("dist-scan", help="可选相位：#266 构建产物管理密钥扫描")
    p_ds.add_argument(
        "--dist", metavar="DIR",
        help="产物目录（默认 plaita-console/frontend/dist）",
    )
    args = parser.parse_args()
    if args.cmd == "ci":
        return cmd_ci(args.override)
    if args.cmd == "dist-scan":
        return cmd_dist_scan(args.dist)
    return cmd_trailers(args.range)


if __name__ == "__main__":
    sys.exit(main())
