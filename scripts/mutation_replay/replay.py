#!/usr/bin/env python3
"""变异重放：验证 ``mutations/*.json`` 的定义确实被回归测试杀死。

每个 JSON 保存一条变异的**逐字节定义原文**（``old``）、变异文本（``new``）、
目标文件与 ``expect_fail``（pytest 节点 id）。本脚本对每条变异：

1. 从 ``git archive HEAD`` 导出一棵**干净树**到临时目录；
2. 先在干净树上跑 ``expect_fail``：必须**全绿**（否则是「测试本来就红」，
   不算杀死，直接判失败）；
3. 把 ``old`` 替换成 ``new``（要求 ``old`` 在目标文件中**恰好出现一次**）；
4. 在变异树上跑同一组节点：必须**至少 1 条红**（变异被杀死）；
5. 还原该文件，继续下一条。

隔离纪律：``cwd=<tree>`` + ``PYTHONPATH=<tree>``，并在开头断言
``plaita.__file__`` 落在临时树内（本机 venv 的 editable 安装指向主 checkout）。

用法::

    python scripts/mutation_replay/replay.py --list
    python scripts/mutation_replay/replay.py --one m04-cursor-overshoot-window-end
    python scripts/mutation_replay/replay.py --all
    python scripts/mutation_replay/replay.py --all --keep       # 保留临时树

退出码 0 = 全部杀死；1 = 有存活/基线不绿/定义无法应用。
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT = Path(__file__).resolve()
ROOT = SCRIPT.parents[2]
MUTATIONS_DIR = SCRIPT.parent / "mutations"


def load_mutations():
    specs = []
    for path in sorted(MUTATIONS_DIR.glob("*.json")):
        specs.append(json.loads(path.read_text()))
    return specs


def run(cmd, cwd):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(cwd)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        cmd, cwd=str(cwd), env=env, capture_output=True, text=True
    )


def export_tree(dest: Path, source: str = "worktree"):
    """导出干净树：``source=head`` 用 ``git archive HEAD``，``worktree`` 再叠加
    工作区改动/未跟踪文件（便于**提交前**先验证变异定义）。"""
    dest.mkdir(parents=True, exist_ok=True)
    tar = subprocess.run(
        ["git", "-C", str(ROOT), "archive", "HEAD"], capture_output=True
    )
    if tar.returncode != 0:
        raise SystemExit(f"git archive HEAD 失败: {tar.stderr.decode()}")
    extract = subprocess.run(
        ["tar", "-x", "-C", str(dest)], input=tar.stdout, capture_output=True
    )
    if extract.returncode != 0:
        raise SystemExit(f"解包失败: {extract.stderr.decode()}")

    if source != "worktree":
        return
    status = subprocess.run(
        ["git", "-C", str(ROOT), "status", "--porcelain", "-z"],
        capture_output=True,
    )
    for entry in status.stdout.decode().split("\0"):
        if not entry:
            continue
        code, path = entry[:2], entry[3:]
        src, dst = ROOT / path, dest / path
        if code.strip() == "D":
            if dst.exists():
                dst.unlink()
            continue
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        elif src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def assert_import_is_tree(tree: Path, python: str):
    code = (
        "import os, plaita, sys;"
        "p=os.path.realpath(plaita.__file__);"
        f"t=os.path.realpath({str(tree)!r});"
        "sys.exit(0 if p.startswith(t + os.sep) else 1)"
    )
    res = subprocess.run([python, "-c", code], cwd=str(tree), capture_output=True)
    if res.returncode != 0:
        raise SystemExit(
            f"导入隔离失败：plaita 未解析到 {tree}（venv editable 指向别处？）"
        )


def pytest_cmd(python: str, nodes):
    return [python, "-m", "pytest", *nodes, "-q", "-p", "no:cacheprovider"]


def replay_one(spec, tree: Path, python: str, verbose: bool):
    target = tree / spec["file"]
    original = target.read_text()
    old, new = spec["old"], spec["new"]
    if original.count(old) != 1:
        return False, f"定义原文在 {spec['file']} 出现 {original.count(old)} 次（应为 1）"

    # 1) 基线：干净树必须全绿
    base = run(pytest_cmd(python, spec["expect_fail"]), tree)
    if base.returncode != 0:
        return False, "基线不绿（测试本来就红，无法用于杀死变异）"

    # 2) 变异 + 跑同一组
    target.write_text(original.replace(old, new, 1))
    try:
        mutated = run(pytest_cmd(python, spec["expect_fail"]), tree)
    finally:
        target.write_text(original)

    if mutated.returncode == 0:
        return False, "变异存活（expect_fail 全绿）"
    detail = ""
    for line in mutated.stdout.splitlines():
        if line.startswith(("FAILED", "ERROR")) and "tests/" in line:
            detail = line.strip()
            break
    if verbose:
        print(f"    killed-by: {detail}")
    return True, detail or "red"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--all", action="store_true", help="重放全部变异")
    ap.add_argument("--one", metavar="NAME", help="只重放一条（去 .json 的名字）")
    ap.add_argument("--list", action="store_true", help="列出全部变异定义")
    ap.add_argument("--tree", metavar="DIR", help="复用已导出的干净树（不导出）")
    ap.add_argument("--keep", action="store_true", help="保留临时导出树")
    ap.add_argument("--source", choices=["worktree", "head"], default="worktree",
                    help="worktree=HEAD+工作区改动（提交前验证，默认）；head=仅 HEAD")
    ap.add_argument("--python", default=sys.executable, help="python 解释器")
    args = ap.parse_args()

    specs = load_mutations()
    if args.list:
        for s in specs:
            print(f"{s['name']}  [{s['issue']}/{s['concern']}]  {s['file']}")
            print(f"    {s['rationale']}")
        return 0
    if args.one:
        specs = [s for s in specs if s["name"] == args.one]
        if not specs:
            print(f"未找到变异 {args.one!r}", file=sys.stderr)
            return 2
    elif not args.all:
        ap.print_help()
        return 0

    if args.tree:
        tree = Path(args.tree).resolve()
    else:
        tmp = Path(tempfile.mkdtemp(prefix="plaita-mutation-replay-"))
        tree = tmp
        export_tree(tree, args.source)
    assert_import_is_tree(tree, args.python)
    print(f"树: {tree}  (source={args.source})\n")

    results = []
    for spec in specs:
        print(f"[{spec['name']}] {spec['file']}")
        killed, detail = replay_one(spec, tree, args.python, verbose=True)
        results.append((spec["name"], killed, detail))
        print(f"    {'KILLED' if killed else 'SURVIVED'}  {detail}\n")

    survived = [r for r in results if not r[1]]
    print("=" * 60)
    print(f"合计: {len(results)}  杀死: {len(results) - len(survived)}  存活: {len(survived)}")
    for name, _k, detail in survived:
        print(f"  存活: {name}  ({detail})")

    if args.keep and not args.tree:
        print(f"（临时树保留: {tree}）")
    elif not args.tree:
        shutil.rmtree(tree, ignore_errors=True)
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
