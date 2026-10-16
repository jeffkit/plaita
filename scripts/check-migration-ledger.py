#!/usr/bin/env python3
"""MIGRATION.md 版本账本对账（plaita#94）。

背景：v0.6.0 / v0.6.1 已打 tag 发布，但 MIGRATION.md 没有任何 0.6 段，全部
break 压在「Unreleased（0.5.x）」标题下——账本与发布事实脱节，且没有任何
门禁能发现这种漂移。本脚本是该门禁，三个机械断言：

  L1 (tag⇒分段)   每个发布 tag `v<X.Y.Z>`（纯 semver），MIGRATION.md 必须存在
                  `## <X.Y.Z>` 分段。已发布的版本不允许没有账本条目。
  L2 (Unreleased 标题) Unreleased 分段标题必须是裸 `## Unreleased`，不得携带
                  过期的版本括注（如 `## Unreleased（0.5.x）`——读者会把
                  已发布 break 记到错误版本账上）。
  L3 (账本漂移)   当前 Unreleased 段的 `###` 条目不得与「最新发布 tag 时刻」
                  Unreleased 段的 `###` 条目重叠：在最新 tag 的 Unreleased 里
                  就已存在的条目随该 tag 出货，必须归属到 `## <X.Y.Z>` 已发布
                  分段，不得滞留 Unreleased。标题归一化时剥掉
                  「，建议以 <版本> 发布」这类发布时已应摘除的待定措辞。

用法：
    python scripts/check-migration-ledger.py        # 全绿 exit 0，任一红 exit 1

负向对照（本脚本自身必须双向可用）：
  - 删掉任一已发布版本的 `## <X.Y.Z>` 分段  ⇒ L1 红；
  - 把 Unreleased 标题改回 `## Unreleased（0.5.x）` ⇒ L2 红；
  - 把任一已发布条目移回 Unreleased 段      ⇒ L3 红。
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

SEMVER_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
PENDING_RELEASE_PHRASE = re.compile(r"，?建议以 ?\d+\.\d+(\.\d+)? 发布")
PYPROJECT_VERSION = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)


def git(*args: str, cwd: Path) -> str:
    out = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    )
    return out.stdout


def repo_root() -> Path:
    return Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True
        ).stdout.strip()
    )


def normalize_title(title: str) -> str:
    """归一化 ### 条目标题：剥待定发布措辞、加粗标记与多余空白。"""
    t = title.replace("**", "").strip()
    t = PENDING_RELEASE_PHRASE.sub("", t)
    return re.sub(r"\s+", " ", t).strip()


def headings_with_lines(text: str, level: int = 2) -> list[tuple[int, str]]:
    """返回 (行号, 标题文本)；标题文本不含前导 # 与空格。"""
    pat = re.compile(r"^#{%d} (.*)$" % level)
    return [
        (i, m.group(1).rstrip())
        for i, line in enumerate(text.splitlines(), start=1)
        if (m := pat.match(line))
    ]


def section_span(hs: list[tuple[int, str]], title_match) -> tuple[int, int] | None:
    """返回匹配标题分段的 (起始行, 结束行)（含头不含下一分段头）。"""
    for idx, (lineno, title) in enumerate(hs):
        if title_match(title):
            end = hs[idx + 1][0] - 1 if idx + 1 < len(hs) else None
            return (lineno, end)
    return None


def h3_titles_in_span(text: str, start: int, end: int | None) -> list[str]:
    lines = text.splitlines()
    body = lines[start:] if end is None else lines[start - 1 : end]
    return [m.group(1).strip() for ln in body if (m := re.match(r"^### (.*)$", ln))]


def main() -> int:
    root = repo_root()
    migration_path = root / "MIGRATION.md"
    text = migration_path.read_text(encoding="utf-8")

    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    m = PYPROJECT_VERSION.search(pyproject)
    current_version = m.group(1) if m else None

    tags = [
        s.strip()
        for s in git("tag", "--list", cwd=root).splitlines()
        if s.strip() and SEMVER_TAG.match(s.strip())
    ]
    release_versions = [t[1:] for t in tags]
    release_versions.sort(key=lambda v: tuple(int(x) for x in v.split(".")))

    failures: list[str] = []

    def check(cid: str, ok: bool, detail_ok: str, detail_bad: str) -> None:
        if ok:
            print(f"  ✓ [{cid}] {detail_ok}")
        else:
            print(f"  ✗ [{cid}] {detail_bad}")
            failures.append(f"{cid}: {detail_bad}")

    print(f"pyproject version = {current_version}")
    print(f"release tags      = {', '.join('v' + v for v in release_versions) or '(none)'}")

    # ---- L1: 每个 v<X.Y.Z> tag ⇒ MIGRATION.md 必须有 ## <X.Y.Z> 分段 ----
    h2 = headings_with_lines(text, 2)
    h2_titles = {t for _, t in h2}
    for v in release_versions:
        ok = v in h2_titles
        tag_is_current = v == current_version
        note = "（= pyproject 当前版本）" if tag_is_current else ""
        check(
            "L1",
            ok,
            f"tag v{v} ⇒ `## {v}` 分段存在{note}",
            f"tag v{v} 已发布但 MIGRATION.md 无 `## {v}` 分段{note}"
            if ok is False
            else "",
        )

    # ---- L2: Unreleased 分段标题必须是裸 `## Unreleased` ----
    unreleased_span = section_span(h2, lambda t: t.startswith("Unreleased"))
    if unreleased_span is None:
        print("  · [L2] 无 Unreleased 分段（全部版本已发布收账）——跳过")
    else:
        lineno, title = next((ln, t) for ln, t in h2 if t.startswith("Unreleased"))
        check(
            "L2",
            title == "Unreleased",
            f"MIGRATION.md:{lineno} 分段标题为裸 `## Unreleased`",
            f"MIGRATION.md:{lineno} Unreleased 标题携带过期括注：`## {title}`"
            "（已发布 break 会被记到错误版本账上）",
        )

    # ---- L3: 当前 Unreleased 不得含最新 tag 时已在 Unreleased 的条目 ----
    if unreleased_span is not None and release_versions:
        latest_v = release_versions[-1]
        tagged_text = git("show", f"v{latest_v}:MIGRATION.md", cwd=root)
        tagged_h2 = headings_with_lines(tagged_text, 2)
        tagged_span = section_span(tagged_h2, lambda t: t.startswith("Unreleased"))
        if tagged_span is None:
            print(f"  · [L3] v{latest_v} 的 MIGRATION.md 无 Unreleased 段——跳过")
        else:
            shipped = {
                normalize_title(t)
                for t in h3_titles_in_span(tagged_text, tagged_span[0], tagged_span[1])
            }
            start, end = unreleased_span
            current_titles = h3_titles_in_span(text, start, end)
            stale = [t for t in current_titles if normalize_title(t) in shipped]
            check(
                "L3",
                not stale,
                f"当前 Unreleased 的 {len(current_titles)} 个条目均未随 v{latest_v} 出货",
                f"v{latest_v} 发布时已在 Unreleased 的 {len(stale)} 个条目仍滞留"
                f" Unreleased（随最新 tag 出货，须归入对应 `## <已发布版本>` 分段）：",
            )
            for t in stale:
                print(f"      - ### {t}")

    print("")
    if failures:
        print(f" ✗ MIGRATION.md 版本账本对账失败（{len(failures)} 处）")
        return 1
    print(" ✓ MIGRATION.md 版本账本对账通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
