#!/usr/bin/env python3
"""日志站点判定表校验（plaita#168 门禁）。

对账三断言：
  T1 (覆盖)   判定表必须覆盖验收 grep 的全部命中行——代码侧每行命中（含
              显式 N/A）在表中有条目，表中也不得有代码已不存在的条目。
  T2 (词表)   插值站点 class ∈ {A,B,C,D}；非插值条目必须显式标 N/A。
  T3 (重算)   用 scripts/log_site_taxonomy.py 的规则表对当前代码重算，
              结果必须与已提交 JSON 完全一致（判定表与判据不漂移）。

用法：python scripts/check-log-site-taxonomy.py   # 全绿 exit 0，任一红 exit 1

负向对照：从 JSON 删任一条目 ⇒ T1 红；给插值站点标 N/A ⇒ T2 红；
改规则表后不重算 JSON ⇒ T3 红。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from log_site_taxonomy import build_rows  # noqa: E402

VALID_INTERP = {"A", "B", "C", "D"}
VALID_ANY = VALID_INTERP | {"N/A"}


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    table_path = root / "docs" / "log-site-taxonomy.json"
    failures: list[str] = []

    if not table_path.exists():
        print("  ✗ [T1] docs/log-site-taxonomy.json 不存在——判定表未建立，"
              "全部插值站点处于无判定状态")
        return 1

    table = json.loads(table_path.read_text(encoding="utf-8"))
    rows = table.get("sites", [])
    recomputed = build_rows(root)
    current = {r["site"]: r for r in recomputed["sites"]}

    # ---- T1 覆盖（双向）----
    table_sites = [r.get("site") for r in rows]
    if len(table_sites) != len(set(table_sites)):
        dupes = sorted({s for s in table_sites if table_sites.count(s) > 1})
        failures.append(f"T1: 判定表存在重复 site 条目: {dupes[:10]}")
    table_set = set(table_sites)
    missing = sorted(set(current) - table_set)
    stale = sorted(table_set - set(current))
    if missing:
        failures.append(
            f"T1: {len(missing)} 个现存日志站点缺判定行（首 20）: {missing[:20]}")
    if stale:
        failures.append(
            f"T1: {len(stale)} 个判定行对应的站点已不存在（首 20）: {stale[:20]}")

    # ---- T2 词表 ----
    bad_vocab, bad_na = [], []
    for r in rows:
        site, cls, interp = r.get("site"), r.get("class"), r.get("interp")
        if interp and cls not in VALID_INTERP:
            bad_vocab.append(f"{site}({cls})")
        if not interp and cls != "N/A":
            bad_na.append(f"{site}({cls})")
        if cls not in VALID_ANY:
            bad_vocab.append(f"{site}({cls})")
    if bad_vocab:
        failures.append(f"T2: 插值站点出现 A/B/C/D 之外的类（首 20）: {bad_vocab[:20]}")
    if bad_na:
        failures.append(f"T2: 非插值条目未显式标 N/A（首 20）: {bad_na[:20]}")

    # ---- T3 重算一致 ----
    if table != recomputed:
        diffs = []
        for r in recomputed["sites"]:
            t = {x["site"]: x for x in rows}.get(r["site"])
            if t and (t.get("class") != r["class"] or t.get("rationale") != r["rationale"]):
                diffs.append(r["site"])
        detail = f"（类/理由漂移站点首 10: {diffs[:10]}）" if diffs else ""
        failures.append(f"T3: 判定表与判据重算结果不一致{detail}"
                        "——改代码或改规则后须 `python scripts/log_site_taxonomy.py --emit` 重算")

    m = recomputed["meta"]["counts"]
    print(f"  · 站点清点：grep 命中 {m['grep_hit_lines']} = 插值 {m['interpolated_sites']}"
          f" + 常量 {m['constant_message_sites']} + 误命中 {m['grep_false_hits']}")
    print(f"  · 判定分布：{recomputed['meta']['class_distribution']}")
    for f in failures:
        print(f"  ✗ {f}")
    print("")
    if failures:
        print(f" ✗ 日志站点判定表校验失败（{len(failures)} 处）")
        return 1
    print(" ✓ 日志站点判定表校验通过（T1 覆盖 / T2 词表 / T3 重算）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
