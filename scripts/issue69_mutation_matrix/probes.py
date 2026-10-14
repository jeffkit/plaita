#!/usr/bin/env python3
"""plaita#69 r5 —— 活子对象**多位置探针**表（docs §X 的数据源）。

用法::

    cd <checkout> && PYTHONPATH=. python scripts/issue69_mutation_matrix/probes.py

输出：

* §X  多位置探针：每个位置要么**具名拒绝**、要么落在**引擎物化的私有副本**上，
      两者都必须对父活 ``$INPUT`` 与 checkpoint **零写穿**；
* §Xb 容器轴的两套状态：**零登记**（不做任何前置求值）与**有登记**（先跑
      ``$INPUT.cfg``）—— verifier 指出 r4 的 9 个 ``live-root-child`` 格在零登记下
      全部写穿；r5 的 default-deny 在两套状态下都必须逐格拒。

退出码：0 = 断言通过；1 = 任一位置写穿或判决不符。
"""
from __future__ import annotations

import copy
import sys

from plaita.core.expression_parser import freshness_scope

from tests.unit.test_issue69_mutation_matrix import (
    CONTAINER_KEY_NAMES,
    CONTAINER_NESTINGS,
    CONTAINER_VALUE_KINDS,
    PARSER,
    _container_path,
    _container_with,
    _context,
    _live_objects_ctx,
    build_container_axis_cells,
    build_r5_probe_rows,
)


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def container_axis(zero_registration: bool) -> list:
    problems = []
    if zero_registration:
        ctx = _context()
        live_root = ctx.get_state("$INPUT")
        live_child = live_root["cfg"]           # 从未被当路径终点交出过
    else:
        ctx, live_root, live_child = _live_objects_ctx()
    before_root = copy.deepcopy(live_root)
    before_child = copy.deepcopy(live_child)
    rows = []
    for _cell_id, bucket, key, kind, nesting in build_container_axis_cells():
        # r6：物化与就地改写必须属于**同一次**逻辑求值（真实用法），否则 default-deny
        # 会按"跨求值对象"拒绝引擎副本。
        with freshness_scope():
            if kind == "live-root":
                value = live_root
            elif kind == "live-root-child":
                value = live_child
            else:
                value = ctx.evaluate(copy.deepcopy(live_child))
            container = _container_with(key, value, nesting)
            path = _container_path(key, nesting)
            try:
                PARSER.evaluate(f"$F.set({path},'k','v')", container)
                outcome = "allow"
            except ValueError as exc:
                outcome = "refuse" if "plaita#69" in str(exc) else "native-error"
        rows.append((bucket, kind, nesting, outcome))
        if kind != "materialised-copy" and outcome != "refuse":
            problems.append(f"live cell not refused: {bucket}/{kind}/{nesting} -> {outcome}")
        if kind == "materialised-copy" and outcome != "allow":
            problems.append(f"engine copy refused: {bucket}/{kind}/{nesting} -> {outcome}")
    if before_root != live_root or before_child != live_child:
        problems.append("axis wrote through to the live root/child")
    return rows, problems


def main() -> int:
    problems = []

    section("§X r6 多位置探针（活子对象 / 零登记 / X1 逐字）")
    print("| position | verdict | live/checkpoint changed | detail |")
    print("|---|---|---|---|")
    for row in build_r5_probe_rows():
        verdict = "refuse" if row["refused"] else "allow/isolated"
        print(f"| {row['position']} | {verdict} | {row['live_changed']} |"
              f" {row['detail'][:70].replace('|', '/')} |")
        if row["live_changed"]:
            problems.append(f"WRITE-THROUGH: {row['position']} :: {row['detail'][:80]}")
    print(f"\npositions={len(build_r5_probe_rows())} "
          f"refused={sum(1 for r in build_r5_probe_rows() if r['refused'])} "
          f"write-through={sum(1 for r in build_r5_probe_rows() if r['live_changed'])}")

    section("§Xb 容器轴：零登记 vs 有登记（27 格 × 2）")
    for label, zero in (("zero-registration", True), ("pre-registered", False)):
        rows, axis_problems = container_axis(zero)
        counts = {}
        for *_head, outcome in rows:
            counts[outcome] = counts.get(outcome, 0) + 1
        print(f"{label:18s} cells={len(rows)} {counts}")
        for kind in CONTAINER_VALUE_KINDS:
            verdicts = {o for _b, k, _n, o in rows if k == kind}
            print(f"    {kind:20s} verdicts={sorted(verdicts)}")
        problems.extend(f"[{label}] {p}" for p in axis_problems)

    section("结果")
    if problems:
        for problem in problems:
            print(f"  PROBLEM: {problem}")
        return 1
    print("  多位置探针：全部具名拒绝或私有副本隔离，父活 state / checkpoint 零写穿。")
    print("  容器轴：零登记与有登记两套状态下判决一致（不再依赖任何前置登记副作用）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
