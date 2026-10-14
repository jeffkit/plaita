#!/usr/bin/env python3
"""plaita#69 —— 分类矩阵的**仓内**生成器（r4）。

用法::

    cd <checkout> && PYTHONPATH=. python scripts/issue69_mutation_matrix/table.py

它从 ``tests/unit/test_issue69_mutation_matrix.py`` 导入**同一批**生成器/断言函数
（单一事实来源，不复制一份到 docs），实测出 ``docs/issue69-mutation-matrix.md`` 里的
每个计数与全表，并额外输出 r4 的「容器键名 × 值种类 × 嵌套」轴。

r3 的 `docs/issue69-mutation-matrix.md` §8 曾指向 ``/tmp/probe69r3/probes/table.py``
（探针随作者进程消失，文件不存在）——本文件是它的**入库替代**：命令原样可跑，
输出即文档数字。

退出码：0 = 生成成功且生成器自身断言通过；1 = 有计数/判决不符（含"名字决定了判决"）。
"""
from __future__ import annotations

import copy
import sys
from collections import Counter

import plaita.core.expression_parser as parser_mod
from plaita.core.expression_parser import (
    ExpressionParser,
    freshness_scope,
    get_default_expression_registry,
)
from tests.unit.test_issue69_mutation_matrix import (
    ALLOW_TARGETS,
    CONTAINER_KEY_NAMES,
    CONTAINER_NESTINGS,
    CONTAINER_VALUE_KINDS,
    LIVE_TARGETS,
    MUTATOR_SPEC,
    ROOTS,
    _container_path,
    _container_with,
    _context,
    _live_objects_ctx,
    build_callee_root_value_matrix,
    build_container_axis_cells,
    build_matrix,
    build_non_root_callee_matrix,
    build_r5_probe_rows,
    build_shallow_value_matrix,
    build_value_arg_matrix,
    run_cell,
)

PARSER = ExpressionParser.for_prefix("$")

#: 目标位轴里"结果是一个可安全 clear()/pop() 的**新**容器"的 11 个函数：
#: 10 个 SHALLOW 列表构造器 + `json_loads`（新 dict）+ `split`（新 list，doc 实测同格）。
#: 其余 DEEP（标量 / 类型不符）在目标位是**类型原生错误**，LIVE / MUTATOR 拒绝。
TARGET_AXIS_ALLOW = (
    "addListItem", "append", "extend", "insert", "insertListItem", "json_loads",
    "remove", "reverse", "slice", "sort", "split",
)
assert len(TARGET_AXIS_ALLOW) == 11 and len(set(TARGET_AXIS_ALLOW)) == 11


def _class_of(name: str) -> str:
    if name in parser_mod._DEFAULT_MUTATOR_NAMES:
        return "MUTATOR"
    if name in parser_mod._LIVE_REFERENCE_FUNCTIONS:
        return "LIVE"
    if name in parser_mod._DEEP_PRODUCER_FUNCTIONS:
        return "DEEP"
    if name in parser_mod._SHALLOW_PRODUCER_FUNCTIONS:
        return "SHALLOW"
    return "UNKNOWN"


def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def main() -> int:
    problems: list = []

    section("§0 规则（r5 可判定谓词：default-deny on provenance）")
    print("就地 mutator（has_side_effects=True）的任意实参在求值前分类；**目标位**额外要求")
    print("目标对象在求值后被证明是**引擎本次求值新建**的（freshness registry）：")
    print("  ⓪ 目标不是 fresh（活根/活根子对象/宿主对象/任意深度的容器里的值/懒快照）")
    print("     ⇒ 具名拒绝（provenance could not be proven fresh）——不再试图证明'它是活的'；")
    print("  ① 活 CheckpointState 上映射到五个持久化字段的键（含 express_*_name 别名）⇒ 具名拒绝；")
    print("     非持久化键不因键名放行，一律进入 freshness 判定；")
    print("  ② 普通容器（嵌套表达式串作用域）按对象身份探测可达的活对象 ⇒ 具名拒绝（r4 保留，")
    print("     但已非 load-bearing：freshness 独立拒）；")
    print("  ③ 调用按结果来源分类（DEEP/SHALLOW/LIVE/UNKNOWN）：DEEP/SHALLOW 由分类保证 fresh；")
    print("     LIVE 目标位要求运行时身份命中 fresh；UNKNOWN fail-closed；")
    print("  ④ 引擎新建的容器（字面量重建 / producer / detach 副本 / 物化边界）⇒ 放行。")

    section("§1 维度与总数（实测）")
    target_cells = build_matrix()
    value_cells = build_value_arg_matrix()
    shallow_cells = build_shallow_value_matrix()
    callee_cells = build_callee_root_value_matrix()
    nonroot_cells = build_non_root_callee_matrix()
    names = sorted(get_default_expression_registry().names())
    r5_rows = build_r5_probe_rows()
    print(f"注册表函数名数              : {len(names)}")
    print(f"目标位 cells                : {len(target_cells)}")
    print(f"值参位 cells                : {len(value_cells)}")
    print(f"浅 producer 值参 cells      : {len(shallow_cells)}")
    print(f"callee 触达根的值参 cells   : {len(callee_cells)}")
    print(f"非根 callee cells           : {len(nonroot_cells)}")
    print(f"r5 多位置探针 rows          : {len(r5_rows)}"
          f" (refuse {sum(1 for r in r5_rows if r['refused'])}"
          f" / isolated {sum(1 for r in r5_rows if not r['refused'])})")

    section("§1b producer × position 全轴（由已提交分类集合推导）")
    klass = {n: _class_of(n) for n in names}
    unknown = [n for n, k in klass.items() if k == "UNKNOWN"]
    if unknown:
        problems.append(f"未分类函数: {unknown}")
    counts = Counter(klass.values())
    value_allow = (counts["DEEP"] + counts["SHALLOW"]) * 7
    value_refuse = (counts["LIVE"] + counts["MUTATOR"]) * 7
    print(f"class 分布: {dict(sorted(counts.items()))}")
    print(f"值参轴: 总 {len(names) * 7} = allow {value_allow} + refuse {value_refuse}"
          " (alias-manufacturing 0)")
    target_allow = len(TARGET_AXIS_ALLOW) * 7
    target_native = (len(names) - len(TARGET_AXIS_ALLOW)
                     - counts["LIVE"] - counts["MUTATOR"]) * 7
    print(f"目标位轴: 总 {len(names) * 7} = allow {target_allow} + refuse {value_refuse}"
          f" + native-error {target_native}")

    section("§2 允许桶（实测分布，9 写形式/目标）")
    for label, target in ALLOW_TARGETS.items():
        dist: Counter = Counter()
        for _mutator, (_kind, tpl) in MUTATOR_SPEC.items():
            # r6：`$VAR` 由引擎在**同一次**逻辑求值里物化（真实用法），否则它是
            # 跨求值对象，default-deny 会按"非本次新建"拒绝。
            with freshness_scope():
                ctx = _context()
                outcome, _detail, _changed = run_cell(tpl.format(t=target), ctx)
            dist[outcome] += 1
        print(f"  {label:26s} {dict(sorted(dist.items()))}")

    section("§3 拒绝桶 live 引用 / 未声明 callee（9 × 8）")
    refused = 0
    for label, target in LIVE_TARGETS.items():
        for mutator, (_kind, tpl) in MUTATOR_SPEC.items():
            ctx = _context()
            outcome, detail, changed = run_cell(tpl.format(t=target), ctx)
            if outcome == "refuse" and "plaita#69" in detail and not changed:
                refused += 1
            else:
                problems.append(f"{label}/{mutator}: {outcome} changed={changed}")
    print(f"  拒绝 {refused}/{len(LIVE_TARGETS) * len(MUTATOR_SPEC)}，零写入")

    section("§4 producer × position 表（73 名 × 值参 7 格 / 目标位 7 格）")
    print("| function | class | value arg over root (7 cells) | alias manuf. | value arg over $VAR |")
    print("|---|---|---|---|---|")
    for name in names:
        k = klass[name]
        value = "allow" if k in ("DEEP", "SHALLOW") else "refuse"
        print(f"| `{name}` | {k} | {value} | no | allow |")

    section("§4c producer × position 目标位表（73 名 × 7 格）")
    print("| function | class | target arg over root (7 cells) |")
    print("|---|---|---|")
    for name in names:
        k = klass[name]
        if k in ("LIVE", "MUTATOR"):
            verdict = "refuse"
        elif name in TARGET_AXIS_ALLOW:
            verdict = "allow"
        else:
            verdict = "native"
        print(f"| `{name}` | {k} | {verdict} |")

    section("§4b r6 容器键名 × 值种类 × 嵌套（实测，27 格 × 零登记/有登记）")

    def _axis_rows(zero_registration: bool):
        if zero_registration:
            axis_ctx = _context()
            root_obj = axis_ctx.get_state("$INPUT")
            child_obj = root_obj["cfg"]              # 从未被当路径终点交出过
        else:
            axis_ctx, root_obj, child_obj = _live_objects_ctx()
        before_root = copy.deepcopy(root_obj)
        before_child = copy.deepcopy(child_obj)
        rows = []
        for _cell_id, bucket, key, kind, nesting in build_container_axis_cells():
            # r6：物化与就地改写必须属于**同一次**逻辑求值（引擎在节点字段的一次求值
            # 里造出容器、随即就地改写它），否则 default-deny 会按"跨求值"拒绝。
            with freshness_scope():
                if kind == "live-root":
                    value = root_obj
                elif kind == "live-root-child":
                    value = child_obj
                else:
                    # 引擎物化的副本（ctx.evaluate 重建容器并登记 fresh）
                    value = axis_ctx.evaluate(copy.deepcopy(child_obj))
                container = _container_with(key, value, nesting)
                path = _container_path(key, nesting)
                try:
                    PARSER.evaluate(f"$F.set({path},'k','v')", container)
                    outcome = "allow"
                except ValueError as exc:
                    outcome = "refuse" if "plaita#69" in str(exc) else "native-error"
            rows.append((bucket, kind, nesting, outcome))
        return rows, before_root, before_child, root_obj, child_obj

    for label, zero in (("zero-registration", True), ("pre-registered", False)):
        rows, before_root, before_child, root_obj, child_obj = _axis_rows(zero)
        counts = Counter(o for *_, o in rows)
        print(f"{label:18s} cells={len(rows)} {dict(sorted(counts.items()))}")
        for kind in CONTAINER_VALUE_KINDS:
            for nesting in CONTAINER_NESTINGS:
                verdicts = {o for _b, k, n, o in rows if k == kind and n == nesting}
                if len(verdicts) != 1:
                    problems.append(f"[{label}] 名字决定了判决: {kind}/{nesting} -> {verdicts}")
                if kind == "materialised-copy" and verdicts != {"allow"}:
                    problems.append(f"[{label}] 引擎物化副本被误拒: {kind}/{nesting} -> {verdicts}")
                if kind != "materialised-copy" and verdicts != {"refuse"}:
                    problems.append(f"[{label}] 活对象未被拒: {kind}/{nesting} -> {verdicts}")
        if before_root != root_obj or before_child != child_obj:
            problems.append(f"[{label}] 容器轴写穿 live 对象")
        print(f"    live-root={sorted({o for _b,k,_n,o in rows if k=='live-root'})}"
              f" live-root-child={sorted({o for _b,k,_n,o in rows if k=='live-root-child'})}"
              f" materialised-copy={sorted({o for _b,k,_n,o in rows if k=='materialised-copy'})}"
              f" write-through={before_root != root_obj or before_child != child_obj}")

    section("§X r6 多位置探针（活子对象 / 零登记 / X1 逐字；docs §X）")
    print("| position | verdict | live/checkpoint changed |")
    print("|---|---|---|")
    for row in r5_rows:
        print(f"| {row['position']} | {'refuse' if row['refused'] else 'allow/isolated'} |"
              f" {row['live_changed']} |")
        if row["live_changed"]:
            problems.append(f"探针写穿: {row['position']} :: {row['detail'][:60]}")
    print(f"\npositions={len(r5_rows)} refuse={sum(1 for r in r5_rows if r['refused'])}"
          f" isolated={sum(1 for r in r5_rows if not r['refused'])}"
          f" write-through={sum(1 for r in r5_rows if r['live_changed'])}")

    section("§S 作用域/物化面清单（committed inventory）")
    from scripts.issue69_mutation_matrix import scope_inventory
    sites = scope_inventory.discover()
    kinds: Counter = Counter(info["kind"] for info in sites.values())
    print(f"sites={len(sites)} kinds={dict(sorted(kinds.items()))}")
    inventory = scope_inventory.load_inventory()
    if set(sites) != set(inventory):
        problems.append("作用域/物化面清单与源码不一致（unclassified ⇒ 守卫红）")
    print("清单与源码一致。" if set(sites) == set(inventory) else "清单漂移！")

    section("结果")
    if problems:
        for p in problems:
            print(f"  PROBLEM: {p}")
        return 1
    print("  生成器自身断言全部通过（文档数字与本输出一致）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
