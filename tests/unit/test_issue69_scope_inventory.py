"""plaita#69 r7 —— 作用域/物化面清单的**守卫测试**（unclassified ⇒ refuse）。

X1/X2 的根因是"某个状态派生对象变成求值作用域 / 子流程入口 / 集合初值"这类调用点从未
被穷举：``Reduce.initial`` 与嵌套表达式串的作用域都不在名单上。r6 把这一面变成
``scripts/issue69_mutation_matrix/scope_inventory.json`` 的 committed 清单，
本文件断言**当前源码里发现的每个点都在清单里**；新增一个未分类的点 ⇒ 本测试红。

清单的判据字段是 ``(kind, file, callee, kwargs)``（**不含行号也不含列号**，见下）；行号由
``scope_inventory.py`` 的打印输出给出（对应 docs §S 的 ``grep -n`` 证据）。

r7（Finding 3）：删除了四个**作用域外**的死登记（`child._materialise_child_input` /
`loop._eval_collection` / `loop.Reduce._eval_initial` / `concurrent._resolve_branch_input`
的 `note_fresh_closure`）——`fresh-register` 10 → **6**；`context.setup_flow` 的 `$GLOBAL`
快照改走 `_snapshot_value` ⇒ `materialise` 13 → **14**；总数 90 → **87**（物化点保留）。
`test_four_dead_boundary_registrations_are_gone` 钉住删除，且断言对应物化点仍在。

r6 修正的守卫缺陷（verifier 报的 + 本轮新发现的第三个）：

* **列号**：旧键含 ``col_offset`` ⇒ 只改缩进就会 missing+stale 假红。现在键既无行号也
  无列号（``test_indentation_only_edit_is_green`` / ``test_forty_blank_lines_above_green``）。
* **间接分发**：``getattr(state, "evaluate")(value)`` 以前不被发现。现在归一到
  ``getattr.evaluate`` 后分类（``test_getattr_indirect_entry_is_detected``）。
* **同缩进同形状碰撞**（新发现）：旧键在同函数内两个**同缩进**的同形状调用点会**相同**
  ⇒ 后一个静默覆盖前一个。r6 用遍历序 ``#k`` 序号区分，因此清单从 87 涨到 **90**
  （`context.py` 的 list/dict 两个 ``note_fresh_object``、`concurrent.py` 的两个
  ``_snapshot_value`` 与两个 ``execution.evaluate`` 此前被吞掉）。
"""
from __future__ import annotations

import unittest

from scripts.issue69_mutation_matrix import scope_inventory as inv


def _keys(source: str) -> set:
    return set(inv.discover_source(source, "probe.py"))


class TestIssue69ScopeInventory(unittest.TestCase):
    def test_inventory_is_present_and_non_trivial(self):
        inventory = inv.load_inventory()
        self.assertGreater(len(inventory), 50)

    def test_every_discovered_site_is_classified(self):
        found = inv.discover()
        inventory = inv.load_inventory()
        missing = sorted(set(found) - set(inventory))
        stale = sorted(set(inventory) - set(found))
        drift = sorted(k for k in set(found) & set(inventory)
                       if inv._stable(found[k]) != inv._stable(inventory[k]))
        self.assertEqual([], missing,
                         f"未分类的作用域/物化调用点（必须在清单里登记）: {missing[:3]}")
        self.assertEqual([], stale, f"清单里的点已不存在: {stale[:3]}")
        self.assertEqual([], drift, f"清单登记内容漂移: {drift[:3]}")

    def test_kind_counts_are_pinned(self):
        """计数钉住：新增/删除一个点（即使 JSON 被顺手重算）也会让这里可见。

        plaita#69 r7：`child._materialise_child_input` / `loop._eval_collection` /
        `loop.Reduce._eval_initial` / `concurrent._resolve_branch_input` 四处的
        `note_fresh_closure` 是**作用域外的死登记**，已删除 ⇒ `fresh-register`
        10 → 6；同时 `context.ExecutionContext.setup_flow` 的 `$GLOBAL` 每 run 快照
        从 `copy.deepcopy` 改走物化边界 `_snapshot_value` ⇒ `materialise` 13 → 14。
        总数 90 → **87**。

        plaita#164：新增第三个登记钩子 `note_scratch_object`（宿主显式 scratch 捐献，
        在**读取点**登记；判据是按对象身份的**单个**对象、不遍历闭包）⇒ `fresh-register`
        6 → **7**、总数 87 → **88**。分类器 `_classify` 同步把该钩子归入
        `fresh-register`——新增登记点必须被穷举，这正是本清单存在的理由。
        """
        found = inv.discover()
        kinds: dict = {}
        for info in found.values():
            kinds[info["kind"]] = kinds.get(info["kind"], 0) + 1
        # plaita#268：摘除嵌套表达式串的递归求值 ⇒ `_compile_variable.thunk` 里
        # `parser.evaluate` 的 explore-scope 点 15 → 14，总数 88 → 87。
        self.assertEqual(
            {"explore-scope": 14, "state-scope": 27, "child-input": 25,
             "materialise": 14, "fresh-register": 7}, kinds)
        self.assertEqual(87, len(found))

    def test_unclassified_site_would_trip_the_guard(self):
        """负控：拿掉任意一条 ⇒ missing 非空；凭空多一条 ⇒ stale 非空（判定不是恒真）。"""
        found = inv.discover()
        inventory = inv.load_inventory()
        victim = sorted(set(found) & set(inventory))[0]
        trimmed = {k: v for k, v in inventory.items() if k != victim}
        self.assertTrue(set(found) - set(trimmed))
        extra = dict(inventory)
        extra["bogus::site::evaluate::pos9::kw[]"] = {
            "kind": "explore-scope", "file": "bogus.py", "callee": "evaluate",
            "kwargs": "", "line": 1}
        self.assertTrue(len(extra) == len(inventory) + 1)
        self.assertTrue(set(extra) - set(found))

    def test_shape_matcher_catches_the_known_x1_x2_sites(self):
        """X1/X2 的共同根因面必须在清单里被点名（不是靠"没有这种形状"）。"""
        found = inv.discover()
        joined = "\n".join(sorted(found))
        # X1 嵌套串作用域（#268 摘除嵌套求值后该点不存在，见
        # test_four_dead_boundary_registrations_are_gone 的同型负控）
        self.assertNotIn(
            "expression_parser.py::ExpressionParser._compile_variable.thunk::parser.evaluate",
            joined)
        self.assertIn("loop.py::Reduce._eval_initial::execution.evaluate", joined)  # X2 initial
        self.assertIn("loop.py::Reduce._eval_initial::_snapshot_value", joined)
        # r7：循环条件隔离副本是**唯一**保留的 fresh-register 现场（在显式作用域内）。
        self.assertIn("loop.py::_condition_context::note_fresh_closure", joined)

    def test_four_dead_boundary_registrations_are_gone(self):
        """r7 Finding 3：四个作用域外的死登记点必须不在清单里（删干净，不留死调用点）。"""
        found = inv.discover()
        joined = "\n".join(sorted(found))
        for dead in (
            "child.py::_materialise_child_input::note_fresh_closure",
            "concurrent.py::Parallel._resolve_branch_input::note_fresh_closure",
            "loop.py::BaseCollectionNode._eval_collection::note_fresh_closure",
            "loop.py::Reduce._eval_initial::note_fresh_closure",
        ):
            with self.subTest(site=dead):
                self.assertNotIn(dead, joined)
                # 物化点本身仍在（只删登记，不删隔离）
                prefix = dead.rsplit("::", 1)[0]
                self.assertIn(prefix, joined)


class TestIssue69ScopeInventoryKeyStability(unittest.TestCase):
    """键的稳定性：行号/列号/空行/缩进都不得移动它（verifier 缺陷 (a) 的回归守卫）。"""

    BASE = "def f(state, value):\n    return state.evaluate(value)\n"

    def test_indentation_only_edit_is_green(self):
        """只改缩进（键必须完全不变）。"""
        deeper = "def f(state, value):\n        return state.evaluate(value)\n"
        self.assertEqual(_keys(self.BASE), _keys(deeper))

    def test_forty_blank_lines_above_are_green(self):
        """上方插 40 行空行（键必须完全不变）——旧键含行号/列号时这里会假红。"""
        shifted = "def f(state, value):\n" + "\n" * 40 + "    return state.evaluate(value)\n"
        self.assertEqual(_keys(self.BASE), _keys(shifted))

    def test_renaming_an_unrelated_function_is_green(self):
        """上方插入无关函数（行号整体下移）同样不得影响键。"""
        shifted = "def unrelated():\n    pass\n" + self.BASE
        self.assertEqual(_keys(self.BASE), _keys(shifted))

    def test_same_indent_same_shape_sites_do_not_collide(self):
        """同函数内同缩进、同形状的两个点必须各占一条（旧 col 键会静默吞掉一个）。"""
        source = ("def f(state, a, b):\n"
                  "    x = state.evaluate(a)\n"
                  "    y = state.evaluate(b)\n"
                  "    return x, y\n")
        keys = _keys(source)
        self.assertEqual(2, len(keys))
        self.assertTrue(any(k.endswith("::#1") for k in keys), sorted(keys))


class TestIssue69ScopeInventoryGetattr(unittest.TestCase):
    """间接分发：``getattr(state, "evaluate")(value)`` 必须与直接调用同判。"""

    def test_getattr_indirect_entry_is_detected(self):
        source = 'def f(state, value):\n    return getattr(state, "evaluate")(value)\n'
        keys = _keys(source)
        self.assertEqual(1, len(keys))
        key = keys.pop()
        self.assertIn("::getattr.evaluate::pos1::kw[]", key)
        self.assertEqual("state-scope", inv.discover_source(
            source, "probe.py")[key]["kind"])

    def test_getattr_indirect_explore_scope_is_detected(self):
        source = ('def f(parser, v, ctx):\n'
                  '    return getattr(parser, "evaluate")(v, ctx, None)\n')
        found = inv.discover_source(source, "probe.py")
        self.assertEqual(1, len(found))
        self.assertEqual("explore-scope", next(iter(found.values()))["kind"])

    def test_getattr_on_another_object_is_still_classified(self):
        """一般形式：``getattr(<任意 state/parser 对象>, "evaluate")(...)``。"""
        source = ('def f(execution, v):\n'
                  '    return getattr(execution, "evaluate")(v)\n')
        found = inv.discover_source(source, "probe.py")
        self.assertEqual({"state-scope"}, {i["kind"] for i in found.values()})

    def test_dynamic_attribute_name_is_a_documented_blind_spot(self):
        """**已知局限（负控）**：属性名动态计算 ⇒ 静态扫描不到（清单不声称覆盖它）。"""
        source = 'def f(state, value, name):\n    return getattr(state, name)(value)\n'
        self.assertEqual(set(), _keys(source))


if __name__ == "__main__":
    unittest.main()
