"""plaita#69 r8 —— 快照路径的**单拷贝纪律**（G1）+ 具名拒绝（G3）+ 循环条件根过拒（G2）。

r7 的独立复核（PASS-WITH-GAPS）钉住了一个**第二条拷贝路径**：
``plaita/core/state.py::_snapshot_value`` 用宿主方法读容器
（``value.items()`` / ``enumerate(value)`` / ``value.copy()``），因此

* ``set`` 子类把 ``copy()`` 覆写成 ``return self`` 时，快照**就是活宿主对象**
  （``$GLOBAL.s is <flow 定义里的 set>``）；
* ``dict``/``list`` 子类覆写的 ``items`` / ``__iter__`` 被真实调用（结果仍是普通
  容器，但"不调用宿主钩子"的书面承诺被证伪）。

本文件把修复后的契约钉死（**按测试**而非按措辞）：

* 快照只通过**基类协议**读宿主容器（``dict.items(sub)`` / ``list.__iter__(sub)``
  / ``tuple.__iter__(sub)`` / ``set.__iter__(sub)`` / ``frozenset.__iter__(sub)``
  / 基类 ``__len__``），结果必然是**普通** ``dict``/``list``/``tuple``/``set``/
  ``frozenset``，且与活对象 ``is`` 不相等；
* 覆盖 ``$GLOBAL``（``setup_flow``）、子 ``$INPUT``
  （``child._materialise_child_input``）、``$PARENT``（``_LazyRootSnapshot``）与
  checkpoint（``to_checkpoint_dict``）四条路径；
* 宿主钩子计数器恒为 **0**；
* 假容器（``__class__`` 伪造）在 ``_engine_copy`` 与快照路径都得到**具名**错误
  （G3），不再漏裸 ``TypeError``；
* 循环条件根里只要有一个非数据模型对象，整根保留活引用且不登记（G2 的既有行为，
  文档化为"就地写整根被拒、请先物化要改的部分"）。

机械守卫（AST）在 ``tests/unit/test_issue69_single_copy_point.py`` 的
``snapshot_host_protocol_calls``。
"""
from __future__ import annotations

import unittest
from collections.abc import MutableMapping
from typing import Any

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.errors import StateSnapshotError
from plaita.core.executor import FlowExecution
from plaita.core.expression_parser import freshness_scope
from plaita.core.flow import Flow
from plaita.core.state import _LazyRootSnapshot, _snapshot_value
from plaita.io import Property
from plaita.node import Assignment, End, Loop, Start
from plaita.node.child import _materialise_child_input
from plaita.node.decide import CONDITION_OP_TRUTHY, Condition

#: 宿主钩子计数器：三个覆写各自记账（跨测试清空）。
HOOKS: list = []


class HookSet(set):
    """``copy()`` 返回 ``self`` —— r7 下快照就是活对象（G1 反例）。"""

    def copy(self):
        HOOKS.append("set.copy")
        return self


class HookFrozenSet(frozenset):
    def copy(self):
        HOOKS.append("frozenset.copy")
        return self


class HookDict(dict):
    def items(self):
        HOOKS.append("dict.items")
        return dict.items(self)


class HookList(list):
    def __iter__(self):
        HOOKS.append("list.__iter__")
        return list.__iter__(self)


class _Cfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "r8-snapshot"


def _payload() -> dict:
    """同一个载荷形状：三种带钩子的子类各一个 + 一个纯数据兄弟键。"""
    return {
        "s": HookSet({1, 2}),
        "fs": HookFrozenSet({3}),
        "d": HookDict({"a": 1}),
        "l": HookList([1, 2]),
        "plain": {"x": 0},
    }


def _assert_plain_and_detached(case: unittest.TestCase, snapshot: Any,
                               live: dict) -> None:
    """四条路径共用的判决：普通容器、非活对象、结构相等。"""
    for key, expected in (("s", set), ("fs", frozenset), ("d", dict), ("l", list)):
        got = snapshot[key]
        case.assertIs(type(got), expected,
                      f"{key}: snapshot must be a plain {expected.__name__}, got "
                      f"{type(got).__name__}")
        case.assertIsNot(got, live[key], f"{key}: snapshot is the live host object")
        case.assertEqual(live[key], got)
    case.assertIs(type(snapshot["plain"]), dict)
    case.assertIsNot(snapshot["plain"], live["plain"])


def _global_flow(global_context: dict) -> Flow:
    return Flow(flow_id="r8-global", version="1", runtime="python",
                global_context=global_context,
                input_type=Property(data_type=types.OBJECT),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="s", next="e"),
                       End(id="e", resultType="success", output="$GLOBAL")])


class TestIssue69R8SnapshotIsAnEngineOwnedRebuild(unittest.TestCase):
    """G1：快照是引擎自有重建 —— 普通容器、零宿主钩子、绝不 ``is`` 活对象。"""

    def setUp(self) -> None:
        HOOKS.clear()

    def test_snapshot_value_rebuilds_hooked_subclasses_as_plain_containers(self):
        live = _payload()
        self.assertEqual({1, 2}, _snapshot_value(live["s"]))
        self.assertIs(type(_snapshot_value(live["s"])), set)
        self.assertIs(type(_snapshot_value(live["fs"])), frozenset)
        self.assertIs(type(_snapshot_value(live["d"])), dict)
        self.assertIs(type(_snapshot_value(live["l"])), list)
        self.assertEqual([], HOOKS, HOOKS)
        snapshot = _snapshot_value(live)
        _assert_plain_and_detached(self, snapshot, live)
        self.assertEqual([], HOOKS, HOOKS)

    def test_global_context_path(self):
        live = _payload()
        execution = FlowExecution()
        execution.run_compatible(_global_flow(live), False, {})
        stored = execution.context.get("$GLOBAL")
        _assert_plain_and_detached(self, stored, live)
        self.assertEqual([], HOOKS, HOOKS)
        # Flow 定义本体原样（快照是私有副本，跨 run 不泄漏）
        self.assertIs(type(live["s"]), HookSet)

    def test_child_input_path(self):
        live = _payload()
        snapshot = _materialise_child_input(live)
        _assert_plain_and_detached(self, snapshot, live)
        self.assertEqual([], HOOKS, HOOKS)

    def test_parent_path(self):
        live = _payload()
        lazy = _LazyRootSnapshot({"$INPUT": live})
        snapshot = _snapshot_value(lazy)
        self.assertIs(type(snapshot), dict)
        _assert_plain_and_detached(self, snapshot["$INPUT"], live)
        self.assertEqual([], HOOKS, HOOKS)
        # 惰性视图自身的根键读取也走同一条物化边界
        self.assertIs(type(lazy["$INPUT"]["s"]), set)
        self.assertEqual([], HOOKS, HOOKS)

    def test_checkpoint_paths(self):
        from plaita.core.state import CheckpointState

        live = _payload()
        state = CheckpointState()
        state["$INPUT"] = live
        checkpoint = state.to_checkpoint_dict()
        _assert_plain_and_detached(self, checkpoint["$INPUT"], live)
        self.assertEqual([], HOOKS, HOOKS)
        restored = CheckpointState.from_checkpoint_dict(checkpoint)
        _assert_plain_and_detached(self, restored["$INPUT"], live)
        self.assertEqual([], HOOKS, HOOKS)

    def test_child_flow_end_to_end_keeps_the_live_object_untouched(self):
        """黑盒半边：真跑一次 InlineFlow，活宿主 set 零变化、无宿主钩子。"""
        from plaita.node.child import InlineFlow

        live = _payload()
        child = Flow(flow_id="r8-child", version="1", runtime="python",
                     input_type=Property(data_type=types.OBJECT),
                     output_type=Property(data_type=types.ANY),
                     nodes=[Start(id="s", next="e"),
                            End(id="e", resultType="success", output="$INPUT")])
        node = InlineFlow(id="c", input={"s": "$INPUT.s", "cmd": "$F.concat('c')"},
                          child_flow=child, next="end")
        parent = Flow(flow_id="r8-parent", version="1", runtime="python",
                      input_type=Property(data_type=types.OBJECT),
                      output_type=Property(data_type=types.ANY),
                      nodes=[Start(id="start", next="c"), node,
                             End(id="end", resultType="success", output="$NODE.c")])
        payload = dict(live)
        payload["cmd"] = "text"
        execution = FlowExecution()
        execution.run_compatible(parent, False, payload)
        child_input = execution.context.get("$NODE")["c"]
        self.assertIs(type(child_input["s"]), set)
        self.assertIsNot(child_input["s"], live["s"])
        self.assertEqual(HookSet({1, 2}), child_input["s"])
        # 活对象与 checkpoint 零变化
        self.assertEqual(HookSet({1, 2}), live["s"])
        self.assertEqual([], HOOKS, HOOKS)

    def test_snapshot_set_is_still_named_refused_for_every_in_place_write(self):
        """安全规则回归：``$GLOBAL`` 的 set 快照经 DSL 的**每一次**就地写都具名拒绝、0 写。

        r7 下这个位置是**活宿主对象**，r8 下是普通 set —— 两种情况下 default-deny 都
        必须逐条具名拒绝（拒绝依据是 provenance，与快照类型无关），活对象与快照零变化。
        """
        live = _payload()
        cfg = _Cfg()
        cfg.global_context = live
        ctx = ExecutionContext()
        ctx.setup_flow(cfg, ({},), {})
        snapshot_before = set(ctx.get_state("$GLOBAL")["s"])
        attempts = ("$F.set($GLOBAL.s,'k','v')",
                    "$F.setListItem($GLOBAL.s,0,'v')",
                    "$F.pop($GLOBAL.s,1)",
                    "$F.clear($GLOBAL.s)")
        for expr in attempts:
            with self.subTest(expr=expr):
                with self.assertRaises(ValueError) as cm:
                    ctx.evaluate(expr)
                self.assertIn("plaita#69", str(cm.exception))
        self.assertEqual(HookSet({1, 2}), live["s"])          # Flow 定义本体 0 变化
        self.assertEqual(snapshot_before, set(ctx.get_state("$GLOBAL")["s"]))
        self.assertEqual([], HOOKS, HOOKS)

    def test_global_custom_mapping_rule_is_unchanged(self):
        """既有已核实规则回归：``$GLOBAL`` 里的自定义 ``Mapping`` 按引用共享、
        就地写**具名拒绝**、活对象零变化（它不是数据模型容器，重建语义不适用）。"""
        from plaita.core.expression_parser import _fresh_has
        from tests.unit.test_issue69_mutation_gate import _alias_context

        class MyMapping(MutableMapping):
            def __init__(self, data=None):
                self._d = dict(data or {})

            def __getitem__(self, key):
                return self._d[key]

            def __setitem__(self, key, value):
                self._d[key] = value

            def __delitem__(self, key):
                del self._d[key]

            def __iter__(self):
                return iter(self._d)

            def __len__(self):
                return len(self._d)

        mapping = MyMapping({"a": 1})
        execution = FlowExecution()
        execution.run_compatible(_global_flow({"m": mapping, "cfg": {"x": 0}}),
                                 False, {})
        stored = execution.context.get("$GLOBAL")["m"]
        self.assertIs(stored, mapping)              # 引用共享（非容器）
        with freshness_scope():
            parsed = _alias_context()
            parsed.set_state("$GLOBAL", {"m": mapping, "cfg": {"x": 0}})
            with self.assertRaises(ValueError) as cm:
                parsed.evaluate("$F.set($GLOBAL.m,'k','v')")
        self.assertIn("plaita#69", str(cm.exception))
        self.assertEqual({"a": 1}, dict(mapping))   # 0 写穿
        self.assertEqual({"x": 0}, dict(parsed.get_state("$GLOBAL")["cfg"]))
        self.assertFalse(_fresh_has(mapping))


class TestIssue69R8SpoofedClassIsNamed(unittest.TestCase):
    """G3：``__class__`` 伪造的"假容器"必须得到**具名**错误，而不是裸 ``TypeError``。"""

    @staticmethod
    def _liar(base: type, name: str):
        class Liar:
            __class__ = base

            def items(self):                # 宿主钩子：绝不能被调用
                raise AssertionError("host items() was called")

            def __iter__(self):
                raise AssertionError("host __iter__() was called")

            def copy(self):
                raise AssertionError("host copy() was called")

        Liar.__name__ = name
        Liar.__qualname__ = name
        return Liar()

    def test_engine_copy_refuses_every_spoofed_container_kind_by_name(self):
        import plaita.core.expression_parser as pm

        for base in (dict, list, tuple, set, frozenset):
            value = self._liar(base, f"Liar_{base.__name__}")
            with self.subTest(kind=base.__name__):
                self.assertTrue(isinstance(value, base))       # 伪造成功
                self.assertIsNot(type(value), base)
                with self.assertRaises(pm._EngineCopyRefused) as cm:
                    pm._engine_copy(value)
                self.assertEqual(type(value).__name__, cm.exception.type_name)
                self.assertEqual(type(value).__name__, f"Liar_{base.__name__}")
                self.assertIn("spoofed __class__", str(cm.exception))

    def test_engine_copy_refuses_a_spoofed_scalar_by_name(self):
        import plaita.core.expression_parser as pm

        value = self._liar(int, "Liar_int")
        self.assertTrue(isinstance(value, int))
        with self.assertRaises(pm._EngineCopyRefused) as cm:
            pm._engine_copy(value)
        self.assertEqual("Liar_int", cm.exception.type_name)

    def test_detach_alias_maps_the_spoofed_class_to_the_named_dsl_error(self):
        """DSL 面：浅 producer 的值参去别名把具名拒绝翻译成文档承诺的 `ValueError`。"""
        from tests.unit.test_issue69_mutation_gate import _alias_context

        with freshness_scope():
            ctx = _alias_context()
            ctx.set_state("$INPUT", {"items": [self._liar(dict, "Liar_dict")]})
            ctx.set_state("$VAR", ctx.evaluate({}))
            with self.assertRaises(ValueError) as cm:
                ctx.evaluate("$F.set($VAR,'k',$F.slice($INPUT.items,0,1))")
        self.assertIn("not part of the persistable data model", str(cm.exception))
        self.assertIn("Liar_dict", str(cm.exception))

    def test_snapshot_path_refuses_every_spoofed_container_kind_by_name(self):
        for base in (dict, list, tuple, set, frozenset):
            value = self._liar(base, f"Liar_{base.__name__}")
            with self.subTest(kind=base.__name__):
                with self.assertRaises(StateSnapshotError) as cm:
                    _snapshot_value(value)
                self.assertIn(type(value).__name__, cm.exception.message)
                self.assertIn(base.__name__, cm.exception.message)

    def test_a_spoofed_nested_value_is_named_in_the_snapshot_path(self):
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value({"ok": {"a": 1}, "bad": self._liar(set, "Liar_set")})
        self.assertIn("Liar_set", cm.exception.message)

    def test_spoofed_class_in_a_checkpoint_is_named(self):
        from plaita.core.state import CheckpointState

        state = CheckpointState()
        state["$INPUT"] = {"bad": self._liar(dict, "Liar_dict")}
        with self.assertRaises(StateSnapshotError) as cm:
            state.to_checkpoint_dict()
        self.assertIn("Liar_dict", cm.exception.message)


def _loop_flow(condition_expr: str) -> Flow:
    child = Flow(flow_id="r8-g2-child", version="1", runtime="python",
                 input_type=Property(data_type=types.OBJECT),
                 output_type=Property(data_type=types.ANY),
                 nodes=[Start(id="s", next="a"),
                        Assignment(id="a", output="$INPUT.item",
                                   output_type=Property(data_type=types.ANY), next="e"),
                        End(id="e", resultType="success", output="$NODE.a")])
    return Flow(flow_id="r8-g2", version="1", runtime="python",
                input_type=Property(data_type=types.OBJECT),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="s", next="n"),
                       Loop(id="n", collection=[1],
                            condition=Condition(field=condition_expr,
                                                operator=CONDITION_OP_TRUTHY, value=True),
                            child_flow=child, next="e"),
                       End(id="e", resultType="success", output="$NODE.n")])


def _run_loop(payload: dict, condition_expr: str):
    execution = FlowExecution()
    raised = None
    try:
        execution.run_compatible(_loop_flow(condition_expr), False, payload)
    except Exception as exc:                       # noqa: BLE001 - 判决即探针对象
        raised = exc
    return raised, str(raised) if raised is not None else ""


class TestIssue69R8LoopConditionNonDataModelRoot(unittest.TestCase):
    """G2：**文档化**的过拒行为（选择 (a)：保持"整根活引用 + 不登记"的二元不变量）。

    规则（用户可见，见 MIGRATION.md / docs-site）：循环条件的根一旦含**任何**非数据
    模型对象，``_condition_context`` 的 ``_engine_copy`` 整根拒绝 ⇒ 该根保留活引用且
    不登记 fresh ⇒ 该根内**任何**就地写（包括兄弟纯数据子树）都被 default-deny 具名
    拒绝。要就地改就先物化要改的部分。
    """

    DIRTY = {"items": [{"n": 1}], "blob": bytearray(b"x"), "cfg": {"a": 1}}
    CLEAN = {"items": [{"n": 1}], "cfg": {"a": 1}}

    def test_non_data_model_sub_object_refuses_the_whole_root(self):
        payload = {"items": [{"n": 1}], "blob": bytearray(b"x"), "cfg": {"a": 1}}
        for expr in ("$F.set($INPUT.items[0],'k','v')",    # 冒犯子树
                     "$F.set($INPUT.cfg,'k','v')"):        # 兄弟纯数据子树：同样被拒
            with self.subTest(expr=expr):
                raised, detail = _run_loop(payload, expr)
                self.assertIsNotNone(raised, "整根不登记 ⇒ 必须具名拒绝")
                self.assertIn("plaita#69", detail)
        self.assertEqual({"items": [{"n": 1}], "blob": bytearray(b"x"),
                          "cfg": {"a": 1}}, payload)       # 活对象零变化

    def test_white_box_root_stays_live_and_unregistered(self):
        """过拒的机制证据：根是**活引用**且不在 freshness 作用域里。"""
        from plaita.core.context import ExecutionContext
        from plaita.core.expression_parser import _fresh_has, freshness_scope
        from plaita.node.loop import _condition_context

        payload = {"items": [{"n": 1}], "blob": bytearray(b"x")}
        ctx = ExecutionContext()
        ctx.setup_flow(_Cfg(), ({},), {})
        ctx.set_state("$INPUT", payload)
        condition = Condition(field="$F.set($INPUT.items[0],'k','v')",
                              operator=CONDITION_OP_TRUTHY, value=True)
        with freshness_scope():
            isolated = _condition_context(ctx, condition, item=1, index=0)
            self.assertIs(isolated["$INPUT"], ctx.get_state("$INPUT"))
            self.assertFalse(_fresh_has(isolated["$INPUT"]))

    def test_opaque_object_and_custom_mapping_behave_the_same(self):
        class Opaque:
            pass

        class MyMapping(MutableMapping):
            def __init__(self):
                self._d: dict = {}

            def __getitem__(self, key):
                return self._d[key]

            def __setitem__(self, key, value):
                self._d[key] = value

            def __delitem__(self, key):
                del self._d[key]

            def __iter__(self):
                return iter(self._d)

            def __len__(self):
                return len(self._d)

        for blob in (Opaque(), MyMapping()):
            payload = {"items": [{"n": 1}], "blob": blob}
            with self.subTest(blob=type(blob).__name__):
                raised, detail = _run_loop(payload, "$F.set($INPUT.items[0],'k','v')")
                self.assertIsNotNone(raised)
                self.assertIn("plaita#69", detail)
                self.assertEqual({"n": 1}, payload["items"][0])

    def test_pure_data_root_is_still_allowed_negative_control(self):
        """负控：纯数据载荷（同一表达式）仍然放行 ⇒ 过拒只由非数据模型对象触发。"""
        payload = {"items": [{"n": 1}], "cfg": {"a": 1}}
        raised, detail = _run_loop(payload, "$F.set($INPUT.items[0],'k','v')")
        self.assertIsNone(raised, detail)
        # 就地写落在隔离副本上 ⇒ 活载荷零变化（r6/r7 已核实的语义）
        self.assertEqual({"n": 1}, payload["items"][0])

    def test_materialising_the_part_is_the_documented_workaround(self):
        """文档承诺的逃生口：先物化要改的部分（deep producer 目标）⇒ 放行。"""
        payload = {"items": [{"n": 1}], "blob": bytearray(b"x")}
        raised, detail = _run_loop(
            payload,
            "$F.set($F.json_loads($F.json_dumps($INPUT.items[0])),'k','v')")
        self.assertIsNone(raised, detail)
        self.assertEqual({"n": 1}, payload["items"][0])


if __name__ == "__main__":
    unittest.main()
