"""plaita#69 r7 —— 统一化回归：C1 反例（循环条件拷贝）+ 四个边界副本的语义。

Finding 1（规则级）：r6 的 `plaita/node/loop.py::_condition_context` 仍用
``copy.deepcopy`` 造隔离副本，于是载荷里 `__deepcopy__`/`__copy__` 返回 ``self`` 的
``dict``/``list`` 子类会把**活宿主对象**放进每次求值的 freshness 作用域，循环条件的
就地 mutate 直接写穿活 state 与 checkpoint：

    class Evil(dict):
        def __deepcopy__(self, memo): return self
        def __copy__(self): return self
    Loop(collection=[1],
         condition=Condition(field="$F.set($INPUT.items[0],'k','v')", operator="truthy"))

r7 后 `_condition_context` 走**唯一**的数据模型拷贝入口 ``_engine_copy``：容器子类被
用基类协议**归一化**成 plain dict/list（宿主钩子一次都不调用），副本是引擎现造的，
就地改写落在副本上、活对象与 checkpoint **零变化**。

Finding 3：四个边界物化点（`child._materialise_child_input` /
`loop._eval_collection` / `loop.Reduce._eval_initial` /
`concurrent._resolve_branch_input`）的 `note_fresh_closure` 调用点在**任何求值作用域
之外**，r6 起就是 no-op（死代码）。r7 删除它们并钉住语义：子执行对私有副本的就地改写
是**具名拒绝**（default-deny），父活 state 与 checkpoint 零写穿。并行分支的拒绝原因
**不是** "``ContextVar`` 不跨线程"——``mode="coroutine"`` 在 MainThread 里同样被拒，
根因是调用点在作用域之外（见 ``test_parallel_modes_have_no_active_scope_at_boundary``）。
"""
from __future__ import annotations

import threading
import unittest

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution
from plaita.core.expression_parser import _FRESH_SCOPE, freshness_scope
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import Assignment, End, Start
from plaita.node.decide import CONDITION_OP_TRUTHY, Condition
from plaita.node.loop import Loop, _condition_context

from tests.unit import test_issue69_mutation_matrix as matrix_mod
from tests.unit.test_issue69_mutation_matrix import (
    _r4_child_flow,
    _r5_live_payload,
    _r5_run_flow,
    build_r5_probe_rows,
)


class _Cfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "r7-uniformity"


class EvilDict(dict):
    hooks: list = []

    def __deepcopy__(self, memo):
        EvilDict.hooks.append("__deepcopy__")
        return self

    def __copy__(self):
        EvilDict.hooks.append("__copy__")
        return self


class EvilList(list):
    hooks: list = []

    def __deepcopy__(self, memo):
        EvilList.hooks.append("__deepcopy__")
        return self

    def __copy__(self):
        EvilList.hooks.append("__copy__")
        return self


def _literal_child_flow() -> Flow:
    return Flow(flow_id="r7-child", version="1", runtime="python",
                input_type=Property(data_type=types.OBJECT),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="s", next="a"),
                       Assignment(id="a", output="$INPUT.item",
                                  output_type=Property(data_type=types.ANY), next="e"),
                       End(id="e", resultType="success", output="$NODE.a")])


def _condition_loop_flow(condition_expr: str) -> Flow:
    return Flow(
        flow_id="r7-evil-loop", version="1", runtime="python",
        input_type=Property(data_type=types.OBJECT),
        output_type=Property(data_type=types.ANY),
        nodes=[Start(id="s", next="n"),
               Loop(id="n", collection=[1],
                    condition=Condition(field=condition_expr,
                                        operator=CONDITION_OP_TRUTHY, value=True),
                    child_flow=_literal_child_flow(), next="e"),
               End(id="e", resultType="success", output="$NODE.n")])


class TestIssue69R7LoopConditionCopy(unittest.TestCase):
    """Finding 1(c) 的 verifier 反例逐字回归。"""

    def setUp(self) -> None:
        EvilDict.hooks.clear()
        EvilList.hooks.clear()

    def test_evil_dict_in_the_condition_root_detaches_with_zero_write_through(self):
        live = EvilDict({"n": 1})
        execution = FlowExecution()
        raised = None
        try:
            execution.run_compatible(_condition_loop_flow("$F.set($INPUT.items[0],'k','v')"),
                                     False, {"items": [live]})
        except Exception as exc:                    # noqa: BLE001 - 判决就是探针的对象
            raised = exc
        self.assertIsNone(raised, "the engine-owned copier must detach, not refuse")
        self.assertEqual({"n": 1}, dict(live), "live host object was modified")
        self.assertIs(live, execution.context.get("$INPUT")["items"][0])
        self.assertEqual({"n": 1},
                         execution.context.to_checkpoint_dict()["$INPUT"]["items"][0])
        self.assertEqual([], EvilDict.hooks, "copy protocol hooks were invoked")

    def test_evil_list_in_the_condition_root_detaches_with_zero_write_through(self):
        live = EvilList([1, 2])
        execution = FlowExecution()
        raised = None
        try:
            execution.run_compatible(
                _condition_loop_flow("$F.setListItem($INPUT.items[0],0,99)"),
                False, {"items": [live]})
        except Exception as exc:                    # noqa: BLE001
            raised = exc
        self.assertIsNone(raised)
        self.assertEqual([1, 2], list(live), "live host list was modified")
        self.assertIs(live, execution.context.get("$INPUT")["items"][0])
        self.assertEqual([1, 2],
                         execution.context.to_checkpoint_dict()["$INPUT"]["items"][0])
        self.assertEqual([], EvilList.hooks)

    def test_condition_isolation_copy_is_a_plain_engine_owned_object(self):
        """白盒：隔离副本与活对象零共享；就地写只改副本。"""
        ctx = ExecutionContext()
        ctx.setup_flow(_Cfg(), ({},), {})
        live = EvilDict({"n": 1})
        ctx.set_state("$INPUT", {"items": [live]})
        condition = Condition(field="$F.set($INPUT.items[0],'k','v')",
                              operator=CONDITION_OP_TRUTHY, value=True)
        with freshness_scope():
            isolated = _condition_context(ctx, condition, item=None, index=0)
            copied = isolated["$INPUT"]["items"][0]
            self.assertIsNot(copied, live)
            self.assertIs(type(copied), dict)
            self.assertEqual({"n": 1}, copied)
            condition.match(isolated, ctx.express_prefix)
            self.assertEqual({"n": 1, "k": "v"}, copied)      # 写落在私有副本上
        self.assertEqual({"n": 1}, dict(live))                # 活对象零变化
        self.assertEqual({"n": 1}, ctx.context.to_checkpoint_dict()["$INPUT"]["items"][0])
        self.assertEqual([], EvilDict.hooks)

    def test_negative_control_is_discriminating(self):
        """负控：同形状的**纯 JSON** 载荷也必须放行（判决不是无差别拒绝）。"""
        execution = FlowExecution()
        raised = None
        try:
            execution.run_compatible(_condition_loop_flow("$F.set($INPUT.items[0],'k','v')"),
                                     False, {"items": [{"n": 1}]})
        except Exception as exc:                    # noqa: BLE001
            raised = exc
        self.assertIsNone(raised)
        self.assertEqual({"n": 1}, execution.context.get("$INPUT")["items"][0])


class TestIssue69R7BoundaryCopySemantics(unittest.TestCase):
    """Finding 3：四个死登记点删除后的语义（具名拒绝 + 零写穿）。"""

    FOUR_SHAPES = (
        "Reduce.initial (live sub-object)",
        "loop-item",
        "map-item",
        "parallel-branch-input (live sub-object)",
    )

    def test_four_boundary_shapes_refuse_in_place_mutation_of_the_copy(self):
        rows = {row["position"]: row for row in build_r5_probe_rows()}
        for position in self.FOUR_SHAPES:
            with self.subTest(position=position):
                row = rows[position]
                self.assertTrue(row["refused"], f"{position}: {row['detail'][:160]}")
                # 探针 detail 截断到 120 字符；用门的具名消息片段而不是完整
                # "plaita#69"（它在更后面）。
                self.assertIn("in-place mutator", row["detail"])
                self.assertFalse(row["live_changed"])

    def _parallel_flow(self, mode: str) -> Flow:
        from plaita.node.concurrent import Parallel
        branch_input = {"$myroot": "$INPUT.cfg",
                        "cmd": "$F.concat(\"$F.set($myroot,'k','v')\")"}
        return Flow(
            flow_id=f"r7-parallel-{mode}", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=Property(data_type=types.ANY),
            nodes=[Start(id="s", next="p"),
                   Parallel(id="p", name="p", mode=mode,
                            branches=[{"name": "b", "flow": _r4_child_flow(),
                                       "input": branch_input}],
                            join_branches=["b"], next="e"),
                   End(id="e", resultType="success", output="$NODE.p")])

    def test_parallel_modes_have_no_active_scope_at_boundary(self):
        """因果纠正：thread 与 coroutine 两种模式下边界处都没有活动作用域。"""
        from plaita.node import concurrent as concurrent_mod
        original = concurrent_mod.Parallel._resolve_branch_input
        records = []

        def spy(self, pb, execution):
            records.append({"scope_active": _FRESH_SCOPE.get() is not None,
                            "thread": threading.current_thread().name})
            return original(self, pb, execution)

        concurrent_mod.Parallel._resolve_branch_input = spy
        try:
            for mode in ("thread", "coroutine"):
                records.clear()
                payload = _r5_live_payload()
                start = {"cfg": payload}
                raised, live, checkpoint, pristine = _r5_run_flow(
                    self._parallel_flow(mode), start)
                with self.subTest(mode=mode):
                    self.assertTrue(records, f"{mode}: boundary resolver never ran")
                    for record in records:
                        self.assertFalse(
                            record["scope_active"],
                            f"{mode}: a freshness scope was active at the boundary "
                            f"({record}) — the refusal cause would be misattributed")
                    # 私有副本 + 具名拒绝：父活 state 与 checkpoint 零写穿
                    self.assertEqual(pristine, live)
                    self.assertEqual(pristine, checkpoint)
                    detail = self._parallel_error(mode, raised)
                    self.assertIn("plaita#69", detail)
        finally:
            concurrent_mod.Parallel._resolve_branch_input = original

    @staticmethod
    def _parallel_error(mode: str, raised) -> str:
        # `_r5_run_flow` **重绑**模块全局 `_R5_LAST_NODE`，必须动态取。
        last_node = matrix_mod._R5_LAST_NODE
        node = last_node.get("p") if isinstance(last_node, dict) else {}
        if isinstance(node, dict):
            for value in node.values():
                if isinstance(value, dict) and "__parallel_error__" in value:
                    return str(value["__parallel_error__"])
        return str(raised) if raised is not None else ""

    def test_parallel_thread_refusal_is_not_a_contextvar_thread_artifact(self):
        """因果对照：thread 模式在非 MainThread 的 worker 里、coroutine 模式在
        MainThread 里，两者**同样**被拒 —— 说明拒绝与"线程"无关，根因是作用域未开。"""
        from plaita.node import concurrent as concurrent_mod
        original = concurrent_mod.Parallel._resolve_branch_input
        threads = []

        def spy(self, pb, execution):
            threads.append(threading.current_thread().name)
            return original(self, pb, execution)

        concurrent_mod.Parallel._resolve_branch_input = spy
        try:
            observed = {}
            for mode in ("thread", "coroutine"):
                threads.clear()
                raised, live, checkpoint, pristine = _r5_run_flow(
                    self._parallel_flow(mode), {"cfg": _r5_live_payload()})
                with self.subTest(mode=mode):
                    self.assertTrue(threads)
                    self.assertEqual(pristine, live)
                    self.assertEqual(pristine, checkpoint)
                    self.assertIn("plaita#69", self._parallel_error(mode, raised))
                    observed[mode] = threads[0]
            self.assertNotEqual("MainThread", observed["thread"],
                                "thread mode must run the boundary off the main thread")
            self.assertEqual("MainThread", observed["coroutine"],
                             "coroutine mode runs on the main thread and is refused too")
        finally:
            concurrent_mod.Parallel._resolve_branch_input = original


class TestIssue69R7GlobalContextCopy(unittest.TestCase):
    """`$GLOBAL`（`setup_flow`）不再 `copy.deepcopy` —— 走**物化边界** `_snapshot_value`。

    `$GLOBAL` 是 Flow 定义里的每 run 快照（review-fix B2：容器必须私有，否则
    `$GLOBAL.cfg.timeout = 30` 会写进 Flow 定义本体、跨 run 泄漏）。r7 把它从
    `copy.deepcopy`（会调用宿主 `__deepcopy__`，Finding 1 的通道）改到与 checkpoint /
    `$PARENT` / 子流程输入**同一个**物化边界：容器按结构重建为精确类型、宿主 copy 钩子
    零调用；JSON 不可表达的非容器宿主对象按引用共享（进不了 checkpoint，也做不了就地
    改写目标——#69 门 default-deny 拒绝）。
    """

    def setUp(self) -> None:
        EvilDict.hooks.clear()

    @staticmethod
    def _flow(global_context) -> Flow:
        return Flow(flow_id="r7-global", version="1", runtime="python",
                    global_context=global_context,
                    input_type=Property(data_type=types.OBJECT),
                    output_type=Property(data_type=types.ANY),
                    nodes=[Start(id="s", next="e"),
                           End(id="e", resultType="success", output="$GLOBAL")])

    def test_global_context_subclass_is_normalised_without_copy_hooks(self):
        evil = EvilDict({"a": 1})
        execution = FlowExecution()
        execution.run_compatible(self._flow({"cfg": evil}), False, {})
        stored = execution.context.get("$GLOBAL")["cfg"]
        self.assertIs(type(stored), dict)
        self.assertIsNot(stored, evil)          # 与 Flow 定义本体不共享容器（B2）
        self.assertEqual({"a": 1}, stored)
        self.assertEqual([], EvilDict.hooks)    # copy 协议钩子一次都没调用

    def test_global_context_container_is_private_per_run(self):
        """B2 回归：run 内（节点/宿主）改 `$GLOBAL.cfg.x` 不得改进 Flow 定义本体。"""
        definition = {"cfg": {"x": 0}}
        flow = self._flow(definition)
        first = FlowExecution()
        first.run_compatible(flow, False, {})
        first.context.get("$GLOBAL")["cfg"]["x"] = 30      # 就地改本次 run 的快照
        second = FlowExecution()
        second.run_compatible(flow, False, {})
        self.assertEqual(0, second.context.get("$GLOBAL")["cfg"]["x"])
        self.assertEqual({"cfg": {"x": 0}}, dict(flow.global_context))

    def test_global_context_non_container_host_object_is_shared_by_reference(self):
        """物化边界语义（与 `_snapshot_value` 文档一致）：非容器宿主对象按引用共享。"""
        class Opaque:
            pass

        opaque = Opaque()
        execution = FlowExecution()
        execution.run_compatible(self._flow({"obj": opaque}), False, {})
        self.assertIs(opaque, execution.context.get("$GLOBAL")["obj"])

    def test_lazy_parent_snapshot_is_materialised_before_the_engine_copy(self):
        """`_condition_context` 的 `_LazyRootSnapshot` 分支：先 materialize 再拷贝。"""
        from plaita.core.state import _LazyRootSnapshot

        lazy = _LazyRootSnapshot({"$NODE": {"n1": {"v": 1}}})
        ctx = ExecutionContext()
        ctx.setup_flow(_Cfg(), ({},), {})
        ctx.set_state("$PARENT", lazy)
        condition = Condition(field="$PARENT.$NODE.n1.v", operator=CONDITION_OP_TRUTHY,
                              value=True)
        with freshness_scope():
            isolated = _condition_context(ctx, condition, item=None, index=0)
            copied = isolated["$PARENT"]
            self.assertIsNot(copied, lazy)
            self.assertIs(type(copied), dict)
            self.assertEqual({"$NODE": {"n1": {"v": 1}}}, copied)
            self.assertTrue(condition.match(isolated, ctx.express_prefix))

    def test_uncopyable_root_stays_live_but_unregistered(self):
        """非数据模型的根：保留活引用但**不登记** fresh（就地写 ⇒ default-deny 拒绝）。"""
        from plaita.core.expression_parser import _fresh_has

        class Opaque:
            pass

        opaque = Opaque()
        ctx = ExecutionContext()
        ctx.setup_flow(_Cfg(), ({},), {})
        ctx.set_state("$GLOBAL", {"x": opaque})
        condition = Condition(field="$GLOBAL.x", operator=CONDITION_OP_TRUTHY, value=True)
        with freshness_scope():
            isolated = _condition_context(ctx, condition, item=None, index=0)
            self.assertIs(isolated["$GLOBAL"], ctx.get_state("$GLOBAL"))   # 活引用
            self.assertFalse(_fresh_has(isolated["$GLOBAL"]))
            self.assertTrue(condition.match(isolated, ctx.express_prefix))  # 只读仍可用


if __name__ == "__main__":
    unittest.main()
