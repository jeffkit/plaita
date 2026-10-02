"""ML 修复包钉测试 — Loop/While 循环条件求值上下文（plaita/node/loop.py）。

ML1（条件上下文）：历史上每轮 ``deepcopy(execution.context)`` 评条件，生产上下文
携带全部上游输出（AGENTRUN 大文本），成本 O(迭代数 × 上下文体积)。修复后顶层
浅拷贝 + 注入 LOOP-* 键，仅对「条件文本引用到的根键」deepcopy 隔离。本文件钉：

* 条件求值不在 execution.context 残留 LOOP-ITEM/LOOP-INDEX/LOOP-RESULT（sync/arun、
  默认与自定义 express_prefix）；
* 条件里的 mutate 函数（$F.set / $F.pop，见 core/expression.py has_side_effects）
  只能改到副本——不污染 $GLOBAL / $INPUT（契约与
  tests/unit/test_loop.py::TestLoopConditionIsolation 同源，扩展到 While/arun/
  嵌套结构/LOOP-ITEM）；
* 条件未引用的大体积上游数据不被 deepcopy（性能钉：repr 长度探针）。

ML2（输出语义）：Loop 输出 = 最后一次迭代结果（空集合 None），不再保留全部迭代
结果列表。Map/Filter 是聚合语义不受影响（由既有 test_loop.py/test_loop_arun.py 覆盖）。
"""
from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import patch

from plaita.core import types
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.decide import CONDITION_OP_EQ, Condition
from plaita.node.loop import Loop, While


# ---------------------------------------------------------------------------
# flow 构造辅助
# ---------------------------------------------------------------------------

def _index_child_flow() -> Flow:
    """子流程：返回 $INPUT.index（当前轮次）。"""
    return Flow(
        flow_id="child", version="1", runtime="python",
        input_type=Property(
            data_type=types.OBJECT,
            children={
                "item": Property(data_type=types.ANY),
                "index": Property(data_type=types.INTEGER),
            },
        ),
        output_type=Property(data_type=types.INTEGER),
        nodes=[
            Start(id="s", next="e"),
            End(id="e", resultType="success", output="$INPUT.index"),
        ],
    )


def _echo_child_flow() -> Flow:
    """子流程：原样返回 item。"""
    return Flow(
        flow_id="child-echo", version="1", runtime="python",
        input_type=Property(
            data_type=types.OBJECT,
            children={
                "item": Property(data_type=types.ANY),
                "index": Property(data_type=types.INTEGER),
            },
        ),
        output_type=Property(data_type=types.ANY),
        nodes=[
            Start(id="s", next="e"),
            End(id="e", resultType="success", output="$INPUT.item"),
        ],
    )


def _double_child_flow() -> Flow:
    """子流程：返回 item * 2。"""
    return Flow(
        flow_id="child-double", version="1", runtime="python",
        input_type=Property(
            data_type=types.OBJECT,
            children={
                "item": Property(data_type=types.INTEGER),
                "index": Property(data_type=types.INTEGER),
            },
        ),
        output_type=Property(data_type=types.INTEGER),
        nodes=[
            Start(id="s", next="e"),
            End(id="e", resultType="success", output="$F.mul($INPUT.item, 2)"),
        ],
    )


def _while_flow(condition, max_iterations=100, prefix: str = "$"):
    return Flow(
        flow_id="w", version="1", runtime="python",
        input_type=Property(data_type=types.OBJECT),
        nodes=[
            Start(id="start", next="w"),
            While(id="w", child_flow=_index_child_flow(),
                  condition=condition, max_iterations=max_iterations, next="end"),
            End(id="end", resultType="success", output=f"{prefix}NODE.w"),
        ],
    )


def _loop_flow(collection, condition=None, child=None, extra_input_children: dict = None):
    kwargs = {"collection": collection, "child_flow": child or _echo_child_flow()}
    if condition is not None:
        kwargs["condition"] = condition
    return Flow(
        flow_id="l", version="1", runtime="python",
        input_type=Property(
            data_type=types.OBJECT,
            children={"items": Property(data_type=types.ARRAY, item_type=Property(data_type=types.ANY)),
                      **(extra_input_children or {})},
        ),
        nodes=[
            Start(id="start", next="l"),
            Loop(id="l", **kwargs, next="end"),
            End(id="end", resultType="success", output="$NODE.l"),
        ],
    )


def _run(execution: FlowExecution, flow: Flow, **params):
    return execution.run_compatible(flow, False, **params)


async def _arun(execution: FlowExecution, flow: Flow, **params):
    return await execution.arun_compatible(flow, False, **params)


# ---------------------------------------------------------------------------
# ML1-A：条件求值不在执行上下文残留 LOOP-* 键
# ---------------------------------------------------------------------------

class TestNoResidualInContext(unittest.TestCase):
    """循环结束后 execution.context 无 LOOP-ITEM/LOOP-INDEX/LOOP-RESULT 残留。"""

    def _assert_no_residual(self, execution: FlowExecution, prefix: str = "$"):
        for suffix in ("LOOP-ITEM", "LOOP-INDEX", "LOOP-RESULT"):
            self.assertNotIn(
                f"{prefix}{suffix}", execution.context,
                f"条件求值不应向 execution.context 残留 {prefix}{suffix}",
            )

    def test_loop_sync_no_residual(self):
        cond = Condition(field="$LOOP-INDEX", operator="lt", value=3)
        execution = FlowExecution()
        result = _run(execution, _loop_flow("$INPUT.items", cond), items=[1, 2, 3])
        self.assertEqual(result, 3)  # echo 子流程：最后一次迭代返回 item=3
        self._assert_no_residual(execution)

    def test_while_sync_no_residual(self):
        cond = Condition(field="$LOOP-INDEX", operator="lt", value=3)
        execution = FlowExecution()
        result = _run(execution, _while_flow(cond))
        self.assertEqual(result, 2)
        self._assert_no_residual(execution)

    def test_custom_express_prefix_no_residual(self):
        """express_prefix 可配置（如 "#"）：注入键与残留检查都按前缀走。"""
        cond = Condition(field="#LOOP-INDEX", operator="lt", value=2)
        execution = FlowExecution()
        execution.express_prefix = "#"
        execution.clean()
        result = _run(execution, _while_flow(cond, prefix="#"))
        self.assertEqual(result, 1)
        self._assert_no_residual(execution, prefix="#")


class TestNoResidualInContextAsync(unittest.IsolatedAsyncioTestCase):

    async def test_loop_arun_no_residual(self):
        cond = Condition(field="$LOOP-INDEX", operator="lt", value=3)
        execution = FlowExecution()
        result = await _arun(execution, _loop_flow("$INPUT.items", cond), items=[1, 2, 3])
        self.assertEqual(result, 3)  # echo 子流程：最后一次迭代返回 item=3
        for key in ("$LOOP-ITEM", "$LOOP-INDEX", "$LOOP-RESULT"):
            self.assertNotIn(key, execution.context)

    async def test_while_arun_no_residual(self):
        cond = Condition(field="$LOOP-INDEX", operator="lt", value=3)
        execution = FlowExecution()
        result = await _arun(execution, _while_flow(cond))
        self.assertEqual(result, 2)
        for key in ("$LOOP-ITEM", "$LOOP-INDEX", "$LOOP-RESULT"):
            self.assertNotIn(key, execution.context)


# ---------------------------------------------------------------------------
# ML1-B：条件 mutate 函数只改副本，不污染原 context（引用根键隔离）
# ---------------------------------------------------------------------------

class TestConditionMutationIsolation(unittest.TestCase):
    """$F.set/$F.pop 在条件里 mutate 时，被引用根键必须与原 context 隔离。

    与全量 deepcopy 等价的隔离语义：未引用的根共享读（只读安全），引用的根
    deepcopy。契约源出 tests/unit/test_loop.py::TestLoopConditionIsolation。
    """

    def test_loop_set_on_global_does_not_pollute(self):
        cond = Condition(
            field="$F.set($GLOBAL, 'counter', $LOOP-RESULT)",
            operator=CONDITION_OP_EQ, value=0,
        )
        flow = Flow(
            flow_id="iso", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT),
            global_context={"counter": 0, "nested": {"deep": {"x": 1}}},
            nodes=[
                Start(id="start", next="l"),
                Loop(id="l", collection=[1, 2, 3],
                     child_flow=_echo_child_flow(), condition=cond, next="end"),
                End(id="end", resultType="success", output="$NODE.l"),
            ],
        )
        execution = FlowExecution()
        result = _run(execution, flow)
        self.assertEqual(result, 1)  # $F.set 返回 None，None==0 False → 首轮即 break
        global_ctx = execution.context.get("$GLOBAL", {})
        self.assertEqual(global_ctx.get("counter"), 0, "condition 的 $F.set 不应污染 $GLOBAL.counter")
        self.assertEqual(global_ctx.get("nested"), {"deep": {"x": 1}}, "嵌套结构不应被改写")

    def test_while_set_on_global_does_not_pollute(self):
        cond = Condition(
            field="$F.set($GLOBAL, 'counter', $LOOP-INDEX)",
            operator=CONDITION_OP_EQ, value=999,  # 恒 False：执行 1 轮即停
        )
        flow = Flow(
            flow_id="w-iso", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT),
            global_context={"counter": 0},
            nodes=[
                Start(id="start", next="w"),
                While(id="w", child_flow=_index_child_flow(),
                      condition=cond, max_iterations=5, next="end"),
                End(id="end", resultType="success", output="$NODE.w"),
            ],
        )
        execution = FlowExecution()
        _run(execution, flow)
        self.assertEqual(
            execution.context.get("$GLOBAL", {}).get("counter"), 0,
            "While 条件求值的 $F.set 不应污染 $GLOBAL.counter",
        )

    def test_loop_pop_on_input_items_does_not_shrink_source(self):
        """条件对 $INPUT.items（被引用根）做 $F.pop：原列表不能被裁剪。"""
        cond = Condition(
            field="$F.pop($INPUT.items, -1)",
            operator=CONDITION_OP_EQ, value=0,  # None==0 False → 首轮 break
        )
        execution = FlowExecution()
        _run(execution, _loop_flow("$INPUT.items", cond), items=[1, 2, 3, 4, 5])
        self.assertEqual(
            execution.context.get("$INPUT", {}).get("items"), [1, 2, 3, 4, 5],
            "条件求值中的 $F.pop 不应裁剪原 $INPUT.items",
        )

    def test_loop_mutating_loop_item_does_not_corrupt_collection(self):
        """条件对 $LOOP-ITEM 做 $F.pop：原集合元素（与 item 共享对象）不能被改。

        $LOOP-ITEM 在条件文本中被引用 → 注入值也须 deepcopy 隔离。
        """
        cond = Condition(
            field="$F.pop($LOOP-ITEM, 0)",
            operator=CONDITION_OP_EQ, value=0,  # None==0 False → 首轮 break
        )
        execution = FlowExecution()
        _run(execution, _loop_flow("$INPUT.items", cond), items=[[1, 2], [3, 4]])
        self.assertEqual(
            execution.context.get("$INPUT", {}).get("items"), [[1, 2], [3, 4]],
            "条件对 $LOOP-ITEM 的 mutate 不应穿透到原集合元素",
        )


class TestConditionMutationIsolationAsync(unittest.IsolatedAsyncioTestCase):

    async def test_loop_arun_set_on_global_does_not_pollute(self):
        cond = Condition(
            field="$F.set($GLOBAL, 'counter', $LOOP-RESULT)",
            operator=CONDITION_OP_EQ, value=0,
        )
        flow = Flow(
            flow_id="iso-async", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT),
            global_context={"counter": 0},
            nodes=[
                Start(id="start", next="l"),
                Loop(id="l", collection=[1, 2, 3],
                     child_flow=_echo_child_flow(), condition=cond, next="end"),
                End(id="end", resultType="success", output="$NODE.l"),
            ],
        )
        execution = FlowExecution()
        result = await _arun(execution, flow)
        self.assertEqual(result, 1)
        self.assertEqual(
            execution.context.get("$GLOBAL", {}).get("counter"), 0,
            "arun 路径条件求值同样不得污染 $GLOBAL",
        )


# ---------------------------------------------------------------------------
# ML1-C：条件未引用的大体积上游数据不被 deepcopy（性能钉）
# ---------------------------------------------------------------------------

class TestUnreferencedPayloadNotCopied(unittest.TestCase):
    """条件只引用 $LOOP-INDEX 时，任何 deepcopy 调用都不应触到大对象。

    用 repr 长度做「大对象」探针：修复前每轮 deepcopy 整个 context（含
    $INPUT 大负载），必然出现大对象拷贝；修复后只拷注入的小键。
    """

    def test_while_condition_index_only_skips_big_payload(self):
        import plaita.node.loop as loop_mod

        copied_repr_lengths = []
        real_deepcopy = loop_mod.deepcopy

        def spying_deepcopy(obj, memo=None):
            copied_repr_lengths.append(len(repr(obj)))
            return real_deepcopy(obj, memo)

        blob = {"agentrun": "x" * 4096, "trace": list(range(500))}
        cond = Condition(field="$LOOP-INDEX", operator="lt", value=20)
        flow = Flow(
            flow_id="w-big", version="1", runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={"blob": Property(data_type=types.ANY)},
            ),
            nodes=[
                Start(id="start", next="w"),
                While(id="w", child_flow=_index_child_flow(),
                      condition=cond, next="end"),
                End(id="end", resultType="success", output="$NODE.w"),
            ],
        )
        execution = FlowExecution()
        with patch.object(loop_mod, "deepcopy", spying_deepcopy):
            result = _run(execution, flow, blob=blob)
        self.assertEqual(result, 19)
        self.assertTrue(
            copied_repr_lengths,
            "LOOP-INDEX 被条件引用时仍应走 deepcopy 隔离（int 拷贝是零成本）",
        )
        self.assertLess(
            max(copied_repr_lengths), 100,
            f"deepcopy 触及了大对象（max repr len={max(copied_repr_lengths)}），"
            "未引用的大体积上游数据不应被拷贝",
        )


# ---------------------------------------------------------------------------
# ML2：Loop 输出 = 最后一次迭代结果
# ---------------------------------------------------------------------------

class TestLoopLastResultSemantics(unittest.TestCase):

    def test_returns_last_iteration_result(self):
        execution = FlowExecution()
        result = _run(execution, _loop_flow("$INPUT.items", child=_double_child_flow()), items=[1, 2, 3])
        self.assertEqual(result, 6, "输出应为最后一次迭代的结果（3×2）")

    def test_empty_collection_returns_none(self):
        execution = FlowExecution()
        result = _run(execution, _loop_flow("$INPUT.items", child=_double_child_flow()), items=[])
        self.assertIsNone(result)

    def test_condition_break_returns_last_executed_result(self):
        # items [1,2,3] → 逐轮结果 2,4,6；继续条件 $LOOP-INDEX != 1
        # → index=1（result=4）不满足继续条件即 break，输出最后执行的一轮结果
        cond = Condition(field="$LOOP-INDEX", operator="ne", value=1)
        execution = FlowExecution()
        result = _run(execution, _loop_flow("$INPUT.items", cond, child=_double_child_flow()), items=[1, 2, 3])
        self.assertEqual(result, 4, "条件不再满足即 break，输出为最后执行的一轮结果")

    def test_arun_returns_last_iteration_result(self):
        async def _case():
            execution = FlowExecution()
            return await _arun(
                execution, _loop_flow("$INPUT.items", child=_double_child_flow()), items=[1, 2, 3]
            )

        self.assertEqual(asyncio.run(_case()), 6)


# ---------------------------------------------------------------------------
# ML1-D：性能冒烟（防回归到逐轮全量 deepcopy）
# ---------------------------------------------------------------------------

class TestLargeContextPerformanceSmoke(unittest.TestCase):
    """300 轮 While × ~1MB 上下文：修复后 ~0.2s；若回归为逐轮全量 deepcopy
    则 ≈8s（本机实测 27ms/copy）。阈值 5s 对两侧都留足余量。
    """

    def test_while_large_context_condition_stays_fast(self):
        blob = [{"agent_run": "x" * 400, "trace": list(range(120))} for _ in range(1000)]
        cond = Condition(field="$LOOP-INDEX", operator="lt", value=300)
        flow = Flow(
            flow_id="w-perf", version="1", runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={"blob": Property(data_type=types.ANY)},
            ),
            nodes=[
                Start(id="start", next="w"),
                While(id="w", child_flow=_index_child_flow(),
                      condition=cond, max_iterations=310, next="end"),
                End(id="end", resultType="success", output="$NODE.w"),
            ],
        )
        execution = FlowExecution()
        t0 = time.perf_counter()
        result = _run(execution, flow, blob=blob)
        wall = time.perf_counter() - t0
        self.assertEqual(result, 299)
        self.assertLess(
            wall, 5.0,
            f"300 轮 × ~1MB 上下文循环耗时 {wall:.2f}s —— 疑似回归为逐轮全量 deepcopy",
        )


if __name__ == "__main__":
    unittest.main()
