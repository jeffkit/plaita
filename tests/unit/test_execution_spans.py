"""execution trace span 模型（plaita#344）——每轮子节点耗时/迭代归属/并发帧配对。

验收条款（issue 六条的可执行化）：

1. **并发负控（先红后绿）**：parallel 两分支含同 id 节点交错执行——旧实现
   `_open[node_id]` 互踩（快分支先结束会"偷走"慢分支的 start），快/慢两对
   时长必错；新实现按 span_id 配对，两对时长各自正确且 span_id 互异、
   iteration/frame_path 归属正确。
2. **loop 3 轮**：子节点产生 3 组事件对，iteration=0..2（与 $LOOP-INDEX 同
   基，0-based——issue 文本写 1..3，落地取引擎既有 LOOP-INDEX 口径，回评注
   记），frame_path=["loop1"]，parent_span_id=各自轮的 loop span。
3. **attempt 语义分离**：同帧重入（resume 形状）attempt 递增；循环轮次不使
   attempt 递增（每轮自己的 span attempt=1）。
4. **旧消费面不破**：timeline 既有字段全保留；node_timings 聚合形状向后兼容
   （attempts/total 语义不变，occurrences 为 additive 新键）。
5. **回调契约**：严格签名 handler（无 **kwargs）零改动继续工作（span kwarg
   按签名能力分发）。
6. 记录面覆盖：node_timings + execution_timeline 双面；loop + parallel 两帧型
   （child 内联子流程同机制覆盖）。
"""
import json
import time
import uuid

import pytest

from plaita.core.callback import CallbackManager, FlowCallback
from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution
from plaita.node import Node, get_default_registry

get_default_registry()

# 确保扩展节点（delay 等）可用（本文件只用内置面，但保持与仓内测试同习惯）
from plaita.server.node_timings import NodeTimingCallback  # noqa: E402
from plaita.server.execution_timeline import ExecutionTimelineCallback  # noqa: E402


class SpanSleepNode(Node):
    """测试专用休眠节点（sleep 不是方言成员；branch/childFlow 经默认 registry
    延迟解析 → 全局注册）。"""

    node_type = "span_sleep_test"
    node_name = "span 测试休眠"
    execution_tier = "inline"
    sleep_ms: int = 0

    def execute(self, execution):
        time.sleep(self.sleep_ms / 1000.0)
        return {"slept": self.sleep_ms}


get_default_registry().register(SpanSleepNode)


def _flow(def_dict):
    return Flow.model_validate(def_dict)


def _parallel_flow(slow_ms, fast_ms):
    """parallel 两分支同 id 节点 "x"：慢分支 sleep slow_ms，快分支 sleep fast_ms。"""
    def branch_nodes(ms):
        return {"flow_id": f"b-{ms}", "version": "1", "runtime": "python", "nodes": [
            {"type": "start", "id": "bs", "next": "x"},
            {"type": "span_sleep_test", "id": "x", "sleep_ms": ms, "next": "be"},
            {"type": "end", "id": "be", "output": "done"},
        ]}

    return _flow({
        "flow_id": "par",
        "nodes": [
            {"type": "start", "id": "s", "next": "p"},
            {
                "type": "parallel",
                "id": "p",
                "joinBranches": ["slow", "fast"],
                "branches": [
                    {"name": "slow", "flow": branch_nodes(slow_ms)},
                    {"name": "fast", "flow": branch_nodes(fast_ms)},
                ],
                "next": "e",
            },
            {"type": "end", "id": "e", "output": "done"},
        ],
    })


def _loop_flow(rounds=3):
    return _flow({
        "flow_id": "lp",
        "version": "1",
        "nodes": [
            {"type": "start", "id": "s", "next": "loop1"},
            {
                "type": "loop",
                "id": "loop1",
                "collection": "$INPUT.items",
                "childFlow": {"runtime": "python", "nodes": [
                    {"type": "start", "id": "cs", "next": "step"},
                    {"type": "assignment", "id": "step", "output": "$INPUT.item", "next": "ce"},
                    {"type": "end", "id": "ce", "output": "ok"},
                ]},
                "next": "e",
            },
            {"type": "end", "id": "e", "output": "done"},
        ],
    })


def _timeline():
    tl = ExecutionTimelineCallback(clock=time.time)
    tl.start_run()
    return tl


def _recorder():
    return NodeTimingCallback(clock=time.time)


def _run_with(flow, handlers, **params):
    ex = FlowExecution(callback_handlers=list(handlers))
    ex.run_compatible(flow, False, **params)
    return ex


# ---------------------------------------------------------------------------
# 1. 并发负控（parallel 同 id 节点交错）
# ---------------------------------------------------------------------------


class TestParallelConcurrentPairing:
    def test_both_branch_durations_recorded_correctly(self):
        """旧实现：快分支先结束，慢分支的 start 被覆盖/偷走 → 慢分支时长≈0。
        新实现：按 span_id 配对，慢 ≥200ms、快 <200ms，两对齐全。"""
        rec = _recorder()
        _run_with(_parallel_flow(slow_ms=250, fast_ms=30), [rec])
        snap = rec.snapshot()
        assert "x" in snap, f"x 未记录: {sorted(snap)}"
        occ = snap["x"].get("occurrences")
        assert occ is not None, "缺少 occurrences 明细（span 模型未生效）"
        assert len(occ) == 2, f"应有两轮 x 记录，实得 {len(occ)}"
        durations = sorted(o["duration_ms"] for o in occ)
        # 慢分支 ≥200ms；旧实现该值≈0/缺失
        assert durations[-1] >= 200, f"慢分支时长未被正确配对: {durations}"
        assert durations[0] < 200, f"快分支时长异常: {durations}"

    def test_timeline_pairs_spans_not_node_ids(self):
        tl = _timeline()
        _run_with(_parallel_flow(slow_ms=250, fast_ms=30), [tl])
        events = tl.snapshot().get("events") or tl.drain().get("events")
        starts = [e for e in events if e.get("type") == "node_start" and e.get("node_id") == "x"]
        ends = [e for e in events if e.get("type") == "node_end" and e.get("node_id") == "x"]
        assert len(starts) == 2 and len(ends) == 2
        span_ids = {e.get("span_id") for e in starts}
        assert len(span_ids) == 2, f"两轮 x 的 span_id 应互异: {span_ids}"
        # frame 归属：两轮都在 parallel 帧 "p" 下、分支名互异
        frames = {e.get("frame_path")[-1] if e.get("frame_path") else None for e in starts}
        assert frames == {"p"}
        iters = {e.get("iteration") for e in starts}
        assert iters == {"slow", "fast"}, f"分支名应作为迭代限定符: {iters}"


# ---------------------------------------------------------------------------
# 2. loop 三轮：每轮独立事件对 + iteration + parent span
# ---------------------------------------------------------------------------


class TestLoopIterationAttribution:
    def test_loop_rounds_have_distinct_spans_and_iterations(self):
        tl = _timeline()
        flow = _loop_flow(rounds=3)
        ex = FlowExecution(callback_handlers=[tl])
        ex.run_compatible(flow, False, items=list(range(3)))
        events = tl.drain().get("events")
        starts = [e for e in events if e.get("type") == "node_start" and e.get("node_id") == "step"]
        assert len(starts) == 3, f"子节点应有 3 组 start，实得 {len(starts)}"
        iters = sorted(e.get("iteration") for e in starts)
        assert iters == [0, 1, 2], f"iteration 应为 0..2（LOOP-INDEX 口径）: {iters}"
        for e in starts:
            assert e.get("frame_path") == ["loop1"], e.get("frame_path")
        # parent_span_id = 各自轮 loop1 的 span
        loop_spans = {e.get("span_id") for e in events
                      if e.get("type") == "node_start" and e.get("node_id") == "loop1"}
        child_parents = {e.get("parent_span_id") for e in starts}
        assert child_parents and child_parents <= loop_spans, (
            f"子节点 parent_span 应指向 loop span: {child_parents} vs {loop_spans}")

    def test_timings_occurrences_per_round(self):
        rec = _recorder()
        _run_with(_loop_flow(rounds=3), [rec], items=list(range(3)))
        snap = rec.snapshot()
        occ = snap.get("step", {}).get("occurrences") or []
        assert len(occ) == 3
        assert sorted(o.get("iteration") for o in occ) == [0, 1, 2]


# ---------------------------------------------------------------------------
# 3. attempt 语义分离
# ---------------------------------------------------------------------------


class TestAttemptSemantics:
    def test_iteration_does_not_inflate_attempt(self):
        """3 轮循环的子节点：每轮自己的 attempt=1（轮次由 iteration 表达）。"""
        tl = _timeline()
        _run_with(_loop_flow(rounds=3), [tl], items=list(range(3)))
        events = tl.drain().get("events")
        starts = [e for e in events if e.get("type") == "node_start" and e.get("node_id") == "step"]
        assert [e.get("attempt") for e in starts] == [1, 1, 1]

    def test_same_frame_reentry_increments_attempt(self):
        """同帧重入（resume 形状：同一 execution 上二次 run_node）→ attempt=2。"""
        tl = _timeline()
        flow = _flow({
            "flow_id": "r",
            "nodes": [
                {"type": "start", "id": "s", "next": "x"},
                {"type": "assignment", "id": "x", "output": 1},
            ],
        })
        ex = FlowExecution(callback_handlers=[tl])
        ex._ctx.setup_flow(flow, (), {})
        from plaita.core.runner import NodeRunner

        runner = NodeRunner(ex._ctx, node_execution=ex)
        node = flow.find_node_by_id("x")
        import asyncio

        asyncio.run(runner.run_node(flow, node, callback_manager=ex.callback_manager))
        asyncio.run(runner.run_node(flow, node, callback_manager=ex.callback_manager))
        events = tl.drain().get("events")
        starts = [e for e in events if e.get("type") == "node_start" and e.get("node_id") == "x"]
        assert [e.get("attempt") for e in starts] == [1, 2]


# ---------------------------------------------------------------------------
# 4/5. 兼容面
# ---------------------------------------------------------------------------


class TestBackwardCompat:
    def test_node_timings_aggregate_shape_preserved(self):
        rec = _recorder()
        _run_with(_loop_flow(rounds=3), [rec], items=list(range(3)))
        entry = rec.snapshot()["step"]
        assert entry["attempts"] == 3  # 聚合语义不变：计每次执行
        assert entry["total_duration_ms"] >= entry.get("duration_ms", 0)
        assert "started_at" in entry and "ended_at" in entry

    def test_strict_signature_handler_still_works(self):
        """严格签名 handler（无 **kwargs）：span kwarg 不破它（按签名能力分发）。"""
        seen = []

        class Strict(FlowCallback):
            def on_node_start(self, flow, node):  # 故意无 **kwargs
                seen.append(node.id)

        flow = _loop_flow(rounds=2)
        _run_with(flow, [Strict()], items=[0, 1])
        assert seen.count("step") == 2

    def test_span_kwarg_reaches_span_aware_handler(self):
        seen = []

        class SpanAware(FlowCallback):
            def on_node_start(self, flow, node, **kwargs):
                seen.append(kwargs.get("span"))

        _run_with(_loop_flow(rounds=1), [SpanAware()], items=[0])
        assert any(s is not None and getattr(s, "span_id", None) for s in seen)
