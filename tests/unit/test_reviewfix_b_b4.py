"""review-fix B4 回归：``run_distributed`` 非重入守卫。

历史行为：``run_distributed`` 没有任何重入防护（对比 ``run_compatible``/
``arun_compatible`` 有 ``_begin_run``），docstring 鼓励复用同一实例——两个线程
并发推进同一实例时共享 ``CheckpointState`` 读改写串扰、``clean()`` 互踩，
且无任何报错。review-fix B4 起每步调用期间持有 ``_running`` 守卫；
跨步骤**顺序**复用（合法用法）不受影响。
"""
from __future__ import annotations

import threading
import time
import unittest
from typing import ClassVar

from plaita.core.errors import FlowErrorException, FlowExecutionException
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.node import End, Start
from plaita.node.basic import Node


_GATE_EVENTS: dict = {}


class _GateNode(Node):
    """execute() 阻塞在 threading.Event 上，制造可控的重叠窗口。

    pydantic 模型不允许随意挂实例属性，Event 放模块级、按节点 id 区分。
    """

    node_type: ClassVar[str] = "gate_probe"
    node_name: ClassVar[str] = "gate"

    def execute(self, execution=None):
        _GATE_EVENTS[self.id].wait(timeout=10)
        return "gate-done"


class _FailNode(Node):
    """execute() 必然抛错，制造确定性的失败步。"""

    node_type: ClassVar[str] = "fail_probe"
    node_name: ClassVar[str] = "fail"

    def execute(self, execution=None):
        raise RuntimeError("b4 intentional failure")


def _sequential_flow_def():
    return {
        "flow_id": "b4-seq",
        "nodes": [
            {"id": "s", "type": "start", "name": "start", "next": "a"},
            {"id": "a", "type": "assignment", "name": "assign",
             "output": "step-value", "outputType": {"data_type": "string"}, "next": "e"},
            {"id": "e", "type": "end", "name": "end",
             "resultType": "success", "output": "$NODE.a"},
        ],
    }


class TestRunDistributedReentryGuard(unittest.TestCase):
    def test_concurrent_overlap_raises_reentry_error(self):
        """第一步在跑时并发第二跑：第二跑必须报重入错误，第一跑不受影响。"""
        flow = Flow(flow_id="b4-overlap", nodes=[
            Start(id="s", next="g"),
            _GateNode(id="g"),
            End(id="e", resultType="success", output="$NODE.g"),
        ])
        _GATE_EVENTS["g"] = threading.Event()
        execution = FlowExecution()

        first_result = {}

        def step_one():
            try:
                first_result["out"] = execution.run_distributed(flow, {})
            except Exception as e:  # pragma: no cover - 失败时让断言可见
                first_result["error"] = e

        t = threading.Thread(target=step_one)
        t.start()
        try:
            # 等 step_one 真正进入节点执行（gate 阻塞中）
            deadline = time.time() + 5
            while time.time() < deadline and not execution._running:
                time.sleep(0.01)
            self.assertTrue(execution._running, "第一步应持有 _running")

            with self.assertRaises(FlowExecutionException) as cm:
                execution.run_distributed(flow, {})
            self.assertIn("already running", str(cm.exception))
        finally:
            _GATE_EVENTS["g"].set()
            t.join(timeout=10)

        self.assertNotIn("error", first_result)
        self.assertFalse(first_result["out"].get("is_end"))
        self.assertFalse(execution._running, "第一步返回后守卫应释放")

    def test_sequential_multi_step_resume_still_works(self):
        """合法用法不受守卫破坏：同一实例顺序推进多步直至 End。

        distributed 首步语义：跑 Start 并解析后继后**继续执行后继节点**
        （_determine_current_node → _execute_current_node），故本 flow
        两步完成——step1 跑 s+a，step2 跑 End。
        """
        flow = Flow.model_validate(_sequential_flow_def())
        execution = FlowExecution()

        step1 = execution.run_distributed(flow, {})
        self.assertFalse(step1.get("is_end"))

        step2 = execution.run_distributed(flow, saved_context=step1["context"])
        self.assertTrue(step2.get("is_end"))
        self.assertEqual(step2.get("result"), "step-value")
        self.assertFalse(execution._running, "每步返回后守卫应释放")

    def test_guard_released_after_failed_step(self):
        """一步失败（守卫经 finally 释放）后，同实例可继续顺序复用。"""
        flow = Flow(flow_id="b4-fail", nodes=[
            Start(id="s", next="f"),
            _FailNode(id="f"),
            End(id="e", resultType="success", output="$NODE.f"),
        ])
        execution = FlowExecution()

        # 节点执行抛错 → run_distributed 归一化抛 FlowErrorException
        with self.assertRaises(FlowErrorException) as cm:
            execution.run_distributed(flow, {})
        self.assertIn("b4 intentional failure", str(cm.exception))
        self.assertFalse(execution._running, "失败步返回后守卫必须释放")

        # 同实例顺序复用不受影响（换一个合法 flow 完整跑通）
        good = Flow.model_validate({
            "flow_id": "b4-after-fail",
            "nodes": [
                {"id": "s", "type": "start", "name": "start", "next": "e"},
                {"id": "e", "type": "end", "name": "end",
                 "resultType": "success", "output": "ok"},
            ],
        })
        out = execution.run_distributed(good, {})
        self.assertTrue(out.get("is_end"))


if __name__ == "__main__":
    unittest.main()
