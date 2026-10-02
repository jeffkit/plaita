"""clean 修复包 C1-4 — eager 模式 FEE 不发 on_flow_end、lazy 发但抹掉 error。

问题（2026-10 评审）：
- ``finish_normal`` 对 ``FlowExecutionException`` 直接透传不回调——注释声称
  raise site 已发，但全仓 ``on_flow_end`` 只在 _error_normalization 出现，
  raise site 从未发过（eager FEE 生命周期回调整个缺失）。
- ``emit_flow_end_on_close`` lazy 分支把 FEE 抹成 ``result=None, error=None``，
  宿主无法从回调区分失败与正常结束。

修复：两处 FEE 分支均补发 ``on_flow_end``，error 带异常自身的
``{code, message}``（code 取异常类/实例的 ``code`` 属性，可区分
-520 节点 abort / -1 超时 / -4 取消 / -500 兜底），``exception`` 原样透传。
正常结束与节点级错误（非 FEE）路径零变化。
"""

import unittest
from unittest.mock import MagicMock

from plaita.core._error_normalization import emit_flow_end_on_close, finish_normal
from plaita.core.callback import FlowCallback
from plaita.core.errors import (
    FlowCancelledException,
    FlowExecutionException,
    FlowTimeoutError,
    NodeExecutionError,
)
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow


def _failing_flow() -> Flow:
    """节点执行失败（NodeExecutionError，FEE 子类）的最小 flow。"""
    return Flow.model_validate({
        "runtime": "python", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "d"},
            {"type": "decision", "id": "d",
             "conditions": [{"field": "$INPUT.x", "operator": "gt", "value": 1}],
             "next": "end"},
            {"type": "end", "id": "end"},
        ],
    })


def _ok_flow() -> Flow:
    return Flow.model_validate({
        "runtime": "python", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": {"ok": True}},
        ],
    })


class _Recorder(FlowCallback):
    def __init__(self):
        self.events = []

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs):
        self.events.append({
            "result": result, "error": error,
            "exc_type": type(exception).__name__ if exception else None,
        })


class EagerFeeFiresFlowEndTest(unittest.TestCase):
    def test_node_error_fires_on_flow_end_with_error(self):
        rec = _Recorder()
        with self.assertRaises(Exception) as cm:
            FlowExecution.run(_failing_flow(), {}, callback_handlers=[rec])
        self.assertIsInstance(cm.exception, FlowExecutionException)
        self.assertEqual(len(rec.events), 1, "eager FEE 必须恰好发一次 on_flow_end")
        ev = rec.events[0]
        self.assertIsNone(ev["result"])
        self.assertIsInstance(ev["error"], dict)
        self.assertIn("code", ev["error"])
        self.assertIn("message", ev["error"])
        self.assertNotEqual(ev["error"]["code"], 0)
        self.assertEqual(ev["exc_type"], "NodeExecutionError")

    def test_error_code_distinguishes_failure_kinds(self):
        """error.code 取异常自身 code（可区分），不再是恒定缺省。"""
        flow = MagicMock()
        cb = MagicMock()

        async def failing():
            raise FlowTimeoutError()

        import asyncio
        with self.assertRaises(FlowTimeoutError):
            asyncio.run(finish_normal(failing(), flow, cb))
        args, kwargs = cb.on_flow_end.call_args
        self.assertEqual(args[2]["code"], -1)
        self.assertEqual(args[2]["message"], "Flow execution timeout")

    def test_cancelled_exception_carries_minus_four(self):
        flow = MagicMock()
        cb = MagicMock()

        async def failing():
            raise FlowCancelledException()

        import asyncio
        with self.assertRaises(FlowCancelledException):
            asyncio.run(finish_normal(failing(), flow, cb))
        args, kwargs = cb.on_flow_end.call_args
        self.assertEqual(args[2]["code"], -4)
        self.assertEqual(args[2]["message"], "Flow execution cancelled")

    def test_exception_passthrough_unchanged(self):
        """异常对象原样向上抛（传播语义不变），回调只是补发。"""
        flow = MagicMock()
        cb = MagicMock()
        boom = NodeExecutionError("boom")

        async def failing():
            raise boom

        import asyncio
        with self.assertRaises(NodeExecutionError) as cm:
            asyncio.run(finish_normal(failing(), flow, cb))
        self.assertIs(cm.exception, boom)
        self.assertIs(cb.on_flow_end.call_args.kwargs.get("exception"), boom)


class LazyFeeCarriesErrorTest(unittest.TestCase):
    def test_lazy_node_error_on_flow_end_has_error(self):
        rec = _Recorder()
        gen = FlowExecution.run(_failing_flow(), {}, mode="generator", callback_handlers=[rec])
        with self.assertRaises(FlowExecutionException):
            list(gen)
        self.assertEqual(len(rec.events), 1)
        ev = rec.events[0]
        self.assertIsNone(ev["result"])
        self.assertIsInstance(ev["error"], dict)
        self.assertIn("code", ev["error"])
        self.assertEqual(ev["exc_type"], "NodeExecutionError")

    def test_eager_and_lazy_error_shapes_match(self):
        eager_rec, lazy_rec = _Recorder(), _Recorder()
        with self.assertRaises(FlowExecutionException):
            FlowExecution.run(_failing_flow(), {}, callback_handlers=[eager_rec])
        gen = FlowExecution.run(_failing_flow(), {}, mode="generator", callback_handlers=[lazy_rec])
        with self.assertRaises(FlowExecutionException):
            list(gen)
        self.assertEqual(eager_rec.events[0]["error"], lazy_rec.events[0]["error"])
        self.assertEqual(eager_rec.events[0]["exc_type"], lazy_rec.events[0]["exc_type"])


class UnchangedPathsTest(unittest.TestCase):
    def test_eager_success_unchanged(self):
        rec = _Recorder()
        result = FlowExecution.run(_ok_flow(), {}, callback_handlers=[rec])
        self.assertEqual(result, {"ok": True})
        self.assertEqual(rec.events, [{"result": {"ok": True}, "error": None, "exc_type": None}])

    def test_lazy_clean_exit_unchanged(self):
        rec = _Recorder()
        list(FlowExecution.run(_ok_flow(), {}, mode="generator", callback_handlers=[rec]))
        self.assertEqual(rec.events, [{"result": None, "error": None, "exc_type": None}])

    def test_non_fee_exception_normalization_unchanged(self):
        """非 FEE 异常：-500 包装 + on_flow_end 一次（既有行为零变化）。"""
        flow = MagicMock()
        cb = MagicMock()

        async def failing():
            raise RuntimeError("raw")

        import asyncio
        from plaita.core.errors import FlowErrorException
        with self.assertRaises(FlowErrorException):
            asyncio.run(finish_normal(failing(), flow, cb))
        cb.on_flow_end.assert_called_once()
        args, kwargs = cb.on_flow_end.call_args
        self.assertEqual(args[2], {"code": -500, "message": "raw"})
        self.assertIs(kwargs.get("exception"), cb.on_flow_end.call_args.kwargs["exception"])

    def test_clean_exit_still_result_none(self):
        """lazy finally 无异常 → 历史 result=None 形态保留。"""
        flow = MagicMock()
        cb = MagicMock()
        emit_flow_end_on_close(flow, None, cb)
        cb.on_flow_end.assert_called_once_with(flow, result=None)

    def test_non_fee_on_close_unchanged(self):
        flow = MagicMock()
        cb = MagicMock()
        exc = RuntimeError("oops")
        emit_flow_end_on_close(flow, exc, cb)
        args, kwargs = cb.on_flow_end.call_args
        self.assertIs(args[1], None)
        self.assertEqual(args[2], {"code": -500, "message": "oops"})
        self.assertIs(kwargs.get("exception"), exc)


if __name__ == "__main__":
    unittest.main()
