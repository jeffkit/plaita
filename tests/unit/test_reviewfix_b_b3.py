"""review-fix B3 回归：lazy run prepare 抛错不得毒化 FlowExecution 实例。

历史行为：``run_compatible``/``arun_compatible`` 在 ``_begin_run()`` 之后调用
``_prepare_strategy``；lazy=True 时 ``finally`` 只在非 lazy 才复位 ``_running``，
而 ``on_lazy_finally`` 只在 generator 成功交出后才可能注册——prepare 一抛错，
``_running`` 永久卡 True，同实例下次 run 报 "already running" 伪装真实死因。

伴生修复：``setup_flow`` 对非法 ``expose_env`` 先验证后提交，避免抛错路径把
脏 allowlist 留在实例上、把下次 ``clean()`` 也炸掉。
"""
from __future__ import annotations

import unittest

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow


_BAD_DEF = {
    "flow_id": "b3-bad",
    "exposeEnv": ["*"],  # 非法通配符 → setup_flow 校验抛 ValueError
    "nodes": [
        {"id": "s", "type": "start", "name": "start", "next": "e"},
        {"id": "e", "type": "end", "name": "end",
         "resultType": "success", "output": "ok"},
    ],
}

_GOOD_DEF = {
    "flow_id": "b3-good",
    "nodes": [
        {"id": "s", "type": "start", "name": "start", "next": "e"},
        {"id": "e", "type": "end", "name": "end",
         "resultType": "success", "output": "ok"},
    ],
}


class TestLazyPrepareFailureUnpoisonsInstance(unittest.TestCase):
    def test_sync_lazy_failure_resets_running_and_instance_reusable(self):
        """lazy run 抛错后 _running 复位，同实例可再跑正常 flow。"""
        bad = Flow.model_validate(_BAD_DEF)
        good = Flow.model_validate(_GOOD_DEF)
        execution = FlowExecution()

        with self.assertRaises(ValueError) as cm:
            list(execution.run_compatible(bad, True, {}))
        self.assertIn("expose_env", str(cm.exception))
        self.assertFalse(execution._running,
                         "prepare 抛错后 _running 必须复位")

        result = execution.run_compatible(good, False, {})
        self.assertEqual(result, "ok")

    def test_rerunning_same_bad_flow_reports_original_error(self):
        """同实例重跑同一个坏 flow：报原始 expose_env 错误而非 already running。"""
        bad = Flow.model_validate(_BAD_DEF)
        execution = FlowExecution()

        with self.assertRaises(ValueError):
            list(execution.run_compatible(bad, True, {}))
        with self.assertRaises(ValueError) as cm2:
            list(execution.run_compatible(bad, True, {}))
        self.assertIn("expose_env", str(cm2.exception))
        self.assertNotIn("already running", str(cm2.exception))

    def test_bad_prepare_does_not_leave_dirty_expose_env(self):
        """setup_flow 抛错路径不得把非法 allowlist 留在实例上。"""
        bad = Flow.model_validate(_BAD_DEF)
        good = Flow.model_validate(_GOOD_DEF)
        execution = FlowExecution()

        with self.assertRaises(ValueError):
            list(execution.run_compatible(bad, True, {}))
        self.assertNotIn("*", execution._ctx.expose_env,
                         "非法 allowlist 不得提交到实例")

        # 同实例再跑好 flow：clean() 不得被脏 allowlist 炸掉
        self.assertEqual(execution.run_compatible(good, False, {}), "ok")

    def test_lazy_success_still_holds_running_until_close(self):
        """修复不得改变既有语义：lazy 成功交出 generator 后 _running 仍持有，
        直到 generator 耗尽/关闭才复位。"""
        good = Flow.model_validate(_GOOD_DEF)
        execution = FlowExecution()
        gen = execution.run_compatible(good, True, {})
        self.assertTrue(execution._running)
        list(gen)
        self.assertFalse(execution._running)

    def test_async_lazy_failure_resets_running_and_instance_reusable(self):
        """async 路径同修：arun_compatible lazy 抛错后实例可复用。"""
        import asyncio

        bad = Flow.model_validate(_BAD_DEF)
        good = Flow.model_validate(_GOOD_DEF)
        execution = FlowExecution()

        async def scenario():
            with self.assertRaises(ValueError) as cm:
                agen = await execution.arun_compatible(bad, True, {})
                async for _ in agen:
                    pass
            self.assertIn("expose_env", str(cm.exception))
            self.assertFalse(execution._running)

            result = await execution.arun_compatible(good, False, {})
            self.assertEqual(result, "ok")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
