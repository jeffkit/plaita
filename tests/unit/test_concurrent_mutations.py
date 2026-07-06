"""变异测试专项断言 — plaita.node.concurrent

针对 mutmut 扫描出的 26 个 survived 变异逐一构造最小杀灭测试。
每个测试类顶部注释标明对应的变异 ID 和代码行为。

运行方式（在 pyproject.toml 已将 test_concurrent_mutations.py 纳入
pytest_add_cli_args_test_selection 的前提下）:
    PYENV_VERSION=loki mutmut run
"""
from __future__ import annotations

import asyncio
import threading
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from plaita.node.concurrent import (
    COROUTINE,
    PROCESS,
    THREAD,
    Parallel,
    ParallelBranch,
    _BG_STATE,
    _get_bg_state,
)


# ---------------------------------------------------------------------------
# Helpers (尽量不依赖真实 Flow/Execution，只 mock 必要接口)
# ---------------------------------------------------------------------------

def _simple_branch_dict(name: str, val=1) -> dict:
    return {
        "name": name,
        "input": val,
        "flow": {
            "id": f"f_{name}",
            "version": "0.1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
            ],
        },
    }


def _make_parallel(mode: str = THREAD, join_branches: list[str] | None = None) -> Parallel:
    jb = join_branches or ["b1"]
    return Parallel.model_validate({
        "id": "p_mut",
        "mode": mode,
        "branches": [_simple_branch_dict("b1", 1)],
        "join_branches": jb,
    })


def _fresh_execution(exec_id: str = "exec-mut") -> MagicMock:
    m = MagicMock()
    m.execution_id = exec_id
    return m


# ---------------------------------------------------------------------------
# wait_background_branches
#   mutmut_1: futures = None
#   mutmut_2: _get_bg_state(None)["futures"]  (wrong execution)
#   mutmut_5: if futures:   (inverted guard)
# ---------------------------------------------------------------------------

class TestWaitBackgroundBranchesKillMutations(unittest.TestCase):
    """三个 survived 变异均依赖「当有已完成 Future 时 done 应非零」这条断言。

    mutmut_1 (futures=None): None 是假值 → 早返回 done=0, not_done=0 → 断言失败
    mutmut_2 (wrong exec):   wrong key → futures=[] → 早返回 0/0 → 断言失败
    mutmut_5 (if futures:):  非空时进入早返回 → done=0 → 断言失败
    """

    def setUp(self):
        # 每个测试前清空全局后台状态，避免交叉污染
        _BG_STATE.clear()

    def test_wait_returns_correct_done_count_when_futures_exist(self):
        """有已完成 Future 时 done 应 == 1（杀灭 mutmut_1 / mutmut_2 / mutmut_5）。"""
        p = _make_parallel()
        execution = _fresh_execution("wait-test-exec")

        # 创建并注册一个已完成的 future
        fut: Future = Future()
        fut.set_result("done_value")
        state = _get_bg_state(execution)
        state["futures"].append(fut)

        result = p.wait_background_branches(execution)

        self.assertEqual(result["done"], 1, "至少一个 future 已完成，done 应为 1")
        self.assertEqual(result["not_done"], 0)

    def test_wait_uses_correct_execution_not_none(self):
        """不同 execution_id 下状态隔离，确认确实用了传入的 execution（杀灭 mutmut_2）。"""
        p = _make_parallel()
        exec_a = _fresh_execution("exec-A")
        exec_b = _fresh_execution("exec-B")

        fut: Future = Future()
        fut.set_result(42)
        # 只给 exec_a 添加 future
        _get_bg_state(exec_a)["futures"].append(fut)

        result_b = p.wait_background_branches(exec_b)
        # exec_b 没有任何 future → 早返回 0/0 是预期行为
        self.assertEqual(result_b, {"done": 0, "not_done": 0})

        # exec_a 有 future → done 应为 1，证明方法使用了正确的 execution
        result_a = p.wait_background_branches(exec_a)
        self.assertEqual(result_a["done"], 1)

    def test_empty_futures_returns_zeros(self):
        """无 future 时应早返回 done=0（正常路径的基线验证）。"""
        p = _make_parallel()
        execution = _fresh_execution("wait-empty-exec")
        # 不往 state 里加 future
        result = p.wait_background_branches(execution)
        self.assertEqual(result, {"done": 0, "not_done": 0})


# ---------------------------------------------------------------------------
# thread_execute
#   mutmut_1: pool_execute(None, execution)   — THREAD → None
#   mutmut_2: pool_execute(THREAD, None)      — execution → None
#   mutmut_3: pool_execute(execution)          — 少一个位置参数
#   mutmut_4: pool_execute(THREAD, )           — execution 被删
# ---------------------------------------------------------------------------

class TestThreadExecuteArgs(unittest.TestCase):
    """thread_execute 必须以 (THREAD, execution) 精确参数调用 pool_execute。"""

    def test_thread_execute_passes_exact_args_to_pool_execute(self):
        """assert_called_once_with 同时杀灭 mutmut_1/2/3/4。"""
        p = _make_parallel(mode=THREAD)
        execution = _fresh_execution()
        with patch.object(Parallel, "pool_execute", return_value={"b1": 1}) as mock_pool:
            result = p.thread_execute(execution)
        mock_pool.assert_called_once_with(THREAD, execution)
        self.assertEqual(result, {"b1": 1})

    def test_thread_execute_not_none_for_mode_arg(self):
        """专门验证第一个参数是 THREAD 字符串而非 None（杀灭 mutmut_1）。"""
        p = _make_parallel(mode=THREAD)
        execution = _fresh_execution()
        captured: list = []
        with patch.object(Parallel, "pool_execute", side_effect=lambda *a: captured.extend(a) or {}) as _:
            p.thread_execute(execution)
        # captured = [THREAD, execution]
        self.assertEqual(captured[0], THREAD)
        self.assertIs(captured[1], execution)

    def test_thread_execute_not_none_for_execution_arg(self):
        """专门验证第二个参数是真实 execution 对象而非 None（杀灭 mutmut_2）。"""
        p = _make_parallel(mode=THREAD)
        execution = _fresh_execution("specific-exec")
        captured: list = []
        with patch.object(Parallel, "pool_execute", side_effect=lambda *a: captured.extend(a) or {}):
            p.thread_execute(execution)
        self.assertIsNotNone(captured[1])
        self.assertIs(captured[1], execution)


# ---------------------------------------------------------------------------
# process_execute
#   mutmut_1: pool_execute(None, execution)
#   mutmut_2: pool_execute(PROCESS, None)
#   mutmut_3: pool_execute(execution)
#   mutmut_4: pool_execute(PROCESS, )
# ---------------------------------------------------------------------------

class TestProcessExecuteArgs(unittest.TestCase):
    """process_execute 必须以 (PROCESS, execution) 精确参数调用 pool_execute。"""

    def test_process_execute_passes_exact_args_to_pool_execute(self):
        """杀灭 mutmut_1/2/3/4。"""
        p = _make_parallel(mode=PROCESS)
        execution = _fresh_execution()
        with patch.object(Parallel, "pool_execute", return_value={"b1": 2}) as mock_pool:
            result = p.process_execute(execution)
        mock_pool.assert_called_once_with(PROCESS, execution)
        self.assertEqual(result, {"b1": 2})

    def test_process_execute_mode_constant_is_process(self):
        """专门验证第一个参数是 PROCESS 字符串（杀灭 mutmut_1）。"""
        p = _make_parallel(mode=PROCESS)
        execution = _fresh_execution()
        captured: list = []
        with patch.object(Parallel, "pool_execute", side_effect=lambda *a: captured.extend(a) or {}):
            p.process_execute(execution)
        self.assertEqual(captured[0], PROCESS)

    def test_process_execute_execution_is_not_none(self):
        """专门验证第二个参数是真实 execution（杀灭 mutmut_2）。"""
        p = _make_parallel(mode=PROCESS)
        execution = _fresh_execution("proc-exec")
        captured: list = []
        with patch.object(Parallel, "pool_execute", side_effect=lambda *a: captured.extend(a) or {}):
            p.process_execute(execution)
        self.assertIsNotNone(captured[1])
        self.assertIs(captured[1], execution)


# ---------------------------------------------------------------------------
# coroutine_execute
#   mutmut_1: 整段错误消息字符串被变为空/缩短
#   mutmut_2: "XXparallel mode='coroutine'...XX"  (XX 前后缀)
#   mutmut_3: "PARALLEL MODE='COROUTINE'..."       (全大写)
# ---------------------------------------------------------------------------

class TestCoroutineExecuteErrorMessage(unittest.TestCase):
    """coroutine_execute 必须抛 ValueError，消息内容必须精确（大小写、无 XX 前缀）。"""

    def setUp(self):
        self.p = _make_parallel(mode=COROUTINE)

    def _get_error_message(self) -> str:
        with self.assertRaises(ValueError) as ctx:
            self.p.coroutine_execute(MagicMock())
        return str(ctx.exception)

    def test_error_message_contains_expected_phrase(self):
        """消息包含关键短语（大小写敏感），杀灭 mutmut_1/2/3。"""
        msg = self._get_error_message()
        self.assertIn("parallel mode='coroutine' is no longer supported", msg)

    def test_error_message_is_lowercase_not_uppercase(self):
        """消息是小写，不是全大写（杀灭 mutmut_3: PARALLEL MODE=...）。"""
        msg = self._get_error_message()
        self.assertNotIn("PARALLEL MODE", msg)
        self.assertNotIn("COROUTINE", msg.split("'")[0])  # 不含大写 COROUTINE 在前缀中

    def test_error_message_has_no_xx_prefix_suffix(self):
        """消息无 XX 前缀/后缀（杀灭 mutmut_2: XXparallel...XX）。"""
        msg = self._get_error_message()
        self.assertNotIn("XX", msg)

    def test_error_message_mentions_thread_or_process(self):
        """消息提示替代方案 thread/process（确保完整性，辅助杀灭 mutmut_1）。"""
        msg = self._get_error_message()
        self.assertTrue(
            "thread" in msg.lower() or "process" in msg.lower(),
            f"Expected thread/process in error message, got: {msg!r}",
        )


# ---------------------------------------------------------------------------
# execute — execution 参数传递
#   mutmut_3: coroutine_execute(None)   (execution → None)
#   mutmut_6: process_execute(None)     (execution → None)
#   mutmut_9: thread_execute(None)      (execution → None)
# ---------------------------------------------------------------------------

class TestExecutePassesExecutionThrough(unittest.TestCase):
    """execute() 必须把 execution 原样透传给对应的内部方法，不能替换为 None。"""

    def test_execute_coroutine_mode_passes_execution_not_none(self):
        """execute(COROUTINE) → coroutine_execute(execution)，杀灭 mutmut_3。"""
        p = _make_parallel(mode=COROUTINE)
        execution = _fresh_execution("ce-exec")
        with patch.object(Parallel, "coroutine_execute", side_effect=ValueError("stub")) as mock_ce:
            with self.assertRaises(ValueError):
                p.execute(execution)
        mock_ce.assert_called_once_with(execution)
        # 确保不是 None
        actual_arg = mock_ce.call_args[0][0]
        self.assertIs(actual_arg, execution)

    def test_execute_process_mode_passes_execution_not_none(self):
        """execute(PROCESS) → process_execute(execution)，杀灭 mutmut_6。"""
        p = _make_parallel(mode=PROCESS)
        execution = _fresh_execution("pe-exec")
        with patch.object(Parallel, "process_execute", return_value={}) as mock_pe:
            p.execute(execution)
        mock_pe.assert_called_once_with(execution)
        actual_arg = mock_pe.call_args[0][0]
        self.assertIs(actual_arg, execution)

    def test_execute_thread_mode_passes_execution_not_none(self):
        """execute(THREAD) → thread_execute(execution)，杀灭 mutmut_9。"""
        p = _make_parallel(mode=THREAD)
        execution = _fresh_execution("te-exec")
        with patch.object(Parallel, "thread_execute", return_value={}) as mock_te:
            p.execute(execution)
        mock_te.assert_called_once_with(execution)
        actual_arg = mock_te.call_args[0][0]
        self.assertIs(actual_arg, execution)


# ---------------------------------------------------------------------------
# exec_branch_async — 精准断言
#   mutmut_6:  arun_compatible(pb.flow, None, input_value)  (False → None)
#   mutmut_12: logger.debug(None, pb.name, rs)              (format string → None)
#   mutmut_13: logger.debug("...", None, rs)                (pb.name → None)
#   mutmut_14: logger.debug("...", pb.name, None)           (rs → None)
#   mutmut_15: logger.debug(pb.name, rs)                    (移除格式串，参数整体移位)
#   mutmut_16: logger.debug("...", rs)                      (移除 pb.name)
#   mutmut_17: logger.debug("...", pb.name, )               (移除 rs)
#   mutmut_18: logger.debug("XXasync branch...XX", ...)     (XX 前后缀)
#   mutmut_19: logger.debug("ASYNC BRANCH...", ...)         (全大写)
# ---------------------------------------------------------------------------

class TestExecBranchAsyncMutations(unittest.IsolatedAsyncioTestCase):
    """使用 IsolatedAsyncioTestCase 避免 asyncio event loop 冲突。"""

    def _make_pb_and_execution(self, branch_name="mut_branch", input_val=99):
        pb = ParallelBranch.model_validate(_simple_branch_dict(branch_name, input_val))
        execution = MagicMock()
        child_exec = MagicMock()
        execution.get_child_execution.return_value = child_exec
        execution.evaluate.return_value = input_val
        return pb, execution, child_exec

    async def test_arun_compatible_called_with_false_not_none(self):
        """arun_compatible 第二参数必须是 False（非 None），杀灭 mutmut_6。"""
        p = _make_parallel()
        pb, execution, child_exec = self._make_pb_and_execution()

        # 记录调用参数
        call_args: list = []

        async def _capture_arun(flow, lazy, input_value):
            call_args.append({"flow": flow, "lazy": lazy, "input_value": input_value})
            return "result_from_arun"

        child_exec.arun_compatible = _capture_arun

        result = await p.exec_branch_async(pb, execution)

        self.assertEqual(result, "result_from_arun")
        self.assertEqual(len(call_args), 1, "arun_compatible 应被调用一次")
        # 精确断言 lazy=False（而非 None 或其他值）
        self.assertIs(call_args[0]["lazy"], False,
                      f"期望 lazy=False，实际: {call_args[0]['lazy']!r}")

    async def test_exec_branch_async_debug_log_contains_branch_name_and_result(self):
        """debug 日志必须同时含有 pb.name 和 rs 的字符串表示，杀灭 mutmut_12-19。

        mutmut_12 (None as fmt):   logging 格式化失败，消息错误/缺失
        mutmut_13 (pb.name→None):  日志含 None 而非分支名
        mutmut_14 (rs→None):       日志含 None 而非结果
        mutmut_15 (移除格式串):    pb.name 成为格式串，rs 成为参数，消息完全错位
        mutmut_16 (移除 pb.name):  日志只有 rs，无分支名
        mutmut_17 (移除 rs):        日志有分支名无结果
        mutmut_18 (XX 前缀后缀):   消息含 XX 字样
        mutmut_19 (全大写):         消息全大写
        """
        p = _make_parallel()
        pb, execution, child_exec = self._make_pb_and_execution(
            branch_name="log_test_branch", input_val=123
        )

        async def _mock_arun(flow, lazy, input_value):
            return "log_result_sentinel"

        child_exec.arun_compatible = _mock_arun

        with self.assertLogs("plaita.node.concurrent", level="DEBUG") as log_ctx:
            result = await p.exec_branch_async(pb, execution)

        self.assertEqual(result, "log_result_sentinel")

        combined_output = "\n".join(log_ctx.output)

        # 必须包含 pb.name（杀灭 mutmut_13/15/16）
        self.assertIn("log_test_branch", combined_output,
                      "日志应包含分支名 pb.name")

        # 必须包含 rs 的字符串表示（杀灭 mutmut_14/15/17）
        self.assertIn("log_result_sentinel", combined_output,
                      "日志应包含执行结果 rs")

        # 格式串关键词应为小写（杀灭 mutmut_19）
        self.assertIn("async branch", combined_output.lower(),
                      "日志应含 'async branch' 关键词")
        self.assertNotIn("ASYNC BRANCH", combined_output,
                         "日志格式串不应全大写（mutmut_19）")

        # 不应含 XX 前后缀（杀灭 mutmut_18）
        self.assertNotIn("XX", combined_output,
                         "日志不应含 XX 前缀/后缀（mutmut_18）")

    async def test_exec_branch_async_branch_name_not_none_in_log(self):
        """pb.name 出现在日志中而非 None（精确杀灭 mutmut_13）。"""
        p = _make_parallel()
        pb, execution, child_exec = self._make_pb_and_execution(branch_name="named_branch")

        async def _mock_arun(flow, lazy, input_value):
            return "value"

        child_exec.arun_compatible = _mock_arun

        with self.assertLogs("plaita.node.concurrent", level="DEBUG") as log_ctx:
            await p.exec_branch_async(pb, execution)

        combined = "\n".join(log_ctx.output)
        # mutmut_13 会把 pb.name 变成 None，日志将含 "None" 而不含 "named_branch"
        self.assertIn("named_branch", combined)
        # 不允许日志中把 None 误当分支名记录
        self.assertNotIn("DEBUG:plaita.node.concurrent:None", combined)

    async def test_exec_branch_async_result_not_none_in_log(self):
        """rs 出现在日志中而非 None（精确杀灭 mutmut_14）。"""
        p = _make_parallel()
        pb, execution, child_exec = self._make_pb_and_execution()

        async def _mock_arun(flow, lazy, input_value):
            return "actual_result_xyz"

        child_exec.arun_compatible = _mock_arun

        with self.assertLogs("plaita.node.concurrent", level="DEBUG") as log_ctx:
            await p.exec_branch_async(pb, execution)

        combined = "\n".join(log_ctx.output)
        # mutmut_14 把 rs 改成 None，日志将丢失真实结果
        self.assertIn("actual_result_xyz", combined)


if __name__ == "__main__":
    unittest.main()
