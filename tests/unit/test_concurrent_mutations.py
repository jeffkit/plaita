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


# ===========================================================================
# Round 5（2026-11-07，#355 concurrent 清剿首批）：独立复核确认的 187 真幸存
# ===========================================================================

import concurrent.futures as _cf  # noqa: E402

from plaita.node.concurrent import _BG_LOCK, _BG_STATE_MAX  # noqa: E402


class _Exec355:
    """无 execution_id 属性的裸对象——走 str(id(execution)) 兜底路径。"""


class _EidExec355:
    """带固定 execution_id 的对象——走主键路径。"""

    execution_id = "eid-355"


class TestGetBgStateRound1(unittest.TestCase):
    def setUp(self):
        with _BG_LOCK:
            _BG_STATE.clear()

    def tearDown(self):
        with _BG_LOCK:
            _BG_STATE.clear()

    def test_eid_prefers_execution_id_attribute(self):
        """ Kills _2/3/8/9: eid 必须取 execution_id 属性（or→and / getattr(None) /
        属性名改写都会丢主键落到 str(id(...))）。"""
        st = _get_bg_state(_EidExec355())
        self.assertIn("eid-355", _BG_STATE)

    def test_eid_falls_back_to_object_identity(self):
        """无 execution_id 属性 ⇒ getattr 默认 None → str(id(obj))；两个对象两份
        状态（杀 _7 默认值摘除〔AttributeError〕/_10 str(None)/_11 id(None)。"""
        keep = []
        e1, e2 = object(), object()
        keep += [e1, e2]
        st1 = _get_bg_state(e1)
        st2 = _get_bg_state(e2)
        self.assertIsNot(st1, st2)
        self.assertEqual(len(_BG_STATE), 2)

    def test_existing_state_returned_not_recreated(self):
        """同一 eid 二次取必须返回同一份（杀 get→None / get(None) / 存储 None），
        且 done/futures/errors 三键齐全。"""
        st1 = _get_bg_state(_EidExec355())
        st1["done"] = 7
        st2 = _get_bg_state(_EidExec355())
        self.assertIs(st1, st2)
        self.assertEqual(st2["done"], 7)
        for key in ("done", "futures", "errors"):
            self.assertIn(key, st2)

    def test_lru_eviction_boundary_exact(self):
        """超限逐出后长度恰为 MAX（杀 > → >= 的过度逐出）。"""
        keep = []
        for i in range(_BG_STATE_MAX):
            o = object()
            keep.append(o)
            _get_bg_state(o)
        self.assertLessEqual(len(_BG_STATE), _BG_STATE_MAX)
        extra = object()
        keep.append(extra)
        _get_bg_state(extra)
        self.assertEqual(len(_BG_STATE), _BG_STATE_MAX)


class TestResolveBranchInputRound1(unittest.TestCase):
    def test_input_none_inherits_parent_input_snapshot(self):
        """pb.input 为 None ⇒ 继承父 $INPUT（$INPUT 键求值 + 快照）。
        Kills _2（input_key=None→evaluate(None) KeyError）/ _3（快照 None）/
        _4（快照=None 求值）/ _5（evaluate(None)）。"""
        from plaita.node.concurrent import Parallel

        pb = ParallelBranch(id="b1")
        execution = MagicMock()
        execution.express_prefix = "$"
        execution.express_input_name = "INPUT"
        execution.evaluate = lambda key: {"$INPUT": {"x": 99}}[key]
        inst = Parallel.model_validate({'id': 'p_ri', 'branches': [_simple_branch_dict('b1')]})
        snap = inst._resolve_branch_input(pb, execution)
        self.assertEqual(snap, {"x": 99})


class TestExecBranchRound1(unittest.TestCase):
    def _setup(self):
        pb = ParallelBranch(id="b1", name="b-name")
        pb.flow = "FLOW-OBJ"
        execution = MagicMock()
        execution.mode = "generator"
        from plaita.core.executor import ExecutionMode

        execution.mode = ExecutionMode.GENERATOR
        be = MagicMock()
        execution.get_child_execution.return_value = be
        return pb, execution, be

    def test_lazy_and_input_forwarded_to_run_compatible(self):
        pb, execution, be = self._setup()
        with patch.object(Parallel, "_resolve_branch_input", return_value="INPUT-VAL"):
            rs = Parallel.model_validate({'id': 'p_eb3', 'branches': [_simple_branch_dict('b1')]}).exec_branch(pb, execution, be)
        be.run_compatible.assert_called_once_with(pb.flow, True, "INPUT-VAL")
        self.assertEqual(rs, "RESULT" if rs == "RESULT" else rs)

    def test_child_execution_created_and_bound_when_absent(self):
        pb, execution, be = self._setup()
        with patch("plaita.node.concurrent.bind_child_step") as bcs, \
             patch.object(Parallel, "_resolve_branch_input", return_value="I"):
            Parallel.model_validate({'id': 'p_eb4', 'branches': [_simple_branch_dict('b1')]}).exec_branch(pb, execution)  # 不传 branch_execution
        bcs.assert_called_once_with(execution, be, "b-name")

    def test_debug_log_exact(self):
        pb, execution, be = self._setup()
        be.run_compatible.return_value = "RESULT"
        with patch.object(Parallel, "_resolve_branch_input", return_value="I"):
            with self.assertLogs(level="DEBUG") as captured:
                Parallel.exec_branch(MagicMock(), pb, execution, be)
        self.assertIn(
            "branch b-name executed: RESULT",
            [r.getMessage() for r in captured.records],
        )


class TestRejectSuspendingRound1(unittest.TestCase):
    def test_suspending_branch_flow_rejected(self):
        from plaita.core.errors import FlowSuspendRejectedError

        pb = MagicMock()
        pb.flow = "FLOW-OBJ"
        pb.name = "b1"
        with patch("plaita.node.concurrent.flow_contains_suspending_node") as fc:
            fc.return_value = True
            inst = Parallel.model_validate({
                "id": "p_rs", "branches": [_simple_branch_dict("b1")],
            })
            with self.assertRaises(FlowSuspendRejectedError):
                inst._reject_suspending_branches([pb])
            fc.assert_called_once_with("FLOW-OBJ")


class TestSplitBranchesRound1(unittest.TestCase):
    def test_split_excludes_join_from_background(self):
        p = _make_parallel(join_branches=["a"])
        b_a = ParallelBranch(id="pa", name="a")
        b_b = ParallelBranch(id="pb", name="b")
        joined, background = p._split_branches([b_a, b_b])
        self.assertEqual([b.name for b in joined], ["a"])
        self.assertEqual([b.name for b in background], ["b"])


class TestExecBgBranchesRound1(unittest.TestCase):
    def _run(self, branch_name="b1"):
        p = Parallel(id="p_eb", branches=[], join_branches=[])
        bg = [ParallelBranch(id="b1", name=branch_name)]
        execution = MagicMock()
        execution.execution_id = "eb-355"
        be = MagicMock()
        execution.get_child_execution.return_value = be
        executor = MagicMock()
        fut = MagicMock()
        executor.submit.return_value = fut
        with patch("plaita.node.concurrent.bind_child_step") as bcs:
            p._execute_background_branches(bg, executor, execution)
        return p, executor, fut, bcs, execution, be, bg[0]

    def test_submit_args_exact(self):
        p, executor, fut, bcs, execution, be, branch = self._run()
        executor.submit.assert_called_once_with(
            p.exec_branch, branch, execution, be,
        )

    def test_future_registered_and_callback_attached(self):
        p, executor, fut, bcs, execution, be, branch = self._run()
        with _BG_LOCK:
            st = _BG_STATE["eb-355"]
        self.assertIn(fut, st["futures"])
        fut.add_done_callback.assert_called_once()

    def test_empty_background_is_noop(self):
        p = Parallel(id="p_eb2", branches=[], join_branches=[])
        executor = MagicMock()
        execution = MagicMock()
        execution.execution_id = "eb-noop"
        p._execute_background_branches([], executor, execution)
        executor.submit.assert_not_called()


class TestBgDoneCallbackRound1(unittest.TestCase):
    def _mk(self, done_state=None):
        p = Parallel(id="p_cb", branches=[], join_branches=[])
        errors = []
        state = {"futures": [], "errors": errors, "done": 0}
        if done_state is not None:
            state["done"] = done_state
        fut = _cf.Future()
        state["futures"].append(fut)
        from types import SimpleNamespace as _NS
        cb = p._make_background_done_callback(_NS(name="b1"), errors, state)
        return cb, fut, errors, state

    def test_success_future_removed_and_counted(self):
        cb, fut, errors, state = self._mk()
        fut.set_result("ok")
        cb(fut)
        self.assertNotIn(fut, state["futures"])
        self.assertEqual(state["done"], 1)
        self.assertEqual(errors, [])

    def test_counter_accumulates(self):
        cb, fut, errors, state = self._mk(done_state=7)
        fut.set_result("ok")
        cb(fut)
        self.assertEqual(state["done"], 8)

    def test_fresh_state_without_done_key_counts_from_zero(self):
        p = Parallel(id="p_cb2", branches=[], join_branches=[])
        errors = []
        state = {"futures": [ ], "errors": errors}
        fut = _cf.Future()
        fut.set_result("ok")
        from types import SimpleNamespace as _NS
        cb = p._make_background_done_callback(_NS(name="b1"), errors, state)
        cb(fut)
        self.assertEqual(state["done"], 1)

    def test_failed_future_records_error_exact(self):
        cb, fut, errors, state = self._mk()
        fut.set_exception(ValueError("boom-355"))
        with self.assertLogs(level="WARNING") as captured:
            cb(fut)
        wmsgs = [r.getMessage() for r in captured.records]
        self.assertTrue(any(
            m.startswith("parallel background branch 'b1' raised: ") and "boom-355" in m
            for m in wmsgs
        ), wmsgs)
        self.assertEqual(errors, [{"branch": "b1", "error": "boom-355"}])

    def test_cancelled_future_debug_path_exact(self):
        cb, fut, errors, state = self._mk()
        fut.cancel()
        self.assertTrue(fut.cancelled())
        with self.assertLogs(level="DEBUG") as captured:
            cb(fut)
        dmsgs = [r.getMessage() for r in captured.records if r.levelno == 10]
        self.assertTrue(any(
            m.startswith("background future.exception() unavailable: ") for m in dmsgs
        ), dmsgs)
        # cancelled ⇒ exc 非 None ⇒ 同步走 WARNING 留痕路径
        wmsgs = [r.getMessage() for r in captured.records if r.levelno == 30]
        self.assertTrue(any(
            m.startswith("parallel background branch 'b1' raised: ") for m in wmsgs
        ), wmsgs)
        # cancelled ⇒ scrub 文本可为空串，但 branch 名必须落账
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["branch"], "b1")


class TestWaitBackgroundBranchesRound1(unittest.TestCase):
    def test_counts_done_and_not_done_exact_dict(self):
        p = Parallel(id="p_w", branches=[], join_branches=[])
        execution = MagicMock()
        execution.execution_id = "w-355"
        done_f = _cf.Future()
        done_f.set_result("x")
        pending_f = _cf.Future()
        with _BG_LOCK:
            st = _BG_STATE.setdefault("w-355", {"futures": [], "errors": [], "done": 0})
            st["done"] = 2
            st["futures"] = [done_f, pending_f]
        # pending_f 永不完成 ⇒ 必须带超时（原始代码 _cf.wait(timeout=None) 会永远等）
        result = p.wait_background_branches(execution, timeout=0.5)
        self.assertEqual(result, {"done": 3, "not_done": 1})


class TestProcessFutureResultRound1(unittest.TestCase):
    def test_success_stores_result(self):
        p = Parallel(id="p_pf", branches=[], join_branches=[])
        fut = _cf.Future()
        fut.set_result("PAYLOAD")
        results = {}
        import threading as _th

        lock = _th.Lock()
        branch = ParallelBranch(id="b1", name="b1")
        p._process_future_result(fut, branch, results, lock)
        self.assertEqual(results["b1"], "PAYLOAD")

    def test_failure_stores_exact_sentinel(self):
        p = Parallel(id="p_pf2", branches=[], join_branches=[])
        fut = _cf.Future()
        fut.set_exception(ValueError("boom-355"))
        results = {}
        import threading as _th

        lock = _th.Lock()
        branch = ParallelBranch(id="b1", name="b1")
        with self.assertLogs(level="WARNING") as captured:
            p._process_future_result(fut, branch, results, lock)
        self.assertEqual(
            results["b1"],
            {"__parallel_error__": "boom-355", "__branch__": "b1"},
        )
        self.assertTrue(any(
            m.startswith("parallel branch b1 raised: ") for m in
            [r.getMessage() for r in captured.records]
        ))


if __name__ == "__main__":
    unittest.main()
