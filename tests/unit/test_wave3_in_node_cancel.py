"""波次③ 步内可中断（in-node cancel）单测——设计稿 §3.3、目标 A。

现状缺口：取消标志键只在**步界**检查，一个节点跑起来就打断不了——agentrun
默认 ``timeout_secs=1800``，取消后最多白等 30 分钟。

本测试覆盖新加的「取消监听」闭环：

- T-in-1  取消监听线程周期性轮询标志键，命中即 ``execution.cancel()``
  （同时置位 ``cancel_event`` / ``cancel_requested``）；
- T-in-2  端到端：真实 worker 步循环 + 真实 code 沙箱节点（``sleep``）——
  并发写取消标志键 → 节点在**远小于自身超时**的时间内被 killpg 中止，
  执行终态化为 ``cancelled``；
- T-in-3  回滚开关 ``PLAITA_DISABLE_CANCEL_INTERRUPT=1`` 不启动监听线程；
- T-in-4  协作节点（agentrun）把 ``execution.cancel_event`` 透传给 agentproc
  ``RunOptions``（用 mock 断言，不真跑 agent CLI）；
- T-in-5  无 redis 客户端的内存 worker 登记为空转、``_cancel_poll_once`` no-op。
"""
import time
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

import fakeredis

from plaita.core.executor import FlowExecution
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker
from plaita.server.tenant_context import reset_current_tenant, set_current_tenant
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


@pytest.fixture(autouse=True)
def _register_code_node_scoped():
    """在默认注册表临时启用 code 节点（subprocess 后端）。

    ``register_code_node`` 会改写模块级 ``_DEFAULT_SANDBOX_BACKEND`` /
    ``_ALLOWED_SANDBOX_BACKENDS`` 并注册到**进程级默认注册表**——import 期调用
    会污染同进程其他测试模块（如 test_code_default_backend 期望 docker 默认）。
    故只在测试运行期注册，退出时完整还原。
    """
    from plaita.node import get_default_registry, register_code_node
    from plaita.node import code as code_mod

    registry = get_default_registry()
    saved_default = code_mod._DEFAULT_SANDBOX_BACKEND
    saved_allowed = code_mod._ALLOWED_SANDBOX_BACKENDS
    saved_node = registry.get("code")
    register_code_node(default_backend="subprocess",
                       allowed_backends=["subprocess", "restricted"])
    try:
        yield
    finally:
        code_mod._DEFAULT_SANDBOX_BACKEND = saved_default
        code_mod._ALLOWED_SANDBOX_BACKENDS = saved_allowed
        if saved_node is None:
            registry.unregister("code")
        else:
            registry.register(saved_node)


# 长跑 code 节点：sleep 60s（其 subprocess 沙箱墙钟由 PLAITA_SANDBOX_TIMEOUT
# 控制——测试里放大到 60s，取消命中时必须在 ~几秒内被杀，远小于 60s）。
_LONG_RUNNING_FLOW = {
    "flow_id": "f-cancel",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "slow"},
        {
            "id": "slow",
            "type": "code",
            "language": "python",
            "sandbox_backend": "subprocess",
            "code": "def run(inp=None) -> str:\n    import time\n    time.sleep(60)\n    return 'slept'\n",
            "next": "end",
        },
        {"id": "end", "type": "end", "output": "$NODE.slow"},
    ],
}

_CANCEL_KEY = "plaita:execution:cancel:exec-in-1"


def _make_worker(fake_redis, storage=None, flow_storage=None, **kwargs):
    kwargs.setdefault("lease_ttl_seconds", 60)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:wave3-in-node",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
        **kwargs,
    )


def _save_running_state(storage, execution_id="exec-in-1", context=None):
    state = ExecutionState(
        execution_id=execution_id,
        flow_id="f-cancel",
        flow_version="1",
        context=context if context is not None else {"$LAST_NODE": "start", "$NODE": {}},
        status="running",
    )
    storage.save_execution_state(execution_id, state)
    return state


class TestCancelWatcherUnit:
    """取消监听线程单元行为。"""

    def test_poll_once_calls_execution_cancel_when_flag_present(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        execution = MagicMock()
        worker._register_cancel_watch("exec-in-1", execution)

        # 无标志 → 不 cancel
        worker._cancel_poll_once()
        execution.cancel.assert_not_called()

        # 写标志 → 命中，cancel() 被调、登记被撤（幂等，再轮询不再调）
        fake.set(_CANCEL_KEY, "ts", ex=7 * 86400)
        worker._cancel_poll_once()
        assert execution.cancel.call_count == 1
        worker._cancel_poll_once()
        assert execution.cancel.call_count == 1, "命中后应撤登记，不重复 cancel"

    def test_poll_once_returns_none_when_no_active_executions(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        # 空登记表 → 不抛、no-op
        worker._cancel_poll_once()

    def test_disable_cancel_interrupt_does_not_start_thread(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        monkeypatch.setenv("PLAITA_DISABLE_CANCEL_INTERRUPT", "1")
        worker._start_cancel_watcher()
        assert worker._cancel_thread is None
        worker._stop_cancel_watcher()

    def test_watcher_starts_and_stops(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake, cancel_poll_seconds=0.05)
        worker._start_cancel_watcher()
        assert worker._cancel_thread is not None
        assert worker._cancel_thread.is_alive()
        worker._stop_cancel_watcher()
        assert worker._cancel_thread is None

    def test_memory_worker_registration_is_noop(self):
        worker = FlowWorker(MemoryExecutionStorage(), MemoryFlowStorage())
        # 基类登记/轮询安全：无 redis 客户端 → _cancel_requested False
        worker._register_cancel_watch("e1", MagicMock())
        worker._cancel_poll_once()
        worker._unregister_cancel_watch("e1")

    def test_watch_registration_follows_tenant_and_cancel(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        execution = MagicMock()
        token = set_current_tenant("tenant-x")
        try:
            worker._register_cancel_watch("exec-in-1", execution)
        finally:
            reset_current_tenant(token)
        # 租户路由：标志写在 tenant-x 命名空间下
        fake.set("plaita:tenant-x:execution:cancel:exec-in-1", "ts", ex=7 * 86400)
        worker._cancel_poll_once()
        assert execution.cancel.call_count == 1


class TestInNodeCancelEndToEnd:
    """端到端：真实 worker 步循环 + 真实 code 沙箱长跑节点。"""

    def test_long_running_code_node_is_killed_far_before_its_timeout(self, monkeypatch):
        # subprocess 沙箱墙钟放到 60s——若取消不生效，测试会挂 60s 才超时；
        # 取消生效则应在几秒内被 killpg 中止。
        monkeypatch.setenv("PLAITA_SANDBOX_TIMEOUT", "60")

        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(_LONG_RUNNING_FLOW)
        _save_running_state(storage)

        worker = _make_worker(
            fake, storage, flow_storage,
            cancel_poll_seconds=0.1,  # 缩短轮询间隔，加速测试
        )
        # 启动取消监听线程（run() 里的调用点）
        worker._start_cancel_watcher()
        try:
            # 并发写取消标志键——模拟 console 取消
            import threading

            def _cancel_writer():
                time.sleep(2.0)  # 等节点确实进入 sleep
                fake.set(_CANCEL_KEY, "ts", ex=7 * 86400)

            t = threading.Thread(target=_cancel_writer, daemon=True)
            t.start()

            start = time.monotonic()
            # 走 resume 入口（无 start 幂等键负担；标志在 2s 后才写，故入口
            # 检查不会短路，进入步循环后由监听线程中止在途节点）
            result = worker.resume_flow("f-cancel", "exec-in-1", "continue")
            elapsed = time.monotonic() - start
            t.join(timeout=2)
        finally:
            worker._stop_cancel_watcher()

        # 断言：中止耗时远小于节点自身 60s 超时（留足 CI 余量：< 20s）
        assert elapsed < 20, f"取消未在途中断，耗 {elapsed:.1f}s（节点超时 60s）"
        state = storage.load_execution_state("exec-in-1")
        assert state.status == "cancelled", f"期望 cancelled，实际 {state.status}"
        assert state.end_time is not None
        # 租约正确释放（finally）
        assert fake.get("plaita:execution:lease:exec-in-1") is None

    def test_cancel_watch_unregistered_after_run(self, monkeypatch):
        """执行结束后取消登记被撤销（无泄漏）。"""
        monkeypatch.setenv("PLAITA_SANDBOX_TIMEOUT", "60")
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        # 短 flow：立即 end
        flow_storage.save_flow({
            "flow_id": "f-quick",
            "version": "1",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "ok"},
            ],
        })
        state = ExecutionState(
            execution_id="exec-in-2", flow_id="f-quick", flow_version="1",
            context={"$LAST_NODE": "start", "$NODE": {}}, status="running",
        )
        storage.save_execution_state("exec-in-2", state)
        worker = _make_worker(fake, storage, flow_storage)
        worker._start_cancel_watcher()
        try:
            worker.resume_flow("f-quick", "exec-in-2", "continue")
        finally:
            worker._stop_cancel_watcher()
        assert "exec-in-2" not in worker._cancel_watch


class TestAgentRunCancelWiring:
    """agentrun 协作取消透传（不真跑 agent CLI）。"""

    def test_agentrun_passes_cancel_event_to_runoptions(self):
        pytest.importorskip("plaita_nodes")
        from plaita_nodes.agent_run import AgentRunNode
        import threading

        cancel_event = threading.Event()
        node = AgentRunNode(id="a", agent="glm-52", prompt="hi", timeout_secs=1800)

        captured = {}

        class _FakeResult:
            error = ""
            exit_code = 0
            reply = "{}"
            session_id = ""
            usage = None
            timed_out = False

        def _fake_agentproc_run(profile, options):
            captured["cancel_event"] = options.cancel_event
            return _FakeResult()

        execution = MagicMock()
        execution.evaluate.side_effect = lambda x: x
        execution.get_global_variable.return_value = False
        execution.cancel_event = cancel_event

        with patch("plaita_nodes.agent_run.register_recursive_direct"), \
             patch("plaita_nodes.agent_run.resolve_agent",
                   return_value={"executor": "recursive", "env": {}, "model": None}), \
             patch("agentproc.runner.run", side_effect=_fake_agentproc_run):
            # 别名 recursive → recursive-direct（register_recursive_direct 注册的
            # 名字）；本测只关心透传，手工补注册名。
            with patch.dict("agentproc.EXECUTORS",
                            {"recursive-direct": {"cli_name": "recursive"}}, clear=False):
                try:
                    node.execute(execution)
                except Exception:
                    pass  # 结果解析可能失败，我们只关心 cancel_event 透传

        assert captured.get("cancel_event") is cancel_event, (
            "agentrun 未把 execution.cancel_event 透传给 agentproc RunOptions"
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
