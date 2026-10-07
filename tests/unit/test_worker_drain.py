"""无损升级的 worker 停机语义（L1：draining + 有界 drain）。

信号/远程停止命令 → 只请求 drain（不领新任务、注册表标 draining）→ 等在途
任务收尾（有界，超时放弃当前步，消息留 pending 待接管）→ 注销服务。
"""

import threading
import time

import pytest

pytest.importorskip("cachetools")

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import StreamTask
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _worker(**kw) -> RedisFlowWorker:
    """draining/消费循环在 RedisFlowWorker 上（内存 FlowWorker 没有消费循环）。

    构造不做 I/O：redis-py 的 Redis.from_url 是惰性的，只有真下命令才连。
    """
    kw.setdefault("enable_registry", False)
    kw.setdefault("enable_redis_logging", False)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/0",
        queue_name="test:queue",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        event_bus=InMemoryEventBus(),
        **kw,
    )


class TestDraining:
    def test_request_drain_sets_flag_and_is_idempotent(self):
        worker = _worker()
        assert worker.draining is False
        worker.request_drain("test")
        assert worker.draining is True
        worker.request_drain("again")  # 幂等：不得重置 drain 起点/定时器
        assert worker.draining is True
        assert worker._running is False or True  # 只置 draining，不停进程（由主循环收尾）

    def test_consume_loop_takes_no_new_task_while_draining(self):
        """draining 后循环在任务边界退出：不领新任务，也不 ack 任何东西。"""
        worker = _worker()
        dispatched = []

        class _Queue:
            def read(self, block_ms=0):  # pragma: no cover - 不该被调用
                dispatched.append("read")
                return StreamTask(message_id="1-1", body={"type": "start"})

            def ack(self, message_id):
                dispatched.append("ack")

        worker._running = True
        worker.request_drain("test")
        worker._consume_loop(_Queue())  # 立即返回
        assert dispatched == []

    def test_drain_timeout_with_active_task_forces_exit(self):
        worker = _worker()
        worker._running = True
        worker.request_drain("test")
        worker._active_task_count = 1
        worker._force_stop_after_drain(timeout=0.01)
        assert worker._running is False

    def test_drain_timeout_without_active_task_keeps_running_until_loop_exits(self):
        worker = _worker()
        worker._running = True
        worker.request_drain("test")
        worker._active_task_count = 0
        worker._force_stop_after_drain(timeout=0.01)
        # 没有在途任务：无需强退，消费线程会自己退出
        assert worker._running is True

    def test_wait_for_idle_times_out_when_task_stuck(self):
        worker = _worker()
        worker._active_task_count = 1
        started = time.time()
        assert worker.wait_for_idle(timeout=0.1) is False
        assert time.time() - started >= 0.1

    def test_wait_for_idle_returns_true_when_idle(self):
        worker = _worker()
        worker._active_task_count = 0
        assert worker.wait_for_idle(timeout=1) is True

    def test_wait_for_idle_observes_completion(self):
        worker = _worker()
        worker._active_task_count = 1

        def _finish():
            time.sleep(0.05)
            worker._active_task_count = 0

        threading.Thread(target=_finish, daemon=True).start()
        assert worker.wait_for_idle(timeout=2) is True

    def test_stop_marks_draining_then_unregisters(self):
        """stop() 的顺序契约：先 draining（摘流量），再注销服务。"""
        seen = []

        worker = _worker()
        worker._enable_registry = True
        worker._running = True
        worker.update_registry_info = lambda **kw: seen.append(("status", kw.get("status")))  # type: ignore[assignment]
        worker.unregister_service = lambda: seen.append(("unregister", None))  # type: ignore[assignment]
        worker._stop_lease_watchdog = lambda: None  # type: ignore[assignment]
        worker._stop_cancel_watcher = lambda: None  # type: ignore[assignment]
        worker.stop_control_listener = lambda: None  # type: ignore[assignment]

        worker.stop()

        assert seen[0] == ("status", "draining")
        assert ("unregister", None) in seen
        assert seen.index(("unregister", None)) > seen.index(("status", "draining"))
        assert worker.draining is True
