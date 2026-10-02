"""ReviewFix D3 回归：SIGTERM/SIGINT 优雅停机，不再腰斩在途任务。

基线缺陷：signal_handler 里 ``worker.stop(); sys.exit(0)``——SystemExit 在
``_dispatch_task`` 任意字节码边界抛出，在途执行（可能正卡在 LLM 调用/存储
写入之间）当场腰斩、消息未 ack 退化为 crash 式重投；与控制通道
``_on_stop_command`` 的 drain 语义（只置位、等当前任务做完）不一致。

修复：信号路径收敛到模块级 ``_request_graceful_stop``（只调 ``worker.stop()``
置位），由 ``run()`` 主循环在任务边界自然退出（read 已切 ≤1s 分片）。
"""
import signal
import threading
from unittest.mock import Mock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")
pytest.importorskip("cachetools")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker, _request_graceful_stop
from plaita.server.task_queue import StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage


class RecordingQueue:
    """替代 RedisStreamTaskQueue 的假队列。"""

    max_deliveries = 5
    consumer_name = "test-consumer"

    def __init__(self, tasks):
        self._tasks = list(tasks)
        self.acked = []
        self.poison_count = 0
        self.failed_count = 0
        self.dead_lettered = []

    def ensure_group(self):
        pass

    def read(self, block_ms=0):
        if self._tasks:
            return self._tasks.pop(0)
        return None

    def ack(self, message_id):
        self.acked.append(message_id)

    def note_poison(self):
        self.poison_count += 1

    def note_failed(self):
        self.failed_count += 1

    def note_lease_conflict(self):
        pass

    def dead_letter(self, task, *, reason):
        self.dead_lettered.append((task.message_id, reason))
        self.ack(task.message_id)


def _make_worker_with_slow_task(storage, started, proceed):
    """构造一个「在途任务卡在执行中」的 worker：dispatch 等待放行信号。"""
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:reviewfix-d3",
        execution_storage=storage,
        flow_storage=Mock(),
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        enable_registry=False,
        enable_redis_logging=False,
        read_block_ms=100,
    )
    queue = RecordingQueue(
        [
            StreamTask(
                message_id="msg-inflight",
                body={"type": "resume", "flow_id": "flow-x",
                      "execution_id": "exec-inflight", "resume_type": "continue"},
            )
        ]
    )
    worker._get_task_queue = lambda: queue

    def slow_dispatch(message_data):
        # 模拟在途执行正卡在 LLM 调用/存储写入之间
        started.set()
        assert proceed.wait(timeout=10), "测试放行信号超时"
        # 任务完整推进到终态并落盘
        state = ExecutionState(
            execution_id=message_data["execution_id"],
            flow_id=message_data["flow_id"],
            status="completed",
            context={"done": True},
        )
        worker.execution_storage.save_execution_state(state.execution_id, state)

    worker._dispatch_task = slow_dispatch
    return worker, queue


def test_signal_stop_only_requests_graceful_stop(monkeypatch=None):
    """_request_graceful_stop 只调 worker.stop() 置位，不抛 SystemExit。"""
    stop_calls = []

    class FakeWorker:
        _running = True

        def stop(self):
            stop_calls.append(1)
            FakeWorker._running = False

    fake = FakeWorker()
    # 不抛 SystemExit 即通过（旧实现等价体会在 dispatch 帧内 raise SystemExit）
    _request_graceful_stop(fake, signal.SIGTERM)
    _request_graceful_stop(fake, signal.SIGINT)

    assert len(stop_calls) == 2
    assert fake._running is False


def test_sigterm_during_inflight_task_drains_to_terminal_state():
    """信号到达时在途任务不被腰斩：完整落终态、消息 ack、主循环随后退出。"""
    storage = MemoryExecutionStorage()
    started = threading.Event()
    proceed = threading.Event()
    worker, queue = _make_worker_with_slow_task(storage, started, proceed)

    runner = threading.Thread(target=worker.run, daemon=True)
    runner.start()

    assert started.wait(timeout=5), "在途任务未开始执行"

    # 模拟 SIGTERM 到达（等价 signal_handler 的调用路径）
    _request_graceful_stop(worker, signal.SIGTERM)
    assert worker._running is False, "信号处理后应已请求停机"

    # 放行在途任务 → 它必须完整跑完而不是被 SystemExit 打断
    proceed.set()
    runner.join(timeout=5)

    assert not runner.is_alive(), "主循环未在当前任务完成后退出"
    assert queue.acked == ["msg-inflight"], "在途任务未被 ack（被腰斩了）"
    assert queue.dead_lettered == []
    assert queue.poison_count == 0 and queue.failed_count == 0

    saved = storage.load_execution_state("exec-inflight")
    assert saved is not None, "在途任务终态未落盘"
    assert saved.status == "completed"
    assert saved.context == {"done": True}


def test_stop_is_idempotent_for_handler_plus_run_finally():
    """信号 handler 与 run() finally 会先后调用 stop()，必须幂等。"""
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:reviewfix-d3",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=Mock(),
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        enable_registry=False,
        enable_redis_logging=False,
    )
    worker.stop()
    worker.stop()  # 第二次（run() finally 路径）不应抛异常
    assert worker._running is False
