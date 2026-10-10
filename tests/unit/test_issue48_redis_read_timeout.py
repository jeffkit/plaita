"""#48 回归：Redis 读超时不得炸穿 worker 主循环。

基线缺陷（2026-10-07 本机两次实测 worker 进程退出）：``task_queue.read()``
只吞 ``RedisConnectionError``，而 redis-py 8.x 里 ``TimeoutError`` 与它是
``RedisError`` 下**互不继承**的兄弟分支（``issubclass`` 实测为 False）；
``xreadgroup`` 那一支的 ``except RedisTimeoutError`` 只覆盖 BLOCK 到期，包不住
它之前的 ``ensure_group``。于是 ``xgroup_create`` 的读超时从 ``_read_once`` 直穿
``_consume_loop`` → ``run()`` → ``main()`` 的 ``sys.exit(1)``：worker 进程退出，
launchd 拉起后 XCLAIM 存量 PEL 触发交接重入队风暴。

修复三层（本文件逐层验证）：
1. ``RedisStreamTaskQueue.read()`` 吞瞬态家族（ConnectionError/TimeoutError/socket）
   → warning + 退避 + 返回 None；
2. ``ensure_group()`` 分层：瞬态只记 warning 并返回，协议/权限错误仍上抛；
3. ``RedisFlowWorker._consume_forever()`` 进程级兜底：Redis 故障记 error + 退避 +
   继续，绝不退出进程（含 except 分支里 ``ack``/``dead_letter`` 再抛的路径）。
"""

import logging
import socket
import threading
import time
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")
pytest.importorskip("cachetools")

import fakeredis
from redis.exceptions import AuthenticationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import RedisStreamTaskQueue, StreamTask, enqueue_task
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

QUEUE_LOGGER = "plaita.server.task_queue"
BACKOFF = "RECONNECT_BACKOFF_SECONDS"

# 三种「读超时」注入：redis 客户端超时、redis 连接类、socket 层超时。
TRANSIENT_CASES = [
    pytest.param(lambda: RedisTimeoutError("Timeout reading from socket"), id="redis-TimeoutError"),
    pytest.param(lambda: RedisConnectionError("connection refused"), id="redis-ConnectionError"),
    pytest.param(lambda: socket.timeout("timed out"), id="socket-timeout"),
]


class TestQueueSwallowsTransientReadErrors:
    """第 1/2 层：队列自身的瞬态容忍（不抛、warning、返回 None）。"""

    def setup_method(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:issue48"
        self.queue = RedisStreamTaskQueue(
            self.redis, self.stream, group_name="g48", consumer_name="c1"
        )

    @pytest.mark.parametrize("make_exc", TRANSIENT_CASES)
    def test_read_returns_none_when_ensure_group_raises_transient(self, make_exc, caplog):
        """崩溃链第一跳：ensure_group（每轮 _read_once 的第一行）抛读超时。"""
        with patch.object(self.queue, "ensure_group", side_effect=make_exc()), \
             patch(f"plaita.server.task_queue.{BACKOFF}", 0), \
             caplog.at_level(logging.WARNING, logger=QUEUE_LOGGER):
            assert self.queue.read(block_ms=10) is None  # 不抛

        assert any("Redis 读失败" in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize("make_exc", TRANSIENT_CASES)
    def test_ensure_group_transient_logs_warning_and_returns(self, make_exc, caplog):
        """ensure_group 本身：瞬态 → warning + 返回（由上层轮询重试）。"""
        with patch.object(self.redis, "xgroup_create", side_effect=make_exc()), \
             caplog.at_level(logging.WARNING, logger=QUEUE_LOGGER):
            self.queue.ensure_group()  # 不抛

        assert any("消费组确保失败" in r.getMessage() for r in caplog.records)

    def test_ensure_group_non_transient_still_raises(self):
        """协议/权限错误（ResponseError）仍上抛——配置问题不能静默空转。"""
        with patch.object(
            self.redis, "xgroup_create", side_effect=ResponseError("NOPERM no permissions")
        ):
            with pytest.raises(ResponseError):
                self.queue.ensure_group()

    def test_ensure_group_authentication_error_still_raises(self):
        """权限错误必须快速失败：``AuthenticationError`` 在 redis-py 8.x 里挂在
        ``ConnectionError`` 下，会被瞬态元组误吞 → 密码/ACL 配错退化成每秒一条
        warning 的永久空转（worker 拿着「组不存在」的状态空转）。"""
        with patch.object(
            self.redis, "xgroup_create",
            side_effect=AuthenticationError("invalid username-password pair"),
        ):
            with pytest.raises(AuthenticationError):
                self.queue.ensure_group()

    def test_read_authentication_error_still_raises(self, caplog):
        """读路径同款：权限错误不得被 read() 的瞬态容忍吞成「空轮询」。"""
        self.queue.ensure_group()
        with patch.object(
            self.redis, "xreadgroup",
            side_effect=AuthenticationError("invalid username-password pair"),
        ), patch(f"plaita.server.task_queue.{BACKOFF}", 0):
            with pytest.raises(AuthenticationError):
                self.queue.read(block_ms=10)

    def test_ensure_group_busygroup_still_ignored(self):
        with patch.object(
            self.redis,
            "xgroup_create",
            side_effect=ResponseError("BUSYGROUP Consumer Group name already exists"),
        ):
            self.queue.ensure_group()  # 不抛（存量行为）

    def test_read_swallows_socket_timeout_from_xreadgroup(self, caplog):
        """socket 层超时不经 redis 包装直接穿出连接池时同样不炸。"""
        self.queue.ensure_group()
        with patch.object(self.redis, "xreadgroup", side_effect=socket.timeout("timed out")), \
             patch(f"plaita.server.task_queue.{BACKOFF}", 0), \
             caplog.at_level(logging.WARNING, logger=QUEUE_LOGGER):
            assert self.queue.read(block_ms=10) is None

        assert any("Redis 读失败" in r.getMessage() for r in caplog.records)

    def test_read_still_treats_xreadgroup_redis_timeout_as_empty_poll(self):
        """既有约定不变：BLOCK 到期的 redis TimeoutError 属常态空轮询（静默 None）。"""
        self.queue.ensure_group()
        with patch.object(
            self.redis, "xreadgroup", side_effect=RedisTimeoutError("Timeout reading from socket")
        ):
            assert self.queue.read(block_ms=10) is None

    def test_read_swallows_transient_from_xclaim(self, caplog):
        """reclaim 路径（#48 建议 4）：xclaim 抛读超时不得炸穿。"""
        enqueue_task(self.redis, self.stream, {"type": "start", "flow_id": "f1"})
        self.queue.ensure_group()
        # 让消息进入 PEL 且 idle 超阈，逼出 xclaim 路径
        self.redis.xreadgroup(
            groupname="g48", consumername="other", streams={self.stream: ">"}, count=1
        )
        self.queue.claim_min_idle_ms = 0

        with patch.object(self.redis, "xclaim", side_effect=RedisTimeoutError("read timed out")), \
             patch(f"plaita.server.task_queue.{BACKOFF}", 0), \
             caplog.at_level(logging.WARNING, logger=QUEUE_LOGGER):
            assert self.queue.read(block_ms=10) is None

        assert any("Redis 读失败" in r.getMessage() for r in caplog.records)

    def test_read_recovers_after_transient_ensure_group_failure(self):
        """组已存在时瞬态故障只丢掉那一轮 xgroup_create：本轮照常读到任务。"""
        enqueue_task(self.redis, self.stream, {"type": "start", "flow_id": "recovered"})
        self.queue.ensure_group()  # 组先建好（真实部署里上一轮已建）
        with patch.object(self.redis, "xgroup_create", side_effect=RedisTimeoutError("blip")), \
             patch(f"plaita.server.task_queue.{BACKOFF}", 0):
            task = self.queue.read(block_ms=50)

        assert task is not None, "瞬态建组失败不应吞掉本轮正常读取"
        assert task.body["flow_id"] == "recovered"

    def test_message_stays_pending_when_read_fails(self):
        """边界：读失败仍不 ack，消息留在队列里（at-least-once 语义不变）。"""
        enqueue_task(self.redis, self.stream, {"type": "start", "flow_id": "f1"})
        self.queue.ensure_group()
        with patch.object(self.queue, "ensure_group", side_effect=RedisTimeoutError("boom")), \
             patch(f"plaita.server.task_queue.{BACKOFF}", 0):
            assert self.queue.read(block_ms=10) is None

        assert self.redis.xlen(self.stream) == 1, "消息未被消费也未丢失"
        assert self.redis.xpending(self.stream, "g48")["pending"] == 0


def _worker(**kw) -> RedisFlowWorker:
    """构造不做 I/O（redis-py 的 from_url 是惰性的）。"""
    kw.setdefault("enable_registry", False)
    kw.setdefault("enable_redis_logging", False)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue48",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        event_bus=InMemoryEventBus(),
        **kw,
    )


class _ScriptedQueue:
    """按剧本产出 read() 结果的假队列：异常对象 = 抛出，否则作为任务返回。

    剧本耗尽后调 ``on_exhaust``（测试用它停掉 worker 的消费循环）。
    """

    max_deliveries = 5
    consumer_name = "c1"

    def __init__(self, script=None, on_exhaust=None):
        self.script = list(script or [])
        self.on_exhaust = on_exhaust
        self.reads = 0
        self.acks = []

    def _read(self):
        self.reads += 1
        if self.script:
            item = self.script.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        if self.on_exhaust is not None:
            self.on_exhaust()
        return None

    def ensure_group(self):
        pass

    def read(self, block_ms=0):
        return self._read()

    def ack(self, message_id):
        self.acks.append(message_id)

    def note_poison(self):
        pass

    def note_failed(self):
        pass

    def note_lease_conflict(self):
        pass

    def dead_letter(self, task, *, reason):
        self.acks.append(task.message_id)

    def sweep_acked_residue(self, batch_size=256):
        return 0


class _FlakyQueue(_ScriptedQueue):
    """每次 read 都抛 ``exc``，直到 ``stop_after`` 次后停掉 worker。"""

    def __init__(self, exc, stop_after, worker):
        super().__init__(on_exhaust=lambda: setattr(worker, "_running", False))
        self.exc = exc
        self.stop_after = stop_after

    def read(self, block_ms=0):
        self.reads += 1
        if self.reads >= self.stop_after:
            self.on_exhaust()
        raise self.exc


class TestConsumeForeverSurvivesRedisFailures:
    """第 3 层：消费循环最外层兜底（绝不退出进程）。"""

    @pytest.mark.parametrize("make_exc", TRANSIENT_CASES)
    def test_transient_errors_do_not_exit_loop(self, make_exc, caplog):
        worker = _worker()
        worker._running = True
        queue = _FlakyQueue(make_exc(), stop_after=3, worker=worker)

        with patch(f"plaita.server.flow_worker.{BACKOFF}", 0), caplog.at_level(logging.ERROR):
            worker._consume_forever(queue)  # 不抛、不退出

        assert queue.reads >= 3, "兜底后必须继续轮询（不是退出循环）"
        assert any(
            "Redis 故障中断消费循环" in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_non_transient_redis_error_also_keeps_process_alive(self):
        """协议类 RedisError（如 NOGROUP/WRONGTYPE）同样不得退出进程。"""
        worker = _worker()
        worker._running = True
        queue = _FlakyQueue(ResponseError("NOGROUP No such consumer group"), 2, worker)

        with patch(f"plaita.server.flow_worker.{BACKOFF}", 0):
            worker._consume_forever(queue)
        assert queue.reads >= 2

    def test_recovers_and_consumes_after_redis_returns(self):
        """Redis 恢复后正常消费：瞬态失败只丢轮次，不丢消息语义。"""
        worker = _worker()
        worker._running = True
        task = StreamTask(message_id="1-1", body={"type": "start", "flow_id": "f1"})
        queue = _ScriptedQueue(
            script=[RedisTimeoutError("boom"), None, task],
            on_exhaust=lambda: setattr(worker, "_running", False),
        )
        dispatched = []
        worker._dispatch_task = lambda body, delivery_count=None: dispatched.append(body)

        with patch(f"plaita.server.flow_worker.{BACKOFF}", 0):
            worker._consume_forever(queue)

        assert dispatched == [task.body], "恢复后应正常派发任务"
        assert queue.acks == [task.message_id], "派发成功后 ack"

    def test_drain_interrupts_backoff_immediately(self):
        """停机语义不回归：draining 立即打断退避（不睡满 backoff 才看标志）。"""
        worker = _worker()
        worker._running = True
        queue = _FlakyQueue(RedisTimeoutError("boom"), stop_after=10_000, worker=worker)

        def _drain():
            time.sleep(0.05)
            worker.request_drain("signal 15")

        threading.Thread(target=_drain, daemon=True).start()
        started = time.monotonic()
        with patch(f"plaita.server.flow_worker.{BACKOFF}", 30.0):
            worker._consume_forever(queue)
        elapsed = time.monotonic() - started
        if worker._drain_timer is not None:
            worker._drain_timer.cancel()

        assert elapsed < 1.0, f"draining 未打断退避（{elapsed:.1f}s，backoff=30s）"
        assert queue.reads >= 1


class TestRunSurvivesRedisTimeout:
    """端到端（进程级）：run() 不再因 Redis 读超时退出。"""

    def test_run_returns_normally_on_repeated_read_timeouts(self, caplog):
        worker = _worker(redis_client=fakeredis.FakeRedis(decode_responses=True))
        queue = _ScriptedQueue(
            script=[RedisTimeoutError("Timeout reading from socket")] * 4,
            on_exhaust=lambda: setattr(worker, "_running", False),
        )
        worker._get_task_queue = lambda: queue

        with patch(f"plaita.server.flow_worker.{BACKOFF}", 0), caplog.at_level(logging.ERROR):
            worker.run()  # 不抛、不 sys.exit(1)

        assert queue.reads >= 4
        assert worker._running is False
        assert any(
            "Redis 故障中断消费循环" in r.getMessage() for r in caplog.records
        ), "兜底必须留下 error 证据（进程存活但可观测）"

    def test_main_still_treats_construction_failure_as_fatal(self):
        """main() 的 exit(1) 语义保留：构造失败仍是致命错（Redis 抖动不是）。"""
        from plaita.server import flow_worker as fw

        # 入口的副作用（CodeNode 全局白名单注册 / writefile jail 环境变量 /
        # 沙箱清扫线程）都 patch 掉：本测试只关心 sys.exit(1) 分支，
        # 且注册 CodeNode 白名单会污染同进程后续用例（test_loop 的 unsafe 档）。
        with patch.object(fw, "create_storage_component", return_value=Mock()), \
             patch.object(fw, "create_event_bus", return_value=Mock()), \
             patch.object(fw, "RedisFlowWorker", side_effect=RuntimeError("bad config")), \
             patch.object(fw, "_code_node_enabled", return_value=False), \
             patch.object(fw, "_paused_sweeper", return_value=None), \
             patch.object(fw, "apply_writefile_jail", return_value=None), \
             patch("sys.argv", ["flow-worker", "--no-registry"]):
            with pytest.raises(SystemExit) as excinfo:
                fw.main()

        assert excinfo.value.code == 1
