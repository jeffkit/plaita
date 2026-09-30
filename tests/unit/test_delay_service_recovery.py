"""DelayService 重启恢复（pending ZSET + sweeper）单测。

覆盖 plaita#11：延迟任务出队后落 ``plaita:delay:pending``，sweeper 到点触发；
进程重启后 pending 中的到期任务被补触发，不再永久 suspended。
"""

import asyncio
import json
import threading
import time

import fakeredis
import pytest

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.delay_service import (
    DELAY_LOCK_KEY,
    DelayService,
)


def _task(execution_id="exec-1", trigger_offset_ms=-1000, **overrides):
    """构造一个默认已到期的延迟任务（trigger 在过去 1 秒）。"""
    task = {
        "type": "delay",
        "delay_ms": 1000,
        "trigger_timestamp": int(time.time() * 1000) + trigger_offset_ms,
        "node_id": "wait",
        "execution_id": execution_id,
        "flow_id": "flow-1",
        "event_type": "delay_trigger",
        "tenant_id": "default",
    }
    task.update(overrides)
    return task


@pytest.fixture()
def redis_server():
    return fakeredis.FakeServer()


@pytest.fixture()
def redis_client(redis_server):
    return fakeredis.FakeRedis(server=redis_server, decode_responses=False)


@pytest.fixture()
def service(redis_client):
    svc = DelayService(InMemoryEventBus(), {}, redis_client=redis_client)
    yield svc
    svc.shutdown()


def _seed_pending(redis_client, task):
    raw = json.dumps(task, ensure_ascii=False)
    redis_client.zadd("plaita:delay:pending", {raw: int(task["trigger_timestamp"])})
    return raw


def _next_message(pubsub, timeout=5):
    """取下一条真实 message（fakeredis 2.36 的 ignore_subscribe_messages 会
    连后续消息一起吞掉，这里手动跳过订阅确认）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        message = pubsub.get_message(timeout=0.5)
        if message and message.get("type") == "message":
            return message
    return None


# ---------- 消费侧：queue → pending ----------


class TestEnqueuePending:
    def test_valid_task_lands_in_pending_with_trigger_score(self, service, redis_client):
        task = _task(trigger_offset_ms=60_000)  # 未来 1 分钟，不应被触发
        raw = json.dumps(task, ensure_ascii=False)

        assert service._enqueue_pending(task, raw) is True

        members = redis_client.zrange("plaita:delay:pending", 0, -1)
        assert len(members) == 1
        assert json.loads(members[0]) == task
        score = redis_client.zscore("plaita:delay:pending", members[0])
        assert score == pytest.approx(task["trigger_timestamp"])

    def test_task_without_trigger_timestamp_scores_from_delay_ms(self, service, redis_client):
        task = _task()
        del task["trigger_timestamp"]
        task["delay_ms"] = 5000

        before = int(time.time() * 1000)
        assert service._enqueue_pending(task, json.dumps(task)) is True
        score = redis_client.zscore("plaita:delay:pending", json.dumps(task))
        assert before + 5000 <= score <= before + 5000 + 5000

    def test_invalid_config_dropped_not_pending(self, service, redis_client):
        task = _task()
        del task["event_type"]  # 基类必填字段，缺失即毒丸

        assert service._enqueue_pending(task, json.dumps(task)) is False
        assert redis_client.zcard("plaita:delay:pending") == 0


# ---------- sweeper：pending → 触发 ----------


class TestSweepDue:
    def test_due_task_fired_and_removed(self, service, redis_client, redis_server):
        task = _task()  # 已到期
        raw = _seed_pending(redis_client, task)

        # 同 server 的另一连接订阅触发频道，捕获发布
        subscriber = fakeredis.FakeRedis(server=redis_server)
        pubsub = subscriber.pubsub()
        pubsub.subscribe("plaita:events:delay_trigger")

        service._sweep_due()

        message = _next_message(pubsub)
        assert message is not None, "到期任务应被触发发布"
        event = json.loads(message["data"])
        assert event["correlation_id"] == "exec-1"
        assert event["data"]["trigger_type"] == "delay_completed"
        assert event["data"]["planned_trigger_timestamp"] == task["trigger_timestamp"]
        assert event["data"]["success"] is True

        assert redis_client.zcard("plaita:delay:pending") == 0
        assert redis_client.exists(DELAY_LOCK_KEY.format(execution_id="exec-1")) == 0

    def test_future_task_stays_pending(self, service, redis_client):
        _seed_pending(redis_client, _task(trigger_offset_ms=60_000))

        service._sweep_due()

        assert redis_client.zcard("plaita:delay:pending") == 1

    def test_external_lock_blocks_firing(self, service, redis_client):
        _seed_pending(redis_client, _task())
        # 另一实例持有触发锁
        redis_client.set(DELAY_LOCK_KEY.format(execution_id="exec-1"), "other", px=30_000)

        service._sweep_due()

        assert redis_client.zcard("plaita:delay:pending") == 1

    def test_publish_failure_keeps_task_for_retry(self, service, redis_client, monkeypatch):
        task = _task()
        _seed_pending(redis_client, task)

        def boom(event_type, event_data):
            raise ConnectionError("redis down")

        monkeypatch.setattr(service, "_publish_trigger", boom)
        service._sweep_due()

        # 发布失败：任务保留、锁保留（30s 退避后重试）
        assert redis_client.zcard("plaita:delay:pending") == 1
        assert redis_client.exists(DELAY_LOCK_KEY.format(execution_id="exec-1")) == 1

    def test_task_without_execution_id_cleared_as_poison(self, service, redis_client):
        task = _task(execution_id="")
        del task["execution_id"]
        _seed_pending(redis_client, task)

        service._sweep_due()

        assert redis_client.zcard("plaita:delay:pending") == 0

    def test_corrupt_member_cleared_as_poison(self, service, redis_client):
        redis_client.zadd("plaita:delay:pending", {"not-json": 0})

        service._sweep_due()

        assert redis_client.zcard("plaita:delay:pending") == 0

    def test_stale_snapshot_does_not_double_fire(self, service, redis_client):
        """锁内复查成员：另一实例已触发出集后，本实例的旧快照不再重复触发。"""
        task = _task()
        raw = _seed_pending(redis_client, task)

        fired = []

        def spy_publish(event_type, event_data):
            fired.append(event_type)

        service._publish_trigger = spy_publish

        # 模拟时序：sweep 快照取出成员后，另一实例完成触发并出集
        redis_client.zrem("plaita:delay:pending", raw)
        service._fire_if_due(task, raw, int(time.time() * 1000))
        assert fired == []
        # 被别家触发出集的场景同样要释放本实例抢到的锁
        assert redis_client.exists(DELAY_LOCK_KEY.format(execution_id="exec-1")) == 0

        # 对照：成员还在时正常触发
        _seed_pending(redis_client, task)
        member = redis_client.zrangebyscore("plaita:delay:pending", "-inf", int(time.time() * 1000))[0]
        if isinstance(member, bytes):
            member = member.decode()
        service._fire_if_due(task, member, int(time.time() * 1000))
        assert fired == ["delay_trigger"]
        assert redis_client.zcard("plaita:delay:pending") == 0


# ---------- 启动恢复 ----------


class TestStartupRecovery:
    def test_restart_recovers_expired_task(self, redis_server, redis_client):
        """核心场景：进程带着 pending 崩溃，重启后首扫补触发。"""
        task = _task()
        _seed_pending(redis_client, task)

        subscriber = fakeredis.FakeRedis(server=redis_server)
        pubsub = subscriber.pubsub()
        pubsub.subscribe("plaita:events:delay_trigger")

        svc = DelayService(InMemoryEventBus(), {}, redis_client=redis_client)
        try:
            assert svc.start_service() is True
            message = _next_message(pubsub)
            assert message is not None, "重启后到期任务应被补触发"
            event = json.loads(message["data"])
            assert event["correlation_id"] == "exec-1"
            assert event["data"]["trigger_type"] == "delay_completed"
        finally:
            svc.stop_service()

    def test_restart_leaves_future_task_pending(self, redis_client):
        _seed_pending(redis_client, _task(trigger_offset_ms=60_000))

        svc = DelayService(InMemoryEventBus(), {}, redis_client=redis_client)
        try:
            assert svc.start_service() is True
            deadline = time.time() + 3
            while time.time() < deadline and redis_client.zcard("plaita:delay:pending"):
                time.sleep(0.1)
            assert redis_client.zcard("plaita:delay:pending") == 1
        finally:
            svc.stop_service()

    def test_pending_count_reported(self, service, redis_client):
        _seed_pending(redis_client, _task())
        _seed_pending(redis_client, _task(execution_id="exec-2"))

        info = service.get_pending_tasks_info()

        assert info["pending_task_count"] == 2


# ---------- 内存模式（无 redis）回归 ----------


class TestMemoryMode:
    def test_start_without_redis_still_ok(self):
        svc = DelayService(InMemoryEventBus(), {})
        try:
            assert svc.start_service() is True
            assert svc._consumer_thread is None
            assert svc._sweeper_thread is None
        finally:
            svc.shutdown()

    def test_handle_task_waits_for_trigger_timestamp_then_fires(self):
        """handle_task 契约：sleep 到 trigger_timestamp 后触发 delay_completed。"""
        svc = DelayService(InMemoryEventBus(), {})
        calls = []

        async def spy_trigger(event_type, event_data):
            calls.append((event_type, event_data))

        svc.trigger_event = spy_trigger
        task = _task(trigger_offset_ms=150)

        start = time.time()
        ok = asyncio.run(svc.handle_task(task))

        assert ok is True
        assert time.time() - start >= 0.1, "应等待到触发时刻"
        assert len(calls) == 1
        event_type, event_data = calls[0]
        assert event_type == "delay_trigger"
        assert event_data["execution_id"] == "exec-1"
        assert event_data["trigger_type"] == "delay_completed"
        assert event_data["planned_trigger_timestamp"] == task["trigger_timestamp"]


# ---------- 并发回归：多执行互不阻塞 ----------


class TestConcurrentExecutions:
    def test_two_due_tasks_both_fired_in_one_sweep(self, service, redis_client):
        fired = []
        service._publish_trigger = lambda event_type, event_data: fired.append(event_data["execution_id"])
        _seed_pending(redis_client, _task(execution_id="exec-1"))
        _seed_pending(redis_client, _task(execution_id="exec-2"))

        service._sweep_due()

        assert sorted(fired) == ["exec-1", "exec-2"]
        assert redis_client.zcard("plaita:delay:pending") == 0
