"""A2R19-03：worker 启动期注册失败后的自愈。

缺陷基线（台账 r19 §2）：``RedisFlowWorker.run()`` 启动时只调用一次
``register_service()``；启动瞬间 Redis 不可达 → 注册返回 False → 心跳线程
从未启动 → Redis 恢复后队列读会自动重连，但注册键**永远不再出现**
（``/api/services``、容量快照、亲和派发、readiness 都看不到该 worker）。

修法：``_ensure_service_registration()`` 失败时启动后台指数退避重试
（1s→30s 封顶），成功后 ``register_service()`` 顺带启动心跳；``stop()``
先停重试再注销。

负控（还原修复前实现）：单次 ``register_service()`` 失败后即使 Redis 恢复，
1.5s 后注册表仍为空——本文件第一条用例钉住该缺陷前提；自愈用例在旧实现上
因 ``_ensure_service_registration`` 不存在而红。
"""
from __future__ import annotations

import json
import logging
import threading
import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis  # noqa: E402

from plaita.server.flow_worker import RedisFlowWorker  # noqa: E402
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402

REGISTRY_PATTERN = "plaita:registry:flow_worker:*"


class _FlakyRedis:
    """``setex`` 可切换失败的 fakeredis 代理（模拟注册写入瞬断）。

    只让写注册键的 ``setex`` 失败：本缺陷的观察面是「读已恢复、注册写失败
    后不再重试」，隔离它比整体断连更精确。
    """

    def __init__(self, inner):
        self._inner = inner
        self.down = True

    def setex(self, *args, **kwargs):
        if self.down:
            raise ConnectionError("simulated Redis down (registration write)")
        return self._inner.setex(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


@pytest.fixture()
def flaky():
    return _FlakyRedis(fakeredis.FakeRedis(decode_responses=True))


def _worker(redis) -> RedisFlowWorker:
    """骨架 worker（不走 ``__init__``，同 test_issue42_worker_capacity_report 口径）。"""
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w._enable_registry = True
    w._running = True
    w._registry = ServiceRegistry(redis, ttl=30, heartbeat_interval=0.2)
    w._service_info = ServiceInfo(
        instance_id="w-r19",
        service_type="flow_worker",
        host="test-host/127.0.0.1",
        metadata={"queue_name": "plaita:flow:queue"},
    )
    w._registry_registered = False
    w._registry_retry_thread = None
    w._registry_retry_stop = threading.Event()
    return w


def _keys(redis):
    return sorted(redis.keys(REGISTRY_PATTERN))


def _wait_keys(redis, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        keys = _keys(redis)
        if keys:
            return keys
        time.sleep(0.05)
    return _keys(redis)


def _cleanup(worker, redis):
    # getattr：负控跑旧实现时该方法不存在，清理不应把缺陷前提用例也染红
    stop = getattr(worker, "_stop_registry_retry", None)
    if stop is not None:
        stop()
    worker._registry.stop_heartbeat()
    keys = _keys(redis)
    if keys:
        redis.delete(*keys)


def test_single_register_failure_does_not_self_heal(flaky):
    """缺陷前提：旧路径只注册一次；Redis 恢复后注册表依旧为空。"""
    worker = _worker(flaky)
    try:
        assert worker.register_service() is False
        assert _keys(flaky) == []
        flaky.down = False
        time.sleep(1.5)
        assert _keys(flaky) == []  # 无重试 = 永不注册（旧实现行为）
    finally:
        _cleanup(worker, flaky)


def test_ensure_registration_self_heals_after_redis_recovers(flaky, caplog):
    """修复：启动失败 → 后台退避重试 → Redis 恢复后注册键出现且心跳存活。"""
    worker = _worker(flaky)
    try:
        with caplog.at_level(logging.WARNING):
            assert worker._ensure_service_registration() is False
            assert worker.registry_registered is False
            assert _keys(flaky) == []
            flaky.down = False
            keys = _wait_keys(flaky)
        assert keys == ["plaita:registry:flow_worker:w-r19"]
        assert worker.registry_registered is True
        heartbeat = worker._registry._heartbeat_thread
        assert heartbeat is not None and heartbeat.is_alive()
        assert any("已进入后台重试" in rec.message for rec in caplog.records)
        assert any("服务注册已恢复" in rec.message for rec in caplog.records)
        # 只产生一个注册键（重试成功后不再重复注册）
        assert len(keys) == 1
    finally:
        _cleanup(worker, flaky)


def test_ensure_registration_success_starts_heartbeat(flaky):
    worker = _worker(flaky)
    flaky.down = False
    try:
        assert worker._ensure_service_registration() is True
        assert worker.registry_registered is True
        assert _keys(flaky) == ["plaita:registry:flow_worker:w-r19"]
        heartbeat = worker._registry._heartbeat_thread
        assert heartbeat is not None and heartbeat.is_alive()
        assert worker._registry_retry_thread is None
    finally:
        _cleanup(worker, flaky)


def test_retry_success_preserves_startup_drain_status(flaky):
    """A2R19-03 × S18-02A：重试注册成功时不得把启动期 draining 复活成 running。

    SIGTERM 早于注册成功（Redis 启动瞬断 + 停机请求）时，重试线程走
    ``register_service()``；r50 的 register 状态机保留 draining，派发白名单
    也必须继续拒绝该实例。
    """
    worker = _worker(flaky)
    worker._service_info.status = "draining"
    try:
        assert worker._ensure_service_registration() is False
        assert worker.registry_registered is False
        flaky.down = False
        keys = _wait_keys(flaky)
        assert keys == ["plaita:registry:flow_worker:w-r19"]
        data = json.loads(flaky.get(keys[0]))
        assert data["status"] == "draining"
        from plaita.server.capacity import DispatchCandidate
        assert DispatchCandidate.from_service_info(
            worker._service_info).dispatchable is False
        heartbeat = worker._registry._heartbeat_thread
        assert heartbeat is not None and heartbeat.is_alive()
        # 心跳（0.2s）不得把 draining 写回 running
        time.sleep(0.5)
        assert json.loads(flaky.get(keys[0]))["status"] == "draining"
        assert worker.registry_registered is True
    finally:
        _cleanup(worker, flaky)


def test_stop_cancels_pending_registration_retry(flaky):
    """停机后不得被重试线程写回注册键。"""
    worker = _worker(flaky)
    try:
        assert worker._ensure_service_registration() is False
        assert worker._registry_retry_thread is not None
        worker._stop_registry_retry()
        assert worker._registry_retry_thread is None
        flaky.down = False
        time.sleep(1.5)
        assert _keys(flaky) == []
    finally:
        _cleanup(worker, flaky)
