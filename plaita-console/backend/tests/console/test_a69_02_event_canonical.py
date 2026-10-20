"""A69-02（台账 r70 / PR2）：console 事件发布写引擎正典键形。

历史缺陷：console ``POST /api/events/publish`` 自建事件体，写
``plaita:event:{id}`` + ``plaita:event:type:{type}``（TTL 24h），而引擎
``RedisEventStorage`` 的读面/回扫只认 ``plaita:event:events:{id}`` +
``plaita:event:types:{type}``（7d）。于是：

* 引擎 ``get_event`` 返回 None、``list_events`` 为空；
* ``EventReconciler`` 不会重放（Pub/Sub 丢通知后手动事件不可恢复）；
* 24h 后记录本身消失，短于订阅 7d 的挂起等待窗口。

本组用例逐条钉住修复后形态（与 duo-a-ledger r70 §2.6 的负控表一一对应）：

1. canonical 记录存在、legacy 键不再写、引擎 ``get_event`` 可读；
2. 引擎 ``list_events(event_type=...)`` 可见；
3. 停 EventFilter 后 publish，``EventReconciler.scan_once`` 能重放；
4. 记录/索引 TTL 对齐 ``event_retention_ttl_seconds()``（默认 7d）；
5. 记录体 timestamp 与 zset score 同源（只取一次 ``time.time()``）；
6. console ``GET /events`` 两形并存按 event_id 去重且 canonical 优先；
7. legacy 记录仍可读（只读兼容，自然过期）；
8. 旧扫描逻辑（回滚后的 console）能读 canonical（回滚安全）；
9. 受信租户写入 canonical 顶层，``data.tenant_id`` 被剥离。
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

from fastapi import Depends, FastAPI, Request
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import events as events_api  # noqa: E402

import fakeredis  # noqa: E402

from plaita.event.core import EventSubscription  # noqa: E402
from plaita.event.memory import InMemoryEventSubscriptionStorage  # noqa: E402
from plaita.event.redis import RedisEventStorage  # noqa: E402
from plaita.server.event_filter import EventFilter  # noqa: E402
from plaita.server.event_reconcile import EventReconciler  # noqa: E402
from plaita.server.task_queue import PAYLOAD_FIELD  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage  # noqa: E402

QUEUE = "plaita:a69-02:queue"
TENANT_A = "acme"
TENANT_B = "globex"
BASE_TS = 1_700_000_000.0

RECORD_KEY = "plaita:event:events:{}"
TYPE_KEY = "plaita:event:types:{}"
LEGACY_RECORD_KEY = "plaita:event:{}"
LEGACY_TYPE_KEY = "plaita:event:type:{}"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _publish_client(redis_client, tenant: str = TENANT_A) -> TestClient:
    app = FastAPI()
    app.state.redis = redis_client

    async def _auth(request: Request):
        request.state.tenant_id = tenant
        request.state.role = "editor"
        request.state.actor = f"editor@{tenant}"
        request.state.platform_admin = False
        return {"tenant_id": tenant}

    app.include_router(events_api.router, prefix="/api", dependencies=[Depends(_auth)])
    return TestClient(app)


def _storage_on(server: fakeredis.FakeServer) -> RedisEventStorage:
    """与同步客户端共享同一 FakeServer 的引擎存储（async 客户端）。"""
    storage = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=3600)
    storage.redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    return storage


def _queue_tasks(redis_client, queue: str = QUEUE) -> List[dict]:
    tasks = []
    for _mid, fields in redis_client.xrange(queue, min="-", max="+"):
        raw = fields.get(PAYLOAD_FIELD) or fields.get(PAYLOAD_FIELD.encode())
        if isinstance(raw, bytes):
            raw = raw.decode()
        tasks.append(json.loads(raw))
    return tasks


def _state(execution_id: str, flow_id: str, tenant: str) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id=flow_id,
        status="suspended",
        context={},
        tenant_id=tenant,
    )


def _old_console_scan(redis_client) -> List[Tuple[str, Dict]]:
    """复刻升级前 ``GET /events`` 的扫描逻辑（无 canonical 优先级/去重）。

    回滚场景：旧 console 实例面对新 canonical 记录时必须仍能展示（键形与
    序列化都未变）。本函数就是负控 8 的「旧读面」。
    """
    records: List[Tuple[str, Dict]] = []
    cursor = 0
    keys: List[str] = []
    while True:
        cursor, batch = redis_client.scan(cursor=cursor, match="plaita:event:*", count=100)
        keys.extend(k if isinstance(k, str) else k.decode() for k in batch)
        if cursor == 0:
            break
    for key in keys:
        if any(seg in key for seg in (":subscription:", ":processing:", ":type:", ":types:", ":channel:")):
            continue
        key_type = redis_client.type(key)
        if isinstance(key_type, bytes):
            key_type = key_type.decode()
        if key_type != "string":
            continue
        raw = redis_client.get(key)
        if not raw:
            continue
        records.append((key, json.loads(raw)))
    return records


# ---------------------------------------------------------------- 1


def test_publish_writes_engine_record():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)

    r = client.post(
        "/api/events/publish",
        json={
            "event_type": "approval",
            "data": {"approved": True},
            "correlation_id": "exec-a",
        },
    )
    assert r.status_code == 200, r.text
    event_id = r.json()["event_id"]

    assert sync.exists(RECORD_KEY.format(event_id)) == 1, "未写引擎正典记录键"
    assert sync.exists(LEGACY_RECORD_KEY.format(event_id)) == 0, (
        "修复后不得再写 legacy 24h 记录键"
    )
    assert sync.exists(LEGACY_TYPE_KEY.format("approval")) == 0

    async def _t():
        storage = _storage_on(server)
        return await storage.get_event(event_id)

    got = _run(_t())
    assert got is not None, "引擎 get_event 读不到 console 发布的事件"
    assert got.event_id == event_id
    assert got.event_type == "approval"
    assert got.correlation_id == "exec-a"
    assert got.tenant_id == TENANT_A


# ---------------------------------------------------------------- 2


def test_publish_visible_to_list_events():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)

    r = client.post(
        "/api/events/publish",
        json={"event_type": "approval", "data": {"n": 1}},
    )
    event_id = r.json()["event_id"]

    async def _t():
        storage = _storage_on(server)
        return await storage.list_events(event_type="approval", limit=10)

    got = _run(_t())
    assert [e.event_id for e in got] == [event_id], (
        f"引擎 list_events 看不到 console 事件: {got!r}"
    )
    record = json.loads(sync.get(RECORD_KEY.format(event_id)))
    assert got[0].timestamp == record["timestamp"]


# ---------------------------------------------------------------- 3


def test_publish_replayed_by_reconciler():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)

    r = client.post(
        "/api/events/publish",
        json={
            "event_type": "approval",
            "data": {"approved": True},
            "correlation_id": "exec-a",
        },
    )
    assert r.status_code == 200, r.text
    event_id = r.json()["event_id"]

    async def _t():
        storage = _storage_on(server)
        execution_storage = MemoryExecutionStorage()
        execution_storage.save_execution_state(
            "exec-a", _state("exec-a", "flow-a", TENANT_A)
        )
        subscription_storage = InMemoryEventSubscriptionStorage()
        await subscription_storage.store_subscription(
            EventSubscription(
                event_type="approval",
                correlation_id="exec-a",
                flow_id="flow-a",
                tenant_id=TENANT_A,
            )
        )
        # 构造即「已停」的 EventFilter：模拟 Pub/Sub 通知丢失/过滤器重启，
        # 只能依赖事件存储回扫补偿。
        event_filter = EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=sync,
            event_bus=None,
            queue_name=QUEUE,
            enable_subscription_timeout_checker=False,
        )
        reconciler = EventReconciler(storage, event_filter, sync)
        return await reconciler.scan_once()

    replayed = _run(_t())
    assert replayed == 1, f"EventReconciler 未重放 console 发布的事件（replayed={replayed}）"
    tasks = _queue_tasks(sync)
    assert len(tasks) == 1
    assert tasks[0]["data"]["event_id"] == event_id
    assert tasks[0]["execution_id"] == "exec-a"
    assert tasks[0]["resume_type"] == "event"


# ---------------------------------------------------------------- 4


def test_publish_ttl_aligned(monkeypatch):
    monkeypatch.delenv("PLAITA_EVENT_TTL_SECONDS", raising=False)
    # 局部导入：修复前没有该 helper，本用例必须逐条红而不是整文件 collection error。
    from plaita.event.redis import event_retention_ttl_seconds

    expected = event_retention_ttl_seconds()
    assert expected == 7 * 86400

    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)
    r = client.post("/api/events/publish", json={"event_type": "approval", "data": {}})
    event_id = r.json()["event_id"]

    record_ttl = sync.ttl(RECORD_KEY.format(event_id))
    index_ttl = sync.ttl(TYPE_KEY.format("approval"))
    assert 0 < record_ttl <= expected, f"记录 TTL 越界: {record_ttl}"
    assert 0 < index_ttl <= expected, f"索引 TTL 越界: {index_ttl}"
    assert abs(record_ttl - expected) <= 2, f"记录 TTL 未对齐 7d: {record_ttl}"
    assert abs(index_ttl - expected) <= 2, f"索引 TTL 未对齐 7d: {index_ttl}"


# ---------------------------------------------------------------- 5


def test_publish_single_timestamp():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)
    r = client.post("/api/events/publish", json={"event_type": "approval", "data": {}})
    event_id = r.json()["event_id"]

    record = json.loads(sync.get(RECORD_KEY.format(event_id)))
    score = sync.zscore(TYPE_KEY.format("approval"), event_id)
    assert score is not None
    assert record["timestamp"] == score, (
        f"记录 timestamp 与 zset score 漂移: {record['timestamp']} != {score}"
    )


def test_event_ttl_env_override_and_invalid_fallback(monkeypatch, caplog):
    """``PLAITA_EVENT_TTL_SECONDS``：合法覆盖；非法/非正数回退 + WARNING。"""
    import logging

    from plaita.event.redis import (
        DEFAULT_EVENT_TTL_SECONDS,
        event_retention_ttl_seconds,
    )

    monkeypatch.setenv("PLAITA_EVENT_TTL_SECONDS", "120")
    assert event_retention_ttl_seconds() == 120

    with caplog.at_level(logging.WARNING, logger="plaita.event.redis"):
        monkeypatch.setenv("PLAITA_EVENT_TTL_SECONDS", "0")
        assert event_retention_ttl_seconds() == DEFAULT_EVENT_TTL_SECONDS
        monkeypatch.setenv("PLAITA_EVENT_TTL_SECONDS", "not-a-number")
        assert event_retention_ttl_seconds() == DEFAULT_EVENT_TTL_SECONDS

    warnings = [r.getMessage() for r in caplog.records if "PLAITA_EVENT_TTL_SECONDS" in r.getMessage()]
    assert len(warnings) >= 2, f"非法 TTL 未打 WARNING: {warnings}"


# ---------------------------------------------------------------- 6


def test_console_list_dedupes_both_shapes():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)

    event_id = "evt-both-shapes"
    ts = time.time()
    base = {
        "event_id": event_id,
        "event_type": "approval",
        "timestamp": ts,
        "correlation_id": None,
        "source": "plaita-console",
        "tenant_id": TENANT_A,
    }
    sync.set(
        RECORD_KEY.format(event_id),
        json.dumps({**base, "data": {"shape": "canonical"}}, default=str),
    )
    sync.set(
        LEGACY_RECORD_KEY.format(event_id),
        json.dumps({**base, "data": {"shape": "legacy"}}, default=str),
    )

    r = client.get("/api/events")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 1, f"两种键形未按 event_id 去重: {body}"
    assert len(body["events"]) == 1
    assert body["events"][0]["data"]["shape"] == "canonical", "canonical 未优先"


# ---------------------------------------------------------------- 7


def test_legacy_record_still_readable():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)

    event_id = "evt-legacy-only"
    sync.set(
        LEGACY_RECORD_KEY.format(event_id),
        json.dumps({
            "event_id": event_id,
            "event_type": "approval",
            "data": {"legacy": True},
            "timestamp": time.time(),
            "source": "old-console",
            "tenant_id": TENANT_A,
        }),
    )
    r = client.get("/api/events")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == 1
    assert body["events"][0]["event_id"] == event_id
    assert body["degraded"] is False


# ---------------------------------------------------------------- 8


def test_old_reader_sees_canonical():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync)
    r = client.post("/api/events/publish", json={"event_type": "approval", "data": {}})
    event_id = r.json()["event_id"]

    records = _old_console_scan(sync)
    canonical = [
        (key, info) for key, info in records
        if key.startswith("plaita:event:events:") and info.get("event_id") == event_id
    ]
    assert canonical, (
        "回滚后的旧 console 扫描逻辑读不到 canonical 记录（键形/序列化变了）"
    )
    assert canonical[0][1]["data"] == {}


# ---------------------------------------------------------------- 9


def test_trusted_tenant_and_data_strip():
    server = fakeredis.FakeServer()
    sync = FakeRedis(server=server, decode_responses=True)
    client = _publish_client(sync, tenant=TENANT_A)
    r = client.post(
        "/api/events/publish",
        json={
            "event_type": "approval",
            "data": {"tenant_id": TENANT_B, "approved": True},
            "correlation_id": "exec-a",
        },
    )
    assert r.status_code == 200, r.text
    event_id = r.json()["event_id"]

    stored = json.loads(sync.get(RECORD_KEY.format(event_id)))
    assert stored["tenant_id"] == TENANT_A, "受信租户未写入 canonical 记录顶层"
    assert "tenant_id" not in stored["data"], "客户端自报租户必须被剥离"
    assert stored["data"]["approved"] is True
    assert stored["source"] == "plaita-console"
