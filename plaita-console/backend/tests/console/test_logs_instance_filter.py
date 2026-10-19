"""#102：集群档 /api/logs 实例筛选必须按条目 instance_id 收窄。

修复前：/logs 只在 service_type 存在时才收窄 Redis key，收窄后也从不按条目
instance_id 过滤；日志页唯一能发出的查询形态是「只有 instance_id」（Cluster
卡片深链）——筛选静默失效，同 service_type 其他实例的日志混进结果。SSE 侧
同样只按 level 过滤。本文件以 fakeredis 复刻发布侧 key/频道格式
（plaita/server/log_handler.py：stream key 与 pubsub 频道均带 service_type
与 instance_id），覆盖列表与 SSE 两条路径。
"""
import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import pytest  # noqa: E402
import fakeredis  # noqa: E402
from fakeredis import FakeRedis  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from api import logs as logs_api  # noqa: E402

SERVICE = "flow_worker"
KEY_A = f"plaita:logs:{SERVICE}:worker-A"
KEY_B = f"plaita:logs:{SERVICE}:worker-B"
CHANNEL_A = f"plaita:logs:stream:{SERVICE}:worker-A"
CHANNEL_B = f"plaita:logs:stream:{SERVICE}:worker-B"

MSG_A = {
    "timestamp": "2026-10-16T10:00:00",
    "level": "ERROR",
    "service_type": SERVICE,
    "instance_id": "worker-A",
    "message": "A-error-1",
    "context": "{}",
}
MSG_B = {
    "timestamp": "2026-10-16T10:00:01",
    "level": "ERROR",
    "service_type": SERVICE,
    "instance_id": "worker-B",
    "message": "B-error-9",
    "context": "{}",
}


@pytest.fixture()
def client():
    # #259：SSE 走 redis.asyncio pubsub；sync/async 共享同一 FakeServer，
    # 发布侧仍可用 sync FakeRedis（与 C4-4 的执行流测试同款）。
    server = fakeredis.FakeServer()
    fake = FakeRedis(server=server, decode_responses=True)
    async_fake = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    fake.xadd(KEY_A, MSG_A)
    fake.xadd(KEY_B, MSG_B)
    app = FastAPI()
    app.state.local_mode = False
    app.state.redis = fake
    app.state.execution_sse_aioredis = async_fake
    app.include_router(logs_api.router, prefix="/api")
    return TestClient(app), fake, async_fake


def _rows(resp):
    assert resp.status_code == 200, resp.text
    return [(log["instance_id"], log["message"]) for log in resp.json()["logs"]]


# ---------------------------------------------------------------- 列表路径

def test_logs_instance_only_filters_entries(client):
    """日志页深链形态（只有 instance_id）：只准返回 worker-A 的行。"""
    http, _, _ = client
    assert _rows(http.get("/api/logs?instance_id=worker-A")) == [
        ("worker-A", "A-error-1")]


def test_logs_service_type_plus_instance_same_result(client):
    """service_type+instance_id 与只有 instance_id 的结果必须一致。"""
    http, _, _ = client
    assert _rows(http.get("/api/logs?service_type=flow_worker&instance_id=worker-A")) == [
        ("worker-A", "A-error-1")]


def test_logs_service_type_only_keeps_all_instances(client):
    """守卫：只按 service_type 筛选仍返回该类型全部实例（不许矫枉过正）。"""
    http, _, _ = client
    rows = _rows(http.get("/api/logs?service_type=flow_worker"))
    assert ("worker-A", "A-error-1") in rows
    assert ("worker-B", "B-error-9") in rows


def test_logs_no_filter_keeps_all(client):
    """守卫：无筛选返回全部。"""
    http, _, _ = client
    rows = _rows(http.get("/api/logs"))
    assert len(rows) == 2


# ---------------------------------------------------------------- SSE 路径

class _StubRequest:
    """stream_logs 只用 request.is_disconnected 与 app.state 的 async 客户端。

    is_disconnected 必须 await asyncio.sleep(0) 真实让渡：fakeredis 的
    pubsub.get_message(timeout) 不阻塞、立即返回 None，若这里用零窗口
    await，事件循环会被生成器热循环饿死（publish 与超时定时器都无法调度）。
    """

    def __init__(self, async_client):
        self.app = SimpleNamespace(
            state=SimpleNamespace(
                local_mode=False, execution_sse_aioredis=async_client
            )
        )

    async def is_disconnected(self):
        await asyncio.sleep(0)
        return False


def _drive_stream(fake, async_fake, *, service_type, instance_id, timeout=8.0):
    """起 stream_logs 生成器 → 订阅生效后依次发 B、A 消息 → 收集 log 事件。

    ``redis=fake`` 仍传 sync 客户端（端点签名的可用性闸门参数）；流式消费
    实际走 ``async_fake``——sync/async 共享 FakeServer，发布侧照旧用 sync。
    """

    async def _run():
        stub = _StubRequest(async_fake)
        resp = await logs_api.stream_logs(
            request=stub,
            service_type=service_type,
            instance_id=instance_id,
            level=None,
            redis=fake,
        )
        agen = resp.body_iterator
        events = []

        async def consume():
            while True:
                ev = await agen.__anext__()
                if ev.get("event") == "log":
                    events.append(json.loads(ev["data"]))
                    if events[-1].get("instance_id") == "worker-A":
                        return

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.3)  # psubscribe 生效
        fake.publish(CHANNEL_B, json.dumps(MSG_B))
        await asyncio.sleep(0.1)
        fake.publish(CHANNEL_A, json.dumps(MSG_A))
        await asyncio.wait_for(task, timeout=timeout)
        await agen.aclose()
        return events

    return asyncio.run(_run())


def test_stream_instance_only_drops_other_instances(client):
    """SSE 仅给 instance_id：worker-B 的消息不得产出 event: log。"""
    http, fake, async_fake = client
    events = _drive_stream(
        fake, async_fake, service_type=None, instance_id="worker-A")
    assert [e["instance_id"] for e in events] == ["worker-A"]
    assert all(e["message"] == "A-error-1" for e in events)


def test_stream_service_type_plus_instance_precise_channel(client):
    """SSE service_type+instance_id 齐备：走精确频道，B 根本到不了订阅。"""
    http, fake, async_fake = client
    events = _drive_stream(
        fake, async_fake, service_type=SERVICE, instance_id="worker-A")
    assert [e["instance_id"] for e in events] == ["worker-A"]
