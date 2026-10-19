"""#259：logs SSE 空闲消费不得阻塞事件循环。

历史实现（`api/logs.py`）在 async 事件生成器里调同步
``pubsub.get_message(timeout=1.0)``：idle 时生成器既不 yield 也不让出事件
循环，单条 viewer 连接即可冻结整个 console（E2E：idle 流期间 /health 2s
超时）。修法与 executions.py 的 C4-4 一致——redis.asyncio pubsub，异步
``get_message`` 带超时，断开检查前有真实挂起点。

本组用共享 FakeServer 的 sync+async fakeredis 复刻发布/消费链路：

- jitter 回归对齐 C4-4 ``test_event_loop_not_blocked_while_stream_pending``；
- /health 探针形态：idle 流挂起时同循环内其他请求亚秒级返回；
- 载荷语义零变化：精确频道才收、通配频道按条目收窄、事件形状逐字不变。
"""
import asyncio
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import fakeredis
import httpx
import pytest
from fastapi import FastAPI

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import logs as logs_api  # noqa: E402

SERVICE = "flow_worker"
INSTANCE = "inst-a"
EXACT_CHANNEL = f"plaita:logs:stream:{SERVICE}:{INSTANCE}"
OTHER_CHANNEL = f"plaita:logs:stream:{SERVICE}:inst-b"

MSG = {
    "timestamp": "2026-10-19T10:00:00",
    "level": "ERROR",
    "service_type": SERVICE,
    "instance_id": INSTANCE,
    "tenant_id": "default",
    "message": "boom-1",
    "context": "{}",
}


class _StubRequest:
    """stream_logs 只用 request.is_disconnected 与 app.state 的 async 客户端。

    is_disconnected 真实让渡（sleep(0)）：否则 fetch/disconnect 检查会把
    生成器变成热循环。``redis=`` 参数仍传 sync FakeRedis——修复后它只作
    端点可用性闸门；负控还原同步实现时它会重新成为阻塞源。
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


@pytest.fixture()
def env():
    """sync（发布/闸门）+ async（消费）共享一个 FakeServer。"""
    server = fakeredis.FakeServer()
    sync_r = fakeredis.FakeRedis(server=server, decode_responses=True)
    async_r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    return sync_r, async_r


async def _open_stream(sync_r, async_r, *, service_type=None, instance_id=None):
    resp = await logs_api.stream_logs(
        request=_StubRequest(async_r),
        service_type=service_type,
        instance_id=instance_id,
        level=None,
        redis=sync_r,
    )
    return resp.body_iterator


def test_event_loop_not_blocked_while_logs_stream_pending(env):
    """核心回归（对齐 C4-4）：idle 流挂起时 0.05s 睡眠不得漂移。

    同步实现每轮 get_message(timeout=1.0) 独占事件循环 ~1s；异步实现下
    jitter 应远低于 0.5s 阈值。
    """
    sync_r, async_r = env

    async def scenario():
        agen = await _open_stream(sync_r, async_r)
        consumer = asyncio.create_task(agen.__anext__())  # 挂在 get_message
        await asyncio.sleep(0.1)

        t0 = time.monotonic()
        await asyncio.sleep(0.05)
        jitter = time.monotonic() - t0
        assert jitter < 0.5, f"事件循环被 logs SSE 消费阻塞（sleep 漂移 {jitter:.2f}s）"

        sync_r.publish(EXACT_CHANNEL, json.dumps(MSG))
        event = await asyncio.wait_for(consumer, timeout=5)
        assert event["event"] == "log"
        await agen.aclose()

    asyncio.run(scenario())


def test_health_probe_served_while_logs_stream_idle(env):
    """/health 探针形态：idle 流挂起时其他请求必须亚秒级返回。"""
    sync_r, async_r = env

    async def scenario():
        agen = await _open_stream(sync_r, async_r)
        consumer = asyncio.create_task(agen.__anext__())  # idle：无消息
        await asyncio.sleep(0.1)

        app = FastAPI()

        @app.get("/health")
        async def health():
            # 真实探针要经 socket 读写（多轮事件循环让渡）；进程内 ASGITransport
            # 若全程无挂起点会单步跑完，测不出同步 get_message 的阻塞。
            await asyncio.sleep(0.01)
            return {"status": "healthy"}

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://console"
        ) as client:
            for _ in range(3):
                started = time.monotonic()
                resp = await asyncio.wait_for(client.get("/health"), timeout=2.0)
                elapsed = time.monotonic() - started
                assert resp.status_code == 200
                assert elapsed < 0.5, (
                    f"idle logs SSE 期间 /health 被阻塞（{elapsed:.2f}s）"
                )

        sync_r.publish(EXACT_CHANNEL, json.dumps(MSG))
        event = await asyncio.wait_for(consumer, timeout=5)
        assert event["event"] == "log"
        await agen.aclose()

    asyncio.run(scenario())


def test_payload_semantics_unchanged_exact_channel(env):
    """精确频道：只发 instance 自己的消息，事件形状与载荷逐字不变。"""
    sync_r, async_r = env

    async def scenario():
        agen = await _open_stream(
            sync_r, async_r, service_type=SERVICE, instance_id=INSTANCE
        )
        consumer = asyncio.create_task(agen.__anext__())
        await asyncio.sleep(0.3)  # subscribe 生效

        # 同 service_type 的另一实例走不同精确频道——不得到达本订阅
        sync_r.publish(
            OTHER_CHANNEL,
            json.dumps({**MSG, "instance_id": "inst-b", "message": "other"}),
        )
        await asyncio.sleep(0.1)
        sync_r.publish(EXACT_CHANNEL, json.dumps(MSG))

        event = await asyncio.wait_for(consumer, timeout=5)
        assert event == {"event": "log", "data": json.dumps(MSG)}
        await agen.aclose()

    asyncio.run(scenario())


def test_payload_semantics_unchanged_wildcard_instance_filter(env):
    """通配频道 + 仅 instance_id：按条目 instance_id 收窄（#102 不回退）。"""
    sync_r, async_r = env

    async def scenario():
        agen = await _open_stream(sync_r, async_r, instance_id=INSTANCE)
        consumer = asyncio.create_task(agen.__anext__())
        await asyncio.sleep(0.3)

        sync_r.publish(
            OTHER_CHANNEL,
            json.dumps({**MSG, "instance_id": "inst-b", "message": "other"}),
        )
        await asyncio.sleep(0.1)
        sync_r.publish(EXACT_CHANNEL, json.dumps(MSG))

        event = await asyncio.wait_for(consumer, timeout=5)
        assert json.loads(event["data"]) == MSG
        await agen.aclose()

    asyncio.run(scenario())
