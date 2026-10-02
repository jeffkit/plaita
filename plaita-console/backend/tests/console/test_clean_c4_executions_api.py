"""C4-3/C4-4 回归（console 后端）。

C4-3：执行列表服务端投影 + 终态 TTL 读时补偿。
- 列表路径 SCAN 后按块 EVAL+cjson 投影取列表字段，完整 context 不再逐键
  GET/json.loads 进 console 内存（EVAL 不可用时回退全量路径，行为不劣化）；
- 详情读取对无 TTL 的终态键补 EXPIRE（fenced 写路径落盘的终态键没有 TTL）；
- 控制面直写的终态（挂起执行取消）带 TTL。

C4-4：SSE 一次性票据 + 双轨鉴权 + redis.asyncio pubsub。
- EventSource 无法带头，鉴权部署下直连 401——先 POST ticket 再 ?ticket= 连接；
- stream 路由 raw Route 注册（绕过 router 级 require_auth，端点内双轨裁决：
  ticket 严格校验，否则回退 require_auth），挂载方式与 main.py 一致；
- async pubsub 消费不阻塞事件循环：两个并发 SSE 流期间其他 API 响应不受影响。
"""
import asyncio
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, NamedTuple

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import fakeredis  # noqa: E402

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402

ADMIN_KEY = "c4-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def client(redis, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers(tenant: str = None):
    h = {"X-Admin-API-Key": ADMIN_KEY}
    if tenant:
        h["X-Tenant-ID"] = tenant
    return h


def _seed_execution(redis, execution_id, status="running", flow_id="f1",
                    start_time="2026-09-01T00:00:00", tenant=None,
                    context_size=200_000):
    ns = f"plaita:{tenant}" if tenant else "plaita"
    key = f"{ns}:execution:{execution_id}"
    redis.set(key, json.dumps({
        "execution_id": execution_id,
        "flow_id": flow_id,
        "flow_name": "Demo Flow",
        "status": status,
        "start_time": start_time,
        "last_update_time": start_time,
        "context": {"blob": "x" * context_size},
    }))
    return key


class GetSpyRedis:
    """记录 get(key) 调用的透明代理：断言列表路径不 GET 状态键全量。"""

    def __init__(self, inner):
        self._inner = inner
        self.get_calls = []

    def get(self, key):
        self.get_calls.append(key if isinstance(key, str) else key.decode())
        return self._inner.get(key)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class NoEvalRedis(GetSpyRedis):
    """模拟无 Lua 的受限环境：EVAL 抛错 → 回退全量路径。"""

    def eval(self, *args, **kwargs):
        raise RuntimeError("EVAL disabled in this environment")


class TestListProjection:
    def test_projection_avoids_full_state_gets(self, redis, monkeypatch):
        from fastapi import Depends, FastAPI
        from starlette.testclient import TestClient as _TC

        _seed_execution(redis, "e1", status="running")
        _seed_execution(redis, "e2", status="completed", flow_id="f2",
                        start_time="2026-09-02T00:00:00")
        # 同前缀机制键：不得出现在列表，也不得干扰投影
        redis.set("plaita:execution:cancel:e1", "2026-09-01T00:00:00")
        redis.set("plaita:execution:lease:e1", "holder-1:1")
        redis.xadd("plaita:execution:some-dlq", {"payload": "x"})

        spy = GetSpyRedis(redis)
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        app = FastAPI()
        app.state.redis = spy
        app.state.local_mode = False
        app.include_router(
            executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
        )
        client = _TC(app)

        resp = client.get("/api/executions", headers=_headers())
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["total"] == 2
        rows = {r["execution_id"]: r for r in body["executions"]}
        assert set(rows) == {"e1", "e2"}
        assert rows["e1"]["status"] == "running"
        assert rows["e2"]["flow_id"] == "f2"
        assert rows["e1"]["tenant_id"] == "default"
        # 列表行不携带全量 context
        assert rows["e1"]["context"] is None
        # 排序：start_time 最新在前
        assert [r["execution_id"] for r in body["executions"]] == ["e2", "e1"]
        # 状态键全量 GET 从未发生（投影走 EVAL）
        state_gets = [k for k in spy.get_calls if ":execution:e" in k]
        assert state_gets == [], f"列表路径不应全量 GET 状态键: {state_gets}"

    def test_filters_and_pagination(self, redis, client):
        _seed_execution(redis, "e1", status="running", flow_id="f1",
                        start_time="2026-09-01T00:00:00")
        _seed_execution(redis, "e2", status="completed", flow_id="f2",
                        start_time="2026-09-02T00:00:00")
        _seed_execution(redis, "e3", status="completed", flow_id="f1",
                        start_time="2026-09-03T00:00:00")
        r = client.get("/api/executions?status=completed", headers=_headers())
        assert [e["execution_id"] for e in r.json()["executions"]] == ["e3", "e2"]
        r = client.get("/api/executions?flow_id=f2", headers=_headers())
        assert [e["execution_id"] for e in r.json()["executions"]] == ["e2"]
        r = client.get("/api/executions?page=2&size=2", headers=_headers())
        body = r.json()
        assert body["total"] == 3
        assert body["page"] == 2
        assert [e["execution_id"] for e in body["executions"]] == ["e1"]

    def test_fallback_full_path_when_eval_unavailable(self, redis, monkeypatch):
        _seed_execution(redis, "e1", status="running")
        _seed_execution(redis, "e2", status="completed", flow_id="f2",
                        start_time="2026-09-02T00:00:00")
        spy = NoEvalRedis(redis)
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        from fastapi import Depends, FastAPI

        app = FastAPI()
        app.state.redis = spy
        app.state.local_mode = False
        app.include_router(
            executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
        )
        client = TestClient(app)
        resp = client.get("/api/executions", headers=_headers())
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        rows = {r["execution_id"]: r for r in body["executions"]}
        assert rows["e1"]["status"] == "running"
        assert rows["e2"]["flow_id"] == "f2"
        # 回退路径行同样保持轻量（不携带全量 context），只是取数方式退化为全量 GET
        assert rows["e1"]["context"] is None
        state_gets = [k for k in spy.get_calls if ":execution:e" in k]
        assert len(state_gets) == 2

    def test_tenant_view_isolated(self, redis, client):
        _seed_execution(redis, "e-acme", tenant="acme")
        _seed_execution(redis, "e-def", tenant="def")
        r = client.get("/api/executions", headers=_headers("acme"))
        assert [e["execution_id"] for e in r.json()["executions"]] == ["e-acme"]


class TestTerminalTTLHeal:
    def test_detail_read_heals_missing_ttl_on_terminal(self, redis, client):
        key = _seed_execution(redis, "e-heal", status="completed")
        assert redis.ttl(key) == -1
        r = client.get("/api/executions/e-heal", headers=_headers())
        assert r.status_code == 200
        assert r.json()["status"] == "completed"
        assert redis.ttl(key) > 0, "终态键读取后应被补上 TTL"

    def test_non_terminal_not_healed(self, redis, client):
        key = _seed_execution(redis, "e-run", status="running")
        r = client.get("/api/executions/e-run", headers=_headers())
        assert r.status_code == 200
        assert redis.ttl(key) == -1

    def test_suspended_cancel_write_has_ttl(self, redis, client):
        key = _seed_execution(redis, "e-susp", status="suspended")
        r = client.post("/api/executions/e-susp/cancel", headers=_headers())
        assert r.status_code == 200, r.text
        assert json.loads(redis.get(key))["status"] == "cancelled"
        assert redis.ttl(key) > 0, "控制面直写的终态键应带 TTL"


# ---- C4-4：SSE 一次性票据 + 双轨鉴权 + async pubsub ----
#
# 测试基础设施说明：starlette 1.7 的 TestClient.handle_request 会等 ASGI 响应
# **整体完成**才返回（testclient.py `portal.call(self.app, ...)`）——无限 SSE 流
# 永远不完成，TestClient 一律挂死（历史 stream 路由因此从未有测试覆盖）。
# 因此：JSON 路径（票据签发、401/404）用 TestClient；流式路径用手工 ASGI
# 驱动（_drive_asgi）；生成器语义用模块级 _execution_event_stream 直测。

STREAM_PATH = "/api/executions/{eid}/stream"


class _StreamEnv(NamedTuple):
    redis: Any  # sync FakeRedis（与 async 共享 FakeServer）
    app: Any
    client: TestClient


@pytest.fixture()
def stream_env(monkeypatch):
    """共享 FakeServer 的 sync+async fakeredis + 与 main.py 同式挂载的 app。"""
    server = fakeredis.FakeServer()
    sync_r = fakeredis.FakeRedis(server=server, decode_responses=True)
    async_r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = sync_r
    app.state.local_mode = False
    # 注入 app 级 async 客户端（_execution_stream_async_redis 的测试接缝）
    app.state.execution_sse_aioredis = async_r
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return _StreamEnv(sync_r, app, TestClient(app))


async def _drive_asgi(app, method, path, headers=None, query=b"", until=None,
                      timeout=8.0):
    """手工驱动一次流式 ASGI 请求。

    返回 (status, body_bytes, finished)：finished=True 表示 app 侧响应已完整
    结束（401/404 等 JSON 短响应）；SSE 成功路径在 until 子串出现（或超时）
    后取消 app 任务——生成器 finally 由此触发，等于客户端断连。
    """
    import anyio

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query,
        "root_path": "",
        "server": ("testserver", 80),
        "client": ("testclient", 50000),
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
    }
    box = {"status": None, "done": False}
    chunks: list = []

    async def send(message):
        if message["type"] == "http.response.start":
            box["status"] = message["status"]
        elif message["type"] == "http.response.body":
            chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                box["done"] = True

    async def receive():
        await anyio.sleep(math.inf)  # 流式客户端不再发数据；挂起至取消

    async def run():
        try:
            await app(scope, receive, send)
        except Exception:
            pass
        finally:
            box["done"] = True

    async with anyio.create_task_group() as tg:
        tg.start_soon(run)
        deadline = time.time() + timeout
        while time.time() < deadline:
            body = b"".join(chunks)
            if box["done"] or (until is not None and until in body):
                break
            await anyio.sleep(0.02)
        finished = box["done"]  # 快照须在取消前：cancel 会经 run() 的 finally 置 done
        tg.cancel_scope.cancel()
    return box["status"], b"".join(chunks), finished


def _drive(app, **kwargs):
    import anyio

    return anyio.run(lambda: _drive_asgi(app, **kwargs))


class TestStreamTicket:
    def test_ticket_mint(self, stream_env):
        """鉴权客户端换票据：60s、含 ticket 值。"""
        redis, _app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        r = client.post("/api/executions/e1/stream/ticket", headers=_headers())
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["ticket"] and body["expires_in"] == 60

    def test_ticket_has_ttl(self, stream_env):
        """票据键 60s TTL 自清理。"""
        redis, _app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        ticket = client.post("/api/executions/e1/stream/ticket", headers=_headers()).json()["ticket"]
        ttl = redis.ttl(f"plaita:sse:ticket:{ticket}")
        assert 0 < ttl <= 60

    def test_ticket_e2e_stream_connect_without_headers(self, stream_env):
        """端到端：POST 换票据 → **无头** GET ?ticket= 建立 SSE 并收到 initial_state。

        全程走与 main.py 相同的挂载（router 级 require_auth），证明 raw Route
        双轨鉴权真实生效——EventSource 场景的命门。
        """
        redis, app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        ticket = client.post("/api/executions/e1/stream/ticket", headers=_headers()).json()["ticket"]
        status, body, done = _drive(
            app, method="GET",
            path=STREAM_PATH.format(eid="e1"),
            query=f"ticket={ticket}".encode(),
            until=b"initial_state",
        )
        assert status == 200, body
        assert b"initial_state" in body
        assert not done, "SSE 流应保持推送（而非一次性响应）"

    def test_ticket_single_use_e2e(self, stream_env):
        """票据一次性：首个流消费后，同一票据再用 → 401（无头请求）。"""
        redis, app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        ticket = client.post("/api/executions/e1/stream/ticket", headers=_headers()).json()["ticket"]
        status, _body, _done = _drive(
            app, method="GET",
            path=STREAM_PATH.format(eid="e1"),
            query=f"ticket={ticket}".encode(),
            until=b"initial_state",
        )
        assert status == 200
        status2, body2, done2 = _drive(
            app, method="GET",
            path=STREAM_PATH.format(eid="e1"),
            query=f"ticket={ticket}".encode(),
        )
        assert status2 == 401 and done2
        assert "票据" in body2.decode()

    def test_header_auth_still_accepted_on_stream(self, stream_env):
        """双轨并行：无票据但带头鉴权的 stream 请求照常工作。"""
        redis, app, _client = stream_env
        _seed_execution(redis, "e1", status="running")
        status, body, _done = _drive(
            app, method="GET",
            path=STREAM_PATH.format(eid="e1"),
            headers=_headers(),
            until=b"initial_state",
        )
        assert status == 200
        assert b"initial_state" in body

    def test_ticket_restores_tenant_context_e2e(self, stream_env):
        """票据恢复签发方租户上下文：无头请求按签发视角定位执行。"""
        redis, app, client = stream_env
        _seed_execution(redis, "e-acme", tenant="acme")
        ticket = client.post(
            "/api/executions/e-acme/stream/ticket", headers=_headers(tenant="acme")
        ).json()["ticket"]
        status, body, _done = _drive(
            app, method="GET",
            path=STREAM_PATH.format(eid="e-acme"),
            query=f"ticket={ticket}".encode(),
            until=b"initial_state",
        )
        assert status == 200, "票据应恢复 acme 租户视角"
        assert b"initial_state" in body

    def test_invalid_ticket_rejected_even_with_auth(self, stream_env):
        """无效票据一律 401——即使请求本身带头鉴权（票据路径严格校验）。"""
        redis, _app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        r = client.get(
            f"{STREAM_PATH.format(eid='e1')}?ticket=bogus-ticket", headers=_headers()
        )
        assert r.status_code == 401

    def test_ticket_bound_to_execution(self, stream_env):
        """票据绑定 execution：e1 的票据用于 e2 → 401。"""
        redis, _app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        _seed_execution(redis, "e2", status="running")
        ticket = client.post("/api/executions/e1/stream/ticket", headers=_headers()).json()["ticket"]
        r = client.get(f"{STREAM_PATH.format(eid='e2')}?ticket={ticket}")
        assert r.status_code == 401

    def test_expired_or_missing_ticket_401(self, stream_env):
        """过期/不存在的票据 → 401。"""
        redis, _app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        r = client.get(f"{STREAM_PATH.format(eid='e1')}?ticket=never-existed")
        assert r.status_code == 401

        ticket = client.post("/api/executions/e1/stream/ticket", headers=_headers()).json()["ticket"]
        redis.delete(f"plaita:sse:ticket:{ticket}")  # 模拟过期
        r = client.get(f"{STREAM_PATH.format(eid='e1')}?ticket={ticket}")
        assert r.status_code == 401

    def test_mint_for_unknown_execution_404(self, stream_env):
        _redis, _app, client = stream_env
        r = client.post("/api/executions/nope/stream/ticket", headers=_headers())
        assert r.status_code == 404

    def test_stream_without_any_auth_401(self, stream_env):
        """无票据无头的 stream 请求 → 401（require_auth 回退分支）。"""
        redis, _app, client = stream_env
        _seed_execution(redis, "e1", status="running")
        r = client.get(STREAM_PATH.format(eid="e1"))
        assert r.status_code == 401

    def test_stream_unknown_execution_404_after_auth(self, stream_env):
        _redis, _app, client = stream_env
        r = client.get(STREAM_PATH.format(eid="nope"), headers=_headers())
        assert r.status_code == 404

    def test_mint_captures_minting_tenant(self, stream_env):
        """票据载荷钉住签发方租户视角（消费端据此恢复；平台视角签发为 null）。"""
        redis, _app, client = stream_env
        _seed_execution(redis, "e-acme", tenant="acme")
        ticket = client.post(
            "/api/executions/e-acme/stream/ticket", headers=_headers(tenant="acme")
        ).json()["ticket"]
        payload = json.loads(redis.get(f"plaita:sse:ticket:{ticket}"))
        assert payload["tenant_id"] == "acme"
        assert payload["execution_id"] == "e-acme"

        # 平台视角（api-key 不带 X-Tenant-ID）可跨租户读，签发的票据为 null，
        # 消费端同为平台视角——与签发方权限一致，无越权放大
        ticket2 = client.post(
            "/api/executions/e-acme/stream/ticket", headers=_headers()
        ).json()["ticket"]
        payload2 = json.loads(redis.get(f"plaita:sse:ticket:{ticket2}"))
        assert payload2["tenant_id"] is None


class _FakeRequest:
    """直测 _execution_event_stream 用的最小 Request 替身。"""

    def __init__(self, app_state):
        from types import SimpleNamespace

        self.app = SimpleNamespace(state=app_state)

    async def is_disconnected(self):
        return False


class FlakyPubsub:
    """模拟 Redis 抖动：前 N 次 get_message 抛错。"""

    def __init__(self, inner, failures=2):
        self._inner = inner
        self._failures = failures

    async def subscribe(self, *args, **kwargs):
        return await self._inner.subscribe(*args, **kwargs)

    async def get_message(self, **kwargs):
        if self._failures > 0:
            self._failures -= 1
            raise RuntimeError("simulated redis blip")
        return await self._inner.get_message(**kwargs)

    async def unsubscribe(self, *args, **kwargs):
        return await self._inner.unsubscribe(*args, **kwargs)

    async def aclose(self):
        return await self._inner.aclose()


class FlakyAsyncClient:
    def __init__(self, inner):
        self._inner = inner

    def pubsub(self):
        return FlakyPubsub(self._inner.pubsub())


class TestAsyncPubsubStream:
    def test_update_pushed_via_async_pubsub(self, stream_env):
        """集群档 SSE：pubsub 发布 → update 事件推送（async 消费真实链路）。"""
        import anyio
        from types import SimpleNamespace

        redis, app, _client = stream_env

        async def scenario():
            request = _FakeRequest(app.state)
            gen = executions_api._execution_event_stream(
                request, {"execution_id": "e1", "status": "running"},
                "plaita:execution:events:e1",
            )
            initial = await gen.__anext__()
            assert initial["event"] == "initial_state"
            await asyncio.sleep(0.15)  # fakeredis 订阅落定
            consumer = asyncio.create_task(gen.__anext__())
            for i in range(3):
                redis.publish(
                    "plaita:execution:events:e1",
                    json.dumps({"execution_id": "e1", "status": "running", "seq": i}),
                )
                await asyncio.sleep(0.05)
            update = await asyncio.wait_for(consumer, timeout=5)
            assert update["event"] == "update"
            assert json.loads(update["data"])["seq"] == 0
            await gen.aclose()  # 触发 finally：pubsub 关闭

        anyio.run(scenario)

    def test_event_loop_not_blocked_while_stream_pending(self, stream_env):
        """核心回归：SSE 消费挂在 get_message 等待上时事件循环不被阻塞。

        历史实现同步 pubsub.get_message(timeout=1.0) 每轮阻塞事件循环至多
        1s——并发流期间所有其他协程/API 卡顿秒级。async 消费下 0.05s 睡眠
        不应漂移；同步实现下该测试会超阈值失败。
        """
        import anyio

        redis, app, _client = stream_env

        async def scenario():
            request = _FakeRequest(app.state)
            gen = executions_api._execution_event_stream(
                request, {"execution_id": "e1"}, "plaita:execution:events:e1"
            )
            await gen.__anext__()  # initial_state（订阅已建立）
            consumer = asyncio.create_task(gen.__anext__())  # 挂在 get_message
            await asyncio.sleep(0.1)
            t0 = time.monotonic()
            await asyncio.sleep(0.05)
            jitter = time.monotonic() - t0
            assert jitter < 0.5, f"事件循环被 SSE 消费阻塞（sleep 漂移 {jitter:.2f}s）"
            redis.publish("plaita:execution:events:e1", json.dumps({"n": 1}))
            update = await asyncio.wait_for(consumer, timeout=5)
            assert update["event"] == "update"
            await gen.aclose()

        anyio.run(scenario)

    def test_redis_blip_does_not_kill_stream(self, stream_env):
        """Redis 抖动（get_message 抛错）不终结 SSE：退避重试后继续推送。"""
        import anyio
        from types import SimpleNamespace

        redis, app, _client = stream_env

        async def scenario():
            state = SimpleNamespace(
                execution_sse_aioredis=FlakyAsyncClient(app.state.execution_sse_aioredis)
            )
            request = _FakeRequest(state)
            gen = executions_api._execution_event_stream(
                request, {"execution_id": "e1"}, "plaita:execution:events:e1"
            )
            # 两次 blip 各退避 0.5s 后仍正常发出 initial_state
            initial = await asyncio.wait_for(gen.__anext__(), timeout=5)
            assert initial["event"] == "initial_state"
            await asyncio.sleep(0.15)
            consumer = asyncio.create_task(gen.__anext__())
            redis.publish("plaita:execution:events:e1", json.dumps({"n": 2}))
            update = await asyncio.wait_for(consumer, timeout=5)
            assert json.loads(update["data"])["n"] == 2
            await gen.aclose()

        anyio.run(scenario)
