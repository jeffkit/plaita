"""plaita#166：执行表示面的 ``$INPUT`` 明文凭据 —— 非提交者读面必须先脱敏。

``#157`` 的独立验收做了带真 Redis + 真 worker 的 sink 攻击，结论是：``params``
与 ``$INPUT`` 属**功能数据**（flow 要用它），掩码族（``#158``/``#159``/``#160``/
``#161``/``#165``）只覆盖**日志**出口，载体（队列）与盘上（execution row 的
``context.$INPUT``）都按引用留存明文。

本文件钉住的是**读面**那一半（本单取 #166 的选项 B）：

* 队列载体、worker 侧 ``$INPUT``/checkpoint 的**功能副本逐字节不变**（那是
  ``#157``/``#122`` 的通道，本单不改）；
* 控制台的**执行表示**（``GET /executions/{id}`` 与 SSE ``initial_state``）在
  **序列化之前**把凭据值换成占位——键名保留（看得出抹的是哪个键），值不可读；
* 本地单机档的 ``input``（``input_json`` 的记录副本）同判。

needle 复用 ``#88``/``#158`` 族：URL userinfo 口令（``https://u:pw@host``）与
显式凭据键（``api_key``）。判据是「响应体里没有这些字节」，不是「调用了某个
函数」——静态判据是可伪造通道。
"""
import json
import sys
from pathlib import Path

from fakeredis import FakeRedis
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402

QUEUE = "plaita:flow:queue"
# 与 main.py 同式的 admin key 鉴权（SSE raw Route 端点内自己裁决，见 C4-4）
ADMIN_KEY = "w6a-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s

# ---- needle：分别是「凭据键的值」与「URL userinfo 口令」（+ path token）----
NEEDLE_KEY = "sk-live-3f9a2b7c1d4e"
NEEDLE_URL_PW = "Passw0rd-9f2a"
NEEDLE_PATH_TOKEN = "Ks9Qm4Zt"
REPO_URL = f"https://svcuser:{NEEDLE_URL_PW}@git.example.com/x.git"
HOOK_URL = f"https://open.feishu.cn/open-apis/bot/v2/hook/{NEEDLE_PATH_TOKEN}"
NEEDLES = (NEEDLE_KEY, NEEDLE_URL_PW, NEEDLE_PATH_TOKEN)
PARAMS = {
    "watch_agent": "deepseek-flash",
    "api_key": NEEDLE_KEY,
    "repo": REPO_URL,
    "notify": HOOK_URL,
}


def _client(redis, *, local=None) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = local is not None
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app)


def _seed(redis, eid: str, context=None, status: str = "suspended") -> None:
    redis.set(
        f"plaita:execution:{eid}",
        json.dumps(
            {
                "execution_id": eid,
                "flow_id": "f1",
                "flow_version": "1.0.0",
                "status": status,
                "context": {"$INPUT": PARAMS, "$LAST_NODE": "start"} if context is None else context,
            }
        ),
    )


def _assert_no_needle(text: str) -> None:
    leaked = [n for n in NEEDLES if n in text]
    assert not leaked, f"执行表示里仍留着明文凭据：{leaked}\n{text[:600]}"


def _carrier(redis, stream_key: str = QUEUE) -> dict:
    entries = redis.xrange(stream_key)
    assert len(entries) == 1, f"预期恰一条消息，实际 {len(entries)}"
    fields = entries[0][1]
    payload = fields["payload"] if "payload" in fields else fields[b"payload"]
    return json.loads(payload)


class _StubLocal:
    """本地单机档接缝：只提供路由用到的两个读函数。"""

    def __init__(self, info):
        self._info = info

    def get_local_execution(self, execution_id, tenant_id=None):
        return dict(self._info)


class TestDetailRepresentationIsRedacted:
    """`GET /executions/{id}` —— 同租户任何人都能读到的那个面。"""

    def test_cluster_detail_has_no_needle(self):
        redis = FakeRedis(decode_responses=True)
        _seed(redis, "e166-1")

        r = _client(redis).get("/api/executions/e166-1")

        assert r.status_code == 200, r.text
        _assert_no_needle(r.text)

    def test_cluster_detail_keeps_shape_and_functional_peers(self):
        """脱敏是**具名**的：键名与非凭据值原样，凭据值换占位。"""
        redis = FakeRedis(decode_responses=True)
        _seed(redis, "e166-2")

        ctx = _client(redis).get("/api/executions/e166-2").json()["context"]

        inp = ctx["$INPUT"]
        assert inp["watch_agent"] == "deepseek-flash"
        assert inp["api_key"] == "***"
        assert inp["repo"] != REPO_URL and inp["notify"] != HOOK_URL
        assert ctx["$LAST_NODE"] == "start"

    def test_local_detail_input_face_is_redacted(self, monkeypatch):
        """本地单机档：``input``（``input_json`` 的记录副本）同判，不落明文。"""
        info = {
            "execution_id": "e166-3",
            "flow_id": "f1",
            "status": "completed",
            "context": {"$INPUT": PARAMS},
            "input": dict(PARAMS),
        }
        monkeypatch.setattr(
            executions_api, "get_local_executor", lambda request: _StubLocal(info)
        )

        r = _client(FakeRedis(decode_responses=True), local=True).get(
            "/api/executions/e166-3"
        )

        assert r.status_code == 200, r.text
        _assert_no_needle(r.text)

    def test_local_list_face_is_redacted(self, monkeypatch):
        """本地档列表行同样带全量 ``context``/``input``——同一个面，同判。"""
        info = {
            "execution_id": "e166-3b",
            "flow_id": "f1",
            "status": "completed",
            "context": {"$INPUT": PARAMS},
            "input": dict(PARAMS),
        }

        class _StubLocalList(_StubLocal):
            def list_local_executions(self, status=None, flow_id=None, tenant_id=None):
                return [dict(self._info)]

        monkeypatch.setattr(
            executions_api, "get_local_executor", lambda request: _StubLocalList(info)
        )

        r = _client(FakeRedis(decode_responses=True), local=True).get("/api/executions")

        assert r.status_code == 200, r.text
        _assert_no_needle(r.text)


class TestSseInitialStateIsRedacted:
    """SSE ``initial_state`` 同样是整行执行表示——同一条脱敏必须覆盖它。

    走**真路由函数**（``stream_execution_endpoint``，raw Route），只把 async
    pubsub 客户端换成桩——这样「路由有没有把脱敏接上」是自测的，而不是靠读代码。
    """

    def _first_event(self, monkeypatch, redis, eid: str):
        import anyio
        from starlette.requests import Request

        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        _seed(redis, eid)

        app = FastAPI()
        app.state.redis = redis
        app.state.local_mode = False
        app.state.execution_sse_aioredis = _StubAsyncClient()
        request = Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": f"/api/executions/{eid}/stream",
                "raw_path": f"/api/executions/{eid}/stream".encode(),
                "query_string": b"",
                "root_path": "",
                "server": ("testserver", 80),
                "client": ("testclient", 50000),
                "headers": [(b"x-admin-api-key", ADMIN_KEY.encode())],
                "path_params": {"execution_id": eid},
                "app": app,
            }
        )

        async def _run():
            response = await executions_api.stream_execution_endpoint(request)
            agen = response.body_iterator
            try:
                return await agen.__anext__()
            finally:
                await agen.aclose()

        return anyio.run(_run)

    def test_initial_state_event_has_no_needle(self, monkeypatch):
        first = self._first_event(monkeypatch, FakeRedis(decode_responses=True), "e166-4")
        text = first if isinstance(first, str) else json.dumps(first, default=str)

        assert "initial_state" in text, text[:300]
        _assert_no_needle(text)


class _StubAsyncClient:
    """``_execution_stream_async_redis`` 的测试接缝：只提供 ``pubsub()``。"""

    def pubsub(self):
        return _StubPubSub()


class _StubPubSub:
    async def subscribe(self, channel):
        return None

    async def get_message(self, **kwargs):
        return None

    async def unsubscribe(self, channel):
        return None

    async def aclose(self):
        return None


class TestFunctionalCopyIsUntouched:
    """功能副本（队列载体 / 被脱敏对象的本体）必须逐字节不变。"""

    def test_carrier_keeps_plaintext_params(self):
        """载体是 #157 的功能通道：本单不得改动它（worker 靠它绑 `$INPUT`）。"""
        redis = FakeRedis(decode_responses=True)
        r = _client(redis).post(
            "/api/executions", json={"flow_id": "f1", "params": dict(PARAMS)}
        )
        assert r.status_code == 200, r.text

        msg = _carrier(redis)
        assert msg["params"] == PARAMS, "载体里的调度入参被改动了（#157 通道回归）"

    def test_redaction_does_not_mutate_its_input(self):
        """脱敏返回**新对象**：盘上/内存里的功能副本一个字节都不许被改。"""
        from plaita.core.url_redaction import redact_credential_values

        original = json.loads(json.dumps(PARAMS))
        out = redact_credential_values(PARAMS)

        assert out is not PARAMS and out != PARAMS
        assert PARAMS == original, "脱敏就地改了功能副本"
        assert out["watch_agent"] == PARAMS["watch_agent"]
