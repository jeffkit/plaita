"""plaita#93 读面回归（console 后端）：执行详情端点透出**结构化** error。

worker 落盘 `state.error` 里的 `code` / `error_type` / `details` / `response`
（`plaita.server.flow_worker._error_fields`）必须原样穿过
`GET /api/executions/{id}`——console 侧是 `ExecutionInfo.error: Dict[str, Any]`，
不存在第二套错误码来源，本测试钉住「读面能取到」这一条。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import fakeredis  # noqa: E402

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402

ADMIN_KEY = "w15a-93-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis():
    return fakeredis.FakeRedis(decode_responses=True)


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


def _worker_shaped_error() -> dict:
    """用 worker 的真实落盘组装路径造一份 HTTP 节点失败 error。"""
    from plaita.core.errors import NodeException, NodeExecutionError
    from plaita.node.http import (
        HttpNodeErrorInfo,
        HttpNodeResponse,
        _summarize_error_frame_response,
    )
    from plaita.server.flow_worker import _error_fields

    response = HttpNodeResponse(status=502, status_text="Bad Gateway",
                                headers={"Set-Cookie": "sid=SUPERSECRET"},
                                data="<html>upstream</html>")
    exc = NodeException(1002, "发送请求错误: connection refused")
    exc.details = HttpNodeErrorInfo(code=1002, message="发送请求错误: connection refused",
                                    request=None, response=response)
    exc.response = _summarize_error_frame_response(response)
    # 引擎 abort 形态：runner 把节点异常包成带码的 NodeExecutionError（原始
    # 异常挂 __cause__，HTTP 明细挂在它身上）。
    node_error = NodeExecutionError("执行节点http_a出错了: 发送请求错误", node="http_a",
                                    code=1002)
    node_error.__cause__ = exc
    return {"message": "节点执行失败（重试 2 次后仍失败）: boom",
            "node_retries": 2, "failed_node": "http_a", **_error_fields(node_error)}


def test_execution_detail_exposes_structured_error(redis, client):
    err = _worker_shaped_error()
    redis.set("plaita:execution:e-93", json.dumps({
        "execution_id": "e-93", "flow_id": "f1", "status": "error",
        "start_time": "2026-10-17T00:00:00", "last_update_time": "2026-10-17T00:00:01",
        "context": {}, "error": err,
    }))
    r = client.get("/api/executions/e-93", headers={"X-Admin-API-Key": ADMIN_KEY})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "error"
    got = body["error"]
    assert got["code"] == 1002
    assert got["error_type"] == "node_error"
    assert got["response"]["status"] == 502
    assert got["details"]["code"] == 1002
    # 既有消费方（只读 message / node_retries）不受影响
    assert got["node_retries"] == 2
    assert "boom" in got["message"]
    # 读面同样不得带未脱敏凭据值
    assert "SUPERSECRET" not in json.dumps(got)
