"""A2R24-01 负控：console 错误响应统一形状（抽 3 类端点）。

契约：所有错误响应的 ``detail`` 都是同一对象
``{"code": <机器码>, "message": <人读文案>, ...历史业务键}``：

- 手工 404（string detail）→ ``code=not_found``；
- 手工 409（历史 dict：message/conflict/latest_updated_at）→
  ``code=schedule_stale`` 且保留 ``conflict``/``latest_updated_at``；
- 自动 422（validation list）与手工 422 → 同一 envelope（``errors`` 承载字段级明细）；
- 未捕获异常 → JSON 500 ``internal_error``（不再 ``text/plain``），异常原文不出网。

负控（只回退 ``error_contract`` 挂载/源站 detail）：detail 回到 string/list、
500 回到 text/plain、或 409 丢 ``code`` 即红。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from starlette.exceptions import HTTPException as StarletteHTTPException  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from error_contract import install_error_handlers  # noqa: E402
from plaita.server.services.schedule_service import SCHEDULES_KEY  # noqa: E402
from services import flow_store  # noqa: E402


def _assert_envelope(body: dict, code: str) -> dict:
    """detail 必须是统一对象，且 code/message 齐备。"""
    detail = body["detail"]
    assert isinstance(detail, dict), f"detail 不是统一对象: {detail!r}"
    assert detail.get("code") == code, detail
    assert isinstance(detail.get("message"), str) and detail["message"], detail
    return detail


@pytest.fixture()
def flows_client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path / 'a2r24-flows.db'}")
    app = FastAPI()
    app.state.redis = None  # 本地单机档：flows 的引擎同步跳过
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    install_error_handlers(app)
    app.include_router(flows_api.router, prefix="/api")
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def schedules_client() -> tuple[TestClient, fakeredis.FakeRedis]:
    redis = fakeredis.FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    install_error_handlers(app)
    app.include_router(schedules_api.router, prefix="/api")
    return TestClient(app), redis


def test_string_detail_404_has_unified_shape(flows_client: TestClient):
    """第 1 类：string detail（业务流程 404）。"""
    resp = flows_client.get("/api/flows/not-there")
    assert resp.status_code == 404, resp.text
    assert resp.headers["content-type"].startswith("application/json")
    detail = _assert_envelope(resp.json(), "not_found")
    assert "流程不存在" in detail["message"]


def test_manual_422_and_auto_422_share_envelope(flows_client: TestClient):
    """第 2 类：手工 422（string）与 FastAPI 自动 422（list）同形状。"""
    manual = flows_client.put(
        "/api/flows/not-there/versions/not-semver",
        json={"definition": "{}"},
    )
    assert manual.status_code == 422, manual.text
    manual_detail = _assert_envelope(manual.json(), "validation_error")
    assert "semver" in manual_detail["message"]

    auto = flows_client.get("/api/executions", params={"page": 0})
    assert auto.status_code == 422, auto.text
    auto_detail = _assert_envelope(auto.json(), "validation_error")
    assert isinstance(auto_detail.get("errors"), list) and auto_detail["errors"]


def test_legacy_dict_409_is_normalized_and_keeps_business_keys(
    schedules_client: tuple[TestClient, fakeredis.FakeRedis],
):
    """第 3 类：历史结构化 409 dict → 补 code，业务键不丢。"""
    client, redis = schedules_client
    stale = {
        "schedule_id": "s1",
        "tenant_id": "default",
        "name": "n",
        "flow_id": "f",
        "version": None,
        "cron": "* * * * *",
        "params": {},
        "enabled": True,
        "created_at": "t0",
        "updated_at": "t0",
        "next_run_at": "0",
    }
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(stale))
    ok = client.put(
        "/api/schedules/s1",
        json={"enabled": False, "expected_updated_at": "t0"},
    )
    assert ok.status_code == 200, ok.text
    resp = client.put(
        "/api/schedules/s1",
        json={"enabled": True, "expected_updated_at": "t0"},
    )
    assert resp.status_code == 409, resp.text
    detail = _assert_envelope(resp.json(), "schedule_stale")
    assert detail["conflict"] == "schedule_stale"
    assert detail["latest_updated_at"] == ok.json()["updated_at"]


def test_unhandled_exception_is_json_500_without_exception_text(
    flows_client: TestClient,
):
    """第 4 类：未捕获异常 → JSON 500；异常原文只进日志。"""
    app = flows_client.app

    @app.get("/api/_a2r24-boom")
    async def _boom():
        raise RuntimeError("secret-token-should-not-leak")

    resp = flows_client.get("/api/_a2r24-boom")
    assert resp.status_code == 500, resp.text
    assert resp.headers["content-type"].startswith("application/json")
    detail = _assert_envelope(resp.json(), "internal_error")
    assert "secret-token-should-not-leak" not in json.dumps(detail)
    assert detail["message"] == "内部服务器错误"


def test_create_app_installs_error_handlers():
    """真实进程入口必须挂统一处理器（而非只在新测试的 mini-app 上）。"""
    import main as main_module

    handlers = main_module.app.exception_handlers
    assert StarletteHTTPException in handlers
    assert Exception in handlers
