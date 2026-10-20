"""A2R24-04 / A2R24-03 负控：services 本地档 503 + logs/stats 采样标注。

A2R24-04：本地单机档（``app.state.redis=None``）下 services 模块**全部 GET**
必须是 503 ``application/json``，与同模块 list/topology 一致；历史三个端点
（types/list、{id}、{id}/status）在 None 上抛 ``NoneType`` → 500 text/plain。

A2R24-03：``/logs/stats`` 平台视角按每键最新 N 条采样估算级别分布。响应必须
显式标注 ``estimated=true`` 与 ``sample_size``；``truncated`` 仍只表示预算/
键扫描触界，不得拿来冒充「这是精确值」。

负控：回退 ``get_redis`` 的 None 判断（500 而非 503）、删掉 estimated/
sample_size 字段即红。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import logs as logs_api  # noqa: E402
from api import services as services_api  # noqa: E402
from error_contract import install_error_handlers  # noqa: E402


def _local_services_client() -> TestClient:
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    install_error_handlers(app)
    app.include_router(services_api.router, prefix="/api")
    # 未修复时 handler 会把 NoneType 500 包成 JSON；raise_server_exceptions=False
    # 让负控稳定看到「500 ≠ 503」而不是测试抛异常。
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "path",
    [
        "/api/services",
        "/api/services/topology",
        "/api/services/types/list",
        "/api/services/abc",
        "/api/services/abc/status",
    ],
)
def test_local_mode_all_service_gets_are_503_json(path: str):
    client = _local_services_client()
    resp = client.get(path)
    assert resp.status_code == 503, resp.text
    assert resp.headers["content-type"].startswith("application/json")
    detail = resp.json()["detail"]
    assert isinstance(detail, dict), detail
    assert detail["code"] == "service_unavailable", detail
    assert "本地单机档" in detail["message"], detail


def _seed_log_stream(redis) -> str:
    """300 条：较早 200 ERROR + 最新 100 INFO（采样只看最新 100 条）。"""
    key = "plaita:logs:svc-a"
    for i in range(200):
        redis.xadd(key, {"level": "ERROR", "message": f"e{i}", "tenant_id": "default"})
    for i in range(100):
        redis.xadd(key, {"level": "INFO", "message": f"i{i}", "tenant_id": "default"})
    return key


def _logs_client(*, tenant: bool) -> tuple[TestClient, fakeredis.FakeRedis]:
    redis = fakeredis.FakeRedis(decode_responses=True)
    _seed_log_stream(redis)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    if tenant:
        @app.middleware("http")
        async def _bind_tenant(request, call_next):
            request.state.tenant_id = "default"
            return await call_next(request)
    install_error_handlers(app)
    app.include_router(logs_api.router, prefix="/api")
    return TestClient(app), redis


def _level_counts(body: dict) -> dict:
    return {row["level"]: row["count"] for row in body["stats"]}


def test_platform_stats_is_labeled_as_estimate():
    """平台视角：300 条实际 200 ERROR/100 INFO；采样估算必须显式标注。"""
    client, _ = _logs_client(tenant=False)
    resp = client.get("/api/logs/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_logs"] == 300
    assert body["truncated"] is False
    assert body["estimated"] is True, "平台视角是采样估算，必须标注 estimated"
    assert body["sample_size"] == 100
    # 估算值可以仍是 300 INFO（当前实现），但不得再冒充精确统计。
    assert _level_counts(body) == {"INFO": 300}


def test_tenant_stats_is_exact_and_not_labeled_estimated():
    """租户视角逐条精确统计：estimated=false、sample_size=null。"""
    client, _ = _logs_client(tenant=True)
    resp = client.get("/api/logs/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_logs"] == 300
    assert body["truncated"] is False
    assert body["estimated"] is False
    assert body["sample_size"] is None
    assert _level_counts(body) == {"ERROR": 200, "INFO": 100}


def _seed_a2r28_instance_streams(redis, *, streams: int = 20, per_stream: int = 150):
    """A2R28 探针口径：``plaita:logs:{service}:{instance}`` 四段键 + 条目数超采样窗。"""
    for s in range(streams):
        key = f"plaita:logs:flow_worker:inst{s}"
        for i in range(per_stream):
            redis.xadd(
                key,
                {
                    "level": "ERROR" if i % 5 == 0 else "INFO",
                    "message": f"m{i}",
                    "tenant_id": "default",
                },
            )


def _logs_client_with(redis, *, tenant: bool) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    if tenant:
        @app.middleware("http")
        async def _bind_tenant(request, call_next):
            request.state.tenant_id = "default"
            return await call_next(request)
    install_error_handlers(app)
    app.include_router(logs_api.router, prefix="/api")
    return TestClient(app)


def test_platform_stats_labels_estimate_for_a2r28_instance_key_shape():
    """A2R28 键形（四段）仍按 service 段归组，且 estimated/sample_size 口径不变。

    r52 的实例日志下推把键切成 ``plaita:logs:{service}:{instance}``；stats 的
    平台分支仍用 ``parts[2]`` 取 service_type，并按每键最新 100 条采样。这里用
    20 流 × 150 条（样本未覆盖整条流）验证：total_logs 是 xlen 之和、estimated
    为 true、sample_size=实际采样条数，而不是退回静默估算。
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    _seed_a2r28_instance_streams(redis)
    client = _logs_client_with(redis, tenant=False)
    body = client.get("/api/logs/stats").json()

    assert body["truncated"] is False
    assert body["total_logs"] == 20 * 150
    assert body["estimated"] is True
    assert body["sample_size"] == 20 * 100
    assert {row["service_type"] for row in body["stats"]} == {"flow_worker"}
    # 每流最后 100 条：20 ERROR/80 INFO；按 150/100 放大后每流 30/120。
    assert _level_counts(body) == {"ERROR": 600, "INFO": 2400}


def test_tenant_stats_is_exact_for_a2r28_instance_key_shape():
    """同一键形下租户视角仍逐条精确：estimated=false、sample_size=null。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    _seed_a2r28_instance_streams(redis)
    client = _logs_client_with(redis, tenant=True)
    body = client.get("/api/logs/stats").json()

    assert body["truncated"] is False
    assert body["total_logs"] == 20 * 150
    assert body["estimated"] is False
    assert body["sample_size"] is None
    # 每流 150 条中 i%5==0 为 ERROR → 30 ERROR/120 INFO。
    assert _level_counts(body) == {"ERROR": 600, "INFO": 2400}
