"""A2R35-01：caps 数值字段形状收紧后，console 保存/发布必须 422 具名拒绝。

console 的第五入口身份：保存/发布走 ``Flow.model_validate``
（services/validation.py），与运行期加载同门。B2 第六十一轮把
``maxRedirects`` / ``requestTimeout`` / ``retryTimes`` 的布尔/数字字符串宽松
强转（``true``→1、``"5"``→5）与 ``retryTimes`` 负数（B2R27-01 口径变更：
原「接受 + 运行期钳 0」）统一为构造期具名拒绝，此处验证 API 面返回
422 ``invalid_definition`` 且文案带字段名；合法值不回归。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from services import flow_store  # noqa: E402


def _caps_def(extra: dict) -> str:
    node = {"type": "http", "id": "h", "method": "GET",
            "url": "https://example.com", "next": "end"}
    node.update(extra)
    return json.dumps({
        "flow_id": "caps-shape",
        "nodes": [
            {"type": "start", "id": "start", "next": "h"},
            node,
            {"type": "end", "id": "end"},
        ],
    })


@pytest.fixture()
def client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path / 'flows.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    client = TestClient(app)
    assert client.post("/api/flows", json={"flow_id": "caps-shape"}).status_code == 200
    return client


@pytest.mark.parametrize("extra, field", [
    ({"maxRedirects": True}, "maxRedirects"),
    ({"maxRedirects": "5"}, "maxRedirects"),
    ({"requestTimeout": True}, "requestTimeout"),
    ({"requestTimeout": "600"}, "requestTimeout"),
    ({"errorHandler": {"strategy": "abort", "retryTimes": True}}, "retryTimes"),
    ({"errorHandler": {"strategy": "abort", "retryTimes": -5}}, "retryTimes"),
])
def test_caps_shape_violation_saves_422(client: TestClient, extra, field):
    r = client.put(
        "/api/flows/caps-shape/versions/0.0.1",
        json={"definition": _caps_def(extra), "layout": "{}"},
    )
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "invalid_definition"
    assert field in detail["message"], detail["message"]


def test_caps_legal_values_still_save_200(client: TestClient):
    r = client.put(
        "/api/flows/caps-shape/versions/0.0.1",
        json={"definition": _caps_def({
            "maxRedirects": 5, "requestTimeout": 600,
            "errorHandler": {"strategy": "abort", "retryTimes": 3},
        }), "layout": "{}"},
    )
    assert r.status_code == 200, r.text
