"""第四十二轮 B2：S17-04 本地档 SSE 终态事件回归。

后端在终态时显式发 ``finished``（即使与上一条 ``update`` 载荷相同），
保证「最后一条事件一定是 finished」。前端据此正常收尾，不再把服务器
正常关闭当断连永久轮询（A2R16-13）。

负控：删掉 ``yield {"event": "finished", ...}`` → 本文件事件序列断言红；
前端终态守卫由 ``frontend/scripts/check-api-transport.mjs`` 单独钉住。
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from services import examples, flow_store  # noqa: E402


@pytest.fixture()
def app(tmp_path, monkeypatch) -> FastAPI:
    from config import Settings

    def _settings():
        s = Settings()
        s.console_env = "dev"
        s.admin_api_key = ""
        # stream 端点走 require_auth；本地档测试显式放开 insecure（仅测试进程）
        s.allow_insecure_admin = True
        return s

    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/sse.db")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(executions_api.router, prefix="/api")
    return app


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _wait_terminal(client: TestClient, execution_id: str, timeout=10.0) -> dict:
    deadline = time.time() + timeout
    body = {}
    while time.time() < deadline:
        body = client.get(f"/api/executions/{execution_id}").json()
        if body["status"] in ("completed", "failed", "cancelled"):
            return body
        time.sleep(0.1)
    raise AssertionError(f"执行未在 {timeout}s 内终结: {body}")


def _event_names(raw: bytes) -> list[str]:
    # p9 教训：测试解析必须兼容 \r\n\r\n（SSE 规范两种换行都合法）
    text = raw.decode("utf-8", "replace").replace("\r\n", "\n")
    names = []
    for block in text.split("\n\n"):
        for line in block.split("\n"):
            if line.startswith("event:"):
                names.append(line[len("event:"):].strip())
    return names


def test_local_stream_ends_with_single_finished_event(client: TestClient):
    examples.seed_example_flows()
    r = client.post("/api/executions", json={"flow_id": "hello-plaita"})
    assert r.status_code == 200, r.text
    execution_id = r.json()["execution_id"]
    _wait_terminal(client, execution_id)

    with client.stream(
        "GET", f"/api/executions/{execution_id}/stream"
    ) as response:
        assert response.status_code == 200
        raw = b"".join(response.iter_bytes())

    names = _event_names(raw)
    assert names, raw.decode("utf-8", "replace")
    assert names[0] == "initial_state"
    assert names[-1] == "finished"
    assert names.count("finished") == 1
    assert "finished" not in names[:-1]

    # finished 的 data 是整行执行表示（已完成状态）
    text = raw.decode("utf-8", "replace").replace("\r\n", "\n")
    finished_block = next(
        block for block in text.split("\n\n") if "event: finished" in block
    )
    assert '"status": "completed"' in finished_block
