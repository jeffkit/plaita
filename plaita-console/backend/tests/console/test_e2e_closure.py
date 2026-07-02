"""
端到端闭环：控制台发布 flow → PlaitaClient 默认 URL + HMAC 拉取 → run_flow 成功。

启动一个真实的 uvicorn 服务（仅挂载 flow_version 契约路由，旁路 Redis），
用 FlowStore 发布 echo 流程，再用 PlaitaClient._fetch_flow + Flow.run 跑通。
"""
import json
import socket
import sys
import threading
from pathlib import Path
from time import sleep, time

import pytest
import uvicorn

BACKEND_DIR = Path(__file__).resolve().parents[2]
WORKTREE_ROOT = Path(__file__).resolve().parents[4]
# 先把 worktree 根放上路径，使 `plaita` 解析为 v2 工作区内被编辑的副本（而非已安装包）
for p in (WORKTREE_ROOT, BACKEND_DIR):
    p_str = str(p)
    if p_str not in sys.path:
        sys.path.insert(0, p_str)

from fastapi import FastAPI  # noqa: E402
from api import flow_version  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.client import PlaitaClient  # noqa: E402
from plaita.core.flow import Flow  # noqa: E402

SECRET_ID = "e2e-id"
SECRET_KEY = "e2e-secret-key"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _echo_def() -> str:
    return json.dumps(
        {
            "flow_id": "echo",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def server(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_SECRET_ID", SECRET_ID)
    monkeypatch.setenv("PLAITA_CONSOLE_SECRET_KEY", SECRET_KEY)
    flow_store.init_engine(f"sqlite:///{tmp_path / 'e2e.db'}")

    store = flow_store.get_flow_store()
    store.create_flow("echo")
    store.save_flow_definition("echo", "1.0.0", _echo_def(), "{}")
    store.publish_version("echo", "1.0.0")

    app = FastAPI()
    app.include_router(flow_version.router, prefix="/api")

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server_ = uvicorn.Server(config)
    thread = threading.Thread(target=server_.run, daemon=True)
    thread.start()

    url = f"http://127.0.0.1:{port}/api/flowVersion/semver/detail"
    # 等待服务就绪
    deadline = time() + 10
    import requests

    while time() < deadline:
        try:
            requests.post(url, data={"flowId": "x", "version": "0"}, timeout=1)
            break
        except Exception:
            sleep(0.1)
    yield url
    server_.should_exit = True
    thread.join(timeout=5)


def test_e2e_plaita_client_fetch_and_run(server: str):
    client = PlaitaClient(secret_id=SECRET_ID, secret_key=SECRET_KEY, url=server)
    flow_dict = client._fetch_flow("echo", "1.0.0")
    assert flow_dict["flow_id"] == "echo"

    flow = Flow.model_validate(flow_dict)
    result = flow.run(name="kongjie")
    assert result == "kongjie"
