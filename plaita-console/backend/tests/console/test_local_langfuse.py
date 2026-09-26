"""本地档 Langfuse 观测接线（plaita-console × plaita.obs）。

验证三件事：
1. 开启时 trace id = console 的 execution_id（经 $EXECUTION_ID 解析链）；
2. auto 模式默认关（没配 LANGFUSE_PUBLIC_KEY 不产生任何上报）；
3. 缺 plaita[langfuse] 依赖时降级为不观测，执行不受影响。

langfuse SDK 从不真装：fake 模块注入 sys.modules（None 值 = import 报错）。
"""
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


# ---------------------------------------------------------------- fake SDK

class FakeSpan:
    def __init__(self, store, **kwargs):
        self.store = store
        self.kwargs = kwargs
        store["spans"].append(kwargs)

    def update(self, **kwargs):
        pass

    def end(self, **kwargs):
        pass

    def start_observation(self, **kwargs):
        return FakeSpan(self.store, **kwargs)


class FakeRootSpan(FakeSpan):
    def __init__(self, store, **kwargs):
        super().__init__(store, **kwargs)
        store["roots"].append(kwargs)


class FakeLangfuseClient:
    """SDK v4 形状：create_trace_id + start_observation。"""

    instances: list = []

    def __init__(self, **kwargs):
        self.store = {"roots": [], "spans": [], "flushes": 0, "seeds": []}
        FakeLangfuseClient.instances.append(self)

    def create_trace_id(self, seed=None):
        import hashlib

        self.store["seeds"].append(str(seed))
        return hashlib.md5(str(seed).encode()).hexdigest()

    def start_observation(self, **kwargs):
        return FakeRootSpan(self.store, **kwargs)

    def flush(self):
        self.store["flushes"] += 1


class FakeLangfuseModule:
    Langfuse = FakeLangfuseClient


@pytest.fixture()
def clean_instances():
    FakeLangfuseClient.instances = []
    yield
    FakeLangfuseClient.instances = []


@pytest.fixture()
def app(tmp_path) -> FastAPI:
    db_file = tmp_path / "local.db"
    flow_store.init_engine(f"sqlite:///{db_file}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(executions_api.router, prefix="/api")
    return app


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _wait_completed(client: TestClient, execution_id: str, timeout=10.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get(f"/api/executions/{execution_id}").json()
        if body["status"] in ("completed", "failed", "cancelled"):
            return body
        time.sleep(0.1)
    raise AssertionError(f"执行未在 {timeout}s 内终结: {body}")


def _start(client: TestClient) -> str:
    examples.seed_example_flows()
    r = client.post("/api/executions", json={"flow_id": "hello-plaita"})
    assert r.status_code == 200
    return r.json()["execution_id"]


# ---------------------------------------------------------------- 用例

def test_trace_id_is_console_execution_id(app, client, clean_instances, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_LANGFUSE", "true")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setitem(sys.modules, "langfuse", FakeLangfuseModule)

    execution_id = _start(client)
    _wait_completed(client, execution_id)

    assert len(FakeLangfuseClient.instances) == 1
    store = FakeLangfuseClient.instances[0].store
    assert store["roots"], "未产生任何 trace"
    assert store["seeds"] == [execution_id]  # 语义 id = console 执行实例 ID
    assert store["roots"][0]["name"] == "hello-plaita"
    assert store["flushes"] >= 1


def test_auto_mode_off_without_public_key(app, client, clean_instances, monkeypatch):
    monkeypatch.delenv("PLAITA_CONSOLE_LANGFUSE", raising=False)
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)

    execution_id = _start(client)
    _wait_completed(client, execution_id)
    assert FakeLangfuseClient.instances == []


def test_forced_on_without_public_key_still_observes(app, client, clean_instances, monkeypatch):
    """PLAITA_CONSOLE_LANGFUSE=true 强制开：无 public_key 也构造（SDK 侧如何
    报错由它自己决定，console 只负责不阻塞执行）。"""
    monkeypatch.setenv("PLAITA_CONSOLE_LANGFUSE", "true")
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "langfuse", FakeLangfuseModule)

    execution_id = _start(client)
    _wait_completed(client, execution_id)
    assert len(FakeLangfuseClient.instances) == 1
    assert FakeLangfuseClient.instances[0].store["roots"]
    assert FakeLangfuseClient.instances[0].store["seeds"] == [execution_id]


def test_missing_extra_degrades_gracefully(app, client, clean_instances, monkeypatch):
    """缺 plaita[langfuse]：sys.modules["langfuse"]=None 使 import 报 ImportError，
    执行必须照常完成（观测可选，不阻塞业务）。"""
    monkeypatch.setenv("PLAITA_CONSOLE_LANGFUSE", "true")
    monkeypatch.setitem(sys.modules, "langfuse", None)

    execution_id = _start(client)
    body = _wait_completed(client, execution_id)
    assert body["status"] == "completed"
    assert FakeLangfuseClient.instances == []
