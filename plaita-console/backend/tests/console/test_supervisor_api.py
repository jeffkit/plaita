"""Supervisor 面板后端单测（W1 接线）：proposer 选择与结构化报错。

- 默认（请求不带 proposer）→ FlowSourceProposer（@flow 源码提案 + 编译门）
- 显式 proposer="flow" 同上；"prompt" 保留 legacy PromptProposer；"static" 照旧
- 未知 proposer 值 → 400
- 缺 PLAITA_AI_PROPOSER_* env → 400 可操作报错（detail 指明缺哪个 env），而非 500

plaita-ai 是懒加载可选依赖：本机工作区有源码（且 plaita 引擎可导入）就用
**真实的** plaita_ai.supervisor——FlowSourceProposer 的 env 校验是真的，只把
Supervisor 换成记录器（不跑循环、不碰网络）；不可导入则整文件 skip，与
「backend 默认部署不依赖 plaita-ai」一致。
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


def _find_plaita_ai_src() -> Path | None:
    """在同工作区找 plaita-ai 源码（含 plaita_ai/supervisor.py 的目录）。"""
    for base in (BACKEND_DIR.parent.parent, BACKEND_DIR.parent):
        candidate = base / "plaita-ai"
        if (candidate / "plaita_ai" / "supervisor.py").is_file():
            return candidate
    return None


try:
    import plaita_ai.supervisor as plaita_supervisor
except ImportError:
    _src = _find_plaita_ai_src()
    if _src is None:
        pytest.skip("plaita-ai 源码不在本机工作区，跳过 supervisor 面板接线测试", allow_module_level=True)
    sys.path.insert(0, str(_src))
    try:
        import plaita_ai.supervisor as plaita_supervisor
    except ImportError as exc:
        pytest.skip(f"plaita-ai 不可导入（{exc}），跳过 supervisor 面板接线测试", allow_module_level=True)


class _RecordingSupervisor:
    """替换真 Supervisor：只记录收到的 proposer/policy，不跑循环（不碰网络）。"""

    last_proposer = None
    last_policy = None

    def __init__(self, client, policy=None, proposer=None):
        type(self).last_proposer = proposer
        type(self).last_policy = policy

    def run_loop(self, flow_id, dataset):
        return {
            "flow_id": flow_id,
            "final_status": "stubbed",
            "iterations": [],
            "iterations_run": 0,
        }


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    from api import supervisor as supervisor_api

    ds_root = tmp_path / "datasets"
    ds_root.mkdir()
    (ds_root / "demo.json").write_text(
        json.dumps({"cases": [{"id": "c1", "input": {"q": "hi"}, "expect": {"contains": "hi"}}]}),
        encoding="utf-8",
    )
    monkeypatch.setenv("PLAITA_SUPERVISOR_DATASET_DIR", str(ds_root))
    # 回环客户端需要 admin key；proposer env 显式清空，避免吃本机配置
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", "test-admin-key")
    monkeypatch.delenv("PLAITA_AI_PROPOSER_BASE_URL", raising=False)
    monkeypatch.delenv("PLAITA_AI_PROPOSER_MODEL", raising=False)
    monkeypatch.delenv("PLAITA_AI_PROPOSER_API_KEY", raising=False)

    app = FastAPI()
    app.include_router(supervisor_api.router, prefix="/api")
    return TestClient(app)


@pytest.fixture()
def stub_loop(monkeypatch):
    monkeypatch.setattr(plaita_supervisor, "Supervisor", _RecordingSupervisor)
    _RecordingSupervisor.last_proposer = None
    _RecordingSupervisor.last_policy = None
    return _RecordingSupervisor


def _post(client: TestClient, **body) -> object:
    return client.post("/api/flows/demo-flow/supervisor/iterate", json=body)


def test_default_proposer_is_flow_source(client, stub_loop, monkeypatch):
    """不传 proposer（存量前端兼容）→ 自动走 FlowSourceProposer（编译门）。"""
    monkeypatch.setenv("PLAITA_AI_PROPOSER_BASE_URL", "http://llm.local/v1")
    monkeypatch.setenv("PLAITA_AI_PROPOSER_MODEL", "test-model")
    resp = _post(client, dataset="demo.json", max_iterations=1)
    assert resp.status_code == 200, resp.text
    assert resp.json()["final_status"] == "stubbed"
    assert isinstance(stub_loop.last_proposer, plaita_supervisor.FlowSourceProposer)


def test_explicit_flow_prompt_static(client, stub_loop, monkeypatch):
    """显式三种取值各归其位：flow=编译门提案、prompt=legacy、static=脚本。"""
    monkeypatch.setenv("PLAITA_AI_PROPOSER_BASE_URL", "http://llm.local/v1")
    monkeypatch.setenv("PLAITA_AI_PROPOSER_MODEL", "test-model")

    resp = _post(client, dataset="demo.json", proposer="flow")
    assert resp.status_code == 200, resp.text
    assert isinstance(stub_loop.last_proposer, plaita_supervisor.FlowSourceProposer)

    resp = _post(client, dataset="demo.json", proposer="prompt")
    assert resp.status_code == 200, resp.text
    assert isinstance(stub_loop.last_proposer, plaita_supervisor.PromptProposer)

    resp = _post(client, dataset="demo.json", proposer="static")
    assert resp.status_code == 200, resp.text
    assert isinstance(stub_loop.last_proposer, plaita_supervisor.StaticProposer)


def test_default_flow_without_env_is_structured_400(client, stub_loop):
    """缺 env：默认 flow 路径返回 400 + 可操作 detail（而非 500）。"""
    resp = _post(client, dataset="demo.json")
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert "FlowSourceProposer" in detail
    assert "PLAITA_AI_PROPOSER_BASE_URL" in detail
    assert "PLAITA_AI_PROPOSER_MODEL" in detail
    assert stub_loop.last_proposer is None  # 构造失败 → 循环根本没建


def test_prompt_without_env_is_400_too(client, stub_loop):
    """legacy prompt 缺 env 同样 400（既有风格不回归）。"""
    resp = _post(client, dataset="demo.json", proposer="prompt")
    assert resp.status_code == 400, resp.text
    assert "PLAITA_AI_PROPOSER_BASE_URL" in resp.json()["detail"]


def test_unknown_proposer_rejected(client, stub_loop):
    """未知取值 → 400，指明可选值；不构造任何 proposer。"""
    resp = _post(client, dataset="demo.json", proposer="magic")
    assert resp.status_code == 400, resp.text
    assert "flow | prompt | static" in resp.json()["detail"]
    assert stub_loop.last_proposer is None
