"""plaita#222：执行表示的 ``output`` / ``nodes[*].output`` 同属读面，必须脱敏。

``#166`` 把脱敏施加在「构造表示时」，但遍历面只列了 ``("context", "input")``。
同一个 ``ExecutionInfo`` 里的 ``output``（流程最终输出）与 ``nodes[*].output``
（本地档节点级 trace）原样返回——本地单机档 ``GET /executions`` /
``GET /executions/{id}`` 因此仍把凭据明文回给任意已认证 viewer。

本文件用**真实本地执行**跑 ``assignment(output="$INPUT")`` →
``end(output="$INPUT")``（最常见的「原样回显入参」写法），钉两件事：

* 读面（详情 + 列表）整份响应体零命中 needle，含 ``output`` 与
  ``nodes[*].output``；
* 盘上功能副本（``input_json`` / ``context_json`` / ``nodes_json`` /
  ``output_json``）逐字节不变——``#166`` 的「只改表示、不动功能副本」红线。

needle 复用 ``#166`` 族：显式凭据键值 + URL userinfo 口令 + webhook path token。
判据是「响应体里没有这些字节」，不是「调用了某个函数」。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from sqlalchemy import text
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from services import flow_store  # noqa: E402

FLOW_ID = "redact-output-flow"

# ---- needle：与 #166 同一组（凭据键值 / URL userinfo 口令 / path token）----
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


def _flow_def() -> str:
    """``assignment``/``end`` 都把整份 ``$INPUT`` 当输出——凭据的主要落点。"""
    return json.dumps(
        {
            "flow_id": FLOW_ID,
            "name": FLOW_ID,
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "assign"},
                {"id": "assign", "type": "assignment", "output": "$INPUT", "next": "end"},
                {"id": "end", "type": "end", "output": "$INPUT"},
            ],
        },
        ensure_ascii=False,
    )


@pytest.fixture()
def local_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'console.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app)


def _publish() -> None:
    store = flow_store.get_flow_store()
    store.ensure_flow(FLOW_ID, tenant_id="default")
    store.save_flow_definition(
        FLOW_ID, "1.0.0", _flow_def(), status="published", tenant_id="default"
    )


def _run_to_completion(client: TestClient) -> str:
    _publish()
    r = client.post("/api/executions", json={"flow_id": FLOW_ID, "params": dict(PARAMS)})
    assert r.status_code == 200, r.text
    eid = r.json()["execution_id"]

    deadline = time.time() + 20.0
    body = {}
    while time.time() < deadline:
        body = client.get(f"/api/executions/{eid}").json()
        if body.get("status") in ("completed", "failed", "cancelled"):
            assert body["status"] == "completed", f"执行未成功完成：{body}"
            return eid
        time.sleep(0.05)
    raise AssertionError(f"执行未在 20s 内终结：{body}")


def _assert_no_needle(text: str) -> None:
    leaked = [n for n in NEEDLES if n in text]
    assert not leaked, f"执行表示里仍留着明文凭据：{leaked}\n{text[:800]}"


def _disk_copy(eid: str) -> tuple:
    """盘上功能副本原文：``input_json`` / ``context_json`` / ``nodes_json`` /
    ``output_json``。脱敏只许改表示，不许改这四列。"""
    with flow_store._engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT input_json, context_json, nodes_json, output_json "
                "FROM local_executions WHERE execution_id = :eid"
            ),
            {"eid": eid},
        ).one()
    return tuple(row)


def _assert_functional_copy_has_needle(raw: tuple) -> None:
    """前置：功能副本确实是明文（否则「零命中」可能只是没落进 output/nodes）。"""
    input_json, context_json, nodes_json, output_json = raw
    for label, value in (
        ("input_json", input_json),
        ("context_json", context_json),
        ("nodes_json", nodes_json),
        ("output_json", output_json),
    ):
        assert value is not None, f"{label} 未落库"
        assert NEEDLE_KEY in value, f"{label} 功能副本里没有凭据明文（用例前提不成立）"


def test_local_detail_output_and_node_output_are_redacted(local_client):
    """``GET /executions/{id}``：``output`` 与 ``nodes[*].output`` 零命中。"""
    eid = _run_to_completion(local_client)
    before = _disk_copy(eid)
    _assert_functional_copy_has_needle(before)

    r = local_client.get(f"/api/executions/{eid}")

    assert r.status_code == 200, r.text
    _assert_no_needle(r.text)

    body = r.json()
    # 形状保留：键名与非凭据值原样，凭据值换占位（不是把整个 output 抹掉）
    assert body["output"]["api_key"] == "***"
    assert body["output"]["watch_agent"] == "deepseek-flash"
    assert body["output"]["repo"] != REPO_URL
    assert body["output"]["notify"] != HOOK_URL

    node_outputs = {n["id"]: n["output"] for n in body["nodes"]}
    for node_id in ("assign", "end"):
        assert node_outputs[node_id]["api_key"] == "***", node_id
        assert node_outputs[node_id]["watch_agent"] == "deepseek-flash", node_id
        assert node_outputs[node_id]["repo"] != REPO_URL, node_id
        assert node_outputs[node_id]["notify"] != HOOK_URL, node_id

    assert _disk_copy(eid) == before, "读面脱敏改动了盘上功能副本（#166 红线）"


def test_local_list_output_and_node_output_are_redacted(local_client):
    """``GET /executions``：同一行表示的另一条出口，同判。"""
    eid = _run_to_completion(local_client)
    before = _disk_copy(eid)
    _assert_functional_copy_has_needle(before)

    r = local_client.get("/api/executions")

    assert r.status_code == 200, r.text
    _assert_no_needle(r.text)

    row = next(x for x in r.json()["executions"] if x["execution_id"] == eid)
    assert row["output"]["api_key"] == "***"
    assert row["output"]["watch_agent"] == "deepseek-flash"
    assert row["output"]["repo"] != REPO_URL
    assert row["output"]["notify"] != HOOK_URL

    node_outputs = {n["id"]: n["output"] for n in row["nodes"]}
    for node_id in ("assign", "end"):
        assert node_outputs[node_id]["api_key"] == "***", node_id
        assert node_outputs[node_id]["watch_agent"] == "deepseek-flash", node_id

    assert _disk_copy(eid) == before, "读面脱敏改动了盘上功能副本（#166 红线）"
