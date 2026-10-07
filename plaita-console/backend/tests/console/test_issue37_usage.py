"""#37 console 本地档用量归集：本地执行详情页不再只有 Langfuse 深链。

``local_executions`` 新增 ``usage_json`` 列；``_LocalTraceCallback`` 在写
``nodes_json`` 的同一次更新里回写用量（不开观测后端也可见）。
"""
import sqlite3
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store, local_executor  # noqa: E402

USAGE = {
    "total": {"input": 10, "output": 5, "total": 15},
    "nodes": {"llm": {"input": 10, "output": 5, "total": 15}},
}


class _NodeStub:
    def __init__(self, node_id: str = "llm"):
        self.id = node_id
        self.node_type = "assignment"
        self.name = node_id
        self.input = {}


@pytest.fixture()
def store(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path}/usage.db")
    return flow_store


def _seed_row(store, execution_id: str) -> None:
    store.insert_local_execution(
        execution_id=execution_id, flow_id="f", flow_version="1.0.0"
    )


def test_node_result_usage_persisted_to_execution(store):
    _seed_row(store, "e1")
    callback = local_executor._LocalTraceCallback("e1")
    node = _NodeStub()
    callback.on_node_start(None, node)
    callback.on_node_end(None, node, result={
        "model": "GLM-5.2",
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    })

    assert store.get_local_execution("e1")["usage"] == USAGE


def test_run_without_usage_keeps_null(store):
    _seed_row(store, "e2")
    callback = local_executor._LocalTraceCallback("e2")
    node = _NodeStub()
    callback.on_node_start(None, node)
    callback.on_node_end(None, node, result={"plain": "value"})

    assert store.get_local_execution("e2")["usage"] is None


def test_resume_seeds_persisted_usage(store):
    _seed_row(store, "e3")
    callback = local_executor._LocalTraceCallback("e3", initial_usage=USAGE)
    node = _NodeStub("llm2")
    callback.on_node_start(None, node)
    callback.on_node_end(None, node, result={
        "model": "GLM-5.2",
        "usage": {"input_tokens": 1, "output_tokens": 2},
    })

    usage = store.get_local_execution("e3")["usage"]
    assert usage["total"] == {"input": 11, "output": 7, "total": 18}
    assert set(usage["nodes"]) == {"llm", "llm2"}


def test_resume_local_execution_passes_usage_through(store, monkeypatch):
    """resume 入口把已落盘 usage 交给执行线程（不丢挂起前用量）。"""
    _seed_row(store, "e4")
    store.update_local_execution(
        "e4", status="suspended", context_json='{"last_node_id": "wait"}',
        usage_json='{"total": {"input": 3}, "nodes": {"llm": {"input": 3}}}',
    )
    captured = {}

    def _fake_spawn(execution_id, target, *args, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(local_executor, "_spawn", _fake_spawn)
    monkeypatch.setattr(local_executor, "_load_definition", lambda *a, **k: {"flow_id": "f"})

    assert local_executor.resume_local_execution(
        store, "e4", resume_type="continue", data=None
    )
    assert captured["initial_usage"] == {"total": {"input": 3}, "nodes": {"llm": {"input": 3}}}


def test_legacy_sqlite_gains_usage_json_column(tmp_path):
    db_file = tmp_path / "legacy.db"
    con = sqlite3.connect(db_file)
    con.execute(
        "CREATE TABLE local_executions ("
        "id INTEGER PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL DEFAULT '', "
        "execution_id VARCHAR(64) NOT NULL, flow_id VARCHAR(128) NOT NULL, "
        "flow_version VARCHAR(64) NOT NULL DEFAULT '', status VARCHAR(16) NOT NULL DEFAULT 'running', "
        "input_json TEXT NOT NULL DEFAULT '{}', output_json TEXT NOT NULL DEFAULT 'null', "
        "error_json TEXT NOT NULL DEFAULT 'null', nodes_json TEXT NOT NULL DEFAULT '[]', "
        "context_json TEXT NOT NULL DEFAULT 'null', invoker VARCHAR(64) NOT NULL DEFAULT 'local', "
        "start_time DATETIME, end_time DATETIME, last_update_time DATETIME)"
    )
    con.commit()
    con.close()

    flow_store.init_engine(f"sqlite:///{db_file}")

    con = sqlite3.connect(db_file)
    columns = {row[1] for row in con.execute("PRAGMA table_info(local_executions)")}
    con.close()
    assert "usage_json" in columns
