"""
FlowStore 单测：覆盖存取、版本列表、发布、幂等/状态守卫、删除、重启不丢、semver 唯一。
"""
import json
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 让 backend 包可被导入（无论以包还是脚本方式运行 pytest）
BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import Base  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402


def _echo_definition(flow_id: str = "echo") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": "echo",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def store(tmp_path) -> FlowStore:
    """基于临时文件的 sqlite 引擎 + FlowStore（便于重启模拟）。"""
    db_file = tmp_path / "test_flow.db"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine, expire_on_commit=False)
    return FlowStore(session_local)


def test_save_and_get_roundtrip(store: FlowStore):
    layout = json.dumps({"start": {"x": 0, "y": 0}, "end": {"x": 200, "y": 0}})
    res = store.save_flow_definition("echo", "0.0.1", _echo_definition(), layout)
    assert res.flow_id == "echo"
    assert res.version == "0.0.1"
    assert res.status == "draft"

    got = store.get_version("echo", "0.0.1")
    assert got is not None
    assert got.status == "draft"
    assert json.loads(got.definition)["flow_id"] == "echo"
    assert json.loads(got.layout)["end"]["x"] == 200


def test_list_versions_ordered(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(), "{}")
    store.save_flow_definition("echo", "0.0.2", _echo_definition(), "{}")
    versions = store.list_versions("echo")
    assert [v.version for v in versions] == ["0.0.1", "0.0.2"]
    assert all(v.flow_id == "echo" for v in versions)


def test_list_flows(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(), "{}")
    store.save_flow_definition("greet", "0.0.1", _echo_definition("greet"), "{}")
    flows = store.list_flows()
    ids = {f.flow_id for f in flows}
    assert ids == {"echo", "greet"}


def test_publish_draft_to_published(store: FlowStore):
    store.save_flow_definition("echo", "1.0.0", _echo_definition(), "{}")
    out = store.publish_version("echo", "1.0.0")
    assert out.status == "published"
    assert out.published_at is not None
    # 持久化生效
    assert store.get_version("echo", "1.0.0").status == "published"


def test_publish_is_idempotent_and_state_guarded(store: FlowStore):
    store.save_flow_definition("echo", "1.0.0", _echo_definition(), "{}")
    first = store.publish_version("echo", "1.0.0")
    second = store.publish_version("echo", "1.0.0")
    assert first.status == second.status == "published"
    # 幂等：published_at 不应被二次刷新
    assert first.published_at == second.published_at


def test_publish_nonexistent_raises(store: FlowStore):
    with pytest.raises(LookupError):
        store.publish_version("echo", "9.9.9")


def test_delete_version(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(), "{}")
    assert store.delete_version("echo", "0.0.1") is True
    assert store.get_version("echo", "0.0.1") is None
    with pytest.raises(LookupError):
        store.delete_version("echo", "0.0.1")


def test_save_upsert_overwrites_draft(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(), "{}")
    new_def = _echo_definition()
    new_def_obj = json.loads(new_def)
    new_def_obj["desc"] = "updated"
    store.save_flow_definition("echo", "0.0.1", json.dumps(new_def_obj), "{}")
    versions = store.list_versions("echo")
    assert len(versions) == 1  # semver 唯一：仍是同一行
    assert json.loads(versions[0].definition)["desc"] == "updated"


def test_save_cannot_overwrite_published(store: FlowStore):
    store.save_flow_definition("echo", "1.0.0", _echo_definition(), "{}")
    store.publish_version("echo", "1.0.0")
    with pytest.raises(ValueError):
        store.save_flow_definition("echo", "1.0.0", _echo_definition(), "{}")


def test_semver_uniqueness_across_flows(store: FlowStore):
    # 不同 flow 可使用相同 version 号
    store.save_flow_definition("echo", "1.0.0", _echo_definition("echo"), "{}")
    store.save_flow_definition("greet", "1.0.0", _echo_definition("greet"), "{}")
    assert store.get_version("echo", "1.0.0") is not None
    assert store.get_version("greet", "1.0.0") is not None


def test_restart_persists_data(tmp_path):
    """模拟重启：新引擎读取同一文件，数据仍在。"""
    db_file = tmp_path / "restart.db"
    url = f"sqlite:///{db_file}"

    from services.flow_store import init_engine, get_flow_store

    init_engine(url)
    get_flow_store().save_flow_definition("echo", "1.0.0", _echo_definition(), "{}")
    get_flow_store().publish_version("echo", "1.0.0")

    # 重新初始化（模拟重启），数据应持久
    init_engine(url)
    got = get_flow_store().get_version("echo", "1.0.0")
    assert got is not None
    assert got.status == "published"
    assert json.loads(got.definition)["flow_id"] == "echo"
