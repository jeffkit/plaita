"""A54-06（duo-a 第五十四轮）：404 补清扫不清 flow，同名重建继承可执行流程。

基线 ``5ccaf6a``：正常删除路径在 ``flows`` 非空时拒绝（有流程先清空），但
404 补清扫路径（租户行已不存在）只清本地表/Redis/队列，**不删 flow**。
随后同 slug ``create_tenant`` 成功，旧 ``flows/flow_versions`` 直接重新归给
新租户——静默继承、无归档标记、无审计。

本文件钉住：
1. 404 路径遇到残留 flow ⇒ 具名 ``TenantError``（不静默返回 404），
   且不产生任何清理副作用；
2. ``create_tenant`` 面对任何前代遗留数据（flow/节点/属性类型/Copilot/
   部署）一律拒绝同名继承，要求显式清理/归档；
3. 显式清掉残留后：``delete_tenant`` 的 404 补清扫可清理其余资产，
   随后同名 create 成功（正常退役流程不被永久堵死）。

负控（验收时手工执行）：移除两处 ``_orphan_asset_counts`` 拒绝门 ⇒
``delete_tenant`` 静默返回 False、``create_tenant`` 成功继承旧 flow，
第 1/2 组断言精确红。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import delete, select

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import FlowRecord, NodeDescriptor, Tenant  # noqa: E402
from services import flow_store, tenants_svc  # noqa: E402

TENANT = "orphan"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "a54-06.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Orphan")
    return {"store": store}


def _drop_tenant_row(store) -> None:
    with store._session_local() as session:
        session.delete(session.get(Tenant, TENANT))
        session.commit()


def _flow_count(store) -> int:
    with store._session_local() as session:
        return len(
            session.scalars(
                select(FlowRecord).where(FlowRecord.tenant_id == TENANT)
            ).all()
        )


def _node_count(store) -> int:
    with store._session_local() as session:
        return len(
            session.scalars(
                select(NodeDescriptor).where(NodeDescriptor.tenant_id == TENANT)
            ).all()
        )


def test_orphan_flow_blocks_404_cleanup_and_recreation(env):
    store = env["store"]
    store.ensure_flow("tpl-flow", tenant_id=TENANT)
    _drop_tenant_row(store)

    with pytest.raises(tenants_svc.TenantError) as excinfo:
        tenants_svc.delete_tenant(store, TENANT)
    assert "流程残留" in str(excinfo.value)
    # 拒绝门先于一切清理：flow 还在，同名 create 也拿不到它。
    assert _flow_count(store) == 1

    with pytest.raises(tenants_svc.TenantError) as excinfo:
        tenants_svc.create_tenant(store, TENANT, "Orphan again")
    assert "前代遗留数据" in str(excinfo.value)
    assert _flow_count(store) == 1

    # 显式清理后可走正常退役：404 补清扫返回 False（租户行已不在），
    # 同名 create 成功且不继承旧流程。
    with store._session_local() as session:
        session.execute(delete(FlowRecord).where(FlowRecord.tenant_id == TENANT))
        session.commit()
    assert tenants_svc.delete_tenant(store, TENANT) is False
    tenants_svc.create_tenant(store, TENANT, "Orphan again")
    assert _flow_count(store) == 0


def test_orphan_non_flow_assets_also_block_silent_inheritance(env):
    store = env["store"]
    with store._session_local() as session:
        session.add(NodeDescriptor(
            tenant_id=TENANT,
            node_type="old_custom",
            node_name="Old",
            category="custom",
            schema_json="{}",
            is_builtin=False,
        ))
        session.commit()
    _drop_tenant_row(store)

    with pytest.raises(tenants_svc.TenantError) as excinfo:
        tenants_svc.create_tenant(store, TENANT, "Orphan again")
    assert "node_descriptors=1" in str(excinfo.value)

    # 404 补清扫可以清掉非 flow 残留（flow 为空不触发拒绝门）。
    assert tenants_svc.delete_tenant(store, TENANT) is False
    assert _node_count(store) == 0
    tenants_svc.create_tenant(store, TENANT, "Orphan again")
    assert _node_count(store) == 0
