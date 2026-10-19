"""A2R25-06：Redis-only 恢复回滚租户墓碑后，启动对账必须回落 disabled。

场景：租户在 DB 已删除，Redis 随后从删除前的快照恢复（``plaita:tenant_status``
里该租户是 ``active``）。worker 的 ``tenant_is_active`` 只读 Redis 短视图，
旧字段会让已删除租户重新按 active 放行——``publish_all_tenant_status`` 必须
以 DB 为权威，把 DB 不存在的非 default 租户回落成 #255 的 disabled 墓碑。

pre-fix 预期：删除的 acme 在 Redis 恢复后仍是 active（对账只回填 DB 现存
租户，不识别反向漂移），``tenant_is_active`` 为 True → 用例红。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store, tenants_svc  # noqa: E402
from plaita.server.tenant_status import tenant_is_active  # noqa: E402

TENANT = "acme"
OTHER = "beta"
STATUS_KEY = "plaita:tenant_status"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "r56.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    fake = FakeRedis(decode_responses=True)
    tenants_svc.create_tenant(store, TENANT, "Acme", redis_client=fake)
    tenants_svc.create_tenant(store, OTHER, "Beta", redis_client=fake)
    return store, fake


def _delete_db_row_only(store, tenant_id: str) -> None:
    """删 DB 行但不写 Redis 墓碑——模拟「DB 已删、Redis 还是旧快照」。"""
    assert tenants_svc.delete_tenant(store, tenant_id, redis_client=None) is True


def test_deleted_tenant_restored_as_active_falls_back_to_disabled(env):
    store, fake = env
    _delete_db_row_only(store, TENANT)
    # Redis 快照恢复到删除前：旧字段是 active（没有 disabled 墓碑）。
    fake.hset(STATUS_KEY, key=TENANT, value="active")
    assert tenant_is_active(fake, TENANT) is True  # 前提：恢复后确实 fail-open active

    summary = tenants_svc.publish_all_tenant_status(fake, store) or {}

    assert tenant_is_active(fake, TENANT) is False, (
        "Redis-only 恢复不得复活已删除租户（#255 墓碑语义）"
    )
    assert fake.hget(STATUS_KEY, TENANT) == "disabled"
    assert summary.get("disabled_stale") == 1
    assert summary.get("failed") == 0


def test_db_status_wins_over_restored_snapshot(env):
    store, fake = env
    tenants_svc.set_tenant_status(store, TENANT, "disabled", redis_client=None)
    fake.hset(STATUS_KEY, key=TENANT, value="active")  # 旧快照

    summary = tenants_svc.publish_all_tenant_status(fake, store)

    assert summary["failed"] == 0
    assert fake.hget(STATUS_KEY, TENANT) == "disabled"
    assert tenant_is_active(fake, TENANT) is False
    # 现存租户的正常状态照常回填（对账不得只做删除检测）。
    assert fake.hget(STATUS_KEY, OTHER) == "active"
