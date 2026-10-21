"""#284 (r56): create_tenant must publish the new generation before active.

Pre-fix ``create_tenant`` published ``status=active`` first, then generation
(best-effort). A generation publish failure left the tenant active with the old
generation, so old-generation DLQ/subscription writes were allowed. Fix:
publish the new generation after flush/before commit; failure aborts create
(transaction rollback, no active window). Startup reconciliation uses the same
generation → status order.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
import fakeredis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store, tenants_svc  # noqa: E402
from plaita.server.tenant_status import (  # noqa: E402
    read_tenant_generation,
    tenant_write_allowed,
)

TENANT = "acme"


@pytest.fixture()
def store(tmp_path, monkeypatch):
    db_file = tmp_path / "issue284.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    return flow_store.get_flow_store()


def test_generation_publish_failure_aborts_create(store, monkeypatch):
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(tenants_svc, "_publish_generation", lambda *a, **k: False)

    with pytest.raises(tenants_svc.TenantError):
        tenants_svc.create_tenant(store, TENANT, redis_client=fake)

    # 事务回滚：无租户行；Redis 不处于 active 窗口。
    with pytest.raises(LookupError):
        tenants_svc.get_tenant(store, TENANT)
    assert fake.hget("plaita:tenant_status", TENANT) != "active"


def test_generation_published_before_active(store, monkeypatch):
    fake = fakeredis.FakeRedis(decode_responses=True)
    order: list[str] = []
    real_gen = tenants_svc._publish_generation
    real_status = tenants_svc._publish_status

    def gen(*a, **k):
        order.append("generation")
        return real_gen(*a, **k)

    def status(*a, **k):
        order.append("active")
        return real_status(*a, **k)

    monkeypatch.setattr(tenants_svc, "_publish_generation", gen)
    monkeypatch.setattr(tenants_svc, "_publish_status", status)

    tenants_svc.create_tenant(store, TENANT, redis_client=fake)
    assert order[:2] == ["generation", "active"], order
    generation = read_tenant_generation(fake, TENANT)
    assert generation
    assert fake.hget("plaita:tenant_status", TENANT) == "active"

    # 旧世代（任意其他值）在 active 新世代下被写闸拒绝。
    allowed, reason = tenant_write_allowed(fake, TENANT, "old-generation")
    assert allowed is False
    assert reason == "generation_changed"
