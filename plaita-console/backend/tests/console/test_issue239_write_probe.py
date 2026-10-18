"""plaita#239 phase 2：凭据写调用点的租户来源 fail-closed。

owner 裁决（2026-10-18）：``save_credential`` / ``delete_credential`` 在来源为
``default``（从未显式/派生设置）时抛具名 ``TenantContextMissingError``，
不触达底层写；``explicit`` / ``derived`` 照常。探针计数保留（含被拒尝试）。
"""
from __future__ import annotations

import sys
from contextvars import Context
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import credentials_svc, flow_store  # noqa: E402
from plaita.core.parallel_executor import ThreadParallelExecutor  # noqa: E402
from plaita.tenant_context import (  # noqa: E402
    TENANT_SOURCE_DEFAULT,
    TENANT_SOURCE_DERIVED,
    TENANT_SOURCE_EXPLICIT,
    TenantContextMissingError,
    reset_current_tenant,
    reset_write_probe,
    set_current_tenant,
    write_probe_snapshot,
)


@pytest.fixture()
def cred_store(tmp_path, monkeypatch):
    db_file = tmp_path / "p239.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    reset_write_probe()
    yield
    reset_write_probe()


def test_credentials_writes_default_source_fail_closed(cred_store):
    def _run():
        with pytest.raises(TenantContextMissingError):
            credentials_svc.save_credential("probe", "generic", {"x": 1})
        with pytest.raises(TenantContextMissingError):
            credentials_svc.delete_credential("probe")
        return write_probe_snapshot()

    snapshot = Context().run(_run)
    assert snapshot[("credentials.save", TENANT_SOURCE_DEFAULT)] == 1
    assert snapshot[("credentials.delete", TENANT_SOURCE_DEFAULT)] == 1
    assert credentials_svc.get_credential_record("probe") is None, (
        "fail-closed 时不得落库"
    )


def test_credentials_writes_explicit_source_counted(cred_store):
    def _run():
        token = set_current_tenant("tenant-b")
        try:
            credentials_svc.save_credential(
                "probe-b", "generic", {"x": 2}, tenant_id="tenant-b")
            ok = credentials_svc.delete_credential("probe-b", tenant_id="tenant-b")
        finally:
            reset_current_tenant(token)
        return ok, write_probe_snapshot()

    ok, snapshot = Context().run(_run)
    assert ok is True
    assert snapshot[("credentials.save", TENANT_SOURCE_EXPLICIT)] == 1
    assert snapshot[("credentials.delete", TENANT_SOURCE_EXPLICIT)] == 1


def test_credentials_writes_derived_source_succeeds(cred_store):
    def _run():
        token = set_current_tenant("tenant-b")
        try:
            def _write():
                credentials_svc.save_credential(
                    "probe-d", "generic", {"x": 3}, tenant_id="tenant-b")
                return credentials_svc.delete_credential(
                    "probe-d", tenant_id="tenant-b")

            ok = ThreadParallelExecutor(max_workers=1).submit(_write).result()
        finally:
            reset_current_tenant(token)
        return ok, write_probe_snapshot()

    ok, snapshot = Context().run(_run)
    assert ok is True
    assert snapshot[("credentials.save", TENANT_SOURCE_DERIVED)] == 1
    assert snapshot[("credentials.delete", TENANT_SOURCE_DERIVED)] == 1
