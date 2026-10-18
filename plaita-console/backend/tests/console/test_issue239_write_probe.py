"""plaita#239 先行：凭据写调用点的来源探针（只观测，行为零变更）。

`save_credential` / `delete_credential` 记录 (site, 来源) 计数；default 来源
额外 DEBUG。探针不拦截、不改变返回值——default 来源的写仍然成功（fail-closed
决策待 #239 后半 owner 裁决）。
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
from plaita.tenant_context import (  # noqa: E402
    TENANT_SOURCE_DEFAULT,
    TENANT_SOURCE_EXPLICIT,
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


def test_credentials_writes_default_source_counted_and_still_succeed(cred_store):
    def _run():
        credentials_svc.save_credential("probe", "generic", {"x": 1})
        ok = credentials_svc.delete_credential("probe")
        return ok, write_probe_snapshot()

    ok, snapshot = Context().run(_run)
    assert ok is True, "探针不得改变写行为（default 来源仍成功）"
    assert snapshot[("credentials.save", TENANT_SOURCE_DEFAULT)] == 1
    assert snapshot[("credentials.delete", TENANT_SOURCE_DEFAULT)] == 1


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
