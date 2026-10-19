"""plaita#257（P1 security）：删除租户的凭据旁文件必须先清、成功才 commit DB。

缺陷（与 #251 的 delete_credential 同构，租户路径未对齐）：旧
``tenants_svc.delete_tenant`` 先 ``session.commit()`` 删租户/凭据行，再删
引擎可读的凭据旁文件。unlink 失败（权限/磁盘）后租户行已消失，重试删除
只会拿到 404——「租户已删、旧密钥仍可被引擎读到」的不可恢复死路；同名
租户重建后即继承旧密文。

修复契约：
- 旁文件（含密钥）先清、成功后才 commit DB；失败方向 fail-safe：
  宁可「租户在而凭据不可达」，不可「租户删而凭据残留」；
- 失败可重试直至干净：事务回滚后租户行仍在，重试成功；
  旧版本留下的「行已删、旁文件在」404 死路由 404 路径补清扫；
- ``save_credential`` 拒绝已删/不存在的租户（堵迟到写把 store 复活）。

负控（必红）：把 ``delete_tenant`` 改回「先 commit 后 unlink」，本文件
fail-safe 用例看到 tenant row None + 引擎仍解出明文；去掉
``save_credential`` 的租户存在性检查，拒绝写回用例必红。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import select

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import Credential, FlowRecord, Tenant, TenantMember  # noqa: E402
from services import credentials_svc, flow_store, tenants_svc  # noqa: E402
from plaita.credentials import CredentialError, get_credential  # noqa: E402
from plaita.tenant_context import reset_current_tenant, set_current_tenant  # noqa: E402

TENANT = "acme"
SECRET = "ACME-SECRET-42"


def _sidecar(base: Path, tenant_id: str) -> Path:
    """#215 字面规则：``<stem>.<tenant_id><suffix>``（不借导出函数自证）。"""
    return base.with_name(f"{base.stem}.{tenant_id}{base.suffix}")


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "issue257.db"
    creds = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(creds))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    credentials_svc.save_credential(
        "prod-db", "api", {"token": SECRET}, tenant_id=TENANT
    )
    return {
        "store": store,
        "creds": creds,
        "sidecar": _sidecar(creds, TENANT),
    }


def _engine_secret(tenant_id: str):
    token = set_current_tenant(tenant_id)
    try:
        try:
            return get_credential("prod-db")
        except CredentialError:
            return None
    finally:
        reset_current_tenant(token)


def _deny_unlink(monkeypatch, sidecar: Path):
    real_unlink = Path.unlink

    def _deny(self, *args, **kwargs):
        if self == sidecar:
            raise PermissionError(f"denied: {self}")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _deny)
    return real_unlink


def test_sidecar_failure_is_fail_safe_and_retryable(env, monkeypatch):
    """unlink 注入 → 租户/凭据/旁文件都还在（fail-safe），重试成功且引擎读不到。"""
    sidecar = env["sidecar"]
    assert sidecar.is_file()
    assert _engine_secret(TENANT) == {"token": SECRET}

    real_unlink = _deny_unlink(monkeypatch, sidecar)
    with pytest.raises(PermissionError):
        tenants_svc.delete_tenant(env["store"], TENANT)

    # fail-safe 方向：租户在而凭据不可达（此处旁文件也还在），不可反向。
    assert sidecar.is_file()
    with env["store"]._session_local() as session:
        assert session.get(Tenant, TENANT) is not None
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == TENANT)
        ).all() != []

    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert tenants_svc.delete_tenant(env["store"], TENANT) is True

    assert not sidecar.exists(), "重试成功后凭据旁文件必须清理"
    assert _engine_secret(TENANT) is None, "引擎不得再解出已删租户的明文"
    with env["store"]._session_local() as session:
        assert session.get(Tenant, TENANT) is None
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == TENANT)
        ).all() == []


def test_save_credential_refuses_deleted_tenant(env):
    """删租户后的迟到写被拒，不能把凭据行/旁文件复活。"""
    sidecar = env["sidecar"]
    assert tenants_svc.delete_tenant(env["store"], TENANT) is True
    assert not sidecar.exists()

    with pytest.raises(ValueError, match="租户不存在或已删除"):
        credentials_svc.save_credential(
            "prod-db", "api", {"token": "RESURRECTED"}, tenant_id=TENANT
        )

    assert credentials_svc.list_credentials(TENANT) == []
    assert not sidecar.exists(), "拒绝写回时不得重新导出旁文件"
    assert credentials_svc.get_credential_record("prod-db", TENANT) is None


def test_save_credential_refuses_unknown_tenant(env):
    """从未存在的租户同样拒绝写回（清理后存储不得被凭空重建）。"""
    with pytest.raises(ValueError, match="租户不存在或已删除"):
        credentials_svc.save_credential(
            "ghost", "api", {"token": "GHOST"}, tenant_id="ghost-tenant"
        )
    assert credentials_svc.list_credentials("ghost-tenant") == []


def test_legacy_404_retry_sweeps_sidecar(env):
    """旧版本留下的「租户行已删、旁文件仍在」404 死路：重试删除补清扫。

    模拟方式是直接删 DB 行（旧实现 commit 后的中间态），旁文件保持不动。
    """
    sidecar = env["sidecar"]
    with env["store"]._session_local() as session:
        for model in (Credential, TenantMember):
            for row in session.scalars(
                select(model).where(model.tenant_id == TENANT)
            ).all():
                session.delete(row)
        session.delete(session.get(Tenant, TENANT))
        session.commit()
    assert sidecar.is_file(), "前提：旁文件在旧删除路径后仍残留"

    assert tenants_svc.delete_tenant(env["store"], TENANT) is False
    assert not sidecar.exists(), "404 路径必须补清扫旧残留"
    assert _engine_secret(TENANT) is None


def test_flow_guard_still_rejects_before_sidecar_cleanup(env):
    """「仍有 flow 先清空」的拒绝门先于一切清理，旁文件不得被提前删掉。"""
    sidecar = env["sidecar"]
    env["store"].create_flow("busy-flow", tenant_id=TENANT)

    with pytest.raises(tenants_svc.TenantError, match="仍有"):
        tenants_svc.delete_tenant(env["store"], TENANT)

    assert sidecar.is_file(), "拒绝删除时不得触碰凭据旁文件"
    with env["store"]._session_local() as session:
        assert session.get(Tenant, TENANT) is not None
        assert session.scalars(
            select(FlowRecord).where(FlowRecord.tenant_id == TENANT)
        ).all() != []
