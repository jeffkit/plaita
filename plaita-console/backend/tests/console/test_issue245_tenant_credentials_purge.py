"""plaita#245（P1 security）：删除租户必须同步清理凭据。

缺口：``tenants_svc.delete_tenant`` 只删 ``Tenant`` / ``TenantMember`` 行，
该租户的 ``Credential`` 行与引擎导出旁文件全部保留。平台管理员重建同名
租户后，新租户的租户管理员即可通过 ``GET /api/credentials`` 读到旧租户
明文；worker 也会继续把旧旁文件当新租户的凭据源加载。

修法：``delete_tenant`` 同一事务删除 ``Credential.tenant_id == tenant_id``
的行，提交后删除 ``credentials_file(tenant_id)``（导入失败显式抛出）。

负控（必红）：去掉 ``delete_tenant`` 的 Credential 删除 + 旁文件清理后，
本文件 3 例全 FAILED——删除后的残留探针**直接断言字面旁文件路径不存在**，
不依赖导出函数自证；删文件失败探针也因清理行缺失而拿不到 ``PermissionError``。

旁文件路径按 #215 约定**字面**拼出（``<stem>.<tenant><suffix>``），不借
``credentials_svc.credentials_file`` 计算期望值——否则命名回归时导出与
断言两端一起错、用例仍绿。
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import Depends, FastAPI
from sqlalchemy import select
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import tenants as tenants_api  # noqa: E402
from auth import require_auth  # noqa: E402
from models.flow import Credential, Tenant  # noqa: E402
from services import credentials_svc, flow_store, tenants_svc, users_svc  # noqa: E402
from plaita.credentials import CredentialError, get_credential  # noqa: E402
from plaita.tenant_context import reset_current_tenant, set_current_tenant  # noqa: E402

ADMIN_KEY = "test-admin-key-245"


def _sidecar(base: Path, tenant_id: str) -> Path:
    """#215 字面规则：``<stem>.<tenant_id><suffix>``。"""
    return base.with_name(f"{base.stem}.{tenant_id}{base.suffix}")


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def console(tmp_path, monkeypatch):
    """独立 DB + 独立凭据导出树；平台管理员 root 已登录。"""
    db_file = tmp_path / "issue245.db"
    creds = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(creds))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", _settings)
    monkeypatch.setattr("auth.get_settings", _settings)

    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    users_svc.create_user(
        store, "root", "root-password-1", "admin", platform_admin=True
    )
    users_svc.add_member(store, "default", "root", "admin")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        credentials_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        tenants_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    client = TestClient(app)
    login = client.post(
        "/api/auth/login", json={"username": "root", "password": "root-password-1"}
    )
    assert login.status_code == 200, login.text
    headers = {"Authorization": f"Bearer {login.json()['token']}"}
    return SimpleNamespace(client=client, store=store, creds=creds, headers=headers)


def _create_tenant(console, tenant_id: str) -> None:
    r = console.client.post(
        "/api/tenants", json={"id": tenant_id}, headers=console.headers
    )
    assert r.status_code == 200, r.text


def _save_credential(console, tenant_id: str, name: str, secret: str) -> None:
    r = console.client.post(
        "/api/credentials",
        json={"name": name, "type": "api", "data": {"token": secret}},
        headers={**console.headers, "X-Tenant-ID": tenant_id},
    )
    assert r.status_code == 200, r.text


def test_delete_tenant_purges_credentials_and_sidecar(console):
    """删租户 → DB 行 / 旁文件都没了；重建同名租户读不到旧明文。"""
    _create_tenant(console, "acme")
    _save_credential(console, "acme", "prod-db", "ACME-SECRET-42")

    sidecar = _sidecar(console.creds, "acme")
    assert sidecar.is_file(), "保存凭据应导出旁文件"
    # 先钉住命名与内容，确认删除的是同一份文件
    assert sidecar == console.creds.with_name(f"{console.creds.stem}.acme.json")
    assert "ACME-SECRET-42" not in sidecar.read_text()
    assert credentials_svc.get_credential_record(
        "prod-db", tenant_id="acme"
    )["data"]["token"] == "ACME-SECRET-42"

    r = console.client.delete("/api/tenants/acme", headers=console.headers)
    assert r.status_code == 200, r.text

    # 负控探针：旁文件残留 = 租户已删但 worker 仍能读到旧密文
    assert not sidecar.exists(), f"删除租户后凭据旁文件残留: {sidecar}"
    assert credentials_svc.list_credentials("acme") == []
    assert credentials_svc.get_credential_record("prod-db", tenant_id="acme") is None
    with console.store._session_local() as session:
        rows = session.scalars(
            select(Credential).where(Credential.tenant_id == "acme")
        ).all()
        assert rows == []

    # 重建同名租户：列表为空、详情 404、worker 侧解析不到旧值
    _create_tenant(console, "acme")
    acme_headers = {**console.headers, "X-Tenant-ID": "acme"}
    assert console.client.get("/api/credentials", headers=acme_headers).json() == {
        "credentials": [],
        "total": 0,
    }
    assert (
        console.client.get("/api/credentials/prod-db", headers=acme_headers).status_code
        == 404
    )
    assert not sidecar.exists(), "重建租户本身不得把旧旁文件带回来"

    token = set_current_tenant("acme")
    try:
        with pytest.raises(CredentialError) as exc:
            get_credential("prod-db")
        assert "（无）" in str(exc.value)
    finally:
        reset_current_tenant(token)


def test_delete_tenant_only_purges_its_own_credentials(console):
    """清理面必须按租户收窄：不能误删其他租户/default 的导出文件。"""
    _create_tenant(console, "acme")
    _create_tenant(console, "beta")
    _save_credential(console, "default", "default-bot", "DEFAULT-SECRET")
    _save_credential(console, "acme", "acme-bot", "ACME-SECRET")
    _save_credential(console, "beta", "beta-bot", "BETA-SECRET")

    default_file = console.creds
    beta_file = _sidecar(console.creds, "beta")
    assert default_file.is_file() and beta_file.is_file()

    assert (
        console.client.delete("/api/tenants/acme", headers=console.headers).status_code
        == 200
    )

    assert not _sidecar(console.creds, "acme").exists()
    assert default_file.is_file(), "删除 acme 不得清掉 default 的凭据文件"
    assert beta_file.is_file(), "删除 acme 不得清掉 beta 的凭据文件"
    assert credentials_svc.get_credential_record(
        "default-bot", tenant_id="default"
    )["data"]["token"] == "DEFAULT-SECRET"
    assert credentials_svc.get_credential_record(
        "beta-bot", tenant_id="beta"
    )["data"]["token"] == "BETA-SECRET"
    assert credentials_svc.list_credentials("acme") == []


def test_sidecar_removal_failure_is_loud(console, monkeypatch):
    """旁文件删不掉必须显式抛错（不能静默留下可被 worker 读取的旧密文）。"""
    _create_tenant(console, "acme")
    _save_credential(console, "acme", "prod-db", "ACME-SECRET-42")
    sidecar = _sidecar(console.creds, "acme")
    assert sidecar.is_file()

    real_unlink = Path.unlink

    def _deny_sidecar(self, *args, **kwargs):
        if self == sidecar:
            raise PermissionError(f"denied: {self}")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", _deny_sidecar)

    with pytest.raises(PermissionError):
        tenants_svc.delete_tenant(console.store, "acme")

    # 失败必须可观测且可恢复（plaita#257 顺序反转）：旁文件仍在（未伪造成
    # 清理成功），DB 事务已回滚——租户行/凭据行都还在，重试不会撞 404。
    assert sidecar.is_file()
    with console.store._session_local() as session:
        assert session.scalars(select(Tenant).where(Tenant.id == "acme")).first() is not None
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == "acme")
        ).all() != []

    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert tenants_svc.delete_tenant(console.store, "acme") is True
    assert not sidecar.exists()
    with console.store._session_local() as session:
        assert session.scalars(select(Tenant).where(Tenant.id == "acme")).first() is None
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == "acme")
        ).all() == []
