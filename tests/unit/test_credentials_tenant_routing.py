"""凭据租户路由单测（plaita#24 集群档断裂修复）。

引擎侧 ``plaita.credentials`` 原生按租户上下文（``plaita.tenant_context``）
路由凭据文件：default/空 = 基础文件（历史兼容），其余租户 = 旁文件
``<stem>.<tenant_id><suffix>``——与 console 凭据页按租户导出的命名一致。
"""
import json
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from plaita.credentials import (
    CredentialError,
    credential_type,
    credentials_file,
    get_credential,
    tenant_credentials_file,
)
from plaita.tenant_context import (
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)


@pytest.fixture()
def cred_env(tmp_path, monkeypatch):
    """基础凭据文件 + Fernet key，返回写条目的辅助函数。"""
    base = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(base))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY", Fernet.generate_key().decode())
    return base


def _write(path: Path, name: str, data: dict) -> None:
    from cryptography.fernet import Fernet

    token = Fernet(json.loads('"' + __import__("os").environ["PLAITA_CREDENTIALS_KEY"] + '"')).encrypt(
        json.dumps(data).encode()
    ).decode()
    path.write_text(json.dumps({name: {"type": "generic", "data": token}}))


class TestTenantCredentialsFile:
    def test_default_and_empty_use_base_file(self):
        base = Path("/data/creds.json")
        assert tenant_credentials_file(None, base) == base
        assert tenant_credentials_file("", base) == base
        assert tenant_credentials_file("default", base) == base

    def test_named_tenant_uses_side_file(self):
        base = Path("/data/creds.json")
        assert tenant_credentials_file("acme", base) == Path("/data/creds.acme.json")
        assert tenant_credentials_file("t-1234", base) == Path("/data/creds.t-1234.json")

    def test_side_file_naming_matches_console_export_rule(self, tmp_path):
        # console 导出（credentials_svc.credentials_file）按
        # <stem>.<tenant><suffix> 命名旁文件；这里按同一规则独立推导，
        # 引擎侧解析路径必须与之逐字相等（两端命名漂移 = 租户全取空）。
        base = tmp_path / "creds.json"
        for tid in ("acme", "t-1234", "x"):
            console_path = base.with_name(f"{base.stem}.{tid}{base.suffix}")
            assert tenant_credentials_file(tid, base) == console_path

    def test_default_context_reads_base_file(self, cred_env):
        assert credentials_file() == cred_env


class TestGetCredentialTenantRouting:
    def test_default_tenant_reads_base_file(self, cred_env):
        _write(cred_env, "shared", {"url": "https://default.example"})
        assert get_credential("shared")["url"] == "https://default.example"

    def test_named_tenant_reads_side_file_not_base(self, cred_env):
        _write(cred_env, "shared", {"url": "https://default.example"})
        side = tenant_credentials_file("acme", cred_env)
        _write(side, "shared", {"url": "https://acme.example"})

        token = set_current_tenant("acme")
        try:
            assert credentials_file() == side
            # 同名凭据取到自己租户的值，不是 default 的
            assert get_credential("shared")["url"] == "https://acme.example"
        finally:
            reset_current_tenant(token)

    def test_default_credentials_invisible_to_named_tenant(self, cred_env):
        _write(cred_env, "default-only", {"url": "https://default.example"})

        token = set_current_tenant("acme")
        try:
            with pytest.raises(CredentialError) as exc:
                get_credential("default-only")
            # 可用列表不泄露其他租户（default）配置了哪些凭据名——报错里
            # 只出现被请求的名字本身（"凭据 'x' 不存在"），不出现 default
            # 文件内的其他条目。
            assert "（无）" in str(exc.value)
        finally:
            reset_current_tenant(token)

    def test_named_tenant_own_credential_resolves(self, cred_env):
        _write(tenant_credentials_file("acme", cred_env), "acme-bot",
               {"url": "https://acme.example"})

        token = set_current_tenant("acme")
        try:
            assert get_credential("acme-bot")["url"] == "https://acme.example"
        finally:
            reset_current_tenant(token)

    def test_missing_lists_only_current_tenant_names(self, cred_env):
        _write(cred_env, "a", {"k": "v"})
        _write(tenant_credentials_file("acme", cred_env), "b", {"k": "v"})

        token = set_current_tenant("acme")
        try:
            with pytest.raises(CredentialError) as exc:
                get_credential("nope")
            assert "b" in str(exc.value) and "a" not in str(exc.value)
        finally:
            reset_current_tenant(token)

    def test_empty_tenant_context_falls_back_to_default_file(self, cred_env):
        _write(cred_env, "x", {"k": "v"})
        # 直接置空租户（旧生产方兼容路径），应读基础文件
        token = set_current_tenant(None)
        try:
            assert get_credential("x")["k"] == "v"
        finally:
            reset_current_tenant(token)

    def test_credential_type_follows_tenant(self, cred_env):
        _write(cred_env, "t1", {"k": "v"})
        _write(tenant_credentials_file("acme", cred_env), "t2", {"k": "v"})

        token = set_current_tenant("acme")
        try:
            assert credential_type("t2") == "generic"
            assert credential_type("t1") is None
        finally:
            reset_current_tenant(token)


def test_tenant_namespace_unchanged_for_engine_keys():
    # 防回归哨兵：本修复只动凭据文件路径，Redis 键 namespace 映射不受影响
    assert tenant_namespace("acme") == "plaita:acme"
    assert tenant_namespace("default") == "plaita"
