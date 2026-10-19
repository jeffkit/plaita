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
    DEFAULT_FILE,
    DEFAULT_KEY_FILE,
    CredentialError,
    credential_type,
    credentials_base_file,
    credentials_file,
    credentials_key_file,
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
    """写入/合并一个条目（同文件多次写入不丢已有条目）。"""
    from cryptography.fernet import Fernet

    store = {}
    if path.is_file():
        store = json.loads(path.read_text())
    token = Fernet(json.loads('"' + __import__("os").environ["PLAITA_CREDENTIALS_KEY"] + '"')).encrypt(
        json.dumps(data).encode()
    ).decode()
    store[name] = {"type": "generic", "data": token}
    path.write_text(json.dumps(store))


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


class TestDefaultPathResolution:
    """A66-01/A66-03：默认导出/密钥路径与 DB 同目录（console 同源）。

    导出文件是 DB 的派生缓存；按 cwd 落盘会让同 DB 多副本各持一份、删除/
    轮换不收敛。默认路径必须跟着 DB 走，显式 env 仍然优先。
    """

    def test_sqlite_db_dir_is_default_base(self, tmp_path, monkeypatch):
        db = tmp_path / "console.db"
        monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db}")
        monkeypatch.delenv("PLAITA_CREDENTIALS_FILE", raising=False)
        monkeypatch.delenv("PLAITA_CREDENTIALS_KEY_FILE", raising=False)

        assert credentials_base_file() == tmp_path / DEFAULT_FILE
        assert credentials_key_file() == tmp_path / DEFAULT_KEY_FILE

    def test_non_sqlite_falls_back_to_cwd(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", "postgresql://u:p@h/db")
        monkeypatch.delenv("PLAITA_CREDENTIALS_FILE", raising=False)
        monkeypatch.chdir(tmp_path)

        assert credentials_base_file() == tmp_path / DEFAULT_FILE

    def test_explicit_env_wins_over_db_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path / 'db.sqlite'}")
        explicit = tmp_path / "explicit.json"
        monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(explicit))

        assert credentials_base_file() == explicit

    def test_empty_env_treated_as_unset(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path / 'db.sqlite'}")
        monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", "")

        assert credentials_base_file() == tmp_path / DEFAULT_FILE


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


class TestReservedTenantNames:
    """plaita#215：root / admin 等保留样租户名不得落到 default 基础文件。

    历史缺口：既有用例只覆盖 default/空/acme/t-1234，从未使用 ``root``。
    若 ``tenant_credentials_file`` 回归成把 ``root`` 归入 default，现有 12 条
    全绿，而名为 root 的租户会读取**平台默认凭据文件**（跨租户串用）。
    这里同时钉路径与真实读取值，避免只测字符串。
    """

    def test_root_and_admin_route_to_side_files_not_base(self):
        base = Path("/data/creds.json")
        assert tenant_credentials_file("root", base) == Path("/data/creds.root.json")
        assert tenant_credentials_file("admin", base) == Path("/data/creds.admin.json")
        # default 是唯一读基础文件的租户（历史兼容），保留原行为
        assert tenant_credentials_file("default", base) == base

    def test_root_tenant_reads_its_side_file_value(self, cred_env):
        # 旁文件名按文档规则**字面**拼出（不借 tenant_credentials_file 算写入路径，
        # 否则函数本身回归时写读两端一起错、用例仍绿——负控会漏）。
        side = cred_env.with_name(f"{cred_env.stem}.root{cred_env.suffix}")
        _write(cred_env, "shared", {"url": "https://default.example"})
        _write(cred_env, "default-only", {"url": "https://default-only.example"})
        _write(side, "shared", {"url": "https://root.example"})
        _write(side, "root-only", {"url": "https://root-only.example"})

        token = set_current_tenant("root")
        try:
            assert tenant_credentials_file("root", cred_env) == side
            assert credentials_file() == side
            # 同名凭据必须取 root 旁文件的值，而不是平台默认文件的值
            assert get_credential("shared")["url"] == "https://root.example"
            assert get_credential("root-only")["url"] == "https://root-only.example"
            assert credential_type("shared") == "generic"
            # default 文件里的条目对 root 不可见（报错只列当前租户文件内的名字）
            with pytest.raises(CredentialError) as exc:
                get_credential("default-only")
            assert "root-only" in str(exc.value)
            assert "default-only" in str(exc.value)  # 只有请求名本身，不是泄漏来源
        finally:
            reset_current_tenant(token)

    def test_root_and_admin_do_not_share_side_files(self, cred_env):
        for tid in ("root", "admin"):
            side = cred_env.with_name(f"{cred_env.stem}.{tid}{cred_env.suffix}")
            _write(side, f"{tid}-only", {"url": f"https://{tid}.example"})
            token = set_current_tenant(tid)
            try:
                assert get_credential(f"{tid}-only")["url"] == f"https://{tid}.example"
            finally:
                reset_current_tenant(token)


def test_tenant_namespace_unchanged_for_engine_keys():
    # 防回归哨兵：本修复只动凭据文件路径，Redis 键 namespace 映射不受影响
    assert tenant_namespace("acme") == "plaita:acme"
    assert tenant_namespace("default") == "plaita"


class TestTenantRootIsNotDefault:
    """plaita#215：**名为 ``root`` 的租户必须走旁文件，不得归入 default**。

    缺口来源：现有用例覆盖 ``default`` / 空 / ``acme`` / ``t-1234``，**但没有 ``root``**。
    ``root`` 能通过 console 的 slug 正则（``^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$``），
    是**合法租户名**；而它的名字又"很像保留字"，极易在后续改动中被顺手写进
    default 分支（负控实证：把 ``credentials.py`` 改成
    ``if tid in (DEFAULT_TENANT_ID, "root")`` 后，**原有 12 条全绿**，
    ``root`` 悄悄读到平台默认凭据文件 ⇒ 跨租户凭据串用）。

    本类补的就是这个回归面。**不只测路径，也测 ``get_credential`` 的端到端取值**——
    只测路径会漏掉"路径对了但读取仍走 base"的实现方式。
    """

    @pytest.mark.parametrize("tid", ["root", "admin", "system", "defaults"])
    def test_reserved_looking_names_use_side_files(self, tid):
        base = Path("/data/creds.json")
        # 非 default 一律旁文件；保留样名字不享受 default 待遇
        assert tenant_credentials_file(tid, base) == Path(f"/data/creds.{tid}.json")

    def test_tenant_root_reads_own_credential_end_to_end(self, cred_env):
        """``root`` 读到的是**旁文件里的值**，不是默认凭据文件里的同名凭据。"""
        _write(cred_env, "shared", {"url": "https://default.example"})
        side = tenant_credentials_file("root", cred_env)
        assert side == cred_env.with_name(f"{cred_env.stem}.root{cred_env.suffix}")
        _write(side, "shared", {"url": "https://root.example"})

        token = set_current_tenant("root")
        try:
            assert credentials_file() == side
            assert get_credential("shared")["url"] == "https://root.example"
        finally:
            reset_current_tenant(token)

    def test_tenant_root_cannot_read_default_only_credential(self, cred_env):
        """默认凭据文件里独占的条目对 ``root`` **不可见**（跨租户隔离）。"""
        _write(cred_env, "default-only", {"url": "https://default.example"})

        token = set_current_tenant("root")
        try:
            with pytest.raises(CredentialError) as exc:
                get_credential("default-only")
            assert "（无）" in str(exc.value)
        finally:
            reset_current_tenant(token)
