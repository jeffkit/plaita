"""plaita#226 A3/A4/A5：凭据租户隔离与密钥文件权限回归钉。

- **A3**：`list_credentials(tenant_id)` 必须只返回该租户的元信息（删掉
  `if tenant_id is not None:` 过滤后 A 视角会看到 B 的凭据名称/类型）；
- **A4**：`get_credential_record(name, tenant_id)` 跨租户必须返回 None（删掉
  同一过滤块后直接泄露 B 的凭据**明文**）；
- **A5**：无 `PLAITA_CREDENTIALS_KEY` 时首次生成的密钥文件必须 `0o600`
  （改成 `0o644` 后宿主其他用户可读 Fernet key，静态加密失效）。A67-05 起
  由 `os.open(O_EXCL, 0o600)` 直接创建；历史实现是 `write_bytes` 后 chmod。

负控：A3/A4 把两处 `if tenant_id is not None:` 改为 `if False:`；A5 把创建
模式改为 `0o644`（A67-05 前为 `os.chmod(key_file, 0o600)`）。证据见
`docs/proofs/226-six-regression-gaps.md` 与 `docs/proofs/data/226-*`。
"""
from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import credentials_svc, flow_store  # noqa: E402
from models.flow import Tenant  # noqa: E402


@pytest.fixture()
def cred_store(tmp_path, monkeypatch):
    """独立 DB + 无 env key 的密钥文件路径（A5 需要首次生成）。"""
    db_file = tmp_path / "issue226.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    _ensure_tenants("default")
    return tmp_path


def _ensure_tenants(*tenant_ids: str) -> None:
    """plaita#257：save_credential 拒绝不存在的租户，先建出归属行。"""
    store = flow_store.get_flow_store()
    with store._session_local() as session:
        for tid in tenant_ids:
            if session.get(Tenant, tid) is None:
                session.add(Tenant(
                    id=tid, name=tid, status="active",
                    # contract_secret_id 唯一：测试租户各自一份占位值
                    contract_secret_id=f"t-{tid}", contract_secret_key="k",
                ))
        session.commit()


def _seed_two_tenants() -> None:
    _ensure_tenants("A", "B")
    # 同名 shared 在两租户各一份，值不同——跨租户读取会立刻暴露。
    credentials_svc.save_credential(
        "a", "generic", {"url": "https://a.example"}, tenant_id="A")
    credentials_svc.save_credential(
        "b", "generic", {"url": "https://b.example"}, tenant_id="B")
    credentials_svc.save_credential(
        "shared", "generic", {"url": "https://a.shared"}, tenant_id="A")
    credentials_svc.save_credential(
        "shared", "generic", {"url": "https://b.shared"}, tenant_id="B")


# -- A3: list_credentials tenant filter -------------------------------------


def test_list_credentials_is_tenant_scoped(cred_store):
    _seed_two_tenants()

    names_a = [c["name"] for c in credentials_svc.list_credentials("A")]
    names_b = [c["name"] for c in credentials_svc.list_credentials("B")]
    assert names_a == ["a", "shared"], f"A 视角泄露了其他租户: {names_a!r}"
    assert names_b == ["b", "shared"], f"B 视角泄露了其他租户: {names_b!r}"

    # 平台全量视角（tenant_id=None）仍是跨租户，语义不变
    all_names = {c["name"] for c in credentials_svc.list_credentials(None)}
    assert all_names == {"a", "b", "shared"}


# -- A4: get_credential_record tenant filter --------------------------------


def test_get_credential_record_is_tenant_scoped(cred_store):
    _seed_two_tenants()

    assert credentials_svc.get_credential_record("b", "A") is None, (
        "A 租户读到了 B 的凭据（跨租户明文泄露）"
    )
    assert credentials_svc.get_credential_record("a", "B") is None
    assert (credentials_svc.get_credential_record("b", "B")["data"]["url"]
            == "https://b.example")
    # 同名凭据各读各的
    assert (credentials_svc.get_credential_record("shared", "A")["data"]["url"]
            == "https://a.shared")
    assert (credentials_svc.get_credential_record("shared", "B")["data"]["url"]
            == "https://b.shared")


# -- A5: key file mode ------------------------------------------------------


def test_generated_key_file_is_0600(cred_store):
    key_file = Path(os.environ["PLAITA_CREDENTIALS_KEY_FILE"])
    assert not key_file.exists(), "fixture 应提供全新密钥文件路径"

    credentials_svc.save_credential("k", "generic", {"x": 1})

    assert key_file.is_file(), "无 env key 时应首次生成密钥文件"
    mode = stat.S_IMODE(key_file.stat().st_mode)
    assert mode == 0o600, (
        f"密钥文件权限必须 0o600（其他用户不得读 Fernet key），实得 {oct(mode)}"
    )
