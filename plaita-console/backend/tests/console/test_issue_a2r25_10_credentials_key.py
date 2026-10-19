"""A2R25-10：错/缺 key 恢复不得产生混合密钥。

缺陷（duo-a2-ledger 第二十五轮）：`reconcile_exports` 只重写密文、不解密，
缺/错 key 时仍返回 green；`GET /credentials/{name}` 抛裸 500；更糟的是
`save_credential` 不验旧行能否解密，错 key 下的第一次保存会用错误密钥加密
新行并重写整个导出，把库变成**同一 DB 混合密钥**（导出行无 key id，切回
原 key 后新行不可解）。

修复契约：
1. 导出文件在有密文时带 `__plaita_meta__` 版本头（format/version/key_id）；
2. `save_credential` 写前逐条验证旧密文可由当前 key 解开，失败抛
   `CredentialsKeyMismatchError`（reason=mismatch/missing），DB 与导出文件
   逐字节不变；
3. 启动对账在 key 不匹配时跳过该租户的导出重写（原文件保留）并在摘要里
   报 `key_mismatch`；
4. 详情 API 返回 503 JSON（`credentials_key_mismatch`），保存 API 返回
   409，不再 500 text/plain / 静默 200；
5. 引擎读侧忽略保留键，旧的无头导出仍可读，下次保存自动升级格式。

负控：去掉 `save_credential` 的 `_first_undecryptable` 预检 → 保存成功且
导出文件被重写；去掉对账的 verify/skip → 错 key 下文件被覆盖。
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import credentials as credentials_api  # noqa: E402
from plaita.credentials import STORE_META_KEY  # noqa: E402
from services import credentials_svc, flow_store  # noqa: E402

SECRET_A = {"secret": "encrypted-with-A"}
SECRET_B = {"secret": "encrypted-with-B"}


def _key_id(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()[:16]


@pytest.fixture()
def env(tmp_path, monkeypatch) -> SimpleNamespace:
    db_file = tmp_path / "cred.db"
    creds = tmp_path / "creds.json"
    key_file = tmp_path / "creds.key"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(creds))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(key_file))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()

    credentials_svc.save_credential("old-a", "generic", SECRET_A)
    key_a = key_file.read_text().strip()

    app = FastAPI()
    app.include_router(credentials_api.router, prefix="/api")
    return SimpleNamespace(
        creds=creds,
        key_file=key_file,
        key_a=key_a,
        client=TestClient(app),
    )


def _export(env) -> dict:
    return json.loads(env.creds.read_text(encoding="utf-8"))


def test_export_carries_key_id_header(env):
    data = _export(env)
    meta = data[STORE_META_KEY]
    assert meta["format"] == "plaita-credentials"
    assert meta["version"] == 2
    assert meta["key_id"] == _key_id(env.key_a)
    assert data["old-a"]["type"] == "generic"
    # 密钥指纹不是密钥本身
    assert env.key_a not in env.creds.read_text(encoding="utf-8")


def test_reserved_meta_key_cannot_be_a_credential(env):
    with pytest.raises(ValueError, match="保留键"):
        credentials_svc.save_credential(STORE_META_KEY, "generic", SECRET_B)
    # 文件顶层的保留键仍是版本头，DB 里没有同名凭据行。
    assert _export(env)[STORE_META_KEY]["format"] == "plaita-credentials"
    assert [c["name"] for c in credentials_svc.list_credentials()] == ["old-a"]


def test_wrong_key_save_rejected_and_export_preserved(env):
    raw_before = env.creds.read_bytes()
    env.key_file.write_text(Fernet.generate_key().decode())

    with pytest.raises(credentials_svc.CredentialsKeyMismatchError) as exc:
        credentials_svc.save_credential("new-b", "generic", SECRET_B)
    assert exc.value.reason == "mismatch"
    # 负控判据：错 key 保存必须不动导出（旧实现会重写成混合密钥）
    assert env.creds.read_bytes() == raw_before
    assert credentials_svc.get_credential_record("new-b") is None
    with pytest.raises(credentials_svc.CredentialsKeyMismatchError):
        credentials_svc.get_credential_record("old-a")

    summary = credentials_svc.reconcile_exports()
    assert summary["key_mismatch"] == 1
    assert summary["mismatched_tenants"] == ["default"]
    assert summary["refreshed"] == 0
    assert env.creds.read_bytes() == raw_before

    # 恢复原 key 后写入/读取都回到正常，导出头重新指向 A。
    env.key_file.write_text(env.key_a)
    credentials_svc.save_credential("new-b", "generic", SECRET_B)
    assert credentials_svc.get_credential_record("old-a")["data"] == SECRET_A
    assert credentials_svc.get_credential_record("new-b")["data"] == SECRET_B
    data = _export(env)
    assert data[STORE_META_KEY]["key_id"] == _key_id(env.key_a)
    assert data["old-a"]["type"] == "generic"
    assert data["new-b"]["type"] == "generic"


def test_missing_key_save_rejected_and_export_preserved(env):
    raw_before = env.creds.read_bytes()
    env.key_file.unlink()

    with pytest.raises(credentials_svc.CredentialsKeyMismatchError) as exc:
        credentials_svc.save_credential("new-b", "generic", SECRET_B)
    assert exc.value.reason == "missing"
    assert env.creds.read_bytes() == raw_before

    with pytest.raises(credentials_svc.CredentialsDisabledError):
        credentials_svc.get_credential_record("old-a")

    summary = credentials_svc.reconcile_exports()
    assert summary["key_mismatch"] == 1
    assert summary["rebuilt"] == 0
    assert summary["refreshed"] == 0
    assert env.creds.read_bytes() == raw_before


def test_api_wrong_key_is_named_json_not_500(env):
    env.key_file.write_text(Fernet.generate_key().decode())
    raw_before = env.creds.read_bytes()

    r = env.client.get("/api/credentials/old-a")
    assert r.status_code == 503, r.text
    assert r.headers["content-type"].startswith("application/json")
    detail = r.json()["detail"]
    assert detail["error"] == "credentials_key_mismatch"
    assert detail["reason"] == "mismatch"

    r = env.client.post(
        "/api/credentials",
        json={"name": "new-b", "type": "generic", "data": SECRET_B},
    )
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "credentials_key_mismatch"
    assert detail["reason"] == "mismatch"
    assert env.creds.read_bytes() == raw_before


def test_api_missing_key_detail_is_503(env):
    env.key_file.unlink()
    r = env.client.get("/api/credentials/old-a")
    assert r.status_code == 503, r.text
    assert r.json()["detail"]["error"] == "credentials_disabled"

    r = env.client.post(
        "/api/credentials",
        json={"name": "new-b", "type": "generic", "data": SECRET_B},
    )
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "credentials_key_mismatch"
    assert detail["reason"] == "missing"


def test_delete_keeps_header_when_key_is_correct(env):
    credentials_svc.save_credential("old-b", "generic", SECRET_B)
    assert credentials_svc.delete_credential("old-b") is True
    data = _export(env)
    assert data[STORE_META_KEY]["key_id"] == _key_id(env.key_a)
    assert "old-b" not in data and "old-a" in data


def test_delete_under_wrong_key_is_recovery_path_without_false_header(env):
    credentials_svc.save_credential("old-b", "generic", SECRET_B)
    env.key_file.write_text(Fernet.generate_key().decode())

    # 错 key 仍可撤销条目（恢复入口）；但导出文件不得声明当前 key 的 id。
    assert credentials_svc.delete_credential("old-a") is True
    data = _export(env)
    assert STORE_META_KEY not in data
    assert "old-a" not in data
    assert "old-b" in data


def test_engine_ignores_meta_and_reads_legacy_headerless_file(env):
    from plaita.credentials import CredentialError, credential_type, get_credential

    assert get_credential("old-a") == SECRET_A
    assert credential_type("old-a") == "generic"
    with pytest.raises(CredentialError) as exc:
        get_credential(STORE_META_KEY)
    # 保留键被过滤：错误信息里「当前可用」只列真实凭据名，不把内部键
    # 当成一条凭据列出来（请求名本身会被回显，那是正常错误文案）。
    assert "当前租户可用: old-a" in str(exc.value)

    # 旧版本导出的无头文件仍可读；下一次保存自动补上版本头。
    legacy = _export(env)
    legacy.pop(STORE_META_KEY)
    env.creds.write_text(json.dumps(legacy, ensure_ascii=False, indent=1))
    assert get_credential("old-a") == SECRET_A

    credentials_svc.save_credential("new-b", "generic", SECRET_B)
    data = _export(env)
    assert data[STORE_META_KEY]["key_id"] == _key_id(env.key_a)
    assert get_credential("new-b") == SECRET_B
