"""A2R25-08/09：备份敏感面——SQLite 0600 + 凭据文件/默认库进 .gitignore
+ `local_executions.input_json/context_json` 与 `tenants.contract_secret_key`
字段级加密（余量）。

A2R25-08：flow_store 库里还有 `local_executions.input_json/context_json`
（执行 params/$INPUT 明文）与 `tenants.contract_secret_key`（明文 HMAC key）。
SQLite 默认 0644 会被同机其他 UID 直接读走；`init_engine` 现在在首次连接
前建出/收紧为 0600（含存量库 0644 → 0600）。

A2R25-08 余量（本轮）：0600 只挡同机 UID，挡不住备份/`shutil.copy2`/归档。
三列现在经 `secure_fields.EncryptedText` 落盘即 `enc:v1:` 密文（主密钥与
凭据加密同源、按用途 HKDF 派生），ORM 读取透明解密；存量明文在
`init_engine` 回填并 VACUUM。裸 SQL/直接扫 DB 文件零命中。

A2R25-09：`.plaita-credentials.json`（密文）与 `.plaita-credentials.key`
（Fernet key）默认落 DB 同目录 = 仓库根；`git add -A` 会把两者一起 staged。
`.gitignore` 与 `.dockerignore` 同口径排除。

负控：
- 去掉 `init_engine` 的 `_secure_sqlite_db_permissions` 调用 → 新库 0644
  （umask 022 下）红；删除 `.gitignore` 两行 → `git check-ignore` rc=1 红；
- 把三列的 `EncryptedText` 换回 `Text` → 裸读/备份扫描用例红；
- 去掉 `backfill_sensitive_columns` 调用 → 存量明文回填用例红；
- 用错密钥重开库 → 解密必须 fail-loud（不能静默返回 None/明文）。
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import text

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from secure_fields import (  # noqa: E402
    ENCRYPTED_PREFIX,
    FieldCryptoError,
    decrypt_value,
    encrypt_value,
)
from services import flow_store, signature, tenants_svc  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[4]

SECRET_PARAM = "sk-PLAINTEXT-PARAM-A2R25"
SECRET_CTX = "PLAINTEXT-PW-A2R25"


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_new_sqlite_db_is_0600(tmp_path):
    db_file = tmp_path / "console.db"
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    assert db_file.is_file()
    assert _mode(db_file) == 0o600


def test_existing_sqlite_db_is_tightened(tmp_path):
    db_file = tmp_path / "legacy.db"
    db_file.write_bytes(b"")
    os.chmod(db_file, 0o644)
    assert _mode(db_file) == 0o644

    flow_store.init_engine(f"sqlite:///{db_file}")
    assert _mode(db_file) == 0o600


def test_sqlite_memory_is_left_alone():
    # 不应尝试为 :memory: 建文件或报错。
    flow_store.init_engine("sqlite:///:memory:")
    flow_store.ensure_tenant_bootstrap()
    flow_store.get_flow_store().list_flows()


def _git_check_ignore(path: str) -> int:
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), "check-ignore", "-q", "--no-index", path],
        capture_output=True,
        text=True,
        check=False,
    ).returncode


@pytest.mark.parametrize(
    "path",
    [
        ".plaita-credentials.json",
        ".plaita-credentials.key",
        ".plaita-credentials.tenant-a.json",
        "plaita_console.db",
        "subdir/.plaita-credentials.key",
    ],
)
def test_gitignore_covers_credentials_and_default_db(path):
    if not (REPO_ROOT / ".git").exists():
        pytest.skip("不在 git 工作树内（安装包/归档目录），无法验证 .gitignore")
    assert _git_check_ignore(path) == 0, f"{path} 未被 .gitignore 忽略"


# ============ A2R25-08/09 余量：字段级加密（input/context/contract key） ============


@pytest.fixture()
def encrypted_db(tmp_path, monkeypatch):
    """文件库 + 显式密钥文件（避免用例碰 cwd 下的真实凭据 key）。"""
    db_file = tmp_path / "console.db"
    key_file = tmp_path / "field.key"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(key_file))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    return {"db": db_file, "key": key_file, "url": f"sqlite:///{db_file}"}


def _raw_local(execution_id: str) -> tuple:
    with flow_store._engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT input_json, context_json FROM local_executions "
                "WHERE execution_id = :eid"
            ),
            {"eid": execution_id},
        ).one()
    return tuple(row)


def _seed_sensitive_execution(execution_id: str = "enc-e1") -> None:
    flow_store.insert_local_execution(
        execution_id,
        "flow-1",
        "1.0.0",
        status="completed",
        input_json=json.dumps({"api_key": SECRET_PARAM}, ensure_ascii=False),
    )
    flow_store.update_local_execution(
        execution_id,
        context_json=json.dumps(
            {"$INPUT": {"api_key": SECRET_PARAM, "password": SECRET_CTX}},
            ensure_ascii=False,
        ),
    )


def test_local_execution_sensitive_columns_are_ciphertext(encrypted_db):
    """① 裸 SQL / 直接扫 DB 文件零命中；ORM 读取仍透明解密。"""
    _seed_sensitive_execution()

    input_json, context_json = _raw_local("enc-e1")
    for label, value in (("input_json", input_json), ("context_json", context_json)):
        assert value.startswith(ENCRYPTED_PREFIX), f"{label} 未加密落盘"
        assert SECRET_PARAM not in value, f"{label} 裸读仍含明文 params"
        assert SECRET_CTX not in value, f"{label} 裸读仍含明文 $INPUT"

    raw_bytes = encrypted_db["db"].read_bytes()
    assert SECRET_PARAM.encode() not in raw_bytes, "DB 文件字节仍含明文 params"
    assert SECRET_CTX.encode() not in raw_bytes, "DB 文件字节仍含明文 $INPUT"

    # 功能面不受影响：读取侧解密回原对象（否则执行页/恢复全坏）。
    info = flow_store.get_local_execution("enc-e1")
    assert info["input"] == {"api_key": SECRET_PARAM}
    assert info["context"]["$INPUT"] == {
        "api_key": SECRET_PARAM,
        "password": SECRET_CTX,
    }

    # 密钥文件按既有凭据口径 0600；否则字段加密只是换了个可读文件。
    assert _mode(encrypted_db["key"]) == 0o600


def test_contract_secret_key_is_ciphertext_and_still_verifies(encrypted_db):
    """② contract_secret_key 裸读为密文；验签路径仍拿到明文并可用。"""
    store = flow_store.get_flow_store()
    created = tenants_svc.create_tenant(store, "acme", "Acme")
    secret = created["contract_secret_key"]
    assert secret

    with flow_store._engine.connect() as conn:
        raw = conn.execute(
            text("SELECT contract_secret_key FROM tenants WHERE id = 'acme'")
        ).scalar()
    assert raw.startswith(ENCRYPTED_PREFIX), "contract_secret_key 未加密落盘"
    assert secret not in raw
    # 存量 PG 列是 VARCHAR(128)（未迁移）；48 字符 secret 的 AES-GCM 密文
    # 是 111 字符，必须保持在 128 以内，否则升级后租户创建/轮换写入失败。
    assert len(raw) <= 128, f"密文长度 {len(raw)} 超出存量 VARCHAR(128)"
    assert secret.encode() not in encrypted_db["db"].read_bytes()

    found = tenants_svc.find_tenant_by_contract_secret_id(
        store, created["contract_secret_id"]
    )
    assert found["contract_secret_key"] == secret, "读取侧必须透明解密"

    from plaita.client import generate_signature

    authorization = generate_signature(
        secret, created["contract_secret_id"], 600, int(time.time())
    )
    assert signature.verify_authorization(
        authorization, created["contract_secret_id"], found["contract_secret_key"]
    )


def test_legacy_plaintext_rows_are_backfilled_on_init(encrypted_db):
    """升级窗口：存量明文在 init_engine 回填，裸读/文件扫描不再命中。"""
    with flow_store._engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO local_executions (tenant_id, execution_id, flow_id, "
                "flow_version, status, input_json, output_json, error_json, "
                "nodes_json, usage_json, context_json, invoker, start_time, "
                "last_update_time) VALUES ('default','legacy','flow-1','1.0.0',"
                "'completed', :inp, 'null', 'null', '[]', 'null', :ctx, 'local', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ),
            {
                "inp": json.dumps({"api_key": SECRET_PARAM}),
                "ctx": json.dumps({"$INPUT": {"password": SECRET_CTX}}),
            },
        )
        conn.execute(
            text(
                "INSERT INTO tenants (id, name, status, contract_secret_id, "
                "contract_secret_key, created_at) VALUES ('old','Old','active',"
                "'t-old', :key, CURRENT_TIMESTAMP)"
            ),
            {"key": "OLD-CONTRACT-SECRET"},
        )
    assert SECRET_PARAM.encode() in encrypted_db["db"].read_bytes()
    assert b"OLD-CONTRACT-SECRET" in encrypted_db["db"].read_bytes()

    flow_store._engine.dispose()
    flow_store.init_engine(encrypted_db["url"])  # 启动回填 + VACUUM

    input_json, context_json = _raw_local("legacy")
    assert input_json.startswith(ENCRYPTED_PREFIX)
    assert context_json.startswith(ENCRYPTED_PREFIX)
    with flow_store._engine.connect() as conn:
        raw_key = conn.execute(
            text("SELECT contract_secret_key FROM tenants WHERE id = 'old'")
        ).scalar()
    assert raw_key.startswith(ENCRYPTED_PREFIX)

    raw_bytes = encrypted_db["db"].read_bytes()
    assert SECRET_PARAM.encode() not in raw_bytes, "回填后旧页仍残留明文 params"
    assert SECRET_CTX.encode() not in raw_bytes, "回填后旧页仍残留明文 $INPUT"
    assert b"OLD-CONTRACT-SECRET" not in raw_bytes, "回填后旧页仍残留契约密钥"

    info = flow_store.get_local_execution("legacy")
    assert info["input"] == {"api_key": SECRET_PARAM}
    assert info["context"]["$INPUT"] == {"password": SECRET_CTX}


def test_wrong_key_fails_loud_instead_of_plaintext(encrypted_db, monkeypatch):
    """错密钥：裸读仍是密文，读取路径必须报错（不得静默 None/明文）。"""
    _seed_sensitive_execution("wrong-key-e1")
    flow_store._engine.dispose()

    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY", Fernet.generate_key().decode())
    flow_store.init_engine(encrypted_db["url"])

    raw_input, _ = _raw_local("wrong-key-e1")
    assert raw_input.startswith(ENCRYPTED_PREFIX)

    with pytest.raises(FieldCryptoError):
        flow_store.get_local_execution("wrong-key-e1")


def test_naive_copy2_backup_contains_no_plaintext(encrypted_db, tmp_path):
    """A2R25-08 的原始攻击面：`shutil.copy2` 朴素备份。

    备份保留 0600，且三列在备份里仍是密文（0600 是第二道闸，不是唯一防线）。
    """
    _seed_sensitive_execution("backup-e1")
    store = flow_store.get_flow_store()
    created = tenants_svc.create_tenant(store, "backup-tenant", "Backup")
    flow_store._engine.dispose()

    backup = tmp_path / "backup.db"
    shutil.copy2(encrypted_db["db"], backup)

    assert _mode(backup) == 0o600
    data = backup.read_bytes()
    assert SECRET_PARAM.encode() not in data
    assert SECRET_CTX.encode() not in data
    assert created["contract_secret_key"].encode() not in data


def test_field_keys_are_purpose_separated(encrypted_db):
    """同一主密钥、不同用途子密钥：跨列解密必须失败（HKDF info 分隔）。"""
    token = encrypt_value("cross-purpose", purpose="local-execution-input")
    assert token.startswith(ENCRYPTED_PREFIX)
    assert encrypt_value(token, purpose="local-execution-input") == token
    with pytest.raises(FieldCryptoError):
        decrypt_value(token, purpose="tenant-contract-secret")


def test_key_unavailable_falls_back_with_observable_warning(
    encrypted_db, tmp_path, monkeypatch, caplog
):
    """密钥无法建立时按明文降级，但必须有 WARNING（禁止静默降级）。"""
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x")
    monkeypatch.setenv(
        "PLAITA_CREDENTIALS_KEY_FILE", str(blocker / "field.key")
    )
    flow_store.init_engine(encrypted_db["url"])

    with caplog.at_level(logging.WARNING, logger="secure_fields"):
        flow_store.insert_local_execution(
            "fallback-e1",
            "flow-1",
            "1.0.0",
            input_json=json.dumps({"api_key": "PLAIN-FALLBACK"}),
        )

    raw_input, _ = _raw_local("fallback-e1")
    assert not raw_input.startswith(ENCRYPTED_PREFIX), "无密钥时无法加密，应走降级路径"
    assert "PLAIN-FALLBACK" in raw_input
    assert any(
        "字段加密密钥不可用" in rec.message and "落明文" in rec.message
        for rec in caplog.records
    ), "降级必须留下 WARNING（A2R25-08）"
