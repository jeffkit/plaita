"""plaita#223：凭据读侧对空/残文件具名报错，不静默当成「无凭据」。

文件缺失 = 合法的「尚未配置」（空 store）；文件存在却为空/截断/非对象
= 导出被中断或损坏，必须 fail-loud——否则所有凭据表现为「不存在」，
把排查方向引向重新创建凭据，掩盖真正的写失败。
"""
import os
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from plaita.credentials import (
    CredentialError,
    CredentialStoreCorruptError,
    credential_type,
    get_credential,
)

TRUNCATED = '{\n "feishu": {\n  "type": "webhook",\n  "data": "gAAAAA'


@pytest.fixture()
def store(tmp_path, monkeypatch) -> Path:
    path = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(path))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY", Fernet.generate_key().decode())
    return path


def test_missing_file_is_empty_store_not_corruption(store):
    with pytest.raises(CredentialError) as exc:
        get_credential("anything")
    assert not isinstance(exc.value, CredentialStoreCorruptError)
    assert "不存在" in str(exc.value)


def test_valid_empty_store_is_not_corruption(store):
    store.write_text("{}", encoding="utf-8")
    with pytest.raises(CredentialError) as exc:
        get_credential("anything")
    assert not isinstance(exc.value, CredentialStoreCorruptError)
    assert "不存在" in str(exc.value)


def test_empty_file_raises_named_error(store):
    store.write_text("", encoding="utf-8")
    with pytest.raises(CredentialStoreCorruptError) as exc:
        get_credential("feishu")
    message = str(exc.value)
    assert "为空" in message
    assert str(store) in message
    assert "重新保存任意一条凭据" in message
    # 不得再退化成「凭据不存在」的误导文案
    assert "当前租户可用" not in message


def test_whitespace_only_file_raises_named_error(store):
    store.write_text(" \n\t\n", encoding="utf-8")
    with pytest.raises(CredentialStoreCorruptError, match="为空"):
        get_credential("feishu")


def test_truncated_json_raises_named_error(store):
    store.write_text(TRUNCATED, encoding="utf-8")
    with pytest.raises(CredentialStoreCorruptError) as exc:
        get_credential("feishu")
    message = str(exc.value)
    assert "不是合法 JSON" in message
    assert str(store) in message


@pytest.mark.parametrize("payload", ["[]", "null", '"text"', "123"])
def test_non_object_json_raises_named_error(store, payload):
    store.write_text(payload, encoding="utf-8")
    with pytest.raises(CredentialStoreCorruptError, match="顶层必须是对象"):
        get_credential("feishu")


def test_credential_type_fails_loud_on_corruption(store):
    store.write_text(TRUNCATED, encoding="utf-8")
    with pytest.raises(CredentialStoreCorruptError):
        credential_type("feishu")


def test_corruption_error_is_credential_error_subclass(store):
    store.write_text("", encoding="utf-8")
    with pytest.raises(CredentialError):
        get_credential("feishu")


@pytest.mark.skipif(os.geteuid() == 0, reason="root 无视文件权限，无法构造 OSError")
def test_unreadable_file_raises_named_error(store):
    store.write_text("{}", encoding="utf-8")
    store.chmod(0)
    try:
        with pytest.raises(CredentialStoreCorruptError, match="无法读取"):
            get_credential("feishu")
    finally:
        store.chmod(0o600)
