"""A67-04/A67-05 残面（console 侧）：回退告警 + 密钥 0600 创建/对账收紧。

A67-04：console 的 ``_db_dir`` 与引擎各自实现「SQLite ⇒ DB 目录，否则 cwd」，
未设置 DB URL / PG / 相对 URL 时静默回退 cwd。修复契约：console 委托引擎
``_default_credentials_dir``（单源），启动对账/首次路径解析必须留下 WARNING，
显式 ``PLAITA_CREDENTIALS_FILE``/``KEY_FILE`` 不触发解析告警。

A67-05：密钥文件先 ``write_bytes``（通常 0644）再 ``chmod 0600``，窗口内可读
44B Fernet key；存量 0644 key 启动对账不收紧。修复契约：``os.open(O_EXCL,
0o600)`` 创建（不再有宽权限窗口），并发落败者复用胜者 key；启动对账把存量
key 收紧到 0600 并在摘要里报 ``key_permission_tightened``。字段加密
（``secure_fields``，与凭据共用同一 key 文件）同样覆盖相对 DB URL/相对显式
key 路径告警、O_EXCL 0600 创建与落败者复用前收紧。

负控形态：
- 去掉回退 WARNING → 前三个用例的 caplog 断言红；
- 还原 write_bytes + chmod → O_EXCL/chmod 调用断言红（KeyError/断言失败）；
- 去掉对账收紧 → 0644 用例的 mode/摘要断言红。
"""
from __future__ import annotations

import logging
import os
import stat
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

import plaita.credentials as credentials
import secure_fields
from services import credentials_svc, flow_store


@pytest.fixture(autouse=True)
def _reset_warning_state():
    credentials._reset_path_fallback_warnings()
    credentials._KEY_PERMISSION_WARNINGS.clear()
    secure_fields.reset_for_tests()
    yield
    credentials._reset_path_fallback_warnings()
    credentials._KEY_PERMISSION_WARNINGS.clear()
    secure_fields.reset_for_tests()


def _messages(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "plaita.credentials"
    ]


def _init_sqlite(tmp_path, monkeypatch, *, db_url: str | None = None) -> Path:
    """初始化真实 SQLite console store；DB URL env 可故意指向别的后端。"""
    db = tmp_path / "console.db"
    if db_url is not None:
        monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", db_url)
    else:
        monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db}")
    flow_store.init_engine(f"sqlite:///{db}")
    flow_store.ensure_tenant_bootstrap()
    return db


# -- A67-04：console 路径解析的回退告警 ---------------------------------------


def test_console_key_path_missing_db_env_warns(tmp_path, monkeypatch, caplog):
    for name in (
        "PLAITA_CONSOLE_DB_URL",
        "PLAITA_CREDENTIALS_FILE",
        "PLAITA_CREDENTIALS_KEY_FILE",
        "PLAITA_CREDENTIALS_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    credentials._reset_path_fallback_warnings()
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_svc.credentials_key_file() == (
        tmp_path / ".plaita-credentials.key"
    )
    messages = _messages(caplog)
    assert any("默认回退到当前工作目录" in message for message in messages), messages
    assert any(str(tmp_path) in message for message in messages)


def test_console_relative_sqlite_url_warns(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", "sqlite:///./relative.db")
    monkeypatch.delenv("PLAITA_CREDENTIALS_FILE", raising=False)
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY_FILE", raising=False)
    monkeypatch.chdir(tmp_path)
    credentials._reset_path_fallback_warnings()
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_svc.credentials_key_file() == (
        tmp_path / ".plaita-credentials.key"
    )
    messages = _messages(caplog)
    assert any("相对路径" in message for message in messages), messages


def test_console_explicit_relative_key_file_warns(tmp_path, monkeypatch, caplog):
    """显式相对 PLAITA_CREDENTIALS_KEY_FILE 也按各进程 cwd 解析（A67-04）。"""
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", "rel/creds.key")
    monkeypatch.chdir(tmp_path)
    credentials._reset_path_fallback_warnings()
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_svc.credentials_key_file() == Path("rel/creds.key")
    messages = _messages(caplog)
    assert any("是相对路径" in message for message in messages), messages


def test_secure_fields_relative_sqlite_url_warns(tmp_path, monkeypatch, caplog):
    """字段加密密钥的解析不能绕过 A67-04 的相对 DB URL 告警。"""
    for name in (
        "PLAITA_CONSOLE_DB_URL",
        "PLAITA_CREDENTIALS_FILE",
        "PLAITA_CREDENTIALS_KEY_FILE",
        "PLAITA_CREDENTIALS_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    secure_fields.reset_for_tests()
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    secure_fields.configure("sqlite:///./relative.db")
    key = secure_fields._load_master_key(generate=True)

    assert len(key) == 32  # base64 解码后的主密钥
    assert len((tmp_path / ".plaita-credentials.key").read_bytes()) == 44
    messages = _messages(caplog)
    assert any(
        "相对路径" in message and "relative.db" in message for message in messages
    ), messages


def test_secure_fields_explicit_relative_key_file_warns(tmp_path, monkeypatch, caplog):
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", "rel/field.key")
    monkeypatch.chdir(tmp_path)
    secure_fields.reset_for_tests()
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    secure_fields.configure(None)

    assert secure_fields._state.key_source == ("file", Path("rel/field.key"))
    messages = _messages(caplog)
    assert any("是相对路径" in message for message in messages), messages


def test_console_reconcile_warns_on_non_sqlite_url(tmp_path, monkeypatch, caplog):
    """PG URL 无法推断共享目录：启动对账必须告警，不阻断。"""
    monkeypatch.delenv("PLAITA_CREDENTIALS_FILE", raising=False)
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY_FILE", raising=False)
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.chdir(tmp_path)
    _init_sqlite(tmp_path, monkeypatch, db_url="postgresql://u:p@db/plaita")
    credentials._reset_path_fallback_warnings()
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    summary = credentials_svc.reconcile_exports()

    messages = _messages(caplog)
    assert any("不是 sqlite" in message for message in messages), messages
    assert summary["key_permission_tightened"] is False


# -- A67-05：密钥创建即 0600 --------------------------------------------------


def test_key_created_with_o_excl_0600_without_chmod_window(tmp_path, monkeypatch):
    key_file = tmp_path / "new.key"
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(key_file))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)

    real_open = os.open
    open_calls: list[tuple[str, int, int]] = []
    chmod_calls: list[tuple[str, int]] = []

    def spy_open(path, flags, mode=0o777):
        open_calls.append((str(path), flags, mode))
        return real_open(path, flags, mode)

    def spy_chmod(path, mode):
        chmod_calls.append((str(path), mode))

    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "chmod", spy_chmod)

    key = credentials_svc._load_key_bytes(generate=True)

    assert key_file.read_bytes() == key
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    key_calls = [call for call in open_calls if call[0] == str(key_file)]
    assert len(key_calls) == 1, key_calls
    _, flags, mode = key_calls[0]
    assert flags & os.O_CREAT and flags & os.O_EXCL and flags & os.O_WRONLY
    assert mode == 0o600
    # 负控：旧实现 write_bytes 后补 chmod，会在这里留下 chmod 记录/0644 窗口
    assert chmod_calls == [], chmod_calls


def test_key_created_0600_even_with_umask_000_and_chmod_denied(tmp_path, monkeypatch):
    """创建权限不依赖 umask 或后续 chmod：最坏 umask + chmod 拒绝仍是 0600。

    负控（还原 write_bytes + chmod 旧实现）：umask=000 下 write_bytes 先落
    0666，chmod 被拒绝后文件保持 0666——本条与 chmod 窗口用例同时红。
    """
    key_file = tmp_path / "new.key"
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(key_file))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)

    def deny_chmod(path, mode):
        raise OSError("chmod denied")

    monkeypatch.setattr(os, "chmod", deny_chmod)
    previous_umask = os.umask(0o000)
    try:
        key = credentials_svc._load_key_bytes(generate=True)
    finally:
        os.umask(previous_umask)

    assert key_file.read_bytes() == key
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600


def test_key_creation_loser_reuses_winner_key(tmp_path, monkeypatch):
    """O_EXCL 落败者不得重新生成 key，并收紧胜者遗留的宽权限文件（A67-05）。"""
    key_file = tmp_path / "race.key"
    winner = Fernet.generate_key()
    key_file.write_bytes(winner)
    # 滚动升级：旧版本可能刚以 0644 创建了胜者文件；落败者复用前须收紧。
    key_file.chmod(0o644)

    real_open = os.open

    def racing_open(path, flags, mode=0o777):
        if str(path) == str(key_file):
            raise FileExistsError
        return real_open(path, flags, mode)

    monkeypatch.setattr(os, "open", racing_open)

    assert credentials_svc._create_key_file(key_file) == winner
    assert key_file.read_bytes() == winner
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600


def test_secure_fields_key_created_o_excl_0600(tmp_path, monkeypatch):
    """字段加密密钥与凭据密钥同一创建口径：无 0644→chmod 窗口。"""
    key_file = tmp_path / "field.key"

    real_open = os.open
    open_calls: list[tuple[str, int, int]] = []
    chmod_calls: list[tuple[str, int]] = []

    def spy_open(path, flags, mode=0o777):
        open_calls.append((str(path), flags, mode))
        return real_open(path, flags, mode)

    def spy_chmod(path, mode):
        chmod_calls.append((str(path), mode))

    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "chmod", spy_chmod)

    key = secure_fields._create_key_file(key_file)

    assert key_file.read_bytes() == key
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    key_calls = [call for call in open_calls if call[0] == str(key_file)]
    assert len(key_calls) == 1, key_calls
    _, flags, mode = key_calls[0]
    assert flags & os.O_CREAT and flags & os.O_EXCL and flags & os.O_WRONLY
    assert mode == 0o600
    assert chmod_calls == [], chmod_calls


def test_secure_fields_key_creation_loser_tightens_0644(
    tmp_path, monkeypatch, caplog
):
    """O_EXCL 落败者复用旧版本以 0644 建好的胜者 key 前必须收紧（A67-05）。"""
    key_file = tmp_path / "field.key"
    winner = Fernet.generate_key()
    key_file.write_bytes(winner)
    key_file.chmod(0o644)

    real_open = os.open

    def racing_open(path, flags, mode=0o777):
        if str(path) == str(key_file):
            raise FileExistsError
        return real_open(path, flags, mode)

    monkeypatch.setattr(os, "open", racing_open)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert secure_fields._create_key_file(key_file) == winner
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    assert any("宽于 0600" in message for message in _messages(caplog))


def test_reconcile_tightens_existing_0644_key(tmp_path, monkeypatch, caplog):
    key_file = tmp_path / "creds.key"
    creds_file = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(creds_file))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(key_file))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    _init_sqlite(tmp_path, monkeypatch)

    credentials_svc.save_credential("cred-a", "api", {"token": "SECRET-A"})
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    key_file.chmod(0o644)
    credentials._reset_path_fallback_warnings()
    credentials._KEY_PERMISSION_WARNINGS.clear()
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    summary = credentials_svc.reconcile_exports()

    assert summary["key_permission_tightened"] is True
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    assert any("宽于 0600" in message for message in _messages(caplog))
