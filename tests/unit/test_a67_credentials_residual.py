"""A67-04/A67-05 残面：回退 cwd 必须可观测 + 密钥权限创建即 0600。

A67-04（P2）：未显式配置 ``PLAITA_CONSOLE_DB_URL``/``PLAITA_CREDENTIALS_FILE``
时，引擎按 cwd 解析凭据文件且**无任何告警**。升级后 worker 忘记注入 DB env
会继续解出旧 cwd 文件里的已吊销/已轮换值，运维却看不到分叉。修复契约：
回退/相对路径/非 SQLite/内存库都必须留下 WARNING（不阻断运行），同类告警
按原因去重。

A67-05（P3）：密钥文件历史实现先 ``write_bytes``（umask 下 0644）再 chmod，
窗口内可读 44B Fernet key；已存在的 0644 key 读取/对账都不收紧。修复契约：
创建即 0600（console 侧 O_EXCL，见 console 用例），引擎读取存量 key 时
收紧到 0600 + WARNING。

负控形态：
- 移除回退告警 → ``test_worker_without_db_env_*`` 的 stderr 断言红；
- 移除读取时收紧 → 0644 用例红。
"""
from __future__ import annotations

import json
import logging
import os
import stat
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

import plaita.credentials as credentials
from plaita.credentials import (
    DEFAULT_FILE,
    DEFAULT_KEY_FILE,
    _load_key,
    _reset_path_fallback_warnings,
    credentials_base_file,
    credentials_key_file,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = sys.executable


@pytest.fixture(autouse=True)
def _reset_warning_state():
    _reset_path_fallback_warnings()
    credentials._KEY_PERMISSION_WARNINGS.clear()
    yield
    _reset_path_fallback_warnings()
    credentials._KEY_PERMISSION_WARNINGS.clear()


def _clean_env(monkeypatch) -> None:
    for name in (
        "PLAITA_CONSOLE_DB_URL",
        "PLAITA_CREDENTIALS_FILE",
        "PLAITA_CREDENTIALS_KEY_FILE",
        "PLAITA_CREDENTIALS_KEY",
    ):
        monkeypatch.delenv(name, raising=False)


def _messages(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "plaita.credentials"
    ]


# -- A67-04：回退路径必须可观测 ------------------------------------------------


def test_missing_db_env_warns_once_per_component(tmp_path, monkeypatch, caplog):
    _clean_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_base_file() == tmp_path / DEFAULT_FILE
    assert credentials_key_file() == tmp_path / DEFAULT_KEY_FILE

    messages = _messages(caplog)
    assert any(
        "默认回退到当前工作目录" in message and str(tmp_path) in message
        for message in messages
    ), messages
    assert any("PLAITA_CREDENTIALS_FILE" in message for message in messages)
    # 文件/密钥两个组件各告警一次；重复解析不再刷日志
    count = len(messages)
    assert count == 2, messages
    credentials_base_file()
    credentials_key_file()
    assert len(_messages(caplog)) == count


def test_relative_sqlite_url_warns(tmp_path, monkeypatch, caplog):
    _clean_env(monkeypatch)
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", "sqlite:///./relative.db")
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_base_file() == tmp_path / DEFAULT_FILE
    messages = _messages(caplog)
    assert any("相对路径" in message for message in messages), messages
    assert any(str(tmp_path) in message for message in messages)


def test_non_sqlite_url_warns_and_falls_back_to_cwd(tmp_path, monkeypatch, caplog):
    _clean_env(monkeypatch)
    monkeypatch.setenv(
        "PLAITA_CONSOLE_DB_URL", "postgresql://u:SUPER-SECRET@db/plaita"
    )
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_base_file() == tmp_path / DEFAULT_FILE
    messages = _messages(caplog)
    assert any("不是 sqlite" in message for message in messages), messages
    # 回退告警会进启动日志/采集：不得把 DB URL 的 userinfo 口令写出来
    assert all("SUPER-SECRET" not in message for message in messages), messages


def test_memory_sqlite_warns(tmp_path, monkeypatch, caplog):
    _clean_env(monkeypatch)
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", "sqlite:///:memory:")
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_base_file() == tmp_path / DEFAULT_FILE
    assert any("内存 SQLite" in message for message in _messages(caplog))


def test_explicit_file_env_suppresses_fallback_warning(tmp_path, monkeypatch, caplog):
    _clean_env(monkeypatch)
    explicit = tmp_path / "explicit.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(explicit))
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert credentials_base_file() == explicit
    assert _messages(caplog) == []


def test_worker_without_db_env_warns_but_still_reads_old_cwd_file(tmp_path):
    """升级形态：无 DB env 的 worker 仍读旧 cwd 文件，但 stderr 必须告警。"""
    key = Fernet.generate_key()
    token = (
        Fernet(key)
        .encrypt(json.dumps({"token": "OLD-SECRET"}).encode())
        .decode()
    )
    (tmp_path / DEFAULT_FILE).write_text(
        json.dumps({"cred-old": {"type": "api", "data": token}}), encoding="utf-8"
    )
    (tmp_path / DEFAULT_KEY_FILE).write_bytes(key)

    env = {k: v for k, v in os.environ.items() if not k.startswith("PLAITA_")}
    env["PYTHONPATH"] = str(REPO_ROOT)
    code = textwrap.dedent(
        """
        import json
        from plaita.credentials import get_credential
        print(json.dumps(get_credential("cred-old")))
        """
    )
    result = subprocess.run(
        [PYTHON, "-c", code],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    last = [line for line in result.stdout.splitlines() if line.strip()][-1]
    # 告警是新增可观测面，不改变「文件缺失视为未配置/旧文件仍可读」的语义
    assert json.loads(last) == {"token": "OLD-SECRET"}
    assert "默认回退到当前工作目录" in result.stderr
    assert str(tmp_path) in result.stderr


# -- A67-05：存量密钥权限收紧 ------------------------------------------------


def test_existing_0644_key_tightened_on_engine_read(tmp_path, monkeypatch, caplog):
    key_file = tmp_path / "creds.key"
    key = Fernet.generate_key()
    key_file.write_bytes(key)
    key_file.chmod(0o644)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(key_file))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert _load_key() == key
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    assert any("宽于 0600" in message for message in _messages(caplog))


def test_key_tighten_failure_warns_scrubbed_and_still_reads(
    tmp_path, monkeypatch, caplog
):
    """只读挂载等 chmod 失败：不阻断解密，WARNING 走 #151 掩码（不传裸异常）。"""
    key_file = tmp_path / "creds.key"
    key = Fernet.generate_key()
    key_file.write_bytes(key)
    key_file.chmod(0o644)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(key_file))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)

    def deny_chmod(path, mode):
        raise OSError("chmod denied")

    monkeypatch.setattr(os, "chmod", deny_chmod)
    caplog.set_level(logging.WARNING, logger="plaita.credentials")

    assert _load_key() == key
    assert stat.S_IMODE(key_file.stat().st_mode) == 0o644
    messages = _messages(caplog)
    assert any("收紧失败" in message for message in messages), messages
