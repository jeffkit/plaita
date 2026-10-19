"""A2R25-08/09：备份敏感面——SQLite 0600 + 凭据文件/默认库进 .gitignore。

A2R25-08：flow_store 库里还有 `local_executions.input_json/context_json`
（执行 params/$INPUT 明文）与 `tenants.contract_secret_key`（明文 HMAC key）。
SQLite 默认 0644 会被同机其他 UID 直接读走；`init_engine` 现在在首次连接
前建出/收紧为 0600（含存量库 0644 → 0600）。

A2R25-09：`.plaita-credentials.json`（密文）与 `.plaita-credentials.key`
（Fernet key）默认落 DB 同目录 = 仓库根；`git add -A` 会把两者一起 staged。
`.gitignore` 与 `.dockerignore` 同口径排除。

负控：去掉 `init_engine` 的 `_secure_sqlite_db_permissions` 调用 → 新库 0644
（umask 022 下）红；删除 `.gitignore` 两行 → `git check-ignore` rc=1 红。
"""
from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[4]


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
