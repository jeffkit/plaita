"""plaita#223：凭据导出必须原子替换——写到一半失败不得毁掉旧文件。

负控手法：在写临时文件的 ``write`` 中途 / ``fsync`` / ``replace`` 三点注入
``OSError``，断言 (1) 旧导出文件逐字节不变，(2) 引擎侧仍能解析旧凭据，
(3) 失败路径不残留临时文件。旧实现直接 ``path.write_text`` 会被第一处注入
定位到目标文件本身（截断后失败），因此这些断言在旧实现上必红。
"""
import inspect
import json
import os
import stat
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import credentials_svc, flow_store  # noqa: E402

OLD_URL = "https://old.example/hook"
NEW_URL = "https://new.example/hook"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "cred.db"
    creds = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(creds))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    flow_store.init_engine(f"sqlite:///{db_file}")
    # plaita#257：save_credential 拒绝不存在的租户；default 由 bootstrap 建行。
    flow_store.ensure_tenant_bootstrap()
    credentials_svc.save_credential("feishu", "webhook", {"url": OLD_URL})
    credentials_svc.save_credential("other", "generic", {"k": "v"})
    return creds


def _engine_credential(name: str):
    from plaita.credentials import get_credential

    return get_credential(name)


class _ExplodingWriter:
    """模拟写到一半 ENOSPC：写一半后抛错，绝不触碰目标文件。"""

    def __init__(self, fh):
        self._fh = fh

    def __enter__(self):
        self._fh.__enter__()
        return self

    def __exit__(self, *exc):
        return self._fh.__exit__(*exc)

    def write(self, text):
        self._fh.write(text[: max(1, len(text) // 2)])
        self._fh.flush()
        raise OSError(28, "No space left on device")

    def flush(self):
        self._fh.flush()

    def fileno(self):
        return self._fh.fileno()


def _assert_failed_export_preserved(creds: Path, before: bytes) -> None:
    assert creds.read_bytes() == before, "失败的导出改动了上一份完好文件"
    assert list(creds.parent.glob(f".{creds.name}.*.tmp")) == [], "失败路径残留临时文件"
    assert _engine_credential("feishu")["url"] == OLD_URL
    assert _engine_credential("other")["k"] == "v"


def test_successful_export_is_atomic_and_0600(env):
    raw = env.read_text(encoding="utf-8")
    assert json.loads(raw)["feishu"]["type"] == "webhook"
    assert stat.S_IMODE(env.stat().st_mode) == 0o600


def _hits_atomic_writer() -> bool:
    """注入点必须在原子写 helper 内命中，防止异常来自 DB 阶段造成假绿。"""
    return any(frame.function == "_atomic_write_text" for frame in inspect.stack())


def test_partial_temp_write_keeps_previous_file_intact(env, monkeypatch):
    before = env.read_bytes()
    real_fdopen = os.fdopen
    hits = []

    def exploding_fdopen(fd, *args, **kwargs):
        assert _hits_atomic_writer(), "os.fdopen 注入未命中原子写路径"
        hits.append(True)
        return _ExplodingWriter(real_fdopen(fd, *args, **kwargs))

    monkeypatch.setattr(os, "fdopen", exploding_fdopen)

    with pytest.raises(OSError, match="No space left on device"):
        credentials_svc.save_credential("feishu", "webhook", {"url": NEW_URL})

    assert hits
    _assert_failed_export_preserved(env, before)
    # 读侧不静默：旧文件完好 ⇒ 正常解析出旧值，而不是报「不存在」
    assert _engine_credential("feishu")["url"] == OLD_URL


def test_fsync_failure_keeps_previous_file_intact(env, monkeypatch):
    before = env.read_bytes()
    hits = []

    def boom(fd):
        assert _hits_atomic_writer(), "os.fsync 注入未命中原子写路径"
        hits.append(True)
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "fsync", boom)

    with pytest.raises(OSError, match="No space left on device"):
        credentials_svc.save_credential("feishu", "webhook", {"url": NEW_URL})

    assert hits
    _assert_failed_export_preserved(env, before)


def test_replace_failure_keeps_previous_file_intact(env, monkeypatch):
    before = env.read_bytes()
    hits = []

    def boom(src, dst):
        assert _hits_atomic_writer(), "os.replace 注入未命中原子写路径"
        hits.append(True)
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(os, "replace", boom)

    with pytest.raises(OSError, match="Input/output error"):
        credentials_svc.save_credential("feishu", "webhook", {"url": NEW_URL})

    assert hits
    _assert_failed_export_preserved(env, before)


def test_delete_last_credential_still_exports_empty_store(env):
    credentials_svc.delete_credential("feishu")
    credentials_svc.delete_credential("other")
    assert json.loads(env.read_text(encoding="utf-8")) == {}
    assert stat.S_IMODE(env.stat().st_mode) == 0o600
