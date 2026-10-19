"""plaita#251：凭据 DB 与导出旁文件的顺序一致性。

缺陷（issue 正文，delete_credential）：先 ``session.commit()`` 删 DB 行，再
``_export_store``。导出失败时 DB 行已消失、旁文件仍是旧密文——引擎继续能
读到已删凭据，重试删除因 DB 无行直接返回 False，旁文件永远清不掉。

同一模式也存在于 ``save_credential``：先 commit 再导出，导出失败时 DB 已
有新行、旁文件仍是旧值（本单任务明确要求的「DB 不留新行」面）。

修复契约（plaita#251）：同一事务内先 ``flush`` 让导出声看到未提交变更，
**原子导出成功后**才 ``commit``；导出失败回滚事务，DB/旁文件都保持旧状；
commit 失败则按回滚后的 DB 状态补导一次，避免旁文件超前。

负控：恢复「commit 后直接 _export_store」的 DB-first 顺序，本文件
save/delete 用例必红（delete 精确红：``db row None but engine read
{'token': 'SECRET'}``）。
"""
import json
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import credentials_svc, flow_store  # noqa: E402

SECRET = {"token": "SECRET"}
NEW_SECRET = {"token": "NEW"}


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
    credentials_svc.save_credential("old", "generic", SECRET)
    return creds


def _engine(name: str):
    """引擎侧真实读取（旁文件）；不存在返回 None。"""
    from plaita.credentials import CredentialError, get_credential

    try:
        return get_credential(name)
    except CredentialError:
        return None


def _fail_export(monkeypatch):
    """在原子写入口注入 OSError；返回开关以便重试时恢复。"""
    real_write = credentials_svc._atomic_write_text
    state = {"fail": True}

    def maybe_fail(path, text):
        if state["fail"]:
            raise OSError("export failed")
        return real_write(path, text)

    monkeypatch.setattr(credentials_svc, "_atomic_write_text", maybe_fail)
    return state


class TestSaveOrder:
    def test_export_failure_leaves_no_new_db_row(self, env, monkeypatch):
        """本单任务判据：export 失败 ⇒ DB 不留新行，旁文件不含新凭据。"""
        state = _fail_export(monkeypatch)
        with pytest.raises(OSError, match="export failed"):
            credentials_svc.save_credential("new", "generic", NEW_SECRET)

        assert credentials_svc.get_credential_record("new") is None
        assert _engine("new") is None
        assert _engine("old") == SECRET
        assert "new" not in json.loads(env.read_text(encoding="utf-8"))
        assert state["fail"] is True  # 注入确实命中导出阶段

    def test_update_export_failure_keeps_old_row_and_file(self, env, monkeypatch):
        """更新路径同款：导出失败 ⇒ DB 回滚到旧值，旁文件保持旧值。"""
        _fail_export(monkeypatch)
        with pytest.raises(OSError, match="export failed"):
            credentials_svc.save_credential("old", "generic", NEW_SECRET)

        record = credentials_svc.get_credential_record("old")
        assert record is not None
        assert record["data"] == SECRET
        assert _engine("old") == SECRET
        assert json.loads(env.read_text(encoding="utf-8"))["old"]["type"] == "generic"


class TestDeleteOrder:
    def test_export_failure_keeps_db_row_and_engine_value(self, env, monkeypatch):
        """issue 正控：导出失败 ⇒ DB 行与旁文件都保留（可回滚重试），
        绝不出现 DB=None + 引擎仍可读旧凭据。"""
        _fail_export(monkeypatch)
        with pytest.raises(OSError, match="export failed"):
            credentials_svc.delete_credential("old")

        record = credentials_svc.get_credential_record("old")
        engine = _engine("old")
        # 负控精确红文案：db row None but engine read {'token': 'SECRET'}
        assert not (record is None and engine is not None), (
            f"db row {record} but engine read {engine}"
        )
        assert record is not None
        assert record["data"] == SECRET
        assert engine == SECRET

    def test_delete_can_be_retried_after_export_failure(self, env, monkeypatch):
        """导出失败后 DB 行还在 ⇒ 重试能真正清理 DB 与旁文件。"""
        state = _fail_export(monkeypatch)
        with pytest.raises(OSError, match="export failed"):
            credentials_svc.delete_credential("old")

        state["fail"] = False
        assert credentials_svc.delete_credential("old") is True
        assert credentials_svc.get_credential_record("old") is None
        assert _engine("old") is None
        assert json.loads(env.read_text(encoding="utf-8")) == {}

    def test_success_path_unchanged(self, env):
        """成功路径：save 后 DB/旁文件一致；delete 后两处都清除。"""
        credentials_svc.save_credential("new", "generic", NEW_SECRET)
        assert credentials_svc.get_credential_record("new")["data"] == NEW_SECRET
        assert _engine("new") == NEW_SECRET

        assert credentials_svc.delete_credential("old") is True
        assert credentials_svc.get_credential_record("old") is None
        assert _engine("old") is None
        stored = json.loads(env.read_text(encoding="utf-8"))
        assert "old" not in stored
        assert "new" in stored
