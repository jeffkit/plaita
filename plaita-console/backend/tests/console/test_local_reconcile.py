"""本地单机模式的启动对账（僵尸 running 执行）。

本地模式没有队列重投：console 重启后，重启前 running 的执行会失去唯一的执行
线程而永远卡在 running。这里覆盖对账的三种口径、幂等性与边界（只碰 running）。
"""

import json
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store as fs  # noqa: E402
from services import reconcile as rc  # noqa: E402


@pytest.fixture()
def store(tmp_path):
    fs.init_engine(f"sqlite:///{tmp_path / 'reconcile.db'}")
    return fs.get_flow_store()


def _insert(execution_id: str, status: str, **kw) -> None:
    fs.insert_local_execution(
        execution_id=execution_id,
        flow_id=kw.get("flow_id", "demo"),
        flow_version="1.0.0",
        status=status,
        tenant_id="default",
    )


def _row(execution_id: str) -> dict:
    return {
        row["execution_id"]: row for row in fs.list_local_executions()
    }[execution_id]


class TestReconcileOrphans:
    def test_running_becomes_suspended_with_reason(self, store):
        _insert("e-running", "running")
        summary = rc.reconcile_orphan_local_executions(mode="suspend", store=store)

        assert summary["mode"] == "suspend"
        assert summary["scanned"] == 1 and summary["reconciled"] == 1
        row = _row("e-running")
        assert row["status"] == "suspended"
        # 必须留下可读原因：不然运维只看到「已暂停」不知道是重启造成的
        err = row["error"]
        assert err["reason"] == "interrupted_by_restart"
        assert "重启" in err["message"]
        # 挂起态不该有结束时间（它是可恢复的，不是终态）
        assert row["end_time"] is None

    def test_fail_mode_terminates_with_end_time(self, store):
        _insert("e-running", "running")
        summary = rc.reconcile_orphan_local_executions(mode="fail", store=store)

        assert summary["reconciled"] == 1
        row = _row("e-running")
        assert row["status"] == "failed"
        assert row["end_time"] is not None
        assert row["error"]["mode"] == "fail"

    def test_off_mode_touches_nothing(self, store):
        _insert("e-running", "running")
        summary = rc.reconcile_orphan_local_executions(mode="off", store=store)

        assert summary["mode"] == "off"
        assert summary["reconciled"] == 0
        assert _row("e-running")["status"] == "running"

    def test_only_running_is_touched(self, store):
        _insert("e-susp", "suspended")
        _insert("e-done", "completed")
        _insert("e-failed", "failed")
        _insert("e-cancelled", "cancelled")
        _insert("e-running", "running")

        summary = rc.reconcile_orphan_local_executions(mode="suspend", store=store)

        assert summary["scanned"] == 1, "只应对 running 计数"
        assert summary["reconciled"] == 1
        assert _row("e-susp")["status"] == "suspended"
        assert _row("e-done")["status"] == "completed"
        assert _row("e-failed")["status"] == "failed"
        assert _row("e-cancelled")["status"] == "cancelled"

    def test_idempotent_across_restarts(self, store):
        _insert("e-running", "running")
        first = rc.reconcile_orphan_local_executions(mode="suspend", store=store)
        second = rc.reconcile_orphan_local_executions(mode="suspend", store=store)

        assert first["reconciled"] == 1
        assert second["scanned"] == 0 and second["reconciled"] == 0

    def test_mode_from_env_with_fallback(self, store, monkeypatch):
        _insert("e-running", "running")
        monkeypatch.setenv("PLAITA_CONSOLE_RECONCILE_ORPHANS", "bogus")
        summary = rc.reconcile_orphan_local_executions(store=store)
        assert summary["mode"] == rc.DEFAULT_MODE, "非法值必须回退默认而不是乱选"

    def test_env_mode_off_is_respected(self, store, monkeypatch):
        _insert("e-running", "running")
        monkeypatch.setenv("PLAITA_CONSOLE_RECONCILE_ORPHANS", "off")
        summary = rc.reconcile_orphan_local_executions(store=store)
        assert summary["reconciled"] == 0
        assert _row("e-running")["status"] == "running"

    def test_condition_update_protects_concurrent_finish(self, store):
        """CAS 语义：对账期间执行自己完成（running→completed）则让过，不覆写。"""
        _insert("e-race", "running")
        original = fs.update_local_execution_status_if

        def racing(execution_id, expected, new_status):
            # 模拟「读取后、写入前，执行线程完成了」
            original(execution_id, "running", "completed")
            return original(execution_id, expected, new_status)

        fs.update_local_execution_status_if = racing  # type: ignore[assignment]
        try:
            summary = rc.reconcile_orphan_local_executions(mode="suspend", store=store)
        finally:
            fs.update_local_execution_status_if = original  # type: ignore[assignment]

        assert summary["reconciled"] == 0 and summary["skipped"] == 1
        assert _row("e-race")["status"] == "completed"

    def test_empty_store_is_noop(self, store):
        summary = rc.reconcile_orphan_local_executions(mode="suspend", store=store)
        assert summary == {
            "mode": "suspend",
            "scanned": 0,
            "reconciled": 0,
            "skipped": 0,
            "failed": 0,
        }
