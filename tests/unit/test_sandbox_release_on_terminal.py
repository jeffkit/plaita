"""终态沙箱释放（``FlowWorker._release_sandboxes``）。

背景（2026-10-08 实测）：分布式步进下**每个 step 都新建 handlers 列表**——
生命周期回调若每步新建，agent 节点在早先 step 产生的 workspace 快照到 flow
结束那一步已丢失，终态释放静默失效（plaita#22 跑完 24 分钟后实例仍 running、
日志零条 ``sandbox lifecycle``）。修复两点：

1. 回调**按执行缓存**（``_sandbox_lifecycle_for``），同一执行跨 step 复用；
2. 终态落盘时从**持久化上下文**（``$NODE.*.workspace``）收集快照释放——
   跨 step、跨进程都成立；成功 kill、失败/取消 pause 保现场。
"""

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("plaita_nodes", reason="沙箱可选依赖未装")

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import FlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class _FakeLifecycle:
    """记录 drain 调用（替代 SandboxLifecycleCallback）。"""

    def __init__(self):
        self.drained = []

    def drain(self, snapshots=None, phase="end", keep_data=None):
        self.drained.append({"snapshots": list(snapshots or []), "phase": phase,
                             "keep_data": keep_data})
        return [{"id": s.get("id"), "outcome": "clean"} for s in (snapshots or [])]


def _worker(monkeypatch, fake):
    worker = FlowWorker(
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        event_bus=InMemoryEventBus(),
    )
    monkeypatch.setattr(worker, "_sandbox_lifecycle_handler", lambda: fake)
    return worker


def _state(status: str) -> ExecutionState:
    return ExecutionState(
        execution_id="e1", status=status,
        context={"$NODE": {"impl": {"workspace": {"id": "e1:main", "driver": "ags",
                                                  "path": "/home/user/plaita-ws/repo"}}}},
    )


class TestTerminalSandboxRelease:
    def test_completed_kills_with_context_snapshots(self, monkeypatch):
        fake = _FakeLifecycle()
        worker = _worker(monkeypatch, fake)
        worker._persist_state_or_raise("e1", _state("completed"), "end")
        assert len(fake.drained) == 1
        call = fake.drained[0]
        assert call["phase"] == "terminal"
        assert call["keep_data"] is False               # 成功 → kill 不留现场
        assert [s["id"] for s in call["snapshots"]] == ["e1:main"]
        assert "e1" not in worker._sandbox_callbacks    # 终态后回收缓存

    def test_error_keeps_scene(self, monkeypatch):
        fake = _FakeLifecycle()
        worker = _worker(monkeypatch, fake)
        worker._persist_state_or_raise("e1", _state("error"), "end")
        assert fake.drained[0]["keep_data"] is True     # 失败 → pause 保现场

    def test_non_terminal_is_noop(self, monkeypatch):
        fake = _FakeLifecycle()
        worker = _worker(monkeypatch, fake)
        worker._sandbox_lifecycle_for("e1")             # 步进时已装配（_handlers_for）
        worker._persist_state_or_raise("e1", _state("running"), "step")
        assert fake.drained == []
        assert worker._sandbox_callbacks.get("e1") is fake   # 仍在途：缓存保留

    def test_callback_instance_is_reused_across_steps(self, monkeypatch):
        """同一执行的多个 step 必须复用同一个回调实例（否则快照跨步丢失）。"""
        fake = _FakeLifecycle()
        worker = _worker(monkeypatch, fake)
        first = worker._sandbox_lifecycle_for("e1")
        second = worker._sandbox_lifecycle_for("e1")
        assert first is second is fake

    def test_no_snapshots_in_context_is_silent(self, monkeypatch):
        fake = _FakeLifecycle()
        worker = _worker(monkeypatch, fake)
        state = ExecutionState(execution_id="e2", status="completed", context={})
        worker._persist_state_or_raise("e2", state, "end")
        assert fake.drained == []
