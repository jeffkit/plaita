"""plaita#213 回归：quick-test 挂起事件发布的线程桥必须复制 ContextVar。

## 缺陷

``api/cluster.py::run_quick_test`` 的事件测试分支在「已有 running loop」时用
raw ``executor.submit(asyncio.run, publish_event())`` 把发布协程丢进新线程；
``submit`` 不传播 contextvars——发布协程里的租户 / fence 退回默认值，
EventBus 发布及按租户路由的存储可能写错命名空间（#199/#212 同根第三处）。

修复：在**调用线程** ``contextvars.copy_context()``，worker 内
``ctx.run`` 驱动 ``asyncio.run``（与 #199 ``run_async_from_sync`` 同口径）。

## 契约

- 在 async 请求上下文里 ``set_current_tenant("tenant-b")`` + 第二个 fence
  ContextVar，走到 quick-test 的挂起发布分支；``event_bus.publish`` 内必须看到
  tenant-b / fence-b，且凭据路由读 tenant-b 旁文件；
- copy_context 是隔离副本：发布后调用方 ContextVar 不变；
- default 租户行为不变。

## 负控

还原 ``executor.submit(asyncio.run, publish_event())`` 后，租户/fence/旁文件
断言必红（publish 内读到 default）；default 用例保持绿。证据见
``docs/proofs/213-quick-test-context.md`` 与
``docs/proofs/data/213-negative-control.txt``。
"""
from __future__ import annotations

import asyncio
import json
import sys
from contextvars import ContextVar
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api.cluster import QuickTestRequest, run_quick_test  # noqa: E402
from plaita.credentials import credentials_file, get_credential  # noqa: E402
from plaita.tenant_context import (  # noqa: E402
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
)

_FENCE: ContextVar[str] = ContextVar("issue213_fence", default="no-fence")


def _write(path: Path, key: bytes, entries: dict) -> None:
    store = {
        name: {
            "type": "generic",
            "data": Fernet(key).encrypt(json.dumps(data).encode()).decode(),
        }
        for name, data in entries.items()
    }
    path.write_text(json.dumps(store))


@pytest.fixture()
def cred_env(tmp_path, monkeypatch):
    """default 基础文件 + tenant-b 旁文件（同名 shared 值不同）。"""
    base = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(base))
    key = Fernet.generate_key()
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY", key.decode())
    _write(base.with_name("creds.tenant-b.json"), key, {
        "shared": {"url": "https://tenant-b.example"},
    })
    _write(base, key, {
        "shared": {"url": "https://default.example"},
    })
    return base


class _RecordingBus:
    """在发布协程内记录 ContextVar 与凭据路由结果。"""

    def __init__(self):
        self.calls: list[dict] = []

    async def publish(self, event):
        self.calls.append({
            "tenant": current_tenant(),
            "fence": _FENCE.get(),
            "file": credentials_file().name,
            "shared": get_credential("shared")["url"],
            "event_type": getattr(event, "event_type", None),
        })
        return "evt-213"


class _FlowSentinel:
    """Flow.model_validate 的替身；quick-test 只把它转交给 FlowExecution.run。"""


def _install_fakes(monkeypatch, bus: _RecordingBus) -> None:
    from plaita import Flow, FlowExecution

    monkeypatch.setattr("plaita.event.memory.InMemoryEventBus", lambda: bus)
    monkeypatch.setattr(
        Flow, "model_validate", classmethod(lambda cls, data: _FlowSentinel())
    )
    # 避免测试改全局节点注册表（quick-test 的模板注册副作用）。
    monkeypatch.setattr("plaita.node.register_code_node", lambda **kw: None, raising=False)
    monkeypatch.setattr("plaita.node.node_register", lambda *a, **kw: None, raising=False)

    def fake_run(flow, params=None, mode=None, context=None, event_bus=None,
                 resume_type=None, resume_data=None, **kwargs):
        if resume_type == "event":
            return {
                "is_end": True,
                "is_suspend": False,
                "result": {"processed": True},
                "context": {"step": 2},
                "execution_id": "exec-213",
            }
        return {
            "is_end": False,
            "is_suspend": True,
            "result": {"event_type": "test_event"},
            "context": {"step": 1},
            "execution_id": "exec-213",
        }

    monkeypatch.setattr(FlowExecution, "run", staticmethod(fake_run))


def _run_quick_test_case(tenant: str, fence: str) -> dict:
    box: dict = {}

    async def main():
        tenant_token = set_current_tenant(tenant)
        fence_token = _FENCE.set(fence)
        try:
            box["response"] = await run_quick_test(
                QuickTestRequest(test_type="event"), None,
                {"platform_admin": True},
            )
            box["caller_after"] = (current_tenant(), _FENCE.get())
        finally:
            _FENCE.reset(fence_token)
            reset_current_tenant(tenant_token)

    asyncio.run(main())
    return box


def test_quick_test_publish_sees_caller_tenant_and_fence(monkeypatch, cred_env):
    """核心性质：发布协程必须看到调用线程的租户/fence（#213）。"""
    bus = _RecordingBus()
    _install_fakes(monkeypatch, bus)
    box = _run_quick_test_case("tenant-b", "fence-b")

    assert box["response"].success is True, box["response"]
    assert len(bus.calls) == 1, f"挂起事件应发布一次: {bus.calls!r}"
    call = bus.calls[0]
    assert call["tenant"] == "tenant-b", (
        "quick-test 发布协程丢了租户 ContextVar（#213）；读到 default 说明 "
        "submit 未在 copy_context 下运行"
    )
    assert call["fence"] == "fence-b", "worker 路径丢了第二个 ContextVar（fence）"
    assert call["file"] == "creds.tenant-b.json", "凭据路由读到了 default 文件"
    assert call["shared"] == "https://tenant-b.example"
    assert box["caller_after"] == ("tenant-b", "fence-b"), (
        "copy_context 是隔离副本，发布协程不得改写调用方 ContextVar"
    )


def test_quick_test_publish_default_tenant_unchanged(monkeypatch, cred_env):
    """default 租户行为不变：base 文件 + default-only 语义不受桥修复影响。"""
    bus = _RecordingBus()
    _install_fakes(monkeypatch, bus)
    box = _run_quick_test_case("default", "fence-default")

    assert box["response"].success is True, box["response"]
    call = bus.calls[0]
    assert call["tenant"] == "default"
    assert call["fence"] == "fence-default"
    assert call["file"] == "creds.json"
    assert call["shared"] == "https://default.example"
