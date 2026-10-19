"""plaita#213 残面：quick-test 发布超时后必须快速失败，不能被 worker 拖住。

## 缺陷

``api/cluster.py::run_quick_test`` 的事件发布桥写成::

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(ctx.run, _run_publish_derived)
        event_id = future.result(timeout=5)

``with`` 退出的 ``__exit__`` 等价 ``shutdown(wait=True)``：EventBus 后端
（Redis 等）卡住时，``result(timeout=5)`` 抛超时后仍会一直等发布线程跑完，
5s 上限对请求延迟完全无效——quick-test 请求挂到发布自己返回为止。

## 修法

去掉 ``with``，显式 ``executor.shutdown(wait=False, cancel_futures=True)``，
并把 ``concurrent.futures.TimeoutError`` 转成带说明的 ``TimeoutError``：
请求在超时附近快速失败，后台线程待其自然收尾（Python 无法强杀运行中线程）。

## 契约

- 发布阻塞超过 ``_QUICK_TEST_PUBLISH_TIMEOUT``：请求在超时附近返回
  ``success=False``，message/error 含「事件发布超时」；
- 返回时发布线程**仍在阻塞**（确实没有等待它），且只发布一次（没有误走
  同步回退分支）；
- 释放后端后线程能收尾，不留常驻资源。

## 负控

还原 ``with ThreadPoolExecutor(...)`` + ``result(timeout=5)``（保留本文件），
超时用例的延迟 / 「返回时线程仍阻塞」断言必红；证据见
``docs/proofs/data/213b-quick-test-timeout-negative-control.txt``。
"""
from __future__ import annotations

import asyncio
import sys
import threading
import time
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import api.cluster as cluster_api  # noqa: E402
from api.cluster import QuickTestRequest, run_quick_test  # noqa: E402

#: 发布线程被「卡住」的时长；旧实现 request 延迟≈此值。
_BLOCK_SECONDS = 3.0


class _BlockingBus:
    """publish 阻塞到 ``release`` 才返回，模拟不可达的 EventBus 后端。"""

    def __init__(self):
        self.release = threading.Event()
        self.started = threading.Event()
        self.finished = threading.Event()
        self.calls = 0

    async def publish(self, event):
        self.calls += 1
        self.started.set()
        self.release.wait(_BLOCK_SECONDS)
        self.finished.set()
        return "evt-213b"


class _FlowSentinel:
    """Flow.model_validate 的替身；quick-test 只把它转交给 FlowExecution.run。"""


def _install_fakes(monkeypatch) -> _BlockingBus:
    from plaita import Flow, FlowExecution

    bus = _BlockingBus()
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
                "execution_id": "exec-213b",
            }
        return {
            "is_end": False,
            "is_suspend": True,
            "result": {"event_type": "test_event"},
            "context": {"step": 1},
            "execution_id": "exec-213b",
        }

    monkeypatch.setattr(FlowExecution, "run", staticmethod(fake_run))
    return bus


def test_publish_timeout_fails_fast_without_waiting_worker(monkeypatch):
    """核心性质：超时即返回，且返回时发布线程仍在阻塞（没被 shutdown 等待）。"""
    bus = _install_fakes(monkeypatch)
    # raising=False：旧实现没有这个旋钮（硬编码 5s），负控回退后本用例仍须
    # 走完行为断言再红，而不是在 monkeypatch 处因缺属性直接报错。
    monkeypatch.setattr(cluster_api, "_QUICK_TEST_PUBLISH_TIMEOUT", 0.2, raising=False)

    async def main():
        started = time.monotonic()
        response = await run_quick_test(QuickTestRequest(test_type="event"))
        return response, time.monotonic() - started

    try:
        response, elapsed = asyncio.run(main())

        assert response.success is False, response
        assert "事件发布超时" in (response.message + (response.error or "")), (
            f"超时没有转成具名错误: {response.message!r}"
        )
        # 旧实现：with 退出等 worker 跑满 _BLOCK_SECONDS（3s，且随后走成功
        # 路径），这里要求超时附近返回。
        assert elapsed < 1.5, f"超时后仍等待发布线程：{elapsed:.2f}s"
        assert bus.started.is_set(), "发布线程从未启动"
        assert not bus.finished.is_set(), (
            "返回时发布已完成——说明请求等到了 worker 结束，超时未生效"
        )
        assert bus.calls == 1, (
            f"发布被调用了 {bus.calls} 次：超时异常误触发了同步回退分支"
        )
    finally:
        bus.release.set()

    # 释放后端后线程自然收尾，避免 executor 在解释器退出时 join 卡住。
    assert bus.finished.wait(2.0), "发布线程未随 release 收尾"


def test_publish_within_timeout_still_succeeds(monkeypatch):
    """正常路径不回归：发布在期限内完成时 quick-test 仍成功。"""
    bus = _install_fakes(monkeypatch)
    bus.release.set()  # 不阻塞

    response = asyncio.run(run_quick_test(QuickTestRequest(test_type="event")))

    assert response.success is True, response
    assert bus.calls == 1
    assert bus.finished.is_set()
