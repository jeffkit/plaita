"""plaita#239 先行：租户来源三态标记 + 跨边界派生 + 写探针（只观测）。

- ``explicit``：当前上下文显式 ``set_current_tenant``；
- ``derived``：经执行边界（线程/进程/asyncio 桥）从上一上下文派生；
- ``default``：从未设置（``current_tenant()`` 回退 default）。

标记必须随 #237 的 ``copy_context``/``ctx.run`` 链传播：边界 worker 内
``mark_tenant_derived()`` 把 explicit 降级为 derived，default 保持 default。
写探针 ``record_write_probe`` 只计数 + default 来源 DEBUG，不改变任何写行为。
"""
from __future__ import annotations

import asyncio
import contextvars
import pickle
from contextvars import Context

import pytest

from plaita.core.async_utils import run_async_from_sync
from plaita.core.context import ExecutionContext
from plaita.core.parallel_executor import ThreadParallelExecutor
from plaita.server.tenant_context import TenantRoutingFlowStorage
from plaita.tenant_context import (
    TENANT_SOURCE_DEFAULT,
    TENANT_SOURCE_DERIVED,
    TENANT_SOURCE_EXPLICIT,
    current_tenant,
    current_tenant_source,
    mark_tenant_derived,
    record_write_probe,
    reset_current_tenant,
    reset_write_probe,
    set_current_tenant,
    write_probe_snapshot,
)


def _state():
    return current_tenant(), current_tenant_source()


# -- 三态基础 ---------------------------------------------------------------


def test_default_source_when_never_set():
    assert Context().run(_state) == ("default", TENANT_SOURCE_DEFAULT)


def test_explicit_set_then_reset():
    def _run():
        token = set_current_tenant("tenant-b")
        try:
            return _state()
        finally:
            reset_current_tenant(token)

    assert Context().run(_run) == ("tenant-b", TENANT_SOURCE_EXPLICIT)


def test_mark_derived_is_noop_for_default():
    def _run():
        mark_tenant_derived()
        return _state()

    assert Context().run(_run) == ("default", TENANT_SOURCE_DEFAULT)


# -- 跨边界：derived --------------------------------------------------------


def test_thread_executor_submit_marks_derived():
    def _run():
        token = set_current_tenant("tenant-b")
        try:
            ex = ThreadParallelExecutor(max_workers=1)
            return ex.submit(_state).result()
        finally:
            reset_current_tenant(token)

    assert Context().run(_run) == ("tenant-b", TENANT_SOURCE_DERIVED)


def test_thread_executor_map_marks_derived():
    def _run():
        token = set_current_tenant("tenant-b")
        try:
            ex = ThreadParallelExecutor(max_workers=2)
            return ex.map(lambda _: _state(), [1, 2])
        finally:
            reset_current_tenant(token)

    assert Context().run(_run) == [
        ("tenant-b", TENANT_SOURCE_DERIVED),
        ("tenant-b", TENANT_SOURCE_DERIVED),
    ]


def test_async_bridge_marks_derived():
    async def _probe():
        return _state()

    async def _main():
        token = set_current_tenant("tenant-b")
        try:
            # 已有 running loop → run_async_from_sync 走 worker 线程桥
            return run_async_from_sync(_probe())
        finally:
            reset_current_tenant(token)

    assert asyncio.run(_main()) == ("tenant-b", TENANT_SOURCE_DERIVED)


def test_caller_source_stays_explicit_after_boundary():
    def _run():
        token = set_current_tenant("tenant-b")
        try:
            ThreadParallelExecutor(max_workers=1).submit(_state).result()
            return _state()
        finally:
            reset_current_tenant(token)

    assert Context().run(_run) == ("tenant-b", TENANT_SOURCE_EXPLICIT)


def test_execution_context_pickle_marks_derived():
    def _make_blob():
        token = set_current_tenant("tenant-b")
        try:
            return pickle.dumps(ExecutionContext())
        finally:
            reset_current_tenant(token)

    blob = Context().run(_make_blob)

    def _load():
        pickle.loads(blob)
        return _state()

    assert Context().run(_load) == ("tenant-b", TENANT_SOURCE_DERIVED)


# -- 写探针：只观测 ---------------------------------------------------------


def test_write_probe_counts_default_and_logs_debug(caplog):
    def _run():
        reset_write_probe()
        with caplog.at_level("DEBUG", logger="plaita.tenant_context"):
            record_write_probe("unit.default")
        return write_probe_snapshot()

    snapshot = Context().run(_run)
    assert snapshot[("unit.default", TENANT_SOURCE_DEFAULT)] == 1
    assert any(
        "source=default" in r.getMessage() for r in caplog.records
    ), [r.getMessage() for r in caplog.records]


def test_write_probe_counts_explicit_source():
    def _run():
        reset_write_probe()
        token = set_current_tenant("tenant-b")
        try:
            record_write_probe("unit.explicit")
        finally:
            reset_current_tenant(token)
        return write_probe_snapshot()

    snapshot = Context().run(_run)
    assert snapshot[("unit.explicit", TENANT_SOURCE_EXPLICIT)] == 1
    assert not [k for k in snapshot if k[1] == TENANT_SOURCE_DEFAULT]


def test_storage_write_probe_counts_and_does_not_change_behavior():
    class _FakeFlowStorage:
        def save_flow(self, flow):
            return True

    def _run():
        reset_write_probe()
        writer = TenantRoutingFlowStorage()
        writer._current_storage = lambda: _FakeFlowStorage()  # type: ignore[assignment]
        ok = writer.save_flow({"flow_id": "f"})
        return ok, write_probe_snapshot()

    ok, snapshot = Context().run(_run)
    assert ok is True, "探针不得改变写行为"
    assert snapshot[("storage.save_flow", TENANT_SOURCE_DEFAULT)] == 1
