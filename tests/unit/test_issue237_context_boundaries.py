"""plaita#237：跨边界执行单元的租户 ContextVar 传播契约。

同根复发史：#58 节点线程 → #199 ``run_async_from_sync`` → #212
``async_gen_to_sync`` → #213 console ``executor.submit`` → 本单
``ThreadParallelExecutor`` / ``Parallel.arun`` 的 ``run_in_executor`` /
进程模式分支。每个边界都必须显式携带租户（ContextVar 或序列化体），否则
worker 里 ``current_tenant()`` 退回 default，凭据路由串到默认租户。

本文件钉住新补的三处：
1. ``ThreadParallelExecutor.submit/map``（Parallel/Map 分支线程）——
   在调用线程 ``copy_context()``，每个副本只进入一次；
2. ``Parallel.arun`` 的 thread/process 分支——``asyncio.to_thread``
   （``run_in_executor`` 不复制 contextvars）；
3. 进程模式——``ExecutionContext.__getstate__`` 携带租户，
   ``__setstate__`` 在子进程恢复（Context 不能 pickle）。

负控：分别还原上述三处 ⇒ 对应用例红（见
``docs/proofs/237-tenant-context-contract.md``）。
"""
from __future__ import annotations

import pickle
from contextvars import ContextVar
from typing import ClassVar

import pytest

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.parallel_executor import ThreadParallelExecutor
from plaita.io import Property
from plaita.node import End, Node, Start
from plaita.node.concurrent import Parallel
from plaita.tenant_context import (
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
)

_FENCE: ContextVar[str] = ContextVar("issue237_fence", default="no-fence")


class _TenantProbe(Node):
    """返回执行侧当前租户；用于真实 Parallel 分支验证。"""

    node_type: ClassVar[str] = "issue237_probe"

    def run(self, execution):
        return current_tenant()


def _string_flow(flow_id: str) -> Flow:
    return Flow(
        flow_id=flow_id,
        version="1",
        runtime="python",
        output_type=Property(data_type=types.STRING),
        nodes=[
            Start(id="start", next="p"),
            _TenantProbe(id="p", next="end"),
            End(id="end", resultType="success", output="$NODE.p"),
        ],
    )


def _parallel_flow(mode: str) -> Flow:
    return Flow(
        flow_id=f"parent-{mode}",
        version="1",
        runtime="python",
        output_type=Property(data_type=types.OBJECT),
        nodes=[
            Start(id="start", next="par"),
            Parallel(
                id="par", name="par", mode=mode, join_branches=["A"],
                branches=[{"name": "A", "flow": _string_flow(f"branch-{mode}")}],
                next="end",
            ),
            End(id="end", resultType="success", output="$NODE.par"),
        ],
    )


@pytest.fixture()
def tenant_b():
    token = set_current_tenant("tenant-b")
    fence_token = _FENCE.set("fence-b")
    try:
        yield
    finally:
        _FENCE.reset(fence_token)
        reset_current_tenant(token)


# -- 1. ThreadParallelExecutor.submit/map -----------------------------------


def test_thread_executor_submit_propagates_tenant_and_fence(tenant_b):
    ex = ThreadParallelExecutor(max_workers=1)
    got = ex.submit(lambda: (current_tenant(), _FENCE.get())).result()
    assert got == ("tenant-b", "fence-b"), (
        "ThreadParallelExecutor.submit 丢了调用线程的租户/fence（#237）"
    )


def test_thread_executor_map_propagates_each_item(tenant_b):
    ex = ThreadParallelExecutor(max_workers=2)
    got = ex.map(lambda i: (i, current_tenant(), _FENCE.get()), [1, 2])
    assert got == [(1, "tenant-b", "fence-b"), (2, "tenant-b", "fence-b")]


# -- 2. Parallel thread 分支（asyncio.to_thread + executor） -----------------


def test_parallel_thread_branch_sees_tenant(tenant_b):
    result = FlowExecution().run_compatible(_parallel_flow("thread"), False)
    assert result == {"A": "tenant-b"}, (
        f"Parallel thread 分支读到 {result!r}；应继承调用方租户（#237）"
    )


def test_parallel_thread_does_not_leak_back_to_caller(tenant_b):
    FlowExecution().run_compatible(_parallel_flow("thread"), False)
    assert current_tenant() == "tenant-b"
    assert _FENCE.get() == "fence-b"


# -- 3. 进程模式：ExecutionContext pickle 携带租户 ---------------------------


def test_context_pickle_carries_tenant(tenant_b):
    ctx = ExecutionContext()
    blob = pickle.dumps(ctx)
    token = set_current_tenant("default")
    try:
        rebuilt = pickle.loads(blob)
        assert rebuilt is not None
        assert current_tenant() == "tenant-b", (
            "ExecutionContext pickle 未携带租户；子进程分支会退回 default（#237）"
        )
    finally:
        reset_current_tenant(token)


@pytest.mark.slow
def test_parallel_process_branch_sees_tenant(tenant_b):
    result = FlowExecution().run_compatible(_parallel_flow("process"), False)
    assert result == {"A": "tenant-b"}, (
        f"Parallel process 分支读到 {result!r}；租户未随执行体跨进程（#237）"
    )
