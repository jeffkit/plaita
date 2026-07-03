"""P1-2 回归: Parallel 后台分支 (非 join) 失败不再静默。

历史上后台分支是纯 fire-and-forget: 不持 future 引用、无 done_callback、无超时,
分支崩溃全静默, 运维侧无信号。0.5.0 起:
- 持有 future 引用到 ``execution.__background_futures__``;
- ``done_callback`` 把失败记进 ``execution.__background_errors__`` 并 logger.warning;
- ``wait_background_branches`` 供调试期 join。
不改变 fire-and-forget 语义 (不等结果)。
"""
from __future__ import annotations

import logging

import pytest

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.node import End, Start
from plaita.node.concurrent import Parallel, ParallelBranch, THREAD


def _make_parallel_flow(join_branches, background_branches, mode=THREAD):
    """join_branches / background_branches: dict {name: flow_json_str}。"""
    branches = []
    for name, flow_json in {**join_branches, **background_branches}.items():
        branches.append({"name": name, "flow": flow_json, "input": "$INPUT"})
    join_names = list(join_branches.keys())
    data = {
        "flow_id": "parallel-bg",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "s", "next": "p"},
            {"type": "parallel", "id": "p",
             "mode": mode, "joinBranches": join_names,
             "branches": branches, "next": "e"},
            {"type": "end", "id": "e", "output": "$NODE.p", "resultType": "success"},
        ],
    }
    return Flow.model_validate(data)


def _branch_flow_json(out, fail=False):
    """生成一个简单子流程: start -> end(output=out)。fail=True 时让 end output
    引用不存在的 $INPUT.xxx 触发解析期错误? 改用 code 节点抛错更稳。但 code 需注册。
    用 assignment + 不存在的 $NODE 引用, 或直接让 end output = "$INPUT.x" 在 input
    为标量时会报错。这里用一个会抛 ValueError 的表达式。"""
    # 用一个永远抛错的输出表达式: $INPUT.__boom__ 不存在 -> 表达式解析为 None 不抛。
    # 改用 code 节点不可行 (需注册)。用 end output 引用 $NODE.不存在 节点 -> NodeNotFoundError。
    if fail:
        # start.next 指向不存在的节点 -> 执行期 next_node 抛 NodeNotFoundError, 可靠。
        return (
            '{"flow_id":"bg-fail","nodes":['
            '{"type":"start","id":"s","next":"ghost"},'
            '{"type":"end","id":"e","output":"ok","resultType":"success"}'
            ']}'
        )
    return (
        f'{{"flow_id":"bg-ok","nodes":['
        f'{{"type":"start","id":"s","next":"e"}},'
        f'{{"type":"end","id":"e","output":"{out}","resultType":"success"}}'
        f']}}'
    )


class TestBackgroundBranchObservability:
    def _run_on(self, flow, execution):
        # 用 execution.execute 在 *本* 实例上跑, 让后台 future/error 挂到我们持有的
        # execution 上 (flow.run 会内部新建另一个 FlowExecution)。
        return execution.execute(flow, {"value": 1})

    def test_failed_background_branch_is_recorded(self, caplog):
        flow = _make_parallel_flow(
            join_branches={"j": _branch_flow_json("join-ok")},
            background_branches={"bg": _branch_flow_json("x", fail=True)},
        )
        execution = FlowExecution()
        self._run_on(flow, execution)
        par = next(n for n in flow.nodes if n.node_type == "parallel")
        par.wait_background_branches(execution, timeout=5)

        # done_callback 在 future 完成后由 worker 线程异步触发, 可能晚于
        # concurrent.futures.wait 返回; 轮询最多 2s 等 callback 落地。
        import time as _t
        errors = par.get_background_errors(execution)
        deadline = _t.time() + 2
        while not errors and _t.time() < deadline:
            _t.sleep(0.01)
            errors = par.get_background_errors(execution)
        assert errors, "background branch failure should be recorded"
        assert any(e["branch"] == "bg" for e in errors)

    def test_failed_background_branch_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger="plaita.node.concurrent"):
            flow = _make_parallel_flow(
                join_branches={"j": _branch_flow_json("join-ok")},
                background_branches={"bg": _branch_flow_json("x", fail=True)},
            )
            execution = FlowExecution()
            self._run_on(flow, execution)
            par = next(n for n in flow.nodes if n.node_type == "parallel")
            par.wait_background_branches(execution, timeout=5)
        assert any("background branch" in r.message and "bg" in r.message
                   for r in caplog.records)

    def test_successful_background_branch_records_no_error(self):
        flow = _make_parallel_flow(
            join_branches={"j": _branch_flow_json("join-ok")},
            background_branches={"bg": _branch_flow_json("bg-ok-val")},
        )
        execution = FlowExecution()
        self._run_on(flow, execution)
        par = next(n for n in flow.nodes if n.node_type == "parallel")
        par.wait_background_branches(execution, timeout=5)
        assert par.get_background_errors(execution) == []

    def test_future_references_are_held(self):
        flow = _make_parallel_flow(
            join_branches={"j": _branch_flow_json("join-ok")},
            background_branches={"bg": _branch_flow_json("bg-ok-val")},
        )
        execution = FlowExecution()
        self._run_on(flow, execution)
        # future 引用存在模块级 _BG_STATE, 经 wait 接口确认持有且能 join
        par = next(n for n in flow.nodes if n.node_type == "parallel")
        stats = par.wait_background_branches(execution, timeout=5)
        assert stats["done"] == 1
        assert stats["not_done"] == 0
