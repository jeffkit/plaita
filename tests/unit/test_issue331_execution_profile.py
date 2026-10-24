"""plaita#331 后半 — 执行能力契约的消费面：profile 校验 / 递归闭包 / NodeRunner 兜底 / 矩阵账本。

前半（#330 / B1 r89）落了 5 token 词表 + ``Node.execution_requirements`` + 注册三路同闸；
本文件钉后半四件事（#331 正文 + 评论区 A2 r61 评审 + owner 裁决「5 token 封闭，不加第六」）：

1. ``validate_for_profile(flow, profile)`` **独立于 ``model_validate``**，只在执行入口
   （Normal / Generator / Distributed 三路 + ``FlowExecution.run`` 分派）与 console
   publish/start 调用；profile 判据 = ADR-001 runtime 组合（normal/generator 进程内、
   distributed 池、console local 后端）各自满足的 token 集。入口拒绝发生在**任何节点
   执行前**（与 #330 运行期「执行到挂起点才拒」的区别钉）。
2. 递归闭包：child / loop 族 / parallel 分支 + reference 引用的 flow，
   ``required_capabilities(flow, recursive=True)`` 累积 token；帧路径根→叶。
3. NodeRunner 在每个 suspending 节点 ``execute`` **前**断言宿主契约——覆盖绕过
   strategies 的旁路（直接驱动 ``_advance_one``）；与入口共用同一判定函数，触达即
   入口漏了：具名错误 + WARNING，不静默。
4. ``registry.capability_matrix()`` → 治理账本 ``docs/governance/architecture.yaml``
   ``gov.capability_matrix``；差分断言「注册表新增 node_type 而矩阵未变 → 红」。

守恒：distributed 入口对 loop/parallel 帧内挂起**放行**（只判 token 不判帧类型），运行期
作用域闸（NS-M3，``frame boundary``）照抛；子执行（parent 非空）不闸、carrier 原样上抛。
"""
from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from plaita.core.capabilities import (
    EXECUTION_CAPABILITY_TOKENS,
    EXECUTION_PROFILES,
    SUSPEND_ALLOWED_FRAME_NODE_TYPES,
    SUSPEND_CAPABILITY_TOKENS,
    check_execution_requirements,
    diff_capability_matrix,
    profile_for_mode,
    profile_tokens,
    required_capabilities,
    validate_for_profile,
)
from plaita.core.errors import (
    ExecutionProfileError,
    FlowErrorType,
    FlowSuspendedCarrier,
    SuspendNotExecutableError,
)
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.runner import NodeRunner
from plaita.core.strategies import ExecutionMode, _advance_one
from plaita.node import NodeRegistry
from plaita.node.basic import Node
from plaita.node.capabilities import shipped_capability_matrix
from plaita.server.nodes.approval_node import ApprovalNode
from plaita.server.nodes.delay_node import DelayNode

REPO_ROOT = Path(__file__).resolve().parents[2]
LEDGER = REPO_ROOT / "docs" / "governance" / "architecture.yaml"


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------

CALLS: list[str] = []


class _Counter(Node):
    node_type = "r103_counter"
    node_name = "计数桩"
    dry_run_safe = True

    def execute(self, execution):
        CALLS.append(self.id)
        return "counted"


class _SideEffectSuspend(Node):
    """有外部副作用的挂起节点（hitl_await 形状：execute 先登记再 pending）。"""

    node_type = "r103_side_effect_suspend"
    is_suspending = True
    execution_requirements = ("suspend:root", "event_bus")

    def execute(self, execution):
        CALLS.append(f"side_effect:{self.id}")
        return {"status": "pending"}


@pytest.fixture(autouse=True)
def _reset_calls():
    CALLS.clear()
    yield
    CALLS.clear()


def _registry(*extra) -> NodeRegistry:
    reg = NodeRegistry()
    for cls in extra:
        reg.register(cls)
    return reg


def _flow(nodes, flow_id="f", registry=None):
    return Flow.model_validate(
        {"flow_id": flow_id, "version": "1", "runtime": "python", "nodes": nodes},
        registry=registry,
    )


EVENT_CHILD = {
    "flow_id": "child1",
    "version": "1",
    "runtime": "python",
    "nodes": [
        {"type": "start", "id": "cstart", "next": "wait"},
        {"type": "event", "id": "wait", "event_type": "inner", "next": "cend"},
        {"type": "end", "id": "cend", "output": "y"},
    ],
}


def _event_root(registry=None):
    """start → counter(挂起点**之前**) → event → counter → end。"""
    return _flow(
        [
            {"type": "start", "id": "start", "next": "before"},
            {"type": "r103_counter", "id": "before", "next": "wait"},
            {"type": "event", "id": "wait", "event_type": "approve_me", "next": "after"},
            {"type": "r103_counter", "id": "after", "next": "end"},
            {"type": "end", "id": "end", "output": "x"},
        ],
        flow_id="root1",
        registry=registry or _registry(_Counter),
    )


def _http_root():
    return _flow(
        [
            {"type": "start", "id": "start", "next": "h"},
            {"type": "http", "id": "h", "url": "http://example.invalid/", "method": "GET", "next": "end"},
            {"type": "end", "id": "end", "output": "x"},
        ],
        flow_id="http1",
    )


def _delay_root():
    return _flow(
        [
            {"type": "start", "id": "start", "next": "d"},
            {"type": "delay", "id": "d", "delay_seconds": 5, "next": "end"},
            {"type": "end", "id": "end", "output": "x"},
        ],
        flow_id="delay1",
        registry=_registry(DelayNode),
    )


def _approval_root():
    return _flow(
        [
            {"type": "start", "id": "start", "next": "ap"},
            {
                "type": "approval",
                "id": "ap",
                "event_type": "ap_decision",
                "approval_title": "t",
                "approval_content": "c",
                "approvers": ["u1"],
                "next": "end",
            },
            {"type": "end", "id": "end", "output": "x"},
        ],
        flow_id="approval1",
        registry=_registry(ApprovalNode),
    )


# ---------------------------------------------------------------------------
# 1. profile 表与词表（ADR-001 runtime 组合）
# ---------------------------------------------------------------------------


class TestProfileTable:
    def test_builtin_profiles_are_subsets_of_closed_vocabulary(self):
        for name, tokens in EXECUTION_PROFILES.items():
            assert tokens <= EXECUTION_CAPABILITY_TOKENS, name

    def test_profile_semantics_per_adr_001(self):
        """normal/generator = 进程内 Python（无 park/resume 基础设施）；distributed =
        全集；local（console 进程内后端）= 有事件总线/挂起、**无**持久队列+延迟排程。"""
        assert EXECUTION_PROFILES["normal"] == frozenset({"network", "sandbox"})
        assert EXECUTION_PROFILES["generator"] == EXECUTION_PROFILES["normal"]
        assert EXECUTION_PROFILES["distributed"] == EXECUTION_CAPABILITY_TOKENS
        assert EXECUTION_PROFILES["local"] == EXECUTION_CAPABILITY_TOKENS - {"queue"}

    def test_profile_for_mode(self):
        assert profile_for_mode(None) == "normal"
        assert profile_for_mode(ExecutionMode.NORMAL) == "normal"
        assert profile_for_mode(ExecutionMode.GENERATOR) == "generator"
        assert profile_for_mode(ExecutionMode.DISTRIBUTED) == "distributed"
        assert profile_for_mode("distributed") == "distributed"

    def test_unknown_profile_is_named_error(self):
        with pytest.raises(ValueError) as ei:
            profile_tokens("staging")
        assert "staging" in str(ei.value)
        assert "distributed" in str(ei.value)  # 合法集点名

    def test_custom_profile_rejects_unknown_token(self):
        with pytest.raises(ValueError) as ei:
            profile_tokens({"network", "service:approval"})
        assert "service:approval" in str(ei.value)

    def test_vocabulary_single_source_reexported_by_node_layer(self):
        """词表迁入 core（入口在 core 消费）；``plaita.node.capabilities`` 公开路径守恒
        且**同一对象**（前半测试 import 路径不变）。"""
        from plaita.node import capabilities as node_caps

        assert node_caps.EXECUTION_CAPABILITY_TOKENS is EXECUTION_CAPABILITY_TOKENS
        assert node_caps.SUSPEND_CAPABILITY_TOKENS is SUSPEND_CAPABILITY_TOKENS

    def test_frame_allowlist_single_source_with_scope_gate(self):
        from plaita.core import strategies

        assert strategies._SUSPEND_ALLOWED_FRAME_NODE_TYPES is SUSPEND_ALLOWED_FRAME_NODE_TYPES
        assert SUSPEND_ALLOWED_FRAME_NODE_TYPES == frozenset({"child", "reference"})


# ---------------------------------------------------------------------------
# 2. 递归闭包
# ---------------------------------------------------------------------------


class TestRecursiveClosure:
    def test_root_level_tokens(self):
        assert required_capabilities(_event_root()) == frozenset({"suspend:root", "event_bus"})
        assert required_capabilities(_http_root()) == frozenset({"network"})

    def test_nested_child_accumulates_tokens(self):
        """http(根) + child(event) + 孙 child(delay)：token 并集；recursive=False 只看根层。"""
        grandchild = {
            "flow_id": "gc",
            "version": "1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "gs", "next": "d"},
                {"type": "delay", "id": "d", "delay_seconds": 1, "next": "ge"},
                {"type": "end", "id": "ge", "output": "z"},
            ],
        }
        child = {
            "flow_id": "c",
            "version": "1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "cs", "next": "w"},
                {"type": "event", "id": "w", "event_type": "e", "next": "inner"},
                {"type": "child", "id": "inner", "childFlow": grandchild, "next": "ce"},
                {"type": "end", "id": "ce", "output": "y"},
            ],
        }
        root = _flow(
            [
                {"type": "start", "id": "start", "next": "h"},
                {"type": "http", "id": "h", "url": "http://example.invalid/", "method": "GET", "next": "call"},
                {"type": "child", "id": "call", "childFlow": child, "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
            flow_id="root",
            registry=_registry(DelayNode),
        )
        assert required_capabilities(root) == frozenset({"network", "suspend:root", "event_bus", "queue"})
        assert required_capabilities(root, recursive=False) == frozenset({"network"})

    def test_sites_carry_frame_path_root_to_leaf(self):
        grandchild = {
            "flow_id": "gc",
            "version": "1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "gs", "next": "w"},
                {"type": "event", "id": "w", "event_type": "e", "next": "ge"},
                {"type": "end", "id": "ge", "output": "z"},
            ],
        }
        child = {
            "flow_id": "c",
            "version": "1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "cs", "next": "inner"},
                {"type": "child", "id": "inner", "childFlow": grandchild, "next": "ce"},
                {"type": "end", "id": "ce", "output": "y"},
            ],
        }
        root = _flow(
            [
                {"type": "start", "id": "start", "next": "call"},
                {"type": "child", "id": "call", "childFlow": child, "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ]
        )
        report = check_execution_requirements(root, "normal")
        (site,) = report["sites"]
        assert site["frame_path"] == ["call(child)", "inner(child)", "w(event)"]
        assert site["flow_id"] == "gc"
        assert site["tokens"] == ["event_bus", "suspend:root"]
        assert report["missing"] == ["event_bus", "suspend:root"]

    def test_parallel_branch_flow_is_part_of_closure(self):
        root = _flow(
            [
                {"type": "start", "id": "start", "next": "par"},
                {
                    "type": "parallel",
                    "id": "par",
                    "joinBranches": ["b1"],
                    "branches": [{"name": "b1", "flow": EVENT_CHILD}],
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "x"},
            ]
        )
        assert required_capabilities(root) == frozenset({"suspend:root", "event_bus"})
        (site,) = check_execution_requirements(root, "normal")["sites"]
        assert site["frame_path"][0] == "par(parallel)"

    def test_undeclared_suspending_node_is_implicitly_suspend_root(self, monkeypatch):
        """过渡期（旋钮降级放行注册的空声明挂起节点）：闭包按 fail-closed 记 implicit
        ``suspend:root``，site.cause=undeclared——入口在 normal 下仍拒。"""

        class _Naked(Node):
            node_type = "r103_naked"
            is_suspending = True

            def execute(self, execution):
                return {"status": "pending"}

        monkeypatch.setenv("PLAITA_SUSPEND_GUARD", "warn")
        reg = _registry(_Naked)
        monkeypatch.delenv("PLAITA_SUSPEND_GUARD")
        flow = _flow(
            [
                {"type": "start", "id": "start", "next": "n"},
                {"type": "r103_naked", "id": "n", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
            registry=reg,
        )
        report = check_execution_requirements(flow, "normal")
        (site,) = report["sites"]
        assert site["cause"] == "undeclared"
        assert report["missing"] == ["suspend:root"]
        with pytest.raises(SuspendNotExecutableError) as ei:
            validate_for_profile(flow, "normal")
        assert ei.value.cause == "undeclared"

    def test_unresolved_reference_is_recorded_not_fatal(self):
        """reference 未注入 child_flow（调度器职责）：闭包记 unresolved、不当能力缺口。"""
        flow = _flow(
            [
                {"type": "start", "id": "start", "next": "ref"},
                {"type": "reference", "id": "ref", "flow_id": "other", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ]
        )
        report = validate_for_profile(flow, "normal")
        assert report["missing"] == []
        assert report["unresolved"] == [{"node_id": "ref", "flow_id": "other"}]

    def test_reference_resolver_hook_brings_referenced_flow_into_closure(self):
        flow = _flow(
            [
                {"type": "start", "id": "start", "next": "ref"},
                {"type": "reference", "id": "ref", "flow_id": "child1", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ]
        )
        resolved = Flow.model_validate(EVENT_CHILD)
        tokens = required_capabilities(flow, resolve_reference=lambda node: resolved)
        assert tokens == frozenset({"suspend:root", "event_bus"})


# ---------------------------------------------------------------------------
# 3. validate_for_profile：执行入口四路 + 语义
# ---------------------------------------------------------------------------


class TestEntryGate:
    def test_normal_entry_rejects_before_any_node_runs(self):
        """#331 与 #330 的区别钉：挂起点**之前**的节点也不执行（#330 运行期闸会先跑 before）。"""
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(_event_root(), False)
        assert CALLS == []
        body = ei.value.to_dict()
        assert body["stage"] == "entry"
        assert body["profile"] == "normal"
        assert body["required"] == ["event_bus", "suspend:root"]
        assert body["missing"] == ["event_bus", "suspend:root"]
        assert body["node_id"] == "wait"
        assert body["node_type"] == "event"
        assert body["flow_id"] == "root1"
        assert body["cause"] == "environment"
        assert body["fix"]["action"] == "run_distributed"
        assert body["fix"]["call"] is None  # 明确告知不可本地执行
        assert body["frame_path"] == ["wait(event)"]
        assert body["message"].startswith("这不是源码缺陷，不要删除/替换该节点")
        assert "'wait'" in body["message"] and "root1" in body["message"]
        assert body["error_type"] == "suspend_not_executable"

    def test_generator_entry_rejects_before_first_step(self):
        ex = FlowExecution()
        ex.mode = "generator"
        with pytest.raises(SuspendNotExecutableError) as ei:
            for _ in ex.run_compatible(_event_root(), True):
                pass
        assert CALLS == []
        assert ei.value.stage == "entry"
        assert ei.value.profile == "generator"

    def test_async_entry_twin_rejects(self):
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            asyncio.run(ex.arun_compatible(_event_root(), False))
        assert ei.value.stage == "entry"
        assert CALLS == []

    def test_run_classmethod_normal_rejects_and_distributed_accepts(self):
        with pytest.raises(SuspendNotExecutableError):
            FlowExecution.run(_approval_root())
        r = FlowExecution.run(_approval_root(), mode="distributed")
        assert r.get("is_suspend") is True

    def test_distributed_entry_accepts_and_parks(self):
        """验收：approval flow 在 normal 拒绝、在 distributed 跑通（挂起）。"""
        flow = _approval_root()
        with pytest.raises(SuspendNotExecutableError):
            flow.run()
        r = FlowExecution().run_distributed(flow, {})
        assert r.get("is_suspend") is True

    def test_distributed_entry_does_not_pre_empt_scope_gate(self):
        """守恒：distributed 入口只判 token——loop 帧内挂起放行，运行期作用域闸照抛
        ``frame boundary``（NS-M3 27 格的判据），不被入口改写成 suspend_not_executable。"""
        flow = _flow(
            [
                {"type": "start", "id": "start", "next": "loop"},
                {"type": "loop", "id": "loop", "collection": "$INPUT.items", "childFlow": EVENT_CHILD, "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ]
        )
        assert validate_for_profile(flow, "distributed")["missing"] == []
        with pytest.raises(Exception) as ei:
            FlowExecution().run_distributed(flow, {"items": [1]})
        assert "frame boundary" in str(ei.value)
        assert "suspend_not_executable" not in str(ei.value)

    def test_loop_frame_site_fix_is_restructure_at_entry(self):
        flow = _flow(
            [
                {"type": "start", "id": "start", "next": "loop"},
                {"type": "loop", "id": "loop", "collection": "$INPUT.items", "childFlow": EVENT_CHILD, "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ]
        )
        with pytest.raises(SuspendNotExecutableError) as ei:
            validate_for_profile(flow, "normal")
        fix = ei.value.fix
        assert fix["action"] == "restructure"
        assert "'loop'" in fix["message"]
        assert ei.value.frame_path == ["loop(loop)", "wait(event)"]

    def test_local_profile_accepts_approval_but_rejects_delay(self):
        """console local 后端（ADR-001 §3.4）：进程内事件总线可挂起/恢复 → approval 过；
        无持久队列+延迟排程 → delay 具名拒，missing 恰为 ``queue``。"""
        ex = FlowExecution()
        ex.execution_profile = "local"
        r = ex.run_distributed(_approval_root(), {})
        assert r.get("is_suspend") is True

        ex2 = FlowExecution()
        ex2.execution_profile = "local"
        with pytest.raises(Exception) as ei:
            ex2.run_distributed(_delay_root(), {})
        # run_distributed 对外契约归一为 FlowErrorException，但语义经 __cause__ 可辨
        cause = ei.value.__cause__
        assert isinstance(cause, SuspendNotExecutableError)
        assert cause.missing == ["queue"]
        assert cause.profile == "local"
        assert cause.node_type == "delay"

    def test_execution_profile_propagates_to_children(self):
        ex = FlowExecution()
        ex.execution_profile = "local"
        child = ex.get_child_execution()
        assert child.execution_profile == "local"

    def test_custom_profile_missing_network_is_execution_profile_error(self):
        """非挂起族缺口（network/sandbox）走独立错误类；内置三 profile 均含这两 token，
        仅自定义 profile 可见。"""
        with pytest.raises(ExecutionProfileError) as ei:
            validate_for_profile(_http_root(), frozenset())
        err = ei.value
        assert err.error_type is FlowErrorType.EXECUTION_PROFILE_UNSATISFIED
        body = err.to_dict()
        assert body["error_type"] == "execution_profile_unsatisfied"
        assert body["missing"] == ["network"]
        assert body["required"] == ["network"]
        assert body["sites"][0]["node_id"] == "h"
        assert body["flow_id"] == "http1"

    def test_builtin_profiles_satisfy_side_effect_family(self):
        for p in ("normal", "generator", "distributed", "local"):
            assert validate_for_profile(_http_root(), p)["missing"] == []

    def test_child_execution_skips_entry_gate_and_propagates_carrier(self):
        """子执行（parent 非空）不闸：distributed 根下的子执行里挂起节点照常
        ``execute`` 并以 carrier 原样上抛（NS-M3 传播半边守恒）。"""
        root = FlowExecution()
        root.mode = "distributed"
        child = root.get_child_execution()
        with pytest.raises(FlowSuspendedCarrier):
            child.run_compatible(Flow.model_validate(EVENT_CHILD), False)

    def test_check_is_non_raising_report(self):
        report = check_execution_requirements(_event_root(), "normal")
        assert set(report) == {"profile", "required", "missing", "sites", "unresolved"}
        assert report["profile"] == "normal"
        assert check_execution_requirements(_event_root(), "distributed")["missing"] == []

    def test_guard_warn_degrades_entry_to_warning(self, monkeypatch, caplog):
        """旋钮 warn：入口拒绝降 WARNING 放行，交回运行期闸/呈现（#330 warn 档=裸 carrier）。"""
        monkeypatch.setenv("PLAITA_SUSPEND_GUARD", "warn")
        ex = FlowExecution()
        ex.mode = "normal"
        with caplog.at_level(logging.WARNING):
            with pytest.raises(FlowSuspendedCarrier):
                ex.run_compatible(_event_root(), False)
        msgs = [r.message for r in caplog.records]
        assert any("suspend-guard=warn" in m and "entry" in m for m in msgs)
        # 降级后运行期闸仍在：挂起点之前的节点跑了、之后的没跑
        assert CALLS == ["before"]


# ---------------------------------------------------------------------------
# 4. NodeRunner 兜底断言（旁路面）
# ---------------------------------------------------------------------------


class TestRunnerGuard:
    def _flow(self):
        return _flow(
            [
                {"type": "start", "id": "start", "next": "s"},
                {"type": "r103_side_effect_suspend", "id": "s", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
            flow_id="bypass1",
            registry=_registry(_SideEffectSuspend),
        )

    def test_bypass_is_blocked_before_execute_with_warning(self, caplog):
        """直接驱动 ``_advance_one``（绕过入口与策略）：execute 前具名拒 + WARNING；
        副作用计数 0——这正是 hitl_await 形状在 normal 下要堵的洞。"""
        flow = self._flow()
        ex = FlowExecution()
        ex.mode = "normal"
        ex._ctx.setup_flow(flow, (), {})
        runner = NodeRunner(ex._ctx, node_execution=ex)
        node = flow.find_node_by_id("s")
        with caplog.at_level(logging.WARNING):
            with pytest.raises(SuspendNotExecutableError) as ei:
                asyncio.run(_advance_one(flow, runner, ex.callback_manager, node))
        assert CALLS == []
        assert ei.value.stage == "runner"
        assert ei.value.profile == "normal"
        assert ei.value.missing == ["event_bus", "suspend:root"]
        assert any("runner" in r.message and "suspend" in r.message.lower() for r in caplog.records)

    def test_distributed_host_lets_execute_run_then_carrier(self):
        """distributed 宿主：runner 放行 → execute 跑 → ``_advance_one`` 抛 carrier（不变）。"""
        flow = self._flow()
        ex = FlowExecution()
        ex.mode = "distributed"
        ex._ctx.setup_flow(flow, (), {})
        runner = NodeRunner(ex._ctx, node_execution=ex)
        node = flow.find_node_by_id("s")
        with pytest.raises(FlowSuspendedCarrier):
            asyncio.run(_advance_one(flow, runner, ex.callback_manager, node))
        assert CALLS == ["side_effect:s"]

    def test_runner_uses_root_host_profile_for_subflows(self):
        """含子流程：normal 根下直接推进 child 节点（旁路），子执行里的挂起节点在
        execute 前被 runner 闸拦——宿主身份取执行链的根。子流程节点用内置
        ``event``（嵌套 child_flow dict 经默认 registry 延迟解析，自定义桩
        node_type 不可见）。"""
        root = _flow(
            [
                {"type": "start", "id": "start", "next": "call"},
                {
                    "type": "child",
                    "id": "call",
                    "childFlow": EVENT_CHILD,
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "x"},
            ],
        )
        ex = FlowExecution()
        ex.mode = "normal"
        ex._ctx.setup_flow(root, (), {})
        runner = NodeRunner(ex._ctx, node_execution=ex)
        with pytest.raises(SuspendNotExecutableError) as ei:
            asyncio.run(_advance_one(root, runner, ex.callback_manager, root.find_node_by_id("call")))
        assert ei.value.stage == "runner"
        assert ei.value.profile == "normal"
        assert ei.value.node_id == "wait"
        assert ei.value.flow_id == "child1"
        assert CALLS == []

    def test_non_suspending_nodes_are_not_gated_by_runner(self):
        flow = _flow(
            [
                {"type": "start", "id": "start", "next": "c"},
                {"type": "r103_counter", "id": "c", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
            registry=_registry(_Counter),
        )
        ex = FlowExecution()
        ex.execution_profile = frozenset()  # 空能力集宿主
        ex._ctx.setup_flow(flow, (), {})
        runner = NodeRunner(ex._ctx, node_execution=ex)
        asyncio.run(_advance_one(flow, runner, ex.callback_manager, flow.find_node_by_id("c")))
        assert CALLS == ["c"]


# ---------------------------------------------------------------------------
# 5. capability_matrix → 治理账本（差分断言 = 承重墙）
# ---------------------------------------------------------------------------


class TestCapabilityMatrix:
    def test_matrix_shape_sorted_and_complete(self):
        reg = _registry(_SideEffectSuspend, _Counter)
        matrix = reg.capability_matrix()
        assert list(matrix) == sorted(matrix)
        assert matrix["r103_side_effect_suspend"] == ["event_bus", "suspend:root"]
        assert matrix["r103_counter"] == []  # 空声明也入矩阵：新增 node_type 必可见
        assert matrix["event"] == ["event_bus", "suspend:root"]
        assert set(matrix) == set(reg.list_types())

    def test_shipped_matrix_matches_governance_ledger(self):
        """差分断言主格：plaita 自带注册面（内置 + 本仓 entry points + opt-in code）
        的矩阵 == 账本 ``gov.capability_matrix``。新增/改声明不改账本 → 红。"""
        ledger = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))["gov"]["capability_matrix"]
        matrix = shipped_capability_matrix()
        assert diff_capability_matrix(ledger, matrix) == {}
        assert matrix == ledger

    def test_differential_detects_new_node_type_red_instance(self):
        """先红实例：注册表多出一个 node_type 而账本未变 → diff 点名 added。"""
        ledger = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))["gov"]["capability_matrix"]
        drifted = dict(shipped_capability_matrix())
        drifted["r103_new_node"] = ["network"]
        diff = diff_capability_matrix(ledger, drifted)
        assert diff == {"added": {"r103_new_node": ["network"]}}

    def test_differential_detects_declaration_change(self):
        ledger = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))["gov"]["capability_matrix"]
        drifted = dict(shipped_capability_matrix())
        drifted["event"] = ["event_bus"]
        diff = diff_capability_matrix(ledger, drifted)
        assert diff == {"changed": {"event": {"ledger": ["event_bus", "suspend:root"], "registry": ["event_bus"]}}}

    def test_shipped_matrix_covers_declared_families(self):
        matrix = shipped_capability_matrix()
        assert matrix["event"] == ["event_bus", "suspend:root"]
        assert matrix["approval"] == ["event_bus", "suspend:root"]
        assert matrix["delay"] == ["event_bus", "queue", "suspend:root"]
        assert matrix["http"] == ["network"]
        assert matrix["code"] == ["sandbox"]
        assert matrix["start"] == []

    def test_export_script_check_mode(self, tmp_path):
        """脚本 ``--check``：账本一致 rc=0；篡改账本副本 rc=1 且 stdout 点名差分。"""
        script = REPO_ROOT / "scripts" / "export_capability_matrix.py"
        env = {"PYTHONPATH": str(REPO_ROOT)}
        ok = subprocess.run(
            [sys.executable, str(script), "--check", "--ledger", str(LEDGER)],
            capture_output=True, text=True, env={**env, "PATH": ""},
        )
        assert ok.returncode == 0, ok.stdout + ok.stderr

        tampered = tmp_path / "architecture.yaml"
        data = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
        data["gov"]["capability_matrix"].pop("event")
        tampered.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
        red = subprocess.run(
            [sys.executable, str(script), "--check", "--ledger", str(tampered)],
            capture_output=True, text=True, env={**env, "PATH": ""},
        )
        assert red.returncode == 1
        assert "event" in red.stdout
