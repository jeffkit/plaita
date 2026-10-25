"""plaita#331 后半 — 执行台阶契约的消费面：环境校验 / 递归闭包 / NodeRunner 兜底 / 矩阵账本。

前半（#330 / B1 r89）落了 token 词表 + ``Node.execution_requirements`` + 注册三路同闸；
后半（#331 / B3 r103）落了消费面四件事。**2026-10-25 台阶化（owner 裁决）**：token 词表
退役为三值台阶（``inline < managed < managed:queue``，两语义模型）——判定从「token 集
合 ⊆」变为「台阶全序比较」，本文件全体断言按台阶口径重写：

1. ``validate_for_profile(flow, profile)`` **独立于 ``model_validate``**，只在执行入口
   （Normal / Generator / Distributed 三路 + ``FlowExecution.run`` 分派）与 console
   publish/start 调用；环境台阶 = 模式推导（normal/generator→inline、distributed→
   managed:queue）或 console 显式档（local→managed）。入口拒绝发生在**任何节点执行前**
   （与 #330 运行期「执行到挂起点才拒」的区别钉）。**睡死坑修复钉**：approval 归
   ``managed:queue`` 族——旧 local 档（=managed）下从「放行后睡死」反转为「发布/受理即拒」。
2. 递归闭包：child / loop 族 / parallel 分支 + reference 引用的 flow，
   ``required_tier(flow, recursive=True)`` = 站点台阶的 max；帧路径根→叶。
3. NodeRunner 在每个托管节点 ``execute`` **前**断言宿主契约——覆盖绕过 strategies 的
   旁路；与入口共用同一判定函数，触达即入口漏了：具名错误 + WARNING，不静默。
4. ``registry.capability_matrix()``（``node_type → 台阶``）→ 治理账本
   ``docs/governance/architecture.yaml`` ``gov.capability_matrix``；差分断言「注册表
   新增 node_type 而矩阵未变 → 红」。

守恒：distributed（managed:queue）入口对 loop/parallel 帧内挂起**放行**（只判台阶不判
帧类型），运行期作用域闸（NS-M3，``frame boundary``）照抛；子执行（parent 非空）不闸、
carrier 原样上抛。``ExecutionProfileError`` 在台阶路径下不再产出（保留给非台阶维度）。
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
    EXECUTION_TIERS,
    LEGACY_CAPABILITY_TOKENS,
    SUSPEND_ALLOWED_FRAME_NODE_TYPES,
    check_execution_requirements,
    diff_capability_matrix,
    normalize_profile,
    profile_for_mode,
    required_tier,
    tier_rank,
    validate_for_profile,
)
from plaita.core.errors import (
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
    """有外部副作用的挂起节点（hitl_await 形状：execute 先登记再 pending）。

    台阶化后走 legacy 折算声明（迁移兼容路径的活样例）。"""

    node_type = "r103_side_effect_suspend"
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
# 1. 台阶表与环境别名（两语义模型）
# ---------------------------------------------------------------------------


class TestTierTable:
    def test_tier_ladder_is_total_order(self):
        """两语义终版（owner 第二轮裁决）：只有两级，managed:queue 子档废除——
        「唤醒源是哪个岗」= wake_service 接线 + 点名闸，不进模式词汇。"""
        assert EXECUTION_TIERS == ("inline", "managed")
        assert [tier_rank(t) for t in EXECUTION_TIERS] == [0, 1]

    def test_legacy_names_map_per_adr_001(self):
        """旧四 profile 名 → 台阶：normal/generator=inline；local/distributed
        同折 managed（distributed 曾折 managed:queue，子档废除后并级）。"""
        assert normalize_profile("normal") == "inline"
        assert normalize_profile("generator") == "inline"
        assert normalize_profile("local") == "managed"
        assert normalize_profile("distributed") == "managed"

    def test_legacy_token_sets_collapse_to_tiers(self):
        """旧 token 集合 → 台阶折算：sandbox/network 从不区分宿主（inline）；
        suspend:root/nested、event_bus、queue 同折 managed——旧 queue 家族的
        「要配套服务」半边由 wake_service 接线 + 点名闸承接，不进台阶。"""
        assert normalize_profile({"network", "sandbox"}) == "inline"
        assert normalize_profile({"network"}) == "inline"
        assert normalize_profile(frozenset()) == "inline"
        assert normalize_profile({"suspend:root"}) == "managed"
        assert normalize_profile({"suspend:nested", "event_bus"}) == "managed"
        assert normalize_profile({"suspend:root", "event_bus", "queue"}) == "managed"

    def test_profile_for_mode(self):
        assert profile_for_mode(None) == "inline"
        assert profile_for_mode(ExecutionMode.NORMAL) == "inline"
        assert profile_for_mode(ExecutionMode.GENERATOR) == "inline"
        assert profile_for_mode(ExecutionMode.DISTRIBUTED) == "managed"
        assert profile_for_mode("distributed") == "managed"

    def test_custom_profile_rejects_unknown_token(self):
        """fail-closed 守恒（旧 #331 口径）：自定义 token 集合含未知值具名拒——
        防拼写错误静默折算成 inline 放行。"""
        with pytest.raises(ValueError) as ei:
            normalize_profile({"network", "service:approval"})
        assert "service:approval" in str(ei.value)

    def test_unknown_profile_is_named_error(self):
        with pytest.raises(ValueError) as ei:
            normalize_profile("staging")
        assert "staging" in str(ei.value)
        assert "managed" in str(ei.value)  # 合法台阶点名

    def test_vocabulary_single_source_reexported_by_node_layer(self):
        """台阶表住 core（入口在 core 消费）；``plaita.node.capabilities`` 公开路径
        守恒且**同一对象**（前半测试 import 路径不变）。"""
        from plaita.node import capabilities as node_caps

        assert node_caps.EXECUTION_TIERS is EXECUTION_TIERS
        assert node_caps.LEGACY_CAPABILITY_TOKENS is LEGACY_CAPABILITY_TOKENS

    def test_frame_allowlist_single_source_with_scope_gate(self):
        from plaita.core import strategies

        assert strategies._SUSPEND_ALLOWED_FRAME_NODE_TYPES is SUSPEND_ALLOWED_FRAME_NODE_TYPES
        assert SUSPEND_ALLOWED_FRAME_NODE_TYPES == frozenset({"child", "reference"})


# ---------------------------------------------------------------------------
# 2. 递归闭包
# ---------------------------------------------------------------------------


class TestRecursiveClosure:
    def test_root_level_tiers(self):
        assert required_tier(_event_root()) == "managed"
        assert required_tier(_http_root()) == "inline"

    def test_nested_child_takes_max_tier(self):
        """http(根) + child(event) + 孙 child(delay)：台阶取 max=managed:queue；
        recursive=False 只看根层（http → inline）。"""
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
        assert required_tier(root) == "managed"
        assert required_tier(root, recursive=False) == "inline"

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
        report = check_execution_requirements(root, "inline")
        (site,) = report["sites"]
        assert site["frame_path"] == ["call(child)", "inner(child)", "w(event)"]
        assert site["flow_id"] == "gc"
        assert site["tier"] == "managed"
        assert report["missing"] == ["managed"]

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
        assert required_tier(root) == "managed"
        (site,) = check_execution_requirements(root, "inline")["sites"]
        assert site["frame_path"][0] == "par(parallel)"

    def test_undeclared_suspending_node_is_implicitly_managed(self, monkeypatch):
        """过渡期（旋钮降级放行注册的裸声明挂起节点）：闭包按 fail-closed 记
        implicit ``managed``，site.cause=undeclared——入口在 inline 下仍拒。"""

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
        report = check_execution_requirements(flow, "inline")
        (site,) = report["sites"]
        assert site["cause"] == "undeclared"
        assert site["tier"] == "managed"
        assert report["missing"] == ["managed"]
        with pytest.raises(SuspendNotExecutableError) as ei:
            validate_for_profile(flow, "inline")
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
        report = validate_for_profile(flow, "inline")
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
        tier = required_tier(flow, resolve_reference=lambda node: resolved)
        assert tier == "managed"


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
        assert body["profile"] == "inline"
        assert body["required"] == ["managed"]
        assert body["missing"] == ["managed"]
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
        assert ei.value.profile == "inline"

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
        """验收：approval flow 在 inline（normal）拒绝、在托管面（distributed 模式根
        默认 managed:queue）跑通（挂起）。"""
        flow = _approval_root()
        with pytest.raises(SuspendNotExecutableError):
            flow.run()
        r = FlowExecution().run_distributed(flow, {})
        assert r.get("is_suspend") is True

    def test_distributed_entry_does_not_pre_empt_scope_gate(self):
        """守恒：managed:queue 入口只判台阶——loop 帧内挂起放行，运行期作用域闸照抛
        ``frame boundary``（NS-M3 27 格的判据），不被入口改写成 suspend_not_executable。"""
        flow = _flow(
            [
                {"type": "start", "id": "start", "next": "loop"},
                {"type": "loop", "id": "loop", "collection": "$INPUT.items", "childFlow": EVENT_CHILD, "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ]
        )
        assert validate_for_profile(flow, "managed")["missing"] == []
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
            validate_for_profile(flow, "inline")
        fix = ei.value.fix
        assert fix["action"] == "restructure"
        assert "'loop'" in fix["message"]
        assert ei.value.frame_path == ["loop(loop)", "wait(event)"]

    def test_queue_family_passes_tier_gate_wake_service_wired(self):
        """两级模型钉（owner 第二轮裁决）：delay/approval 台阶=managed——托管
        环境下**台阶闸放行**（不再有 managed:queue 子档拒绝）；「唤醒服务在
        不在岗」由 wake_service 接线 + 受理门点名承接（console 面测试，
        test_queue_service_presence_probe / test_issue331_execution_profile_
        gate 的本地静态拒）。节点 wake_service 接线在此钉。"""
        from plaita.core.capabilities import WAKE_SERVICE_NODE_TYPES
        for flow, node_type, svc in (
            (_approval_root(), "approval", "approval"),
            (_delay_root(), "delay", "delay"),
        ):
            assert validate_for_profile(flow, "managed", stage="publish")["missing"] == []
            with pytest.raises(SuspendNotExecutableError):
                validate_for_profile(flow, "inline", stage="publish")
            assert node_type in WAKE_SERVICE_NODE_TYPES
        assert DelayNode.wake_service == "delay"
        assert ApprovalNode.wake_service == "approval"

    def test_execution_profile_propagates_to_children(self):
        ex = FlowExecution()
        ex.execution_profile = "managed"
        child = ex.get_child_execution()
        assert child.execution_profile == "managed"

    def test_builtin_environments_satisfy_side_effect_family(self):
        """http（inline 台阶）在所有内置环境（含旧名与空 token 集合）下恒过。"""
        for p in ("inline", "managed", "normal", "generator", "distributed", "local", frozenset()):
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
        report = check_execution_requirements(_event_root(), "inline")
        assert set(report) == {"profile", "required", "missing", "sites", "unresolved"}
        assert report["profile"] == "inline"
        assert report["required"] == "managed"
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
        副作用计数 0——这正是 hitl_await 形状在 inline 下要堵的洞。"""
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
        assert ei.value.profile == "inline"
        assert ei.value.missing == ["managed"]
        assert any("runner" in r.message and "suspend" in r.message.lower() for r in caplog.records)

    def test_distributed_host_lets_execute_run_then_carrier(self):
        """managed:queue 宿主：runner 放行 → execute 跑 → ``_advance_one`` 抛 carrier（不变）。"""
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
        """含子流程：inline 根下直接推进 child 节点（旁路），子执行里的挂起节点在
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
        assert ei.value.profile == "inline"
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
        ex.execution_profile = frozenset()  # 空 token 集合宿主 → inline
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
        # legacy 折算声明 → managed
        assert matrix["r103_side_effect_suspend"] == "managed"
        assert matrix["r103_counter"] == "inline"  # inline 也入矩阵：新增 node_type 必可见
        assert matrix["event"] == "managed"
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
        drifted["r103_new_node"] = "inline"
        diff = diff_capability_matrix(ledger, drifted)
        assert diff == {"added": {"r103_new_node": "inline"}}

    def test_differential_detects_declaration_change(self):
        ledger = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))["gov"]["capability_matrix"]
        drifted = dict(shipped_capability_matrix())
        drifted["event"] = "inline"
        diff = diff_capability_matrix(ledger, drifted)
        assert diff == {"changed": {"event": {"ledger": "managed", "registry": "inline"}}}

    def test_shipped_matrix_covers_declared_families(self):
        matrix = shipped_capability_matrix()
        assert matrix["event"] == "managed"
        # 两级模型：队列族台阶=managed，唤醒源在 wake_service 字段（不在矩阵）
        assert matrix["approval"] == "managed"
        assert matrix["delay"] == "managed"
        assert matrix["http"] == "inline"
        assert matrix["code"] == "inline"
        assert matrix["start"] == "inline"

    def test_wake_service_wiring_on_queue_family(self):
        """唤醒源接线单源钉：队列族 wake_service=service_type 同名；event 无
        唤醒源（走通用事件总线）。"""
        assert DelayNode.wake_service == "delay"
        assert ApprovalNode.wake_service == "approval"
        from plaita.server.nodes.http_callback_node import HttpCallbackNode
        from plaita.server.nodes.redis_queue_node import RedisQueueNode
        from plaita.server.nodes.kafka_queue_node import KafkaQueueNode
        assert HttpCallbackNode.wake_service == "http_callback"
        assert RedisQueueNode.wake_service == "redis_queue"
        assert KafkaQueueNode.wake_service == "kafka_queue"
        from plaita.node.event_node import EventNode
        assert EventNode.wake_service is None

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
