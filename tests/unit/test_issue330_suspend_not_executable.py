"""plaita#330 — 挂起节点 normal/generator 根出口的结构化 fail-closed 呈现 + 注册期不变量。

NS-M3（#311）已把 `_advance_one` 闸落地（is_suspending → ``FlowSuspendedCarrier``，
Normal/Generator/flow_run/子流程四路结构全覆盖）；本单残余（#330 评论区 A2 评审）：

1. **呈现面**：模式根出口（parent is None）把 carrier 身份转换为
   :class:`SuspendNotExecutableError`（独立异常类，非新抛点——闸点维持 carrier，
   传播半边不动）；错误体 {error_type, node_id, node_type, source_line, flow_id,
   cause, fix, frame_path}，首行写死「这不是源码缺陷，不要删除/替换该节点」。
2. **flow_run 呈现**（plaita-ai 侧，另一文件）。
3. **注册期不变量**：``is_suspending=True ⇒ execution_requirements 非空且含
   suspend:* token``；词表 5 token（suspend:root|nested / event_bus / queue /
   sandbox / network）。
4. **回滚旋钮**：``PLAITA_SUSPEND_GUARD=warn`` 只降本单新增两半边（呈现保持
   carrier + WARNING、注册不变量降 WARNING），NS-M3 运行期闸不受影响。

三路负控（Normal / flow_run / 子流程）+ Generator 各 1-2 格 fail-closed 先红后绿；
NS-M3 既有传播面守恒格（distributed child 停挂 / 迭代帧具名拒）引既有 27 格，
本文件另置 2 格轻量守恒。
"""
from __future__ import annotations

import logging

import pytest

from plaita.core.errors import (
    FlowErrorType,
    FlowSuspendedCarrier,
    SuspendNotExecutableError,
)
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.node import NodeRegistry
from plaita.node.basic import Node
from plaita.server.nodes.approval_node import ApprovalNode
from plaita.server.nodes.delay_node import DelayNode


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------

#: 副作用执行计数（挂起点之后的节点绝不能跑——#330 验收第 6 条负控形制）
AFTER_CALLS: list[str] = []


class _CounterNode(Node):
    node_type = "r89_counter"
    node_name = "计数桩"
    dry_run_safe = True

    def execute(self, execution):
        AFTER_CALLS.append(self.id)
        return "counted"


def _fresh_registry(*extra) -> NodeRegistry:
    reg = NodeRegistry()
    for cls in extra:
        reg.register(cls)
    return reg


def _event_root_flow():
    return Flow.model_validate(
        {
            "flow_id": "s1",
            "version": "1",
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "wait"},
                {"type": "event", "id": "wait", "event_type": "approve_me", "next": "after"},
                {"type": "r89_counter", "id": "after", "next": "end"},
                {"type": "end", "id": "end", "output": "x"},
            ],
        },
        registry=_fresh_registry(_CounterNode),
    )


@pytest.fixture(autouse=True)
def _reset_counter():
    AFTER_CALLS.clear()
    yield
    AFTER_CALLS.clear()


# ---------------------------------------------------------------------------
# 1. Normal 根：结构化具名失败（呈现面）
# ---------------------------------------------------------------------------


class TestRootNormalPresentation:
    def test_event_node_named_error_structured_body(self):
        """approval 家族代表（event）在 normal 根：SuspendNotExecutableError +
        完整结构化错误体。"""
        flow = _event_root_flow()
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False)
        err = ei.value
        body = err.to_dict()
        assert body["error_type"] == "suspend_not_executable"
        assert body["node_id"] == "wait"
        assert body["node_type"] == "event"
        assert body["flow_id"] == "s1"
        assert body["cause"] == "environment"
        assert body["fix"]["action"] == "run_distributed"
        assert body["frame_path"][-1] == "wait(event)"
        assert err.error_type is FlowErrorType.SUSPEND_NOT_EXECUTABLE
        assert err.source_line is None  # JSON 前端无行号回标

    def test_message_first_line_pin(self):
        """首行写死「这不是源码缺陷」——防 AI 作者误删挂起节点（#330 规格 1）。"""
        flow = _event_root_flow()
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False)
        assert ei.value.to_dict()["message"].startswith(
            "这不是源码缺陷，不要删除/替换该节点"
        )
        assert "'wait'" in str(ei.value)

    def test_conversion_preserves_carrier_as_cause(self):
        """转换是呈现层身份转换：闸点抛的原 carrier 挂在 ``__cause__``。"""
        flow = _event_root_flow()
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False)
        assert isinstance(ei.value.__cause__, FlowSuspendedCarrier)

    @pytest.mark.parametrize(
        "extra,node_type",
        [(ApprovalNode, "approval"), (DelayNode, "delay")],
    )
    def test_extended_family_named_error(self, extra, node_type):
        """approval/delay（server 扩展族）同面：具名失败 + node_type 回标。"""
        node = (
            {
                "type": node_type,
                "id": "wait",
                "event_type": "ext_resume",
                "approval_title": "t",
                "approval_content": "c",
                "approvers": ["u1"],
                "delay_seconds": 5,
                "next": "end",
            },
        )[0]
        flow = Flow.model_validate(
            {
                "flow_id": "sx",
                "version": "1",
                "runtime": "python",
                "nodes": [
                    {"type": "start", "id": "start", "next": "wait"},
                    node,
                    {"type": "end", "id": "end", "output": "x"},
                ],
            },
            registry=_fresh_registry(extra),
        )
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False)
        assert ei.value.to_dict()["node_type"] == node_type

    def test_side_effect_after_suspend_point_never_runs(self):
        """负控：挂起点之后的副作用节点执行计数 = 0（拒绝发生在推进前）。"""
        flow = _event_root_flow()
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError):
            ex.run_compatible(flow, False)
        assert AFTER_CALLS == []


class TestRootGeneratorPresentation:
    def test_generator_root_named_failure(self):
        """generator（debug）根：与 Go ExecuteGenerator 的 Step{Error} 对齐——
        具名失败，绝不 yield 挂起步、绝不落到成功分支（A2 评审 §2 采 Throw）。"""
        flow = _event_root_flow()
        ex = FlowExecution()
        ex.mode = "generator"
        with pytest.raises(SuspendNotExecutableError):
            for _step in ex.run_compatible(flow, True):
                pass

    def test_flow_run_entry_named_failure(self):
        """``flow.run()``（flow_run 的底层入口）同一具名失败。"""
        flow = _event_root_flow()
        with pytest.raises(SuspendNotExecutableError):
            flow.run()


# ---------------------------------------------------------------------------
# 2. 子流程三路：child / loop 帧（normal 根）
# ---------------------------------------------------------------------------

_CHILD_WITH_EVENT = {
    "flow_id": "child1",
    "version": "1",
    "runtime": "python",
    "nodes": [
        {"type": "start", "id": "cstart", "next": "cwait"},
        {"type": "event", "id": "cwait", "event_type": "inner", "next": "cend"},
        {"type": "end", "id": "cend", "output": "y"},
    ],
}


class TestSubflowNormalPresentation:
    def _root_with_child(self):
        return Flow.model_validate(
            {
                "flow_id": "R",
                "version": "1",
                "runtime": "python",
                "nodes": [
                    {"type": "start", "id": "start", "next": "call"},
                    {"type": "child", "id": "call", "childFlow": _CHILD_WITH_EVENT, "next": "end"},
                    {"type": "end", "id": "end", "output": "x"},
                ],
            },
            registry=_fresh_registry(_CounterNode),
        )

    def test_child_event_named_error_frame_path(self):
        """child 内挂起：normal 根具名失败；frame_path 根→叶点名 call(child)。"""
        flow = self._root_with_child()
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False)
        body = ei.value.to_dict()
        assert body["node_id"] == "cwait"
        assert body["flow_id"] == "child1"
        assert body["fix"]["action"] == "run_distributed"
        assert body["frame_path"] == ["call(child)", "cwait(event)"]

    def test_child_side_effect_count_zero(self):
        flow = self._root_with_child()
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError):
            ex.run_compatible(flow, False)
        assert AFTER_CALLS == []

    def test_loop_frame_error_fix_restructure(self):
        """loop 迭代帧内挂起：normal 根同样具名失败，且 fix 点名 restructure——
        分布式也救不了（作用域闸 v1 只放 child/reference）。"""
        flow = Flow.model_validate(
            {
                "flow_id": "loop1",
                "version": "1",
                "runtime": "python",
                "nodes": [
                    {"type": "start", "id": "start", "next": "loop"},
                    {
                        "type": "loop",
                        "id": "loop",
                        "collection": "$INPUT.items",
                        "childFlow": _CHILD_WITH_EVENT,
                        "next": "end",
                    },
                    {"type": "end", "id": "end", "output": "x"},
                ],
            },
        )
        ex = FlowExecution()
        ex.mode = "normal"
        with pytest.raises(SuspendNotExecutableError) as ei:
            ex.run_compatible(flow, False, items=[1, 2])
        body = ei.value.to_dict()
        assert body["fix"]["action"] == "restructure"
        assert "'loop'" in body["fix"]["message"]


# ---------------------------------------------------------------------------
# 3. NS-M3 传播面守恒（轻量 2 格；27 格全集在 test_nested_suspend.py）
# ---------------------------------------------------------------------------


class TestDistributedConserved:
    def test_distributed_child_event_still_parks(self):
        """三态验收 #1：distributed + child/reference → 真挂起（NS-M3 主路径）。"""
        flow = Flow.model_validate(
            {
                "flow_id": "DR",
                "version": "1",
                "runtime": "python",
                "nodes": [
                    {"type": "start", "id": "start", "next": "call"},
                    {"type": "child", "id": "call", "childFlow": _CHILD_WITH_EVENT, "next": "end"},
                    {"type": "end", "id": "end", "output": "x"},
                ],
            }
        )
        r1 = FlowExecution().run_distributed(flow, {})
        assert r1.get("is_suspend") is True

    def test_distributed_loop_frame_still_named_rejected(self):
        """三态验收 #2：distributed + 迭代帧 → FlowSuspendRejectedError 具名拒。"""
        flow = Flow.model_validate(
            {
                "flow_id": "loop2",
                "version": "1",
                "runtime": "python",
                "nodes": [
                    {"type": "start", "id": "start", "next": "loop"},
                    {
                        "type": "loop",
                        "id": "loop",
                        "collection": "$INPUT.items",
                        "childFlow": _CHILD_WITH_EVENT,
                        "next": "end",
                    },
                    {"type": "end", "id": "end", "output": "x"},
                ],
            }
        )
        with pytest.raises(Exception) as ei:
            FlowExecution().run_distributed(flow, {"items": [1]})
        assert "frame boundary" in str(ei.value)


# ---------------------------------------------------------------------------
# 4. 注册期不变量（#330 §4 / #331 前半）
# ---------------------------------------------------------------------------


class _NakedSuspend(Node):
    node_type = "r89_naked_suspend"
    is_suspending = True

    def execute(self, execution):
        return {"status": "pending"}


class _DeclaredSuspend(Node):
    node_type = "r89_declared_suspend"
    is_suspending = True

    def execute(self, execution):
        return {"status": "pending"}


class TestRegistrationInvariant:
    def test_naked_suspending_node_rejected_at_register(self):
        """is_suspending=True 且无 execution_requirements：注册即红（HitlAwaitNode
        形状的通用防线；registry.register / 插件 discover 两路同闸）。"""
        reg = NodeRegistry()
        with pytest.raises(Exception) as ei:
            reg.register(_NakedSuspend)
        assert "r89_naked_suspend" in str(ei.value)
        assert "suspend" in str(ei.value)

    def test_unknown_token_rejected(self):
        class _Typo(Node):
            node_type = "r89_typo"
            is_suspending = True

            def execute(self, execution):
                return {"status": "pending"}

        _Typo.execution_requirements = ("suspend:root", "event_bas")
        reg = NodeRegistry()
        with pytest.raises(Exception) as ei:
            reg.register(_Typo)
        assert "event_bas" in str(ei.value)

    def test_declared_suspending_node_registers(self, ):
        _DeclaredSuspend.execution_requirements = ("suspend:root", "event_bus")
        reg = NodeRegistry()
        reg.register(_DeclaredSuspend)
        assert reg.get("r89_declared_suspend") is _DeclaredSuspend

    def test_builtin_suspending_family_declares(self):
        """内置挂起族已声明（词表声明半边）：非空且含 suspend token。"""
        from plaita.node import get_default_registry
        from plaita.node.capabilities import SUSPEND_CAPABILITY_TOKENS

        registry = get_default_registry()
        for node_type in ("event",):
            cls = registry.get(node_type)
            reqs = tuple(cls.execution_requirements)
            assert reqs, f"{node_type} 缺 execution_requirements"
            assert set(reqs) & SUSPEND_CAPABILITY_TOKENS

    def test_extended_family_declares(self):
        for cls in (ApprovalNode, DelayNode):
            reqs = tuple(cls.execution_requirements)
            assert reqs, f"{cls.node_type} 缺 execution_requirements"

    def test_side_effect_family_declares(self):
        """副作用族代表声明 network/sandbox token（HTTP/Redis/Code）。

        redis/code 不在默认 registry（code 是 opt-in、redis 非内置），按类断言。
        """
        from plaita.node import get_default_registry
        from plaita.node.code import CodeNode
        from plaita.node.redis import RedisNode

        registry = get_default_registry()
        assert registry.get("http").execution_requirements == ("network",)
        assert RedisNode.execution_requirements == ("network",)
        assert CodeNode.execution_requirements == ("sandbox",)

    def test_plugin_discovery_records_load_error_for_contract_violation(self, monkeypatch):
        """entry_points 插件路径：契约违规记入 _load_errors（#98 失败现场形制），
        不炸整个发现过程。"""
        class _FakeEP:
            name = "r89_bad_plugin"

            def load(self):
                return _NakedSuspend

        import plaita.node as node_pkg

        # py3.10+ 形制：entry_points(group=...) 直接返回 ep 元组。
        monkeypatch.setattr(node_pkg, "entry_points", lambda **kwargs: [_FakeEP()])
        reg = NodeRegistry()
        reg.discover()
        assert "r89_bad_plugin" in reg._load_errors
        assert reg.get("r89_naked_suspend") is None

    def test_guard_warn_degrades_registration_to_warning(self, monkeypatch, caplog):
        monkeypatch.setenv("PLAITA_SUSPEND_GUARD", "warn")
        reg = NodeRegistry()
        with caplog.at_level(logging.WARNING, logger="plaita.node.capabilities"):
            reg.register(_NakedSuspend)
        assert reg.get("r89_naked_suspend") is _NakedSuspend
        assert any("r89_naked_suspend" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 5. 回滚旋钮（#330 §7 收窄形态，A2 评审 §5）
# ---------------------------------------------------------------------------


class TestSuspendGuardKnob:
    def test_warn_keeps_carrier_presentation_with_warning(self, monkeypatch, caplog):
        """PLAITA_SUSPEND_GUARD=warn：呈现半边回滚为 NS-M3 原样（裸 carrier），
        只多一条 WARNING。运行期闸本身不受影响（照抛）。"""
        monkeypatch.setenv("PLAITA_SUSPEND_GUARD", "warn")
        flow = _event_root_flow()
        ex = FlowExecution()
        ex.mode = "normal"
        with caplog.at_level(logging.WARNING):
            with pytest.raises(FlowSuspendedCarrier):
                ex.run_compatible(flow, False)
        assert any("suspend" in r.message.lower() for r in caplog.records)

    def test_invalid_value_falls_back_strict(self, monkeypatch, caplog):
        """非法旋钮值：WARNING + 按严格档处理（与 #151 env 投影纪律同形）。"""
        monkeypatch.setenv("PLAITA_SUSPEND_GUARD", "nonsense")
        flow = _event_root_flow()
        ex = FlowExecution()
        ex.mode = "normal"
        with caplog.at_level(logging.WARNING):
            with pytest.raises(SuspendNotExecutableError):
                ex.run_compatible(flow, False)
