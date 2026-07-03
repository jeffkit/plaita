"""P0-1 回归：Flow.parse_flow 不再隐式耦合默认 registry。

历史上 ``Flow.parse_flow`` 这个 ``model_validator(mode="before")`` 内部调用
``get_default_registry().parse_node`` 把节点 dict 解析成 ``Node`` 子类。这让
Flow 解析隐式依赖模块级单例 registry 的当前状态——import 期注册了有 bug 的
节点会让全进程解析任何 flow 都出错，且 Pydantic validator 产生了副作用。

现在节点解析改为显式 ``Flow.resolve_nodes``，``model_validate`` 默认自动调一次。
本文件钉死这些行为。
"""
from __future__ import annotations

import pytest

from plaita.core.flow import Flow
from plaita.node import NodeRegistry, get_default_registry, register_code_node


ECHO = """
{
    "flow_id": "echo",
    "nodes": [
        {"type": "start", "id": "start", "next": "end"},
        {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"}
    ]
}
"""


class TestParseFlowDoesNotTouchRegistry:
    def test_flow_constructs_when_registry_parse_node_is_broken(self, monkeypatch):
        """``Flow.model_validate`` 走默认 registry 解析节点，但若解析**失败**
        应来自 ``resolve_nodes`` 阶段而非 validator 阶段。用一个解析必抛的
        registry 注入 ``model_validate``，确认报错来自解析步骤、且 Flow
        的纯结构部分 (flow_id / nodes 数量) 在解析前已就绪。"""
        class BrokenRegistry:
            def parse_node(self, node_dict):
                raise RuntimeError("registry broken on purpose")

        # 用坏 registry 解析应抛
        with pytest.raises(RuntimeError, match="registry broken"):
            Flow.model_validate(
                {"flow_id": "x", "nodes": [{"type": "start", "id": "s", "next": "e"},
                                           {"type": "end", "id": "e"}]},
                registry=BrokenRegistry(),
            )

    def test_parse_flow_validator_runs_without_registry(self, monkeypatch):
        """把默认 registry 的 parse_node 替换成抛错——如果 parse_flow 仍调它,
        ``Flow.model_validate`` 会抛; 现在解析在 resolve_nodes, 用自定义好
        registry 仍能成功。"""
        reg = get_default_registry()
        monkeypatch.setattr(reg, "parse_node", lambda d: (_ for _ in ()).throw(RuntimeError("must not be called")))

        # 用一个独立的好 registry 解析, 不触发改坏的默认 registry
        fresh = NodeRegistry()
        flow = Flow.model_validate(
            {"flow_id": "x", "nodes": [{"type": "start", "id": "s", "next": "e"},
                                       {"type": "end", "id": "e"}]},
            registry=fresh,
        )
        assert flow.flow_id == "x"
        assert len(flow.nodes) == 2
        assert flow.nodes[0].node_type == "start"


class TestResolveNodes:
    def test_resolve_nodes_is_idempotent(self):
        flow = Flow.from_string(ECHO)
        first_ids = [n.id for n in flow.nodes]
        flow.resolve_nodes()  # second call should be a no-op
        flow.resolve_nodes()
        assert [n.id for n in flow.nodes] == first_ids
        # nodes are typed Node instances, not dicts
        assert all(not isinstance(n, dict) for n in flow.nodes)

    def test_resolve_nodes_with_custom_registry(self):
        """自定义 registry 能注入并解析——支持隔离 registry 场景。"""
        fresh = NodeRegistry()
        flow = Flow.model_validate(
            {"flow_id": "x", "nodes": [{"type": "start", "id": "s", "next": "e"},
                                       {"type": "end", "id": "e"}]},
            registry=fresh,
        )
        assert flow.nodes[0].node_type == "start"

    def test_unresolved_flow_has_dict_nodes(self):
        """直接 super().model_validate 绕过 resolve_nodes 时节点保持 dict——
        证明解析确实从 validator 移出。"""
        flow = super(Flow, Flow).model_validate(
            {"flow_id": "x", "nodes": [{"type": "start", "id": "s", "next": "e"}]}
        )
        assert isinstance(flow.nodes[0], dict)
        # resolve 后变成 Node
        flow.resolve_nodes()
        assert not isinstance(flow.nodes[0], dict)


class TestExecutionResolvesDictNodes:
    def test_flow_execution_resolves_dict_nodes_before_run(self):
        """直接 ``Flow(...)`` 构造的 dict 节点 flow, 经 FlowExecution.run 仍能跑
        ——_ensure_flow_resolved 兜底解析。"""
        flow = super(Flow, Flow).model_validate(
            {"flow_id": "echo-direct",
             "nodes": [{"type": "start", "id": "start", "next": "end"},
                       {"type": "end", "id": "end", "output": "$INPUT.name",
                        "resultType": "success"}]}
        )
        assert isinstance(flow.nodes[0], dict)  # 未解析
        result = flow.run(name="kongjie")
        assert result == "kongjie"
        # 跑完后已被解析
        assert not isinstance(flow.nodes[0], dict)


class TestCodeNodeOptInPreserved:
    def test_code_node_still_requires_opt_in(self):
        """CodeNode 不在默认 registry: 解析 ``type: code`` 仍报错并带指引。
        行为与 0.4.x 一致, P0-1 不改变安全边界。测试自隔离 registry 状态,
        不依赖其他测试是否注册过 CodeNode。"""
        reg = get_default_registry()
        reg.unregister("code")  # 确保起始未注册
        with pytest.raises(RuntimeError, match="unRecognized node type: code"):
            Flow.model_validate(
                {"flow_id": "x", "nodes": [{"type": "start", "id": "s", "next": "c"},
                                           {"type": "code", "id": "c",
                                            "code": "def run(input):\n    return input",
                                            "next": "e"},
                                           {"type": "end", "id": "e"}]}
            )
        # opt-in 后能解析
        register_code_node()
        try:
            flow = Flow.model_validate(
                {"flow_id": "x", "nodes": [{"type": "start", "id": "s", "next": "c"},
                                           {"type": "code", "id": "c",
                                            "code": "def run(input):\n    return input",
                                            "next": "e"},
                                           {"type": "end", "id": "e"}]}
            )
            assert flow.nodes[1].node_type == "code"
        finally:
            reg.unregister("code")
