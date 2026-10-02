"""FoT planner prompt sections — authoring-spec 注入 + 已注册节点清单。

离线测试（duck-type 录制模型，无 API key）：
- COMPOSE/REVIEW 系统提示词包含 authoring-spec 内容；缺失时降级为硬约束底线；
- NodeRegistry 里已注册的业务节点以大写占位符 + 字段清单进入提示词，
  内置类型 / 通用 TOOL / FoT 动态工具节点被排除；
- 提示词模板键变更不回归（所有 {xxx_section} 都有实参）。
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, ClassVar, Optional

import pytest

pytest.importorskip("langchain", reason="langchain extra not installed: pip install 'plaita-ai[agent]'")
pytest.importorskip("plaita", reason="plaita runtime not installed")

from plaita import Node
from plaita.node import get_default_registry

import plaita_ai.agent.fot.planner as planner_module
from plaita_ai.agent.fot.planner import (
    _registered_nodes_section,
    plan_flow_source,
    review_flow_source,
)
from plaita_ai.agent.fot.tools import ToolNode


class FotPingNode(Node):
    """测试用业务节点：回声。"""

    node_type: ClassVar[str] = "fot_test_ping"
    node_name: ClassVar[str] = "ping"
    target: Optional[str] = None
    content: Optional[Any] = None

    def execute(self, execution):
        return self.content


@pytest.fixture()
def ping_node():
    registry = get_default_registry()
    registry.register(FotPingNode)
    yield FotPingNode
    registry.unregister("fot_test_ping")


@pytest.fixture(autouse=True)
def clear_tool_registry():
    ToolNode.clear()
    yield
    ToolNode.clear()


class _RecordingModel:
    """录制 invoke 入参的 duck-type 模型（planner 只用 invoke + .content）。"""

    def __init__(self, content: str = '```python\n@flow("x")\ndef x(INPUT):\n    return 1\n```'):
        self.content = content
        self.calls = []

    def invoke(self, messages, *args, **kwargs):
        self.calls.append(list(messages))
        return SimpleNamespace(content=self.content)


def _system_of(model: _RecordingModel) -> str:
    assert model.calls, "model 未被调用"
    first = model.calls[0][0]
    return first.content


def test_compose_prompt_contains_authoring_spec(monkeypatch, ping_node):
    monkeypatch.setattr(planner_module, "_AUTHORING_REFERENCE", "AUTHORING-SPEC-MARKER 硬约束正文")
    model = _RecordingModel()
    plan_flow_source(model, task="测试任务")
    system = _system_of(model)
    assert "AUTHORING-SPEC-MARKER" in system
    assert "编写规范" in system
    # 注册的业务节点以大写占位符 + 字段清单出现
    assert "FOT_TEST_PING(target, content)" in system
    assert "测试用业务节点" in system


def test_compose_prompt_falls_back_without_spec(monkeypatch, ping_node):
    monkeypatch.setattr(planner_module, "_AUTHORING_REFERENCE", "")
    model = _RecordingModel()
    plan_flow_source(model, task="测试任务")
    system = _system_of(model)
    assert "编写规范文档未加载" in system
    assert "同名赋值" in system


def test_registered_nodes_exclude_builtin_and_tools(monkeypatch, ping_node):
    def echo(text: str) -> str:
        """回显文本。"""
        return text

    monkeypatch.setattr(planner_module, "_AUTHORING_REFERENCE", "SPEC")
    model = _RecordingModel()
    plan_flow_source(model, task="t", tools=[echo])
    system = _system_of(model)
    # 只扫「已注册业务节点」段落：内置类型与通用 TOOL 不进节点清单
    in_nodes = False
    node_lines = []
    for line in system.splitlines():
        if line.startswith("## "):
            in_nodes = line.startswith("## 已注册业务节点")
            continue
        if in_nodes and line.startswith("- "):
            node_lines.append(line)
    assert node_lines, "节点清单段落缺失"
    for line in node_lines:
        assert not any(
            line.startswith(f"- {t.upper()}(")
            for t in ("http", "code", "event", "child", "reference", "parallel",
                      "map", "filter", "find", "loop", "reduce", "start",
                      "end", "if", "assignment", "switch", "bool", "tool")
        ), f"内置类型泄漏进节点清单: {line}"
    # 业务节点在、动态工具节点不在节点清单（工具走自己的 TOOL(action=...) 段落）
    assert any(line.startswith("- FOT_TEST_PING(") for line in node_lines)
    assert not any(line.startswith("- ECHO(") for line in node_lines)
    assert 'TOOL(action="echo"' in system  # 工具在可用工具段落


def test_review_prompt_carries_sections(monkeypatch, ping_node):
    monkeypatch.setattr(planner_module, "_AUTHORING_REFERENCE", "AUTHORING-SPEC-MARKER")
    model = _RecordingModel()
    errors = [planner_module.CompileError(line=1, message="boom")]
    review_flow_source(model, task="t", source="```python\n@flow(\"x\")\ndef x(INPUT):\n    return 1\n```",
                       errors=errors)
    system = _system_of(model)
    assert "AUTHORING-SPEC-MARKER" in system
    assert "FOT_TEST_PING(" in system


def test_registered_nodes_section_empty_when_registry_unavailable(monkeypatch):
    class _Boom:
        def list_types(self):
            raise RuntimeError("registry gone")

    monkeypatch.setattr(planner_module, "get_default_registry", lambda: _Boom())
    assert _registered_nodes_section([]) == ""


def test_registered_nodes_section_caps_at_limit(monkeypatch, ping_node):
    class _Many:
        def list_types(self):
            return [f"node_{i:02d}" for i in range(60)] + ["fot_test_ping"]

        def get(self, node_type):
            if node_type == "fot_test_ping":
                return FotPingNode
            return type(
                f"Node_{node_type}",
                (Node,),
                {
                    "node_type": node_type,
                    "__annotations__": {"alpha": Optional[str]},
                    "alpha": None,
                    "__doc__": f"{node_type} 文档。",
                },
            )

    monkeypatch.setattr(planner_module, "get_default_registry", lambda: _Many())
    section = _registered_nodes_section([])
    lines = [ln for ln in section.splitlines() if ln.startswith("- ")]
    assert len(lines) <= planner_module._MAX_LISTED_NODES + 1
    assert "FOT_TEST_PING(target, content)" in section
    assert "省略" in section
