"""端到端测试：examples/agent 三个 Agent 编排案例。

覆盖自定义节点（LLMNode / ToolNode / RetrieverNode）注册、Flow 解析、运行全链路，
确保示例代码不会随仓库演进而悄悄失效。
"""
import pytest

from examples.agent import demo

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# RAG
# ---------------------------------------------------------------------------

def test_rag_returns_answer_built_from_retrieved_docs():
    flow = demo._load("rag")
    out = demo._run(flow, demo._GLOBALS, question="plaita 有几种执行模式")
    # responder 是 echo FakeLLM，输出即 prompt；检索结果应被拼进 prompt。
    assert "执行模式" in out
    assert "Normal" in out
    assert "Distributed" in out


def test_rag_retrieve_node_returns_top_docs():
    flow = demo._load("rag")
    # 直接驱动到 retrieve 节点：用 Generator 模式逐步取，验证检索库可用。
    steps = list(flow.debug(question="执行模式有哪些"))
    retrieved = next(s["result"] for s in steps if s["id"] == "retrieve")
    assert isinstance(retrieved, list)
    assert any("执行模式" in d for d in retrieved)


# ---------------------------------------------------------------------------
# Tool-use Agent
# ---------------------------------------------------------------------------

def test_tool_use_routes_to_weather():
    flow = demo._load("tool_use")
    out = demo._run(flow, demo._GLOBALS, question="北京今天天气如何", city="北京")
    assert out == "北京：晴，25°C，东南风 3 级"


def test_tool_use_routes_to_calc():
    flow = demo._load("tool_use")
    out = demo._run(flow, demo._GLOBALS, question="3 加 5 是多少", a=3, b=5)
    assert out == "3 + 5 = 8"


def test_tool_use_defaults_to_search():
    flow = demo._load("tool_use")
    out = demo._run(flow, demo._GLOBALS, question="plaita 是什么")
    assert "plaita 是什么" in out
    assert "结果" in out


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("message,role", [
    ("这个多少钱？有优惠吗", "销售"),
    ("程序一直报错打不开", "技术支持"),
    ("我要退款，看下账单", "计费"),
    ("你好呀", "通用"),
])
def test_router_classifies_to_correct_branch(message, role):
    flow = demo._load("router")
    out = demo._run(flow, demo._GLOBALS, message=message)
    assert role in out


# ---------------------------------------------------------------------------
# 节点注册与工具
# ---------------------------------------------------------------------------

def test_custom_nodes_registered():
    from plaita import get_default_registry
    reg = get_default_registry()
    assert "llm" in reg
    assert "tool" in reg
    assert "retrieve" in reg


def test_tool_registry_and_retriever_library():
    from examples.agent.nodes import ToolNode, RetrieverNode
    assert ToolNode.get_tool("weather") is not None
    assert ToolNode.get_tool("calc") is not None
    assert ToolNode.get_tool("search") is not None
    assert RetrieverNode.get_library("kb") is not None


def test_demo_main_runs_clean(capsys):
    """demo.main 端到端可跑，且不抛异常。"""
    demo.main()
    captured = capsys.readouterr()
    assert "RAG" in captured.out
    assert "Tool-use" in captured.out
    assert "Router" in captured.out
