"""Tests for FoT agent (LangChain FakeListChatModel, no API key)."""

from langchain_core.language_models.fake_chat_models import FakeListChatModel

from plaita_ai.agent.fot import FoTAgent
from plaita_ai.agent.fot.tools import ToolNode, register_tool_node


@ToolNode.register("echo")
def echo(text: str) -> str:
    """回显文本。"""
    return text


VALID_FLOW = '''```python
@flow("use_echo", input_type="object")
def use_echo(INPUT):
    r = TOOL(action="echo", params={"text": INPUT.q})
    return r
```'''

FIXED_FLOW = '''```python
@flow("use_echo", input_type="object")
def use_echo(INPUT):
    r = TOOL(action="echo", params={"text": INPUT.q})
    return r
```'''


def test_fot_agent_compiles_and_runs():
    register_tool_node(echo)
    model = FakeListChatModel(responses=[VALID_FLOW])
    agent = FoTAgent(model=model, tools=[echo])
    result = agent.invoke({"task": "回显用户问题", "q": "hello"})
    assert result.ok
    assert result.result == "hello"
    assert "@flow" in result.source
    assert result.attempts == 1


def test_fot_agent_self_corrects_on_compile_error():
    register_tool_node(echo)
    bad_flow = '''```python
@flow("bad", input_type="object")
def bad(INPUT):
    return f"hi {INPUT.q}"
```'''
    model = FakeListChatModel(responses=[bad_flow, FIXED_FLOW])
    agent = FoTAgent(model=model, tools=[echo], max_compile_retries=2)
    result = agent.invoke({"task": "回显", "q": "world"})
    assert result.ok
    assert result.result == "world"
    assert result.attempts == 2
