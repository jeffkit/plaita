"""End-to-end tests for FoTAgent — real compile loop with deterministic LLM.

FakeToolCallingModel is overkill for FoT (it doesn't use the tool-calling
loop), but we verify the full plan→compile→(review)→run path with a fake
chat model that returns scripted @flow source.
"""

from __future__ import annotations

import pytest
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from plaita_ai.agent.fot import FoTAgent
from plaita_ai.agent.fot.tools import ToolNode, register_tool_node


@pytest.fixture(autouse=True)
def clear_tool_registry():
    ToolNode.clear()
    yield
    ToolNode.clear()


def echo(text: str) -> str:
    """回显文本。"""
    return text


VALID_FLOW = '''```python
@flow("use_echo", input_type="object")
def use_echo(INPUT):
    r = TOOL(action="echo", params={"text": INPUT.q})
    return r
```'''

# 编译失败的源码（f-string），用来验证 review 自纠
BAD_FLOW = '''```python
@flow("use_echo", input_type="object")
def use_echo(INPUT):
    return f"{INPUT.q}"
```'''

FIXED_FLOW = '''```python
@flow("use_echo", input_type="object")
def use_echo(INPUT):
    r = TOOL(action="echo", params={"text": INPUT.q})
    return r
```'''


def test_fot_e2e_plan_compile_run():
    model = FakeListChatModel(responses=[VALID_FLOW])
    agent = FoTAgent(model=model, tools=[echo])
    result = agent.invoke({"task": "回显用户问题", "q": "hello"})
    assert result.ok is True
    assert result.result == "hello"
    assert result.attempts == 1
    assert "@flow" in result.source
    assert result.run is not None and result.run.ok


def test_fot_e2e_self_correction():
    """bad flow → compile error → review → fixed flow → run."""
    model = FakeListChatModel(responses=[BAD_FLOW, FIXED_FLOW])
    agent = FoTAgent(model=model, tools=[echo], max_compile_retries=2)
    result = agent.invoke({"task": "回显", "q": "world"})
    assert result.ok is True
    assert result.result == "world"
    assert result.attempts == 2


def test_fot_e2e_compile_exhausted_reports_errors():
    """All attempts fail → ok=False with compile_errors, no run."""
    model = FakeListChatModel(responses=[BAD_FLOW, BAD_FLOW, BAD_FLOW])
    agent = FoTAgent(model=model, tools=[echo], max_compile_retries=2)
    result = agent.invoke({"task": "回显", "q": "x"})
    assert result.ok is False
    assert result.result is None
    assert result.compile_errors  # 含编译错误
    assert result.run is None  # 没走到执行
