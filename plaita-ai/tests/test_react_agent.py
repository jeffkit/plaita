"""Tests for Plaita ReAct builtin tools and agent factory."""

import json

from langchain.tools import tool
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from plaita_ai.agent.react import PlaitaAgent, build_plaita_tools
from plaita_ai.agent.react.prompts import build_system_prompt


GOOD_SRC = '''
@flow("greet", input_type="object")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
'''


def test_build_plaita_tools_compile_and_run():
    tools = {t.name: t for t in build_plaita_tools()}
    compile_out = json.loads(tools["plaita_compile_flow"].invoke({"source": GOOD_SRC}))
    assert compile_out["ok"] is True

    run_out = json.loads(
        tools["plaita_run_flow"].invoke(
            {"source": GOOD_SRC, "inputs_json": '{"name": "bob"}'}
        )
    )
    assert run_out["ok"] is True
    assert run_out["result"] == "hi BOB"


def test_plaita_agent_graph_builds_with_flow():
    model = FakeListChatModel(responses=["ok"])
    agent = PlaitaAgent(model=model, tools=[])
    assert agent._graph is not None
    assert agent.enable_flow is True


def test_plaita_agent_vanilla_react_mode():
    """enable_flow=False → no plaita tools, plain ReAct prompt."""
    @tool
    def echo(x: str) -> str:
        """Echo input."""
        return x

    model = FakeListChatModel(responses=["echoed"])
    agent = PlaitaAgent(model=model, tools=[echo], enable_flow=False)
    assert agent.enable_flow is False
    prompt = build_system_prompt(enable_flow=False)
    assert "@flow" not in prompt
    assert "工具" in prompt or "tool" in prompt.lower()


def test_user_tools_registered_as_tool_node():
    """User tools passed via tools= are registered so @flow can TOOL(action=...) them."""
    @tool
    def greet(name: str) -> str:
        """Greet."""
        return f"hi {name}"

    PlaitaAgent(model=FakeListChatModel(responses=["ok"]), tools=[greet])
    from plaita_ai.agent.fot.tools import ToolNode

    assert ToolNode.get_tool("greet") is not None
