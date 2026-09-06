"""End-to-end tests for PlaitaAgent — drives the real ReAct tool-calling loop.

Uses ``FakeToolCallingModel`` (no API key) so the LangGraph ``create_agent``
loop genuinely executes the plaita compile/run tools and user tools.
"""

from __future__ import annotations

import json

import pytest
pytest.importorskip("langchain", reason="langchain extra not installed: pip install 'plaita-ai[agent]'")
from langchain.tools import tool

from plaita_ai.agent.react import PlaitaAgent
from tests._fakes import FakeToolCallingModel, ai_text, ai_tool_call


# --- user tools -----------------------------------------------------------

@tool
def weather(city: str) -> str:
    """查询城市天气。"""
    return f"{city}：晴，25°C"


@tool
def calc(a: int, b: int) -> str:
    """两数求和。"""
    return f"{a} + {b} = {a + b}"


GOOD_GREET = '''
@flow("greet", input_type="object")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
'''

WEATHER_FLOW = '''
@flow("weather_flow", input_type="object")
def weather_flow(INPUT):
    r = TOOL(action="weather", params={"city": INPUT.city})
    return r
'''


# --- cases ----------------------------------------------------------------

def test_react_direct_tool_call_no_flow():
    """Simple task → agent calls user tool directly, no @flow."""
    model = FakeToolCallingModel(
        [
            ai_tool_call("weather", {"city": "北京"}),
            ai_text("北京：晴，25°C"),
        ]
    )
    agent = PlaitaAgent(model=model, tools=[weather])
    result = agent.invoke("查北京天气")
    assert "北京" in result.text
    # 确实走了工具调用（有一条 ToolMessage）
    tool_msgs = [m for m in result.messages if m.type == "tool"]
    assert tool_msgs and "北京" in tool_msgs[0].content


def test_react_escalates_to_flow_for_orchestration():
    """Multi-step task → agent drafts @flow, compiles, runs via plaita tools."""
    compile_result = json.dumps(
        {"ok": True, "flow_id": "greet", "ir": {"flow_id": "greet"}, "errors": []},
        ensure_ascii=False,
    )
    run_result = json.dumps(
        {"ok": True, "result": "hi ALICE", "error": None, "error_type": None},
        ensure_ascii=False,
    )
    model = FakeToolCallingModel(
        [
            ai_tool_call("plaita_compile_flow", {"source": GOOD_GREET}),
            ai_tool_call("plaita_run_flow", {"source": GOOD_GREET, "inputs_json": '{"name": "alice"}'}),
            ai_text("结果是 hi ALICE"),
        ]
    )
    agent = PlaitaAgent(model=model, tools=[])
    result = agent.invoke("把 name=alice 转大写并返回 hi ALICE")
    tool_names = [tc["name"] for m in result.messages if m.type == "ai"
                  for tc in (m.tool_calls or [])]
    assert "plaita_compile_flow" in tool_names
    assert "plaita_run_flow" in tool_names
    assert "ALICE" in result.text
    # 编译工具的 ToolMessage 应含 ok:true
    compile_msgs = [m for m in result.messages if m.type == "tool"
                    and "plaita_compile_flow" in str(m.name)]
    assert compile_msgs and '"ok": true' in compile_msgs[0].content


def test_react_flow_self_corrects_on_compile_error():
    """Agent emits bad @flow, gets compile errors, fixes, recompiles, runs."""
    bad_src = '''
@flow("bad", input_type="object")
def bad(INPUT):
    return f"hi {INPUT.name}"
'''
    compile_bad = json.dumps(
        {
            "ok": False,
            "flow_id": None,
            "ir": None,
            "errors": [{"line": 4, "message": "不支持的表达式 JoinedStr（f-string）"}],
        },
        ensure_ascii=False,
    )
    compile_good = json.dumps(
        {"ok": True, "flow_id": "greet", "ir": {"flow_id": "greet"}, "errors": []},
        ensure_ascii=False,
    )
    run_good = json.dumps(
        {"ok": True, "result": "hi BOB", "error": None, "error_type": None},
        ensure_ascii=False,
    )
    model = FakeToolCallingModel(
        [
            ai_tool_call("plaita_compile_flow", {"source": bad_src}),
            ai_tool_call("plaita_compile_flow", {"source": GOOD_GREET}),
            ai_tool_call("plaita_run_flow", {"source": GOOD_GREET, "inputs_json": '{"name": "bob"}'}),
            ai_text("修好后结果是 hi BOB"),
        ]
    )
    # patch compile output sequence by relying on real compile_flow: bad→fail, good→ok
    agent = PlaitaAgent(model=model, tools=[])
    result = agent.invoke("把 name=bob 转大写拼接 hi")
    tool_names = [tc["name"] for m in result.messages if m.type == "ai"
                  for tc in (m.tool_calls or [])]
    # 编译被调了至少两次（bad 然后 good）
    assert tool_names.count("plaita_compile_flow") >= 2
    assert "plaita_run_flow" in tool_names
    assert "BOB" in result.text


def test_react_flow_calls_user_tool_via_tool_node():
    """@flow uses TOOL(action="weather") — user tool registered as ToolNode."""
    run_result = json.dumps(
        {"ok": True, "result": "上海：晴，25°C", "error": None, "error_type": None},
        ensure_ascii=False,
    )
    model = FakeToolCallingModel(
        [
            ai_tool_call("plaita_compile_flow", {"source": WEATHER_FLOW}),
            ai_tool_call("plaita_run_flow", {"source": WEATHER_FLOW, "inputs_json": '{"city": "上海"}'}),
            ai_text("上海：晴，25°C"),
        ]
    )
    agent = PlaitaAgent(model=model, tools=[weather])
    result = agent.invoke("查上海天气，用 @flow")
    assert "上海" in result.text
    # plaita_run_flow 的输出里应含 weather 工具的返回
    run_msgs = [m for m in result.messages if m.type == "tool" and "plaita_run_flow" in str(m.name)]
    assert run_msgs and "上海" in run_msgs[0].content


def test_react_vanilla_mode_no_plaita_tools():
    """enable_flow=False → plaita tools absent; agent only has user tools."""
    model = FakeToolCallingModel(
        [
            ai_tool_call("calc", {"a": 3, "b": 5}),
            ai_text("3 + 5 = 8"),
        ]
    )
    agent = PlaitaAgent(model=model, tools=[calc], enable_flow=False)
    result = agent.invoke("3+5=?")
    tool_names = [tc["name"] for m in result.messages if m.type == "ai"
                  for tc in (m.tool_calls or [])]
    assert tool_names == ["calc"]  # 没有 plaita_* 工具
    assert "8" in result.text
