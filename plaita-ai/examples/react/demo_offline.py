"""Plaita ReAct 内置 Agent — 离线演示（无需 LLM API key）。

展示：
1. 内置 plaita 工具（compile / run / list_nodes / dsl_reference）作为 function-call 工具
2. 自适应 system prompt（默认普通 ReAct，仅复杂任务才升级到 @flow）
3. 同一工具既能被 Agent 直接调用，也能在 @flow 里 TOOL(...) 调用
"""

from __future__ import annotations

import json

from langchain.tools import tool

from plaita_ai.agent.react import build_plaita_tools
from plaita_ai.agent.react.prompts import build_system_prompt


@tool
def weather(city: str) -> str:
    """查询城市天气（示例）。"""
    return f"{city}：晴，25°C"


FLOW = '''
@flow("weather_flow")
def weather_flow(INPUT):
    r = TOOL(action="weather", params={"city": INPUT.city})
    return r
'''


def main() -> None:
    print("=== System prompt (enable_flow=True, excerpt) ===")
    print(build_system_prompt(enable_flow=True)[:500], "...\n")

    print("=== System prompt (enable_flow=False, vanilla ReAct) ===")
    print(build_system_prompt(enable_flow=False), "\n")

    # plaita 工具与用户工具平级，都是 function-call tool
    tools = {t.name: t for t in build_plaita_tools(flow_tools=[weather])}

    print("=== plaita_list_nodes (sample) ===")
    nodes = json.loads(tools["plaita_list_nodes"].invoke({}))
    print([n for n in nodes if n["node_type"] in ("tool", "http", "if")][:5], "...\n")

    print("=== plaita_compile_flow ===")
    compiled = json.loads(tools["plaita_compile_flow"].invoke({"source": FLOW}))
    print("ok:", compiled["ok"], "flow_id:", compiled.get("flow_id"))

    print("\n=== plaita_run_flow（@flow 内部用 TOOL 调 weather）===")
    ran = json.loads(
        tools["plaita_run_flow"].invoke(
            {"source": FLOW, "inputs_json": '{"city": "上海"}'}
        )
    )
    print("ok:", ran["ok"], "result:", ran.get("result"))


if __name__ == "__main__":
    main()
