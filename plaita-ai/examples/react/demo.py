"""Plaita 内置 ReAct Agent — 在线 demo（需要 LLM API key）。

定位：**普通 ReAct 为主，@flow 是可选增强**。
- 简单工具调用：Agent 直接调 function-call 工具（普通 ReAct loop）。
- 复杂多步编排：Agent 自行决定写 @flow，用 plaita_compile_flow / plaita_run_flow。
- 同一工具两种用法都能用：直接调用，或在 @flow 里 TOOL(action=...) 调用。

用法（OpenAI 示例）::

    export OPENAI_API_KEY=sk-...
    python examples/react/demo.py

其他模型：改 ``MODEL`` 为 init_chat_model 支持的字符串，如 ``anthropic:claude-sonnet-4-6``。
"""

from __future__ import annotations

import os
import sys

from langchain.tools import tool

from plaita_ai.agent.react import PlaitaAgent

MODEL = os.environ.get("PLAITA_AGENT_MODEL", "openai:gpt-4o-mini")


@tool
def weather(city: str) -> str:
    """查询指定城市的天气。"""
    return f"{city}：晴，25°C，东南风 3 级"


@tool
def calc(a: int, b: int) -> str:
    """两数求和。"""
    return f"{a} + {b} = {a + b}"


def main() -> None:
    if not os.environ.get("OPENAI_API_KEY") and MODEL.startswith("openai:"):
        print(
            "需要 OPENAI_API_KEY（或设置 PLAITA_AGENT_MODEL 指向其他 provider）。\n"
            "离线演示请运行: python examples/react/demo_offline.py",
            file=sys.stderr,
        )
        sys.exit(1)

    # tools 既是 Agent 的 function-call 工具，也自动注册为 ToolNode 供 @flow 调用。
    agent = PlaitaAgent(
        model=MODEL,
        tools=[weather, calc],
        instruction=(
            "简单问答直接调工具即可；只有当任务需要多步编排（分支/循环/并行/可复现流程）"
            "时才用 @flow + plaita_run_flow。"
        ),
    )

    question = os.environ.get(
        "PLAITA_DEMO_QUESTION",
        "查一下北京的天气，city=北京",
    )
    print("Q:", question)
    result = agent.invoke(question)
    print("\nA:", result.text)


if __name__ == "__main__":
    main()
