"""FoT Agent 离线 demo — FakeListChatModel，无需 API key。"""

from __future__ import annotations

from langchain_core.language_models.fake_chat_models import FakeListChatModel

from plaita_ai.agent.fot import FoTAgent
from plaita_ai.agent.fot.tools import ToolNode


@ToolNode.register("weather")
def weather(city: str) -> str:
    """查询指定城市天气。"""
    return f"{city}：晴，25°C"


FLOW = '''```python
@flow("weather_flow", input_type="object")
def weather_flow(INPUT):
    r = TOOL(action="weather", params={"city": INPUT.city})
    return r
```'''


def main() -> None:
    model = FakeListChatModel(responses=[FLOW])
    agent = FoTAgent(model=model, tools=[weather])
    result = agent.invoke({"task": "查询城市天气", "city": "北京"})
    print("ok:", result.ok)
    print("result:", result.result)
    print("attempts:", result.attempts)
    print("--- source ---")
    print(result.source)


if __name__ == "__main__":
    main()
