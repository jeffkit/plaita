"""数据源工具双轨示例 — 代码轨 HttpToolSource + 配置轨 load_tool_bundle。"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from plaita_ai.agent.fot.tools import ToolNode, list_tools
from plaita_ai.tools import HttpToolSource, load_tool_bundle, register_source


def main() -> None:
    ToolNode.clear()

    # --- 代码轨 ---
    register_source(
        HttpToolSource(
            name="get_user",
            description="根据 ID 查询用户",
            url="https://api.example.com/users/{user_id}",
            response_path="$.data",
        )
    )

    # --- 配置轨（本目录 tools.yaml；native 项会因模块不存在而跳过演示）---
    # 这里用 dict 演示，避免依赖 myapp.formatters
    load_tool_bundle(
        {
            "tools": [
                {
                    "type": "http",
                    "name": "get_weather",
                    "description": "查询城市天气",
                    "url": "https://api.weather.example.com/v1/{city}",
                    "response_path": "$.result",
                }
            ]
        }
    )

    print("registered:", ToolNode.list_tool_names())
    print("--- list_tools ---")
    for line in list_tools(as_code=True):
        print(line)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json.return_value = {"data": {"id": "u1", "name": "Ada"}}
    mock_resp.text = ""
    with patch("plaita_ai.tools.source.http.requests") as req:
        req.request.return_value = mock_resp
        print("get_user:", ToolNode.get_tool("get_user")(user_id="u1"))

    print("example yaml:", Path(__file__).with_name("tools.yaml"))


if __name__ == "__main__":
    main()
