"""LangChain toolkit → plaita ToolNode 示例（无需真实 community toolkit）。"""

from __future__ import annotations

from langchain_core.tools import BaseTool, BaseToolkit

from plaita_ai.agent.fot.tools import ToolNode, list_tools
from plaita_ai.tools import register_langchain_toolkit


class _ReadTool(BaseTool):
    name: str = "read_file"
    description: str = "Read a file (demo)"

    def _run(self, path: str) -> str:
        return f"<contents of {path}>"


class _WriteTool(BaseTool):
    name: str = "write_file"
    description: str = "Write a file (demo)"

    def _run(self, path: str, text: str) -> str:
        return f"wrote {len(text)} bytes to {path}"


class _DemoFSToolkit(BaseToolkit):
    def get_tools(self):
        return [_ReadTool(), _WriteTool()]


def main() -> None:
    ToolNode.clear()
    register_langchain_toolkit(
        _DemoFSToolkit(),
        prefix="fs_",
        include=["read_file", "write_file"],
    )
    print("tools:", ToolNode.list_tool_names())
    for line in list_tools(as_code=True):
        print(line)
    print("read:", ToolNode.get_tool("fs_read_file")(path="/tmp/a.txt"))


if __name__ == "__main__":
    main()
