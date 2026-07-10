"""Tests for LangChain BaseTool / BaseToolkit → ToolNode adapter."""

from __future__ import annotations

from typing import Annotated, Optional

import pytest

from plaita_ai.agent.fot.tools import ToolNode, normalize_tool, register_tool_node

pytest.importorskip("langchain_core")

from langchain_core.tools import BaseTool, BaseToolkit, InjectedToolCallId, tool
from pydantic import BaseModel, Field

from plaita_ai.tools.langchain import (
    adapt_langchain_tool,
    register_langchain_tool,
    register_langchain_toolkit,
    schema_from_langchain_tool,
)


@pytest.fixture(autouse=True)
def _clear():
    ToolNode.clear()
    yield
    ToolNode.clear()


class _UpperTool(BaseTool):
    """Toolkit-style tool: _run only, no .func."""

    name: str = "upper_text"
    description: str = "Uppercase the query"

    def _run(self, query: str, limit: int = 10) -> str:
        return query.upper()[:limit]


class _DemoToolkit(BaseToolkit):
    def get_tools(self):
        return [_UpperTool(), _EchoTool()]


class _EchoTool(BaseTool):
    name: str = "echo_text"
    description: str = "Echo text"

    def _run(self, text: str) -> str:
        return text


class TestNormalizeToolWithoutFunc:
    def test_normalize_toolkit_style_tool(self):
        name, func, desc = normalize_tool(_UpperTool())
        assert name == "upper_text"
        assert "Uppercase" in desc
        assert func(query="hi", limit=2) == "HI"

    def test_register_tool_node_accepts_base_tool(self):
        specs = register_tool_node(_UpperTool())
        assert specs[0].name == "upper_text"
        assert [p.name for p in specs[0].schema.params] == ["query", "limit"]
        assert ToolNode.get_tool("upper_text")(query="ab", limit=1) == "A"


class TestRegisterLangChain:
    def test_structured_tool_from_decorator(self):
        @tool
        def add(x: int, y: int = 1) -> int:
            """Add two numbers."""
            return x + y

        spec = register_langchain_tool(add)
        assert spec.name == "add"
        assert [p.name for p in spec.schema.params] == ["x", "y"]
        assert ToolNode.get_tool("add")(x=2, y=3) == 5

    def test_prefix_and_toolkit_filter(self):
        specs = register_langchain_toolkit(
            _DemoToolkit(),
            prefix="demo_",
            include=["upper_text"],
        )
        assert len(specs) == 1
        assert specs[0].name == "demo_upper_text"
        assert specs[0].placeholder == "DEMO_UPPER_TEXT"
        assert "echo_text" not in ToolNode.list_tool_names()
        assert ToolNode.get_tool("demo_upper_text")(query="ok") == "OK"

    def test_exclude(self):
        specs = register_langchain_toolkit(
            [_UpperTool(), _EchoTool()],
            exclude=["echo_text"],
        )
        assert {s.name for s in specs} == {"upper_text"}

    def test_schema_skips_injected_tool_call_id(self):
        class Args(BaseModel):
            query: str = Field(description="q")
            tool_call_id: Annotated[str, InjectedToolCallId]

        class InjectedTool(BaseTool):
            name: str = "with_injected"
            description: str = "has injected"
            args_schema: type[BaseModel] = Args

            def _run(self, query: str, tool_call_id: Optional[str] = None) -> str:
                return query

        schema = schema_from_langchain_tool(InjectedTool())
        names = [p.name for p in schema.params]
        assert "query" in names
        assert "tool_call_id" not in names

    def test_adapt_returns_triple(self):
        name, func, schema = adapt_langchain_tool(_EchoTool(), prefix="x_")
        assert name == "x_echo_text"
        assert schema.name == "x_echo_text"
        assert func(text="z") == "z"
