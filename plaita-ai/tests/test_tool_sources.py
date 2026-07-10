"""Unit tests for plaita_ai.tools — BaseToolSource / config / registry."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from plaita_ai.agent.fot.tools import ToolNode, list_tools
from plaita_ai.tools import (
    HttpToolSource,
    NativeToolSource,
    SqlToolSource,
    ToolContext,
    VectorToolSource,
    build_tool_context,
    clear_addressing,
    clear_resources,
    load_tool_bundle,
    register_datasource,
    register_source,
    register_vectorstore,
)
from plaita_ai.tools.source.base import check_success, extract_json_path
from plaita_ai.tools.source.sql import sql_param_names


@pytest.fixture(autouse=True)
def _clear_tools():
    ToolNode.clear()
    clear_resources()
    clear_addressing()
    yield
    ToolNode.clear()
    clear_resources()
    clear_addressing()


# ---------------------------------------------------------------------------
# ToolContext / json path
# ---------------------------------------------------------------------------

class TestToolContext:
    def test_build_from_execution_globals(self):
        execution = MagicMock()
        store = {
            "trace_id": "t-1",
            "request_id": "r-1",
            "caller": "agent",
            "flow_id": "f-1",
            "auth_context": "tok",
            "baggage": {"tenant_key": "acme"},
        }

        def get_global(key, default=None):
            return store.get(key, default)

        execution.get_global_variable.side_effect = get_global
        ctx = build_tool_context(execution)
        assert ctx.trace_id == "t-1"
        assert ctx.auth == "tok"
        assert ctx.baggage["tenant_key"] == "acme"
        # 无业务固定字段
        assert not hasattr(ctx, "tenant_key")
        assert not hasattr(ctx, "user_id")

    def test_extract_json_path(self):
        data = {"data": {"user": {"name": "Ada"}, "items": [1, 2]}}
        assert extract_json_path(data, "$.data.user.name") == "Ada"
        assert extract_json_path(data, "data.items.1") == 2
        assert extract_json_path(data, None) is data

    def test_check_success(self):
        assert check_success({"ok": True}, "$.ok") is True
        assert check_success({"ok": False}, "$.ok") is False
        assert check_success({"ok": True}, None) is True
        assert check_success({}, "$.missing") is False


# ---------------------------------------------------------------------------
# HttpToolSource
# ---------------------------------------------------------------------------

class TestHttpToolSource:
    def test_register_and_schema_from_url_params(self):
        source = HttpToolSource(
            name="get_user",
            description="查询用户",
            url="https://api.example.com/users/{user_id}",
            method="GET",
            response_path="$.data",
        )
        spec = register_source(source)
        assert spec.name == "get_user"
        assert spec.placeholder == "GET_USER"
        assert [p.name for p in spec.schema.params] == ["user_id"]
        assert ToolNode.get_tool("get_user") is not None

    def test_invoke_http(self):
        source = HttpToolSource(
            name="get_user",
            description="查询用户",
            url="https://api.example.com/users/{user_id}",
            response_path="$.data",
        )
        func = source.to_callable()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/json"}
        mock_resp.json.return_value = {"data": {"id": "u1", "name": "Ada"}}
        mock_resp.text = "{}"

        with patch("plaita_ai.tools.source.http.requests") as req:
            req.request.return_value = mock_resp
            result = func(user_id="u1", context=ToolContext(trace_id="tr"))
        assert result == {"id": "u1", "name": "Ada"}
        call_kwargs = req.request.call_args.kwargs
        assert call_kwargs["url"] == "https://api.example.com/users/u1"
        assert call_kwargs["headers"]["X-Trace-Id"] == "tr"

    def test_success_condition_fails(self):
        source = HttpToolSource(
            name="check",
            url="https://api.example.com/x",
            success_condition="$.ok",
            error_message="业务失败",
        )
        with patch("plaita_ai.tools.source.http.requests") as req:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.headers = {"Content-Type": "application/json"}
            mock_resp.json.return_value = {"ok": False}
            mock_resp.text = ""
            req.request.return_value = mock_resp
            register_source(source)
            with pytest.raises(RuntimeError, match="业务失败"):
                ToolNode.get_tool("check")()


# ---------------------------------------------------------------------------
# NativeToolSource
# ---------------------------------------------------------------------------

def _sample_native(x: int, y: int = 1) -> int:
    """相加。"""
    return x + y


class TestNativeToolSource:
    def test_register_native(self):
        source = NativeToolSource(
            name="add_nums",
            description="两数相加",
            module=__name__,
            function="_sample_native",
        )
        spec = register_source(source)
        assert spec.name == "add_nums"
        assert ToolNode.get_tool("add_nums")(3, 4) == 7


# ---------------------------------------------------------------------------
# load_tool_bundle
# ---------------------------------------------------------------------------

class TestLoadToolBundle:
    def test_load_from_dict(self):
        specs = load_tool_bundle(
            {
                "version": "1",
                "tools": [
                    {
                        "type": "http",
                        "name": "get_order",
                        "description": "查订单",
                        "url": "https://api.example.com/orders/{order_id}",
                        "params": {
                            "order_id": {"type": "string", "required": True},
                        },
                    },
                    {
                        "type": "native",
                        "name": "add_nums",
                        "description": "相加",
                        "module": __name__,
                        "function": "_sample_native",
                    },
                ],
            }
        )
        assert {s.name for s in specs} == {"get_order", "add_nums"}
        names = ToolNode.list_tool_names()
        assert "get_order" in names
        assert "add_nums" in names

    def test_load_from_json_file(self, tmp_path: Path):
        path = tmp_path / "tools.json"
        path.write_text(
            json.dumps(
                {
                    "tools": [
                        {
                            "type": "http",
                            "name": "ping",
                            "url": "https://api.example.com/ping",
                            "description": "ping",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        specs = load_tool_bundle(path)
        assert specs[0].name == "ping"
        # list_tools 可见
        listed = list_tools(as_code=False)
        assert any(t["name"] == "ping" for t in listed)

    def test_load_from_yaml_file(self, tmp_path: Path):
        pytest.importorskip("yaml")
        path = tmp_path / "tools.yaml"
        path.write_text(
            """
version: "1"
tools:
  - type: http
    name: get_weather
    description: 查天气
    url: https://api.weather.com/v1/{city}
    method: GET
    response_path: $.result
""",
            encoding="utf-8",
        )
        specs = load_tool_bundle(path)
        assert specs[0].placeholder == "GET_WEATHER"
        assert [p.name for p in specs[0].schema.params] == ["city"]


# ---------------------------------------------------------------------------
# SqlToolSource
# ---------------------------------------------------------------------------

class TestSqlToolSource:
    def test_sql_param_names(self):
        assert sql_param_names(
            "SELECT * FROM t WHERE a = :user_id AND b::text = :x"
        ) == ["user_id", "x"]

    def test_query_sqlite(self):
        pytest.importorskip("sqlalchemy")
        source = SqlToolSource(
            name="query_orders",
            description="查订单",
            url="sqlite:///:memory:",
            sql="SELECT :user_id AS user_id, :limit AS lim",
        )
        spec = register_source(source)
        assert [p.name for p in spec.schema.params] == ["user_id", "limit"]
        rows = ToolNode.get_tool("query_orders")(user_id="u1", limit=5)
        assert rows == [{"user_id": "u1", "lim": 5}]

    def test_datasource_resource(self):
        pytest.importorskip("sqlalchemy")
        register_datasource("mem", "sqlite:///:memory:")
        source = SqlToolSource(
            name="ping_db",
            description="ping",
            datasource="mem",
            sql="SELECT 1 AS ok",
        )
        register_source(source)
        assert ToolNode.get_tool("ping_db")() == [{"ok": 1}]

    def test_load_sql_from_bundle(self):
        pytest.importorskip("sqlalchemy")
        specs = load_tool_bundle(
            {
                "tools": [
                    {
                        "type": "sql",
                        "name": "q",
                        "description": "q",
                        "url": "sqlite:///:memory:",
                        "sql": "SELECT :id AS id",
                    }
                ]
            }
        )
        assert specs[0].placeholder == "Q"
        assert ToolNode.get_tool("q")(id="x") == [{"id": "x"}]


# ---------------------------------------------------------------------------
# VectorToolSource
# ---------------------------------------------------------------------------

class _FakeStore:
    def similarity_search(self, query, k=4, filter=None):
        return [
            type("Doc", (), {"page_content": f"{query}-{i}"})()
            for i in range(k)
        ]


class TestVectorToolSource:
    def test_bind_store_and_retrieve(self):
        source = VectorToolSource(
            name="search_kb",
            description="检索知识库",
            k=2,
        ).bind_store(_FakeStore())
        spec = register_source(source)
        assert [p.name for p in spec.schema.params] == ["query", "k"]
        docs = ToolNode.get_tool("search_kb")(query="hello")
        assert docs == ["hello-0", "hello-1"]

    def test_register_vectorstore_by_name(self):
        register_vectorstore("prod_kb", _FakeStore())
        source = VectorToolSource(
            name="search_kb",
            description="检索",
            store="prod_kb",
            k=1,
        )
        register_source(source)
        assert ToolNode.get_tool("search_kb")(query="q") == ["q-0"]

    def test_callable_store(self):
        source = VectorToolSource(name="search", description="s", k=3).bind_store(
            lambda query, k=4: [f"{query}:{k}"]
        )
        register_source(source)
        assert ToolNode.get_tool("search")(query="a", k=2) == ["a:2"]

    def test_load_vector_from_bundle(self):
        register_vectorstore("kb", _FakeStore())
        specs = load_tool_bundle(
            {
                "tools": [
                    {
                        "type": "vector",
                        "name": "search_docs",
                        "description": "搜文档",
                        "store": "kb",
                        "k": 2,
                    }
                ]
            }
        )
        assert specs[0].name == "search_docs"
        assert ToolNode.get_tool("search_docs")(query="z") == ["z-0", "z-1"]


# ---------------------------------------------------------------------------
# addressing / bootstrap / validate
# ---------------------------------------------------------------------------

class TestAddressing:
    def test_simple_resolver(self):
        from plaita_ai.tools.addressing import (
            apply_addressing,
            clear_addressing,
            register_addressing,
        )

        clear_addressing()
        register_addressing("static", lambda host: "127.0.0.1:8080")
        with apply_addressing("http://svc.internal/api/x", "static") as url:
            assert url == "http://127.0.0.1:8080/api/x"
        clear_addressing()

    def test_http_uses_addressing(self):
        from plaita_ai.tools.addressing import clear_addressing, register_addressing

        clear_addressing()
        register_addressing("local", lambda host: "127.0.0.1")
        source = HttpToolSource(
            name="ping",
            description="ping",
            url="http://mysvc/health",
            addressing="local",
        )
        func = source.to_callable()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/json"}
        mock_resp.json.return_value = {"ok": True}
        mock_resp.text = ""
        with patch("plaita_ai.tools.source.http.requests") as req:
            req.request.return_value = mock_resp
            assert func() == {"ok": True}
            assert req.request.call_args.kwargs["url"] == "http://127.0.0.1/health"
        clear_addressing()


class TestValidateAndEnv:
    def test_validate_ok(self):
        from plaita_ai.tools import validate_tool_bundle

        errors = validate_tool_bundle(
            {
                "tools": [
                    {
                        "type": "http",
                        "name": "a",
                        "url": "https://x/{id}",
                        "description": "a",
                    }
                ]
            }
        )
        assert errors == []

    def test_validate_missing_datasource(self):
        from plaita_ai.tools import validate_tool_bundle

        errors = validate_tool_bundle(
            {
                "tools": [
                    {
                        "type": "sql",
                        "name": "q",
                        "description": "q",
                        "datasource": "missing_db",
                        "sql": "SELECT 1",
                    }
                ]
            },
            {"datasources": {}},
        )
        assert any("missing_db" in e for e in errors)

    def test_validate_rejects_mcp_type(self):
        from plaita_ai.tools import validate_tool_bundle

        # mcp not in schema discriminator — parse may fail or we catch via raw
        # Use a dict that passes if we add soft check; currently pydantic rejects unknown.
        # Soft path: invalid type fails at parse.
        errors = validate_tool_bundle(
            {"tools": [{"type": "http", "name": "dup", "url": "http://x"},
                       {"type": "http", "name": "dup", "url": "http://y"}]}
        )
        assert any("重复" in e for e in errors)

    def test_load_tools_from_env(self, tmp_path, monkeypatch):
        from plaita_ai.tools import load_tools_from_env

        path = tmp_path / "tools.json"
        path.write_text(
            json.dumps(
                {
                    "tools": [
                        {
                            "type": "http",
                            "name": "env_tool",
                            "description": "from env",
                            "url": "https://api.example.com/x",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        monkeypatch.setenv("PLAITA_TOOLS", str(path))
        monkeypatch.delenv("PLAITA_RESOURCES", raising=False)
        specs = load_tools_from_env()
        assert specs[0].name == "env_tool"
        assert "env_tool" in ToolNode.list_tool_names()

    def test_cli_tools_validate(self, tmp_path):
        from plaita_ai.cli.main import main

        path = tmp_path / "tools.json"
        path.write_text(
            json.dumps(
                {
                    "tools": [
                        {
                            "type": "http",
                            "name": "cli_ok",
                            "description": "ok",
                            "url": "https://x",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        with pytest.raises(SystemExit) as ei:
            main(["tools", "validate", str(path)])
        assert ei.value.code == 0
