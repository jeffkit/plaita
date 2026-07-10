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
    ToolContext,
    build_tool_context,
    load_tool_bundle,
    register_source,
)
from plaita_ai.tools.source.base import check_success, extract_json_path


@pytest.fixture(autouse=True)
def _clear_tools():
    ToolNode.clear()
    yield
    ToolNode.clear()


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
