"""Tests for plaita_ai.ops_mcp — tool registration, dispatch, error wrapping."""

from __future__ import annotations

import json

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai import ops_mcp

from ._console_fake import FakeConsole


class _FakeMCP:
    """Captures @mcp.tool() registrations so tests can call them directly."""

    def __init__(self) -> None:
        self.tools = {}

    def tool(self):
        def deco(fn):
            self.tools[fn.__name__] = fn
            return fn

        return deco


def make_wired(fake: FakeConsole):
    ops_mcp.set_client(
        ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())
    )
    fake_mcp = _FakeMCP()
    ops_mcp.register(fake_mcp)
    return fake_mcp.tools


def teardown_function(_):
    ops_mcp.set_client(None)


def test_console_tools_registered():
    tools = make_wired(FakeConsole())
    for name in [
        "console_flow_list",
        "console_flow_get",
        "console_flow_version_get",
        "console_flow_version_save",
        "console_flow_publish",
        "console_flow_diff",
        "console_flow_metrics",
        "console_runs_list",
        "console_run_get",
        "console_run_start",
        "console_run_cancel",
        "console_dry_run",
        "eval_run",
        "eval_compare",
        "supervisor_iterate",
    ]:
        assert name in tools, f"missing tool: {name}"


def test_console_flow_get_shapes_versions():
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": "{}", "1.0.1": "{}"}, published="1.0.0")
    tools = make_wired(fake)
    out = json.loads(tools["console_flow_get"]("demo"))
    assert out["latest"] == "1.0.1"
    assert out["published"] == "1.0.0"


def test_error_wrapped_as_ok_false():
    tools = make_wired(FakeConsole())  # empty console
    out = json.loads(tools["console_flow_get"]("ghost"))
    assert out["ok"] is False
    assert "no versions" in out["error"] or "404" in out["error"]


def test_unconfigured_console_reports_actionable_error():
    ops_mcp.set_client(None)
    fake_mcp = _FakeMCP()
    ops_mcp.register(fake_mcp)
    import os

    env_backup = os.environ.pop("PLAITA_CONSOLE_URL", None)
    try:
        out = json.loads(fake_mcp.tools["console_flow_list"]())
        assert out["ok"] is False
        assert "PLAITA_CONSOLE_URL" in out["error"]
    finally:
        if env_backup is not None:
            os.environ["PLAITA_CONSOLE_URL"] = env_backup


def test_supervisor_iterate_end_to_end_manual_gate():
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")

    def handler(flow_json, input_data):
        result = {"text": "good output"} if '"v": 2' in flow_json else {"text": "bad output"}
        return {"result": result, "nodes": [], "error": None}

    fake.dry_run_handler = handler

    import tempfile
    from pathlib import Path

    tmp = Path(tempfile.mkdtemp()) / "cases.json"
    tmp.write_text(
        json.dumps({"cases": [{"id": "c1", "input": {}, "expect": {"contains": "good"}}]}), encoding="utf-8"
    )
    tools = make_wired(fake)
    # "static" proposer without candidates → no_proposal; then verify wiring works
    out = json.loads(tools["supervisor_iterate"]("demo", str(tmp), proposer="static"))
    assert out["final_status"] == "no_proposal"
