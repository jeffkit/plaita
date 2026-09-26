"""Tests for plaita_ai.supervisor_flow — the @flow dogfood template."""

from __future__ import annotations

import json

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.evals import Dataset
from plaita_ai.flow_runner import compile_flow
from plaita_ai.supervisor import StaticProposer, SupervisorPolicy
from plaita_ai.supervisor_flow import (
    SUPERVISOR_FLOW_SOURCE,
    make_tools_module,
    run_supervisor_flow,
)

from ._console_fake import FakeConsole

DEF_V2 = '{"version": 2}'


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())


def seed(fake: FakeConsole) -> FakeConsole:
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")

    def handler(flow_json, input_data):
        out = {"text": "good output"} if flow_json == DEF_V2 else {"text": "bad output"}
        return {"result": out, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    return fake


def make_dataset() -> Dataset:
    return Dataset([{"id": "c1", "input": {}, "expect": {"contains": "good"}}])


def test_template_compiles():
    from plaita.node import get_default_registry

    from plaita_ai.agent.fot.tools import ToolNode

    get_default_registry().register(ToolNode)  # TOOL dispatcher isn't builtin
    result = compile_flow(SUPERVISOR_FLOW_SOURCE)
    assert result.ok, result.errors
    assert result.flow_id == "supervisor-iterate"
    node_types = [n["type"] for n in result.ir["nodes"]]
    assert node_types.count("tool") == 3


def test_run_supervisor_flow_improves_and_gates():
    fake = seed(FakeConsole())
    tools = make_tools_module(
        make_client(fake),
        make_dataset(),
        policy=SupervisorPolicy(promote_gate="manual", min_improvement=0.02),
        proposer=StaticProposer([{"definition": DEF_V2, "rationale": "emit good"}]),
    )
    verdict = run_supervisor_flow("demo", tools=tools)
    assert verdict["status"] == "improved"
    assert verdict["candidate_version"] == "1.0.1"
    assert verdict["promoted"] is False
    assert verdict["promotion_ticket"]["command"].startswith("POST /api/flows/demo/publish")
    # the loop published nothing
    assert fake.published["demo"] == "1.0.0"


def test_run_supervisor_flow_no_proposal_path():
    fake = seed(FakeConsole())
    tools = make_tools_module(
        make_client(fake),
        make_dataset(),
        proposer=StaticProposer([]),  # exhausted → sup_propose returns error
    )
    verdict = run_supervisor_flow("demo", tools=tools)
    assert verdict["status"] == "no_proposal"
