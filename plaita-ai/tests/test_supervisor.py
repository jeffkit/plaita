"""Tests for plaita_ai.supervisor — the gated self-iteration loop."""

from __future__ import annotations

import pytest

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.evals import Dataset
from plaita_ai.supervisor import PromptProposer, StaticProposer, Supervisor, SupervisorPolicy

from ._console_fake import FakeConsole


DEF_V1 = '{"version": 1}'
DEF_V2 = '{"version": 2}'  # the improvement
DEF_V3 = '{"version": 3}'  # a regression


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())


def make_dataset(cases: int = 2) -> Dataset:
    return Dataset([{"id": f"c{i}", "input": {"i": i}} for i in range(cases)])  # type: ignore[list-item]


def seed_with_baseline(fake: FakeConsole, baseline_score: float = 0.5) -> FakeConsole:
    """Baseline version 1.0.0 whose dry-run output scores via 'contains'."""
    fake.seed_flow("demo", {"1.0.0": DEF_V1}, published="1.0.0")
    marker = "good" if baseline_score >= 1.0 else "bad"

    def handler(flow_json, input_data):
        # The improved definition (2) emits the passing marker; everything
        # else emits whatever its content says (3 → regressed marker).
        if flow_json == DEF_V2:
            out = {"text": "good output"}
        elif flow_json == DEF_V3:
            out = {"text": "worse output"}
        else:
            out = {"text": marker}
        return {"result": out, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    return fake


def expects_good(dataset: Dataset) -> Dataset:
    for case in dataset.cases:
        case.expect = {"contains": "good"}
    return dataset


# -- policy ---------------------------------------------------------------------


def test_policy_gate_validation():
    with pytest.raises(ValueError):
        SupervisorPolicy(promote_gate="yolo")


# -- single iteration -------------------------------------------------------------


def test_improved_candidate_returns_promotion_ticket():
    fake = seed_with_baseline(FakeConsole())
    client = make_client(fake)
    dataset = expects_good(make_dataset())
    proposer = StaticProposer([{"definition": DEF_V2, "rationale": "emit the good marker"}])
    sup = Supervisor(client, SupervisorPolicy(promote_gate="manual", min_improvement=0.02), proposer)

    outcome = sup.iterate("demo", dataset)
    assert outcome["status"] == "improved"
    assert outcome["candidate_version"] == "1.0.1"
    assert outcome["promoted"] is False
    ticket = outcome["promotion_ticket"]
    assert ticket["version"] == "1.0.1"
    assert "publish" in ticket["command"]
    # candidate saved as draft, nothing published by the loop
    detail = client.get_flow("demo")
    assert fake.published["demo"] == "1.0.0"
    assert {v["version"]: v["status"] for v in detail["versions"]}["1.0.1"] == "draft"


def test_auto_gate_publishes():
    fake = seed_with_baseline(FakeConsole())
    client = make_client(fake)
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        client,
        SupervisorPolicy(promote_gate="auto", min_improvement=0.02),
        StaticProposer([{"definition": DEF_V2, "rationale": "fix"}]),
    )
    outcome = sup.iterate("demo", dataset)
    assert outcome["promoted"] is True
    assert "promotion_ticket" not in outcome
    assert fake.published["demo"] == "1.0.1"


def test_regression_is_not_promoted():
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(min_improvement=0.02),
        StaticProposer([{"definition": DEF_V3, "rationale": "oops"}]),
    )
    outcome = sup.iterate("demo", dataset)
    assert outcome["status"] == "no_improvement"
    assert "promotion_ticket" not in outcome
    assert fake.published["demo"] == "1.0.0"


def test_no_proposer_returns_no_proposal():
    fake = seed_with_baseline(FakeConsole())
    sup = Supervisor(make_client(fake))
    outcome = sup.iterate("demo", expects_good(make_dataset()))
    assert outcome["status"] == "no_proposal"


def test_baseline_needs_a_version():
    fake = FakeConsole()  # no flows at all
    sup = Supervisor(make_client(fake), proposer=StaticProposer())
    with pytest.raises(Exception, match="not found in the console"):
        sup.iterate("ghost", expects_good(make_dataset()))


# -- the loop -------------------------------------------------------------------


def test_run_loop_stops_at_target():
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(target_score=1.0, promote_gate="manual", min_improvement=0.02),
        StaticProposer([{"definition": DEF_V2, "rationale": "fix"}]),
    )
    result = sup.run_loop("demo", dataset)
    assert result["final_status"] == "target_reached"
    assert result["iterations_run"] == 1


def test_run_loop_pauses_after_consecutive_failures():
    # A dataset with zero scored cases makes every candidate report avg=None,
    # which the loop counts as a failed iteration.
    fake = seed_with_baseline(FakeConsole())
    empty = Dataset([])
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(max_consecutive_failures=2, max_iterations=5),
        StaticProposer([{"definition": DEF_V2, "rationale": "x"} for _ in range(5)]),
    )
    result = sup.run_loop("demo", empty)
    assert result["final_status"] == "paused"
    assert result["iterations_run"] == 2


def test_run_loop_budget_exhaustion_without_improvement():
    # Proposer keeps offering regressions; loop should run to budget and stop.
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(max_iterations=3, min_improvement=0.02),
        StaticProposer([{"definition": DEF_V3, "rationale": "nope"} for _ in range(3)]),
    )
    result = sup.run_loop("demo", dataset)
    assert result["final_status"] == "budget_exhausted"
    assert result["iterations_run"] == 3
    assert all(it["status"] == "no_improvement" for it in result["iterations"])


def test_static_proposer_exhaustion_ends_loop():
    fake = seed_with_baseline(FakeConsole())
    dataset = expects_good(make_dataset())
    sup = Supervisor(
        make_client(fake),
        SupervisorPolicy(max_iterations=5, min_improvement=0.02),
        StaticProposer([]),
    )
    result = sup.run_loop("demo", dataset)
    assert result["final_status"] == "no_proposal"
    assert result["iterations_run"] == 1


# -- prompt proposer parsing (offline) --------------------------------------------


def test_prompt_proposer_requires_config():
    with pytest.raises(Exception, match="PLAITA_AI_PROPOSER"):
        PromptProposer({})


def test_prompt_proposer_parse_tolerates_fences():
    content = 'Sure!\n```json\n{"definition": "{\\\"a\\\": 1}", "rationale": "tweak"}\n```\n'
    parsed = PromptProposer._parse(content)
    assert parsed is not None
    assert parsed["definition"] == '{"a": 1}'
    assert PromptProposer._parse("no json here") is None
    assert PromptProposer._parse('{"no_definition": true}') is None
