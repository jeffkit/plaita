"""Tests for plaita_ai.canary — split/shadow routing, recording, verdicts."""

from __future__ import annotations

import pytest

from plaita_ai.canary import CanaryPolicy, CanaryRun, shadow_once
from plaita_ai.console_client import ConsoleClient, ConsoleConfig

from ._console_fake import FakeConsole


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())


def seed(fake: FakeConsole) -> FakeConsole:
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}', "1.1.0": '{"v": 2}'}, published="1.0.0")
    # baseline arm succeeds; candidate arm fails half the time by name parity
    fake.run_handler = lambda flow_id, version, params: (
        ("completed", {"from": version or "1.0.0"})
        if version in (None, "1.0.0") or params.get("name") != "flaky"
        else ("failed", None)
    )
    return fake


# -- policy -----------------------------------------------------------------


def test_policy_validation():
    with pytest.raises(ValueError):
        CanaryPolicy(mode="blue-green")
    with pytest.raises(ValueError):
        CanaryPolicy(ratio=1.5)


def test_baseline_defaults_to_published():
    fake = seed(FakeConsole())
    run = CanaryRun(make_client(fake), "demo", "1.1.0")
    assert run.baseline_version == "1.0.0"


# -- split mode ----------------------------------------------------------------


def test_route_ratio_extremes_and_sticky():
    fake = seed(FakeConsole())
    run = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="split", ratio=0.0))
    assert all(run.route({"canary_key": k})["arm"] == "baseline" for k in ["a", "b", "c"])

    full = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="split", ratio=1.0))
    assert all(full.route({"canary_key": k})["arm"] == "candidate" for k in ["a", "b", "c"])

    sticky = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="split", ratio=0.5))
    first = sticky.route({"canary_key": "user-7"})["arm"]
    assert all(sticky.route({"canary_key": "user-7"})["arm"] == first for _ in range(5))


def test_split_invoke_records_and_reports():
    fake = seed(FakeConsole())
    run = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="split", ratio=1.0))
    out = run.invoke({"canary_key": "k1", "name": "ok"})
    assert out["canary"]["arm"] == "candidate"
    assert out["ok"] is True
    rep = run.report()
    assert rep["candidate"]["count"] == 1
    assert rep["candidate"]["success_rate"] == 1.0
    assert rep["baseline"]["count"] == 0


def test_state_round_trip():
    fake = seed(FakeConsole())
    run = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="split", ratio=1.0))
    run.invoke({"canary_key": "k1", "name": "ok"})
    state = run.to_dict()
    revived = CanaryRun.from_dict(make_client(fake), state)
    assert revived.baseline_version == "1.0.0"
    assert revived.candidate_version == "1.1.0"
    assert len(revived.records) == 1
    assert revived.report()["candidate"]["count"] == 1


# -- shadow mode -----------------------------------------------------------------


def test_shadow_invoke_keeps_baseline_result():
    fake = seed(FakeConsole())
    fake.dry_run_handler = lambda flow_json, input_data: {"result": {"shadow": True}, "nodes": [], "error": None}
    run = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="shadow"))
    out = run.invoke({"canary_key": "k1", "name": "ok"})
    assert out["ok"] is True  # baseline result returned to the caller
    assert out["canary"]["shadow"]["ok"] is True
    assert out["canary"]["shadow"]["output"] == {"shadow": True}
    rep = run.report()
    assert rep["baseline"]["count"] == 1 and rep["candidate"]["count"] == 1


def test_shadow_candidate_failure_recorded_not_raised():
    fake = seed(FakeConsole())
    fake.dry_run_handler = lambda flow_json, input_data: {"result": None, "nodes": [], "error": "boom"}
    run = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="shadow"))
    out = run.invoke({"canary_key": "k1"})
    assert out["ok"] is True  # caller unaffected
    assert out["canary"]["shadow"]["ok"] is False
    rep = run.report()
    assert rep["candidate"]["success_rate"] == 0.0


def test_shadow_once_helper():
    fake = seed(FakeConsole())
    fake.dry_run_handler = lambda flow_json, input_data: {"result": {"v": 2}, "nodes": [], "error": None}
    out = shadow_once(make_client(fake), "demo", "1.1.0", {"name": "ada"})
    assert out["baseline_version"] == "1.0.0"
    assert out["shadow"]["output"] == {"v": 2}


# -- verdict ----------------------------------------------------------------------


def test_verdict_paths():
    fake = seed(FakeConsole())
    run = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="split", ratio=1.0))
    assert run.verdict()["recommendation"] == "keep_running"  # no traffic yet

    # shadow mode records both arms per invocation; baseline real + candidate dry-run
    fake.dry_run_handler = lambda flow_json, input_data: {"result": {"v": 2}, "nodes": [], "error": None}
    shadow = CanaryRun(make_client(fake), "demo", "1.1.0", policy=CanaryPolicy(mode="shadow"))
    for i in range(10):
        shadow.invoke({"canary_key": f"k{i}", "name": "ok"})
    verdict = shadow.verdict(min_count=10)
    assert verdict["recommendation"] == "promote"
    assert "publish" in verdict["promotion_command"]

    # a candidate that never succeeds → rollback
    fake2 = seed(FakeConsole())
    fake2.dry_run_handler = lambda flow_json, input_data: {"result": None, "nodes": [], "error": "boom"}
    bad = CanaryRun(make_client(fake2), "demo", "1.1.0", policy=CanaryPolicy(mode="shadow"))
    for i in range(10):
        bad.invoke({"canary_key": f"k{i}", "name": "ok"})
    assert bad.verdict(min_count=10)["recommendation"] == "rollback"


def test_shadow_baseline_dry_run_fallback():
    """Consoles without a queue: baseline_mode='dry-run' keeps both arms in-process."""
    fake = seed(FakeConsole())
    fake.dry_run_handler = lambda flow_json, input_data: {
        "result": {"dry": flow_json}, "nodes": [], "error": None
    }
    run = CanaryRun(
        make_client(fake), "demo", "1.1.0",
        policy=CanaryPolicy(mode="shadow", baseline_mode="dry-run"),
    )
    out = run.invoke({"canary_key": "k1"})
    assert out["ok"] is True
    rep = run.report()
    assert rep["baseline"]["count"] == 1 and rep["candidate"]["count"] == 1
    assert rep["baseline"]["success_rate"] == 1.0
    with pytest.raises(ValueError):
        CanaryPolicy(mode="shadow", baseline_mode="quantum")
