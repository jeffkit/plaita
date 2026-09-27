"""Tests for plaita_ai.ops — versions, semver, diff, metrics."""

from __future__ import annotations

import pytest

from plaita_ai.console_client import ConsoleClient, ConsoleConfig
from plaita_ai.ops import (
    flow_metrics,
    flow_versions,
    latest_version,
    next_patch_version,
    parse_semver,
    published_version,
    version_diff,
)

from ._console_fake import FakeConsole


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport())


# -- semver helpers ------------------------------------------------------------


def test_parse_and_bump_semver():
    assert parse_semver("1.2.3") == (1, 2, 3)
    assert parse_semver("v1.2.3") is None
    assert parse_semver("") is None
    assert next_patch_version("1.2.3") == "1.2.4"
    assert next_patch_version("0.0.9") == "0.0.10"
    with pytest.raises(Exception):
        next_patch_version("latest")


def test_latest_and_published():
    versions = [
        {"version": "1.0.0", "status": "published"},
        {"version": "1.0.2", "status": "draft"},
        {"version": "1.0.10", "status": "draft"},
        {"version": "not-semver", "status": "draft"},
    ]
    assert latest_version(versions) == "1.0.10"
    assert published_version(versions) == "1.0.0"
    assert published_version([{"version": "1.0.0", "status": "draft"}]) is None


# -- console-backed ops ----------------------------------------------------------


def test_flow_versions_sorted_and_flagged():
    fake = FakeConsole()
    fake.seed_flow(
        "demo",
        {"1.0.0": "{}", "1.0.1": "{}", "0.9.0": "{}"},
        published="1.0.0",
    )
    result = flow_versions(make_client(fake), "demo")
    assert result["latest"] == "1.0.1"
    assert result["published"] == "1.0.0"
    assert [v["version"] for v in result["versions"]] == ["0.9.0", "1.0.0", "1.0.1"]
    flagged = {v["version"]: v["is_published"] for v in result["versions"]}
    assert flagged == {"0.9.0": False, "1.0.0": True, "1.0.1": False}


def test_version_diff_changed_and_same():
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"a": 1}', "1.1.0": '{"a": 2}'})
    client = make_client(fake)
    changed = version_diff(client, "demo", "1.0.0", "1.1.0")
    assert changed["changed"]
    assert "-{\"a\": 1}" in changed["unified_diff"]
    assert "+{\"a\": 2}" in changed["unified_diff"]
    same = version_diff(client, "demo", "1.0.0", "1.0.0")
    assert not same["changed"]
    assert same["unified_diff"] == ""


def test_flow_metrics_aggregation():
    fake = FakeConsole()
    fake.seed_execution("demo", status="completed", start_time="2026-09-26T10:00:00", end_time="2026-09-26T10:00:02")
    fake.seed_execution("demo", status="completed", start_time="2026-09-26T10:01:00", end_time="2026-09-26T10:01:10")
    fake.seed_execution(
        "demo",
        status="failed",
        error={"message": "boom"},
        start_time="2026-09-26T10:02:00",
        end_time="2026-09-26T10:02:01",
    )
    metrics = flow_metrics(make_client(fake), "demo")
    assert metrics["scanned"] == 3
    assert metrics["by_status"] == {"completed": 2, "failed": 1}
    assert metrics["success_rate"] == round(2 / 3, 4)
    assert metrics["avg_duration_s"] == round((2 + 10 + 1) / 3, 3)
    assert len(metrics["recent_failures"]) == 1
    assert metrics["recent_failures"][0]["error"] == {"message": "boom"}


def test_flow_metrics_empty_flow():
    fake = FakeConsole()
    metrics = flow_metrics(make_client(fake), "demo")
    assert metrics["scanned"] == 0
    assert metrics["success_rate"] is None
    assert metrics["avg_duration_s"] is None


def test_published_version_picks_highest_among_ever_published():
    # Real console keeps status="published" on every previously published
    # version — production resolution must take the highest semver.
    versions = [
        {"version": "1.0.0", "status": "published"},
        {"version": "2.0.0", "status": "published"},
        {"version": "2.0.1", "status": "draft"},
    ]
    assert published_version(versions) == "2.0.0"
