"""Tests for plaita_ai.console_client against the in-memory fake console."""

from __future__ import annotations

import pytest

from plaita_ai.console_client import (
    TERMINAL_STATUSES,
    ConsoleClient,
    ConsoleClientError,
    ConsoleConfig,
)

from ._console_fake import FakeConsole


def make_client(fake: FakeConsole, **config_overrides) -> ConsoleClient:
    config_kwargs = {"admin_api_key": "test-key"}
    config_kwargs.update(config_overrides)
    config = ConsoleConfig(base_url="http://fake", **config_kwargs)
    return ConsoleClient(config, transport=fake.transport())


def test_from_env_requires_url():
    with pytest.raises(ConsoleClientError, match="PLAITA_CONSOLE_URL"):
        ConsoleConfig.from_env({})


def test_admin_key_auth_flow():
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": "{}"}, published="1.0.0")
    client = make_client(fake)
    flows = client.list_flows()
    assert flows["total"] == 1
    # Every request carried the admin key (the fake 401s otherwise).
    assert any(m == "GET" and p == "/api/flows" for m, p in fake.requests)


def test_login_fallback_when_no_admin_key():
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": "{}"}, published="1.0.0")
    client = make_client(fake, admin_api_key=None, username="sup", password="pw")
    flows = client.list_flows()
    assert flows["total"] == 1
    # login happened once, then the session token rode along
    assert ("POST", "/api/auth/login") in [(m, p) for m, p in fake.requests]
    bad = make_client(fake, admin_api_key=None, username="wrong", password="creds")
    with pytest.raises(ConsoleClientError, match="401"):
        bad.list_flows()


def test_save_and_publish_version():
    fake = FakeConsole()
    fake.seed_flow("demo", {"1.0.0": '{"a": 1}'}, published="1.0.0")
    client = make_client(fake)
    saved = client.save_version("demo", "1.0.1", '{"a": 2}', created_by="sup")
    assert saved["version"] == "1.0.1"
    detail = client.get_flow("demo")
    versions = {v["version"]: v["status"] for v in detail["versions"]}
    assert versions == {"1.0.0": "published", "1.0.1": "draft"}
    published = client.publish_version("demo", "1.0.1")
    assert published["status"] == "published"
    assert fake.published["demo"] == "1.0.1"


def test_dry_run_passthrough():
    fake = FakeConsole()

    def handler(flow_json, input_data):
        return {"result": {"greeting": f"hi {input_data.get('name')}"}, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    client = make_client(fake)
    out = client.dry_run('{"x": 1}', input={"name": "ada"})
    assert out["result"] == {"greeting": "hi ada"}
    assert out["error"] is None


def test_wait_execution_terminal_and_timeout():
    fake = FakeConsole()
    exec_id = fake.seed_execution("demo", status="completed", output={"done": True})
    client = make_client(fake)
    done = client.wait_execution(exec_id, timeout_s=1, poll_s=0.01)
    assert done["status"] == "completed"

    stuck = fake.seed_execution("demo", status="running")
    with pytest.raises(ConsoleClientError, match="still 'running'"):
        client.wait_execution(stuck, timeout_s=0.05, poll_s=0.01)


def test_error_carries_status_and_detail():
    fake = FakeConsole()
    client = make_client(fake)
    with pytest.raises(ConsoleClientError) as excinfo:
        client.get_flow("missing")
    assert excinfo.value.status == 404
    assert "no such flow" in str(excinfo.value.detail)


def test_terminal_statuses():
    assert TERMINAL_STATUSES == frozenset({"completed", "failed", "error", "cancelled"})
