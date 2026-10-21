"""#272 (r53): worker CLI budget overrides must not diverge from producers.

Producers (console/schedule/event_filter) hash the four budget values from env
only (``envelope_for_stream``). A worker started with legal CLI
``--lease-ttl-seconds 600 --max-deliveries 3`` but env defaults 120/5 hashes a
different ``budget_generation``: first delivery executes, but a failed/crashed
carrier left in PEL is rejected by the worker's budget gate forever
(``config_budget_stale``, pending until the 30-day age gate).

Fix: the worker CLI preflight compares its actual four budget values with
``envelope_budget_values()`` (the producer tuple). Any mismatch is a named
``ConfigError``/exit 2 giving both values and the env key; env-only or matching
CLI values proceed, so both ends hash the same budget.
"""
from __future__ import annotations

import argparse
import sys
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.runtime_config import (
    DEFAULT_DLQ_MAX_LEN,
    DEFAULT_LEASE_TTL_SECONDS,
    DEFAULT_MAX_DELIVERIES,
    budget_generation,
    envelope_budget_values,
    envelope_for_stream,
    resolve_consumer_group,
    resolve_dlq_key,
    resolve_queue_name,
)
from plaita.server import flow_worker
from plaita.server.flow_worker import RedisFlowWorker, _cli_startup_problems, main
from plaita.server.task_queue import RedisStreamTaskQueue, enqueue_task
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

STREAM = "test:r53-budget"
DEFAULT_AGE = 30 * 86400
BUDGET_ENV = (
    "PLAITA_MAX_DELIVERIES",
    "PLAITA_LEASE_TTL_SECONDS",
    "PLAITA_TASK_MAX_AGE_SECONDS",
    "PLAITA_DLQ_MAX_LEN",
)


@pytest.fixture(autouse=True)
def _clean_budget_env(monkeypatch):
    for name in BUDGET_ENV + ("PLAITA_CONSUMER_GROUP", "PLAITA_DLQ_KEY"):
        monkeypatch.delenv(name, raising=False)


def _args(**overrides):
    queue = resolve_queue_name("worker")
    values = {
        "queue_name": queue,
        "consumer_group": resolve_consumer_group(),
        "dlq_key": resolve_dlq_key(queue, empty_policy="default"),
        "lease_ttl_seconds": DEFAULT_LEASE_TTL_SECONDS,
        "max_deliveries": DEFAULT_MAX_DELIVERIES,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _budget(*, max_deliveries=5, lease=120, age=DEFAULT_AGE, dlq=1000):
    return budget_generation(
        max_deliveries=max_deliveries,
        lease_ttl_seconds=lease,
        task_max_age_seconds=age,
        dlq_max_len=dlq,
    )


def _queue(redis, consumer, budget, age=DEFAULT_AGE):
    return RedisStreamTaskQueue(
        redis,
        STREAM,
        group_name="g",
        consumer_name=consumer,
        claim_min_idle_ms=0,
        budget_generation=budget,
        task_max_age_seconds=age,
    )


def test_cli_budget_override_rejected_with_both_values():
    problems = _cli_startup_problems(
        _args(lease_ttl_seconds=600, max_deliveries=3)
    )
    text = "\n".join(problems)
    assert "--lease-ttl-seconds" in text
    assert "600" in text and str(DEFAULT_LEASE_TTL_SECONDS) in text
    assert "PLAITA_LEASE_TTL_SECONDS" in text
    assert "--max-deliveries" in text
    assert "3" in text and str(DEFAULT_MAX_DELIVERIES) in text
    assert "PLAITA_MAX_DELIVERIES" in text


def test_matching_env_and_cli_budget_accepted():
    env = {"PLAITA_LEASE_TTL_SECONDS": "600", "PLAITA_MAX_DELIVERIES": "3"}
    with patch.dict("os.environ", env, clear=False):
        problems = _cli_startup_problems(
            _args(lease_ttl_seconds=600, max_deliveries=3)
        )
    assert problems == []


@pytest.mark.parametrize(
    "key,actual,expected,label,env_key",
    [
        ("lease_ttl_seconds", 600, 120, "--lease-ttl-seconds", "PLAITA_LEASE_TTL_SECONDS"),
        ("max_deliveries", 3, 5, "--max-deliveries", "PLAITA_MAX_DELIVERIES"),
        ("task_max_age_seconds", 1, DEFAULT_AGE, "task_max_age_seconds", "PLAITA_TASK_MAX_AGE_SECONDS"),
        ("dlq_max_len", 7, 1000, "dlq_max_len", "PLAITA_DLQ_MAX_LEN"),
    ],
)
def test_each_budget_key_mismatch_rejected(
    key, actual, expected, label, env_key, monkeypatch
):
    args = _args()
    if key == "lease_ttl_seconds":
        args = _args(lease_ttl_seconds=actual)
    elif key == "max_deliveries":
        args = _args(max_deliveries=actual)
    elif key == "task_max_age_seconds":
        monkeypatch.setattr(flow_worker, "task_max_age_seconds", lambda: actual)
    else:
        monkeypatch.setattr(flow_worker, "_dlq_max_len_from_env", lambda: actual)

    text = "\n".join(_cli_startup_problems(args))
    assert label in text
    assert str(actual) in text
    assert str(expected) in text
    assert env_key in text


def test_main_rejects_cli_budget_mismatch(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "plaita-worker",
            "--lease-ttl-seconds",
            "600",
            "--max-deliveries",
            "3",
        ],
    )
    with patch("plaita.server.flow_worker.RedisFlowWorker") as worker_cls:
        with pytest.raises(SystemExit) as excinfo:
            main()
    assert excinfo.value.code == 2
    assert not worker_cls.called
    err = capsys.readouterr().err
    assert "600" in err and "120" in err and "PLAITA_LEASE_TTL_SECONDS" in err
    assert "3" in err and "5" in err and "PLAITA_MAX_DELIVERIES" in err


def test_envelope_budget_values_are_the_producer_source():
    env = {"PLAITA_LEASE_TTL_SECONDS": "600", "PLAITA_MAX_DELIVERIES": "3"}
    values = envelope_budget_values(env)
    assert values["lease_ttl_seconds"] == 600
    assert values["max_deliveries"] == 3
    envelope = envelope_for_stream(STREAM, role="console", environ=env)
    assert envelope["budget_generation"] == budget_generation(**values)


def test_matching_budget_reclaims_failed_carrier():
    """env/CLI agree at 600/3: producer and worker hashes match; PEL reclaims."""
    env = {"PLAITA_LEASE_TTL_SECONDS": "600", "PLAITA_MAX_DELIVERIES": "3"}
    producer_budget = envelope_for_stream(
        STREAM, role="console", environ=env
    )["budget_generation"]
    assert producer_budget == _budget(max_deliveries=3, lease=600)

    redis = fakeredis.FakeRedis(decode_responses=True)
    enqueue_task(
        redis,
        STREAM,
        {"type": "start", "flow_id": "f", "execution_id": "e"},
        budget_generation=producer_budget,
    )
    seed = _queue(redis, "c1", producer_budget)
    seed.ensure_group()
    assert seed.read(block_ms=1) is not None

    reader = _queue(redis, "c2", producer_budget)
    reclaimed = reader.read(block_ms=1)
    assert reclaimed is not None
    assert reclaimed.body["execution_id"] == "e"
    assert reader.stats()["config_budget_stale"] == 0


def test_divergent_cli_budget_would_strand_pel():
    """Pre-fix probe: producer default 120/5, worker queue 600/3 -> no reclaim.

    This bypasses the CLI preflight (constructs the queue directly) to pin the
    failure mode the preflight now prevents.
    """
    producer_budget = envelope_for_stream(
        STREAM, role="console", environ={}
    )["budget_generation"]
    worker_budget = _budget(max_deliveries=3, lease=600)
    assert producer_budget != worker_budget

    redis = fakeredis.FakeRedis(decode_responses=True)
    enqueue_task(
        redis,
        STREAM,
        {"type": "start", "flow_id": "f", "execution_id": "e"},
        budget_generation=producer_budget,
    )
    seed = _queue(redis, "c1", worker_budget)
    seed.ensure_group()
    assert seed.read(block_ms=1) is not None  # first delivery does not gate budget

    reader = _queue(redis, "c2", worker_budget)
    assert reader.read(block_ms=1) is None
    assert reader.stats()["config_budget_stale"] > 0
