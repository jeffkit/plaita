"""#271 (r95): PLAITA_TASK_MAX_AGE_SECONDS=0 must mean "age gate off" on both sides.

Worker side uses ``plaita.core.retention.task_max_age_seconds`` (0 for
``<=0``). Producer ``envelope_for_stream`` used ``_safe_int(minimum=1)`` and
turned 0 into the 30-day default, so producer and worker budget generations
differed. With the age gate off (0), a failed/crashed carrier left in PEL was
never reclaimed (``config_budget_stale`` forever) and had no age/DLX exit.

Fix: producers use the same core parser (0/negative -> 0, illegal -> 30d +
WARNING), so the budget hash matches the worker and reclaim can proceed.
"""
from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.retention import (
    DEFAULT_TASK_MAX_AGE_SECONDS,
    task_max_age_seconds,
)
from plaita.runtime_config import (
    DEFAULT_DLQ_MAX_LEN,
    DEFAULT_LEASE_TTL_SECONDS,
    DEFAULT_MAX_DELIVERIES,
    budget_generation,
    envelope_for_stream,
    load_runtime_config,
)
from plaita.server.task_queue import RedisStreamTaskQueue, enqueue_task

STREAM = "test:r95-age-zero"


def _budget(age: int) -> str:
    return budget_generation(
        max_deliveries=DEFAULT_MAX_DELIVERIES,
        lease_ttl_seconds=DEFAULT_LEASE_TTL_SECONDS,
        task_max_age_seconds=age,
        dlq_max_len=DEFAULT_DLQ_MAX_LEN,
    )


def _queue(redis, consumer: str, budget: str) -> RedisStreamTaskQueue:
    return RedisStreamTaskQueue(
        redis,
        STREAM,
        group_name="g",
        consumer_name=consumer,
        claim_min_idle_ms=0,
        budget_generation=budget,
        task_max_age_seconds=0,
    )


def test_age_zero_budget_matches_worker():
    env = {"PLAITA_TASK_MAX_AGE_SECONDS": "0"}
    envelope = envelope_for_stream(STREAM, role="worker", environ=env)
    cfg = load_runtime_config("worker", environ=env)

    assert cfg.task_max_age_seconds == 0
    assert envelope["budget_generation"] == _budget(0)
    assert envelope["budget_generation"] == cfg.generation.budget


def test_age_negative_keeps_same_zero_semantics():
    """core.retention's documented <=0 = off applies to producers too."""
    env = {"PLAITA_TASK_MAX_AGE_SECONDS": "-1"}
    assert task_max_age_seconds(env) == 0
    assert envelope_for_stream(STREAM, role="worker", environ=env)[
        "budget_generation"
    ] == _budget(0)


def test_age_illegal_falls_back_to_30d_with_warning(caplog):
    env = {"PLAITA_TASK_MAX_AGE_SECONDS": "not-a-number"}
    with caplog.at_level("WARNING"):
        assert task_max_age_seconds(env) == DEFAULT_TASK_MAX_AGE_SECONDS
    assert any(
        "PLAITA_TASK_MAX_AGE_SECONDS" in record.getMessage()
        for record in caplog.records
    )
    assert envelope_for_stream(STREAM, role="worker", environ=env)[
        "budget_generation"
    ] == _budget(DEFAULT_TASK_MAX_AGE_SECONDS)


def test_age_zero_reclaim_not_budget_stale():
    """age=0 carrier left in PEL is reclaimable (budget matches, no stale).

    Pre-fix the producer envelope used 30d while the worker used 0: reclaim
    returned None and ``config_budget_stale`` incremented forever.
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    env = {"PLAITA_TASK_MAX_AGE_SECONDS": "0"}
    producer_budget = envelope_for_stream(
        STREAM, role="worker", environ=env
    )["budget_generation"]
    enqueue_task(
        redis,
        STREAM,
        {"type": "start", "flow_id": "f", "execution_id": "e"},
        budget_generation=producer_budget,
    )

    worker_budget = _budget(0)
    # Post-fix producer/worker budgets are equal; the reclaim below is the
    # behavioral proof (pre-fix: mismatch -> None + config_budget_stale).
    seed = _queue(redis, "c1", worker_budget)
    seed.ensure_group()
    assert seed.read(block_ms=1) is not None  # first delivery -> PEL

    reader = _queue(redis, "c2", worker_budget)
    reclaimed = reader.read(block_ms=1)

    assert reclaimed is not None, (
        "age=0 carrier must be reclaimable; pre-fix producer used 30d while "
        "worker used 0 -> budget mismatch -> permanent pending"
    )
    assert reclaimed.body["execution_id"] == "e"
    assert producer_budget == worker_budget
    assert reader.stats()["config_budget_stale"] == 0
