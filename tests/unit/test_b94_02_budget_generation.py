"""b94-02 (r94): budget_generation must hash the *effective* budget.

The worker passes its constructor/CLI-effective budget as ``overrides`` to
``load_runtime_config``.  Pre-fix only identity overrides were consumed; the
four budget knobs came from env-or-defaults (lease 300) while the worker
actually enforced its constructor values (default lease 120).  The hash and
``event=config.effective`` therefore described a budget the process did not
use, and aligning env to the documented 120 flipped the hash (false
``config.budget_stale``).

These tests pin four facts:
1. default deployment (env unset, init passes 120) hashes 120;
2. environment aligned to 120 leaves the hash/behavior unchanged;
3. CLI-style overrides change the hash accordingly;
4. ``config.effective`` self-reports exactly the enforced values.
"""
from __future__ import annotations

import logging
import os

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita import runtime_config as rc
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

BUDGET_ENV = (
    "PLAITA_MAX_DELIVERIES",
    "PLAITA_LEASE_TTL_SECONDS",
    "PLAITA_TASK_MAX_AGE_SECONDS",
    "PLAITA_DLQ_MAX_LEN",
)
DEFAULT_BUDGET = {
    "max_deliveries": 5,
    "lease_ttl_seconds": 120,
    "task_max_age_seconds": 30 * 24 * 3600,
    "dlq_max_len": 1000,
}


def _clean_env(extra=None):
    env = {k: v for k, v in os.environ.items() if k not in BUDGET_ENV}
    env.update(extra or {})
    return env


def _overrides(**kwargs):
    values = dict(DEFAULT_BUDGET)
    values.update(kwargs)
    return values


def _config(env=None, **overrides):
    return rc.load_runtime_config(
        "worker", environ=_clean_env(env), overrides=_overrides(**overrides)
    )


def test_default_deployment_hash_uses_actual_120():
    """env unset + init passes 120 ⇒ hash is built from 120, not 300."""
    cfg = _config()
    assert cfg.lease_ttl_seconds == 120
    assert cfg.generation.budget == rc.budget_generation(**DEFAULT_BUDGET)
    stale_300 = rc.budget_generation(
        **{**DEFAULT_BUDGET, "lease_ttl_seconds": 300}
    )
    assert cfg.generation.budget != stale_300


def test_env_aligned_to_120_does_not_flip_hash():
    """Documented alignment (PLAITA_LEASE_TTL_SECONDS=120) is behavior-neutral."""
    default_cfg = _config()
    env_cfg = rc.load_runtime_config(
        "worker", environ=_clean_env({"PLAITA_LEASE_TTL_SECONDS": "120"})
    )
    assert default_cfg.lease_ttl_seconds == env_cfg.lease_ttl_seconds == 120
    assert default_cfg.generation.budget == env_cfg.generation.budget
    assert default_cfg.generation.generation == env_cfg.generation.generation


def test_cli_overrides_change_hash():
    """Constructor/CLI budget changes must move the budget hash."""
    base = _config()
    cli_lease = _config(lease_ttl_seconds=600)
    cli_deliveries = _config(max_deliveries=3)
    assert cli_lease.lease_ttl_seconds == 600
    assert cli_lease.generation.budget != base.generation.budget
    assert cli_deliveries.max_deliveries == 3
    assert cli_deliveries.generation.budget != base.generation.budget


def test_remaining_budget_overrides_are_consumed():
    """task_max_age / dlq_max_len overrides are part of the hash too."""
    cfg = _config(task_max_age_seconds=3600, dlq_max_len=7)
    assert cfg.task_max_age_seconds == 3600
    assert cfg.dlq_max_len == 7
    assert cfg.generation.budget == rc.budget_generation(
        max_deliveries=5,
        lease_ttl_seconds=120,
        task_max_age_seconds=3600,
        dlq_max_len=7,
    )


def _worker(monkeypatch, caplog, **kwargs):
    for name in BUDGET_ENV:
        monkeypatch.delenv(name, raising=False)
    caplog.set_level(logging.INFO)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:b94-02-budget",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        enable_registry=False,
        enable_redis_logging=False,
        **kwargs,
    )
    lines = [
        record.getMessage()
        for record in caplog.records
        if "event=config.effective" in record.getMessage()
    ]
    assert lines, "worker did not emit event=config.effective"
    return worker, lines[-1]


def test_config_effective_matches_actual_default(monkeypatch, caplog):
    worker, line = _worker(monkeypatch, caplog)
    assert "lease_ttl=120" in line
    assert "max_deliveries=5" in line
    assert worker.lease_ttl_seconds == 120
    assert worker._runtime_config.lease_ttl_seconds == worker.lease_ttl_seconds
    assert worker._runtime_config.max_deliveries == worker._max_deliveries
    assert worker._runtime_config.generation.budget == rc.budget_generation(
        max_deliveries=worker._max_deliveries,
        lease_ttl_seconds=worker.lease_ttl_seconds,
        task_max_age_seconds=worker._runtime_config.task_max_age_seconds,
        dlq_max_len=worker._runtime_config.dlq_max_len,
    )


def test_config_effective_matches_cli_overrides(monkeypatch, caplog):
    worker, line = _worker(
        monkeypatch, caplog, lease_ttl_seconds=600, max_deliveries=3
    )
    assert "lease_ttl=600" in line
    assert "max_deliveries=3" in line
    assert worker.lease_ttl_seconds == 600
    assert worker._runtime_config.lease_ttl_seconds == 600
    assert worker._runtime_config.max_deliveries == 3 == worker._max_deliveries
    assert worker._runtime_config.generation.budget == rc.budget_generation(
        max_deliveries=3,
        lease_ttl_seconds=600,
        task_max_age_seconds=worker._runtime_config.task_max_age_seconds,
        dlq_max_len=worker._runtime_config.dlq_max_len,
    )
