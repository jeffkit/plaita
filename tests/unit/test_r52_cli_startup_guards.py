"""#269/#270 (r95): CLI overrides must not bypass startup fail-fast.

#269: ``--consumer-group`` / ``--dlq-key`` are worker-only overrides while
console/schedule/event_filter write the env-resolved stream/group/DLQ. A
mismatching worker consumes a different topology than producers write to
(plaita#328: this startup check **is** the family gate now — the per-message
``config_family`` gate was deleted; a wrong-queue worker must fail to start
instead of stranding messages in PEL). The CLI preflight must reject a
mismatching override with both values; a CLI value matching env is accepted.

#270: ``--lease-ttl-seconds`` / ``--max-deliveries`` 0/negative previously
bypassed ``load_runtime_config(strict=True)`` (which ran before argparse) and
were silently clamped to 1 (1s lease has no renewal headroom). The CLI
preflight must fail fast with exit code 2, naming the flag and minimum, before
constructing the worker.
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
    resolve_consumer_group,
    resolve_dlq_key,
    resolve_queue_name,
)
from plaita.server.flow_worker import (
    RedisFlowWorker,
    _cli_startup_problems,
    main,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _args(**overrides):
    queue = resolve_queue_name("worker")
    values = {
        "queue_name": queue,
        "consumer_group": resolve_consumer_group(),
        "dlq_key": resolve_dlq_key(queue, empty_policy="default"),
        "lease_ttl_seconds": 120,
        "max_deliveries": 5,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest.fixture(autouse=True)
def _clean_identity_env(monkeypatch):
    for name in (
        "PLAITA_CONSUMER_GROUP",
        "PLAITA_DLQ_KEY",
        "PLAITA_LEASE_TTL_SECONDS",
        "PLAITA_MAX_DELIVERIES",
    ):
        monkeypatch.delenv(name, raising=False)


class TestCliStartsGuards:
    def test_group_override_rejected_with_both_values(self):
        problems = _cli_startup_problems(_args(consumer_group="cli-group"))
        text = "\n".join(problems)
        assert "--consumer-group" in text
        assert "cli-group" in text
        assert "plaita-workers" in text
        assert "whole deployment" in text

    def test_group_matching_env_accepted(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSUMER_GROUP", "cli-group")
        assert _cli_startup_problems(_args(consumer_group="cli-group")) == []

    def test_dlq_override_rejected_with_both_values(self):
        problems = _cli_startup_problems(_args(dlq_key="custom:dlq"))
        text = "\n".join(problems)
        assert "--dlq-key" in text
        assert "custom:dlq" in text
        assert "plaita:flow:queue:dlq" in text

    def test_dlq_matching_env_accepted(self, monkeypatch):
        monkeypatch.setenv("PLAITA_DLQ_KEY", "custom:dlq")
        assert _cli_startup_problems(_args(dlq_key="custom:dlq")) == []

    @pytest.mark.parametrize("value", [0, -1])
    def test_lease_below_one_rejected(self, value):
        problems = _cli_startup_problems(_args(lease_ttl_seconds=value))
        text = "\n".join(problems)
        assert "--lease-ttl-seconds" in text
        assert f"{value}" in text
        assert "minimum 1" in text

    @pytest.mark.parametrize("value", [0, -1])
    def test_max_deliveries_below_one_rejected(self, value):
        problems = _cli_startup_problems(_args(max_deliveries=value))
        text = "\n".join(problems)
        assert "--max-deliveries" in text
        assert f"{value}" in text
        assert "minimum 1" in text

    def test_lease_one_is_legal_but_extreme(self, monkeypatch):
        """1s is allowed when env agrees (issue #270 acceptance #3)."""
        monkeypatch.setenv("PLAITA_LEASE_TTL_SECONDS", "1")
        assert _cli_startup_problems(_args(lease_ttl_seconds=1)) == []
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name="test:r95-lease-one",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=fakeredis.FakeRedis(decode_responses=True),
            enable_registry=False,
            enable_redis_logging=False,
            lease_ttl_seconds=1,
        )
        assert worker.lease_ttl_seconds == 1
        assert worker._watchdog_interval() == 1.0


def _worker(**kwargs):
    queue = resolve_queue_name("worker")
    values = {
        "redis_url": "redis://localhost:6379/15",
        "queue_name": queue,
        "execution_storage": MemoryExecutionStorage(),
        "flow_storage": MemoryFlowStorage(),
        "redis_client": fakeredis.FakeRedis(decode_responses=True),
        "enable_registry": False,
        "enable_redis_logging": False,
        "consumer_group": resolve_consumer_group(),
        "dlq_key": resolve_dlq_key(queue, empty_policy="default"),
    }
    values.update(kwargs)
    return RedisFlowWorker(**values)


class TestTopologyAlignment:
    """plaita#328：family 哈希已删除——「同一部署」的判据直接是 worker 实际
    消费的 queue/group/dlq 三元组与生产者 env 解析值逐项相等。"""

    def test_accepted_cli_values_match_producer_topology(self):
        """#269 positive: accepted (env-matching) worker values == producer topology."""
        worker = _worker()
        queue = resolve_queue_name("worker")
        assert worker.queue_name == queue
        assert worker._consumer_group == resolve_consumer_group()
        assert worker._dlq_key == resolve_dlq_key(queue, empty_policy="default")

    def test_worker_only_group_override_is_rejected_at_startup(self):
        """旧 family 分叉机制已不存在；worker-only 覆盖由启动预检直接拒绝。"""
        problems = _cli_startup_problems(_args(consumer_group="cli-group"))
        assert problems and "--consumer-group" in "\n".join(problems)



class TestMainFailFast:
    def test_main_rejects_lease_zero_before_worker(self, monkeypatch, capsys):
        monkeypatch.setattr(
            sys, "argv", ["plaita-worker", "--lease-ttl-seconds", "0"]
        )
        with patch("plaita.server.flow_worker.RedisFlowWorker") as worker_cls:
            with pytest.raises(SystemExit) as excinfo:
                main()
        assert excinfo.value.code == 2
        assert not worker_cls.called
        err = capsys.readouterr().err
        assert "--lease-ttl-seconds" in err
        assert "minimum 1" in err

    def test_main_rejects_max_deliveries_zero_before_worker(
        self, monkeypatch, capsys
    ):
        monkeypatch.setattr(
            sys, "argv", ["plaita-worker", "--max-deliveries", "0"]
        )
        with patch("plaita.server.flow_worker.RedisFlowWorker") as worker_cls:
            with pytest.raises(SystemExit) as excinfo:
                main()
        assert excinfo.value.code == 2
        assert not worker_cls.called
        err = capsys.readouterr().err
        assert "--max-deliveries" in err
        assert "minimum 1" in err

    def test_main_rejects_group_override_before_worker(self, monkeypatch, capsys):
        monkeypatch.setattr(
            sys,
            "argv",
            ["plaita-worker", "--consumer-group", "cli-group"],
        )
        with patch("plaita.server.flow_worker.RedisFlowWorker") as worker_cls:
            with pytest.raises(SystemExit) as excinfo:
                main()
        assert excinfo.value.code == 2
        assert not worker_cls.called
        err = capsys.readouterr().err
        assert "--consumer-group" in err
        assert "cli-group" in err
        assert "plaita-workers" in err
