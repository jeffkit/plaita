"""#275 (r54): CLI identity values must be normalized before use, not just compared.

#269 stripped only the comparison copies. ``--consumer-group "plaita-workers "``
passed the guard (stripped equal) while the worker actually joined the distinct
group ``"plaita-workers "``: the same stream message was delivered to both
groups. ``--dlq-key`` had the same trailing-space hole; ``--queue-name`` was not
guarded at all (worker consumed a different stream than producers wrote).

Fix: ``_cli_startup_problems`` strips and **writes back** queue/group/DLQ on
``args`` before judging; queue-name must be non-empty and equal to
``resolve_queue_name("worker")`` (env/default), with both values + env key in
the error. A truly different value is still rejected.
"""
from __future__ import annotations

import argparse

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
from plaita.server.flow_worker import _cli_startup_problems
from plaita.server.task_queue import RedisStreamTaskQueue, enqueue_task

STREAM = "test:r54-cli"


@pytest.fixture(autouse=True)
def _clean_identity_env(monkeypatch):
    for name in (
        "PLAITA_QUEUE_NAME",
        "QUEUE_NAME",
        "PLAITA_CONSUMER_GROUP",
        "PLAITA_DLQ_KEY",
    ):
        monkeypatch.delenv(name, raising=False)


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


class TestNormalization:
    def test_group_trailing_space_written_back(self):
        args = _args(consumer_group="plaita-workers ")
        assert _cli_startup_problems(args) == []
        assert args.consumer_group == "plaita-workers"

    def test_dlq_trailing_space_written_back(self):
        args = _args(dlq_key="plaita:flow:queue:dlq ")
        assert _cli_startup_problems(args) == []
        assert args.dlq_key == "plaita:flow:queue:dlq"

    def test_queue_trailing_space_written_back(self):
        args = _args(queue_name=" plaita:flow:queue ")
        assert _cli_startup_problems(args) == []
        assert args.queue_name == "plaita:flow:queue"

    def test_group_real_difference_still_rejected(self):
        text = "\n".join(
            _cli_startup_problems(_args(consumer_group="other-group"))
        )
        assert "--consumer-group" in text
        assert "other-group" in text
        assert "PLAITA_CONSUMER_GROUP" in text

    def test_dlq_real_difference_still_rejected(self):
        text = "\n".join(
            _cli_startup_problems(_args(dlq_key="custom:dlq"))
        )
        assert "--dlq-key" in text
        assert "custom:dlq" in text
        assert "PLAITA_DLQ_KEY" in text

    def test_queue_difference_rejected_with_both_streams(self):
        text = "\n".join(
            _cli_startup_problems(_args(queue_name="plaita:flow:other"))
        )
        assert "--queue-name" in text
        assert "plaita:flow:other" in text
        assert "plaita:flow:queue" in text
        assert "PLAITA_QUEUE_NAME" in text

    def test_empty_queue_rejected(self):
        text = "\n".join(_cli_startup_problems(_args(queue_name="   ")))
        assert "--queue-name" in text
        assert "empty value" in text


def test_raw_trailing_space_group_would_dual_deliver():
    """Evidence for the pre-fix mechanism the normalization now prevents.

    This bypasses the CLI guard and shows that Redis treats ``g`` and ``g ``
    as two consumer groups: the same Stream entry is delivered to both.
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    enqueue_task(redis, STREAM, {"type": "start", "flow_id": "f", "execution_id": "e"})
    q1 = RedisStreamTaskQueue(
        redis, STREAM, group_name="g", consumer_name="c1", claim_min_idle_ms=0
    )
    q2 = RedisStreamTaskQueue(
        redis, STREAM, group_name="g ", consumer_name="c2", claim_min_idle_ms=0
    )
    q1.ensure_group()
    q2.ensure_group()
    t1 = q1.read(block_ms=1)
    t2 = q2.read(block_ms=1)
    assert t1 is not None and t2 is not None
    assert t1.message_id == t2.message_id
