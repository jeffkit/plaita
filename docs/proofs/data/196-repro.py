#!/usr/bin/env python3
"""plaita#196 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/196-repro.py

Two scenarios (fakeredis, no real Redis):

1. the issue's 9-message repro: first 8 leases active, ``e8`` is the only
   orphan; three ``_reclaim_one`` rounds must surface ``e8``;
2. the acceptance scenario: 20 pending, first 15 active, last 5 orphans; five
   orphans must be reclaimed within 10 rounds and the active entries'
   ``times_delivered`` must stay at 1 (#47 guard unchanged).
"""
from __future__ import annotations

import json
import sys

import fakeredis

from plaita.server.task_queue import PAYLOAD_FIELD, RedisStreamTaskQueue


def _seed(redis, stream, count):
    writer = RedisStreamTaskQueue(
        redis, stream, group_name="g", consumer_name="dead", claim_min_idle_ms=0
    )
    writer.ensure_group()
    for i in range(count):
        redis.xadd(stream, {
            PAYLOAD_FIELD: json.dumps({"type": "start", "execution_id": f"e{i}"}),
        })
    redis.xreadgroup("g", "dead", {stream: ">"}, count=count)
    assert int(redis.xpending(stream, "g")["pending"]) == count


def _reader(redis, stream, active_before):
    return RedisStreamTaskQueue(
        redis,
        stream,
        group_name="g",
        consumer_name="c1",
        claim_min_idle_ms=0,
        reclaim_lease_guard=(
            lambda body: int(str(body["execution_id"]).lstrip("e")) < active_before
        ),
    )


def _times_delivered(redis, stream, mid):
    for entry in redis.xpending_range(stream, "g", min=mid, max=mid, count=1):
        return int(entry["times_delivered"])
    return 0


def _nine():
    stream = "plaita:proof:196:nine"
    redis = fakeredis.FakeRedis(decode_responses=True)
    _seed(redis, stream, 9)
    q = _reader(redis, stream, 8)
    reclaimed = []
    for _ in range(3):
        task = q._reclaim_one()
        if task is not None:
            reclaimed.append(task.body["execution_id"])
    return {
        "reclaimed": reclaimed,
        "pending_total": int(redis.xpending(stream, "g")["pending"]),
    }


def _twenty():
    stream = "plaita:proof:196:twenty"
    redis = fakeredis.FakeRedis(decode_responses=True)
    _seed(redis, stream, 20)
    q = _reader(redis, stream, 15)
    mids = [entry["message_id"] for entry in redis.xpending_range(
        stream, "g", min="-", max="+", count=20)]
    active = mids[:15]

    reclaimed = []
    rounds = 0
    for rounds in range(1, 11):
        task = q._reclaim_one()
        if task is not None:
            reclaimed.append(task.body["execution_id"])
        if len(reclaimed) == 5:
            break

    return {
        "reclaimed": sorted(reclaimed),
        "rounds": rounds,
        "active_times_delivered": [_times_delivered(redis, stream, mid) for mid in active],
        "pending_total": int(redis.xpending(stream, "g")["pending"]),
    }


def run() -> dict:
    return {
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "nine": _nine(),
        "twenty": _twenty(),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
