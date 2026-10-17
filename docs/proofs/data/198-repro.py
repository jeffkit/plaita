#!/usr/bin/env python3
"""plaita#198 repro / before-after probe.

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    PYTHONPATH=. .venv/bin/python docs/proofs/data/198-repro.py

Scenario 1 (interleaved): a stale ``enabled=True`` snapshot is read, a console
PUT disables the schedule (``enabled=False``), then ``fire_schedule`` writes
back from the stale snapshot.  Before the fix ``enabled`` flips back to true;
after the fix the CAS merge only touches the trigger fields.

Scenario 2 (CAS conflict): when ``cas_update_schedule`` exists, a console write
based on an old ``updated_at`` raises ``ScheduleConflictError`` (the API turns
this into a structured 409).  On the pre-fix tree the helper does not exist.
"""
from __future__ import annotations

import json
import sys

import fakeredis

from plaita.server.services.schedule_service import SCHEDULES_KEY, fire_schedule

try:  # pragma: no cover - absent on the pre-fix tree
    from plaita.server.services.schedule_service import (
        ScheduleConflictError,
        cas_update_schedule,
    )
except ImportError:  # pre-fix tree
    ScheduleConflictError = None  # type: ignore[assignment]
    cas_update_schedule = None  # type: ignore[assignment]

QUEUE = "plaita:flow:queue"


def _stale(**overrides):
    base = {
        "schedule_id": "s1",
        "tenant_id": "default",
        "name": "n",
        "flow_id": "f",
        "version": None,
        "cron": "* * * * *",
        "params": {},
        "enabled": True,
        "created_at": "t0",
        "updated_at": "t0",
        "next_run_at": "0",
    }
    base.update(overrides)
    return base


def _get(redis):
    return json.loads(redis.hget(SCHEDULES_KEY, "s1"))


def _interleaved():
    redis = fakeredis.FakeRedis(decode_responses=True)
    stale = _stale()
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(stale))

    # console PUT：读当前 → 停用 → 整份写回（旧实现）
    current = _get(redis)
    current["enabled"] = False
    current["updated_at"] = "t1"
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(current))
    after_put = _get(redis)["enabled"]

    fire_schedule(redis, stale, QUEUE, trigger_kind="cron", due_at=0)
    final = _get(redis)
    return {
        "after_put_enabled": after_put,
        "after_fire_enabled": final["enabled"],
        "last_fired_at": bool(final.get("last_fired_at")),
        "last_enqueue_ok": final.get("last_enqueue_ok"),
        "cron": final.get("cron"),
    }


def _cas_conflict():
    if cas_update_schedule is None:
        return {"available": False}
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.hset(SCHEDULES_KEY, "s1", json.dumps(_stale(updated_at="t2")))
    try:
        cas_update_schedule(
            redis, "s1", lambda cur: cur.update({"enabled": False}),
            expected_updated_at="t1", check_updated_at=True,
        )
    except ScheduleConflictError as exc:  # type: ignore[misc]
        return {
            "available": True,
            "conflict": True,
            "latest_updated_at": exc.latest_updated_at,
            "enabled_after": _get(redis)["enabled"],
        }
    return {"available": True, "conflict": False}


def run():
    return {
        "plaita_file": __import__("plaita").__file__,
        "python": sys.version.split()[0],
        "interleaved": _interleaved(),
        "cas": _cas_conflict(),
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
