#!/usr/bin/env python3
"""Drain legacy Redis List tasks into a Stream (List→Stream migration).

Usage:
  python scripts/drain_list_queue_to_stream.py \\
    --redis-url redis://localhost:6379/0 \\
    --list-key plaita:flow:queue \\
    --stream-key plaita:flow:queue:v2
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--list-key", required=True, help="旧 List 键")
    parser.add_argument("--stream-key", required=True, help="目标 Stream 键")
    parser.add_argument("--limit", type=int, default=0, help="最多迁移条数，0=全部")
    args = parser.parse_args()

    try:
        import redis
    except ImportError:
        print("需要 redis 包: pip install plaita[redis]", file=sys.stderr)
        return 1

    from plaita.server.task_queue import enqueue_task

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    moved = 0
    while True:
        if args.limit and moved >= args.limit:
            break
        raw = client.lpop(args.list_key)
        if raw is None:
            break
        try:
            task = json.loads(raw)
        except json.JSONDecodeError:
            print(f"skip invalid json: {raw!r}", file=sys.stderr)
            continue
        if not isinstance(task, dict):
            print(f"skip non-object: {raw!r}", file=sys.stderr)
            continue
        mid = enqueue_task(client, args.stream_key, task)
        moved += 1
        print(f"moved -> {args.stream_key} id={mid} type={task.get('type')}")

    left = client.llen(args.list_key)
    print(f"done: moved={moved}, list_remaining={left}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
