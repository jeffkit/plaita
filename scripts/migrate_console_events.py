#!/usr/bin/env python
"""一次性回填 console legacy 事件到引擎正典键形（台账 r70 A69-02，可选）。

**默认只报告、不写入**；``--apply`` 才真正落盘。需 operator 显式执行。

背景：A69-02 之前 console ``POST /api/events/publish`` 写
``plaita:event:{id}``（24h），引擎读面/回扫只认
``plaita:event:events:{id}`` + ``plaita:event:types:{type}``（7d）。升级后新
发布已直接写正典，但升级前 ≤24h 内发布、且 Pub/Sub 通知已丢失的手动事件，
回扫永远补不到。本脚本把这些记录按**剩余 TTL**（不超过正典默认 7d）复制成
正典记录。

设计约束（与 duo-a-ledger r70 §2.4 一致）：

* **不删旧键**：legacy 记录继续按原 TTL 自然过期，回滚/对照都安全；
* **不自动执行**：启动期扫描/并发写风险大于收益，只做一次性人工工具；
* **幂等**：canonical 记录已存在则跳过；重复执行不会制造重复事件；
* **跳过畸形记录**：缺 ``timestamp`` / 非法 JSON / 非 string 键一律跳过并计数。

用法::

    # 报告模式（默认）
    PLAITA_REDIS_URL=redis://localhost:6379/0 python scripts/migrate_console_events.py

    # 真正写入
    PLAITA_REDIS_URL=redis://localhost:6379/0 python scripts/migrate_console_events.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, Optional

# 脚本可能从 worktree/任意 cwd 执行；editable 安装可能指向另一个检出，
# 显式把**本脚本所在仓**前置到 sys.path（与 console tests 的 conftest 同法）。
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from plaita.core.url_redaction import _scrub_url_text
from plaita.event.core import Event
from plaita.event.redis import (
    event_record_key,
    event_retention_ttl_seconds,
    store_event_sync,
)
from plaita.redis_client import make_redis_client

LEGACY_PREFIX = "plaita:event:"
#: 这些前缀下的键不是 legacy 事件记录（正典/类型索引/频道/订阅/处理记录）。
_RESERVED_PREFIXES = (
    "plaita:event:events:",
    "plaita:event:types:",
    "plaita:event:type:",
    "plaita:event:channel:",
    "plaita:event:subscription:",
    "plaita:event:processing:",
)


def _scan_legacy_keys(client) -> list:
    keys = []
    cursor = 0
    while True:
        cursor, batch = client.scan(
            cursor=cursor, match=f"{LEGACY_PREFIX}*", count=200
        )
        for raw in batch:
            key = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
            if key.startswith(_RESERVED_PREFIXES):
                continue
            keys.append(key)
        if cursor == 0:
            break
    return keys


def _replay_ttl(client, key: str, default_ttl: int) -> Optional[int]:
    """legacy 记录剩余 TTL：``-1``（无过期）按默认；``-2`` / ``0`` 跳过。

    ``TTL`` 返回 ``0`` 表示键仍在但剩余不足 1 秒（最后一次采样可能正好落在
    过期边界）。此时不能拿 ``0`` 去 ``SET ... EX 0``——Redis 会以
    ``invalid expire time`` 拒绝并让整批回填中断；该记录也等价于已过期，
    跳过等它自然消失即可。
    """
    ttl = client.ttl(key)
    if ttl == -2:
        return None
    if ttl == -1:
        return default_ttl
    if ttl <= 0:
        return None
    return min(default_ttl, ttl)


def migrate(client, apply: bool = False) -> Dict[str, int]:
    default_ttl = event_retention_ttl_seconds()
    stats = {
        "scanned": 0,
        "migrated": 0,
        "skipped_existing": 0,
        "skipped_invalid": 0,
        "skipped_expired": 0,
    }
    for key in _scan_legacy_keys(client):
        stats["scanned"] += 1
        try:
            if client.type(key) not in ("string", b"string"):
                stats["skipped_invalid"] += 1
                continue
            raw = client.get(key)
            if not raw:
                stats["skipped_expired"] += 1
                continue
            info = json.loads(raw)
            if not isinstance(info, dict) or "timestamp" not in info:
                stats["skipped_invalid"] += 1
                continue
            # legacy 键形是 ``plaita:event:{event_id}``，旧 console 的记录体一定有
            # event_id。缺失/空白时从键后缀派生：``Event`` 会给缺失字段生成新
            # uuid，若原样校验，每次重跑都会写出一个新 canonical 记录，破坏幂等。
            event_id = info.get("event_id")
            if not isinstance(event_id, str) or not event_id.strip():
                derived = key[len(LEGACY_PREFIX):]
                if not derived:
                    stats["skipped_invalid"] += 1
                    continue
                info = {**info, "event_id": derived}
            event = Event.model_validate(info)
        except Exception as exc:  # noqa: BLE001 — 单条畸形不阻断整批
            stats["skipped_invalid"] += 1
            print(f"skip invalid {key}: {_scrub_url_text(exc)}")
            continue

        ttl = _replay_ttl(client, key, default_ttl)
        if ttl is None:
            stats["skipped_expired"] += 1
            continue
        if client.exists(event_record_key(event.event_id)) == 1:
            stats["skipped_existing"] += 1
            continue
        print(
            f"{'migrate' if apply else 'would migrate'} {key} -> "
            f"events:{event.event_id} (type={event.event_type}, ttl={ttl}s)"
        )
        if apply:
            # A78-01：记录按 legacy 剩余 TTL 复制，但类型桶 TTL 用正典默认值
            # 且只延长不缩短——否则一条剩余 5s 的 legacy 会把同类型 7d 记录
            # 的索引桶砍到 5s，桶到期后 list_events/回扫对活记录全盲。
            store_event_sync(client, event, ttl=ttl, index_ttl=default_ttl)
        stats["migrated"] += 1
    return stats


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="回填 console legacy 事件到引擎正典键形（A69-02，一次性工具）"
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("PLAITA_REDIS_URL", "redis://localhost:6379/0"),
        help="Redis 连接地址（默认取 PLAITA_REDIS_URL）",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="真正写入正典记录；缺省只报告（dry-run）",
    )
    args = parser.parse_args(argv)

    try:
        client = make_redis_client(args.redis_url, decode_responses=True)
        client.ping()
    except Exception as exc:  # noqa: BLE001 — 运维脚本：失败给退出码
        print(f"Redis 不可用: {_scrub_url_text(exc)}", file=sys.stderr)
        return 2

    stats = migrate(client, apply=args.apply)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] scanned={stats['scanned']} migrated={stats['migrated']} "
        f"existing={stats['skipped_existing']} invalid={stats['skipped_invalid']} "
        f"expired={stats['skipped_expired']}"
    )
    if not args.apply and stats["migrated"]:
        print("未写入任何键；确认后加 --apply 执行。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
