"""A69-02 一次性回填脚本（``scripts/migrate_console_events.py``）语义回归。

脚本是升级窗口内「旧 24h legacy 记录 → 引擎正典 ``events:{id}``/``types:{type}``」
的唯一人工迁移路径，规格（duo-a-ledger r70 §2.4）要求：

* 默认 dry-run，``--apply`` 才写入；不删旧键、canonical 已存在则跳过、幂等；
* 按 legacy 剩余 TTL 复制（不超过正典默认 7d）；
* 畸形/缺 timestamp/已消失记录跳过，不中断整批。

本组用例覆盖此前无测试的两条边界：``TTL`` 返回 ``0``（键仍在但剩余不足
1 秒）不得以 ``SET ... EX 0`` 中断整批；以及正常的 dry-run/apply/幂等语义。
"""
from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.redis import event_record_key

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "migrate_console_events",
    REPO_ROOT / "scripts" / "migrate_console_events.py",
)
backfill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backfill)

LEGACY_PREFIX = "plaita:event:"


def _legacy(client, event_id: str, ttl: int = 3600, **overrides) -> None:
    record = {
        "event_id": event_id,
        "event_type": "approval",
        "timestamp": time.time(),
        "data": {},
    }
    record.update(overrides)
    client.set(f"{LEGACY_PREFIX}{event_id}", json.dumps(record), ex=ttl)


def test_dry_run_writes_nothing_and_apply_is_idempotent():
    client = fakeredis.FakeRedis(decode_responses=True)
    _legacy(client, "evt-1")

    dry = backfill.migrate(client, apply=False)
    assert dry["migrated"] == 1
    assert client.exists(event_record_key("evt-1")) == 0, "dry-run 不得写 canonical"

    applied = backfill.migrate(client, apply=True)
    assert applied["migrated"] == 1
    assert client.exists(event_record_key("evt-1")) == 1
    assert client.exists(f"{LEGACY_PREFIX}evt-1") == 1, "回填不得删除 legacy 键"
    assert 0 < client.ttl(event_record_key("evt-1")) <= 3600, "应按剩余 TTL 复制"

    again = backfill.migrate(client, apply=True)
    assert again["migrated"] == 0
    assert again["skipped_existing"] == 1, "重复执行必须幂等"


def test_zero_remaining_ttl_is_skipped_not_crashing(monkeypatch):
    """``TTL`` 返回 0 = 键即将过期；``SET ... EX 0`` 会被 Redis 拒绝。"""
    client = fakeredis.FakeRedis(decode_responses=True)
    _legacy(client, "evt-last-second")
    monkeypatch.setattr(client, "ttl", lambda key: 0)

    stats = backfill.migrate(client, apply=True)

    assert stats["migrated"] == 0
    assert stats["skipped_expired"] == 1
    assert client.exists(event_record_key("evt-last-second")) == 0


def test_missing_event_id_derives_from_key_and_stays_idempotent():
    """记录体缺 event_id 时按 legacy 键后缀派生，重复执行不得生成新记录。

    ``Event`` 对缺失 ``event_id`` 会生成新 uuid；旧实现每次 ``--apply`` 都写一条
    不同 id 的 canonical 记录，既破坏脚本「重复执行不制造重复事件」的承诺，也让
    operator 误以为回填在推进。legacy 键形本身携带 id，派生即可。
    """
    client = fakeredis.FakeRedis(decode_responses=True)
    client.set(
        f"{LEGACY_PREFIX}evt-no-id",
        json.dumps({
            "event_type": "approval",
            "timestamp": time.time(),
            "data": {"legacy": True},
        }),
        ex=3600,
    )

    first = backfill.migrate(client, apply=True)
    assert first["migrated"] == 1
    assert first["skipped_invalid"] == 0
    assert client.exists(event_record_key("evt-no-id")) == 1, (
        "缺失 event_id 未按 legacy 键后缀派生"
    )

    second = backfill.migrate(client, apply=True)
    assert second["migrated"] == 0
    assert second["skipped_existing"] == 1
    canonical = list(client.scan_iter("plaita:event:events:*"))
    assert len(canonical) == 1, f"重复回填制造了额外 canonical 记录: {canonical}"


def test_malformed_and_missing_timestamp_are_skipped():
    client = fakeredis.FakeRedis(decode_responses=True)
    client.set(f"{LEGACY_PREFIX}bad-json", "not-json", ex=3600)
    _legacy(client, "no-timestamp", timestamp=None)
    # 去掉 timestamp 字段（_legacy 的 None 会被 json 序列化保留）
    record = json.loads(client.get(f"{LEGACY_PREFIX}no-timestamp"))
    record.pop("timestamp", None)
    client.set(f"{LEGACY_PREFIX}no-timestamp", json.dumps(record), ex=3600)
    _legacy(client, "good")

    stats = backfill.migrate(client, apply=True)

    assert stats["migrated"] == 1
    assert stats["skipped_invalid"] == 2
    assert client.exists(event_record_key("good")) == 1
    assert client.exists(event_record_key("bad-json")) == 0
    assert client.exists(event_record_key("no-timestamp")) == 0
