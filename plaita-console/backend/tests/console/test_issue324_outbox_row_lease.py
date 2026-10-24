"""#324：flow publish outbox 按行 lease——解除单副本约束。

规格（jeffkit/plaita#324；边界：不做全局 leader、不改 outbox 表结构）：
- 按行 lease：``{ns}:outbox:lease:{id}``，谁抢到谁投，TTL 短于重试间隔；
- 现有读回校验保留，重复写天然幂等；
- 验收：2 副本跑 1 小时，无重复交付；失败行按退避重试。

本文件为格级守恒（租约互斥/TTL 域/释放语义/退避保持/对账同租约）；
「2 副本无重复交付」的**跨进程行为级红相**只能脱离进程内
``OUTBOX_DELETE_LOCK`` 复现（同进程两线程本就被它串行化）——由
``docs/proofs/data/r97-lease-replica-driver.py``（fakeredis TCP 面两真
子进程 + 共享 SQLite）实弹，证据 ``docs/proofs/data/r97-lease-{pre,post}.txt``。

负控（修复前全红）：租约面新符号缺失（AttributeError）与新统计键缺失
（KeyError）；1 小时墙钟按正文裁量压缩为两副本×数十轮循环（真实性见 proofs）。
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fakeredis import FakeRedis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import FlowPublishOutbox  # noqa: E402
from services import engine_outbox  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402
from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402


def _demo_definition(flow_id: str) -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": "outbox row lease",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name",
                 "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def env_enabled(monkeypatch):
    monkeypatch.setenv(engine_outbox.TARGET_URL_ENV, "redis://target-env:6379/0")
    yield


@pytest.fixture()
def store(tmp_path) -> FlowStore:
    engine = create_engine(f"sqlite:///{tmp_path / 'lease.db'}", future=True)
    from models.flow import Base

    Base.metadata.create_all(engine)
    return FlowStore(sessionmaker(bind=engine, expire_on_commit=False))


@pytest.fixture()
def sessions(store: FlowStore):
    return store._session_local


@pytest.fixture()
def target_redis() -> FakeRedis:
    return FakeRedis(decode_responses=True)


def _rows(sessions, **filters):
    with sessions() as session:
        stmt = select(FlowPublishOutbox).order_by(FlowPublishOutbox.id.asc())
        for col, val in filters.items():
            stmt = stmt.where(getattr(FlowPublishOutbox, col) == val)
        return session.scalars(stmt).all()


def _publish_two(store: FlowStore):
    """种两条 pending outbox 行（flow a 先、flow b 后，id 序）。"""
    for fid in ("a", "b"):
        store.ensure_flow(fid)
        store.save_flow_definition(fid, "1.0.0", _demo_definition(fid), "{}")
        store.publish_version(fid, "1.0.0")


class Unreachable:
    """连接级不可达替身：任何属性访问即 ConnectionError（同 r71 负控形）。"""

    def __getattr__(self, name):
        raise ConnectionError("connection refused")


# ---- TTL 域（TTL 短于重试间隔） ----


def test_lease_ttl_default_is_below_retry_base():
    assert engine_outbox.DEFAULT_LEASE_TTL_SECONDS > 0
    assert (engine_outbox.DEFAULT_LEASE_TTL_SECONDS
            < engine_outbox._RETRY_BACKOFF_BASE_SECONDS)


def test_lease_ttl_env_valid_value_honored(monkeypatch):
    monkeypatch.setenv(engine_outbox.LEASE_TTL_ENV, "0.5")
    assert engine_outbox.lease_ttl_seconds() == 0.5


@pytest.mark.parametrize("raw", ["0", "-1", "2.0", "10", "inf", "nan", "abc"])
def test_lease_ttl_env_invalid_falls_back_with_warning(monkeypatch, caplog, raw):
    import logging

    monkeypatch.setenv(engine_outbox.LEASE_TTL_ENV, raw)
    with caplog.at_level(logging.WARNING):
        value = engine_outbox.lease_ttl_seconds()
    assert value == engine_outbox.DEFAULT_LEASE_TTL_SECONDS
    assert any(engine_outbox.LEASE_TTL_ENV in r.message for r in caplog.records)


def test_lease_key_shape_uses_tenant_namespace():
    assert engine_outbox._outbox_lease_key("t1", 7) == "plaita:t1:outbox:lease:7"
    assert engine_outbox._outbox_lease_key("", 7) == "plaita:outbox:lease:7"


# ---- 租约原语：互斥 / TTL / 释放 token 比对 ----


def test_lease_acquire_release_roundtrip(target_redis: FakeRedis):
    lease = engine_outbox.acquire_row_lease(target_redis, "", 7)
    assert lease is not None
    assert target_redis.exists("plaita:outbox:lease:7")
    lease.release()
    assert not target_redis.exists("plaita:outbox:lease:7")
    # 释放后他副本可立即抢到
    assert engine_outbox.acquire_row_lease(target_redis, "", 7) is not None


def test_lease_exclusive_among_replicas_per_row(target_redis: FakeRedis):
    first = engine_outbox.acquire_row_lease(target_redis, "", 7)
    assert first is not None
    # 他副本抢同一行 → 失败；抢另一行 → 成功（按行隔离）
    assert engine_outbox.acquire_row_lease(target_redis, "", 7) is None
    assert engine_outbox.acquire_row_lease(target_redis, "", 8) is not None


def test_lease_ttl_applied_as_px(monkeypatch, target_redis: FakeRedis):
    monkeypatch.setenv(engine_outbox.LEASE_TTL_ENV, "0.5")
    lease = engine_outbox.acquire_row_lease(target_redis, "", 7)
    assert lease is not None
    pttl = target_redis.pttl("plaita:outbox:lease:7")
    assert 0 < pttl <= 500


def test_stale_release_does_not_delete_foreign_lease(
        monkeypatch, target_redis: FakeRedis):
    """TTL 过期后迟到释放（token 不符）不得误删他副本租约。"""
    monkeypatch.setenv(engine_outbox.LEASE_TTL_ENV, "0.05")
    stale = engine_outbox.acquire_row_lease(target_redis, "", 7)
    assert stale is not None
    time.sleep(0.12)  # 过期
    current = engine_outbox.acquire_row_lease(target_redis, "", 7)
    assert current is not None
    stale.release()  # 迟到释放：token 不符 → 不删
    assert target_redis.exists("plaita:outbox:lease:7")
    assert engine_outbox.acquire_row_lease(target_redis, "", 7) is None
    current.release()  # 真持有者释放 → 可再抢
    assert engine_outbox.acquire_row_lease(target_redis, "", 7) is not None


def test_lease_acquire_unreachable_returns_open_sentinel(
        env_enabled, caplog):
    """租约面不可达 → LEASE_UNAVAILABLE 哨兵（fail-open，照常行动），
    且释放为 no-op（无 client 不炸）。"""
    import logging

    with caplog.at_level(logging.WARNING):
        lease = engine_outbox.acquire_row_lease(Unreachable(), "", 7)
    assert lease is engine_outbox.LEASE_UNAVAILABLE
    assert any("租约面不可达" in r.message for r in caplog.records)
    lease.release()  # no-op：不触碰任何 client


def test_delivery_unreachable_target_fail_open_keeps_retry_accounting(
        store: FlowStore, sessions, env_enabled):
    """目标不可达：租约 fail-open 行动，重试记账与单副本时代一致
    （attempts/退避/DLQ 链不受租约面改写）——#299 语义守恒格。"""
    _publish_two(store)
    stats = engine_outbox.run_delivery_cycle(Unreachable(), sessions)
    assert stats["lease_open"] == 2 and stats["lease_skipped"] == 0
    assert stats["retried"] == 2 and stats["delivered"] == 0
    for row in _rows(sessions):
        assert row.status == "pending" and row.attempts == 1
        assert row.next_attempt_at is not None


# ---- 投递面：谁抢到谁投 ----


def test_delivery_skips_row_held_by_other_replica(
        store: FlowStore, sessions, env_enabled, target_redis: FakeRedis):
    _publish_two(store)
    rows = _rows(sessions)
    held = engine_outbox.acquire_row_lease(target_redis, "", rows[0].id)
    assert held is not None

    stats = engine_outbox.run_delivery_cycle(target_redis, sessions)

    assert stats["delivered"] == 1 and stats["lease_skipped"] == 1
    assert stats["skipped"] == 0 and stats["errors"] == 0
    # 被持有行完全未动（无 key、无 attempts、仍 pending）
    assert not target_redis.exists("plaita:flow:a:1.0.0")
    row_a = _rows(sessions, flow_id="a")[0]
    assert row_a.status == "pending" and (row_a.attempts or 0) == 0
    # 未持有行正常投递（读回校验保留）
    assert target_redis.exists("plaita:flow:b:1.0.0")
    assert _rows(sessions, flow_id="b")[0].status == "delivered"

    held.release()
    stats2 = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert stats2["delivered"] == 1 and stats2["lease_skipped"] == 0
    assert target_redis.exists("plaita:flow:a:1.0.0")


def test_stale_lease_expires_retry_not_blocked(
        monkeypatch, store: FlowStore, sessions, env_enabled,
        target_redis: FakeRedis):
    """持有者崩溃（无释放）：TTL 过期后到期重试照常（TTL 短于重试间隔）。"""
    monkeypatch.setenv(engine_outbox.LEASE_TTL_ENV, "0.05")
    _publish_two(store)
    rows = _rows(sessions)
    crashed = engine_outbox.acquire_row_lease(target_redis, "", rows[0].id)
    assert crashed is not None  # 崩溃：不 release

    stats = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert stats["delivered"] == 1 and stats["lease_skipped"] == 1

    time.sleep(0.12)  # 租约过期
    stats2 = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert stats2["delivered"] == 1 and stats2["lease_skipped"] == 0
    assert _rows(sessions, flow_id="a")[0].status == "delivered"


def test_failed_delivery_releases_lease_backoff_intact(
        monkeypatch, store: FlowStore, sessions, env_enabled,
        target_redis: FakeRedis):
    """失败行：租约即放、退避表与单副本一致（attempts/next_attempt_at），
    退避到期后他副本可正常重投。"""
    calls = {"n": 0}
    from services.engine_sync import sync_flow_to_engine as real_sync

    def flaky_sync(client, flow_id, version, definition, tenant_id=""):
        calls["n"] += 1
        if calls["n"] <= 2:  # 每行首次尝试各失败一次
            raise ConnectionError("boom once")
        return real_sync(client, flow_id, version, definition,
                         tenant_id=tenant_id)

    monkeypatch.setattr(engine_outbox, "sync_flow_to_engine", flaky_sync)
    _publish_two(store)
    now = datetime.utcnow()

    first = engine_outbox.run_delivery_cycle(target_redis, sessions, now=now)
    assert first["retried"] == 2 and first["delivered"] == 0
    row_a = _rows(sessions, flow_id="a")[0]
    assert row_a.status == "pending" and row_a.attempts == 1
    assert row_a.next_attempt_at is not None
    delta = (row_a.next_attempt_at - now).total_seconds()
    assert delta == pytest.approx(
        engine_outbox._RETRY_BACKOFF_BASE_SECONDS, abs=0.1)

    # 退避期内：未到期不投；他副本可抢到租约（失败即放、无残留）
    probe = engine_outbox.acquire_row_lease(target_redis, "", row_a.id)
    assert probe is not None
    probe.release()
    early = engine_outbox.run_delivery_cycle(
        target_redis, sessions,
        now=row_a.next_attempt_at - timedelta(milliseconds=100))
    assert early["scanned"] == 0

    # 退避到期：重投成功，读回校验后 delivered
    calls["n"] = 99  # 后续全成功
    done = engine_outbox.run_delivery_cycle(
        target_redis, sessions, now=row_a.next_attempt_at + timedelta(seconds=1))
    assert done["delivered"] == 2
    assert target_redis.exists("plaita:flow:a:1.0.0")
    assert _rows(sessions, flow_id="a")[0].status == "delivered"


# ---- 对账面：同租约纪律 ----


def test_reconcile_skips_row_held_by_other_replica(
        store: FlowStore, sessions, env_enabled, target_redis: FakeRedis):
    _publish_two(store)
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)[
        "delivered"] == 2
    rows = _rows(sessions)
    held = engine_outbox.acquire_row_lease(target_redis, "", rows[0].id)
    assert held is not None

    stats = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert stats["checked"] == 2 and stats["lease_skipped"] == 1
    assert stats["requeued"] == 0 and stats["errors"] == 0
    assert _rows(sessions, flow_id="a")[0].status == "delivered"

    held.release()
    healthy = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert healthy["checked"] == 2 and healthy["lease_skipped"] == 0
    assert healthy["requeued"] == 0
    assert _rows(sessions, flow_id="a")[0].status == "delivered"


def test_reconcile_requeue_releases_lease(
        store: FlowStore, sessions, env_enabled, target_redis: FakeRedis):
    _publish_two(store)
    assert engine_outbox.run_delivery_cycle(target_redis, sessions)[
        "delivered"] == 2
    target_redis.delete("plaita:flow:a:1.0.0")  # 目标缺 key → 应重投

    stats = engine_outbox.run_reconcile_cycle(target_redis, sessions)
    assert stats["requeued"] == 1 and stats["lease_skipped"] == 0
    row_a = _rows(sessions, flow_id="a")[0]
    assert row_a.status == "pending"
    # 重投动作已完成，租约即放（他副本可抢；探针用完即放）
    probe = engine_outbox.acquire_row_lease(target_redis, "", row_a.id)
    assert probe is not None
    probe.release()

    done = engine_outbox.run_delivery_cycle(target_redis, sessions)
    assert done["delivered"] == 1
    assert _rows(sessions, flow_id="a")[0].status == "delivered"
