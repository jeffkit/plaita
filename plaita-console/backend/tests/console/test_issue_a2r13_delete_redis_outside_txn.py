"""A2R13（r38 回归自修）：租户删除的 Redis 全量清理必须移出 SQLite 写事务。

## 缺陷（``BEGIN IMMEDIATE`` 持锁跨 Redis 清理）

``delete_tenant`` 为关闭删除 × 并发写窗口先 ``BEGIN IMMEDIATE``（A50-01/02），
随后在**同一事务内**做 Redis 全量清理（SCAN 租户命名空间、平台键逐条收窄、
共享队列遍历）。清理是秒级操作，写锁因此被持有数秒：

- 并发写（他租户照样是同一个 SQLite 写锁）等满 5s timeout →
  ``database is locked``；
- 清理中途失败 / commit 失败时连接未正常归还，连接池被 PENDING 状态拖住，
  后续请求全站 500 直到重启。

## 修法

1. DB 删除 + 墓碑（廉价 HSET）先提交——写锁毫秒级释放；
2. 秒级 Redis/队列清理放到事务**之外**；
3. 清理失败 loud 上抛，重试 DELETE 走 404 补清扫继续（#257 可重试语义）。

## 负控（必红）

- 把 ``_purge_runtime_after_commit`` 的两步调用挪回 ``session.commit()``
  之前 ⇒ ``test_slow_redis_purge_does_not_block_other_tenant_write`` 的
  并发写耗时回到清理时长，精确红；
- 清理失败后连接池/连接状态必须可继续服务
  （``test_redis_failure_after_commit_leaves_pool_healthy``）。
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from sqlalchemy import select

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import Tenant, TenantMember  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

TENANT = "acme"
OTHER = "beta"

#: 模拟一次慢 Redis 全量清理（SCAN 数万键的真实量级）。
_PURGE_SECONDS = 1.2
#: 写路径必须明显早于清理结束（旧实现 = 等满清理时长）。
_WRITE_BUDGET_SECONDS = 0.6


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "a2r13.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    fake = FakeRedis(decode_responses=True)
    tenants_svc.create_tenant(store, TENANT, "Acme", redis_client=fake)
    tenants_svc.create_tenant(store, OTHER, "Beta", redis_client=fake)
    users_svc.create_user(store, "bob", "bob-password-1", "viewer")
    # 让清理确有内容可扫（真实清理路径，不是早退）。
    fake.set(f"plaita:{TENANT}:execution:exec-1", "{}")
    fake.hset("plaita:schedules", "sched-acme", "{}")
    return {"store": store, "redis": fake}


class _SlowPurgeRedis:
    """``hgetall``（清理第一步）阻塞并置位事件——模拟秒级 SCAN。"""

    def __init__(self, inner, started: threading.Event):
        self._inner = inner
        self._started = started

    def __getattr__(self, name):
        if name == "hgetall":
            def _slow(*args, **kwargs):
                self._started.set()
                time.sleep(_PURGE_SECONDS)
                return self._inner.hgetall(*args, **kwargs)
            return _slow
        return getattr(self._inner, name)


class _FailingPurgeRedis:
    """清理第一步抛 Redis 瞬断；其余操作透传。"""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        if name == "hgetall":
            def _boom(*args, **kwargs):
                raise OSError("redis down")
            return _boom
        return getattr(self._inner, name)


def test_slow_redis_purge_does_not_block_other_tenant_write(env):
    """并发写他租户不被删除侧的 Redis 清理拖住（旧实现等满清理时长）。"""
    store, fake = env["store"], env["redis"]
    purge_started = threading.Event()
    outcome = {}

    def delete():
        try:
            outcome["delete"] = tenants_svc.delete_tenant(
                store, TENANT, redis_client=_SlowPurgeRedis(fake, purge_started)
            )
        except Exception as exc:  # noqa: BLE001
            outcome["delete"] = f"{type(exc).__name__}: {exc}"

    worker = threading.Thread(target=delete)
    worker.start()
    assert purge_started.wait(5.0), "删除未走到 Redis 清理"

    started = time.monotonic()
    users_svc.add_member(store, OTHER, "bob", "viewer")
    elapsed = time.monotonic() - started

    worker.join(10.0)
    assert not worker.is_alive(), "删除线程未结束"
    assert outcome["delete"] is True, outcome
    assert elapsed < _WRITE_BUDGET_SECONDS, (
        f"他租户写被删除侧的 Redis 清理阻塞 {elapsed:.2f}s"
        f"（清理时长 {_PURGE_SECONDS}s）——Redis 清理仍在写事务内"
    )
    with store._session_local() as session:
        assert session.scalars(
            select(TenantMember).where(
                TenantMember.tenant_id == OTHER,
                TenantMember.username == "bob",
            )
        ).first() is not None


def test_redis_failure_after_commit_leaves_pool_healthy(env):
    """注入 Redis 清理失败：loud 上抛后连接池与后续读写仍健康。"""
    store, fake = env["store"], env["redis"]
    engine = store._session_local.kw["bind"]

    with pytest.raises(OSError, match="redis down"):
        tenants_svc.delete_tenant(
            store, TENANT, redis_client=_FailingPurgeRedis(fake)
        )

    # DB 删除已提交，连接必须已归还（不得停在 PENDING/checked-out）。
    if hasattr(engine.pool, "checkedout"):
        assert engine.pool.checkedout() == 0, engine.pool.status()
    with store._session_local() as session:
        assert session.get(Tenant, TENANT) is None
        assert session.get(Tenant, OTHER) is not None

    # 后续写路径照常（旧回归形态：全站 500 直到重启）。
    users_svc.create_user(store, "carol", "carol-password-1", "viewer")
    users_svc.add_member(store, OTHER, "carol", "editor")

    # 重试 DELETE = 404 补清扫：返回 False，但 Redis 收尾完成。
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=fake) is False
    assert fake.keys(f"plaita:{TENANT}:*") == []
