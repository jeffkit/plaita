"""plaita#255（P1 security）：删除租户必须清理租户作用域运行数据。

缺口：``delete_tenant`` 只删凭据/成员/租户行，运行数据全部留存——
- 本地档 SQLite ``local_schedules`` / ``local_schedule_fires`` /
  ``local_executions`` / ``local_logs``；
- 集群档 Redis 租户命名空间 ``{ns}:execution:*``（状态/取消/fence/租约/
  重试计数）、``{ns}:start-dedup:*``、``{ns}:flow*`` 发布定义；
- 平台键里归属该租户的调度（``plaita:schedules`` + fires/lock）、事件记录、
  事件订阅。

后果：重建同名租户后新成员直接读到旧执行/日志/调度；旧 enabled 调度继续
按 cron 入队，同名 flow 发布后恢复真实执行；删除后仍在队列里的 start/resume
消息因 ``tenant_is_active`` 对缺失状态 fail-open 而照常执行。

本文件钉住（主控确认帖判据）：
1. 四表 + Redis 命名空间 + 平台键条目按租户收窄清理，失败 loud；
2. 「仍有 flow 先清空」拒绝门先于清理（同 #257 文件）；
3. 重建同名租户后读不到旧执行/日志/调度，旧 enabled 调度不再触发；
4. 删除写 ``disabled`` 墓碑 → 在途调度/消息被运行面闸拦下；
5. 不得误删 default/他租户数据（含任务队列 Stream）。

负控（必红）：去掉 ``_purge_local_tenant_runtime`` 调用 → 本地残留用例红；
去掉 ``_purge_tenant_redis`` / 墓碑 / 调度条目清理 → 对应断言红。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from sqlalchemy import select

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import (  # noqa: E402
    Credential,
    LocalLog,
    LocalScheduleFire,
    Tenant,
)
from services import (  # noqa: E402
    credentials_svc,
    flow_store,
    local_scheduler,
    tenants_svc,
)
from plaita.server.services.schedule_service import fire_schedule  # noqa: E402
from plaita.server.tenant_status import tenant_is_active  # noqa: E402

TENANT = "acme"
OTHER = "beta"
QUEUE = "plaita:flow:queue"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "issue255.db"
    creds = tmp_path / "creds.json"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(creds))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    fake = FakeRedis(decode_responses=True)
    tenants_svc.create_tenant(store, TENANT, "Acme", redis_client=fake)
    tenants_svc.create_tenant(store, OTHER, "Beta", redis_client=fake)
    return {"store": store, "redis": fake, "creds": creds}


def _seed_local(store) -> None:
    for tid in (TENANT, OTHER):
        local_scheduler.create_schedule(store, {
            "schedule_id": f"sched-{tid}",
            "tenant_id": tid,
            "name": f"job-{tid}",
            "flow_id": f"flow-{tid}",
            "cron": "* * * * *",
            # acme 到期（可触发），beta 未启用（隔离断言只看行存活）
            "enabled": tid == TENANT,
            "next_run_at": "1" if tid == TENANT else "9999999999999",
        })
        flow_store.insert_local_execution(
            f"exec-local-{tid}", f"flow-{tid}", "1.0.0", status="completed",
            input_json='{"secret": "INPUT-%s"}' % tid, tenant_id=tid,
        )
        flow_store.insert_local_log(
            f"exec-local-{tid}", "INFO", "test", f"LOG-{tid}", tenant_id=tid,
        )
        with store._session_local() as session:
            session.add(LocalScheduleFire(
                schedule_id=f"sched-{tid}", execution_id=f"exec-local-{tid}",
                trigger_kind="cron", status="fired",
            ))
            session.commit()


def _seed_redis(redis) -> None:
    # 租户命名空间：执行状态族的代表键（状态/取消/fence/租约/重试计数/start-dedup）
    redis.set(f"plaita:{TENANT}:execution:exec-{TENANT}", json.dumps({"flow_id": "f"}))
    redis.set(f"plaita:{TENANT}:execution:cancel:exec-{TENANT}", "1")
    redis.set(f"plaita:{TENANT}:execution:fence:exec-{TENANT}", "3")
    redis.set(f"plaita:{TENANT}:execution:lease:exec-{TENANT}", "worker-1")
    redis.set(f"plaita:{TENANT}:execution:noderetry:exec-{TENANT}:node-1", "2")
    redis.set(f"plaita:{TENANT}:start-dedup:dedup-1", "1")
    # flow 发布定义与索引
    redis.set(f"plaita:{TENANT}:flow:flow-{TENANT}:1.0.0", json.dumps({"nodes": []}))
    redis.sadd(f"plaita:{TENANT}:flow_list", f"flow-{TENANT}")
    redis.sadd(f"plaita:{TENANT}:flow_versions:flow-{TENANT}", "1.0.0")
    # 他租户 / default 隔离对照
    redis.set(f"plaita:{OTHER}:execution:exec-{OTHER}", json.dumps({"flow_id": "f"}))
    redis.set(f"plaita:{OTHER}:flow:flow-{OTHER}:1.0.0", json.dumps({"nodes": []}))
    redis.set("plaita:execution:exec-default", json.dumps({"flow_id": "f"}))

    # 平台调度 HASH + 触发历史/锁
    redis.hset("plaita:schedules", f"sched-{TENANT}", json.dumps({
        "schedule_id": f"sched-{TENANT}", "tenant_id": TENANT,
        "flow_id": f"flow-{TENANT}", "cron": "* * * * *", "enabled": True,
    }))
    redis.hset("plaita:schedules", f"sched-{OTHER}", json.dumps({
        "schedule_id": f"sched-{OTHER}", "tenant_id": OTHER,
        "flow_id": f"flow-{OTHER}", "cron": "* * * * *", "enabled": True,
    }))
    for tid in (TENANT, OTHER):
        redis.rpush(f"plaita:schedule:fires:sched-{tid}", "{}")
        redis.set(f"plaita:schedule:lock:sched-{tid}", "1")

    # 事件记录（console + 引擎两种形状）与类型索引
    redis.set(f"plaita:event:evt-{TENANT}", json.dumps({
        "event_id": f"evt-{TENANT}", "event_type": "approval", "tenant_id": TENANT,
    }))
    redis.set(f"plaita:event:events:evt-{TENANT}-engine", json.dumps({
        "event_id": f"evt-{TENANT}-engine", "event_type": "approval",
        "tenant_id": TENANT,
    }))
    redis.set(f"plaita:event:evt-{OTHER}", json.dumps({
        "event_id": f"evt-{OTHER}", "event_type": "approval", "tenant_id": OTHER,
    }))
    redis.zadd("plaita:event:type:approval", {
        f"evt-{TENANT}": 1, f"evt-{TENANT}-engine": 2, f"evt-{OTHER}": 3,
    })
    redis.zadd("plaita:event:types:approval", {
        f"evt-{TENANT}": 1, f"evt-{TENANT}-engine": 2, f"evt-{OTHER}": 3,
    })

    # 事件订阅记录 + 索引/处理记录/超时标记
    redis.set(f"plaita:subscription:data:sub-{TENANT}", json.dumps({
        "subscription_id": f"sub-{TENANT}", "event_type": "approval",
        "correlation_id": f"exec-{TENANT}", "flow_id": f"flow-{TENANT}",
        "node_id": f"node-{TENANT}", "tenant_id": TENANT,
    }))
    redis.set(f"plaita:subscription:data:sub-{OTHER}", json.dumps({
        "subscription_id": f"sub-{OTHER}", "event_type": "approval",
        "correlation_id": f"exec-{OTHER}", "flow_id": f"flow-{OTHER}",
        "node_id": f"node-{OTHER}", "tenant_id": OTHER,
    }))
    redis.sadd("plaita:subscription:type:approval", f"sub-{TENANT}", f"sub-{OTHER}")
    redis.sadd(f"plaita:subscription:correlation:exec-{TENANT}", f"sub-{TENANT}")
    redis.sadd(f"plaita:subscription:flow:flow-{TENANT}", f"sub-{TENANT}")
    redis.sadd(f"plaita:subscription:node:node-{TENANT}", f"sub-{TENANT}")
    redis.set(f"plaita:subscription:processed:sub-{TENANT}:evt-{TENANT}", "1")
    redis.set(f"plaita:subscription:processed:sub-{OTHER}:evt-{OTHER}", "1")
    redis.set(f"plaita:event_filter:timeout:sub-{TENANT}", "1")

    # 配额名额与上限
    redis.zadd(f"plaita:tenant_running:{TENANT}", {f"exec-{TENANT}": 1})
    redis.zadd(f"plaita:tenant_running:{OTHER}", {f"exec-{OTHER}": 1})
    redis.hset("plaita:tenant_quota", TENANT, "5")
    redis.hset("plaita:tenant_quota", OTHER, "7")


def _local_rows(store, model):
    with store._session_local() as session:
        return session.scalars(select(model)).all()


def test_delete_tenant_purges_runtime_data_and_recreate_is_clean(env, monkeypatch):
    store, redis = env["store"], env["redis"]
    _seed_local(store)
    credentials_svc.save_credential(
        "prod-db", "api", {"token": "ACME-SECRET-42"}, tenant_id=TENANT
    )
    _seed_redis(redis)
    sidecar = env["creds"].with_name(f"{env['creds'].stem}.{TENANT}.json")
    assert sidecar.is_file()

    # 删除前：本地调度确实会触发（否则「不再触发」无从对照）
    fire_calls: list = []
    monkeypatch.setattr(
        local_scheduler, "fire",
        lambda st, sched, trigger_kind="cron": fire_calls.append(sched["schedule_id"]),
    )
    local_scheduler._scan_once(store)
    assert f"sched-{TENANT}" in fire_calls

    assert tenants_svc.delete_tenant(store, TENANT, redis_client=redis) is True

    # 1) 墓碑：删除 = 运行面显式不可用，不是「缺失即放行」
    assert redis.hget("plaita:tenant_status", TENANT) == "disabled"
    assert tenant_is_active(redis, TENANT) is False
    assert tenant_is_active(redis, OTHER) is True

    # 2) 租户命名空间整体清空，他租户/default 键不受影响
    assert redis.keys(f"plaita:{TENANT}:*") == []
    assert redis.exists(f"plaita:{OTHER}:execution:exec-{OTHER}") == 1
    assert redis.exists(f"plaita:{OTHER}:flow:flow-{OTHER}:1.0.0") == 1
    assert redis.exists("plaita:execution:exec-default") == 1

    # 3) 平台调度条目 + 触发历史/锁按租户收窄
    assert redis.hget("plaita:schedules", f"sched-{TENANT}") is None
    assert redis.exists(f"plaita:schedule:fires:sched-{TENANT}") == 0
    assert redis.exists(f"plaita:schedule:lock:sched-{TENANT}") == 0
    assert redis.hget("plaita:schedules", f"sched-{OTHER}") is not None
    assert redis.exists(f"plaita:schedule:fires:sched-{OTHER}") == 1

    # 4) 事件记录从两种形状删除，zset 只摘除本租户成员
    assert redis.exists(f"plaita:event:evt-{TENANT}") == 0
    assert redis.exists(f"plaita:event:events:evt-{TENANT}-engine") == 0
    assert redis.exists(f"plaita:event:evt-{OTHER}") == 1
    assert redis.zscore("plaita:event:type:approval", f"evt-{TENANT}") is None
    assert redis.zscore("plaita:event:types:approval", f"evt-{TENANT}-engine") is None
    assert redis.zscore("plaita:event:type:approval", f"evt-{OTHER}") is not None

    # 5) 订阅记录/索引/处理记录/超时标记
    assert redis.exists(f"plaita:subscription:data:sub-{TENANT}") == 0
    assert redis.exists(f"plaita:subscription:data:sub-{OTHER}") == 1
    assert redis.sismember("plaita:subscription:type:approval", f"sub-{TENANT}") == 0
    assert redis.sismember("plaita:subscription:type:approval", f"sub-{OTHER}") == 1
    assert redis.exists(f"plaita:subscription:processed:sub-{TENANT}:evt-{TENANT}") == 0
    assert redis.exists(f"plaita:subscription:processed:sub-{OTHER}:evt-{OTHER}") == 1
    assert redis.exists(f"plaita:event_filter:timeout:sub-{TENANT}") == 0

    # 6) 配额键
    assert redis.zcard(f"plaita:tenant_running:{TENANT}") == 0
    assert redis.hget("plaita:tenant_quota", TENANT) is None
    assert redis.zcard(f"plaita:tenant_running:{OTHER}") == 1
    assert redis.hget("plaita:tenant_quota", OTHER) == "7"

    # 7) 本地 SQLite 四表：本租户全清、他租户保留
    assert sidecar.exists() is False
    assert local_scheduler.list_schedules(store, tenant_id=TENANT) == []
    assert flow_store.list_local_executions(tenant_id=TENANT) == []
    assert flow_store.list_local_logs(tenant_id=TENANT) == []
    assert local_scheduler.list_schedules(store, tenant_id=OTHER) != []
    assert flow_store.list_local_executions(tenant_id=OTHER) != []
    assert flow_store.list_local_logs(tenant_id=OTHER) != []
    with store._session_local() as session:
        fires = session.scalars(
            select(LocalScheduleFire).where(
                LocalScheduleFire.schedule_id == f"sched-{TENANT}"
            )
        ).all()
        assert fires == []

    # 8) 旧 enabled 调度不再触发：本地扫描 0 次；集群 stale 字典被墓碑拦下
    fire_calls.clear()
    local_scheduler._scan_once(store)
    assert fire_calls == [], "旧 enabled 调度删除后仍被本地调度循环触发"
    stale = {
        "schedule_id": f"sched-{TENANT}", "tenant_id": TENANT,
        "flow_id": f"flow-{TENANT}", "cron": "* * * * *", "params": {},
        "next_run_at": "1",
    }
    assert fire_schedule(redis, stale, QUEUE) is None
    assert redis.xlen(QUEUE) == 0, "墓碑必须拦住在途调度入队"

    # 9) 重建同名租户：墓碑被覆盖回 active，旧数据不可读
    tenants_svc.create_tenant(store, TENANT, "Acme again", redis_client=redis)
    assert redis.hget("plaita:tenant_status", TENANT) == "active"
    assert tenant_is_active(redis, TENANT) is True
    assert flow_store.list_local_executions(tenant_id=TENANT) == []
    assert flow_store.list_local_logs(tenant_id=TENANT) == []
    assert local_scheduler.list_schedules(store, tenant_id=TENANT) == []
    assert credentials_svc.list_credentials(TENANT) == []
    assert redis.keys(f"plaita:{TENANT}:*") == []


def test_redis_cleanup_failure_after_commit_is_compensated_by_404_retry(
    env, monkeypatch,
):
    """A2R13：Redis 清理失败发生在 **DB 提交之后**（不再回滚事务）。

    旧实现把秒级 Redis 全量清理放在 ``BEGIN IMMEDIATE`` 写事务里：并发写撞
    ``database is locked``，commit 失败还会污染连接池（全站 500 到重启）。
    现在 DB 删除先提交，写锁毫秒级释放；Redis 失败 loud 上抛，重试 DELETE
    走 404 补清扫继续清理。
    """
    store, fake = env["store"], env["redis"]
    _seed_local(store)
    sidecar = env["creds"].with_name(f"{env['creds'].stem}.{TENANT}.json")
    credentials_svc.save_credential(
        "prod-db", "api", {"token": "ACME-SECRET-42"}, tenant_id=TENANT
    )
    _seed_redis(fake)
    assert sidecar.is_file()

    class _SchedulesDown:
        """调度 HASH 读取失败（Redis 瞬断）——清理第一步就炸。"""

        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            if name == "hgetall":
                def _boom(*args, **kwargs):
                    raise OSError("redis down")
                return _boom
            return getattr(self._inner, name)

    with pytest.raises(OSError, match="redis down"):
        tenants_svc.delete_tenant(store, TENANT, redis_client=_SchedulesDown(fake))

    # 1) DB 删除已提交（与旧「回滚」相反）：租户/凭据/本地运行数据都不在。
    with store._session_local() as session:
        assert session.get(Tenant, TENANT) is None
        assert session.scalars(
            select(Credential).where(Credential.tenant_id == TENANT)
        ).all() == []
    assert flow_store.list_local_executions(tenant_id=TENANT) == []
    assert not sidecar.exists(), "#257：旁文件仍须先于 commit 清掉"
    assert fake.hget("plaita:tenant_status", TENANT) == "disabled"

    # 2) 连接池健康：失败后的普通读写照常（PENDING 状态不得跨请求残留）。
    engine = store._session_local.kw["bind"]
    if hasattr(engine.pool, "checkedout"):
        assert engine.pool.checkedout() == 0
    with store._session_local() as session:
        assert session.scalars(select(Tenant).where(Tenant.id == OTHER)).all()

    # 3) 重试走 404 补清扫：返回 False，但 Redis 清理继续完成。
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=fake) is False
    assert redis_keys_clean(fake, TENANT)


def redis_keys_clean(redis, tenant_id: str) -> bool:
    return redis.keys(f"plaita:{tenant_id}:*") == []


def test_delete_default_does_not_touch_platform_queue_or_other_tenants(env):
    """default 与平台键共用 plaita: 前缀：队列 Stream/他租户数据必须保留。"""
    store, redis = env["store"], env["redis"]
    redis.xadd(QUEUE, {"payload": "{}"})
    redis.xadd(f"{QUEUE}:dlq", {"payload": "{}"})
    redis.set("plaita:flow:flow-default:1.0.0", json.dumps({"nodes": []}))
    redis.sadd("plaita:flow_list", "flow-default")
    redis.sadd("plaita:flow_versions:flow-default", "1.0.0")
    redis.set("plaita:execution:exec-default", json.dumps({"flow_id": "flow-default"}))
    redis.set("plaita:start-dedup:dedup-default", "1")
    redis.hset("plaita:schedules", "sched-default", json.dumps({
        "schedule_id": "sched-default", "tenant_id": "default",
        "flow_id": "flow-default", "cron": "* * * * *", "enabled": True,
    }))
    redis.set(f"plaita:{OTHER}:execution:exec-{OTHER}", json.dumps({"flow_id": "f"}))

    assert tenants_svc.delete_tenant(store, "default", redis_client=redis) is True

    # 租户数据清空
    assert redis.exists("plaita:execution:exec-default") == 0
    assert redis.exists("plaita:start-dedup:dedup-default") == 0
    assert redis.exists("plaita:flow:flow-default:1.0.0") == 0
    assert redis.exists("plaita:flow_list") == 0
    assert redis.exists("plaita:flow_versions:flow-default") == 0
    assert redis.hget("plaita:schedules", "sched-default") is None
    # 平台机制键/他租户不受影响
    assert redis.type(QUEUE) == "stream" and redis.xlen(QUEUE) == 1
    assert redis.type(f"{QUEUE}:dlq") == "stream"
    assert redis.exists(f"plaita:{OTHER}:execution:exec-{OTHER}") == 1
    assert redis.hget("plaita:tenant_status", "default") == "disabled"
