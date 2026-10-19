"""A54-09（duo-a 第五十四轮）：租户删除只清单一队列键，分叉队列旧 start 存活。

基线 ``5ccaf6a`` 的 ``_purge_tenant_queue`` 只清 ``PLAITA_CONSOLE_TASK_QUEUE``
（缺省 ``plaita:flow:queue``）。worker/调度用 ``PLAITA_QUEUE_NAME`` /
``QUEUE_NAME`` 消费、或 v1/v2 并行时，另一条队列里的旧 start 在墓碑被同名
重建覆盖后仍可执行（A50-05/A51-01 的同一结果）。

本文件钉住：
1. 删除排空的键集合覆盖：console 队列、worker env 队列、显式 allowlist、
   Redis 里已存在的同族版本队列（``plaita:flow:queue:v2``）；
2. 每条队列只删归属租户的条目（他租户消息/非 Stream 键不误伤）；
3. 返回值为各队列删除条数之和。

负控（验收时手工执行）：把 ``_purge_tenant_queue`` 恢复为只清
``_task_queue_name()`` 单键 ⇒ v2/worker/allowlist 队列的 acme 条目存活，
第 1 组断言精确红。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store, tenants_svc  # noqa: E402

TENANT = "acme"
OTHER = "beta"
DEFAULT_QUEUE = "plaita:flow:queue"
V2_QUEUE = "plaita:flow:queue:v2"
WORKER_QUEUE = "custom:worker"
ALLOW_QUEUE = "custom:allow"
QUEUES = (DEFAULT_QUEUE, V2_QUEUE, WORKER_QUEUE, ALLOW_QUEUE)


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "a54-09.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    for name in (
        "PLAITA_CONSOLE_TASK_QUEUE",
        "PLAITA_QUEUE_NAME",
        "QUEUE_NAME",
        "PLAITA_CONSOLE_TENANT_PURGE_QUEUES",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PLAITA_QUEUE_NAME", WORKER_QUEUE)
    monkeypatch.setenv("PLAITA_CONSOLE_TENANT_PURGE_QUEUES", ALLOW_QUEUE)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    tenants_svc.create_tenant(store, OTHER, "Beta")
    redis = FakeRedis(decode_responses=True)
    for queue in QUEUES:
        _xadd_task(redis, queue, TENANT)
        _xadd_task(redis, queue, OTHER)
    # 同族前缀下的非 Stream 键：不能被当成队列/误删。
    redis.set(f"{DEFAULT_QUEUE}:config", "not-a-stream")
    return {"store": store, "redis": redis}


def _xadd_task(redis, queue: str, tenant_id: str) -> None:
    redis.xadd(queue, {
        "payload": json.dumps({
            "type": "start",
            "flow_id": f"flow-{tenant_id}",
            "tenant_id": tenant_id,
            "execution_id": f"exec-{tenant_id}",
        }),
        "schema_version": "1",
    })


def _queue_tenants(redis, queue: str) -> list:
    return [
        json.loads(fields["payload"]).get("tenant_id")
        for _, fields in redis.xrange(queue)
    ]


def test_delete_purges_all_queue_family(env):
    deleted = tenants_svc.delete_tenant(
        env["store"], TENANT, redis_client=env["redis"]
    )
    assert deleted is True
    for queue in QUEUES:
        assert _queue_tenants(env["redis"], queue) == [OTHER], queue
    # 非 Stream 同族键原样保留。
    assert env["redis"].get(f"{DEFAULT_QUEUE}:config") == "not-a-stream"


def test_purge_returns_total_across_queues(env):
    # 4 条队列各 1 条 acme 消息。
    assert tenants_svc._purge_tenant_queue(env["redis"], TENANT) == 4
    for queue in QUEUES:
        assert _queue_tenants(env["redis"], queue) == [OTHER], queue


def test_queue_family_skips_non_stream_keys(env):
    names = tenants_svc._tenant_queue_names(env["redis"])
    assert set(QUEUES).issubset(set(names))
    assert f"{DEFAULT_QUEUE}:config" not in names
