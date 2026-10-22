"""plaita#273 / #274：队列档 start 的「已受理未接管」窗口闭环。

#273：POST /api/executions 返回 queued+id 但状态行要等 worker 消费才落——
立即 GET/CANCEL 双 404，worker 停机/积压期排队执行不可查不可取消，租户配额
已在 POST 占用。修复契约：

- POST 在入队**之前**同步落 ``queued`` 占位状态行（经 storage 写路径，索引
  同步维护）；worker 消费时在租约内以 running 行覆盖接管（占位行 context
  为空，``_existing_start_checkpoint`` 仍按首投处理，不误转 resume）；
- CANCEL 对 queued 行写取消标志（覆盖并发消费竞窗）+ 条件置终态 cancelled
  并释放租户 start 配额——worker 全灭时排队执行也能取消、配额立即回收；
- worker 稍后消费带标志的 start 时在 start 入口终态化 cancelled（既有语义）。

#274：同一 dedup_key 两次 POST 各自铸造新 id，worker 只认首条映射，第二个
id 永久 404（响应丢失后的幂等重试拿幽灵 id）。修复契约：

- POST 在铸造 id 后、入队前以 **worker 同源键**（``{ns}:start-dedup:{key}``，
  SET NX + 7d）做生产侧认领；命中即返回既有 execution_id（同键必同 id），
  不再入队、不再占配额；
- 映射指向的执行行已被删除（force DELETE 后重试）→ 释放孤儿映射后按全新
  start 处理（新 id——旧 id 有墓碑，worker 拒绝重建，绝不能复用）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

from plaita.server.tenant_quota import (  # noqa: E402
    publish_tenant_quota,
    tenant_running_count,
)

ADMIN_KEY = "cluster-admin-key"
QUEUE = "plaita:flow:queue"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue273-274.db")
    return FakeRedis(decode_responses=True)


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _post_start(client, **overrides):
    body = {"flow_id": "f1", "params": {"x": 1}}
    body.update(overrides)
    return client.post("/api/executions", json=body, headers=_headers())


def _queued_ids(redis):
    return [
        json.loads(entry[1]["payload"])["execution_id"]
        for entry in redis.xrange(QUEUE)
        if json.loads(entry[1]["payload"]).get("type") == "start"
    ]


class TestIssue273QueuedVisibility:
    def test_post_then_immediate_get_returns_queued_row(self, env):
        """POST 成功后立刻 GET：200 且 status=queued，不是 404（#273 验收①）。"""
        client = _client(env)
        resp = _post_start(client)
        assert resp.status_code == 200, resp.text
        eid = resp.json()["execution_id"]

        got = client.get(f"/api/executions/{eid}", headers=_headers())
        assert got.status_code == 200, got.text
        assert got.json()["status"] == "queued"
        assert got.json()["flow_id"] == "f1"

    def test_placeholder_row_in_list_and_index(self, env):
        """占位行同步进执行列表索引（列表/状态过滤立即可见）。"""
        client = _client(env)
        resp = _post_start(client)
        eid = resp.json()["execution_id"]

        listed = client.get(
            "/api/executions", params={"status": "queued"}, headers=_headers()
        )
        assert listed.status_code == 200, listed.text
        ids = {row["execution_id"] for row in listed.json()["executions"]}
        assert eid in ids

    def test_immediate_cancel_succeeds_without_worker(self, env):
        """worker 停机：POST 后立刻 CANCEL 200、取消标志在位、行终态化。"""
        client = _client(env)
        eid = _post_start(client).json()["execution_id"]

        resp = client.post(f"/api/executions/{eid}/cancel", headers=_headers())
        assert resp.status_code == 200, resp.text
        assert resp.json()["success"] is True

        # 取消标志键在位（并发消费竞窗由 worker start 入口检查兜底）
        assert env.exists(f"plaita:execution:cancel:{eid}")

        got = client.get(f"/api/executions/{eid}", headers=_headers())
        assert got.status_code == 200, got.text
        assert got.json()["status"] == "cancelled"
        assert got.json()["end_time"]

    def test_cancel_releases_tenant_quota_immediately(self, env):
        """取消排队执行立即释放租户 start 配额（不等 worker/条目 TTL）。"""
        publish_tenant_quota(env, "default", 1)
        client = _client(env)
        eid = _post_start(client).json()["execution_id"]
        assert tenant_running_count(env, "default") == 1

        assert client.post(
            f"/api/executions/{eid}/cancel", headers=_headers()
        ).status_code == 200
        assert tenant_running_count(env, "default") == 0

        # 名额释放后同租户可再次提交
        assert _post_start(client).status_code == 200

    def test_worker_takeover_overwrites_placeholder(self, env):
        """worker 接管：queued 占位行（空 context）按首投处理，running 行覆盖。"""
        client = _client(env)
        eid = _post_start(client).json()["execution_id"]

        # 模拟 worker 接管的先行落行（P0 可见性路径）：running 行直接覆盖
        from plaita.storage.base import ExecutionState
        from plaita.storage.redis import RedisExecutionStorage
        from plaita.server.tenant_context import tenant_namespace

        storage = RedisExecutionStorage(
            client=env, namespace=tenant_namespace("default")
        )
        assert storage.save_execution_state(
            eid,
            ExecutionState(
                execution_id=eid, flow_id="f1", status="running",
                tenant_id="default", context={"$INPUT": {"x": 1}},
                invoker="worker",
            ),
        )
        got = client.get(f"/api/executions/{eid}", headers=_headers())
        assert got.status_code == 200
        assert got.json()["status"] == "running"

    def test_placeholder_write_failure_fails_post_without_enqueue(self, env, monkeypatch):
        """占位行落盘失败：POST 5xx、不入队、配额回滚（不静默降级）。"""
        client = _client(env)
        publish_tenant_quota(env, "default", 5)

        def broken_save(self, execution_id, state):
            return False

        monkeypatch.setattr(
            executions_api.RedisExecutionStorage,
            "save_execution_state",
            broken_save,
        )
        resp = _post_start(client)
        assert resp.status_code == 503, resp.text
        assert env.xlen(QUEUE) == 0
        assert tenant_running_count(env, "default") == 0

    def test_enqueue_failure_cleans_placeholder_and_quota(self, env, monkeypatch):
        """入队失败：占位行清理、配额回滚（不留幽灵 queued 行）。"""
        client = _client(env)
        publish_tenant_quota(env, "default", 5)

        def broken_enqueue(message, redis, **kwargs):
            raise ConnectionError("redis down")

        monkeypatch.setattr(executions_api, "_enqueue", broken_enqueue)
        with pytest.raises(ConnectionError):
            _post_start(client)
        assert env.xlen(QUEUE) == 0
        assert tenant_running_count(env, "default") == 0
        assert not env.keys("plaita:execution:*")


class TestIssue274DedupSameId:
    def test_same_dedup_key_returns_same_execution_id(self, env):
        """同租户同 dedup_key 两次 POST：同 id（#274 验收①）。"""
        client = _client(env)
        first = _post_start(client, dedup_key="order-123").json()
        second = _post_start(client, dedup_key="order-123").json()

        assert second["execution_id"] == first["execution_id"]
        # 幂等重试不重复入队、不重复占配额
        assert len(_queued_ids(env)) == 1

    def test_retry_response_id_is_gettable(self, env):
        """响应丢失后的重试：返回的 id 立刻 GET 得到（不 404）。"""
        client = _client(env)
        first = _post_start(client, dedup_key="order-123").json()
        second = _post_start(client, dedup_key="order-123").json()

        got = client.get(
            f"/api/executions/{second['execution_id']}", headers=_headers()
        )
        assert got.status_code == 200, got.text
        assert got.json()["status"] == "queued"

    def test_dedup_claim_uses_worker_same_key(self, env):
        """生产侧认领写 worker 同源键（消费侧自命中闭环的前提）。"""
        client = _client(env)
        first = _post_start(client, dedup_key="order-123").json()
        assert env.get("plaita:start-dedup:order-123") == first["execution_id"]
        assert 0 < env.ttl("plaita:start-dedup:order-123") <= 7 * 86400

    def test_different_key_or_tenant_get_different_ids(self, env):
        """不同 dedup_key / 不同租户同 key：id 不同，互不覆盖（#274 验收②）。"""
        client = _client(env)
        a = _post_start(client, dedup_key="k-a").json()["execution_id"]
        b = _post_start(client, dedup_key="k-b").json()["execution_id"]
        assert a != b

        # 不同租户同 key：键按租户 namespace 路由，acme 的 k-a 是新执行
        tenant_headers = {"X-Admin-API-Key": ADMIN_KEY, "X-Tenant-ID": "acme"}
        resp = client.post(
            "/api/executions",
            json={"flow_id": "f1", "dedup_key": "k-a"},
            headers=tenant_headers,
        )
        assert resp.status_code == 200, resp.text
        acme_id = resp.json()["execution_id"]
        assert acme_id != a
        # 各租户映射落在自己的 namespace，互不覆盖
        assert env.get("plaita:start-dedup:k-a") == a
        assert env.get("plaita:acme:start-dedup:k-a") == acme_id

    def test_no_dedup_key_keeps_fresh_id_each_post(self, env):
        """无 dedup_key：保持每次新 id 的既有契约（#274 验收③）。"""
        client = _client(env)
        ids = {_post_start(client).json()["execution_id"] for _ in range(3)}
        assert len(ids) == 3
        assert not env.keys("plaita:start-dedup:*")

    def test_orphan_mapping_after_delete_starts_fresh(self, env):
        """映射指向的执行已被 force 删除 → 释放孤儿映射，按新 id 全新启动。"""
        client = _client(env)
        first = _post_start(client, dedup_key="order-123").json()
        eid = first["execution_id"]
        # 占位行非终态 → force 删除（先取消再删的路径会写墓碑）
        assert client.post(
            f"/api/executions/{eid}/cancel", headers=_headers()
        ).status_code == 200
        assert client.delete(
            f"/api/executions/{eid}", params={"force": "true"},
            headers=_headers(),
        ).status_code == 200

        retry = _post_start(client, dedup_key="order-123")
        assert retry.status_code == 200, retry.text
        new_id = retry.json()["execution_id"]
        assert new_id != eid
        # 新 id 立刻可查，映射已转向新执行
        got = client.get(f"/api/executions/{new_id}", headers=_headers())
        assert got.status_code == 200
        assert env.get("plaita:start-dedup:order-123") == new_id
