"""plaita#264（P1）：删除租户不清理 DLQ——同名重建的新租户可读前代死信。

## 缺陷（duo-a 第五十三轮 A53-01）

``delete_tenant`` 只排空任务 Stream：DLQ 是平台级键（``<queue>:dlq``），条目
归属只在信封内 ``payload.tenant_id``。删除后 DLQ 原样保留；同名 ``create_tenant``
把墓碑覆盖回 active 后，新租户 viewer 按同一个 slug 重新读到前代死信的完整
payload（含 params/data），回灌后旧执行还会以新租户身份真跑。

## 修复契约

1. 删除时按条目 ``payload.tenant_id`` 收窄 XDEL（对齐 ``_purge_tenant_queue``）；
   无法归属/畸形条目跳过并 WARNING；Redis 失败 loud（可重试），不误删他租户；
2. 读面纵深：租户视角要求 ``payload.tenant_id == 当前租户``（缺字段/畸形不再
   回退 default）**且** ``dead_lettered_at >= 租户 created_at``（当前世代）；
   前代/无法归属条目只对平台可见；
3. 正常死信不回归：当前世代的正常条目仍对本租户可见，他租户条目仍不可见。

## 负控（还原修复后必须精确红）

- 去掉删除排空：``test_delete_purges_tenant_dlq_entries`` 由「旧条目消失」变红；
- 去掉世代过滤（只比 ``payload.tenant_id``）：同名重建用例读到前代 payload；
- 恢复「缺失 tenant_id 回退 default」：无法归属条目进入 default 视角（本文件
  用 acme 视角断言不可见，另以 default 视角作对照见 ``_seed`` 注释）。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import queues as queues_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "issue264-admin-key"
TENANT = "acme"
OTHER = "beta"
DLQ_KEY = "plaita:flow:queue:v2:dlq"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _dlq_entry(
    tenant: str | None,
    secret: str,
    *,
    dead_lettered_at: float | None = None,
    raw_payload: str | None = None,
) -> str:
    payload = {
        "type": "start",
        "flow_id": f"flow-{tenant}",
        "execution_id": f"exec-{secret}",
        "params": {"secret": secret},
    }
    if tenant is not None:
        payload["tenant_id"] = tenant
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": "plaita:flow:queue:v2",
        "source_id": "1-0",
        "delivery_count": 5,
        "dead_lettered_at": (
            time.time() if dead_lettered_at is None else dead_lettered_at
        ),
        "schema_version": 1,
        "payload": payload if raw_payload is None else raw_payload,
    }
    return json.dumps(envelope)


class Env:
    def __init__(self, client, redis, store):
        self.client = client
        self.redis = redis
        self.store = store


def _seed(redis: FakeRedis) -> dict:
    now = time.time()
    ids = {
        # 当前世代的正常死信（alice）。
        "acme_now": redis.xadd(
            DLQ_KEY, {"payload": _dlq_entry(TENANT, "acme-now", dead_lettered_at=now)}
        ),
        # 前代 acme 死信（时间早于本测试创建的租户世代）。
        "acme_old": redis.xadd(
            DLQ_KEY,
            {"payload": _dlq_entry(TENANT, "acme-old", dead_lettered_at=now - 3600)},
        ),
        # 他租户正常死信。
        "beta_now": redis.xadd(
            DLQ_KEY, {"payload": _dlq_entry(OTHER, "beta-now", dead_lettered_at=now)}
        ),
        # 缺 tenant_id：不可归属（不得回退 default/acme）。
        "unattributed": redis.xadd(
            DLQ_KEY,
            {"payload": _dlq_entry(None, "no-tenant", dead_lettered_at=now)},
        ),
        # 整条信封不可解析：不可归属且读面按 raw 容错。
        "malformed": redis.xadd(DLQ_KEY, {"payload": "not-json {broken"}),
    }
    # 同族非 Stream 键：清理时跳过并 WARNING，不得把 delete_tenant 变成误删。
    redis.set("ops:deadletters:dlq", "not-a-stream")
    return ids


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    db_file = tmp_path / "issue264.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.delenv("PLAITA_DLQ_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    tenants_svc.create_tenant(store, OTHER, "Beta")
    users_svc.create_user(
        store,
        "viewer_acme",
        "viewer-password-1",
        "viewer",
        memberships=[{"tenant_id": TENANT, "role": "viewer"}],
    )
    users_svc.create_user(
        store,
        "viewer_beta",
        "viewer-password-1",
        "viewer",
        memberships=[{"tenant_id": OTHER, "role": "viewer"}],
    )
    redis = FakeRedis(decode_responses=True)
    _seed(redis)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        queues_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return Env(TestClient(app), redis, store)


def _login(client: TestClient, username: str, password: str, tenant: str) -> dict:
    r = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    info = r.json()
    if info.get("active_tenant") != tenant:
        r = client.post(
            "/api/auth/switch-tenant",
            json={"tenant_id": tenant},
            headers={"Authorization": f"Bearer {info['token']}"},
        )
        assert r.status_code == 200, r.text
        info = r.json()
    return {"Authorization": f"Bearer {info['token']}"}


def _admin() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _entries(client, headers) -> list:
    body = client.get("/api/queues/dlq", headers=headers).json()
    return [
        entry
        for stream in body["streams"]
        if stream["name"] == DLQ_KEY
        for entry in stream["entries"]
    ]


class TestReadSurfaceGeneration:
    def test_viewer_sees_only_current_generation_entries(self, env):
        headers = _login(env.client, "viewer_acme", "viewer-password-1", TENANT)
        entries = _entries(env.client, headers)
        assert [e["payload"]["params"]["secret"] for e in entries] == ["acme-now"]
        # 前代/他租户/不可归属条目一条都不得出现。
        assert all(e["tenant_id"] == TENANT for e in entries)

    def test_other_tenant_isolated(self, env):
        headers = _login(env.client, "viewer_beta", "viewer-password-1", OTHER)
        entries = _entries(env.client, headers)
        assert [e["payload"]["params"]["secret"] for e in entries] == ["beta-now"]

    def test_platform_view_still_sees_all(self, env):
        entries = _entries(env.client, _admin())
        secrets = {
            e["payload"].get("params", {}).get("secret")
            for e in entries
            if isinstance(e["payload"], dict)
            and e["payload"].get("params", {}).get("secret")
        }
        assert secrets == {"acme-now", "acme-old", "beta-now", "no-tenant"}
        malformed = [e for e in entries if e["payload"].get("raw")]
        assert malformed and malformed[0]["reason"] == ""

    def test_same_name_recreate_does_not_expose_predecessor_payload(self, env):
        """P1 主场景：删除 → 同名重建 → 新 viewer 读不到前代 payload。"""
        tenants_svc.delete_tenant(env.store, TENANT, redis_client=env.redis)
        tenants_svc.create_tenant(env.store, TENANT, "Acme again")
        users_svc.create_user(
            env.store,
            "viewer_new",
            "viewer-password-1",
            "viewer",
            memberships=[{"tenant_id": TENANT, "role": "viewer"}],
        )
        headers = _login(env.client, "viewer_new", "viewer-password-1", TENANT)
        assert _entries(env.client, headers) == []

        # 读面纵深独立于删除排空：重建后再人工放入一条「前代时间戳」的死信，
        # 新租户仍不得看到（平台视角仍可见，作为判别对照）。
        env.redis.xadd(
            DLQ_KEY,
            {
                "payload": _dlq_entry(
                    TENANT, "resurrected-old", dead_lettered_at=time.time() - 3600
                )
            },
        )
        assert _entries(env.client, headers) == []
        platform_secrets = {
            e["payload"].get("params", {}).get("secret")
            for e in _entries(env.client, _admin())
        }
        assert "resurrected-old" in platform_secrets

        # 当前世代的正常死信仍可见（不误伤新租户）。
        env.redis.xadd(
            DLQ_KEY,
            {"payload": _dlq_entry(TENANT, "new-generation-now", dead_lettered_at=time.time())},
        )
        assert [
            e["payload"]["params"]["secret"] for e in _entries(env.client, headers)
        ] == ["new-generation-now"]


class TestDeletePurge:
    def test_delete_purges_tenant_dlq_entries(self, env):
        entries_before = env.redis.xlen(DLQ_KEY)
        assert tenants_svc.delete_tenant(
            env.store, TENANT, redis_client=env.redis
        )
        # 只删 acme 的两条（当前世代 + 前代），他租户/不可归属/畸形原样保留。
        assert env.redis.xlen(DLQ_KEY) == entries_before - 2
        remaining = [
            json.loads(fields["payload"])["payload"].get("tenant_id")
            for _, fields in env.redis.xrange(DLQ_KEY)
            if fields["payload"].startswith("{")
        ]
        assert set(remaining) == {OTHER, None}
        # 同族非 Stream 键不能被误删。
        assert env.redis.get("ops:deadletters:dlq") == "not-a-stream"

    def test_delete_failure_is_loud(self, env, monkeypatch):
        from plaita.server import task_queue as task_queue_mod

        def boom(*args, **kwargs):
            raise RuntimeError("redis dlq purge failed")

        monkeypatch.setattr(task_queue_mod, "purge_tenant_dlq", boom)
        with pytest.raises(RuntimeError, match="redis dlq purge failed"):
            tenants_svc.delete_tenant(env.store, TENANT, redis_client=env.redis)
        # 失败 loud：条目仍在（不伪装成功），他租户数据未受影响。
        assert env.redis.xlen(DLQ_KEY) >= 4
