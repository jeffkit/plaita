"""A135-01（duo-a 第一百三十五轮，P2）：租户隔离流进 console 列表/replay 面。

A125-01/02 收容终局把拒绝死信写 ``{dlq}:quarantine:{tenant}`` 并承诺「租户
恢复后按信封内源世代人工重放可救」，但该键不以 ``:dlq`` 结尾、也不是登记
队列键——DLQ 列表与 replay 的五处 ``:dlq`` 后缀执法点对它一律拒绝（404
不可达），收容债无从清偿。本文件钉修复后契约：

- 隔离流键形态（已登记 ``*:dlq`` 基键 + 非空租户段）进 DLQ 看板：平台视角
  全量；租户视角只收本租户的隔离流（别租户连键名存在性都不泄露——越权格）；
- replay 接受隔离流键：显式传 key 或按条目 id 定位均可；默认目标按基 DLQ
  反推为源任务队列；信封世代按源继承（A125-01 停用租户载体 / A125-02 无
  tenant_generation 的 legacy 租户世代载体两场景）；
- 清债面：回灌成功（含 already_replayed 补摘）后隔离条目**摘除**；失败路径
  （schema_too_new 等）条目保留；平台 DLQ 原条目照旧不删除、响应形状逐键
  不变（守恒）。
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
from services import audit as audit_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "r135-quarantine-admin-key"
TENANT = "acme"
OTHER = "beta"
DLQ_KEY = "plaita:flow:queue:v2:dlq"
QKEY = f"{DLQ_KEY}:quarantine:{TENANT}"
QKEY_OTHER = f"{DLQ_KEY}:quarantine:{OTHER}"
TARGET = "plaita:flow:queue:v2"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "r135-quarantine.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    users_svc.create_user(
        store,
        "viewer_acme",
        "viewer-password-1",
        "viewer",
        memberships=[{"tenant_id": TENANT, "role": "viewer"}],
    )
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        queues_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app), redis, store


def _admin() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _viewer(client: TestClient) -> dict:
    r = client.post(
        "/api/auth/login",
        json={"username": "viewer_acme", "password": "viewer-password-1"},
    )
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _seed_quarantine(
    redis,
    *,
    quarantine_key: str = QKEY,
    tenant: str = TENANT,
    schema_version: int = 1,
    execution_id: str = "exec-q-1",
    with_snapshot: bool = True,
    with_tenant_generation: bool = True,
) -> str:
    """A125 收容信封（与 task_queue._quarantine_rejected_dead_letter 同形）。

    plaita#328：信封不再带 config_* 代际哈希；``with_snapshot`` 种
    ``max_deliveries`` 预算快照（回灌须按源快照继承）。
    """
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": TARGET,
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": time.time(),
        "schema_version": schema_version,
        "tenant_write_rejected": "tenant_disabled",
        "quarantined": True,
        "payload": {
            "type": "start",
            "flow_id": "f1",
            "version": "1",
            "tenant_id": tenant,
            "execution_id": execution_id,
            "params": {"x": 1},
        },
    }
    if with_snapshot:
        envelope["max_deliveries"] = 7
    if with_tenant_generation:
        envelope["tenant_generation"] = "gen-2"
    return str(redis.xadd(quarantine_key, {"payload": json.dumps(envelope)}))


def _seed_dlq(redis, *, execution_id="exec-dlq-1") -> str:
    """纯 :dlq 信封（test_r53 同形，守恒对照）。"""
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": TARGET,
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": 1700000000.0,
        "schema_version": 1,
        "payload": {
            "type": "start",
            "flow_id": "f1",
            "version": "1",
            "tenant_id": TENANT,
            "execution_id": execution_id,
            "params": {"x": 1},
        },
    }
    return str(redis.xadd(DLQ_KEY, {"payload": json.dumps(envelope)}))


def _replay(client: TestClient, dlq_id: str, *, body: dict | None = None) -> dict:
    r = client.post(
        f"/api/queues/dlq/{dlq_id}/replay",
        json=body or {"dlq_key": QKEY},
        headers=_admin(),
    )
    assert r.status_code == 200, r.text
    return r.json()


def _stream_names(client: TestClient, headers: dict) -> list[str]:
    body = client.get("/api/queues/dlq", headers=headers).json()
    return [s["name"] for s in body["streams"]]


class TestQuarantineList:
    def test_listed_for_platform_view(self, env):
        client, redis, _store = env
        _seed_quarantine(redis)
        names = _stream_names(client, _admin())
        assert QKEY in names
        body = client.get("/api/queues/dlq", headers=_admin()).json()
        stream = next(s for s in body["streams"] if s["name"] == QKEY)
        assert stream["length"] == 1
        entry = stream["entries"][0]
        assert entry["reason"] == "max_deliveries=5"
        assert entry["tenant_id"] == TENANT

    def test_absent_when_no_quarantine_exists(self, env):
        client, _redis, _store = env
        names = _stream_names(client, _admin())
        assert not any(":quarantine:" in name for name in names)

    def test_other_tenant_quarantine_hidden_from_tenant_view(self, env):
        """越权格：租户视角不泄露别租户隔离流的键名存在性。"""
        client, redis, _store = env
        _seed_quarantine(redis)
        _seed_quarantine(redis, quarantine_key=QKEY_OTHER, tenant=OTHER)
        names = _stream_names(client, _viewer(client))
        assert QKEY in names, "本租户隔离流必须可见"
        assert QKEY_OTHER not in names, "别租户隔离流连键名都不可泄露"
        # 平台视角全量。
        both = _stream_names(client, _admin())
        assert QKEY in both and QKEY_OTHER in both


class TestQuarantineReplay:
    def test_disabled_tenant_carrier_saved_then_entry_removed(self, env):
        """A125-01 场景：停用租户收容载体，重放可救；成功后摘条（清债）。"""
        client, redis, _store = env
        qid = _seed_quarantine(redis)
        payload = _replay(client, qid)
        assert payload["replayed"] is True and payload["reason"] == "ok"
        assert payload["quarantine"] is True
        assert payload["entry_removed"] is True
        assert payload["schema_version"] == 1
        assert redis.xlen(QKEY) == 0, "回灌成功后隔离条目必须摘除"
        assert redis.xlen(TARGET) == 1
        # 源快照/世代按信封继承（replay 不猜来源；plaita#328 信封只剩
        # schema_version + tenant_generation + max_deliveries 快照）。
        _, fields = redis.xrange(TARGET)[0]
        assert fields["schema_version"] == "1"
        assert fields["max_deliveries"] == "7"
        assert fields["tenant_generation"] == "gen-2"
        for gone in ("config_family", "config_generation", "budget_generation", "config_effective_at"):
            assert gone not in fields
        # 摘条后再打同一 id：条目已不在 → 具名 404（非 already_replayed）。
        r = client.post(
            f"/api/queues/dlq/{qid}/replay", json={"dlq_key": QKEY}, headers=_admin()
        )
        assert r.status_code == 404

    def test_legacy_tenant_generation_inherited_as_missing(self, env):
        """A125-02 半边：无 tenant_generation 的 legacy 世代载体——缺失按
        legacy 继承，不按当前 meta 猜（目标条目不写该字段）。"""
        client, redis, _store = env
        qid = _seed_quarantine(redis, with_tenant_generation=False)
        payload = _replay(client, qid)
        assert payload["replayed"] is True and payload["reason"] == "ok"
        assert redis.xlen(QKEY) == 0
        _, fields = redis.xrange(TARGET)[0]
        assert "tenant_generation" not in fields
        assert fields["max_deliveries"] == "7"

    def test_failed_replay_keeps_entry(self, env):
        client, redis, _store = env
        qid = _seed_quarantine(redis, schema_version=999)
        payload = _replay(client, qid)
        assert payload["replayed"] is False
        assert payload["reason"] == "schema_too_new"
        assert payload["quarantine"] is True
        assert payload["entry_removed"] is False
        assert redis.xlen(QKEY) == 1, "失败路径条目保留，修复配置后可重试"
        assert redis.xlen(TARGET) == 0

    def test_resolvable_without_explicit_dlq_key(self, env):
        client, redis, _store = env
        qid = _seed_quarantine(redis)
        payload = _replay(client, qid, body={})
        assert payload["replayed"] is True
        assert redis.xlen(TARGET) == 1
        assert redis.xlen(QKEY) == 0

    def test_quarantine_key_rejected_as_target(self, env):
        client, redis, _store = env
        dlq_id = _seed_dlq(redis)
        r = client.post(
            f"/api/queues/dlq/{dlq_id}/replay",
            json={"dlq_key": DLQ_KEY, "target_stream": QKEY},
            headers=_admin(),
        )
        assert r.status_code == 422
        assert redis.xlen(TARGET) == 0

    def test_malformed_quarantine_keys_stay_404(self, env):
        """键形态不合法（基键非 ``*:dlq`` / 租户段为空）照旧拒——泛化不放水。"""
        client, redis, _store = env
        qid = _seed_quarantine(redis)
        for bad in (
            "plaita:flow:queue:v2:quarantine:acme",  # 基键不是 :dlq
            f"{DLQ_KEY}:quarantine:",  # 租户段为空
            f"{DLQ_KEY}:quarantine: ",  # 空白租户段
        ):
            r = client.post(
                f"/api/queues/dlq/{qid}/replay",
                json={"dlq_key": bad},
                headers=_admin(),
            )
            assert r.status_code == 404, bad
        assert redis.xlen(TARGET) == 0

    def test_already_replayed_backfills_entry_removal(self, env):
        """摘条失败的残留：already_replayed（幂等标记在案）也补摘。"""
        client, redis, _store = env
        qid = _seed_quarantine(redis)
        from plaita.server.dlq_replay import marker_key

        redis.set(
            marker_key(QKEY, qid),
            json.dumps(
                {
                    "actor": "earlier-admin",
                    "ts": time.time(),
                    "dlq_key": QKEY,
                    "dlq_id": qid,
                    "tenant_id": TENANT,
                    "schema_version": 1,
                    "execution_id": "exec-q-1",
                    "target_stream": TARGET,
                    "target_message_id": "1234-0",
                    "attempts": 1,
                    "force": False,
                }
            ),
            ex=7 * 86400,
        )
        payload = _replay(client, qid)
        assert payload["replayed"] is False
        assert payload["reason"] == "already_replayed"
        assert payload["quarantine"] is True
        assert payload["entry_removed"] is True
        assert redis.xlen(QKEY) == 0

    def test_audit_records_quarantine_removal(self, env):
        client, redis, _store = env
        qid = _seed_quarantine(redis)
        _replay(client, qid)
        rows = audit_svc.list_audit(action="dlq.replay", limit=10)
        assert len(rows) == 1
        detail = rows[0]["detail"]
        assert detail["reason"] == "ok"
        assert detail["quarantine"] is True
        assert detail["entry_removed"] is True


class TestDlqConservation:
    """纯 ``:dlq`` 流行为逐键守恒：原条目保留、响应形状不变。"""

    def test_dlq_entry_kept_and_response_shape_unchanged(self, env):
        client, redis, _store = env
        dlq_id = _seed_dlq(redis)
        payload = _replay(client, dlq_id, body={"dlq_key": DLQ_KEY})
        assert payload["replayed"] is True and payload["reason"] == "ok"
        assert "quarantine" not in payload
        assert "entry_removed" not in payload
        assert "target_tenant" not in payload, "target_tenant 只回显在审计 detail"
        assert set(payload.keys()) == {
            "replayed",
            "reason",
            "target_message_id",
            "execution_id",
            "tenant_id",
            "schema_version",
            "detail",
        }
        assert redis.xlen(DLQ_KEY) == 1, "平台 DLQ 原条目保留（证据）"
        assert redis.xlen(TARGET) == 1

    def test_known_dlq_zero_rows_still_listed(self, env):
        client, _redis, _store = env
        names = _stream_names(client, _admin())
        assert "plaita:flow:queue:dlq" in names
        assert "plaita:flow:queue:v2:dlq" in names


class TestQuarantineKeyForm:
    def test_form_parsing(self):
        assert queues_api._is_quarantine_key(QKEY)
        assert queues_api._quarantine_tenant(QKEY) == TENANT
        assert queues_api._quarantine_tenant(QKEY_OTHER) == OTHER
        # 租户 id 自身含 :quarantine: 段时按最后一段切分。
        weird = f"{DLQ_KEY}:quarantine:a:quarantine:b"
        assert queues_api._is_quarantine_key(weird)
        assert queues_api._quarantine_tenant(weird) == "a:quarantine:b"
        for not_quarantine in (
            DLQ_KEY,
            TARGET,
            "plaita:flow:queue:v2:quarantine:acme",  # 基键非 :dlq
            f"{DLQ_KEY}:quarantine:",  # 空租户段
            "attacker:dlq:quarantine:acme",  # 基键未登记
        ):
            assert not queues_api._is_quarantine_key(not_quarantine), not_quarantine
            assert queues_api._quarantine_tenant(not_quarantine) is None

    def test_is_stream_key_recognizes_quarantine(self):
        assert queues_api._is_stream_key(QKEY)
        assert queues_api._is_stream_key(DLQ_KEY)
        assert not queues_api._is_stream_key("plaita:flow:queue:v2:quarantine:acme")
