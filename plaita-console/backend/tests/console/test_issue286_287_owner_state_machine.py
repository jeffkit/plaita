"""#286/#287 (r59): start dedup token 状态机的端到端交错。

#286：B 的孤儿 release 决定落在 A 续期/落行/入队之后——旧 GET→DEL 会删除
A 的映射，双 200/双载体。现在 release 是 Lua 原子复核 token/owner/row/墓碑，
stale 决定必须失败，B 收敛到 A。
#287：旧 `<map>:owner=claiming:A` 在新代码下是 live claim（迁移/只读兼容），
B 不得换新 id；旧 `enqueued:A`+row 不得重复补 carrier；`foo:owner` 反向
碰撞先迁 old owner 再认领。
"""
from __future__ import annotations

import json
import sys
import threading
import time
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
from plaita.server import start_dedup as sd  # noqa: E402
from plaita.server.tenant_quota import (  # noqa: E402
    publish_tenant_quota,
    tenant_running_count,
)
from plaita.tenant_context import (  # noqa: E402
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    reset_current_tenant,
    set_current_tenant,
)

ADMIN_KEY = "cluster-admin-key-r59"
QUEUE = "plaita:flow:queue"
TENANT = "default"


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
    flow_store.init_engine(f"sqlite:///{tmp_path}/r59.db")
    redis = FakeRedis(decode_responses=True)
    publish_tenant_quota(redis, TENANT, 10)
    return redis


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


def _post(client, **overrides):
    body = {"flow_id": "f1", "params": {"x": 1}}
    body.update(overrides)
    return client.post("/api/executions", json=body, headers=_headers())


def _queued_ids(redis):
    return [
        json.loads(entry[1]["payload"])["execution_id"]
        for entry in redis.xrange(QUEUE)
        if json.loads(entry[1]["payload"]).get("type") == "start"
    ]


def _tenant_post(name, results, key, fn):
    token = set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    try:
        results[name] = fn(key)
    finally:
        reset_current_tenant(token)


def test_stale_release_after_a_completed_cannot_delete(env, monkeypatch):
    """#286：B 在 release 前暂停，A 续期/落行/入队成功后 B 的旧 release 失败。"""
    monkeypatch.setattr(executions_api, "_START_DEDUP_OWNER_CLAIM_TTL_SECONDS", 1)
    a_entered = threading.Event()
    a_release = threading.Event()
    b_entered = threading.Event()
    b_allow = threading.Event()
    state = {"placeholder": True, "release": True, "a_id": None}

    real_ph = executions_api._write_queued_placeholder
    real_release = executions_api._release_start_dedup_if_owner

    def slow_placeholder(redis, tenant, execution_id, *args, **kwargs):
        if state["placeholder"]:
            state["placeholder"] = False
            state["a_id"] = execution_id
            a_entered.set()
            assert a_release.wait(10), "A not released"
        return real_ph(redis, tenant, execution_id, *args, **kwargs)

    def gated_release(redis, tenant, key, execution_id, **kwargs):
        if state["release"]:
            state["release"] = False
            b_entered.set()
            assert b_allow.wait(10), "B release not allowed"
        return real_release(redis, tenant, key, execution_id, **kwargs)

    monkeypatch.setattr(executions_api, "_write_queued_placeholder", slow_placeholder)
    monkeypatch.setattr(executions_api, "_release_start_dedup_if_owner", gated_release)
    results: dict[str, object] = {}
    try:
        ta = threading.Thread(
            target=_tenant_post,
            args=("A", results, "seam-key", lambda k: _post(_client(env), dedup_key=k)),
        )
        ta.start()
        assert a_entered.wait(5)
        time.sleep(1.3)  # claiming owner/token TTL 过期

        tb = threading.Thread(
            target=_tenant_post,
            args=("B", results, "seam-key", lambda k: _post(_client(env), dedup_key=k)),
        )
        tb.start()
        assert b_entered.wait(5), "B 未走到 stale release"
        a_release.set()
        ta.join(10)
        b_allow.set()
        tb.join(10)
    finally:
        a_release.set()
        b_allow.set()

    id_a = state["a_id"]
    assert results["A"].status_code == 200, results["A"].text
    assert results["A"].json()["execution_id"] == id_a
    assert results["B"].status_code == 200, results["B"].text
    assert results["B"].json()["execution_id"] == id_a, "B 必须收敛到 A，不得换新 id"
    assert _queued_ids(env) == [id_a], "只允许一条 start 载体"
    assert tenant_running_count(env, TENANT) == 1
    owner = sd.read_owner(env, TENANT, "seam-key")
    assert owner.state == sd.STATE_ENQUEUED and owner.execution_id == id_a


def test_legacy_claiming_owner_is_not_stolen(env):
    """#287：旧 claiming:A 在新代码下是 live claim，B 不换新 id。"""
    old = "a" * 32
    env.set(sd.map_key(TENANT, "mig-key"), old, ex=3600)
    env.set(sd.legacy_owner_key(TENANT, "mig-key"), f"claiming:{old}", ex=60)

    resp = _post(_client(env), dedup_key="mig-key")
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["error"] == "execution_start_in_progress"
    assert resp.headers.get("Retry-After") == "1"
    assert sd.read_mapped(env, TENANT, "mig-key") == old
    assert _queued_ids(env) == []
    assert tenant_running_count(env, TENANT) == 0


def test_legacy_enqueued_owner_does_not_requeue_carrier(env):
    """#287：旧 enqueued:A + row A ⇒ 新代码只返回 A，不重复补 carrier。"""
    old = "b" * 32
    env.set(sd.map_key(TENANT, "mig-enq"), old, ex=3600)
    env.set(sd.execution_row_key(TENANT, old), json.dumps({"status": "queued"}))
    env.set(sd.legacy_owner_key(TENANT, "mig-enq"), f"enqueued:{old}", ex=3600)

    resp = _post(_client(env), dedup_key="mig-enq")
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_id"] == old
    assert _queued_ids(env) == [], "carrier 已存在（旧 enqueued），不得重复补"


def test_reverse_collision_does_not_clobber_foo_claim(env):
    """foo 的旧 owner 键 == foo:owner 的 map 键：迁移后两个 key 各自独立。"""
    foo_id = "c" * 32
    env.set(sd.map_key(TENANT, "foo"), foo_id, ex=3600)
    env.set(sd.legacy_owner_key(TENANT, "foo"), f"claiming:{foo_id}", ex=60)

    resp = _post(_client(env), dedup_key="foo:owner")
    assert resp.status_code == 200, resp.text
    new_id = resp.json()["execution_id"]
    assert new_id != foo_id
    assert sd.read_mapped(env, TENANT, "foo") == foo_id
    foo_owner = sd.read_owner(env, TENANT, "foo")
    assert foo_owner.state == sd.STATE_CLAIMING and foo_owner.execution_id == foo_id
    assert sd.read_mapped(env, TENANT, "foo:owner") == new_id
    assert _queued_ids(env) == [new_id]


def test_force_delete_after_guard_xdels_late_carrier(env, monkeypatch):
    """⑤：guard/reserve 后 force DELETE，A 的迟到载体必须被清收。"""
    entered = threading.Event()
    release = threading.Event()
    real_enqueue = executions_api._enqueue
    first = {"done": False, "id": None}

    def gated_enqueue(message, redis, **kwargs):
        if message.get("type") == "start" and not first["done"]:
            first["done"] = True
            first["id"] = message["execution_id"]
            entered.set()
            assert release.wait(9), "A not released"
        return real_enqueue(message, redis)

    monkeypatch.setattr(executions_api, "_enqueue", gated_enqueue)
    results: dict[str, object] = {}
    try:
        ta = threading.Thread(
            target=_tenant_post,
            args=("A", results, "fd-key", lambda k: _post(_client(env), dedup_key=k)),
        )
        ta.start()
        assert entered.wait(5)
        id_a = first["id"]
        deleted = _client(env).delete(
            f"/api/executions/{id_a}", params={"force": "true"}, headers=_headers()
        )
        assert deleted.status_code == 200, deleted.text

        resp_b = _post(_client(env), dedup_key="fd-key")
        assert resp_b.status_code == 200, resp_b.text
        id_b = resp_b.json()["execution_id"]
        assert id_b != id_a
        release.set()
        ta.join(9)
    finally:
        release.set()

    resp_a = results["A"]
    assert resp_a.status_code == 200, resp_a.text
    assert resp_a.json()["execution_id"] == id_b
    assert _queued_ids(env) == [id_b], "A 的迟到载体必须被 XDEL 清收"
    assert env.exists(sd.tombstone_key(id_a))
    assert tenant_running_count(env, TENANT) == 1
