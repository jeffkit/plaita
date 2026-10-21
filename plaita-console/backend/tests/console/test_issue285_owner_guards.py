"""#285 (r57): owner 过期/墓碑后原认领者 A 的迟到写必须被复核拒绝。

#280 的 owner 机制只约束后来者 B；A 在 ``_write_queued_placeholder`` /
``_enqueue`` / ``_set_start_dedup_owner`` 前不复核所有权，导致 owner 过期或
force DELETE 后 A 仍写行/入队/覆盖 owner：同 dedup_key 两个 200、两个 id、
两条载体，A 的 200 还可能是墓碑 id。本文件覆盖：

1. owner 过期 + 慢占位行：B 换新 id 后 A 不得再落行/入队/覆盖 owner；
2. force DELETE + live claiming：A 不得返回自己的墓碑 id；
3. owner 键后缀碰撞（foo 与 foo:owner）改用独立前缀；
4. 3+ 败者在 A 失败时不得拿到 ghost 200（409 in-progress，重试可收敛）；
5. owner CAS 绝不覆盖另一个 id 的 owner。
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

ADMIN_KEY = "cluster-admin-key-285"
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
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue285.db")
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


def _tenant_post(name, results, key, fn):
    token = set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    try:
        results[name] = fn(key)
    finally:
        reset_current_tenant(token)


def test_owner_expiry_slow_placeholder_single_id(env, monkeypatch):
    """A 的 claiming TTL 过期、B 换新 id 后，A 的迟到写必须放弃。"""
    monkeypatch.setattr(executions_api, "_START_DEDUP_OWNER_CLAIM_TTL_SECONDS", 1)
    entered = threading.Event()
    release = threading.Event()
    real_placeholder = executions_api._write_queued_placeholder
    first = {"done": False, "id": None}

    def slow_placeholder(redis, tenant, execution_id, *args, **kwargs):
        if not first["done"]:
            first["done"] = True
            first["id"] = execution_id
            entered.set()
            assert release.wait(8), "A 未在超时内恢复"
        return real_placeholder(redis, tenant, execution_id, *args, **kwargs)

    monkeypatch.setattr(executions_api, "_write_queued_placeholder", slow_placeholder)
    results: dict[str, object] = {}

    def post(key):
        return _post_start(_client(env), dedup_key=key)

    try:
        thread_a = threading.Thread(
            target=_tenant_post, args=("A", results, "slow-key", post)
        )
        thread_a.start()
        assert entered.wait(5), "A 未进入占位行写窗口"
        time.sleep(1.3)  # claiming TTL（缩放后）已过期，行仍未落

        resp_b = post("slow-key")
        assert resp_b.status_code == 200, resp_b.text
        id_b = resp_b.json()["execution_id"]

        release.set()
        thread_a.join(8)
    finally:
        # monkeypatch 自动还原；确保线程不会留在阻塞状态
        release.set()

    resp_a = results["A"]
    id_a = first["id"]
    assert resp_a.status_code == 200, resp_a.text
    assert resp_a.json()["execution_id"] == id_b, "A 不得返回自己的过期 id"
    assert env.get(executions_api._start_dedup_map_key(TENANT, "slow-key")) == id_b
    owner = env.get(executions_api._start_dedup_owner_key(TENANT, "slow-key"))
    assert owner and owner.endswith(id_b) and not owner.endswith(id_a)
    assert _queued_ids(env) == [id_b], "只允许一条可执行载体"
    assert not env.get(f"plaita:execution:{id_a}")
    assert env.exists(f"plaita:exec_tombstone:{id_a}")
    assert tenant_running_count(env, TENANT) == 1


def test_force_delete_inflight_a_loses_claim(env, monkeypatch):
    """A 写完占位行后被 force DELETE；B 接管后 A 不得入队/返回墓碑 id。"""
    entered = threading.Event()
    release = threading.Event()
    real_placeholder = executions_api._write_queued_placeholder
    first = {"done": False, "id": None}

    def paused_after_placeholder(redis, tenant, execution_id, *args, **kwargs):
        ok = real_placeholder(redis, tenant, execution_id, *args, **kwargs)
        if not first["done"]:
            first["done"] = True
            first["id"] = execution_id
            entered.set()
            assert release.wait(8), "A 未在超时内恢复"
        return ok

    monkeypatch.setattr(
        executions_api, "_write_queued_placeholder", paused_after_placeholder
    )
    results: dict[str, object] = {}

    def post(key):
        return _post_start(_client(env), dedup_key=key)

    try:
        thread_a = threading.Thread(
            target=_tenant_post, args=("A", results, "fd-key", post)
        )
        thread_a.start()
        assert entered.wait(5), "A 未写完占位行"
        id_a = first["id"]
        deleted = _client(env).delete(
            f"/api/executions/{id_a}", params={"force": "true"}, headers=_headers()
        )
        assert deleted.status_code == 200, deleted.text
        assert env.exists(f"plaita:exec_tombstone:{id_a}")

        resp_b = post("fd-key")
        assert resp_b.status_code == 200, resp_b.text
        id_b = resp_b.json()["execution_id"]
        assert id_b != id_a

        release.set()
        thread_a.join(8)
    finally:
        release.set()

    resp_a = results["A"]
    assert resp_a.status_code == 200, resp_a.text
    assert resp_a.json()["execution_id"] == id_b, "A 不得返回已墓碑的自己的 id"
    assert _queued_ids(env) == [id_b], "A 的旧载体不得入队"
    assert env.get(executions_api._start_dedup_map_key(TENANT, "fd-key")) == id_b
    owner = env.get(executions_api._start_dedup_owner_key(TENANT, "fd-key"))
    assert owner and owner.endswith(id_b)
    assert not env.get(f"plaita:execution:{id_a}")
    assert tenant_running_count(env, TENANT) == 1


def test_owner_key_namespace_avoids_suffix_collision(env):
    """dedup_key='foo' 与 'foo:owner' 的 map/owner 键不得互覆（独立前缀）。"""
    client = _client(env)
    r1 = _post_start(client, dedup_key="foo")
    r2 = _post_start(client, dedup_key="foo:owner")
    assert r1.status_code == r2.status_code == 200
    id1 = r1.json()["execution_id"]
    id2 = r2.json()["execution_id"]
    assert id1 != id2

    map_foo = executions_api._start_dedup_map_key(TENANT, "foo")
    map_owner = executions_api._start_dedup_map_key(TENANT, "foo:owner")
    owner_foo = executions_api._start_dedup_owner_key(TENANT, "foo")
    owner_owner = executions_api._start_dedup_owner_key(TENANT, "foo:owner")
    assert len({map_foo, map_owner, owner_foo, owner_owner}) == 4
    assert env.get(map_foo) == id1
    assert env.get(map_owner) == id2
    assert env.get(owner_foo) == f"enqueued:{id1}"
    assert env.get(owner_owner) == f"enqueued:{id2}"

    # 重试各自仍收敛到自己的 id（旧实现此时会互相覆盖）。
    assert _post_start(client, dedup_key="foo").json()["execution_id"] == id1
    assert _post_start(client, dedup_key="foo:owner").json()["execution_id"] == id2
    assert _queued_ids(env) == [id1, id2]


def test_losers_get_in_progress_not_ghost_200_when_winner_fails(env, monkeypatch):
    """A 落行失败时，等待中的 B/C/D 不得拿到不存在的 A id（ghost 200）。"""
    entered = threading.Event()
    release = threading.Event()
    first = {"done": False}
    _real_placeholder = executions_api._write_queued_placeholder

    def failing_placeholder(*args, **kwargs):
        if not first["done"]:
            first["done"] = True
            entered.set()
            assert release.wait(8), "A 未在超时内失败"
            return False
        return _real_placeholder(*args, **kwargs)

    monkeypatch.setattr(executions_api, "_write_queued_placeholder", failing_placeholder)
    results: dict[str, object] = {}

    def post(key):
        return _post_start(_client(env), dedup_key=key)

    try:
        thread_a = threading.Thread(
            target=_tenant_post, args=("A", results, "many-key", post)
        )
        thread_a.start()
        assert entered.wait(5), "A 未进入占位行写窗口"
        losers = []
        for name in ("B", "C", "D"):
            t = threading.Thread(
                target=_tenant_post, args=(name, results, "many-key", post)
            )
            t.start()
            losers.append(t)
        for t in losers:
            t.join(10)
        release.set()
        thread_a.join(10)
    finally:
        release.set()

    assert results["A"].status_code == 503, results["A"].text
    for name in ("B", "C", "D"):
        resp = results[name]
        assert resp.status_code == 409, (name, resp.text)
        assert resp.json()["detail"]["error"] == "execution_start_in_progress"
        assert "execution_id" not in resp.json()
    assert _queued_ids(env) == []
    assert tenant_running_count(env, TENANT) == 0
    assert results["A"].json()["detail"]["code"] == "execution_state_persist_failed"

    # 幂等重查/重试：同一 dedup_key 换新 id 正常受理。
    retry = _post_start(_client(env), dedup_key="many-key")
    assert retry.status_code == 200, retry.text
    new_id = retry.json()["execution_id"]
    assert _queued_ids(env) == [new_id]
    assert tenant_running_count(env, TENANT) == 1


def test_owner_cas_never_overwrites_other_id(env):
    """owner CAS：映射/owner 属他人时返回 False 且原 owner 不变。"""
    id_other = "b" * 32
    id_stale = "a" * 32
    claimed, _ = executions_api._claim_start_dedup_at_post(
        env, TENANT, "cas-key", id_other
    )
    assert claimed
    assert env.get(executions_api._start_dedup_owner_key(TENANT, "cas-key")) == (
        f"claiming:{id_other}"
    )

    assert not executions_api._set_start_dedup_owner(
        env, TENANT, "cas-key", id_stale,
        state=executions_api._START_DEDUP_OWNER_ENQUEUED,
        ttl=3600,
        allow_from_state=executions_api._START_DEDUP_OWNER_CLAIMING,
    )
    assert env.get(executions_api._start_dedup_owner_key(TENANT, "cas-key")) == (
        f"claiming:{id_other}"
    )

    # 释放旧 id 不得清掉他人的映射/owner。
    executions_api._release_start_dedup_if_owner(env, TENANT, "cas-key", id_stale)
    assert env.get(executions_api._start_dedup_map_key(TENANT, "cas-key")) == id_other
    assert env.get(executions_api._start_dedup_owner_key(TENANT, "cas-key")) == (
        f"claiming:{id_other}"
    )
