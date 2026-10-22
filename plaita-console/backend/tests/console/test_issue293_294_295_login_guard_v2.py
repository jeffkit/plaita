"""#293/#294/#295 + B112-01/02 (r65): login_guard 设计级收口。

#293：IP canonical 化 + IPv6 /64 聚合；account 桶按窗口滑动，旧 IP 不再续锁。
#294：break-glass 必须 用户白名单 + 来源 IP 白名单 + 一次性 HMAC token；
失败照常计数；每次尝试独立审计 auth.login_break_glass(break_glass=true)。
#295：Redis throttle 用 redis TIME，不写进程 monotonic；跨副本/重启一致。
B112-01：0.0.0.0/0 等全 trust 配置拒绝；IPv4-mapped 进入 IPv4 CIDR 匹配。
B112-02：MAX_TRACKED 满表新键具名饱和（不静默放行）。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest
import fakeredis
from fastapi import FastAPI
from sqlalchemy import select
from starlette.testclient import TestClient

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from models.flow import AuditLog  # noqa: E402
from services import flow_store, login_guard, users_svc  # noqa: E402

WRONG = {"username": "admin", "password": "wrong-password"}
RIGHT = {"username": "admin", "password": "admin-password-1"}


def _app(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path}/r65.db")
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", RIGHT["password"], "admin")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    return app, store


def _client(app, ip):
    return TestClient(app, client=(ip, 50000))


@pytest.fixture(autouse=True)
def _clean():
    login_guard.reset_all()
    yield
    login_guard.reset_all()


# ---- #293 ---------------------------------------------------------------

def test_293_same_ipv6_64_is_one_bucket():
    throttle = login_guard.LoginThrottle()
    now = 1000.0
    waits = [
        throttle.record_failure(f"2001:db8:0:1::{i}", "admin", now=now)
        for i in (1, 2, 3)
    ]
    assert waits == [0, 0, 0], "同一 /64 的 3 个地址不得触发账号锁"
    assert throttle.retry_after("2001:db8:0:1::4", "admin", now=now) == 0
    assert list(throttle._account_ips.get("admin", {})) == ["2001:db8:0:1::/64"]


def test_293_three_distinct_ipv6_64_lock_account():
    throttle = login_guard.LoginThrottle()
    now = 1000.0
    waits = [
        throttle.record_failure(f"2001:db8:0:{i}::1", "admin", now=now)
        for i in (1, 2, 3)
    ]
    assert waits[-1] == int(throttle.lockout_seconds)
    assert throttle.retry_after("2001:db8:0:9::1", "admin", now=now) > 0


def test_293_canonical_ip_and_mapped_forms():
    assert login_guard.canonical_ip("::ffff:192.0.2.1") == "192.0.2.1"
    assert login_guard.canonical_ip("2001:0db8:0:0:0:0:0:1") == "2001:db8::1"
    assert login_guard.bucket_ip("::ffff:192.0.2.1") == "192.0.2.1"
    assert login_guard.bucket_ip("2001:db8:0:1::1") == "2001:db8:0:1::/64"

    throttle = login_guard.LoginThrottle()
    now = 1000.0
    for ip in ("192.0.2.1", "::ffff:192.0.2.1", "2001:0db8:0:0:0:0:0:1"):
        throttle.record_failure(ip, "admin", now=now)
    # IPv4 + 2001:db8::/64 两个桶（mapped/展开形式不额外计）
    assert len(throttle._account_ips["admin"]) == 2
    assert throttle.retry_after("192.0.2.1", "admin", now=now) == 0


def test_293_old_ips_outside_window_do_not_relock():
    throttle = login_guard.LoginThrottle()
    now = 1000.0
    for i in (1, 2, 3):
        throttle.record_failure(f"198.51.100.{i}", "admin", now=now)
    assert throttle.retry_after("198.51.100.1", "admin", now=now) > 0
    later = now + throttle.lockout_seconds + 1
    # 锁过期且旧 IP 已出窗口：单个旧 IP 一次失败不得续锁
    assert throttle.record_failure("198.51.100.1", "admin", now=later) == 0
    assert throttle.retry_after("198.51.100.1", "admin", now=later) == 0
    # 再次需要 3 个窗口内不同 IP 才锁，且退避翻倍
    waits = [
        throttle.record_failure(f"203.0.113.{i}", "admin", now=later)
        for i in (1, 2, 3)
    ]
    assert waits[-1] == int(throttle.lockout_seconds * 2)


def test_293_redis_account_window_and_bucket_aggregation():
    redis = fakeredis.FakeRedis(decode_responses=True)
    throttle = login_guard.RedisLoginThrottle(
        redis, window_seconds=0.2, lockout_seconds=0.2
    )
    for i in (1, 2, 3):
        throttle.record_failure(f"2001:db8:0:1::{i}", "admin")
    assert throttle.retry_after("2001:db8:0:1::4", "admin") == 0, "同 /64 不锁"
    for i in (1, 2, 3):
        throttle.record_failure(f"2001:db8:0:{i}::1", "admin")
    assert throttle.retry_after("2001:db8:0:9::1", "admin") > 0
    time.sleep(0.35)
    # 窗口+锁均过期：旧 IP 单次失败不再续锁
    throttle.record_failure("2001:db8:0:1::1", "admin")
    assert throttle.retry_after("2001:db8:0:1::1", "admin") == 0


# ---- #295 ---------------------------------------------------------------

def test_295_redis_uses_server_time_not_process_clocks(monkeypatch):
    redis = fakeredis.FakeRedis(decode_responses=True)
    t1 = login_guard.RedisLoginThrottle(redis)
    t2 = login_guard.RedisLoginThrottle(redis)
    for _ in range(5):
        t1.record_failure("203.0.113.10", "admin")
    # 进程时钟被打到极端值也不影响 Redis 服务端时间语义（只替换本模块引用，
    # 不污染 fakeredis 自身使用的全局 time 模块）
    import types
    monkeypatch.setattr(
        login_guard, "time",
        types.SimpleNamespace(time=lambda: 0.0, monotonic=lambda: 0.0),
    )
    wait = t2.retry_after("203.0.113.10", "admin")
    assert 0 < wait <= int(t1.lockout_seconds) + 1


def test_295_new_process_reads_existing_lock_same_scale():
    redis = fakeredis.FakeRedis(decode_responses=True)
    t1 = login_guard.RedisLoginThrottle(redis)
    for _ in range(5):
        t1.record_failure("203.0.113.11", "admin")
    t2 = login_guard.RedisLoginThrottle(redis)  # 模拟新进程/新主机
    wait = t2.retry_after("203.0.113.11", "admin")
    assert 0 < wait <= int(t1.lockout_seconds) + 1


# ---- #294 ---------------------------------------------------------------

def _break_glass_env(monkeypatch, *, ips="127.0.0.1/32"):
    monkeypatch.setenv("PLAITA_LOGIN_BREAK_GLASS_USERS", "admin")
    monkeypatch.setenv("PLAITA_LOGIN_BREAK_GLASS_SECRET", "y" * 64)
    monkeypatch.setenv("PLAITA_LOGIN_BREAK_GLASS_ALLOWED_IPS", ips)


def _audit_rows(store, action):
    with store._session_local() as session:
        rows = session.scalars(
            select(AuditLog).where(AuditLog.action == action)
        ).all()
        return [json.loads(r.detail_json) for r in rows]


def test_294_username_list_without_token_does_not_bypass(tmp_path, monkeypatch):
    _break_glass_env(monkeypatch)
    app, _store = _app(tmp_path)
    client = _client(app, "127.0.0.1")
    codes = [
        client.post("/api/auth/login", json=WRONG).status_code
        for _ in range(20)
    ]
    assert 429 in codes, "仅用户名 env 不得无限爆破"
    assert client.post("/api/auth/login", json=RIGHT).status_code == 429


def test_294_valid_one_time_token_and_audit(tmp_path, monkeypatch):
    _break_glass_env(monkeypatch)
    app, store = _app(tmp_path)
    client = _client(app, "127.0.0.1")
    for _ in range(5):
        assert client.post("/api/auth/login", json=WRONG).status_code == 401
    token = login_guard.break_glass_token("admin", ttl_seconds=60)
    ok = client.post(
        "/api/auth/login", json=RIGHT,
        headers={"X-Break-Glass-Token": token},
    )
    assert ok.status_code == 200, ok.text
    rows = _audit_rows(store, "auth.login_break_glass")
    assert rows and rows[-1]["break_glass"] is True
    assert rows[-1]["allowed"] is True
    assert rows[-1]["outcome"] == "ok"
    # token 单次：成功后再用同 token（先重新上锁）不再有效
    for _ in range(5):
        client.post("/api/auth/login", json=WRONG)
    reused = client.post(
        "/api/auth/login", json=RIGHT,
        headers={"X-Break-Glass-Token": token},
    )
    assert reused.status_code == 429


def test_294_break_glass_failures_are_counted(tmp_path, monkeypatch):
    _break_glass_env(monkeypatch)
    app, _store = _app(tmp_path)
    client = _client(app, "127.0.0.1")
    for _ in range(5):
        assert client.post("/api/auth/login", json=WRONG).status_code == 401
    token = login_guard.break_glass_token("admin", ttl_seconds=60)
    assert client.post(
        "/api/auth/login", json=WRONG,
        headers={"X-Break-Glass-Token": token},
    ).status_code == 401
    assert login_guard.retry_after("127.0.0.1", "admin") > 0


def test_294_wrong_source_or_invalid_token_fail_closed(tmp_path, monkeypatch):
    _break_glass_env(monkeypatch, ips="10.0.0.0/8")
    app, _store = _app(tmp_path)
    client = _client(app, "203.0.113.70")  # 不在允许来源
    token = login_guard.break_glass_token("admin", ttl_seconds=60)
    for _ in range(5):
        assert client.post("/api/auth/login", json=WRONG).status_code == 401
    blocked = client.post(
        "/api/auth/login", json=RIGHT,
        headers={"X-Break-Glass-Token": token},
    )
    assert blocked.status_code == 429

    _break_glass_env(monkeypatch)  # 回环允许
    client2 = _client(app, "127.0.0.1")
    for _ in range(5):
        client2.post("/api/auth/login", json=WRONG)
    bad = client2.post(
        "/api/auth/login", json=RIGHT,
        headers={"X-Break-Glass-Token": "bg1.9999999999.dead.beef"},
    )
    assert bad.status_code == 429, "无效 token 不得绕过既有锁"


# ---- B112-01 / B112-02 --------------------------------------------------

def test_b112_01_broad_trusted_proxies_rejected(monkeypatch):
    monkeypatch.setenv("PLAITA_TRUSTED_PROXIES", "0.0.0.0/0,::/0")
    assert login_guard.trusted_proxy_networks() == []
    assert login_guard.client_ip_from("203.0.113.9", "198.51.100.77") == "203.0.113.9"


def test_b112_01_mapped_peer_matches_ipv4_cidr(monkeypatch):
    monkeypatch.setenv("PLAITA_TRUSTED_PROXIES", "10.0.0.0/8")
    assert login_guard.client_ip_from(
        "::ffff:10.0.0.1", "203.0.113.50"
    ) == "203.0.113.50"
    assert login_guard.client_ip_from(
        "10.0.0.1", "2001:0db8:0:0:0:0:0:1"
    ) == "2001:db8::1"


def test_b112_02_max_tracked_saturation_not_silent():
    throttle = login_guard.LoginThrottle(max_tracked=3)
    now = 1000.0
    for i in range(3):
        throttle.record_failure("203.0.113.10", f"filler{i}", now=now)
    first = throttle.record_failure("203.0.113.10", "admin", now=now)
    assert first == int(login_guard.SATURATION_SECONDS), "满表新键必须具名拒绝"
    assert throttle.retry_after("203.0.113.10", "admin", now=now) == int(
        login_guard.SATURATION_SECONDS
    )
