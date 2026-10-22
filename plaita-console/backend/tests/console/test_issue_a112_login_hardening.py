"""A112-01/02 + B112-01/02 (r64): login_guard 硬化 NC-L1~L11。

- L1 共享代理：可信 peer + XFF 真实 IP，受害者正确登录 200；
- L2 伪造 XFF：peer 不可信时忽略 XFF，锁定仍生效；
- L3 代理链：XFF 从右向左取首个 untrusted；
- L4 分布式爆破：多 IP 打同一账号触发账号桶；
- L5 单 IP 多用户名 fan-out：走 IP 桶，账号不被全局锁（NAT 侧另一 IP 可登录）；
- L6 break-glass：锁定中仍可登录（仍校验口令）；
- L7 多 worker：Redis 共享计数（两个 throttle 实例合计 N 次即锁）；
- L8 锁过期后退避翻倍；
- L9 计时枚举：missing/disabled 也跑固定 PBKDF2，比值 ≤2×；
- L10 MAX_TRACKED 不驱逐活动锁；
- L11 锁定期内正确/错误口令都 429。
"""
from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

import pytest
import fakeredis
from fastapi import FastAPI
from starlette.testclient import TestClient

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from models.flow import User  # noqa: E402
from services import flow_store, login_guard, users_svc  # noqa: E402
from sqlalchemy import select  # noqa: E402

WRONG = {"username": "admin", "password": "wrong-password"}
RIGHT = {"username": "admin", "password": "admin-password-1"}


def _app(tmp_path, *, redis_client=None):
    flow_store.init_engine(f"sqlite:///{tmp_path}/login.db")
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", RIGHT["password"], "admin")
    users_svc.create_user(store, "viewer", "viewer-password-1", "viewer")
    users_svc.create_user(store, "auditor", "auditor-password-1", "viewer")
    app = FastAPI()
    app.state.redis = redis_client
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    return app


def _client(app, ip):
    return TestClient(app, client=(ip, 50000))


@pytest.fixture(autouse=True)
def _clean():
    login_guard.reset_all()
    yield
    login_guard.reset_all()


def _lock(client, username="admin", times=5):
    for _ in range(times):
        assert client.post("/api/auth/login", json={
            "username": username, "password": "nope"
        }).status_code == 401


def test_l1_shared_proxy_victim_real_ip_can_login(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_TRUSTED_PROXIES", "127.0.0.1")
    app = _app(tmp_path)
    proxy = _client(app, "127.0.0.1")
    # 攻击者从同一代理出口打 admin（无/伪 XFF），把 proxy IP 的 per-key 锁满
    _lock(proxy)
    # 受害者经同一代理、真实 IP 在 XFF：不同 (ip,user) 键，正确登录 200
    victim = proxy.post(
        "/api/auth/login", json=RIGHT,
        headers={"X-Forwarded-For": "203.0.113.50"},
    )
    assert victim.status_code == 200, victim.text


def test_l2_spoofed_xff_does_not_bypass_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_TRUSTED_PROXIES", "127.0.0.1")
    app = _app(tmp_path)
    direct = _client(app, "203.0.113.9")  # 不可信 peer：XFF 必须忽略
    _lock(direct)
    spoof = direct.post(
        "/api/auth/login", json=RIGHT,
        headers={"X-Forwarded-For": "198.51.100.77"},
    )
    assert spoof.status_code == 429, spoof.text


def test_l3_proxy_chain_right_to_left_first_untrusted(monkeypatch):
    monkeypatch.setenv(
        "PLAITA_TRUSTED_PROXIES", "10.0.0.1/32,10.0.0.2/32"
    )
    assert login_guard.client_ip_from(
        "10.0.0.2", "203.0.113.60, 10.0.0.1"
    ) == "203.0.113.60"
    assert login_guard.client_ip_from(
        "10.0.0.2", "10.0.0.1"
    ) == "10.0.0.1"  # 全链可信时退回最左
    assert login_guard.client_ip_from(
        "203.0.113.9", "198.51.100.77"
    ) == "203.0.113.9"  # peer 不可信，忽略 XFF


def test_l4_distributed_ips_lock_account(tmp_path):
    app = _app(tmp_path)
    # 3 个不同 IP 各错一次即触发账号桶（第 3 次仍按 401 返回，锁从下一次生效）
    for i in range(3):
        assert _client(app, f"198.51.100.{i+1}").post(
            "/api/auth/login", json=WRONG
        ).status_code == 401
    assert _client(app, "198.51.100.4").post(
        "/api/auth/login", json=WRONG
    ).status_code == 429
    # 账号桶已锁：新 IP 正确口令也 429
    blocked = _client(app, "198.51.100.99").post(
        "/api/auth/login", json=RIGHT
    )
    assert blocked.status_code == 429, blocked.text


def test_l5_single_ip_fanout_uses_ip_bucket_not_account_lock(tmp_path):
    app = _app(tmp_path)
    attacker = _client(app, "192.0.2.50")
    _lock(attacker, "admin")
    _lock(attacker, "viewer")
    # 同 IP fan-out 触发 IP 桶：该 IP 后续请求 429（含未被尝试过的账号）
    assert attacker.post("/api/auth/login", json=RIGHT).status_code == 429
    assert attacker.post("/api/auth/login", json={
        "username": "auditor", "password": "auditor-password-1"
    }).status_code == 429
    # 账号未被全局锁：另一 IP 的正确登录仍 200
    other = _client(app, "192.0.2.51").post(
        "/api/auth/login", json=RIGHT
    )
    assert other.status_code == 200, other.text


def test_l6_break_glass_bypasses_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_LOGIN_BREAK_GLASS_USERS", "admin")
    app = _app(tmp_path)
    client = _client(app, "203.0.113.70")
    _lock(client)
    # break-glass 绕过限速：错误口令仍按 401（不锁），正确口令 200
    assert client.post("/api/auth/login", json=WRONG).status_code == 401
    resp = client.post("/api/auth/login", json=RIGHT)
    assert resp.status_code == 200, resp.text


def test_l7_shared_redis_counters_across_workers():
    redis = fakeredis.FakeRedis(decode_responses=True)
    t1 = login_guard.RedisLoginThrottle(redis)
    t2 = login_guard.RedisLoginThrottle(redis)
    now = 1000.0
    for _ in range(3):
        t1.record_failure("203.0.113.80", "admin", now=now)
    assert t2.retry_after("203.0.113.80", "admin", now=now) == 0
    for _ in range(2):
        t2.record_failure("203.0.113.80", "admin", now=now)
    assert t1.retry_after("203.0.113.80", "admin", now=now) > 0
    assert t2.retry_after("203.0.113.80", "admin", now=now) > 0


def test_l7_endpoint_uses_shared_redis_counter(tmp_path):
    redis = fakeredis.FakeRedis(decode_responses=True)
    login_guard.reset_all(redis)
    app = _app(tmp_path, redis_client=redis)
    c1 = _client(app, "203.0.113.81")
    for _ in range(3):
        assert c1.post("/api/auth/login", json=WRONG).status_code == 401
    # 另一个 worker 实例共享同一 Redis（同一 store，不重复建用户）：合计第 6 次即锁
    app2 = FastAPI()
    app2.state.redis = redis
    app2.state.local_mode = False
    app2.state.store = app.state.store
    app2.include_router(auth_users.router, prefix="/api")
    c2 = _client(app2, "203.0.113.81")
    for _ in range(2):
        assert c2.post("/api/auth/login", json=WRONG).status_code == 401
    assert c2.post("/api/auth/login", json=WRONG).status_code == 429
    login_guard.reset_all(redis)


def test_l8_account_lock_expiry_then_escalation():
    throttle = login_guard.LoginThrottle()
    now = 1000.0
    first = 0
    for i in range(3):
        first = throttle.record_failure(f"203.0.113.{90+i}", "admin", now=now)
    assert first == int(throttle.lockout_seconds)
    now += first + 1
    second = 0
    for i in range(3, 6):
        second = throttle.record_failure(f"203.0.113.{90+i}", "admin", now=now)
    assert second == int(throttle.lockout_seconds * 2)


def test_l9_timing_ratio_for_missing_and_disabled(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path}/timing.db")
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", RIGHT["password"], "admin")
    users_svc.create_user(store, "disabled", "disabled-password-1", "viewer")
    with store._session_local() as session:
        row = session.scalars(
            select(User).where(User.username == "disabled")
        ).first()
        row.disabled = True
        session.commit()

    def sample(username, n=6):
        vals = []
        for i in range(n):
            login_guard.reset_all()
            t = time.perf_counter()
            out = users_svc.login(store, username, "wrong-password")
            vals.append((time.perf_counter() - t) * 1000)
            assert out is None
        return vals

    base = statistics.median(sample("admin"))
    missing = statistics.median(sample("no-such-user-xyz"))
    disabled = statistics.median(sample("disabled"))
    assert base / max(missing, 1e-9) <= 2.0, (base, missing)
    assert base / max(disabled, 1e-9) <= 2.0, (base, disabled)


def test_l10_active_lock_not_evicted_by_max_tracked():
    # 先以较大容量建立 victim 活动锁 + 三个更晚到期的锁，再把容量收紧到 3，
    # 模拟表满后的新键写入：旧实现会丢最早锁定项（victim），新实现不驱逐活动锁。
    throttle = login_guard.LoginThrottle(max_tracked=10, lockout_seconds=900)
    now = 1000.0
    for _ in range(5):
        throttle.record_failure("10.0.0.10", "admin", now=now)
    for i in range(3):
        for _ in range(5):
            throttle.record_failure(f"10.0.0.{20+i}", f"filler{i}", now=now + 100)
    throttle.max_tracked = 3
    before = throttle.retry_after("10.0.0.10", "admin", now=now)
    assert before > 0
    throttle.record_failure("10.0.0.99", "newkey", now=now + 100)
    after = throttle.retry_after("10.0.0.10", "admin", now=now)
    assert after == before, (before, after)


def test_l11_account_lock_accepts_neither_correct_nor_wrong(tmp_path):
    app = _app(tmp_path)
    for i in range(3):
        assert _client(app, f"203.0.113.{100+i}").post(
            "/api/auth/login", json=WRONG
        ).status_code == 401
    fresh = _client(app, "203.0.113.200")
    assert fresh.post("/api/auth/login", json=RIGHT).status_code == 429
    assert fresh.post("/api/auth/login", json=WRONG).status_code == 429
