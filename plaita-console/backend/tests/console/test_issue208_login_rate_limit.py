"""#208：登录端点限速 + 默认监听地址回归。

- 连续 5 次错误口令后第 6 次 429（带 Retry-After），锁定期内正确口令也拒；
- 成功登录清零计数；不同账号（同一 IP）互不牵连；
- 默认 host=127.0.0.1，显式 ``PLAITA_CONSOLE_HOST`` 可覆盖，非回环监听有 warning 文案。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from config import (  # noqa: E402
    LOOPBACK_HOSTS,
    Settings,
    bind_warning,
    get_settings,
)
from services import flow_store, login_guard, users_svc  # noqa: E402

WRONG = {"username": "admin", "password": "wrong-password"}
RIGHT = {"username": "admin", "password": "admin-password-1"}


def _client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue208.db")
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", RIGHT["password"], "admin")
    users_svc.create_user(store, "viewer", "viewer-password-1", "viewer")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clean_throttle():
    login_guard.reset_all()
    yield
    login_guard.reset_all()


def test_sixth_failure_is_429_with_retry_after(tmp_path):
    client = _client(tmp_path)
    codes = [client.post("/api/auth/login", json=WRONG).status_code for _ in range(6)]
    assert codes[:5] == [401] * 5, codes
    assert codes[5] == 429, f"第 6 次错误口令应为 429，实际 {codes[5]}（连续状态码 {codes}）"
    blocked = client.post("/api/auth/login", json=WRONG)
    assert blocked.status_code == 429
    assert blocked.headers.get("Retry-After"), "429 必须带 Retry-After"


def test_correct_password_rejected_during_lockout(tmp_path):
    client = _client(tmp_path)
    for _ in range(5):
        assert client.post("/api/auth/login", json=WRONG).status_code == 401
    r = client.post("/api/auth/login", json=RIGHT)
    assert r.status_code == 429, f"锁定期内正确口令必须被拒（否则爆破仍可撞库），实际 {r.status_code}"


def test_success_resets_failure_counter(tmp_path):
    client = _client(tmp_path)
    for _ in range(4):
        assert client.post("/api/auth/login", json=WRONG).status_code == 401
    assert client.post("/api/auth/login", json=RIGHT).status_code == 200
    # 成功一次后计数清零：前 5 次错误仍应是 401，第 6 次才锁
    codes = [client.post("/api/auth/login", json=WRONG).status_code for _ in range(6)]
    assert codes[:5] == [401] * 5, codes
    assert codes[5] == 429, codes


def test_other_username_not_blocked(tmp_path):
    client = _client(tmp_path)
    for _ in range(5):
        assert client.post("/api/auth/login", json=WRONG).status_code == 401
    assert client.post("/api/auth/login", json=WRONG).status_code == 429
    r = client.post(
        "/api/auth/login",
        json={"username": "viewer", "password": "viewer-password-1"},
    )
    assert r.status_code == 200, "限速键是 (IP, username)，不应牵连同 IP 的其他账号"


def test_lockout_escalates_on_repeat(tmp_path):
    throttle = login_guard.LoginThrottle()
    now = 1000.0
    first = 0
    for _ in range(5):
        first = throttle.record_failure("1.2.3.4", "admin", now=now)
    assert first == int(throttle.lockout_seconds)
    # 锁定期满后再触发一次：退避翻倍
    now += first + 1
    second = 0
    for _ in range(5):
        second = throttle.record_failure("1.2.3.4", "admin", now=now)
    assert second == int(throttle.lockout_seconds * 2), (first, second)


def test_failure_window_expiry_forgets_old_failures(tmp_path):
    throttle = login_guard.LoginThrottle()
    now = 1000.0
    for _ in range(4):
        throttle.record_failure("1.2.3.4", "admin", now=now)
    now += throttle.window_seconds + 1
    assert throttle.record_failure("1.2.3.4", "admin", now=now) == 0, (
        "窗口外的失败不应累计到锁定阈值"
    )


def test_default_host_is_loopback(monkeypatch):
    monkeypatch.delenv("PLAITA_CONSOLE_HOST", raising=False)
    assert Settings().host in LOOPBACK_HOSTS
    assert Settings().host == "127.0.0.1"
    assert get_settings().host == "127.0.0.1"


def test_host_can_be_overridden_explicitly(monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_HOST", "0.0.0.0")
    assert get_settings().host == "0.0.0.0", "部署方必须能显式覆盖监听地址"


def test_public_bind_has_startup_warning(monkeypatch):
    assert bind_warning("127.0.0.1") is None
    assert bind_warning("::1") is None
    warning = bind_warning("0.0.0.0")
    assert warning and "127.0.0.1" not in warning and "PLAITA_CONSOLE_HOST" in warning
