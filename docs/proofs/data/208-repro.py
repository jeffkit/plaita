#!/usr/bin/env python3
"""plaita#208 repro / before-after probe：登录端点限速 + 默认监听地址。

Run from ``plaita-console/backend`` with the repo root on ``sys.path``::

    cd plaita-console/backend
    PYTHONPATH=/tmp/w38 python /tmp/w38/docs/proofs/data/208-repro.py

断言的是**期望行为**（修后绿、修前红）：

1. 连续 5 次错误口令后第 6 次必须 429（带 Retry-After），锁定期内正确口令也 429；
2. 限速器清理后正确口令恢复 200（正常登录行为与现状一致）；
3. ``Settings().host`` / ``get_settings().host`` 默认 127.0.0.1，显式
   ``PLAITA_CONSOLE_HOST=0.0.0.0`` 可覆盖。

修复前（origin/main）输出 RED：第 6 次仍是 401、默认 host 仍是 0.0.0.0。
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# docs/proofs/data/208-repro.py → parents[3] = 仓根
REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_DIR = REPO_ROOT / "plaita-console" / "backend"
for _p in (str(REPO_ROOT), str(BACKEND_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi import FastAPI  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from api import auth_users  # noqa: E402
from config import Settings, get_settings
from services import flow_store, users_svc

failures: list[str] = []


def check(name: str, fn) -> None:
    try:
        fn()
    except AssertionError as exc:
        failures.append(f"{name}: {exc}")
        print(f"  RED {name}: {exc}")
    else:
        print(f"  GREEN {name}")


def _client(tmpdir: str) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmpdir}/repro208.db")
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", "admin-password-1", "admin")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    return TestClient(app)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="plaita208-") as tmpdir:
        client = _client(tmpdir)

        def lockout_after_failures() -> None:
            codes = []
            for _ in range(6):
                r = client.post(
                    "/api/auth/login",
                    json={"username": "admin", "password": "wrong-password"},
                )
                codes.append(r.status_code)
            assert codes[:5] == [401] * 5, f"前 5 次错误口令状态码 {codes[:5]}（期望全 401）"
            assert codes[5] == 429, (
                f"第 6 次错误口令返回 {codes[5]}（期望 429）：登录端点零限速，"
                f"6 次错误口令状态码={codes}"
            )
            r = client.post(
                "/api/auth/login",
                json={"username": "admin", "password": "admin-password-1"},
            )
            assert r.status_code == 429, (
                f"锁定期内正确口令返回 {r.status_code}（期望 429）：锁定期未生效"
            )
            assert r.headers.get("Retry-After"), "429 响应缺 Retry-After 头"

        def unlock_clears_state() -> None:
            try:
                from services import login_guard
            except ImportError:
                raise AssertionError("services.login_guard 不存在：限速实现缺失")
            login_guard.reset_all()
            r = client.post(
                "/api/auth/login",
                json={"username": "admin", "password": "admin-password-1"},
            )
            assert r.status_code == 200, f"限速器清理后正确口令返回 {r.status_code}（期望 200）"

        def host_default_safe() -> None:
            saved = os.environ.pop("PLAITA_CONSOLE_HOST", None)
            try:
                assert Settings().host == "127.0.0.1", (
                    f"Settings().host={Settings().host!r}（期望 127.0.0.1）"
                )
                assert get_settings().host == "127.0.0.1", (
                    f"get_settings().host={get_settings().host!r}（期望 127.0.0.1）"
                )
                os.environ["PLAITA_CONSOLE_HOST"] = "0.0.0.0"
                assert get_settings().host == "0.0.0.0", (
                    "显式 PLAITA_CONSOLE_HOST=0.0.0.0 覆盖失效（部署可显式覆盖是验收条件）"
                )
            finally:
                os.environ.pop("PLAITA_CONSOLE_HOST", None)
                if saved is not None:
                    os.environ["PLAITA_CONSOLE_HOST"] = saved

        check("连续错误口令第 6 次 429（含锁定期内正确口令）", lockout_after_failures)
        check("限速器清理后正确口令 200", unlock_clears_state)
        check("默认 host=127.0.0.1 且 env 可显式覆盖", host_default_safe)

    if failures:
        print(f"\nRED: {len(failures)} 项未通过")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nGREEN: #208 全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
