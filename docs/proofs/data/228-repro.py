#!/usr/bin/env python3
"""plaita#228 repro / 探针：flows publish 的租户过滤（同名 flow + 同版本）。

在「A、B 两租户各建同名 flow ``shared-publish``，B 有 1.0.0 草稿（definition 带
``B-SECRET``），A 没有该版本」的形状下，以 A 租户发布 ``1.0.0``：

- 原实现（``publish_version(..., tenant_id=tenant)``）：A 无本租户版本行 ⇒ 404，
  B 的版本仍是 draft，响应不含 B marker；
- 变异实现（``tenant_id=None``）：命中 B 的行 ⇒ 200 + B 完整定义（跨租户读），
  且 B 的版本被置 published（跨租户写）。

运行（工作树内，无需 pytest）：:

    PYTHONPATH=/tmp/w47 /Users/jeff.jie/.venvs/plaita/bin/python \
        docs/proofs/data/228-repro.py

退出码 0 = 租户隔离成立（原实现）；1 = 隔离被打破（变异实现）。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[3]
BACKEND = REPO / "plaita-console" / "backend"
sys.path.insert(0, str(BACKEND))

from fastapi import Depends, FastAPI  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from api import auth_users, flows as flows_api, tenants as tenants_api  # noqa: E402
from api import flow_version as flow_version_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, users_svc  # noqa: E402

ADMIN_KEY = "repro-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _definition(marker: str) -> str:
    return json.dumps({"nodes": [
        {"type": "start", "id": "start", "next": "end"},
        {"type": "end", "id": "end", "resultType": "success", "output": marker},
    ]})


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="w47-228-")
    db_url = f"sqlite:///{tmp}/tenants.db"
    os.environ["PLAITA_CONSOLE_DB_URL"] = db_url
    flow_store.init_engine(db_url)
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "root", "root-password-1", "admin",
                          platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(flows_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])
    app.include_router(tenants_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])
    app.include_router(flow_version_api.router, prefix="/api")
    client = TestClient(app)

    with patch("config.get_settings", _settings), \
            patch("auth.get_settings", _settings):
        login = client.post("/api/auth/login",
                            json={"username": "root",
                                  "password": "root-password-1"})
        assert login.status_code == 200, login.text
        root_h = {"Authorization": f"Bearer {login.json()['token']}"}
        for tenant in ("tenant-a", "tenant-b"):
            r = client.post("/api/tenants", json={"id": tenant}, headers=root_h)
            assert r.status_code == 200, r.text

        a_h = {**root_h, "X-Tenant-ID": "tenant-a"}
        b_h = {**root_h, "X-Tenant-ID": "tenant-b"}
        flow_id = "shared-publish"

        # B：同名 flow + 1.0.0 草稿（带 B-SECRET marker）
        assert client.post("/api/flows", json={"flow_id": flow_id},
                           headers=b_h).status_code == 200
        assert client.put(
            f"/api/flows/{flow_id}/versions/1.0.0",
            json={"definition": _definition("B-SECRET"), "layout": "{}",
                  "created_by": "b"},
            headers=b_h,
        ).status_code == 200

        # A：同名 flow，但没有 1.0.0 版本
        assert client.post("/api/flows", json={"flow_id": flow_id},
                           headers=a_h).status_code == 200
        cross = client.post(f"/api/flows/{flow_id}/publish",
                            json={"version": "1.0.0"}, headers=a_h)
        b_after_cross = client.get(
            f"/api/flows/{flow_id}/versions/1.0.0", headers=b_h)

        # A 建自己的 1.0.0 并发布（正控：本租户发布仍可用）
        assert client.put(
            f"/api/flows/{flow_id}/versions/1.0.0",
            json={"definition": _definition("A-ONLY"), "layout": "{}",
                  "created_by": "a"},
            headers=a_h,
        ).status_code == 200
        own = client.post(f"/api/flows/{flow_id}/publish",
                          json={"version": "1.0.0"}, headers=a_h)
        b_final = client.get(f"/api/flows/{flow_id}/versions/1.0.0", headers=b_h)

    b_after_body = b_after_cross.json() if b_after_cross.status_code == 200 else {}
    b_final_body = b_final.json() if b_final.status_code == 200 else {}
    own_body = own.json() if own.status_code == 200 else {}
    report = {
        "a_publish_without_own_version": {
            "status": cross.status_code,
            "leaked_b_definition": "B-SECRET" in cross.text,
        },
        "b_version_after_a_cross_publish": {
            "status": b_after_body.get("status"),
            "definition_has_b_secret": "B-SECRET" in (b_after_body.get("definition") or ""),
        },
        "a_publish_own_version": {
            "status": own.status_code,
            "version_status": own_body.get("status"),
            "definition_has_a_only": "A-ONLY" in (own_body.get("definition") or ""),
            "definition_has_b_secret": "B-SECRET" in (own_body.get("definition") or ""),
        },
        "b_version_final": {
            "status": b_final_body.get("status"),
            "definition_has_b_secret": "B-SECRET" in (b_final_body.get("definition") or ""),
        },
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    isolated = (
        cross.status_code == 404
        and not report["a_publish_without_own_version"]["leaked_b_definition"]
        and report["b_version_after_a_cross_publish"]["status"] == "draft"
        and report["b_version_after_a_cross_publish"]["definition_has_b_secret"]
        and own.status_code == 200
        and report["a_publish_own_version"]["version_status"] == "published"
        and report["a_publish_own_version"]["definition_has_a_only"]
        and not report["a_publish_own_version"]["definition_has_b_secret"]
        and report["b_version_final"]["status"] == "draft"
        and report["b_version_final"]["definition_has_b_secret"]
    )
    print("\nISOLATED" if isolated else "\nCROSS-TENANT LEAK")
    return 0 if isolated else 1


if __name__ == "__main__":
    sys.exit(main())
