"""#304：published 版本删除零守卫连锁——删除前置守卫 + start 具名拒绝。

issue 机制（A2R49-01 实弹复现面）：``delete_version``/``delete_flow`` 对
published 版本无状态、无在途执行检查；删行同步清引擎定义键 → worker
``resume_flow``（钉死版本，租约/try 之前）定义缺失抛裸 ValueError →
run() poison ack → 挂起执行永久挂起；start 侧 ``_resolve_start_version``
None 照常入队 → 幽灵 queued 行（非终态无 TTL）+ 配额名额不释放；
删最新 published 静默改靶旧版。对照面（published 覆盖 409 / 非终态执行
删除 409）有守卫，唯独版本删除裸奔。

修法口径（**状态守卫可 force，引用守卫不可越**；二选一拍板记录于
``api/flows.py`` 守卫节）：

- 状态守卫：published 版本默认 409 ``version_published``（与「published
  不可覆盖」同口径）；``force=true`` 显式逃生；
- 引用守卫：非终态执行引用 → 409 ``version_in_use``（附清单），force
  **不可越**（级联取消不采用：取消是异步意图，删行+清定义键后取消路径
  自身需要定义键收口，竞窗造新孤儿；「先取消→待终态→再删」是唯一
  无窗口序）；
- 定义键清理只在守卫通过后发生；
- start 显式版本不存在/未发布 → 404 ``version_not_found``（可判定路径
  拒绝；存储不可用 fail-open 保留）；latest 无 published → 响应 warnings。

负控（修复前全红）：A2 探针三场景对应——publish 删（场景 1/2）、
挂起中删（场景 4/5）、start 幽灵行（场景 8/9）。
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.redis import RedisExecutionStorage  # noqa: E402

ADMIN_KEY = "issue304-admin"

FLOW_DEF = json.dumps(
    {
        "flow_id": "guard-flow",
        "desc": "issue304",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.name",
             "resultType": "success"},
        ],
    }
)


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis(tmp_path, monkeypatch) -> FakeRedis:
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'issue304.db'}")
    return FakeRedis(decode_responses=True)


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    deps = [Depends(require_auth)]
    app.include_router(flows_api.router, prefix="/api", dependencies=deps)
    app.include_router(executions_api.router, prefix="/api", dependencies=deps)
    return TestClient(app)


def _headers() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_flow(client: TestClient, flow_id: str = "guard-flow") -> None:
    client.post("/api/flows", json={"flow_id": flow_id}, headers=_headers())
    r = client.put(f"/api/flows/{flow_id}/versions/1.0.0",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 200, r.text


def _publish(client: TestClient, flow_id: str = "guard-flow",
             version: str = "1.0.0") -> None:
    r = client.post(f"/api/flows/{flow_id}/publish", json={"version": version},
                    headers=_headers())
    assert r.status_code == 200, r.text


def _eng_key(flow_id: str = "guard-flow", version: str = "1.0.0") -> str:
    return f"plaita:flow:{flow_id}:{version}"


def _seed_execution(redis: FakeRedis, execution_id: str, flow_id: str,
                    version: str, status: str = "suspended") -> None:
    storage = RedisExecutionStorage(client=redis, namespace="plaita")
    assert storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id=flow_id,
            flow_version=version,
            tenant_id="default",
            context={"$LAST_NODE": "w", "$NODE": {}},
            status=status,
            start_time=datetime.now().isoformat(),
            last_update_time=datetime.now().isoformat(),
        ),
    )


def _detail_code(r) -> str:
    try:
        return str((r.json() or {}).get("detail", {}).get("code", ""))
    except Exception:  # noqa: BLE001
        return ""


# ============ 场景一：publish 删（A2 探针 P1a/P1f） ============

def test_delete_published_version_default_409(redis):
    """published 版本删除默认 409；版本行与引擎键原样（零副作用的拒绝）。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)
    assert redis.exists(_eng_key()) == 1

    r = client.delete("/api/flows/guard-flow/versions/1.0.0", headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_published"
    # 拒绝零副作用：版本行在、引擎键在（清理只在守卫通过后发生）
    versions = flow_store.get_flow_store().list_versions("guard-flow", tenant_id="default")
    assert any(v.version == "1.0.0" for v in versions)
    assert redis.exists(_eng_key()) == 1


def test_delete_published_version_force_without_refs_ok(redis):
    """force=true 且无非终态执行引用 → 允许删；行与引擎键一并清。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)

    r = client.delete("/api/flows/guard-flow/versions/1.0.0?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text
    versions = flow_store.get_flow_store().list_versions("guard-flow", tenant_id="default")
    assert all(v.version != "1.0.0" for v in versions)
    assert redis.exists(_eng_key()) == 0


def test_delete_draft_version_without_refs_still_ok(redis):
    """draft 且无引用：删除照常放行（存量行为零回归）。"""
    client = _client(redis)
    _seed_flow(client)  # 1.0.0 保持 draft

    r = client.delete("/api/flows/guard-flow/versions/1.0.0", headers=_headers())
    assert r.status_code == 200, r.text


# ============ 场景二：挂起中删（A2 探针 P1c 的守卫半边） ============

def test_delete_version_with_suspended_ref_409_even_force(redis):
    """挂起执行引用（published 亦然）→ force 也 409，附执行清单。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)
    _seed_execution(redis, "exec-susp-304", "guard-flow", "1.0.0")

    r = client.delete("/api/flows/guard-flow/versions/1.0.0?force=true",
                      headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_in_use"
    listing = r.json()["detail"]["executions"]
    assert any(e["execution_id"] == "exec-susp-304" for e in listing)
    # 版本行仍在（挂起执行的定义真源未被拆）
    versions = flow_store.get_flow_store().list_versions("guard-flow", tenant_id="default")
    assert any(v.version == "1.0.0" for v in versions)


def test_delete_version_after_cancel_to_terminal_force_ok(redis):
    """先取消待终态 → 引用判据放行 → force 删除成功（引导序可用）。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)
    _seed_execution(redis, "exec-susp-304", "guard-flow", "1.0.0")
    # 执行到终态（cancelled）→ 引用消失
    storage = RedisExecutionStorage(client=redis, namespace="plaita")
    state = storage.load_execution_state("exec-susp-304")
    state.status = "cancelled"
    state.end_time = datetime.now().isoformat()
    assert storage.save_execution_state("exec-susp-304", state)

    r = client.delete("/api/flows/guard-flow/versions/1.0.0?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text


def test_delete_flow_with_published_or_referenced_409(redis):
    """delete_flow 聚合守卫：published（no force）拒；draft+挂起引用也拒。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)

    r = client.delete("/api/flows/guard-flow", headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_published"
    # 状态守卫拒绝：flow 与版本行原样
    assert client.get("/api/flows/guard-flow", headers=_headers()).status_code == 200

    # draft-only flow + 挂起执行引用 draft → 引用判据同样拦（不分状态）
    _seed_flow(client, flow_id="draft-flow")
    _seed_execution(redis, "exec-draft-ref", "draft-flow", "1.0.0")
    r = client.delete("/api/flows/draft-flow", headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_in_use"


def test_delete_flow_clean_200(redis):
    """无 published、无引用的 flow：删除照常（存量零回归）。

    #308 升级断言：删后版本行必须清零——此前级联只靠 SQLite 未强制的
    FK ondelete=CASCADE 声明，版本行原样残留（幽灵 start + 重建撞 409）。
    """
    client = _client(redis)
    _seed_flow(client)  # draft-only

    r = client.delete("/api/flows/guard-flow", headers=_headers())
    assert r.status_code == 200, r.text
    assert client.get("/api/flows/guard-flow", headers=_headers()).status_code == 404
    versions = flow_store.get_flow_store().list_versions(
        "guard-flow", tenant_id="default"
    )
    assert versions == []


def test_delete_guard_scan_unreachable_fail_closed_503(redis):
    """引用扫描不可达（Redis 探活失败）→ 503 fail-closed，不带疑删除。"""

    class BrokenRedis:
        def ping(self):
            raise ConnectionError("boom")

    client = _client(redis)
    app = FastAPI()
    app.state.redis = BrokenRedis()
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(flows_api.router, prefix="/api",
                       dependencies=[Depends(require_auth)])
    client_broken = TestClient(app)

    _seed_flow(client)
    _publish(client)
    r = client_broken.delete("/api/flows/guard-flow/versions/1.0.0?force=true",
                             headers=_headers())
    assert r.status_code == 503, r.text
    assert _detail_code(r) == "version_in_use_scan_failed"


# ============ 场景三：start 幽灵行（A2 探针 P1b/P1e） ============

def test_start_explicit_missing_version_404_no_placeholder(redis):
    """显式版本不存在 → 404 具名拒绝；不落 queued 占位行、不占配额。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)

    r = client.post("/api/executions",
                    json={"flow_id": "guard-flow", "version": "9.9.9"},
                    headers=_headers())
    assert r.status_code == 404, r.text
    assert _detail_code(r) == "version_not_found"
    # 幽灵行不落：执行索引里没有任何非终态行
    storage = RedisExecutionStorage(client=redis, namespace="plaita")
    assert storage.list_executions() == []


def test_start_explicit_draft_version_404(redis):
    """显式版本存在但未发布（draft）→ 404（exists=true 文案区分）。"""
    client = _client(redis)
    _seed_flow(client)  # draft 1.0.0

    r = client.post("/api/executions",
                    json={"flow_id": "guard-flow", "version": "1.0.0"},
                    headers=_headers())
    assert r.status_code == 404, r.text
    body = r.json()["detail"]
    assert body["code"] == "version_not_found"
    assert body["exists"] is True


def test_start_latest_resolved_version_echo_and_no_published_warning(redis):
    """latest：解析到 published → 响应回显版本；无 published → warnings 不阻断。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)

    r = client.post("/api/executions", json={"flow_id": "guard-flow"},
                    headers=_headers())
    assert r.status_code == 200, r.text
    assert r.json().get("version") == "1.0.0"

    # 删掉唯一 published（force，无引用）→ latest 无 published：仍受理但带警告
    r = client.delete("/api/flows/guard-flow/versions/1.0.0?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text
    r = client.post("/api/executions", json={"flow_id": "guard-flow"},
                    headers=_headers())
    assert r.status_code == 200, r.text
    assert "version" not in r.json()
    assert r.json().get("warnings"), "无 published 的 latest start 必须带警告"
