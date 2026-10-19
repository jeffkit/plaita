"""台账 r48 调度器生命周期族四格（A48-01/02/03/04）。

- A48-01：集群档不得启动本地调度循环；已落盘的 ``local_schedules`` 保留但
  不执行（启动与 lifespan 关停都不留幽灵线程）。
- A48-02：本地到点触发以 ``next_run_at`` 条件更新作为 due 幂等键——先推进再
  执行；历史写失败/进程崩溃后同一 due 不再起第二条执行。
- A48-03：本地 ``/schedules/{id}/history`` 先做租户归属校验（跨租户 404，
  平台视角仍全量）。
- A48-04：触发前复验租户存活性与调度行：非 default 租户行缺失 = 已删除
  （#255 墓碑语义），扫描快照不能为已删除租户起执行。

每格都对应一条「还原修复即红」的负控：
1. A48-01：cluster 档无条件 ``start_scheduler_loop`` → 线程存活 + ghost 执行；
2. A48-02：执行后才单独提交 next_run_at → 同一 due 双执行；
3. A48-03：history 本地分支直接查 fires → 跨租户 200；
4. A48-04：租户闸用 lenient ``tenant_is_active``（缺失=active）→ 删除后快照
   仍执行。
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main as main_module  # noqa: E402
from api import auth_users  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from models.flow import Tenant  # noqa: E402
from services import flow_store  # noqa: E402
from services import local_scheduler as sched  # noqa: E402
from services import tenants_svc, users_svc  # noqa: E402
from sqlalchemy import select, text  # noqa: E402

GOOD_DEF = json.dumps({"nodes": [
    {"type": "start", "id": "start", "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
]})

ADMIN_KEY = "a48-admin-key"
TENANT_A = "acme"
TENANT_B = "globex"


def _settings(db_url: str = "sqlite:///./unused.db"):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    s.db_url = db_url
    # 空 URL ⇒ HMAC 重放保护走进程内 store，测试不依赖真实 Redis
    s.redis_url = ""
    return s


def _seed_tenant_flow(store, tenant_id: str, flow_id: str) -> None:
    store.ensure_flow(flow_id, tenant_id=tenant_id)
    store.save_flow_definition(
        flow_id, "1.0.0", GOOD_DEF, status="draft", tenant_id=tenant_id
    )
    store.publish_version(flow_id, "1.0.0", tenant_id=tenant_id)


def _due(offset_seconds: float = -1.0) -> str:
    return str(
        (datetime.now() + timedelta(seconds=offset_seconds)).timestamp() * 1000
    )


def _mk_schedule(store, schedule_id: str, *, tenant: str = "default",
                 flow: str = "hello", enabled: bool = True,
                 next_run_at: str | None = None) -> dict:
    return sched.create_schedule(store, {
        "schedule_id": schedule_id,
        "tenant_id": tenant,
        "name": schedule_id,
        "flow_id": flow,
        "version": "1.0.0",
        "cron": "* * * * *",
        "params": {},
        "enabled": enabled,
        "created_by": "test",
        "next_run_at": _due() if next_run_at is None else next_run_at,
    })


def _exec_count(store) -> int:
    with store._session_local() as session:
        return session.execute(text("select count(*) from local_executions")).scalar()


def _fire_count(store, schedule_id: str) -> int:
    from models.flow import LocalScheduleFire

    with store._session_local() as session:
        return len(session.scalars(
            select(LocalScheduleFire).where(
                LocalScheduleFire.schedule_id == schedule_id
            )
        ).all())


@pytest.fixture()
def store(tmp_path, monkeypatch):
    db_file = tmp_path / "a48.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    st = flow_store.get_flow_store()
    _seed_tenant_flow(st, "default", "hello")
    tenants_svc.create_tenant(st, TENANT_A, "Acme")
    tenants_svc.create_tenant(st, TENANT_B, "Globex")
    _seed_tenant_flow(st, TENANT_A, "hello")
    _seed_tenant_flow(st, TENANT_B, "hello")
    # 上一个用例可能留下未 join 的线程（模块级全局）
    sched.stop_scheduler_loop(timeout=2)
    yield st
    sched.stop_scheduler_loop(timeout=2)
    # 等进程内执行线程收尾，避免下一个用例重建引擎时它还在写旧表
    deadline = time.time() + 5
    while time.time() < deadline:
        with st._session_local() as session:
            running = session.execute(
                text("select count(*) from local_executions where status = 'running'")
            ).scalar()
        if not running:
            break
        time.sleep(0.05)


# ---- A48-01：集群档不启动本地调度循环 ----

def test_cluster_mode_does_not_start_local_scheduler(store, tmp_path, monkeypatch):
    """cluster 档：无本地调度线程、无 ghost execution；遗留行保留不执行。"""
    _mk_schedule(store, "ghost-cluster")

    settings = _settings(f"sqlite:///{tmp_path / 'a48.db'}")
    fake = FakeRedis(decode_responses=True)
    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    monkeypatch.setattr(main_module, "connect_redis", lambda url: fake)
    monkeypatch.setattr(
        main_module, "_active_cluster_redis_url",
        lambda: "redis://cluster.invalid:6379/0",
    )

    with TestClient(main_module.app) as client:
        assert main_module.app.state.local_mode is False
        assert not (sched._thread is not None and sched._thread.is_alive()), (
            "cluster 档启动了本地调度线程"
        )
        # 线程若存在会在启动后立刻扫一轮；留出窗口确认没有 ghost 执行
        time.sleep(0.8)
        assert _exec_count(store) == 0, "cluster 档为遗留本地调度起了 ghost execution"
        assert _fire_count(store, "ghost-cluster") == 0
        assert client.get("/health").json()["local_mode"] is False

    assert not (sched._thread is not None and sched._thread.is_alive()), (
        "lifespan 退出后本地调度线程仍存活"
    )
    time.sleep(0.3)
    assert _exec_count(store) == 0, "lifespan 退出后仍触发本地调度"
    # 迁移/禁用语义：行保留（切回本地档仍可用），只是不执行
    assert sched.get_schedule(store, "ghost-cluster") is not None


def test_stop_scheduler_loop_joins(store):
    sched.start_scheduler_loop(store)
    thread = sched._thread
    assert thread is not None and thread.is_alive()
    sched.stop_scheduler_loop(timeout=5)
    assert not thread.is_alive()
    assert sched._thread is None


# ---- A48-02：next_run_at 条件推进 = 本地 due 幂等键 ----

def test_record_fire_failure_does_not_replay_same_due(store, monkeypatch):
    """首扫已起执行、_record_fire 抛错 → 第二扫同一 due 不得再起执行。"""
    _mk_schedule(store, "s-rec")
    before = _exec_count(store)
    real_record = sched._record_fire
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated sqlite write failure")
        return real_record(*args, **kwargs)

    monkeypatch.setattr(sched, "_record_fire", flaky)
    with pytest.raises(RuntimeError):
        sched._scan_once(store)

    assert _exec_count(store) == before + 1
    # due 已在执行前推进：恢复后的下一扫不再触发
    assert sched._scan_once(store) == 0
    assert _exec_count(store) == before + 1
    assert _fire_count(store, "s-rec") == 0  # 首次历史写失败是事实，不补写


def test_crash_after_fire_does_not_replay_same_due(store):
    """fire(cron) 返回后进程崩溃（排班未单独回写）→ 重启扫描不得双执行。"""
    _mk_schedule(store, "s-crash")
    snapshot = sched.get_schedule(store, "s-crash")
    before = _exec_count(store)

    execution_id = sched.fire(store, snapshot, trigger_kind="cron")
    assert execution_id is not None
    assert _exec_count(store) == before + 1

    # 「重启后」的扫描：同一 due 已被认领
    assert sched._scan_once(store) == 0
    assert _exec_count(store) == before + 1
    assert _fire_count(store, "s-crash") == 1


def test_cron_success_advances_once_and_paused_skips(store):
    """正向控制：成功路径恰好 1 条 + next 推进；暂停调度 0 条。"""
    _mk_schedule(store, "s-ok")
    _mk_schedule(store, "s-paused", enabled=False, next_run_at="")
    before = sched.get_schedule(store, "s-ok")["next_run_at"]
    execs_before = _exec_count(store)

    assert sched._scan_once(store) == 1
    after = sched.get_schedule(store, "s-ok")
    assert _exec_count(store) == execs_before + 1
    assert after["next_run_at"] != before
    assert float(after["next_run_at"]) > time.time() * 1000 - 60_000
    assert after["last_run_at"]

    # 同一轮不该再触发；暂停的不进入触发路径
    assert sched._scan_once(store) == 0
    assert _exec_count(store) == execs_before + 1
    assert _fire_count(store, "s-paused") == 0


# ---- A48-03：本地触发历史租户归属 ----

@pytest.fixture()
def tenant_api(tmp_path, monkeypatch):
    db_file = tmp_path / "tenant_history.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    st = flow_store.get_flow_store()
    tenants_svc.create_tenant(st, TENANT_A, "Acme")
    tenants_svc.create_tenant(st, TENANT_B, "Globex")
    users_svc.create_user(
        st, "viewer_acme", "viewer-password-1", "viewer",
        memberships=[{"tenant_id": TENANT_A, "role": "viewer"}],
    )
    for tenant, sid, execution_id in (
        (TENANT_A, "sched-A", "exec-own-A"),
        (TENANT_B, "sched-B", "exec-SECRET-B"),
    ):
        _mk_schedule(st, sid, tenant=tenant, enabled=False, next_run_at="")
        sched._record_fire(st, sid, execution_id, "cron", "fired", "")

    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.local_mode = True
    app.state.redis = None
    app.state.store = st
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        schedules_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app), st


def test_local_history_enforces_tenant_ownership(tenant_api):
    client, _store = tenant_api
    login = client.post(
        "/api/auth/login",
        json={"username": "viewer_acme", "password": "viewer-password-1"},
    )
    assert login.status_code == 200, login.text
    assert login.json()["active_tenant"] == TENANT_A
    headers = {"Authorization": f"Bearer {login.json()['token']}"}

    own = client.get("/api/schedules/sched-A/history", headers=headers)
    assert own.status_code == 200
    assert own.json()["records"][0]["execution_id"] == "exec-own-A"

    cross = client.get("/api/schedules/sched-B/history", headers=headers)
    assert cross.status_code == 404, cross.text
    assert "exec-SECRET-B" not in cross.text

    missing = client.get("/api/schedules/no-such/history", headers=headers)
    assert missing.status_code == 404

    # 平台视角（API-Key，tenant_scope=None）保持全量可读
    platform = client.get(
        "/api/schedules/sched-B/history", headers={"X-Admin-API-Key": ADMIN_KEY}
    )
    assert platform.status_code == 200, platform.text
    assert platform.json()["records"][0]["execution_id"] == "exec-SECRET-B"


# ---- A48-04：租户删除后不得用旧快照起执行 ----

def _delete_tenant_row(store, tenant_id: str) -> None:
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is not None:
            session.delete(row)
            session.commit()


def test_deleted_tenant_stale_snapshot_does_not_execute(store):
    """租户行被删（#257 orphan 形态，清理尚未跑到）→ 旧快照 fire 不得执行。"""
    _mk_schedule(store, "s-acme", tenant=TENANT_A)
    snapshot = sched.get_schedule(store, "s-acme")
    _delete_tenant_row(store, TENANT_A)

    assert users_svc.tenant_is_active(store, TENANT_A) is True  # 兼容语义仍 fail-open
    assert users_svc.tenant_is_executable(store, TENANT_A) is False  # 运行面严格

    before = _exec_count(store)
    assert sched.fire(store, snapshot, trigger_kind="cron") is None
    assert _exec_count(store) == before
    assert _fire_count(store, "s-acme") == 0
    # 未认领 due：租户恢复/同名重建后仍按原排班可触发（不是永久烧掉）
    assert sched.get_schedule(store, "s-acme")["next_run_at"] == snapshot["next_run_at"]


def test_deleted_tenant_after_purge_stale_snapshot_does_not_execute(store):
    """A54-06 世代：租户行缺失且 flow 残留时，#255 404 补清扫是具名拒绝门
    （API 409）；平台显式清除 flow 后才完成退役，旧快照全程不得复活执行。"""
    _mk_schedule(store, "s-acme", tenant=TENANT_A)
    snapshot = sched.get_schedule(store, "s-acme")
    _delete_tenant_row(store, TENANT_A)

    # 残留 flow 时 404 补清扫必须 loud 拒绝，不得静默清掉运行数据后
    # 让同名重建继承旧流程（A54-06）；拒绝时凭据/运行数据一字不动。
    with pytest.raises(tenants_svc.TenantError, match="流程残留"):
        tenants_svc.delete_tenant(store, TENANT_A)
    assert sched.get_schedule(store, "s-acme") is not None

    # 拒绝期间旧快照仍被运行面租户闸拦下（A48-04）。
    before = _exec_count(store)
    assert sched.fire(store, snapshot, trigger_kind="cron") is None
    assert _exec_count(store) == before
    assert _fire_count(store, "s-acme") == 0

    # 平台显式归档/清除 flow 后重试删除：404 补清扫完成（返回 False），
    # 本地调度与触发历史随运行数据一并清掉，旧快照永不复活。
    store.delete_flow("hello", tenant_id=TENANT_A)
    assert tenants_svc.delete_tenant(store, TENANT_A) is False
    assert sched.get_schedule(store, "s-acme") is None
    assert sched.fire(store, snapshot, trigger_kind="cron") is None
    assert _exec_count(store) == before
    assert _fire_count(store, "s-acme") == 0


def test_active_tenant_stale_snapshot_still_fires(store):
    """负控反向：活跃租户的旧快照仍必须触发 1 条（不能一刀切误杀）。"""
    _mk_schedule(store, "s-live", tenant=TENANT_A)
    snapshot = sched.get_schedule(store, "s-live")
    before = _exec_count(store)

    execution_id = sched.fire(store, snapshot, trigger_kind="cron")
    assert execution_id is not None
    assert _exec_count(store) == before + 1
    assert _fire_count(store, "s-live") == 1
    info = flow_store.get_local_execution(execution_id)
    assert info["tenant_id"] == TENANT_A
