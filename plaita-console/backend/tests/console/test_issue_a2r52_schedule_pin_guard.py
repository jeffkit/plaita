"""A2R52-01/02：schedule 系版本引用（删除守卫第三判据 + update 同口校验）
与 schedule_id 同毫秒碰撞防覆盖。

机制（A2 台账第五十二轮，r56 核销红）：schedule 是版本的**第二类引用方**，
#307（执行扫描翻页）/#308（FK 级联）均未覆盖——

- A2R52-01a：``update`` 无 ``_validate_flow``（仅 create 调）→ PUT 钉不存在
  版本 200 落行（create 同值 422），cron 每 tick 产出
  ``flow_definition_missing`` 垃圾执行；修法=update 钉新版本与 create 同口
  422（空串=解钉不拦，不触碰 version 的更新不复查流程存在性）。
- A2R52-01b：``_enforce_version_delete_guard`` 引用判据只扫非终态执行 →
  钉 draft 的启用调度在 draft 删除（免 force）后原样保留并继续 cron 触发
  （fire 面 enqueue=ok 假健康）；修法=守卫第三判据扫钉版调度行 → 409
  ``version_pinned_by_schedule`` 附调度清单；force 语义照 published 状态
  守卫（可越但须显式），force 放行时被钉调度级联暂停并标注
  ``pinned_version_missing``（触发面伤害随删除同步止住，行不删保配置）。
- A2R52-02：``schedule_id``=毫秒时间戳无随机后缀，同毫秒两连 create 集群档
  hset 静默覆盖（双 200、列表 total=1、先建者无声丢失）；修法=生成侧加
  uuid 后缀 + 写入侧结构性兜底（集群 hsetnx / 本地唯一约束 → 409
  ``schedule_id_conflict``）。

负控（修复前红）：A2 r56 探针 01a/01b/01c（``~/.issue-keeper/duty/
a2-r56-scripts/test_a2r52_axis1_schedule_lifecycle-r56.py``，本仓 pre 证据
``docs/proofs/data/r83-a2r52-schedule-refs-pre.txt``）。
"""
import json
import sys
import uuid as uuid_mod
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

ADMIN_KEY = "a2r52-admin"

FLOW_ID = "pin-flow"
VER_PUB = "1.0.0"
VER_DRAFT = "2.0.0"
CRON = "0 9 * * *"

FLOW_DEF = json.dumps(
    {
        "flow_id": FLOW_ID,
        "desc": "a2r52",
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
    flow_store.init_engine(f"sqlite:///{tmp_path / 'a2r52.db'}")
    return FakeRedis(decode_responses=True)


def _client(redis, *, local_mode: bool = False) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = local_mode
    app.state.store = flow_store.get_flow_store()
    deps = [Depends(require_auth)]
    app.include_router(flows_api.router, prefix="/api", dependencies=deps)
    app.include_router(schedules_api.router, prefix="/api", dependencies=deps)
    return TestClient(app)


def _headers() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_flow(client: TestClient, *, with_draft: bool = False,
               publish: bool = True) -> None:
    client.post("/api/flows", json={"flow_id": FLOW_ID}, headers=_headers())
    r = client.put(f"/api/flows/{FLOW_ID}/versions/{VER_PUB}",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 200, r.text
    if publish:
        r = client.post(f"/api/flows/{FLOW_ID}/publish",
                        json={"version": VER_PUB}, headers=_headers())
        assert r.status_code == 200, r.text
    if with_draft:
        r = client.put(f"/api/flows/{FLOW_ID}/versions/{VER_DRAFT}",
                       json={"definition": FLOW_DEF}, headers=_headers())
        assert r.status_code == 200, r.text


def _mk_sched(client: TestClient, *, version: str = VER_PUB,
              enabled: bool = True, name: str = "s1"):
    return client.post("/api/schedules", json={
        "name": name, "flow_id": FLOW_ID, "version": version,
        "cron": CRON, "enabled": enabled,
    }, headers=_headers())


def _detail(r) -> dict:
    body = r.json() or {}
    return body.get("detail") if isinstance(body.get("detail"), dict) else {}


# ============ A2R52-01a：update 钉版本与 create 同口 ============

def test_update_pin_nonexistent_version_422_row_untouched(redis):
    """PUT 钉不存在版本 → 422（与 create 同口），调度行原样。"""
    client = _client(redis)
    _seed_flow(client)
    r = _mk_sched(client, version=VER_PUB)
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.put(f"/api/schedules/{sid}", json={"version": "9.9.9"},
                    headers=_headers())
    assert r2.status_code == 422, r2.text
    got = client.get(f"/api/schedules/{sid}", headers=_headers()).json()
    assert got["version"] == VER_PUB, "不存在版本不得写入调度行"
    assert got["runtime"] == "python"


def test_update_unpin_empty_version_skips_validation(redis):
    """空串=解钉（跟随 latest published），不做版本存在性校验。"""
    client = _client(redis)
    _seed_flow(client)
    r = _mk_sched(client, version=VER_PUB)
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.put(f"/api/schedules/{sid}", json={"version": ""},
                    headers=_headers())
    assert r2.status_code == 200, r2.text
    assert r2.json()["version"] is None


def test_update_name_only_on_orphan_schedule_ok(redis):
    """不触碰 version 的更新不复查流程存在性（存量孤儿行仍可编辑）。"""
    client = _client(redis)
    _seed_flow(client, with_draft=True, publish=False)
    r = _mk_sched(client, version=VER_PUB, name="before")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]
    # force 删整个 flow（钉版调度被级联暂停但行保留）→ 孤儿行
    r2 = client.delete(f"/api/flows/{FLOW_ID}?force=true", headers=_headers())
    assert r2.status_code == 200, r2.text

    r3 = client.put(f"/api/schedules/{sid}", json={"name": "after"},
                    headers=_headers())
    assert r3.status_code == 200, r3.text
    assert r3.json()["name"] == "after"


# ============ A2R52-01b：删除守卫第三判据（钉版调度） ============

def test_delete_draft_pinned_by_schedule_409_with_listing(redis):
    """钉 draft 的启用调度：删 draft → 409 附调度清单，拒绝零副作用。"""
    client = _client(redis)
    _seed_flow(client, with_draft=True, publish=False)
    r = _mk_sched(client, version=VER_DRAFT, name="pinned")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER_DRAFT}",
                       headers=_headers())
    assert r2.status_code == 409, r2.text
    detail = _detail(r2)
    assert detail.get("code") == "version_pinned_by_schedule"
    assert detail.get("total") == 1
    listing = detail.get("schedules") or []
    assert len(listing) == 1
    assert listing[0]["schedule_id"] == sid
    assert listing[0]["version"] == VER_DRAFT
    assert listing[0]["enabled"] is True
    # 拒绝零副作用：版本行在、调度行原样（enabled 不变）
    versions = flow_store.get_flow_store().list_versions(
        FLOW_ID, tenant_id="default")
    assert any(v.version == VER_DRAFT for v in versions)
    got = client.get(f"/api/schedules/{sid}", headers=_headers()).json()
    assert got["enabled"] is True


def test_delete_draft_pinned_by_paused_schedule_still_409(redis):
    """暂停调度钉版同样拦（行在即可复启用，钉住事实不随 enabled 消失）。"""
    client = _client(redis)
    _seed_flow(client, with_draft=True, publish=False)
    r = _mk_sched(client, version=VER_DRAFT, enabled=False)
    assert r.status_code == 200, r.text

    r2 = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER_DRAFT}",
                       headers=_headers())
    assert r2.status_code == 409, r2.text
    assert _detail(r2).get("code") == "version_pinned_by_schedule"


def test_delete_draft_pinned_force_ok_and_schedule_paused_marked(redis):
    """force 放行：版本删除落地，被钉调度级联暂停并标注（行保留）。"""
    client = _client(redis)
    _seed_flow(client, with_draft=True, publish=False)
    r = _mk_sched(client, version=VER_DRAFT, name="pinned")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER_DRAFT}?force=true",
                       headers=_headers())
    assert r2.status_code == 200, r2.text
    versions = flow_store.get_flow_store().list_versions(
        FLOW_ID, tenant_id="default")
    assert all(v.version != VER_DRAFT for v in versions)
    # 级联暂停：cron 触发面停（enabled=False + next_run_at 空）+ 标注透出
    got = client.get(f"/api/schedules/{sid}", headers=_headers()).json()
    assert got["enabled"] is False
    assert got.get("next_run_at", "") == ""
    assert got.get("pinned_version_missing") is True


def test_delete_draft_without_schedule_still_ok(redis):
    """负控：无调度钉版 → draft 删除照常放行（存量行为零回归）。"""
    client = _client(redis)
    _seed_flow(client, with_draft=True, publish=False)
    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER_DRAFT}",
                      headers=_headers())
    assert r.status_code == 200, r.text


def test_delete_version_other_version_unaffected(redis):
    """调度钉 2.0.0 时删 1.0.0 不受累（scan_version 过滤）。"""
    client = _client(redis)
    _seed_flow(client, with_draft=True)  # 1.0.0 published + 2.0.0 draft
    r = _mk_sched(client, version=VER_DRAFT)
    assert r.status_code == 200, r.text

    r2 = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER_PUB}",
                       headers=_headers())
    assert r2.status_code == 409, r2.text  # published 状态守卫（既有判据）
    r3 = client.delete(
        f"/api/flows/{FLOW_ID}/versions/{VER_PUB}?force=true",
        headers=_headers())
    assert r3.status_code == 200, r3.text
    got = client.get(f"/api/schedules/{r.json()['schedule_id']}",
                     headers=_headers()).json()
    assert got["enabled"] is True, "未被删版本的钉版调度不受级联暂停"


def test_delete_flow_blocked_when_any_version_pinned(redis):
    """delete_flow 聚合判据：任一版本被钉即拦；force 放行并级联暂停。"""
    client = _client(redis)
    _seed_flow(client, with_draft=True, publish=False)
    r = _mk_sched(client, version=VER_DRAFT)
    assert r.status_code == 200, r.text

    r2 = client.delete(f"/api/flows/{FLOW_ID}", headers=_headers())
    assert r2.status_code == 409, r2.text
    assert _detail(r2).get("code") == "version_pinned_by_schedule"

    r3 = client.delete(f"/api/flows/{FLOW_ID}?force=true", headers=_headers())
    assert r3.status_code == 200, r3.text
    got = client.get(f"/api/schedules/{r.json()['schedule_id']}",
                     headers=_headers()).json()
    assert got["enabled"] is False
    assert got.get("pinned_version_missing") is True


def test_local_delete_draft_pinned_409_and_force_pauses(redis):
    """本地档同判据（SQLite 面）：409 拒绝；force 放行并暂停（行保留）。"""
    client = _client(redis, local_mode=True)
    _seed_flow(client, with_draft=True, publish=False)
    r = _mk_sched(client, version=VER_DRAFT, name="local-pinned")
    assert r.status_code == 200, r.text
    sid = r.json()["schedule_id"]

    r2 = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER_DRAFT}",
                       headers=_headers())
    assert r2.status_code == 409, r2.text
    assert _detail(r2).get("code") == "version_pinned_by_schedule"

    r3 = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER_DRAFT}?force=true",
                       headers=_headers())
    assert r3.status_code == 200, r3.text
    got = client.get(f"/api/schedules/{sid}", headers=_headers()).json()
    assert got["enabled"] is False
    assert got.get("next_run_at", "") == ""


# ============ A2R52-02：schedule_id 同毫秒碰撞防覆盖 ============

class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: ARG003
        return datetime(2026, 10, 23, 12, 0, 0, 123456)


class _FrozenUuid:
    hex = "cafebabe"


def test_same_ms_double_create_distinct_ids_both_stored(redis, monkeypatch):
    """同毫秒两连 create：两 id 不同、双双落库（无静默覆盖）。"""
    monkeypatch.setattr(schedules_api, "datetime", _FrozenDatetime)
    client = _client(redis)
    _seed_flow(client)
    r1 = _mk_sched(client, version=VER_PUB, name="first")
    r2 = _mk_sched(client, version=VER_PUB, name="second")
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    id1, id2 = r1.json()["schedule_id"], r2.json()["schedule_id"]
    assert id1 != id2, "随机后缀缺失：同毫秒 id 相撞"
    lst = client.get("/api/schedules", headers=_headers()).json()
    names = {s["name"] for s in lst.get("schedules", [])}
    assert lst["total"] == 2
    assert names == {"first", "second"}


def test_cluster_create_id_conflict_409(redis, monkeypatch):
    """结构性兜底：id 撞行时 hsetnx 拒绝 → 409 schedule_id_conflict。"""
    monkeypatch.setattr(schedules_api, "datetime", _FrozenDatetime)
    monkeypatch.setattr(uuid_mod, "uuid4", lambda: _FrozenUuid)
    client = _client(redis)
    _seed_flow(client)
    r1 = _mk_sched(client, version=VER_PUB, name="first")
    assert r1.status_code == 200, r1.text
    r2 = _mk_sched(client, version=VER_PUB, name="second")
    assert r2.status_code == 409, r2.text
    detail = _detail(r2)
    assert detail.get("code") == "schedule_id_conflict"
    assert detail.get("schedule_id") == r1.json()["schedule_id"]
    # 先建者原样保留（不再是覆盖即真）
    lst = client.get("/api/schedules", headers=_headers()).json()
    assert lst["total"] == 1
    assert lst["schedules"][0]["name"] == "first"


def test_local_create_duplicate_id_conflict_409(redis, monkeypatch):
    """本地档唯一约束兜底：同 id 二插 409（此前 IntegrityError 裸穿 500）。"""
    monkeypatch.setattr(schedules_api, "datetime", _FrozenDatetime)
    monkeypatch.setattr(uuid_mod, "uuid4", lambda: _FrozenUuid)
    client = _client(redis, local_mode=True)
    _seed_flow(client)
    r1 = _mk_sched(client, version=VER_PUB, name="first")
    assert r1.status_code == 200, r1.text
    r2 = _mk_sched(client, version=VER_PUB, name="second")
    assert r2.status_code == 409, r2.text
    assert _detail(r2).get("code") == "schedule_id_conflict"
    lst = client.get("/api/schedules", headers=_headers()).json()
    assert lst["total"] == 1
    assert lst["schedules"][0]["name"] == "first"
