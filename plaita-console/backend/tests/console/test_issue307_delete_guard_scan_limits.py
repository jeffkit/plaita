"""#307：删除守卫引用判据两处失效——规模截断漏扫 + 扫描异常 fail-open。

issue 机制（A2R50-01 实弹复现面，探针归档
``~/.issue-keeper/duty/a2-r50-scripts/test_a2r50_axis1_delete_guard.py``）：

- **① 规模截断**：``find_flow_version_active_executions`` 调
  ``list_executions(query={"flow_id": …})`` 未传 limit——存储层默认
  ``limit=100`` 且按 ``start_time`` **升序**，截断保留最老 100 条；忙
  flow（30 天 TTL 内 cron 即可产生 >100 条终态）的尾部非终态执行对守卫
  不可见 → ``delete_version/delete_flow?force=true`` 放行，#304「引用
  守卫 force 不可越」在该规模档形同虚设。
- **② 异常吞没**：``list_executions`` 整体 ``except Exception → []``，
  守卫唯一前置检查是 ``redis.ping()``——ping 通过后的批量读失败
  （mget/scan_iter 超时等）被判「无引用」放行，违反自身 fail-closed
  契约（``VersionInUseScanError`` → 503 ``version_in_use_scan_failed``）。

修法口径（一个函数收口 + 存储层严格模式）：

- 判据读面**翻页穷尽**（显式 limit+offset 循环至短页/空页，收满 limit
  条非终态即提前停——不停在「默认窗口」上）；
- ``list_executions`` 增 ``strict=True``：失败上抛不吞；守卫路径转
  ``VersionInUseScanError`` 维持 503 fail-closed。默认吞成空列表的
  对外列表语义不变（#307 边界：不动列表端点分页语义）。

负控（修复前红/绿格）：``docs/proofs/data/r79-issue307-negative-control-pre.txt``。
"""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import redis as redis_py
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api.executions import (  # noqa: E402
    VersionInUseScanError,
    find_flow_version_active_executions,
)
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.redis import RedisExecutionStorage  # noqa: E402

ADMIN_KEY = "issue307-admin"
FLOW_ID = "guard-flow"
VER = "1.0.0"

FLOW_DEF = json.dumps(
    {
        "flow_id": FLOW_ID,
        "desc": "issue307",
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
    flow_store.init_engine(f"sqlite:///{tmp_path / 'issue307.db'}")
    return FakeRedis(decode_responses=True)


def _client(rd) -> TestClient:
    app = FastAPI()
    app.state.redis = rd
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    deps = [Depends(require_auth)]
    app.include_router(flows_api.router, prefix="/api", dependencies=deps)
    app.include_router(executions_api.router, prefix="/api", dependencies=deps)
    return TestClient(app)


def _headers() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_flow(client: TestClient) -> None:
    client.post("/api/flows", json={"flow_id": FLOW_ID}, headers=_headers())
    r = client.put(f"/api/flows/{FLOW_ID}/versions/{VER}",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 200, r.text
    r = client.post(f"/api/flows/{FLOW_ID}/publish", json={"version": VER},
                    headers=_headers())
    assert r.status_code == 200, r.text


def _eng_key() -> str:
    return f"plaita:flow:{FLOW_ID}:{VER}"


def _detail_code(r) -> str:
    try:
        return str((r.json() or {}).get("detail", {}).get("code", ""))
    except Exception:  # noqa: BLE001
        return ""


def _seed_exec(rd, eid: str, status: str, start_time: datetime) -> None:
    st = RedisExecutionStorage(client=rd, namespace="plaita")
    assert st.save_execution_state(
        eid,
        ExecutionState(
            execution_id=eid, flow_id=FLOW_ID, flow_version=VER,
            tenant_id="default",
            context={"$LAST_NODE": "w", "$NODE": {}},
            status=status,
            start_time=start_time.isoformat(),
            last_update_time=start_time.isoformat(),
        ),
    )


def _seed_busy_tail(rd, n_completed: int) -> None:
    """n 条已终态（老，start_time 升序在前）+ 1 条挂起（最新，排尾部）
    ——真实忙 flow 的分布：最老 100 条几乎恒为终态。"""
    base = datetime(2026, 9, 1, 0, 0, 0)
    for i in range(n_completed):
        _seed_exec(rd, f"exec-old-{i:04d}", "completed",
                   base + timedelta(minutes=i))
    _seed_exec(rd, "exec-susp-tail", "suspended",
               datetime(2026, 10, 22, 12, 0, 0))


class _PingOkBulkFail(FakeRedis):
    """ping 正常（守卫唯一前置探活通过），批量读抛 TimeoutError
    （模拟 socket 超时——mget/scan_iter 同族一并失败）。"""

    def mget(self, *a, **kw):
        raise redis_py.exceptions.TimeoutError("simulated socket timeout")

    def scan_iter(self, *a, **kw):
        raise redis_py.exceptions.TimeoutError("simulated socket timeout")


# ============ ① 规模截断：120 终态（老）+ 1 挂起（尾） ============

def test_scan_beyond_default_window_finds_tail_suspended(redis):
    """判据直读：默认窗口外尾部的挂起执行必须被翻页穷尽命中。"""
    _seed_busy_tail(redis, 120)
    st = RedisExecutionStorage(client=redis, namespace="plaita")
    # 失效面锚点：带 query 的列表读仍受默认 limit 截断（语义不动）
    assert len(st.list_executions(query={"flow_id": FLOW_ID})) == 100
    active = find_flow_version_active_executions(redis, "default", FLOW_ID, VER)
    assert [s.execution_id for s in active] == ["exec-susp-tail"]


def test_delete_version_force_beyond_100_409(redis):
    """121 条（120 终态 + 尾 1 挂起）：delete_version?force=true → 409。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_busy_tail(redis, 120)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_in_use"
    body = r.json()["detail"]
    assert body["total"] == 1
    assert body["executions"][0]["execution_id"] == "exec-susp-tail"
    assert redis.exists(_eng_key()) == 1, "引擎定义键应原样（删除被拦截）"


def test_delete_flow_force_aggregate_beyond_100_409(redis):
    """delete_flow 聚合判据（version=None）同走翻页穷尽，不再被截断绕行。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_busy_tail(redis, 120)

    r = client.delete(f"/api/flows/{FLOW_ID}?force=true", headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_in_use"


# ============ 边界两格：100 / 101 ============

def test_boundary_101_suspended_at_position_101_409(redis):
    """100 终态 + 1 挂起 = 101 条：挂起恰在默认窗口外一格 → 409。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_busy_tail(redis, 100)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_in_use"


def test_boundary_100_suspended_within_window_409(redis):
    """对照格：99 终态 + 1 挂起 = 100 条（窗口内）→ 守卫一直会拦，
    证明失效边界恰是默认 limit=100，非其他因素。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_busy_tail(redis, 99)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_in_use"


# ============ ② 扫描异常：ping 通、批量读炸 → 不得吞成「无引用」 ============

def test_scan_error_raises_version_in_use_scan_error(redis):
    """判据直读：批量读失败 → 具名 VersionInUseScanError（不吞成空列表）。"""
    _seed_exec(redis, "exec-susp-one", "suspended",
               datetime(2026, 10, 22, 12, 0, 0))
    flaky = _PingOkBulkFail(decode_responses=True)
    flaky.ping()  # 探活通过——守卫既有前置检查拦不住这一类故障

    with pytest.raises(VersionInUseScanError):
        find_flow_version_active_executions(flaky, "default", FLOW_ID, VER)


def test_delete_endpoint_scan_error_503_fail_closed(redis):
    """端到端：扫描故障 → 503 version_in_use_scan_failed（fail-closed）。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_exec(redis, "exec-susp-one", "suspended",
               datetime(2026, 10, 22, 12, 0, 0))
    client.app.state.redis = _PingOkBulkFail(decode_responses=True)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 503, r.text
    assert _detail_code(r) == "version_in_use_scan_failed"


def test_list_executions_strict_mode_raises(redis):
    """存储层 strict 模式：失败上抛；默认模式仍吞成空列表（列表语义不变）。"""
    _seed_exec(redis, "exec-susp-one", "suspended",
               datetime(2026, 10, 22, 12, 0, 0))
    flaky = _PingOkBulkFail(decode_responses=True)
    st = RedisExecutionStorage(client=flaky, namespace="plaita")

    assert st.list_executions(query={"flow_id": FLOW_ID}) == []
    with pytest.raises(redis_py.exceptions.TimeoutError):
        st.list_executions(query={"flow_id": FLOW_ID}, strict=True)


# ============ 存量零回归锚：无引用 + 引用消失后放行 ============

def test_delete_version_force_clean_ok(redis):
    """无任何执行：force 删除照常（守卫翻页穷尽不改变放行语义）。"""
    client = _client(redis)
    _seed_flow(client)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text
