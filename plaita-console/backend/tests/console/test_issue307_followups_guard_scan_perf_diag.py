"""#307 追补双单（A1 r126 台账 A126-01/A126-02，B3 第八十五轮）：

- **A126-01 守卫翻页性能**：#307 的翻页穷尽走存储层 query 分支——该分支
  每页 ``_index_slice(0, None)`` + mget **整载租户全索引**再内存过滤，
  ``offset`` 只作用于过滤后切片 ⇒ 穷尽扫描代价 = O(pages×T)（实弹
  T=1600/flow 800 终态：9 次 load / 14400 次反序列化 = 单次大 limit 对照
  9.0×、墙钟 23.6×；delete 端点 O(M/100×T) 无墙钟上限）。修法 = 守卫改
  **单次大 limit 显式传参**（穷尽性等价、代价回 O(T)），上限饱和 =
  穷尽核实能力不足 → 仍 fail-closed（503），不静默截断——防「默认参数
  当全量」回潮：大 limit 显式传参 ≠ 不传。
- **A126-02 strict 503 可诊断性**：单条坏行砖死租户全域 delete（连删
  3×503），而异常 str / console 日志 / 503 detail 三处均不具名坏行——
  「表面健康（非 strict 列表如常）、删除砖死」且「稍后重试」文案对
  持久坏行是错误指引。修法 = strict 扫描异常具名坏行（state key +
  原因摘要，不泄值）+ 503 detail 具名 + 结构化 WARNING 日志可检索 +
  503 文案按瞬态/持久分流；runbook 补恢复程序。

负控（修复前红/绿格）：``docs/proofs/data/r85-issue307-followups-negative-control-{pre,post}.txt``。
"""
import json
import logging
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

ADMIN_KEY = "issue307-followups-admin"
FLOW_ID = "guard-flow"
VER = "1.0.0"
STATE_KEY_PREFIX = "plaita:execution:"

FLOW_DEF = json.dumps(
    {
        "flow_id": FLOW_ID,
        "desc": "issue307-followups",
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
    flow_store.init_engine(f"sqlite:///{tmp_path / 'issue307-followups.db'}")
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


def _seed_exec(rd, eid: str, flow_id: str, status: str,
               start_time: datetime) -> None:
    st = RedisExecutionStorage(client=rd, namespace="plaita")
    assert st.save_execution_state(
        eid,
        ExecutionState(
            execution_id=eid, flow_id=flow_id, flow_version=VER,
            tenant_id="default",
            context={"$LAST_NODE": "w", "$NODE": {}},
            status=status,
            start_time=start_time.isoformat(),
            last_update_time=start_time.isoformat(),
        ),
    )


def _detail(r) -> dict:
    try:
        return (r.json() or {}).get("detail") or {}
    except Exception:  # noqa: BLE001
        return {}


def _detail_code(r) -> str:
    return str(_detail(r).get("code", ""))


def _detail_message(r) -> str:
    return str(_detail(r).get("message", ""))


BASE = datetime(2026, 9, 1, 0, 0, 0)


# ============ A126-01：守卫扫描单次化（请求数/反序列化量对账） ============

def _count_loads(monkeypatch):
    """计量 _load_indexed_states 调用数与反序列化总量（A1 r126 probe2 量法）。"""
    calls = {"n": 0, "ids": 0}
    orig = RedisExecutionStorage._load_indexed_states

    def counted(self, ids, strict=False):
        calls["n"] += 1
        calls["ids"] += len(ids)
        return orig(self, ids, strict=strict)

    monkeypatch.setattr(RedisExecutionStorage, "_load_indexed_states", counted)
    return calls


def _seed_tenant_with_tail(rd, n_terminal: int, n_noise: int):
    """n 条 x-flow 终态（老）+ n 条噪声 flow 终态 + 1 条 x-flow 挂起（尾）。

    A1 r126 实弹剧本（T=1600/flowX=800 终态）的等比缩小：挂起执行排
    索引尾部，守卫必须扫到尾才算穷尽。"""
    for i in range(n_terminal):
        _seed_exec(rd, f"x-{i:04d}", FLOW_ID, "completed",
                   BASE + timedelta(minutes=i))
        _seed_exec(rd, f"y-{i:04d}", "noise-flow", "completed",
                   BASE + timedelta(minutes=i))
    _seed_exec(rd, "x-susp-tail", FLOW_ID, "suspended",
               datetime(2026, 10, 22, 12, 0, 0))
    return n_terminal * 2 + 1  # 租户全索引行数 T


def test_guard_scan_single_call_and_o_of_t(redis, monkeypatch):
    """判据直读：守卫扫描 = 单次 list_executions（大 limit 显式传参），
    反序列化总量 ≤ T 一次（翻页剧本下是 pages×T）。"""
    t_total = _seed_tenant_with_tail(redis, 125, 125)
    calls = _count_loads(monkeypatch)
    li_kwargs = []
    orig_li = RedisExecutionStorage.list_executions

    def spy_li(self, *a, **kw):
        li_kwargs.append(kw)
        return orig_li(self, *a, **kw)

    monkeypatch.setattr(RedisExecutionStorage, "list_executions", spy_li)

    active = find_flow_version_active_executions(redis, "default", FLOW_ID, VER)

    assert [s.execution_id for s in active] == ["x-susp-tail"]
    # 穷尽性成立（尾部挂起命中）且请求数 = 1 次量级
    assert calls["n"] == 1, f"list_executions 调了 {calls['n']} 次（翻页回潮）"
    # 反序列化总量对账：单次 O(T)（翻页剧本 = pages×T）
    assert calls["ids"] <= t_total, (
        f"反序列化 {calls['ids']} 条 > 租户全索引 {t_total} 条（多页整载回潮）")
    # 大 limit 显式传参 ≠ 不传/默认参数当全量（#307 修前形态的回归门）
    assert len(li_kwargs) == 1
    kw = li_kwargs[0]
    assert kw.get("strict") is True
    assert kw.get("offset") == 0
    assert kw.get("limit", 0) == executions_api._GUARD_SCAN_SINGLE_LIMIT
    assert kw.get("limit", 0) > 100, "limit 未显式放大（默认参数当全量回潮）"


def test_delete_endpoint_single_pass_and_guard_verdict(redis, monkeypatch):
    """端到端守恒：单次化后 #307 的判据行为不变——忙 flow 尾部挂起执行
    仍被命中（force 删除 409），且扫描请求数为 1。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_tenant_with_tail(redis, 60, 60)
    calls = _count_loads(monkeypatch)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 409, r.text
    assert _detail_code(r) == "version_in_use"
    assert calls["n"] == 1


def test_guard_cap_saturation_fail_closed(redis, monkeypatch):
    """上限饱和 = 结果可能是截断窗 → fail-closed（503），不静默当「核实过」。"""
    client = _client(redis)
    _seed_flow(client)
    cap = 30
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap)
    # 恰好 cap 条 flow 执行（全终态，无挂起）——单次调用返回值触顶
    for i in range(cap):
        _seed_exec(redis, f"c-{i:03d}", FLOW_ID, "completed",
                   BASE + timedelta(minutes=i))

    with pytest.raises(VersionInUseScanError):
        find_flow_version_active_executions(redis, "default", FLOW_ID, VER)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 503, r.text
    assert _detail_code(r) == "version_in_use_scan_failed"


def test_guard_below_cap_clean_pass(redis, monkeypatch):
    """对照格：cap-1 条（全终态、无挂起）→ 未触顶 → 正常放行 200。"""
    client = _client(redis)
    _seed_flow(client)
    cap = 30
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap)
    for i in range(cap - 1):
        _seed_exec(redis, f"c-{i:03d}", FLOW_ID, "completed",
                   BASE + timedelta(minutes=i))

    active = find_flow_version_active_executions(redis, "default", FLOW_ID, VER)
    assert active == []
    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text


# ============ A126-02：strict 坏行三盲——异常/日志/503 具名 + 文案分流 ============

def _inject_corrupt_row(rd, eid: str, *, backfill_ready: bool) -> None:
    """稳态注入形（A1 r126 剧本）：先让索引就绪（ready 置位），**再**落
    合法行（``save_execution_state`` 无条件 zadd 索引成员）、砸坏状态值。
    顺序不能反——ready 未置位时的首次列表会走回填，坏行在回填里被
    非 strict 跳过、进不了索引，就测不到稳态 strict 路径。"""
    if backfill_ready:
        RedisExecutionStorage(client=rd, namespace="plaita").list_executions()
        assert rd.exists("plaita:execution:index:ready") == 1
    _seed_exec(rd, eid, FLOW_ID, "completed", BASE)
    rd.set(f"{STATE_KEY_PREFIX}{eid}", "\x00corrupt-not-json{")


def test_strict_scan_exception_names_corrupt_row(redis):
    """判据直读：strict 扫描异常具名坏行（state key + execution_id），
    且不泄坏行值。"""
    for i in range(3):
        _seed_exec(redis, f"good-{i}", FLOW_ID, "completed",
                   BASE + timedelta(minutes=i))
    _inject_corrupt_row(redis, "exec-bad", backfill_ready=True)

    with pytest.raises(VersionInUseScanError) as ei:
        find_flow_version_active_executions(redis, "default", FLOW_ID, VER)
    msg = str(ei.value)
    assert "exec-bad" in msg, f"异常未具名坏行: {msg[:200]!r}"
    assert f"{STATE_KEY_PREFIX}exec-bad" in msg
    assert "corrupt-not-json" not in msg, "坏行值泄漏进异常消息"


def test_corrupt_row_log_searchable_by_key(redis, caplog):
    """日志出口：坏行具名进结构化 WARNING，可按 state key 检索。"""
    _seed_exec(redis, "good-0", FLOW_ID, "completed", BASE)
    _inject_corrupt_row(redis, "exec-bad", backfill_ready=True)

    with caplog.at_level(logging.WARNING, logger="plaita"):
        with pytest.raises(VersionInUseScanError):
            find_flow_version_active_executions(redis, "default", FLOW_ID, VER)
    msgs = [r.getMessage() for r in caplog.records]
    assert any(f"{STATE_KEY_PREFIX}exec-bad" in m for m in msgs), (
        f"日志未具名坏行: {msgs[:3]!r}")


def test_delete_503_persistent_names_row_no_retry_guidance(redis):
    """端到端：单坏行 → 503 detail 具名坏行 + 持久文案（不再「稍后重试」）；
    连删两次结果一致（持久，非瞬态抖动）。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_exec(redis, "good-0", FLOW_ID, "completed", BASE)
    _inject_corrupt_row(redis, "exec-bad", backfill_ready=True)

    for _ in range(2):
        r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                          headers=_headers())
        assert r.status_code == 503, r.text
        assert _detail_code(r) == "version_in_use_scan_failed"
        msg = _detail_message(r)
        assert "exec-bad" in msg, f"503 detail 未具名坏行: {msg[:200]!r}"
        assert "稍后重试" not in msg, "持久坏行不应给「稍后重试」指引"


def test_transient_scan_error_keeps_retry_guidance(redis):
    """文案分流守恒：真瞬态（批量读故障）→ 维持「稍后重试」。"""
    client = _client(redis)
    _seed_flow(client)
    _seed_exec(redis, "good-0", FLOW_ID, "completed", BASE)

    class _PingOkBulkFail(FakeRedis):
        def mget(self, *a, **kw):
            raise redis_py.exceptions.TimeoutError("simulated socket timeout")

        def scan_iter(self, *a, **kw):
            raise redis_py.exceptions.TimeoutError("simulated socket timeout")

    flaky = _PingOkBulkFail(decode_responses=True)
    flaky.ping()
    client.app.state.redis = flaky

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 503, r.text
    assert _detail_code(r) == "version_in_use_scan_failed"
    assert "稍后重试" in _detail_message(r)


def test_legacy_backfill_corrupt_key_named(redis):
    """回填路径变体：ready 缺失 + 坏存量键 → 异常同样具名坏行。"""
    rd = redis
    rd.set(f"{STATE_KEY_PREFIX}legacy-bad", "\x00garbage{")

    with pytest.raises(VersionInUseScanError) as ei:
        find_flow_version_active_executions(rd, "default", FLOW_ID, VER)
    assert "legacy-bad" in str(ei.value)


# ============ 守恒格：合法空索引 / 非 strict 列表语义 ============

def test_legit_empty_index_delete_not_harmed(redis):
    """新租户零执行：delete 照常 200（守卫不误伤合法空索引）。"""
    client = _client(redis)
    _seed_flow(client)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text


def test_non_strict_list_still_skips_corrupt_row(redis):
    """#307 边界不动：非 strict 列表对坏行维持跳过降级（表面健康语义）。"""
    _seed_exec(redis, "good-0", FLOW_ID, "completed", BASE)
    _inject_corrupt_row(redis, "exec-bad", backfill_ready=True)

    st = RedisExecutionStorage(client=redis, namespace="plaita")
    rows = st.list_executions()
    assert [s.execution_id for s in rows] == ["good-0"]
