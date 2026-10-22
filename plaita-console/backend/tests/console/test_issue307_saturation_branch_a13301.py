"""#307 追补三类分流（A133-01，B1 第八十四轮任务②）：

A126-02 把删除守卫 503 文案分成「瞬态 → 稍后重试 / 坏行 → 具名持久」两类，
但**饱和触顶**（flow 执行数 ≥ ``_GUARD_SCAN_SINGLE_LIMIT``）落在第三种形态：
executions.py 的饱和 raise 无 ``__cause__``（不是包装任何底层异常）⇒
flows.py 分流的 ``isinstance(e.__cause__, ExecutionStateCorruptError)``
恒 False ⇒ 落「稍后重试」。饱和是**持久**拒绝（数据量 vs 上限配置，大
flow 连删恒 503）——「稍后重试」永不自愈，违 A126-02 自立判据；且上限
无配置面，触顶租户无自助路径（唯一出路 = 改代码重发布）。

修法（三件）：①饱和具名子类 ``VersionInUseScanSaturatedError``（继承
``VersionInUseScanError``，存量 ``except VersionInUseScanError`` 零回归）；
②flows.py 分流补第三类「饱和=持久，具名上限与自助路径」；③上限改具名
env 旋钮 ``PLAITA_GUARD_SCAN_SINGLE_LIMIT``（默认 10000 不变；非法值回落
默认 + WARNING，配置面坏了不拆 fail-closed 闸）。

负控（修复前红/绿格）：
  格① 饱和分流（E2E·RED 主格）：触顶 → 503 文案具名「饱和=持久」+ 当前
  上限数值 + env 旋钮名，无「稍后重试」；
  格② 坏行分流守恒（基线绿锁定）：``__cause__`` 坏行 → 具名坏行文案；
  格③ 瞬态分流守恒（基线绿锁定）：批量读故障 → 维持「稍后重试」；
  格④ 触顶自助路径：默认上限触顶 503（饱和文案）→ 调大上限 → 同一数据
  删除放行（env 旋钮 = 解锁出口）；
  格⑤ env 解析直格：有效值生效 / 缺省 10000 / 非整数·非正数回落默认。
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

ADMIN_KEY = "a13301-admin"
FLOW_ID = "sat-flow"
VER = "1.0.0"
STATE_KEY_PREFIX = "plaita:execution:"
KNOB = "PLAITA_GUARD_SCAN_SINGLE_LIMIT"

FLOW_DEF = json.dumps(
    {
        "flow_id": FLOW_ID,
        "desc": "issue307-saturation-a13301",
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
    flow_store.init_engine(f"sqlite:///{tmp_path / 'a13301.db'}")
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


BASE = datetime(2026, 9, 1, 0, 0, 0)


def _seed_exec(rd, eid: str, flow_id: str, status: str) -> None:
    st = RedisExecutionStorage(client=rd, namespace="plaita")
    assert st.save_execution_state(
        eid,
        ExecutionState(
            execution_id=eid, flow_id=flow_id, flow_version=VER,
            tenant_id="default",
            context={"$LAST_NODE": "w", "$NODE": {}},
            status=status,
            start_time=BASE.isoformat(),
            last_update_time=BASE.isoformat(),
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


def _seed_full_flow(rd, cap: int) -> None:
    """恰 cap 条全终态执行（单次调用返回值触顶，无挂起 = 非终态引用为零）。"""
    for i in range(cap):
        _seed_exec(rd, f"s-{i:03d}", FLOW_ID, "completed")


# ============ 格①：饱和第三类分流（E2E·RED 主格） ============

def test_saturation_503_persistent_guidance_not_retry(redis, monkeypatch):
    """触顶 → 503 文案 = 第三类「饱和=持久」：具名上限数值 + env 旋钮名，
    不给「稍后重试」（A126-02：持久拒绝不指引重试）。

    修复前：饱和 raise 无 __cause__ → 落瞬态支 → 文案含「稍后重试」（红）。
    """
    client = _client(redis)
    _seed_flow(client)
    cap = 30
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap)
    _seed_full_flow(redis, cap)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 503, r.text
    assert _detail_code(r) == "version_in_use_scan_failed"
    msg = _detail_message(r)
    assert "饱和" in msg, f"503 未落饱和第三类分流: {msg[:200]!r}"
    assert "持久" in msg, "饱和文案未声明持久语义"
    assert str(cap) in msg, f"饱和文案未具名当前上限 {cap}: {msg[:200]!r}"
    assert KNOB in msg, f"饱和文案未具名 env 旋钮 {KNOB}: {msg[:200]!r}"
    assert "稍后重试" not in msg, "持久饱和不得给「稍后重试」指引"


# ============ 格②/③：既有两类分流守恒（基线绿锁定） ============

def test_corrupt_row_branch_conserved(redis, monkeypatch):
    """坏行类守恒：__cause__ 坏行 → 具名坏行文案（新饱和支不遮蔽旧支）。

    饱和支排在坏行支之前——本格钉住「饱和支只吃饱和、不吃坏行」。"""
    client = _client(redis)
    _seed_flow(client)
    cap = 30
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap)
    _seed_full_flow(redis, cap - 1)  # cap-1：不触顶
    # 稳态注入坏行（A1 r126 剧本：先让索引就绪，再落行砸坏状态值）
    RedisExecutionStorage(client=redis, namespace="plaita").list_executions()
    _seed_exec(redis, "exec-bad", FLOW_ID, "completed")
    redis.set(f"{STATE_KEY_PREFIX}exec-bad", "\x00corrupt-not-json{")

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 503, r.text
    assert _detail_code(r) == "version_in_use_scan_failed"
    msg = _detail_message(r)
    assert "exec-bad" in msg, f"坏行未具名: {msg[:200]!r}"
    assert "稍后重试" not in msg
    assert "饱和" not in msg, "坏行不得误落饱和支（分流次序回归）"


def test_transient_branch_conserved(redis, monkeypatch):
    """瞬态类守恒：批量读故障 → 维持「稍后重试」（新饱和支不遮蔽旧支）。"""
    client = _client(redis)
    _seed_flow(client)
    cap = 30
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap)
    _seed_full_flow(redis, cap - 1)

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
    assert "稍后重试" in _detail_message(r), "真瞬态必须维持「稍后重试」指引"


# ============ 格④：触顶自助路径 ============

def test_cap_hit_then_raise_knob_unlocks_delete(redis, monkeypatch):
    """自助路径：默认上限触顶 503（饱和文案）→ 调大上限 → 同一数据放行。

    env 旋钮 = 触顶租户的解锁出口（修前唯一出路是改代码重发布）。"""
    client = _client(redis)
    _seed_flow(client)
    cap = 30
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap)
    _seed_full_flow(redis, cap)

    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 503, r.text
    assert "饱和" in _detail_message(r)

    # 运维调大旋钮（env 生效 = 模块常量更新；同一数据不重试即解锁）
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap + 1)
    r = client.delete(f"/api/flows/{FLOW_ID}/versions/{VER}?force=true",
                      headers=_headers())
    assert r.status_code == 200, (
        f"调大上限后同一数据应删除放行（自助路径），实际 {r.status_code}: {r.text[:200]}"
    )


# ============ 格⑤：env 解析直格 ============

def test_env_knob_parsing(redis):
    """具名旋钮解析：有效值生效 / 缺省默认 10000 / 非法值回落默认。"""
    f = executions_api._guard_scan_limit_from_env
    assert f({}) == 10_000, "缺省必须回落默认 10000（默认不变承诺）"
    assert f({KNOB: ""}) == 10_000
    assert f({KNOB: "20000"}) == 20_000, "有效整数必须生效"
    assert f({KNOB: "abc"}) == 10_000, "非整数回落默认（不静默生效坏值）"
    assert f({KNOB: "-5"}) == 10_000, "非正数回落默认（fail-closed 闸不拆）"
    assert f({KNOB: "0"}) == 10_000


def test_saturated_subclass_identity(redis, monkeypatch):
    """具名身份直格：触顶抛 ``VersionInUseScanSaturatedError``（仍是
    ``VersionInUseScanError`` 子类 ⇒ 存量 except 零回归）。"""
    cap = 30
    monkeypatch.setattr(executions_api, "_GUARD_SCAN_SINGLE_LIMIT", cap)
    _seed_full_flow(redis, cap)

    with pytest.raises(VersionInUseScanError) as ei:
        find_flow_version_active_executions(redis, "default", FLOW_ID, VER)
    from api.executions import VersionInUseScanSaturatedError
    assert isinstance(ei.value, VersionInUseScanSaturatedError), (
        f"触顶必须抛具名饱和子类，实际 {type(ei.value).__name__}"
    )
    assert KNOB in str(ei.value), "异常消息未具名 env 旋钮"
