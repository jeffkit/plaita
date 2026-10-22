"""#308：delete_flow 版本行孤儿——SQLite 外键级联未强制 + 删除级联缺失。

机制（A1 r124 RED×4 实弹，issue 正文）：``DELETE /api/flows/{id}`` 的版本行
级联只靠 ``FlowVersion`` 复合外键的 ``ondelete=CASCADE`` **DB 级声明**；
SQLite 默认不强制外键、console 全程零 PRAGMA，而默认 ``db_url`` 就是
SQLite——删 flow 200 后版本行原样残留（含 published）：

1. 已删 flow **显式**版本 start 200 幽灵入队——#304 的 start 具名 404
   （``version_not_found``）被整体绕过（修后随版本行清零自然 404）；
2. 已删 flow **latest** start 解析到残留 published 行、200 且回声版本号
   （假确认）。修后不再命中孤儿；状态仍 200（存量 #300/#304 latest
   fail-open 契约：console 一无所知的直部署 flow 同样受理，「已删」与
   「从未注册」DB 态不可区分），改为具名 warning 点名「无 flow 记录」。
   issue 验收原文的 latest 404 与该契约冲突（39 个既有用例踩在其上），
   带数据上报拍板，不在本单单方面改契约；
3. 同 id 重建后 ``PUT 1.0.0`` 撞孤儿 published → 409。force 逃生**不可用**
   （``_save_flow_once`` 的 published 不可覆盖是无条件闸，``force`` 只绕
   乐观锁 CAS——两道闸不同源）；唯一逃生是先
   ``DELETE /versions/1.0.0?force=true`` 清掉孤儿行再 PUT（本文件实证）。

修法双层（勿只靠 DB 声明）：

- 连接层：console 引擎每次 **checkout** 拨 ``PRAGMA foreign_keys=ON``
  （幂等；挂 checkout 而非 connect——池化连接被上一任使用者显式 OFF 后
  归还，下一次 checkout 仍被强制拨回，无「带 OFF 复用」窗口）；
- 存储层：``delete_flow`` 显式级联删版本行（先子后父同一事务，PG/MySQL
  同样生效）。迁移会话反向：``migrations/env.py`` 对 SQLite 连接显式
  OFF（与存量孤儿行共存，batch 表重建不被历史数据卡死）；
  ``_migrate_tenant_schema`` 回填改父表先于子表（FK 强制下子行不得先插）。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

ADMIN_KEY = "issue308-admin"

FLOW_DEF = json.dumps(
    {
        "flow_id": "cascade-flow",
        "desc": "issue308",
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
def db_url(tmp_path) -> str:
    return f"sqlite:///{tmp_path / 'issue308.db'}"


@pytest.fixture()
def redis(db_url, monkeypatch) -> FakeRedis:
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(db_url)
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


def _seed_flow(client: TestClient, flow_id: str = "cascade-flow") -> None:
    r = client.post("/api/flows", json={"flow_id": flow_id}, headers=_headers())
    assert r.status_code == 200, r.text
    r = client.put(f"/api/flows/{flow_id}/versions/1.0.0",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 200, r.text


def _publish(client: TestClient, flow_id: str = "cascade-flow",
             version: str = "1.0.0") -> None:
    r = client.post(f"/api/flows/{flow_id}/publish", json={"version": version},
                    headers=_headers())
    assert r.status_code == 200, r.text


def _detail_code(r) -> str:
    try:
        return str((r.json() or {}).get("detail", {}).get("code", ""))
    except Exception:  # noqa: BLE001
        return ""


def _version_count(engine, flow_id: str = "cascade-flow") -> int:
    """直接数 DB 里的版本行（不看 API 读面，孤几行只有 DB 数得出）。"""
    with engine.connect() as conn:
        return int(conn.execute(
            text("SELECT COUNT(*) FROM flow_versions WHERE flow_id = :fid"),
            {"fid": flow_id},
        ).fetchone()[0])


def _ghost_execution_rows(redis: FakeRedis) -> int:
    """执行索引里的行数（幽灵入队 = >0；与 RedisExecutionStorage 同 ns）。"""
    return int(redis.zcard("plaita:execution:index") or 0)


# ============ RED×4（issue 正文验收面） ============

def test_delete_flow_clears_version_rows(redis, db_url):
    """删 flow 后版本行清零（draft-only；issue RED ①的上半）。"""
    client = _client(redis)
    _seed_flow(client)

    r = client.delete("/api/flows/cascade-flow", headers=_headers())
    assert r.status_code == 200, r.text
    engine = flow_store.get_init_engine()
    assert _version_count(engine) == 0, (
        "delete_flow 后版本行必须清零（SQLite FK CASCADE 未强制，不能只靠 DB 声明）"
    )


def test_delete_flow_clears_published_version_rows(redis):
    """删 flow（含 published，走 force 逃生）后版本行清零（issue RED ①的下半）。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)

    r = client.delete("/api/flows/cascade-flow?force=true", headers=_headers())
    assert r.status_code == 200, r.text
    engine = flow_store.get_init_engine()
    assert _version_count(engine) == 0


def test_start_explicit_on_deleted_flow_404(redis):
    """已删 flow 显式版本 start → 404 version_not_found（issue RED ②）。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)
    assert client.delete(
        "/api/flows/cascade-flow?force=true", headers=_headers()
    ).status_code == 200

    r = client.post("/api/executions",
                    json={"flow_id": "cascade-flow", "version": "1.0.0"},
                    headers=_headers())
    assert r.status_code == 404, r.text
    assert _detail_code(r) == "version_not_found"
    # 幽灵行不落：执行索引零新增
    assert _ghost_execution_rows(redis) == 0


def test_start_latest_on_deleted_flow_observable_degradation(redis):
    """已删 flow latest start：不再解析到孤儿 published（版本回声消失），
    且降级形态具名可观测（warning 点名「无 flow 记录」）。

    issue 验收原文要求 latest 404；实测与 #300/#304 的存量 latest
    fail-open 契约冲突（console 一无所知的 flow——CLI/worker 直部署面——
    latest start 同样 200 受理，39 个既有用例踩在该契约上），「已删」与
    「从未注册」在 DB 态不可区分，故本单不单方面改契约：降级可观测 +
    权威防线在删除级联（latest 不再命中孤儿 published）+ 显式 start 具名
    404。404 化需 owner 拍板（tombstone 或改直部署面语义）。
    """
    client = _client(redis)
    _seed_flow(client)
    _publish(client)
    assert client.delete(
        "/api/flows/cascade-flow?force=true", headers=_headers()
    ).status_code == 200

    r = client.post("/api/executions", json={"flow_id": "cascade-flow"},
                    headers=_headers())
    assert r.status_code == 200, r.text
    # 关键：删除级联修后，latest 不再解析到孤儿 published——响应无版本回声
    assert "version" not in r.json()
    warnings = r.json().get("warnings") or []
    assert any("无 flow 记录" in w for w in warnings), warnings


def test_rebuild_same_id_put_1_0_0_ok(redis):
    """同 id 重建后 PUT 1.0.0 正常（issue RED ④：孤儿 published 撞 409）。"""
    client = _client(redis)
    _seed_flow(client)
    _publish(client)
    assert client.delete(
        "/api/flows/cascade-flow?force=true", headers=_headers()
    ).status_code == 200

    # 同 id 重建：flow 行 + PUT 同版本号
    r = client.post("/api/flows", json={"flow_id": "cascade-flow"},
                    headers=_headers())
    assert r.status_code == 200, r.text
    r = client.put("/api/flows/cascade-flow/versions/1.0.0",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 200, r.text
    # 重建后的完整生命周期可走通：publish 不再撞残留 published
    _publish(client)


# ============ 待核项：force 逃生对孤儿形态（issue 正文留修方核验） ============

def test_force_escape_on_legacy_orphan_form(redis):
    """孤儿形态（存量库遗留）下 force 逃生路径核验（结论落注释）。

    手工重演 pre-fix 孤儿：父行被绕过 API 的裸 SQL 删掉（对 FK 未强制的
    老库等价），版本行残留；再按 issue 场景「同 id 重建 flow 后 PUT」。
    结论：
    - ``PUT 1.0.0`` → 409；``PUT force=true`` → **仍 409**（published 不可
      覆盖是无条件闸，force 只绕乐观锁 CAS）——A1「砖死」属实；
    - 唯一逃生 = ``DELETE /versions/1.0.0?force=true`` 清孤儿行 → PUT 200。
    """
    client = _client(redis)
    _seed_flow(client)
    _publish(client)
    engine = flow_store.get_init_engine()
    with engine.connect() as conn:
        # 本连接显式 FK OFF（重演存量孤儿；checkout PRAGMA 只在 checkout
        # 时拨 ON，此后同一连接上的显式 OFF 对本连接生效）
        conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
        conn.exec_driver_sql("DELETE FROM flows WHERE flow_id = 'cascade-flow'")
        conn.commit()
    assert _version_count(engine) == 1, "重演失败：孤儿版本行应残留"

    # 同 id 重建 flow 行（issue 场景前置），PUT 撞孤儿 published
    r = client.post("/api/flows", json={"flow_id": "cascade-flow"},
                    headers=_headers())
    assert r.status_code == 200, r.text
    r = client.put("/api/flows/cascade-flow/versions/1.0.0",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 409, r.text
    # force 不可越 published 不可覆盖闸（待核项结论：PUT 侧无逃生）
    r = client.put("/api/flows/cascade-flow/versions/1.0.0?force=true",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 409, r.text
    # 唯一逃生：先删孤儿版本行（force 可越状态守卫、不可越引用守卫——此处
    # 无执行引用）再重建
    r = client.delete("/api/flows/cascade-flow/versions/1.0.0?force=true",
                      headers=_headers())
    assert r.status_code == 200, r.text
    r = client.put("/api/flows/cascade-flow/versions/1.0.0",
                   json={"definition": FLOW_DEF}, headers=_headers())
    assert r.status_code == 200, r.text


# ============ 修法双层：连接层 PRAGMA（checkout 收口） ============

def test_sqlite_fk_enforced_per_checkout(db_url, monkeypatch):
    """console 引擎每次 checkout 都见 foreign_keys=ON；孤儿版本行插入被拒。

    池复用面：显式 OFF 后归还的连接，下一次 checkout 必须被拨回 ON
    （这是挂 checkout 而非 connect 的原因）。
    """
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    engine = flow_store.init_engine(db_url)

    # 第一次 checkout：FK ON
    with engine.connect() as conn:
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
    # 上一任显式 OFF 后归还……
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
    # ……下一次 checkout 必须回到 ON
    with engine.connect() as conn:
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        # 孤儿版本行（无 flows 父行）插入被 FK 拒绝
        with pytest.raises(IntegrityError):
            conn.execute(text(
                "INSERT INTO flow_versions (tenant_id, flow_id, version, status,"
                " definition, layout, created_at, created_by)"
                " VALUES ('default','ghost','1.0.0','draft','','',datetime('now'),'')"
            ))
            conn.commit()


def test_delete_flow_cascade_without_fk_pragma(tmp_path):
    """显式级联层独立成立：无 PRAGMA 的裸引擎上 delete_flow 仍清版本行。

    防「只靠 DB 声明」回归——PG/MySQL 或任何未拨 PRAGMA 的引擎同样清零。
    """
    engine = create_engine(f"sqlite:///{tmp_path / 'bare.db'}", future=True)
    flow_store.Base.metadata.create_all(engine)
    store = flow_store.FlowStore(sessionmaker(bind=engine, expire_on_commit=False))
    record = store.create_flow("bare-flow")
    store.save_flow_definition(flow_id="bare-flow", version="1.0.0",
                               definition=FLOW_DEF, tenant_id=record.tenant_id)
    assert _version_count(engine, "bare-flow") == 1

    assert store.delete_flow("bare-flow") is True
    assert _version_count(engine, "bare-flow") == 0


# ============ 迁移面：FK 强制与存量库迁移共存 ============

def test_alembic_upgrade_with_orphan_rows_succeeds(tmp_path, monkeypatch):
    """库停在 0003_flow_publish_outbox + 带孤儿版本行 → upgrade head 走通。

    运行期连接强制 FK 后，升级必须仍然可跑：env.py 对迁移会话显式
    ``PRAGMA foreign_keys=OFF``（钉语句级行为——移除即红）。这是对未来
    recreate 型 batch 迁移（表拷贝，如改列类型/改约束）的防御：届时老版本
    delete_flow 残留的孤儿行会在拷贝时撞 fk_flow_versions_flow。本路径的
    0003_flow_version_runtime 是 add_column（SQLite 原位 ALTER，不拷表），
    孤儿行原样共存、不被升级顺手删（清理是运维一次性动作）。
    """
    db_file = tmp_path / "upgrade.db"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    flow_store.Base.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
        # 退回 0003_flow_publish_outbox 形态：flow_versions 尚无 runtime 列
        conn.exec_driver_sql("ALTER TABLE flow_versions DROP COLUMN runtime")
        conn.execute(text(
            "INSERT INTO flows (tenant_id, flow_id, author, \"desc\","
            " created_at, updated_at)"
            " VALUES ('default', 'real-flow', '', '', datetime('now'), datetime('now'))"
        ))
        conn.execute(text(
            "INSERT INTO flow_versions (tenant_id, flow_id, version, status,"
            " definition, layout, created_at, created_by) VALUES"
            " ('default','real-flow','1.0.0','published','','',datetime('now'),''),"
            " ('default','ghost-flow','1.0.0','published','','',datetime('now'),'')"
        ))
        conn.execute(text(
            "CREATE TABLE alembic_version"
            " (version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
        ))
        conn.execute(text(
            "INSERT INTO alembic_version VALUES ('0003_flow_publish_outbox')"
        ))
        conn.commit()
    engine.dispose()

    from sqlalchemy import event as sa_event
    from sqlalchemy.engine import Engine as SaEngine

    # 迁移连接上的 FK 状态：env.py 经 DBAPI 直拨 OFF（不触发 SQLAlchemy
    # 语句事件），在首条 alembic_version 语句（必经迁移连接）执行时采样。
    # 采样须锚定迁移连接——init_engine 里 inspect 反射等会另开连接（checkout
    # 即 ON），不能当迁移会话的证据。
    state: dict = {"checked": False, "fk": None}

    def _spy(conn, cursor, stmt, params, many, ctx):
        if not state["checked"] and "alembic_version" in str(stmt):
            state["checked"] = True
            state["fk"] = conn.exec_driver_sql("PRAGMA foreign_keys").scalar()

    sa_event.listen(SaEngine, "before_cursor_execute", _spy)
    try:
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        engine2 = flow_store.init_engine(f"sqlite:///{db_file}")
    finally:
        sa_event.remove(SaEngine, "before_cursor_execute", _spy)
    assert engine2.schema_summary.get("action") == "upgrade"
    # 钉 env.py：迁移会话必须显式关闭 FK（移除该行即红——recreate 型 batch
    # 迁移会被存量孤儿行卡死）
    assert state["checked"] and state["fk"] == 0, (
        f"迁移会话 FK 未关闭（fk={state['fk']}）——recreate 型 batch 迁移"
        "会被存量孤儿行卡死（env.py 防御被移除？）"
    )
    with engine2.connect() as conn:
        cols = [r[1] for r in conn.exec_driver_sql(
            "PRAGMA table_info(flow_versions)"
        ).fetchall()]
        rows = conn.execute(text(
            "SELECT flow_id FROM flow_versions ORDER BY flow_id"
        )).fetchall()
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
    assert "runtime" in cols, "0003_flow_version_runtime 未落地"
    assert [r[0] for r in rows] == ["ghost-flow", "real-flow"], (
        f"孤儿行应原样共存、不被升级吞掉: {rows}"
    )


def test_legacy_multitenant_rebuild_with_fk_on(tmp_path, monkeypatch):
    """旧库（无 tenant_id 列）+ FK 强制连接：多租户重建照常，回填父先子后。

    flow_versions 必须先删（子）、flows 后删（父），回填反向父先子后——
    FK 强制下任何一侧倒序都会撞约束、启动即崩。
    """
    db_file = tmp_path / "legacy.db"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    # 旧形态：无 tenant_id、无 FK（历史 create_all 产物）；带存量数据。
    # 列集对齐历史 flow_versions（含 created_by——新表 NOT NULL 且无
    # server_default，回填缺它必炸）。
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE flows (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " flow_id VARCHAR(128) NOT NULL, author VARCHAR(128) NOT NULL,"
            " \"desc\" TEXT NOT NULL, created_at DATETIME, updated_at DATETIME)"
        ))
        conn.execute(text(
            "CREATE TABLE flow_versions (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " flow_id VARCHAR(128) NOT NULL, version VARCHAR(64) NOT NULL,"
            " status VARCHAR(16) NOT NULL, definition TEXT NOT NULL,"
            " layout TEXT NOT NULL, created_at DATETIME, published_at DATETIME,"
            " created_by VARCHAR(128) NOT NULL)"
        ))
        conn.execute(text(
            "INSERT INTO flows (flow_id, author, \"desc\", created_at, updated_at)"
            " VALUES ('legacy-flow', '', '', datetime('now'), datetime('now'))"
        ))
        conn.execute(text(
            "INSERT INTO flow_versions (flow_id, version, status, definition,"
            " layout, created_at, created_by)"
            " VALUES ('legacy-flow', '0.1.0', 'draft', '', '', datetime('now'), '')"
        ))
    engine.dispose()

    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    engine2 = flow_store.init_engine(f"sqlite:///{db_file}")
    with engine2.connect() as conn:
        flow_row = conn.execute(text(
            "SELECT tenant_id, flow_id FROM flows WHERE flow_id='legacy-flow'"
        )).fetchone()
        ver_rows = conn.execute(text(
            "SELECT tenant_id, flow_id, version FROM flow_versions"
        )).fetchall()
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
    assert flow_row == ("default", "legacy-flow"), f"flows 回填失败: {flow_row}"
    assert ver_rows == [("default", "legacy-flow", "0.1.0")], (
        f"flow_versions 回填失败: {ver_rows}"
    )


# ============ 存量语义锚：flow 在但无 published 的 latest 仍受理+警告 ============

def test_latest_start_flow_exists_without_published_keeps_200_warning(redis):
    """#308 不改存量契约的锚：flow 在但无 published 的 latest 仍受理+警告
    （#304 语义原样），且 warning 走「无已发布版本」分支而非「无记录」。"""
    client = _client(redis)
    _seed_flow(client)  # draft-only，未 publish

    r = client.post("/api/executions", json={"flow_id": "cascade-flow"},
                    headers=_headers())
    assert r.status_code == 200, r.text
    warnings = r.json().get("warnings") or []
    assert warnings, "flow 在但无 published 的 latest start 必须带警告"
    assert any("无已发布版本" in w for w in warnings), warnings
    assert not any("无 flow 记录" in w for w in warnings), warnings


def test_latest_start_unregistered_flow_keeps_fail_open_with_named_warning(redis):
    """console 一无所知的 flow（CLI/worker 直部署面）latest start：存量
    fail-open 契约原样（200 受理），降级以「无 flow 记录」具名可观测。"""
    client = _client(redis)

    r = client.post("/api/executions", json={"flow_id": "never-registered"},
                    headers=_headers())
    assert r.status_code == 200, r.text
    warnings = r.json().get("warnings") or []
    assert any("无 flow 记录" in w for w in warnings), warnings
