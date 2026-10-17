"""
C5-1 乐观并发控制单测：草稿保存 base_updated_at 比对 / 强制覆盖 / allocate_version 原子分配。

对应修复：services/flow_store.py（updated_at 列 + VersionConflictError + allocate_version）
与 api/flows.py（SaveVersionRequest 透传 + 结构化 409 + VersionView.updated_at）。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from models.flow import Base  # noqa: E402
from services import flow_store  # noqa: E402
from services.flow_store import FlowStore, VersionConflictError  # noqa: E402


def _echo_definition(flow_id: str = "echo", tag: str = "v1") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": tag,
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def store(tmp_path) -> FlowStore:
    db_file = tmp_path / "test_conc.db"
    engine = create_engine(f"sqlite:///{db_file}", future=True)
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine, expire_on_commit=False)
    return FlowStore(session_local)


# ---- 服务层：乐观锁 ----

def test_updated_at_exposed_and_bumps_on_save(store: FlowStore):
    res = store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    first = store.get_version("echo", "0.0.1")
    assert first is not None and first.updated_at is not None
    base = first.updated_at.isoformat()

    res2 = store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))
    second = store.get_version("echo", "0.0.1")
    assert second is not None and second.updated_at is not None
    assert second.updated_at.isoformat() != base  # 保存后基准必须前进
    assert json.loads(second.definition)["desc"] == "b"


def test_stale_base_conflicts(store: FlowStore):
    # 「标签页 A」写入后，「标签页 B」仍持旧基准保存 → 冲突
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    stale_base = store.get_version("echo", "0.0.1").updated_at.isoformat()
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))  # A 的更新

    with pytest.raises(VersionConflictError) as ei:
        store.save_flow_definition(
            "echo", "0.0.1", _echo_definition(tag="c"), base_updated_at=stale_base
        )
    latest = store.get_version("echo", "0.0.1").updated_at.isoformat()
    assert ei.value.latest_updated_at == latest  # 错误携带最新 updated_at
    # 内容未被 B 覆盖
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "b"


def test_matching_base_saves(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    base = store.get_version("echo", "0.0.1").updated_at.isoformat()
    store.save_flow_definition(
        "echo", "0.0.1", _echo_definition(tag="b"), base_updated_at=base
    )
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "b"


def test_force_overrides_conflict(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    stale_base = store.get_version("echo", "0.0.1").updated_at.isoformat()
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))

    store.save_flow_definition(
        "echo", "0.0.1", _echo_definition(tag="c"),
        base_updated_at=stale_base, force=True,
    )
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "c"


def test_no_base_keeps_legacy_behavior(store: FlowStore):
    # 未带 base_updated_at 的调用方（存量 API 消费者）行为不变
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == "b"


def test_published_still_blocked_before_conflict_check(store: FlowStore):
    store.save_flow_definition("echo", "1.0.0", _echo_definition())
    store.publish_version("echo", "1.0.0")
    with pytest.raises(ValueError, match="已发布"):
        store.save_flow_definition(
            "echo", "1.0.0", _echo_definition(tag="x"), base_updated_at="2000-01-01T00:00:00"
        )


def test_version_conflict_error_is_value_error(store: FlowStore):
    """VersionConflictError 兼容既有 ValueError → 409 映射。"""
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="a"))
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="b"))
    stale = "2000-01-01T00:00:00"
    with pytest.raises(ValueError):
        store.save_flow_definition("echo", "0.0.1", _echo_definition(), base_updated_at=stale)


# ---- 服务层：allocate_version（另存新版本不撞号）----

def test_allocate_version_assigns_next_patch(store: FlowStore):
    store.save_flow_definition("echo", "0.0.1", _echo_definition())
    store.save_flow_definition("echo", "0.0.2", _echo_definition())
    res = store.save_flow_definition(
        "echo", "0.0.2", _echo_definition(tag="fresh"), allocate_version=True
    )
    assert res.version == "0.0.3"  # 忽略调用方的 0.0.2，按库内最大分配
    assert json.loads(store.get_version("echo", "0.0.3").definition)["desc"] == "fresh"


def test_allocate_version_avoids_stale_collision(store: FlowStore):
    """双「标签页」：A 已创建 0.0.2，B 本地列表陈旧仍算 0.0.2 → 分配 0.0.3，不覆盖。"""
    store.save_flow_definition("echo", "0.0.1", _echo_definition())
    # A 另存 0.0.2（B 不知道）
    a = store.save_flow_definition(
        "echo", "0.0.2", _echo_definition(tag="from-A"), allocate_version=True
    )
    assert a.version == "0.0.2"
    # B 用陈旧列表也「另存 0.0.2」
    b = store.save_flow_definition(
        "echo", "0.0.2", _echo_definition(tag="from-B"), allocate_version=True
    )
    assert b.version == "0.0.3"
    # A 的内容不被 B 覆盖
    assert json.loads(store.get_version("echo", "0.0.2").definition)["desc"] == "from-A"
    assert json.loads(store.get_version("echo", "0.0.3").definition)["desc"] == "from-B"


def test_allocate_version_on_empty_flow(store: FlowStore):
    res = store.save_flow_definition("echo", "0.0.1", _echo_definition(), allocate_version=True)
    assert res.version == "0.0.1"


def test_next_semver_ignores_non_semver(store: FlowStore):
    assert FlowStore._next_semver_of([]) == "0.0.1"
    assert FlowStore._next_semver_of(["0.0.9", "0.1.0", "junk", ""]) == "0.1.1"
    assert FlowStore._next_semver_of(["1.2.3"]) == "1.2.4"


# ---- API 层：透传 + 结构化 409 ----

@pytest.fixture()
def api_client(tmp_path) -> TestClient:
    import api.flows as flows_api

    flow_store.init_engine(f"sqlite:///{tmp_path / 'api_conc.db'}")
    app = FastAPI()
    app.state.redis = None      # 本地单机模式：发布/删除跳过引擎同步
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


def _setup_ready_flow(client: TestClient):
    assert client.post("/api/flows", json={"flow_id": "echo"}).status_code == 200
    r = client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="a"), "layout": "{}"},
    )
    assert r.status_code == 200, r.text


def test_api_save_without_base_sets_updated_at(api_client: TestClient):
    _setup_ready_flow(api_client)
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="b"), "layout": "{}"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["updated_at"]  # VersionView 暴露 updated_at

    r2 = api_client.get("/api/flows/echo/versions/0.0.1")
    assert r2.json()["updated_at"] == body["updated_at"]


def test_api_stale_base_returns_structured_409(api_client: TestClient):
    _setup_ready_flow(api_client)
    stale = api_client.get("/api/flows/echo/versions/0.0.1").json()["updated_at"]
    # 他人更新
    api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="other"), "layout": "{}"},
    )
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={
            "definition": _echo_definition(tag="stale"),
            "layout": "{}",
            "base_updated_at": stale,
        },
    )
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["conflict"] == "version_stale"
    assert detail["latest_updated_at"]
    assert detail["message"]


def test_api_force_overrides(api_client: TestClient):
    _setup_ready_flow(api_client)
    stale = api_client.get("/api/flows/echo/versions/0.0.1").json()["updated_at"]
    api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="other"), "layout": "{}"},
    )
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={
            "definition": _echo_definition(tag="forced"),
            "layout": "{}",
            "base_updated_at": stale,
            "force": True,
        },
    )
    assert r.status_code == 200, r.text
    assert json.loads(r.json()["definition"])["desc"] == "forced"


def test_api_allocate_version_returns_actual(api_client: TestClient):
    _setup_ready_flow(api_client)
    r = api_client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_definition(tag="new"), "layout": "{}", "allocate_version": True},
    )
    assert r.status_code == 200, r.text
    assert r.json()["version"] == "0.0.2"


# ---- #101 守卫：并发保存的「读-比-写」必须原子（CAS 条件更新）----
#
# 缺陷（issue #101）：main 上保存既有草稿走「独立 SELECT 读 updated_at 比对 →
# 按主键的 ORM UPDATE 落库」三步，两步之间无行锁无条件更新。两个持同一
# base_updated_at 的并发保存会双双通过冲突检查，后写者静默覆盖先写者，
# 双方都拿到成功。守卫用两个线程 + engine 事件按「语句形状」强制复现
# read-compare-write 竞态窗口（对修复前后的实现都成立，不耦合实现细节）：
#
#   1. writer-A 发起落 definition 的 UPDATE（main 上是不带 updated_at 谓词的
#      主键定位写）之前，必须等 writer-B 完成它的冲突检查读；
#   2. writer-B 完成冲突检查读（SELECT updated_at FROM flow_versions）之后、
#      发起自己的任何写语句之前，必须等 writer-A 的整个保存结束。
#
# 即：双方都先读到旧基准，A 写完提交后 B 才落笔——main 上两者都成功（红），
# 修复后 B 的条件 UPDATE 撞 rowcount==0 → VersionConflictError（恰好一方成功）。

def _race_store(tmp_path):
    """独立引擎（NullPool：连接只在创建线程内使用，避免 pysqlite 跨线程报错）。"""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    db_file = tmp_path / "test_race101.db"
    engine = create_engine(
        f"sqlite:///{db_file}",
        future=True,
        poolclass=NullPool,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine, expire_on_commit=False)
    return FlowStore(session_local), engine


def _wait_or_die(event: "threading.Event", what: str) -> None:
    if not event.wait(timeout=15):
        raise RuntimeError(f"#101 竞态闸门超时：{what}")


def _run_same_base_race(store: FlowStore, engine) -> dict:
    """两线程持同一 base_updated_at 并发保存，返回 {tag: 结果}。"""
    import threading

    from sqlalchemy import event

    base = store.get_version("echo", "0.0.1").updated_at.isoformat()

    b_checked = threading.Event()  # writer-B 完成冲突检查读
    a_done = threading.Event()     # writer-A 整个保存结束

    def before_execute(conn, cursor, statement, parameters, context, executemany):
        name = threading.current_thread().name
        stmt = " ".join(statement.split()).lower()
        if not stmt.startswith("update"):
            return
        if name == "writer-A" and "updated_at" not in stmt:
            # main 上的落 definition 写法（主键定位、无 updated_at 谓词）：
            # 必须等 writer-B 先完成它的检查读，才能开始写
            _wait_or_die(b_checked, "writer-B 未完成冲突检查读")
        elif name == "writer-B":
            # writer-B 的任何写都必须等 writer-A 的保存完整结束。
            # 注意：只能在这里停等——B 此刻尚无任何未决游标/事务，不持锁；
            # 若停在 SELECT 的 after 钩子里，未消费的游标会持有语句级
            # SHARED 锁，把 A 的 COMMIT 伪造成 database is locked。
            _wait_or_die(a_done, "writer-A 未完成保存")

    def after_execute(conn, cursor, statement, parameters, context, executemany):
        name = threading.current_thread().name
        stmt = " ".join(statement.split()).lower()
        if name == "writer-B" and "select updated_at from flow_versions" in stmt:
            # 这一条就是「读基准做比对」——它已读到旧基准（A 尚未开写），
            # 只设旗标、不停留（停留会让未消费游标持锁，见上）
            b_checked.set()

    event.listen(engine, "before_cursor_execute", before_execute)
    event.listen(engine, "after_cursor_execute", after_execute)

    results: dict = {}

    def worker(tag: str) -> None:
        try:
            store.save_flow_definition(
                "echo", "0.0.1", _echo_definition(tag=tag), base_updated_at=base
            )
            results[tag] = "OK"
        except VersionConflictError as e:
            results[tag] = ("CONFLICT", e.latest_updated_at)
        except Exception as e:  # noqa: BLE001 — 其他异常原样暴露，不能伪装成功
            results[tag] = ("ERROR", repr(e))
        finally:
            if tag == "A":
                a_done.set()

    threads = [
        threading.Thread(target=worker, args=(tag,), name=f"writer-{tag}")
        for tag in ("A", "B")
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive(), f"线程 {t.name} 30s 未结束（疑似死锁）"
    return results


def test_concurrent_same_base_saves_exactly_one_wins(tmp_path):
    """#101 守卫：双标签页持同一 base 并发保存 → 恰好一方成功、另一方 409。"""
    store, engine = _race_store(tmp_path)
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="init"))
    results = _run_same_base_race(store, engine)

    oks = [t for t, r in results.items() if r == "OK"]
    conflicts = [t for t, r in results.items() if isinstance(r, tuple) and r[0] == "CONFLICT"]
    errors = [t for t, r in results.items() if isinstance(r, tuple) and r[0] == "ERROR"]
    assert not errors, f"并发保存出现意外异常: {results}"
    assert len(oks) == 1, f"应恰好一方成功，实际 {results}"
    assert len(conflicts) == 1, f"应恰好一方收到 VersionConflictError，实际 {results}"
    winner = oks[0]
    # 最终落库内容 = 成功一方的内容（失败方的编辑不得静默覆盖）
    assert json.loads(store.get_version("echo", "0.0.1").definition)["desc"] == winner
    # 冲突错误必须携带最新基准，供前端「加载最新」
    loser_latest = results[conflicts[0]][1]
    final_base = store.get_version("echo", "0.0.1").updated_at.isoformat()
    assert loser_latest == final_base, f"冲突错误应携带最新 updated_at: {loser_latest} != {final_base}"


def test_save_with_base_writes_definition_via_conditional_update(tmp_path):
    """#101 守卫：带 base 保存时，落 definition 的 UPDATE 必须含 updated_at 谓词
    （主键定位的无条件 ORM UPDATE 不得再出现——那正是丢编辑的竞态窗口）。"""
    from sqlalchemy import event

    store, engine = _race_store(tmp_path)
    store.save_flow_definition("echo", "0.0.1", _echo_definition(tag="init"))
    base = store.get_version("echo", "0.0.1").updated_at.isoformat()

    updates: list = []
    event.listen(
        engine, "before_cursor_execute",
        lambda conn, cursor, statement, parameters, context, executemany:
        updates.append(" ".join(statement.split()))
        if statement.lstrip().upper().startswith("UPDATE") else None,
    )
    store.save_flow_definition(
        "echo", "0.0.1", _echo_definition(tag="cas"), base_updated_at=base
    )
    assert updates, "保存应产生 UPDATE 语句"
    definition_writes = [u for u in updates if "definition" in u.lower()]
    assert definition_writes, f"应有落 definition 的 UPDATE: {updates}"
    for u in definition_writes:
        assert "updated_at" in u.lower(), (
            f"落 definition 的 UPDATE 缺少 updated_at 条件（读-比-写非原子）: {u}"
        )
