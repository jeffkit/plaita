"""#314 负控：A134-02 幸存者复核的 commit 前残窗——并发 import 排队到删除
提交之后落地孤儿版本行（B2 r92）。

缺陷（A1 r136 实弹，探针 ``/private/tmp/a136_probe3_survivor_toctou.py``，
复刻件 ``~/.issue-keeper/duty/b2-r92-scripts/``）：幸存者复核只兜「复核
时点已提交」；FK 关闭引擎（裸 engine/存量 SQLite）上，删除事务持写锁使
窗内 ``save_flow_definition`` 的新版本 INSERT **排队到删除提交之后**才
落地——保存侧一切更早的存在性检查都发生在排队前，孤儿版本行
（flows 已删、flow_versions 残留）直接落库，「零删除或全删除」契约破。

修法（本仓 fix）：
- 保存侧父行锚点写（``_assert_flow_row_alive``，零变异 UPDATE）：INSERT
  取得的写锁内复核父行，消失即 409 整体回滚——孤儿不可能落库；
- FK 强制引擎（console init_engine / PG / MySQL）上 INSERT 具名失败为
  IntegrityError，翻译成 409（不再被重试环误报成「版本已存在」）；
- 删除侧幸存者复核在 ``_outbox_purge`` 后、commit 前二次闸（封 MVCC 缝）。

判绿口径=并发 import × delete_flow 最坏时序终态二者其一：import 具名
失败或删除具名 409；**不得出现 flows 已删而版本行残留**。先红后绿证据：
docs/proofs/r92-a134-02-residual-window.md。
"""
import json
import sys
import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import FlowRecord, FlowVersion  # noqa: E402
from services import flow_store  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402

TENANT = "default"
FLOW = "r314-flow"
DEF = json.dumps({"nodes": []})


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = "r314-admin"
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis(tmp_path, monkeypatch):
    """console 形态引擎：init_engine（SQLite 每次 checkout 强制 FK）。"""
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path / 'issue314.db'}")
    return None


def _raw_store(tmp_path) -> FlowStore:
    """裸引擎形态：metadata.create_all、无 checkout PRAGMA——FK 声明在、
    强制关（SQLite 默认部署/脚本/探针现实，#314 孤儿面所在）。"""
    engine = create_engine(
        f"sqlite:///{tmp_path / 'raw.db'}",
        connect_args={"timeout": 15, "check_same_thread": False},
    )
    FlowRecord.__table__.metadata.create_all(engine)
    return FlowStore(sessionmaker(bind=engine, expire_on_commit=False))


def _arm_window_purge(monkeypatch, entered: threading.Event) -> None:
    """注入缝与 A1 探针同款：delete_flow 事务内、幸存者复核与 commit 之间
    （_outbox_purge）停表，让并发 import 的写排队到删除提交之后。"""
    orig = flow_store._outbox_purge

    def sleepy(session, **kw):
        out = orig(session, **kw)
        entered.set()
        time.sleep(1.2)
        return out

    monkeypatch.setattr(flow_store, "_outbox_purge", sleepy)


def _run_race(store: FlowStore, monkeypatch) -> dict:
    """delete_flow(expected_versions) × save_flow_definition(新版本) 最坏
    时序；返回 {"delete": ..., "import": ...} 文本结果。"""
    store.create_flow(FLOW, tenant_id=TENANT)
    store.save_flow_definition(flow_id=FLOW, version="1.0.0",
                               definition=DEF, tenant_id=TENANT)
    entered = threading.Event()
    _arm_window_purge(monkeypatch, entered)
    result: dict = {}

    def deleter():
        try:
            store.delete_flow(FLOW, tenant_id=TENANT,
                              expected_versions={"1.0.0": "draft"})
            result["delete"] = "committed"
        except Exception as e:  # noqa: BLE001 — 探针形制，文本入断言
            result["delete"] = f"{type(e).__name__}: {e}"

    def importer():
        entered.wait(timeout=10)
        time.sleep(0.1)
        try:
            store.save_flow_definition(flow_id=FLOW, version="2.0.0",
                                       definition=DEF, tenant_id=TENANT)
            result["import"] = "saved"
        except Exception as e:  # noqa: BLE001
            result["import"] = f"{type(e).__name__}: {e}"

    threads = [threading.Thread(target=deleter), threading.Thread(target=importer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads), "竞态线程未在超时内结束"
    return result


def _rows(store: FlowStore):
    with store._session_local() as s:
        parent = s.scalars(select(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == FLOW)).first()
        versions = s.scalars(select(FlowVersion).where(
            FlowVersion.tenant_id == TENANT, FlowVersion.flow_id == FLOW)).all()
        return parent, [v.version for v in versions]


# ============ 端到端：最坏时序终态 ============


def test_race_fk_engine_import_named_409_no_orphan(redis, monkeypatch):
    """console 形态（FK 强制）：窗内 import 排到删除提交后 → INSERT 撞 FK
    → 翻译成 409 具名冲突（不再是「版本已存在」误报）；零孤儿。"""
    store = flow_store.get_flow_store()
    result = _run_race(store, monkeypatch)
    assert result["delete"] == "committed", result
    assert result["import"].startswith("VersionConflictError"), result
    assert "并发删除" in result["import"], result
    parent, versions = _rows(store)
    assert parent is None and versions == []


def test_race_fk_off_engine_anchor_rejects_no_orphan(tmp_path, monkeypatch):
    """裸引擎形态（FK 关）：窗内 INSERT 排到删除提交后落地 → 父行锚点写
    rowcount=0 → 409 整体回滚（版本行随事务消失）；零孤儿。"""
    store = _raw_store(tmp_path)
    result = _run_race(store, monkeypatch)
    assert result["delete"] == "committed", result
    assert result["import"].startswith("VersionConflictError"), result
    assert "并发删除" in result["import"], result
    parent, versions = _rows(store)
    assert parent is None and versions == []


# ============ 具名面：FK IntegrityError 不再误报「已存在」 ============


def test_fk_violation_translated_not_misleading_dup(redis):
    """FK 强制引擎上父行先亡：INSERT 命名 IntegrityError 若不翻译会穿
    save_flow_definition 重试环变成 ValueError「版本已存在」——直调
    _save_flow_once 钉死翻译行为。"""
    store = flow_store.get_flow_store()
    store.create_flow(FLOW, tenant_id=TENANT)
    with store._session_local() as s:
        s.execute(delete(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == FLOW))
        s.commit()
    with pytest.raises(flow_store.VersionConflictError) as ei:
        store._save_flow_once(
            flow_id=FLOW, version="9.9.9", definition=DEF, layout="",
            status="draft", created_by="", tenant_id=TENANT,
            base_updated_at=None, force=False, allocate_version=False,
        )
    assert "并发删除" in str(ei.value)


# ============ 锚点门直证：raise → 回滚原子成对；正控零副作用 ============


def test_anchor_gate_rolls_back_version_row_with_parent_gone(tmp_path):
    store = _raw_store(tmp_path)
    store.create_flow(FLOW, tenant_id=TENANT)
    with store._session_local() as s:
        s.execute(delete(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == FLOW))
        s.add(FlowVersion(tenant_id=TENANT, flow_id=FLOW, version="2.0.0",
                          status="draft", definition=DEF))
        s.flush()
        with pytest.raises(flow_store.VersionConflictError):
            store._assert_flow_row_alive(s, flow_id=FLOW, tenant_id=TENANT)
        s.rollback()
    parent, versions = _rows(store)
    assert parent is not None, "误回滚：父行不应随锚点 409 消失"
    assert versions == [], "孤儿面：版本行必须随锚点 409 回滚"


def test_anchor_gate_pass_clean_and_zero_mutation(tmp_path):
    """正控：父行健在时锚点静默通过，且零变异（author/updated_at 不动）。"""
    store = _raw_store(tmp_path)
    store.create_flow(FLOW, tenant_id=TENANT)
    with store._session_local() as s:
        rec = s.scalars(select(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == FLOW)).first()
        before = (rec.author, rec.updated_at)
        store._assert_flow_row_alive(s, flow_id=FLOW, tenant_id=TENANT)
        s.rollback()
    with store._session_local() as s:
        rec = s.scalars(select(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == FLOW)).first()
        assert (rec.author, rec.updated_at) == before
