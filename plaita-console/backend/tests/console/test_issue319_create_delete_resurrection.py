"""#319 负控：create_flow × delete_flow 同 PK 复生/吞没竞态（#314 残面）。

缺陷（B2 r92 残面注记：PG MVCC 下删除可吞掉刚重建的同名 flow）：
``delete_flow`` 的父行收尾删除用 ``session.delete(record)``——ORM DELETE 只按
代理主键 id 定位，且实际执行在 commit 前 flush（``_outbox_purge`` 之后）。
SELECT 观察到提交之间，旧行可被并发删除并由同 id create 重建：SQLite rowid
复用使重建行 id 与观察行相同 → 旧删除的收尾 DELETE **命中重建行**
（rowcount=1，静默「成功」，create 侧已拿到 201）；PG 新 id → 零命中仅
WARNING、假报成功。两面即「删除吞掉刚重建的同名 flow」——create 已成功而
其行/版本被并发旧删除吃掉，终态「create 201 + delete 200 + 行消失」。
``expected_versions`` 钉版路径（A134-02）挡版本行漂移，但版本同名同状态
（重建期 save 同 version/status）可穿过钉版 DELETE，父行收尾仍无任何钉。

修法（本仓 fix）：父行删除改钉观察快照（``id + created_at`` 双条件 DELETE +
rowcount 复核，``created_at`` 不可变且重建必换值；PG/MySQL 上 id 序列不复用
同样使 rowcount=0）→ 失配即 VersionConflictError（409 语义），事务整体回滚
（本事务已执行版本行/outbox purge 一并还原）——与 ``_delete_versions_pinned``
「快照失配 → 具名 409、零删除」同语义收敛。

判绿口径：create×delete 任何交错终态二者其一——create 胜（行+版本=新生命
周期一致态）或 delete 具名 409（零删除、create 产物完好）；不得出现
「create 成功而其行/版本被吃」。先红后绿证据：docs/proofs/r92-i319-*.md。

时序确定性（无竞态赌运气）：``delete()`` 构造点停表（SELECT 已完成、任何写
锁未取）→ delete#1 提交旧行死亡 → create 重建提交（SQLite rowid 复用同 id）
→ 放行陈旧删除收尾。
"""
import json
import sys
import threading
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.flow import FlowRecord, FlowVersion  # noqa: E402
from services import flow_store  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402

TENANT = "default"
FLOW = "r319-flow"
DEF = json.dumps({"nodes": []})


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = "r319-admin"
    s.allow_insecure_admin = False
    return s


def _make_store(form: str, tmp_path, monkeypatch) -> FlowStore:
    """console 形态（init_engine，FK 强制）× 裸形态（raw engine，FK 关）。"""
    if form == "console":
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        flow_store.init_engine(f"sqlite:///{tmp_path / 'issue319.db'}")
        return flow_store.get_flow_store()
    engine = create_engine(
        f"sqlite:///{tmp_path / 'raw.db'}",
        connect_args={"timeout": 15, "check_same_thread": False},
    )
    FlowRecord.__table__.metadata.create_all(engine)
    return FlowStore(sessionmaker(bind=engine, expire_on_commit=False))


def _arm_first_delete_freeze(monkeypatch):
    """在 flow_store 模块的首次 ``delete()`` 构造点停表阻塞——此时陈旧删除
    已完成 SELECT 观察（autocommit 读，锁即取即放）、任何写锁未取；主线程
    据此在窗口内完成 delete#1 + create 重建，经 release 放行收尾。返回
    ``(entered, release)`` 两个事件。"""
    entered = threading.Event()
    release = threading.Event()
    orig_delete = flow_store.delete
    state = {"fired": False}

    def freezing_delete(*args, **kwargs):
        if not state["fired"]:
            state["fired"] = True
            entered.set()
            assert release.wait(timeout=10), "主线程未在超时内完成窗口注入"
        return orig_delete(*args, **kwargs)

    monkeypatch.setattr(flow_store, "delete", freezing_delete)
    return entered, release


def _rows(store: FlowStore):
    with store._session_local() as s:
        parent = s.scalars(select(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == FLOW)).first()
        versions = [v.version for v in s.scalars(select(FlowVersion).where(
            FlowVersion.tenant_id == TENANT, FlowVersion.flow_id == FLOW)).all()]
        return parent, versions


def _run_interleave(store: FlowStore, monkeypatch, *, pinned: bool,
                    rebuild_with_version: bool) -> dict:
    """确定性三段交错：setup 旧生命周期 → 陈旧删除停表 → delete#1 + create
    重建（+可选同名同状态版本）→ 放行收尾。返回各步结果文本。"""
    store.create_flow(FLOW, tenant_id=TENANT)
    store.save_flow_definition(flow_id=FLOW, version="1.0.0",
                               definition=DEF, tenant_id=TENANT)

    entered, release = _arm_first_delete_freeze(monkeypatch)
    result: dict = {}

    def stale_deleter():
        try:
            if pinned:
                ok = store.delete_flow(FLOW, tenant_id=TENANT,
                                       expected_versions={"1.0.0": "draft"})
            else:
                ok = store.delete_flow(FLOW, tenant_id=TENANT)
            result["stale_delete"] = "committed" if ok else "false"
        except Exception as e:  # noqa: BLE001 — 探针形制，文本入断言
            result["stale_delete"] = f"{type(e).__name__}: {e}"

    deleter = threading.Thread(target=stale_deleter)
    deleter.start()
    assert entered.wait(timeout=10), "陈旧删除未到达停表点"

    # delete#1：另一会话完整删除旧行（陈旧删除此刻停在构造点、不持任何锁，
    # 可干净提交）。
    store.delete_flow(FLOW, tenant_id=TENANT)
    # create 重建：pre-check 看到已提交的「不存在」→ 新生命周期落库
    # （SQLite rowid 复用：重建行 id 与观察行相同——收尾 DELETE 的命中面）。
    store.create_flow(FLOW, tenant_id=TENANT)
    result["create"] = "created"
    if rebuild_with_version:
        store.save_flow_definition(flow_id=FLOW, version="1.0.0",
                                   definition=DEF, tenant_id=TENANT)
        result["rebuild_save"] = "saved"

    release.set()
    deleter.join(timeout=30)
    assert not deleter.is_alive(), "陈旧删除线程未在超时内结束"
    return result


# ============ None 路径（存量直调方，无条件级联）：父行收尾吃掉重建行 ============


@pytest.mark.parametrize("form", ["console", "raw"])
def test_none_path_delete_eats_rebuilt_row(form, tmp_path, monkeypatch):
    """陈旧删除（expected_versions=None）收尾 DELETE 按 id 命中重建行 →
    「create 201 + delete 200 + 行消失」。修后：删除具名 409、重建行完好。"""
    store = _make_store(form, tmp_path, monkeypatch)
    result = _run_interleave(store, monkeypatch, pinned=False,
                             rebuild_with_version=False)
    assert result["create"] == "created", result
    # 修复判据：陈旧删除不得假报成功，重建行必须存活。
    assert result["stale_delete"].startswith("VersionConflictError"), result
    assert "并发" in result["stale_delete"], result
    parent, versions = _rows(store)
    assert parent is not None, "复生/吞没形态：create 成功而其行被吃"
    assert versions == []


# ============ 钉版路径：同名同状态版本穿过钉版 DELETE，父行收尾仍吃重建 ============


@pytest.mark.parametrize("form", ["console", "raw"])
def test_pinned_path_delete_eats_rebuilt_row_and_version(form, tmp_path,
                                                         monkeypatch):
    """重建期 save 同 version/status（1.0.0/draft）穿过 expected_versions 钉版
    DELETE（rowcount=1），父行收尾 DELETE 按 id 命中重建行 → 行+版本双吃。
    修后：父行钉版失配 409，事务整体回滚、版本行随回滚还原。"""
    store = _make_store(form, tmp_path, monkeypatch)
    result = _run_interleave(store, monkeypatch, pinned=True,
                             rebuild_with_version=True)
    assert result["create"] == "created" and result["rebuild_save"] == "saved", result
    assert result["stale_delete"].startswith("VersionConflictError"), result
    parent, versions = _rows(store)
    assert parent is not None, "吞没形态：重建行被陈旧删除收尾吃掉"
    assert versions == ["1.0.0"], "吞没形态：重建版本未随 409 回滚还原"


# ============ 正控：无重建的正常删除不受钉版影响 ============


@pytest.mark.parametrize("form", ["console", "raw"])
def test_normal_delete_unaffected(form, tmp_path, monkeypatch):
    store = _make_store(form, tmp_path, monkeypatch)
    store.create_flow(FLOW, tenant_id=TENANT)
    store.save_flow_definition(flow_id=FLOW, version="1.0.0",
                               definition=DEF, tenant_id=TENANT)
    assert store.delete_flow(FLOW, tenant_id=TENANT,
                             expected_versions={"1.0.0": "draft"}) is True
    parent, versions = _rows(store)
    assert parent is None and versions == []
