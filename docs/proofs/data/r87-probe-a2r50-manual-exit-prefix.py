"""A2R50-02 修前基线探针（r56 归档剧本换根复跑，ARCHIVE=本 r87 worktree 修前树）。

剧本出处：~/.issue-keeper/duty/a2-r56-scripts/test_a2r50_axis23_outbox_schedule-r56.py
（A2 r50 原探针，r56 核销轮换根复跑两红格维持）。本副本只保留与本单相关的
R50-02a 三格（resolve/discard×删除竞窗 + 幽灵链），R50-04（调度陈旧章，
A2R50-03 另单）不在本单范围。

确定性重放手法（与剧本一致）：monkeypatch ``_get_scoped_row`` 在返回前插入
mark_deleting（独立事务已提交）——= 真实竞窗中人工出口「读后、写前」被
删除协议插队，行对象保持陈旧读。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

ARCHIVE = "/private/tmp/plaita-b3-r87"
BACKEND = f"{ARCHIVE}/plaita-console/backend"
for p in (ARCHIVE, BACKEND):
    if p not in sys.path:
        sys.path.insert(0, p)

from models.flow import FlowPublishOutbox  # noqa: E402
from services import engine_outbox  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402

TARGET_URL = "redis://target-env:6379/0"


def _demo_definition(flow_id: str = "demo") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": "a2r50",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name",
                 "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def env_enabled(monkeypatch):
    monkeypatch.setenv(engine_outbox.TARGET_URL_ENV, TARGET_URL)
    yield


@pytest.fixture()
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'a2r50-outbox.db'}",
                           future=True)
    from models.flow import Base

    Base.metadata.create_all(engine)
    return FlowStore(sessionmaker(bind=engine, expire_on_commit=False))


@pytest.fixture()
def sessions(store: FlowStore):
    return store._session_local


def _publish_version(store: FlowStore, flow_id: str = "demo",
                     version: str = "1.0.0", tenant_id: str = ""):
    store.ensure_flow(flow_id, tenant_id=tenant_id)
    store.save_flow_definition(flow_id, version, _demo_definition(flow_id),
                               "{}", tenant_id=tenant_id)
    return store.publish_version(flow_id, version, tenant_id=tenant_id or None)


def _row_status(sessions, row_id):
    with sessions() as s:
        row = s.get(FlowPublishOutbox, row_id)
        return None if row is None else row.status


# ============ R50-02a：discard/resolve × deleting 竞窗（确定性重放） ============

@pytest.mark.parametrize("action", ["discard", "resolve"])
def test_r50_02a_manual_exit_race_overwrites_tombstone(
        store, sessions, env_enabled, monkeypatch, action):
    _publish_version(store)
    with sessions() as s:
        row = s.scalars(select(FlowPublishOutbox)).first()
        row_id = row.id
    # 行进 DLQ（failed）——人工出口的合法前置态
    with sessions() as s:
        r = s.get(FlowPublishOutbox, row_id)
        r.status = engine_outbox.STATUS_FAILED
        s.commit()

    # 竞窗注入：resolve/discard 的「读」返回后、状态检查/写之前，
    # 删除协议插队跑完 mark_deleting（独立事务已提交）——真实线程竞窗中
    # 人工出口的读发生在 mark 之前，此处以确定性顺序钉住同一交错。
    orig_get = engine_outbox._get_scoped_row

    def racing_get(session, rid, tenant_id):
        row_obj = orig_get(session, rid, tenant_id)
        if row_obj is not None and row_obj.id == row_id:
            engine_outbox.mark_deleting(sessions, tenant_id="",
                                        flow_id="demo")
        return row_obj

    monkeypatch.setattr(engine_outbox, "_get_scoped_row", racing_get)

    fn = engine_outbox.discard_row if action == "discard" \
        else engine_outbox.resolve_row
    # 期望（正确行为）：读到陈旧 failed 后应在锁内重读发现 deleting → 409；
    # 实际（红）：直接以陈旧态放行，tombstone 被覆盖（resolve）/行被删（discard）
    brief = fn(sessions, row_id, tenant_id="", actor="a2r50")
    print(f"\n[R50-02a/{action}] 人工出口在竞窗内返回：status={brief['status']} "
          f"(应被 deleting 闸拒绝)")
    assert False, (
        f"{action} 竞窗放行（返回 {brief['status']}）——tombstone check-then-act "
        f"无锁实锤"
    )


def test_r50_02a_ghost_after_failed_delete(store, sessions, env_enabled,
                                           monkeypatch):
    """完整危害链：discard 竞窗删行 → 删除事务失败 restore → 幽灵不可自愈。"""
    _publish_version(store)
    with sessions() as s:
        row = s.scalars(select(FlowPublishOutbox)).first()
        row_id = row.id
    with sessions() as s:
        r = s.get(FlowPublishOutbox, row_id)
        r.status = engine_outbox.STATUS_FAILED
        s.commit()

    orig_get = engine_outbox._get_scoped_row

    def racing_get(session, rid, tenant_id):
        row_obj = orig_get(session, rid, tenant_id)
        if row_obj is not None and row_obj.id == row_id:
            engine_outbox.mark_deleting(sessions, tenant_id="",
                                        flow_id="demo")
        return row_obj

    monkeypatch.setattr(engine_outbox, "_get_scoped_row", racing_get)
    brief = engine_outbox.discard_row(sessions, row_id, tenant_id="",
                                      actor="a2r50")
    print(f"\n[R50-02a] 竞窗 discard 结果：{brief and brief['status']}")
    assert brief is not None, "discard 被闸拒绝（竞窗不存在）"

    # 删除协议继续：清目标 → 删除事务失败 → restore_deleting
    snapshot = [(row_id, engine_outbox.STATUS_FAILED)]
    restored = engine_outbox.restore_deleting(sessions, snapshot,
                                              tenant_id="", flow_id="demo")
    print(f"[R50-02a] restore_deleting 恢复 {restored} 行"
          f"（行已删，published 版本的投递行永久消失）")
    assert restored == 0
    assert _row_status(sessions, row_id) is None, "行已被 discard 删除"
    # 源表仍在且已发布——但无 outbox 行：投递扫不到（pending/failed）、
    # 对账扫不到（delivered）→ 目标 key 一旦清理即幽灵，且重复 publish
    # 幂等早退不重建行
    ver = store.get_version("demo", "1.0.0", tenant_id="")
    assert ver is not None and ver.status == "published"
    with sessions() as s:
        due = s.scalars(select(FlowPublishOutbox)).all()
    assert due == [], "无任何行可供投递/对账自愈——幽灵实锤"
