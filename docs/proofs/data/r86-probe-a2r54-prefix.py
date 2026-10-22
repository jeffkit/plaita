#!/usr/bin/env python3
"""A2R54 探针：flows 版本 API 的 publish×save 竞窗（基线 origin/main d3d3c12 clean archive）。

候选1（A2R54-01）：publish_flow 的 #300 能力门读 get_version 快照（flows.py:701→707→709），
publish_version 在新会话重 SELECT——两步之间落进的 CAS 草稿保存使「门校验对象≠发布对象」。
候选2（A2R54-02）：_save_flow_once 的既有行分支「SELECT 状态检查（draft）→ CAS UPDATE」两语句
之间夹进 publish 提交：CAS 谓词（WHERE updated_at=:base）不含 status，且 publish_version
不前进 updated_at（ORM 无此列，仅保存路径 touch）⇒ 已发布行被静默打回 draft+定义覆盖。

判红口径=断言「正确行为」失败即红。探针为 faithful 语句序复刻（真实代码路径的两语句由
两个会话按真实顺序执行），非行为模拟。
"""
import os, sys, textwrap

sys.path.insert(0, '/private/tmp/plaita-b3-r86/plaita-console/backend')
sys.path.insert(0, '/private/tmp/plaita-b3-r86')

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from services.flow_store import FlowStore  # noqa: E402
from models.flow import Base, FlowVersion  # noqa: E402

DB = '/tmp/a2r54_probe.db'
T, F, V = 't1', 'f1', '0.0.1'
results = []

def fresh_store(with_updated_at: bool):
    if os.path.exists(DB):
        os.remove(DB)
    engine = create_engine('sqlite:///' + DB)
    Base.metadata.create_all(engine)
    # updated_at 列由 services.flow_store import 时的 compat 层补进 ORM 元数据
    # （flow_store.py:77-78 append_column），create_all 即带列，无需 ALTER。
    store = FlowStore(sessionmaker(bind=engine))
    if not with_updated_at:
        store._updated_at_ok = False  # 强制走 legacy 降级分支（列不可用口径）
    return store

def seed_draft(store, definition):
    store.ensure_flow(F, tenant_id=T)
    store.save_flow_definition(flow_id=F, version=V, definition=definition,
                               tenant_id=T, created_by='probe')

def raw_row(store):
    with store._session_local() as s:
        return s.scalars(select(FlowVersion).where(
            FlowVersion.tenant_id == T, FlowVersion.flow_id == F,
            FlowVersion.version == V)).first()

# ---------- 候选1：能力门快照 × publish_version 重 SELECT ----------
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
gate_snapshot = store.get_version(F, V, tenant_id=T)          # flows.py:701 门输入=D1
# 竞窗内并发草稿保存（合法 CAS：base 恰是最新 updated_at）
store.save_flow_definition(flow_id=F, version=V, definition='D2-UNGATED', tenant_id=T,
                           base_updated_at=gate_snapshot.updated_at, created_by='racer')
out = store.publish_version(F, V, tenant_id=T)                # flows.py:709 重 SELECT
red1 = out.status == 'published' and out.definition == 'D2-UNGATED'
results.append(('A2R54-01 门快照TOCTOU：未过门的 D2 被发布', red1,
                f'published={out.status} definition={out.definition}'))

# 对照绿1：无并发保存时门对象==发布对象
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
snap = store.get_version(F, V, tenant_id=T)
out = store.publish_version(F, V, tenant_id=T)
green1 = out.status == 'published' and out.definition == 'D1'
results.append(('对照绿1 无竞窗：门对象==发布对象', green1,
                f'published={out.status} definition={out.definition}'))

# ---------- 候选2：SELECT(draft)→publish commit→CAS UPDATE 三明治（真 CAS 路径）----------
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
base = store.get_version(F, V, tenant_id=T).updated_at
with store._session_local() as s1:
    existing = s1.scalars(select(FlowVersion).where(          # _save_flow_once 状态检查读
        FlowVersion.tenant_id == T, FlowVersion.flow_id == F,
        FlowVersion.version == V)).first()
    assert existing.status == 'draft', '前置：读时为 draft'
    store.publish_version(F, V, tenant_id=T)                  # 并发发布在此提交
    ok = store._cas_update_draft(s1, flow_id=F, version=V, tenant_id=T,  # CAS UPDATE
                                 definition='D2', layout='', status='draft',
                                 base_updated_at=base, force=False, runtime=None)
    s1.commit()
row = raw_row(store)
red2 = bool(ok) and row.status == 'draft' and row.definition == 'D2' and row.published_at is not None
results.append(('A2R54-02 CAS状态盲写：已发布行被打回draft+改定义', red2,
                f'cas_ok={ok} status={row.status} definition={row.definition} '
                f'published_at残留={row.published_at is not None}'))

# 对照绿2：无窗口时既有 published 检查拦截（真实代码语义）
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
store.publish_version(F, V, tenant_id=T)
blocked = False
try:
    store.save_flow_definition(flow_id=F, version=V, definition='D2', tenant_id=T,
                               created_by='late-saver')
except ValueError as e:
    blocked = '已发布' in str(e)
row = raw_row(store)
green2 = blocked and row.status == 'published' and row.definition == 'D1'
results.append(('对照绿2 无窗口：published 检查拦截保存', green2,
                f'blocked={blocked} status={row.status} definition={row.definition}'))

# ---------- 佐证格：updated_at 列缺失（legacy 降级）同样打回 ----------
store = fresh_store(with_updated_at=False)
seed_draft(store, 'D1')
with store._session_local() as s1:
    existing = s1.scalars(select(FlowVersion).where(
        FlowVersion.tenant_id == T, FlowVersion.flow_id == F,
        FlowVersion.version == V)).first()
    assert existing.status == 'draft'
    store.publish_version(F, V, tenant_id=T)
    ok = store._cas_update_draft(s1, flow_id=F, version=V, tenant_id=T,
                                 definition='D2', layout='', status='draft',
                                 base_updated_at=None, force=False, runtime=None)
    s1.commit()
row = raw_row(store)
proof = bool(ok) and row.status == 'draft' and row.definition == 'D2'
results.append(('佐证 legacy降级(列缺)：同样打回draft', proof,
                f'cas_ok={ok} status={row.status} definition={row.definition}'))

print(textwrap.dedent("""
=== A2R54 probe output ==="""))
for name, hit, detail in results:
    print(f"[{'RED' if name.startswith('A2R54') else 'GREEN'}-{'HIT' if hit else 'MISS'}] {name} | {detail}")
verdict_red = sum(1 for n, h, _ in results if n.startswith('A2R54') and h)
verdict_green = sum(1 for n, h, _ in results if n.startswith('对照') and h)
print(f"TOTAL: {verdict_red} red / {verdict_green + (1 if results[-1][1] else 0)} green/proof")
sys.exit(0 if verdict_red >= 2 else 1)
