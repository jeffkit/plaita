#!/usr/bin/env python3
"""r86 修后负控：A2R54-01/02（flows 版本发布面）修复验证——B3 第八十六轮。

修法（分支 fix/a2r54-publish-gate）：
- 修① A2R54-02：publish_version 状态翻转改单条条件 UPDATE 并**前进 updated_at**
  （谓词 status='draft' + updated_at 快照钉扎）；_cas_update_draft 谓词补
  ``AND status='draft'``。
- 修② A2R54-01：publish_version 增 ``gate_fn``——发布事务内对重 SELECT 的
  fresh 行 re-gate（校验对象=落盘对象）；API publish/import 两处注入
  _gate_runtime_capability 包装。

判绿口径=断言「正确行为」成立即绿。G2 的竞窗落在 gate_fn 回调内（faithful
复刻「门校验期间的并发保存」，含 Redis 往返窗口）；G3/G4 沿用 A2 r54/r56
探针的两会话语句序复刻。探针零真实 Redis/网络（sqlite tmp + 内存桩）。
"""
import os, sys, textwrap

sys.path.insert(0, '/private/tmp/plaita-b3-r86/plaita-console/backend')
sys.path.insert(0, '/private/tmp/plaita-b3-r86')

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from services.flow_store import FlowStore, VersionConflictError  # noqa: E402
from services.capability_gate import UnsupportedNodeTypesError  # noqa: E402
from models.flow import Base, FlowVersion  # noqa: E402

DB = '/tmp/a2r86_probe.db'
T, F, V = 't1', 'f1', '0.0.1'
results = []

def fresh_store(with_updated_at: bool):
    if os.path.exists(DB):
        os.remove(DB)
    for suffix in ('-wal', '-shm'):
        p = DB + suffix
        if os.path.exists(p):
            os.remove(p)
    engine = create_engine('sqlite:///' + DB)
    Base.metadata.create_all(engine)
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

def gate_reject_ungated(fresh):
    """#300 门桩：定义含 UNGATED 视为含不支持节点，结构化拒绝（同真实门语义）。"""
    if 'UNGATED' in (fresh.definition or ''):
        raise UnsupportedNodeTypesError({
            'error': 'unsupported_node_types',
            'runtime': getattr(fresh, 'runtime', None) or 'python',
            'unsupported': ['proxy'], 'supported': [],
        })

# ---------- G1 修②：re-gate 拒未过门 fresh 行（门快照 D1→竞窗落 D2→发布拒绝） ----------
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
gate_snapshot = store.get_version(F, V, tenant_id=T)          # API 层门快照=D1
store.save_flow_definition(flow_id=F, version=V, definition='D2-UNGATED', tenant_id=T,
                           base_updated_at=gate_snapshot.updated_at.isoformat(),
                           created_by='racer')                 # 竞窗内合法 CAS 草稿保存
raised = None
try:
    store.publish_version(F, V, tenant_id=T, gate_fn=gate_reject_ungated)
except UnsupportedNodeTypesError as e:
    raised = e
row = raw_row(store)
g1 = (raised is not None and row.status == 'draft'
      and row.definition == 'D2-UNGATED' and row.published_at is None)
results.append(('G1 修② re-gate：未过门 fresh 行发布被拒，版本保持draft', g1,
                f'raised={raised is not None} status={row.status} '
                f'definition={row.definition} published_at={row.published_at}'))

# ---------- G2 修②钉扎：竞窗落在 gate_fn 内（门校验期间并发保存）→ 409 拒发布 ----------
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
snap_out = store.get_version(F, V, tenant_id=T)                # API 快照 base=U0
race_base = snap_out.updated_at.isoformat()
conflict = None
def gate_with_race(fresh):
    global conflict
    # 门校验期间（含 Redis 往返窗口）并发草稿保存落进：合法 CAS（base=U0）
    store.save_flow_definition(flow_id=F, version=V, definition='D2-RACE', tenant_id=T,
                               base_updated_at=race_base, created_by='racer2')
try:
    store.publish_version(F, V, tenant_id=T, gate_fn=gate_with_race)
except VersionConflictError as e:
    conflict = e
row = raw_row(store)
g2 = (conflict is not None and row.status == 'draft'
      and row.definition == 'D2-RACE' and row.published_at is None)
results.append(('G2 修②钉扎：门校验期间并发保存→发布409拒绝', g2,
                f'conflict={conflict is not None} status={row.status} '
                f'definition={row.definition} latest={getattr(conflict, "latest_updated_at", None)}'))

# ---------- G3 修①：SELECT(draft)→publish→CAS 三明治被 updated_at 前进+status 谓词挡住 ----------
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
base = store.get_version(F, V, tenant_id=T).updated_at
with store._session_local() as s1:
    existing = s1.scalars(select(FlowVersion).where(
        FlowVersion.tenant_id == T, FlowVersion.flow_id == F,
        FlowVersion.version == V)).first()
    assert existing.status == 'draft', '前置：读时为 draft'
    store.publish_version(F, V, tenant_id=T)                   # 并发发布在此提交
    after_pub = s1.execute(text(                               # s1 裸 SQL 重读（updated_at 不在 ORM）
        "SELECT updated_at, status FROM flow_versions "
        "WHERE tenant_id=:tid AND flow_id=:fid AND version=:ver"),
        {"tid": T, "fid": F, "ver": V}).first()
    advanced = after_pub[0] is not None and after_pub[0] != base
    ok = store._cas_update_draft(s1, flow_id=F, version=V, tenant_id=T,  # CAS UPDATE
                                 definition='D2', layout='', status='draft',
                                 base_updated_at=base, force=False, runtime=None)
    s1.commit()
row = raw_row(store)
g3 = (advanced and not ok and row.status == 'published'
      and row.definition == 'D1' and row.published_at is not None)
results.append(('G3 修① 三明治：publish前进updated_at+CAS status谓词→保存409', g3,
                f'updated_at前进={advanced} cas_ok={ok} status={row.status} '
                f'definition={row.definition}'))

# ---------- G4 修①legacy 档：updated_at 列缺失，status 谓词同样挡住三明治 ----------
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
g4 = (not ok and row.status == 'published' and row.definition == 'D1')
results.append(('G4 修①legacy档：status谓词挡住打回（不再依赖updated_at列）', g4,
                f'cas_ok={ok} status={row.status} definition={row.definition}'))

# ---------- G5 import 直发链同构：promotion 草稿 + 门快照→竞窗→re-gate 拒 ----------
store = fresh_store(with_updated_at=True)
store.ensure_flow(F, tenant_id=T)
store.save_flow_definition(flow_id=F, version=V, definition='D1', tenant_id=T,
                           created_by='promotion')             # import 链草稿（deployments 语义）
stored = store.get_version(F, V, tenant_id=T)                  # import.publish 门快照
store.save_flow_definition(flow_id=F, version=V, definition='D2-UNGATED', tenant_id=T,
                           base_updated_at=stored.updated_at.isoformat(),
                           created_by='racer3')
raised5 = None
try:
    store.publish_version(F, V, tenant_id=T, gate_fn=gate_reject_ungated)
except UnsupportedNodeTypesError:
    raised5 = True
row = raw_row(store)
g5 = (raised5 is True and row.status == 'draft' and row.definition == 'D2-UNGATED')
results.append(('G5 import直发链：同构 re-gate 拒未过门定义', g5,
                f'raised={raised5 is True} status={row.status} definition={row.definition}'))

# ---------- 对照绿A：无竞窗，门对象==发布对象（gate_fn 语义下照常发布） ----------
store = fresh_store(with_updated_at=True)
seed_draft(store, 'D1')
out = store.publish_version(F, V, tenant_id=T, gate_fn=gate_reject_ungated)
ga = out.status == 'published' and out.definition == 'D1'
results.append(('对照绿A 无竞窗：门对象==发布对象（修复不误伤正常发布）', ga,
                f'published={out.status} definition={out.definition}'))

# ---------- 对照绿B：幂等重发布（无 gate_fn 旧调用面不变） ----------
out2 = store.publish_version(F, V, tenant_id=T)
gb = out2.status == 'published' and out2.definition == 'D1'
results.append(('对照绿B 幂等重发布：旧调用面行为不变', gb,
                f'published={out2.status} definition={out2.definition}'))

# ---------- 对照绿C：无窗口时 published 检查拦截保存（语义守恒） ----------
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
gc = blocked and row.status == 'published' and row.definition == 'D1'
results.append(('对照绿C published 检查拦截保存（守恒）', gc,
                f'blocked={blocked} status={row.status} definition={row.definition}'))

print(textwrap.dedent("""
=== r86 postfix probe output ==="""))
for name, hit, detail in results:
    print(f"[{'GREEN' if name.startswith(('G', '对照')) and hit else 'FAIL'}-{name.split(' ')[0]}] {name} | {detail}")
verdict = all(hit for _, hit, _ in results)
print(f"TOTAL: {sum(1 for _, h, _ in results if h)}/{len(results)} green; ALL_GREEN={verdict}")
sys.exit(0 if verdict else 1)
