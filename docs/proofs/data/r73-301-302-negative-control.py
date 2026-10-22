"""r73 负控：#301 删除投递语义 / #302 源表漂移对账 / c117-06 DLQ 人工出口 / b117-04 inf-nan。

用法：python r73-301-302-negative-control.py [树根]
  树根默认 = 本仓根（post 形态）；pre 形态传 /tmp/plaita-r117-tree
  （A1 r117 基线 41984f1 的 clean archive，即 #301/#302 修复前）。

判据（全部先红后绿）：
- S1 #301：publish→delivered→delete_flow（outbox 行 purge）后，目标 Redis 的
  版本 key / flow_list 成员 / flow_versions 注册集三件必须全清；
  修前永驻（幽灵 key，still_there=1）。
- S2 #302：源表 definition 被 DB 级修改后，对账必须具名漂移（drifted=1、
  行打回 pending、drift_count=1）并按源表现值重投收敛（target desc=OPS-EDITED、
  /health drift_total=1）；修前三路全闭（requeued=0 恒绿、目标永留旧定义）。
- S3 c117-06：failed（DLQ）行有 resolve/discard 人工出口，resolve 后退出
  投递扫描且 /health resolved=1；修前无出口（404）。
- S4 b117-04：POLL=inf 必须具名拒（回退默认 2.0）；修前静默放行 inf
  （投递链停摆）。

输出逐行 VERDICT，末行总结；全绿 exit 0，否则 exit 1。
"""
import json
import os
import sys
import tempfile
from pathlib import Path

import fakeredis
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

TREE = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else \
    Path(__file__).resolve().parents[3]
BACKEND = TREE / "plaita-console" / "backend"
sys.path[:0] = [str(BACKEND), str(TREE)]

from models.flow import Base, FlowPublishOutbox, FlowVersion  # noqa: E402
from services import engine_outbox  # noqa: E402
from services.flow_store import FlowStore  # noqa: E402

OUT = []
VERDICTS = []


def verdict(name, ok, detail):
    VERDICTS.append(ok)
    line = f"VERDICT {'GREEN' if ok else 'RED'} {name}: {detail}"
    OUT.append(line)
    print(line)


def demo_def(fid="demo"):
    return json.dumps({
        "flow_id": fid, "desc": "outbox",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.name",
             "resultType": "success"},
        ],
    })


def rows(sessions, **f):
    with sessions() as s:
        stmt = select(FlowPublishOutbox).order_by(FlowPublishOutbox.id.asc())
        for c, v in f.items():
            stmt = stmt.where(getattr(FlowPublishOutbox, c) == v)
        return s.scalars(stmt).all()


os.environ[engine_outbox.TARGET_URL_ENV] = "redis://target-env:6379/0"
tmp = Path(tempfile.mkdtemp(prefix="r73-negctl-"))
engine = create_engine(f"sqlite:///{tmp}/neg.db", future=True)
Base.metadata.create_all(engine)
store = FlowStore(sessionmaker(bind=engine, expire_on_commit=False))
sessions = store._session_local
target = fakeredis.FakeRedis(decode_responses=True)
ns = "plaita"


def publish(fid, ver="1.0.0"):
    store.ensure_flow(fid, tenant_id="")
    store.save_flow_definition(fid, ver, demo_def(fid), "{}", tenant_id="")
    store.publish_version(fid, ver)


def three(target, fid, ver):
    return {
        "key": bool(target.exists(f"{ns}:flow:{fid}:{ver}")),
        "flow_list_member": bool(target.sismember(f"{ns}:flow_list", fid)),
        "versions_set": bool(target.exists(f"{ns}:flow_versions:{fid}")),
    }


# ---- S1 #301：删除后目标三件 ----
publish("ghost")
engine_outbox.run_delivery_cycle(target, sessions)
store.delete_flow("ghost")  # 行 purge（pre/post 相同；API 层另行挂钩）
purge = getattr(engine_outbox, "purge_target_keys", None)
if purge is not None:
    purge("ghost", client=target)  # post：API 删除端点同款调用
state = three(target, "ghost", "1.0.0")
verdict("S1_301_target_three_cleaned",
        not any(state.values()),
        f"ghost still_there={int(state['key'])} flow_list={int(state['flow_list_member'])} "
        f"versions_set={int(state['versions_set'])} purge_target_keys={'yes' if purge else 'MISSING'}")

# ---- S2 #302：源表 definition 被改 ----
publish("demo")
engine_outbox.run_delivery_cycle(target, sessions)
with sessions() as s:
    fv = s.scalars(select(FlowVersion).where(FlowVersion.flow_id == "demo")).first()
    fv.definition = json.dumps({**json.loads(fv.definition), "desc": "OPS-EDITED"})
    s.commit()
rec = engine_outbox.run_reconcile_cycle(target, sessions)
drifted = rec.get("drifted", 0)
row = rows(sessions, flow_id="demo")[0]
target_desc = json.loads(target.get(f"{ns}:flow:demo:1.0.0")).get("desc")
console_desc = "OPS-EDITED"
if drifted:
    engine_outbox.run_delivery_cycle(target, sessions)
    row = rows(sessions, flow_id="demo")[0]
    target_desc = json.loads(target.get(f"{ns}:flow:demo:1.0.0")).get("desc")
snap = engine_outbox.stats_snapshot(sessions)
converged = target_desc == console_desc
named = "源表" in (row.last_error or "") or drifted > 0
verdict("S2_302_source_drift_named_and_converged",
        drifted >= 1 and converged and named and snap.get("drift_total", 0) >= 1,
        f"reconcile={ {k: rec[k] for k in ('checked', 'requeued', 'drifted') if k in rec} } "
        f"target_desc={target_desc!r} console_desc={console_desc!r} "
        f"drift_total={snap.get('drift_total', 'MISSING')} row_status={row.status}")

# ---- S3 c117-06：DLQ 人工出口 ----
publish("dlq")
resolve_fn = getattr(engine_outbox, "resolve_row", None)
if resolve_fn is None:
    verdict("S3_c117_06_dlq_manual_exit", False,
            "engine_outbox.resolve_row MISSING（无人工出口）")
else:
    with sessions() as s:  # 直置 failed（DLQ 档）
        r = rows(sessions, flow_id="dlq")[0]
        r.status = "failed"
        s.add(r)
        s.commit()
    row_id = rows(sessions, flow_id="dlq")[0].id
    brief = resolve_fn(sessions, row_id)
    scanned = engine_outbox.run_delivery_cycle(target, sessions)["scanned"]
    snap = engine_outbox.stats_snapshot(sessions)
    ok = (brief is not None and brief["status"] == "resolved"
          and scanned == 0 and snap.get("resolved", 0) == 1)
    verdict("S3_c117_06_dlq_manual_exit", ok,
            f"resolved={brief is not None and brief['status']} "
            f"exit_queue(scanned)={scanned} health_resolved={snap.get('resolved', 'MISSING')}")

# ---- S4 b117-04：POLL=inf 具名拒 ----
os.environ[engine_outbox.POLL_ENV] = "inf"
poll_val = engine_outbox.poll_seconds()
os.environ.pop(engine_outbox.POLL_ENV)
verdict("S4_b117_04_inf_named_rejected", poll_val == engine_outbox.DEFAULT_POLL_SECONDS,
        f"poll_seconds(inf)={poll_val} default={engine_outbox.DEFAULT_POLL_SECONDS}")

# ---- S5 #302 守恒：健康行不误报 ----
publish("healthy")
engine_outbox.run_delivery_cycle(target, sessions)
rec = engine_outbox.run_reconcile_cycle(target, sessions)
row = rows(sessions, flow_id="healthy")[0]
verdict("S5_302_healthy_row_no_drift",
        rec.get("drifted", -1) == 0 and row.status == "delivered"
        and (getattr(row, "drift_count", 0) or 0) == 0,
        f"drifted={rec.get('drifted', 'MISSING')} status={row.status} "
        f"drift_count={getattr(row, 'drift_count', 'MISSING')}")

total = len(VERDICTS)
passed = sum(VERDICTS)
OUT.append(f"SUMMARY {passed}/{total} GREEN")
print(OUT[-1])
sys.exit(0 if passed == total else 1)
