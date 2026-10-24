"""r92 探针：create_flow × delete_flow 同 PK 复生竞态（plaita#319 负控勘探）。

跑全部可确定交错格（seam 注入 + 双线程），报告每种终态分类：
- LEGAL_DEL: 删除成功 + 行消失（版本空）
- LEGAL_NEW: 创建成功 + 行在（版本空=新生命周期）
- GONE_BOTH: 删除 404/创建 409（一致）
- RESURRECT: 删除返回 True 且终态行在  ← 复生形态（违例）
- GHOST:     创建抛错但终态行在      ← 无人认领的行（违例）
- ORPHAN_V:  行不在但版本行残留      ← #314 已管，越界即报
引擎两形态：raw（裸 engine，FK 关）× console init_engine（FK 强制）。
"""
import json
import sys
import threading
import time
from pathlib import Path

BACKEND = Path("/Users/kong/projects/infra4agent/plaita/plaita-console/backend")
sys.path.insert(0, str(BACKEND))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from models.flow import FlowRecord, FlowVersion
from services import flow_store
from services.flow_store import FlowStore

TENANT = "default"
FLOW = "probe-flow"
DEF = json.dumps({"nodes": []})


def classify(result, store):
    """按 (create 结果, delete 结果, 终态行/版本) 分类终态。"""
    with store._session_local() as s:
        parent = s.scalars(select(FlowRecord).where(
            FlowRecord.tenant_id == TENANT, FlowRecord.flow_id == FLOW)).first()
        versions = s.scalars(select(FlowVersion).where(
            FlowRecord.tenant_id == TENANT, FlowVersion.flow_id == FLOW)).all()
    versions = [v.version for v in versions]
    c, d = result["create"], result["delete"]
    row_alive = parent is not None
    if not row_alive and versions:
        return "ORPHAN_V"
    if d == "committed" and row_alive:
        return "RESURRECT"
    if c == "created" and not row_alive:
        return "GONE_AFTER_CREATE"
    if c.startswith("ValueError") and row_alive:
        return "GHOST"
    if c == "created":
        return "LEGAL_NEW"
    return "LEGAL_DEL"


def run_round(engine_form, seam, idx):
    """单轮：建旧生命周期 → 双线程交错 → 分类终态。"""
    db = Path(f"/private/tmp/r92probe_{engine_form}_{seam}_{idx}.db")
    if db.exists():
        db.unlink()
    if engine_form == "raw":
        eng = create_engine(f"sqlite:///{db}",
                            connect_args={"timeout": 15, "check_same_thread": False})
        FlowRecord.__table__.metadata.create_all(eng)
        store = FlowStore(sessionmaker(bind=eng, expire_on_commit=False))
    else:
        from config import Settings
        s = Settings()
        s.console_env = "dev"
        s.admin_api_key = "probe"
        s.allow_insecure_admin = False
        flow_store.init_engine(f"sqlite:///{db}")
        store = flow_store.get_flow_store()

    # 旧生命周期：flow 在 + 一版草稿
    store.create_flow(FLOW, tenant_id=TENANT)
    store.save_flow_definition(flow_id=FLOW, version="1.0.0",
                               definition=DEF, tenant_id=TENANT)

    result = {"create": "?", "delete": "?"}
    entered = threading.Event()
    release = threading.Event()

    orig_purge = flow_store._outbox_purge

    def sleepy_purge(session, **kw):
        out = orig_purge(session, **kw)
        if seam == "del_purge":
            entered.set()
            release.wait(timeout=10)  # 删除事务持写锁停表
        return out

    orig_atg = flow_store.assert_tenant_generation

    def sleepy_atg(session, tenant_id, *a, **kw):
        out = orig_atg(session, tenant_id, *a, **kw)
        if seam == "del_atg":
            entered.set()
            release.wait(timeout=10)
        return out

    def deleter():
        flow_store._outbox_purge = sleepy_purge
        flow_store.assert_tenant_generation = sleepy_atg
        try:
            ok = store.delete_flow(FLOW, tenant_id=TENANT)
            result["delete"] = "committed" if ok else "false"
        except Exception as e:  # noqa: BLE001
            result["delete"] = f"{type(e).__name__}"
        finally:
            flow_store._outbox_purge = orig_purge
            flow_store.assert_tenant_generation = orig_atg
            entered.set()
            release.set()

    def creator():
        if seam in ("del_purge", "del_atg"):
            entered.wait(timeout=10)  # 等删除事务进缝（行在，预检必 409）
        elif seam == "create_precommit":
            pass
        try:
            rec = store.create_flow(FLOW, tenant_id=TENANT)
            result["create"] = "created"
        except Exception as e:  # noqa: BLE001
            result["create"] = f"{type(e).__name__}: {e}"[:60]
        if seam == "create_precommit":
            release.set()

    if seam == "create_precommit":
        # 创建侧先走到 commit 前（assert_tenant_generation 停表），删除并发进入
        def sleepy_create_atg(session, tenant_id, *a, **kw):
            out = orig_atg(session, tenant_id, *a, **kw)
            entered.set()
            release.wait(timeout=10)
            return out
        flow_store.assert_tenant_generation = sleepy_create_atg

    threads = [threading.Thread(target=deleter), threading.Thread(target=creator)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    flow_store.assert_tenant_generation = orig_atg
    flow_store._outbox_purge = orig_purge
    alive = any(t.is_alive() for t in threads)
    if alive:
        return "HANG", result
    return classify(result, store), result


def main():
    matrix = [
        ("raw", "del_purge"),       # 删除事务内 purge 缝持写锁，创建并发进入
        ("console", "del_purge"),
        ("raw", "del_atg"),         # 删除事务内 generation 复核缝（更晚，锁面同）
        ("console", "del_atg"),
        ("raw", "create_precommit"),  # 创建 INSERT 已落、commit 前停表，删除并发
        ("console", "create_precommit"),
        ("raw", "none"),            # 无缝自由竞（20 轮扫随机窗）
        ("console", "none"),
    ]
    bad = 0
    for form, seam in matrix:
        n = 20 if seam == "none" else 3
        seen = {}
        for i in range(n):
            cls, res = run_round(form, seam, i)
            seen.setdefault(cls, []).append(res)
        flag = "OK " if all(k.startswith("LEGAL") or k == "GONE_BOTH"
                            for k in seen) else "★★★ "
        print(f"{flag}{form:8s} {seam:16s} n={n}: ", end="")
        for k, v in seen.items():
            print(f"{k}×{len(v)}", end=" ")
            if k in ("RESURRECT", "GHOST", "ORPHAN_V", "GONE_AFTER_CREATE"):
                bad += 1
                print(f"  样本={v[0]}", end="")
        print()
    print(f"\n违例总数（格型）: {bad}")


if __name__ == "__main__":
    main()
