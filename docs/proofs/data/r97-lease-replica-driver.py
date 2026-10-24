"""r97 #324 跨进程 2 副本实弹驱动：按行 lease 前/后重复投递对表。

形态（任务书裁量：1 小时墙钟压缩 + 两真进程形态）：
- 父进程起 fakeredis ``TcpFakeServer``（真 TCP、跨进程共享键空间——租约
  SET NX 的原子性跨进程成立）+ 共享 SQLite 文件种 6 条 pending 行
  （f1..f5 正常 + ``flaky`` 混沌行：前 0.8s 目标侧拒绝，验「失败行按退避
  重试、恢复后自动补投」）；
- 并发拉起 2 个**真子进程**（--child，独立 DB 连接 + 独立 Redis 连接），
  收到 ``go`` 键后同时开跑 ``run_delivery_cycle`` 循环（poll 0.05s ×
  约 6s ≈ 每副本百余轮，覆盖 抢租约/释放/退避重试 全路径；1h×2s poll
  =1800 轮/副本的压缩模拟，真实性在 proofs 注明）；
- 子进程 monkeypatch 包一层 ``sync_flow_to_engine`` 计数（真投递仍走原
  函数），各自落 JSON 结果；父汇总断言：**每行投递次数合计 == 1**
  （零重复交付）、无子进程 fatal。

用法：``r97-lease-replica-driver.py --tree <worktree-root> --out <json>``
基线（origin/main 的 engine_outbox.py）预期红（每行 2 次投递）；
修复树预期绿（每行恰 1 次）。rc：0=零重复（绿），1=有重复（红）。
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO_PACKAGE_STABLE_NOTE = (
    "被测差异面=各树的 services/engine_outbox.py；plaita 核心包经 venv "
    "editable finder 同源解析（两树一致），不影响对表。"
)


def _setup_path(tree: str) -> Path:
    backend = Path(tree) / "plaita-console" / "backend"
    sys.path.insert(0, str(backend))
    sys.path.insert(0, str(tree))
    return backend


def _child(tree: str, port: int, db: str, out: str, run_seconds: float) -> int:
    backend = _setup_path(tree)
    os.environ["PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL"] = (
        f"redis://127.0.0.1:{port}/0")

    import redis

    from plaita.tenant_context import (
        DEFAULT_TENANT_ID,
        TENANT_SOURCE_EXPLICIT,
        set_current_tenant,
    )
    from services import engine_outbox
    from services.engine_sync import sync_flow_to_engine as real_sync

    set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
    client.ping()

    # 等齐发令枪：两子进程同时开跑（放大跨副本扫描重叠窗）。
    while not client.exists("go"):
        time.sleep(0.005)

    deliveries: dict = {}
    lease_skipped = 0
    cycles = 0
    fatal = None

    def counting_sync(target, flow_id, version, definition, tenant_id=""):
        if flow_id == "flaky" and client.exists("chaos"):
            raise ConnectionError("chaos: target rejecting")
        ok = real_sync(target, flow_id, version, definition,
                       tenant_id=tenant_id)
        if ok:
            deliveries[flow_id] = deliveries.get(flow_id, 0) + 1
        return ok

    engine_outbox.sync_flow_to_engine = counting_sync

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(f"sqlite:///{db}", future=True,
                           connect_args={"timeout": 15})
    session_local = sessionmaker(bind=engine, expire_on_commit=False)

    deadline = time.monotonic() + run_seconds
    try:
        while time.monotonic() < deadline:
            stats = engine_outbox.run_delivery_cycle(client, session_local)
            cycles += 1
            lease_skipped += stats.get("lease_skipped", 0)
            time.sleep(0.05)
    except Exception as e:  # noqa: BLE001 — 驱动自身异常须上报
        fatal = f"{type(e).__name__}: {e}"

    Path(out).write_text(json.dumps({
        "pid": os.getpid(), "cycles": cycles,
        "deliveries": deliveries, "lease_skipped": lease_skipped,
        "fatal": fatal,
    }))
    return 0


def _parent(tree: str, out: str, run_seconds: float) -> int:
    tmp = tempfile.mkdtemp(prefix="r97-lease-")
    db = str(Path(tmp) / "outbox.db")

    # fakeredis TCP 面（真协议、跨进程共享键空间）
    from fakeredis import TcpFakeServer

    server = TcpFakeServer(("127.0.0.1", 0))
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    backend = _setup_path(tree)
    os.environ["PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL"] = (
        f"redis://127.0.0.1:{port}/0")

    from plaita.tenant_context import (
        DEFAULT_TENANT_ID,
        TENANT_SOURCE_EXPLICIT,
        set_current_tenant,
    )
    set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from models.flow import Base
    from services.flow_store import FlowStore

    engine = create_engine(f"sqlite:///{db}", future=True)
    Base.metadata.create_all(engine)
    store = FlowStore(sessionmaker(bind=engine, expire_on_commit=False))
    flows = ["f1", "f2", "f3", "f4", "f5", "flaky"]
    for fid in flows:
        definition = json.dumps({
            "flow_id": fid, "desc": "r97 lease probe",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name",
                 "resultType": "success"},
            ],
        })
        store.ensure_flow(fid)
        store.save_flow_definition(fid, "1.0.0", definition, "{}")
        store.publish_version(fid, "1.0.0")

    env = dict(os.environ)
    env["PYTHONPATH"] = f"{backend}:{tree}"
    outs = [str(Path(tmp) / f"child{i}.json") for i in (0, 1)]
    kids = [
        subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--child",
             "--tree", tree, "--port", str(port), "--db", db,
             "--out", outs[i], "--run-seconds", str(run_seconds)],
            env=env, cwd=str(tree),
        )
        for i in (0, 1)
    ]

    import redis as redis_mod

    ctrl = redis_mod.Redis(host="127.0.0.1", port=port, decode_responses=True)
    ctrl.set("chaos", "1")          # flaky 行先拒绝（失败退避格）
    ctrl.set("go", "1")             # 发令枪
    time.sleep(0.8)
    ctrl.delete("chaos")            # 恢复 → flaky 行退避到期自动补投

    codes = [k.wait(timeout=run_seconds + 30) for k in kids]
    results = [json.loads(Path(p).read_text()) for p in outs]

    totals: dict = {}
    for r in results:
        for fid, n in r["deliveries"].items():
            totals[fid] = totals.get(fid, 0) + 1
    missing = [f for f in flows if totals.get(f, 0) == 0]
    dup = {f: n for f, n in totals.items() if n > 1}
    fatals = [r["fatal"] for r in results if r["fatal"]]
    bad_rc = [c for c in codes if c != 0]

    print(f"== r97 跨进程 2 副本驱动（tree={tree}，run={run_seconds}s）==")
    for i, r in enumerate(results):
        print(f"child{i}: pid={r['pid']} cycles={r['cycles']} "
              f"deliveries={r['deliveries']} lease_skipped={r['lease_skipped']} "
              f"fatal={r['fatal']}")
    print(f"per-flow totals: {totals}")
    print(f"missing(未投递): {missing}")
    print(f"duplicates(重复投递): {dup}")
    print(f"child fatals: {fatals}; child rc: {codes}")

    verdict = (not dup) and (not missing) and (not fatals) and (not bad_rc)
    print(f"VERDICT: {'ZERO-DUPLICATE-GREEN' if verdict else 'RED'}")
    Path(out).write_text(json.dumps({
        "tree": tree, "run_seconds": run_seconds,
        "children": results, "totals": totals,
        "duplicates": dup, "missing": missing,
        "fatals": fatals, "child_rc": codes,
        "verdict": "ZERO-DUPLICATE-GREEN" if verdict else "RED",
    }, ensure_ascii=False, indent=1))
    return 0 if verdict else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tree", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--run-seconds", type=float, default=6.0)
    ap.add_argument("--child", action="store_true")
    ap.add_argument("--port", type=int)
    ap.add_argument("--db")
    args = ap.parse_args()
    if args.child:
        sys.exit(_child(args.tree, args.port, args.db, args.out,
                        args.run_seconds))
    sys.exit(_parent(args.tree, args.out, args.run_seconds))
