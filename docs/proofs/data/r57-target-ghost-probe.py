"""r57 S18-02B 探针：A46-02 目标不可用 → 剥 target 广播（真实 Redis）。

形态复刻 `~/.issue-keeper/duty/a2-r18-scripts/r46_dispatch_target_ghost.py`：
派发时 A 仍 running（被容量路由选中），派发后 A 进入 stopping（目标不可用）。
B 收到 target=A 的消息：
- 修复后：剥 target 按广播重投 → 自己（或任意 worker）可领 → 正常终态；
- 负控（R57_KEEP_TARGET=1 模拟修复前）：保留 target → 反复交接 → 4 次后
  AffinityHandoverExhausted。

用法：
  PYTHONPATH=<wt> <wt>/.venv/bin/python /tmp/r57_target_ghost.py
  R57_KEEP_TARGET=1 ... （负控）
"""
import json
import logging
import os
import sys
import threading
import time

WT = "/Users/kong/projects/infra4agent/plaita-wt3"
sys.path.insert(0, WT)
sys.path.insert(0, WT + "/plaita-console/backend")

import redis

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.registry import ServiceInfo, ServiceRegistry
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

DB = 14
R = redis.Redis.from_url(f"redis://localhost:6379/{DB}", decode_responses=True)
QUEUE = "r57:queue:target"
GROUP = "r57-target"
EID = "r57-target-exec"

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def clean():
    for pat in ("r57:queue:*", "plaita:registry:flow_worker:*", "plaita:dispatch:ledger*",
                "*:execution:affinityhandover:*", "plaita:execution:lease:*",
                "plaita:execution:fence*", "plaita:execution:cancel*"):
        for k in R.scan_iter(pat):
            R.delete(k)


def make(tag):
    return RedisFlowWorker(
        redis_url=f"redis://localhost:6379/{DB}",
        queue_name=QUEUE,
        consumer_group=GROUP,
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        event_bus=InMemoryEventBus(),
        redis_client=R,
        enable_registry=True,
        enable_redis_logging=False,
        registry_ttl=30,
        heartbeat_interval=60,  # 冻结自动心跳
        concurrency=1,
    )


def key_of(w):
    return f"{ServiceRegistry.REGISTRY_PREFIX}:{w.SERVICE_TYPE}:{w.instance_id}"


def set_status(w, status):
    R.set(key_of(w), ServiceInfo(
        instance_id=w.instance_id, service_type=w.SERVICE_TYPE,
        host="127.0.0.1", status=status,
    ).to_json(), ex=30)


def main():
    clean()
    os.environ["PLAITA_CONSOLE_DISPATCH_SHADOW"] = "0"

    a = make("a")
    a.register_service()
    a._running = True
    a.request_drain("signal 15")
    if a._drain_timer:
        a._drain_timer.cancel()
        a._drain_timer = None
    # 派发窗口：A 的键仍是 running（派发侧选中它）
    set_status(a, "running")

    b = make("b")
    # 只让路由看到 A：B 不注册（其 instance_id 仍可用于 target 比对）
    b._registry_registered = True

    from services import dispatcher as dispatcher_svc
    from plaita.server.capacity import decide_route, load_worker_capacity
    route = decide_route(load_worker_capacity(R))
    print("[route-while-draining]", {
        "a_draining_in_memory": a.draining,
        "a_registry_status": json.loads(R.get(key_of(a))).get("status"),
        "target_is_a": route.target_instance_id == a.instance_id,
        "rejected": [(r.instance_id, r.reason) for r in route.rejected],
    })

    b.flow_storage.save_flow(TEST_FLOW)
    b.execution_storage.save_execution_state(EID, ExecutionState(
        execution_id=EID, flow_id="f1", flow_version="1",
        context={"$LAST_NODE": "start"}, status="running",
    ))

    res = dispatcher_svc.dispatch_one(
        R, EID, flow_id="f1", execution_id=EID, queue_name=QUEUE,
        dry_run=False, shadow=False,
    )
    payload = json.loads(R.xrange(QUEUE, count=1)[0][1]["payload"])
    print("[dispatch-one]", {
        "enqueued": res["enqueued"], "decision": res["decision"],
        "target_is_a": res["target"] == a.instance_id,
        "message_target": payload.get("target_instance_id"),
    })

    # 派发后目标 A 进入 stopping（A46-02 的目标不可用窗口）
    set_status(a, "stopping")

    if os.environ.get("R57_KEEP_TARGET") == "1":
        # 负控：模拟修复前（无目标复核）——保留 target 原样交接
        b._unavailable_dispatch_target = lambda body: None
        print("[negative-control]", "keep-target (pre-fix shape)")

    b._running = True
    q = b._get_task_queue()
    t = threading.Thread(target=b._consume_loop, args=(q,), daemon=True)
    t.start()
    deadline = time.time() + 12
    while time.time() < deadline:
        st = b.execution_storage.load_execution_state(EID)
        if st is not None and st.status in ("error", "completed", "cancelled"):
            break
        time.sleep(0.1)
    b._running = False
    t.join(timeout=2)

    st = b.execution_storage.load_execution_state(EID)
    handover_key = f"plaita:execution:affinityhandover:{EID}"
    pending = R.xpending(QUEUE, GROUP)
    print("[after]", {
        "state_status": getattr(st, "status", None) if st is not None else None,
        "state_error_type": (getattr(st, "error", None) or {}).get("type"),
        "handover_counter": R.get(handover_key),
        "pending": pending.get("pending") if isinstance(pending, dict) else pending,
        "stream_len": R.xlen(QUEUE),
        "dlq_len": R.xlen(QUEUE + ":dlq"),
    })

    try:
        a._registry.stop_heartbeat()
        b._registry.stop_heartbeat()
    except Exception:
        pass
    clean()


if __name__ == "__main__":
    main()
