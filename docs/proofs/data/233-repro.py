#!/usr/bin/env python3
"""plaita#233 repro：挂起「落盘 → 派发」窗口无补偿载体。

复现路径（全部实跑，不 mock 被测逻辑；只 patch 引擎与 rpush 瞬断）：

1. 盘上 execution 处于 ``running``，resume(continue) 正常推进到挂起节点；
2. 落盘 ``suspended`` 后派发外延服务任务，rpush 瞬断 → ``ServiceDispatchError``
   （按设计消息不 ack，等重投）；
3. Redis 恢复后重投**同一载体** → 基线命中 #33 的 suspended 幂等短路，
   直接 ack，**不再派发** ⇒ ``plaita:delay:queue`` 永远 0 条，执行永久
   ``suspended``（``error=None``、``end_time=None``），无任何载体能补；
4. start 载体（dedup 命中）同样只回报现状，不补派。

修复后（本脚本的 PASS 判据）：

- 第 2 步之后盘上留有 ``dispatch_pending`` 凭据；
- 第 3 步重投补派**恰 1 条**任务，状态仍 ``suspended``、不重跑挂起节点；
- 第 4 步再投一次不产生第 2 条任务（幂等）；
- start 载体（dedup 命中）同样不产生第 2 条。

运行（仓库根）::

    PYTHONPATH=. /Users/jeff.jie/.venvs/plaita/bin/python docs/proofs/data/233-repro.py

退出码：0 = 修复生效（绿）；1 = 仍有永久 suspended 缺口（红）。
"""
from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker, ServiceDispatchError
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}
QUEUE = "plaita:delay:queue"
EXEC = "exec-dispatch-retry"
CHECKPOINT = {"$LAST_NODE": "a", "$NODE": {}}
SUSPEND_RESULT = {
    "execution_id": EXEC,
    "is_suspend": True,
    "service_config": {"type": "delay", "delay_ms": 1000},
    "context": CHECKPOINT,
}


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="probe:233:queue",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _queued(fake):
    return [json.loads(item) for item in fake.lrange(QUEUE, 0, -1)]


def main() -> int:
    worker, storage, fake = _worker()
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status="running",
            context=dict(CHECKPOINT),
        ),
    )

    # ---- [1] 首次 resume：落盘 suspended 后 rpush 瞬断 ----
    orig_rpush = fake.rpush
    fake.rpush = MagicMock(side_effect=ConnectionError("redis down during rpush"))
    ran_first = False
    try:
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            FE.return_value.run_distributed.return_value = SUSPEND_RESULT
            try:
                worker.resume_flow("f1", EXEC, "continue")
            except ServiceDispatchError as exc:
                print(f"[1] 首次 resume → ServiceDispatchError（消息不 ack，等重投）: {exc}")
            ran_first = FE.return_value.run_distributed.called
    finally:
        fake.rpush = orig_rpush

    state = storage.load_execution_state(EXEC)
    marker = getattr(state, "dispatch_pending", None)
    print(f"[1] 盘上状态: status={state.status!r} end_time={state.end_time!r} error={state.error!r}")
    print(f"[1] 派发凭据 dispatch_pending = {marker}")
    print(f"[1] {QUEUE} 长度 = {fake.llen(QUEUE)}（派发瞬断，应为 0）")

    # ---- [2] Redis 恢复，重投同一 continue 载体 ----
    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        FE.return_value.run_distributed.return_value = SUSPEND_RESULT
        redelivered = worker.resume_flow("f1", EXEC, "continue")
        rerun = FE.return_value.run_distributed.called
    print(f"[2] 重投 resume_type=continue 返回: {redelivered}")
    print(f"[2] 挂起节点是否重跑: {rerun}")
    qlen_after = fake.llen(QUEUE)
    print(f"[2] {QUEUE} 长度 = {qlen_after}")

    # ---- [3] 再投一次：不得产生第 2 条任务 ----
    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        second = worker.resume_flow("f1", EXEC, "continue")
    qlen_second = fake.llen(QUEUE)
    print(f"[3] 第二次重投返回: {second}; {QUEUE} 长度 = {qlen_second}")

    # ---- [4] start 载体（幂等键命中）同样不得产生第 2 条 ----
    fake.set("plaita:start-dedup:probe233", EXEC)
    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        hit = worker.start_flow("f1", {}, version="1", dedup_key="probe233")
        start_rerun = FE.return_value.run_distributed.called
    qlen_start = fake.llen(QUEUE)
    print(f"[4] start 幂等命中返回: {hit}; 是否重跑={start_rerun}")
    print(f"[4] {QUEUE} 长度 = {qlen_start}")

    state = storage.load_execution_state(EXEC)
    print(f"[5] 末态: status={state.status!r} end_time={state.end_time!r} "
          f"error={state.error!r} marker={getattr(state, 'dispatch_pending', None)}")

    ok = (
        ran_first
        and isinstance(marker, dict)
        and marker.get("pending") is True
        and qlen_after == 1
        and rerun is False
        and qlen_second == 1
        and start_rerun is False
        and qlen_start == 1
        and state.status == "suspended"
        and state.end_time is None
        and state.error is None
        and getattr(state, "dispatch_pending", None) is None
    )
    if ok:
        print("\nRESULT: PASS —— 重投载体恰补派 1 条任务、不重跑挂起节点、再投幂等")
        return 0
    print("\nRESULT: FAIL —— 挂起派发缺口仍在（执行可能永久 suspended）")
    return 1


if __name__ == "__main__":
    sys.exit(main())
