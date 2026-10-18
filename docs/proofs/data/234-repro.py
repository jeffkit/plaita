#!/usr/bin/env python3
"""plaita#234 repro：start 载体重投覆写 checkpoint 并整条 flow 重放。

复现路径（at-least-once 的正常路径，不是人为构造）：

1. 带预铸 ``execution_id`` 的 start 消息第一次处理：跑了若干节点，checkpoint
   落在盘上（``status=running`` / ``suspended``），随后处理函数抛
   ``StatePersistError`` / ``ServiceDispatchError`` / 进程崩溃 → 消息**不 ack**；
2. XCLAIM 重投**同一条 start 消息**（body 原样）→ ``start_flow`` 不看盘上状态：
   新建 ``context={}`` 的 ``ExecutionState`` 无条件覆写，再以**不带
   ``saved_context``** 的 ``run_distributed`` 从首节点整条重跑；
3. 结果：已完成节点的外部副作用全部重放（最多 ``max_deliveries`` 次），原
   checkpoint（``$LAST_NODE`` / ``$NODE`` / ``$INPUT``）被永久抹成 ``{}``，
   此后连正确的事件 resume 也失去续跑现场。

修复后（本脚本的 PASS 判据）：

- 盘上 running + 非空 checkpoint ⇒ 转 resume 语义：``run_distributed`` 收到
  ``saved_context`` = 原 checkpoint，且调用瞬间盘上 checkpoint 未被覆写；
- 终态行 ⇒ 幂等回报现状，不重跑（不复写终态行）；
- 挂起行 ⇒ 不重跑挂起节点（#33 短路，#233 补派）；
- 首投（盘上无行）⇒ 行为不变（不带 ``saved_context``）。

运行（仓库根）::

    PYTHONPATH=. /Users/jeff.jie/.venvs/plaita/bin/python docs/proofs/data/234-repro.py

退出码：0 = 修复生效（绿）；1 = 仍会整条重放/覆写 checkpoint（红）。
"""
from __future__ import annotations

import sys
from unittest.mock import patch

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "b"},
        {"id": "b", "type": "assignment", "result": {"step": 2}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}
EXEC = "exec-replay"
CHECKPOINT = {
    "$LAST_NODE": "b",
    "$NODE": {"a": {"step": 1}, "b": {"step": 2}},
    "$INPUT": {"order_id": "O-1"},
}


def _worker():
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="probe:234:queue",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage


def _seed(storage, status, context, flow_id="f1", execution_id=EXEC):
    storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id=flow_id,
            flow_version="1",
            status=status,
            context=dict(context),
        ),
    )


def _call_start_capturing(worker, storage, flow_id="f1", execution_id=EXEC, context=None):
    """投一条 start 消息，记录 run_distributed 的入参与调用瞬间的盘上 context。"""
    captured = {}

    def fake_run_distributed(flow, params=None, execution_id=None, **kwargs):
        snapshot = storage.load_execution_state(execution_id)
        captured["context_at_call"] = dict(snapshot.context) if snapshot else None
        captured["kwargs"] = dict(kwargs)
        return {"execution_id": execution_id, "is_end": True, "context": {"$LAST_NODE": "end"}}

    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        FE.return_value.run_distributed.side_effect = fake_run_distributed
        out = worker.start_flow(flow_id, {"order_id": "O-1"}, version="1",
                                execution_id=execution_id)
        called = FE.return_value.run_distributed.called
    return out, captured, called


def main() -> int:
    worker, storage = _worker()

    # ---- [1] 盘上 running + 非空 checkpoint，重投同一 start 消息 ----
    _seed(storage, "running", CHECKPOINT)
    before = storage.load_execution_state(EXEC)
    print(f"[1] 重投前盘上: status={before.status!r} $LAST_NODE={before.context.get('$LAST_NODE')!r} "
          f"$NODE keys={sorted(before.context.get('$NODE', {}))}")

    out, captured, called = _call_start_capturing(worker, storage)
    print(f"[2] run_distributed kwargs = {captured['kwargs']}")
    print(f"[2] saved_context 是否传入 = {'saved_context' in captured['kwargs']}")
    print(f"[3] run_distributed 调用瞬间盘上 context = {captured['context_at_call']}")
    print(f"[4] 返回 = {out}")

    resume_ok = captured["kwargs"].get("saved_context") == CHECKPOINT
    checkpoint_kept = captured["context_at_call"] == CHECKPOINT

    # ---- [5] 终态行：重投 start 不得重跑 ----
    _seed(storage, "completed", {"$LAST_NODE": "end"})
    out_terminal, captured_terminal, called_terminal = _call_start_capturing(worker, storage)
    print(f"[5] 终态行重投 start → 是否重跑={called_terminal}, 返回={out_terminal}")

    # ---- [6] 挂起行：重投 start 不得重跑挂起节点 ----
    _seed(storage, "suspended", CHECKPOINT)
    out_susp, captured_susp, called_susp = _call_start_capturing(worker, storage)
    print(f"[6] 挂起行重投 start → 是否重跑={called_susp}, 返回={out_susp}")

    # ---- [7] 首投（盘上无行）：行为不变 ----
    worker2, storage2 = _worker()
    out_first, captured_first, called_first = _call_start_capturing(
        worker2, storage2, execution_id="exec-first"
    )
    first_ok = called_first and "saved_context" not in captured_first["kwargs"]
    print(f"[7] 首投: 是否跑={called_first}, "
          f"saved_context 是否传入={'saved_context' in captured_first['kwargs']}")

    ok = (
        resume_ok
        and checkpoint_kept
        and not called_terminal
        and not called_susp
        and first_ok
    )
    if ok:
        print("\nRESULT: PASS —— start 重投转 resume/幂等，不重放、不覆写 checkpoint")
        return 0
    print("\nRESULT: FAIL —— start 重投仍会覆写 checkpoint 并整条重放")
    return 1


if __name__ == "__main__":
    sys.exit(main())
