#!/usr/bin/env python3
"""plaita#221 repro：陈旧的 event resume 会解析**下一个**事件节点。

    PYTHONPATH=/tmp/w46 .venvs/plaita/bin/python /tmp/w46-out/repro-221.py

流程 ``start → waitA(event a.done) → waitB(event b.done) → end``，逐步驱动
``run_distributed``：

1. step1 挂起在 waitA，拿到订阅 ``sub-1``；
2. 用 waitA 的订阅 resume ⇒ waitA 完成；step3 推进到 waitB 并挂起（``sub-2``）；
3. 再喂一条 **waitA 的陈旧 event resume**（subscription_id=sub-1）——
   修复前：waitB 被静默写成 completed + waitA 的事件载荷，无任何报错。

修复后同一脚本应抛 ``ResumeGuardError``（执行保持挂起、waitB 仍 pending）。
脚本按「修复前=红 / 修复后=绿」两种形态输出并给出退出码：
bug 复现 ⇒ exit 1（红），绑定校验生效 ⇒ exit 0（绿）。
"""
from __future__ import annotations

import sys

from plaita.core.errors import ResumeGuardError
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.node import End, Start
from plaita.node.event_node import EventNode


class _Bus:
    """最小 event bus：每次注册发一个可追踪的订阅 id。"""

    def __init__(self):
        self.registered = []
        self.unregistered = []

    def register_subscription(self, **kwargs):
        sid = f"sub-{len(self.registered) + 1}"
        self.registered.append((sid, kwargs))
        return sid

    def unregister_subscription(self, sid):
        self.unregistered.append(sid)
        return True


def _flow():
    return Flow(
        flow_id="proof221",
        nodes=[
            Start(id="start", next="waitA"),
            EventNode(id="waitA", event_type="a.done", next="waitB"),
            EventNode(id="waitB", event_type="b.done", next="end"),
            End(id="end"),
        ],
    )


def _node_state(step, node_id):
    return (step.get("context") or {}).get("$NODE", {}).get(node_id, {})


def main() -> int:
    flow = _flow()
    bus = _Bus()
    execution = FlowExecution(event_bus=bus)

    step1 = execution.run_distributed(flow)
    wait_a = _node_state(step1, "waitA")
    sub_a = wait_a.get("subscription_id")
    print("step1 suspend at:", step1.get("id"), "is_suspend=", step1.get("is_suspend"))
    print("  waitA subscription_id =", sub_a)

    stale_resume = {
        "event_id": "evt-a",
        "event_type": "a.done",
        "event_data": {"who": "A"},
        "subscription_id": sub_a,
    }
    step2 = execution.run_distributed(
        flow,
        saved_context=step1["context"],
        resume_type="event",
        resume_data=stale_resume,
    )
    print("step2 resume(A) -> node", step2.get("id"), "is_suspend=", step2.get("is_suspend"))
    print("  waitA status =", _node_state(step2, "waitA").get("status"))

    step3 = execution.run_distributed(flow, saved_context=step2["context"])
    wait_b = _node_state(step3, "waitB")
    sub_b = wait_b.get("subscription_id")
    print("step3 continue -> suspend at:", step3.get("id"), "is_suspend=", step3.get("is_suspend"))
    print("  waitB subscription_id =", sub_b)
    print("  waitB status =", wait_b.get("status"))

    # ---- 关键：陈旧的 A resume 落在 waitB 上 ----
    try:
        step4 = execution.run_distributed(
            flow,
            saved_context=step3["context"],
            resume_type="event",
            resume_data=dict(stale_resume),  # 仍带 sub-1
        )
    except Exception as exc:  # noqa: BLE001 — run_distributed 会归一化，按异常链判
        cause = exc
        guard = None
        while cause is not None:
            if isinstance(cause, ResumeGuardError):
                guard = cause
                break
            cause = cause.__cause__
        if guard is None:
            raise
        # 修复后：绑定校验拒绝陈旧消息，执行保持挂起、waitB 不被写。
        print("step4 STALE resume(A) while suspended at waitB -> ResumeGuardError: %s" % guard)
        print("  waitB status =", _node_state(step3, "waitB").get("status"), "(仍未推进)")
        print("RESULT: GREEN —— 陈旧 resume 被订阅绑定校验拒绝")
        return 0

    wb = _node_state(step4, "waitB")
    print("step4 STALE resume(A) while suspended at waitB -> node",
          step4.get("id"), "is_suspend=", step4.get("is_suspend"), "is_end=", step4.get("is_end"))
    print("  waitB status      =", wb.get("status"))
    print("  waitB event_data  =", wb.get("event_data"))
    print("  waitB event_type  =", wb.get("event_type"))
    print("  waitB 收到 b.done? ->", wb.get("event_type") == "b.done"
          and (wb.get("event_data") or {}).get("event_type") == "b.done")

    step5 = execution.run_distributed(flow, saved_context=step4["context"])
    end_result = (step5.get("context") or {}).get("$NODE", {}).get("end")
    print("step5 continue -> node", step5.get("id"), "is_end=", step5.get("is_end"))
    print("flow end result   =", step5.get("result"))
    print("RESULT: RED —— 陈旧 resume 静默解决了 waitB，流程带 A 的载荷继续")
    return 1


if __name__ == "__main__":
    sys.exit(main())
