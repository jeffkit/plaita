#!/usr/bin/env python3
"""演示：重复 resume 时副作用只生效一次。

运行：python -m examples.server_demo.idempotent_resume_demo
"""
from __future__ import annotations

from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage


class SideEffectStore:
    """业务侧幂等门闩（生产可用 Redis SET NX）。"""

    def __init__(self):
        self._done = set()
        self.emails_sent = 0

    def send_approval_email(self, execution_id: str, decision_id: str) -> bool:
        key = (execution_id, decision_id)
        if key in self._done:
            return False
        self._done.add(key)
        self.emails_sent += 1
        return True


def apply_resume_side_effects(
    storage: MemoryExecutionStorage,
    effects: SideEffectStore,
    execution_id: str,
    decision_id: str,
) -> str:
    state = storage.load_execution_state(execution_id)
    if not state:
        return "missing"
    if state.status in ("completed", "error"):
        return "already_terminal"
    # 副作用在改状态之前用稳定键去重
    applied = effects.send_approval_email(execution_id, decision_id)
    state.status = "completed"
    storage.save_execution_state(execution_id, state)
    return "applied" if applied else "duplicate_skipped"


def main() -> None:
    storage = MemoryExecutionStorage()
    effects = SideEffectStore()
    execution_id = "exec-demo-1"
    storage.save_execution_state(
        execution_id,
        ExecutionState(
            execution_id=execution_id,
            flow_id="approval",
            context={},
            status="suspended",
        ),
    )

    r1 = apply_resume_side_effects(storage, effects, execution_id, "dec-9")
    r2 = apply_resume_side_effects(storage, effects, execution_id, "dec-9")
    print("first:", r1, "second:", r2, "emails_sent:", effects.emails_sent)
    assert r1 == "applied" and r2 == "already_terminal"
    assert effects.emails_sent == 1
    print("ok: duplicate resume did not re-send email")


if __name__ == "__main__":
    main()
