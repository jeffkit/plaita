"""#310：定义缺失终态化助手的读盘失败分支——「交还重投」声明与实现脱钩。

机制（issue #310，A1 r134 实弹）：``_terminalize_definition_missing`` 的
state=None 现读分支在读盘瞬断时仅 WARNING + ``return``（零终态化），调用方
（start/resume 入口）随即无条件 ``raise FlowDefinitionMissingError``——刻意
继承 ValueError ⇒ run() 按畸形消息 poison ack。注释自称「读盘瞬断交还重投」，
实际消息被吃、执行行永驻非终态（start=queued 占位 / resume=suspended），
start 准入名额靠 24h TTL 兜底。B123-01（``05e8623``）把 resume 面切到
state=None 现读后此分支新激活，但其 6 格负控无读失败格，窗口由此漏网。

修法裁量（方案①，与注释对齐）：读失败分支改抛 ``StatePersistError``
（RuntimeError 族，非 ValueError）——run() 兜底不 ack、留 pending 等
at-least-once 重投，「交还重投」成真；重投后读盘恢复且定义仍缺失 → 走正常
终态化。不选「读失败也具名终态化」：现读防线的意义就在不凭空构造行覆写，
读不到行时无从安全终态化；且瞬断被定成 error 违反瞬态/持久分界（#80
家族纪律：StatePersistError 只表示可重试失败）。

负控（修复前全红）：
  格①（助手直格）：读失败 ⇒ 必须 StatePersistError 上抛（非 ValueError）+
  行原样（不终态化、不塞 error）；
  格②（run() 全链·ack 判定）：交付一读失败 ⇒ 消息**未被 ack**、零 poison
  （重投可见）；交付二（重投、读恢复、定义仍缺失）⇒ 终态化 error 落行
  （机器可判身份 ``flow_definition_missing``）后消息才 ack。
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import (
    FlowDefinitionMissingError,
    RedisFlowWorker,
    StatePersistError,
)
from plaita.server.task_queue import StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-b310"


def _seed(storage: MemoryExecutionStorage, status: str = "suspended") -> None:
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status=status,
            tenant_id="",
            context={"$LAST_NODE": "node1"},
        ),
    )


def _arm_read_failure(
    storage: MemoryExecutionStorage, *, once: bool = False, prearm: bool = False
):
    """注入读盘瞬断：``load_execution_state`` 抛 OSError。返回原读函数。

    ``once=True`` 时只断一次（模拟瞬断：本轮读失败交还重投，下一轮重投读
    盘恢复）——格② 的两段交付依赖这个时序。``prearm=True`` 直接置位（格①
    直格：不经过 resume 前置读，助手进函数即读盘失败）。
    """
    original_load = storage.load_execution_state
    armed = {"on": bool(prearm)}

    def gated_load(execution_id: str):
        if armed["on"]:
            if once:
                armed["on"] = False  # 一次性瞬断，重投后读恢复
            raise OSError("injected transient read failure (#310)")
        return original_load(execution_id)

    storage.load_execution_state = gated_load  # type: ignore[method-assign]
    storage._b310_armed = armed  # type: ignore[attr-defined]
    storage._b310_original_load = original_load  # type: ignore[attr-defined]
    return original_load


def test_terminalize_read_failure_raises_transient_row_untouched():
    """格①（直格·RED 主格）：读失败 ⇒ StatePersistError 上抛，行原样。

    修复前：WARNING + return，无异常上抛 ⇒ DID NOT RAISE（红）。
    修复后：StatePersistError（RuntimeError 族）上抛，非 ValueError ⇒
    run() 不会 poison ack；行仍 suspended、无 error（交还重投，不就地定罪）。
    """
    storage = MemoryExecutionStorage()
    _seed(storage)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue310",
        execution_storage=storage,
        flow_storage=MemoryFlowStorage(),
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        enable_registry=False,
        enable_redis_logging=False,
        read_block_ms=100,
    )
    _arm_read_failure(storage, prearm=True)

    with pytest.raises(StatePersistError) as excinfo:
        worker._terminalize_definition_missing(EXEC, "f1", "1", None, phase="test")

    assert not isinstance(excinfo.value, ValueError), (
        f"读失败载体必须是非 ValueError 瞬态（run() 不 ack 重投），实际 "
        f"{type(excinfo.value).__name__} ⊂ ValueError ⇒ poison ack 吞消息（#310 病灶）"
    )
    saved = storage._b310_original_load(EXEC)  # type: ignore[attr-defined]
    assert saved.status == "suspended", (
        f"读失败交还重投，不得就地终态化，实际 {saved.status!r}"
    )
    assert saved.error is None, "交还重投路径不得塞 error"


class RecordingQueue:
    """替代 RedisStreamTaskQueue 的假队列（同款 test_reviewfix_d1）。"""

    max_deliveries = 5
    consumer_name = "test-consumer"

    def __init__(self, tasks):
        self._tasks = list(tasks)
        self.acked = []
        self.poison_count = 0
        self.failed_count = 0
        self.dead_lettered = []
        self.lease_conflicts = 0
        self.on_poison = None
        self.on_failed = None
        self.on_dead_letter = None

    def ensure_group(self):
        pass

    def read(self, block_ms=0):
        if self._tasks:
            return self._tasks.pop(0)
        return None

    def ack(self, message_id):
        self.acked.append(message_id)

    def note_poison(self):
        self.poison_count += 1
        if self.on_poison:
            self.on_poison()

    def note_failed(self):
        self.failed_count += 1
        if self.on_failed:
            self.on_failed()

    def note_lease_conflict(self):
        self.lease_conflicts += 1

    def dead_letter(self, task, *, reason):
        self.dead_lettered.append((task.message_id, reason))
        self.ack(task.message_id)
        if self.on_dead_letter:
            self.on_dead_letter()

    def dead_letter_if_allowed(self, task, *, reason):
        self.dead_letter(task, reason=reason)
        return True


def test_resume_read_failure_not_acked_redelivery_terminalizes():
    """格②（run() 全链）：交付一不 ack（重投可见），交付二终态化后才 ack。

    时序：msg-1（交付一）→ resume 预读成功 → 取定义缺失 → 助手现读盘
    **瞬断** → 修法后 StatePersistError 上抛 ⇒ run() 兜底不 ack、note_failed；
    msg-2（交付二=重投模拟）→ 读盘恢复 → 定义仍缺失 → 助手正常终态化 error
    → FlowDefinitionMissingError poison ack。执行行以机器可判身份落终态，
    「交还重投」注释与实现对齐。

    修复前：交付一直接 FlowDefinitionMissingError（ValueError）⇒ poison ack
    ⇒ msg-1 出现在 acked、poison_count=1（红）。
    """
    storage = MemoryExecutionStorage()
    _seed(storage)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue310",
        execution_storage=storage,
        flow_storage=MemoryFlowStorage(),
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        enable_registry=False,
        enable_redis_logging=False,
        read_block_ms=100,
    )
    queue = RecordingQueue([])
    worker._get_task_queue = lambda: queue
    _arm_read_failure(storage, once=True)
    armed = storage._b310_armed  # type: ignore[attr-defined]
    def_calls = {"n": 0}

    def missing_get_flow_definition(flow_id, version=None):
        def_calls["n"] += 1
        if def_calls["n"] == 1:
            # 仅交付一置位（一次性瞬断）；交付二（重投）读盘恢复。
            armed["on"] = True
        raise FlowDefinitionMissingError(
            f"injected: flow {flow_id}@{version} missing (#310)"
        )

    worker.get_flow_definition = missing_get_flow_definition  # type: ignore[method-assign]

    body = {
        "type": "resume",
        "flow_id": "f1",
        "execution_id": EXEC,
        "resume_type": "event",
        # A69-04：合成载体（非数字 message_id）需带年龄证据。
        "timestamp": datetime.now().isoformat(),
    }
    queue._tasks.append(
        StreamTask(message_id="msg-310-d1", body=dict(body), delivery_count=1)
    )
    queue._tasks.append(
        StreamTask(message_id="msg-310-d2", body=dict(body), delivery_count=2)
    )

    # 交付一以 note_failed 收场（不 ack）→ 循环继续 → 交付二 poison ack 收场。
    done = {"failed": False}

    def on_failed():
        done["failed"] = True

    def on_poison():
        worker.stop()

    queue.on_failed = on_failed
    queue.on_poison = on_poison

    import threading

    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    thread.join(timeout=10.0)
    assert not thread.is_alive(), "run() 未在交付二 poison 后退出"
    assert done["failed"], "交付一未走 note_failed（未触发瞬态不 ack 路径）"

    assert queue.acked == ["msg-310-d2"], (
        f"交付一（读失败）的消息必须留 pending 重投，实际 acked={queue.acked!r}"
        f"（poison ack 吞掉唯一恢复载体，#310 病灶）"
    )
    assert queue.poison_count == 1, (
        f"只有交付二（终态化后）允许 poison，实际 poison_count={queue.poison_count}"
    )
    assert queue.failed_count == 1
    assert queue.dead_lettered == [], "首次失败不应进 DLQ"

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "error", (
        f"重投后定义仍缺失必须终态化，实际 {saved.status!r}"
    )
    assert saved.error is not None
    assert saved.error.get("error_type") == "flow_definition_missing"
    assert saved.end_time
