"""节点级耗时采集（NodeTimingCallback）与 worker 落盘。

背景：执行状态里原本只有整条流程的 start_time/end_time，节点维度无任何时间
信息，执行详情页因此说不出「哪个节点慢」。本用例覆盖采集器的语义边界，以及
worker 是否真的把它写进了 ExecutionState.node_timings。
"""

import threading
import time

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("fakeredis")

from unittest.mock import MagicMock

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker
from plaita.server.node_timings import NodeTimingCallback
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class _Node:
    def __init__(self, node_id: str):
        self.id = node_id


class _Clock:
    """可推进的假时钟（秒）。"""

    def __init__(self, start: float = 1_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += seconds


class TestNodeTimingCallback:
    def test_is_a_full_flow_callback(self):
        """必须继承 FlowCallback：分发器对每个钩子逐个 getattr，缺一个就打一条
        「Error in on_flow_xxx callback ... has no attribute」告警（每个 flow 两条，
        2026-10-08 实测纯噪声）。"""
        from plaita.core.callback import FlowCallback

        assert issubclass(NodeTimingCallback, FlowCallback)
        cb = NodeTimingCallback()
        for hook in ("on_flow_start", "on_flow_end", "on_flow_suspend", "on_flow_resume"):
            getattr(cb, hook)(None)          # 基类 no-op，不得抛 AttributeError

    def test_records_duration_and_epoch_ms(self):
        clock = _Clock()
        cb = NodeTimingCallback(clock=clock)
        cb.on_node_start(None, _Node("a"))
        clock.tick(1.5)
        cb.on_node_end(None, _Node("a"))
        entry = cb.snapshot()["a"]
        assert entry["duration_ms"] == 1500
        assert entry["started_ms"] == 1_000_000
        assert entry["ended_ms"] == 1_001_500
        assert entry["attempts"] == 1
        assert entry["failed"] is False
        assert entry["started_at"] and entry["ended_at"]

    def test_loop_visits_accumulate_attempts_and_total(self):
        clock = _Clock()
        cb = NodeTimingCallback(clock=clock)
        for _ in range(3):
            cb.on_node_start(None, _Node("loop"))
            clock.tick(0.2)
            cb.on_node_end(None, _Node("loop"))
        entry = cb.snapshot()["loop"]
        assert entry["attempts"] == 3
        assert entry["duration_ms"] == 200  # 最后一次
        assert entry["total_duration_ms"] == 600

    def test_end_without_start_does_not_invent_duration(self):
        cb = NodeTimingCallback(clock=_Clock())
        cb.on_node_end(None, _Node("orphan"))
        entry = cb.snapshot()["orphan"]
        assert "duration_ms" not in entry
        assert entry["attempts"] == 1

    def test_failure_marks_entry_but_never_raises(self):
        cb = NodeTimingCallback(clock=_Clock())
        cb.on_node_start(None, _Node("bad"))
        cb.on_node_end(None, _Node("bad"), error=RuntimeError("boom"))
        assert cb.snapshot()["bad"]["failed"] is True

        # 无 id 的对象：内部异常被吞，不影响执行
        cb.on_node_start(None, object())
        cb.on_node_end(None, object())
        assert "bad" in cb.snapshot()

    def test_snapshot_is_a_copy(self):
        cb = NodeTimingCallback(clock=_Clock())
        cb.on_node_start(None, _Node("a"))
        cb.on_node_end(None, _Node("a"))
        snap = cb.snapshot()
        snap["a"]["duration_ms"] = 999
        assert cb.snapshot()["a"]["duration_ms"] != 999

    # ── 在跑节点必须可见（2026-10-10 沙箱误杀事故回归）────────────────────

    def test_snapshot_exposes_running_node_without_ended_at(self):
        """在跑节点也要进 snapshot，且形态是「有 started_at、无 ended_at」。

        事故（plaita#27/#31/#55/#32/#62）：``sandbox_agent`` 节点（impl）在 AGS
        沙箱里跑 35 分钟，期间宿主侧只调用了 ``on_node_start``——旧 ``snapshot()``
        只回 ``_timings``，于是 node_timings 里**看不到这个在跑节点**。keeper 的
        活性判据①（有 started 无 ended = 活证据）因此永不命中，落到判据③
        「末节点截止时间停滞超 1800s」→ 把健康 run 判死 cancel。

        本测试就是那条判据的宿主侧契约：在跑 = ``ended_at`` 为空字符串。
        """
        clock = _Clock()
        cb = NodeTimingCallback(clock=clock)
        cb.on_node_start(None, _Node("done"))
        clock.tick(2)
        cb.on_node_end(None, _Node("done"))
        clock.tick(30)
        cb.on_node_start(None, _Node("impl"))     # 沙箱节点开跑，不结束

        snap = cb.snapshot()
        assert "impl" in snap, "在跑节点必须在 node_timings 里可见（否则 keeper 会误杀）"
        impl = snap["impl"]
        assert impl["started_at"], "在跑节点必须有 started_at"
        assert not impl["ended_at"], "在跑节点的 ended_at 必须为空（= 活证据）"
        assert impl["ended_ms"] is None, "不得为在跑节点编造结束时间"

    def test_running_node_does_not_clobber_completed_record(self):
        """同一节点重跑：已完成记录优先，不被在跑态覆盖。"""
        clock = _Clock()
        cb = NodeTimingCallback(clock=clock)
        cb.on_node_start(None, _Node("a"))
        clock.tick(1)
        cb.on_node_end(None, _Node("a"))
        cb.on_node_start(None, _Node("a"))        # 第二轮在跑
        entry = cb.snapshot()["a"]
        assert entry["ended_at"], "已完成的节点不应因新一轮 start 而退回在跑态"

    def test_snapshot_running_entry_matches_keeper_predicate(self):
        """与 keeper ``worker_alive`` 判据① 的实际读法对齐（防两侧口径漂移）。

        keeper 读法（issue-keeper ``console_exec.worker_alive``）：
            if not v.get("started_at") or v.get("ended_at"): continue
            return True          # 有 started、无 ended → 活
        """
        cb = NodeTimingCallback(clock=_Clock())
        cb.on_node_start(None, _Node("impl"))
        snap = cb.snapshot()
        live = [nid for nid, v in snap.items()
                if v.get("started_at") and not v.get("ended_at")]
        assert live == ["impl"], "keeper 判据① 必须能从 snapshot 里认出在跑节点"


class TestExecutionStateCarriesTimings:
    def test_defaults_to_none_for_old_states(self):
        assert ExecutionState(execution_id="e", context={}).node_timings is None

    def test_model_dump_roundtrip(self):
        state = ExecutionState(
            execution_id="e",
            context={},
            node_timings={"a": {"duration_ms": 5, "attempts": 1}},
        )
        dumped = state.model_dump()
        assert dumped["node_timings"] == {"a": {"duration_ms": 5, "attempts": 1}}
        assert ExecutionState(**dumped).node_timings == {"a": {"duration_ms": 5, "attempts": 1}}


class TestWorkerPersistsTimings:
    def _flow(self):
        return {
            "flow_id": "timing-flow",
            "name": "计时流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "assign1"},
                {"id": "assign1", "type": "assignment", "output": {"step": 1}, "next": "assign2"},
                {"id": "assign2", "type": "assignment", "output": {"step": 2}, "next": "end"},
                {"id": "end", "type": "end", "output": "success"},
            ],
        }

    def test_start_flow_records_node_timings(self):
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(self._flow())
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=InMemoryEventBus(),
        )
        result = worker.start_flow(flow_id="timing-flow", params={}, version="1.0.0")
        execution_id = result["execution_id"]

        state = execution_storage.load_execution_state(execution_id)
        assert state is not None
        timings = state.node_timings
        assert timings, "落盘的执行状态必须带节点耗时"
        for node_id in ("start", "assign1", "assign2", "end"):
            assert node_id in timings, f"{node_id} 缺少耗时记录"
            entry = timings[node_id]
            assert entry["duration_ms"] >= 0
            assert entry["ended_ms"] >= entry["started_ms"]
            assert entry["attempts"] == 1

        # 终态落盘后回收采集器，避免长跑 worker 泄漏
        assert execution_id not in worker._node_timings

    def test_worker_without_execution_keeps_state_untouched(self):
        worker = FlowWorker(
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            event_bus=InMemoryEventBus(),
        )
        state = ExecutionState(execution_id="ghost", context={})
        worker._collect_node_timings("ghost", state)
        assert state.node_timings is None


def _progress_worker(storage) -> RedisFlowWorker:
    """``_publish_node_progress`` 挂在 RedisFlowWorker（与看门狗同层），
    构造参数与 tests/unit/test_wave12_cancellation.py 的工厂保持一致。"""
    import fakeredis

    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:node-timings-progress",
        execution_storage=storage,
        flow_storage=MemoryFlowStorage(),
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        lease_ttl_seconds=60,
        enable_registry=False,
        enable_redis_logging=False,
    )


class TestWatchdogPublishesRunningNode:
    """长节点（沙箱 sandbox_agent）期间的活性心跳——2026-10-10 五单误杀事故回归。

    事故链：落盘只发生在**节点边界**，而 impl 是单个 35 分钟长节点 →
    期间 snapshot() 从不被调用 → node_timings 停在进入 impl 前 →
    keeper 判据①（有 started 无 ended = 活证据）读不到 → 判据③判死 cancel。

    修复：看门狗续租成功后顺带发布在跑节点进度。
    """

    _worker = staticmethod(_progress_worker)

    def _running_state(self, storage, execution_id="exec-long"):
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            flow_version="1",
            status="running",
            context={},
            last_update_time="2020-01-01T00:00:00",
        )
        storage.save_execution_state(execution_id, state)
        return state

    def test_running_node_is_published_during_long_node(self):
        """在跑节点必须被写进执行状态，且 last_update_time 被刷新。"""
        storage = MemoryExecutionStorage()
        worker = self._worker(storage)
        eid = "exec-long"
        self._running_state(storage, eid)
        # 模拟：节点已开跑（on_node_start 已触发），但尚未结束
        worker._node_timings[eid] = NodeTimingCallback(clock=_Clock())
        worker._node_timings[eid].on_node_start(None, _Node("impl"))

        worker._publish_node_progress(eid)

        state = storage.load_execution_state(eid)
        assert state is not None
        timings = state.node_timings or {}
        assert "impl" in timings, "在跑节点必须落盘（否则 keeper 看不见、会误杀）"
        assert timings["impl"]["started_at"]
        assert not timings["impl"]["ended_at"], "在跑节点 ended_at 必须为空"
        assert state.last_update_time != "2020-01-01T00:00:00", "last_update_time 必须刷新"

    def test_publish_merges_and_keeps_existing_nodes(self):
        """合并语义：不得抹掉状态里已有的旧节点记录。"""
        storage = MemoryExecutionStorage()
        worker = self._worker(storage)
        eid = "exec-merge"
        state = self._running_state(storage, eid)
        state.node_timings = {"gates": {"started_at": "2020-01-01T00:00:00",
                                        "ended_at": "2020-01-01T00:00:01"}}
        storage.save_execution_state(eid, state)
        worker._node_timings[eid] = NodeTimingCallback(clock=_Clock())
        worker._node_timings[eid].on_node_start(None, _Node("impl"))

        worker._publish_node_progress(eid)

        timings = storage.load_execution_state(eid).node_timings
        assert "gates" in timings, "已有节点记录不能被抹掉"
        assert "impl" in timings

    def test_publish_skips_terminal_execution(self):
        """已终态的执行不再发布进度（避免把终态状态改回 running 语义）。"""
        storage = MemoryExecutionStorage()
        worker = self._worker(storage)
        eid = "exec-done"
        state = self._running_state(storage, eid)
        state.status = "completed"
        storage.save_execution_state(eid, state)
        worker._node_timings[eid] = NodeTimingCallback(clock=_Clock())
        worker._node_timings[eid].on_node_start(None, _Node("impl"))

        worker._publish_node_progress(eid)

        assert storage.load_execution_state(eid).node_timings is None

    def test_publish_never_raises_on_storage_failure(self):
        """观测路径：落盘炸了也不能打断正在跑的节点。"""
        storage = MagicMock()
        storage.load_execution_state.side_effect = RuntimeError("boom")
        worker = self._worker(storage)
        eid = "exec-boom"
        worker._node_timings[eid] = NodeTimingCallback(clock=_Clock())
        worker._node_timings[eid].on_node_start(None, _Node("impl"))
        worker._publish_node_progress(eid)      # 不得抛

    def test_publish_is_noop_without_timing_collector(self):
        worker = self._worker(MemoryExecutionStorage())
        worker._publish_node_progress("nonexistent")   # 不得抛

    def test_publish_ignores_false_save_result(self):
        """后端吞异常的失败形态（``save_execution_state`` 返回 False）：
        心跳是观测路径，只告警、不抛、不编造成功。"""

        class _FalseSaveStorage(MemoryExecutionStorage):
            def save_execution_state(self, execution_id, state):
                return False

        storage = _FalseSaveStorage()
        worker = self._worker(storage)
        eid = "exec-false-save"
        MemoryExecutionStorage.save_execution_state(storage, eid, ExecutionState(
            execution_id=eid, flow_id="f1", flow_version="1",
            status="running", context={},
        ))
        worker._node_timings[eid] = NodeTimingCallback(clock=_Clock())
        worker._node_timings[eid].on_node_start(None, _Node("impl"))

        worker._publish_node_progress(eid)      # 不得抛

        assert storage.load_execution_state(eid).node_timings is None


class TestHeartbeatLockWaitIsBounded:
    """心跳等状态写锁**有界**（2026-10-10 评审）。

    ``_persist_state_or_raise`` 把 ``_state_write_lock`` 一路持到沙箱回收
    （``_release_sandboxes``）结束，而看门狗是**串行**过本机所有活跃执行的一条
    线程——无界等锁会让一次慢回收把**所有**执行的续租一起拖住，续租预算只有
    TTL（默认 ~120s），拖过即失租双跑。心跳只是观测，跳过一周期无害。
    """

    _worker = staticmethod(_progress_worker)

    def test_publish_skips_instead_of_blocking_on_held_lock(self):
        storage = MemoryExecutionStorage()
        worker = self._worker(storage)
        eid = "exec-lock-held"
        state = ExecutionState(
            execution_id=eid, flow_id="f1", flow_version="1",
            status="running", context={},
        )
        storage.save_execution_state(eid, state)
        worker._node_timings[eid] = NodeTimingCallback(clock=_Clock())
        worker._node_timings[eid].on_node_start(None, _Node("impl"))

        # 另一线程持锁（模拟推进写正在做沙箱回收）：RLock 可重入，必须换线程
        lock = worker._state_write_lock(eid)
        held = threading.Event()
        release = threading.Event()

        def holder():
            lock.acquire()
            held.set()
            release.wait(10)
            lock.release()

        holder_thread = threading.Thread(target=holder, daemon=True)
        holder_thread.start()
        assert held.wait(5), "持锁线程未就绪"

        from plaita.server.flow_worker import HEARTBEAT_LOCK_TIMEOUT_SECONDS

        try:
            started = time.monotonic()
            worker._publish_node_progress(eid)
            elapsed = time.monotonic() - started
        finally:
            release.set()
            holder_thread.join(timeout=5)

        assert elapsed < HEARTBEAT_LOCK_TIMEOUT_SECONDS + 5, (
            f"心跳必须放弃等锁而不是无限阻塞，实测 {elapsed:.2f}s"
        )
        assert storage.load_execution_state(eid).node_timings is None, (
            "等不到锁的这一周期必须整跳（不得半写）"
        )

        # 锁释放后的下一周期照常发布（跳过是暂时的，不是把锁弄坏）
        worker._publish_node_progress(eid)
        assert "impl" in (storage.load_execution_state(eid).node_timings or {})


class _GatedStorage(MemoryExecutionStorage):
    """心跳线程 load 时踩一脚刹车：把「读-改-写」窗口拉长成可观测。

    评审复现手法就是这个窗口——让终态写落在心跳的 load 与 save 之间。
    """

    def __init__(self):
        super().__init__()
        self.in_load = threading.Event()
        self.release = threading.Event()

    def load_execution_state(self, execution_id):
        state = super().load_execution_state(execution_id)
        if threading.current_thread().name == "heartbeat":
            self.in_load.set()
            self.release.wait(5)
        return state


class TestHeartbeatSerializesWithStateWrites:
    """心跳与推进写是同一行执行状态的**两个写者**，必须串行。

    2026-10-10 评审 blocker：``_publish_node_progress`` 是「load → 合并
    node_timings → save」的**整行写回**，而消费线程可以在它的 load 与 save
    之间落终态写——终态被整行回滚成 running + **旧 checkpoint** + 新
    ``last_update_time``：既不会被 keeper 回收（open node + 新鲜
    last_update_time 恰是它的「活着」判据），后续 resume 还会从旧 checkpoint
    重放节点副作用（消息已 ack，没有写者会再回来修）。
    """

    def _worker(self, storage):
        return TestWatchdogPublishesRunningNode()._worker(storage)

    def _running_with_open_node(self, storage, eid="exec-race"):
        TestWatchdogPublishesRunningNode()._running_state(storage, eid)
        worker = self._worker(storage)
        worker._node_timings[eid] = NodeTimingCallback(clock=_Clock())
        worker._node_timings[eid].on_node_start(None, _Node("impl"))
        return worker

    def test_terminal_write_is_not_rolled_back_by_heartbeat(self):
        storage = _GatedStorage()
        worker = self._running_with_open_node(storage)
        eid = "exec-race"

        heartbeat = threading.Thread(
            target=worker._publish_node_progress, args=(eid,), name="heartbeat"
        )
        heartbeat.start()
        assert storage.in_load.wait(5), "心跳没有进入 load（测试编排失效）"

        terminal_done = threading.Event()

        def consume_thread_terminal_write():
            state = storage.load_execution_state(eid)
            state.status = "completed"
            state.context = {"$NODE": {"impl": {"status": "success"}}}
            state.end_time = "2026-10-10T00:00:00"
            worker._persist_state_or_raise(eid, state, "completed")
            terminal_done.set()

        writer = threading.Thread(
            target=consume_thread_terminal_write, name="consume"
        )
        writer.start()
        assert not terminal_done.wait(0.3), (
            "终态写与心跳未串行：心跳会在终态写之后落盘，把 completed + "
            "checkpoint 整行回滚（假活僵尸 + 断点回退）"
        )
        storage.release.set()
        heartbeat.join(5)
        writer.join(5)
        assert not heartbeat.is_alive() and not writer.is_alive()

        final = storage.load_execution_state(eid)
        assert final.status == "completed", "心跳不得把终态写回滚"
        assert final.context == {"$NODE": {"impl": {"status": "success"}}}, (
            "心跳不得把 checkpoint 回滚成旧值"
        )
        assert final.end_time == "2026-10-10T00:00:00"

    def test_terminal_row_skipped_even_if_heartbeat_runs_after(self):
        """心跳排在终态写之后：读到终态即收手（不刷 last_update_time）。"""
        storage = MemoryExecutionStorage()
        worker = self._running_with_open_node(storage)
        eid = "exec-race"

        state = storage.load_execution_state(eid)
        state.status = "completed"
        state.end_time = "2026-10-10T00:00:00"
        state.last_update_time = "2026-10-10T00:00:00"
        storage.save_execution_state(eid, state)

        worker._publish_node_progress(eid)

        final = storage.load_execution_state(eid)
        assert final.status == "completed"
        assert final.last_update_time == "2026-10-10T00:00:00"
        assert final.node_timings is None, "终态行不得被心跳补上在跑节点"


class TestHeartbeatCarriesWatchContext:
    """看门狗线程没有本执行的租户/fence 世代上下文，心跳必须显式带上。

    不带 ⇒ 默认租户 namespace（多租户下读不到行、心跳整条失效）+ 无世代 CAS
    的裸写（跨进程绕开 fencing 世代门）。
    """

    def test_publish_applies_and_restores_fence_token(self):
        from plaita.storage.fenced import current_fence_token

        seen = []

        class _Capturing(MemoryExecutionStorage):
            def save_execution_state(self, execution_id, state):
                seen.append(current_fence_token())
                return super().save_execution_state(execution_id, state)

        storage = _Capturing()
        worker = TestHeartbeatSerializesWithStateWrites()._running_with_open_node(storage)
        seen.clear()   # 只关心心跳那一次写

        worker._publish_node_progress("exec-race", fence_token=7)

        assert seen == [7], "心跳写必须带 fence 世代（否则绕开 fenced CAS）"
        assert current_fence_token() is None, "心跳结束后必须复位世代（线程复用）"

    def test_tenant_scoped_execution_is_reachable_from_watchdog(self):
        """多租户：心跳必须落在执行所属租户的 namespace。

        看门狗线程的 ContextVar 是 default；`TenantRoutingExecutionStorage`
        按它选后端——不带 ``tenant_id`` 时非 default 租户的执行**读都读不到**，
        心跳整条失效（keeper 照样误杀长节点）。
        """
        pytest.importorskip("lupa")
        import fakeredis

        from plaita.server.tenant_context import (
            TenantRoutingExecutionStorage,
            reset_current_tenant,
            set_current_tenant,
        )

        fake = fakeredis.FakeStrictRedis(decode_responses=True)
        storage = TenantRoutingExecutionStorage(client=fake)
        token = set_current_tenant("acme")
        try:
            storage.save_execution_state(
                "exec-t",
                ExecutionState(
                    execution_id="exec-t", flow_id="f1", status="running",
                    context={}, last_update_time="2020-01-01T00:00:00",
                ),
            )
        finally:
            reset_current_tenant(token)

        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name="test:node-timings-tenant",
            execution_storage=storage,
            flow_storage=MemoryFlowStorage(),
            redis_client=fake,
            lease_ttl_seconds=60,
            enable_registry=False,
            enable_redis_logging=False,
        )
        worker._node_timings["exec-t"] = NodeTimingCallback(clock=_Clock())
        worker._node_timings["exec-t"].on_node_start(None, _Node("impl"))

        # 看门狗线程的 ContextVar 是 default → 必须显式带租户
        worker._publish_node_progress("exec-t", tenant_id="acme")

        raw = fake.get("plaita:acme:execution:exec-t")
        assert raw and "impl" in raw, "心跳必须写进执行所属租户的 namespace"

    def test_watchdog_passes_fence_token_and_tenant(self):
        """续租成功后调心跳时，登记进 `_lease_watch` 的世代/租户原样传入。"""
        from plaita.server.execution_lease import NullExecutionLease

        storage = MemoryExecutionStorage()
        worker = TestHeartbeatSerializesWithStateWrites()._running_with_open_node(storage)
        # RedisFlowWorker 默认租约是 Redis 实现（这里没有真的 acquire 过），
        # 换 Null 实现让 renew 返回 True —— 本用例只钉「传参」这一件事。
        worker.execution_lease = NullExecutionLease()
        worker._register_lease_watch(
            "exec-race", "holder:3", MagicMock(), fence_token=3
        )
        calls = []

        def _fake_publish(execution_id, **kwargs):
            calls.append((execution_id, kwargs))

        worker._publish_node_progress = _fake_publish  # type: ignore[assignment]
        worker._watchdog_renew_once()

        assert calls == [("exec-race", {"fence_token": 3, "tenant_id": "default"})]
