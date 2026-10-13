"""机器亲和闸（路线二首版）：worker 不领「路径不在本机」的任务。

背景（plaita#41 双机实证）：任务参数 repo/run_dir 是派发方机器的绝对路径。
本机抢到就跑 → OSError 路径不存在 → 烧 delivery 配额 → 死信。本闸让不亲和的
worker 礼貌让出（不 ack、留 pending），交给持有该路径的 worker。

红线：宽松优先——拿不到路径/相对路径/路径存在 → 一律放行，宁可多跑一次失败，
不可误拦本机该跑的任务。
"""
from __future__ import annotations

import os
import tempfile

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("redis")
pytest.importorskip("fakeredis")

import fakeredis  # noqa: E402

from plaita.server.flow_worker import RedisFlowWorker, TaskNotForThisWorker  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402


def _worker():
    # 亲和闸挂在 RedisFlowWorker（消费入口所在类）
    return RedisFlowWorker.__new__(RedisFlowWorker)


def test_absolute_missing_repo_is_mismatch():
    w = _worker()
    msg = {"type": "start", "params": {"repo": "/nonexistent/path/to/repo-xyz"}}
    reason = w._detect_affinity_mismatch(msg)
    assert reason is not None and "repo" in reason


def test_existing_repo_is_affine():
    w = _worker()
    with tempfile.TemporaryDirectory() as d:
        msg = {"type": "start", "params": {"repo": d}}
        assert w._detect_affinity_mismatch(msg) is None


def test_no_params_is_affine():
    w = _worker()
    assert w._detect_affinity_mismatch({"type": "start"}) is None
    assert w._detect_affinity_mismatch({"type": "start", "params": {}}) is None


def test_relative_path_is_affine():
    """相对路径/标识符无法判定 → 放行（宽松原则）。"""
    w = _worker()
    assert w._detect_affinity_mismatch({"type": "start", "params": {"repo": "plaita"}}) is None


def test_run_dir_probes_parent_repo():
    """run_dir 首次运行时可能尚未建，查其父（repo/.flowcast/runs）已存在即放行。"""
    w = _worker()
    with tempfile.TemporaryDirectory() as d:
        # repo 存在，run_dir 尚不存在 → 应亲和（首次运行）
        msg = {"type": "start", "params": {
            "repo": d,
            "run_dir": f"{d}/.flowcast/runs/pipeline-1-2026",
        }}
        assert w._detect_affinity_mismatch(msg) is None


def test_resume_message_not_checked():
    """resume 任务不做亲和判（路径已由首次 start 验过）。"""
    w = _worker()
    msg = {"type": "resume", "params": {"repo": "/nonexistent/xyz"}}
    assert w._detect_affinity_mismatch(msg) is None


def test_dispatch_raises_not_for_this_worker():
    """消费入口：不亲和 → 抛 TaskNotForThisWorker（run() 据此不 ack 留 pending）。"""
    w = _worker()
    with pytest.raises(TaskNotForThisWorker):
        w._dispatch_task(
            {"type": "start", "flow_id": "f", "params": {"repo": "/nonexistent/xyz"}}
        )


def test_affinity_disable_switch(monkeypatch):
    """PLAITA_DISABLE_AFFINITY=1 → 不拦（不分路径直接进派发）。"""
    from plaita.server import flow_worker as fw
    monkeypatch.setenv("PLAITA_DISABLE_AFFINITY", "1")
    assert fw._affinity_disabled() is True
    w = _worker()
    # 关了闸，不亲和的也会进 _dispatch_task（后续失败与我们无关，只要不是
    # TaskNotForThisWorker 即可）
    with pytest.raises(Exception) as ei:
        w._dispatch_task({"type": "start", "flow_id": "f",
                          "params": {"repo": "/nonexistent/xyz"}})
    assert not isinstance(ei.value, TaskNotForThisWorker)


def _real_redis_or_skip():
    """`test_spin_terminates_in_simulated_two_host_loop` 专用：**真实 Redis**。

    为什么不能用 fakeredis（2026-10-13 实测）：
    该用例依赖「**循环反复 read 到新副本**」这条语义，而 **fakeredis 2.36.2 的
    consumer-group 游标有缺陷** —— ack 原条 + 入队新副本后，group 的
    `last-delivered-id` 落后于流中最新 id，但 fakeredis 报 `lag: 0`，
    于是 `read('>')` 认为"无新消息"返回 `None`，循环**第一轮就 break**
    ⇒ `rounds` 恒为 0~1 ⇒ `assert rounds <= 上限` **永远平凡通过**，
    **这条守卫实际上没有在看护任何东西**（`#86` 是 P1 且未修）。

    真实 redis-server 下同一段流程正常（实测：第二次 read 仍能取到，
    `pending=1`）。本机 `redis-server` 可用（v8.0.2）。

    取真实实例：优先 `PLAITA_TEST_REDIS_URL`，否则试 `127.0.0.1:6379/15`；
    都不可用则 **skip 并说明**（绝不静默退回 fakeredis —— 那等于假装有守卫）。
    """
    import os
    import redis as _redis

    url = os.environ.get("PLAITA_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")
    try:
        c = _redis.Redis.from_url(url, decode_responses=True,
                                  socket_connect_timeout=2)
        c.ping()
    except Exception as exc:  # noqa: BLE001
        import pytest
        pytest.skip(
            f"需要真实 Redis 才能验证交接循环有界性（fakeredis 游标有缺陷，"
            f"会让该断言平凡通过）：{url} 不可达（{exc}）。"
            f"起一个：redis-server --port 6379 或设 PLAITA_TEST_REDIS_URL"
        )
    c.flushdb()
    return c


def _full_worker(redis_client, storage):
    """**真构造**的 worker（有 __init__ 的全部登记表）。

    终态化路径要经 `_persist_state_or_raise`（node_timings / sandbox_callbacks /
    状态写锁等），`__new__` 骨架 worker 缺这些表。亲和判定本身不需要，但
    「达上限即终态化」的用例必须用真 worker，否则测的是骨架而非生产路径。
    """
    from plaita.server.flow_worker import RedisFlowWorker as _W
    return _W(
        redis_url="redis://localhost:6379/15",
        queue_name="q:test",
        execution_storage=storage,
        flow_storage=MemoryFlowStorage(),
        redis_client=redis_client,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


class TestAffinityHandover:
    """交接：不亲和任务 ack 原条 + 重入队新副本（delivery 归 1），不死信。

    回归 2026-10-06 双机实测缺陷：只「留 pending」会被对端反复 XCLAIM 虚增
    delivery，5 轮触顶 → 误死信。
    """

    def _worker_with_redis(self):
        import fakeredis
        w = RedisFlowWorker.__new__(RedisFlowWorker)
        w.redis_client = fakeredis.FakeRedis(decode_responses=True)
        w.queue_name = "q:test"
        return w

    class _FakeQueue:
        def __init__(self):
            self.acked = []
            self.conflicts = 0
        def ack(self, mid):
            self.acked.append(mid)
        def note_lease_conflict(self):
            self.conflicts += 1

    def test_handover_acks_and_reenqueues(self):
        w = self._worker_with_redis()
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f",
                "params": {"repo": "/nonexistent/xyz"}}
        task = type("T", (), {"message_id": "1-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("no affinity"))
        assert q.acked == ["1-0"], "原条应被 ack（移出 PEL，防对端再抢）"
        assert w.redis_client.xlen("q:test") == 1, "应重入队恰好一份新副本"

    def test_handover_is_idempotent_no_pingpong(self):
        """同一消息二次让出 → 计数到 2，仍 < 上限 ⇒ 仍交接（有界，非布尔抑制）。

        旧实现在此「第二次不再重入队」（布尔标记 30s 命中）；新实现改为**计数**：
        上限内的每次交接都照做，超上限才停。语义差异是刻意的——布尔标记的
        TTL(30s) 短于回收周期(60s)，抑制间歇失效；计数 + 长 TTL 才能真正有界。
        """
        w = self._worker_with_redis()
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-1",
                "params": {"repo": "/nonexistent/xyz"}}
        task = type("T", (), {"message_id": "1-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        assert w.redis_client.xlen("q:test") == 1
        # 第二次（模拟新副本又被本机抢到）——上限内仍交接
        task2 = type("T", (), {"message_id": "2-0", "body": body})()
        w._handover_non_affine(task2, q, RuntimeError("x"))
        assert w.redis_client.xlen("q:test") == 2, "上限内应继续交接（计数而非布尔）"
        assert q.acked == ["1-0", "2-0"]

    def test_handover_without_body_leaves_pending(self):
        w = self._worker_with_redis()
        q = self._FakeQueue()
        task = type("T", (), {"message_id": "1-0", "body": None})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        assert q.acked == [] and w.redis_client.xlen("q:test") == 0

    def test_handover_counter_key_carries_ttl(self):
        """计数键必须带 **长于回收周期** 的 TTL（claim_min_idle_ms=60s）。

        旧实现是 `SET key 1 EX 30`：30s < 60s ⇒ 同一任务绕一圈回来时计数必已
        过期、重置为 1 ⇒ 「有界」形同虚设（事故时约 47% 抑制未命中）。
        """
        from plaita.server.task_queue import DEFAULT_CLAIM_MIN_IDLE_MS
        w = self._worker_with_redis()
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-ttl",
                "params": {"repo": "/nonexistent/xyz"}}
        task = type("T", (), {"message_id": "1-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        key = w._affinity_handover_key(body)
        ttl = w.redis_client.ttl(key)
        assert ttl > DEFAULT_CLAIM_MIN_IDLE_MS / 1000.0, (
            f"计数键 TTL={ttl}s 必须 > 回收周期 "
            f"{DEFAULT_CLAIM_MIN_IDLE_MS / 1000.0}s，否则计数会在绕回前过期"
        )
        assert ttl <= RedisFlowWorker.AFFINITY_HANDOVER_TTL_SECONDS

    def test_handover_counter_key_keyed_by_execution_id(self):
        """计数键按 execution_id（全局稳定），不是 body 指纹/run_dir。

        新副本**原样重投同一 body** ⇒ execution_id 不变 ⇒ 能跨「新 msg id」
        连续累计——这正是旧实现缺失的维度。
        """
        w = self._worker_with_redis()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-k",
                "params": {"repo": "/x"}}
        assert "exec-k" in w._affinity_handover_key(body)

    def test_handover_counter_survives_new_message_ids(self):
        """无界性根因回归：N 次交接后，队列里每条副本 times_delivered 仍为 1。

        这正是 `max_deliveries=5` **永不触发**的证明——因为 `enqueue_task`
        每次都铸新 msg id。故队列层**不可能**提供上界，上界只能由 worker 侧
        计数（`AFFINITY_HANDOVER_MAX`）承担。此用例把该事实钉死：若哪天有人
        以为 delivery 计数能兜住交接，这条会失败。
        """
        from plaita.server.task_queue import RedisStreamTaskQueue

        w = self._worker_with_redis()
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-unbounded",
                "params": {"repo": "/nonexistent/xyz"}}

        # 用**真队列**（fakeredis 后端）跑满上限内的交接，观察 delivery
        real_q = RedisStreamTaskQueue(
            w.redis_client, "q:test", consumer_name="c1",
        )
        real_q.ensure_group()
        for i in range(RedisFlowWorker.AFFINITY_HANDOVER_MAX):
            task = type("T", (), {"message_id": f"{i}-0", "body": body})()
            w._handover_non_affine(task, real_q, RuntimeError("x"))

        entries = w.redis_client.xrange("q:test")
        assert len(entries) == RedisFlowWorker.AFFINITY_HANDOVER_MAX
        # 每条都是全新 msg id ⇒ 从未被投递过 ⇒ delivery 恒为 1
        assert len({msg_id for msg_id, _f in entries}) == len(entries), (
            "每次交接都是**全新 msg id**"
        )
        assert real_q.max_deliveries == 5, "队列层上限仍是 5（本用例证明它够不着）"
        pending = w.redis_client.xpending("q:test", real_q.group_name)
        assert pending["pending"] == 0, "交接会 ack 原条，故 PEL 里没有累积"
        # 且**没有任何一条**副本进过消费者的手 ⇒ times_delivered 恒为 1
        # （无消费者读过 = 每条都是「首投」，max_deliveries 永远够不着）
        assert real_q.read(block_ms=1) is not None
        assert w.redis_client.xpending(
            "q:test", real_q.group_name)["pending"] == 1
        rec = w.redis_client.xpending_range("q:test", real_q.group_name, "-", "+", 10)
        assert rec and all(r["times_delivered"] == 1 for r in rec), (
            "新副本 delivery 恒为 1 ⇒ max_deliveries=5 永不触发 ⇒ 队列层无界"
        )

    def test_handover_bounded_then_terminalizes(self):
        """**有界性**：预置计数已达上限 ⇒ 单轮交接不再 ack/重投，改成终态化。

        照 `tests/unit/test_worker_node_retry.py:342` 的先例——预置计数键、
        断言**单轮**行为，不跑满预算。
        """
        import fakeredis
        from plaita.storage.memory import MemoryExecutionStorage

        from plaita.storage.base import ExecutionState

        import fakeredis
        storage = MemoryExecutionStorage()
        w = _full_worker(fakeredis.FakeRedis(decode_responses=True), storage)
        storage.save_execution_state("exec-cap", ExecutionState(
            execution_id="exec-cap", flow_id="f", status="running", context={},
        ))
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-cap",
                "params": {"repo": "/nonexistent/xyz"}}
        # 预置已达上限（下一轮 INCR 后 = MAX+1）
        w.redis_client.set(w._affinity_handover_key(body),
                           str(RedisFlowWorker.AFFINITY_HANDOVER_MAX))

        task = type("T", (), {"message_id": "9-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("无人亲和"))

        assert q.acked == [], "达上限不得 ack（ack 会让消息凭空消失）"
        assert w.redis_client.xlen("q:test") == 0, "达上限不得再重投（终止自旋）"
        state = storage.load_execution_state("exec-cap")
        assert state.status == "error", "达上限必须终态化（可观测，不再空转）"
        assert state.error["type"] == "AffinityHandoverExhausted"
        assert state.error["affinity_handovers"] == (
            RedisFlowWorker.AFFINITY_HANDOVER_MAX + 1
        )
        assert state.end_time

    def test_handover_cap_exceeded_does_not_overwrite_terminal_state(self):
        """达上限时若执行**已终态** ⇒ 不覆写状态，且**ack 消费**该死信。

        ack 的理由（2026-10-11 生产实测补强）：执行已终态 ⇒ 该消息**永远不可能**
        再被处理，留在 PEL 只会被对端每 `claim_min_idle_ms`(60s) 反复 XCLAIM
        （实测 count 5→6→7→8 递增、日志持续刷），是「有界但不停」的噪声。
        ack 是有损的，但**仅对已终态消息**使用 ⇒ 不会丢任何活工作。
        """
        from plaita.storage.base import ExecutionState
        from plaita.storage.memory import MemoryExecutionStorage

        import fakeredis
        storage = MemoryExecutionStorage()
        w = _full_worker(fakeredis.FakeRedis(decode_responses=True), storage)
        storage.save_execution_state("exec-done", ExecutionState(
            execution_id="exec-done", flow_id="f", status="completed", context={},
        ))
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-done",
                "params": {"repo": "/nonexistent/xyz"}}
        w.redis_client.set(w._affinity_handover_key(body), "99")
        task = type("T", (), {"message_id": "9-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        assert q.acked == ["9-0"], "已终态 ⇒ ack 消费死信（终止 PEL 自旋）"
        assert w.redis_client.xlen("q:test") == 0, "且不再重投"
        assert storage.load_execution_state("exec-done").status == "completed", "不覆写状态"

    def test_handover_exhausted_without_execution_id_leaves_pending(self):
        """无 execution_id（老消息/畸形消息）达上限 ⇒ 留 pending，绝不丢消息。"""
        w = self._worker_with_redis()
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f",
                "params": {"repo": "/nonexistent/xyz"}}
        key = w._affinity_handover_key(body)
        w.redis_client.set(key, str(RedisFlowWorker.AFFINITY_HANDOVER_MAX))
        task = type("T", (), {"message_id": "9-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        assert q.acked == [], "无 id 可终态化 ⇒ 不 ack（消息必须留在 PEL）"
        assert w.redis_client.xlen("q:test") == 0, "但也不再重投（终止自旋）"

    # ── fail-closed：计数不可用 ⇒ 界不可用 ⇒ 不得交接 ──────────────────────
    #
    # 2026-10-11 独立验收者的**部署阻断级**发现：修复第一版在计数自增失败时
    # `return 1`（「保守不误杀」）。但本计数是自锁场景下**唯一**的终止条件
    # （`max_deliveries` 因「新副本 delivery 恒为 1」够不着）⇒ count 恒为 1
    # ⇒ 永不 > 上限 ⇒ **无界风暴原样复发**。实测：`pipeline()` 抛错时
    # **500 次交接 / 500 份副本**；`WRONGTYPE` 键污染时 **50/50 无界**。
    # 故这里必须 **fail-closed**（与 `noderetry` 的同款 `return 1` 形似实不同：
    # 那里另有兜底，这里没有）。

    def test_counter_unavailable_is_fail_closed(self):
        """`_record_affinity_handover` 计数不可用时返回 **None**（不是 1）。

        None 是「不可用」的显式信号，调用方据此走留 pending 分支——不能与
        「计数 = 1」同义（那正是 fail-open 的洞）。
        """
        w = self._worker_with_redis()
        assert w._record_affinity_handover("k") == 1, "正常路径返回自增后的值"

        class _BoomPipe:
            def incr(self, *a): return self
            def expire(self, *a): return self
            def execute(self): raise RuntimeError("redis down")

        class _Boom(fakeredis.FakeRedis):
            def pipeline(self, *a, **k): return _BoomPipe()

        w_bad = self._worker_with_redis()
        w_bad.redis_client = _Boom(decode_responses=True)
        assert w_bad._record_affinity_handover("k") is None, (
            "计数自增失败必须返回 None（不可用），不得返回 1（= fail-open 无界）"
        )

        # 无 redis 客户端 / 无 incr：同样视为不可用（fail-closed）
        w_none = RedisFlowWorker.__new__(RedisFlowWorker)
        w_none.__dict__.pop("redis_client", None)
        assert w_none._record_affinity_handover("k") is None

    def test_spin_bounded_when_counter_raises(self):
        """★ 反证：计数自增抛异常 ⇒ 仍必须有界（旧版此处 500 次无界）。"""
        import fakeredis as _fr
        from plaita.storage.base import ExecutionState
        from plaita.storage.memory import MemoryExecutionStorage

        class _BoomPipe:
            def incr(self, *a): return self
            def expire(self, *a): return self
            def execute(self): raise RuntimeError("redis down")

        class _Boom(_fr.FakeRedis):
            def pipeline(self, *a, **k): return _BoomPipe()

        shared = _Boom(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-boom", ExecutionState(
            execution_id="exec-boom", flow_id="f", status="running", context={},
        ))
        # ★ 必须把**同一个**故障客户端注入 worker，否则新构造的 worker 会用
        # 干净客户端而绕开被测路径（本用例第一版就踩了这个假绿）。
        w = _full_worker(shared, storage)
        assert w.redis_client is shared
        rounds, copies = self._drain_handover_loop(
            shared, exec_id="exec-boom", worker=w)
        assert rounds <= 50, (
            f"计数抛异常时交接未收敛（{rounds} 轮）——fail-open 复发，无界风暴"
        )
        assert copies <= 1, f"不得产生新副本（实际 {copies} 份）"

    def test_spin_bounded_when_counter_key_is_wrongtype(self):
        """★ 反证：计数键被污染成非整数（WRONGTYPE）⇒ 仍必须有界。

        比「redis 瞬断」更现实的场景：键被误写成 list/string 非法值 ⇒ `INCR`
        抛 `WRONGTYPE`。旧版按 1 放行 ⇒ 50/50 无界。
        """
        import fakeredis as _fr
        from plaita.storage.base import ExecutionState
        from plaita.storage.memory import MemoryExecutionStorage

        shared = _fr.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-wt", ExecutionState(
            execution_id="exec-wt", flow_id="f", status="running", context={},
        ))
        w = _full_worker(shared, storage)
        key = w._affinity_handover_key({"execution_id": "exec-wt"})
        shared.delete(key)
        shared.rpush(key, "polluted")  # list ⇒ INCR 抛 WRONGTYPE

        rounds, copies = self._drain_handover_loop(
            shared, exec_id="exec-wt", worker=w)
        assert rounds <= 50, f"键污染时交接未收敛（{rounds} 轮）——fail-open 复发"
        assert copies <= 1, f"不得产生新副本（实际 {copies} 份）"
        # 且**不**误终态化：计数失效是本机基础设施故障，不是「无宿主可跑」
        assert storage.load_execution_state("exec-wt").status == "running", (
            "计数不可用不得终态化（那是基础设施故障，会判死可跑任务）"
        )

    def test_spin_bounded_when_no_redis_client(self):
        """★ 反证：无 redis 客户端 ⇒ 不得交接（纵深防御，生产不可达但须 fail-closed）。

        生产可达性已核实：`RedisFlowWorker.__init__` 的
        `redis_client = redis_client or Redis.from_url(redis_url)` 保证实例恒有
        真客户端，故该分支只有骨架/内存 worker 会走到。但仍须 fail-closed——
        否则将来有人绕开 `__init__` 就会静默退化成无界。
        """
        w = RedisFlowWorker.__new__(RedisFlowWorker)
        w.__dict__.pop("redis_client", None)
        w.queue_name = "q:noredis"
        q = self._FakeQueue()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-noredis",
                "params": {"repo": "/nonexistent/xyz"}}
        task = type("T", (), {"message_id": "1-0", "body": body})()
        w._handover_non_affine(task, q, RuntimeError("x"))
        assert q.acked == [], "计数不可用 ⇒ 不得 ack（消息必须留在 PEL）"

    def test_handover_limits_are_class_attrs_without_env_override(self):
        """部署一致性（验收要求 4）：上限与 TTL 是**类属性**且**无环境变量覆盖**。

        两台机器必须同值——若走 env（如 `PLAITA_AFFINITY_HANDOVER_MAX`），
        两机可配出不同的界，弱侧会先放行、强侧先终态化，「有界」就不成立。
        本用例钉死「只能改代码、不能改 env」这一部署契约。
        """
        import inspect

        from plaita.server.flow_worker import FlowWorker

        # 类属性（定义在基类 FlowWorker 上，RedisFlowWorker 继承）——两台机器
        # 只要同一份代码即同值。
        owner = next(c for c in RedisFlowWorker.__mro__
                     if "AFFINITY_HANDOVER_MAX" in c.__dict__)
        assert owner is FlowWorker
        assert isinstance(owner.__dict__["AFFINITY_HANDOVER_MAX"], int)
        assert isinstance(owner.__dict__["AFFINITY_HANDOVER_TTL_SECONDS"], int)
        assert RedisFlowWorker.AFFINITY_HANDOVER_MAX == 3
        assert RedisFlowWorker.AFFINITY_HANDOVER_TTL_SECONDS >= 180, (
            "TTL 必须 ≫ 回收周期（claim_min_idle_ms=60s）"
        )
        src = inspect.getsource(RedisFlowWorker._record_affinity_handover)
        src += inspect.getsource(RedisFlowWorker._handover_non_affine)
        for bad in ("PLAITA_AFFINITY_HANDOVER_MAX",
                    "PLAITA_AFFINITY_HANDOVER_TTL_SECONDS",
                    "environ", "getenv"):
            assert bad not in src, (
                f"交接上界不得由环境变量覆盖（两机会配不一致）: {bad}"
            )

    @staticmethod
    def _drain_handover_loop(shared, exec_id="exec-wt", rounds_max=500, worker=None):
        """跑「读队列头 → 交接」直到队列空，返回 (轮数, 流内副本数)。

        ``worker`` 可注入（必须与 ``shared`` 用**同一个**客户端，否则本用例会
        假绿：新构造的 worker 拿着干净客户端，绕开了被测的故障路径）。
        """
        from plaita.server.task_queue import RedisStreamTaskQueue, enqueue_task
        from plaita.storage.base import ExecutionState
        from plaita.storage.memory import MemoryExecutionStorage

        if worker is None:
            storage = MemoryExecutionStorage()
            storage.save_execution_state(exec_id, ExecutionState(
                execution_id=exec_id, flow_id="f", status="running", context={},
            ))
            worker = _full_worker(shared, storage)
        # ★ 必须用 **worker 自己的** queue_name：交接时新副本入的是
        # `self.queue_name`。用别的流名会让「副本数」恒为 0 而**假绿**
        # （我第一版就踩了这个坑：worker 是 q:test、我数的是 q:t）。
        queue_name = worker.queue_name
        q = RedisStreamTaskQueue(shared, queue_name, consumer_name="c1")
        q.ensure_group()
        enqueue_task(shared, queue_name, {
            "type": "start", "flow_id": "f", "execution_id": exec_id,
            "params": {"repo": "/nonexistent/xyz"},
        })
        n = 0
        while n < rounds_max:
            t = q.read(block_ms=1)
            if t is None:
                break
            worker._handover_non_affine(t, q, RuntimeError("x"))
            n += 1
        return n, len(shared.xrange(queue_name))

    def test_spin_terminates_in_simulated_two_host_loop(self):
        """★ 有界性反证：模拟事故的双机 ping-pong **跑满 N 轮后必然停止**。

        事故实态（keeper 只预告 run_dir、不 mkdir；两台机器 runs/ 都在、run_dir
        都不在）：两台都判不亲和 → 无限交接，实测持续 ≥85 分钟。

        本用例让两个 worker 共用一份 fakeredis（= 共享队列），反复「谁拿到谁
        交接」，断言**总交接轮数有上界**且留下显式终止证据。旧实现（布尔标记，
        TTL 30s < 回收 60s）在此会无限产副本——把 TTL 拉长也只是每台各停一次，
        仍无**次数**上界（这正是 review-verdict 红队①的否证）。

        刻意**不删**计数键、不 stub 时间：收敛靠的正是「TTL 远长于回收周期」
        （见 test_handover_counter_key_carries_ttl）。
        """
        from plaita.server.task_queue import RedisStreamTaskQueue
        from plaita.storage.base import ExecutionState
        from plaita.storage.memory import MemoryExecutionStorage

        shared = _real_redis_or_skip()
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-spin", ExecutionState(
            execution_id="exec-spin", flow_id="f", status="running", context={},
        ))
        hosts = [_full_worker(shared, storage) for _ in range(2)]
        for k in list(shared.scan_iter(match="*affinityhandover*")):
            shared.delete(k)  # 从 0 起算

        real_q = RedisStreamTaskQueue(shared, "q:test", consumer_name="c1")
        real_q.ensure_group()
        body = {"type": "start", "flow_id": "f", "execution_id": "exec-spin",
                "params": {"repo": "/nonexistent/xyz"}}
        # 起点：生产里 keeper 派发**首条**消息（此后由交接循环续命）
        from plaita.server.task_queue import enqueue_task
        enqueue_task(shared, "q:test", dict(body))

        rounds = 0
        while rounds < 200:
            # 读**队列头**（= 消费语义）：交接会 ack 原条 + 追加新副本，故头部
            # 恒为最新副本。用 read() 取（真实队列语义），不用 xrange、避免反复
            # 处理同一条陈旧 id（那是模拟失真，会虚假地「不收敛」）。
            task = real_q.read(block_ms=1)
            if task is None:
                break
            hosts[rounds % 2]._handover_non_affine(
                task, real_q, RuntimeError("无宿主亲和"))
            rounds += 1
            assert rounds <= 50, (
                f"交接未收敛（已跑 {rounds} 轮）——有界性失效，"
                "即 2026-10-11 无限交接风暴复发"
            )

        # 有界：总轮数 ≤ 上限 × 机器数 + 1（最后一条走达限分支、不再产副本）
        assert rounds <= RedisFlowWorker.AFFINITY_HANDOVER_MAX * len(hosts) + 1, (
            f"总交接轮数 {rounds} 超出上界"
        )
        assert rounds >= RedisFlowWorker.AFFINITY_HANDOVER_MAX, "上限内应照常交接"
        # 且必须留下**显式终止证据**：执行被终态化 error（可观测，非静默空转）
        state = storage.load_execution_state("exec-spin")
        assert state.status == "error"
        assert state.error["type"] == "AffinityHandoverExhausted"
        assert state.error["affinity_handovers"] > RedisFlowWorker.AFFINITY_HANDOVER_MAX


# ── 按仓拒跑名单（2026-10-07）：PLAITA_WORKER_DENY_REPOS ──


def test_deny_repo_blocks_even_when_path_exists(monkeypatch):
    """路径在本机存在（大仓软链双机可解析）时仍可按仓名拒收——重仓留给
    大容量 worker（2 核远端跑 cargo 饱和，实测 load 3.59）。"""
    w = _worker()
    with tempfile.TemporaryDirectory() as d:
        repo = os.path.join(d, "recursive")
        os.makedirs(repo)
        monkeypatch.setenv("PLAITA_WORKER_DENY_REPOS", "recursive, ilink-hub")
        reason = w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": repo}})
        assert reason is not None and "拒跑名单" in reason
        # 不在名单的仓照常亲和
        other = os.path.join(d, "plaita")
        os.makedirs(other)
        assert w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": other}}) is None


def test_deny_repo_empty_is_noop(monkeypatch):
    """空名单 = 不拒（默认零行为变化）。"""
    w = _worker()
    monkeypatch.delenv("PLAITA_WORKER_DENY_REPOS", raising=False)
    with tempfile.TemporaryDirectory() as d:
        repo = os.path.join(d, "recursive")
        os.makedirs(repo)
        assert w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": repo}}) is None


def test_deny_repo_matches_basename_only(monkeypatch):
    """按仓名（basename）匹配，路径前缀不同不影响；resume 消息不查（同亲和闸口径）。"""
    w = _worker()
    monkeypatch.setenv("PLAITA_WORKER_DENY_REPOS", "recursive")
    with tempfile.TemporaryDirectory() as d:
        repo = os.path.join(d, "some", "other", "recursive")
        os.makedirs(repo)
        assert w._detect_affinity_mismatch(
            {"type": "start", "params": {"repo": repo}}) is not None
        assert w._detect_affinity_mismatch(
            {"type": "resume", "params": {"repo": repo}}) is None


class TestRunDirHostOwnership:
    """run_dir 的**宿主归属**（2026-10-11 事故后的**删除**回归闸）。

    历史：`aa74ab6` 为修 plaita#62/#63/#65 的 `sync_in` 必失败，在此加了
    「`runs/` 存在而本 run 目录不存在 ⇒ run 属别的宿主 ⇒ 拒领」的补判。它
    **不可满足**（keeper 只预告 run_dir、不建目录 ⇒ 该状态在**每台**机器上
    都成立，含单机），交接又不产生目录 ⇒ 自锁；叠加「重入队新副本 delivery
    恒为 1、`max_deliveries` 永不触发」⇒ **无界**交接风暴（双机各 2600+ 次、
    持续 ≥85 分钟、日志 26 MB）。

    ⇒ 该补判已**删除**（「目录不存在」无法区分「属别的宿主」与「还没建」，
    且经 `/Users/kong/projects → /home/ubuntu/projects` 软链后两台解析到同一
    路径 ⇒ 归属信号在本层根本不可得）。本组用例钉死删除后的行为。

    ⚠️ 判据是**纯文件系统**判断、不碰 Redis ⇒ 必须用真 `tmp_path`；用
    fakeredis 会假绿（review-verdict §B1）。
    """

    def _w(self):
        return RedisFlowWorker.__new__(RedisFlowWorker)

    def test_missing_run_dir_accepted_when_runs_exists(self, tmp_path):
        """★ 反证用例（2026-10-11 无限交接风暴）：`runs/` 存在 + 本 run 目录不存在。

        **这是生产唯一路径**：keeper 只**预告** run_dir、不创建它
        （`issue_keeper/keeper.py` 拼路径后直接 `start_execution`，中间无
        `mkdir`），目录由**执行期**才建。于是对「尚未开跑的 run」：

          - `.flowcast/runs/`（父目录）在**两台机器都**存在（彼此跑过别的 run）；
          - `run_dir` 本身在**两台机器都不存在**。

        `aa74ab6` 在此加了 `isdir(dirname(path)) and not exists(path) ⇒ 拒领`，
        结果**每一台** worker（含单机）都判「不亲和」；而 ack+重入队**不产生
        目录** ⇒ 判据永远无法满足 ⇒ 自锁。叠加「新副本 delivery 恒为 1」⇒
        无界。**必须放行**。

        注意：本判据是**纯文件系统**判断，不碰 Redis——故必须用真 `tmp_path`，
        用 fakeredis 会因 Redis 侧一切正常而**假绿**（review-verdict §B1）。
        """
        base = tmp_path / "repo"
        # 关键：建 runs/（既有测试的夹具缺的正是这步——只建 .flowcast 会绕开
        # 出事的那条分支，所以当时全绿）。再放一个**别的** run，模拟生产实态。
        (base / ".flowcast" / "runs" / "pipeline-65-1011101914").mkdir(parents=True)
        run_dir = base / ".flowcast" / "runs" / "pipeline-150-1011120037"
        assert (base / ".flowcast" / "runs").is_dir(), "父目录存在（前置条件）"
        assert not run_dir.exists(), "本 run 目录尚未创建（前置条件）"
        msg = {"type": "start", "params": {
            "repo": str(base), "run_dir": str(run_dir)}}
        assert self._w()._detect_affinity_mismatch(msg) is None, (
            "「runs/ 存在而本 run 目录不存在」= run 还没开跑，不是「属别的宿主」"
            "——判不亲和即自锁（无任何宿主能认领）"
        )

    def test_own_run_dir_accepted(self, tmp_path):
        """本 run 目录已存在 ⇒ 是本机的 ⇒ 放行。"""
        base = tmp_path / "repo"
        (base / ".flowcast" / "runs" / "MINE").mkdir(parents=True)
        msg = {"type": "start", "params": {
            "repo": str(base),
            "run_dir": str(base / ".flowcast" / "runs" / "MINE")}}
        assert self._w()._detect_affinity_mismatch(msg) is None

    def test_brand_new_run_accepted(self, tmp_path):
        """全新 run（runs/ 尚不存在）⇒ 不拦，留给派发方本机领取。

        与上一个用例的区别：这里是「仓从未跑过 run」（runs/ 都没建）。
        生产实态**通常不是**这样（runs/ 早就在了），故它**不能**替代
        `test_missing_run_dir_accepted_when_runs_exists` 的反证作用。
        """
        base = tmp_path / "repo"
        (base / ".flowcast").mkdir(parents=True)
        msg = {"type": "start", "params": {
            "repo": str(base),
            "run_dir": str(base / ".flowcast" / "runs" / "BRAND-NEW")}}
        assert self._w()._detect_affinity_mismatch(msg) is None

    def test_missing_repo_root_still_rejected(self, tmp_path):
        """仓根不存在（真·别的机器）⇒ 仍拒领——删除补判**没有**削弱原判据。

        这是「删 6 行」后必须保住的既有能力：`probe`（仓根）不存在即不亲和。
        """
        msg = {"type": "start", "params": {
            "repo": str(tmp_path / "no-such-repo"),
            "run_dir": str(tmp_path / "no-such-repo/.flowcast/runs/x")}}
        reason = self._w()._detect_affinity_mismatch(msg)
        assert reason is not None and "在本机不存在" in reason

    def test_host_ownership_predicate_is_gone(self):
        """守卫：已删除的「宿主归属补判」不得被重新加回（事故回归闸）。

        该判据的删除是 2026-10-11 无限交接风暴的修复本体；它「看起来合理」
        （目录不在 = 别的宿主），极易被后人复辟。此处按源码文本断言其缺席。
        """
        import inspect
        import plaita.server.flow_worker as m
        src = inspect.getsource(m.RedisFlowWorker._detect_affinity_mismatch)
        assert "的父目录存在而本 run 目录不存在" not in src, (
            "宿主归属补判已复辟——该判据不可满足且导致无界自锁，见函数内注释"
        )
        assert "isdir(os.path.dirname(path))" not in src
