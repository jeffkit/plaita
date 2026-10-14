#!/usr/bin/env python
"""#109/#110 自证封闭语料（规则 → 笛卡尔积矩阵 → 逐格独立谓词 → 分类 → 缺口）。

    PYTHONPATH=<tree> python scripts/selfclose_109_110.py            # 期望 gap=0
    PYTHONPATH=<tree> python scripts/selfclose_109_110.py --inject X # 负控：必须 gap>0
    X ∈ {untracked, no-drain, fence-off, type-first}

纪律（针对 r2 验收的 F3）：
- **每个格的期望由独立谓词从「输入」算出**，不看实现返回值再回填。
- **不灌水**：110 维不含 concurrency（旧版 81 格 = 27 探针 × 3 标签）；global 格
  有真实期望（SCAN 全量）而不是恒 True。
- **围栏必须有格真的触发**：shutdown-during-send 先关停再发 ⇒ 期望 `refused == n`。
- **retry 分支**单列一格（`retry_policy != None` 的登记点）。
- `--inject` 把实现打回缺陷（负控）：矩阵必须出现 GAP，否则语料无检验力。

规则（谓词）：
  R1  109 normal/gc：finished == n 且 marked == n 且 pending 清空（该完成的不丢）。
  R2  109 shutdown/loop-close：finished + cancelled + refused == n、inflight == 0、
      pending 清空、被取消/被拒的每一条都点名 event_id+handler_id（无第三态）。
  R3  109 shutdown-during-send：**先关停再发** ⇒ refused == n（围栏真的触发）。
  R4  109 retry（retry_policy != None）：任务被登记且收尾被排空（覆盖另一登记点）。
  R5  110 correlation 索引：redis returned == 2（该 correlation 的订阅数）且
      data: GET == 2、同类型其它 correlation 的 n 条一条不碰；memory/sqlalchemy
      list 层双条件预过滤 returned == 1；type-only：三者 returned == n+1；
      global：三者 returned == n+2（SCAN 全量）。
  R6  110 parity：同 (corr,type) 下三后端 find_matching_subscriptions 集合相同。
  R7  F1：空 event_type 在 EventNode / EventSubscription 构造期被拒（ValidationError）。
"""
import argparse
import asyncio
import itertools
import logging
import os
import sys

TREE = os.environ.get("PLAITA_TREE", "")
if TREE:
    sys.path.insert(0, TREE)
import plaita  # noqa: E402

RP = os.path.realpath(plaita.__file__)
if TREE:
    assert RP.startswith(os.path.realpath(TREE)), f"wrong tree: {RP}"
print(f"# tree: {RP}")

from unittest.mock import patch  # noqa: E402

import fakeredis  # noqa: E402
import fakeredis.aioredis  # noqa: E402

import plaita.event.redis as redis_mod  # noqa: E402
from plaita.event.core import Event, EventSubscription, RetryPolicy  # noqa: E402
from plaita.event.memory import InMemoryEventBus  # noqa: E402
from plaita.event.redis import RedisEventBus, RedisEventSubscriptionStorage  # noqa: E402
from plaita.event.sqlalchemy import Base, SqlalchemyEventSubscriptionStorage  # noqa: E402

SERVER = fakeredis.FakeServer()
BACKENDS = ["memory", "redis", "sqlalchemy"]
LIFECYCLES = ["normal", "gc", "shutdown", "loop-close"]
INBOUNDS = [0, 1, "N"]
INDEXES = ["correlation", "type-only", "global"]
CONCURRENCY = ["single", "concurrent", "shutdown-during-send"]


async def _fake_from_url(url):
    return fakeredis.aioredis.FakeRedis(server=SERVER, decode_responses=False)


class _Tracker:
    def __init__(self):
        self.marked = []

    async def is_event_processed(self, e, h):
        return False

    async def mark_event_processed(self, e, h):
        self.marked.append((e, h))

    async def record_processing_attempt(self, *a, **k):
        pass

    async def close(self):
        pass


class _Stub:
    async def close(self):
        pass

    async def aclose(self):
        pass


class _Catch(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def _n(inbound):
    return {"0": 0, "1": 1, "N": 5}[str(inbound)]


# ---------------------------------------------------------------- #109 生命周期
async def probe_109(lifecycle, inbound, concurrency, retry=False):
    n = _n(inbound)
    release = asyncio.Event()
    finished = []

    async def handler(event):
        await release.wait()
        finished.append(event.event_id)

    bus = RedisEventBus(redis_url="redis://unused/0")
    bus.processing_tracker, bus.event_storage = _Tracker(), _Stub()
    bus.subscription_storage, bus.pubsub, bus.redis = _Stub(), _Stub(), _Stub()
    policy = RetryPolicy(max_retries=0) if retry else None
    bus.handlers["h1"] = (handler, "*", None, policy)
    catch = _Catch()
    logger = logging.getLogger("plaita.event.redis")
    logger.addHandler(catch)
    events = [Event(event_type="t.demo", data={}) for _ in range(n)]

    async def send_all():
        for ev in events:
            await bus._process_message(
                {"type": "message", "data": ev.model_dump_json().encode()}, "h1")

    try:
        if concurrency == "shutdown-during-send":
            # R3（对所有 lifecycle 一致）：先进入关停态，再让分发到达
            async def closer():
                with patch.object(redis_mod, "DISPATCH_DRAIN_TIMEOUT_SECONDS", 0.05):
                    await bus.close()
            c = asyncio.create_task(closer())
            await asyncio.sleep(0)
            await send_all()
            await c
            refused = len([m for m in catch.lines if "拒绝新的分发" in m])
            named = all(("event_id=" in m and "handler_id=h1" in m)
                        for m in catch.lines if "拒绝新的分发" in m)
            inflight = [t for t in asyncio.all_tasks()
                        if t is not asyncio.current_task() and not t.done()
                        and "_process" in t.get_coro().__qualname__]
            accounted = len(finished) + refused
            detail = (f"finished={len(finished)} refused={refused} "
                      f"inflight={len(inflight)} accounted={accounted}/{n}")
            return detail, (refused == n and accounted == n and not inflight
                            and len(bus._pending_tasks) == 0 and named)

        if concurrency == "single":
            await send_all()
        else:
            await asyncio.gather(*[
                bus._process_message(
                    {"type": "message", "data": ev.model_dump_json().encode()}, "h1")
                for ev in events
            ])

        if lifecycle in ("normal", "gc"):
            release.set()
            for _ in range(300):
                if not bus._pending_tasks:
                    break
                await asyncio.sleep(0.01)
            if lifecycle == "gc":
                import gc
                for _ in range(60):
                    gc.collect()
                    await asyncio.sleep(0.01)
            marked = len(set(e for e, _ in bus.processing_tracker.marked))
            detail = f"finished={len(finished)} marked={marked} pending={len(bus._pending_tasks)}"
            return detail, (len(finished) == n and marked == n
                            and len(bus._pending_tasks) == 0)

        with patch.object(redis_mod, "DISPATCH_DRAIN_TIMEOUT_SECONDS", 0.05):
            await asyncio.wait_for(bus.close(), timeout=10)
        refused = len([m for m in catch.lines if "拒绝新的分发" in m])
        cancelled = len([m for m in catch.lines
                         if "取消窗口" in m or "未在" in m])
        inflight = [t for t in asyncio.all_tasks()
                    if t is not asyncio.current_task() and not t.done()
                    and "_process" in t.get_coro().__qualname__]
        named = all(("event_id=" in m and "handler_id=h1" in m)
                    for m in catch.lines if "拒绝新的分发" in m or "取消" in m)
        accounted = len(finished) + cancelled + refused
        detail = (f"finished={len(finished)} cancelled={cancelled} refused={refused} "
                  f"inflight={len(inflight)} accounted={accounted}/{n}")
        # R2：要么完成、要么被取消/拒绝并点名
        ok = (accounted == n and not inflight and len(bus._pending_tasks) == 0
              and named)
        return detail, ok
    finally:
        logger.removeHandler(catch)
        release.set()


# ---------------------------------------------------------------- #110 订阅查询
async def probe_110(backend, index, inbound):
    n = _n(inbound)
    corr, etype = "CID", "shared.type"
    prefix = f"sc{os.urandom(4).hex()}:"
    gets = []
    engine = None
    if backend == "redis":
        storage = RedisEventSubscriptionStorage("redis://localhost:6379/9", prefix)
        with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
            await storage.initialize()
        real_get = storage.redis.get

        async def counting_get(key):
            gets.append(str(key))
            return await real_get(key)

        storage.redis.get = counting_get
    elif backend == "memory":
        storage = InMemoryEventBus().subscription_storage
    else:
        from sqlalchemy.ext.asyncio import create_async_engine
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        storage = SqlalchemyEventSubscriptionStorage(engine)

    for k in range(n):
        await storage.store_subscription(EventSubscription(
            event_type=etype, correlation_id=f"other-{k}", subscription_id=f"{prefix}o{k}"))
    await storage.store_subscription(EventSubscription(
        event_type=etype, correlation_id=corr, subscription_id=f"{prefix}hit"))
    await storage.store_subscription(EventSubscription(
        event_type="other.type", correlation_id=corr, subscription_id=f"{prefix}wrongtype"))

    if index == "correlation":
        got = await storage.list_subscriptions(event_type=etype, correlation_id=corr)
    elif index == "type-only":
        got = await storage.list_subscriptions(event_type=etype)
    else:
        got = await storage.list_subscriptions()
    ids = sorted(s.subscription_id for s in got)
    data_gets = len([k for k in gets if "data:" in k])
    # 独立谓词：期望只由 (backend, index, n) 决定
    touched_others = [i for i in ids if i.endswith(tuple(f"o{k}" for k in range(n)))]
    if index == "correlation":
        want = 1 if backend in ("memory", "sqlalchemy") else 2
        ok = len(ids) == want and not touched_others
        if backend == "redis":
            ok = ok and data_gets == 2
        detail = (f"returned={len(ids)} data_GET={data_gets} "
                  f"touched_other_corr={len(touched_others)} (want {want})")
    elif index == "type-only":
        want = n + 1
        ok = len(ids) == want and not any(i.endswith("wrongtype") for i in ids)
        detail = f"returned={len(ids)} (want {want})"
    else:
        want = n + 2
        ok = len(ids) == want
        detail = f"returned={len(ids)} (want {want}, SCAN 全量)"
    if backend == "redis":
        await storage.close()
    elif engine is not None:
        await engine.dispose()
    return detail, ok


async def probe_parity(inbound):
    event = Event(event_type="shared.type", data={}, correlation_id="CID")
    sets = {}
    for backend in BACKENDS:
        if backend == "memory":
            storage = InMemoryEventBus().subscription_storage
            engine = None
        elif backend == "redis":
            storage = RedisEventSubscriptionStorage(
                "redis://localhost:6379/9", f"par{os.urandom(4).hex()}:")
            with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
                await storage.initialize()
            engine = None
        else:
            from sqlalchemy.ext.asyncio import create_async_engine
            engine = create_async_engine("sqlite+aiosqlite://")
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            storage = SqlalchemyEventSubscriptionStorage(engine)
        n = _n(inbound)
        for k in range(n):
            await storage.store_subscription(EventSubscription(
                event_type="shared.type", correlation_id=f"other-{k}",
                subscription_id=f"{backend}-o{k}"))
        await storage.store_subscription(EventSubscription(
            event_type="shared.type", correlation_id="CID", subscription_id=f"{backend}-hit"))
        await storage.store_subscription(EventSubscription(
            event_type="other.type", correlation_id="CID", subscription_id=f"{backend}-wrongtype"))
        got = await storage.find_matching_subscriptions(event, {})
        sets[backend] = sorted(s.subscription_id.split("-")[-1] for s in got)
        if backend == "redis":
            await storage.close()
        elif engine is not None:
            await engine.dispose()
    same = len(set(tuple(v) for v in sets.values())) == 1 and sets["redis"] == ["hit"]
    return f"sets={sets} (want all == ['hit'])", same


def probe_empty_type():
    from pydantic import ValidationError
    from plaita.node.event_node import EventNode
    bad = []
    for label, fn in (("EventNode", lambda: EventNode(id="e", event_type="")),
                      ("EventSubscription", lambda: EventSubscription(event_type=""))):
        try:
            fn()
            bad.append(label)
        except ValidationError:
            pass
    ok = not bad and EventSubscription(event_type="*").event_type == "*"
    return f"empty_rejected={not bad} (非空 '*' 仍可)", ok


# ---------------------------------------------------------------- 负控注入
def apply_inject(name):
    if name is None:
        return
    if name == "untracked":
        redis_mod.RedisEventBus._track_task = lambda self, task, eid, hid: None
    elif name == "no-drain":
        async def _noop(self):
            self._pending_tasks.clear()
        redis_mod.RedisEventBus._drain_pending_tasks = _noop
    elif name == "fence-off":
        orig_close = redis_mod.RedisEventBus.close

        async def close_then_clear_fence(self):
            await orig_close(self)
            self._closing = False

        redis_mod.RedisEventBus.close = close_then_clear_fence
    elif name == "type-first":
        async def list_event_type_first(self, event_type=None, correlation_id=None,
                                        flow_id=None, node_id=None):
            await self.initialize()
            if event_type:
                ids = await self.redis.smembers(f"{self.key_prefix}type:{event_type}")
            elif correlation_id:
                ids = await self.redis.smembers(
                    f"{self.key_prefix}correlation:{correlation_id}")
            elif flow_id:
                ids = await self.redis.smembers(f"{self.key_prefix}flow:{flow_id}")
            elif node_id:
                ids = await self.redis.smembers(f"{self.key_prefix}node:{node_id}")
            else:
                return []
            out = []
            for sid in ids:
                s = await self.get_subscription(
                    sid.decode() if isinstance(sid, bytes) else sid)
                if s:
                    out.append(s)
            return out
        redis_mod.RedisEventSubscriptionStorage.list_subscriptions = list_event_type_first
    else:
        raise SystemExit(f"unknown inject: {name}")


async def main(inject):
    apply_inject(inject)
    rows = []
    gaps = []

    def add(cell, detail, ok):
        rows.append((cell, detail, "OK" if ok else "GAP"))
        if not ok:
            gaps.append((cell, detail))

    for lifecycle, inbound, conc in itertools.product(LIFECYCLES, INBOUNDS, CONCURRENCY):
        detail, ok = await probe_109(lifecycle, inbound, conc)
        add(("109", "redis", lifecycle, f"in={inbound}", "-", conc), detail, ok)
    for lifecycle in ("normal", "shutdown"):
        for inbound in (1, "N"):
            detail, ok = await probe_109(lifecycle, inbound, "single", retry=True)
            add(("109-retry", "redis", lifecycle, f"in={inbound}", "-", "single"),
                detail, ok)
    for backend, index, inbound in itertools.product(BACKENDS, INDEXES, INBOUNDS):
        detail, ok = await probe_110(backend, index, inbound)
        add(("110", backend, "normal", f"in={inbound}", index, "-"), detail, ok)
    for inbound in INBOUNDS:
        detail, ok = await probe_parity(inbound)
        add(("110-parity", "memory/redis/sqlalchemy", "normal", f"in={inbound}",
             "correlation", "-"), detail, ok)
    detail, ok = probe_empty_type()
    add(("110-F1", "models", "normal", "in=-", "empty-type", "-"), detail, ok)

    for cell, detail, verdict in rows:
        print(f"{verdict:>4} | {' | '.join(str(c) for c in cell)} | {detail}")
    n_ok = len([r for r in rows if r[2] == "OK"])
    print(f"\n# inject={inject} cells={len(rows)} OK={n_ok} GAP={len(gaps)}")
    for g in gaps[:8]:
        print(f"# GAP: {g}")
    return 1 if gaps else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--inject", default=None,
                    choices=[None, "untracked", "no-drain", "fence-off", "type-first"])
    a = ap.parse_args()
    sys.exit(asyncio.run(main(a.inject)))
