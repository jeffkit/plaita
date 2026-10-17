"""#85 复现：EventReconciler 把游标无条件推到窗口上界 ⇒ 窗口内超 batch_size 的事件永久漏扫。"""
import asyncio, time
from plaita.event.core import Event
from plaita.event.memory import MemoryEventStorage
from plaita.server.event_reconcile import EventReconciler


class RecordingFilter:
    def __init__(self):
        self.seen = []

    async def handle_event(self, event):
        self.seen.append(event.event_id)


class FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value
        return True


async def main():
    store = MemoryEventStorage()
    base = time.time() - 10
    events = []
    for i in range(5):
        ev = Event(event_type="flow.paused", data={"i": i}, source="t",
                   event_id=f"ev{i}", timestamp=base + i * 0.001)
        events.append(ev)
        await store.store_event(ev)
    rec = RecordingFilter()
    redis = FakeRedis()
    r = EventReconciler(store, rec, redis, backfill_seconds=3600, batch_size=2)

    for rnd in range(1, 5):
        n = await r.scan_once()
        print(f"scan#{rnd} replayed: {n}  seen={rec.seen}  cursor={redis.kv.get(EventReconciler.CURSOR_KEY)}")
    missing = [e.event_id for e in events if e.event_id not in rec.seen]
    print("events never replayed:", missing)
    print("VERDICT:", "DEFECT-ALIVE" if missing else "OK")

asyncio.run(main())
