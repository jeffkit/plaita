"""#79 复现：RedisEventStorage.list_events 返回完整性（生产客户端 decode_responses=False）。"""
import asyncio, time
import fakeredis.aioredis
from plaita.event.core import Event
from plaita.event.redis import RedisEventStorage


async def main():
    server = fakeredis.FakeServer()
    store = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=3600)
    # 与生产 initialize() 同参：decode_responses=False（redis-py 默认）
    store.redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    print("client decode_responses =", store.redis.connection_pool.connection_kwargs.get("decode_responses"))

    base = time.time() - 100
    n = 12
    for i in range(n):
        await store.store_event(Event(
            event_type=f"t{i:02d}", data={"i": i}, source="repro",
            event_id=f"ev{i:02d}", timestamp=base + i,
        ))

    typed = await store.list_events(event_type="t00", limit=200)
    all_events = await store.list_events(limit=200)
    limited = await store.list_events(limit=5)
    print(f"typed   list_events('t00', limit=200) = {len(typed)} (expect 1)")
    print(f"all     list_events(limit=200)        = {len(all_events)} (expect {n})")
    print(f"all     types = {sorted(e.event_type for e in all_events)}")
    ts = [e.timestamp for e in all_events]
    print(f"all     globally time-ordered = {ts == sorted(ts)}")
    print(f"limit=5 list_events(limit=5)          = {len(limited)} (expect 5), "
          f"ids={[e.event_id for e in limited]} (expect earliest ev00..ev04)")

    # 11 类型 limit=11
    server2 = fakeredis.FakeServer()
    store2 = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=3600)
    store2.redis = fakeredis.aioredis.FakeRedis(server=server2, decode_responses=False)
    for i in range(11):
        await store2.store_event(Event(event_type=f"u{i:02d}", data={}, source="r",
                                       event_id=f"u{i:02d}", timestamp=base + i))
    got = await store2.list_events(limit=11)
    print(f"11 types limit=11 -> {len(got)} (expect 11)")

    print("VERDICT:", "DEFECT-ALIVE" if (len(all_events) != n or ts != sorted(ts)
                                         or len(limited) != 5 or len(got) != 11) else "OK")

asyncio.run(main())
