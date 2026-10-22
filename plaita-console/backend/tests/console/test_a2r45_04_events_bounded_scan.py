"""A2R45-04（duo-a2-ledger 第四十五轮）：``/api/events`` 与 ``/api/events/subscriptions``
两读面的键枚举必须接入有界 ``redis_scan.scan_keys``（#202 原语）。

缺口（基线 ``95fe2ad``）：``api/events.py`` 自带 ``_scan_keys``——``SCAN`` 游标循环
直到 0、``count=100`` 且**无任何上限**（键数 / 轮次 / 墙钟都没有），随后对每个匹配
键再各发一次 ``TYPE`` + ``GET``。两端点都只要 viewer 的 GET 权限：共享 Redis 的
keyspace 一大（执行键、日志流、噪声键都在同一库），最低权限的 viewer 反复刷新
事件页就是一次次全库遍历。仓内 logs / queues / services / executions / metrics
五处已统一走 ``scan_keys``，events 是最后一个无界面。

钉住：
1. **有界**：10 万键 keyspace 下单请求 SCAN 轮次在预算内（``DEFAULT_MAX_ROUNDS``）、
   单次 ``count`` ≤ ``PAGE_SIZE_MAX``，种子事件 / 订阅仍被正确返回；
2. **分页保持**：``limit`` / ``cursor`` / ``next_cursor`` / ``truncated`` 的 A2R24-05
   语义不变——翻页语义与「扫描触界」分开表达；
3. **触界可观测**：超出预算的 keyspace 上响应 ``scan_truncated: true``（列表只是
   匹配键的一部分、``total`` 哨兵化为 ``-1``，A2R47-04：未扫完连 ``0`` 也不
   可信）并记 WARNING，不静默给「空列表 = 没有事件」；
4. ``KEYS`` 永不被调用（替身一调用即抛）。

负控：仅还原 ``api/events.py`` 的 ``_scan_keys`` 接线 ⇒ 用例 1（轮次超预算）与
用例 3（无 ``scan_truncated`` 字段）精确红；用例 2 的翻页断言保持绿。
"""
from __future__ import annotations

import fnmatch
import json
import logging
import sys
from pathlib import Path

from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import events as events_api  # noqa: E402
from api.redis_scan import DEFAULT_MAX_ROUNDS, PAGE_SIZE_MAX  # noqa: E402

EVENT_PREFIX = "plaita:event:events:"
SUB_PREFIX = "plaita:subscription:data:"


class _KeyspaceRedis:
    """受控 keyspace：只实现两端点用到的命令，SCAN 成本与页大小成正比。

    fakeredis 的 SCAN 每次调用都重建整张键表（O(N)/次），10 万键下要跑分钟级；
    这里用 O(页大小) 的实现，成本口径与真实 Redis 一致。``keys`` 一调用就抛错。
    """

    def __init__(self, noise: int = 0) -> None:
        self._data: dict[str, str] = {}
        self._noise = int(noise)
        self._sorted: list[str] | None = None
        self.keys_calls: list[str] = []
        self.scan_calls: list[dict] = []

    def set(self, key: str, value: str) -> None:
        if key not in self._data:
            self._sorted = None
        self._data[key] = value

    def _keys(self) -> list[str]:
        if self._sorted is None:
            noise = [f"plaita:noise:{i:07d}" for i in range(self._noise)]
            self._sorted = sorted(self._data) + noise
        return self._sorted

    def keys(self, pattern: str = "*", *args, **kwargs):
        self.keys_calls.append(pattern)
        raise AssertionError(f"端点调用了 redis KEYS({pattern})——全库阻塞扫描（#202）")

    def scan(self, cursor=0, match=None, count=10):
        self.scan_calls.append({"count": count, "match": match})
        keys = self._keys()
        start = int(cursor)
        end = min(start + int(count), len(keys))
        batch = [k for k in keys[start:end] if fnmatch.fnmatch(k, match or "*")]
        return (0 if end >= len(keys) else end), batch

    def type(self, key: str) -> str:
        return "string" if key in self._data else "none"

    def get(self, key: str):
        return self._data.get(key)


class _BottomlessRedis(_KeyspaceRedis):
    """游标按 count 推进、总量远超任何预算、没有匹配键——模拟超大 keyspace。"""

    def __init__(self, total: int = 10_000_000) -> None:
        super().__init__()
        self.total = total

    def scan(self, cursor=0, match=None, count=10):
        self.scan_calls.append({"count": count, "match": match})
        nxt = int(cursor) + int(count)
        return (0 if nxt >= self.total else nxt), []


def _event(event_id: str, ts: float, tenant: str = "default") -> str:
    return json.dumps({
        "event_id": event_id, "event_type": "order.created",
        "data": {"n": event_id}, "timestamp": ts, "tenant_id": tenant,
    })


def _subscription(sub_id: str, created_at: float, tenant: str = "default") -> str:
    return json.dumps({
        "subscription_id": sub_id, "event_type": "order.created",
        "flow_id": "f1", "node_id": "n1", "created_at": created_at, "tenant_id": tenant,
    })


def _seed(redis: _KeyspaceRedis) -> None:
    for i in range(1, 6):
        redis.set(f"{EVENT_PREFIX}e{i}", _event(f"e{i}", 1000.0 + i))
    for i in range(1, 4):
        redis.set(f"{SUB_PREFIX}s{i}", _subscription(f"s{i}", 2000.0 + i))


def _app(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(events_api.router, prefix="/api")
    return TestClient(app)


def _rounds_by_pattern(redis: _KeyspaceRedis, since: int) -> dict[str, int]:
    rounds: dict[str, int] = {}
    for call in redis.scan_calls[since:]:
        rounds[call["match"]] = rounds.get(call["match"], 0) + 1
    return rounds


def _get_bounded(client: TestClient, redis: _KeyspaceRedis, url: str):
    """单请求内每个 pattern 的 SCAN 轮次都必须在预算内，页大小被 clamp。"""
    before = len(redis.scan_calls)
    resp = client.get(url)
    assert resp.status_code == 200, resp.text
    rounds = _rounds_by_pattern(redis, before)
    assert rounds, f"{url} 没有走 SCAN"
    assert max(rounds.values()) <= DEFAULT_MAX_ROUNDS, (
        f"{url} 单个 pattern 的 SCAN 轮次超预算: {rounds}"
    )
    counts = [c["count"] for c in redis.scan_calls[before:] if c["count"] is not None]
    assert counts and max(counts) <= PAGE_SIZE_MAX, counts
    assert redis.keys_calls == []
    return resp


# ---------------------------------------------------------------- 1/2. 10 万键下有界 + 分页保持


def test_list_events_is_bounded_and_paging_survives_on_100k_keyspace():
    redis = _KeyspaceRedis(noise=100_000)
    _seed(redis)
    client = _app(redis)

    page1 = _get_bounded(client, redis, "/api/events?limit=2").json()
    assert [e["event_id"] for e in page1["events"]] == ["e5", "e4"]
    assert page1["total"] == 5 and page1["returned"] == 2
    assert page1["truncated"] is True and page1["next_cursor"]
    assert page1["scan_truncated"] is False
    assert page1["degraded"] is False

    page2 = _get_bounded(client, redis, f"/api/events?limit=2&cursor={page1['next_cursor']}").json()
    assert [e["event_id"] for e in page2["events"]] == ["e3", "e2"]
    assert page2["truncated"] is True and page2["next_cursor"]

    page3 = _get_bounded(client, redis, f"/api/events?limit=2&cursor={page2['next_cursor']}").json()
    assert [e["event_id"] for e in page3["events"]] == ["e1"]
    assert page3["truncated"] is False and page3["next_cursor"] is None
    assert page3["scan_truncated"] is False


def test_list_subscriptions_is_bounded_on_100k_keyspace():
    redis = _KeyspaceRedis(noise=100_000)
    _seed(redis)
    client = _app(redis)

    body = _get_bounded(client, redis, "/api/events/subscriptions").json()
    assert [s["subscription_id"] for s in body["subscriptions"]] == ["s3", "s2", "s1"]
    assert body["total"] == 3
    assert body["scan_truncated"] is False
    assert body["degraded"] is False


# ---------------------------------------------------------------- 3. 触界可观测


def test_list_events_reports_scan_truncation_instead_of_silent_empty(caplog):
    redis = _BottomlessRedis()
    client = _app(redis)

    with caplog.at_level(logging.WARNING, logger="api.redis_scan"):
        resp = _get_bounded(client, redis, "/api/events")
    body = resp.json()
    assert body["events"] == [] and body["total"] == -1  # A2R47-04：未扫完，0 也不可信
    assert body["scan_truncated"] is True, body
    # 翻页语义与扫描触界分开：本页之后没有更多页
    assert body["truncated"] is False and body["next_cursor"] is None
    assert "有界 SCAN 触界" in caplog.text


def test_list_subscriptions_reports_scan_truncation(caplog):
    redis = _BottomlessRedis()
    client = _app(redis)

    with caplog.at_level(logging.WARNING, logger="api.redis_scan"):
        resp = _get_bounded(client, redis, "/api/events/subscriptions")
    body = resp.json()
    assert body["subscriptions"] == [] and body["total"] == -1  # A2R47-04：未扫完，0 也不可信
    assert body["scan_truncated"] is True, body
    assert "有界 SCAN 触界" in caplog.text


# ---------------------------------------------------------------- 4. 自带扫描器已退场


def test_events_module_has_no_private_unbounded_scanner():
    """接线单源：events 不再自带 ``_scan_keys``，而是用 ``redis_scan.scan_keys``。"""
    assert not hasattr(events_api, "_scan_keys")
    from api import redis_scan

    assert getattr(events_api, "scan_keys", None) is redis_scan.scan_keys
