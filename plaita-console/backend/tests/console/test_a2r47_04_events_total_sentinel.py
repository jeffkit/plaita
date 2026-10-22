"""A2R47-04（duo-a2-ledger 第四十七轮，P2）：events 分页 total 语义三张嘴收敛。

缺口（基线 ``daadc00``）：``/api/events`` 的 ``total`` 三个口径互相矛盾——
端点 docstring「匹配总数」（无预算限定）、模型注释「受扫描预算约束…是
下界」、实现「已扫前缀内 cursor 过滤后计数」（逐页递减 5000→4950→…）。
naive 客户端（follow ``next_cursor`` 直到 ``truncated=false``——游标生命
周期恰由 ``truncated`` 驱动，最自然的写法）在触界 keyspace 上收割匹配键的
一小部分后，终页 ``truncated=false`` 给出「读完了」假象，把前缀计数当全集
静默丢数据（A2 实测：100k 键空间收 5000/100000、丢 95%）。

修法（哨兵 + 契约钉死 + 文档统一）：
- ``scan_truncated=true`` ⇒ ``total=-1`` 哨兵（events / subscriptions 两
  端点同口径）——未扫完的 keyspace 没有可信的「匹配总数」（连 ``0`` 也
  不可信）；
- 契约钉死：完整遍历终止条件 = ``truncated=false`` **且**
  ``scan_truncated=false``（合取）；``total`` 仅在完整扫描时是匹配总数；
- 模块 docstring / 模型注释 / 端点 docstring / ops-runbook 四处统一。

负控（pre 精确红 → post 收敛）：
- naive 客户端探针（真预算 + 6000 事件 keyspace，pre 形态 = 终页
  ``truncated=false`` 且 ``total>0``「假完整」）：断言「客户端可依契约判定
  不完整」——pre 精确红，post 绿（哨兵可判定）；
- 哨兵契约用例（小预算）：触界页 ``total=-1``、未触界页 total 照旧；
- subscriptions 同口径。
"""
from __future__ import annotations

import fnmatch
import json
import sys
from pathlib import Path

from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import events as events_api  # noqa: E402

EVENT_PREFIX = "plaita:event:events:"
SUB_PREFIX = "plaita:subscription:data:"


class _KeyspaceRedis:
    """受控 keyspace：只实现端点用到的命令；``keys`` 一调用就抛。"""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        self._sorted: list[str] | None = None

    def set(self, key: str, value: str) -> None:
        if key not in self._data:
            self._sorted = None
        self._data[key] = value

    def _keys(self) -> list[str]:
        if self._sorted is None:
            self._sorted = sorted(self._data)
        return self._sorted

    def keys(self, pattern: str = "*", *args, **kwargs):
        raise AssertionError(f"端点调用了 redis KEYS({pattern})——全库阻塞扫描（#202）")

    def scan(self, cursor=0, match=None, count=10):
        keys = self._keys()
        start = int(cursor)
        end = min(start + int(count), len(keys))
        batch = [k for k in keys[start:end] if fnmatch.fnmatch(k, match or "*")]
        return (0 if end >= len(keys) else end), batch

    def type(self, key: str) -> str:
        return "string" if key in self._data else "none"

    def get(self, key: str):
        return self._data.get(key)


def _seed_events(redis: _KeyspaceRedis, n: int) -> None:
    """键名零垫有序：SCAN 前缀 = 键序前缀，收割量可精确断言。"""
    for i in range(1, n + 1):
        redis.set(
            f"{EVENT_PREFIX}{i:06d}",
            json.dumps({
                "event_id": f"e{i:06d}", "event_type": "order.created",
                "data": {}, "timestamp": 1000.0 + i, "tenant_id": "default",
            }),
        )


def _seed_subscriptions(redis: _KeyspaceRedis, n: int) -> None:
    for i in range(1, n + 1):
        redis.set(
            f"{SUB_PREFIX}{i:04d}",
            json.dumps({
                "subscription_id": f"s{i:04d}", "event_type": "order.created",
                "flow_id": "f1", "created_at": 2000.0 + i, "tenant_id": "default",
            }),
        )


def _app(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(events_api.router, prefix="/api")
    return TestClient(app)


def test_total_is_sentinel_when_scan_truncated(monkeypatch):
    """触界页：``total=-1`` 哨兵——已扫前缀内的计数不得冒充匹配总数。"""
    redis = _KeyspaceRedis()
    _seed_events(redis, 12)
    monkeypatch.setattr(events_api, "EVENT_SCAN_MAX_KEYS", 8)
    client = _app(redis)

    body = client.get("/api/events").json()

    assert body["scan_truncated"] is True
    assert body["total"] == -1, body
    assert 0 < len(body["events"]) <= 8
    assert body["returned"] == len(body["events"])
    # 窗口内翻页语义不变（本页之后没有更多页）
    assert body["truncated"] is False and body["next_cursor"] is None


def test_total_normal_when_scan_complete():
    """正控：完整扫描时 ``total`` 照旧是匹配总数。"""
    redis = _KeyspaceRedis()
    _seed_events(redis, 5)
    client = _app(redis)

    body = client.get("/api/events").json()

    assert body["scan_truncated"] is False
    assert body["total"] == 5 and len(body["events"]) == 5


def test_naive_client_detects_incompleteness_under_sentinel():
    """naive 客户端（follow next_cursor 至 truncated=false）在触界 keyspace
    （真预算：6000 事件 > 5000 键预算）上——pre：终页 ``truncated=false`` 且
    ``total>0``，「完整」假象静默丢 1000/6000；post：哨兵使其依契约可判定
    不完整。"""
    redis = _KeyspaceRedis()
    _seed_events(redis, 6000)
    client = _app(redis)

    pages = []
    url = "/api/events?limit=200"
    for _ in range(40):
        body = client.get(url).json()
        pages.append(body)
        if not body["truncated"]:
            break
        url = f"/api/events?limit=200&cursor={body['next_cursor']}"
    else:
        raise AssertionError("翻页未在预算轮次内终止")

    last = pages[-1]
    assert last["truncated"] is False  # naive 终止条件已满足
    # 钉死契约：完整遍历 = truncated=false 且 scan_truncated=false 且 total>=0
    complete = (
        last["truncated"] is False
        and last["scan_truncated"] is False
        and last["total"] >= 0
    )
    assert complete is False, (last["total"], last["scan_truncated"])
    assert all(p["total"] == -1 for p in pages)
    assert all(p["scan_truncated"] is True for p in pages)
    # 只收割了已扫前缀：6000 事件中扫到 5000 个键
    assert sum(len(p["events"]) for p in pages) == 5000


def test_subscriptions_total_sentinel_when_scan_truncated(monkeypatch):
    """subscriptions 同口径：触界页 ``total=-1``。"""
    redis = _KeyspaceRedis()
    _seed_subscriptions(redis, 6)
    monkeypatch.setattr(events_api, "EVENT_SCAN_MAX_KEYS", 4)
    client = _app(redis)

    body = client.get("/api/events/subscriptions").json()

    assert body["scan_truncated"] is True
    assert body["total"] == -1, body
    assert len(body["subscriptions"]) == 4


def test_subscriptions_total_normal_when_scan_complete():
    """正控：完整扫描时 subscriptions ``total`` 照旧。"""
    redis = _KeyspaceRedis()
    _seed_subscriptions(redis, 3)
    client = _app(redis)

    body = client.get("/api/events/subscriptions").json()

    assert body["scan_truncated"] is False
    assert body["total"] == 3
