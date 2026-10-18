"""#202：console logs/queues 读面必须告别 ``KEYS`` 全库扫描（改有界 ``SCAN``）。

Redis 单线程执行命令，``KEYS`` 在百万键级会阻塞所有客户端；这些端点只有
GET 权限，最低权限 viewer 反复请求即可拖死共享 Redis。本组判据：

1. **调用面**：5 个端点（/logs、/logs/instance/{id}、/logs/stats、/queues、
   /queues/dlq）在「KEYS 一调用就抛错」的 Redis 替身上仍全部 200——不再有
   ``KEYS`` 路径；每个用例 teardown 再断言 ``keys_calls == []``（负控抓手）。
2. **正确性**：10 万键 keyspace 下种子日志 / 队列 / 死信仍被返回。
3. **有界**：共用扫描工具的轮次 / 键数 / 耗时三道上限触界即停并标记
   ``truncated``；页大小被 clamp 到 ``PAGE_SIZE_MAX``，单条 SCAN 不会退化成
   一次长阻塞；触界记 warning（fail-safe 可见，不静默）。
4. **已知键直读**：``/queues`` 的字面键走 EXISTS，只有通配项才 SCAN。
"""
import fnmatch
import json
import logging
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import logs as logs_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
from api.redis_scan import PAGE_SIZE_MAX, scan_keys  # noqa: E402

LOG_SERVICE = "flow_worker"
LOG_KEY_A = f"plaita:logs:{LOG_SERVICE}:worker-A"
LOG_KEY_B = f"plaita:logs:{LOG_SERVICE}:worker-B"
FLOW_STREAM = "plaita:flow:queue"
REDIS_QUEUE = "plaita:redis_queue:acme-1"
DLQ_STREAM = "plaita:flow:queue:dlq"


class NoKeysRedis(FakeRedis):
    """``KEYS`` 一被调用就抛错——把「读面还在全库扫描」变成精确红。

    ``scan`` 顺带记录每次调用的 ``count`` / ``match``，用来断言页大小上限与
    「字面键不扫描」。
    """

    def __init__(self) -> None:
        super().__init__(decode_responses=True)
        self.keys_calls: list[str] = []
        self.scan_calls: list[dict] = []

    def keys(self, pattern: str = "*", *args, **kwargs):
        self.keys_calls.append(pattern)
        raise AssertionError(
            f"端点调用了 redis KEYS({pattern})——共享 Redis 上的一次全库阻塞扫描（#202）"
        )

    def scan(self, *args, **kwargs):
        self.scan_calls.append(
            {"count": kwargs.get("count"), "match": kwargs.get("match")}
        )
        return super().scan(*args, **kwargs)


def _seed(redis: FakeRedis) -> None:
    redis.xadd(LOG_KEY_A, {
        "timestamp": "2026-10-18T10:00:00", "level": "ERROR",
        "service_type": LOG_SERVICE, "instance_id": "worker-A",
        "message": "A-error-1", "context": "{}",
    })
    redis.xadd(LOG_KEY_B, {
        "timestamp": "2026-10-18T10:00:01", "level": "INFO",
        "service_type": LOG_SERVICE, "instance_id": "worker-B",
        "message": "B-info-2", "context": "{}",
    })
    redis.xadd(FLOW_STREAM, {"payload": json.dumps({"type": "start", "flow_id": "f1"})})
    redis.rpush(REDIS_QUEUE, json.dumps({"task": 1}))
    redis.xadd(DLQ_STREAM, {"payload": json.dumps({"reason": "boom"})})


def _app(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(logs_api.router, prefix="/api")
    app.include_router(queues_api.router, prefix="/api")
    return TestClient(app)


@pytest.fixture()
def env():
    redis = NoKeysRedis()
    _seed(redis)
    client = _app(redis)
    yield client, redis
    assert redis.keys_calls == [], (
        f"端点仍走了 KEYS 全库扫描：{redis.keys_calls}（#202）"
    )


# ---------------------------------------------------------------- 调用面（5 个端点）

def test_logs_does_not_call_keys(env):
    client, _ = env
    body = client.get("/api/logs").json()
    assert {(r["instance_id"], r["message"]) for r in body["logs"]} == {
        ("worker-A", "A-error-1"),
        ("worker-B", "B-info-2"),
    }


def test_logs_instance_does_not_call_keys(env):
    client, _ = env
    body = client.get("/api/logs/instance/worker-A").json()
    assert [(r["instance_id"], r["message"]) for r in body["logs"]] == [
        ("worker-A", "A-error-1")
    ]


def test_logs_stats_does_not_call_keys(env):
    client, _ = env
    body = client.get("/api/logs/stats").json()
    assert body["total_logs"] == 2
    assert {s["service_type"] for s in body["stats"]} == {LOG_SERVICE}


def test_queues_does_not_call_keys_and_skips_scans_for_literal_keys(env):
    client, redis = env
    body = client.get("/api/queues").json()
    names = {q["name"] for q in body["queues"]}
    assert {FLOW_STREAM, REDIS_QUEUE} <= names
    # 已知字面键直读：只允许 KNOWN_QUEUES 里的两个通配项各扫一次
    assert {c["match"] for c in redis.scan_calls} == {
        "plaita:redis_queue:*",
        "plaita:kafka_queue:*",
    }


def test_queues_dlq_does_not_call_keys(env):
    client, _ = env
    body = client.get("/api/queues/dlq").json()
    streams = {s["name"]: s for s in body["streams"]}
    assert DLQ_STREAM in streams
    assert streams[DLQ_STREAM]["length"] == 1


def test_scan_page_size_is_clamped_on_endpoints(env):
    """端点不得传入超大 count——否则单条 SCAN 就退化成一次长阻塞。"""
    client, redis = env
    client.get("/api/logs")
    client.get("/api/queues")
    counts = [c["count"] for c in redis.scan_calls if c["count"] is not None]
    assert counts, "端点根本没有走 SCAN"
    assert max(counts) <= PAGE_SIZE_MAX


# ---------------------------------------------------------------- 正确性：10 万键

class _KeyspaceRedis:
    """受控 keyspace：只实现端点用到的命令，SCAN 成本与页大小成正比。

    fakeredis 的 SCAN 每次调用都重建整张键表（O(N)/次），10 万键下要跑分钟级；
    这里用 O(页大小) 的实现跑「10 万键 + 端点仍返回正确结果」这一条，成本口径
    与真实 Redis 一致。``keys`` 仍一调用就抛错（负控抓手）。
    """

    def __init__(self, noise: int = 0) -> None:
        self._data: dict[str, tuple[str, list]] = {}
        self._noise = int(noise)
        self._sorted: list[str] | None = None
        self.keys_calls: list[str] = []
        self.scan_calls: list[dict] = []

    # ---- 写入 ----
    def _touch(self) -> None:
        self._sorted = None

    def xadd(self, key, fields):
        if key not in self._data:
            self._data[key] = ("stream", [])
            self._touch()
        entries = self._data[key][1]
        entry_id = f"{len(entries) + 1}-0"
        entries.append((entry_id, dict(fields)))
        return entry_id

    def rpush(self, key, value):
        if key not in self._data:
            self._data[key] = ("list", [])
            self._touch()
        self._data[key][1].append(value)

    def set(self, key, value):
        if key not in self._data:
            self._touch()
        self._data[key] = ("string", [value])

    # ---- 命令 ----
    def _keys(self) -> list[str]:
        if self._sorted is None:
            noise = [f"plaita:noise:{i:07d}" for i in range(self._noise)]
            self._sorted = sorted(self._data) + noise
        return self._sorted

    def keys(self, pattern="*", *args, **kwargs):
        self.keys_calls.append(pattern)
        raise AssertionError(
            f"端点调用了 redis KEYS({pattern})——共享 Redis 上的一次全库阻塞扫描（#202）"
        )

    def scan(self, cursor=0, match=None, count=10):
        self.scan_calls.append({"count": count, "match": match})
        keys = self._keys()
        start = int(cursor)
        end = min(start + int(count), len(keys))
        batch = [k for k in keys[start:end] if fnmatch.fnmatch(k, match or "*")]
        return (0 if end >= len(keys) else end), batch

    def exists(self, key):
        return 1 if key in self._data else 0

    def type(self, key):
        item = self._data.get(key)
        return item[0] if item else "none"

    def xlen(self, key):
        item = self._data.get(key)
        return len(item[1]) if item and item[0] == "stream" else 0

    def llen(self, key):
        item = self._data.get(key)
        return len(item[1]) if item and item[0] == "list" else 0

    def zcard(self, key):
        item = self._data.get(key)
        return len(item[1]) if item and item[0] == "zset" else 0

    def xrevrange(self, key, max="+", min="-", count=None):
        item = self._data.get(key)
        if not item or item[0] != "stream":
            raise RuntimeError("WRONGTYPE")
        rows = list(reversed(item[1]))
        return rows if count is None else rows[: int(count)]

    def xrange(self, key, min="-", max="+", count=None):
        item = self._data.get(key)
        if not item or item[0] != "stream":
            raise RuntimeError("WRONGTYPE")
        rows = list(item[1])
        return rows if count is None else rows[: int(count)]


def _get_bounded(client, redis, url):
    """单请求内每个 pattern 的 SCAN 轮次都必须在预算内（#202 有界）。"""
    before = len(redis.scan_calls)
    resp = client.get(url)
    rounds: dict[str, int] = {}
    for call in redis.scan_calls[before:]:
        rounds[call["match"]] = rounds.get(call["match"], 0) + 1
    assert rounds, f"{url} 没有走 SCAN"
    assert max(rounds.values()) <= 250, f"{url} 单个 pattern 的 SCAN 轮次超预算: {rounds}"
    return resp


def test_endpoints_stay_correct_on_100k_keyspace():
    redis = _KeyspaceRedis(noise=100_000)
    _seed(redis)
    client = _app(redis)

    logs = _get_bounded(client, redis, "/api/logs").json()
    assert {(r["instance_id"], r["message"]) for r in logs["logs"]} == {
        ("worker-A", "A-error-1"),
        ("worker-B", "B-info-2"),
    }
    instance = _get_bounded(client, redis, "/api/logs/instance/worker-A").json()
    assert [(r["instance_id"], r["message"]) for r in instance["logs"]] == [
        ("worker-A", "A-error-1")
    ]
    stats = _get_bounded(client, redis, "/api/logs/stats").json()
    assert stats["total_logs"] == 2

    queues = _get_bounded(client, redis, "/api/queues").json()
    names = {q["name"] for q in queues["queues"]}
    assert {FLOW_STREAM, REDIS_QUEUE} <= names

    dlq = _get_bounded(client, redis, "/api/queues/dlq").json()
    assert DLQ_STREAM in {s["name"] for s in dlq["streams"]}
    assert redis.keys_calls == []


# ---------------------------------------------------------------- 有界扫描工具

class _BottomlessRedis:
    """游标永不归零、也没有匹配键——模拟超出预算的超大 keyspace。"""

    def __init__(self, total: int = 10_000_000) -> None:
        self.total = total
        self.calls = 0
        self.counts: list[int] = []

    def scan(self, cursor=0, match=None, count=10):
        self.calls += 1
        self.counts.append(count)
        nxt = int(cursor) + int(count)
        return (0 if nxt >= self.total else nxt), []


class _DenseRedis:
    """每页都返回满页匹配键（匹配密集区）。"""

    def __init__(self, total: int) -> None:
        self.total = total
        self.calls = 0

    def scan(self, cursor=0, match=None, count=10):
        self.calls += 1
        start = int(cursor)
        end = min(start + int(count), self.total)
        keys = [f"plaita:logs:svc:{i}" for i in range(start, end)]
        return (0 if end >= self.total else end), keys


def test_bottomless_scan_stops_at_round_budget():
    fake = _BottomlessRedis()
    result = scan_keys(fake, "plaita:logs:*", count=100, max_rounds=7)
    assert fake.calls == 7
    assert result.rounds == 7
    assert result.truncated is True
    assert result.keys == []


def test_scan_stops_at_key_budget():
    fake = _DenseRedis(total=10_000)
    result = scan_keys(
        fake, "plaita:logs:*", count=100, max_keys=250, max_rounds=10_000
    )
    assert len(result.keys) == 250
    assert result.truncated is True
    assert fake.calls < 10_000


def test_scan_stops_at_time_budget():
    fake = _BottomlessRedis()
    result = scan_keys(fake, "plaita:logs:*", count=100, time_budget_s=0.0)
    assert result.truncated is True
    assert fake.calls == 1  # 第一页后即到期，不再空转


def test_scan_returns_everything_within_budget():
    fake = _DenseRedis(total=250)
    result = scan_keys(fake, "plaita:logs:*", count=100)
    assert result.truncated is False
    assert len(result.keys) == 250
    assert len(set(result.keys)) == 250


def test_scan_dedupes_repeated_keys():
    class _Repeating:
        def __init__(self):
            self.calls = 0

        def scan(self, cursor=0, match=None, count=10):
            self.calls += 1
            if self.calls == 1:
                return 1, ["plaita:logs:a", "plaita:logs:b"]
            return 0, ["plaita:logs:b", "plaita:logs:c"]

    result = scan_keys(_Repeating(), "plaita:logs:*", count=2)
    assert result.keys.count("plaita:logs:b") == 1
    assert set(result.keys) == {"plaita:logs:a", "plaita:logs:b", "plaita:logs:c"}


def test_scan_page_size_is_clamped():
    fake = _BottomlessRedis()
    scan_keys(fake, "plaita:logs:*", count=10_000_000, max_rounds=3)
    assert fake.counts and max(fake.counts) <= PAGE_SIZE_MAX


def test_truncation_is_logged_not_silent(caplog):
    fake = _BottomlessRedis()
    with caplog.at_level(logging.WARNING, logger="api.redis_scan"):
        result = scan_keys(fake, "plaita:logs:*", count=100, max_rounds=3)
    assert result.truncated is True
    assert "有界 SCAN 触界" in caplog.text


def test_truncation_warning_scrubs_pattern_credentials(caplog):
    fake = _BottomlessRedis()
    pattern = "plaita:logs:svc:*?access_token=SECRET-VALUE"
    with caplog.at_level(logging.WARNING, logger="api.redis_scan"):
        scan_keys(fake, pattern, count=100, max_rounds=2)
    assert "有界 SCAN 触界" in caplog.text
    assert "SECRET-VALUE" not in caplog.text
