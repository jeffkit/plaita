#!/usr/bin/env python3
"""#202 repro：console logs/queues 读面用 KEYS 全库扫描 → viewer 级 Redis 阻塞 DoS。

口径（受控 keyspace，不依赖真 Redis 的百万键；Redis 单线程执行命令，因此
「单条命令的阻塞时长」就是它对**其它所有客户端**的影响）：

  1) 10 万键（2 条日志 Stream + 1 条 flow 队列 Stream + 1 条 Redis 队列 list
     + 1 条 DLQ Stream，其余为无关噪声键）。假 Redis 按每键槽 2µs 模拟
     单线程命令耗时：
       - ``KEYS``  = 一条命令遍历全库 → 100k × 2µs = 200ms 单命令阻塞
       - ``SCAN``  = 每页 count 槽 → 500 槽 = 1ms（可与其他客户端交错）
     断言：5 个 GET 端点（/logs、/logs/instance/{id}、/logs/stats、/queues、
     /queues/dlq）都**不调用 KEYS**；单条命令模拟阻塞 ≤ 5ms。
  2) 1000 万虚拟键槽（不落内存）：超界必须**有界 fail-safe**——仍 200、快速
     返回、不 KEYS、不无界空转。
  3) 正确性：10 万键下种子日志 / DLQ 条目仍被返回。

运行：``PYTHONPATH=/tmp/w40 /Users/jeff.jie/.venvs/plaita/bin/python \
/tmp/w40-out/repro-202.py``（仓根 /tmp/w40；本脚本自行把 backend 放进 sys.path）
"""
from __future__ import annotations

import fnmatch
import sys
import time
import traceback
from pathlib import Path

_HERE = Path(__file__).resolve()
# 仓内位置 docs/proofs/data/202-repro.py → parents[3] = 仓根；worktree 里也兼容
_CANDIDATES = [p for p in (_HERE.parents[3] if len(_HERE.parents) > 3 else None,) if p]
_CANDIDATES.append(Path("/tmp/w40"))
REPO_ROOT = next(
    (p for p in _CANDIDATES if (p / "plaita-console" / "backend").is_dir()),
    Path("/tmp/w40"),
)
BACKEND = REPO_ROOT / "plaita-console" / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from fastapi import FastAPI  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from api import logs as logs_api  # noqa: E402
from api import queues as queues_api  # noqa: E402

# 模拟单线程 Redis：每个键槽 2µs（KEYS 100k = 200ms；SCAN count=500 = 1ms）
PER_SLOT_SECONDS = 2e-6
# 单条命令可接受的最大模拟阻塞（真 Redis 上 SCAN 一页与 KEYS 全库差两个量级）
MAX_COMMAND_BLOCK_MS = 5.0
# 假 Redis 对超大 keyspace 不做真实 sleep（会跑 20s），直接报出 KEYS 的代价
HUGE_KEYSPACE = 10_000_000
KEYSPACE_NOISE = 100_000

failures: list[str] = []


def fail(msg: str) -> None:
    failures.append(msg)
    print(f"  [FAIL] {msg}")


class SimRedis:
    """最小 Redis 替身：模拟 KEYS/SCAN 的单命令阻塞成本，并记录命令。"""

    def __init__(self, virtual_extra: int = 0) -> None:
        self._data: dict[str, tuple[str, list]] = {}
        self.virtual_extra = int(virtual_extra)
        self._sorted: list[str] | None = None
        self.keys_calls: list[str] = []
        self.scan_calls: int = 0
        self.scan_by_pattern: dict[str, int] = {}
        self.max_scan_page: int = 0
        self.max_command_block_ms: float = 0.0
        self.commands: list[tuple[str, str | None]] = []

    # ---- 种子数据 ----
    def _touch(self) -> None:
        self._sorted = None

    def xadd(self, key: str, fields: dict) -> str:
        if key not in self._data:
            self._data[key] = ("stream", [])
            self._touch()
        entries = self._data[key][1]
        entry_id = f"{len(entries) + 1}-0"
        entries.append((entry_id, dict(fields)))
        return entry_id

    def rpush(self, key: str, value: str) -> None:
        if key not in self._data:
            self._data[key] = ("list", [])
            self._touch()
        self._data[key][1].append(value)

    # ---- 命令 ----
    def _keys_sorted(self) -> list[str]:
        if self._sorted is None:
            self._sorted = sorted(self._data)
        return self._sorted

    def _charge(self, slots: int, name: str, pattern: str | None = None) -> None:
        if slots >= HUGE_KEYSPACE:
            raise RuntimeError(
                f"{name} 需要遍历 {slots} 个键槽——单命令模拟阻塞 "
                f"{slots * PER_SLOT_SECONDS:.1f}s（真实 Redis 会在此期间阻塞所有客户端）"
            )
        self.commands.append((name, pattern))
        if slots <= 0:
            return
        block_ms = slots * PER_SLOT_SECONDS * 1000
        self.max_command_block_ms = max(self.max_command_block_ms, block_ms)
        time.sleep(slots * PER_SLOT_SECONDS)

    def keys(self, pattern: str = "*"):
        self.keys_calls.append(pattern)
        total = len(self._keys_sorted()) + self.virtual_extra
        self._charge(total, "KEYS", pattern)
        return [k for k in self._keys_sorted() if fnmatch.fnmatch(k, pattern)]

    def scan(self, cursor: int = 0, match: str | None = None, count: int = 10):
        self.scan_calls += 1
        self.scan_by_pattern[match or "*"] = self.scan_by_pattern.get(match or "*", 0) + 1
        count = max(1, int(count))
        keys = self._keys_sorted()
        total = len(keys) + self.virtual_extra
        start = int(cursor)
        end = min(start + count, total)
        visible_end = min(end, len(keys))
        batch = [k for k in keys[start:visible_end] if fnmatch.fnmatch(k, match or "*")]
        self.max_scan_page = max(self.max_scan_page, len(batch))
        self._charge(end - start, "SCAN", match)
        return (0 if end >= total else end), batch

    def exists(self, key: str) -> int:
        return 1 if key in self._data else 0

    def type(self, key: str) -> str:
        item = self._data.get(key)
        return item[0] if item else "none"

    def xlen(self, key: str) -> int:
        item = self._data.get(key)
        return len(item[1]) if item and item[0] == "stream" else 0

    def llen(self, key: str) -> int:
        item = self._data.get(key)
        return len(item[1]) if item and item[0] == "list" else 0

    def zcard(self, key: str) -> int:
        item = self._data.get(key)
        return len(item[1]) if item and item[0] == "zset" else 0

    def xrevrange(self, key: str, max: str = "+", min: str = "-", count=None):
        item = self._data.get(key)
        if not item or item[0] != "stream":
            raise RuntimeError("WRONGTYPE")
        rows = list(reversed(item[1]))
        return rows if count is None else rows[: int(count)]

    def xrange(self, key: str, min: str = "-", max: str = "+", count=None):
        item = self._data.get(key)
        if not item or item[0] != "stream":
            raise RuntimeError("WRONGTYPE")
        rows = list(item[1])
        return rows if count is None else rows[: int(count)]


LOG_SERVICE = "flow_worker"
LOG_KEY_A = f"plaita:logs:{LOG_SERVICE}:worker-A"
LOG_KEY_B = f"plaita:logs:{LOG_SERVICE}:worker-B"
FLOW_STREAM = "plaita:flow:queue"
REDIS_QUEUE = "plaita:redis_queue:acme-1"
DLQ_STREAM = "plaita:flow:queue:dlq"

ENDPOINTS = [
    ("/api/logs", "GET /logs"),
    ("/api/logs/instance/worker-A", "GET /logs/instance/{id}"),
    ("/api/logs/stats", "GET /logs/stats"),
    ("/api/queues", "GET /queues"),
    ("/api/queues/dlq", "GET /queues/dlq"),
]


def _seed(redis: SimRedis) -> None:
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
    redis.xadd(FLOW_STREAM, {"payload": '{"type": "start", "flow_id": "f1"}'})
    redis.rpush(REDIS_QUEUE, '{"task": 1}')
    redis.xadd(DLQ_STREAM, {"payload": '{"reason": "boom", "source_stream": "plaita:flow:queue"}'})
    for i in range(KEYSPACE_NOISE):
        redis.rpush(f"plaita:noise:{i:07d}", "x")


def _client(redis: SimRedis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(logs_api.router, prefix="/api")
    app.include_router(queues_api.router, prefix="/api")
    return TestClient(app, raise_server_exceptions=False)


def _call(client: TestClient, redis: SimRedis, url: str, label: str) -> tuple[int, float, dict]:
    redis.keys_calls.clear()
    redis.scan_calls = 0
    redis.scan_by_pattern = {}
    redis.max_scan_page = 0
    redis.max_command_block_ms = 0.0
    redis.commands.clear()
    t0 = time.monotonic()
    resp = client.get(url)
    wall_ms = (time.monotonic() - t0) * 1000
    stats = {
        "keys": len(redis.keys_calls),
        "scans": redis.scan_calls,
        "page_max": redis.max_scan_page,
        "block_ms": redis.max_command_block_ms,
        "wall_ms": wall_ms,
        "scan_max_per_pattern": max(redis.scan_by_pattern.values(), default=0),
    }
    print(
        f"  {label:<28} HTTP {resp.status_code}  KEYS={stats['keys']} "
        f"SCAN={stats['scans']} 单命令阻塞≤{stats['block_ms']:.1f}ms "
        f"扫描页≤{stats['page_max']} 单pattern轮次≤{stats['scan_max_per_pattern']} 墙钟={wall_ms:.0f}ms"
    )
    if stats["keys"]:
        fail(
            f"{label} 调用了 redis KEYS（{'/'.join(redis.keys_calls)}）——"
            f"单命令模拟阻塞 {stats['block_ms']:.1f}ms，共享 Redis 单线程期间"
            "所有客户端（worker/队列/事件总线）一起停摆"
        )
    if stats["block_ms"] > MAX_COMMAND_BLOCK_MS:
        fail(
            f"{label} 单条命令模拟阻塞 {stats['block_ms']:.1f}ms > "
            f"{MAX_COMMAND_BLOCK_MS:.0f}ms（SCAN 分页失败，仍是一次性全库遍历）"
        )
    return resp.status_code, wall_ms, stats


def case_small_keyspace() -> None:
    print(f"\n== case A: 受控 10 万键 keyspace（种子 2 日志 + 1 队列 + 1 DLQ）==")
    redis = SimRedis()
    _seed(redis)
    print(f"  键数 = {len(redis._keys_sorted())}")
    client = _client(redis)

    status, _, _ = _call(client, redis, "/api/logs", "GET /logs")
    body = client.get("/api/logs").json()
    rows = [(r["instance_id"], r["message"]) for r in body["logs"]]
    if status != 200:
        fail(f"GET /logs HTTP {status}（应 200）")
    if rows != [("worker-A", "A-error-1"), ("worker-B", "B-info-2")] and set(rows) != {
        ("worker-A", "A-error-1"), ("worker-B", "B-info-2")
    }:
        fail(f"GET /logs 在 10 万键下未返回种子日志（实际 {rows}）")

    status, _, _ = _call(client, redis, "/api/logs/instance/worker-A", "GET /logs/instance/{id}")
    body = client.get("/api/logs/instance/worker-A").json()
    rows = [(r["instance_id"], r["message"]) for r in body["logs"]]
    if status != 200 or rows != [("worker-A", "A-error-1")]:
        fail(f"GET /logs/instance/worker-A 结果不对（HTTP {status}，{rows}）")

    status, _, _ = _call(client, redis, "/api/logs/stats", "GET /logs/stats")
    stats_body = client.get("/api/logs/stats").json()
    if status != 200 or stats_body.get("total_logs") != 2:
        fail(f"GET /logs/stats 未统计到 2 条日志（HTTP {status}，{stats_body}）")

    status, _, _ = _call(client, redis, "/api/queues", "GET /queues")
    queues_body = client.get("/api/queues").json()
    names = {q["name"] for q in queues_body.get("queues", [])}
    if status != 200 or FLOW_STREAM not in names or REDIS_QUEUE not in names:
        fail(f"GET /queues 漏掉种子队列（HTTP {status}，{sorted(names)}）")

    status, _, _ = _call(client, redis, "/api/queues/dlq", "GET /queues/dlq")
    dlq_body = client.get("/api/queues/dlq").json()
    dlq_names = {s["name"] for s in dlq_body.get("streams", [])}
    dlq_entry_count = sum(len(s.get("entries", [])) for s in dlq_body.get("streams", []))
    if status != 200 or DLQ_STREAM not in dlq_names or dlq_entry_count != 1:
        fail(f"GET /queues/dlq 未返回种子死信（HTTP {status}，{dlq_names}，条目 {dlq_entry_count}）")


def case_huge_keyspace() -> None:
    print(f"\n== case B: 1000 万虚拟键槽（超界 fail-safe）==")
    redis = SimRedis(virtual_extra=HUGE_KEYSPACE)
    _seed(redis)
    client = _client(redis)
    for url, label in ENDPOINTS:
        t0 = time.monotonic()
        status, wall_ms, stats = _call(client, redis, url, label)
        if status != 200:
            fail(f"{label} 超界后未 fail-safe（HTTP {status}，应 200 + 部分结果）")
        if wall_ms > 3000:
            fail(f"{label} 超界后墙钟 {wall_ms:.0f}ms > 3000ms（有界失败）")
        if stats["keys"] == 0 and stats["scan_max_per_pattern"] > 250:
            fail(
                f"{label} 单个 pattern 的 SCAN 轮次无界"
                f"（{stats['scan_max_per_pattern']} 轮 > 250 轮预算）"
            )


def case_bounded_unit() -> None:
    """直接对共用有界扫描工具做单元级断言（超界必须停且标记 truncated）。"""
    print("\n== case C: 有界扫描工具（无限 keyspace 必须停）==")
    try:
        from api.redis_scan import scan_keys  # noqa: F401
    except Exception:
        print("  [SKIP] api.redis_scan 尚不存在（修复前）")
        return

    class BottomlessRedis:
        def __init__(self) -> None:
            self.calls = 0

        def scan(self, cursor=0, match=None, count=10):
            self.calls += 1
            return int(cursor) + int(count), []

    fake = BottomlessRedis()
    t0 = time.monotonic()
    result = scan_keys(fake, "plaita:logs:*", count=100, max_rounds=7)
    elapsed_ms = (time.monotonic() - t0) * 1000
    print(f"  rounds={fake.calls} truncated={result.truncated} keys={len(result.keys)} {elapsed_ms:.0f}ms")
    if fake.calls != 7:
        fail(f"有界扫描未按 max_rounds 停止（实际 {fake.calls} 轮，应 7）")
    if not result.truncated:
        fail("有界扫描触界却未标记 truncated（调用方无法获知结果不完整）")


def main() -> int:
    print("#202 repro: KEYS 全库扫描 vs 有界 SCAN（受控 keyspace）")
    try:
        case_small_keyspace()
        case_huge_keyspace()
        case_bounded_unit()
    except Exception:
        traceback.print_exc()
        fail("repro 执行异常（见上方栈）")
    print()
    if failures:
        print(f"result: RED ({len(failures)} 项)")
        for i, msg in enumerate(failures, 1):
            print(f"  {i}. {msg}")
        return 1
    print("result: GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
