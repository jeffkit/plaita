"""有界 SCAN 工具（plaita#202）。

console 的 logs/queues 读面此前用 ``redis.keys()`` 做全库遍历。Redis 单线程
执行命令：``KEYS`` 在百万键级会**阻塞所有客户端**（worker / 队列 / 事件总线
一并停摆），而这些端点只有 ``require_auth`` 的 GET 权限，最低权限的 viewer
反复请求即可拖死共享 Redis。

本模块把「扫描」统一收进 ``scan_keys``：

* **分页**：每次 ``SCAN`` 的 ``count`` 有硬上限（``PAGE_SIZE_MAX``），单条
  命令的阻塞时长因此与 keyspace 规模无关；
* **有界**：总轮次、收集键数、墙钟三者任一触界即停，返回
  ``truncated=True``（fail-safe：调用方照常给出部分结果，不抛错、不退化回
  ``KEYS``）；
* **可见**：触界记一条 warning（pattern 经 ``_scrub_url_text`` 清洗），不静默
  吞掉「结果不完整」这一事实。

语义与 ``api/events.py`` 此前自带的 ``_scan_keys`` 一致（游标循环直到 0，键统一
解码为 ``str``），只是加了上限；A2R45-04 起 events 两个列表读面也改走本函数，
console 的键枚举单源于此。``SCAN`` 允许重复返回同一个键，这里按首次出现去重
（与 ``KEYS`` 的结果集一致）。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, List

from redis import Redis

from plaita.core.url_redaction import _scrub_url_text

_log = logging.getLogger(__name__)

# 单次 SCAN 的 count 上限：Redis 的 count 只是提示，但过大的 count 会让单条
# 命令退化成一次长阻塞——这与修复目标相反。
PAGE_SIZE_MAX = 1000
DEFAULT_COUNT = 500

# 单次请求的预算：收集键数 / SCAN 轮次 / 墙钟。三者都是硬上限。
# 10 万键 @ count=500 = 200 轮，落在 250 轮预算内（正常返回完整结果）；
# 超出预算的 keyspace（千万级）停在第 250 轮，返回部分结果 + warning。
DEFAULT_MAX_KEYS = 5000
DEFAULT_MAX_ROUNDS = 250
DEFAULT_TIME_BUDGET_S = 1.0


@dataclass(frozen=True)
class ScanResult:
    """一次有界扫描的结果。

    ``truncated`` 为 True 表示**触界提前停止**：``keys`` 只是匹配键的一部分
    （fail-safe 的部分结果），调用方不应把它当成全集。
    """

    keys: List[str] = field(default_factory=list)
    truncated: bool = False
    rounds: int = 0
    elapsed_s: float = 0.0


def escape_glob(value: str) -> str:
    """转义 Redis ``SCAN MATCH`` 通配字符（把外部标识符当字面量）。

    与 ``api/services.py`` 的 ``_escape_glob`` 同义：instance_id / service_type
    来自查询参数，直接拼进 pattern 会让 ``*``/``?``/``[]`` 扩大匹配面（甚至
    退化成全库扫描）。下推键级模式前必须先转义。
    """
    return "".join("\\" + ch if ch in "\\*?[]" else ch for ch in value)


def scan_keys(
    redis: Redis,
    pattern: str,
    *,
    count: int = DEFAULT_COUNT,
    max_keys: int = DEFAULT_MAX_KEYS,
    max_rounds: int = DEFAULT_MAX_ROUNDS,
    time_budget_s: float = DEFAULT_TIME_BUDGET_S,
) -> ScanResult:
    """用**有界** ``SCAN`` 取匹配 ``pattern`` 的键（替代 ``KEYS``）。

    触界时提前返回已收集的键并标记 ``truncated``——绝不 ``KEYS``、绝不无界
    空转。``pattern`` 只用于 SCAN 的 MATCH 与日志，不参与任何键拼接。
    """
    page = max(1, min(int(count), PAGE_SIZE_MAX))
    key_budget = max(1, int(max_keys))
    round_budget = max(1, int(max_rounds))
    deadline = time.monotonic() + max(0.0, float(time_budget_s))

    collected: dict[str, None] = {}
    cursor: Any = 0
    rounds = 0
    truncated = False
    started = time.monotonic()

    while True:
        cursor, batch = redis.scan(cursor=cursor, match=pattern, count=page)
        rounds += 1
        for key in batch:
            collected.setdefault(key if isinstance(key, str) else key.decode(), None)
        if cursor == 0:
            break
        if len(collected) >= key_budget:
            truncated = True
            break
        if rounds >= round_budget:
            truncated = True
            break
        if time.monotonic() >= deadline:
            truncated = True
            break

    keys = list(collected)
    if len(keys) > key_budget:
        keys = keys[:key_budget]
        truncated = True
    elapsed = time.monotonic() - started

    if truncated:
        _log.warning(
            "有界 SCAN 触界，结果可能不完整（fail-safe 返回部分键）：pattern=%s "
            "收集=%d/%d 轮次=%d/%d 耗时=%.0fms",
            _scrub_url_text(pattern),
            len(keys),
            key_budget,
            rounds,
            round_budget,
            elapsed * 1000,
        )

    return ScanResult(keys=keys, truncated=truncated, rounds=rounds, elapsed_s=elapsed)
