"""A53-04（duo-a 第五十三轮）：trim 可观测 / ACK 顺序。

## 缺陷

``dead_letter`` 的 ``XTRIM`` 返回值被丢弃、异常只记 DEBUG：裁剪（未处理证据
被丢）与 XTRIM 失败（DLQ 无界增长）都静默；``dead_lettered``/alert 在 ack
之后，ack 失败时已 durable 的死信对指标/告警不可见。

## 修复契约

- XTRIM removed>0 → ``dlq_trimmed`` + WARNING（限频）；异常 → WARNING +
  ``dlq_trim_failures``；dead_lettered/error/alert 先于 ack，ack 失败另计
  ``dlq_ack_failures`` 且 WARNING 含 ``source_ack=failed``；
- alert 增加 ``dlq_id/dlq_length/dlq_max_len/trimmed_count/schema_version``；
- stats 暴露 ``dlq_max_len`` / ``dlq_saturated``。

负控（还原修复后精确红）：把 XTRIM 异常处理改回 DEBUG/丢弃返回值、
``dead_lettered`` 移回 ack 之后 → 本文件相应用例红。
"""
from __future__ import annotations

import logging

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.task_queue import RedisStreamTaskQueue

STREAM = "plaita:flow:queue:trim-test"
DLQ = f"{STREAM}:dlq"


def _fake():
    return fakeredis.FakeRedis(decode_responses=True)


def _queue(redis, *, max_len=3, hook=None):
    return RedisStreamTaskQueue(
        redis,
        STREAM,
        group_name="trim-group",
        consumer_name="c1",
        dlq_max_len=max_len,
        on_dead_letter=hook,
    )


def _dead_letter_n(q, n, *, reason="max_deliveries=5"):
    dlq_ids = []
    for i in range(n):
        q.enqueue({"type": "start", "flow_id": "f1", "n": i})
        task = q.read(block_ms=10)
        assert task is not None
        dlq_ids.append(q.dead_letter(task, reason=reason))
    return dlq_ids


class TestTrimObservability:
    def test_trimmed_metric_and_warning(self, caplog):
        redis = _fake()
        q = _queue(redis, max_len=3)
        caplog.set_level(logging.WARNING, logger="plaita.server.task_queue")
        _dead_letter_n(q, 5)
        assert redis.xlen(DLQ) == 3
        assert q.stats()["dlq_trimmed"] == 2
        assert q.stats()["dlq_trim_failures"] == 0
        warnings = [r.message for r in caplog.records if "DLQ 裁剪" in r.message]
        assert warnings, "裁剪未处理证据必须 WARNING（不得静默）"
        assert "max_len=3" in warnings[0]

    def test_xtrim_failure_is_warning_and_counted(self, caplog, monkeypatch):
        redis = _fake()
        q = _queue(redis, max_len=2)
        caplog.set_level(logging.WARNING, logger="plaita.server.task_queue")

        def boom(*args, **kwargs):
            raise RuntimeError("xtrim unavailable")

        monkeypatch.setattr(redis, "xtrim", boom)
        _dead_letter_n(q, 3)
        assert redis.xlen(DLQ) == 3, "XTRIM 失败时 DLQ 无界增长（降级可见）"
        assert q.stats()["dlq_trim_failures"] == 3
        assert q.stats()["dlq_trimmed"] == 0
        warnings = [r.message for r in caplog.records if "xtrim(" in r.message]
        assert warnings, "XTRIM 失败必须 WARNING 级（基线只有 DEBUG）"
        assert any("DLQ 无界增长" in message for message in warnings)

    def test_ack_failure_keeps_durable_dead_letter_observable(self, caplog, monkeypatch):
        redis = _fake()
        q = _queue(redis, max_len=10)
        q.enqueue({"type": "start", "flow_id": "f1"})
        task = q.read(block_ms=10)
        assert task is not None
        caplog.set_level(logging.WARNING, logger="plaita.server.task_queue")

        def boom(_message_id):
            raise RuntimeError("xack unavailable")

        monkeypatch.setattr(q, "ack", boom)
        with pytest.raises(RuntimeError, match="xack unavailable"):
            q.dead_letter(task, reason="max_deliveries=5")
        # 条目已 XADD（durable）：指标/错误日志不得因 ack 失败漏记。
        assert redis.xlen(DLQ) == 1
        assert q.stats()["dead_lettered"] == 1
        assert q.stats()["dlq_ack_failures"] == 1
        assert any("source_ack=failed" in r.message for r in caplog.records)

    def test_alert_carries_dlq_fields(self):
        events = []
        redis = _fake()
        q = _queue(redis, max_len=2, hook=events.append)
        _dead_letter_n(q, 3)
        assert len(events) == 3
        event = events[-1]
        assert event["dlq_id"]
        assert event["dlq_length"] == 2
        assert event["dlq_max_len"] == 2
        assert event["trimmed_count"] == 1
        assert event["schema_version"] == 1

    def test_stats_reports_capacity_and_saturation(self):
        redis = _fake()
        q = _queue(redis, max_len=2)
        _dead_letter_n(q, 2)
        stats = q.stats()
        assert stats["dlq_max_len"] == 2
        assert stats["dlq_saturated"] is True
        assert stats["dlq_length"] == 2
