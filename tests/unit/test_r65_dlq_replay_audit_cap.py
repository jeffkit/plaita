"""第六十五轮（duo-a-ledger 第九十六轮 b/P2）：replay 审计流有界化。

## 缺陷

``plaita:dlq:replay:audit`` 此前只 XADD、无 MAXLEN/XTRIM/TTL——审计流随平台
管理员操作无限增长（每次回灌/具名拒绝/force 都 +1 条，永不收缩）。

## 修复契约（本文件钉住）

1. 每次审计写入后 XTRIM 精确裁剪（``approximate=False``），XLEN 恒 ≤ 上限；
   上限 env 可调（``PLAITA_REPLAY_AUDIT_MAX_LEN``，默认
   ``DEFAULT_REPLAY_AUDIT_MAX_LEN``）；
2. 口径取舍：**持久但有界**（有损 FIFO）——键**无 TTL**：整键过期会一次性
   抹掉全部审计历史，比 FIFO 裁剪更糟；长期归档请外送（ops-runbook 注记）；
3. 降级可观测（AGENTS.md）：裁剪丢证据 → WARNING（限频，稳态触顶逐条告警
   是噪音）；XTRIM 失败 → 审计行照写 + WARNING（无界增长可见）；
4. 非法/≤0 的 env 不回退到无界——WARNING + 默认上限（审计流必须有界）。

## 负控（还原修复后必须精确红）

- 去掉写入后的 XTRIM：``test_audit_stream_capped_at_env_limit`` 看到
  XLEN=5 > 3；
- 去掉 env 解析（恒默认）：``test_audit_cap_env_overrides_default`` 红；
- XTRIM 异常改回吞掉：``test_audit_trim_failure_is_observable`` 红；
- 裁剪 WARNING 去掉限频/去掉：``test_trim_warning_is_rate_limited`` 红。
"""
from __future__ import annotations

import logging

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.dlq_replay import (
    DEFAULT_REPLAY_AUDIT_MAX_LEN,
    REPLAY_AUDIT_STREAM,
    replay_audit_max_len,
    replay_dead_letter,
)
import plaita.server.dlq_replay as dlq_replay_module

DLQ = "plaita:flow:queue:v2:dlq"
TARGET = "plaita:flow:queue:v2"


def _fake():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture(autouse=True)
def _reset_trim_warning_rate_limit():
    """裁剪 WARNING 限频是模块级状态——逐测试归零，互不泄漏。"""
    dlq_replay_module._last_audit_trim_warning_at = 0.0
    yield
    dlq_replay_module._last_audit_trim_warning_at = 0.0


def _seed_dead_letter(redis, *, execution_id="exec-1") -> str:
    import json

    from plaita.runtime_config import envelope_for_stream

    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": TARGET,
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": 1700000000.0,
        "schema_version": 1,
        # r58：源/目标 family 一致的正控。
        "config_family": envelope_for_stream(TARGET, role="console")["config_family"],
        "payload": {
            "type": "start",
            "flow_id": "f1",
            "version": "1",
            "tenant_id": "acme",
            "execution_id": execution_id,
        },
    }
    return redis.xadd(DLQ, {"payload": json.dumps(envelope)})


def _force_replay(redis, dlq_id):
    return replay_dead_letter(
        redis,
        dlq_key=DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
        force=True,
    )


def _audit_rows(redis):
    return [fields for _, fields in redis.xrange(REPLAY_AUDIT_STREAM)]


class TestAuditStreamBounded:
    def test_audit_stream_capped_at_env_limit(self, monkeypatch):
        """超限后 XLEN 恒 ≤ 上限，且留下的审计内容仍可读（最新 3 条）。"""
        redis = _fake()
        monkeypatch.setenv("PLAITA_REPLAY_AUDIT_MAX_LEN", "3")
        dlq_id = _seed_dead_letter(redis)

        target_ids = []
        for _ in range(5):
            result = _force_replay(redis, dlq_id)
            assert result.replayed and result.reason == "ok"
            target_ids.append(result.target_message_id)

        assert redis.xlen(REPLAY_AUDIT_STREAM) == 3

        rows = _audit_rows(redis)
        assert len(rows) == 3
        # 内容仍可读：action/result/dlq_id 齐全，且留下的是最新 3 条审计
        assert all(row["action"] == "dlq.replay" for row in rows)
        assert all(row["result"] == "ok" for row in rows)
        assert all(row["dlq_id"] == dlq_id for row in rows)
        assert [row["target_message_id"] for row in rows] == target_ids[-3:]

    def test_audit_cap_default_without_env(self, monkeypatch):
        monkeypatch.delenv("PLAITA_REPLAY_AUDIT_MAX_LEN", raising=False)
        assert replay_audit_max_len() == DEFAULT_REPLAY_AUDIT_MAX_LEN

    def test_audit_cap_env_overrides_default(self, monkeypatch):
        monkeypatch.setenv("PLAITA_REPLAY_AUDIT_MAX_LEN", "7")
        assert replay_audit_max_len() == 7

    def test_audit_cap_invalid_env_falls_back_with_warning(self, monkeypatch, caplog):
        """非法/≤0 的 env 不回退到无界：WARNING + 默认上限。"""
        for raw in ("abc", "-5", "0"):
            monkeypatch.setenv("PLAITA_REPLAY_AUDIT_MAX_LEN", raw)
            with caplog.at_level(logging.WARNING, logger="plaita.server.dlq_replay"):
                assert replay_audit_max_len() == DEFAULT_REPLAY_AUDIT_MAX_LEN
        warnings = [r for r in caplog.records if "PLAITA_REPLAY_AUDIT_MAX_LEN" in r.message]
        assert len(warnings) == 3, "每个非法值都必须留 WARNING 痕迹"


class TestAuditTrimObservability:
    def test_audit_trim_failure_is_observable(self, monkeypatch, caplog):
        """XTRIM 失败：审计行照写（写入与裁剪解耦），WARNING 留痕。"""
        redis = _fake()
        monkeypatch.delenv("PLAITA_REPLAY_AUDIT_MAX_LEN", raising=False)
        dlq_id = _seed_dead_letter(redis)

        def boom(*args, **kwargs):
            raise RuntimeError("xtrim unavailable")

        monkeypatch.setattr(redis, "xtrim", boom)
        with caplog.at_level(logging.WARNING, logger="plaita.server.dlq_replay"):
            result = _force_replay(redis, dlq_id)

        assert result.replayed
        assert redis.xlen(REPLAY_AUDIT_STREAM) == 1, "裁剪失败不得吞掉审计行"
        warnings = [r.message for r in caplog.records if "xtrim(" in r.message]
        assert warnings, "XTRIM 失败必须 WARNING（无界增长可见）"
        assert any("无界增长" in message for message in warnings)

    def test_trim_warning_is_rate_limited(self, monkeypatch, caplog):
        """稳态触顶后每次写入都裁 1 条——WARNING 限频，不逐条刷屏。"""
        redis = _fake()
        monkeypatch.setenv("PLAITA_REPLAY_AUDIT_MAX_LEN", "3")
        dlq_id = _seed_dead_letter(redis)

        with caplog.at_level(logging.WARNING, logger="plaita.server.dlq_replay"):
            for _ in range(5):
                assert _force_replay(redis, dlq_id).replayed

        assert redis.xlen(REPLAY_AUDIT_STREAM) == 3
        trim_warnings = [r for r in caplog.records if "审计流裁剪" in r.message]
        assert trim_warnings, "裁剪丢审计证据必须 WARNING（不得静默）"
        assert len(trim_warnings) == 1, "限频窗口内多次裁剪只告警一次"
        assert "max_len=3" in trim_warnings[0].message
