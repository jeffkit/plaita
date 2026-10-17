"""#98 回归：节点插件发现失败必须可观测、可重试、可归因。

旧实现两处缺陷：
- ``discover()`` 在扫描**之前**置 ``_discovered = True``：``entry_points()``
  抛错后本进程永久不再重试（第二次 ``discover()`` 静默返回）；
- 每个 ``ep.load()`` 的异常被 ``except Exception:`` 丢弃（无异常对象、无
  traceback、无承载属性），解析期只报 ``unRecognized node type``，把
  「环境缺依赖」误导向「节点名写错」。

本文件是 issue 验收条件里点名的那份单测。
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

import plaita.node as nodemod


def _failing_entry_point(name: str = "delay", exc: BaseException | None = None):
    ep = MagicMock()
    ep.name = name
    ep.value = f"some.plugin:{name}"
    ep.load.side_effect = exc or ModuleNotFoundError("No module named 'kafka'")
    return ep


class TestDiscoveryFailureIsRetryable:
    def test_entry_points_failure_leaves_registry_retryable(self):
        reg = nodemod.NodeRegistry()
        with patch.object(nodemod, "entry_points", side_effect=RuntimeError("boom")) as mocked:
            with pytest.raises(RuntimeError, match="boom"):
                reg.discover()
            assert reg._discovered is False, (
                "扫描失败后 _discovered 仍为 True —— 本进程永久放弃插件发现"
            )

            # 依赖补好后再次 discover() 必须真的重新扫描（而不是静默返回）
            with pytest.raises(RuntimeError, match="boom"):
                reg.discover()
            assert mocked.call_count == 2, "第二次 discover() 没有重试 entry_points 扫描"

    def test_successful_scan_stays_idempotent(self):
        reg = nodemod.NodeRegistry()
        ep = MagicMock()
        ep.name = "ok_plugin"
        ep.load.return_value = type(
            "OkNode", (), {"node_type": "ok_plugin_type"}
        )
        with patch.object(nodemod, "entry_points", return_value=[ep]) as mocked:
            reg.discover()
            assert reg._discovered is True
            reg.discover()
            assert mocked.call_count == 1, "成功发现后不应重复扫描"
            assert ep.load.call_count == 1


class TestLoadFailureIsObservable:
    def test_load_error_recorded_and_logged_with_traceback(self, caplog):
        reg = nodemod.NodeRegistry()
        ep = _failing_entry_point("delay")

        with patch.object(nodemod, "entry_points", return_value=[ep]):
            with caplog.at_level(logging.WARNING, logger="plaita.node"):
                reg.discover()  # 单个插件加载失败不应中断整个扫描

        assert hasattr(reg, "_load_errors"), "加载失败没有承载在任何可查询属性上"
        recorded = reg._load_errors["delay"]
        assert isinstance(recorded, ModuleNotFoundError)
        assert "kafka" in str(recorded)

        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert warnings, "插件加载失败至少应有一条 warning 日志"
        assert "kafka" in caplog.text, "日志丢了异常文本（只剩插件名）"
        # 2026-10-17 裁决（收口批）：本处**不再要求 `exc_info`**，且反过来禁止它。
        # 理由：`exc_info=True` 会让 Formatter **重新渲染 traceback**，其末行就是
        # `str(exc)` ⇒ 绕过 `_scrub_exception_text` 的清洗（plaita#151 的 C 类判据）。
        # 插件在 import 期就可能建立连接，异常文本里可能带 DSN/URL ⇒ **安全边界优先**；
        # 代价是丢掉 traceback 结构，但**异常类型与已清洗的文本仍然保留**（下方断言）。
        assert "ModuleNotFoundError" in caplog.text, "日志丢了异常类型（traceback 可弃，类型不可弃）"
        assert not any(r.exc_info for r in warnings), (
            "不得使用 exc_info：Formatter 会重渲染 traceback 从而绕过清洗（#151）"
        )

    def test_healthy_plugins_still_load_when_a_sibling_fails(self):
        reg = nodemod.NodeRegistry()
        healthy = MagicMock()
        healthy.name = "fine"
        healthy.load.return_value = type("FineNode", (), {"node_type": "fine_type"})
        broken = _failing_entry_point("delay")

        with patch.object(nodemod, "entry_points", return_value=[healthy, broken]):
            reg.discover()

        assert "fine_type" in reg
        assert "delay" in reg._load_errors


class TestParseNodePointsAtTheLoadFailure:
    def test_hint_names_the_plugin_load_failure(self):
        reg = nodemod.NodeRegistry()
        ep = _failing_entry_point("delay")

        with patch.object(nodemod, "entry_points", return_value=[ep]):
            reg.discover()

        with pytest.raises(RuntimeError) as excinfo:
            reg.parse_node({"type": "delay"})

        message = str(excinfo.value)
        assert "plugin" in message.lower(), (
            f"报错文案仍把失败插件当成拼写错误: {message}"
        )
        assert "kafka" in message, f"报错文案没有带上真实失败原因: {message}"

    def test_hint_for_unknown_type_is_unchanged(self):
        reg = nodemod.NodeRegistry()
        with pytest.raises(RuntimeError, match="unRecognized node type: nope"):
            reg.parse_node({"type": "nope"})


class TestCopyCarriesDiscoveryState:
    def test_copied_registry_can_still_parse_missing_types(self):
        reg = nodemod.NodeRegistry()
        ep = _failing_entry_point("delay")
        with patch.object(nodemod, "entry_points", return_value=[ep]):
            reg.discover()

        copied = reg.copy()
        with pytest.raises(RuntimeError, match="plugin"):
            copied.parse_node({"type": "delay"})
