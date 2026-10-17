"""plaita#165 机制守卫：入口上限触发必须**响亮**（WARNING + 计数）。

「政策与机制分离」的机制半边：上限值本身是可回滚的政策（独立 ``[policy]`` 提交），
但**触发面**是机制——任何一次「因超限而 fail-closed」都必须留下可观测证据：

* ``logger.warning``（WARNING 级，含 ``length`` / ``limit`` / 触发点；**不回显输入**）；
* 模块级 ``_cap_trip_count`` 单调 +1。

本文件用**相对** ``_MAX_CLEAN_INPUT`` 的长度断言（``threshold + 1`` / ``threshold``），
所以政策把 2048 调宽到 64 KiB 时这些守卫**语义不变**、无需改判据；数值本身由
``test_issue165_policy_window.py``（``[policy]`` 提交）单独钉住。
"""
from __future__ import annotations

import unittest
from unittest import mock

import plaita.server.factory  # noqa: F401 — 生产路径：注册 _mask_url
from plaita.core import url_redaction as url_redaction_module
from plaita.core.url_redaction import (
    UNPRINTABLE_URL,
    _cap_trip_count,
    _mask_webhook_url_for_log,
    _scrub_url_text,
)

LOGGER = "plaita.core.url_redaction"


def _webhook_url(length: int) -> str:
    """构造恰好 ``length`` 字符、凭据在 path 里的**正常** webhook URL。"""
    prefix = "https://hooks.slack.com/services/"
    assert length >= len(prefix) + 1
    return prefix + "K" * (length - len(prefix))


class TestIssue165CapTripSignal(unittest.TestCase):
    """机制：超限 ⇒ WARNING + 计数；未超限 ⇒ 计数不变。"""

    def setUp(self) -> None:
        self._saved_count = url_redaction_module._cap_trip_count
        url_redaction_module._cap_trip_count = 0

    def tearDown(self) -> None:
        url_redaction_module._cap_trip_count = self._saved_count

    def test_over_cap_warns_and_counts(self) -> None:
        """阈值 +1 字符：``_safe_render`` 入口必须 WARNING 且计数 +1。"""
        url = _webhook_url(url_redaction_module._MAX_CLEAN_INPUT + 1)
        with self.assertLogs(LOGGER, level="WARNING") as captured:
            result = _mask_webhook_url_for_log(url)

        self.assertEqual(result, UNPRINTABLE_URL)
        self.assertEqual(url_redaction_module._cap_trip_count, 1)
        self.assertEqual(len(captured.output), 1, captured.output)
        message = captured.output[0]
        self.assertIn(f"length={len(url)}", message)
        self.assertIn(f"limit={url_redaction_module._MAX_CLEAN_INPUT}", message)
        self.assertIn("safe_render", message)
        # 只记长度/阈值：输入里的凭据本体绝不进日志。
        self.assertNotIn("KKKKKKKKKKKKKKKK", message)

    def test_at_cap_does_not_count(self) -> None:
        """恰好等于阈值：正常掩码，计数不变，无 WARNING。"""
        url = _webhook_url(url_redaction_module._MAX_CLEAN_INPUT)
        with self.assertNoLogs(LOGGER, level="WARNING"):
            result = _mask_webhook_url_for_log(url)

        self.assertEqual(result, "https://hooks.slack.com/***/***")
        self.assertEqual(url_redaction_module._cap_trip_count, 0)

    def test_under_cap_does_not_count(self) -> None:
        """普通短 URL：计数不变（负控——守卫不许空转计数）。"""
        with self.assertNoLogs(LOGGER, level="WARNING"):
            result = _mask_webhook_url_for_log(
                "https://hooks.slack.com/services/T000/B000/XyZ"
            )

        self.assertEqual(result, "https://hooks.slack.com/***/***/***/***")
        self.assertEqual(url_redaction_module._cap_trip_count, 0)

    def test_absolute_url_ref_trip_is_loud(self) -> None:
        """``_scrub_url_text`` 里的绝对 URL 超限：也走同一套 WARNING + 计数。"""
        url = _webhook_url(url_redaction_module._MAX_CLEAN_INPUT + 1)
        # 把**累计**预算放大，隔离出「单条绝对 URL 超限」这一条路径
        # （否则 threshold+1 的 token 会先撞上 64 KiB 的累计预算）。
        with mock.patch.object(url_redaction_module, "_MAX_SCRUB_URL_CHARS", 1 << 20):
            with self.assertLogs(LOGGER, level="WARNING") as captured:
                result = _scrub_url_text(f"boom {url} end")

        self.assertEqual(result, f"boom {UNPRINTABLE_URL} end")
        self.assertEqual(url_redaction_module._cap_trip_count, 1)
        self.assertIn("absolute_url_ref", captured.output[0])

    def test_relative_url_ref_trip_is_loud(self) -> None:
        """scheme-relative 引用超限：同样 WARNING + 计数。"""
        ref = "/" + "K" * (url_redaction_module._MAX_CLEAN_INPUT + 1)
        with self.assertLogs(LOGGER, level="WARNING") as captured:
            result = _scrub_url_text(f"url: {ref}")

        self.assertEqual(result, f"url: {UNPRINTABLE_URL}")
        self.assertEqual(url_redaction_module._cap_trip_count, 1)
        self.assertIn("relative_url_ref", captured.output[0])

    def test_cumulative_scrub_budget_trip_is_loud(self) -> None:
        """累计 URL 预算用尽：无 token 超单条上限，也必须 WARNING + 计数。"""
        with mock.patch.object(url_redaction_module, "_MAX_SCRUB_URL_CHARS", 16):
            with self.assertLogs(LOGGER, level="WARNING") as captured:
                result = _scrub_url_text("https://h/a https://h/b")

        self.assertEqual(result, f"https://h/*** {UNPRINTABLE_URL}")
        self.assertEqual(url_redaction_module._cap_trip_count, 1)
        self.assertIn("scrub_budget", captured.output[0])

    def test_counter_is_monotonic_across_trips(self) -> None:
        """连续触发只增不减（进程级计数，不是「本次调用」的局部量）。"""
        over = _webhook_url(url_redaction_module._MAX_CLEAN_INPUT + 1)
        for expected in (1, 2, 3):
            with self.assertLogs(LOGGER, level="WARNING"):
                self.assertEqual(_mask_webhook_url_for_log(over), UNPRINTABLE_URL)
            self.assertEqual(url_redaction_module._cap_trip_count, expected)

    def test_count_is_exported_for_observability(self) -> None:
        """计数是模块级名字（监控/测试可直接读），不是函数内局部量。"""
        self.assertIsInstance(_cap_trip_count, int)
        self.assertIs(_cap_trip_count, url_redaction_module._cap_trip_count)

    def test_bytes_over_cap_trips_too(self) -> None:
        """``bytes`` 按字节数计量，同样触发（与 ``str`` 同一判据）。"""
        payload = b"https://h/" + b"K" * url_redaction_module._MAX_CLEAN_INPUT
        with self.assertLogs(LOGGER, level="WARNING") as captured:
            result = _mask_webhook_url_for_log(payload)

        self.assertEqual(result, UNPRINTABLE_URL)
        self.assertEqual(url_redaction_module._cap_trip_count, 1)
        self.assertIn(f"length={len(payload)}", captured.output[0])


if __name__ == "__main__":
    unittest.main()
