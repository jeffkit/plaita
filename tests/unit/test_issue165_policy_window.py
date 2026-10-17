"""plaita#165 ``[policy]``：入口上限从 2048 提到 **64 KiB**（宽到正常流量绝不触及）。

政策与机制分离：

* 机制（``test_issue165_cap_trip_signal.py``）= 成本有界 + 触发响亮（WARNING + 计数），
  与门槛数值无关；
* 政策（本文件）= 门槛值本身。**正常但很长的 URL** 不再被静默整段掩掉，只有真正
  超宽（> 64 KiB）的输入才 fail-closed。

本文件同时钉住**负控**：把门槛临时改回 2048，同一条 3000 字符 URL 必须又被整段
掩掉——证明「修后能正常掩码」确实由政策值决定，而不是掩码器行为碰巧改变。
"""
from __future__ import annotations

import pathlib
import unittest
from unittest import mock

import plaita.server.factory  # noqa: F401 — 生产路径：注册 _mask_url
from plaita.core import url_redaction as url_redaction_module
from plaita.core.url_redaction import (
    UNPRINTABLE_URL,
    _mask_url_for_log,
    _mask_webhook_url_for_log,
    _scrub_url_text,
)

LOGGER = "plaita.core.url_redaction"
ROOT = pathlib.Path(__file__).resolve().parents[2]


def _webhook_url(length: int) -> str:
    prefix = "https://hooks.slack.com/services/"
    assert length >= len(prefix) + 1
    return prefix + "K" * (length - len(prefix))


def _redis_url(length: int) -> str:
    prefix = "redis://user:"
    suffix = "@host:6379/0"
    assert length >= len(prefix) + len(suffix) + 1
    return prefix + "K" * (length - len(prefix) - len(suffix)) + suffix


class TestIssue165PolicyWindow(unittest.TestCase):
    """政策：64 KiB 宽窗；正常长 URL 正常掩码，超宽才 fail-closed。"""

    def setUp(self) -> None:
        self._saved_count = url_redaction_module._cap_trip_count
        url_redaction_module._cap_trip_count = 0

    def tearDown(self) -> None:
        url_redaction_module._cap_trip_count = self._saved_count

    def test_threshold_is_the_wide_policy_value(self) -> None:
        """门槛 = ``64 * 1024``（64 KiB）；不是 2048，也不是「无上限」。"""
        self.assertEqual(url_redaction_module._MAX_CLEAN_INPUT, 64 * 1024)
        self.assertEqual(url_redaction_module._MAX_CLEAN_INPUT, 65536)

    def test_threshold_is_documented_at_the_constant(self) -> None:
        """源码级防漂移：常量旁边必须写清「政策」与「64 KiB」依据。"""
        source = (ROOT / "plaita" / "core" / "url_redaction.py").read_text(
            encoding="utf-8"
        )
        self.assertEqual(source.count("_MAX_CLEAN_INPUT = 64 * 1024"), 1)
        self.assertIn("政策，不是机制", source)
        self.assertIn("64 KiB", source)

    def test_3000_char_webhook_url_masks_normally(self) -> None:
        """红证的正向：长度 3000 的正常 webhook URL 保留安全前缀。"""
        url = _webhook_url(3000)
        with self.assertNoLogs(LOGGER, level="WARNING"):
            result = _mask_webhook_url_for_log(url)

        self.assertEqual(result, "https://hooks.slack.com/***/***")
        self.assertEqual(url_redaction_module._cap_trip_count, 0)

    def test_3000_char_redis_url_masks_normally(self) -> None:
        """同样 3000 字符的 Redis URL：userinfo 口令被掩，端点保留。"""
        url = _redis_url(3000)
        with self.assertNoLogs(LOGGER, level="WARNING"):
            result = _mask_url_for_log(url)

        self.assertEqual(result, "redis://user:***@host:6379/0")
        self.assertEqual(url_redaction_module._cap_trip_count, 0)

    def test_3000_char_url_inside_exception_text_masks_normally(self) -> None:
        """``_scrub_url_text``（异常文本构造面）里的同一条 URL 也正常掩码。"""
        url = _webhook_url(3000)
        with self.assertNoLogs(LOGGER, level="WARNING"):
            result = _scrub_url_text(f"boom {url} end")

        self.assertEqual(result, "boom https://hooks.slack.com/***/*** end")
        self.assertEqual(url_redaction_module._cap_trip_count, 0)

    def test_wide_window_still_masks_several_real_sizes(self) -> None:
        """宽窗覆盖 3 KB / 8 KB / 16 KB 三档正常长 URL，全部正常掩码。"""
        for length in (3000, 8000, 16000):
            with self.subTest(length=length):
                url = _webhook_url(length)
                with self.assertNoLogs(LOGGER, level="WARNING"):
                    result = _mask_webhook_url_for_log(url)
                self.assertEqual(
                    result, "https://hooks.slack.com/***/***", length
                )
                self.assertEqual(url_redaction_module._cap_trip_count, 0)

    def test_exactly_at_wide_threshold_still_masks(self) -> None:
        """边界：恰好 64 KiB 仍正常掩码（``<=`` 判据）；只超 1 字符才 fail-closed。"""
        url = _webhook_url(url_redaction_module._MAX_CLEAN_INPUT)
        with self.assertNoLogs(LOGGER, level="WARNING"):
            result = _mask_webhook_url_for_log(url)
        self.assertEqual(result, "https://hooks.slack.com/***/***")
        self.assertEqual(url_redaction_module._cap_trip_count, 0)

        over = _webhook_url(url_redaction_module._MAX_CLEAN_INPUT + 1)
        with self.assertLogs(LOGGER, level="WARNING") as captured:
            self.assertEqual(_mask_webhook_url_for_log(over), UNPRINTABLE_URL)
        self.assertEqual(url_redaction_module._cap_trip_count, 1)
        self.assertIn(f"limit={url_redaction_module._MAX_CLEAN_INPUT}", captured.output[0])

    def test_negative_control_reverting_to_2048_caps_the_same_url(self) -> None:
        """负控：把门槛临时改回 2048 ⇒ 同一条 3000 字符 URL 又整段掩掉。"""
        url = _webhook_url(3000)
        with mock.patch.object(url_redaction_module, "_MAX_CLEAN_INPUT", 2048):
            with self.assertLogs(LOGGER, level="WARNING") as captured:
                result = _mask_webhook_url_for_log(url)

        self.assertEqual(result, UNPRINTABLE_URL)
        self.assertEqual(url_redaction_module._cap_trip_count, 1)
        self.assertIn("length=3000", captured.output[0])
        self.assertIn("limit=2048", captured.output[0])


if __name__ == "__main__":
    unittest.main()
