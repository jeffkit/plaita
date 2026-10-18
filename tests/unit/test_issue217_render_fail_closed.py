"""plaita#217 R4/R5/R6：``url_redaction`` 三条 fail-closed 分支的回归钉。

缺口形态（issue 正文 R4/R5/R6）：三条分支当前实现正确，但把任一条改成「泄漏原文」
或「让异常穿出去」，既有 ``#158/#159/#165`` 测试**仍全绿**——因为那些用例只走
「正常 str 输入 + 抛异常的渲染器」这两条路径，从不覆盖：

* **R4** ``_safe_render`` 的 ``if not isinstance(rendered, str): return UNPRINTABLE_URL``
  —— 渲染器返回 ``None`` / 自定义对象时，``str(rendered)`` 会把对象里的凭据
  重新变回可读文本进日志；
* **R5** ``_safe_render`` 的 ``except UnicodeEncodeError`` —— 孤立代理字符
  （``os.environ`` 的 surrogateescape 口令）必须退化为常量，**不得**抛；抛了就是
  ``#158`` r1 的启动路径崩溃类；
* **R6** ``_mask_url_for_log`` 的 ``if _mask_url is None`` —— 纯 node 进程 /
  factory 未注册时应 fail-closed，直出原文即凭据进日志。

三条都必须**只**对本模块的行为下断言（不经由业务调用点），否则「调用点碰巧先
拒绝」会让负控失真。

负控（未入库，验收时手工执行；原始输出见 ``/tmp/w42-out/neg-R*.log``）：

* R4：``return UNPRINTABLE_URL`` → ``return str(rendered)`` ⇒
  ``TestIssue217R4NonStrRenderResult`` 两条红；
* R5：``except UnicodeEncodeError:`` → ``except TypeError:`` ⇒
  ``TestIssue217R5SurrogateFailsClosed`` 两条红（异常穿出）；
* R6：未注册分支 ``return UNPRINTABLE_URL`` → ``return str(url)`` ⇒
  ``TestIssue217R6UnregisteredMasker`` 两条红。

每个类都配**正控**（正常 str 结果、已注册掩码器），防止「恒返回常量」这种坏实现
让本文件变绿。
"""
from __future__ import annotations

import unittest
from unittest import mock

# 生产路径：factory 模块体把 ``_mask_url`` 注册进 url_redaction。
# 不 import 则 ``_mask_url`` 为 None，正控会走 fail-closed 分支。
import plaita.server.factory  # noqa: F401
from plaita.core import url_redaction
from plaita.core.url_redaction import (
    UNPRINTABLE_URL,
    _mask_url_for_log,
    _mask_webhook_url,
    _safe_render,
)

REDIS_URL = "redis://user:s3cret-pw@127.0.0.1:6379/0"
PASSWORD = "s3cret-pw"
SLACK_URL = "https://hooks.slack.com/services/T000/B000/Zq4Vm8Lp"


class _RenderCarriesSecret:
    """渲染器返回非 ``str``：只有 ``str()`` 才会吐出凭据原文（R4 的泄露形态）。"""

    def __str__(self) -> str:  # pragma: no cover — 负控下才会被调用
        return REDIS_URL

    def __repr__(self) -> str:  # pragma: no cover
        return "<_RenderCarriesSecret>"


class TestIssue217R4NonStrRenderResult(unittest.TestCase):
    """R4：渲染器返回非 ``str`` ⇒ 安全常量，绝不 ``str()`` 回原文。"""

    def test_non_str_render_result_degrades_to_safe_constant(self) -> None:
        rendered = _safe_render(REDIS_URL, lambda _url: _RenderCarriesSecret())
        self.assertEqual(rendered, UNPRINTABLE_URL)

    def test_non_str_render_result_never_carries_the_password(self) -> None:
        rendered = _safe_render(REDIS_URL, lambda _url: _RenderCarriesSecret())
        self.assertNotIn(PASSWORD, rendered)
        self.assertNotIn(REDIS_URL, rendered)

    def test_none_render_result_degrades_to_safe_constant(self) -> None:
        """``None`` 是最常见的非 str 返回（渲染器忘 return / 早退分支）。"""
        rendered = _safe_render(REDIS_URL, lambda _url: None)
        self.assertEqual(rendered, UNPRINTABLE_URL)

    def test_positive_control_str_result_passes_through(self) -> None:
        """正控：正常 str 结果原样返回（不是「恒返回常量」）。"""
        rendered = _safe_render(SLACK_URL, _mask_webhook_url)
        self.assertEqual(rendered, "https://hooks.slack.com/***/***/***/***")
        self.assertNotIn("Zq4Vm8Lp", rendered)


class TestIssue217R5SurrogateFailsClosed(unittest.TestCase):
    """R5：渲染结果不可 utf-8 编码（孤立代理字符）⇒ 安全常量且**不抛**。"""

    def test_surrogate_render_result_degrades_without_raising(self) -> None:
        raw = "\ud800"  # 孤立代理字符：POSIX surrogateescape 口令的形态
        try:
            rendered = _safe_render("redis://user:" + raw + "@127.0.0.1:6379/0", lambda url: url)
        except UnicodeEncodeError as exc:
            raise AssertionError(
                "_safe_render 让异常穿出去了（调用方在日志/停机/注册路径上不能抛）: "
                + type(exc).__name__
            ) from None
        self.assertEqual(rendered, UNPRINTABLE_URL)

    def test_surrogate_in_webhook_path_degrades_without_raising(self) -> None:
        """同一 fail-closed 对 webhook 渲染器同样成立（结果由渲染器产出，非输入）。"""
        raw = "https://hooks.slack.com/services/T000/\ud800/B000"

        def renderer(_url: object) -> str:
            return raw

        try:
            rendered = _safe_render("ignored", renderer)
        except UnicodeEncodeError as exc:
            raise AssertionError(
                "_safe_render 让异常穿出去了（调用方在日志路径上不能抛）: "
                + type(exc).__name__
            ) from None
        self.assertEqual(rendered, UNPRINTABLE_URL)

    def test_positive_control_encodable_result_passes_through(self) -> None:
        rendered = _safe_render(SLACK_URL, _mask_webhook_url)
        self.assertEqual(rendered, "https://hooks.slack.com/***/***/***/***")


class TestIssue217R6UnregisteredMasker(unittest.TestCase):
    """R6：``_mask_url`` 未注册（纯 node 进程 / factory 未 import）⇒ fail-closed。"""

    def test_unregistered_masker_degrades_to_safe_constant(self) -> None:
        with mock.patch.object(url_redaction, "_mask_url", None):
            rendered = _mask_url_for_log(REDIS_URL)
        self.assertEqual(rendered, UNPRINTABLE_URL)

    def test_unregistered_masker_never_returns_the_raw_url(self) -> None:
        with mock.patch.object(url_redaction, "_mask_url", None):
            rendered = _mask_url_for_log(REDIS_URL)
        self.assertNotIn(PASSWORD, rendered)
        self.assertNotEqual(rendered, REDIS_URL)

    def test_positive_control_registered_masker_still_renders(self) -> None:
        """正控：注册了掩码器时正常渲染（未注册分支不是唯一路径）。"""
        with mock.patch.object(url_redaction, "_mask_url", _mask_webhook_url):
            rendered = _mask_url_for_log(SLACK_URL)
        self.assertEqual(rendered, "https://hooks.slack.com/***/***/***/***")
        self.assertNotIn("Zq4Vm8Lp", rendered)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
