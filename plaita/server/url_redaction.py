"""plaita#159 —— webhook URL 的**凭据写在 path/query 里**，日志只允许出现端点。

``#88`` 的 ``factory._mask_url`` 抹的是 **userinfo 口令**，``path`` / ``query``
**原样保留**（那是它的诊断价值所在，也是它的契约）。但四家主流 webhook 的凭据
恰恰不在 userinfo，而在 path / query：

===========================  ==================================================
Slack                        ``https://hooks.slack.com/services/T…/B…/X…``
飞书                          ``https://open.feishu.cn/open-apis/bot/v2/hook/…``
钉钉                          ``https://oapi.dingtalk.com/robot/send?access_token=…``
企业微信                      ``https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=…``
===========================  ==================================================

即：**谁能读日志，谁就能往该 webhook 投任意内容**（Slack 的
``/services/…`` 三个 segment 合起来就是凭据）。``#158`` 的守卫只认
``redis_url`` 这个名字，对 ``self.url`` / ``self._alert_webhook`` 视而不见——
``#159`` 因此在 ``#158`` 的雷达下隐身。

本模块只做**渲染**，不参与传输：``WebhookAlerter.url`` 与
``RedisFlowWorker._alert_webhook`` 仍持有原始串，``POST`` 路径一字未动。

设计（与 ``#88`` 同源的纪律：**cannot-leak by construction**，安全性不依赖输入
是否合法）：

* ``_mask_webhook_url`` **total**——任意输入（``bytes`` / 非 UTF-8 / 孤立代理
  字符 / 畸形 URL / ``None`` / 对象）都不抛；
* 渲染结果只在**端点**（``scheme://host[:port]``）上保留诊断价值，path 的
  **每一个** segment 与**整条** query / fragment 一律 ``***``；
* 不能安全渲染时退化为常量 ``UNPRINTABLE_URL``，**绝不**退化回原始串；
* ``_mask_url_for_log`` / ``_mask_webhook_url_for_log`` 再包一层 fail-closed：
  渲染器**任何**异常都退化为 ``UNPRINTABLE_URL``——调用方在日志/停机/注册路径上，
  掩码不许把它们炸掉（``#158`` r1 定验的 FAIL (a)）。
"""
from __future__ import annotations

import logging
from typing import Callable, Optional
from urllib.parse import urlsplit

#: ``#88`` 的掩码器——Redis URL（userinfo 口令）的唯一渲染路径。
#: **按名字 import**：``#88`` 若改名/删除，这里在 import 期即炸（fail-loud），
#: 而不是悄悄退化成一个不再掩码的版本。
from plaita.server.factory import _mask_url

logger = logging.getLogger("plaita.server.url_redaction")

#: 掩码失败时的**安全常量**——绝不退回原始 URL。
UNPRINTABLE_URL = "<unprintable-url>"

#: 被抹掉的 path segment / query / fragment 占位。
PLACEHOLDER = "***"

#: 只承认「端点型」scheme：其余 scheme（``javascript:`` / ``unix:`` / 拿凭据当
#: scheme 的畸形串）无从判断哪一段是端点、哪一段是密钥 ⇒ 整体退化常量。
_SAFE_SCHEMES = frozenset({"http", "https"})


def _coerce_url_text(value: object) -> Optional[str]:
    """把输入收敛成 ``str``；不确定的对象返回 ``None``（⇒ 安全常量）。

    ``bytes`` 按 ``factory`` 的同一约定用 ``utf-8/replace`` 解码——**不**用
    ``str(b"...")``：后者会得到 ``b'https://hooks…'`` 这种带转义的 repr，
    等于把 path 里的密钥换个写法留在日志里。
    """
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001 — 解码兜底，绝不上抛
            # 刻意**不**回显异常文本：它带着原始字节/口令片段，而这里正是掩码器的
            # 输出面（`test_no_silent_swallow` 要求 except 要么 raise、要么记日志）。
            logger.debug("URL 字节解码失败，按不可渲染处理")
            return None
    return None


def _render_path(path: str) -> str:
    """path 的**每一个非空 segment** 换成占位，保留 ``/`` 结构。

    ``/services/T00/B00/XyZ`` → ``/***/***/***``。保留段数与斜杠，运维仍能看出
    「这是哪家的哪种端点形状」，但没有任何一段原文活下来。
    """
    return "/".join(PLACEHOLDER if segment else segment for segment in path.split("/"))


def _mask_webhook_url(url: object) -> str:
    """把 webhook URL 渲染成 ``scheme://host[:port]`` + 形状化 path/query。

    >>> _mask_webhook_url("https://hooks.slack.com/services/T00/B00/XyZ")
    'https://hooks.slack.com/***/***/***'
    >>> _mask_webhook_url("https://oapi.dingtalk.com/robot/send?access_token=sec")
    'https://oapi.dingtalk.com/***/***?***'

    **total**：任何输入都不抛。判据是「有没有 authority」——没有 ``//host`` 的串
    （``javascript:…``、裸 token、``hooks.slack.com/x``）以及非 http(s) scheme
    一律退化为 :data:`UNPRINTABLE_URL`，因为它们无法区分端点与密钥。
    userinfo（``https://user:pw@host/…``）一并丢弃：那是 ``#88`` 的领域，本渲染器
    不做「保留 userinfo」的承诺。
    """
    text = _coerce_url_text(url)
    if text is None:
        return UNPRINTABLE_URL
    if text == "":
        # 空串没有任何凭据，也不是 URL——原样返回（``""`` 不会泄露任何东西）。
        return ""
    try:
        parts = urlsplit(text)
    except Exception:  # noqa: BLE001 — urlsplit 对畸形串会抛（如 Invalid IPv6 URL）
        # 同上：不回显异常文本（可能带原文片段），只记「退化」这一事实。
        logger.debug("URL 无法解析，退化为安全常量")
        return UNPRINTABLE_URL

    if parts.scheme not in _SAFE_SCHEMES or not parts.netloc:
        return UNPRINTABLE_URL

    # netloc 含 userinfo（``user:pw@host``）时只留 host[:port]：口令不是本渲染器的
    # 输出面。IPv6 的方括号与端口原样保留（不做 .port 解析——它会抛）。
    host = parts.netloc.rpartition("@")[2]
    if not host:
        return UNPRINTABLE_URL

    rendered = f"{parts.scheme}://{host}{_render_path(parts.path)}"
    if parts.query:
        rendered += f"?{PLACEHOLDER}"
    if parts.fragment:
        rendered += f"#{PLACEHOLDER}"

    try:
        rendered.encode("utf-8")
    except UnicodeEncodeError:
        # 孤立代理字符（POSIX ``os.environ`` 的 surrogateescape）等「写不进日志」的
        # 字符：任何 utf-8 handler 都会在这里炸 ⇒ 退化常量，绝不原样放出。
        return UNPRINTABLE_URL
    return rendered


def _safe_render(url: object, renderer: Callable[[object], str]) -> str:
    """总兜底：``renderer`` **任何**异常都退化为 :data:`UNPRINTABLE_URL`。

    调用点都在「日志 / 停机 / 注册」这些**不能抛**的路径上（``#158`` r1 定验
    FAIL (a)：一个非 UTF-8 口令曾让 ``RedisFlowWorker.__init__`` 与
    ``services.__main__.main()`` 直接抛 ``UnicodeEncodeError``）。
    """
    try:
        rendered = renderer(url)
    except Exception:  # noqa: BLE001 — fail-closed：掩码失败不得炸调用方
        # 刻意**不**打印异常文本：``UnicodeEncodeError`` 的 ``str()`` 就带着口令里的
        # 那个字符——掩码失败也不能变成新的泄露面。
        logger.debug("URL 掩码失败，退化为安全常量")
        return UNPRINTABLE_URL
    if not isinstance(rendered, str):
        return UNPRINTABLE_URL
    try:
        rendered.encode("utf-8")
    except UnicodeEncodeError:
        return UNPRINTABLE_URL
    return rendered


def _mask_url_for_log(url: object) -> str:
    """Redis/通用 URL（``userinfo`` 口令）进日志前的**总兜底**渲染。

    复用 ``#88`` 的 ``factory._mask_url``——不另起一套掩码（同一个 URL 两套渲染
    正是 ``#88`` 的根因形态）；只在外面加「不抛 + 安全常量」。
    """
    return _safe_render(url, _mask_url)


def _mask_webhook_url_for_log(url: object) -> str:
    """webhook URL（凭据在 path/query）进日志前的**总兜底**渲染。"""
    return _safe_render(url, _mask_webhook_url)
