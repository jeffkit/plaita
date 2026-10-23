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
import re
import traceback
from typing import Callable, Optional
from urllib.parse import urlsplit

#: ``#88`` 的掩码器——Redis URL（userinfo 口令）的唯一渲染路径。
#: **按名字 import**：``#88`` 若改名/删除，这里在 import 期即炸（fail-loud），
#: 而不是悄悄退化成一个不再掩码的版本。
# plaita#160/#161 重切：本模块住 foundation（core），而掩码语法的家在
# server.factory（#88 已落地，50 条锚在 factory.py）—— foundation 不得 import
# server，故用**注册注入**：factory 模块体（server→core 向下）把 ``_mask_url``
# 注册进来；未注册（纯 node 进程）时日志路径 fail-closed 返回不可打印占位，
# 绝不泄漏明文。monkeypatch 口径不变（测试照旧 patch 本模块的 ``_mask_url``）。
_mask_url = None


def register_mask_url_impl(fn) -> None:
    """``plaita.server.factory`` 在模块体里调用：把 ``_mask_url`` 注册进本模块。"""
    global _mask_url
    _mask_url = fn

logger = logging.getLogger("plaita.core.url_redaction")

#: 掩码失败时的**安全常量**——绝不退回原始 URL。
UNPRINTABLE_URL = "<unprintable-url>"

#: 被抹掉的 path segment / query / fragment 占位。
PLACEHOLDER = "***"

# --------------------------------------------------------------------------- #
# plaita#165：清洗入口的**硬上限**（fail-closed 有界化）
# --------------------------------------------------------------------------- #
# 本模块的每个公开清洗入口最终都把手上的串交给掩码器（``factory._mask_url``，
# 经 :data:`_mask_url` 注册注入）。掩码器对**分隔符密集**的输入是超二次的
# （``"x://" * n``：n=20 约 23 ms、n=40 约 168 ms、n=80 约 1458 ms ⇒ 三次量级），
# 而本模块的调用点全在**日志 / 异常文本**路径上（``HTTP.handle_http_node_err``
# 等），输入长度由第三方库/对端决定 ⇒ 一个几百 KB 的错误文本就能占满 worker。
#
# 掩码器自身已按 #165 加了预算与硬上限（``factory._MAX_MASK_INPUT`` 等，超限
# fail-closed）。**这里再加一层同口径的入口上限**，理由是入口的契约比掩码器更强：
# 它必须「任何输入都不抛、且成本有界」，所以超限时**根本不进**掩码器，直接退化为
# :data:`UNPRINTABLE_URL`（绝不回原文 —— 与「不能安全渲染时退化为常量」同一口径）。
# **门槛值是政策，不是机制**（``[policy]`` 提交单独落，可单独回滚）：机制只要求
# 「成本有界」，不要求「把正常但很长的 URL 整段掩掉」。门槛定在 **64 KiB**——正常
# webhook / 策略 URL 与第三方异常文本里的 URL 通常 < 8 KiB，64 KiB 留出近一个数量级
# 的余量，现实流量**绝无可能**触及；真正敌对的分隔符密集输入由掩码器自己的形状
# 闸门（``factory._MAX_SPAN_SCAN_WORK``）在 O(1) 内拦下，不依赖这个入口门槛。
# 依据与冷/热缓存曲线见 ``docs/proofs/165-masker-complexity.md``。
_MAX_CLEAN_INPUT = 64 * 1024
#: 单次 :func:`_scrub_url_text` 交给渲染器的 URL **累计**字符预算。单条 token 合规
#: 不蕴含总量合规（一条 80 KB 文本里可以塞满 2 KB 的敌对 token）。
_MAX_SCRUB_URL_CHARS = 1 << 16

#: #165 机制：**入口上限触发计数**（进程级、单调递增，测试可断言）。
#: 计数与 WARNING 是「政策与机制分离」的观测面：机制保证成本有界，政策决定门槛
#: 宽度；门槛一旦被现实流量触碰，必须**响亮**可见（不许 debug 级静默）。
_cap_trip_count = 0


def _record_cap_trip(where: str, length: int, limit: int) -> None:
    """#165：一次「因超限而 fail-closed」的**响亮**记录（WARNING + 计数）。

    只记长度与阈值，**绝不**回显输入内容——这里正是掩码器的输入面。
    """
    global _cap_trip_count
    _cap_trip_count += 1
    logger.warning(
        "URL 清洗输入超出 #165 上限（%s）：length=%d > limit=%d，已 fail-closed",
        where,
        length,
        limit,
    )


def _input_length(url: object) -> Optional[int]:
    """``str`` / ``bytes`` / ``bytearray`` 的计量长度；其它类型返回 ``None``。"""
    if isinstance(url, (str, bytes, bytearray)):
        return len(url)
    return None


def _enforce_clean_budget(url: object, where: str) -> bool:
    """#165 入口上限守卫：超限时**先**响亮记录，再让调用方 fail-closed。

    与 :func:`_within_clean_budget` 同判据（``len`` ≤ :data:`_MAX_CLEAN_INPUT`），
    区别是**触发面可观测**：每次超限都 WARNING + ``_cap_trip_count += 1``。
    """
    length = _input_length(url)
    if length is not None and length > _MAX_CLEAN_INPUT:
        _record_cap_trip(where, length, _MAX_CLEAN_INPUT)
        return False
    return True


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
        if not _enforce_clean_budget(url, "safe_render"):  # plaita#165：超限不进掩码器
            return UNPRINTABLE_URL
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


def _within_clean_budget(url: object) -> bool:
    """``url`` 是否在 :data:`_MAX_CLEAN_INPUT` 内（#165 的入口硬上限）。

    只看**长度**，不碰内容：``str`` 按字符数，``bytes``/``bytearray`` 按字节数
    （解码后至多同长 —— ``decode('utf-8', 'replace')`` 不会变长）。非这两种类型
    交给 ``_coerce_url_text`` 判定（它们本就 O(1) 退化）。超限一律 fail-closed，
    绝不回原文。

    只是**判据**（无副作用）；需要「触发可观测」的调用点用
    :func:`_enforce_clean_budget`（WARNING + 计数）。
    """
    length = _input_length(url)
    return length is None or length <= _MAX_CLEAN_INPUT


#: A2R58-03：未注册分支触发计数（进程级、单调递增，测试可断言）。WARNING 只在
#: **首次**触发打一条——读面 scrub（timeline/SSE 每帧每字段）是高频路径，无频控
#: 的 WARNING 曾把 console 日志逐条刷屏；之后降 DEBUG。「未注册」是进程级装配
#: 事实，不是逐次事件：响亮一次 + 计数可查即足，噪音无增量信息。
_unregistered_count = 0


def _mask_url_for_log(url: object) -> str:
    global _unregistered_count
    if _mask_url is None:                     # factory 未注册（纯 node 进程）：fail-closed
        _unregistered_count += 1
        if _unregistered_count == 1:
            logger.warning(
                "_mask_url 未注册——日志路径 fail-closed 掩码（本进程后续同类降 DEBUG；当前计数=%d）",
                _unregistered_count,
            )
        else:
            logger.debug("_mask_url 未注册——日志路径 fail-closed 掩码")
        return UNPRINTABLE_URL
    """Redis/通用 URL（``userinfo`` 口令）进日志前的**总兜底**渲染。

    复用 ``#88`` 的 ``factory._mask_url``——不另起一套掩码（同一个 URL 两套渲染
    正是 ``#88`` 的根因形态）；只在外面加「不抛 + 安全常量」。

    #165：超长输入在 :func:`_safe_render` 的入口上限处 fail-closed，**不进**掩码器。
    """
    return _safe_render(url, _mask_url)


def _mask_webhook_url_for_log(url: object) -> str:
    """webhook URL（凭据在 path/query）进日志前的**总兜底**渲染。"""
    return _safe_render(url, _mask_webhook_url)


# --------------------------------------------------------------------------- #
# plaita#151：**环境变量值**进日志前的唯一渲染（#168 判定表 C 类「环境投影」）
# --------------------------------------------------------------------------- #
# 判据来源（#168 `docs/log-site-taxonomy.md` §3 的 C 类第三格）：
#
#   「环境变量**值**（env 投影：插值变量在所在函数内赋值自 ``os.environ`` /
#     ``os.getenv``）⇒ 必须掩」
#
# 与 B 类的分界是「env 变量**名**允许、env 变量**值**必须掩」。值得注意的形态是
# 各处的 `_*_from_env()`：它们的日志文案是「你把 X 配成了非法值，回退默认」——
# 诊断价值在于**「配错了」**这一事实与值的**形状**（不是数字 / 有多长），而不是值
# 的原文。原实现用 ``%r``/``%s`` 回显 ``raw``：一份被误置成 ``access_token=…`` 或
# 直接就是凭据本体的环境变量会被原样写进 stderr / 日志收集器（#151 探针实测 7/7
# 站点把密钥明文送进 ``record.args``）。
#
# #151 的验收口径是「去掉作者数据（改为**长度/形态/计数**）」，故这里**不**回显任何
# 原文片段（连掩码后的残片也不给——env 值没有「端点」那种必须保留的诊断锚点），
# 只给「长度 + 形态」这一对结构事实：
#
#   ``_redact_env_value("abc")``   -> ``<env:text len=3>``
#   ``_redact_env_value("8080")``  -> ``<env:int len=4>``
#   ``_redact_env_value("12.5")``  -> ``<env:number len=4>``
#   ``_redact_env_value("")``      -> ``<env:empty>``
#
# **与掩码语法的关系**：这不是新的一套掩码文法（#88 的根因形态是「同一个 URL 两套
# 渲染」）；它是一条**恒等去值**规则，不解析内容、不做形状匹配，因此不存在「两个
# 实现分叉」的余地。凭据形状的值本来也轮不到它——那些走 :func:`_mask_url_for_log`
# / :func:`_scrub_url_text`。
#
# **total**（与本模块其余渲染器同一判据）：任何输入都返回 ``str``，绝不抛、绝不
# 退回原文；``str()`` / ``len()`` 失败各自退化为具名常量。
_ENV_INT_RE = re.compile(r"[+-]?\d+")
_ENV_NUMBER_RE = re.compile(r"[+-]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?")


def _redact_env_value(value: object) -> str:
    """环境变量**值** → 进日志的形态描述（绝不回显值本体；``#151``）。"""
    if value is None:
        return "<env:unset>"
    try:
        text = value if isinstance(value, str) else str(value)
    except Exception:  # noqa: BLE001 — fail-closed：渲染失败不得炸调用方
        # 刻意不回显异常文本（它可能带着刚被拒绝的值）。
        logger.debug("环境变量值无法字符串化，退化为安全常量")
        return "<env:unrenderable>"
    stripped = text.strip()
    if not stripped:
        return "<env:empty>"
    try:
        length = len(text)
    except Exception:  # noqa: BLE001 — __len__ 可被作者覆写
        logger.debug("环境变量值长度不可测量，退化为安全常量")
        return "<env:unrenderable>"
    if _ENV_INT_RE.fullmatch(stripped):
        shape = "int"
    elif _ENV_NUMBER_RE.fullmatch(stripped):
        shape = "number"
    else:
        shape = "text"
    return f"<env:{shape} len={length}>"


# --------------------------------------------------------------------------- #
# plaita#160 + #161：**任意文本**里的 URL 清洗器（自 ``plaita/node/http.py`` 提取）
# --------------------------------------------------------------------------- #
# 为什么住在这里（jeffkit #161 的口径：「提取成共享渲染器，自然的家是 url_redaction」）：
#
# 1. **唯一实现**。``#159`` r2 把「第三方异常文本里的请求 URL」清洗放在
#    ``HTTP.wrap_http_node_err``（``node/http.py``）。``#161`` 又发现三个**不在
#    HTTP 节点里**的 sink（``server/alerts.py`` / ``server/services/
#    http_callback_service.py`` / ``server/nodes/http_callback_node.py``）同样把
#    第三方异常文本（requests/aiohttp/urllib）交给 ``logger.*``。若各写一份，
#    「同一个 URL 两套渲染」立刻重现——那正是 ``#88`` 的根因形态。
# 2. **依赖方向单一**。本模块只依赖 stdlib + ``server.factory._mask_url``（不触发
#    server extra），``node/http.py`` 反向 import 本模块——不成环；反之把渲染器留在
#    ``node/http.py`` 则 ``server/*`` 要反向依赖 node 层。
# 3. **本模块已是「URL 进日志前的唯一渲染路径」的家**（``#158``/``#159`` 建的），
#    守卫 ``tests/unit/_url_sink_support.py::_MASKER_NAMES`` 认的也正是这里的名字。
#
# 迁出后 ``node/http.py`` 以**同名** import 的只有 ``_mask_policy_url`` /
# ``_scrub_url_text``（外加 ``#161`` 为「历史可达名」再导出的 ``_ABSOLUTE_URL_RE`` /
# ``_CREDENTIAL_RUN_RE`` / ``_token_is_canonical`` / ``_render_is_usable`` /
# ``_leaves_plaintext_payload``；``#160`` 修复轮再加共享凭据键谓词
# ``_is_credential_key``）：既有调用点、``http_mod._mask_policy_url`` 的测试探针、
# 守卫判据全部逐字不变。
#
# **``_mask_url_for_log`` / ``_mask_webhook_url_for_log`` 不在** ``node/http.py``
# 的 import 名单里（``hasattr(plaita.node.http, "_mask_url_for_log") is False``，
# 组合轮定验实测）——它们的调用点（``server/alerts.py`` / ``server/registry.py``
# 等）直接从本模块 import。本条注释在 ``#160`` 修复轮校正，此前写成「四个名字
# 都仍以同名 import」，与代码不符。

# 下面这一整段（`_mask_policy_url` … `_scrub_url_text` + 匹配器/凭据键文法）是
# ``#159`` r4 在 ``plaita/node/http.py`` 上定稿的实现，随 ``#161`` 的提取**逐字**搬入
# 本模块（只把封装的占位常量接到本模块既有的 ``PLACEHOLDER``）。r4 的反例修法：
# token 边界只认 ASCII 空白、分隔符留在 span 内、**绝不恒等替换**、补「凭据键=值」一趟。
def _mask_policy_url(url: object) -> str:
    """策略异常消息里引用请求 URL 的**唯一**渲染路径（``#158``+``#159`` r2）。

    为什么必须在**构造消息时**就掩码：``URLPolicyError`` 不会停在原地——它被
    ``HTTP.handle_http_node_err`` 用 ``str(err)`` 包成 ``NodeException``，
    ``core.runner._handle_node_error`` 再折成 ``NodeExecutionError``；这三层
    异常对象随后被 ``logger.warning(…, e)`` / ``exc_info=True`` /
    ``traceback.format_exception`` 打印（``core/runner.py``、``server/flow_worker.py``）。
    在日志调用点掩码太晚——明文已经进了异常对象，且「logger 实参里有没有 URL 名字」
    这条判据对「异常对象带明文」结构性失明（r2 独立定验的 finding）。

    两类凭据都要抹：HTTP 节点的策略 URL 既可能带 userinfo 口令（``#88`` 的 Redis
    型，``_mask_url_for_log`` 负责），也可能带 webhook 型 path/query token
    （``#159``，``_mask_webhook_url_for_log`` 负责）。两个渲染器**串联**即保守
    形态：先抹 userinfo，再抹每一个 path segment 与整条 query/fragment；任一渲染
    器失败都退化为 ``UNPRINTABLE_URL``，绝不退回原文，也绝不上抛（调用点在策略
    拒绝路径上，掩码不许把它炸成另一种异常）。

    保留的诊断价值：``scheme://host[:port]`` 原样留下（路径形状以 ``***`` 保留），
    足以定位「哪个端点被策略拒绝」。
    """
    return _mask_webhook_url_for_log(_mask_url_for_log(url))


#: 任意文本里的绝对 URL（``scheme://…``）——第三方 HTTP 客户端会把请求 URL 写进
#: 自己的异常文本（``requests``：``Max retries exceeded with url: /path?query``；
#: ``aiohttp``：``url='http://host/path?query'``）。
#:
#: **token 边界 = 只认 ASCII 空白**（``[ \t\r\n\f\v]``），且**只**是 ASCII 空白。
#: 这是 ``#159`` r4 定验反例的正面修法，两条缺一不可：
#:
#: * 旧判据写 ``\s``——对 ``str`` 它是 **Unicode 感知**的，NBSP（``\u00a0``）、
#:   窄空格（``\u202f``）、表意空格（``\u3000``）都会「结束」token ⇒
#:   ``http://h/x\u00a0?t=<secret>`` 被切成 ``http://h/x`` + **带凭据的尾巴**；
#: * 旧判据把 ``, ; ( ) " ' < > { }`` 排除在字符类外——它们**合法地出现在 URL
#:   内部**（``?ids=1,2``、``/robot;v=1``、CSV 型 path）⇒ 匹配在那里截断，而
#:   ``re.sub`` 只替换匹配片段，**匹配之后的尾巴原样留在输出里**，凭据从尾部漏出。
#:
#: 代价（刻意选定的方向）：一旦认出 scheme 就一路吃到下一个 ASCII 空白，散文里
#: URL 后紧跟的 ``,`` / ``;`` / 引号会被一并吃进 path/query 掩码掉——**宁可多掩**。
#: ``[`` / ``]`` 也**不在**排除集里（``#159`` r3）：authority 可以是方括号形态
#: （``http://[::1]:999999/…``、``http://[webhook.internal]:8443/…``），旧判据把
#: 它们排除在外 ⇒ 匹配在 ``[`` 处截断甚至完全不匹配 ⇒ 原文整段穿过清洗器
#: （``aiohttp``/``yarl`` 的 ``InvalidURL`` 文本就是**裸 URL**）。
#:
#: ``{0,31}`` 是 **scheme 体长上限**，纯粹为**线性代价**：不加界时
#: ``[A-Za-z0-9+.\\-]*`` 在「100k 字母数字串（无论后面跟不跟 ``://``）」上退化成
#: O(n²) 回溯——r4 实测 100k 输入 **5318 ms**，加界后 **4.6 ms**（同一台机器，同一
#: 输入）。注意这个上界**不会**放过凭据：匹配总是从 ``://`` 之前的最后一个合法
#: scheme 起点开始、并一路吃到下一个 ASCII 空白，所以「超长 scheme」只让前导几个
#: 字符留在原文，凭据所在的那一段仍整段进掩码。
_ABSOLUTE_URL_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]{0,31}://[^ \t\r\n\f\v]+")
#: :func:`_render_absolute_url_ref` 端点兜底只承认的 scheme（与
#: ``url_redaction._SAFE_SCHEMES`` 同一集合；``redis://`` / ``ftp://`` 退化常量）。
_ENDPOINT_SCHEMES = frozenset({"http", "https"})
#: ``url:`` / ``url=`` 之后的 scheme-relative 引用（只有 requests 这一族这样写）。
#: 边界与 :data:`_ABSOLUTE_URL_RE` 同源：ASCII 空白才结束，分隔符留在引用内。
_RELATIVE_URL_RE = re.compile(
    r"(?P<prefix>\burl[=:][ \t\r\n\f\v]*['\"]?)(?P<ref>/[^ \t\r\n\f\v]*)"
)
#: path segment / query / fragment 的掩码占位（与 url_redaction 同一语义）。
_MASK_PLACEHOLDER = "***"

#: **凭据键 = 值** 的裸串（``#159`` r4 起，r5 重写为线性扫描）。r3 的清洗器只认
#: 「URL 形状」，于是 ``token=K…``、``"x"*100000 + "?access_token=K…"`` 这类
#: **无 scheme、无 ``url:``/``url=`` 标签**的串整段原样穿过（定验残口）。这里补一条
#: **收窄到凭据键**的规则：键名像凭据才掩值；键名不像的一律不碰，否则会把
#: ``code=1002`` / ``port=8080`` / ``block_private_networks=true`` 这类诊断文本
#: 一起吃掉（那是不可接受的可用性代价）。
#:
#: **r5 定验反例（FINDING 1）与修法**：r4 的键文法（``_CREDENTIAL_RUN_RE``）要求键名
#: **直接**跟 ``\s*[=:]\s*["']?``——引号只允许出现在分隔符**之后**，分词又只认
#: ``_ - .``。于是两个方向同时漏（6 面 × 2 路径实测全部原文穿过）：
#:
#: * **引号在键这一侧**：``{"access_token": "K"}`` / ``{'access_token': 'K'}`` /
#:   ``failed "password"=K`` / ``"token": K`` ⇒ ``keymatch=None``；
#: * **驼峰/粘连**：``accessToken`` / ``sessionId`` / ``Authorization`` 没有
#:   ``_ - .`` 分词点 ⇒ 键名判据落空。
#:
#: 现在的判据（**线性扫描**，正则整条移除，见 :func:`_credential_run_spans`）：
#: 扫每一处 ``=`` / ``:``，向左在**有界窗口**里取出紧贴分隔符的键名（剥掉紧贴的
#: ASCII 空白与引号/括号——**两侧都认**），键名按「非字母数字边界 + 驼峰转折」切成
#: 分量，**任一分量**命中 :data:`_CREDENTIAL_STEMS` 即算凭据键（复数只认显式列出的
#: ``credentials``）。
#: 分量**相等**而非子串包含：``monkey`` / ``sortkey`` 仍不命中（诊断文本保住），
#: 而 ``x-api-key``（分量 ``key``）/ ``accessToken``（分量 ``token``）命中。
#:
#: r4 的 ``_CREDENTIAL_RUN_RE`` 有嵌套量词 ``(?:comp sep)*(?:word)(?:sep comp)*``
#: 紧接一个必须出现的 ``[=:]``，在 ``"token_"*n`` 这类「全是分量边界、就是没有
#: 分隔符」的串上**灾难性回溯**——HEAD 实测 n=8000（48 KB）5418 ms、n=30000
#: （180 KB）72095 ms，而 r3 的同类输入是 0.05–0.80 ms（FINDING 2，可达：
#: ``handle_http_node_err`` 6 KB→84.9 ms / 12 KB→331.2 ms）。r5 用单趟扫描替掉
#: 正则，值跨度扫过即跳过。
#:
#: **r6 定验的两处「窗口截断」回归与修法**（r5 引入，均回归到 r4 之下）：
#:
#: * **键名回看**曾有 ``_MAX_KEY_WINDOW = 64`` 上界。前缀侧安全（词干紧贴分隔符），
#:   **后缀侧漏**：``access_token_`` + ``'a'*s`` + ``=SECRET`` 在 ``s ≥ 59`` 时
#:   回看被截断成纯 ``a`` 串 ⇒ 判据落空、原文穿过（r4 在 ``s = 200`` 仍掩）。
#:   现在回看到 :data:`_KEY_CHARS` 里**键 token 的起点**，不设硬上限。
#: * **带引号值**的 ``find`` 曾有 ``_QUOTED_VALUE_WINDOW = 4096`` 上界，找不到闭引号
#:   就 ``v1 = v0 + 4096`` ⇒ **第 4096 个字符之后整段留在输出里**（>4 KB 的
#:   IdP/JWT 断言值连同密钥本体泄漏；r4 同输入掩到真末尾）。现在 ``find`` 到串尾。
#:
#: **线性（摊还）证明**：设访问到的分隔符为 ``s_1 < … < s_m``。
#:
#: * 键回看的区间 ``(b_i, s_i)`` **两两不交**——``s_i`` 本身不是 :data:`_KEY_CHARS`，
#:   从 ``s_j``（j>i）向左的回看一旦碰到 ``s_i`` 就停下，故 ``b_j ≥ s_i``；
#:   空白回看与转义反斜杠回看同理（间隔端点不可能是空白/反斜杠）。
#: * 引号闭引号扫描（:func:`_find_closing_quote`）：产出跨度时 ``i = v1`` 跳过已扫
#:   区间，区间两两不交；未产出跨度只有两种情况——空值（扫 0 字符）或切片恰为
#:   ``***``（扫 3 字符）；未闭合引号则一次扫到 ``n`` 后 ``i = n`` 直接结束。
#:   反斜杠计数是常数空间、每位置访问一次 ⇒ 扫描本身线性（r7 F3 的转义闭引号修法）。
#:
#: 故总体 O(n)、无回溯，不需要任何硬窗口。
#: ASCII 空白——与 :data:`_ABSOLUTE_URL_RE` 同一条边界判据（只有 ASCII 空白结束 token）。
_ASCII_WS = " \t\r\n\f\v"
#: 键名分量内的字符；其余字符都是分量边界（``=`` / ``:`` / 空白 / 引号 / ``&`` …）。
_KEY_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
)
#: 键名两侧紧贴的闭/开引号与括号（``"access_token":``、``failed "password"=``、
#: ``{"token":``）。只用于把键名从引号里剥出来判断，**不参与**输出替换。
_CLOSING_QUOTES = frozenset("\"')]}")
_VALUE_STOP = frozenset(" \t\r\n\f\v&\"'<>")
#: ``Authorization: Bearer <secret>``：认证方案词之后的第二个运行段也是凭据。
_AUTH_SCHEME_WORDS = frozenset({"bearer", "basic", "digest", "token"})
#: 分量切分：非字母数字边界 **加上驼峰转折**（``accessToken`` ⇒ access｜Token，
#: ``APIKey`` ⇒ API｜Key）。r4 只认 ``_ - .``，驼峰整段被当成一个分量 ⇒ 判据落空。
_KEY_SEGMENT_RE = re.compile(r"[A-Z]+(?![a-z])|[A-Z][a-z0-9]*|[a-z0-9]+")
#: 凭据**词干**（分量级：全小写、无分隔符）。分量**相等**才算命中——所以 ``monkey`` /
#: ``sortkey`` 里的 ``key`` 不算。粘连形式（``accesstoken`` / ``authtoken`` /
#: ``secretkey`` …）是**显式**的另一组词干：现实里 HTTP header / JSON 字段确实这么写，
#: 而「任意前缀 + 词干」这种开放规则会把 ``monkey`` / ``sortkey`` 一起吃进来。
_CREDENTIAL_STEMS = frozenset({
    "token", "secret", "password", "passwd", "pwd", "signature", "sig",
    "credential", "credentials", "apikey", "accesskey", "privatekey",
    "clientsecret", "session", "cookie", "bearer", "key", "auth",
    "authorization", "accesstoken", "authtoken", "refreshtoken", "idtoken",
    "bearertoken", "sessionid", "secretkey", "apisecret", "xapikey",
})


def _is_credential_key(key: str) -> bool:
    """键名（可含引号/括号/分隔符）是否凭据键——**分量级**判据。

    ``access_token`` / ``x-api-key`` / ``"client_secret"`` / ``accessToken`` /
    ``Authorization`` / ``credentials`` 命中；``code`` / ``port`` / ``v`` /
    ``monkey`` / ``sortkey`` / ``block_private_networks`` / ``keys`` 不命中
    （诊断文本的主体）。复数只认 :data:`_CREDENTIAL_STEMS` 里**显式列出**的
    ``credentials``——开放复数规则会把 ``keys=…`` 也吃掉。
    """
    for segment in _KEY_SEGMENT_RE.findall(key):
        if segment.lower() in _CREDENTIAL_STEMS:
            return True
    return False


def _key_before_separator(text: str, sep_index: int) -> str:
    """分隔符左侧紧贴的键名（剥掉紧贴的 ASCII 空白与闭引号，再回看到键 token 起点）。

    回看**不设长度上限**（r6）：上界曾是 ``_MAX_KEY_WINDOW = 64``，前缀侧安全但
    后缀侧漏（``access_token_`` + ``'a'*s`` + ``=``，``s ≥ 59`` 时键 token 被截成
    纯 ``a`` 串）。回看到 :data:`_KEY_CHARS` 的起点仍是线性：见文件上方「线性
    （摊还）证明」——各分隔符的键回看区间两两不交。

    ``\\"key\\"`` / ``\\'key\\'``（Java / 双重编码文本里的转义引号）在剥掉闭引号后
    再跳过其转义反斜杠，否则 ``\\`` 会隔断回看、键名判成空串（r6 关闭的残口）。
    """
    j = sep_index
    while j > 0 and text[j - 1] in _ASCII_WS:
        j -= 1
    if j > 0 and text[j - 1] in _CLOSING_QUOTES:
        j -= 1
        while j > 0 and text[j - 1] == "\\":
            j -= 1
    end = j
    while j > 0 and text[j - 1] in _KEY_CHARS:
        j -= 1
    return text[j:end]


def _find_closing_quote(text: str, start: int, quote: str) -> int:
    """从 ``start`` 起找**未被反斜杠转义**的闭引号；找不到返回 ``len(text)``（r7）。

    单趟前向扫：只有**紧邻的连续反斜杠个数为奇数**时，该引号才算被转义
    （``\\"`` 转义、``\\\\"`` 不转义），与 Java / JSON 双重编码文本的约定一致。
    用「先数反斜杠、再判奇偶」而不是「对每个引号往左看」是为了**线性**：
    任一位置最多被访问一次，反斜杠计数是常数空间。

    存在的理由（r7 F3）：``text.find(quote, v0)`` 会把值里被转义的 ``\\"`` 当闭引号
    ⇒ 掩码停在 ``\\"`` 处，其后的原文（含密钥本体）整段留在输出里。r4/r5/r6 三版
    都漏：``access_token="pre\\"Ks9Qm4Zt"`` → ``access_token="***"Ks9Qm4Zt"``。
    """
    n = len(text)
    i = start
    backslashes = 0
    while i < n:
        ch = text[i]
        if ch == "\\":
            backslashes += 1
            i += 1
            continue
        if ch == quote and backslashes % 2 == 0:
            return i
        backslashes = 0
        i += 1
    return n


def _credential_run_spans(text: str) -> list:
    """扫出全部「凭据键 = 值」的**值跨度** ``(v0, v1)``（升序、不重叠）。

    单趟左→右：键回看扫到键 token 起点（区间两两不交，见文件上方摊还证明）；
    值跨度一旦命中就跳到 ``v1`` 继续扫（不回头）⇒ 整体 O(n)、无回溯。已经是占位
    ``***`` 的值不再产出跨度（掩码的不动点，二次清洗必须逐字不变）。

    **带引号的值扫到真末尾**（r6）：不再有 ``_QUOTED_VALUE_WINDOW`` 上界。
    上界的代价是把第 4096 个字符之后的原文整段留在输出里（>4 KB 的断言值 + 密钥
    本体泄漏），而收益（避免二次代价）由 ``i = v1`` 的摊还已经给出——未闭合引号
    一次扫到 ``n`` 即结束，没有「每处都扫到串尾」的形态。

    **值内部的转义引号不再截断掩码**（r7）：闭引号由 :func:`_find_closing_quote`
    找——**未被反斜杠转义**的那个才算闭引号。旧实现（r4/r5/r6 三版）用
    ``text.find`` 直接把 ``\\"`` 当闭引号 ⇒ 掩码停在它前面，其后原文（含密钥本体）
    留在输出里：``access_token="pre\\"Ks9Qm4Zt"`` → ``access_token="***"Ks9Qm4Zt"``。

    值的开引号也认转义形态 ``\\"K\\"``（Java / 双重编码文本），与键侧同一残口。
    """
    n = len(text)
    spans = []
    i = 0
    while i < n:
        ch = text[i]
        if ch != "=" and ch != ":":
            i += 1
            continue
        sep = i
        i += 1
        key = _key_before_separator(text, sep)
        if not key or not _is_credential_key(key):
            continue
        k = i
        while k < n and text[k] in _ASCII_WS:
            k += 1
        if k >= n:
            continue
        escaped = k + 1 < n and text[k] == "\\" and text[k + 1] in "\"'"
        quote = text[k + 1] if escaped else (text[k] if text[k] in "\"'" else "")
        if quote:
            v0 = k + 2 if escaped else k + 1
            if v0 >= n:
                continue
            close = _find_closing_quote(text, v0, quote)
            v1 = close
        else:
            v0 = k
            v1 = v0
            while v1 < n and text[v1] not in _VALUE_STOP:
                v1 += 1
            if (
                v1 < n
                and text[v1] in _ASCII_WS
                and text[v0:v1].lower() in _AUTH_SCHEME_WORDS
            ):
                # ``Authorization: Bearer <secret>``：方案词后的第二个运行段一起掩
                w = v1
                while w < n and text[w] in _ASCII_WS:
                    w += 1
                if w < n and text[w] not in _VALUE_STOP:
                    while w < n and text[w] not in _VALUE_STOP:
                        w += 1
                    v1 = w
        if v1 <= v0 or text[v0:v1] == _MASK_PLACEHOLDER:
            continue
        spans.append((v0, v1))
        i = v1  # v1 > v0 ≥ i-1 ⇒ 不回头
    return spans


def _mask_credential_runs(text: str) -> str:
    """凭据键的**值**换占位，键名与引号原样（看得出抹掉的是哪个键）。"""
    spans = _credential_run_spans(text)
    if not spans:
        return text
    out = []
    last = 0
    for v0, v1 in spans:
        out.append(text[last:v0])
        out.append(_MASK_PLACEHOLDER)
        last = v1
    out.append(text[last:])
    return "".join(out)


def _mask_relative_url_ref(ref: str) -> str:
    """scheme-relative 引用（``/a/b?token=x``）：每个非空 segment 与整条 query 换 ``***``。

    与绝对 URL 同一条「绝不恒等替换」守卫（r4）：渲染结果与入参**恒等**时，只有
    已经是掩码形态（``/***/***?***``）才算合法的幂等不动点，否则退化为
    :data:`UNPRINTABLE_URL`。

    #165：超长引用（> :data:`_MAX_CLEAN_INPUT`）直接退化常量（触发必响亮）。
    """
    if not _enforce_clean_budget(ref, "relative_url_ref"):
        return UNPRINTABLE_URL
    try:
        parts = urlsplit(ref)
    except Exception:  # noqa: BLE001 — 畸形串退化为常量，绝不回原文
        # 刻意**不**打印异常文本：它可能带着刚被清洗掉的原文片段。
        logger.debug("scheme-relative URL 无法解析，退化为安全常量")
        return UNPRINTABLE_URL
    rendered = "/".join(_MASK_PLACEHOLDER if seg else seg for seg in parts.path.split("/"))
    if parts.query:
        rendered += f"?{_MASK_PLACEHOLDER}"
    if parts.fragment:
        rendered += f"#{_MASK_PLACEHOLDER}"
    if rendered == ref and not _payload_is_fully_masked(ref):
        logger.debug("scheme-relative 引用渲染为恒等且非掩码形态，退化为安全常量")
        return UNPRINTABLE_URL
    return rendered


def _mask_bracketed_absolute_url(token: str) -> str:
    """**方括号 authority** 绝对 URL 的端点兜底（``#159`` r3，r4 校正文档）。

    ``urlsplit`` 把方括号里的内容当 IPv6 字面量校验：``[webhook.internal]`` /
    ``[]`` 会抛 ``ValueError('… does not appear to be an IPv4 or IPv6 address')``，
    于是 :func:`_mask_webhook_url` 依契约退化为 ``UNPRINTABLE_URL``——**安全**，但
    把「哪个端点」也一起丢了（运维看不到被拒/失败的 ``host:port``）。

    兜底只保留 ``scheme://authority``：**丢弃 userinfo**（`#88` 的口令形态）与
    path/query/fragment 全部。触发面刻意收窄到「authority 是方括号形态却不是合法
    IPv6 字面量」这一族（``http://[webhook.internal]:8443/…``、``http://[]:8443/…``），
    且 scheme 仍限 :data:`_ENDPOINT_SCHEMES`。

    **非 http(s) 方括号 URL 的真实行为（r4 更正，r3 的文档在此处说错了）**：
    ``redis://[::1]:6379/0`` / ``ftp://[bad]:1/x`` / ``ws://…`` 现在返回
    :data:`UNPRINTABLE_URL` 常量，**不是**「与 r2 逐字一致」——r2 的
    ``_ABSOLUTE_URL_RE`` 把 ``[``/``]`` 排除在字符类外，这些串**根本不匹配**，
    于是 `re.sub` 一动不动、**原文逐字穿过**（既非端点、也非常量）。r4 把方括号
    收进字符类后它们才走到这里，被 scheme 判据拦下退化为常量。

    这个交换是**刻意的 fail-closed**：代价是「一个合法的非 http(s) 方括号 URL
    丢掉全部端点诊断（只剩常量）」，收益是「r2 那种原文穿过的形态结构性消失」。
    可诊断性与不可泄漏冲突时，本单一律选后者。

    与 :func:`_mask_policy_url` 的暴露面**同源**：后者对非方括号 URL 一样原样保留
    authority（``http://host/***``），所以「保留 authority」不是新增暴露类；且结果
    必须 UTF-8 可编码（写不进日志就退化常量，纪律同 ``url_redaction``）。
    本函数**不保证**非恒等——恒等守卫由 :func:`_render_absolute_url_ref` 统一施加。
    """
    scheme, sep, rest = token.partition("://")
    if not sep or scheme.lower() not in _ENDPOINT_SCHEMES:
        return UNPRINTABLE_URL
    authority = rest.split("/", 1)[0]
    authority = authority.split("?", 1)[0].split("#", 1)[0]
    host = authority.rpartition("@")[2]  # 丢 userinfo（#88 的口令形态）
    if not host.startswith("["):
        return UNPRINTABLE_URL
    rendered = f"{scheme.lower()}://{host}"
    try:
        rendered.encode("utf-8")
    except UnicodeEncodeError:
        # 同上：不回显异常文本（它带着那个写不进日志的字符）。
        logger.debug("端点兜底结果无法编码为 UTF-8，退化为安全常量")
        return UNPRINTABLE_URL
    return rendered


def _original_url_payload(token: str) -> str:
    """token 里 ``scheme://authority`` **之后**的 path/query/fragment（无则 ``""``）。"""
    _, sep, rest = token.partition("://")
    if not sep:
        return ""
    cut = len(rest)
    for marker in "/?#":
        pos = rest.find(marker)
        if pos != -1:
            cut = min(cut, pos)
    return rest[cut:]


#: 「已经是掩码形态」的 payload：只由 ``/`` 结构分隔符与 ``***`` 占位组成
#: （``/***/***?***``、``/?***``、``#***``、``/``、``?***``）。用来把
#: **幂等不动点**与**恒等替换**区分开：前者没有明文可泄，后者正是 r3 反例 A。
_MASKED_PAYLOAD_RE = re.compile(r"^(?:/(?:\*\*\*)?)*(?:\?\*\*\*)?(?:#\*\*\*)?$")


def _payload_is_fully_masked(payload: str) -> bool:
    """payload 是否已只剩 ``***`` 占位与结构分隔符（非空）。"""
    return bool(payload) and _MASKED_PAYLOAD_RE.fullmatch(payload) is not None


def _plaintext_pieces(payload: str) -> list:
    """payload 里的**明文片段**：path 的每个非空 segment + query + fragment。

    占位 ``***`` 本身（及其任意子串，如 path segment 恰为 ``*``）不算明文——
    那是清洗器的输出词汇，不是原文。
    """
    body, _, frag = payload.partition("#")
    path, _, query = body.partition("?")
    pieces = [seg for seg in path.split("/") if seg]
    if query:
        pieces.append(query)
    if frag:
        pieces.append(frag)
    return [piece for piece in pieces if piece not in _MASK_PLACEHOLDER]


def _authority_of(token: str) -> str:
    """token 里 ``scheme://`` 之后的 authority（到 ``/`` / ``?`` / ``#`` 为止）。"""
    _, sep, rest = token.partition("://")
    if not sep:
        return ""
    return rest.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]


#: 单标签 host 的**闭合白名单**：只有这些「众所周知的本地名」在没有点号时才保留。
#: 单标签是唯一「在字符层面与不透明凭据 token 无法区分」的形态（``h`` 与
#: ``ks9qm4zt`` 都是合式 LDH 单标签），语法判据在这一格没有信息可用 ⇒ 除白名单外
#: 一律 fail closed。**闭合**（不是前缀/词缀规则）：每加一个名字都要单独审查。
#: 大小写不敏感。``host`` 在列是因为 r5 已钉住 ``http://host:8080`` 必须保留。
_BENIGN_SINGLE_LABEL_HOSTS = frozenset({"h", "host", "localhost"})
#: 合式 DNS 标签（LDH）：字母/数字/连字符，不以连字符开头或结尾，1–63 字符
#: （RFC 1035 的长度上界正是这里挡住「把整条 JWT 放进 host 位置」的结构性理由）。
_DNS_LABEL_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z")


def _is_dns_name(host: str) -> bool:
    """≥2 个合式 LDH 标签的点分名（``example.com`` / ``API.EXAMPLE.COM`` / ``10.1.2.3``）。

    IPv4 点分四段**不需要单独一腿**：纯数字标签本身就是合式 LDH 标签，所以
    ``10.1.2.3`` 由本条覆盖（r7 曾写过一条 ``_is_ipv4_literal``，它是本条的真子集 ⇒
    已删除，避免「看着在守、其实永远走不到」的死判据）。反过来说 ``256.1.2.3`` 也会
    被当作**点分名**收下——语法上它确实是一个点分名，这不是 IP 校验器的位置。
    """
    labels = host.split(".")
    if len(labels) < 2:
        return False
    return all(_DNS_LABEL_RE.match(label) is not None for label in labels)


def _host_is_identifiable(host: str) -> bool:
    """host 是否**语法上可识别的宿主标识**——属性判据（r7；取代 r6 的大小写代理）。

    **属性**：只有「渲染结果**没掩掉任何 path/query 字节**」的 token 才会走到这里
    （见 :func:`_token_is_canonical`）。此时「保留端点」的唯一正当理由是「authority
    是一个**语法上可识别的宿主标识**、不是凭据本体」。判据只依赖 host 的**结构**，
    与大小写**无关**（r6 的「host 里有没有大写 ASCII」是代理，两个方向都错）：

    1. **方括号 authority**（``[…]``）：``]`` 闭合即保留。这是 r3/r4 冻结的**端点
       兜底契约**（:func:`_mask_bracketed_absolute_url`）——``urlsplit`` 把方括号内容
       当 IPv6 字面量校验，``[bad]`` / ``[web,hook]`` / ``[web{hook]`` 一律被拒，
       兜底保留 ``host`` 供运维定位。**代价（已声明，见
       :func:`_scrub_url_text` 残口清单）**：``http://[Ks9Qm4Zt]`` 与合法端点
       ``http://[bad]`` 是**同一个串类**（``[`` + 单个合式 DNS 标签 + ``]``），而
       ``[bad]`` 的保留被既有测试钉住（``http://user:SECRET@[bad]:1/x?…`` →
       ``http://[bad]:1``）⇒ 任何掩掉 ``[Ks9Qm4Zt]`` 的规则都会一起掩掉 ``[bad]``，
       所以本判据在此格**不能** fail closed，只能登记。
    2. 非方括号：剥掉 userinfo 与 ``:port`` 之后，host 必须是
       * 一个 **≥2 标签的点分 DNS 名**（``example.com`` / ``api.example.com`` /
         ``Example.com`` / ``API.EXAMPLE.COM``；每个标签是合式 LDH、1–63 字符。
         ``10.1.2.3`` 这类 IPv4 点分四段由本腿覆盖——见 :func:`_is_dns_name`），或
       * 一个**单标签**名，且其小写形式在**闭合白名单**
         :data:`_BENIGN_SINGLE_LABEL_HOSTS`（``h`` / ``host`` / ``localhost``）。
    3. 其余一律 fail closed（渲染结果不可用 ⇒ 退化为 :data:`UNPRINTABLE_URL`）。

    **与 r6 的两个方向**（r6 把「大写字母」当判据，于是两向都错）：

    * 假阴性（r6 放行、r4 曾是常量）：``http://ks9qm4zt`` / ``http://deadbeef`` /
      ``http://hunter2`` / ``http://550e8400-e29b-41d4-a716-446655440000`` 这类
      **全小写的不透明 token**。现在它们是「非白名单单标签」⇒ fail closed。
    * 假阳性（r6 打成常量、r5/r4 曾保留）：``http://Example.com`` /
      ``http://API.EXAMPLE.COM`` / ``http://EXAMPLE.com:443/`` 这类**合法点分 DNS 名**。
      结构判据不看大小写 ⇒ 保留。

    **代价（诚实边界，逐条可测）**：单标签但**不在**白名单里的名字同样 fail closed
    ⇒ ``http://DESKTOP-ABC123``（真实机器名）、``http://webhook`` 这类合法单标签端点
    也退化为常量。这是与 r5 已钉住的 ``http://h`` / ``http://host:8080`` 同一格的取舍：
    可诊断性与不可泄漏冲突时选后者；该决定由
    ``TestHostIdentityPropertyR7::test_single_label_non_whitelisted_is_declared_masked``
    钉住。

    **点分的不透明 token**（``http://secret.example``、把 JWT 放进 host 位置）只要每个
    标签 ≤63 字符且是 LDH 就仍被保留——**已声明残口**，与「冻结契约的 host 残留」
    （``http://Ks9Qm4Zt/x`` → ``http://Ks9Qm4Zt/***``）同类。63 字符的 DNS 标签上界
    把**真实**的 JWT（单段通常远超 63）挡在 fail-closed 一侧。
    """
    host = host.rpartition("@")[2]
    # 只留 authority 本体：``_authority_of`` 的同一刀（``/`` / ``?`` / ``#`` 之前）。
    # 少了这一刀 ``http://h/`` 的 host 会读成 ``"h/"`` ⇒ 白的单标签名被判黑。
    host = host.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    if host.startswith("["):
        return "]" in host  # 方括号端点兜底：闭合即保留（r4 冻结契约，见 docstring）
    host = host.split(":", 1)[0]
    if not host:
        return False
    if _is_dns_name(host):
        return True
    return host.lower() in _BENIGN_SINGLE_LABEL_HOSTS


def _token_is_canonical(token: str) -> bool:
    """token 是否已是清洗器的**不动点形态**（幂等要求，r4 起；r5 增第三类）。

    三类形态允许「渲染成自身」：

    * ``scheme://authority`` + **已掩码** payload（``/***/***?***``）——这是
      :func:`_mask_policy_url` 的正常输出，二次清洗必须原样留下；
    * ``scheme://[…]:port`` **闭合方括号**端点形态、无 payload——这是
      :func:`_mask_bracketed_absolute_url` 的正常输出（``http://[web,hook]:8443``），
      同样必须是不动点，否则端点兜底的价值在一次清洗后就没了；
    * **无 payload / 只有结构 payload 的裸端点** ``scheme://host[:port]``
      （``http://example.com``、``http://host:8080``、``http://h``、
      ``http://example.com/``）——``#159`` r5 定验 FINDING 3：r4 把这一类也判成
      「渲染成自身却什么也没掩」⇒ 退化为常量，于是**无凭据**的 URL 连端点都丢了
      （``Failed to connect to http://example.com`` → ``Failed to connect to
      <unprintable-url>``）。渲染器的职责是掩**凭据**，不是擦掉 URL；没有 payload
      就意味着没有 path/query 可泄，端点本身保留即是正确渲染。

     **r6 收紧（FINDING 3 的过宽处）+ r7 换成属性判据**：r5 把「无 payload 的裸端点」
     整类放进不动点，于是 ``http://Ks9Qm4Zt``（host 位置就是密钥本体）也从 r4 的常量
     变成了原文——那是 r5 相对 r4 新开的暴露面。现在两条同时成立才算不动点：

     * payload 为**空或只剩结构分隔符**（``/``、``//``、``?``、``#``），或**只有
       fragment 被掩**——即没有掩掉任何 path/query 字节，不能作为「保留可疑
       authority」的理由；payload 的 path/query 里只要有 ``***``（真掩过）就仍是
       合法不动点；
     * authority 必须是**语法上可识别的宿主标识**（r7；见
       :func:`_host_is_identifiable` 里的属性与其代价）：≥2 标签的点分
       DNS 名 / 闭合的方括号端点 / 白名单里的本地单标签名。**与大小写无关**——r6 的
       「host 里有没有大写 ASCII」是代理，两个方向都错（全小写的不透明 token 被放行，
       合法的大写主机名被打成常量）。

    反之 ``http://[web``（``[`` **未闭合**、无 payload，r3 反例 A 的根因）仍**不是**
    不动点：凭据可能落在被截断的方括号里，且它的身份替换正是 r4 修掉的那个缺口。
    """
    payload = _original_url_payload(token)
    if payload:
        if not _payload_is_fully_masked(payload):
            return False
        # 只有 **path / query** 被掩过才算「有真掩码」：fragment 里的 ``***``
        # 不显示请求路径，不能成为保留可疑 authority 的理由
        # （``http://Ks9Qm4Zt/#frag`` → ``http://Ks9Qm4Zt/#***`` 必须 fail closed）。
        if _MASK_PLACEHOLDER in payload.partition("#")[0]:
            return True
        # 纯结构 payload（``/`` / ``?`` / ``#``）或只有 fragment 被掩：
        # 落到下面的 authority 判据（r6，r7 改成属性判据）。
    scheme, sep, rest = token.partition("://")
    if not sep or scheme.lower() not in _ENDPOINT_SCHEMES:
        return False
    authority = _authority_of(token).rpartition("@")[2]
    if not authority:
        return False
    # r7：方括号**不**在这里短路——交给 :func:`_host_is_identifiable` 一并判，
    # 它的方括号分支因此是**活代码**（r6 的同一分支是死代码：唯一的调用者在这里
    # 就先 `return "]" in authority` 了）。
    return _host_is_identifiable(authority)


def _leaves_plaintext_payload(token: str, rendered: str) -> bool:
    """渲染后的 payload 里是否还留着原 token payload 的明文片段（B 类尾部泄漏）。

    与「渲染结果必须是规范形态」（:func:`_token_is_canonical`）是**两个方向**的同一
    件事：规范形态要求 payload 只剩 ``***``，这里再按**原 token 的片段**逐个回查，
    直接回答「原始 path/query 字节有没有留在输出里」。两层都留着是刻意的
    defense-in-depth——``re.sub`` 只替换匹配片段，尾部残留正是 r2/r3 的 B 类。
    """
    rendered_payload = _original_url_payload(rendered)
    return any(
        piece in rendered_payload
        for piece in _plaintext_pieces(_original_url_payload(token))
    )


def _render_is_usable(token: str, rendered: str) -> bool:
    """渲染结果能否**替代**原 token（``#159`` r4 的「绝不恒等 / 绝不残留原文」总守卫）。

    判据逐条：

    1. 不是 ``str`` 或为空 ⇒ 不可用；
    2. 退化为 :data:`UNPRINTABLE_URL` ⇒ 不可用（由调用方决定是否去下一层兜底）；
    3. **渲染结果本身必须是规范形态**（:func:`_token_is_canonical`）——``scheme://``
       + 已掩码 payload、闭合方括号的端点形态，或（r5，r6 收紧）**无 payload /
      只有结构 payload 且 authority 不像承载凭据的裸端点**。
       这一条排掉两类「换了等于没换」：**恒等替换但什么都没掩**（``http://[web``
       方括号未闭合，r3 反例 A 的直接成因）与**掩码没盖住 payload**
       （``http://h/***?t=K`` 的 query 还是明文）。反向地，
       ``http://host/***/***?***`` / ``http://[web,hook]:8443`` / ``http://host``
       这类**已经掩码 / 已是端点形态**的串是规范形态，必须原样留下：否则
       ``_scrub_url_text`` 二次清洗会把它们变成常量，**幂等性**与端点诊断当场失效
       （FINDING 3：r4 把 ``http://host`` 也判成不可用是无凭据 URL 的过度拒绝）；
    4. 渲染后的 payload 里不许残留原 payload 的**明文片段**
       （:func:`_leaves_plaintext_payload`）——双保险，直接对着「原始 path/query
       字节留在输出里」这件事说话；
    5. （r5 加）token 的 authority 带 userinfo 时，渲染结果**不许**再带 ``@``——
       userinfo 是 ``#88`` 的口令形态，两个渲染器都刻意丢掉它
       （``http://user:pw@host`` → ``http://host``）。恒等替换的 userinfo
       （``usable("http://u:p@h", "http://u:p@h")``）因此判不可用。

    该守卫对**每一条**渲染路径施加（``_mask_policy_url`` 首选路径与方括号兜底
    路径），不只守兜底。
    """
    if not isinstance(rendered, str) or not rendered:
        return False
    if rendered == UNPRINTABLE_URL:
        return False
    if "@" in _authority_of(token) and "@" in _authority_of(rendered):
        return False
    if not _token_is_canonical(rendered):
        return False
    return not _leaves_plaintext_payload(token, rendered)


def _render_absolute_url_ref(token: str) -> str:
    """绝对 URL 匹配串 → 掩码形态：:func:`_mask_policy_url` 优先，退化时端点兜底。

    ``_mask_policy_url`` 是策略 URL 的唯一渲染路径（r2 契约），保留它作为**首选**；
    只有当它**不可用**（:func:`_render_is_usable` 为假：退化常量 / 恒等替换 / 尾部
    仍带明文 payload）时，才看 :func:`_mask_bracketed_absolute_url` 能否把端点救回来。
    两层都不可用即 :data:`UNPRINTABLE_URL`——绝不回原文、绝不恒等替换。

    #165：token 超过 :data:`_MAX_CLEAN_INPUT` 时**不**调用掩码器（掩码器对分隔符
    密集的长 token 是超二次的），直接退化常量；累计预算由 :func:`_scrub_url_text`
    在替换器里扣减。
    """
    if not _enforce_clean_budget(token, "absolute_url_ref"):
        return UNPRINTABLE_URL
    primary = _mask_policy_url(token)
    if _render_is_usable(token, primary):
        return primary
    fallback = _mask_bracketed_absolute_url(token)
    if _render_is_usable(token, fallback):
        return fallback
    return UNPRINTABLE_URL


def _scrub_url_text(text: object) -> str:
    """清洗**任意文本**里的 URL 凭据分量（``NodeException`` 消息构造前的最后一道）。

    ``requests`` / ``aiohttp`` 把请求 URL 放进自己的异常文本，
    ``handle_http_node_err`` 只是 ``str(err)`` 转手——URL 不是 plaita 拼的，但一样
    会随 ``NodeException`` / ``NodeExecutionError`` 进 runner/worker 的
    ``logger.warning(…, e)`` / ``exc_info=True``。所以清洗必须在**构造消息时**做
    （在下游 logger 处打补丁正是 r1 守卫的盲区形态）。

    * 绝对 URL 走 :func:`_render_absolute_url_ref`（首选 :func:`_mask_policy_url`：
      保 ``scheme://host[:port]``，抹 path 的每一段与整条 query/fragment；非 http(s)
      scheme 退化常量；方括号 authority 退化时由端点兜底把 ``host[:port]`` 救回）；
      token 边界**只认 ASCII 空白**，且 ``, ; ( ) " ' < > { }`` 一律留在 token 里
      （见 :data:`_ABSOLUTE_URL_RE`）；
    * ``url: /path?query`` 形态的 scheme-relative 引用按时段掩码；
    * **凭据键 = 值** 的裸串（``token=K…``、``…?access_token=K…``，无 scheme 也
      无 ``url:`` 标签）由 :func:`_mask_credential_runs` 掩值（引号在键这一侧、
      驼峰/粘连键名、JSON 引号值、转义引号 ``\"k\":\"v\"`` 一并覆盖，见
      :func:`_credential_run_spans` 上方的判据与线性证明）；
    * **url 端点**在「没有掩到任何字节」时的取舍见 :func:`_token_is_canonical`
      （判据是**属性**：authority 必须是语法上可识别的宿主标识——点分 DNS 名（含
      IPv4 点分四段）/ 白名单本地单标签 / 闭合方括号端点；否则 fail-closed。见
      :func:`_host_is_identifiable`）。

    **判据的诚实边界（r4 明确并钉住）**：本清洗器认的是「URL 形状 **或** 凭据键
    的 ``key=value`` 串」。因此：

    * ``token=K…`` / ``access_token=K…`` 这类**无 scheme 的凭据运行**现在**也**被
      掩掉（r3 的残口之一，r4 关闭）；
    * 键名不像凭据的裸 ``key=value``（``code=1002`` / ``port=8080`` /
      ``block_private_networks=true`` / ``v=1``）**刻意原样返回**——它们是诊断
      文本的主体，掩掉等于把「为什么失败」也一起删了；
    * 因此「裸的、键名不像凭据的串」仍在判据之外，这是**设计选择**而非遗漏，
      由 ``TestBareCredentialRunIsMasked::test_non_credential_key_value_runs_pass_through``
      钉住（该引用的可解析性由 ``TestDocstringReferencesResolve`` 钉住，防漂移）。

    **r6 声明、r7 仍有（逐条给可达性与理由）**：

    * **CJK / 非 ASCII 键名**（``密钥=K…``）不命中——词干表全是 ASCII 小写分量，
      且 :data:`_KEY_CHARS` 只含 ASCII 字母数字与 ``_.-``。现实可达性取决于上游
      是否用中文键名（plaita 自己的字段名全 ASCII）；
    * **``key=>value``**（PHP/数组字面量形状）：``>`` 在 :data:`_VALUE_STOP` 里，
      值在 ``>`` 处**立即结束** ⇒ 跨度为空、原文穿过；
    * **``client_id`` / ``access_key_id``** 是 OAuth/AWS 的**标识符**而非密钥，
      词干表里没有它们 ⇒ 刻意不掩（与 ``code`` / ``port`` 同类；评审已接受）；
    * **方括号 authority 里的凭据**（``http://[Ks9Qm4Zt]``、``http://[Ks9Qm4Zt]:8443``、
      ``http://[Ks9Qm4Zt]/x``）：保留。理由**不是**「没看出来」，而是**与合法端点同串
      类**——``http://[bad]``（``[`` + 单个合式 DNS 标签 + ``]``）的保留被既有测试钉住
      （``http://user:SECRET@[bad]:1/x?…`` → ``http://[bad]:1``），任何掩掉
      ``[Ks9Qm4Zt]`` 的规则都会一起掩掉 ``[bad]``。属于 :func:`_host_is_identifiable`
      第 1 条的**冻结契约代价**；
    * **点分的不透明 token 落在 host 位置**（``http://secret.example``、每个标签
      ≤63 字符的 JWT 形 host）：``_is_dns_name`` 只看标签文法 ⇒ 保留。与「冻结契约的
      host 残留」（``http://Ks9Qm4Zt/x`` → ``http://Ks9Qm4Zt/***``）同类；真实 JWT
      的单段长度通常远超 63 ⇒ 结构性地落在 fail-closed 一侧；
    * **单标签但不在白名单里的合法主机名**（``http://DESKTOP-ABC123``、``http://webhook``）
      退化为常量——:func:`_host_is_identifiable` 的取舍代价（宁 fail closed）。

    ``http://ks9qm4zt`` 这类**全小写的不透明单标签 host** 曾是 r6 的残口
    （判据只看大写字母），r7 已关闭：非白名单单标签一律 fail closed。

    ``{\"token\":\"K\"}``（Java / 双重编码文本里的**转义引号**）曾是残口，r6 关了键侧
    与值的开引号；**值内部的转义引号**（``token="pre\"K"``、``{\"token\":\"a\"bK\"}``）
    在 r4/r5/r6 **三版都漏**（掩码停在 ``\"`` 处、其后原文留在输出里），r7 关闭
    （:func:`_find_closing_quote` 单趟前向扫描，见该函数）。

    **total**（与 ``url_redaction._mask_webhook_url`` 同一判据）：任何输入都返回
    ``str``；清洗过程任何异常退化为 ``UNPRINTABLE_URL``，绝不上抛、绝不回原文；
    非字符串输入先 ``str()``（``_coerce`` 失败同样退化）；**结果必须 UTF-8 可编码**
    ——孤立代理字符（``surrogateescape`` 型输入）原样穿过会让下游任何 utf-8 日志
    handler 抛 ``UnicodeEncodeError``（`#159` r3 定验：旧实现 21 条敌对输入里 3 条
    穿过且不可编码），故同样退化为 ``UNPRINTABLE_URL``。
    """
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception:  # noqa: BLE001 — fail-closed
            logger.debug("异常文本无法字符串化，退化为安全常量")
            return UNPRINTABLE_URL
    try:
        # plaita#165：**累计**预算。单条 token 合规不蕴含总量合规——一条 80 KB 的
        # 第三方异常文本可以塞满 2 KB 的敌对 token，逐个渲染仍是 Θ(n²)。预算用尽
        # 之后的 token 一律退化常量（fail-closed：丢诊断，不丢安全，也不失有界）。
        budget = [_MAX_SCRUB_URL_CHARS]

        def _render_token(match):
            token = match.group(0)
            budget[0] -= len(token)
            if budget[0] < 0:
                _record_cap_trip("scrub_budget", len(text), _MAX_SCRUB_URL_CHARS)
                return UNPRINTABLE_URL
            return _render_absolute_url_ref(token)

        scrubbed = _ABSOLUTE_URL_RE.sub(_render_token, text)
        scrubbed = _RELATIVE_URL_RE.sub(
            lambda m: m.group("prefix") + _mask_relative_url_ref(m.group("ref")), scrubbed,
        )
        scrubbed = _mask_credential_runs(scrubbed)
    except Exception:  # noqa: BLE001 — 清洗失败不得变成新的异常面
        # 同样刻意不回显异常文本：这里正是「把原文抹掉」的那条路径。
        logger.debug("URL 文本清洗失败，退化为安全常量")
        return UNPRINTABLE_URL
    try:
        scrubbed.encode("utf-8")
    except UnicodeEncodeError:
        # 孤立代理字符（POSIX ``os.environ`` 的 surrogateescape / 第三方文本里的
        # 半个字符）：任何 utf-8 handler 都会在这里炸 ⇒ 退化常量，绝不原样放出。
        logger.debug("清洗结果无法编码为 UTF-8，退化为安全常量")
        return UNPRINTABLE_URL
    return scrubbed


# --------------------------------------------------------------------------- #
# plaita#166：**执行表示**里的凭据值（控制台 / 导出等读面）
# --------------------------------------------------------------------------- #
# 为什么需要这一层（与上面那一段的分工）：
#
# * ``_scrub_url_text`` / ``_scrub_exception_text`` 处理的是**一段文本**——日志出口；
# * ``$INPUT`` / checkpoint 是**结构化数据**：凭据既可能落在「键名就叫
#   ``api_key``/``password``」的格子里，也可能藏在某个字符串值的 URL 里。
#   本函数把结构化数据当结构走，两件事都用**上面同一套**判据与清洗器做，
#   不新起第二套文法（``#88`` 的根因形态正是「同一个 URL 两套渲染」）。

#: 凭据键的**整个值**被换成的占位——与日志面同一词汇（``***``）。
REDACTED_VALUE = "***"
#: 结构递归的深度上限：越界即把该子树整棵换成占位（fail-closed，绝不回明文）。
_REDACT_MAX_DEPTH = 64


def _redact_value(obj: object, depth: int) -> object:
    """:func:`redact_credential_values` 的单层实现（递归主体）。"""
    if depth > _REDACT_MAX_DEPTH:
        return REDACTED_VALUE
    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            if isinstance(key, str) and _is_credential_key(key):
                # 整棵子树换占位：``credentials`` / ``auth`` 这类键下面挂什么，
                # 读面都不再展示（键名保留，看得出抹的是哪一个）。
                out[key] = REDACTED_VALUE
            else:
                out[key] = _redact_value(value, depth + 1)
        return out
    if isinstance(obj, (list, tuple)):
        items = [_redact_value(item, depth + 1) for item in obj]
        return tuple(items) if isinstance(obj, tuple) else items
    if isinstance(obj, str):
        return _scrub_url_text(obj)
    return obj


def redact_credential_values(obj: object) -> object:
    """执行表示（读面副本）里的凭据**值**换占位——``plaita#166``。

    为什么是「值」而不是「键」：``$INPUT`` 是**功能数据**（flow 要用它），改键名
    会改语义与形状（调用方按名取值）；键名保留则「看得出被抹的是哪个参数」。
    功能副本（队列载体、worker 侧 ``$INPUT``、盘上 checkpoint）**一字未动**——
    本函数只作用于读面**构造出来的副本**（见调用方：不传盘上对象本体）。

    判据与日志面同源，不新起第二套：

    * ``dict``：键名命中 :func:`_is_credential_key` ⇒ 整棵子树换
      :data:`REDACTED_VALUE`；否则递归；
    * ``list`` / ``tuple``：逐项递归（类型保持）；
    * ``str``：过 :func:`_scrub_url_text`——URL userinfo 口令、path/query 凭据、
      ``凭据键=值`` 的裸运行都在这一趟里；
    * 其余标量（``int`` / ``bool`` / ``None`` …）：原样返回。

    **不改入参**：返回新对象；盘上/内存里的功能副本是调用方的，本函数既不持有
    也不写它（这条由 ``tests/console`` 的 ``test_redaction_does_not_mutate_its_input``
    与载体形状用例钉住）。

    **total**（与 ``_mask_webhook_url`` 同一判据）：任何输入都不抛——递归越界、
    非预期类型、清洗异常一律退化为占位，**绝不**退回明文，也绝不上抛到读面。
    """
    try:
        return _redact_value(obj, 0)
    except Exception:  # noqa: BLE001 — fail-closed：脱敏失败不得炸读面，更不能回明文
        # 刻意不记 exc_info：异常文本可能带着刚被抹掉的明文（同 ``_safe_render``）。
        logger.warning("执行表示脱敏失败，整棵表示退化为占位")
        return REDACTED_VALUE


def _scrub_exception_text(exc: object) -> str:
    """第三方异常对象 → 进日志的**单份文本**（``#161`` 的唯一构造面）。

    ``logger.error("…: %s", e, exc_info=True)`` 形态**无法**在构造面清洗干净：
    ``record.msg``/``args``/``getMessage()`` 只是 ``str(e)`` 的那一行，但
    ``Formatter`` 还会用 ``exc_info`` **重新渲染**一遍 traceback（
    ``traceback.format_exception`` 取 ``str(exc)`` 作为末行）——那一步发生在
    logger 调用**之后**，任何「在调用点传干净参数」的做法都盖不住它，除非
    把 ``exc_info`` 一并去掉。故这里在构造面把 ``str(exc)`` **与整条 traceback**
    一起渲染并清洗，调用方以纯文本参数记录（不再传 ``exc_info``）：诊断价值
    （栈帧、异常链）保留，URL 凭据分量按 :func:`_scrub_url_text` 抹掉。

    ``#158`` r2 的纪律同源：**在消息构造处清洗**，而不是事后给 logger 打补丁
    （下游 logger 补丁对「异常对象已经带着明文」结构性失明）。

    **total**：``traceback`` 渲染失败退回 ``str(exc)``；两者都经
    :func:`_scrub_url_text`，故结果必为 ``str``、必可 UTF-8 编码、必不含原样凭据。
    """
    try:
        text = "".join(
            traceback.format_exception(type(exc), exc, getattr(exc, "__traceback__", None))
        )
    except Exception:  # noqa: BLE001 — 渲染失败退回 str()，绝不因此丢日志
        # 刻意不回显异常文本（可能带原文片段），只记「退化」这一事实。
        logger.debug("异常 traceback 渲染失败，退回异常文本")
        text = exc
    return _scrub_url_text(text)
