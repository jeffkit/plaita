import asyncio
import ipaddress
import json
import logging
import os
import re
import socket
import threading
import time
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse, urlsplit, urlencode
import io
from pydantic import ConfigDict, Field, model_validator

from plaita.node.basic import Node
from plaita.core.errors import NodeException
# 策略异常消息的掩码渲染（#158+#159 r2）。url_redaction 只依赖 stdlib 的
# server.factory，不触发 server extra，也不与 node 层成环。
from plaita.server.url_redaction import (
    UNPRINTABLE_URL,
    _mask_url_for_log,
    _mask_webhook_url_for_log,
)
from plaita.core.http_session import (
    _BlockAllCookies,
    _new_oneshot_session,
    _sync_cookies_enabled,
    _sync_session_cls,
    get_flow_session,
    get_shared_sync_session,
)
from plaita.io import evaluate

try:
    import requests
except ImportError:
    requests = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

# ---------------------------------------------------------------------------
# JSON 编解码后端（2026-10 wave2）：orjson 可用时大 payload 序列化/反序列化快
# 5~10x（120KB 实测 stdlib ~375µs → orjson ~60µs），缺失时回退标准库。
# 安装：pip install plaita[fast]
# ---------------------------------------------------------------------------
try:
    import orjson

    def _json_dumps_bytes(obj) -> bytes:
        # orjson 原生支持 datetime/dataclass 序列化（stdlib 会 TypeError——
        # 此为文档化改进：原先因不可序列化而失败的请求体现在能发出去）；
        # 非法类型同样抛 TypeError，语义边界不变。
        return orjson.dumps(obj)

    _JSON_LOADS = orjson.loads
except ImportError:  # pragma: no cover - fast extra 未装
    orjson = None

    def _json_dumps_bytes(obj) -> bytes:
        return json.dumps(obj).encode("utf-8")

    _JSON_LOADS = json.loads


def _loads_lenient(text: str):
    """JSON 反序列化：orjson 优先，NaN/Infinity 等宽松语法回退 stdlib。

    orjson 按 RFC 严格拒绝 NaN/Infinity（stdlib 默认接受）；两层都失败时
    由调用方回退到原始文本（历史行为）。
    """
    try:
        return _JSON_LOADS(text)
    except Exception:
        return json.loads(text)  # noqa: TRY300 - 宽松兼容层，失败由调用方处理


def _require_http():
    """Raise ImportError with actionable message if HTTP dependencies are missing."""
    missing = []
    if requests is None:
        missing.append("requests")
    if aiohttp is None:
        missing.append("aiohttp")
    if missing:
        raise ImportError(
            f"HTTP dependencies not installed: {', '.join(missing)}. "
            "Install them with: pip install plaita[http]"
        )


# HTTP节点错误代码
HTTP_GEN_REQUEST_ERROR = 1001  # 请求生成错误
HTTP_DO_REQUEST_ERROR = 1002   # 发送请求错误
HTTP_NODE_EXEC_ERROR = 1003    # 节点处理逻辑错误

# 响应上下文键
RESPONSE_CTX_KEY = "RESPONSE"
HEADER_CTX_KEY = "HEADERS"
STATUS_CTX_KEY = "STATUS"

# 响应内容键
RESPONSE_DATA_KEY = "data"
RESPONSE_STATUS_KEY = "status"
RESPONSE_STATUS_TEXT_KEY = "statusText"
RESPONSE_HEADERS_KEY = "headers"


class RawAddressing(Dict):
    """HTTP节点寻址配置（支持表达式）"""
    pass


class RawDelegateParam(Dict):
    """HTTP节点代理参数（支持表达式）"""
    pass


class DelegateParam:
    """HTTP节点代理参数"""
    def __init__(self, name: str = "", params: Optional[bytes] = None):
        self.name = name
        self.params = params

    def empty(self) -> bool:
        """检查代理参数是否为空"""
        return not self.name


class Addressing:
    """寻址配置"""
    def __init__(self, name: str = "", params: Optional[bytes] = None):
        self.name = name
        self.params = params


class HttpResponse:
    """HTTP响应"""
    def __init__(self, raw_request=None, raw_response=None, res=None):
        self.raw_request = raw_request
        self.raw_response = raw_response
        self.res = res

    def empty(self) -> bool:
        """检查响应是否为空"""
        return self is None

    def send_request_fail(self) -> bool:
        """检查请求发送是否失败"""
        return not self.empty() and self.raw_request is not None and self.raw_response is None


class HttpRequestInfo:
    """async 路径的请求快照（与 ``requests.PreparedRequest`` 的 method/url/
    headers/body 字段名对齐的最小载体）。

    C2-1：历史上 ``_send_async`` 的错误分支返回 ``HttpResponse()``——raw_request
    为空，连接类错误被 ``handle_http_node_err`` 归为 1003（节点处理错误），
    而同步同错误归 1002（发送请求错误）。补上请求快照让两条路径分类一致，
    同时错误帧里能带出「请求是什么」（method/url/headers）。
    """

    def __init__(self, method: str, url: str, headers: Optional[Dict[str, str]] = None,
                 body: Optional[bytes] = None):
        self.method = method
        self.url = url
        self.headers = dict(headers) if headers else {}
        self.body = body


class HttpNodeErrorInfo:
    """HTTP节点错误信息"""
    def __init__(self, code: int, message: str, request=None, response=None):
        self.code = code
        self.message = message
        self.request = request
        self.response = response


class HttpNodeResponse:
    """HTTP节点响应"""
    def __init__(self, status: int, status_text: str, headers: Dict, data: Any):
        self.status = status
        self.status_text = status_text
        self.headers = headers
        self.data = data


# ---------------------------------------------------------------------------
# URL 访问策略（2026-09 安全评审 P1-1：SSRF 防护）
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# block_private_networks=True 时拒绝解析到这些网段的目标（回环/内网/链路本地/
# 运营商 NAT/基准测试段 + IPv6 对应段）。
_PRIVATE_NETWORKS = [
    ipaddress.ip_network(n)
    for n in (
        "0.0.0.0/8", "10.0.0.0/8", "127.0.0.0/8", "169.254.0.0/16",
        "172.16.0.0/12", "192.0.0.0/24", "192.168.0.0/16", "100.64.0.0/10",
        "198.18.0.0/15", "::1/128", "fc00::/7", "fe80::/10",
    )
]


class URLPolicyError(Exception):
    """请求 URL 违反节点的 allowedHosts/deniedHosts/blockPrivateNetworks 策略。"""


class ResponseTooLargeError(Exception):
    """响应体超过 ``maxResponseBytes`` 上限（防内网端点拖回 GB 级响应撑爆 worker）。"""


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

#: **凭据键 = 值** 的裸串（``#159`` r4）。r3 的清洗器只认「URL 形状」，于是
#: ``token=K…``、``"x"*100000 + "?access_token=K…"`` 这类**无 scheme、无
#: ``url:``/``url=`` 标签**的串整段原样穿过（定验残口）。这里补一条**收窄到
#: 凭据键**的规则：键名像凭据才掩值；键名不像的一律不碰，否则会把
#: ``code=1002`` / ``port=8080`` / ``block_private_networks=true`` 这类诊断文本
#: 一起吃掉（那是不可接受的可用性代价）。
#:
#: 键名用**分段**文法而不是「``[\\w.\\-]+`` + 事后判据」：``\\w+`` 会让
#: ``failed: password=X`` 里最左匹配落在 ``failed`` 上、把真凭据键 ``password``
#: 吃进 value，``re.sub`` 随后**跳过整个匹配** ⇒ 凭据隐身（r4 实测）。
#: 分段文法（每段必须由 ``_ - .`` 收尾）让最左匹配**只能是**凭据键本身，
#: 且 ``monkey`` / ``sortkey`` 这类「字母串里恰好含 key」不命中。
_KEY_COMPONENT = r"[A-Za-z0-9]+"
_KEY_SEP = r"[_\-.]"
#: 整段命中的凭据词（配合 ``re.I``：``accessToken`` / ``APIKEY`` 同样命中的）。
_CREDENTIAL_KEY_WORDS = (
    r"token|secret|password|passwd|pwd|signature|credential|apikey|accesskey|"
    r"privatekey|clientsecret|session|cookie|bearer|key|sig|auth"
)
_CREDENTIAL_RUN_RE = re.compile(
    r"(?P<prefix>(?<![A-Za-z0-9_.\-])"
    r"(?P<key>(?:" + _KEY_COMPONENT + _KEY_SEP + r")*"
    r"(?:" + _CREDENTIAL_KEY_WORDS + r")"
    r"(?:" + _KEY_SEP + _KEY_COMPONENT + r")*)"
    r"\s*[=:]\s*[\"']?)"
    r"(?P<value>[^ \t\r\n\f\v&\"'<>]+)",
    re.IGNORECASE,
)
#: 与 :data:`_CREDENTIAL_RUN_RE` **同一份词表**的独立键名判据（双保险：万一
#: 上游正则被改松，掩码函数仍会拒掉不像凭据的键）。词的两侧必须是分量边界，
#: 所以 ``monkey`` / ``sortkey`` 不命中，``x-api-key`` / ``access_token`` 命中。
_CREDENTIAL_KEY_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])(?:" + _CREDENTIAL_KEY_WORDS + r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)


def _is_credential_key(key: str) -> bool:
    """``access_token`` / ``x-api-key`` / ``client_secret`` 命中；``code`` / ``monkey`` 不命中。"""
    return _CREDENTIAL_KEY_RE.search(key) is not None


def _mask_credential_run(match) -> str:
    """凭据键的**值**换占位，键名与引号原样（看得出抹掉的是哪个键）。"""
    if not _is_credential_key(match.group("key")):
        return match.group(0)
    return match.group("prefix") + _MASK_PLACEHOLDER


def _mask_relative_url_ref(ref: str) -> str:
    """scheme-relative 引用（``/a/b?token=x``）：每个非空 segment 与整条 query 换 ``***``。

    与绝对 URL 同一条「绝不恒等替换」守卫（r4）：渲染结果与入参**恒等**时，只有
    已经是掩码形态（``/***/***?***``）才算合法的幂等不动点，否则退化为
    :data:`UNPRINTABLE_URL`。
    """
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


def _token_is_canonical(token: str) -> bool:
    """token 是否已是清洗器的**不动点形态**（幂等要求，r4）。

    只有两种形态允许「渲染成自身」：

    * ``scheme://authority`` + **已掩码** payload（``/***/***?***``）——这是
      :func:`_mask_policy_url` 的正常输出，二次清洗必须原样留下；
    * ``scheme://[…]:port`` **闭合方括号**端点形态、无 payload——这是
      :func:`_mask_bracketed_absolute_url` 的正常输出（``http://[web,hook]:8443``），
      同样必须是不动点，否则端点兜底的价值在一次清洗后就没了。

    反之 ``http://[web``（``[`` **未闭合**、无 payload）与 ``http://host``（无
    payload）都不是不动点形态——它们正是「渲染成自身却什么也没掩」的形态，
    :func:`_render_is_usable` 据此判不可用、退化为常量（r3 反例 A 的根因）。
    """
    payload = _original_url_payload(token)
    if payload:
        return _payload_is_fully_masked(payload)
    scheme, sep, rest = token.partition("://")
    if not sep or scheme.lower() not in _ENDPOINT_SCHEMES:
        return False
    authority = rest.rpartition("@")[2]
    return authority.startswith("[") and "]" in authority


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
       + 已掩码 payload，或闭合方括号的端点形态。这一条同时排掉两类「换了等于
       没换」：**恒等替换**（``http://[web`` 方括号未闭合、``http://host`` 无
       payload，都不是规范形态；前者正是 r3 反例 A 的直接成因）与**掩码没盖住
       payload**（``http://h/***?t=K`` 的 query 还是明文）。反向地，
       ``http://host/***/***?***`` / ``http://[web,hook]:8443`` 这类**已经掩码 /
       已是端点形态**的串是规范形态，必须原样留下：否则 ``_scrub_url_text`` 二次
       清洗会把它们变成常量，**幂等性**与端点诊断当场失效；
    4. 渲染后的 payload 里不许残留原 payload 的**明文片段**
       （:func:`_leaves_plaintext_payload`）——双保险，直接对着「原始 path/query
       字节留在输出里」这件事说话。

    该守卫对**每一条**渲染路径施加（``_mask_policy_url`` 首选路径与方括号兜底
    路径），不只守兜底：``_mask_policy_url`` 在 ``http://host``（无
    path/query/fragment）这类 token 上同样返回输入本身。
    """
    if not isinstance(rendered, str) or not rendered:
        return False
    if rendered == UNPRINTABLE_URL:
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
    """
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
      无 ``url:`` 标签）由 :data:`_CREDENTIAL_RUN_RE` 掩值。

    **判据的诚实边界（r4 明确并钉住）**：本清洗器认的是「URL 形状 **或** 凭据键
    的 ``key=value`` 串」。因此：

    * ``token=K…`` / ``access_token=K…`` 这类**无 scheme 的凭据运行**现在**也**被
      掩掉（r3 的残口之一，r4 关闭）；
    * 键名不像凭据的裸 ``key=value``（``code=1002`` / ``port=8080`` /
      ``block_private_networks=true`` / ``v=1``）**刻意原样返回**——它们是诊断
      文本的主体，掩掉等于把「为什么失败」也一起删了；
    * 因此「裸的、键名不像凭据的串」仍在判据之外，这是**设计选择**而非遗漏，
      由 ``TestCredentialRunIsMasked::test_non_credential_key_value_runs_pass_through``
      钉住。

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
        scrubbed = _ABSOLUTE_URL_RE.sub(
            lambda m: _render_absolute_url_ref(m.group(0)), text,
        )
        scrubbed = _RELATIVE_URL_RE.sub(
            lambda m: m.group("prefix") + _mask_relative_url_ref(m.group("ref")), scrubbed,
        )
        scrubbed = _CREDENTIAL_RUN_RE.sub(_mask_credential_run, scrubbed)
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


# ---------------------------------------------------------------------------
# 运营者级 HTTP 策略（2026-10 安全评审 P1，plaita#31）
# ---------------------------------------------------------------------------
# 节点级 blockPrivateNetworks 默认 False（与历史行为一致）；多租户部署需要一道
# **运营者级**硬门——与 code 沙箱后端白名单同构：env 置位后节点声明只能更严、
# 不能更松（effective = 节点声明 OR 运营者开关）。节点在请求期直接读 env，
# 部署入口无需再注入调用点。
HTTP_BLOCK_PRIVATE_ENV = "PLAITA_HTTP_BLOCK_PRIVATE"

# 响应体字节上限。历史实现 ``response.text`` 全量读入内存：内网高速端点 30s 内
# 可拖回 GB 级响应，且会进 ``$NODE.<id>.RESPONSE`` 状态与 checkpoint。默认
# 10MiB；需要更大 body 的部署显式调大。
HTTP_MAX_RESPONSE_BYTES_ENV = "PLAITA_HTTP_MAX_RESPONSE_BYTES"
DEFAULT_MAX_RESPONSE_BYTES = 10 * 1024 * 1024


def operator_block_private() -> bool:
    """运营者是否强制屏蔽私网目标（``PLAITA_HTTP_BLOCK_PRIVATE=1``）。"""
    return os.environ.get(HTTP_BLOCK_PRIVATE_ENV, "").strip().lower() in ("1", "true", "yes")


def http_max_response_bytes() -> int:
    """响应体字节上限（``PLAITA_HTTP_MAX_RESPONSE_BYTES``，默认 10MiB）。"""
    raw = os.environ.get(HTTP_MAX_RESPONSE_BYTES_ENV, "").strip()
    if not raw:
        return DEFAULT_MAX_RESPONSE_BYTES
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_MAX_RESPONSE_BYTES
    return value if value > 0 else DEFAULT_MAX_RESPONSE_BYTES


# ---------------------------------------------------------------------------
# 策略校验用的 DNS 解析缓存（2026-10 BFF 热路径评审）
# ---------------------------------------------------------------------------
# 只服务 _check_policy（SSRF 判定）；实际连接的 DNS 由 HTTP 客户端连接池自己
# 解析/缓存（aiohttp 默认 ttl_dns_cache=10s）。TTL 缺省 30s（可配
# PLAITA_HTTP_DNS_TTL=0 关闭）；**不缓存解析失败**——把瞬时 DNS 抖动缓存成
# 周期性全拒是把 fail-open 放大成 fail-closed。

_DNS_CACHE: Dict[Tuple[str, Optional[int]], Tuple[float, List[str]]] = {}
_DNS_CACHE_LOCK = threading.Lock()


def _dns_ttl_secs() -> float:
    raw = os.environ.get("PLAITA_HTTP_DNS_TTL", "")
    try:
        return max(0.0, float(raw)) if raw else 30.0
    except ValueError:
        return 30.0


def _resolve_host(hostname: str, port: Optional[int]) -> Optional[List[str]]:
    """getaddrinfo 的一次性包装：返回去重排序的地址列表，gaierror 返回 None。"""
    try:
        infos = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
        return sorted({info[4][0] for info in infos})
    except socket.gaierror:
        return None


def _cached_resolve_host(hostname: str, port: Optional[int]) -> Optional[List[str]]:
    ttl = _dns_ttl_secs()
    if ttl <= 0:
        return _resolve_host(hostname, port)
    key = (hostname, port)
    now = time.monotonic()
    with _DNS_CACHE_LOCK:
        hit = _DNS_CACHE.get(key)
        if hit is not None and now - hit[0] < ttl:
            return hit[1]
    ips = _resolve_host(hostname, port)
    if ips is not None:
        with _DNS_CACHE_LOCK:
            _DNS_CACHE[key] = (now, ips)
    return ips


def clear_dns_cache() -> None:
    """清空策略校验 DNS 缓存（DNS 漂移后可显式调用；测试复位用）。"""
    with _DNS_CACHE_LOCK:
        _DNS_CACHE.clear()


def _ip_matches_networks(ip: str, networks) -> bool:
    addr = ipaddress.ip_address(ip)
    if isinstance(networks, list) and networks and isinstance(networks[0], str):
        networks = [ipaddress.ip_network(n, strict=False) for n in networks]
    for net in networks:
        if addr.version == net.version and addr in net:
            return True
        # IPv4-mapped IPv6 归一后比对
        if addr.version == 6 and addr.ipv4_mapped is not None:
            if addr.ipv4_mapped in net:
                return True
    return False


def _host_entry_matches(entry: str, host: str, resolved_ips: Optional[list]) -> bool:
    """单个 allowed/denied 条目是否命中 host（精确 / ``*.suffix`` / CIDR）。"""
    entry = entry.strip().lower()
    if not entry:
        return False
    if "/" in entry:  # CIDR
        try:
            return resolved_ips is not None and any(
                _ip_matches_networks(ip, [entry]) for ip in resolved_ips
            )
        except ValueError:
            return False
    if entry.startswith("*."):
        return host == entry[2:] or host.endswith("." + entry[2:])
    return host == entry


def _validate_host_target(
    hostname: str,
    resolved_ips: Optional[list],
    allowed_hosts: Optional[List[str]],
    denied_hosts: Optional[List[str]],
    block_private_networks: bool,
    url: str,
) -> None:
    """按节点策略校验 host + 已解析地址，违反即抛 :class:`URLPolicyError`。

    :func:`_host_allowed`（请求前校验）与连接期策略（DNS pinning，
    :class:`_PolicyResolver` / ``_PinnedHTTPAdapter``）共用这一份判定——
    校验与建连同源，攻击者权威 DNS 无法先答公网过校验、连接时答内网穿透。

    ``url`` 只用于**诊断后缀**，且一律经 :func:`_mask_policy_url` 渲染后才进
    异常消息：该消息随后会被 ``str(err)`` 包进 ``NodeException`` /
    ``NodeExecutionError`` 并由 runner/worker 的 ``logger.*(…, e)`` 打印
    （``#158``+``#159`` r2 的 finding）。连接期调用点传 ``//host:port`` 这类
    无凭据的合成端点，渲染为 ``UNPRINTABLE_URL`` 也不丢 host（``hostname``
    本来就在消息里）。
    """
    if denied_hosts:
        for entry in denied_hosts:
            if _host_entry_matches(entry, hostname, resolved_ips):
                raise URLPolicyError(
                    f"URL host {hostname!r} matches denied_hosts entry {entry!r}: "
                    f"{_mask_policy_url(url)}"
                )

    if allowed_hosts:
        if not any(_host_entry_matches(entry, hostname, resolved_ips) for entry in allowed_hosts):
            raise URLPolicyError(
                f"URL host {hostname!r} is not in allowed_hosts {list(allowed_hosts)}: "
                f"{_mask_policy_url(url)}"
            )

    if block_private_networks:
        if resolved_ips is None:
            raise URLPolicyError(
                f"Cannot resolve {hostname!r} to verify block_private_networks policy: "
                f"{_mask_policy_url(url)}"
            )
        for ip in resolved_ips:
            if _ip_matches_networks(ip, _PRIVATE_NETWORKS):
                raise URLPolicyError(
                    f"URL host {hostname!r} resolves to private/special address {ip} "
                    f"(block_private_networks=true): {_mask_policy_url(url)}"
                )


def _host_allowed(
    url: str,
    allowed_hosts: Optional[List[str]],
    denied_hosts: Optional[List[str]],
    block_private_networks: bool,
    resolver=None,
) -> None:
    """校验请求 URL 是否满足节点策略，违反即抛 :class:`URLPolicyError`。

    规则按顺序：scheme 必须是 http/https；denied_hosts 命中即拒（支持精确
    域名、``*.suffix`` 通配、CIDR）；allowed_hosts 非空时必须命中其一；
    block_private_networks=True 时解析目标并对每个地址拒绝私网段（含 DNS
    解析——只看字面 host 挡不住域名解析到内网的绕过）。

    ``resolver(hostname, port) -> Optional[List[str]]`` 可注入替代 DNS 解析
    （生产路径传 TTL 缓存版 :func:`_cached_resolve_host`）；缺省保持本函数
    为纯函数——单测直接调用/monkeypatch ``socket.getaddrinfo`` 不受缓存污染。
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        # scheme 单独回显（jeffkit 要的诊断信息）；URL 本体经掩码渲染——它随后会
        # 随 NodeException/NodeExecutionError 进 runner/worker 的日志。
        raise URLPolicyError(
            f"URL scheme {parsed.scheme!r} is not allowed (http/https only): "
            f"{_mask_policy_url(url)!r}"
        )
    hostname = parsed.hostname
    if not hostname:
        raise URLPolicyError(f"URL has no hostname: {_mask_policy_url(url)!r}")
    hostname = hostname.lower().strip("[]")

    resolved_ips: Optional[list] = None
    if denied_hosts or block_private_networks:
        resolved_ips = (resolver or _resolve_host)(
            hostname, parsed.port or (443 if parsed.scheme == "https" else 80),
        )

    _validate_host_target(hostname, resolved_ips, allowed_hosts, denied_hosts,
                          block_private_networks, url)


# 凭据类请求头：跨源重定向时必须剥离（2026-10 评审 C1：restricted 逐跳手动
# 跟随原先原样透传全部头，Authorization/Cookie 会泄漏给重定向目标）。
# 「跨源」判定对齐 requests.Session.should_strip_auth：host 变化，或同 host 的
# scheme / port 变化（https→http、:443→:8443 都算——旧实现只比 hostname，
# 会把 Authorization 明文转发到另一个 origin）；唯一例外是同 host 的
# http:80 → https:443（requests 的历史兼容行为，且是升级不是降级）。
_SENSITIVE_REDIRECT_HEADERS = frozenset({
    "authorization", "cookie", "cookie2",
    "proxy-authorization", "proxy-authenticate", "www-authenticate",
})

_DEFAULT_SCHEME_PORTS = {"http": 80, "https": 443}


def _redirect_changes_origin(from_url: str, to_url: str) -> bool:
    """重定向是否跨源（判定对齐 ``requests.Session.should_strip_auth``）。"""
    old = urlparse(from_url)
    new = urlparse(to_url)
    if old.hostname != new.hostname:
        return True
    if (old.scheme == "http" and old.port in (80, None)
            and new.scheme == "https" and new.port in (443, None)):
        return False
    changed_port = old.port != new.port
    changed_scheme = old.scheme != new.scheme
    default_port = (_DEFAULT_SCHEME_PORTS.get(old.scheme), None)
    if not changed_scheme and old.port in default_port and new.port in default_port:
        return False
    return changed_port or changed_scheme


def _strip_sensitive_headers(headers: Dict[str, str], from_url: str, to_url: str) -> Dict[str, str]:
    """重定向跨源（host / scheme / port 任一变化）时剥离凭据类头；同源原样返回。"""
    if not _redirect_changes_origin(from_url, to_url):
        return headers
    return {
        k: v for k, v in headers.items()
        if k.lower() not in _SENSITIVE_REDIRECT_HEADERS
    }


def _redirect_method(status_code: int, method: str) -> str:
    """重定向后的方法（判定对齐 ``requests.Session.rebuild_method``）。

    303/302：非 HEAD 转 GET；301：**仅** POST 转 GET——PUT/PATCH/DELETE/OPTIONS
    保留方法（旧实现对全部 301 都转 GET，偏离 requests）；307/308 保留。
    """
    upper = method.upper()
    if status_code == 303 and upper != "HEAD":
        return "GET"
    if status_code == 302 and upper != "HEAD":
        return "GET"
    if status_code == 301 and upper == "POST":
        return "GET"
    return method


def _redirect_request(response, request, next_url: str):
    """构造重定向下一跳的 ``PreparedRequest``（语义对齐 requests.resolve_redirects）。

    方法规则见 :func:`_redirect_method`；307/308 保留 body，其余（301/302/303）
    丢弃 body 与 ``content-type``/``transfer-encoding``（requests 的
    ``purged_headers``）。``host``/``content-length`` 恒剥离（由 prepare() 重算），
    跨源再剥离凭据头。
    """
    method = _redirect_method(response.status_code, request.method)
    body = request.body
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }
    if response.status_code not in (307, 308):
        body = None
        headers = {
            k: v for k, v in headers.items()
            if k.lower() not in ("content-type", "transfer-encoding")
        }
    headers = _strip_sensitive_headers(headers, response.url or request.url, next_url)
    return requests.Request(
        method=method, url=next_url, headers=headers, data=body,
    ).prepare()


# ---------------------------------------------------------------------------
# 错误帧摘要（C2-1）：HttpNodeErrorInfo 上的 status/headers/body 要进事件流，
# body 按长度截断（思路同 obs._clip 的防大 payload，错误帧取更小量级）、
# 凭据类头脱敏（复用重定向剥离集，响应侧补 set-cookie）。
# ---------------------------------------------------------------------------
_ERROR_FRAME_BODY_MAX_CHARS = 2048

_SENSITIVE_ERROR_FRAME_HEADERS = _SENSITIVE_REDIRECT_HEADERS | frozenset({"set-cookie"})


def _mask_error_frame_headers(headers: Any) -> Dict[str, Any]:
    """凭据类响应头值替换为 '***'，其余原样保留。"""
    if not isinstance(headers, dict):
        return {}
    return {
        k: ("***" if str(k).lower() in _SENSITIVE_ERROR_FRAME_HEADERS else v)
        for k, v in headers.items()
    }


def _summarize_error_frame_body(data: Any) -> Optional[str]:
    """任意响应体 → 截断后的字符串摘要（JSON 安全，可直接进事件载荷）。"""
    if data is None:
        return None
    if not isinstance(data, str):
        try:
            data = _json_dumps_bytes(data).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001 - 摘要失败不掩盖原始错误
            data = str(data)
    if len(data) > _ERROR_FRAME_BODY_MAX_CHARS:
        return data[:_ERROR_FRAME_BODY_MAX_CHARS] + f"…[{len(data)} chars]"
    return data


def _summarize_error_frame_response(response: Optional["HttpNodeResponse"]) -> Optional[Dict[str, Any]]:
    """HttpNodeResponse → 事件安全的 dict 摘要（status/statusText/headers/body）。"""
    if response is None:
        return None
    return {
        "status": response.status,
        "statusText": response.status_text,
        "headers": _mask_error_frame_headers(response.headers),
        "body": _summarize_error_frame_body(response.data),
    }


# ---------------------------------------------------------------------------
# 连接期 DNS pinning（2026-10 安全评审 P1，plaita#31）
# ---------------------------------------------------------------------------
# 校验与建连两次独立解析是 DNS rebinding TOCTOU 的根因：历史上 _check_policy
# 解析一次、HTTP 客户端连接池再解析一次，攻击者权威 DNS 先答公网 IP 过校验、
# 连接时改答 127.0.0.1/169.254.169.254 即穿透。下面两条路径把**用于建连的
# 解析**本身交给同一份策略判定，并只连通过校验的地址——校验与连接同源。
#
# 策略激活时不再复用共享连接池（pinning 是逐 host 的建连行为，且共享池里的
# 连接可能来自不同策略的请求）；属安全换性能的取舍。

if aiohttp is not None:
    from aiohttp.abc import AbstractResolver

    class _PolicyResolver(AbstractResolver):
        """aiohttp 建连解析器：解析 + 策略校验 + 只返回校验过的地址。

        connector 实际连到本 resolver 返回的地址，恶意域名无法在「校验解析」
        与「建连解析」之间切换答案。策略异常（:class:`URLPolicyError`）直接
        抛出，连接不会建立。
        """

        def __init__(self, executor: "HttpExecutor"):
            self._executor = executor

        async def resolve(self, host: str, port: int = 0,
                          family: int = socket.AF_INET) -> list:
            loop = asyncio.get_running_loop()
            ips = await loop.run_in_executor(
                None, self._executor._resolve_and_validate, host, port or 0,
            )
            results = []
            for ip in ips:
                fam = socket.AF_INET6 if ":" in ip else socket.AF_INET
                if family not in (socket.AF_UNSPEC, fam):
                    continue
                results.append({
                    "hostname": host, "host": ip, "port": port or 0,
                    "family": fam, "proto": socket.IPPROTO_TCP, "flags": 0,
                })
            if not results:
                raise OSError(f"no usable address for {host!r}")
            return results

        async def close(self) -> None:
            return None
else:  # pragma: no cover - http extra 未装
    _PolicyResolver = None


_PINNED_ADAPTER_CLS = None


def _pinned_adapter_cls():
    """惰性构建（并缓存）pinning 版 ``requests`` adapter 类。

    urllib3 的 ``HTTPConnection.host`` 同时用于 Host 头 / TLS SNI（socket 目标
    由 ``_dns_host`` 派生）；因此只在 ``_new_conn`` 建立 socket 的瞬间临时把
    ``host`` 切成策略校验过的 IP，建连后立即还原——连接对象对上层仍是域名，
    pinning 只作用于 socket 目标（``host`` 在 urllib3 1.26+/2.x 是写 ``_dns_host``
    的属性，1.25 是普通属性，两种布局都成立）。
    """
    global _PINNED_ADAPTER_CLS
    if _PINNED_ADAPTER_CLS is not None:
        return _PINNED_ADAPTER_CLS

    from requests.adapters import DEFAULT_POOLBLOCK, HTTPAdapter
    from urllib3 import PoolManager
    from urllib3.connection import HTTPConnection, HTTPSConnection
    from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
    from urllib3.exceptions import HTTPError

    class _PinnedConnectionMixin:
        def __init__(self, *args, _pinned_resolver, **kwargs):
            super().__init__(*args, **kwargs)
            self._pinned_resolver = _pinned_resolver

        def _new_conn(self):
            # _resolve_and_validate 对空解析结果直接抛 URLPolicyError，故 ips 非空。
            ips = self._pinned_resolver(self.host, self.port)
            # urllib3 ≥2 的 ``host`` getter 会剥掉 FQDN 末尾的点；还原时要拿回
            # 原始值（``_dns_host``），否则重连后 Host/SNI 上的尾点被静默丢掉。
            original = getattr(self, "_dns_host", self.host)
            last_error = None
            try:
                # 与 create_connection 一样逐地址回退（默认解析器会试完全部
                # A/AAAA 记录）；只试第一个会让单地址故障变成连接失败。
                for ip in ips:
                    self.host = ip
                    try:
                        return super()._new_conn()
                    except HTTPError as e:
                        last_error = e
            finally:
                self.host = original
            raise last_error

    class _PinnedHTTPConnection(_PinnedConnectionMixin, HTTPConnection):
        pass

    class _PinnedHTTPSConnection(_PinnedConnectionMixin, HTTPSConnection):
        pass

    class _PinnedHTTPConnectionPool(HTTPConnectionPool):
        ConnectionCls = _PinnedHTTPConnection

    class _PinnedHTTPSConnectionPool(HTTPSConnectionPool):
        ConnectionCls = _PinnedHTTPSConnection

    class _PinnedPoolManager(PoolManager):
        def __init__(self, *args, _pinned_resolver=None, **kwargs):
            super().__init__(*args, **kwargs)
            self._pinned_resolver = _pinned_resolver
            self.pool_classes_by_scheme = {
                **self.pool_classes_by_scheme,
                "http": _PinnedHTTPConnectionPool,
                "https": _PinnedHTTPSConnectionPool,
            }

        def _new_pool(self, scheme, host, port, request_context=None):
            # pinning resolver 不能进 connection_pool_kw（会进 PoolKey 的
            # 命名元组构造而报 unexpected keyword）；在建池时注入 conn_kw。
            if request_context is None:
                request_context = self.connection_pool_kw.copy()
            request_context = {**request_context, "_pinned_resolver": self._pinned_resolver}
            return super()._new_pool(scheme, host, port, request_context)

    class _PinnedHTTPAdapter(HTTPAdapter):
        def __init__(self, pinned_resolver=None, **kwargs):
            self._pinned_resolver = pinned_resolver
            super().__init__(**kwargs)

        def init_poolmanager(self, connections, maxsize, block=DEFAULT_POOLBLOCK, **pool_kwargs):
            self._pool_connections = connections
            self._pool_maxsize = maxsize
            self._pool_block = block
            self.poolmanager = _PinnedPoolManager(
                num_pools=connections, maxsize=maxsize, block=block,
                _pinned_resolver=self._pinned_resolver, **pool_kwargs,
            )

    _PINNED_ADAPTER_CLS = _PinnedHTTPAdapter
    return _PINNED_ADAPTER_CLS


def _read_capped_requests_body(response, max_bytes: int) -> bytes:
    """流式读取 requests 响应体，超过 max_bytes 抛 :class:`ResponseTooLargeError`。"""
    chunks: List[bytes] = []
    total = 0
    for chunk in response.iter_content(chunk_size=65536):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ResponseTooLargeError(
                f"HTTP response body exceeds maxResponseBytes={max_bytes}; "
                f"raise {HTTP_MAX_RESPONSE_BYTES_ENV} to allow larger bodies"
            )
        chunks.append(chunk)
    return b"".join(chunks)


async def _read_capped_async_body(content, max_bytes: int) -> bytes:
    """流式读取 aiohttp 响应体，超过 max_bytes 抛 :class:`ResponseTooLargeError`。

    ``content.read(n)`` 允许返回少于 n 的非 EOF 分片（缓冲未喂满），必须循环到
    空分片；单次 ``read(max+1)`` 会把大 body 误当读完并静默截断。
    """
    chunks: List[bytes] = []
    total = 0
    async for chunk in content.iter_chunked(65536):
        total += len(chunk)
        if total > max_bytes:
            raise ResponseTooLargeError(
                f"HTTP response body exceeds maxResponseBytes={max_bytes}; "
                f"raise {HTTP_MAX_RESPONSE_BYTES_ENV} to allow larger bodies"
            )
        chunks.append(chunk)
    return b"".join(chunks)


class HttpExecutor:
    """HTTP执行器"""
    def __init__(self, url, method, query, body, headers, addressing, delegate,
                 request_timeout: float = 30.0,
                 allowed_hosts: Optional[List[str]] = None,
                 denied_hosts: Optional[List[str]] = None,
                 block_private_networks: bool = False,
                 max_redirects: int = 5):
        self.url = url
        self.method = method
        self.query = query
        self.body = body
        self.headers = headers
        self.addressing = addressing
        self.delegate = delegate
        self.c = None
        # 访问策略（2026-09 安全评审 P1-1）。两条路径都手动逐跳跟随：restricted
        # 下每跳复检策略，防止 302 把请求带进被禁网段；默认路径下不读中间跳
        # body，防止敌意 302 绕过响应体上限。上限同为 max_redirects。
        self.request_timeout = request_timeout
        self.allowed_hosts = allowed_hosts
        self.denied_hosts = denied_hosts
        self.block_private_networks = block_private_networks
        self.max_redirects = max_redirects

    @property
    def _effective_block_private(self) -> bool:
        """节点声明 OR 运营者开关（节点只能更严，不能更松）。"""
        return self.block_private_networks or operator_block_private()

    @property
    def max_response_bytes(self) -> int:
        return http_max_response_bytes()

    @property
    def _restrictions_active(self) -> bool:
        return bool(self.allowed_hosts or self.denied_hosts or self._effective_block_private)

    def _check_policy(self, url: str) -> None:
        _host_allowed(url, self.allowed_hosts, self.denied_hosts,
                      self._effective_block_private, resolver=_cached_resolve_host)

    def _resolve_and_validate(self, hostname: str, port: int) -> List[str]:
        """解析 hostname 并按策略校验，返回可连接的地址（连接期 pinning 入口）。"""
        ips = _cached_resolve_host(hostname, port)
        if not ips:
            raise URLPolicyError(
                f"Cannot resolve {hostname!r} to enforce HTTP policy"
            )
        _validate_host_target(hostname, ips, self.allowed_hosts, self.denied_hosts,
                              self._effective_block_private,
                              f"//{hostname}:{port}")
        return ips

    def _new_policy_sync_session(self):
        """restricted 同步请求专用 session：pinning adapter + 每请求独立。

        不走进程级共享 session——pinning 是逐 host 的建连行为，且共享池的连接
        可能来自不同策略的请求。restricted 是安全敏感少数派，冷路径每请求一次
        握手可接受。
        """
        session = _sync_session_cls()()
        # 代理环境变量会让 ``Session.send`` 经 resolve_proxies 把请求交给
        # ProxyManager：连接目标是代理、解析与建连都发生在代理侧，连接期
        # pinning 被整体旁路（egress 走代理的部署恰是 SSRF 最要紧的场景）。
        # 与 aiohttp 侧共享 session 一样钉死 trust_env=False。
        session.trust_env = False
        adapter = _pinned_adapter_cls()(pinned_resolver=self._resolve_and_validate)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        if not _sync_cookies_enabled():
            session.cookies.set_policy(_BlockAllCookies())
        return session

    def _build_request_params(self):
        """Compute (url, headers, data) shared between sync and async paths."""
        url = self.url
        if self.query:
            parsed_url = urlparse(url)
            query_string = urlencode(self.query)
            if parsed_url.query:
                new_query = f"{parsed_url.query}&{query_string}"
            else:
                new_query = query_string
            parts = list(parsed_url)
            parts[4] = new_query
            url = parsed_url._replace(query=new_query).geturl()

        headers = dict(self.headers) if self.headers else {}
        data = _json_dumps_bytes(self.body) if self.body is not None else None
        return url, headers, data

    def handle_request(self, ctx):
        """处理HTTP请求（同步）"""
        if requests is None:
            _require_http()

        request = self.new_request(ctx)
        if request is None:
            return None, Exception("Failed to create request")

        restricted = self._restrictions_active
        if restricted:
            # 策略激活：pinning 是逐 host 的建连行为，共享池里的连接可能来自
            # 不同策略的请求——改用具 pinning adapter 的专用 session。
            session = self._new_policy_sync_session()
        else:
            if self.c is None:
                # 进程级共享连接池 session（fork 后重建、cookie 默认阻断）——
                # 冷路径（仅 Parallel sync 分支桥走到），见 plaita.core.http_session。
                self.c = get_shared_sync_session()
            session = self.c

        try:
            try:
                if restricted:
                    self._check_policy(request.url)
                # 手动逐跳跟随重定向（两条路径同一形状）：
                # * restricted：requests 的自动跟随在 send 内部完成，逐跳策略校验失效；
                # * 默认路径：resolve_redirects 对每个 3xx 都 ``resp.content`` 读尽整段
                #   body（无上限）——敌意 302 可绕过响应体上限把任意大 body 读进内存。
                # 上限统一为 maxRedirects（此前默认路径走 requests 的 30）；
                # stream=True：响应体按上限流式读，避免 GB 级 body 全量进内存。
                response = session.send(request, timeout=self.request_timeout,
                                        allow_redirects=False, stream=True)
                hops = 0
                while response.is_redirect and hops < self.max_redirects:
                    next_url = urljoin(response.url, response.headers.get("Location", ""))
                    next_request = _redirect_request(response, request, next_url)
                    response.close()  # stream=True：中间跳必须显式释放连接（不读 body）
                    # 先切快照再校验：策略拒绝时错误帧带的是将要尝试的下一跳
                    # （与 async 路径一致）。
                    request = next_request
                    if restricted:
                        self._check_policy(next_url)
                    response = session.send(request, timeout=self.request_timeout,
                                            allow_redirects=False, stream=True)
                    hops += 1
                if response.is_redirect:
                    response.close()
                    return HttpResponse(raw_request=request), Exception(
                        f"Too many redirects (> {self.max_redirects})"
                    )
            except URLPolicyError as e:
                return HttpResponse(raw_request=request), e
            except Exception as e:
                return HttpResponse(raw_request=request), e

            try:
                response._content = _read_capped_requests_body(
                    response, self.max_response_bytes)
                data = response.text
                res = None
                try:
                    # 与 async 路径同构：显式解析而非 response.json()——后者经
                    # json.loads(**kwargs) 不接受自定义 loads，且字符集判定
                    # response.text 已完成。
                    res = _loads_lenient(data)
                except Exception:
                    # JSON 解析失败时回退到原始文本——预期分支, 不必记日志。
                    res = data

                return HttpResponse(
                    raw_request=request,
                    raw_response=response,
                    res=res
                ), None
            except ResponseTooLargeError as e:
                # 超限：body 未读尽，显式释放连接，别把未消费的连接留在池里
                # （getattr 兼容单测里的极简响应桩）。带响应快照 → 与 async
                # 超限分支同归 1003（节点级策略错误，非发送失败）。
                close = getattr(response, "close", None)
                if close is not None:
                    close()
                return HttpResponse(raw_request=request, raw_response=response), e
            except Exception as e:
                # 传输类读取失败（截断 / 连接重置 / 解码失败）：不带响应快照 →
                # send_request_fail() 成立，与 async 泛化分支同归 1002。
                close = getattr(response, "close", None)
                if close is not None:
                    close()
                return HttpResponse(raw_request=request), e
        finally:
            if restricted:
                session.close()

    async def handle_request_async(self, ctx):
        """处理HTTP请求（异步，使用 aiohttp）。

        返回 (AsyncHttpResponse, error) 与同步版本保持一致的签名。
        ``AsyncHttpResponse`` 是仅用于 async 路径的轻量包装，字段名与
        ``HttpResponse`` 相同，让 ``HTTP.execute`` 的结果处理代码可复用。

        2026-10 起 session 复用策略：flow 驱动层（``core.async_utils``）为
        每次 flow run 开一个共享 ``ClientSession``（contextvar 传递，确定性
        关闭），本方法优先复用；脱离 flow 驱动的直接调用退回一次性会话。

        策略激活（restricted）时不复用共享 session，改用带
        :class:`_PolicyResolver` 的专用 connector：建连解析本身经策略校验并只
        连校验过的 IP，关闭 DNS rebinding TOCTOU 窗口。
        """
        if aiohttp is None:
            _require_http()

        url, headers, data = self._build_request_params()

        if self._restrictions_active:
            async with _new_oneshot_session(resolver=_PolicyResolver(self)) as policy_session:
                return await self._send_async(policy_session, url, headers, data)

        session = get_flow_session()
        if session is None or session.closed:
            async with _new_oneshot_session() as oneshot:
                return await self._send_async(oneshot, url, headers, data)
        return await self._send_async(session, url, headers, data)

    async def _send_async(self, session, url, headers, data):
        """在给定 session 上执行请求（两条路径都逐跳手动跟随；restricted 时每跳复检策略）。

        aiohttp 自己的自动跟随与同步路径（requests）语义不同：301/302 的非 POST
        方法它会**保留 body**（requests 丢弃），重定向上限也是它自己的 10——
        逐跳手动跟随让 sync/async 同规则、同上限、同错误文案，且中间跳 body
        一样不读。

        错误分支一律携带 ``HttpRequestInfo`` 快照（C2-1）：与同步路径
        ``handle_request`` 的 ``HttpResponse(raw_request=request)`` 对齐，让
        ``send_request_fail()`` 在 async 路径同样成立 → 连接类错误归
        1002(DO_REQUEST)，sync/async 同错误同码。
        """
        raw_request = HttpRequestInfo(method=self.method, url=url, headers=headers, body=data)
        try:
            if self._restrictions_active:
                self._check_policy(url)
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            current_url, current_method, current_data = url, self.method, data
            for _hop in range(self.max_redirects + 1):
                async with session.request(
                    method=current_method,
                    url=current_url,
                    headers=headers,
                    data=current_data,
                    timeout=timeout,
                    allow_redirects=False,
                ) as response:
                    location = response.headers.get("Location")
                    # 无 Location 的 3xx 不算重定向（对齐 requests 的
                    # ``Response.is_redirect``），按普通响应返回。
                    if location and response.status in (301, 302, 303, 307, 308):
                        next_url = urljoin(str(response.url), location)
                        current_method = _redirect_method(response.status, current_method)
                        # 307/308 保留方法**与 body**（与 sync 的 _redirect_request
                        # 同规则）；其余跳丢弃 body。
                        if response.status not in (307, 308):
                            current_data = None
                        # 跨源重定向剥离凭据类头（后续跳沿用裁剪后的头）
                        headers = _strip_sensitive_headers(
                            headers, str(response.url), next_url)
                        current_url = next_url
                        # 快照跟随实际尝试的下一跳（错误帧里带真实 url/headers；
                        # 与 sync 一致——策略拒绝时也是这一跳）
                        raw_request = HttpRequestInfo(
                            method=current_method, url=current_url,
                            headers=headers, body=current_data,
                        )
                        if self._restrictions_active:
                            self._check_policy(next_url)
                        continue
                    try:
                        raw_body = await _read_capped_async_body(
                            response.content, self.max_response_bytes)
                    except ResponseTooLargeError as e:
                        # 携带响应快照：与 sync 路径同归 1003（非发送失败），
                        # 且错误帧能带出实际状态/头。
                        raw_resp = _AiohttpResponseWrapper(
                            status_code=response.status,
                            reason=response.reason,
                            headers=dict(response.headers),
                            body=None,
                        )
                        return HttpResponse(raw_request=raw_request,
                                            raw_response=raw_resp), e
                    # get_encoding() 对无 charset 的 body 依赖已读内容判定；
                    # 把流式读满上限的结果回填，复用其 charset 判定（同
                    # response.text() 的内部路径）。
                    response._body = raw_body
                    text = raw_body.decode(response.get_encoding())
                    try:
                        res = _loads_lenient(text)
                    except Exception:
                        res = text
                    raw_resp = _AiohttpResponseWrapper(
                        status_code=response.status,
                        reason=response.reason,
                        headers=dict(response.headers),
                        body=res,
                    )
                    return HttpResponse(raw_request=raw_request,
                                        raw_response=raw_resp, res=res), None
            return HttpResponse(raw_request=raw_request), Exception(
                f"Too many redirects (> {self.max_redirects})")
        except URLPolicyError as e:
            return HttpResponse(raw_request=raw_request), e
        except Exception as e:
            return HttpResponse(raw_request=raw_request), e

    def new_request(self, ctx):
        """创建HTTP请求（同步路径用）"""
        url, headers, data = self._build_request_params()
        if requests is None:
            _require_http()
        req = requests.Request(
            method=self.method,
            url=url,
            headers=headers,
            data=data if data else None,
        )
        return req.prepare()


class _AiohttpResponseWrapper:
    """Minimal shim that exposes the same attributes as a ``requests.Response``
    so the result-building code in ``HTTP.execute`` / ``HTTP.arun`` can be
    shared without branching on response type."""

    def __init__(self, status_code: int, reason: str, headers: dict, body):
        self.status_code = status_code
        self.reason = reason
        self.headers = headers
        self._body = body

    def json(self):
        return self._body

    @property
    def text(self):
        return self._body if isinstance(self._body, str) else json.dumps(self._body)


class HTTP(Node):
    """HTTP节点实现"""
    node_type: ClassVar[str] = "http"
    node_name: ClassVar[str] = "HTTP请求"
    
    # Literal 生成 schema enum（console 表单渲染下拉）；大小写归一由 setup_http
    # before-validator 完成（先 upper 再校验，存量小写值不受影响）。合法集与
    # validate() 的 valid_methods 保持一致。
    method: Literal[
        "GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "CONNECT", "OPTIONS", "TRACE"
    ] = Field("GET", description="HTTP 方法")
    content_type: str = Field("application/json", description="内容类型")

    # ---- 访问策略（2026-09 安全评审 P1-1：SSRF 防护）----
    # 默认行为与历史完全一致（无限制、30s 超时）；多租户部署应设置
    # allowedHosts 或 blockPrivateNetworks=true，并按需收紧 requestTimeout。
    # 运营者级强制（PLAITA_HTTP_BLOCK_PRIVATE=1）与响应体上限
    # （PLAITA_HTTP_MAX_RESPONSE_BYTES）见模块顶部常量。
    request_timeout: float = Field(
        30.0, alias="requestTimeout",
        description="单次请求（含重定向每一跳）超时秒数",
    )
    allowed_hosts: Optional[List[str]] = Field(
        None, alias="allowedHosts",
        description="主机白名单：精确域名 / *.suffix 通配 / CIDR（按解析后 IP 匹配）；非空时仅允许命中者",
    )
    denied_hosts: Optional[List[str]] = Field(
        None, alias="deniedHosts",
        description="主机黑名单：格式同 allowedHosts，命中即拒绝",
    )
    block_private_networks: bool = Field(
        False, alias="blockPrivateNetworks",
        description="解析目标并对每个地址拒绝回环/内网/链路本地等私网段（多租户建议开启；"
                    "PLAITA_HTTP_BLOCK_PRIVATE=1 时运营者强制开启，节点声明只能更严）",
    )
    max_redirects: int = Field(
        5, alias="maxRedirects",
        description="重定向跟随上限（sync/async 同口径；策略激活时每跳复检）",
    )

    # validator 消费的 camelCase 遗留键（content_type 字段无 alias）
    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"contentType"})
    url: str = Field(..., description="请求URL")
    query: Optional[Any] = Field(None, description="查询参数")
    headers: Optional[Any] = Field(None, description="请求头")
    body: Optional[Any] = Field(None, description="请求体")
    addressing: Optional[Dict] = Field(None, description="寻址配置")
    delegate: Optional[Dict] = Field(None, description="代理配置")

    model_config = ConfigDict(populate_by_name=True)
    
    @model_validator(mode="before")
    @classmethod
    def setup_http(cls, values: Dict) -> Dict:
        # 设置默认值
        method = values.get("method", "GET")
        values["method"] = method.upper()
        
        values["content_type"] = values.get("contentType", values.get("content_type", "application/json"))
        
        # 确保寻址配置存在
        if not values.get("addressing"):
            values["addressing"] = {"name": "domain"}
            
        return values
    
    def get_type(self) -> str:
        """获取节点类型"""
        return self.node_type
    
    def validate(self):
        """验证HTTP节点配置"""
        super().validate()
        if not self.url:
            raise ValueError("URL is required")
        
        # 验证HTTP方法
        valid_methods = ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "CONNECT", "OPTIONS", "TRACE"]
        if self.method not in valid_methods:
            raise ValueError(f"Invalid HTTP method: {self.method}")
    
    def _build_response_result(self, http_rsp, execution):
        """Shared response → result conversion for both sync and async paths."""
        response = {
            RESPONSE_DATA_KEY: http_rsp.res,
            RESPONSE_STATUS_KEY: http_rsp.raw_response.status_code,
            RESPONSE_STATUS_TEXT_KEY: http_rsp.raw_response.reason,
            RESPONSE_HEADERS_KEY: dict(http_rsp.raw_response.headers),
        }
        if execution:
            # #129：响应上下文改为写进**本节点自己的结果**（见 node_result_payload），
            # 由 `update_node_result` 落进 `$NODE.<id>`。
            #
            # 历史实现是 `execution.set_state(f"{prefix}NODE.{self.id}.RESPONSE", …)`，
            # 而 `ExecutionContext.set_state` 当时是**扁平写入**（`self._state[key] = value`），
            # 于是状态表里躺的是字面量键 `"$NODE.<id>.RESPONSE"`；表达式寻址
            # `$NODE.<id>.RESPONSE` 走的是嵌套路径 ⇒ **永远解析不到**
            # （实测静默 None / 误导性 `KeyError: '$NODE' not found`）。
            # 集成测试用带 `set_node_context` 的 mock execution，且只断言 set_state
            # **被调用过**，从未对真实 context 求值过文档里的表达式，故长期掩盖。
            #
            # 现在按「$NODE.<id> 持有该节点的结果」语义落盘：结果即响应结构，
            # 字段名以**文档承诺**的 data/status/headers 为权威（builtin.md:267）。
            self._response_context = dict(response)
        if self.output is None:
            return response[RESPONSE_DATA_KEY]
        return execution.evaluate(self.output)

    def node_result_payload(self):
        """#129：本节点结果里承载的响应上下文字段。

        未发起过请求时返回 ``None``（不改变既有结果形状）。供 sync/async 两条
        结果处理路径调用，使 ``$NODE.<id>.data`` / ``.status`` / ``.headers``
        真正可寻址——这三个名字是 ``docs-site/docs/nodes/builtin.md`` 承诺的，
        也是 ``docs-site/docs/guide/expressions.md:29`` 所描述的
        ``$NODE.<id>.field`` 形态。

        注意：这里返回的是**响应结构本身**（含 data/status/statusText/headers），
        不含大写别名——避免"两套名字都看起来能用"（#129 病根）。
        """
        return getattr(self, "_response_context", None)

    def execute(self, execution):
        """执行HTTP请求（同步）"""
        http_rsp = None
        try:
            executor = self.new_executor(execution)
            if not executor:
                return None, Exception("Failed to create HTTP executor")
            http_rsp, err = executor.handle_request(execution)
            if err:
                # raise 而非 return：把 NodeException 当返回值会让 errorHandler
                # （continue/continue_with 的 defaultValue）永远不生效
                raise self.handle_http_node_err(err, http_rsp)
            return self._build_response_result(http_rsp, execution)
        except Exception as e:
            raise self.handle_http_node_err(e, http_rsp)

    async def arun(self, execution):
        """执行HTTP请求（异步，使用 aiohttp，不阻塞事件循环）。"""
        http_rsp = None
        try:
            executor = self.new_executor(execution)
            if not executor:
                raise RuntimeError("Failed to create HTTP executor")
            http_rsp, err = await executor.handle_request_async(execution)
            if err:
                raise self.handle_http_node_err(err, http_rsp)
            return self._build_response_result(http_rsp, execution)
        except Exception as e:
            raise self.handle_http_node_err(e, http_rsp)
    
    def new_executor(self, ctx):
        """创建HTTP执行器"""
        try:
            parsed_url = self.parse_url(ctx)
            headers = self.parse_header(ctx)
            query = self.parse_query(ctx)
            body = self.parse_body(ctx)
            addressing_param = self.parse_addressing(ctx)
            delegate_param = self.parse_delegate(ctx)
            
            return HttpExecutor(
                url=parsed_url,
                method=self.method,
                query=query,
                body=body,
                headers=headers,
                addressing=addressing_param,
                delegate=delegate_param,
                request_timeout=self.request_timeout,
                allowed_hosts=self.allowed_hosts,
                denied_hosts=self.denied_hosts,
                block_private_networks=self.block_private_networks,
                max_redirects=self.max_redirects,
            )
        except Exception as e:
            raise e
    
    def parse_delegate(self, ctx):
        """解析代理参数"""
        if not self.delegate:
            return DelegateParam()
        
        delegate_name = ""
        if "name" in self.delegate:
            name_value = evaluate(self.delegate["name"], ctx)
            if name_value:
                delegate_name = str(name_value)
        
        delegate_params = None
        if "params" in self.delegate:
            params_value = evaluate(self.delegate["params"], ctx)
            if params_value:
                delegate_params = json.dumps(params_value).encode('utf-8')
        
        return DelegateParam(
            name=delegate_name,
            params=delegate_params
        )
    
    def parse_addressing(self, ctx):
        """解析寻址配置"""
        if not self.addressing:
            return Addressing()
        
        addr_name = ""
        if "name" in self.addressing:
            name_value = evaluate(self.addressing["name"], ctx)
            if name_value:
                addr_name = str(name_value)
        
        addr_params = None
        if "params" in self.addressing:
            params_value = evaluate(self.addressing["params"], ctx)
            if params_value:
                addr_params = json.dumps(params_value).encode('utf-8')
        
        return Addressing(
            name=addr_name,
            params=addr_params
        )
    
    def parse_body(self, ctx):
        """解析请求体"""
        if self.body is None:
            return None
        
        try:
            body = evaluate(self.body, ctx)
            return body
        except Exception as e:
            raise Exception(f"Failed to parse body: {_scrub_url_text(str(e))}")
    
    def parse_query(self, ctx):
        """解析查询参数"""
        if self.query is None:
            return None
        
        try:
            query = evaluate(self.query, ctx)
            if isinstance(query, dict):
                return query
            return None
        except Exception as e:
            raise Exception(f"Failed to parse query: {_scrub_url_text(str(e))}")
    
    def parse_header(self, ctx):
        """解析请求头"""
        headers = {}
        if self.content_type:
            headers["Content-Type"] = self.content_type
        
        if self.headers is None:
            return headers
        
        try:
            parsed_headers = evaluate(self.headers, ctx)
            if isinstance(parsed_headers, dict):
                for key, value in parsed_headers.items():
                    if isinstance(value, str):
                        headers[key] = value
            return headers
        except Exception as e:
            raise Exception(f"Failed to parse headers: {_scrub_url_text(str(e))}")
    
    def parse_url(self, ctx):
        """解析URL"""
        try:
            url_value = evaluate(self.url, ctx)
            return str(url_value)
        except Exception as e:
            raise Exception(f"Failed to parse URL: {_scrub_url_text(str(e))}")
    
    def handle_http_node_err(self, err, http_rsp):
        """处理HTTP节点错误"""
        if err is None:
            return None
        
        if http_rsp is None or http_rsp.empty():
            return self.wrap_http_node_err(HTTP_GEN_REQUEST_ERROR, str(err), http_rsp)
        
        if http_rsp.send_request_fail():
            return self.wrap_http_node_err(HTTP_DO_REQUEST_ERROR, str(err), http_rsp)
        
        return self.wrap_http_node_err(HTTP_NODE_EXEC_ERROR, str(err), http_rsp)
    
    def wrap_http_node_err(self, code, message, rsp):
        """包装HTTP节点错误

        C2-1：历史上这里构造的 ``HttpNodeErrorInfo``（含 request/response 明细）
        被直接丢弃，``NodeException`` 只剩 code/message——errorHandler 与事件层
        无法按 HTTP 状态分支。现在以最小侵入方式挂载（core 层 NodeException
        契约不变，仅加实例属性）：

        - ``exc.details``  — 完整 ``HttpNodeErrorInfo``（结构化 request/response）
        - ``exc.response`` — 事件安全的 dict 摘要（status/statusText/headers/
          body），headers 凭据类脱敏、body 截断（``_summarize_error_frame_response``）

        ``#158``+``#159`` r2：``message`` 是**唯一**的异常消息构造点，先过
        :func:`_scrub_url_text`——第三方客户端（requests/aiohttp）把请求 URL 写进
        自己的异常文本，``handle_http_node_err`` 的 ``str(err)`` 会把它整段转手；
        清洗必须在进 ``NodeException`` 之前（下游 logger 打补丁是盲区形态）。
        """
        message = _scrub_url_text(message)
        error_info = None

        if rsp and not rsp.empty():
            response_info = None
            if rsp.raw_response:
                response_info = HttpNodeResponse(
                    status=rsp.raw_response.status_code,
                    status_text=rsp.raw_response.reason,
                    headers=dict(rsp.raw_response.headers),
                    data=rsp.res
                )

            error_info = HttpNodeErrorInfo(
                code=code,
                message=message,
                request=rsp.raw_request,
                response=response_info
            )

        exc = NodeException(code, message)
        if error_info is not None:
            exc.details = error_info
            exc.response = _summarize_error_frame_response(error_info.response)
        return exc


# 注册HTTP节点类型
def register():
    from plaita.node import get_default_registry
    get_default_registry().register(HTTP) 