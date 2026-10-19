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
from pydantic import ConfigDict, Field, field_validator, model_validator

from plaita.node.basic import Node
from plaita.core.errors import NodeException
# 策略/第三方文本里的 URL 渲染（#158+#159 r2 建，`#160`+`#161` 起为**共享**实现：
# `_scrub_url_text` / `_mask_policy_url` 的家已迁到 url_redaction，node 层只按名字
# import 以免两套渲染分叉）。url_redaction 只依赖 stdlib 的 server.factory，
# 不触发 server extra，也不与 node 层成环（依赖方向单一：node → server）。
#
# 后五个名字（``_ABSOLUTE_URL_RE`` / ``_CREDENTIAL_RUN_RE`` / ``_token_is_canonical`` /
# ``_render_is_usable`` / ``_leaves_plaintext_payload``）本身在 node 层**不再被调用**，
# 是**名字稳定再导出**：``#159`` 的测试与巡检脚本经 ``plaita.node.http._X`` 取渲染器
# 内部件（``http_mod._ABSOLUTE_URL_RE`` / ``getattr(http_mod, "_render_is_usable")`` …），
# 提取实现时若让这些名字消失，等于把「实现搬家」变成「接口破坏」。按名字再导出即可：
# rename/删除会在 import 期即炸（fail-loud），而不是悄悄换一份渲染。
#
# ``_is_credential_key``（``#160`` 修复轮加）是**记录面 header 键判据的第二腿**，
# 同样按名字 import、不复制文法： ``#160`` 的记录面曾经只按 7 名清单掩 header
# 键，``X-Api-Key`` / ``X-Token`` / ``apikey`` / ``X-Goog-Api-Key`` 因此整套穿过。
# 这里直接复用共享凭据键谓词（r5 重写文法时本 import 自动跟上；rename 即 import 期炸）。
from plaita.core.url_redaction import (  # noqa: F401 — 部分名字仅供历史可达性
    UNPRINTABLE_URL,
    _ABSOLUTE_URL_RE,
    _credential_run_spans,
    _find_closing_quote,
    _host_is_identifiable,
    _is_credential_key,
    _leaves_plaintext_payload,
    _mask_policy_url,
    _render_is_usable,
    _scrub_exception_text,
    _scrub_url_text,
    _token_is_canonical,
    logger,
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

# ---- 构造期上限（A2 r22 P1）----
# editor 可发布 flow，worker 为多租户共享；不设上限时 1e9 级的 timeout /
# redirects 足以让单个节点把 worker 钉住（跨租户独占）。数值取工程上远高于
# 正常需求的量级，同时给单节点最坏耗时一个有限上界。构造期（Pydantic）
# 拒绝，console 保存/发布返回 422，而不是等运行期才发现。
MAX_REQUEST_TIMEOUT_SECONDS = 600.0
MAX_REDIRECTS = 20

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

    ``#160``：本对象是**传输忠实**快照——``url`` 与真正发出的请求逐字节相同
    （``wrap_http_node_err`` 直接把 ``rsp.raw_request`` 挂到 ``exc.details``），
    所以它是「实际发了什么」的记录，**不是**日志面。要进日志/序列化请用
    :attr:`url_masked` / :meth:`HttpNodeErrorInfo.redacted`；``repr`` 已 fail-closed
    掩码（安全不依赖「现在还没有运行时读它」）。
    """

    def __init__(self, method: str, url: str, headers: Optional[Dict[str, str]] = None,
                 body: Optional[bytes] = None):
        self.method = method
        self.url = url
        self.headers = dict(headers) if headers else {}
        self.body = body

    @property
    def url_masked(self) -> str:
        """URL 的**掩码视图**：``scheme://host[:port]/***?***``（端点保留，凭据全抹）。"""
        return _mask_policy_url(self.url)

    def __repr__(self) -> str:
        # 掩码面：``#160`` 的 fail-closed 记录渲染。原文只留在属性里（传输面），
        # 本对象（及 ``exc.details`` / ``exc.details.response`` 同族）的任何
        # ``repr``/``str``——含 ``traceback`` 里的对象打印、``repr([obj])``、
        # ``json.dumps(obj, default=str)``、``json.dumps(vars(obj), default=str)``、
        # ``logging`` 的 ``%r``/``%s``——都拿不到凭据：header **值**（不只凭据名）
        # 与 body 都过共享渲染器 :func:`_scrub_url_text`。
        # 该绝对 claim 由 ``tests/unit/test_issue160_exception_attachment_record_surface.py::
        # TestAbsoluteClaimEnforced`` 逐面机械断言（claim 失效即该测试变红）。
        return (
            f"HttpRequestInfo(method={self.method!r}, url={self.url_masked!r}, "
            f"headers={_mask_record_headers(self.headers)!r}, "
            f"body={_mask_record_body(self.body)!r})"
        )


class HttpNodeErrorInfo:
    """HTTP节点错误信息（``exc.details``，``#160`` 起**显式声明为非日志面**）。

    字段名 ``code`` / ``message`` / ``request`` / ``response`` 与 ``#158``+``#159``
    逐字不变；``request``/``response`` 持有的是**传输忠实**快照（``request.url``
    与真正发出去的 ``rsp.raw_request.url`` 字节相同），供错误分支/重试诊断读。

    **选型（``#160`` 验收的二选一）**：这里选「声明非日志面 + 掩码 ``__repr__``」，
    而不是「把字段本身掩码」，理由是：

    1. ``details.request.url`` 的原文被 C2 错误帧契约测试钉住
       （``tests/unit/test_clean_c2_http_error_frame.py`` 断言 ``.../api`` 原样可读），
       掩码字段会破坏那份「记录 = 实际发出内容」的契约，且改动要外溢到别的单；
    2. 泄露信道是**序列化**（``repr``/``str``/traceback/JSON shipper），而不是属性
       访问——``__repr__`` 是唯一必然进日志的那一面，在它上面 fail-closed 恰好
       覆盖信道，且「无人读它」不再是安全前提；
    3. 需要结构化日志的消费者有显式的 :meth:`redacted` **掩码视图**（JSON 安全），
       不必也不该把原始快照塞进日志。
    """

    def __init__(self, code: int, message: str, request=None, response=None):
        self.code = code
        self.message = message
        self.request = request
        self.response = response

    def redacted(self) -> Dict[str, Any]:
        """**掩码视图**（``#160``）：JSON 安全的 dict，可直接 ``json.dumps``。

        形状与事件面同源：``code`` / ``message`` + 请求/响应各自的
        ``method|url|headers|body`` 与 ``status|statusText|headers|body``。
        请求 URL 走 :func:`_mask_policy_url`（端点保留）；**headers 的键**走
        :func:`_is_credential_header_name`（7 名清单 ∪ 共享凭据键谓词），
        **headers 的值**与 body 走 :func:`_scrub_url_text`——记录面每一个值都过
        共享渲染器，判据不分叉（``#160`` 修复轮）；``message`` 再过一次清洗
        （幂等），保证「即使调用方塞了未清洗的 message，视图也不带凭据」。
        """
        request_view = None
        if self.request is not None:
            request_view = {
                "method": getattr(self.request, "method", None),
                "url": _mask_policy_url(getattr(self.request, "url", None)),
                "headers": _mask_record_headers(getattr(self.request, "headers", None)),
                "body": _mask_record_body(getattr(self.request, "body", None)),
            }
        response_view = None
        if self.response is not None:
            response_view = {
                "status": getattr(self.response, "status", None),
                "statusText": getattr(self.response, "status_text", None),
                "headers": _mask_record_headers(getattr(self.response, "headers", None)),
                "body": _mask_record_body(getattr(self.response, "data", None)),
            }
        return {
            "code": self.code,
            "message": _scrub_url_text(self.message),
            "request": request_view,
            "response": response_view,
        }

    def __repr__(self) -> str:
        view = self.redacted()  # 记录面唯一实现：掩码只在这里做一次
        return (
            f"HttpNodeErrorInfo(code={view['code']!r}, message={view['message']!r}, "
            f"request={view['request']!r}, response={view['response']!r})"
        )


class HttpNodeResponse:
    """HTTP节点响应（``exc.details.response``；``#160`` 起同属非日志面）。

    ``headers``（``Set-Cookie`` / ``WWW-Authenticate`` …）与 ``data``（可能回显
    请求凭据）都是**逐字**快照；``repr`` 用记录面渲染器
    （:func:`_render_header_for_record` + :func:`_mask_record_body`）——header 键
    按凭据名判据掩、其余 header **值**与 body 一律过 :func:`_scrub_url_text`。
    """
    def __init__(self, status: int, status_text: str, headers: Dict, data: Any):
        self.status = status
        self.status_text = status_text
        self.headers = headers
        self.data = data

    def __repr__(self) -> str:
        return (
            f"HttpNodeResponse(status={self.status!r}, status_text={self.status_text!r}, "
            f"headers={_mask_record_headers(self.headers)!r}, "
            f"data={_mask_record_body(self.data)!r})"
        )


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
            if resolved_ips is None and any("/" in entry for entry in allowed_hosts if entry):
                # #83：CIDR 条目按解析后 IP 匹配，解析失败必须显式失败——退化成
                # 「not in allowed_hosts」会把「解析不了/配置不生效」伪装成
                # 「目标不在网段内」，把运营者引去查 DNS 而不是查策略。
                raise URLPolicyError(
                    f"Cannot resolve {hostname!r} to verify allowed_hosts CIDR entry: "
                    f"{_mask_policy_url(url)}"
                )
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
    解析——只看字面 host 挡不住域名解析到内网的绕过）。CIDR 条目按解析后
    IP 匹配，因此解析门在 allowed_hosts/denied_hosts 存在含 ``/`` 条目时
    同样打开（#83：此前只配 CIDR 白名单时解析不发生、条目恒不匹配）。

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
    # 解析门（#83）：denied_hosts / block_private_networks 需要 IP；allowed_hosts
    # 里**存在 CIDR 条目**（含 ``/``）时也必须解析——此前门条件不含 allowed_hosts，
    # 只配 CIDR 白名单时恒不匹配、全拒。纯精确域名/``*.suffix`` 白名单不触发解析
    # （不多付一次 DNS），既有用例的纯函数路径保持纯函数。
    if denied_hosts or block_private_networks or any(
        "/" in entry for entry in (allowed_hosts or []) if entry
    ):
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
#
# ``#160`` 修复轮：headers 的**值**不再原样——凭据名（7 名清单 ∪ 共享凭据键谓词）
# 仍换 ``***``，其余值过 ``_scrub_url_text``（响应头回显请求 URL 的一族：
# ``Location`` / ``X-Echo-Url``）。此处是全模块唯一的 header 渲染实现。
# ---------------------------------------------------------------------------
_ERROR_FRAME_BODY_MAX_CHARS = 2048

_SENSITIVE_ERROR_FRAME_HEADERS = _SENSITIVE_REDIRECT_HEADERS | frozenset({"set-cookie"})


def _is_credential_header_name(key: Any) -> bool:
    """header 键是否凭据名（``#160`` 修复轮：判据两条腿，**不**再只看 7 名清单）。

    * 第一腿 = r4 的 7 名清单 :data:`_SENSITIVE_ERROR_FRAME_HEADERS`
      （``authorization`` / ``cookie`` / ``cookie2`` / ``proxy-authorization`` /
      ``proxy-authenticate`` / ``www-authenticate`` / ``set-cookie``）——逐字保留，
      其中 ``cookie2`` 这类**共享谓词不命中**的名字靠这一腿兜住；
    * 第二腿 = ``url_redaction._is_credential_key``（**按名字 import，不复制文法**）：
      ``X-Api-Key`` / ``X-Token`` / ``X-Auth-Token`` / ``apikey`` / ``X-Goog-Api-Key``
      命中。r5 重写凭据键文法时这里自动跟上；改名会在 import 期 fail-loud。
    """
    name = str(key)
    return name.lower() in _SENSITIVE_ERROR_FRAME_HEADERS or _is_credential_key(name)


def _render_header_for_record(key: Any, value: Any) -> Any:
    """**单个 header** 的唯一渲染（事件面与记录面共用）：凭据名 → ``***``；
    其余值一律过共享 URL 渲染器 :func:`_scrub_url_text`。

    ``#160`` 修复轮的根因是**判据分叉**：``_mask_record_body`` 会过
    :func:`_scrub_url_text`，header 却只看名字。于是「值里回显 URL」的两族 header
    （响应侧 ``Location`` / ``X-Echo-Url``，请求侧用户自带 ``X-Api-Key`` 等）整套
    穿过到 ``repr`` / ``redacted()`` / ``exc.response``。
    这里把 header **值**也交给**同一个**渲染器（``_scrub_url_text``，唯一实现），
    判据不再分叉。诊断价值保留：非 URL 值原样（``req-123`` /
    ``application/json``），URL 值退化为 ``scheme://host[:port]/***`` 形状。
    """
    if _is_credential_header_name(key):
        return "***"
    return _scrub_url_text(value)


def _mask_error_frame_headers(headers: Any) -> Dict[str, Any]:
    """错误帧 headers 的**唯一**渲染，事件面与记录面共用（判据不分叉）。

    凭据名（7 名清单 ∪ 共享凭据键谓词）→ ``***``；其余 header **值**过
    :func:`_scrub_url_text`。此前只按 7 名清单掩键、值原样——响应头回显请求 URL
    （``Location`` / ``X-Echo-Url``）时事件载荷与 ``repr`` 一起泄露（``#160`` 修复轮）。
    非 ``dict`` 返回 ``{}``（事件面「宁可少给」的历史行为不变）。
    """
    if not isinstance(headers, dict):
        return {}
    return {k: _render_header_for_record(k, v) for k, v in headers.items()}


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
# plaita#160：``exc.details`` 的**记录面**渲染（``__repr__`` / ``redacted()``）。
#
# 修复轮（组合轮 08b3f7fe 定验 FAIL 的那一点）：记录面**每一个值**都过共享渲染器
# ``_scrub_url_text``——header **值**（请求/响应两侧）、body、URL 一律同一条判据；
# header **键**判据也加宽到共享凭据键谓词（不再只看 7 名清单）。判据**不许分叉**
# 是这一节的第一纪律：``_mask_record_body`` 洗 body 而 header 只按名字，正是原缺口。
#
# 因此 header 渲染只有一份实现 :func:`_render_header_for_record`（事件面
# ``_mask_error_frame_headers`` 也调它）。仍然**刻意原样**的只有事件载荷
# ``exc.response['body']``（r4 定稿的 C2 契约，见 ``HttpNodeErrorInfo`` 选型说明），
# 其原样性由 ``tests/unit/test_issue160_exception_attachment_record_surface.py::
# TestDeclaredRawSurfaces`` 钉住。
# ---------------------------------------------------------------------------


def _mask_record_headers(headers: Any) -> Dict[str, Any]:
    """记录面 headers：先收敛成 ``dict`` 再交给**同一**渲染器（不是第二份判据）。

    ``requests`` 的 ``PreparedRequest.headers`` 是 ``CaseInsensitiveDict``（
    ``MutableMapping`` 而**不是** ``dict`` 子类），事件面的 ``_mask_error_frame_headers``
    对它返回 ``{}``——放在事件面是「宁可少给」，放在记录面会把诊断信息整块吞掉。
    这里显式 ``dict(...)`` 收敛（失败仍退化为 ``{}``），既保住诊断，也不改变
    事件面对「非 dict」的既有行为。
    """
    if headers is None:
        return {}
    if not isinstance(headers, dict):
        try:
            headers = dict(headers)
        except Exception:  # noqa: BLE001 — 记录面不许抛
            logger.debug("记录面 headers 无法收敛为 dict，按不可渲染处理")
            return {}
    try:
        return _mask_error_frame_headers(headers)
    except Exception:  # noqa: BLE001 — 记录面不许抛（渲染失败整块退化为不可渲染）
        logger.debug("记录面 headers 渲染失败，按不可渲染处理")
        return {}


def _mask_record_body(body: Any) -> Optional[str]:
    """记录面（``repr`` / ``redacted()``）里的请求/响应体。

    先按事件面的上限截断（``_summarize_error_frame_body``，防大 payload 进日志），
    再过 :func:`_scrub_url_text` 清洗 URL 凭据分量。``bytes`` 按 ``utf-8/replace``
    解码——**不**用 ``str(b"...")`` 那种带转义的 repr（ASCII 密钥在里面与原文等价，
    例如 ``b'access_token=Ks9Qm4Zt'``）。任何异常退化为 ``UNPRINTABLE_URL``。

    边界（刻意）：只抹**URL 形态**的凭据与凭据类头，不承诺识别 body 里任意
    ``{"token": "…"}`` 字段——那是远端内容语义，超出本渲染器的判据。
    """
    if body is None:
        return None
    if isinstance(body, (bytes, bytearray)):
        try:
            body = bytes(body).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001 — 解码兜底，绝不上抛
            logger.debug("记录面 body 解码失败，按不可渲染处理")
            return UNPRINTABLE_URL
    summary = _summarize_error_frame_body(body)
    if summary is None:
        return None
    return _scrub_url_text(summary)


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
    dry_run_safe: ClassVar[bool] = False
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

    # A2 r22 P1：构造期上限。放在字段 validator（而非 validate()）里，是因为
    # Flow.validate_nodes 把 validate() 异常降级为 warning；只有 Pydantic 校验
    # 才能让 console 保存/发布（Flow.model_validate）与运行期加载一并硬拒绝。
    @field_validator("request_timeout")
    @classmethod
    def _validate_request_timeout_upper_bound(cls, value: float) -> float:
        if value > MAX_REQUEST_TIMEOUT_SECONDS:
            raise ValueError(
                f"requestTimeout 超过上限: {value} > {MAX_REQUEST_TIMEOUT_SECONDS} 秒"
                "（单次请求含每跳重定向的总预算；防止共享 worker 被长期占用）"
            )
        return value

    @field_validator("max_redirects")
    @classmethod
    def _validate_max_redirects_upper_bound(cls, value: int) -> int:
        if value > MAX_REDIRECTS:
            raise ValueError(
                f"maxRedirects 超过上限: {value} > {MAX_REDIRECTS}"
                "（重定向跟随次数；防止敌意链路把共享 worker 拖在单节点上）"
            )
        return value

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

    def _dry_run_result(self, execution):
        """plaita#190：试跑语义下 HTTP 节点不得建连/出网。

        console 的 dry-run 闸门已按「能力声明」在入口拒绝 http 节点（纵深之一）；
        这里再做节点级短路，覆盖不经 console 闸门、但显式携带 ``dry_run`` 的
        执行路径（直接调用 / 其它控制面）。返回形状带 ``dry_run=True`` 与
        ``skipped``，让调用方一眼看出「没有发生网络 I/O」；URL 经共享渲染器
        清洗后回显，与记录面同一口径（#151/#158/#159）。
        """
        if execution is None:
            return None
        try:
            raw = execution.get_global_variable("dry_run", False)
            # 只认显式布尔真值：测试/第三方执行上下文常传 MagicMock，而
            # ``bool(MagicMock)`` 恒为 True——按真值判断会把「假执行上下文」
            # 误判成试跑。试跑旗标由 console 注入 JSON ``true``（Python True）
            # 或 ``_DryRunExecution`` 返回 True，不会以 1/"true" 形式出现。
            requested = isinstance(raw, bool) and raw
        except Exception as exc:  # noqa: BLE001 — 读旗标失败按真实执行处理，但必须留证
            logger.warning(
                "HTTP 节点读取 dry_run 旗标失败，按真实执行处理: %s",
                _scrub_exception_text(exc),
            )
            return None
        if not requested:
            return None
        return {
            "dry_run": True,
            "skipped": "http",
            "method": self.method,
            "url": _scrub_url_text(str(self.url)),
        }

    def execute(self, execution):
        """执行HTTP请求（同步）"""
        dry = self._dry_run_result(execution)
        if dry is not None:
            return dry
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
        dry = self._dry_run_result(execution)
        if dry is not None:
            return dry
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
          body），headers 凭据名脱敏**且其余值过共享 URL 渲染器**（修复轮起与
          记录面同一份实现）、body 截断（``_summarize_error_frame_response``）
          —— 其中 body 是 **r4 定稿的 C2 契约**，刻意原样（由 ``#160`` 的
          ``TestDeclaredRawSurfaces`` 钉住）；进日志/序列化请用
          ``exc.details.redacted()``。

        ``#158``+``#159`` r2：``message`` 是**唯一**的异常消息构造点，先过
        :func:`_scrub_url_text`——第三方客户端（requests/aiohttp）把请求 URL 写进
        自己的异常文本，``handle_http_node_err`` 的 ``str(err)`` 会把它整段转手；
        清洗必须在进 ``NodeException`` 之前（下游 logger 打补丁是盲区形态）。

        ``#160``：``request=rsp.raw_request`` 是**传输忠实**引用（同一个对象、
        ``url`` 逐字节不变——``raw_request`` 还要被别处读，掩码它等于改传输记录）。
        记录面的凭据防护放在 ``HttpNodeErrorInfo``/``HttpRequestInfo``/
        ``HttpNodeResponse`` 的 ``__repr__`` 与 ``redacted()`` 上：``details`` 是
        非日志面，任何进日志的渲染都已掩码——**header 值**也在内（修复轮：响应侧
        ``Location``/``X-Echo-Url`` 这类回显 URL 的头不再原样穿过）。
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