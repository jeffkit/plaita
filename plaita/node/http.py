import ipaddress
import json
import logging
import os
import socket
import threading
import time
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse, urlencode
import io
from pydantic import ConfigDict, Field, model_validator

from plaita.node.basic import Node
from plaita.core.errors import NodeException
from plaita.core.http_session import (
    _new_oneshot_session,
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
        raise URLPolicyError(f"URL scheme {parsed.scheme!r} is not allowed (http/https only): {url!r}")
    hostname = parsed.hostname
    if not hostname:
        raise URLPolicyError(f"URL has no hostname: {url!r}")
    hostname = hostname.lower().strip("[]")

    def _match(entry: str, host: str, resolved_ips: Optional[list]) -> bool:
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

    resolved_ips: Optional[list] = None
    if denied_hosts or block_private_networks:
        resolved_ips = (resolver or _resolve_host)(
            hostname, parsed.port or (443 if parsed.scheme == "https" else 80),
        )

    if denied_hosts:
        for entry in denied_hosts:
            if _match(entry, hostname, resolved_ips):
                raise URLPolicyError(
                    f"URL host {hostname!r} matches denied_hosts entry {entry!r}: {url!r}"
                )

    if allowed_hosts:
        if not any(_match(entry, hostname, resolved_ips) for entry in allowed_hosts):
            raise URLPolicyError(
                f"URL host {hostname!r} is not in allowed_hosts {list(allowed_hosts)}: {url!r}"
            )

    if block_private_networks:
        if resolved_ips is None:
            raise URLPolicyError(
                f"Cannot resolve {hostname!r} to verify block_private_networks policy: {url!r}"
            )
        for ip in resolved_ips:
            if _ip_matches_networks(ip, _PRIVATE_NETWORKS):
                raise URLPolicyError(
                    f"URL host {hostname!r} resolves to private/special address {ip} "
                    f"(block_private_networks=true): {url!r}"
                )


# 凭据类请求头：跨源重定向时必须剥离（2026-10 评审 C1：restricted 逐跳手动
# 跟随原先原样透传全部头，Authorization/Cookie 会泄漏给重定向目标）。
# 语义对齐 RFC 7231 §9.4 与浏览器/requests 的 rebuild_auth——同源（host
# 未变）重定向保留全部头。
_SENSITIVE_REDIRECT_HEADERS = frozenset({
    "authorization", "cookie", "cookie2",
    "proxy-authorization", "proxy-authenticate", "www-authenticate",
})


def _strip_sensitive_headers(headers: Dict[str, str], from_url: str, to_url: str) -> Dict[str, str]:
    """重定向跨源（host 变化）时剥离凭据类头；同源原样返回。"""
    if urlparse(from_url).hostname == urlparse(to_url).hostname:
        return headers
    return {
        k: v for k, v in headers.items()
        if k.lower() not in _SENSITIVE_REDIRECT_HEADERS
    }


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
        # 访问策略（2026-09 安全评审 P1-1）。restrictions_active 时重定向改为
        # 逐跳校验后手动跟随，防止 302 把请求带进被禁网段。
        self.request_timeout = request_timeout
        self.allowed_hosts = allowed_hosts
        self.denied_hosts = denied_hosts
        self.block_private_networks = block_private_networks
        self.max_redirects = max_redirects

    @property
    def _restrictions_active(self) -> bool:
        return bool(self.allowed_hosts or self.denied_hosts or self.block_private_networks)

    def _check_policy(self, url: str) -> None:
        _host_allowed(url, self.allowed_hosts, self.denied_hosts,
                      self.block_private_networks, resolver=_cached_resolve_host)

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
        if self.c is None:
            # 进程级共享连接池 session（fork 后重建、cookie 默认阻断）——
            # 冷路径（仅 Parallel sync 分支桥走到），见 plaita.core.http_session。
            self.c = get_shared_sync_session()

        request = self.new_request(ctx)
        if request is None:
            return None, Exception("Failed to create request")

        try:
            if self._restrictions_active:
                self._check_policy(request.url)
            # restricted 时首跳必须禁自动重定向：requests 的 resolve_redirects
            # 会在 send 内部跟完 302，下面的逐跳策略校验就全部失效
            # （2026-10 评审发现的现状 bug，非本次共享 session 引入）。
            response = self.c.send(request, timeout=self.request_timeout,
                                   allow_redirects=not self._restrictions_active)
            if self._restrictions_active:
                # 逐跳校验的重定向：默认自动跟随会让 302 把请求带进被禁网段
                hops = 0
                while response.is_redirect and hops < self.max_redirects:
                    next_url = urljoin(response.url, response.headers.get("Location", ""))
                    self._check_policy(next_url)
                    method = self.method
                    if response.status_code in (301, 302, 303) and method.upper() != "HEAD":
                        method = "GET"
                    # 跨源重定向剥离凭据类头（host/content-length 恒剥离）
                    hop_headers = _strip_sensitive_headers(
                        request.headers, response.url, next_url)
                    request = requests.Request(
                        method=method, url=next_url,
                        headers={k: v for k, v in hop_headers.items()
                                 if k.lower() not in ("host", "content-length")},
                    ).prepare()
                    response = self.c.send(request, timeout=self.request_timeout, allow_redirects=False)
                    hops += 1
                if response.is_redirect:
                    return HttpResponse(raw_request=request), Exception(
                        f"Too many redirects (> {self.max_redirects})"
                    )
        except URLPolicyError as e:
            return HttpResponse(raw_request=request), e
        except Exception as e:
            return HttpResponse(raw_request=request), e
        
        try:
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
        except Exception as e:
            return HttpResponse(raw_request=request, raw_response=response), e

    async def handle_request_async(self, ctx):
        """处理HTTP请求（异步，使用 aiohttp）。

        返回 (AsyncHttpResponse, error) 与同步版本保持一致的签名。
        ``AsyncHttpResponse`` 是仅用于 async 路径的轻量包装，字段名与
        ``HttpResponse`` 相同，让 ``HTTP.execute`` 的结果处理代码可复用。

        2026-10 起 session 复用策略：flow 驱动层（``core.async_utils``）为
        每次 flow run 开一个共享 ``ClientSession``（contextvar 传递，确定性
        关闭），本方法优先复用；脱离 flow 驱动的直接调用退回一次性会话。
        """
        if aiohttp is None:
            _require_http()

        url, headers, data = self._build_request_params()

        session = get_flow_session()
        if session is None or session.closed:
            async with _new_oneshot_session() as oneshot:
                return await self._send_async(oneshot, url, headers, data)
        return await self._send_async(session, url, headers, data)

    async def _send_async(self, session, url, headers, data):
        """在给定 session 上执行请求（含 restricted 时的逐跳重定向校验）。

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
            follow = not self._restrictions_active
            current_url, current_method = url, self.method
            for hop in range(self.max_redirects + 1):
                async with session.request(
                    method=current_method,
                    url=current_url,
                    headers=headers,
                    data=data if hop == 0 else None,
                    timeout=timeout,
                    allow_redirects=follow,
                ) as response:
                    if response.status in (301, 302, 303, 307, 308) and not follow:
                        next_url = urljoin(str(response.url), response.headers.get("Location", ""))
                        self._check_policy(next_url)
                        current_method = self.method
                        if response.status in (301, 302, 303) and current_method.upper() != "HEAD":
                            current_method = "GET"
                        # 跨源重定向剥离凭据类头（后续跳沿用裁剪后的头）
                        headers = _strip_sensitive_headers(
                            headers, str(response.url), next_url)
                        current_url = next_url
                        # 快照跟随实际尝试的下一跳（错误帧里带真实 url/headers）
                        raw_request = HttpRequestInfo(
                            method=current_method, url=current_url,
                            headers=headers, body=None,
                        )
                        continue
                    text = await response.text()
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
        description="解析目标并对每个地址拒绝回环/内网/链路本地等私网段（多租户建议开启）",
    )
    max_redirects: int = Field(
        5, alias="maxRedirects",
        description="重定向跟随上限（策略激活时逐跳复检）",
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
            # 响应上下文经公开 API 写入 $NODE.<id>.*，下游可引用
            # （$NODE.<id>.RESPONSE / .HEADERS / .STATUS）。历史方法
            # set_node_context 在运行时里从未存在，真实请求一到响应处理
            # 就 AttributeError——集成测试全用带该方法的 mock execution，
            # 掩盖了这条主路径。
            prefix = getattr(execution, "express_prefix", "$") or "$"
            node_prefix = f"{prefix}NODE.{self.id}."
            execution.set_state(f"{node_prefix}{RESPONSE_CTX_KEY}", response)
            execution.set_state(f"{node_prefix}{HEADER_CTX_KEY}", dict(http_rsp.raw_response.headers))
            execution.set_state(f"{node_prefix}{STATUS_CTX_KEY}", http_rsp.raw_response.status_code)
        if self.output is None:
            return response[RESPONSE_DATA_KEY]
        return execution.evaluate(self.output)

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
            raise Exception(f"Failed to parse body: {str(e)}")
    
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
            raise Exception(f"Failed to parse query: {str(e)}")
    
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
            raise Exception(f"Failed to parse headers: {str(e)}")
    
    def parse_url(self, ctx):
        """解析URL"""
        try:
            url_value = evaluate(self.url, ctx)
            return str(url_value)
        except Exception as e:
            raise Exception(f"Failed to parse URL: {str(e)}")
    
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
        """
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