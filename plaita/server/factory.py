"""
统一的存储组件和事件总线工厂

生产控制面路径：execution / flow / EventFilter 队列侧以 **redis**（或 memory 单测）为准。
``db`` / SQLAlchemy 标记为 experimental，需显式
``PLAITA_ALLOW_EXPERIMENTAL_DB=1`` 才可创建（且 execution/flow 的 db 仍因 sync/async
契约断裂而被拒绝）。
"""
import logging
import os
from typing import NamedTuple, Optional
from urllib.parse import parse_qs, quote, unquote, urlsplit

logger = logging.getLogger("plaita.server.factory")

_EXPERIMENTAL_DB_ENV = "PLAITA_ALLOW_EXPERIMENTAL_DB"

#: 「未配置 Redis」时的文档化默认端点（CLI/env 默认值同此，见 flow_worker /
#: event_filter 的 ``--redis-url``）。**只有**「没配」走它；「配错了」不走。
DEFAULT_REDIS_URL = "redis://localhost:6379/0"

#: 支持的 scheme——与队列/事件总线侧 ``redis.Redis.from_url`` 的白名单一致。
#: 解析器必须与之一致，否则同一个 URL 在「存储」与「队列」之间再次分叉
#: （plaita#88 的根因正是存储侧另有一套解析）。
_REDIS_SCHEMES = ("redis", "rediss", "unix")

#: 错误消息/日志里密码的固定掩码——凭据唯一允许出现的形态。
_REDACTED = "***"

#: authority 的结束符：与 ``urlsplit`` 的 netloc 语义一致（`://` 之后到首个
#: ``/`` / ``?`` / ``#``）。注意这**不是**正则能表达的：口令里可以合法出现
#: ``@``、空格、``:`` 等，唯一的权威定界是「authority 里**最后**一个 ``@`` 是
#: userinfo 分隔符，它之前**第一个** ``:`` 才是用户/口令分界」——这正是 r2 的
#: 正则 ``[^/@\s:]*:[^/@\s]*@`` 掩不住
#: ``redis://user:@Pr0d2024x@redis.internal:0`` / ``redis://user:p@ssword@host``
#: / ``redis://user:Super Secret@host`` 的原因（plaita#88 r3）。
_AUTHORITY_DELIMITERS = "/?#"
#: 放宽一档的定界（只认 ``/``）：覆盖「口令里写了裸 ``#``/``?``」的畸形形态
#: （``redis://:pw#x@host:6379/0``）——``urlsplit`` 会把它当 fragment，但用户
#: 意图明显是凭据。
_PATH_DELIMITERS = "/"

#: percent-escape 的十六进制字母大小写两种拼法都要认（``%2F`` / ``%2f``）。
_HEX_LOWER = str.maketrans("ABCDEF", "abcdef")


def _delimiter_stop(text: str, origin: int, delimiters: str) -> int:
    """``origin`` 起首个 ``delimiters`` 字符的位置（没有则 ``len(text)``）。"""
    stop = len(text)
    for ch in delimiters:
        idx = text.find(ch, origin)
        if idx != -1 and idx < stop:
            stop = idx
    return stop


def _scan_last_credential_at(text: str, origin: int, stop: int, hard_query_at: int = -1):
    """在 ``text[origin:stop]`` 里找**最后**一个凭据形状的 ``@``。

    段边界是 ``/``（镜像 authority/path 段结构）。段内要有 ``:``，且该 ``:`` 必须
    在段内首个 ``?``/``#`` **之前**——``redis://:p?w@host``（``?`` 在口令里）因此
    命中，而 ``redis://host/0?x=1:2@y``（query 里的 ``:`` 在 ``?`` 之后）不命中。

    ``hard_query_at``（首个 ``?``/``#`` 的全局位置，-1 = 没有）再加一档：**先**看
    它之前的候选（那才是 authority 侧的 userinfo），只有它之前一个都没有时，才
    接受它之后、且**用户名段为空**（``colon_at == seg_start``）的候选
    （``redis://:p?w@host``）——否则 ``...?db=p@ssword`` 这种「口令重复写在 query
    值里」的 ``@`` 会被误当 userinfo，把真口令挤出掩码之外。

    单趟左→右扫描，O(n)、无回溯。返回 ``(at, seg_start)``；没有 ``(-1, -1)``。
    """
    seg_start = origin
    colon_at = -1
    query_at = -1
    best_at = -1
    best_start = -1
    late_at = -1
    late_start = -1
    i = origin
    while i < stop:
        ch = text[i]
        if ch == "/":
            seg_start = i + 1
            colon_at = -1
            query_at = -1
        elif ch == ":":
            if colon_at == -1:
                colon_at = i
        elif ch == "?" or ch == "#":
            if query_at == -1:
                query_at = i
        elif ch == "@" and colon_at != -1 and (query_at == -1 or colon_at < query_at):
            if hard_query_at == -1 or i < hard_query_at:
                best_at, best_start = i, seg_start
            elif (
                colon_at == seg_start
                or query_at == -1
                # ``:`` 与 ``?``/``#`` 之间是纯数字 ⇒ 那是 authority 的**端口**
                # 冒号（``host:6379?ref=a@b``），不是 userinfo 的用户/口令分界；
                # ``:p#w@``（'#' 在口令里）之间是 ``p`` ⇒ 认。
                or not text[colon_at + 1 : query_at].isdigit()
            ):
                late_at, late_start = i, seg_start
        i += 1
    if best_at != -1:
        return best_at, best_start
    return late_at, late_start


def _locate_credentials(text: str):
    """结构化定位 userinfo：``(start, at, user, password)``；不是 userinfo 则 None。

    规则（**不依赖输入是否合法**，全部单趟线性扫描，无正则/无回溯）：

    1. `anchor = 全串最后一个 ``@```——它是唯一候选 userinfo 分隔符；
    2. ``anchor`` 之前若有 ``://``（scheme 大小写无关），凭据从它之后找；否则从
       串首找——缺 scheme 的 ``user:pw@host``、单斜杠 ``redis:/user:pw@host``、
       凭据在 scheme 前的 ``user:pw@redis://host`` 都落在这里；
    3. 有 ``://`` 时按三档收紧到放宽：
       * **(1) 规整 authority**（首个 ``/?#`` 之前）里有 ``@`` → 那就是 userinfo
         （``redis://user:pw@host/0@x`` 里 path 的 ``@`` 顶不掉它）；这一段认不出
         ``:`` 也返回 ``(start, at, None, None)``，由调用方 fail-closed 抹成
         ``***``（``redis://user@host`` → ``redis://***@host``）；
       * **(2) 段边界放宽到首个 ``/``**，取最后一个「所在段含 ``:``」的 ``@``——
         覆盖裸 ``#``/``?`` 落在口令里的畸形形态（``redis://:pw#x@host``）；要求含
         ``:`` 是为了不把 ``redis://host?ref=a@b`` 的 query ``@`` 误当 userinfo；
       * **(3) 再看 path 首段**（扫到 ``?``/``#`` 之前）——覆盖空 authority 多写
         一个斜杠的形态（``redis:///user:pw@host``）；段里没有 ``:`` 的 ``@`` 就是
         path/query 的（``redis://host:6379/0@x``、``.../0?ref=a@b``）→ 返回 None，
         原样保留；
    4. 缺 ``://`` 的畸形输入**不受**首个 ``/?#`` 限制（r3 的漏点正在这里：
       ``redis:/user:pw@host`` 的 authority 只到 ``redis:``，一个凭据都认不出，
       于是原样回显）：先找最后一个「段里含 ``:``」的 ``@``（``redis:/`` 当 scheme
       前缀保留，``user:pw`` 才是凭据候选）；扫不到就把**整段前缀**当凭据
       （口令本身含 ``/`` 的畸形态 ``redis:/u@u:p/w@/0@x``，只多掩不少掩）；
       连 ``:`` 都没有（``foo@bar``）则 fail-closed：把 anchor 前最后一段抹成
       ``***``；
    5. userinfo 按**第一个** ``:`` 分用户/口令（口令整体返回，含 ``@``/空格/``:``）；
       没有 ``:`` 时 ``user`` 与 ``password`` 都是 None。
    """
    anchor = text.rfind("@")
    if anchor == -1:
        return None
    sep = text.rfind("://", 0, anchor)
    if sep != -1:
        scheme_end = sep + 3
        # (1) 规整 authority（首个 ``/?#`` 之前）：里面有 ``@`` 且认得出 ``:``
        #     就是 userinfo（``redis://user:pw@host/0@x`` 里 path 的 ``@`` 顶不掉它）。
        auth_stop = _delimiter_stop(text, scheme_end, _AUTHORITY_DELIMITERS)
        at_auth = text.rfind("@", scheme_end, auth_stop)
        if at_auth != -1 and ":" in text[scheme_end:at_auth]:
            at, origin = at_auth, scheme_end
        else:
            # (2) 段边界放宽到首个 ``/``：覆盖裸 ``#``/``?``/``@`` 落在口令里
            #     （``redis://user:p?w@host``、``redis://:pw#x@host``、
            #     ``REDIS://u@u:p?w@host``）——r3 的放宽就是这个语义。
            at, origin = _scan_last_credential_at(
                text, scheme_end, _delimiter_stop(text, scheme_end, _PATH_DELIMITERS)
            )
            if at == -1:
                # (3) 全串（到 ``?``/``#`` 前优先）：覆盖空 authority 多写斜杠
                #     （``redis:///user:pw@host``）。段里没有 ``:`` 的 ``@`` 就是
                #     path/query 的（``redis://host:6379/0@x``、``.../0?ref=a@b``）。
                at, origin = _scan_last_credential_at(
                    text,
                    scheme_end,
                    len(text),
                    hard_query_at=_delimiter_stop(text, scheme_end, "?#"),
                )
            if at == -1:
                if at_auth == -1:
                    return None
                # authority 里的 ``@`` 认不出 ``:`` → fail-closed 抹整段
                # （``redis://user@host:6379`` → ``redis://***@host:6379``）。
                at, origin = at_auth, scheme_end
    else:
        # 缺 ``://`` 的畸形输入：锚点仍是全串最后一个 ``@``。先扫凭据形状的段
        # （``redis:/user:pw@host`` → ``user:pw``）；扫不到再把整段前缀当凭据
        # （口令含 ``/`` 的畸形态），连 ``:`` 都没有才是纯 ``foo@bar``——
        # fail-closed 抹掉 userinfo 候选。
        at, origin = _scan_last_credential_at(
            text, 0, len(text), hard_query_at=_delimiter_stop(text, 0, "?#")
        )
        if at == -1:
            if ":" in text[:anchor]:
                origin, at = 0, anchor
            else:
                cut = -1
                for ch in _AUTHORITY_DELIMITERS:
                    idx = text.rfind(ch, 0, anchor)
                    if idx > cut:
                        cut = idx
                origin, at = cut + 1, anchor
    userinfo = text[origin:at]
    if ":" not in userinfo:
        return origin, at, None, None
    user, password = userinfo.split(":", 1)
    return origin, at, user, password


def _lower_hex(encoded: str) -> str:
    """把 ``%4A`` 式转义的十六进制字母改成小写（``%4a``）——URL 里两种拼法都合法。"""
    out = []
    i = 0
    while i < len(encoded):
        if encoded[i] == "%" and i + 2 < len(encoded):
            out.append("%")
            out.append(encoded[i + 1 : i + 3].translate(_HEX_LOWER))
            i += 3
        else:
            out.append(encoded[i])
            i += 1
    return "".join(out)


def _encoding_forms(secret: str) -> frozenset:
    """一个字符串的三种书写形态：原文 / percent-decoded / percent-encoded。"""
    forms = {secret}
    decoded = unquote(secret)
    if decoded:
        forms.add(decoded)
        encoded = quote(decoded, safe="")
        forms.add(encoded)
        forms.add(_lower_hex(encoded))
    raw_encoded = quote(secret, safe="")
    forms.add(raw_encoded)
    forms.add(_lower_hex(raw_encoded))
    forms.discard("")
    return frozenset(forms)


def _secret_forms(secret: str) -> tuple:
    """一个口令候选的**所有书写形态**（原文 / percent-decoded / percent-encoded）。

    兜底重洗（见 ``_mask_url``）要认出「同一个口令换个写法又出现在 query/path
    里」——``?socket_timeout=@Pr0d2024x`` 与 ``?socket_timeout=%40Pr0d2024x``
    都必须能被洗掉。按长度降序返回：长形态先替换，避免短形态把长形态切碎。

    结构定位若多吞了一段（畸形态里口令本身含 ``/``/``@``：候选 ``/:p/w``、
    ``u@u:a\nb``），真正的口令往往是**第一个** ``:``/``@`` 之后那截——把它连同
    它的编码形态一起纳入（只多洗、不少洗）。
    """
    if not secret:
        return ()
    forms = set(_encoding_forms(secret))
    for sep in (":", "@"):
        cut = secret.find(sep)
        if cut != -1:
            forms |= _encoding_forms(secret[cut + 1 :])
    forms.discard("")
    return tuple(sorted(forms, key=len, reverse=True))


def _redact_residuals(masked: str, start: int, at_masked: int, forms: tuple) -> str:
    """兜底重洗：结构层识别到的口令若又出现在别处，一律抹掉。

    只洗两个**凭据面**：``[start, at_masked)``（userinfo 本身，覆盖「用户名 ==
    口令」）与 host 之后的 path/query/fragment。scheme 与 host 原样保留——它们是
    结构性诊断信息，且可能是口令的子串（口令 ``redis``、host ``redis.internal``）；
    无差别全局替换会把 scheme 抹成 ``***://``（假红），没有任何安全收益。
    """
    def scrub(segment: str) -> str:
        for form in forms:
            if form in segment:
                segment = segment.replace(form, _REDACTED)
        return segment

    userinfo = masked[start:at_masked]
    rest = masked[at_masked + 1 :]  # '@' 之后：host + path/query/fragment
    host_end = len(rest)
    for ch in _AUTHORITY_DELIMITERS:
        idx = rest.find(ch)
        if idx != -1 and idx < host_end:
            host_end = idx
    return (
        masked[:start]
        + scrub(userinfo)
        + "@"
        + rest[:host_end]
        + scrub(rest[host_end:])
    )


def _mask_url(value: object) -> str:
    """把 URL 渲染成可安全写进异常/日志的形态：口令一律 ``***``。

    ``redis://user:sup3rsecret@host:6379/0`` → ``redis://user:***@host:6379/0``。
    口令里带 ``@`` / 空格 / tab / nbsp / ``:`` / ``%`` / unicode 等（r2 正则的漏网
    形态）同样整体抹掉；scheme/host/port/db/path 原样保留，消息仍可诊断。

    **cannot-leak by construction**（安全性不依赖输入形态是否合法）：

    * **结构层**：``_locate_credentials`` 以全串最后一个 ``@`` 为锚，覆盖单斜杠
      ``redis:/user:pw@host``、大写 ``REDIS:/...``、凭据在 scheme 前
      ``user:pw@redis://host`` 等畸形形态；认不出 userinfo 时 fail-closed——
      userinfo 整体抹成 ``***``（认得出用户名则保留 ``user:***`` 以便诊断），
      绝不原样返回。
    * **兜底层**：把结构层**识别到的口令**的所有书写形态（原文 / percent-encoded /
      decoded）在 userinfo 段与 path/query/fragment 里再洗一遍——口令重复写在
      ``?socket_timeout=PwX`` 或 ``/PwX`` 里也活不下来。这一层刻意不碰 scheme 与
      host（见 ``_redact_residuals`` 的假红说明）。

    ``_parse_redis_url`` 的**所有**拒绝分支只允许用它渲染原始输入：``RedisUrlError``
    会被 ``flow_worker`` / ``event_filter`` 的 ``logger.error("...: %s", e)`` 原样写进
    生产日志（后者还带 ``exc_info=True``——``from exc`` 的 cause 是**另一个独立渲染
    面**，见 ``_sanitized_cause``）。
    """
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", "replace")
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"

    credentials = _locate_credentials(value)
    if credentials is None:
        return value  # 没有可锚定的 userinfo：不存在凭据（path/query 的 '@' 不算）
    start, at, user, password = credentials
    if password is None:
        # fail-closed：认不出用户/口令分界，整段 userinfo 当凭据处理（连兜底层
        # 的 needle 也用它），否则 ``unix://p?w@host:6379/p?w`` 这种「无 ':' 的
        # userinfo + path 里重复一遍」会漏。
        replacement = _REDACTED
        forms = _secret_forms(value[start:at])
    else:
        replacement = f"{user}:{_REDACTED}"
        forms = _secret_forms(password)
    masked = f"{value[:start]}{replacement}@{value[at + 1:]}"

    if not forms:
        return masked
    return _redact_residuals(masked, start, start + len(replacement), forms)


def _passwords(text: str) -> tuple:
    """原始 URL 里出现的口令字面量（原文 + percent-decoded + encoded 形态）。

    与 ``_mask_url`` 同源（同一套结构化定位），所以带 ``@``/空格/裸 ``#`` 的口令
    也能被完整取出——r2 的正则只取到 ``@`` 前一小截，``_safe_reason`` 因此漏洗。
    认不出用户名的 fail-closed 形态把整段 userinfo 当口令处理（宁可多洗）。
    """
    if not isinstance(text, str):
        return ()
    credentials = _locate_credentials(text)
    if credentials is None:
        return ()
    start, at, _user, password = credentials
    if password is None:
        return _secret_forms(text[start:at])
    return _secret_forms(password)


def _safe_reason(exc: BaseException, text: str) -> str:
    """底层异常的**安全**渲染（只用在嵌套 cause 的位置）。

    不假设 ``urlsplit``/``int``/``float`` 的报错文案里没有整条 URL 或口令：
    先掩码裸 URL、再抹掉口令的所有书写形态，然后才允许进入消息/traceback。
    """
    reason = str(exc)
    for secret in _passwords(text):
        reason = reason.replace(secret, _REDACTED)
    # 先洗口令字面量、再掩 URL：反过来的话 ``could not convert ...: 'p@ssword'``
    # 会被 _mask_url 从中间切开（``...float:***@ssword'``），留下 ≥3 后缀。
    return _mask_url(reason)


def _scrub(value: object, text: str) -> str:
    """把 ``text`` 里识别到的口令从**任意消息字段**里抹掉。

    ``?<口令>=1``（口令写在 query **名**里）、``?db=<口令>`` / ``/<口令>``
    （写在**值**里）这些字段会被拒绝消息原样 ``{...!r}`` 回显——它们不是 URL，
    不能靠 ``_mask_url``，只能按同一套 ``_passwords`` 形态洗。
    """
    text_value = str(value)
    for secret in _passwords(text):
        text_value = text_value.replace(secret, _REDACTED)
    return text_value


def _sanitized_cause(exc: BaseException, text: str) -> BaseException:
    """底层异常的**安全替身**：``from exc`` 的 ``__cause__`` 是对象，会被
    ``traceback`` / ``exc_info=True`` **独立渲染**——只把文案洗进
    ``RedisUrlError`` 的消息不够（plaita#88 r4 Finding 2）。

    返回同类型、只带 ``_safe_reason`` 文案的新异常；它没有 traceback，所以链上
    只剩洗过的消息，原始 URL/口令不会随 ``__cause__`` 落盘。
    """
    safe = _safe_reason(exc, text)
    try:
        return type(exc)(safe)
    except Exception:  # pragma: no cover - 构造签名特殊的异常类型
        return ValueError(safe)


class RedisUrlError(ValueError):
    """Redis URL 具名失败：畸形/不支持的 URL 抛它，绝不静默回落 localhost。

    继承 ``ValueError`` 以贴合「坏配置」的既有语义；关键区别是**带上 URL 与
    原因**，且在建立任何连接之前抛出（fail-closed，启动期一次性暴露）。
    URL 一律是 ``_mask_url`` 渲染过的形态（密码 ``***``）——本异常会被
    ``flow_worker`` / ``event_filter`` 直接写进生产日志。
    """


class RedisTarget(NamedTuple):
    """``_parse_redis_url`` 的解析结果：存储侧连接参数的唯一来源。"""

    host: Optional[str]
    port: int
    db: int
    password: Optional[str]
    username: Optional[str]
    scheme: str
    unix_socket_path: Optional[str] = None
    #: URL query 里 redis-py 认识的连接参数（如 ``socket_timeout``）——原样透传给
    #: ``redis.Redis(**kwargs)``，保证存储侧与队列侧 ``Redis.from_url`` 同参数。
    query_options: Optional[dict] = None

    @property
    def ssl(self) -> bool:
        """``rediss://`` → TLS。历史缺陷把 TLS 目标改写成明文 localhost。"""
        return self.scheme == "rediss"

    def client_kwargs(self) -> dict:
        """``redis.Redis(...)`` 连接参数（与队列 ``Redis.from_url`` 同端点）。"""
        kwargs: dict = {"db": self.db}
        if self.username is not None:
            kwargs["username"] = self.username
        if self.password is not None:
            kwargs["password"] = self.password
        if self.query_options:
            kwargs.update(self.query_options)
        if self.unix_socket_path is not None:
            kwargs["unix_socket_path"] = self.unix_socket_path
            return kwargs
        kwargs["host"] = self.host
        kwargs["port"] = self.port
        if self.ssl:
            kwargs["ssl"] = True
        return kwargs


#: 存储侧**透传**给 ``redis.Redis(**kwargs)`` 的 URL query 参数。名字与队列侧
#: ``redis.connection.URL_QUERY_ARGUMENT_PARSERS`` 同名（``db`` 单独处理），
#: 取值也用它自己的解析器转换——同一个 URL 在存储与队列必须得到同一个值。
_QUERY_PARAMS = frozenset({
    "health_check_interval",
    "legacy_responses",
    "max_connections",
    "protocol",
    "retry_on_error",
    "retry_on_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_read_size",
    "socket_timeout",
    "ssl_check_hostname",
    "ssl_exclude_verify_flags",
    "ssl_include_verify_flags",
    "ssl_min_version",
})

#: redis-py 的 ``parse_url`` 认、客户端却**用不了**的参数：``?timeout=3`` 会被塞进
#: connection_kwargs，而 ``AbstractConnection`` 没有 ``timeout`` 形参——``from_url``
#: 本身能构造，第一次连接就 ``TypeError``。与其把它传下去变成连接期炸，不如启动期
#: 具名拒绝并指向 ``socket_timeout``。这是与 ``parse_url`` 之间**唯一**保留的
#: query 分歧（见 MIGRATION.md）。
_UNSUPPORTED_QUERY_PARAMS = frozenset({"timeout"})


def _coerce_query_value(key: str, value: str, text: str) -> object:
    """已知 query 参数取值：用 redis-py 的解析器转换，非法即具名拒绝（掩码消息）。"""
    try:
        from redis.connection import URL_QUERY_ARGUMENT_PARSERS
    except ImportError:  # pragma: no cover - 没装 redis 时 storage_type=redis 本就不可用
        return value
    parser = URL_QUERY_ARGUMENT_PARSERS.get(key)
    if parser is None:  # pragma: no cover - _QUERY_PARAMS 与 redis-py 同源，防漂移
        return value
    try:
        return parser(value)
    except (TypeError, ValueError) as exc:
        raise RedisUrlError(
            f"Redis URL 的 query 参数 {_scrub(key, text)!r} 取值非法: "
            f"{_safe_reason(exc, text)}（URL={_mask_url(text)!r}）"
        ) from _sanitized_cause(exc, text)


def _parse_query_options(query: dict, text: str) -> dict:
    """URL query → 客户端 kwargs（与队列侧 ``Redis.from_url`` 对齐）。

    * 已知参数（``_QUERY_PARAMS``）→ 按 redis-py 语义转换后透传；值非法 → 具名拒绝；
    * ``?timeout=`` → 具名拒绝（见 ``_UNSUPPORTED_QUERY_PARAMS``）；
    * 其余名字 → **不判死**（``?foo=bar`` 曾让生产启动直接崩）：掩码警告后忽略。
      它们无法透传——``redis.Redis.__init__`` 没有 ``**kwargs``；而队列侧也只是把未知
      名字塞进 connection_kwargs、第一次连接同样 ``TypeError``，故「忽略」不比队列侧
      更差，且不再炸启动。
    """
    options: dict = {}
    unknown = []
    for key, values in query.items():
        if key == "db" or not values:
            continue
        value = values[0]
        if key in _UNSUPPORTED_QUERY_PARAMS:
            raise RedisUrlError(
                f"Redis URL 的 query 参数 {_scrub(key, text)!r} 不被 Redis 客户端支持"
                f"（连接期会 TypeError，请改用 socket_timeout）: {_mask_url(text)}"
            )
        if key in _QUERY_PARAMS:
            options[key] = _coerce_query_value(key, value, text)
        else:
            unknown.append(key)
    if unknown:
        logger.warning(
            "Redis URL 含 redis-py 未定义的 query 参数 %s，已按 parse_url 语义忽略: %s",
            sorted(_scrub(key, text) for key in unknown),
            _mask_url(text),
        )
    return options


def _redis_db_from_url(
    path: Optional[str], db_query: Optional[str], shown: str, text: str
) -> int:
    """URL 路径/``db`` 查询参数 → 数据库编号（非法即具名失败）。

    ``db`` 查询参数优先于路径（与 ``Redis.from_url`` 同序）；``/`` 与空路径
    都表示默认 db 0。``path=None`` 用于 ``unix://``——socket 路径不是 db，
    此时 db 只认查询参数（缺省 0）。

    ``shown`` 必须是 ``_mask_url`` 处理过的 URL——本函数的消息会进生产日志。
    ``text`` 是**原始**输入：``int()`` 失败的 cause 是另一个渲染面，要用它把
    口令从 ``_sanitized_cause`` 里洗掉（``shown`` 已被掩码，取不出口令）。
    """
    if db_query:
        try:
            db = int(db_query)
        except ValueError as exc:
            raise RedisUrlError(
                f"Redis URL 的 db 查询参数不是整数: {_scrub(db_query, text)!r}"
                f"（URL={shown!r}）"
            ) from _sanitized_cause(exc, text)
    elif path is None or path in ("", "/"):
        db = 0
    elif path.startswith("/") and path[1:].isascii() and path[1:].isdigit():
        db = int(path[1:])
    else:
        raise RedisUrlError(
            f"Redis URL 的路径必须是数据库编号（形如 /0）: {_scrub(path, text)!r}"
            f"（URL={shown!r}）"
        )
    if db < 0:
        raise RedisUrlError(f"Redis URL 的数据库编号不能为负: {db}（URL={shown!r}）")
    return db


def _parse_redis_url(redis_url: Optional[str] = None) -> RedisTarget:
    """解析 Redis URL 为连接目标（host/port/db/username/password/scheme）。

    契约（plaita#88）：

    * **未配置**（``None`` / 空串 / 纯空白）→ 文档化默认
      ``DEFAULT_REDIS_URL``（``redis://localhost:6379/0``）。「没配 Redis」不是
      错误，保持既有行为。
    * **合法 URL** → 完整解析，且结果与队列侧 ``Redis.from_url`` 同端点：
      ``redis://`` / ``rediss://``（``ssl=True``）/ ``unix://``；端口可省略
      （默认 6379）、db 可省略（默认 0）；``user:pass@`` 的用户名不再被当成
      host、密码不再丢失；凭据按 percent-encoding 解码；IPv6 字面量
      ``redis://[::1]:6379/0`` 正确取主机。
    * **畸形/不支持** → 抛 ``RedisUrlError``（带 URL 与原因，命名到具体形态）。
      绝不静默回落 localhost——那正是本缺陷：同一进程连两个 Redis。
      消息里的 URL **一律**经 ``_mask_url`` 渲染（密码 ``***``）：它会被
      ``flow_worker`` / ``event_filter`` 原样写进生产日志。
    * **query 参数** → redis-py 认识的那些（``socket_timeout`` /
      ``health_check_interval`` / ``max_connections`` / ``ssl_*`` 等）转换后透传给
      存储客户端，与队列侧同值；``?timeout=`` 因客户端不接受而具名拒绝；未知名字
      不再判死（掩码警告后忽略），见 ``_parse_query_options``。
    """
    raw = "" if redis_url is None else redis_url
    if not isinstance(raw, str):
        raise RedisUrlError(
            f"redis_url 必须是字符串，收到 {type(raw).__name__}: {_mask_url(raw)!r}"
        )
    text = raw.strip()
    if not text:
        # 显式声明的回退（未配置）：不能把「没配」变成硬错误（#99/#95 先例）。
        return RedisTarget("localhost", 6379, 0, None, None, "redis")

    shown = _mask_url(text)
    scheme_token = text.split("://", 1)[0] if "://" in text else ""
    if scheme_token != scheme_token.lower() or scheme_token not in _REDIS_SCHEMES:
        supported = " / ".join(f"{s}://" for s in _REDIS_SCHEMES)
        raise RedisUrlError(
            f"不支持的 Redis URL: {shown!r}；只支持 {supported}"
            f"（scheme 必须小写，缺 scheme 或 http:// 之类都不是 Redis URL）"
        )

    try:
        parts = urlsplit(text)
    except ValueError as exc:  # 非法 IPv6 字面量等
        raise RedisUrlError(
            f"Redis URL 非法（{_safe_reason(exc, text)}）: {shown!r}"
        ) from _sanitized_cause(exc, text)

    if parts.fragment:
        raise RedisUrlError(
            f"Redis URL 不支持 fragment: {shown!r}（凭据里的 '#' 请写成 %23）"
        )
    query = parse_qs(parts.query, keep_blank_values=True)
    # 空 options 归一成 None：与「未配置」早退分支的 RedisTarget 可逐字段比较。
    options = _parse_query_options(query, text) or None
    db_query = query.get("db", [None])[0]
    username = unquote(parts.username) if parts.username else None
    password = unquote(parts.password) if parts.password else None

    if scheme_token == "unix":
        socket_path = unquote(parts.path) if parts.path else None
        if not socket_path:
            raise RedisUrlError(f"unix:// URL 缺少 socket 路径: {shown!r}")
        db = _redis_db_from_url(None, db_query, shown, text)
        return RedisTarget(
            None, 0, db, password, username, "unix", socket_path, options
        )

    try:
        host = unquote(parts.hostname) if parts.hostname else None
        port = parts.port
    except ValueError as exc:  # 端口非整数 / 端口越界
        raise RedisUrlError(
            f"Redis URL 非法（{_safe_reason(exc, text)}）: {shown!r}"
        ) from _sanitized_cause(exc, text)

    if not host:
        raise RedisUrlError(f"Redis URL 缺少主机名: {shown!r}")
    if port is None:
        port = 6379
    if port == 0:
        raise RedisUrlError(f"Redis URL 的端口不能为 0: {shown!r}")
    db = _redis_db_from_url(parts.path, db_query, shown, text)
    return RedisTarget(host, port, db, password, username, scheme_token, None, options)


def _create_async_engine(database_url: str):
    """从 database_url 创建 SQLAlchemy 异步引擎（event/subscription db 后端需要）。"""
    if not database_url:
        raise ValueError("db 后端需要 database_url")
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(database_url)


def _require_experimental_db(feature: str) -> None:
    if os.environ.get(_EXPERIMENTAL_DB_ENV) == "1":
        logger.warning(
            "%s: enabling experimental db/sqlalchemy backend (%s=1)",
            feature,
            _EXPERIMENTAL_DB_ENV,
        )
        return
    raise ValueError(
        f"{feature} 的 type='db'（SQLAlchemy）为 experimental，不在生产控制面支持范围内。"
        f"请改用 redis（推荐）或 memory（单测）。"
        f"若仅为实验，设置环境变量 {_EXPERIMENTAL_DB_ENV}=1 后重试。"
    )


# FlowWorker / EventFilter 以同步方式调用 ExecutionStorage / FlowStorage。
# Sqlalchemy* 实现是 async def，与 ABC 契约断裂，经 factory 创建会导致静默丢状态。
_UNSUPPORTED_SYNC_DB_COMPONENTS = frozenset({"execution", "flow"})


def create_storage_component(storage_type, component_type, **kwargs):
    """
    创建存储组件实例

    Args:
        storage_type: 存储类型 (memory, redis, db)
            - execution / flow：仅 memory、redis（db 永久拒绝，契约断裂）
            - subscription：memory、redis；db 需 PLAITA_ALLOW_EXPERIMENTAL_DB=1
        component_type: 组件类型 (execution, flow, subscription)
        **kwargs: 组件初始化参数 (redis_url, database_url)

    Returns:
        创建的存储组件实例
    """
    if storage_type == "memory":
        if component_type == "execution":
            from plaita.storage.memory import MemoryExecutionStorage
            return MemoryExecutionStorage()
        elif component_type == "flow":
            from plaita.storage.memory import MemoryFlowStorage
            return MemoryFlowStorage()
        elif component_type == "subscription":
            from plaita.event.memory import InMemoryEventSubscriptionStorage
            return InMemoryEventSubscriptionStorage()

    elif storage_type == "redis":
        redis_url = kwargs.get("redis_url", DEFAULT_REDIS_URL)

        if component_type in ("execution", "flow"):
            # #88：解析在建立连接之前完成；畸形/不支持的 URL 在这里抛
            # RedisUrlError（具名、可操作），不再静默回落 localhost:6379/0
            # ——那会让存储与队列/事件总线连到两个 Redis。
            target = _parse_redis_url(redis_url)
            conn = target.client_kwargs()
            # 多租户路由包装器：按 ContextVar 租户选择 namespace
            # （default → 历史 plaita 前缀；其余 plaita:{tenant}）
            if kwargs.get("tenant_routing"):
                from plaita.server.tenant_context import (
                    TenantRoutingExecutionStorage,
                    TenantRoutingFlowStorage,
                )
                if component_type == "execution":
                    return TenantRoutingExecutionStorage(**conn)
                return TenantRoutingFlowStorage(**conn)
            if component_type == "execution":
                from plaita.storage.redis import RedisExecutionStorage
                return RedisExecutionStorage(**conn)
            else:
                from plaita.storage.redis import RedisFlowStorage
                return RedisFlowStorage(**conn)
        elif component_type == "subscription":
            from plaita.event.redis import RedisEventSubscriptionStorage
            return RedisEventSubscriptionStorage(redis_url=redis_url, key_prefix="plaita:subscription:")

    elif storage_type == "db":
        if component_type in _UNSUPPORTED_SYNC_DB_COMPONENTS:
            raise ValueError(
                f"storage_type='db' 不支持 component_type={component_type!r}："
                "SqlalchemyExecutionStorage / SqlalchemyFlowStorage 为 async 实现，"
                "与 ExecutionStorage/FlowStorage 同步 ABC 及 FlowWorker/EventFilter "
                "的同步调用不兼容，会导致状态无法落盘。"
                "请改用 storage_type='redis' 或 'memory'。"
            )
        _require_experimental_db(f"create_storage_component({component_type})")
        database_url = kwargs.get("database_url")
        if component_type == "subscription":
            from plaita.event.sqlalchemy import SqlalchemyEventSubscriptionStorage
            return SqlalchemyEventSubscriptionStorage(engine=_create_async_engine(database_url))
        raise ValueError(f"不支持的组件类型: {component_type}")
    else:
        raise ValueError(f"不支持的存储类型: {storage_type}")


def create_event_bus(bus_type, **kwargs):
    """
    创建事件总线实例

    Args:
        bus_type: 事件总线类型 (memory, redis, db)
            db 需 PLAITA_ALLOW_EXPERIMENTAL_DB=1；生产请用 redis
        **kwargs: 组件初始化参数 (redis_url, database_url)

    Returns:
        创建的事件总线实例
    """
    if bus_type == "memory":
        from plaita.event.memory import InMemoryEventBus
        return InMemoryEventBus()
    elif bus_type == "redis":
        from plaita.event.redis import RedisEventBus
        return RedisEventBus(redis_url=kwargs.get("redis_url"))
    elif bus_type == "db":
        _require_experimental_db("create_event_bus")
        from plaita.event.sqlalchemy import SqlalchemyEventBus
        return SqlalchemyEventBus(engine=_create_async_engine(kwargs.get("database_url")))
    else:
        raise ValueError(f"不支持的事件总线类型: {bus_type}")
