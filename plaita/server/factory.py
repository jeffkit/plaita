"""
统一的存储组件和事件总线工厂

生产控制面路径：execution / flow / EventFilter 队列侧以 **redis**（或 memory 单测）为准。
``db`` / SQLAlchemy 标记为 experimental，需显式
``PLAITA_ALLOW_EXPERIMENTAL_DB=1`` 才可创建（且 execution/flow 的 db 仍因 sync/async
契约断裂而被拒绝）。
"""
import logging
import os
import re
from typing import NamedTuple, Optional
from urllib.parse import parse_qs, unquote, urlsplit

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

#: URL userinfo（``user:password@``）的识别式。故意**不**用 ``urlsplit``：
#: 待掩码的恰恰是畸形 URL，``urlsplit`` 对它可能直接抛错（非法 IPv6 字面量），
#: 掩码自身绝不能失败。字符类允许 '#'/'?'，以覆盖「密码里写了裸 '#'」的形态。
_USERINFO_RE = re.compile(r"(?P<userinfo>[^/@\s:]*:[^/@\s]*)(?P<at>@)")


def _mask_url(value: object) -> str:
    """把 URL 渲染成可安全写进异常/日志的形态：密码一律 ``***``。

    ``redis://user:sup3rsecret@host:6379/0`` → ``redis://user:***@host:6379/0``。
    ``_parse_redis_url`` 的**所有**拒绝分支只允许用它渲染原始输入：``RedisUrlError``
    会被 ``flow_worker`` / ``event_filter`` 的 ``logger.error("...: %s", e)``
    原样写进生产日志（后者还带 ``exc_info=True``），裸 URL 等于把 Redis 口令落盘。
    """
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", "replace")
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"

    def _repl(match) -> str:
        userinfo = match.group("userinfo")
        if ":" not in userinfo:
            return match.group(0)  # 只有用户名：没有可泄露的密码
        return f"{userinfo.split(':', 1)[0]}:{_REDACTED}@"

    return _USERINFO_RE.sub(_repl, value, count=1)


def _passwords(text: str) -> tuple:
    """原始 URL 里出现的密码字面量（percent-encoded 原文 + 解码后形态）。"""
    match = _USERINFO_RE.search(text)
    if match is None or ":" not in match.group("userinfo"):
        return ()
    raw = match.group("userinfo").split(":", 1)[1]
    if not raw:
        return ()
    return tuple({raw, unquote(raw)})


def _safe_reason(exc: BaseException, text: str) -> str:
    """底层异常的**安全**渲染（只用在嵌套 cause 的位置）。

    不假设 ``urlsplit``/``int``/``float`` 的报错文案里没有整条 URL 或口令：
    先掩码裸 URL、再抹掉密码字面量，然后才允许进入消息/traceback。
    """
    reason = _mask_url(str(exc))
    for secret in _passwords(text):
        reason = reason.replace(secret, _REDACTED)
    return reason


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
            f"Redis URL 的 query 参数 {key!r} 取值非法: {_safe_reason(exc, text)}"
            f"（URL={_mask_url(text)!r}）"
        ) from exc


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
                f"Redis URL 的 query 参数 {key!r} 不被 Redis 客户端支持"
                f"（连接期会 TypeError，请改用 socket_timeout）: {_mask_url(text)}"
            )
        if key in _QUERY_PARAMS:
            options[key] = _coerce_query_value(key, value, text)
        else:
            unknown.append(key)
    if unknown:
        logger.warning(
            "Redis URL 含 redis-py 未定义的 query 参数 %s，已按 parse_url 语义忽略: %s",
            sorted(unknown),
            _mask_url(text),
        )
    return options


def _redis_db_from_url(path: Optional[str], db_query: Optional[str], shown: str) -> int:
    """URL 路径/``db`` 查询参数 → 数据库编号（非法即具名失败）。

    ``db`` 查询参数优先于路径（与 ``Redis.from_url`` 同序）；``/`` 与空路径
    都表示默认 db 0。``path=None`` 用于 ``unix://``——socket 路径不是 db，
    此时 db 只认查询参数（缺省 0）。

    ``shown`` 必须是 ``_mask_url`` 处理过的 URL——本函数的消息会进生产日志。
    """
    if db_query:
        try:
            db = int(db_query)
        except ValueError as exc:
            raise RedisUrlError(
                f"Redis URL 的 db 查询参数不是整数: {db_query!r}（URL={shown!r}）"
            ) from exc
    elif path is None or path in ("", "/"):
        db = 0
    elif path.startswith("/") and path[1:].isascii() and path[1:].isdigit():
        db = int(path[1:])
    else:
        raise RedisUrlError(
            f"Redis URL 的路径必须是数据库编号（形如 /0）: {path!r}（URL={shown!r}）"
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
        ) from exc

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
        db = _redis_db_from_url(None, db_query, shown)
        return RedisTarget(
            None, 0, db, password, username, "unix", socket_path, options
        )

    try:
        host = unquote(parts.hostname) if parts.hostname else None
        port = parts.port
    except ValueError as exc:  # 端口非整数 / 端口越界
        raise RedisUrlError(
            f"Redis URL 非法（{_safe_reason(exc, text)}）: {shown!r}"
        ) from exc

    if not host:
        raise RedisUrlError(f"Redis URL 缺少主机名: {shown!r}")
    if port is None:
        port = 6379
    if port == 0:
        raise RedisUrlError(f"Redis URL 的端口不能为 0: {shown!r}")
    db = _redis_db_from_url(parts.path, db_query, shown)
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
