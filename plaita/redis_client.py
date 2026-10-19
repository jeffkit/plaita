"""Redis 客户端构造的单一入口：**显式**连接/读超时预算（A2R20-05）。

为什么不能依赖 redis-py 默认值：``socket_timeout`` / ``socket_connect_timeout``
在 5.0.x 默认 ``None``（∞），8.x 默认 5s；本仓依赖声明是 ``redis>=4.0.0`` /
``>=5.0.0``（不固定主版本），于是「同一条命令的实际预算」随 ``pip install`` 时点
变化。半开 Redis（TCP accept 后不响应）下，未声明的客户端会永久阻塞启动与读面。

本模块把预算固定为显式值（与仓内既有口径对齐）：

============  ========  =====
场景            connect   read
============  ========  =====
worker / 服务    5s       30s   （队列 BLOCK 窗口上限 10s；见 task_queue）
console 交互     2s        5s   （``main.connect_redis`` 既有 2s + 同值读超时）
健康检查         3s        3s
============  ========  =====

优先级：**显式参数 > URL query > 环境变量 > 模块默认**。URL query 的优先级由
redis-py 的 ``from_url`` 决定（``kwargs.update(url_options)``），运维仍可用
``?socket_timeout=…`` 逐 URL 覆盖。环境变量只覆盖**未显式传参**的调用点：
``PLAITA_REDIS_CONNECT_TIMEOUT`` / ``PLAITA_REDIS_SOCKET_TIMEOUT``（正数秒）。
非法值不静默吞掉：WARNING + 回退默认值。

用法（禁止裸 ``Redis.from_url``）::

    from plaita.redis_client import make_redis_client
    client = make_redis_client(url, decode_responses=True)
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict

from plaita.core.url_redaction import _redact_env_value

logger = logging.getLogger(__name__)

#: worker / 扩展服务口径：与 ``plaita/node/redis.py`` 的 5s/30s 对齐。
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_SOCKET_TIMEOUT_SECONDS = 30.0

#: console 交互数据面口径：`main.connect_redis` 既有 connect=2s，read 取同仓 5s。
CONSOLE_CONNECT_TIMEOUT_SECONDS = 2.0
CONSOLE_SOCKET_TIMEOUT_SECONDS = 5.0

#: 健康检查/可达性探测：更短，避免探针本身拖住请求。
HEALTH_CONNECT_TIMEOUT_SECONDS = 3.0
HEALTH_SOCKET_TIMEOUT_SECONDS = 3.0

_CONNECT_ENV = "PLAITA_REDIS_CONNECT_TIMEOUT"
_SOCKET_ENV = "PLAITA_REDIS_SOCKET_TIMEOUT"


def _positive_seconds(value: Any) -> float | None:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


def _resolve_timeout(explicit: float | None, env_name: str, default: float) -> float:
    """显式参数 > env > 默认；非法 env 值 WARNING（降级必须可观测）。"""
    if explicit is not None:
        resolved = _positive_seconds(explicit)
        if resolved is not None:
            return resolved
        logger.warning("%s 需要正数秒，收到 %r；使用默认 %.1fs", "显式超时参数", explicit, default)
        return default

    raw = os.environ.get(env_name)
    if raw is not None and raw.strip() != "":
        resolved = _positive_seconds(raw)
        if resolved is not None:
            return resolved
        logger.warning(
            "%s=%s 非法（需正数秒），使用默认 %.1fs",
            env_name, _redact_env_value(raw), default,
        )
    return default


def redis_timeout_kwargs(
    *,
    connect_timeout: float | None = None,
    socket_timeout: float | None = None,
) -> Dict[str, float]:
    """redis-py 连接池超时参数（connect + read 必须一起显式声明）。"""
    return {
        "socket_connect_timeout": _resolve_timeout(
            connect_timeout, _CONNECT_ENV, DEFAULT_CONNECT_TIMEOUT_SECONDS
        ),
        "socket_timeout": _resolve_timeout(
            socket_timeout, _SOCKET_ENV, DEFAULT_SOCKET_TIMEOUT_SECONDS
        ),
    }


def apply_redis_timeout_defaults(
    kwargs: Dict[str, Any],
    *,
    connect_timeout: float | None = None,
    socket_timeout: float | None = None,
) -> Dict[str, Any]:
    """给已解析的连接参数补默认值；URL query 里显式给出的值优先（``setdefault``）。

    供 ``server/factory`` 的 ``RedisTarget.client_kwargs()`` 路径使用：存储侧
    客户端与队列侧同端点、同超时预算。
    """
    result = dict(kwargs)
    for key, value in redis_timeout_kwargs(
        connect_timeout=connect_timeout, socket_timeout=socket_timeout
    ).items():
        result.setdefault(key, value)
    return result


def _pop_timeouts(
    kwargs: Dict[str, Any],
    connect_timeout: float | None,
    socket_timeout: float | None,
) -> Dict[str, float]:
    return redis_timeout_kwargs(
        connect_timeout=kwargs.pop("socket_connect_timeout", connect_timeout),
        socket_timeout=kwargs.pop("socket_timeout", socket_timeout),
    )


def make_redis_client(
    url: str,
    *,
    connect_timeout: float | None = None,
    socket_timeout: float | None = None,
    **kwargs: Any,
):
    """同步客户端：``Redis.from_url`` + 显式超时（调用方禁裸 ``from_url``）。"""
    import redis

    timeouts = _pop_timeouts(kwargs, connect_timeout, socket_timeout)
    return redis.Redis.from_url(url, **kwargs, **timeouts)


def make_async_redis_client(
    url: str,
    *,
    connect_timeout: float | None = None,
    socket_timeout: float | None = None,
    **kwargs: Any,
):
    """``redis.asyncio`` 客户端：与同步客户端同一份超时预算。"""
    import redis.asyncio as aioredis

    timeouts = _pop_timeouts(kwargs, connect_timeout, socket_timeout)
    return aioredis.from_url(url, **kwargs, **timeouts)
