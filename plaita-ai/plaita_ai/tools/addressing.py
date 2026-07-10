"""HTTP 服务发现 / 寻址插件注册表。

对标 edan ``register_addressing_service``，但保持最小接口：

- 简单：``resolver(host: str) -> str``
- 高级：``resolver(host)`` 返回 context manager，``__enter__`` 得到
  ``new_host`` 或 ``(new_host, feedback)``
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

AddressingResolver = Callable[[str], Any]

_ADDRESSING: Dict[str, AddressingResolver] = {}


def clear_addressing() -> None:
    _ADDRESSING.clear()


def register_addressing(name: str, resolver: AddressingResolver) -> None:
    """注册命名寻址服务。同名覆盖并打 warning。"""
    if name in _ADDRESSING:
        logger.warning("addressing %r already registered, overwriting", name)
    _ADDRESSING[name] = resolver


def has_addressing(name: str) -> bool:
    return name in _ADDRESSING


def list_addressing() -> list[str]:
    return sorted(_ADDRESSING)


def get_addressing(name: str) -> AddressingResolver:
    try:
        return _ADDRESSING[name]
    except KeyError as e:
        raise KeyError(
            f"addressing service {name!r} 未注册。"
            f"已知: {list_addressing()}。请先 register_addressing()。"
        ) from e


@contextmanager
def apply_addressing(url: str, addressing: Optional[str]) -> Iterator[str]:
    """解析 URL 中的 host，经寻址服务替换后 yield 新 URL。

    无 ``addressing`` 时原样 yield。
    """
    if not addressing:
        yield url
        return

    resolver = get_addressing(addressing)
    parsed = urlparse(url)
    host = parsed.netloc
    if not host:
        yield url
        return

    # netloc 可能含 userinfo / port；寻址通常只看 hostname
    hostname = parsed.hostname or host
    result = resolver(hostname)

    if hasattr(result, "__enter__"):
        with result as resolved:
            new_host = _unwrap_host(resolved, hostname)
            yield _replace_host(url, parsed, hostname, new_host)
    else:
        new_host = _unwrap_host(result, hostname)
        yield _replace_host(url, parsed, hostname, new_host)


def _unwrap_host(resolved: Any, fallback: str) -> str:
    if isinstance(resolved, tuple) and resolved:
        return str(resolved[0])
    if resolved is None:
        return fallback
    return str(resolved)


def _replace_host(url: str, parsed: Any, old_hostname: str, new_host: str) -> str:
    """把 hostname 换成 new_host，保留 port / userinfo（若 new_host 未自带）。"""
    netloc = parsed.netloc
    # 若 new_host 已含端口或完整 netloc，直接替换 hostname 段
    if old_hostname in netloc:
        new_netloc = netloc.replace(old_hostname, new_host, 1)
    else:
        new_netloc = new_host
    return urlunparse(parsed._replace(netloc=new_netloc))
