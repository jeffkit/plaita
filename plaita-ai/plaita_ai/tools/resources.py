"""命名资源注册表 — datasources / vectorstores 的运行时绑定。

配置轨：``load_tool_bundle(..., resources=...)`` 写入连接配置，首次使用时惰性创建。
代码轨：``register_datasource`` / ``register_vectorstore`` 直接注入实例。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

from plaita_ai.tools.config.schema import Resources

logger = logging.getLogger(__name__)

_resource_config: Optional[Resources] = None
_engines: Dict[str, Any] = {}
_vectorstores: Dict[str, Any] = {}


def clear_resources() -> None:
    """测试 / 隔离环境重置。"""
    global _resource_config
    _resource_config = None
    _engines.clear()
    _vectorstores.clear()


def set_resource_config(resources: Resources) -> None:
    """保存扁平 Resources 配置（不立即建连）。"""
    global _resource_config
    _resource_config = resources


def get_resource_config() -> Optional[Resources]:
    return _resource_config


def register_datasource(name: str, url_or_engine: Union[str, Any]) -> None:
    """代码轨：注册 SQLAlchemy Engine 或 DSN 字符串。"""
    if isinstance(url_or_engine, str):
        _engines[name] = _create_engine(url_or_engine)
    else:
        _engines[name] = url_or_engine


def register_vectorstore(name: str, store: Any) -> None:
    """代码轨：注册向量库实例（需支持 similarity_search 或可调用）。"""
    _vectorstores[name] = store


def get_sql_engine(name: str) -> Any:
    if name in _engines:
        return _engines[name]
    if _resource_config and name in _resource_config.datasources:
        ds = _resource_config.datasources[name]
        engine = _create_engine(ds.url)
        _engines[name] = engine
        return engine
    raise KeyError(
        f"datasource {name!r} 未注册。请先 register_datasource() "
        "或在 resources.yaml 的 datasources 中声明。"
    )


def get_vectorstore(name: str) -> Any:
    if name in _vectorstores:
        return _vectorstores[name]
    raise KeyError(
        f"vectorstore {name!r} 未注册。请先 register_vectorstore() "
        "（配置轨仅声明元数据，运行时需代码注入具体 store 实例）。"
    )


def _create_engine(url: str) -> Any:
    try:
        from sqlalchemy import create_engine
    except ImportError as e:
        raise ImportError(
            "SqlToolSource 需要 sqlalchemy。安装: pip install sqlalchemy "
            "或 pip install plaita[sqlalchemy]"
        ) from e
    logger.debug("creating SQLAlchemy engine for %s", url.split("@")[-1])
    return create_engine(url)
