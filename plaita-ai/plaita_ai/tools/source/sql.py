"""SqlToolSource — 参数化 SQL 查询工具。"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable, ClassVar, Dict, List, Optional

from plaita_ai.tools.source.base import BaseToolSource, ToolContext

logger = logging.getLogger(__name__)

# :name 占位（排除 Postgres ::cast）
_SQL_PARAM_RE = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")


def sql_param_names(sql: str) -> List[str]:
    """从 SQL 中提取 ``:param`` 名称（去重、保序）。"""
    seen: set[str] = set()
    names: List[str] = []
    for m in _SQL_PARAM_RE.finditer(sql):
        name = m.group(1)
        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


class SqlToolSource(BaseToolSource):
    """关系库查询工具。

    连接来源（优先级）：
    1. ``url`` — 直接 DSN（代码轨便捷写法）
    2. ``datasource`` — 命名资源（``register_datasource`` / resources.yaml）
    """

    type: ClassVar[str] = "sql"

    sql: str
    datasource: Optional[str] = None
    url: Optional[str] = None
    row_limit: int = 100

    def to_callable(self) -> Callable[..., Any]:
        source = self

        def invoke(
            *,
            context: Optional[ToolContext] = None,
            auth_context: Any = None,
            **kwargs: Any,
        ) -> List[Dict[str, Any]]:
            _ = auth_context  # 兼容注入；SQL 侧暂不使用
            if context and context.trace_id:
                logger.debug(
                    "SqlToolSource %s trace_id=%s", source.name, context.trace_id
                )
            engine = source._resolve_engine()
            try:
                from sqlalchemy import text
            except ImportError as e:
                raise ImportError(
                    "SqlToolSource 需要 sqlalchemy。安装: pip install sqlalchemy"
                ) from e

            stmt = text(source.sql)
            with engine.connect() as conn:
                result = conn.execute(stmt, kwargs)
                rows: List[Dict[str, Any]] = []
                for i, row in enumerate(result.mappings()):
                    if i >= source.row_limit:
                        break
                    rows.append(dict(row))
            return rows

        invoke.__name__ = self.name.replace("-", "_")
        invoke.__doc__ = self.description or f"SQL: {self.sql[:80]}"
        return invoke

    def _resolve_engine(self) -> Any:
        if self.url:
            from plaita_ai.tools.resources import _create_engine

            return _create_engine(self.url)
        if self.datasource:
            from plaita_ai.tools.resources import get_sql_engine

            return get_sql_engine(self.datasource)
        raise ValueError(
            f"SqlToolSource {self.name!r}: 需要 url 或 datasource"
        )
