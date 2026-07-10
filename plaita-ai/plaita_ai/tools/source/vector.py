"""VectorToolSource — 向量检索工具。"""

from __future__ import annotations

import logging
from typing import Any, Callable, ClassVar, Dict, List, Optional

from pydantic import PrivateAttr

from plaita_ai.tools.source.base import BaseToolSource, ToolContext

logger = logging.getLogger(__name__)


class VectorToolSource(BaseToolSource):
    """向量库检索工具。

    连接来源（优先级）：
    1. 代码轨 ``bind_store(store)`` / 构造后注入
    2. ``store`` 命名资源（``register_vectorstore``）

    store 需满足其一：
    - 有 ``similarity_search(query, k=..., filter=...)``（LangChain 风格）
    - 可调用 ``store(query, k=...)``
    """

    type: ClassVar[str] = "vector"

    store: Optional[str] = None  # 资源名
    search_type: str = "similarity"
    k: int = 4
    filter: Optional[Dict[str, Any]] = None
    # 直接注入的 store 实例（不进配置序列化）
    _bound_store: Any = PrivateAttr(default=None)

    def bind_store(self, store: Any) -> "VectorToolSource":
        """代码轨：绑定具体向量库实例，返回 self 便于链式调用。"""
        self._bound_store = store
        return self

    def to_callable(self) -> Callable[..., Any]:
        source = self

        def retrieve(
            query: str,
            *,
            k: Optional[int] = None,
            context: Optional[ToolContext] = None,
            auth_context: Any = None,
            **kwargs: Any,
        ) -> List[str]:
            _ = auth_context
            if context and context.trace_id:
                logger.debug(
                    "VectorToolSource %s trace_id=%s", source.name, context.trace_id
                )
            store = source._resolve_store()
            top_k = k if k is not None else source.k
            filt = kwargs.get("filter", source.filter)
            return _run_retrieve(store, query, top_k, source.search_type, filt)

        retrieve.__name__ = self.name.replace("-", "_")
        retrieve.__doc__ = self.description or "向量检索"
        return retrieve

    def _resolve_store(self) -> Any:
        if self._bound_store is not None:
            return self._bound_store
        if self.store:
            from plaita_ai.tools.resources import get_vectorstore

            return get_vectorstore(self.store)
        raise ValueError(
            f"VectorToolSource {self.name!r}: 需要 bind_store() 或 store 资源名"
        )


def _run_retrieve(
    store: Any,
    query: str,
    k: int,
    search_type: str,
    filt: Optional[Dict[str, Any]],
) -> List[str]:
    if callable(store) and not hasattr(store, "similarity_search"):
        result = store(query, k=k)
        return _normalize_docs(result)

    if hasattr(store, "similarity_search"):
        kwargs: Dict[str, Any] = {"k": k}
        if filt is not None:
            kwargs["filter"] = filt
        # mmr / score_threshold 若 store 支持则透传 search_type
        if search_type == "mmr" and hasattr(store, "max_marginal_relevance_search"):
            docs = store.max_marginal_relevance_search(query, **kwargs)
        else:
            docs = store.similarity_search(query, **kwargs)
        return _normalize_docs(docs)

    raise TypeError(
        f"vector store 类型 {type(store).__name__} 不支持："
        "需要 similarity_search 方法或可调用对象"
    )


def _normalize_docs(docs: Any) -> List[str]:
    if docs is None:
        return []
    if isinstance(docs, str):
        return [docs]
    out: List[str] = []
    for d in docs:
        if isinstance(d, str):
            out.append(d)
        elif hasattr(d, "page_content"):
            out.append(str(d.page_content))
        elif isinstance(d, dict) and "page_content" in d:
            out.append(str(d["page_content"]))
        elif isinstance(d, dict) and "content" in d:
            out.append(str(d["content"]))
        else:
            out.append(str(d))
    return out
