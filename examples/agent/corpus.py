"""示例检索语料 —— 注册到 :class:`RetrieverNode` 供 RAG flow 使用。

参考 edan-backend 的 vst_retrieve 节点：把文档库注册成一个"检索库"，flow 里的
``retrieve`` 节点按 ``library`` 名取库、按 ``query`` 做检索。这里用内存关键词检索
代替真实向量库，保证示例零依赖可跑。
"""
from __future__ import annotations

from .nodes import RetrieverNode


# 一组关于 plaita 的迷你知识库。
KB_DOCS = [
    "plaita 是 Plaita 逻辑编排系统的官方 Python 运行时。",
    "plaita 支持三种执行模式：Normal 同步、Generator 单步、Distributed 断点续执。",
    "plaita 用 JSON 节点定义流程，内置 17 种节点，支持自定义节点。",
    "plaita 的类代码 DSL 包括 S-expr 与 @flow 两种形态，均有构建期校验。",
    "plaita 可作为 Agent 编排运行时：LLM 规划、流程执行，节点间用 $NODE 传参。",
]


def register_corpus() -> None:
    RetrieverNode.register("kb", KB_DOCS)
    RetrieverNode.register("default", KB_DOCS)
