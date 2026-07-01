"""Agent 编排示例 —— 自定义 LLM 相关节点。

本模块参考 edan-backend 的 Plaita 自定义节点做法（llm_completion / vst_retrieve /
action），但去掉 Django/LangChain 等外部依赖，做成**开箱即跑**的版本：

- :class:`LLMNode`        —— 调 LLM 生成文本（prompt 模板 + 多角色 model 选择）
- :class:`ToolNode`       —— 把 Python 函数注册成工具节点（类 edan Action）
- :class:`RetrieverNode`  —— 内存关键词检索（类 edan vst_retrieve，不依赖向量库）

三者都继承 :class:`plaita.node.Node`，实现 ``execute(self, execution)``，并在解析
Flow 之前用 ``get_default_registry().register(...)`` 注册。``execution.evaluate``
会解析 ``$INPUT`` / ``$NODE`` / ``$GLOBAL`` 表达式以及 ``{% ... %}`` 插值。

配套的示例流程见 ``flows/``，可运行入口见 ``demo.py``。
"""
from __future__ import annotations

import re
from typing import Any, Callable, ClassVar, Dict, List, Optional

from pydantic import Field

from plaita import Node
from plaita.node import get_default_registry


# ---------------------------------------------------------------------------
# LLM 后端：FakeLLM 开箱即跑，可替换成真实 LLM
# ---------------------------------------------------------------------------

class LLM:
    """LLM 后端的最小协议。"""

    def complete(self, prompt: str, *, system: Optional[str] = None) -> str:  # pragma: no cover
        raise NotImplementedError


class FakeLLM(LLM):
    """确定性、离线可跑的 LLM，用于演示与测试。

    Args:
        rules: ``[(keyword, response), ...]``，按顺序匹配，首个命中的 keyword 出现在
            prompt 中即返回对应 response。用于模拟分类器/规划器等"结构化输出"角色。
        default: 未命中任何规则时的返回值。特殊值 ``"echo"`` 表示原样返回 prompt，
            便于在 RAG 场景里看到节点确实拿到了检索结果。

    想接真实 LLM 时，实现 :class:`LLM` 并通过 ``flow.global_context`` 注入即可（见
    :attr:`LLMNode.model` 说明）。
    """

    def __init__(self, rules: Optional[List[tuple]] = None, default: Any = "echo"):
        self.rules = list(rules or [])
        self.default = default

    def complete(self, prompt: str, *, system: Optional[str] = None) -> str:
        for keyword, response in self.rules:
            if keyword in prompt:
                return response
        if self.default == "echo":
            return prompt
        return str(self.default)


# 模块级兜底 LLM：未注入任何 LLM 时使用，保证流程总跑得起来。
_default_llm = FakeLLM(default="[FakeLLM] no LLM injected; prompt: " + "{prompt}")


def _resolve_llm(execution: Any, model: Optional[str]) -> LLM:
    """按优先级解析本次节点要用的 LLM：命名 model > 全局 llm > 模块默认。"""
    if model:
        llms = execution.get_global_variable("llms", {}) or {}
        if model in llms:
            return llms[model]
    single = execution.get_global_variable("llm", None)
    if isinstance(single, LLM):
        return single
    return _default_llm


# ---------------------------------------------------------------------------
# LLMNode
# ---------------------------------------------------------------------------

class LLMNode(Node):
    """调用 LLM 生成文本。

    JSON 字段（snake_case）：

    - ``prompt``: prompt 模板字符串，支持 ``{% $INPUT.x %}`` / ``{% $NODE.y %}`` 插值
      以及 ``$F.join(...)`` 等表达式函数（由 ``execution.evaluate`` 求值）。
    - ``system``: 可选 system prompt，同样支持表达式。
    - ``model``: 可选，从 ``$GLOBAL.llms[model]`` 取一个命名 LLM；未指定时回落到
      ``$GLOBAL.llm``，再回落到模块默认 :class:`FakeLLM`。

    节点输出：LLM 返回的字符串。
    """

    node_type: ClassVar[str] = "llm"
    node_name: ClassVar[str] = "LLM"

    prompt: Optional[str] = None
    system: Optional[str] = None
    model: Optional[str] = None

    def execute(self, execution: Any) -> str:
        prompt = execution.evaluate(self.prompt) if self.prompt else ""
        system = execution.evaluate(self.system) if self.system else None
        llm = _resolve_llm(execution, self.model)
        return llm.complete(prompt, system=system)


# ---------------------------------------------------------------------------
# ToolNode
# ---------------------------------------------------------------------------

class ToolNode(Node):
    """执行注册的 Python 工具函数（类 edan Action，但去掉 schema/LangChain 部分）。

    JSON 字段：

    - ``action``: 工具名（需先用 :meth:`ToolNode.register` 注册对应函数）。
    - ``params``: 参数字典，值支持表达式，会经 ``execution.evaluate`` 求值后展开。

    用 ``@ToolNode.register("name")`` 把任意函数注册成工具::

        @ToolNode.register("weather")
        def weather(city: str) -> str:
            return f"{city}：晴，25°C"

    节点输出：工具函数的返回值。
    """

    node_type: ClassVar[str] = "tool"
    node_name: ClassVar[str] = "工具"

    action: Optional[str] = None
    params: Optional[Dict[str, Any]] = None

    _tools: ClassVar[Dict[str, Callable]] = {}

    @classmethod
    def register(cls, name: str, func: Optional[Callable] = None) -> Callable:
        if func is None:
            def _decorator(f: Callable) -> Callable:
                cls._tools[name] = f
                return f
            return _decorator
        cls._tools[name] = func
        return func

    @classmethod
    def get_tool(cls, name: str) -> Optional[Callable]:
        return cls._tools.get(name)

    def execute(self, execution: Any) -> Any:
        name = self.action
        if name is None:
            raise ValueError("tool 节点缺少 action 字段")
        func = self.get_tool(name)
        if func is None:
            # action 本身可能是表达式（如 "$NODE.planner"），再求值一次
            resolved = execution.evaluate(name)
            if isinstance(resolved, str):
                name = resolved
                func = self.get_tool(name)
        if func is None:
            raise KeyError(f"未注册的工具: {name!r}（已知：{list(self._tools)}）")
        params = execution.evaluate(self.params) if self.params else {}
        params = params or {}
        return func(**params)


# ---------------------------------------------------------------------------
# RetrieverNode
# ---------------------------------------------------------------------------

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def _tokenize(text: str) -> List[str]:
    """简单分词：CJK 按单字，其余按单词。用于关键词重叠评分。"""
    tokens: List[str] = []
    buf = []
    for ch in text:
        if _CJK_RE.match(ch):
            if buf:
                tokens.append("".join(buf))
                buf = []
            tokens.append(ch)
        elif ch.isalnum():
            buf.append(ch)
        else:
            if buf:
                tokens.append("".join(buf))
                buf = []
    if buf:
        tokens.append("".join(buf))
    return [t for t in tokens if t.strip()]


class Retriever:
    """极简内存检索器：按 query 与 doc 的 token 重叠评分，取 top_k。"""

    def __init__(self, docs: List[str]):
        self.docs = list(docs)

    def retrieve(self, query: str, k: int = 3) -> List[str]:
        q_tokens = set(_tokenize(query))
        if not q_tokens:
            return self.docs[:k]
        scored = sorted(
            self.docs,
            key=lambda d: len(q_tokens & set(_tokenize(d))),
            reverse=True,
        )
        return scored[:k]


class RetrieverNode(Node):
    """内存关键词检索节点（类 edan vst_retrieve，但不依赖向量库）。

    JSON 字段：

    - ``query``: 查询表达式（如 ``"$INPUT.question"``）。
    - ``library``: 检索库名，对应 :meth:`RetrieverNode.register` 注册的库名。
    - ``top_k``: 返回条数，默认 3。

    节点输出：命中文档字符串列表 ``[str, ...]``。
    """

    node_type: ClassVar[str] = "retrieve"
    node_name: ClassVar[str] = "检索"

    query: Optional[str] = None
    library: Optional[str] = "default"
    top_k: int = Field(3)

    _libraries: ClassVar[Dict[str, Retriever]] = {}

    @classmethod
    def register(cls, name: str, docs: List[str]) -> None:
        cls._libraries[name] = Retriever(docs)

    @classmethod
    def get_library(cls, name: str) -> Optional[Retriever]:
        return cls._libraries.get(name)

    def execute(self, execution: Any) -> List[str]:
        query = execution.evaluate(self.query) if self.query else ""
        lib_name = self.library or "default"
        lib = self.get_library(lib_name) or self.get_library("default")
        if lib is None:
            raise KeyError(f"未注册的检索库: {lib_name!r}（已知：{list(self._libraries)}）")
        return lib.retrieve(query, self.top_k)


# ---------------------------------------------------------------------------
# 注册入口
# ---------------------------------------------------------------------------

def register_all() -> None:
    """把三个节点注册到默认 registry。必须在 ``Flow.from_string`` 之前调用。"""
    reg = get_default_registry()
    reg.register(LLMNode)
    reg.register(ToolNode)
    reg.register(RetrieverNode)


__all__ = [
    "LLM",
    "FakeLLM",
    "LLMNode",
    "ToolNode",
    "RetrieverNode",
    "Retriever",
    "register_all",
]
