"""Agent 编排示例 —— 可运行入口。

跑 ``python -m examples.agent.demo`` 或 ``python examples/agent/demo.py`` 即可，
无需任何 API key：所有 LLM 角色都用 :class:`FakeLLM` 离线模拟。

三个案例对应 ``flows/`` 下的 JSON：

1. **RAG**        —— retrieve(检索) → llm(带资料回答)
2. **Tool-use**   —— llm(规划选工具) → switch → tool(执行) → end
3. **Router**     —— llm(意图分类) → switch → llm(分角色回复) → end

要换成真实 LLM：实现 :class:`LLM`，把实例放进 ``flow.global_context["llms"]`` 即可，
flow 本身一行不用改。
"""
from __future__ import annotations

import os
from typing import Any, Dict

from plaita import Flow

from . import corpus, tools  # noqa: F401  导入即注册工具
from .nodes import FakeLLM, register_all

# 确保自定义节点类已注册到默认 registry（必须在 Flow.from_string 之前）。
register_all()
# 注册示例检索库（工具函数已在 tools 导入时由装饰器注册）。
corpus.register_corpus()

_FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def _load(flow_name: str) -> Flow:
    with open(os.path.join(_FLOWS_DIR, f"{flow_name}.json"), encoding="utf-8") as f:
        return Flow.from_string(f.read())


def _run(flow: Flow, globals_ctx: Dict[str, Any], **inputs: Any) -> Any:
    flow.global_context = dict(globals_ctx)
    return flow.run(**inputs)


# ---------------------------------------------------------------------------
# 各角色 LLM（FakeLLM；真实场景替换为真实 LLM 实例即可）
# ---------------------------------------------------------------------------

# 规划器：按问题里的关键词决定工具，default 兜底为 search。
_planner = FakeLLM(
    rules=[("天气", "weather"), ("加", "calc"), ("多少", "calc")],
    default="search",
)

# 意图分类器：按消息里的关键词分类，default 走通用分支。
_classifier = FakeLLM(
    rules=[
        ("价格", "sales"), ("买", "sales"), ("优惠", "sales"),
        ("故障", "support"), ("报错", "support"), ("安装", "support"),
        ("退款", "billing"), ("账单", "billing"), ("发票", "billing"),
    ],
    default="general",
)

# 回复器：原样回显 prompt，便于看到检索结果 / 角色提示确实传到了 LLM 节点。
_responder = FakeLLM(default="echo")

_GLOBALS = {"llms": {"planner": _planner, "classifier": _classifier, "responder": _responder}}


# ---------------------------------------------------------------------------
# 案例 1：RAG
# ---------------------------------------------------------------------------

def run_rag() -> None:
    flow = _load("rag")
    result = _run(flow, _GLOBALS, question="plaita 有几种执行模式")
    print("【RAG】问：plaita 有几种执行模式？")
    print(result)
    print()


# ---------------------------------------------------------------------------
# 案例 2：Tool-use Agent
# ---------------------------------------------------------------------------

def run_tool_use() -> None:
    flow = _load("tool_use")

    cases = [
        {"question": "北京今天天气如何", "city": "北京"},
        {"question": "3 加 5 是多少", "a": 3, "b": 5},
        {"question": "plaita 是什么"},
    ]
    for inp in cases:
        result = _run(flow, _GLOBALS, **inp)
        print(f"【Tool-use】{inp['question']} -> {result}")
    print()


# ---------------------------------------------------------------------------
# 案例 3：意图路由
# ---------------------------------------------------------------------------

def run_router() -> None:
    flow = _load("router")

    cases = ["这个多少钱？有优惠吗", "程序一直报错打不开", "我要退款，看下账单", "你好呀"]
    for msg in cases:
        result = _run(flow, _GLOBALS, message=msg)
        print(f"【Router】{msg} -> {result}")
    print()


def main() -> None:
    run_rag()
    run_tool_use()
    run_router()


if __name__ == "__main__":
    main()
