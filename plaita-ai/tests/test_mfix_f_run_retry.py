"""MF1: FoT run-error retry loop — FakeListChatModel, no API key.

SKILL.md 第 5 步宣称「编译失败或执行报错都把完整错误拼回 prompt 重试」；
本文件锁定运行期错误回喂环：编译通过但 run 失败时，把 RunResult 的
error/error_type 回喂模型定点修复，独立 max_run_retries 预算（默认 3，
对齐 SKILL「最多重试 3 轮」），编译失败路径不消耗 run 预算。
"""

from __future__ import annotations

import pytest
pytest.importorskip("langchain", reason="langchain extra not installed: pip install 'plaita-ai[agent]'")
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from plaita_ai.agent.fot import FoTAgent


RUN_FAILS = '''```python
@flow("div_it")
def div_it(INPUT):
    return F.div(INPUT.x, INPUT.y)
```'''

RUN_FIXED = '''```python
@flow("div_it")
def div_it(INPUT):
    return INPUT.x
```'''

# 编译失败的修正稿（f-string）：修复轮里模型先给出过不了编译的版本
REWRITE_BAD_COMPILE = '''```python
@flow("div_it")
def div_it(INPUT):
    return f"x={INPUT.x}"
```'''

PURE_NOISE = "我觉得这个任务需要更多信息，无法生成代码。"


class _Recording:
    """Duck-typed chat-model wrapper: counts calls and records prompts."""

    def __init__(self, inner):
        self.inner = inner
        self.calls = 0
        self.prompts = []

    def invoke(self, messages, *args, **kwargs):
        self.calls += 1
        self.prompts.append(messages)
        return self.inner.invoke(messages, *args, **kwargs)


def test_run_failure_is_fed_back_and_fixed():
    """首次 run 失败（除零）→ 运行错误回喂 → 修复稿一次通过。

    attempts 只数编译回路（=1）；run_retries 独立计数（=1）；
    第二次 prompt 必须携带运行错误文本与「编译已通过、执行报错」口径。
    """
    recorded = _Recording(FakeListChatModel(responses=[RUN_FAILS, RUN_FIXED]))
    agent = FoTAgent(model=recorded, max_compile_retries=3)
    result = agent.invoke({"task": "返回 x 除以 y", "x": 1, "y": 0})

    assert result.ok is True
    assert result.result == 1
    assert result.attempts == 1  # 编译一次过
    assert result.run_retries == 1  # 一轮 run 修复
    assert result.run is not None and result.run.ok is True
    assert recorded.calls == 2  # compose + 1 次运行错误修复

    repair_prompt = json_of(recorded.prompts[1])
    assert "ZeroDivisionError" in repair_prompt
    assert "编译已通过" in repair_prompt  # RUN_REVIEW_USER 口径，非「编译错误」


def test_run_budget_exhausted_returns_structured_failure():
    """修复稿一直失败 → max_run_retries 轮后返回结构化失败，不抛异常，
    且保留最后一次 RunResult 供诊断。"""
    recorded = _Recording(FakeListChatModel(responses=[RUN_FAILS]))
    agent = FoTAgent(model=recorded, max_compile_retries=3, max_run_retries=2)
    result = agent.invoke({"task": "返回 x 除以 y", "x": 1, "y": 0})

    assert result.ok is False
    assert result.run_retries == 2  # 烧满独立预算
    assert result.attempts == 1
    assert result.run is not None and result.run.ok is False
    assert "ZeroDivisionError" in str(result.run.error)
    # 1 次 compose + 2 轮修复 = 3 次模型调用
    assert recorded.calls == 3


def test_compile_failure_does_not_consume_run_budget():
    """编译回路耗尽直接返回：run 修复环一次都不进（run_retries==0）。"""
    recorded = _Recording(FakeListChatModel(responses=[PURE_NOISE]))
    agent = FoTAgent(model=recorded, max_compile_retries=3)
    result = agent.invoke({"task": "回显", "q": "x"})

    assert result.ok is False
    assert result.attempts == 3  # 编译预算照旧
    assert result.run_retries == 0
    assert result.run is None
    assert recorded.calls == 3  # 全部烧在编译回路上


def test_repair_round_with_compile_failure_then_success():
    """修复轮先给出编译不过的稿子 → 该轮照常烧掉、回喂编译错误 →
    下一轮给出正确稿子。run_retries 计两轮。"""
    recorded = _Recording(
        FakeListChatModel(responses=[RUN_FAILS, REWRITE_BAD_COMPILE, RUN_FIXED])
    )
    agent = FoTAgent(model=recorded, max_compile_retries=3, max_run_retries=3)
    result = agent.invoke({"task": "返回 x 除以 y", "x": 7, "y": 0})

    assert result.ok is True
    assert result.result == 7
    assert result.run_retries == 2
    assert recorded.calls == 3
    # 第三次 prompt 是按编译错误回喂的（上一轮修正版未通过编译）
    third = json_of(recorded.prompts[2])
    assert "未通过编译" in third
    assert "f-string" in third


def test_repair_round_parse_failure_then_success():
    """修复轮输出不含 @flow 源码 → 该轮烧掉并回喂解析提示 → 下一轮成功。"""
    recorded = _Recording(FakeListChatModel(responses=[RUN_FAILS, PURE_NOISE, RUN_FIXED]))
    agent = FoTAgent(model=recorded, max_compile_retries=3, max_run_retries=3)
    result = agent.invoke({"task": "返回 x 除以 y", "x": 3, "y": 0})

    assert result.ok is True
    assert result.result == 3
    assert result.run_retries == 2
    third = json_of(recorded.prompts[2])
    assert "未包含 @flow 源码" in third  # 解析提示回喂进 instruction 段


def json_of(messages) -> str:
    """Flatten a prompt message list into one string for assertions."""
    parts = []
    for message in messages:
        content = getattr(message, "content", message)
        if isinstance(content, dict):
            content = content.get("content", "")
        parts.append(str(content))
    return "\n".join(parts)
