"""FoT parse-failure retry loop tests — FakeListChatModel, no API key.

F3 修复回归：模型输出不含 @flow 代码块时，重试回路要把它当成一次失败
attempt 回喂下一轮，预算耗尽后返回结构化 FoTResult，而不是 ValueError
炸穿 FoTAgent.invoke。
"""

from __future__ import annotations

import pytest
pytest.importorskip("langchain", reason="langchain extra not installed: pip install 'plaita-ai[agent]'")
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from plaita_ai.agent.fot import FoTAgent


PURE_NOISE = "我觉得这个任务需要更多信息，无法生成代码。"

VALID_FLOW = '''```python
@flow("echo_q")
def echo_q(INPUT):
    return INPUT.q
```'''

NOISY_THEN_GOOD = (
    "好的，我来实现这个需求，先分析一下：\n"
    "\n"
    "```python\n"
    "# 这是思路草稿\n"
    "task = \"echo\"\n"
    "```\n"
    "\n"
    "下面是正式的 flow：\n"
    "\n"
    + VALID_FLOW
)


def test_parse_failure_burns_one_attempt_then_recovers():
    """第一轮纯杂讯（解析失败）→ 第二轮正常输出 → 整体成功，attempts=2。"""
    model = FakeListChatModel(responses=[PURE_NOISE, VALID_FLOW])
    agent = FoTAgent(model=model, max_compile_retries=3)
    result = agent.invoke({"task": "回显用户问题", "q": "hello"})
    assert result.ok is True
    assert result.result == "hello"
    assert result.attempts == 2


def test_noisy_response_with_draft_block_succeeds_first_attempt():
    """单次响应里先杂讯/草稿块、后正确代码块——提取成功，一轮即成。"""
    model = FakeListChatModel(responses=[NOISY_THEN_GOOD])
    agent = FoTAgent(model=model, max_compile_retries=3)
    result = agent.invoke({"task": "回显用户问题", "q": "hello"})
    assert result.ok is True
    assert result.result == "hello"
    assert result.attempts == 1


def test_parse_failure_exhausts_budget_returns_structured_failure():
    """全程纯杂讯 → 走完重试预算后返回结构化失败，不抛异常。"""
    model = FakeListChatModel(responses=[PURE_NOISE])
    agent = FoTAgent(model=model, max_compile_retries=3)
    result = agent.invoke({"task": "回显用户问题", "q": "x"})
    assert result.ok is False
    assert result.result is None
    assert result.attempts == 3
    assert result.run is None
    assert result.source == ""
    assert result.compile_errors, "解析失败必须结构化记入 compile_errors"
    messages = [e.message for e in result.compile_errors]
    assert any("@flow" in m and "```python" in m for m in messages)
