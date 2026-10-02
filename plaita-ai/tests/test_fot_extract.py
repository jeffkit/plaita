"""Unit tests for extract_flow_source — no langchain required.

F3 修复回归：模型输出解析不能只看第一个 fenced block，也不能把
「找不到块」的失败炸穿重试回路（见 test_fot_parse_retry.py）。
"""

from __future__ import annotations

import pytest

from plaita_ai.agent.fot.extract import extract_flow_source


FENCED_FLOW = '''```python
@flow("echo_q")
def echo_q(INPUT):
    return INPUT.q
```'''


def test_plain_fenced_block_extracts():
    assert extract_flow_source(FENCED_FLOW).startswith('@flow("echo_q")')


def test_extracts_flow_block_after_non_flow_draft_block():
    """第一个 ```python 块是思路草稿（无 @flow）、第二个才是源码——必须取到第二个。"""
    noisy = (
        "好的，我来实现这个需求，先分析一下：\n"
        "\n"
        "```python\n"
        "# 这是思路草稿\n"
        "task = \"echo\"\n"
        "```\n"
        "\n"
        "下面是正式的 flow：\n"
        "\n"
        + FENCED_FLOW
    )
    src = extract_flow_source(noisy)
    assert src.startswith('@flow("echo_q")')
    assert "思路草稿" not in src


def test_extracts_from_generic_fence():
    generic = FENCED_FLOW.replace("```python", "```", 1)
    assert extract_flow_source(generic).startswith('@flow("echo_q")')


def test_accepts_raw_text_without_fence():
    raw = '@flow("echo_q")\ndef echo_q(INPUT):\n    return INPUT.q'
    assert extract_flow_source(raw) == raw


def test_pure_noise_raises_actionable_error():
    with pytest.raises(ValueError) as exc_info:
        extract_flow_source("我觉得这个任务需要更多信息，无法生成代码。")
    msg = str(exc_info.value)
    assert "@flow" in msg and "```python" in msg


def test_empty_input_raises():
    with pytest.raises(ValueError):
        extract_flow_source("")
    with pytest.raises(ValueError):
        extract_flow_source("   \n  ")


def test_fences_without_flow_marker_fall_through_to_error():
    """只有不含 @flow 的代码块时，维持可操作的 ValueError 而非返回杂讯。"""
    text = "看这段：\n```python\nprint('hello')\n```\n"
    with pytest.raises(ValueError):
        extract_flow_source(text)
