"""Versioned prompts for the LLM benchmark harness.

These instructions are experiment variables — changing them changes benchmark
results. They are externalised here (with a ``PROMPT_VERSION``) so historical
runs stay comparable: the runner stamps ``prompt_version`` into every result
file. Bump the version whenever an instruction text is edited.
"""

from __future__ import annotations

PROMPT_VERSION = "2026-07-01"

FOT_INSTRUCTION = "流程要对任意合法输入都成立，不要硬编码具体测试值。"

REACT_INSTRUCTION = (
    "本任务必须用 @flow 实现（不要纯文本回答）：先写源码、校验、跑样例。"
    "流程要对任意合法输入成立，不要硬编码具体测试值。"
)
