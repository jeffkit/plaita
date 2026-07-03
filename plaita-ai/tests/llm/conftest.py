"""Pytest config for LLM integration tests.

Skips all tests marked ``llm`` unless the LLM API key is resolvable from the
environment (``PLAITA_LLM_API_KEY`` or ``DEEPSEEK_API_KEY``). This keeps the
default ``pytest`` run offline/fast. We no longer read ``~/.zshrc`` — env only.
"""

from __future__ import annotations

import os

import pytest


def _resolve_api_key() -> str | None:
    for name in ("PLAITA_LLM_API_KEY", "DEEPSEEK_API_KEY"):
        key = os.environ.get(name)
        if key and key.strip():
            return key.strip()
    return None


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "llm" in item.keywords:
            item.add_marker(pytest.mark.skipif(
                _resolve_api_key() is None,
                reason="PLAITA_LLM_API_KEY / DEEPSEEK_API_KEY 未提供；跳过 LLM 集成测试",
            ))


def pytest_configure(config):
    config.addinivalue_line("markers", "llm: 需要 LLM API key 的集成测试")
