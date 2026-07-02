"""Pytest config for LLM integration tests.

Skips all tests marked ``llm`` unless ``DEEPSEEK_API_KEY`` is resolvable
(from env or ~/.zshrc). This keeps the default ``pytest`` run offline/fast.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

# Make agent-benchmark/tasks.py importable as `tasks` for the harness.
import sys
from pathlib import Path as _Path

_BENCH = _Path(__file__).resolve().parents[3] / "agent-benchmark"
if str(_BENCH) not in sys.path:
    sys.path.insert(0, str(_BENCH))


def _resolve_deepseek_key() -> str | None:
    key = os.environ.get("DEEPSEEK_API_KEY")
    if key:
        return key.strip()
    zshrc = Path.home() / ".zshrc"
    if zshrc.exists():
        text = zshrc.read_text(encoding="utf-8")
        m = re.search(r'export\s+DEEPSEEK_API_KEY\s*=\s*["\']([^"\']+)["\']', text)
        if m:
            return m.group(1)
    return None


def pytest_collection_modifyitems(config, items):
    llm_marker = pytest.mark.llm
    for item in items:
        if "llm" in item.keywords:
            item.add_marker(pytest.mark.skipif(
                _resolve_deepseek_key() is None,
                reason="DEEPSEEK_API_KEY 未提供；跳过 LLM 集成测试",
            ))


def pytest_configure(config):
    config.addinivalue_line("markers", "llm: 需要 DEEPSEEK_API_KEY 的 LLM 集成测试")
