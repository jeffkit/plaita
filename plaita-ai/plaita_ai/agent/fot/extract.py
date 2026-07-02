"""Extract @flow source from LLM responses."""

from __future__ import annotations

import re

_FLOW_MARKERS = ("@flow", "@childflow")


def extract_flow_source(text: str) -> str:
    """Parse @flow Python source from a markdown fenced block or raw text."""
    if not text or not text.strip():
        raise ValueError("模型返回为空，未找到 @flow 源码")

    fenced_patterns = (
        r"```python\s*\n(.*?)```",
        r"```py\s*\n(.*?)```",
        r"```\s*\n(.*?)```",
    )
    for pattern in fenced_patterns:
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if not match:
            continue
        candidate = match.group(1).strip()
        if _looks_like_flow_source(candidate):
            return candidate

    stripped = text.strip()
    if _looks_like_flow_source(stripped):
        return stripped

    raise ValueError("未能从模型输出中解析 @flow 源码（需要 ```python ... ``` 代码块）")


def _looks_like_flow_source(source: str) -> bool:
    if any(marker in source for marker in _FLOW_MARKERS):
        return True
    return source.startswith("def ") and "INPUT" in source
