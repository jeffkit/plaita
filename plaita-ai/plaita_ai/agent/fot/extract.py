"""Extract @flow source from LLM responses."""

from __future__ import annotations

import re

_FLOW_MARKERS = ("@flow", "@childflow")


def extract_flow_source(text: str) -> str:
    """Parse @flow Python source from a markdown fenced block or raw text.

    扫描全部 fenced code block，取第一个内容含 ``@flow``/``@childflow`` 的——
    模型常先输出思路草稿块再输出正式源码块，只看第一个块会漏提。
    """
    if not text or not text.strip():
        raise ValueError("模型返回为空，未找到 @flow 源码")

    fenced_patterns = (
        r"```python\s*\n(.*?)```",
        r"```py\s*\n(.*?)```",
        r"```\s*\n(.*?)```",
    )
    for pattern in fenced_patterns:
        for match in re.finditer(pattern, text, re.DOTALL | re.IGNORECASE):
            candidate = match.group(1).strip()
            if _looks_like_flow_source(candidate):
                return candidate

    stripped = text.strip()
    if _looks_like_flow_source(stripped):
        return stripped

    raise ValueError(
        "未能从模型输出中解析 @flow 源码（需要只输出一个包含 @flow 的 ```python ... ``` 代码块，不要输出额外说明文字）"
    )


def _looks_like_flow_source(source: str) -> bool:
    # Require at least one @flow or @childflow decorator — plain Python functions
    # that happen to contain "INPUT" would otherwise be misidentified.
    return any(marker in source for marker in _FLOW_MARKERS)
