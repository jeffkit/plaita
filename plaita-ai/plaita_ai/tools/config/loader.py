"""从 YAML/JSON 加载扁平 ToolBundle / Resources。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Union

from plaita_ai.tools.config.schema import Resources, ToolBundle

PathOrData = Union[str, Path, Dict[str, Any]]


def _read_raw(source: PathOrData) -> Dict[str, Any]:
    if isinstance(source, dict):
        return source
    path = Path(source)
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as e:
            raise ImportError(
                "加载 YAML 工具配置需要 PyYAML。安装: pip install PyYAML "
                "或 pip install plaita[yaml]"
            ) from e
        data = yaml.safe_load(text)
    elif suffix == ".json" or not suffix:
        data = json.loads(text)
    else:
        # 尝试 YAML，再 JSON
        try:
            import yaml

            data = yaml.safe_load(text)
        except Exception:
            data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"工具配置根节点必须是 mapping，得到 {type(data).__name__}")
    return data


def parse_tool_bundle(source: PathOrData) -> ToolBundle:
    raw = _read_raw(source)
    # 允许省略 version，直接给 tools 列表的简写
    if "tools" not in raw and isinstance(raw.get("items"), list):
        raw = {"tools": raw["items"]}
    return ToolBundle.model_validate(raw)


def parse_resources(source: PathOrData) -> Resources:
    raw = _read_raw(source)
    return Resources.model_validate(raw)
