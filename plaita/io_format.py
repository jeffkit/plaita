"""
plaita.io_format — Flow 定义的序列化格式加载层。

Flow 的运行时只认 dict（见 ``plaita.core.flow.Flow.parse_flow``），
JSON / YAML 只是 dict 的不同序列化形态。本模块统一入口：

- ``loads(content)``：接受 JSON 或 YAML 字符串，自动识别，返回 dict。
- ``load_file(path)``：按文件后缀（``.json`` / ``.yaml`` / ``.yml``）选择解析器；
  无后缀或无法判断时走 ``loads`` 自动识别。

YAML 是可选依赖：``pip install plaita[yaml]``。未安装时调用 YAML 解析
会抛出带清晰提示的 ``RuntimeError``，JSON 路径不受影响。
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict

__all__ = ["loads", "load_file", "dump_yaml", "requires_yaml"]


_YAML_EXAMPLE = (
    "pip install plaita[yaml]  # 或 pip install PyYAML>=6.0"
)


def requires_yaml() -> Any:
    """导入 PyYAML，未安装时抛出带安装提示的 RuntimeError。"""
    try:
        import yaml  # type: ignore
    except ImportError as e:  # pragma: no cover - 路径由测试覆盖
        raise RuntimeError(
            "YAML 支持需要 PyYAML，请先安装: " + _YAML_EXAMPLE
        ) from e
    return yaml


def _looks_like_json(content: str) -> bool:
    """快速判断是否是 JSON（首字母为 { 或 [，去除前导空白/注释后）。"""
    stripped = content.lstrip()
    return stripped[:1] in ("{", "[")


def loads(content: str) -> Dict[str, Any]:
    """把 JSON 或 YAML 字符串解析成 dict。

    优先按 JSON 解析（保留历史行为与错误信息）；JSON 失败时回退到 YAML。
    空字符串返回空 dict，由上层 ``Flow.parse_flow`` 报「内容为空」。
    """
    if not content or not content.strip():
        return {}

    if _looks_like_json(content):
        try:
            data = json.loads(content)
        except json.JSONDecodeError as json_err:
            # 极少情况下 YAML 也以 { 开头（流式风格），再试一次 YAML
            try:
                yaml = requires_yaml()
                data = yaml.safe_load(content)
            except RuntimeError:
                raise json_err
        return _coerce_to_flow_dict(data, json_err=None)

    yaml = requires_yaml()
    data = yaml.safe_load(content)
    return _coerce_to_flow_dict(data, json_err=None)


def _coerce_to_flow_dict(data: Any, json_err: Any) -> Dict[str, Any]:
    """把解析结果规范成 flow dict；非 mapping 抛 RuntimeError（保持历史契约）。"""
    if isinstance(data, dict):
        return data
    if data is None:
        return {}
    raise RuntimeError(
        "invalid flow content: 顶层必须是 JSON/YAML mapping，"
        f"得到的是 {type(data).__name__}"
    )


def load_file(path: str) -> Dict[str, Any]:
    """按文件后缀读取 flow 定义。

    ``.json`` 走 JSON；``.yaml``/``.yml`` 走 YAML；其它后缀按内容自动识别。
    """
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    ext = os.path.splitext(path)[1].lower()
    if ext == ".json":
        return json.loads(content) if content.strip() else {}
    if ext in (".yaml", ".yml"):
        yaml = requires_yaml()
        data = yaml.safe_load(content) or {}
        if not isinstance(data, dict):
            raise RuntimeError(
                "invalid flow content: 顶层必须是 YAML mapping，"
                f"得到的是 {type(data).__name__}"
            )
        return data
    return loads(content)


def dump_yaml(data: Dict[str, Any]) -> str:
    """把 dict 序列化为 YAML 字符串。"""
    yaml = requires_yaml()
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=False)
