"""工具包启动引导 — 从环境变量 / 路径加载扁平工具清单。"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Optional, Union

from plaita_ai.agent.fot.tools import ToolSpec
from plaita_ai.tools.config.loader import parse_resources, parse_tool_bundle
from plaita_ai.tools.config.schema import Resources, ToolBundle
from plaita_ai.tools.registry import load_tool_bundle

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]

ENV_TOOLS = "PLAITA_TOOLS"
ENV_RESOURCES = "PLAITA_RESOURCES"


def load_tools_from_env(
    *,
    tools_env: str = ENV_TOOLS,
    resources_env: str = ENV_RESOURCES,
) -> List[ToolSpec]:
    """读取 ``PLAITA_TOOLS`` / ``PLAITA_RESOURCES`` 并注册。

    未设置 ``PLAITA_TOOLS`` 时返回空列表（不报错）。
    """
    tools_path = os.environ.get(tools_env, "").strip()
    if not tools_path:
        return []
    resources_path = os.environ.get(resources_env, "").strip() or None
    logger.info(
        "loading tool bundle from %s (resources=%s)",
        tools_path,
        resources_path or "-",
    )
    specs = load_tool_bundle(tools_path, resources_path)
    logger.info("registered %d tool(s) from env", len(specs))
    return specs


def validate_tool_bundle(
    tools: PathLike | dict,
    resources: Optional[PathLike | dict] = None,
) -> List[str]:
    """校验工具配置，返回错误消息列表（空列表 = 通过）。

    只做 schema / 引用完整性检查，**不**注册到 ToolNode。
    """
    errors: List[str] = []
    try:
        bundle = parse_tool_bundle(tools)
    except Exception as e:
        return [f"tools: {e}"]

    try:
        res = parse_resources(resources) if resources is not None else Resources()
    except Exception as e:
        return [f"resources: {e}"]

    errors.extend(_validate_bundle(bundle, res))
    return errors


def _validate_bundle(bundle: ToolBundle, resources: Resources) -> List[str]:
    errors: List[str] = []
    names: set[str] = set()
    for i, cfg in enumerate(bundle.tools):
        prefix = f"tools[{i}] ({getattr(cfg, 'name', '?')})"
        name = getattr(cfg, "name", "") or ""
        if not name:
            errors.append(f"{prefix}: name 不能为空")
        elif name in names:
            errors.append(f"{prefix}: 重复的工具名 {name!r}")
        else:
            names.add(name)

        t = getattr(cfg, "type", None)
        if t == "http":
            if not getattr(cfg, "url", None):
                errors.append(f"{prefix}: http 工具需要 url")
        elif t == "native":
            if not getattr(cfg, "module", None) or not getattr(cfg, "function", None):
                errors.append(f"{prefix}: native 工具需要 module 与 function")
        elif t == "sql":
            if not getattr(cfg, "sql", None):
                errors.append(f"{prefix}: sql 工具需要 sql")
            ds = getattr(cfg, "datasource", None)
            url = getattr(cfg, "url", None)
            if not ds and not url:
                errors.append(f"{prefix}: sql 工具需要 datasource 或 url")
            elif ds and ds not in resources.datasources and not url:
                errors.append(
                    f"{prefix}: datasource {ds!r} 未在 resources.datasources 中声明"
                )
        elif t == "vector":
            store = getattr(cfg, "store", None)
            if store and resources.vectorstores and store not in resources.vectorstores:
                errors.append(
                    f"{prefix}: store {store!r} 未在 resources.vectorstores 中声明"
                )
        elif t == "mcp":
            errors.append(f"{prefix}: mcp 工具类型尚未实现，请移除或改用 native")
        elif t == "rpc":
            errors.append(f"{prefix}: rpc 工具类型尚未实现，请移除或改用 native")

        addressing = getattr(cfg, "addressing", None)
        if addressing is not None and (
            not isinstance(addressing, str) or not addressing.strip()
        ):
            errors.append(f"{prefix}: addressing 必须是非空字符串")

    return errors
