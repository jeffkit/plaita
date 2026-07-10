"""register_source / load_tool_bundle — 桥接到现有 ToolNode。"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from plaita_ai.agent.fot.tools import (
    ParamSchema,
    ToolSchema,
    ToolSpec,
    _NO_DEFAULT,
    _TOOL_MARKER_ATTR,
    register_tool_node,
)
from plaita_ai.tools.config.loader import parse_resources, parse_tool_bundle
from plaita_ai.tools.config.schema import (
    HttpToolConfig,
    NativeToolConfig,
    Resources,
    SqlToolConfig,
    ToolConfig,
    VectorToolConfig,
)
from plaita_ai.tools.resources import set_resource_config
from plaita_ai.tools.source.base import (
    BaseToolSource,
    ParamDef,
    check_success,
)
from plaita_ai.tools.source.http import HttpToolSource
from plaita_ai.tools.source.native import NativeToolSource
from plaita_ai.tools.source.sql import SqlToolSource, sql_param_names
from plaita_ai.tools.source.vector import VectorToolSource

PathOrData = Union[str, Path, Dict[str, Any]]


def _param_defs_from_config(params: Dict[str, Any]) -> Dict[str, ParamDef]:
    result: Dict[str, ParamDef] = {}
    for name, raw in params.items():
        if isinstance(raw, ParamDef):
            result[name] = raw
        elif isinstance(raw, dict):
            result[name] = ParamDef.model_validate(raw)
        elif hasattr(raw, "model_dump"):
            result[name] = ParamDef.model_validate(raw.model_dump())
        else:
            result[name] = ParamDef(type=str(raw))
    return result


def config_to_source(cfg: ToolConfig, resources: Optional[Resources] = None) -> BaseToolSource:
    """把扁平配置项转成 BaseToolSource 实例。"""
    _ = resources  # 连接解析走 resources 注册表；此处仅构造 Source
    params = _param_defs_from_config(getattr(cfg, "params", {}) or {})
    common = {
        "name": cfg.name,
        "description": cfg.description,
        "tags": list(cfg.tags),
        "success_condition": cfg.success_condition,
        "error_message": cfg.error_message,
        "params": params,
    }
    if isinstance(cfg, HttpToolConfig):
        return HttpToolSource(
            **common,
            url=cfg.url,
            method=cfg.method,
            headers=dict(cfg.headers),
            timeout=cfg.timeout,
            response_path=cfg.response_path,
            content_type=cfg.content_type,
        )
    if isinstance(cfg, NativeToolConfig):
        return NativeToolSource(
            **common,
            module=cfg.module,
            function=cfg.function,
        )
    if isinstance(cfg, SqlToolConfig):
        return SqlToolSource(
            **common,
            sql=cfg.sql,
            datasource=cfg.datasource,
            url=cfg.url,
            row_limit=cfg.row_limit,
        )
    if isinstance(cfg, VectorToolConfig):
        return VectorToolSource(
            **common,
            store=cfg.store,
            search_type=cfg.search_type,
            k=cfg.k,
            filter=cfg.filter,
        )
    raise TypeError(f"不支持的工具配置类型: {type(cfg).__name__}")


def schema_from_source(source: BaseToolSource, func: Callable[..., Any]) -> ToolSchema:
    """优先用配置 params 生成 ToolSchema；否则从 callable / SQL / URL 推断。"""
    from plaita_ai.agent.fot.tools import _build_schema

    param_defs = dict(source.params)
    if not param_defs and isinstance(source, HttpToolSource):
        import string

        for _, fname, _, _ in string.Formatter().parse(source.url):
            if fname and fname not in param_defs:
                param_defs[fname] = ParamDef(type="string", required=True)

    if not param_defs and isinstance(source, SqlToolSource):
        for name in sql_param_names(source.sql):
            param_defs[name] = ParamDef(type="string", required=True)

    if not param_defs and isinstance(source, VectorToolSource):
        # retrieve(query, k=...) — k 可选
        param_defs["query"] = ParamDef(type="string", required=True, description="检索查询")
        param_defs["k"] = ParamDef(
            type="integer",
            required=False,
            default=source.k,
            description="返回条数",
        )

    if not param_defs:
        schema = _build_schema(func, source.name, source.description)
        if source.description:
            schema.description = source.description
        return schema

    params: List[ParamSchema] = []
    for name, pdef in param_defs.items():
        required = pdef.required and pdef.default is None
        default = pdef.default if not required else _NO_DEFAULT
        if not pdef.required and pdef.default is None:
            default = None
            required = False
        params.append(
            ParamSchema(
                name=name,
                type=pdef.type,
                required=required,
                default=default,
                description=pdef.description,
                py_annotation="",
            )
        )
    has_auth = False
    has_ctx = False
    try:
        import inspect

        sig = inspect.signature(func)
        has_auth = "auth_context" in sig.parameters
        has_ctx = "context" in sig.parameters
    except (TypeError, ValueError):
        pass

    return ToolSchema(
        name=source.name,
        description=source.description or "无描述",
        params=params,
        has_auth_context=has_auth,
        has_tool_context=has_ctx,
    )


def _wrap_success_check(func: Callable[..., Any], source: BaseToolSource) -> Callable[..., Any]:
    if not source.success_condition:
        return func

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = func(*args, **kwargs)
        if not check_success(result, source.success_condition):
            raise RuntimeError(source.error_message)
        return result

    return wrapper


def _attach_schema(func: Callable[..., Any], schema: ToolSchema) -> Callable[..., Any]:
    setattr(func, _TOOL_MARKER_ATTR, schema)
    return func


def register_source(source: BaseToolSource) -> ToolSpec:
    """代码轨 / 配置轨统一入口：Source → callable → ToolNode。"""
    func = source.to_callable()
    func = _wrap_success_check(func, source)
    schema = schema_from_source(source, func)
    func = _attach_schema(func, schema)
    specs = register_tool_node(func)
    return specs[0]


def register_sources(*sources: BaseToolSource) -> List[ToolSpec]:
    return [register_source(s) for s in sources]


def load_tool_bundle(
    tools: PathOrData,
    resources: Optional[PathOrData] = None,
) -> List[ToolSpec]:
    """从 YAML/JSON（或 dict）加载并注册工具。"""
    bundle = parse_tool_bundle(tools)
    res = parse_resources(resources) if resources is not None else Resources()
    set_resource_config(res)
    specs: List[ToolSpec] = []
    for cfg in bundle.tools:
        source = config_to_source(cfg, res)
        specs.append(register_source(source))
    return specs
