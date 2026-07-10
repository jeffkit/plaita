"""BaseToolSource + ToolContext — 数据源工具的统一抽象。

设计约束：
- ToolContext 只放基础设施语义（trace / caller / auth / baggage）
- 业务字段（tenant、user_id 等）一律进 baggage，由调用方约定
- BaseToolSource 既是配置模型，也可代码直接实例化
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import Any, Callable, ClassVar, Dict, List, Optional

from pydantic import BaseModel, Field


class ToolContext(BaseModel):
    """工具执行时的横切上下文 — 无业务域语义。"""

    trace_id: Optional[str] = None
    request_id: Optional[str] = None
    caller: Optional[str] = None  # "agent" | "flow" | "mcp" | "cli" | ...
    flow_id: Optional[str] = None
    auth: Optional[Any] = None  # 不透明凭证；工具自行解读
    baggage: Dict[str, Any] = Field(default_factory=dict)


def build_tool_context(execution: Any) -> ToolContext:
    """从 flow 执行上下文组装 ToolContext。

    读取约定（均在 ``$GLOBAL``）：
    - ``trace_id`` / ``request_id`` / ``caller`` / ``flow_id``
    - ``auth_context`` → ``auth``（向后兼容现有注入）
    - ``baggage`` → 任意扩展元数据
    """
    def _get(key: str, default: Any = None) -> Any:
        try:
            return execution.get_global_variable(key, default)
        except TypeError:
            # 部分实现不接受 default 关键字
            try:
                val = execution.get_global_variable(key)
                return default if val is None else val
            except Exception:
                return default
        except Exception:
            return default

    baggage = _get("baggage") or {}
    if not isinstance(baggage, dict):
        baggage = {}

    return ToolContext(
        trace_id=_get("trace_id"),
        request_id=_get("request_id"),
        caller=_get("caller"),
        flow_id=_get("flow_id"),
        auth=_get("auth_context"),
        baggage=baggage,
    )


class ParamDef(BaseModel):
    """配置轨参数定义（扁平 schema，非 Component）。"""

    type: str = "string"  # JSON-schema 风格: string/integer/number/boolean/object/array
    required: bool = True
    default: Any = None
    description: str = ""


class BaseToolSource(BaseModel, ABC):
    """所有数据源工具的公共父类。"""

    type: ClassVar[str] = "base"
    name: str
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    success_condition: Optional[str] = None  # 简单路径真值，如 "$.ok"
    error_message: str = "工具调用失败"
    # 配置轨显式参数 schema；代码轨可留空，由 to_callable 签名推断
    params: Dict[str, ParamDef] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}

    @abstractmethod
    def to_callable(self) -> Callable[..., Any]:
        """产出 ToolNode 可注册的 callable。"""

    def accepts_context(self, func: Optional[Callable[..., Any]] = None) -> bool:
        """判断 callable 是否声明 ``context`` 参数。"""
        target = func or self.to_callable()
        try:
            return "context" in inspect.signature(target).parameters
        except (TypeError, ValueError):
            return False


def extract_json_path(data: Any, path: Optional[str]) -> Any:
    """极简 ``$.a.b`` / ``a.b`` 路径抽取（不依赖 jsonpath_ng）。"""
    if not path:
        return data
    cleaned = path.strip()
    if cleaned.startswith("$"):
        cleaned = cleaned[1:]
    if cleaned.startswith("."):
        cleaned = cleaned[1:]
    if not cleaned:
        return data
    cur = data
    for part in cleaned.split("."):
        if part == "":
            continue
        if isinstance(cur, dict):
            if part not in cur:
                raise KeyError(f"json path {path!r}: missing key {part!r}")
            cur = cur[part]
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError as e:
                raise KeyError(f"json path {path!r}: list index expected, got {part!r}") from e
            cur = cur[idx]
        else:
            raise KeyError(f"json path {path!r}: cannot traverse {type(cur).__name__} with {part!r}")
    return cur


def check_success(output: Any, condition: Optional[str]) -> bool:
    """若 *condition* 为空则视为成功；否则按路径抽取后做真值判断。"""
    if not condition:
        return True
    try:
        value = extract_json_path(output, condition)
    except KeyError:
        return False
    return bool(value)
