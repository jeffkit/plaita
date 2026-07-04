import json
import re
import warnings
from collections.abc import MutableMapping
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Iterator, List, Optional, Union

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from plaita.core import types
from plaita.core.expression import (
    ExpressionRegistry,
    FunctionCategory,
    get_default_expression_registry,
)
from plaita.core.expression_parser import (
    ExpressionParser,
    _parser_components_cache,
)
from .logger import logger


def get_value(content, *keys, default=None):
    for key in keys:
        if key in content:
            return content[key]
    return default


class Property(BaseModel):
    """描述一个数据槽的类型 schema。

    只保留运行时真正消费的字段：``data_type`` + 嵌套结构（``children`` /
    ``item_type``）+ 元信息（``name`` / ``label`` / ``desc`` / ``is_required`` /
    ``default_value``）。值域约束（min/max/maxLength/choices/validators/ref）
    历史上曾被解析但 ``match`` 从不读取，已移除以免"声明了却不生效"误导用户。
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    data_type: str = Field(alias=AliasChoices("data_type", "dataType", "type"))
    name: Optional[str] = None
    label: Optional[str] = Field(default=None, alias=AliasChoices("label", "title"))
    desc: Optional[str] = Field(default=None, alias=AliasChoices("desc", "description"))
    is_required: bool = Field(default=False, alias=AliasChoices("is_required", "isRequired"))
    default_value: Optional[Union[str, int, float, bool, Decimal, List, Dict, datetime, date]] = Field(
        default=None, alias=AliasChoices("default_value", "defaultValue", "default")
    )
    item_type: Optional["Property"] = Field(default=None, alias=AliasChoices("item_type", "itemType"))
    children: Optional[Union[List["Property"], Dict[str, "Property"]]] = Field(default_factory=list)

    def handle_object_type(self, content):
        children = content.get("children", {}) or content.get("properties", {})
        self.children = {}
        for key, value in children.items():
            prop = Property.from_json(value)
            if prop.name is None:
                prop.name = key
            self.children[key] = prop
        if isinstance(content.get("required"), list):
            for name in content["required"]:
                if name in self.children:
                    self.children[name].is_required = True

    def handle_array_type(self, content):
        item_type = content.get("item_type") or content.get("items")
        children = content.get("children") or content.get("properties", [])
        if item_type:
            self.item_type = Property.from_json(item_type)
        elif children:
            self.children = [Property.from_json(child) for child in children]

    @classmethod
    def from_json(cls, content):
        if not content:
            return None
        if isinstance(content, cls):
            return content
        if isinstance(content, str):
            content = json.loads(content)

        pro = cls._create_property(content)
        cls._handle_required(pro, content)
        cls._handle_complex_types(pro, content)

        return pro

    @classmethod
    def _create_property(cls, content):
        return cls(
            data_type=get_value(content, "data_type", "dataType", "type"),
            name=content.get("name"),
            label=get_value(content, "label", "title"),
            desc=get_value(content, "desc", "description"),
            is_required=get_value(content, "is_required", "isRequired", default=False),
            default_value=get_value(content, "default_value", "defaultValue", "default"),
        )

    @staticmethod
    def _handle_required(pro, content):
        if isinstance(content.get("required"), bool):
            pro.is_required = content["required"]

    @staticmethod
    def _handle_complex_types(pro, content):
        if pro.data_type == types.OBJECT:
            pro.handle_object_type(content)
        elif pro.data_type == types.ARRAY:
            pro.handle_array_type(content)

    def __str__(self):
        if self.data_type == types.ARRAY:
            if self.item_type:
                return str([{str(self.item_type)}])
            elif self.children:
                return f"{[str(child) for child in self.children]}"
        if self.data_type == types.OBJECT:
            return f"{self.name}: {json.dumps(dict([(k, str(v)) for k, v in self.children.items()]))}"
        return f"{self.data_type}"


class _RegisteredFunctionsProxy(MutableMapping):
    """Dict-like wrapper over ExpressionRegistry for backward compatibility.

    Read access (``proxy["add"]``, ``proxy.get("add")``) returns the
    underlying callable.  Mutation (``proxy["my_fn"] = fn``) registers the
    function in the wrapped registry and emits a DeprecationWarning.
    """

    def __init__(self, registry: ExpressionRegistry) -> None:
        self._registry = registry

    @property
    def _current_registry(self) -> ExpressionRegistry:
        """Always return the live default registry to handle test resets."""
        return get_default_expression_registry()

    def __getitem__(self, name: str):
        desc = self._current_registry.get(name)
        if desc is None:
            raise KeyError(name)
        return desc.func

    def get(self, name, default=None):
        desc = self._current_registry.get(name)
        return desc.func if desc is not None else default

    def __setitem__(self, name: str, func) -> None:
        warnings.warn(
            "Direct mutation of REGISTERED_FUNCTIONS is deprecated. "
            "Use ExpressionRegistry.register() from plaita.core.expression instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._current_registry.register(
            name, func, FunctionCategory.TYPE, override=True,
        )

    def __delitem__(self, name: str) -> None:
        warnings.warn(
            "Direct mutation of REGISTERED_FUNCTIONS is deprecated. "
            "Use plaita.core.expression.ExpressionRegistry instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if name not in self._current_registry:
            raise KeyError(name)
        self._current_registry.unregister(name)

    def __contains__(self, name: object) -> bool:
        return name in self._current_registry

    def __iter__(self) -> Iterator[str]:
        return iter(self._current_registry._functions)

    def __len__(self) -> int:
        return len(self._current_registry)

    def __repr__(self) -> str:
        return f"<_RegisteredFunctionsProxy functions={len(self._registry)}>"


REGISTERED_FUNCTIONS = _RegisteredFunctionsProxy(get_default_expression_registry())


def register_function(name, func):
    """Register a custom expression function (deprecated).

    Use ``plaita.core.expression.get_default_expression_registry().register()``
    instead.
    """
    warnings.warn(
        "register_function() is deprecated. "
        "Use ExpressionRegistry.register() from plaita.core.expression instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    registry = get_default_expression_registry()
    registry.register(name, func, FunctionCategory.TYPE, override=True)


def get_attr(obj, path):
    # Note: used by Property matching (``_match_object``), not by the
    # expression evaluator. Kept here as a structural helper.
    if re.search(r"\[\d+\]", path):  # 判断路径是否有数组索引
        key, index = re.findall(r"([^\[\]]+)", path)
        if isinstance(obj, dict):
            return obj.get(key, [])[int(index)]
        elif hasattr(obj, key):
            return getattr(obj, key, [])[int(index)]
    else:
        if hasattr(obj, "__dict__"):
            return getattr(obj, path, None)
        elif isinstance(obj, dict):
            return obj.get(path, None)


def evaluate(value, context, prefix="$", registry=None):
    """
    使用上下文计算真实的值

    Args:
        value: 待求值的值/表达式
        context: 执行上下文
        prefix: 表达式前缀，默认 "$"
        registry: 可选的 ``ExpressionRegistry``（或兼容的 proxy）。为 None 时
            回退到模块级 ``REGISTERED_FUNCTIONS``，保持向后兼容。传入自定义
            registry 后，``$F.func(...)`` 调用将从该 registry 解析函数，
            从而让 scoped/自定义 registry 真正生效。

    求值由 ``plaita.core.expression_parser.ExpressionParser`` 驱动 —— 单套
    pyparsing 文法覆盖字面量 / 变量路径 / 函数调用 / ``{% ... %}`` 插值，
    解析器按 prefix 构建一次并缓存。历史上的「正则粗筛 + pyparsing 精解」
    双轨制已移除。
    """
    return ExpressionParser.for_prefix(prefix).evaluate(value, context, registry)


def parse_function(expression, context, prefix="$", registry=None):
    """
    解析并执行函数表达式

    Args:
        expression: 表达式字符串
        context: 执行上下文
        prefix: 表达式前缀，默认为 "$"
        registry: 可选的 ``ExpressionRegistry``。为 None 时回退到模块级
            ``REGISTERED_FUNCTIONS``。传入自定义 registry 后，函数调用从该
            registry 解析，使 scoped 表达式引擎生效。

    Returns:
        解析并执行后的结果；若不含 ``{prefix}F.`` 调用则原样返回。
    """
    parser = ExpressionParser.for_prefix(prefix)
    _parser_components_cache[prefix] = parser  # backward-compat for old tests
    return parser.parse_function(expression, context, registry)


def match(props: Property, obj, context=None):
    """
    检查obj是否符合props所描述的格式要求。本方法只应用于运行前做合法性检测。
    :param props: 一个数据结构，描述一个对象具备的属性
    :param obj: 供检查对象
    :param context: 可选的上下文，用于处理复杂的属性匹配
    :return: 如果结构符合要求，则返回True
    """
    # 处理None值的情况
    if obj is None:
        return not props.is_required

    # 处理基本类型匹配
    if props.data_type == types.ANY:
        return True

    # 根据数据类型进行严格匹配
    if props.data_type == types.ARRAY:
        return _match_array(props, obj)
    elif props.data_type == types.OBJECT:
        return _match_object(props, obj)
    elif props.data_type == types.STRING:
        return isinstance(obj, str) and obj
    elif props.data_type == types.INTEGER:
        return type(obj) is int
    elif props.data_type == types.FLOAT:
        return isinstance(obj, (int, float))
    elif props.data_type == types.BOOL:
        return obj is True or obj is False
    elif props.data_type == types.NUMBER:
        return isinstance(obj, (int, float, Decimal))

    return False


def _match_array(props: Property, obj):
    """
    Match array type properties

    :param props: Property describing the array
    :param obj: Object to match against the property
    :return: Boolean indicating if the object matches the property
    """
    if not isinstance(obj, list):
        return False

    if props.item_type:
        return all(match(props.item_type, item) for item in obj)

    if len(props.children) != len(obj):
        return False

    return all(match(child, obj[idx]) for idx, child in enumerate(props.children))


def _match_object(props: Property, obj):
    """
    Match object type properties

    :param props: Property describing the object
    :param obj: Object to match against the property
    :return: Boolean indicating if the object matches the property
    """
    if not isinstance(obj, dict):
        return False

    if not props.children:
        return True

    return all(match(prop, get_attr(obj, field)) for field, prop in props.children.items())
