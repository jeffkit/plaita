import json
import math
import re
import warnings
from collections import namedtuple
from collections.abc import MutableMapping
from copy import copy
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Iterator, List, Optional, Union

import pyparsing as pp
from pydantic import BaseModel, Field, model_validator

from plaita.core import types
from plaita.core.expression import (
    ExpressionRegistry,
    FunctionCategory,
    FunctionDescriptor,
    get_default_expression_registry,
)
from .logger import logger
from plaita.core.types import ValidationError

FieldError = namedtuple("FieldError", ["field", "message"])


class _RegisteredFunctionsProxy(MutableMapping):
    """Dict-like wrapper over ExpressionRegistry for backward compatibility.

    Read access (``proxy["add"]``, ``proxy.get("add")``) returns the
    underlying callable.  Mutation (``proxy["my_fn"] = fn``) registers the
    function in the wrapped registry and emits a DeprecationWarning.
    """

    def __init__(self, registry: ExpressionRegistry) -> None:
        self._registry = registry

    def __getitem__(self, name: str):
        desc = self._registry.get(name)
        if desc is None:
            raise KeyError(name)
        return desc.func

    def get(self, name, default=None):
        desc = self._registry.get(name)
        return desc.func if desc is not None else default

    def __setitem__(self, name: str, func) -> None:
        warnings.warn(
            "Direct mutation of REGISTERED_FUNCTIONS is deprecated. "
            "Use ExpressionRegistry.register() from plaita.core.expression instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._registry.register(
            name, func, FunctionCategory.TYPE, override=True,
        )

    def __delitem__(self, name: str) -> None:
        warnings.warn(
            "Direct mutation of REGISTERED_FUNCTIONS is deprecated. "
            "Use plaita.core.expression.ExpressionRegistry instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if name not in self._registry:
            raise KeyError(name)
        self._registry.unregister(name)

    def __contains__(self, name: object) -> bool:
        return name in self._registry

    def __iter__(self) -> Iterator[str]:
        return iter(self._registry._functions)

    def __len__(self) -> int:
        return len(self._registry)

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


def get_value(content, *keys, default=None):
    for key in keys:
        if key in content:
            return content[key]
    return default


class PropertyException(RuntimeError):
    def __init__(self, *field_errors):
        self.errors = field_errors


class Property(BaseModel):
    data_type: str
    name: Optional[str] = None
    label: Optional[str] = None
    desc: Optional[str] = None
    is_required: bool = False
    choices: Optional[List] = None
    default_value: Optional[Union[str, int, float, bool, Decimal, List, Dict, datetime, date]] = None
    item_type: Optional["Property"] = None
    children: Optional[Union[List["Property"], Dict[str, "Property"]]] = Field(default_factory=list)
    validators: Optional[List[Union[str, Dict]]] = Field(default_factory=list)
    min: Optional[Union[int, float, Decimal]] = None
    max: Optional[Union[int, float, Decimal]] = None
    max_length: Optional[int] = None
    ref: Optional[str] = None
    additional: Optional[Dict] = None

    @model_validator(mode="before")
    @classmethod
    def validate_property(cls, data: Dict) -> Dict:
        """Initialize validators based on property constraints"""
        if not data:
            return data

        # Create a new dict for normalized data
        normalized = data.copy()

        # Handle field name mappings
        field_mappings = {
            "dataType": "data_type",
            "isRequired": "is_required",
            "defaultValue": "default_value",
            "itemType": "item_type",
            "maxLength": "max_length",
        }

        # Normalize field names
        for old_key, new_key in field_mappings.items():
            if old_key in data:
                logger.debug(f"normalize field {old_key} to {new_key}")
                normalized[new_key] = data.pop(old_key)

        # Initialize or get existing validators list
        validators = normalized.get("validators", [])
        if not isinstance(validators, list):
            validators = []

        # Add required validator if needed
        if normalized.get("is_required"):
            if "required" not in validators:
                validators.append("required")

        # Add max_length validator for string type if not already present
        if normalized.get("data_type") == types.STRING and normalized.get("max_length"):
            max_length_validator = {"name": "max_length", "length": normalized["max_length"]}
            if max_length_validator not in validators:
                validators.append(max_length_validator)

        # Add min validator if not already present
        if normalized.get("min") is not None:
            min_validator = {"name": "min", "min_value": normalized["min"]}
            if min_validator not in validators:
                validators.append(min_validator)

        # Add max validator if not already present
        if normalized.get("max") is not None:
            max_validator = {"name": "max", "max_value": normalized["max"]}
            if max_validator not in validators:
                validators.append(max_validator)

        normalized["validators"] = validators
        logger.debug(f"normalized: {normalized}")
        return normalized

    # 2. 简化 OBJECT 和 ARRAY 类型的处理
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

    # 3. 优化后的 from_json 函数
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
            choices=content.get("choices"),
            validators=content.get("validators", []),
            max=content.get("max"),
            min=content.get("min"),
            max_length=get_value(content, "max_length", "maxLength"),
            ref=content.get("ref"),
            additional=content.get("additional"),
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

    def valid(self, value):
        return types.valid(self.data_type, value, self.validators)

    def from_str(self, text):
        # 从字符串转换为对应类型的值，并调用valid函数进行校验
        def parse_json(text_obj):
            try:
                return json.loads(text_obj)
            except json.JSONDecodeError:
                raise ValidationError(f"invalid json string: {text_obj}")

        cast_map = {
            types.STRING: str,
            types.BOOL: bool,
            types.INTEGER: int,
            types.FLOAT: float,
            types.DECIMAL: float,
            types.ARRAY: parse_json,
            types.OBJECT: parse_json,
            types.MAP: parse_json,
        }
        if self.data_type in cast_map:
            value = cast_map[self.data_type](text)
        else:
            raise ValidationError(f"unsupported data type: {self.data_type}")
        if value is None:
            raise ValidationError(f"invalid value: {text}")
        self.valid(value)
        return value

    def property_for_path(self, path: Union[str, List[str]]) -> Optional["Property"]:
        assert self.data_type == types.OBJECT, f'invalid call of "property_for_path" on data_type: {self.data_type}'
        assert path, f"path is required for getting property from {self.name}"
        real_path = path
        if isinstance(path, str):
            real_path = path.split(".")
        if real_path[0] not in self.children:
            return None
        prop = self.children[real_path[0]]
        rest_path = real_path[1:]
        if prop.data_type == types.OBJECT:
            return prop.property_for_path(rest_path)
        return prop

    def __str__(self):
        if self.data_type == types.ARRAY:
            if self.item_type:
                return str([{str(self.item_type)}])
            elif self.children:
                return f"{[str(child) for child in self.children]}"
        if self.data_type == types.OBJECT:
            return f"{self.name}: {json.dumps(dict([(k, str(v)) for k, v in self.children.items()]))}"
        return f"{self.data_type}"


def get_attr(obj, path):
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
    """
    if not isinstance(value, str):
        return _evaluate_non_string(value, context, prefix, registry)

    if not value.startswith(prefix):
        return _evaluate_non_prefix_string(value, context, prefix, registry)

    return _evaluate_prefix_string(value, context, prefix, registry)


def _evaluate_non_string(value, context, prefix, registry=None):
    if isinstance(value, list):
        return [evaluate(item, context, prefix, registry) for item in value]
    if isinstance(value, dict):
        return {key: evaluate(val, context, prefix, registry) for key, val in value.items()}
    return value


def _evaluate_non_prefix_string(value, context, prefix, registry=None):
    pattern = r"\{%\s*(" + re.escape(prefix) + r"(?:(?!\{%|\%\}).)*?)\s*\%}"
    regx = re.compile(pattern, re.DOTALL)
    if not regx.search(value):
        return value
    return regx.sub(lambda m: str(evaluate(m.group(1).strip(), context, prefix, registry)), value)


def _evaluate_prefix_string(value, context, prefix, registry=None):
    pattern = r"^" + re.escape(prefix) + r"F\.([a-zA-Z_][a-zA-Z0-9_]*?)\((.*?)\)$"
    if re.match(pattern, value):
        return parse_function(value, context, prefix, registry)

    paths = value.split(".")
    obj = _get_initial_object(paths, context)
    paths = _adjust_paths(paths)

    for path in paths:
        obj = _get_object_attribute(obj, path, context, prefix, registry)
        if obj is None:
            return None
    return obj


def _get_initial_object(paths, context):
    if len(paths) > 1 and paths[1].isdigit():
        return context[paths[0]][int(paths[1])]
    if re.search(r"\[\-?\d+\]", paths[0]):
        key, index = re.findall(r"([^\[\]]+)", paths[0])
        return context[key][int(index)]
    return context[paths[0]]


def _adjust_paths(paths):
    if len(paths) > 1 and paths[1].isdigit():
        return paths[2:]
    if re.search(r"\[\-?\d+\]", paths[0]):
        return paths[1:]
    return paths[1:]


def _get_object_attribute(obj, path, context, prefix, registry=None):
    if path.isdigit():
        return obj[int(path)]
    # 为子层级的变量补充前缀
    path = f"{prefix}{path}" if path in ["GLOBAL", "INPUT", "NODE", "PARENT", "ENV"] else path
    return evaluate(get_attr(obj, path), obj, prefix, registry)


# 缓存解析器基础组件，避免每次调用都重新创建
_parser_components_cache = {}
_parser_cache_lock = __import__("threading").Lock()


def _get_parser_components(prefix: str):
    """
    获取或创建基础解析器组件
    
    Args:
        prefix: 表达式前缀
        
    Returns:
        解析器基础组件元组 (constant, variable, identifier)
    """
    cache_key = prefix
    
    with _parser_cache_lock:
        if cache_key in _parser_components_cache:
            return _parser_components_cache[cache_key]
    
    # 定义常量
    number = pp.pyparsing_common.number
    boolean = pp.Keyword("True") | pp.Keyword("False") | pp.Keyword("true") | pp.Keyword("false")
    string = pp.QuotedString('"') | pp.QuotedString("'")
    constant = boolean | string | number

    # 定义标识符
    identifier = pp.Word(pp.alphas, pp.alphanums + "_")

    # 支持0层和多层变量路径
    variable_path = pp.Combine(identifier + pp.ZeroOrMore("." + identifier))
    variable_prefix = (
        pp.Literal(f"{prefix}INPUT")
        | pp.Literal(f"{prefix}NODE")
        | pp.Literal(f"{prefix}PARENT")
        | pp.Literal(f"{prefix}GLOBAL")
        | pp.Literal(f"{prefix}ENV")
    )
    # variable需要支持0层和多层变量路径，如 $INPUT, $INPUT.name, $INPUT.step1.name
    variable = pp.Combine(variable_prefix + pp.Optional("." + variable_path))
    
    with _parser_cache_lock:
        _parser_components_cache[cache_key] = (constant, variable, identifier)
    
    return constant, variable, identifier


def _lookup_function(registry, func_name):
    """Resolve a function callable by name from *registry* (or the default).

    Returns ``None`` when the function is not registered.  Callers should
    fall back to the "undefined" sentinel to preserve historical behavior
    (scoped registries deliberately return ``"undefined"`` for functions they
    don't expose — see test_expression.py).
    """
    if registry is None:
        return REGISTERED_FUNCTIONS.get(func_name)
    if isinstance(registry, ExpressionRegistry):
        return registry.get_callable(func_name)
    # 兼容旧的 dict-like proxy
    return registry.get(func_name)


_UNDEFINED = lambda *args, **kwargs: "undefined"  # noqa: E731


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
        解析并执行后的结果，如果无法解析则返回原始表达式
    """
    # 快速检查是否可能包含函数调用
    if f"{prefix}F." not in expression:
        return expression

    # 定义求值函数（需要访问 context 和 prefix）
    def evaluate_function(tokens):
        func_name = tokens[0].split(".")[1].split("(")[0]
        args = [evaluate(arg, context, prefix, registry) for arg in tokens[1]]
        logger.debug("parse_function: func_name=%s, args=%s", func_name, args)
        func = _lookup_function(registry, func_name)
        if func is None:
            # 不再静默：函数未注册时记一条 warning，让拼写错误 / 沙箱漏注册可被
            # 日志捕捉。返回值仍是 "undefined" 以兼容 scoped registry 语义。
            logger.warning(
                "expression function %r not registered (registry=%r); returning 'undefined'",
                func_name, registry,
            )
            func = _UNDEFINED
        return func(*args)

    # 获取缓存的基础组件
    constant, variable, identifier = _get_parser_components(prefix)

    # 创建新的 function_call（因为 Forward 需要每次绑定不同的 parseAction）
    function_call = pp.Forward()
    expr = constant | variable | function_call

    # 定义函数调用规则并绑定 parseAction
    function_call <<= (
        pp.Combine(f"{prefix}F." + identifier + "(") + pp.Group(pp.delimitedList(expr) + pp.Optional(",")) + ")"
    )
    function_call.setParseAction(evaluate_function)

    try:
        parsed = function_call.parseString(expression, parseAll=True)
        return parsed[0]
    except pp.exceptions.ParseException:
        return expression


def match(props: Property, obj, context=None):
    """
    检查obj是否符合props所描述的格式要求。本方法只应用于运行前做合法性检测。
    :param props: 一个数据结构，描述一个对象具备的属性
    :param obj: 供检查对象
    :param context: 可选的上下文，用于处理复杂的属性匹配
    :return: 如果结构符合要求，则返回True
    """
    # 如果props是字符串，尝试从context中解析
    if isinstance(props, str):
        props = find_property(props, context) if context else None
        if not props:
            return False

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


def _match_complex_type(props: Property, obj):
    """Helper method to match complex types"""
    if props.data_type == types.ARRAY:
        return _match_array(props, obj)
    elif props.data_type == types.OBJECT:
        return _match_object(props, obj)
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


def find_property(path, context=None):
    """
    Find a property based on a path, optionally using a context.

    :param path: Property path to find
    :param context: Optional context dictionary to resolve references
    :return: Resolved property or False
    """
    if not context:
        context = {}

    # Determine property source based on path prefix
    if path.startswith("INPUT"):
        # Assuming input_property is a method or attribute in the context
        prop = context.get("input_property")
    elif path.startswith("PARENT"):
        # For parent, recursively find property in parent context
        parent_context = context.get("parent", {})
        return find_property(path[len("PARENT.") :], parent_context)
    elif path.startswith("NODE"):
        # For node, find node's output property
        node_id = path[len("NODE.") :]
        nodes = context.get("nodes", [])
        node = next((n for n in nodes if n.id == node_id), None)
        prop = node.output_property if node else None
    elif path.startswith("GLOBAL"):
        # Global context: create a property from global context keys
        global_context = context.get("global_context", {})
        prop = Property(
            data_type=types.OBJECT, children={k: Property(data_type=types.ANY) for k in global_context.keys()}
        )
    else:
        return False

    if not prop:
        return False

    # If no further path specified, return the property
    if "." not in path:
        return prop

    # Expand property for nested paths
    return _expand_property(prop, path.split(".")[1:])


def _expand_property(prop, path_parts):
    """
    Expand a property based on a path

    :param prop: Initial property
    :param path_parts: List of path parts to traverse
    :return: Expanded property
    """
    if not prop:
        return None

    # For simple types, can't expand further
    if prop.data_type not in [types.ARRAY, types.OBJECT]:
        return prop

    # Create a copy to avoid modifying original
    prop = prop.model_copy()

    if prop.data_type == types.ARRAY:
        if prop.item_type:
            prop.item_type = _expand_property(prop.item_type, path_parts)
        elif prop.children:
            prop.children = [_expand_property(child, path_parts) for child in prop.children]
    elif prop.data_type == types.OBJECT:
        prop.children = {field: _expand_property(val, path_parts) for field, val in prop.children.items()}

    return prop


def expanded_property(prop: Union[Property, str], context=None) -> Optional[Property]:
    """
    展开Property，把一些引用的属性展开成真实结构
    :param prop: 需要展开的属性
    :param context: 上下文，用于解析引用属性
    :return: Property，展开后的真实结构
    """
    if not prop:
        return None

    if isinstance(prop, Property):
        return _expand_property_object(prop, context)
    elif isinstance(prop, str):
        if prop.startswith("@"):
            return _expand_reference_property(prop, context)
        elif prop.startswith("$"):
            # Assuming evaluate is imported or defined in this module
            return evaluate(prop)

    return prop


def _expand_property_object(prop: Property, context=None) -> Property:
    if prop.data_type not in [types.ARRAY, types.OBJECT]:
        return prop

    prop = copy(prop)
    if prop.data_type == types.ARRAY:
        prop = _expand_array_property(prop, context)
    elif prop.data_type == types.OBJECT:
        prop = _expand_object_property(prop, context)

    return prop


def _expand_array_property(prop: Property, context=None) -> Property:
    if prop.item_type:
        prop.item_type = expanded_property(prop.item_type, context)
    elif prop.children:
        prop.children = [expanded_property(child, context) for child in prop.children]
    return prop


def _expand_object_property(prop: Property, context=None) -> Property:
    if not prop.children:
        return prop
    prop.children = {field: expanded_property(val, context) for field, val in prop.children.items()}
    return prop


def _expand_reference_property(prop: str, context=None) -> Optional[Property]:
    """
    展开引用属性
    :param prop: 引用属性字符串
    :param context: 上下文，用于解析引用属性
    :return: 展开后的属性
    """
    if not context:
        context = {}

    if prop.startswith("@INPUT"):
        return _expand_input_property(prop, context)
    if prop.startswith("@OUTPUT"):
        return _expand_output_property(prop, context)
    if prop.startswith("@node#"):
        return _expand_node_property(prop, context)

    return None


def _expand_input_property(prop: str, context: dict) -> Optional[Property]:
    """
    展开输入属性
    :param prop: 输入属性引用
    :param context: 上下文
    :return: 展开后的属性
    """
    input_type = context.get("input_type")
    if not input_type:
        return None

    if "." in prop:
        return input_type.property_for_path(prop[len("@INPUT.") :])
    return input_type


def _expand_output_property(prop: str, context: dict) -> Optional[Property]:
    """
    展开输出属性
    :param prop: 输出属性引用
    :param context: 上下文
    :return: 展开后的属性
    """
    output_type = context.get("output_type")
    if not output_type:
        return None

    if "." in prop:
        return output_type.property_for_path(prop[len("@OUTPUT.") :])
    return output_type


def _expand_node_property(prop: str, context: dict) -> Optional[Property]:
    """
    展开节点属性
    :param prop: 节点属性引用
    :param context: 上下文
    :return: 展开后的属性
    """
    paths = prop.split(".")
    node_id = paths[0].split("-")[-1]
    nodes = context.get("nodes", [])
    node = next((n for n in nodes if n.id == node_id), None)

    if not node:
        return None

    if prop.startswith("@node#in"):
        return node.input_type.property_for_path(paths[1:]) if len(paths) >= 2 else node.input_type
    if prop.startswith("@node#out"):
        return node.output_type.property_for_path(paths[1:]) if len(paths) >= 2 else node.output_type

    return None
