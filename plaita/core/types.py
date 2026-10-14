"""
plaita.core.types — Canonical location for Plaita type definitions.

Migrated from plaita/types.py (which is now a compatibility shim).
"""

import json
from datetime import date, datetime
from decimal import Decimal


class ValidationError(RuntimeError):
    pass


STRING = "string"
BOOL = "boolean"
INTEGER = "integer"
FLOAT = "float"
NUMBER = "number"
DECIMAL = "decimal"
ARRAY = "array"
MAP = "map"
OBJECT = "object"
DATETIME = "datetime"
DATE = "date"
TIMESTAMP = "timestamp"
TYPE = "type"
UNION = "union"
OPTIONAL = "optional"
ANY = "any"
TYPE_NOT_SUPPORTED = "type_not_supported"
NULL = "null"

native_types = {
    STRING: str,
    BOOL: bool,
    INTEGER: int,
    FLOAT: float,
    DECIMAL: Decimal,
    NUMBER: (float, int, Decimal),
    ARRAY: (list, tuple, set),
    MAP: dict,
    OBJECT: dict,
    TYPE: str,
    DATETIME: datetime,
    DATE: date,
    TIMESTAMP: float,
    NULL: type(None),
}


# ---------------------------------------------------------------------------
# plaita#69 r7 —— **可持久化数据模型**的唯一事实来源
# ---------------------------------------------------------------------------
# 本模块是仓内类型的权威表（``native_types``）；#69 的"引擎自有结构拷贝"
# （``expression_parser._engine_copy``）与 freshness 登记面
# （``note_fresh_closure`` / ``note_fresh_object``）都从这里取类型集合，
# 不再各自维护 ad-hoc 列表：
#
#   * 容器集合按**精确类型**（``type(x) is``）判定 —— 子类（``OrderedDict`` /
#     ``namedtuple`` / 覆写 ``__deepcopy__`` 的 ``dict`` 子类）绝不能被当成
#     "数据模型里的容器"直接登记：那是 r6 C1 反例（活对象混进 freshness 作用域）
#     的入口。子类由拷贝器**归一化**成普通基类（见 ``_engine_copy``）。
#   * 标量集合从 ``native_types`` **推导**（``io.py`` 的
#     ``Property.default_value`` 是它的子集，由
#     ``tests/unit/test_issue69_persistable_types.py`` 机械钉住），因此
#     DSL 文档承诺可持久化的标量（``Decimal`` / ``datetime`` / ``date`` …）
#     不会因实现里写死的列表漂移而被误拒。

#: 数据模型里的容器种类。**同一个元组对象**同时被拷贝器与登记面消费（守卫测试
#: 断言 ``_ENGINE_COPY_CONTAINER_TYPES is _FRESH_WALKABLE is
#: PERSISTABLE_CONTAINER_TYPES``）。
PERSISTABLE_CONTAINER_TYPES = (dict, list, tuple, set, frozenset)

#: 其中可就地改写（就地 mutator 唯一能命中的目标种类）。
PERSISTABLE_MUTABLE_TYPES = (dict, list, set)


def _derive_persistable_scalar_types() -> list:
    """从 ``native_types`` 展平出**不可变标量**类型（顺序稳定、去重）。"""
    found: list = []
    for declared in native_types.values():
        candidates = declared if isinstance(declared, tuple) else (declared,)
        for candidate in candidates:
            if candidate in PERSISTABLE_CONTAINER_TYPES:
                continue
            if candidate not in found:
                found.append(candidate)
    return found


#: 数据模型里的不可变标量（``native_types`` 推导 + 两个 r6 起就接受的不可变
#: 额外类型 ``bytes``/``complex``：它们无法成为就地写穿通道，接受不放宽 mutate 面）。
PERSISTABLE_SCALAR_TYPES = tuple(_derive_persistable_scalar_types()) + (bytes, complex)

#: 数据模型接受的**全部**精确类型（容器 + 标量）。
PERSISTABLE_TYPES = PERSISTABLE_CONTAINER_TYPES + PERSISTABLE_SCALAR_TYPES

#: 根本无法就地改写的类型（mutator 命中它们只会原生报错，不需要 fresh 证明）：
#: 标量 + 两个不可变容器。
PERSISTABLE_IMMUTABLE_TYPES = PERSISTABLE_SCALAR_TYPES + (tuple, frozenset)


def get_native_type(data_type: str, str_value=True):
    probable_type = native_types.get(data_type)
    if probable_type is None:
        raise ValueError(f"UnKnown data type {data_type}")
    if isinstance(probable_type, tuple):
        return probable_type[0].__name__ if str_value else probable_type[0]
    return probable_type.__name__ if str_value else probable_type


data_validators = {
    "required": (lambda value: value is not None, "是必填项"),
    "max_length": (lambda value, length: len(value) <= length, "长度不能超过{length}"),
    "min": (lambda value, min_value: value >= min_value, "不能小于{min_value}"),
    "max": (lambda value, max_value: value <= max_value, "不能大于{max_value}"),
}


def register_validator(name, func, message=None):
    if name in data_validators:
        raise ValueError(f"validator {name} already exists")
    data_validators[name] = func, message


def valid(data_type, value, validators=None):
    _validate_data_type(data_type, value)
    _validate_with_validators(data_type, value, validators)


def _validate_data_type(data_type, value):
    if data_type == ANY:
        return
    native_type = native_types.get(data_type)
    if native_type and not isinstance(value, native_type):
        raise ValidationError(f"必须是{data_type}类型的值，但是传入了{json.dumps(value)}")


def _validate_with_validators(data_type, value, validators):
    if not validators:
        return
    for validator in validators:
        name, params = _parse_validator(validator)
        _run_validator(name, value, data_type, params)


def _parse_validator(validator):
    if isinstance(validator, str):
        return validator, {}
    name = validator.get("name")
    if not name:
        raise ValueError(f"未找到指定的Validator: {validator}")
    params = {k: v for k, v in validator.items() if k != "name"}
    return name, params


def _run_validator(name, value, data_type, params):
    validator_func, message = data_validators.get(name)
    if not validator_func:
        raise ValueError(f"validator {name} not found")
    if not validator_func(value, **params):
        _raise_validation_error(message, value, data_type, params, name)


def _raise_validation_error(message, value, data_type, params, name):
    if message:
        raise ValidationError(message.format(value=value, data_type=data_type, **params))
    raise ValidationError(f"数据验证失败: {value}, 校验器: {name}, 参数: {params}")
