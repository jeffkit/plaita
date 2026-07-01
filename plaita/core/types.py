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
