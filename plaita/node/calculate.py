import json
from typing import Callable, ClassVar, Dict, Optional

from pydantic import BaseModel, Field, model_validator

from plaita.core import types
from ..io import Property, evaluate
from .basic import Node

FUNCTIONS = {}


class Function(BaseModel):
    name: str
    func: Callable
    param_type: Optional[Dict[str, Property]] = Field(default_factory=dict)
    return_type: Optional[Property] = None
    label: Optional[str] = None
    description: Optional[str] = None

    def __call__(self, context, *args, **kwargs):
        params = {}
        for key, value in kwargs.items():
            if isinstance(value, Call):
                value = value.invoke(context)
            else:
                value = evaluate(value, context)
            params[key] = value
        return self.func(**params)

    LEGACY_KEYS: ClassVar[frozenset] = frozenset({"paramType", "returnType"})

    @model_validator(mode="before")
    @classmethod
    def _schema_hygiene(cls, values):
        from .basic import warn_unknown_keys
        return warn_unknown_keys(cls, values)

    @model_validator(mode="before")
    @classmethod
    def setup_param_type(cls, values):
        # 兼容有两种风格的命名变量
        values["param_type"] = values.get("paramType") or values.get("param_type")
        values["return_type"] = values.get("returnType") or values.get("return_type")
        return values


def register_function(name, func, param_type, return_type, label=None, description=None):
    # Convert param_type dict to Property objects
    param_properties = {}
    for param_name, param_type_info in param_type.items():
        if isinstance(param_type_info, list):
            # For number types that accept multiple types
            param_properties[param_name] = Property(data_type=param_type_info[0])
        else:
            param_properties[param_name] = Property(data_type=param_type_info)

    # Convert return_type to Property
    if isinstance(return_type, list):
        return_property = Property(data_type=return_type[0])
    else:
        return_property = Property(data_type=return_type)

    FUNCTIONS[name] = Function(
        name=name,
        func=func,
        param_type=param_properties,
        return_type=return_property,
        label=label,
        description=description,
    )


class Call(BaseModel):
    function_name: str
    params: Dict = Field(default_factory=dict)

    @classmethod
    def from_json(cls, content):
        if content is None:
            return None
        if isinstance(content, str):
            content = json.loads(content)
        if isinstance(content, Call):
            return content
        assert isinstance(content, dict), "unknown format of call config : %s" % content
        assert "function_name" in content, "function_name is required for call config"
        function_name = content["function_name"]
        func: Function = FUNCTIONS.get(function_name, None)
        assert func is not None, "function %s not registered " % function_name

        params = content.get("params", {})  # 根据参数的声明确定类型
        real_params = {}
        for key, value in params.items():
            if not isinstance(value, dict):
                real_params[key] = value
                continue
            if "function_name" in value:
                real_params[key] = cls.from_json(value)
        return cls(function_name=function_name, params=real_params)

    def invoke(self, context):
        function: Function = FUNCTIONS.get(self.function_name)
        return function(context, **self.params)


class Calculate(Node):
    """
    求值表达式节点，允许通过内置函数的链式调用，达到求值的目的。
    表达式形如：
    {
        'function_name': 'add',
        'params': {
            'left': 10,
            'right': {
                'function_name': 'mul',
                'params': {
                }
            }
        }
    }
    """

    node_type: ClassVar[str] = "calculate"
    node_name: ClassVar[str] = "求值"
    call: Optional[Call] = None

    @model_validator(mode="before")
    @classmethod
    def setup_calculate(cls, values) -> "Calculate":
        values["call"] = Call.from_json(values.get("expression"))
        return values

    def execute(self, execution):
        return self.call.invoke(execution.context)


number_types = [types.INTEGER, types.FLOAT, types.DECIMAL]


def _register_all_functions() -> None:
    """Register all built-in functions into FUNCTIONS. Called at module load and during test resets."""
    #  #### 数值函数 ####
    register_function(
        "add",
        lambda left, right: left + right,
        {
            "left": number_types,
            "right": number_types,
        },
        number_types,
        "加法",
        "返回两个数值类型的参数相加的结果",
    )

    register_function(
        "sub",
        lambda left, right: left - right,
        {
            "left": number_types,
            "right": number_types,
        },
        number_types,
        "减法",
        "返回两个数值类型的参数相减的结果",
    )

    register_function(
        "multiply",
        lambda left, right: left * right,
        {
            "left": number_types,
            "right": number_types,
        },
        number_types,
        "乘法",
        "返回两个数值类型的参数相乘的结果",
    )

    register_function(
        "div",
        lambda left, right: left / right,
        {
            "left": number_types,
            "right": number_types,
        },
        number_types,
        "除法",
        "返回两个数值类型的参数相除的结果",
    )

    #  #### 字符串函数 ####
    register_function(
        "concat",
        lambda left, right: left + right,
        {"left": types.STRING, "right": types.STRING},
        types.STRING,
        "拼接字符串",
        "",
    )

    register_function(
        "replace",
        lambda source, which, target: source.replace(which, target),
        {"source": types.STRING, "which": types.STRING, "target": types.STRING},
        types.STRING,
        "字符串替换",
        "",
    )


def _reset_functions() -> None:
    """Clear FUNCTIONS and re-register all built-ins. Used in tests to force re-initialization."""
    FUNCTIONS.clear()
    _register_all_functions()


_register_all_functions()
