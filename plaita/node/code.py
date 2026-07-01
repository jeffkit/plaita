import ast
import importlib
from typing import Any, ClassVar, Optional

from pydantic import model_validator

from .basic import Node

try:
    import execjs
except ImportError:
    execjs = None


def _require_execjs():
    """Raise ImportError with actionable message if PyExecJS is not installed."""
    if execjs is None:
        raise ImportError(
            "PyExecJS is required for JavaScript code execution. "
            "Install it with: pip install plaita[code]"
        )

JS_FUNC_NAME = "run"
PYTHON_FUNC_NAME = "run"

LANGUAGE_JS = "js"
LANGUAGE_PYTHON = "python"


def run_js(code, input):
    _require_execjs()
    context = execjs.compile(code)
    result = context.call(JS_FUNC_NAME, input)
    return result


def run_python(code, *args, **kwargs):
    """
    运行python代码,并返回结果。

    :param code: 字符串，python代码。要求定义有且只有一个run函数。
    :param kwargs: 字典，作为参数传递给code中的run函数。
    :return: 返回执行结果
    """
    modules = import_modules(code)
    validate_run_function(code, kwargs)
    return execute_code(code, modules, *args, **kwargs)


def import_modules(code):
    tree = ast.parse(code)
    imports = [node for node in ast.walk(tree) if isinstance(node, ast.Import)]
    modules = {}
    for imp in imports:
        for name in imp.names:
            modules[name.name] = importlib.import_module(name.name)
    return modules


def validate_run_function(code, kwargs):
    tree = ast.parse(code)
    func_def = next(
        (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == PYTHON_FUNC_NAME), None
    )
    if not func_def:
        raise ValueError(f"No {PYTHON_FUNC_NAME} function found")

    func_args_name = [arg.arg for arg in func_def.args.args]
    invalid_args = set(kwargs.keys()) - set(func_args_name)
    if invalid_args:
        raise ValueError(f'Invalid arguments: {", ".join(invalid_args)}')


def execute_code(code, modules, *args, **kwargs):
    exec(code, modules)
    return modules[PYTHON_FUNC_NAME](*args, **kwargs)


Runners = {LANGUAGE_JS: run_js, LANGUAGE_PYTHON: run_python}


def register_runner(language, runner):
    """
    注册runner
    :param language: 语言
    :param runner: 运行器
    :return:
    """
    Runners[language] = runner


class CodeNode(Node):
    """
    代码节点，可执行用户定义的代码
    输入：
    - language：语言，js
    - code: 代码内容，如为js，代码内容为定义一个js函数，函数接受参数输入，函数可返回值作为节点的结果。
    - input：代码节点的输入参数，可以是任意类型值，支持使用变量表达式传递值。

    输出：
    - 函数返回值
    """

    node_type: ClassVar[str] = "code"
    node_name: ClassVar[str] = "代码"

    language: Optional[str] = None
    code: Optional[str] = None
    input: Optional[Any] = None

    @model_validator(mode="before")
    def validate_code_node(cls, data):
        # 如果没有language，默认是python
        if data.get('language') is None:
            data['language'] = "python"
        if data['language'] == "python":
            if not data.get('code'):
                raise ValueError("Python code is required when language is python")

        if data['language'] == "python":
            if not data.get('code'):
                raise ValueError("Python code is required when language is python")
            try:
                tree = ast.parse(data['code'])
                func_def = next(
                    (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == PYTHON_FUNC_NAME), None
                )
                if not func_def:
                    raise ValueError(f"No {PYTHON_FUNC_NAME} function found")
            except ValueError as e:
                raise ValueError(f"Python code validation failed: {str(e)}")
        return data

    def execute(self, execution):
        """
        使用code进行执行，并返回结果
        """
        language = execution.evaluate(self.language)
        code = execution.evaluate(self.code)
        input = execution.evaluate(self.input)
        if language in Runners:
            return Runners[language](code, input)
        else:
            raise ValueError("Unsupported language: {}".format(language))
