"""
plaita.dsl.codeflow — 用纯 Python 函数写 flow（AST 编译到 Flow IR）。

实现拆分：
- ``_common``：占位符、常量、CompileCtx、ErrorHandler
- ``_expr``：表达式与条件
- ``_nodes``：HTTP/CODE/EVENT/CHILD/... 节点调用
- ``_stmt``：if/for/assign/return 语句块
- ``_source``：@flow / @childflow / compile_source 入口

本模块保留历史导入路径，对外 re-export 公开 API。
"""
from __future__ import annotations

from plaita.dsl.codeflow._common import (  # noqa: F401
    CHILD,
    CODE,
    ENV,
    EVENT,
    ErrorHandler,
    F,
    FILTER,
    FIND,
    GLOBAL,
    HTTP,
    INPUT,
    LOOP,
    MAP,
    NODE,
    PARALLEL,
    PARENT,
    REDUCE,
    REFERENCE,
    _CodeflowError,
    _CompileCtx,
    _Placeholder,
    _default_known_node_types,
)
from plaita.dsl.codeflow._expr import _render_arg  # noqa: F401
from plaita.dsl.codeflow._source import (  # noqa: F401
    _compile_fdef,
    childflow,
    compile_func,
    compile_source,
    flow,
    flow_from_source,
)

__all__ = [
    "flow",
    "childflow",
    "flow_from_source",
    "compile_source",
    "compile_func",
    "HTTP",
    "CODE",
    "EVENT",
    "MAP",
    "FILTER",
    "FIND",
    "LOOP",
    "REDUCE",
    "CHILD",
    "REFERENCE",
    "PARALLEL",
    "ErrorHandler",
    "F",
    "INPUT",
    "NODE",
    "GLOBAL",
    "PARENT",
    "ENV",
    "_CodeflowError",
    "_CompileCtx",
    "_Placeholder",
    "_default_known_node_types",
    "_render_arg",
    "_compile_fdef",
]
