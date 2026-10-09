"""
plaita.dsl.codeflow — 用纯 Python 函数写 flow（AST 编译到 Flow IR）。

实现拆分：``_common`` / ``_expr`` / ``_nodes`` / ``_stmt`` / ``_source``；
``_compiler`` 仅作兼容 re-export。对外保持历史导入路径：
``from plaita.dsl.codeflow import flow, flow_from_source, ...``
"""
from __future__ import annotations

from plaita.dsl.codeflow._compiler import (  # noqa: F401
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
    WHILE,
    _CodeflowError,
    _CompileCtx,
    _Placeholder,
    _compile_fdef,
    _default_known_node_types,
    _render_arg,
    childflow,
    compile_func,
    compile_source,
    flow,
    flow_from_source,
)
from plaita.dsl.codeflow._canonical import (  # noqa: F401
    canonical_node,
    count_nodes,
    serialize_canonical,
    to_canonical,
)
from plaita.dsl.codeflow._emit import EmitError, emit_source  # noqa: F401

__all__ = [
    "flow",
    "childflow",
    "flow_from_source",
    "compile_source",
    "compile_func",
    "emit_source",
    "to_canonical",
    "canonical_node",
    "serialize_canonical",
    "count_nodes",
    "EmitError",
    "HTTP",
    "CODE",
    "EVENT",
    "MAP",
    "FILTER",
    "FIND",
    "LOOP",
    "REDUCE",
    "WHILE",
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
]
