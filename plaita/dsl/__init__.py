"""
plaita.dsl — Flow 的 Python Builder DSL。

用 Python 代码声明流程，最终产出一个可执行的 ``Flow`` 对象，
或序列化成 JSON / YAML 字符串。定位是**动态流程构建与修改 API**——
适用于运行时动态组装流程结构、程序化修改已有流程（如 MCP 接口让 AI 修改流程）、
以及测试中构造测试流程。静态定义流程推荐使用 ``@flow`` DSL。

设计目标：

1. **字段名即关键字参数**：IDE 自动补全 + 拼写错误在调用期就暴露。
2. **构建期校验**：``next`` 指向不存在的节点、``switch`` 没有默认分支、
   节点 id 重复等反模式，在 ``build()`` 时直接抛错，而不是拖到运行时。
3. **动态修改**：``remove_node`` / ``update_node`` / ``reroute`` 支持对已有流程结构做外科手术式修改。
4. **子流程用装饰器表达**：``child``/``loop``/``map`` 等节点的 ``childFlow``
   用 ``@child_flow`` 写成普通 Python 函数，缩进层级 = 逻辑层级。

速览::

    from plaita.dsl import build, start, end, if_, cond

    flow = (
        build("adult_check", input_type="object", desc="判断成年")
        .add(start(next="check_age"))
        .add(if_(id="check_age", condition=cond("$INPUT.age", ">=", 18),
                 next="end_adult", else_next="end_minor"))
        .add(end("end_adult", output="成年"))
        .add(end("end_minor", output="未成年"))
        .build()
    )
    flow.run(age=20)  # -> "成年"
"""

from __future__ import annotations

from .builder import (
    FlowBuilder,
    LinearBuilder,
    build,
    linear,
    child_flow,
    cond,
    cond_group,
    error_handler,
    # 节点工厂
    start,
    end,
    assignment,
    if_,
    switch,
    case,
    branch,
    loop,
    map,
    filter,
    find,
    reduce,
    child,
    reference,
    parallel,
    parallel_branch,
    code,
    http,
    event,
)

__all__ = [
    "FlowBuilder",
    "LinearBuilder",
    "build",
    "linear",
    "child_flow",
    "cond",
    "cond_group",
    "error_handler",
    "start",
    "end",
    "assignment",
    "if_",
    "switch",
    "case",
    "branch",
    "loop",
    "map",
    "filter",
    "find",
    "reduce",
    "child",
    "reference",
    "parallel",
    "parallel_branch",
    "code",
    "http",
    "event",
]
