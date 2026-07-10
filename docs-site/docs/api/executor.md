# `plaita.core.executor`

执行策略与 `FlowExecution` facade。定义 `ExecutionMode`、`ExecutionStrategy` Protocol、三种具体策略（Normal / Generator / Distributed）。

!!! note "Facade 属性访问"

    近期清理后，`FlowExecution` **不再**用 `__getattr__` / `__setattr__` 把未知属性透传到 context。
    上下文字段（`context` / `execution_id` / `event_bus` / `cancel_event` 等）为具名 property；
    状态访问走显式 delegate。拼写错误会变成普通实例属性，不再静默落进 state。

::: plaita.core.executor
