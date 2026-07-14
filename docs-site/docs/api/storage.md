# `plaita.storage.base`

状态存储抽象：`ExecutionState`、`ExecutionStorage`、`FlowStorage`。生产路径后端为 **memory / redis**（同步契约）。`sqlalchemy` 模块仍存在但其方法为 async，与本 ABC 及 FlowWorker 不兼容，公开 factory/CLI 已拒绝用作 execution/flow 存储。

::: plaita.storage.base
