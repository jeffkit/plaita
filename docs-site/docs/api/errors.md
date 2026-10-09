# `plaita.core.errors`

规范异常类型与错误处理策略。

**基类 / 业务错误**：`FlowExecutionException`、`FlowResultError`、`NodeException`、`FlowErrorType`、`ResumeType`。

**细分异常**（0.5.x）：`NodeNotFoundError`、`FlowStartMissingError`、`NodeExecutionError`、`NodeTimeoutError`、`FlowTimeoutError`、`FlowErrorException`、`ErrorResultException`、`ResumeError`。

**Worker 侧**（`plaita.server.flow_worker`，非 core）：`ResumeProtocolError`——挂起守卫类 `ResumeError`（如 continue 打在 pending 挂起节点上）不终态化、执行保持原状时上抛（#33）。

**错误策略**：`ErrorStrategy`、`ErrorHandler`、`RecoverableErrorHandler`。

::: plaita.core.errors
