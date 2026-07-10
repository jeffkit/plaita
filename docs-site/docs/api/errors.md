# `plaita.core.errors`

规范异常类型与错误处理策略。

**基类 / 业务错误**：`FlowExecutionException`、`FlowResultError`、`NodeException`、`FlowErrorType`、`ResumeType`。

**细分异常**（0.5.x）：`NodeNotFoundError`、`FlowStartMissingError`、`NodeExecutionError`、`NodeTimeoutError`、`FlowTimeoutError`、`FlowErrorException`、`ErrorResultException`、`ResumeError`。

**错误策略**：`ErrorStrategy`、`ErrorHandler`、`RecoverableErrorHandler`。

::: plaita.core.errors
