# 节点系统

节点（`Node`）是流程的最小执行单元。plaita **默认注册 16 种**内置节点（另有 `CodeNode` 需 `register_code_node()` 显式注册），覆盖起止、赋值、分支、循环、并行、子流程、HTTP 与事件等场景；不够用时还可[自定义节点](custom.md)，或用 [plaita-ai 工具层](../ai/tools.md) 把数据源暴露给 Agent。

## 章节导览

- [内置节点](builtin.md) —— 每种内置节点的 `node_type`、字段与行为
- [自定义节点](custom.md) —— 如何继承 `Node` 实现 `execute` 并注册
- [节点注册表与插件](registry.md) —— `NodeRegistry`、默认注册表、entry_points 自动发现
- [弃用迁移](migration.md) —— `node_register` / `plaita.flow` 等旧 API 迁移到新写法

## 节点基类速览

所有节点继承 `Node`（Pydantic 模型）。关键类变量与字段：

| 成员 | 类型 | 说明 |
|------|------|------|
| `node_type` | `ClassVar[str]` | 流程 JSON 中 `type` 对应的值 |
| `node_name` | `ClassVar[str]` | 展示名 |
| `branching` | `ClassVar[bool]` | 是否分支节点（`switch`/`if`/`case`） |
| `async_node` | `ClassVar[bool]` | 是否异步节点（`event`） |
| `id` / `name` / `desc` | 字段 | 节点 id / 展示名 / 描述 |
| `next` | 字段 | 下一节点 id |
| `output` | 字段 | 输出表达式 |
| `timeout` | 字段 | 节点超时 |
| `timeout_handler` / `error_handler` | 字段 | 超时/错误处理器 |

子类实现 `execute(self, execution=None)` 返回节点输出；`run()` 会调 `execute` 再做 `_validate_output`。
