# `plaita.node`

节点定义与 `NodeRegistry`。包含 `Node` 基类、默认内置节点（Start / End / Switch / Assignment / Bool / SwitchLegacy / Loop / Map / Filter / Find / Reduce / InlineFlow / ReferenceFlow / Parallel / HTTP / EventNode）、默认注册表与插件发现。

!!! note "CodeNode 不在默认注册表"

    0.5.0 起 `CodeNode` 需 `register_code_node()` 显式注册（默认 docker 沙箱）。见 [内置节点 - code](../nodes/builtin.md#code) 与 [迁移指南](../reference/migration-guide.md)。

::: plaita.node

::: plaita.node.basic
