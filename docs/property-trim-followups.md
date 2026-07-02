# Property 瘦身 & @flow 类型简化 — 后续待优化

本次合入的 `refactor/property-trim` 完成了两件事：

1. `plaita/io.py` 的 `Property` 瘦身：删除死代码（`PropertyException` /
   `from_str` / `valid` / `find_property` / `expanded_property` 全家桶 /
   `property_for_path` / `_match_complex_type`），移除运行时不生效的字段
   （`validators` / `min` / `max` / `max_length` / `choices` / `ref` /
   `additional`），用 Pydantic v2 `AliasChoices` 替代手写驼峰归一化。
2. `@flow` / `@childflow` 去掉 `input_type` / `output_type` 参数，编译时
   自动 emit `inputType: {dataType: "object"}`；引擎侧 `_coerce_input_value`
   把 `$INPUT` 统一为 dict，scalar/array 旧写法发 `DeprecationWarning`。

以下是本轮**刻意未做**、建议后续单独处理的点。

## 1. `@flow` 的 `INPUT` 类型从 AST 推导 + 编译期检查

当前 `@flow` 完全不声明字段类型，`$INPUT` 仅在运行时是 dict。可进一步：

- 读取 `INPUT` 参数注解（`TypedDict` / `dataclass` / Pydantic model）
- AST 编译期检查：函数体访问了 `INPUT.age`，但注解里没有 `age` → 报错
- 为 Console / dry-run 自动生成示例 JSON，不必维护 `Property` schema
- 可选：编译产物外包一层 `pydantic.TypeAdapter` 做运行时强转 + 值域校验

价值：让"声明即生效"，替代历史上名实不副的 `Property` 值域字段。

## 2. ~~legacy 传参迁移 + 移除 DeprecationWarning~~ ✅ 已完成

公共 `Flow.run` / `arun` / `debug` / `parse_and_run` 已改为 **dict-only**：
非 dict 位置参数直接 `TypeError`。`_coerce_input_value` 删除 deprecation
分支，对内部子流程调用（InlineFlow / 并行分支 / 循环）的单值位置参数保持
宽松（这是合法的内部机制，不走公共入口）。`tests/` / `skills/` /
`docs-site/` 中的 scalar/array 位置传参示例已迁移为 dict 形态。

> 注：JSON/YAML flow 定义里的 `inputType` 仍可声明，供 Console 展示与
> dry-run；它不再驱动 Python 侧 `run()` 的传参分支。

## 3. `FlowBuilder` / S-expr DSL 的 `input_type` 对齐

`plaita/dsl/builder.py` 的 `FlowBuilder` 和 `plaita/dsl/sexpr.py` 仍保留
`input_type` / `output_type` 参数（独立于 codeflow）。本轮未动，保持 API
表面稳定。后续可考虑统一去掉，与 `@flow` 对齐为"默认 object、不暴露类型参数"。

## 4. JSON/YAML flow 定义中 `output_type` 的语义

`Flow.output_type` 在引擎里**从不被校验**（无 `match(flow.output_type, result)`），
仅作 Console 元数据。后续二选一：

- **做真**：在 flow 结束时对最终输出跑 `match` / `TypeAdapter` 校验
- **做诚**：从 `Flow` 模型去掉 `output_type`，降级为 Console 侧元数据字段

## 5. `Property` 作为纯元数据载体的存废

`Function.param_type` / `Function.return_type`（`calculate.py`）、
`Decide.output_type`、`loop.item_type` 等仍持有 `Property` 但不参与运行时
校验。待第 1、4 项定方向后，可评估是否把 `Property` 整体降级为内部实现
细节（或被 `TypeAdapter` 取代）。

## 6. 子流程入口 `match()` 的弱化

`plaita/node/child.py` 在调用子流程前 `match(self.child_flow.input_property,
self.input)`。由于 `input_property` 通常是无 children 的 object，`_match_object`
对任意 dict 直接返回 True，实际零校验。待类型推导落地后，可改为真校验或
显式删除该 assert。
