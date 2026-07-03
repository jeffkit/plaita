# 升级指南 (Migration Guide)

本文件列出 plaita 历次版本中**破坏向后兼容**的改动，并给出迁移步骤。如果你升级后遇到行为变化，先来这里查。

完整背景（含批评与设计动机）见内部 [docs/ARCHITECTURE_REVIEW_2026-07.md](docs/ARCHITECTURE_REVIEW_2026-07.md)；本文件只关心**怎么改代码**。

---

## 0.4.0

0.4.0 一次性引入了 5 处破坏性变更，集中在**安全模型**与**接口收紧**。下面按"踩坑概率从高到低"排序。

### 1. `$ENV` 改为 allowlist 模型（默认空）

**变更前**：流程表达式 `$ENV.XXX` 可读取任意环境变量，靠一份"敏感前缀黑名单"过滤。
**变更后**：`$ENV` 默认空；Flow 必须显式声明 `expose_env` 才能读到环境变量。

```python
# 变更前
flow = Flow.from_string('{"flow_id": "demo", ...}')
# 流程里 $ENV.HOME 仍能读到

# 变更后
flow = Flow.from_string(
    '{"flow_id": "demo", "exposeEnv": ["HOME", "API_BASE"], ...}',
)
# 或 Python 构造：
flow = Flow(flow_id="demo", expose_env=["HOME", "API_BASE"], ...)
```

**迁移**：搜索代码库所有 `$ENV.` 引用，把 key 名收集起来，加到对应 Flow 的 `expose_env`（JSON 写 `exposeEnv`，Python 写 `expose_env`）。

**深度防御**：即便显式列出了看起来敏感的 key（如 `AWS_SECRET_ACCESS_KEY`），仍会被 `_SENSITIVE_ENV_PREFIXES` 拦下并 warning。如果**确实**需要这类变量，要么重命名环境变量，要么 monkeypatch `plaita.core.context._safe_environment`。

### 2. `CodeNode` 移出默认注册表（opt-in）

**变更前**：`{"type": "code", ...}` 节点开箱即用。
**变更后**：CodeNode 不在默认 `_BUILTIN_NODES` 里，必须显式注册。

```python
from plaita.node import register_code_node

register_code_node()  # 默认注册到全局 registry
# 或自定义 registry：
# register_code_node(my_registry)
```

**迁移**：在 worker / server 启动入口（`main()` 或 FastAPI lifespan）调一次 `register_code_node()`。

未注册时解析流程会得到错误：`unRecognized node type: code. CodeNode was moved out of the default registry in 0.4.0; call register_code_node() at startup to opt in.`

### 3. CodeNode 默认沙箱化（RestrictedPython）

**变更前**：Python 后端走 raw `exec()`，可访问任意内置模块。
**变更后**：默认 `sandbox_backend="restricted"`（RestrictedPython AST 沙箱），允许 import 的模块白名单见 `SANDBOX_SAFE_MODULES`（math/json/re/datetime 等纯计算模块）。

可选后端：

| backend | 隔离强度 | 适用场景 |
|---|---|---|
| `restricted`（默认） | AST 级 | 多租户、纯计算 |
| `subprocess` | 进程级 | 需要 math/datetime/json 以外的模块，半信任 |
| `docker` | 容器级（无网络/只读 FS） | 完全不信任代码，生产多租户 |
| `unsafe` | 无 | 完全信任代码作者（内部开发） |

```json
{
  "type": "code",
  "language": "python",
  "sandbox_backend": "docker",
  "code": "def run(input): ..."
}
```

**迁移**：如果你的 CodeNode 用了 `os`/`sys`/`open`/网络/文件，要么改代码、要么显式设 `sandbox_backend="unsafe"`（仅当你完全信任代码作者）。**JS 后端（PyExecJS）仍未沙箱化，且 PyExecJS 自 2020 年起未维护——不要在生产多租户场景使用。**

### 4. `parallel mode="coroutine"` 下线

**变更前**：`mode="coroutine"` 可用，但实际在任何 running event loop（FastAPI/Starlette）中 100% 崩 `RuntimeError`。
**变更后**：直接 `raise ValueError`，错误消息指引改用 `mode="thread"` 或 `mode="process"`。

**迁移**：把 `"mode": "coroutine"` 改成 `"mode": "thread"`。

```diff
 {
   "type": "parallel",
-  "mode": "coroutine",
+  "mode": "thread",
   "branches": [...]
 }
```

### 5. `Flow.start_node` 删入度 0 启发式

**变更前**：Flow 没有显式 `type=start` 节点时，会扫描节点数组找"未被任何 next/branch 引用的节点"当入口——多个孤儿节点时结果依赖数组顺序。
**变更后**：没有显式 Start 节点直接抛 `FlowStartMissingError`。

**迁移**：

1. 手写 JSON/YAML 的：检查所有 flow 定义，确保每个 flow 都有一个 `{"type": "start", ...}` 节点。
2. 用 `@flow` / `FlowBuilder` 的：框架会自动注入，无需改动。
3. 用可视化编排工具导出的：检查导出结果是否包含 Start 节点。

### 6. `execution.context` 类型由 `dict` 改为 `CheckpointState`

**变更前**：`ExecutionContext.context` / `FlowExecution.context` 返回 `Dict[str, Any]`（内部 `_context` dict 的活引用）。
**变更后**：底层存储改为 `plaita.core.state.CheckpointState`（Pydantic `BaseModel`），`context` 返回该 model 实例。它实现完整 dict-like 协议（`__getitem__`/`__setitem__`/`__contains__`/`get`/`keys`/`items`/`__iter__`/`__eq__`），对 `$INPUT`/`$NODE` 等 prefixed key 的访问与旧 dict 完全一致；`__eq__` 与 plain dict 比较也保持兼容（`assertEqual(ctx.context, {...})` 仍通过）。

```python
# 变更前
assert isinstance(execution.context, dict)

# 变更后
from plaita.core.state import CheckpointState
assert isinstance(execution.context, CheckpointState)
# dict-like 行为不变：
execution.context["$INPUT"]
"$LAST_NODE" in execution.context
execution.context.get("$NODE", {})
```

**迁移**：

- 直接 `isinstance(ctx, dict)` 的断言会失败——改用 `isinstance(ctx, CheckpointState)` 或只断言 dict-like 行为（`in` / `[]` / `.get`）。
- `to_dict()` / `from_dict()` 的存储格式**逐键兼容**，Redis/SQL 里旧的 checkpoint 无需迁移即可加载（round-trip property test 钉死）。
- 把 `execution.context` 当 dict 传给第三方库（`json.dumps` / `dict(...)` / `deepcopy`）仍可用（dict-like 协议）；但需要"纯 dict"的场景请显式 `dict(execution.context)` 或 `execution.to_dict()`。

### 附：`ErrorStrategy` 字段类型 str → enum

**变更前**：`ErrorHandler.strategy` 字段类型 `Optional[str]`，比较时用 `_strategy_eq(value, member)` 容忍字符串/enum 混用。
**变更后**：字段类型 `ErrorStrategy`（enum），validator 接受 enum/str/`continue_with` 下划线别名，比较用 `==`。

**迁移**：99% 的代码无需改动（validator 兼容字符串）。仅当你**手动构造** `ErrorHandler(strategy="continue_with")` 而不是从 JSON 解析时，建议改成 `ErrorHandler(strategy=ErrorStrategy.CONTINUE_WITH)`。`_strategy_eq` 已删除，外部 import 需要清理。

---

## 升级检查清单

升级到 0.4.0 前过一遍：

- [ ] 全局搜索 `$ENV.` —— 列出 key 加到对应 Flow 的 `expose_env`
- [ ] 全局搜索 `type.*code` —— 在启动入口加 `register_code_node()`
- [ ] 全局搜索 `mode.*coroutine` —— 改成 `thread`
- [ ] 全局搜索 flow 定义 —— 确认每个都有 `type=start` 节点
- [ ] 跑一遍单元测试套件：`pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"`
- [ ] 跑一遍集成测试套件：`pytest tests/integration/`

升级遇到本文件没覆盖的行为变化？开 issue 贴复现步骤，会补到这里。
