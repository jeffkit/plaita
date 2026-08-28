# plaita

**Plaita 逻辑编排系统的官方 Python 运行时。**

[![PyPI](https://img.shields.io/pypi/v/plaita)](https://pypi.org/project/plaita/)
[![Python](https://img.shields.io/pypi/pyversions/plaita)](https://pypi.org/project/plaita/)
[![Docs](https://img.shields.io/badge/docs-jeffkit.github.io%2Fplaita-blue)](https://jeffkit.github.io/plaita/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

plaita 将 JSON 格式的逻辑流程定义解析为可执行的 `Flow`，并以插件化的 `Node` 体系驱动执行。它把**流程定义**与**执行逻辑**分离，支持同步阻塞、生成器单步、分布式断点续执三种执行模式，能覆盖从"一次请求即返回"到"跨进程长时运行工作流"的全频谱场景。

---

## 核心特性

- **JSON 流程定义** —— 用节点 + `next`/分支描述控制流，平台可视化编排或手写皆可
- **插件化节点** —— 内置 17 种节点，自定义节点只需实现 `execute`，支持 `entry_points` 自动发现
- **AI Agent 编排** —— 作为典型场景之一：LLM 规划 + plaita 执行，`@flow` / JSON 均有构建期校验，`flow_from_source` 让 AI 生成的流程编译期就拦错
- **三种执行模式** —— Normal 同步、Generator 单步调试、Distributed 跨进程断点续执
- **超时与错误策略** —— 节点级/流程级 ISO 8601 超时、`abort`/`continue`/`continue_with` 错误策略与重试
- **事件系统** —— EventBus 抽象 + memory/redis/sqlalchemy 后端，支撑长时工作流挂起与恢复
- **Python 同构** —— 基于 Pydantic，全异步内核，Python 3.10+

---

## 快速上手

### 安装

```bash
pip install plaita
```

验证安装：

```bash
python3 -m plaita
```

### Hello World

准备一个最简流程文件 `echo.json`，实现回声功能：

```json
{
    "flow_id": "echo",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "end" },
        { "type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success" }
    ]
}
```

执行流程：

```python
from plaita import Flow

flow = Flow.from_string(open("echo.json").read())
print(flow.run(name="kongjie"))  # => "kongjie"
```

---

## 它适合做什么

| 场景 | 推荐模式 | 说明 |
|------|----------|------|
| AI Agent 多步工具编排 | Normal / Distributed | LLM 规划 + plaita 执行；`parallel`/`child` 多工具与子 Agent，`approval` 人工介入 |
| 运行期执行 AI 生成的流程 | Normal + `flow_from_source` | `@flow` 源码编译期校验，比直接生成可执行代码更安全 |
| 请求-响应的即时逻辑 | Normal | 同步阻塞，一次返回结果 |
| 流程可视化调试器 | Generator | 每节点 yield，可单步、检查上下文；天然即 Agent trace |
| 人工审批 / 外部回调 / 定时延迟 | Distributed | 挂起-持久化-事件恢复 |
| 跨服务编排、容错长时工作流 | Distributed | 上下文序列化跨进程恢复 |

---

## 执行模式

### Normal — 同步阻塞

```python
from plaita import Flow

flow = Flow.from_string(open("my_flow.json").read())
result = flow.run(input_key="value")   # 同步返回
# 或异步：result = await flow.arun(input_key="value")
```

### Generator — 单步调试

```python
for step in flow.debug(input_key="value"):
    print(step.node_id, step.output)   # 每节点 yield 一次
```

### Distributed — 断点续执

**无需 Redis，纯内存版即可体验（仅用于本地调试与单元测试；生产部署请改用 `RedisEventBus` + `RedisStorage`，`InMemoryEventBus` 进程重启即丢失所有挂起状态）：**

```python
from plaita import Flow, FlowExecution
from plaita.event.memory import InMemoryEventBus

flow = Flow.from_string(open("my_flow.json").read())
execution = FlowExecution(event_bus=InMemoryEventBus())

# 推进一步（遇到 event 节点则挂起）
step = execution.run_distributed(flow, {"key": "value"})
# 恢复（外部事件到达后）
if step["is_suspend"]:
    step = execution.run_distributed(
        flow, None,
        saved_context=step["context"],
        resume_type="event",
        resume_data={"approved": True},
    )
```

生产部署时换成 `RedisEventBus` 和 `RedisStorage`，代码逻辑不变。

---

## 可选依赖（extras）

plaita 核心极轻，只依赖 `pydantic`、`pyparsing`、`isodate`，其余能力按需安装：

| extra | 安装命令 | 提供能力 |
|-------|---------|---------|
| `redis` | `pip install plaita[redis]` | Redis 存储 / Redis EventBus |
| `server` | `pip install plaita[server]` | FastAPI 服务端、FlowWorker、扩展节点 |
| `code` | `pip install plaita[code]` | `CodeNode`——0.5.0 起**默认 `docker` 容器级沙箱**（`--network none` / `--read-only` / 资源上限，需本机 Docker daemon）；可选 `subprocess` / `restricted`(AST) / `unsafe`；JS 后端仍走 PyExecJS（无沙箱，慎用） |
| `http` | `pip install plaita[http]` | `HTTP` 节点（requests + aiohttp） |
| `yaml` | `pip install plaita[yaml]` | YAML 格式流程定义 |
| `all` | `pip install plaita[all]` | 上述全部 |

缺失 extra 时会抛出**可操作的 `ImportError`**，明确提示安装哪个 extra。

---

## 流程定义方式

plaita 提供三种主要形态定义流程，最终均编译为同一执行模型：

1. **@flow**（推荐 Python API）—— 写普通函数，`@flow` 装饰器编译为 Flow，`flow_from_source` 供 AI 运行期生成
2. **JSON / YAML**（配置文件格式）—— `Flow.from_string(json_or_yaml_str)`，可视化编排工具导出即此格式
3. **FlowBuilder**（动态构建 API）—— `from plaita.dsl import build`，适合运行时动态创建或修改流程

> 完整格式对比及进阶用法见 [流程编写方式文档](https://jeffkit.github.io/plaita/guide/flow-authoring/)。

---

## 内置节点

16 种内置节点覆盖常见控制流与数据处理（`code` 节点不在默认注册表，需 `register_code_node()` 显式 opt-in；0.5.0 起 `register_code_node()` 默认走 `docker` 沙箱，daemon 不可用时拒绝注册——见下方[安全](#安全)小节）：

`start` · `end` · `assignment` · `switch` · `if` · `case` · `loop` · `map` · `filter` · `find` · `reduce` · `child` · `reference` · `parallel` · `http` · `event`

> `parallel` 节点的 `mode` 取 `thread` / `process`；`coroutine` mode 已于 0.4.0 下线（在任何 running event loop 下必崩），改用 `thread`。

扩展节点（通过 `server` extra 或 entry_points）：`delay` · `redis_queue` · `kafka_queue` · `http_callback` · `approval`

---

## 自定义节点

```python
from plaita import Node, NodeRegistry, Flow

class MyNode(Node):
    node_type = "myNode"   # 流程 JSON 里 "type" 字段对应的值
    node_name = "我的节点"  # 展示名

    def execute(self, execution=None):
        return {"result": "ok"}

registry = NodeRegistry()
registry.register(MyNode)

flow = Flow.from_string('....', registry=registry)
result = flow.run()
```

---

## 安全

plaita 在三个地方有显式安全边界，升级到 0.5.0 时行为有变化：

### `$ENV` allowlist（环境变量白名单）

`$ENV.XXX` 表达式默认**读不到**任何环境变量（0.4.0 起 allowlist 模型，默认空）。Flow 必须显式声明 `expose_env` 才能读到：

```python
flow = Flow.from_string(
    '{"flow_id": "demo", "exposeEnv": ["HOME", "API_BASE"], ...}',
)
# 或 Python 构造：Flow(flow_id="demo", expose_env=["HOME", "API_BASE"], ...)
```

0.5.0 起，若流程里引用了 `$ENV.HOME` 但 `expose_env` 为空，`Flow` 解析时 `logger.warning` 一次列出 key 名 + 修复指引（不报错，让沉默变可见）。升级后留意日志。

### CodeNode 默认 `docker` 沙箱

0.5.0 起 `register_code_node()` 默认 `sandbox_backend="docker"`（容器级隔离），**Docker daemon 不可用时拒绝注册**并报错，不允许静默降级：

```python
from plaita.node import register_code_node

register_code_node()                          # 默认 docker, 需本机 Docker daemon
register_code_node(default_backend="subprocess")  # 半信任: 进程级, 不隔离 FS/网络
register_code_node(default_backend="restricted")  # 显式声明半信任 (AST 沙箱, 有已知绕过向量)
register_code_node(default_backend="unsafe")      # 完全信任代码作者
```

daemon 不可用且未显式降级时，注册期报错并指明三种降级路径。

### HMAC nonce Redis 后端（多 worker 重放保护）

plaita-console 的 HMAC 鉴权 nonce 缓存默认是**进程内**单例——gunicorn 多 worker 下每个 worker 独立缓存，3 秒窗口内**跨 worker 重放可行**。生产多 worker 部署必须配 Redis 后端：

```python
from plaita_console.backend.services.signature import (
    configure_nonce_store, enable_replay_protection,
)

configure_nonce_store(redis_url="redis://...")   # 自动切 RedisNonceStore (SET NX EX ttl)
enable_replay_protection()                       # 多 worker 真重放保护
```

未配 Redis store 时 `enable_replay_protection()` 启动 `logger.warning` 明确告知"仅单 worker 保护"。完整 break change 与迁移步骤见 [MIGRATION.md](MIGRATION.md#0500)。

---

## 远程执行（PlaitaClient）

流程托管在 plaita-console 时，可用 `PlaitaClient` 通过 HMAC 鉴权拉取已发布流程并执行：

```python
from plaita.client import PlaitaClient

client = PlaitaClient('your-secret-id', 'your-secret-key')
# 显式指定服务地址（非本地部署时需要）：
# client = PlaitaClient('id', 'key', url='https://your-server/api/flowVersion/semver/detail')

result = client.run_flow('259', '0.0.2', {"age": 14})
print(result)
```

---

## plaita-console — 可视化管理控制台

[plaita-console](plaita-console/) 是配套的可视化管理控制台，功能包括：

- **可视化 Flow 编排** —— 浏览器内拖拽编排，节点面板 + 画布 + 配置抽屉 + 试跑
- **版本管理** —— 草稿/发布版本，semver 版本号
- **执行实例管理** —— 启动、停止、查看执行状态与日志
- **服务拓扑视图** —— 实时展示集群服务结构
- **节点管理** —— 内置节点 schema 查看、自定义节点注册
- **契约接口** —— `/api/flowVersion/semver/detail`（HMAC 鉴权），供 `PlaitaClient` 拉取

### 快速启动

```bash
# Docker Compose 一键启动（推荐）
cd plaita-console/docker
docker-compose up -d
# 访问 http://localhost:5173
```

本地开发启动：

```bash
# 后端
cd plaita-console/backend
pip install -r requirements.txt
python run.py   # 默认 http://localhost:8080

# 前端
cd plaita-console/frontend
pnpm install
pnpm dev        # 默认 http://localhost:5173
```

---

## 项目结构

```
plaita/
├── plaita/              # Python 运行时核心包
│   ├── core/            # 执行核心（Flow/FlowExecution/NodeRunner/ExecutionContext）
│   ├── node/            # 内置节点（17 种）
│   ├── dsl/             # 流程 DSL（YAML/Builder/S-expr/@flow）
│   ├── event/           # 事件系统（memory/redis/sqlalchemy）
│   ├── storage/         # 执行状态与流程定义存储
│   ├── server/          # FlowWorker、扩展节点、外延服务（需 server extra）
│   └── client.py        # 远程 PlaitaClient
├── plaita-console/      # 可视化管理控制台（React + FastAPI）
├── docs-site/           # MkDocs 文档站（部署至 GitHub Pages）
├── examples/agent/      # Agent 编排示例（RAG/Tool-use/Router）
└── tests/               # 单元/集成/E2E 测试
```

---

## 开发

### 运行测试

```bash
pip install -e ".[dev]"
pytest tests/
# 跳过需要真实基础设施的集成测试
pytest tests/ -m "not integration"
```

### 本地预览文档

```bash
pip install -r docs-site/requirements.txt
pip install -e .
mkdocs serve -f docs-site/mkdocs.yml
# 浏览器打开 http://127.0.0.1:8000
```

### 发布至 PyPI

```bash
python -m build
python -m twine upload dist/*
```

---

## 文档

完整文档见 **[https://jeffkit.github.io/plaita/](https://jeffkit.github.io/plaita/)**，包含：

- [指南](https://jeffkit.github.io/plaita/guide/) —— 安装、快速开始、执行模式、表达式、错误处理、回调、调试
- [架构](https://jeffkit.github.io/plaita/architecture/) —— 分层设计、执行引擎、状态管理、时序图
- [节点系统](https://jeffkit.github.io/plaita/nodes/) —— 内置节点手册、自定义节点、注册表与插件
- [断点续执](https://jeffkit.github.io/plaita/distributed/) —— Checkpoint、事件系统、FlowWorker、扩展节点
- [应用场景](https://jeffkit.github.io/plaita/scenarios/) —— Agent 编排、HTTP 集成、审批流等端到端示例
- [API 参考](https://jeffkit.github.io/plaita/api/) —— 由源码 docstring 自动生成

---

## 许可证

MIT


## Generator 提示词候选（针对 GLM-5.2 空转问题）

实测现象：部分轮次 generator 只在回复中描述实现而不写文件（gate 绿基线下
会 vacuously pass，已被 Evaluator 判定拦下）。候选强化措辞（加在 gen prompt 尾部，
逐个试跑对比）：

1. **工具指令型**（推荐先试）：
   「你必须使用文件编辑工具完成本 sprint：逐个创建/修改上述文件，保存到磁盘。
   禁止只在回复中描述实现。完成后运行 pytest 验证，全绿后结束。」
2. **验收前置型**：
   「开始编码前，先在回复第一行列出你将要创建/修改的文件清单；然后逐一写入磁盘；
   最后运行 pytest 并粘贴输出。回复中若没有 pytest 全绿的输出粘贴，视为未完成。」
3. **角色收紧型**（recursive 专用）：
   「以自动提交模式工作（所有工具调用自动批准）。直接修改工作区文件，
   不要征求确认。你的回复只需要：改动文件清单 + pytest 输出摘要。」

配套运行参数：`--permission-mode auto --headless`（已内置于 plaita-nodes
recursive-direct 执行器）。若仍空转，检查 recursive 会话 jsonl 确认工具调用
是否被拒（`permission_denials` 字段）。
