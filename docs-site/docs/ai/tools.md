# 工具节点与数据源工具

`plaita-ai` 把「可被 Agent / `@flow` 调用的能力」统一成 **工具**：注册后既可出现在 LLM 的 tool 列表里，也能在 `@flow` 里以节点形式编排。

本文覆盖两层：

1. **ToolNode 桥接层**（`plaita_ai.agent.fot.tools`）—— Python callable / LangChain BaseTool → plaita 节点
2. **数据源工具层**（`plaita_ai.tools`）—— HTTP / SQL / 向量检索 / Native 的双轨定义（代码 + YAML）

LangChain **不是** plaita / plaita-ai 的必需依赖；只有使用 Agent 或 `register_langchain_*` 时才需要 `plaita-ai[agent]`。

!!! note "与 `examples/agent` / 手写 Node 的区别"

    - **本页**：`plaita-ai` 生产工具层（注册表、YAML、动态节点、MCP `flow_list_tools`）。
    - **`examples/agent/nodes.py` 的 `ToolNode`**：教学用自定义节点，演示如何继承 `Node`；**不是**本页的实现。
    - 需要新控制流语义时写[自定义节点](../nodes/custom.md)；要把数据源/函数交给 Agent 编排时用本页。

## 概念关系

```mermaid
flowchart TB
    subgraph define [定义层]
        Code["代码轨\n@tool / HttpToolSource / ..."]
        YAML["配置轨\ntools.yaml + resources.yaml"]
        LC["可选\nLangChain BaseTool / Toolkit"]
    end

    subgraph bridge [桥接层]
        Reg["register_tool_node /\nregister_source /\nload_tool_bundle"]
        TN["ToolNode._tools\n+ 动态 Node 子类"]
    end

    subgraph use [使用层]
        Agent["FoT / ReAct Agent"]
        Flow["@flow DSL"]
        MCP["MCP flow_list_tools /\nflow_run"]
    end

    Code --> Reg
    YAML --> Reg
    LC --> Reg
    Reg --> TN
    TN --> Agent
    TN --> Flow
    TN --> MCP
```

## 在 @flow 里怎么调用

注册工具后有两种写法（推荐第二种）：

=== "动态节点（推荐）"

    每个工具会注册独立 `node_type`（snake_case），占位符为大写：

    ```python
    @flow("demo", input_type="object")
    def demo(INPUT):
        user = GET_USER(user_id=INPUT.id)      # node_type=get_user
        weather = GET_WEATHER(city=INPUT.city)
        return {"user": user, "weather": weather}
    ```

=== "通用 TOOL 节点（兼容）"

    ```python
    @flow("demo", input_type="object")
    def demo(INPUT):
        user = TOOL(action="get_user", params={"user_id": INPUT.id})
        return user
    ```

占位符规则与其它自定义节点相同：`node_type` 大写 → `GET_USER`。详见 [@flow DSL](../guide/code-dsl.md)。

## 快速上手：注册一个 Python 函数

```python
from plaita_ai.agent.fot.tools import tool, register_tool_node, list_tools

@tool
def get_user(user_id: str) -> dict:
    """根据 ID 查询用户。"""
    return {"id": user_id, "name": "Ada"}

register_tool_node(get_user)
print(list_tools(as_code=True))
# → GET_USER(user_id: str)  # 根据 ID 查询用户。
```

也可用模块扫描：

```python
from plaita_ai.agent.fot.tools import register_tools_from_module
import myapp.tools

register_tools_from_module(myapp.tools, prefix="order_")
```

## 数据源工具（`plaita_ai.tools`）

当工具背后是 HTTP API、SQL、向量库等「有连接/有配置」的数据源时，用 `BaseToolSource` 子类，而不是手写一堆样板函数。

### 代码轨

```python
from plaita_ai.tools import (
    HttpToolSource,
    SqlToolSource,
    VectorToolSource,
    register_source,
    register_datasource,
    register_vectorstore,
)

register_source(HttpToolSource(
    name="get_user",
    description="根据 ID 查询用户",
    url="https://api.example.com/users/{user_id}",
    method="GET",
    response_path="$.data",
))

register_datasource("main_db", "sqlite:///./app.db")
register_source(SqlToolSource(
    name="query_orders",
    description="查询用户订单",
    datasource="main_db",
    sql="SELECT id, status FROM orders WHERE user_id = :user_id LIMIT :limit",
))

register_vectorstore("prod_kb", my_langchain_or_custom_store)
register_source(VectorToolSource(name="search_kb", store="prod_kb", k=5))
```

### 配置轨（扁平 YAML，无 Component/Instance）

```yaml
# tools.yaml
version: "1"
tools:
  - type: http
    name: get_weather
    description: 查询城市天气
    url: https://api.weather.example.com/v1/{city}
    method: GET
    response_path: $.result

  - type: sql
    name: query_orders
    description: 查询用户订单
    datasource: main_db
    sql: |
      SELECT id, status, amount FROM orders
      WHERE user_id = :user_id LIMIT :limit

  - type: vector
    name: search_kb
    description: 知识库检索
    store: prod_kb
    k: 5

  - type: native
    name: format_report
    description: 复杂逻辑走 Python
    module: myapp.formatters
    function: weather_report
```

```yaml
# resources.yaml — 命名连接池（不是动态类型系统）
datasources:
  main_db:
    driver: postgresql
    url: postgresql://user:pass@localhost/app

vectorstores:
  prod_kb:
    provider: chroma
    collection: knowledge_base
```

加载：

```python
from plaita_ai.tools import load_tool_bundle

load_tool_bundle("tools.yaml", "resources.yaml")
```

向量库实例仍需代码注入（配置只声明元数据）：

```python
from plaita_ai.tools import register_vectorstore
register_vectorstore("prod_kb", my_store)
```

### 支持的 `type`

| type | 类 | 说明 |
|------|-----|------|
| `http` | `HttpToolSource` | URL `{param}`、json path 抽取、可选 `addressing` |
| `sql` | `SqlToolSource` | 参数化 `:param` SQL（需 sqlalchemy） |
| `vector` | `VectorToolSource` | `similarity_search` 或可调用 store |
| `native` | `NativeToolSource` | `module.function` 指向已有 Python 函数 |

!!! note "MCP / RPC 工具类型"

    `type: mcp` / `type: rpc` 尚未实现；校验时会明确报错。远程 MCP 工具可先用 `native` 包装，或等后续版本。

## ToolContext（横切上下文）

工具若声明 `context` 参数，运行时会注入通用 `ToolContext`（**无业务域固定字段**）：

| 字段 | 含义 |
|------|------|
| `trace_id` / `request_id` | 可观测性 |
| `caller` | `"agent"` / `"flow"` / `"mcp"` / `"cli"` 等 |
| `flow_id` | 当前 flow 执行 ID（若有） |
| `auth` | 不透明凭证（兼容 `$GLOBAL.auth_context`） |
| `baggage` | 任意扩展元数据（`tenant_key`、`user_id` 等业务字段放这里） |

```python
from plaita_ai.tools import ToolContext

def secured_search(query: str, context: ToolContext | None = None) -> list:
    tenant = (context.baggage or {}).get("tenant_key") if context else None
    ...
```

执行时通过 flow globals 传入：

```python
flow.run(
    {"q": "hello"},
    globals={
        "trace_id": "abc",
        "auth_context": "Bearer ...",
        "baggage": {"tenant_key": "acme"},
    },
)
```

仍支持旧的 `auth_context` 参数注入（只注入 `$GLOBAL.auth_context`）。

## HTTP 寻址插件

```python
from plaita_ai.tools import register_addressing, HttpToolSource, register_source

register_addressing("static", lambda host: "127.0.0.1:8080")

register_source(HttpToolSource(
    name="ping",
    url="http://mysvc/health",
    addressing="static",
))
```

`resolver(host)` 可返回新 host 字符串，或返回 context manager（便于 Polaris 等带回写反馈）。

## LangChain Toolkit 适配（可选）

```bash
pip install "plaita-ai[agent]"   # 提供 langchain-core
# 具体 toolkit 另装 langchain-community 等
```

```python
from plaita_ai.tools import register_langchain_toolkit

register_langchain_toolkit(
    FileManagementToolkit(root_dir="/tmp"),
    prefix="fs_",
    include=["read_file", "write_file"],  # 白名单，避免危险工具全开
)
# @flow: FS_READ_FILE(path=...)
```

也支持单个 `BaseTool`，以及无 `.func`、只实现 `_run` 的 toolkit 风格工具（内部走 `invoke`）。

未安装 langchain 时，`from plaita_ai.tools import HttpToolSource` 等核心 API **不受影响**；调用 `register_langchain_*` 会得到明确的 `ImportError`。

## 启动加载与 CLI

### 环境变量 / MCP

```bash
export PLAITA_TOOLS=/path/to/tools.yaml
export PLAITA_RESOURCES=/path/to/resources.yaml   # 可选
plaita-ai mcp --plugin myapp.bootstrap
```

或：

```bash
plaita-ai mcp --tools tools.yaml --resources resources.yaml
```

MCP 启动时在加载 `--plugin` 之后读取工具清单，注册结果会出现在 `flow_list_tools` 与 server instructions 中。

### 校验与列表

```bash
plaita-ai tools validate tools.yaml --resources resources.yaml
plaita-ai tools list tools.yaml --resources resources.yaml
```

`validate` 只做 schema / 引用检查，不注册；`list` 会注册并打印 `PLACEHOLDER / name / description`。

## 与 Agent 的关系

| 入口 | 如何挂上工具 |
|------|----------------|
| `FoTAgent(tools=[...])` | 构造时 `register_tool_node` |
| `PlaitaAgent(tools=[...])` | 同上；另可用 `flow_only_tools` |
| 预先 `load_tool_bundle` / `register_source` | Agent 与 MCP 共享同一进程注册表 |
| MCP `PLAITA_TOOLS` | 服务启动时自动注册 |

!!! warning "进程级注册表"

    工具名注册在进程级 `ToolNode._tools`。多 Agent 同名会覆盖并 `UserWarning`。
    测试用 `ToolNode.clear()` 重置。

## 示例与源码

- 示例：`plaita-ai/examples/tools/`（`tools.yaml`、`resources.yaml`、`demo.py`、`langchain_demo.py`）
- 源码：`plaita-ai/plaita_ai/tools/`、`plaita-ai/plaita_ai/agent/fot/tools.py`

## 下一步

- [FoT Agent](fot-agent.md) —— 一次规划生成 `@flow`
- [ReAct Agent](react-agent.md) —— 工具循环 + 可选 `@flow` 升级
- [MCP 服务](mcp.md) —— `flow_list_tools` / 插件与工具清单加载
- [@flow DSL](../guide/code-dsl.md) —— 节点占位符语法
