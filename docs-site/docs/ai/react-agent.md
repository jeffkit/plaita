# ReAct Agent（PlaitaAgent）

`PlaitaAgent` 是基于 LangChain 1.x `create_agent` 的**内置 ReAct Agent**，具备以下特性：

- 标准 function-call 工具调用循环（普通 ReAct）
- 可选的 `@flow` 编排升级路径：任务复杂时 Agent 自行决定是否生成 `@flow`
- 同一工具两种用法：Agent 直接调用 **或** 在 `@flow` 里 `TOOL(action="...", ...)` 调用

## 安装

```bash
pip install "plaita-ai[agent,openai]"   # OpenAI
pip install "plaita-ai[agent,anthropic]" # Anthropic / Claude
```

## 快速上手

```python
from langchain.tools import tool
from plaita_ai.agent.react import PlaitaAgent

@tool
def weather(city: str) -> str:
    """查询城市天气。"""
    return f"{city}：晴，25°C"

@tool
def calc(a: int, b: int) -> str:
    """两数求和。"""
    return f"{a} + {b} = {a + b}"

# 创建 Agent（默认启用 @flow 升级路径）
agent = PlaitaAgent(model="openai:gpt-4o-mini", tools=[weather, calc])

# 简单问题 → 直接调 weather 工具
result = agent.invoke("北京天气？")
print(result.text)

# 复杂编排 → Agent 自行决定写 @flow
result = agent.invoke("查北京和上海的天气，并行获取")
print(result.text)
```

## 构造参数

```python
PlaitaAgent(
    model,              # str（init_chat_model 格式）或 BaseChatModel 实例
    tools=[],           # 普通 function-call 工具（同时注册为 ToolNode 供 @flow 使用）
    flow_only_tools=[], # 只在 @flow 里用的工具（不作为直接 function-call 工具）
    instruction="",     # 追加到系统 prompt 的额外指令
    globals_ctx={},     # 注入 flow.global_context（$GLOBAL.*）
    enable_flow=True,   # False 时退化为纯 ReAct，不注入 plaita 工具
    debug=False,        # 传给 create_agent 的调试标志
)
```

### `enable_flow=False`：纯 ReAct 模式

```python
# 完全不涉及 @flow，就是标准 ReAct tool-calling 循环
agent = PlaitaAgent(model="openai:gpt-4o-mini", tools=[weather], enable_flow=False)
```

## 内置 plaita 工具（enable_flow=True 时注入）

| 工具名 | 功能 |
|--------|------|
| `plaita_compile_flow` | 编译校验 `@flow` 源码 |
| `plaita_run_flow` | 编译并执行 |
| `plaita_list_nodes` | 查询已注册节点占位符 |
| `plaita_get_dsl_reference` | 获取 DSL 文档（`scope="summary"` 或 `"full"`） |

## 工具双用途

传给 `tools=` 的工具**自动注册为 ToolNode**，因此同一个函数既能被 Agent 直接调用（function-call），也能在生成的 `@flow` 里被 `TOOL(action="weather", ...)` 调用：

```python
@tool
def weather(city: str) -> str:
    """查天气。"""
    return f"{city}：晴"

agent = PlaitaAgent(model="...", tools=[weather])

# Agent 可以：
# 1. 直接调 weather("北京")
# 2. 生成 @flow 里写 r = TOOL(action="weather", params={"city": INPUT.city})
```

如果某个工具只想在 `@flow` 里用（不作为直接工具），传给 `flow_only_tools=`：

```python
agent = PlaitaAgent(
    model="...",
    tools=[weather],          # 直接调 + @flow 两用
    flow_only_tools=[db_query], # 仅 @flow 可用，不直接暴露给 Agent
)
```

## 多轮对话

```python
from langchain_core.messages import HumanMessage, AIMessage

history = [
    HumanMessage(content="你好"),
    AIMessage(content="你好！有什么可以帮你？"),
]
result = agent.invoke("查北京天气", history=history)
```

## 返回值

```python
result = agent.invoke("...")
result.text      # str：最终 AI 文本回复
result.messages  # List[BaseMessage]：完整消息链（含工具调用记录）
```

## 异步支持

### `ainvoke`（异步调用）

在 FastAPI、asyncio 等异步上下文中使用，避免阻塞事件循环：

```python
# FastAPI 示例
from fastapi import FastAPI
from plaita_ai.agent.react import PlaitaAgent

app = FastAPI()
agent = PlaitaAgent(model="openai:gpt-4o-mini", tools=[weather])

@app.post("/chat")
async def chat(message: str):
    result = await agent.ainvoke(message)
    return {"reply": result.text}
```

### `astream`（流式 token 输出）

逐 token 返回 LLM 的文本回复，适合 SSE / WebSocket 实时推送场景：

```python
# FastAPI SSE 示例
from fastapi.responses import StreamingResponse

@app.post("/chat/stream")
async def chat_stream(message: str):
    async def generate():
        async for token in agent.astream(message):
            yield f"data: {token}\n\n"
    return StreamingResponse(generate(), media_type="text/event-stream")
```

```python
# 命令行流式输出
async for token in agent.astream("北京天气？"):
    print(token, end="", flush=True)
print()
```

!!! note "流式输出说明"
    - 需要底层模型支持 streaming（如 `ChatOpenAI(streaming=True)`、`ChatAnthropic()`）
    - 工具调用步骤不会 stream（静默执行），只有最终文字回复被逐 token 推送
    - 不支持 streaming 的模型会将完整回复作为单个 token 推送

## 与 FoTAgent 对比

| | `PlaitaAgent`（ReAct） | `FoTAgent` |
|---|---|---|
| 模式 | 标准 ReAct + 可选 @flow 升级 | 一次规划整段 @flow |
| 工具调用 | function-call 循环（多轮） | 只在 @flow TOOL 节点里调用 |
| 适合场景 | 通用对话、交互式、探索性任务 | 任务明确、需要确定性流程图 |
| @flow 决策 | Agent 自行判断是否需要 | 始终生成 @flow |

## 注意事项

### ToolNode 全局注册

`tools=` 中的工具注册到进程级全局 `ToolNode._tools`。如果多个 `PlaitaAgent` 实例使用同名工具的不同版本，后注册的会覆盖先注册的，并触发 `UserWarning`。

在测试场景中，用 `ToolNode.clear()` 重置注册表：

```python
from plaita_ai.agent.fot.tools import ToolNode

def teardown():
    ToolNode.clear()
```

### `globals_ctx` 注入运行时依赖

自定义节点（如 `LLMNode`）需要在运行时访问 API 客户端时，通过 `globals_ctx` 注入：

```python
import openai

agent = PlaitaAgent(
    model="openai:gpt-4o-mini",
    tools=[weather],
    globals_ctx={"openai_client": openai.OpenAI()},
)
# @flow 里可通过 GLOBAL.openai_client 访问
```
