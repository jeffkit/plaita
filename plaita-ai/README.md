# plaita-ai

Plaita 的 **AI 集成层**（第二个可安装包）：把 `@flow` 编译/执行能力以 **MCP / CLI / FoT Agent / Skill** 形式交给现有 Agent 使用。

> plaita 本身不是 Agent 运行时；`plaita-ai` 负责「LLM 规划 + `@flow` 生成 + 编译校验 + 执行」这一插件能力。

## 安装

```bash
# 在 pyloki 仓库根目录
pip install -e ./plaita
pip install -e "./plaita-ai"              # MCP + CLI + skill 资源
pip install -e "./plaita-ai[agent]"       # + LangChain 1.x 内置 Agent（ReAct + FoT）
pip install -e "./plaita-ai[agent,openai]"  # + OpenAI 模型集成（可选）
```

## 包结构

```
plaita-ai/
├── plaita_ai/
│   ├── flow_runner.py       # 共享内核：compile / run / list_nodes / skill 文本
│   ├── mcp/server.py        # MCP stdio 服务（优先）
│   ├── cli/main.py          # CLI（与 MCP 共用 flow_runner）
│   ├── agent/react/         # 内置 ReAct Agent（create_agent + plaita 工具）
│   ├── agent/fot/           # FoT Agent（一次性规划 @flow + 编译自纠）
│   └── skills/flow-coder/   # 内置 skill + reference + evals
├── examples/react/          # ReAct 在线/离线 demo
├── examples/fot/            # FoT 离线 demo
└── tests/
```

## 场景 1：给现有 Agent 当插件（MCP 优先）

### MCP Tools

| Tool | 作用 |
|------|------|
| `flow_compile` | 编译 `@flow` 源码，返回 IR 或带行号错误 |
| `flow_run` | 编译并执行 |
| `flow_list_nodes` | 列出已注册节点类型（含自定义节点占位符） |
| `flow_get_skill` | 返回内置 `flow-coder` skill 全文 |
| `flow_get_skill_reference` | 返回 `@flow` 完整语法参考 |

启动：

```bash
plaita-ai mcp
# 或
python -m plaita_ai.mcp.server
```

Cursor `mcp.json` 示例：

```json
{
  "mcpServers": {
    "plaita-flow": {
      "command": "plaita-ai",
      "args": ["mcp"]
    }
  }
}
```

Agent 典型闭环：

1. 读 `flow_get_skill` / 本地 skill → 生成 `@flow` 源码  
2. `flow_compile` → 失败则带错误重生成  
3. `flow_run` → 返回结果  

### CLI

CLI 与 MCP **共用** `flow_runner`（不是 subprocess 包 MCP，而是同一套 Python API）：

```bash
plaita-ai compile flow.py
plaita-ai run flow.py --input '{"name":"alice"}'
plaita-ai list-nodes
plaita-ai skill
plaita-ai mcp
```

## 场景 2：内置 ReAct Agent（`PlaitaAgent`）

**普通 ReAct 为主，`@flow` 是可选编排增强**——基于 LangChain 1.x `create_agent`。

定位：
- 用户只给普通 function-call 工具时，就是标准 ReAct loop，正常调工具。
- plaita 的 compile/run/list/reference 也以 **function-call 工具**注入，与用户工具平级。
- 自适应 prompt：简单任务直接调工具；多步/分支/循环/并行才升级到 `@flow`。
- 同一工具两种用法都行：Agent 直接调用，或在 `@flow` 里 `TOOL(action="...", ...)` 调用。

```python
from langchain.tools import tool
from plaita_ai.agent.react import PlaitaAgent

@tool
def weather(city: str) -> str:
    """查天气。"""
    return f"{city}：晴"

# 纯 ReAct（不要 @flow）
agent = PlaitaAgent(model="openai:gpt-4o-mini", tools=[weather], enable_flow=False)
agent.invoke("北京天气？")

# ReAct + @flow 升级（默认）；tools 自动注册为 ToolNode，@flow 里也能 TOOL(action="weather")
agent = PlaitaAgent(model="openai:gpt-4o-mini", tools=[weather])
agent.invoke("查北京天气并把温度乘以 2")  # Agent 自行决定是否用 @flow
```

| 内置工具 | 作用 |
|---------|------|
| `plaita_compile_flow` | 编译校验 @flow |
| `plaita_run_flow` | 编译并执行 |
| `plaita_list_nodes` | 节点类型 introspection |
| `plaita_get_dsl_reference` | DSL 文档（scope: `summary`/`skill`=SKILL.md，`full`=完整语法参考） |

离线（无需 API key）：`python plaita-ai/examples/react/demo_offline.py`  
在线：`OPENAI_API_KEY=... python plaita-ai/examples/react/demo.py`

与 FoT 的分工：

| | `PlaitaAgent`（ReAct） | `FoTAgent` |
|---|---|---|
| 模式 | 标准 ReAct tool loop；@flow 是可选升级 | 一次规划 @flow + 编译自纠 |
| 适用 | 通用对话、探索、多轮修正 | 任务明确、要确定性流程图 |
| LangChain | `create_agent` | `model.invoke` 规划链 |

## 场景 3：FoT Agent（`FoTAgent`）

**刻意不沿用** edan-backend 的实现：

| edan（旧） | plaita-ai FoT（新） |
|-----------|---------------------|
| `langchain.chains.base.Chain` | 普通 Python 类 `FoTAgent.invoke()` |
| LLM 输出 JSON actions | LLM 输出 `@flow` Python 源码 |
| `compile_actions` + jsonpatch | `compile_source` + 整段重生成 |
| `compose_flow_with_actions` | `flow_from_source` |

使用 LangChain **1.x** 的 `init_chat_model` + `SystemMessage`/`HumanMessage` `invoke`，不用 `AgentExecutor` / JSON workflow。

```python
from langchain.chat_models import init_chat_model
from plaita_ai.agent.fot import FoTAgent

def weather(city: str) -> str:
    """查询天气。"""
    return f"{city}：晴，25°C"

agent = FoTAgent(
    model="openai:gpt-4o-mini",  # 或 init_chat_model(...) 实例
    tools=[weather],
    instruction="优先使用 TOOL 节点，不要编造工具名",
)

result = agent.invoke({"task": "查北京天气", "city": "北京"})
print(result.ok, result.result)
print(result.source)   # 最终 @flow 源码
print(result.attempts) # 编译自纠轮数
```

离线 demo（FakeListChatModel，无需 API key）：

```bash
python plaita-ai/examples/fot/demo.py
```

## Skill

内置 skill 位于 `plaita_ai/skills/flow-coder/`。可安装到 Cursor：

```bash
# 可选：链到 ~/.cursor/skills
ln -sf "$(pwd)/plaita-ai/plaita_ai/skills/flow-coder" ~/.cursor/skills/flow-coder
```

仓库根目录 `skills/flow-coder/` 为兼容入口，内容与 `plaita-ai` 包内 skill 同步维护。

## 开发

```bash
cd plaita-ai
pytest tests/ -q
```

## 后续

- [ ] agent-benchmark 增加 `--arm mcp` 对比  
- [ ] FoT / ReAct：LLMNode / RetrieverNode 与 `examples/agent` 对齐  
- [ ] 修 `@flow` PARALLEL+INPUT / REDUCE 运行时 bug  
