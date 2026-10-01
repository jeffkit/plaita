# plaita-ai

Plaita 的 **AI 集成层**（第二个可安装包）：把 `@flow` 编译/执行能力以 **MCP / CLI / FoT Agent / Skill** 形式交给现有 Agent 使用。

> plaita 本身不是 Agent 运行时；`plaita-ai` 负责「LLM 规划 + `@flow` 生成 + 编译校验 + 执行」这一插件能力。

## 安装

```bash
# 在 plaita 仓库根目录

> **发布策略**：plaita-ai 当前仅支持**源码安装**（`pip install -e ./plaita-ai`），
> 尚未发布到 PyPI（包名注册与发布管线待定，publish.yml 不覆盖本目录）。
> 跑测试需要 dev extra：`pip install -e "plaita-ai[dev]"`（含 pytest-asyncio；
> 不装则 async 用例收集报错）。agent/ReAct 示例还需 `.[agent]`（langchain）。
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
│   ├── skills/flow-coder/   # 内置 skill + reference + evals
│   ├── console_client.py    # console 管理 API 客户端（supervisor 数据面）
│   ├── ops.py / ops_mcp.py  # 版本/run/metrics/diff 高层操作 + console_* MCP 工具
│   ├── evals/               # 运行时评测：数据集 / scorer / 版本对比报告
│   └── supervisor.py        # supervisor 循环：propose→eval→compare→人签 promote
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
| `flow_from_json` | JSON flow definition 反向生成 `@flow` 源码（含 round-trip 编译校验） |
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
plaita-ai emit flow.json            # JSON definition → @flow 源码（--out 写文件）
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

## 场景 4：Supervisor（自迭代工作流，0.2.0）

`flow_*` 工具服务**编辑期**闭环（生成 → 编译自纠 → 执行）；`console_*` / `eval_*` /
`supervisor_*` 工具服务**运行期**闭环——对着一个已部署的 plaita（console），让 Agent
持续观测 flow 的真实运行，用评测集驱动版本迭代，人签后才发布：

```
propose → 保存为下一 patch 版本 → evaluate（评测集打分）→ compare（N-1 vs N）
        → 改善 → 返回 promotion ticket（人工 console_flow_publish 才上线）
        → 未改善/回归 → 丢弃；连续失败 → 暂停
```

**红线：循环本身永远不发布**——`promote_gate` 默认 `manual`，产出的是带对比数据的
promotion ticket，由人执行发布；这是整个形态的安全底座。

环境配置（console 侧）：

```bash
export PLAITA_CONSOLE_URL=http://127.0.0.1:8000     # console 地址
export PLAITA_CONSOLE_ADMIN_API_KEY=...             # 机器首选；或 USERNAME/PASSWORD
# 可选：LLM 提案者 / 评测 judge（OpenAI 兼容端点）
export PLAITA_AI_PROPOSER_BASE_URL=... PLAITA_AI_PROPOSER_MODEL=... PLAITA_AI_PROPOSER_API_KEY=...
# 提案走 CoDeFlow：LLM 返回完整 @flow 源码（编译校验后转 IR 入库），
# context 自动携带 current_source（emit_source 反推的当前源码）供最小改动
export PLAITA_AI_JUDGE_BASE_URL=...   PLAITA_AI_JUDGE_MODEL=...   PLAITA_AI_JUDGE_API_KEY=...
```

MCP 工具（`plaita-ai mcp` 自动注册；未配置 console 时调用返回带指引的错误）：

| 工具 | 作用 |
|------|------|
| `console_flow_list` / `console_flow_get` | flow 清单 / 版本谱（semver 排序、published 标记） |
| `console_flow_version_get` / `_save` | 读 / 保存版本定义（草稿，不上线） |
| `console_flow_publish` | 发布版本——**人工闸门** |
| `console_flow_diff` / `console_flow_metrics` | 版本 diff / 最近执行健康（成功率、时延、失败） |
| `console_runs_list` / `console_run_get` / `_start` / `_cancel` | 执行面（可 wait 到终态） |
| `console_dry_run` | 定义进程内试跑（无部署） |
| `eval_run` / `eval_compare` | 版本过评测集 / 两份报告对比（N-1 vs N） |
| `supervisor_iterate` | 跑一轮自迭代，返回 promotion ticket |

Python 侧同一能力：

```python
from plaita_ai.console_client import client_from_env
from plaita_ai.evals import load_dataset
from plaita_ai.supervisor import Supervisor, SupervisorPolicy, StaticProposer

sup = Supervisor(
    client_from_env(),
    policy=SupervisorPolicy(promote_gate="manual", max_iterations=5),
    proposer=StaticProposer([...]),   # 或 PromptProposer()（PLAITA_AI_PROPOSER_* env）
)
result = sup.run_loop("my-flow", load_dataset("evals/my-flow/"))
# result["iterations"][-1]["promotion_ticket"] → 人确认后 console_flow_publish
```

评测集是纯 JSON（可进 git）：单文件 `{"cases": [...]}` 或目录（`_*.json` 为元数据不当作用例），
每个 case `{"id", "input", "expect"}`；expect 支持 `contains` / `equals_path` /
`not_empty` / `judge`（LLM 评审，未配置 judge 时该维度跳过而非瞎猜）。

## 场景 5：金丝雀 / 影子切分（0.3.0）

promote 之前的最后一道实证：让候选版本先在**真实流量**上证明自己。

- **split**：按 `canary_key` 的稳定哈希把真实执行分流到 baseline/candidate 双臂
  （同一 key 永远同一臂），逐次记录结果;
- **shadow**：生产流量不受影响——baseline 真跑返回给调用方，candidate 用同输入走
  console 进程内 dry-run。**零风险**的上线前验证。

```python
from plaita_ai.canary import CanaryRun, CanaryPolicy, shadow_once

run = CanaryRun(client, "my-flow", candidate_version="1.3.0",
                policy=CanaryPolicy(mode="shadow"))   # 或 mode="split", ratio=0.1
out = run.invoke({"city": "北京"})       # 调用方拿到的永远是 baseline 结果
print(run.report())                       # 双臂成功率/时延对比
print(run.verdict(min_count=10))          # keep_running / promote / rollback + 发布命令
```

运行状态可 `to_dict()` / `from_dict()` 序列化（JSON），金丝雀可以跨进程续跑。
MCP 侧:`canary_shadow_once`（单输入影子检查）/ `canary_verdict`（从序列化状态出建议）。

## Skill

内置 skill 位于 `plaita_ai/skills/`，是唯一权威副本，随包分发。软链到用户 skill 目录即可：

```bash
SKILLS="$(python -c 'import plaita_ai, pathlib; print(pathlib.Path(plaita_ai.__file__).parent / "skills")')"

# Claude Code
ln -snf "$SKILLS/flow-coder"          ~/.claude/skills/flow-coder
ln -snf "$SKILLS/plaita-flow-builder" ~/.claude/skills/plaita-flow-builder
ln -snf "$SKILLS/plaita-flow-runner"  ~/.claude/skills/plaita-flow-runner
```

## 开发

```bash
cd plaita-ai
pytest tests/ -q
```

## 后续

- [x] 0.2.0：console ops 工具面 + runtime evals + supervisor 循环（人签 promote）
- [x] 0.3.0：金丝雀/影子流量切分（split sticky 双臂 + shadow 零风险验证 + verdict 建议）
- [x] agent-benchmark 增加 `--arm mcp` 对比  
- [ ] FoT / ReAct：LLMNode / RetrieverNode 与 `examples/agent` 对齐  
- [ ] 修 `@flow` PARALLEL+INPUT / REDUCE 运行时 bug（REDUCE 现象为 IndexError，由 NodeExecutionError 包裹抛出）  
