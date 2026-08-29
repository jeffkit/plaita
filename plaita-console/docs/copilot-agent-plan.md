# 编排页 Copilot Agent 方案（调研 + 设计）

> 状态：待评审 | 作者：ZCode | 日期：2026-08-29
> 目标：把「输入一句话 → 一次性生成导入」升级为 Copilot 式多轮对话 Agent，可感知当前画布、多次增量编辑、调用页面能力。

## 1. 现状（改动基线）

现有链路（`backend/services/ai_flow.py` + `frontend/src/components/flow/AiGenerateDialog.tsx`）：

- 单发对话框：输入一句话 → `POST /api/flows/ai-generate/stream`（SSE）→ 后端做「宿主」：
  拼接 `_SYNTAX_RULES` + 节点类型摘要 + 需求 → `subprocess` 调 recursive CLI（凭证经
  plaita-nodes 的 agents.json/providers.json 解析，后端不持有模型凭证）→ 取回 @flow 源码 →
  `compile_source` 确定性编译，失败带错误回喂自纠（最多 3 轮）→ `finished{ir}`。
- 前端 `jsonToFlow(ir)` 后 `setGraph` **整体覆盖**当前画布。无会话、无多轮、无局部编辑。

可复用资产：recursive 宿主调用（`plaita-nodes/agent_run.py`）、自纠循环与 prompt 资产、
SSE 事件通道、`flowToJson/jsonToFlow` 双向转换器、store 的细粒度 actions
（setGraph/updateNodeData/addNode/removeNode/enterSubgraph/exitToLevel/setSelected 等，
粒度适合直接映射为 Agent Tool）。

## 2. 技术调研结论

### 2.1 CopilotKit ✅ 推荐采用

- React 生态最成熟的 Agent 前端框架；自带聊天 UI（CopilotSidebar/CopilotChat，可主题定制）、
  会话管理、消息流渲染、工具调用展示。
- v1.50 起 `useAgent` hook（`useCoAgent` 的演进）连接任意 **AG-UI 兼容 agent**，
  暴露 messages / streaming tokens / tools / shared state。
- **前端 Tool 机制**（正是我们要的）：前端注册 action（schema 描述）→ 随 `RunAgentInput.tools`
  发给 agent → agent 发 `TOOL_CALL` 事件 → 前端执行 → `TOOL_CALL_RESULT` 回传 → agent 续跑。
  可配 Generative UI（工具卡片自定义渲染，做「应用/撤销」确认）。
- 接入自建后端：`HttpAgent({ url })` 直连任何实现 AG-UI 协议的 HTTP 端点（不必用 LangGraph 云）；
  我们的 FastAPI 用官方 `ag-ui-protocol` Python SDK 编码事件即可。

### 2.2 AG-UI 协议 ✅ 作为通信层一并采用

- 开放、轻量、事件驱动：`TEXT_MESSAGE_CONTENT`（流式文本）、`TOOL_CALL_START/ARGS/END`
  （工具调用生命周期）、`STATE_SNAPSHOT/STATE_DELTA`（共享状态同步）、`RUN_*`（生命周期）。
- 传输 SSE，与现有事件通道形态一致；语言无关（官方 TS/Python SDK），agent 侧不被框架锁定。

### 2.3 WebMCP ❌ 不适用于本场景（远期观察）

- Chrome 提案实验阶段：让**网页向浏览器里的外部 AI agent**（如浏览器内建的 agent）暴露结构化工具，
  解决的是「外部 agent 操作第三方网站」；不是「页面内嵌 Copilot 与自家后端 agent 双向通信」。
- 我们的诉求是自家 agent + 自家页面函数（同源、可信、需要写操作），AG-UI 的 client tools
  完整覆盖且不受浏览器实验特性限制。保持关注其标准化进展即可。

## 3. 推荐架构

```
┌─ FlowEditor（React Flow）─────────────────────────────┐
│  [Copilot Sidebar]  ← useAgent(HttpAgent→/api/copilot) │
│  前端 Tools（useCopilotAction 注册，schema 随请求下发）：│
│   读: read_flow / read_node / list_upstream            │
│   写: apply_flow（自动应用）/ add_node / update_node    │
│        / remove_node                                   │
│   页控: select_node / enter_subgraph / exit_subgraph   │
│         auto_layout / set_desc …                       │
│   动作: dry_run / save_draft                            │
└──────────────┬───────────────────────────────────────┘
               │ AG-UI SSE（消息流 + TOOL_CALL + RESULT）
┌─ backend FastAPI ─────────────────────────────────────┐
│  POST /api/copilot → 反代 recursive POST /agui          │
│   （+鉴权 + flow 上下文注入 + system prompt 拼装）        │
│  BrainRunner 可插拔：recursive /agui（M1）              │
│   → claude-code stream-json + MCP 桥（M2）              │
│   → codex app-server 翻译（M2+）                        │
│  copilot_threads：thread_id ↔ flow_id/version 入库      │
│  dry-run 编译校验 → 错误自动回喂（沿用自纠闭环）            │
└──────────────────────────────────────────────────────┘
```

关键设计决策：

1. **Agent 大脑可插拔（BrainRunner 抽象），不经 agentproc**：后端定义统一的 brain 接口
   `run(RunAgentInput) -> SSE[AG-UI 事件]`，各大脑直接对接其原生协议：
   - **recursive**（M1 默认）：**recursive 已原生内置 AG-UI 端点 `POST /agui`**（Rust 侧
     `agui_protocol` crate，handlers.rs:1174-1470）——标准 `RunAgentInput` / SSE /
     ToolCall 事件翻译 / thread 会话（transcript 落盘 + session CRUD/fork/resume 端点齐全）。
     console 后端只需做**反向代理 + 鉴权 + flow 上下文注入**，零翻译成本。
     recursive 侧待补一个小 feature：`/agui` 目前未消费 `RunAgentInput.tools`（client
     tools，只用了服务端 tool_registry），需把前端工具暴露为桥工具并复用现成的
     interrupt/resume 机制回填结果。
   - **claude-code**（M2）：经 Claude Agent SDK（stream-json 含结构化 tool_use）+ 官方
     `@ag-ui/claude-agent-sdk`；前端工具经 `--mcp-config` 注入的 stdio MCP 桥承载；
   - **codex**（M2+）：官方 `codex app-server`（JSON-RPC 2.0 over stdio，支持持久 thread
     与工具注册），按 AG-UI 官方 Middleware 指南写翻译适配。
   选择依据与各 CLI 适配调研见 §5。
2. **读画布走 tool，不走状态镜像**：flow JSON 较大且频繁变动，AGENT 主动 `read_flow`
   拉取比 STATE_DELTA 镜像更省 token、无同步竞态；只把 dirty/当前子图路径等小状态走快照。
3. **写画布先「整图应用」后「原子操作」**：MVP 提供 `apply_flow`（agent 输出完整新 IR，
   前端 jsonToFlow → setGraph **自动应用，无需确认**；未保存不落库，撤销能力 M3 补）；
   M2 加 add/update/remove 原子 tools（逐节点可见变化）。
4. **publish 不进 tool**（人手操作）；`save_draft` 由 tool 自动执行（草稿可覆盖，风险低）。
5. **对话历史持久化**：recursive 侧 transcript 已随 thread 落盘；console 侧把
   thread_id ↔ flow_id/version 关联入库（SQLite 新表 copilot_threads），支持回看与审计。

## 4. 前端 Tool 清单（M1 → M2）

| Tool | 语义 | 期 |
|---|---|---|
| read_flow | 返回当前层（或全图含子图）flowJson + dirty + 子图栈路径 + 选中节点 | M1 |
| read_node | 按 id 返回节点完整 fields | M1 |
| apply_flow | 提交完整新 flow IR，前端**自动应用**（jsonToFlow → setGraph，子图栈归位） | M1 |
| select_node | 画布选中并聚焦节点 | M2 |
| add_node / update_node / remove_node | 原子编辑（update 支持按字段 patch） | M2 |
| connect_nodes / disconnect | 连线编辑（映射 onConnect/边变更） | M2 |
| enter_subgraph / exit_subgraph | 子图导航（agent 可进入子图内编辑） | M2 |
| auto_layout | 触发对称布局 | M2 |
| dry_run | 调后端试跑，把执行错误回喂 agent（自纠闭环） | M2 |
| save_draft | 保存草稿（确认卡片） | M3 |

## 5. 大脑可插拔调研：coding CLI × AG-UI（应评审要求补充）

结论：**Claude Code、Codex、recursive 三者均可接**。recursive 原生内置 AG-UI 端点（M1
零翻译直接反代）；claude-code 有官方适配包；codex 按官方 Middleware 指南写翻译层。
**copilot 链路不使用 agentproc**（见 §5.2）。

### 5.1 各 CLI 的 AG-UI 适配现状

| 大脑 | 原生事件能力 | AG-UI 适配 | 工具注入（前端 tool 桥） | 多轮会话 |
|---|---|---|---|---|
| **recursive** | **原生 `POST /agui`**（agui_protocol crate）：标准 RunAgentInput / SSE / ToolCallStart-Args-End / Custom 事件 | **原生，零翻译**（console 反代即可） | `/agui` 尚未消费 `RunAgentInput.tools`——recursive 侧补「前端桥工具 + interrupt/resume 回填」（机制已存在） | thread_id → transcript 落盘，session CRUD/fork/resume 端点齐全 |
| **Claude Code / Agent SDK** | stream-json 含结构化 `assistant / tool_use / tool_result` | **官方 `@ag-ui/claude-agent-sdk`**（npm）现成 | `--mcp-config` 注入 stdio MCP server，把前端 tools 暴露为 MCP tools（桥实现一次即可） | `--resume <session_id>` |
| **Codex CLI** | `codex app-server`：官方 JSON-RPC 2.0 over stdio，流式事件、**支持中程消息注入与持久 thread** | 无官方适配；按 AG-UI 官方 Middleware 指南写翻译层（社区已有 promptfoo / AI SDK 的 Codex provider 先例） | app-server 协议支持注册工具，前端 tools 经桥注册 | `exec resume --json <thread_id>` / app-server thread |
| 其他（gemini-cli / qwen-code / opencode / kimi…） | 各自 `--json`/stream-json 模式 | 各写一个翻译桥 | 同 claude-code 思路（MCP） | 多数支持 session |

### 5.2 agentproc：不纳入 copilot 链路

评估结论：agentproc 的价值在 16 个 CLI profile 的进程封装与会话恢复，但其协议**刻意封闭
为纯文本 6 事件**（spec/protocol.md:308-315 明确不为 tool calls/diff/reasoning 增加类型），
各 bridge 的 `parse_event` 把 tool_use 折叠成文本——而 Copilot 的核心恰是结构化
TOOL_CALL 双向流。且 recursive 已原生说 AG-UI、claude-code 有官方适配包，agentproc 的
归一层反而多一跳。**决定：copilot 的 BrainRunner 直接对接各 CLI 原生协议**；凭证解析
（agents.json/providers.json 体系）仍复用 plaita-nodes 现有约定。agentproc 继续服务
IM 桥接等既有场景。

### 5.3 前端工具桥（关键件，实现一次多处复用）

后端起一个小型 **stdio MCP server**（或函数注册层），把 AG-UI `RunAgentInput.tools`
（read_flow/apply_flow/…）暴露为 agent 侧工具：

```
agent 调 tool → BrainRunner 捕获 → AG-UI TOOL_CALL 事件 → CopilotKit 前端执行
   ↑ 写回结果（MCP response / app-server 应答）← TOOL_CALL_RESULT 事件 ←┘
```

claude-code 用 `--mcp-config` 挂载；codex app-server 走其工具注册；**recursive 侧补
「前端桥工具 + interrupt/resume 回填」**（机制已存在，属小 feature）。该桥同时天然兼容
桌面端 Claude Code 直接编排（后续彩蛋）。

### 5.4 大脑选型建议

- **M1 用 recursive（原生 /agui 反代）**：零翻译成本，且实时 TOOL_CALL 一旦 recursive
  补上 client tools 桥即完整；M1 先以「flow 快照注入 + 完整 IR 输出」跑通。
- **M2 加 claude-code**：结构化 tool_use + 官方 AG-UI 适配 + MCP 工具桥，体验最完整；
  凭证经现有 agents.json/providers.json 体系解析，后端仍不持有模型 key。
- **M2+ 加 codex**：app-server 翻译层（JSON-RPC→AG-UI），按需排期。
- 三者共用同一 BrainRunner 接口与前端 tools，用户在设置中切换，前端零改动。

## 6. 分期计划

- **M1（MVP，约 2–3 天）**：后端 `/api/copilot` 反代 recursive `POST /agui`（+鉴权 +
  flow 上下文注入）；前端 CopilotSidebar 接入 + `read_flow/read_node/apply_flow` 三
  tool（自动应用）；`_SYNTAX_RULES` 与节点摘要注入 recursive system prompt/上下文。
  旧 AiGenerateDialog 保留。
- **M2**：**claude-code BrainRunner**（stream-json 翻译 + MCP 工具桥，实时 TOOL_CALL）、
  细粒度原子 tools + dry_run 自纠闭环 + 子图导航；**copilot_threads 会话持久化
  （thread_id ↔ flow_id/version 入库）**；消息流里渲染 flow diff 摘要卡片；
  **codex app-server 翻译层**（按需）。
- **M3**：Generative UI 完整化（逐工具卡片、撤销）、操作审计；
  brain 配置化切换 UI（设置页选择 recursive/claude-code/codex）。

## 7. 风险与开放问题

- ~~recursive 无 tool call~~ → 已有解法：recursive 侧补 client tools 桥（小 feature），
  M1 期间以快照模式先行。
- recursive 每轮带历史 + flow 快照 → token 消耗偏高；缓解：历史摘要、flow 只发变化
  diff 或节点摘要（M2 优化）。
- CopilotKit 聊天 UI 的深色主题/Inter 字体定制需要一层样式适配（工作量小，样式 token 对齐）。
- AG-UI Python SDK 与 FastAPI 的 SSE 集成为官方路径，成熟度较高；若 SDK 不合用，
  事件结构简单，可手写编码（降级方案）。
- 开放问题：① 大脑选型已定（recursive → claude-code → codex 可插拔，§5）；
  ② ~~apply_flow 确认粒度~~ 已定：自动应用，撤销能力 M3；③ ~~对话历史持久化~~ 已定：
  需要，recursive transcript 落盘 + console `copilot_threads` 关联入库（M2）。

## 8. 参考来源

基础：CopilotKit · AG-UI · WebMCP（见 §2）；大脑适配补充：

- Claude Agent SDK（stream-json，Claude Code 可编程形态）: https://code.claude.com/docs/en/agent-sdk/overview
- AG-UI 官方 Claude Agent SDK 适配包: https://www.jsdelivr.com/package/npm/@ag-ui/claude-agent-sdk
- Codex App Server（官方 JSON-RPC 双向协议）: https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md
- Codex App Server 驱动先例（promptfoo provider）: https://www.promptfoo.dev/docs/providers/openai-codex-app-server/
- AG-UI Middleware（既有协议→AG-UI 翻译指南）: https://docs.ag-ui.com/quickstart/middleware
- agentproc（大仓 Profile Hub，16 CLI 归一/会话恢复）: 大仓 `agentproc/`（spec/protocol.md、hub/、sdk/python）——评估后不纳入 copilot 链路（§5.2）
- **recursive 原生 AG-UI 端点**（本仓）：`recursive/src/http/handlers.rs:1174-1470`（AguiConverter、POST /agui）、`src/http/mod.rs:585`（路由）——标准 RunAgentInput/SSE/ToolCall/thread 会话

基础（§2 调研）：

- CopilotKit: https://github.com/copilotkit/copilotkit · https://docs.copilotkit.ai
- CopilotKit v1.50 useAgent（AG-UI 直连）: https://www.marktechpost.com/2025/12/11/copilotkit-v1-50-brings-ag-ui-agents-directly-into-your-app-with-the-new-useagent-hook/
- AG-UI 协议: https://docs.ag-ui.com/introduction · [Events](https://docs.ag-ui.com/concepts/events) · [Tools](https://docs.ag-ui.com/concepts/tools) · https://github.com/ag-ui-protocol/ag-ui
- HttpAgent + FastAPI 模式: https://webflow.copilotkit.ai/blog/build-a-stock-portfolio-ai-agent-fullstack-pydantic-ai-ag-ui
- Pydantic AI + AG-UI（SSE 事件）: https://pydantic.dev/docs/ai/integrations/ui/ag-ui/
- WebMCP（Chrome 提案）: https://developer.chrome.com/docs/ai/webmcp
