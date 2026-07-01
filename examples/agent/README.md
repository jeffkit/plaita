# Agent 编排示例

用 plaita 做 Agent 编排的**真实可跑**示例：参考 edan-backend 的 Plaita 自定义节点做法，定义三个 LLM 相关节点，串成三个端到端 Agent 案例。**无需任何 API key**，开箱即跑。

## 运行

在仓库根目录：

```bash
python -m examples.agent.demo
```

预期输出（节选）：

```
【RAG】问：plaita 有几种执行模式？
资料：plaita 支持三种执行模式：Normal 同步、Generator 单步、Distributed 断点续执。, plaita 是 Plaita 逻辑编排系统的官方 Python 运行时。
...

【Tool-use】北京今天天气如何 -> 北京：晴，25°C，东南风 3 级
【Tool-use】3 加 5 是多少 -> 3 + 5 = 8
【Tool-use】plaita 是什么 -> 关于「plaita 是什么」的 3 条结果：① … ② … ③ …

【Router】这个多少钱？有优惠吗 -> 你是销售客服。用户输入：这个多少钱？有优惠吗，请给出销售回复。
【Router】程序一直报错打不开 -> 你是技术支持。用户输入：程序一直报错打不开，请给出技术支持回复。
...
```

## 三个自定义节点

都在 `nodes.py`，继承 `plaita.node.Node`，实现 `execute(self, execution)`，并在解析 Flow 前 `register_all()` 注册到默认 registry。

| 节点 | `type` | 作用 | 参考 edan |
|------|--------|------|-----------|
| `LLMNode` | `llm` | 调 LLM 生成文本；prompt 模板支持 `{% $INPUT.x %}` 插值与 `$F.join(...)`；`model` 字段从 `$GLOBAL.llms` 选命名 LLM | `llm_completion` |
| `ToolNode` | `tool` | 把 Python 函数注册成工具，按 `action` 名调用，`params` 展开表达式 | `Action` |
| `RetrieverNode` | `retrieve` | 内存关键词检索，`library` 选库、`query` 检索、`top_k` 取条数 | `vst_retrieve` |

`execution.evaluate()` 负责解析 `$INPUT` / `$NODE` / `$GLOBAL` 表达式与 `{% %}` 插值；`execution.get_global_variable("llms")` 读取注入的 LLM。

## LLM 后端：FakeLLM 与真实 LLM

`FakeLLM` 是确定性离线 LLM：

- `rules=[(keyword, response), ...]`：首个命中 prompt 的关键词决定输出，适合模拟"分类器/规划器"这类结构化输出角色。
- `default="echo"`：原样回显 prompt，便于看到检索结果/角色提示确实传到了 LLM 节点。

**换真实 LLM**：实现 `LLM` 协议（`complete(prompt, *, system=None) -> str`），把实例放进 `flow.global_context["llms"]`，flow 本身不用改：

```python
flow = Flow.from_string(open("flows/rag.json").read())
flow.global_context = {"llms": {"responder": MyOpenAILLM(model="gpt-4o")}}
flow.run(question="plaita 有几种执行模式")
```

## 三个案例

| 案例 | 流程 | 演示点 |
|------|------|--------|
| **RAG** (`flows/rag.json`) | `retrieve → llm` | 检索结果经 `$F.join` 拼进 prompt |
| **Tool-use** (`flows/tool_use.json`) | `llm 规划 → switch → tool → end` | LLM 决策选工具、节点间 `$NODE` 传参 |
| **Router** (`flows/router.json`) | `llm 分类 → switch → llm 分角色回复` | 多角色 LLM、条件路由多分支 |

## 目录结构

```
examples/agent/
├── nodes.py          # 三个自定义节点 + FakeLLM
├── tools.py          # 示例工具函数（weather/calc/search）
├── corpus.py         # 示例检索语料
├── demo.py           # 可运行入口
└── flows/
    ├── rag.json
    ├── tool_use.json
    └── router.json
```

## 关联文档

- [自定义节点](../../docs-site/docs/nodes/custom.md) —— 自定义节点 API 详解
- [Agent 编排场景](../../docs-site/docs/scenarios/agent-orchestration.md) —— 端到端编排叙事
