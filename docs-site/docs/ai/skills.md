# Skill 说明

`plaita-ai` 内置了供 AI Coding Agent（如 Cursor、Claude Code）使用的 skill，帮助 Agent 正确生成并执行 `@flow` DSL。

## 内置 Skill 列表

| Skill 名 | 用途 |
|---------|------|
| `flow-coder` | 指导 AI 用 `@flow` DSL 生成并执行流程（核心 skill） |
| `plaita-flow-builder` | 帮助 AI 用 Builder API 或 JSON 格式构建流程 |
| `plaita-flow-runner` | 帮助 AI 理解并选择执行模式（Normal / Generator / Distributed） |

## 安装到 Claude Code / Cursor

Skills 的权威副本在 `plaita-ai/plaita_ai/skills/`，随 `pip install plaita-ai` 分发。
软链到用户 skill 目录即可：

```bash
SKILLS="$(python -c 'import plaita_ai, pathlib; print(pathlib.Path(plaita_ai.__file__).parent / "skills")')"

# Claude Code
ln -snf "$SKILLS/flow-coder"          ~/.claude/skills/flow-coder
ln -snf "$SKILLS/plaita-flow-builder" ~/.claude/skills/plaita-flow-builder
ln -snf "$SKILLS/plaita-flow-runner"  ~/.claude/skills/plaita-flow-runner

# Cursor（可选）
ln -snf "$SKILLS/flow-coder"          ~/.cursor/skills/flow-coder
```

安装后，Agent 可以在需要时读取 skill 指令，按规范生成并执行 `@flow` 代码。

## 通过 MCP 获取 Skill

即使没有本地安装，Agent 也可以通过 MCP 工具动态拉取 skill 内容：

```
flow_get_skill(skill_name="flow-coder")
→ 返回 SKILL.md 全文

flow_get_skill_reference(skill_name="flow-coder", reference="codeflow-reference.md")
→ 返回完整 @flow 语法参考
```

## flow-coder Skill 说明

**触发时机**：用户说"用 `@flow` 实现…"、"生成一个 flow"、"编排一个流程并执行"。

**工作流程**（五步闭环）：

1. **理解需求**：拆分输入字段、输出形态、步骤结构
2. **生成 `@flow` 源码**：包含所有必要的 `@childflow` 子流程
3. **编译校验**：`flow_compile` 或 `compile_source()`，失败进入第 5 步
4. **执行**：`flow_run` 或 `flow_from_source().run()`
5. **错误回灌自纠**：把带行号的错误回灌 LLM，重生成，最多 3 轮

**关键参考文件**：`references/codeflow-reference.md` — `@flow` 完整语法，生成复杂流程前必读。

## Skill 维护说明

Skills 只有一处权威副本：`plaita-ai/plaita_ai/skills/`。

- 修改 skill 内容直接编辑这里的文件
- 用户目录的软链（`~/.claude/skills/`）直接指向此处，改完即生效，无需同步操作
- `pip install plaita-ai` 会把这个目录打包进 wheel，MCP `flow_get_skill` 工具返回的也是同一份内容
