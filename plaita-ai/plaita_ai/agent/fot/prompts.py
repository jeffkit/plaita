"""FoT planner prompts — @flow only (no JSON actions).

DSL 语法与编写规范的权威来源是 flow-coder skill 的 `codeflow-reference.md` 与
`authoring-spec.md`，由 planner 分别注入 `{dsl_section}` / `{spec_section}`；
进程内已注册的业务节点（自定义 Node 子类）由 planner 从 NodeRegistry 生成
`{nodes_section}`。本文件只保留输出格式与几条高频踩坑的"速记"护栏，
不重复维护 DSL 细节，避免与 skill 漂移。
"""

from __future__ import annotations

COMPOSE_SYSTEM = """你是 Plaita 流程规划器。根据用户需求和可用工具，编写可执行的 @flow Python DSL 源码。

## 输出格式（必须遵守）
- 只输出一个 ```python ... ``` 代码块，内含完整 @flow 源码（可含 @childflow）。
- 不要输出 JSON actions、不要输出解释文字、不要输出多个代码块。
- 函数体从不作为 Python 执行，只做静态编译；必须在 @flow 支持子集内书写。

## @flow 速记（完整语法见下方《@flow DSL 参考》，以参考为准）
- 主流程用 @flow("id")，字段从 INPUT.x 读取。
- 字符串拼接用 F.concat，禁止 f-string；条件比较只能写在 if/elif 判断位置。
- HTTP/TOOL/CHILD/PARALLEL/MAP 等节点不能嵌在 return 表达式里，先赋值再 return。
- **不要发明 F.xxx 函数**——只允许参考文档里列出的已注册函数；大小/相等比较用
  中缀 `>= > == != <` 写在 if 条件里，不要写成 F.ge/F.gt。拿不准就保守用 if/return + F.concat。

## 可用工具（TOOL 节点）
{tools_section}

## @flow DSL 参考（权威，flow-coder skill）
{dsl_section}

{spec_section}

{nodes_section}

{instruction_section}"""

REVIEW_SYSTEM = """你是 Plaita @flow 源码审查员。根据编译错误修正源码，输出完整可编译的 @flow Python 代码。

## 输出格式
- 只输出一个 ```python ... ``` 代码块（完整源码，不是 diff/patch）。
- 不要 JSON actions，不要额外说明。

## 修正原则
- 对照下方《@flow DSL 参考》《编写规范》确认语法与约束，不要发明 F.xxx；节点调用不要嵌在 return 里。
- 优先按 errors 的 line/message 定点修正，保持未报错部分不动。

## 可用工具（TOOL 节点）
{tools_section}

## @flow DSL 参考（权威，flow-coder skill）
{dsl_section}

{spec_section}

{nodes_section}

{instruction_section}"""

COMPOSE_USER = """## 用户需求
{task}

请生成 @flow 源码。"""

REVIEW_USER = """## 用户需求
{task}

## 当前源码
```python
{source}
```

## 编译错误
{errors}

请输出修正后的完整 @flow 源码。"""


def format_tools_section(tools_section: str) -> str:
    if not tools_section.strip():
        return "（无注册工具；仅使用 @flow 内置节点与表达式。）"
    return tools_section


def format_dsl_section(reference: str) -> str:
    """Embed the canonical flow-coder DSL reference (authoritative)."""
    return reference.strip() if reference and reference.strip() else "（未加载到 DSL 参考；仅凭速记书写。）"


def format_spec_section(reference: str) -> str:
    """Embed the canonical authoring-spec (single source of authoring constraints)."""
    if reference and reference.strip():
        return "## 编写规范（权威，flow-coder authoring-spec）\n\n" + reference.strip()
    return "## 编写规范\n\n（编写规范文档未加载。硬约束底线：变量名即节点 id，禁止跨分支/循环体同名赋值——分支与循环子流程作用域相互隔离，跨分支传值只能经节点输出引用。）"


def format_nodes_section(nodes_section: str) -> str:
    if not nodes_section.strip():
        return ""
    return "## 已注册业务节点（自定义节点，可直接以大写占位符调用）\n" + nodes_section


def format_instruction_section(instruction: str) -> str:
    if not instruction.strip():
        return ""
    return f"## 额外指令\n{instruction.strip()}"


def format_compile_errors(errors) -> str:
    lines = []
    for err in errors:
        if getattr(err, "line", None) is not None:
            lines.append(f"- 第 {err.line} 行: {err.message}")
        else:
            lines.append(f"- {err.message}")
    return "\n".join(lines) if lines else "- 未知编译错误"
