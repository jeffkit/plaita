"""FoT planner prompts — @flow only (no JSON actions).

DSL 知识的权威来源是 flow-coder skill 的 `codeflow-reference.md`，由 planner
注入到 `{dsl_section}`。本文件只保留输出格式与几条高频踩坑的"速记"护栏，
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

{instruction_section}"""

REVIEW_SYSTEM = """你是 Plaita @flow 源码审查员。根据编译错误修正源码，输出完整可编译的 @flow Python 代码。

## 输出格式
- 只输出一个 ```python ... ``` 代码块（完整源码，不是 diff/patch）。
- 不要 JSON actions，不要额外说明。

## 修正原则
- 对照下方《@flow DSL 参考》确认语法，不要发明 F.xxx；节点调用不要嵌在 return 里。
- 优先按 errors 的 line/message 定点修正，保持未报错部分不动。

## 可用工具（TOOL 节点）
{tools_section}

## @flow DSL 参考（权威，flow-coder skill）
{dsl_section}

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
