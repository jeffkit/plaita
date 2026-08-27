"""AI 流程生成 —— 后端作为 **agent 宿主**（agentproc 协议），前端经 SSE 消费事件流。

架构约定（ADR-2026-08-27 phase2 修正）：
- console 后端**不直连 LLM 端点**、不持有任何模型凭证；Agent 循环由真实编码
  Agent 执行（默认 recursive/GLM，凭证走其自身 agents.json/providers.json，
  经 plaita-nodes 的 agentproc 封装运行）。
- 本模块职责仅为宿主：构建任务上下文（语法约束+可用节点+需求+上轮错误）、
  以流式方式跑 agent 轮次、用 plaita 编译器做**确定性校验**，校验失败把
  带行号错误作为下一轮任务回喂（自纠决策在 agent）。
- 前端经 POST /api/flows/ai-generate/stream 的 SSE 事件消费：
    {"type":"run_started"} / {"type":"turn_started","attempt":n}
    {"type":"line","text":...} / {"type":"compile_failed","errors":[...]}
    {"type":"finished","ok":bool,"source":...,"ir":{...}|null}
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger("services.ai_flow")

_MAX_FIX_ROUNDS = 3
_AGENT_TIMEOUT_SECS = 1800

_SYNTAX_RULES = """\
## 任务
根据「需求描述」编写可直接执行的 Plaita @flow Python 源码，写入你的最终回复。

## @flow 语法硬约束
- 函数骨架：@flow("<flow_id>") def f(INPUT): 单一主函数；子流程用模块级 @childflow def sub(INPUT): ...
- 语句仅支持：赋值（右侧为节点调用 / F.xxx(...)）、if/elif/else、for x in MAP/FILTER/FIND/REDUCE(coll, id="xx"):、return
- 变量名即节点 id：全局唯一，禁止跨分支同名赋值；下游用 NODE.<变量名>.<字段> 引用输出
- 表达式：INPUT.x / NODE.id.field / F.concat(a,b) / F.join(list,sep)；比较与 and/or/not 只能出现在 if 条件
- 禁止：f-string、三元表达式、推导式、lambda、while、print、任意 Python 内置函数调用、以及显式调用 end(...) 节点
- return 语句就是流程/分支的结束与结果输出：主流程最终 return 结果 dict；不要创建名为 end 的变量
- 节点调用 kwargs 即节点字段；跨分支引用未走的路径节点会 KeyError——各分支都要保证后续引用已赋值
- 【自定义节点调用铁律】用「类型名全大写」作为占位符调用并赋值给小写变量：
      items = BUILD_ITEMS(platforms=INPUT.platforms)
      out = AGENTRUN(agent="glm-52", prompt=F.concat("xx"))
      fw = WRITEFILE(path="/tmp/a.txt", content="x")
  之后用 NODE.<小写变量>.<输出字段> 引用。绝不使用小写的类型名做函数调用
- 子流程 body 内 return 即该项结果；MAP 循环后结果在 NODE.<map id>（列表）
"""


def _workspace() -> Path:
    """Agent 工作区（持久，便于排查 agent 过程产物）。"""
    ws = Path(os.environ.get("PLAITA_CONSOLE_AI_WORKSPACE",
                             os.path.join(tempfile.gettempdir(), "plaita-console-agent-ws")))
    ws.mkdir(parents=True, exist_ok=True)
    return ws


def _available_nodes_digest() -> str:
    try:
        from plaita.node import get_default_registry

        types = sorted(get_default_registry().list_types())
        return "、".join(types)
    except Exception:  # noqa: BLE001
        return "（无法枚举）"


def build_task(requirement: str, last_errors: list[str] | None = None) -> str:
    task = f"{_SYNTAX_RULES}\n## 可用节点类型\n{_available_nodes_digest()}\n\n## 需求描述\n{requirement}\n"
    if last_errors:
        task += ("\n## 上一次生成的编译错误（必须修复后重新输出完整源码）\n"
                 + "\n".join(last_errors) + "\n")
    return task


def _find_flow_in_workspace(workspace: Path) -> str | None:
    """编码型 Agent 常把产物写成文件：扫工作区找含 @flow 的最新源码文件。"""
    import time

    best: tuple[float, str] | None = None
    for pattern in ("*.py", "*.flow", "*.md"):
        for f in workspace.glob(pattern):
            try:
                if "_FLOW_SPEC" in f.name or ".flowcast" in str(f):
                    continue
                text = f.read_text(encoding="utf-8")
            except OSError:
                continue
            if "@flow" in text:
                mtime = f.stat().st_mtime
                if best is None or mtime > best[0]:
                    best = (mtime, text)
    return best[1] if best else None


def _coerce_source(result_text: str, workspace: Path) -> str:
    """从 agent 回复或工作区文件中取出可编译的 @flow 源码。"""
    import re as _re

    def strip_fences(text: str) -> str:
        # 剥掉 ``` / ```python 围栏与结尾悬空的 ``` 行
        return _re.sub(r"^```[a-zA-Z]*\s*|\s*```\s*$", "", text.strip()).strip()

    def has_flow(text: str) -> bool:
        return "@flow" in text and not text.strip().startswith("```") or (
            "@flow" in text)

    def clean(text: str) -> str:
        return strip_fences(text) if "```" in text else text.strip()

    stripped = result_text.strip()
    if has_flow(stripped):
        start = stripped.find("@flow")
        cleaned = clean(stripped[start:])
        return cleaned
    from_file = _find_flow_in_workspace(workspace)
    if from_file and has_flow(from_file):
        start = from_file.find("@flow")
        return clean(from_file[start:])
    return ""


def generate_flow_events(requirement: str) -> Iterator[dict]:
    """宿主生成器：yield AG-UI 风格事件 dict（路由层转 SSE）。"""
    from plaita.dsl.codeflow import compile_source

    yield {"type": "run_started"}

    workspace = _workspace()

    errors: list[str] = []
    ir: dict | None = None
    source = ""

    for attempt in range(1 + _MAX_FIX_ROUNDS):
        yield {"type": "turn_started", "attempt": attempt + 1}
        task = build_task(requirement, errors or None)
        task += "\n\n【重要】不要创建任何文件；把最终的完整 @flow 源码作为你回复正文的最后一段文本直接输出。"

        ok = False
        result_text = ""
        err_text = ""
        try:
            from plaita_nodes.agent_run import recursive_stream_turn

            for event in recursive_stream_turn(task, workspace=str(workspace),
                                               timeout_secs=_AGENT_TIMEOUT_SECS):
                if event["type"] == "line":
                    yield {"type": "line", "text": event["text"][:300]}
                elif event["type"] == "done":
                    ok, result_text, err_text = event["ok"], event["result"], event["error"]
        except ImportError as exc:
            yield {"type": "compile_failed", "errors": [f"agent 运行原语不可用: {exc}"]}
            return
        except Exception as exc:  # noqa: BLE001
            yield {"type": "compile_failed", "errors": [f"agent 轮次异常: {exc}"]}
            return

        if not ok:
            errors = [err_text]
            yield {"type": "compile_failed", "errors": errors}
            continue

        (workspace / ".last_result.txt").write_text(str(result_text)[:20000], encoding="utf-8")
        source = _coerce_source(str(result_text), workspace) or str(result_text)
        (workspace / ".last_source.txt").write_text(source[:20000], encoding="utf-8")
        try:
            ir = compile_source(source)
            yield {"type": "finished", "ok": True, "source": source, "ir": ir}
            return
        except Exception as exc:  # noqa: BLE001
            err_text = str(exc)
            # 提取行号信息以便 agent 定位
            errors = [f"第 {attempt} 轮源码编译失败：{err_text}"]
            yield {"type": "compile_failed", "errors": errors}

    yield {"type": "finished", "ok": False, "source": source, "ir": None,
           "reason": f"多轮自纠失败：{'；'.join(errors[-2:])}"}
