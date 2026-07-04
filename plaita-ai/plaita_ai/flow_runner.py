"""Shared compile/run kernel for MCP, CLI, and FoT agent."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List, Optional

from plaita.dsl.codeflow import compile_source, flow_from_source
from plaita.node import get_default_registry

_CODEFLOW_ERR = re.compile(r"^\[codeflow\] 第 (\?|\d+) 行: (.+)$", re.DOTALL)


@dataclass
class CompileError:
    line: Optional[int]
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {"line": self.line, "message": self.message}


@dataclass
class CompileResult:
    ok: bool
    ir: Optional[Dict[str, Any]] = None
    errors: List[CompileError] = field(default_factory=list)
    flow_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "flow_id": self.flow_id,
            "ir": self.ir,
            "errors": [e.to_dict() for e in self.errors],
        }


@dataclass
class RunResult:
    ok: bool
    result: Any = None
    error: Optional[str] = None
    error_type: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "result": self.result,
            "error": self.error,
            "error_type": self.error_type,
        }


def _parse_codeflow_error(exc: Exception) -> CompileError:
    text = str(exc).strip()
    match = _CODEFLOW_ERR.match(text)
    if not match:
        return CompileError(line=None, message=text)
    line_raw, message = match.group(1), match.group(2)
    line = None if line_raw == "?" else int(line_raw)
    return CompileError(line=line, message=message)


def compile_flow(
    source: str,
    *,
    flow_id: Optional[str] = None,
    **opts: Any,
) -> CompileResult:
    """Compile @flow source to Flow IR. Returns structured errors for LLM self-correction."""
    try:
        ir = compile_source(source, flow_id=flow_id, **opts)
    except Exception as exc:
        err = _parse_codeflow_error(exc)
        return CompileResult(ok=False, errors=[err])

    resolved_flow_id = ir.get("flow_id") or flow_id
    return CompileResult(ok=True, ir=ir, flow_id=resolved_flow_id)


def run_flow(
    source: str,
    inputs: Optional[Dict[str, Any]] = None,
    *,
    flow_id: Optional[str] = None,
    globals_ctx: Optional[Dict[str, Any]] = None,
    **opts: Any,
) -> RunResult:
    """Compile @flow source and execute. Compiles first; does not run on compile failure."""
    compiled = compile_flow(source, flow_id=flow_id, **opts)
    if not compiled.ok:
        messages = "; ".join(
            f"第{e.line}行: {e.message}" if e.line is not None else e.message
            for e in compiled.errors
        )
        return RunResult(ok=False, error=messages or "compile failed", error_type="compile")

    try:
        flow = flow_from_source(source, flow_id=flow_id, **opts)
        if globals_ctx:
            flow.global_context = dict(globals_ctx)
        result = flow.run(**(inputs or {}))
    except Exception as exc:
        error_type = type(exc).__name__
        if hasattr(exc, "error_type"):
            error_type = str(getattr(exc, "error_type"))
        return RunResult(ok=False, error=str(exc), error_type=error_type)

    return RunResult(ok=True, result=result)


def list_node_types(*, discover_plugins: bool = True) -> List[Dict[str, str]]:
    """List registered node types for Planner prompts / MCP introspection."""
    registry = get_default_registry()
    if discover_plugins:
        registry.discover()
    rows: List[Dict[str, str]] = []
    for node_type in sorted(registry.list_types()):
        cls = registry.get(node_type)
        placeholder = node_type.upper()
        rows.append(
            {
                "node_type": node_type,
                "placeholder": placeholder,
                "node_name": getattr(cls, "node_name", node_type) if cls else node_type,
            }
        )
    return rows


def list_tools_registered(as_code: bool = True) -> List[Any]:
    """List all tools registered via ToolNode / @tool / register_tool_node.

    Args:
        as_code: When True (default), return Python-style function signature
            strings — ideal for AI writing @flow DSL.  When False, return
            JSON-serialisable dicts (backward compat / programmatic use).

    Returns an empty list if no tools have been registered yet.
    """
    try:
        from plaita_ai.agent.fot.tools import list_tools
        return list_tools(as_code=as_code)
    except ImportError:
        return []


def get_skill_text(skill_name: str = "flow-coder") -> str:
    """Load bundled skill markdown (for agents without a local skill install)."""
    package = "plaita_ai.skills"
    filename = f"{skill_name}/SKILL.md"
    try:
        return resources.files(package).joinpath(filename).read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, TypeError):
        pass

    # Editable / dev fallback: plaita-ai/plaita_ai/skills/...
    here = Path(__file__).resolve().parent / "skills" / skill_name / "SKILL.md"
    if here.is_file():
        return here.read_text(encoding="utf-8")
    raise FileNotFoundError(f"skill not found: {skill_name}")


def get_skill_reference(skill_name: str, ref_name: str) -> str:
    """Load a bundled skill reference file."""
    package = "plaita_ai.skills"
    path = f"{skill_name}/references/{ref_name}"
    try:
        return resources.files(package).joinpath(path).read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, TypeError):
        pass

    here = Path(__file__).resolve().parent / "skills" / skill_name / "references" / ref_name
    if here.is_file():
        return here.read_text(encoding="utf-8")
    raise FileNotFoundError(f"skill reference not found: {skill_name}/{ref_name}")


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def result_json(data: Any) -> str:
    if hasattr(data, "to_dict"):
        return dumps_json(data.to_dict())
    return dumps_json(data)
