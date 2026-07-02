"""CLI — thin wrapper over the same kernel as MCP tools."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from plaita_ai.flow_runner import (
    compile_flow,
    get_skill_reference,
    get_skill_text,
    list_node_types,
    result_json,
    run_flow,
)


def _read_source(path: Optional[str], stdin: bool) -> str:
    if stdin or path == "-":
        return sys.stdin.read()
    if not path:
        raise SystemExit("error: provide SOURCE path or --stdin")
    return Path(path).read_text(encoding="utf-8")


def _load_json_arg(raw: Optional[str], default: Any) -> Any:
    if raw is None:
        return default
    return json.loads(raw)


def cmd_compile(args: argparse.Namespace) -> int:
    source = _read_source(args.source, args.stdin)
    result = compile_flow(source, flow_id=args.flow_id)
    print(result_json(result))
    return 0 if result.ok else 1


def cmd_run(args: argparse.Namespace) -> int:
    source = _read_source(args.source, args.stdin)
    inputs = _load_json_arg(args.input, {})
    globals_ctx = _load_json_arg(args.globals, None)
    result = run_flow(
        source,
        inputs,
        flow_id=args.flow_id,
        globals_ctx=globals_ctx,
    )
    print(result_json(result))
    return 0 if result.ok else 1


def cmd_list_nodes(_: argparse.Namespace) -> int:
    print(result_json(list_node_types()))
    return 0


def cmd_skill(args: argparse.Namespace) -> int:
    if args.reference:
        text = get_skill_reference(args.skill, args.reference)
    else:
        text = get_skill_text(args.skill)
    print(text)
    return 0


def cmd_mcp(_: argparse.Namespace) -> int:
    from plaita_ai.mcp.server import main as mcp_main

    mcp_main()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="plaita-ai",
        description="Plaita AI integrations — compile/run @flow (same kernel as MCP tools)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_compile = sub.add_parser("compile", help="Compile @flow source to IR JSON")
    p_compile.add_argument("source", nargs="?", help="Source file path, or - with --stdin")
    p_compile.add_argument("--stdin", action="store_true", help="Read source from stdin")
    p_compile.add_argument("--flow-id", dest="flow_id", default=None)
    p_compile.set_defaults(func=cmd_compile)

    p_run = sub.add_parser("run", help="Compile and execute @flow source")
    p_run.add_argument("source", nargs="?", help="Source file path, or - with --stdin")
    p_run.add_argument("--stdin", action="store_true")
    p_run.add_argument("--input", default="{}", help="JSON object for flow inputs")
    p_run.add_argument("--globals", default=None, help="JSON object for flow.global_context")
    p_run.add_argument("--flow-id", dest="flow_id", default=None)
    p_run.set_defaults(func=cmd_run)

    p_nodes = sub.add_parser("list-nodes", help="List registered node types")
    p_nodes.set_defaults(func=cmd_list_nodes)

    p_skill = sub.add_parser("skill", help="Print bundled flow-coder skill text")
    p_skill.add_argument("--skill", default="flow-coder")
    p_skill.add_argument("--reference", default=None, help="e.g. codeflow-reference.md")
    p_skill.set_defaults(func=cmd_skill)

    p_mcp = sub.add_parser("mcp", help="Start MCP stdio server (plaita-flow)")
    p_mcp.set_defaults(func=cmd_mcp)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    code = args.func(args)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
