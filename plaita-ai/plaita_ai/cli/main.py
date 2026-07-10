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


def cmd_mcp(args: argparse.Namespace) -> int:
    from plaita_ai.mcp.server import main as mcp_main

    # CLI flags override / supplement env for tool bundles
    if args.tools:
        import os

        os.environ["PLAITA_TOOLS"] = args.tools
    if args.resources:
        import os

        os.environ["PLAITA_RESOURCES"] = args.resources

    mcp_main(
        plugins=args.plugin or [],
        extra_paths=args.plugin_path or [],
    )
    return 0


def cmd_tools_validate(args: argparse.Namespace) -> int:
    from plaita_ai.tools import validate_tool_bundle

    errors = validate_tool_bundle(args.tools, args.resources)
    if errors:
        for e in errors:
            print(f"error: {e}", file=sys.stderr)
        return 1
    print("ok")
    return 0


def cmd_tools_list(args: argparse.Namespace) -> int:
    """Validate + register tools from a bundle, then print names."""
    from plaita_ai.agent.fot.tools import ToolNode
    from plaita_ai.tools import load_tool_bundle, validate_tool_bundle

    errors = validate_tool_bundle(args.tools, args.resources)
    if errors:
        for e in errors:
            print(f"error: {e}", file=sys.stderr)
        return 1
    ToolNode.clear()
    specs = load_tool_bundle(args.tools, args.resources)
    for s in specs:
        print(f"{s.placeholder}\t{s.name}\t{s.description}")
    return 0


def cmd_llm_benchmark(args: argparse.Namespace) -> int:
    """Drive the LLM benchmark harness over the agent-benchmark task set.

    The runner lives under ``tests/llm`` (a dev/research tool, not shipped in
    the wheel). It is only importable from a source checkout, so we locate the
    ``plaita-ai/`` root relative to this module and put it on ``sys.path``.
    """
    import sys as _sys
    plaita_ai_root = Path(__file__).resolve().parents[2]
    if str(plaita_ai_root) not in _sys.path:
        _sys.path.insert(0, str(plaita_ai_root))
    from tests.llm.runner import run_benchmark  # type: ignore[import-not-found]

    seed: Optional[int] = None if str(args.seed).lower() == "none" else args.seed
    run_benchmark(
        agent=args.agent,
        task_ids=args.task_ids,
        difficulty=args.difficulty,
        include_broken=args.include_broken,
        include_http=args.include_http,
        out_dir=args.out_dir,
        seed=seed,
    )
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
    p_mcp.add_argument(
        "--plugin",
        metavar="MODULE",
        action="append",
        default=[],
        help=(
            "Python module path to import before starting the server "
            "(registers custom nodes). Repeatable. "
            "Also reads PLAITA_PLUGINS env var (comma-separated)."
        ),
    )
    p_mcp.add_argument(
        "--plugin-path",
        metavar="DIR",
        action="append",
        default=[],
        help=(
            "Directory to prepend to sys.path before importing plugins. "
            "Repeatable. Also reads PLAITA_PLUGIN_PATH env var."
        ),
    )
    p_mcp.add_argument(
        "--tools",
        metavar="FILE",
        default=None,
        help="Tool bundle YAML/JSON (sets PLAITA_TOOLS). Also reads PLAITA_TOOLS env.",
    )
    p_mcp.add_argument(
        "--resources",
        metavar="FILE",
        default=None,
        help="Resources YAML/JSON (sets PLAITA_RESOURCES).",
    )
    p_mcp.set_defaults(func=cmd_mcp)

    p_tools = sub.add_parser("tools", help="Validate / list tool bundles")
    tools_sub = p_tools.add_subparsers(dest="tools_command", required=True)

    p_tv = tools_sub.add_parser("validate", help="Validate tools.yaml without registering")
    p_tv.add_argument("tools", help="Path to tools.yaml / tools.json")
    p_tv.add_argument("--resources", default=None, help="Optional resources.yaml")
    p_tv.set_defaults(func=cmd_tools_validate)

    p_tl = tools_sub.add_parser("list", help="Validate, register, and list tools")
    p_tl.add_argument("tools", help="Path to tools.yaml / tools.json")
    p_tl.add_argument("--resources", default=None, help="Optional resources.yaml")
    p_tl.set_defaults(func=cmd_tools_list)

    p_bench = sub.add_parser("llm-benchmark", help="Run FoT/ReAct × agent-benchmark (dev tool, needs checkout)")
    p_bench.add_argument("--agent", choices=["fot", "react", "both"], default="both")
    p_bench.add_argument("--task-ids", nargs="*", default=None)
    p_bench.add_argument("--difficulty", default=None)
    p_bench.add_argument("--include-broken", action="store_true")
    p_bench.add_argument("--include-http", action="store_true")
    p_bench.add_argument("--out-dir", default="runs")
    p_bench.add_argument("--seed", default="0", help="int seed, or 'none' to disable")
    p_bench.set_defaults(func=cmd_llm_benchmark)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    code = args.func(args)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
