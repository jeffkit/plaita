"""plaita-ai — MCP / CLI / FoT agent integrations on top of plaita."""

from plaita_ai.flow_runner import (
    CompileError,
    CompileResult,
    RunResult,
    compile_flow,
    get_skill_text,
    list_node_types,
    run_flow,
)

__all__ = [
    "CompileError",
    "CompileResult",
    "RunResult",
    "compile_flow",
    "get_skill_text",
    "list_node_types",
    "run_flow",
]

try:
    from plaita_ai.agent.fot import FoTAgent, FoTResult
    from plaita_ai.agent.react import PlaitaAgent, PlaitaAgentResult, build_plaita_tools

    __all__ += [
        "FoTAgent",
        "FoTResult",
        "PlaitaAgent",
        "PlaitaAgentResult",
        "build_plaita_tools",
    ]
except ImportError:  # pragma: no cover
    pass

__version__ = "0.1.0"
