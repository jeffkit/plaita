"""Agent integrations for plaita-ai."""

try:
    from plaita_ai.agent.fot import FoTAgent, FoTResult
    from plaita_ai.agent.react import PlaitaAgent, PlaitaAgentResult, build_plaita_tools

    __all__ = [
        "FoTAgent",
        "FoTResult",
        "PlaitaAgent",
        "PlaitaAgentResult",
        "build_plaita_tools",
    ]
except ImportError:  # pragma: no cover - langchain optional
    __all__: list[str] = []
