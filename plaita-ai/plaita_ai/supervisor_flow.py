"""The supervisor loop, expressed as a plaita ``@flow`` — the dogfood template.

The orchestrating steps (collect context → propose → evaluate + gate) become
``TOOL`` nodes in a compiled, versionable, dry-runnable flow, while the LLM
call and the console bookkeeping stay inside plain ``@tool`` functions. The
flow is deliberately linear: it runs ONE iteration and never publishes — the
manual gate lives inside ``sup_evaluate``.

Usage::

    from plaita_ai.console_client import client_from_env
    from plaita_ai.evals import load_dataset
    from plaita_ai.supervisor import StaticProposer
    from plaita_ai.supervisor_flow import make_tools_module, run_supervisor_flow

    tools = make_tools_module(client_from_env(), load_dataset("evals/demo/"),
                              proposer=StaticProposer([...]))
    result = run_supervisor_flow("my-flow", tools=tools)
    print(result.output)   # {"status": "improved", "promotion_ticket": {...}}
"""

from __future__ import annotations

import json
import types
from typing import Any, Dict, Optional

from plaita_ai.agent.fot.tools import register_tools_from_module, tool
from plaita_ai.console_client import ConsoleClient
from plaita_ai.evals import Dataset
from plaita_ai.flow_runner import run_flow
from plaita_ai.supervisor import Proposer, Supervisor, SupervisorPolicy

SUPERVISOR_FLOW_SOURCE = '''
@flow("supervisor-iterate", input_type="object")
def supervisor_iterate(INPUT):
    ctx = TOOL(action="sup_context", params={"flow_id": INPUT.flow_id})
    proposal = TOOL(action="sup_propose", params={"context_json": ctx})
    verdict = TOOL(action="sup_evaluate", params={"flow_id": INPUT.flow_id, "proposal_json": proposal})
    return verdict
'''


def make_tools_module(
    client: ConsoleClient,
    dataset: Dataset,
    policy: Optional[SupervisorPolicy] = None,
    proposer: Optional[Proposer] = None,
) -> types.ModuleType:
    """Build a module of ``@tool`` functions bound to one supervising session.

    Register the returned module with ``register_tools_from_module`` so the
    ``TOOL`` nodes in :data:`SUPERVISOR_FLOW_SOURCE` can resolve them.
    """
    sup = Supervisor(client, policy=policy or SupervisorPolicy(), proposer=proposer)

    @tool
    def sup_context(flow_id: str) -> str:
        """Collect the supervising context: the published definition plus the
        baseline evaluation report (cached per session)."""
        baseline = sup.baseline(flow_id, dataset)
        definition = client.get_version(flow_id, str(baseline.get("version"))).get("definition") or ""
        return json.dumps(
            {"flow_id": flow_id, "definition": definition, "baseline": baseline, "dataset": dataset.to_dict()},
            ensure_ascii=False,
            default=str,
        )

    @tool
    def sup_propose(context_json: str) -> str:
        """Ask the session's proposer for one improved definition.
        Returns {"definition": "...", "rationale": "..."} or {"error": "..."}."""
        context = json.loads(context_json)
        if sup.proposer is None:
            return json.dumps({"error": "no proposer configured"})
        proposal = sup.proposer.propose(context) or {}
        return json.dumps(proposal, ensure_ascii=False, default=str)

    @tool
    def sup_evaluate(flow_id: str, proposal_json: str) -> str:
        """Save the proposed definition as the next patch version, evaluate it
        against the dataset, compare with the baseline, and return the gated
        verdict. Never publishes: on real improvement the verdict carries a
        promotion_ticket for a human to apply."""
        try:
            proposal = json.loads(proposal_json)
        except ValueError:
            proposal = {}
        if not proposal.get("definition"):
            return json.dumps({"status": "no_proposal", "reason": "proposer produced no definition"})
        verdict = sup.evaluate_proposal(flow_id, dataset, proposal)
        return json.dumps(verdict, ensure_ascii=False, default=str)

    module = types.ModuleType("plaita_supervisor_tools")
    module.sup_context = sup_context
    module.sup_propose = sup_propose
    module.sup_evaluate = sup_evaluate
    # register_tools_from_module skips functions whose __module__ differs from
    # the module's __name__ (imported-symbol guard) — the tools are closures
    # defined in *this* file, so re-point them at the synthetic module.
    for fn in (sup_context, sup_propose, sup_evaluate):
        fn.__module__ = module.__name__
    return module


def run_supervisor_flow(
    flow_id: str,
    tools: types.ModuleType,
    source: str = SUPERVISOR_FLOW_SOURCE,
) -> Dict[str, Any]:
    """Compile + run the template flow for one supervising iteration.

    Registers ``tools`` globally (TOOL nodes resolve through the shared
    registry), executes the flow, and returns the final verdict dict (the
    flow's return value is a JSON string).
    """
    from plaita.node import get_default_registry

    from plaita_ai.agent.fot.tools import ToolNode

    # The generic TOOL dispatcher is not part of plaita's builtin registry.
    get_default_registry().register(ToolNode)
    register_tools_from_module(tools)
    result = run_flow(source, inputs={"flow_id": flow_id})
    if not result.ok:
        return {"status": "flow_error", "error": result.error}
    try:
        return json.loads(result.result) if isinstance(result.result, str) else dict(result.result)
    except (ValueError, TypeError):
        return {"status": "flow_error", "error": f"unexpected flow output: {result.result!r}"}
