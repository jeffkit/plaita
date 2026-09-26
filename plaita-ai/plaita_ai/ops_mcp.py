"""MCP tools over the plaita-console management API — the ops plane.

Where ``flow_*`` tools serve the codegen loop (compile/run @flow source),
``console_*`` tools serve the supervisor loop against a *deployed* plaita:
flows, versions, runs, metrics, dry-run, plus eval and supervisor entry
points. Registration is unconditional; the console client is built lazily
from environment on first call (see ``plaita_ai.console_client``), so the
server starts fine without console config and tools report a clear error.
"""

import functools
import inspect
import json
from typing import Any, Dict, Optional

from plaita_ai.console_client import ConsoleClient, ConsoleClientError, client_from_env
from plaita_ai.ops import flow_metrics, flow_versions, version_diff
from plaita_ai.supervisor import PromptProposer, StaticProposer, Supervisor, SupervisorPolicy

_shared_client: Optional[ConsoleClient] = None


def set_client(client: Optional[ConsoleClient]) -> None:
    """Override the shared client (tests)."""
    global _shared_client
    _shared_client = client


def _get_client() -> ConsoleClient:
    global _shared_client
    if _shared_client is None:
        _shared_client = client_from_env()
    return _shared_client


def _json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _guarded(fn):
    """Run a tool body, turning ConsoleClientError into an ok:false payload.

    functools.wraps + an explicit ``__signature__`` keep FastMCP's schema
    generation (name, doc, parameter types) pointed at the real function.
    """

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> str:
        try:
            return _json(fn(*args, **kwargs))
        except ConsoleClientError as exc:
            return _json({"ok": False, "error": str(exc), "detail": exc.detail})
        except (ValueError, KeyError) as exc:
            return _json({"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    wrapper.__signature__ = inspect.signature(fn)  # type: ignore[attr-defined]
    return wrapper


def register(mcp: Any) -> None:
    """Attach console/eval/supervisor tools to a FastMCP instance."""

    # -- flows & versions -------------------------------------------------

    @mcp.tool()
    @_guarded
    def console_flow_list() -> str:
        """List flows registered in the plaita console.

        Returns JSON: {flows: [{flow_id, author, desc, updated_at}], total}
        """
        return _get_client().list_flows()

    @mcp.tool()
    @_guarded
    def console_flow_get(flow_id: str) -> str:
        """Flow detail with its full version list, semver-sorted and with the
        published version flagged.

        Returns JSON: {flow_id, latest, published, versions: [...]}
        """
        return flow_versions(_get_client(), flow_id)

    @mcp.tool()
    @_guarded
    def console_flow_version_get(flow_id: str, version: str) -> str:
        """One version's definition (JSON string), layout, and status."""
        return _get_client().get_version(flow_id, version)

    @mcp.tool()
    @_guarded
    def console_flow_version_save(flow_id: str, version: str, definition: str, created_by: str = "plaita-ai") -> str:
        """Save a definition string under a semver version (draft, not live).

        Args:
            definition: Flow definition as a JSON string.
            created_by: Attribution recorded by the console.
        """
        return _get_client().save_version(flow_id, version, definition, created_by=created_by)

    @mcp.tool()
    @_guarded
    def console_flow_publish(flow_id: str, version: str) -> str:
        """Publish (promote) a saved version — the production switch. This is
        the human gate: call it only after a human approved the version."""
        return _get_client().publish_version(flow_id, version)

    @mcp.tool()
    @_guarded
    def console_flow_diff(flow_id: str, base_version: str, candidate_version: str) -> str:
        """Unified diff between two versions' definitions."""
        return version_diff(_get_client(), flow_id, base_version, candidate_version)

    @mcp.tool()
    @_guarded
    def console_flow_metrics(flow_id: str, limit: int = 50) -> str:
        """Recent-execution health for one flow: by-status counts, success
        rate, average duration, latest failures.

        Args:
            limit: How many recent executions to scan (capped at 200).
        """
        return flow_metrics(_get_client(), flow_id, limit=limit)

    # -- executions ---------------------------------------------------------

    @mcp.tool()
    @_guarded
    def console_runs_list(
        flow_id: Optional[str] = None, status: Optional[str] = None, page: int = 1, size: int = 20
    ) -> str:
        """List executions, optionally filtered by flow and/or status.

        Statuses: running / completed / failed / error / cancelled.
        """
        return _get_client().list_executions(flow_id=flow_id, status=status, page=page, size=size)

    @mcp.tool()
    @_guarded
    def console_run_get(execution_id: str) -> str:
        """One execution: status, timings, error, node trace, output, and the
        Langfuse trace URL when observability is on."""
        return _get_client().get_execution(execution_id)

    @mcp.tool()
    @_guarded
    def console_run_start(
        flow_id: str,
        version: Optional[str] = None,
        params_json: str = "{}",
        wait: bool = True,
        timeout_s: float = 120.0,
    ) -> str:
        """Start a flow execution (latest published version unless one is
        given); with wait=true, poll until a terminal status.

        Args:
            params_json: JSON object passed as flow input fields.
        """
        params = json.loads(params_json) if params_json else {}
        started = _get_client().start_execution(flow_id, version=version, params=params)
        if wait and started.get("execution_id"):
            return _get_client().wait_execution(str(started["execution_id"]), timeout_s=timeout_s)
        return started

    @mcp.tool()
    @_guarded
    def console_run_cancel(execution_id: str) -> str:
        """Cancel a running execution."""
        return _get_client().cancel_execution(execution_id)

    @mcp.tool()
    @_guarded
    def console_dry_run(flow_json: str, input_json: str = "{}") -> str:
        """Compile + execute a definition in-process (no deployment).

        Args:
            flow_json: Flow definition as a JSON string.
            input_json: JSON object passed as flow input.
        """
        return _get_client().dry_run(flow_json, input=json.loads(input_json) if input_json else {})

    # -- evaluation -----------------------------------------------------------

    @mcp.tool()
    @_guarded
    def eval_run(
        flow_id: str, version: str, dataset_path: str, mode: str = "dry-run", timeout_s: float = 120.0
    ) -> str:
        """Evaluate one flow version against a dataset (JSON file or directory
        of case files: {"id", "input", "expect"}).

        mode: "dry-run" (in-process, no deploy) or "execution" (real runs).
        Expect ops: contains / equals_path / not_empty / judge (LLM, optional).
        Returns the full EvalReport (pass_rate, avg_score, per-case detail).
        """
        from plaita_ai.evals import evaluate, load_dataset

        dataset = load_dataset(dataset_path)
        return evaluate(_get_client(), flow_id, version, dataset, mode=mode, timeout_s=timeout_s)

    @mcp.tool()
    @_guarded
    def eval_compare(base_json: str, candidate_json: str) -> str:
        """Diff two EvalReport JSON strings (same flow, two versions) into a
        promote/rollback view: pass_rate/avg_score deltas, improved and
        regressed case ids."""
        from plaita_ai.evals import compare

        base = json.loads(base_json)
        candidate = json.loads(candidate_json)
        return compare(base, candidate)

    # -- supervisor ---------------------------------------------------------

    @mcp.tool()
    @_guarded
    def supervisor_iterate(
        flow_id: str,
        dataset_path: str,
        proposer: str = "prompt",
        max_iterations: int = 1,
    ) -> str:
        """Run the self-iteration loop: evaluate the published baseline, ask
        the proposer for an improved definition, save it as the next patch
        version, evaluate it, compare, and — on real improvement — return a
        PROMOTION TICKET. The loop never publishes: a human applies the
        ticket via console_flow_publish.

        Args:
            proposer: "prompt" (LLM via PLAITA_AI_PROPOSER_* env) or "static".
            max_iterations: Iterations in this call (1 = single step).
        """
        policy = SupervisorPolicy(max_iterations=max(1, max_iterations), promote_gate="manual")
        if proposer == "static":
            agent: Any = StaticProposer()
        else:
            agent = PromptProposer()
        supervisor = Supervisor(_get_client(), policy=policy, proposer=agent)
        from plaita_ai.evals import load_dataset

        return supervisor.run_loop(flow_id, load_dataset(dataset_path))

    # -- canary / shadow -------------------------------------------------

    @mcp.tool()
    @_guarded
    def canary_shadow_once(
        flow_id: str, candidate_version: str, params_json: str = "{}", baseline_version: Optional[str] = None
    ) -> str:
        """Stateless one-input shadow check: the baseline version runs for real
        while the candidate receives the same input as an in-process dry-run.
        Returns both outputs side by side — the zero-risk pre-promote check."""
        from plaita_ai.canary import shadow_once

        params = json.loads(params_json) if params_json else {}
        return shadow_once(_get_client(), flow_id, candidate_version, params, baseline_version=baseline_version)

    @mcp.tool()
    @_guarded
    def canary_verdict(state_json: str, min_count: int = 10, tolerance: float = 0.02) -> str:
        """Promote/rollback recommendation from a serialized CanaryRun state
        (the {"flow_id", "baseline_version", "candidate_version", "policy",
        "records"} dict)."""
        from plaita_ai.canary import CanaryRun

        run = CanaryRun.from_dict(_get_client(), json.loads(state_json))
        return run.verdict(min_count=min_count, tolerance=tolerance)
