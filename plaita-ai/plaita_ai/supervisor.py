"""The supervisor loop: observe a flow's eval baseline, propose an improved
version, evaluate the candidate, and hand out a promote decision.

Deliberately a *structured* control loop, not a free agent: every iteration
is (propose → save as next patch version → evaluate → compare → gate), the
promote gate defaults to manual (the loop returns a promotion ticket; a human
publishes), and a policy caps iterations, regression tolerance, and
consecutive failures. The loop is the same self-correction philosophy as
plaita-ai's codegen path — generator, checker, retry — with the checker
being the evaluation set instead of the compiler.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

from plaita_ai.console_client import ConsoleClient, ConsoleClientError
from plaita_ai.evals import Dataset, compare, evaluate
from plaita_ai.ops import latest_version, next_patch_version, published_version


# -- policy ------------------------------------------------------------------


@dataclass
class SupervisorPolicy:
    """Caps and gates for one supervising run. Conservative by default."""

    max_iterations: int = 5
    #: stop early once avg_score reaches this
    target_score: float = 0.95
    #: an iteration must improve avg_score by at least this to count as improved
    min_improvement: float = 0.02
    #: N consecutive failed iterations pause the loop
    max_consecutive_failures: int = 2
    #: "manual" (default) returns a promotion ticket; "auto" publishes directly
    promote_gate: str = "manual"
    created_by: str = "plaita-ai-supervisor"
    eval_mode: str = "dry-run"
    eval_timeout_s: float = 120.0

    def __post_init__(self) -> None:
        if self.promote_gate not in ("manual", "auto"):
            raise ValueError("promote_gate must be 'manual' or 'auto'")


# -- proposers ---------------------------------------------------------------


class Proposer(Protocol):
    """Produces one candidate definition per call; dict with definition+rationale."""

    def propose(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:  # pragma: no cover
        ...


@dataclass
class StaticProposer:
    """Scripted candidates, popped in order — tests, demos, and human-in-the-loop runs."""

    candidates: List[Dict[str, Any]] = field(default_factory=list)
    _cursor: int = 0

    def propose(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self._cursor >= len(self.candidates):
            return None
        candidate = self.candidates[self._cursor]
        self._cursor += 1
        return candidate


class PromptProposer:
    """Proposer backed by an OpenAI-compatible chat endpoint.

    Configured via env (PLAITA_AI_PROPOSER_BASE_URL / _MODEL / _API_KEY).
    The prompt carries the current definition, the baseline report, and the
    dataset summary; the reply must contain a JSON object with
    ``definition`` and ``rationale``.
    """

    def __init__(self, env: Optional[Dict[str, str]] = None):
        env = dict(os.environ if env is None else env)
        self.base_url = (env.get("PLAITA_AI_PROPOSER_BASE_URL") or "").rstrip("/")
        self.model = env.get("PLAITA_AI_PROPOSER_MODEL") or ""
        self.api_key = env.get("PLAITA_AI_PROPOSER_API_KEY", "")
        if not (self.base_url and self.model):
            raise ConsoleClientError(
                "PromptProposer needs PLAITA_AI_PROPOSER_BASE_URL and PLAITA_AI_PROPOSER_MODEL."
            )

    def propose(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        import httpx

        system = (
            "You are a workflow improvement proposer for the Plaita flow engine. "
            "You receive the current flow definition (JSON), its baseline evaluation "
            "report, and failing cases. Return ONE JSON object only: "
            '{"definition": "<the full improved flow definition JSON string>", '
            '"rationale": "<what you changed and why>"}. '
            "Fix the failing cases without regressing passing ones; change as little as possible."
        )
        user = json.dumps(
            {
                "flow_id": context.get("flow_id"),
                "current_version": context.get("baseline_version"),
                "current_definition": context.get("current_definition"),
                "baseline_report": context.get("baseline"),
                "dataset": context.get("dataset"),
            },
            ensure_ascii=False,
            default=str,
        )
        resp = httpx.post(
            f"{self.base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": self.model,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "temperature": 0.2,
            },
            timeout=120.0,
        )
        resp.raise_for_status()
        content = str(resp.json()["choices"][0]["message"]["content"])
        return self._parse(content)

    @staticmethod
    def _parse(content: str) -> Optional[Dict[str, Any]]:
        try:
            parsed = json.loads(content)
        except ValueError:
            start, end = content.find("{"), content.rfind("}")
            if start < 0 or end <= start:
                return None
            try:
                parsed = json.loads(content[start : end + 1])
            except ValueError:
                return None
        if not isinstance(parsed, dict) or not parsed.get("definition"):
            return None
        return {
            "definition": str(parsed["definition"]),
            "rationale": str(parsed.get("rationale", ""))[:500],
        }


# -- the loop ------------------------------------------------------------------


class Supervisor:
    """One supervising session over one flow, backed by a console client."""

    def __init__(
        self,
        client: ConsoleClient,
        policy: Optional[SupervisorPolicy] = None,
        proposer: Optional[Proposer] = None,
    ):
        self.client = client
        self.policy = policy or SupervisorPolicy()
        self.proposer = proposer
        self._baselines: Dict[str, Dict[str, Any]] = {}

    # -- pieces ------------------------------------------------------------

    def baseline(self, flow_id: str, dataset: Dataset) -> Dict[str, Any]:
        """Evaluate the published version once and cache it for the session."""
        cache_key = f"{flow_id}:{dataset.source}"
        if cache_key not in self._baselines:
            try:
                detail = self.client.get_flow(flow_id)
            except ConsoleClientError as exc:
                if exc.status == 404:
                    raise ConsoleClientError(f"flow {flow_id!r} not found in the console") from exc
                raise
            versions = list(detail.get("versions") or [])
            published = published_version(versions) or latest_version(versions)
            if not published:
                raise ConsoleClientError(
                    f"flow {flow_id!r} has no versions to baseline against"
                )
            self._baselines[cache_key] = evaluate(
                self.client,
                flow_id,
                published,
                dataset,
                mode=self.policy.eval_mode,
                timeout_s=self.policy.eval_timeout_s,
            )
        return self._baselines[cache_key]

    def iterate(self, flow_id: str, dataset: Dataset) -> Dict[str, Any]:
        """One full iteration; returns a JSON-able IterationResult dict."""
        baseline = self.baseline(flow_id, dataset)
        context = {
            "flow_id": flow_id,
            "baseline_version": baseline.get("version"),
            "current_definition": (self.client.get_version(flow_id, str(baseline.get("version")))
                                   .get("definition") or ""),
            "baseline": baseline,
            "dataset": dataset.to_dict(),
        }
        result: Dict[str, Any] = {
            "flow_id": flow_id,
            "baseline_version": baseline.get("version"),
            "baseline_avg_score": baseline.get("avg_score"),
            "status": "no_proposal",
        }
        if self.proposer is None:
            result["reason"] = "no proposer configured"
            return result
        proposal = self.proposer.propose(context)
        if not proposal or not proposal.get("definition"):
            result["reason"] = "proposer returned no candidate"
            return result
        return self.evaluate_proposal(flow_id, dataset, proposal, baseline=baseline)

    def evaluate_proposal(
        self,
        flow_id: str,
        dataset: Dataset,
        proposal: Dict[str, Any],
        baseline: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Save one proposal as the next patch version, evaluate it against the
        dataset, compare with the baseline, and apply the promote gate. This is
        the reusable half of :meth:`iterate` (the flow template calls it
        directly, with the proposal produced upstream)."""
        policy = self.policy
        baseline = baseline or self.baseline(flow_id, dataset)
        result: Dict[str, Any] = {
            "flow_id": flow_id,
            "baseline_version": baseline.get("version"),
            "baseline_avg_score": baseline.get("avg_score"),
        }

        # Save as the next patch version so humans can see the lineage.
        detail = self.client.get_flow(flow_id)
        current_latest = latest_version(list(detail.get("versions") or []))
        if not current_latest:
            raise ConsoleClientError(f"flow {flow_id!r} has no versions")
        next_version = next_patch_version(current_latest)
        self.client.save_version(
            flow_id, next_version, str(proposal["definition"]), created_by=policy.created_by
        )
        result["candidate_version"] = next_version
        result["rationale"] = proposal.get("rationale", "")

        candidate = evaluate(
            self.client,
            flow_id,
            next_version,
            dataset,
            mode=policy.eval_mode,
            timeout_s=policy.eval_timeout_s,
        )
        verdict = compare(baseline, candidate)
        result["candidate_report"] = candidate
        result["comparison"] = verdict

        base_avg = baseline.get("avg_score")
        cand_avg = candidate.get("avg_score")
        failed_hard = cand_avg is None  # whole eval crashed → counts as a failure
        improved = (
            not failed_hard
            and base_avg is not None
            and cand_avg is not None
            and cand_avg >= base_avg + policy.min_improvement
            and not verdict.get("regressions")
        )
        if improved:
            result["status"] = "improved"
            # Refresh the cached baseline: the candidate is the new reference.
            self._baselines[f"{flow_id}:{dataset.source}"] = candidate
        elif failed_hard:
            result["status"] = "failed_iteration"
            return result
        else:
            result["status"] = "no_improvement"
            return result

        # Promote gate.
        ticket = {
            "flow_id": flow_id,
            "version": next_version,
            "baseline_version": baseline.get("version"),
            "avg_score": {"base": base_avg, "candidate": cand_avg},
            "reason": result["rationale"],
            "command": f"POST /api/flows/{flow_id}/publish {{\"version\": \"{next_version}\"}}",
        }
        if policy.promote_gate == "auto":
            self.client.publish_version(flow_id, next_version)
            result["promoted"] = True
        else:
            result["promoted"] = False
            result["promotion_ticket"] = ticket
        return result

    def run_loop(self, flow_id: str, dataset: Dataset) -> Dict[str, Any]:
        """Iterate until target, budget, pause, or proposal exhaustion."""
        iterations: List[Dict[str, Any]] = []
        consecutive_failures = 0
        final_status = "budget_exhausted"
        for i in range(1, self.policy.max_iterations + 1):
            outcome = self.iterate(flow_id, dataset)
            outcome["iteration"] = i
            iterations.append(outcome)
            status = outcome.get("status")
            if status == "failed_iteration":
                consecutive_failures += 1
                if consecutive_failures >= self.policy.max_consecutive_failures:
                    final_status = "paused"
                    break
                continue
            consecutive_failures = 0
            if status == "no_proposal":
                final_status = "no_proposal"
                break
            cand_avg = outcome.get("candidate_report", {}).get("avg_score")
            if status == "improved" and cand_avg is not None and cand_avg >= self.policy.target_score:
                final_status = "target_reached"
                break
        return {
            "flow_id": flow_id,
            "final_status": final_status,
            "iterations": iterations,
            "iterations_run": len(iterations),
        }
