"""Canary / shadow traffic splitting between two versions of one flow.

Two modes over the console data plane:

- **split** — real executions are routed between the baseline (published)
  and the candidate version by a sticky hash of a key field, so a given
  customer always lands on the same arm. Both arms are real.
- **shadow** — production traffic is untouched: every invocation runs the
  baseline for real (its result goes back to the caller) while the candidate
  receives the same input as an in-process console dry-run. Zero-risk
  pre-promote verification on live inputs.

Both arms' outcomes are recorded per invocation; ``report()`` aggregates
them into the promote/rollback view. The run state is plain JSON-able
(``to_dict`` / ``from_dict``) so a canary can outlive one process.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from plaita_ai.console_client import ConsoleClient, ConsoleClientError

SUCCESS_STATUSES = frozenset({"completed", "success", "succeeded"})


@dataclass
class CanaryPolicy:
    mode: str = "split"                      # "split" | "shadow"
    ratio: float = 0.1                       # candidate share (split mode)
    sticky: bool = True                      # same key → same arm
    key_field: str = "canary_key"            # input field used as the sticky key
    execution_timeout_s: float = 120.0
    # How the shadow baseline runs: a real execution needs a working queue
    # (Redis + worker) behind the console; consoles without one fall back to
    # an in-process dry-run for the baseline arm too (pure-compare mode).
    baseline_mode: str = "execution"

    def __post_init__(self) -> None:
        if self.mode not in ("split", "shadow"):
            raise ValueError("mode must be 'split' or 'shadow'")
        if self.baseline_mode not in ("execution", "dry-run"):
            raise ValueError("baseline_mode must be 'execution' or 'dry-run'")
        if not 0.0 <= self.ratio <= 1.0:
            raise ValueError("ratio must be within [0, 1]")


def _bucket(key: str) -> float:
    """Stable [0, 1) bucket for a sticky key."""
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) / float(16 ** 16)


class CanaryRun:
    """One canary engagement over one flow: route, invoke, record, report."""

    def __init__(
        self,
        client: ConsoleClient,
        flow_id: str,
        candidate_version: str,
        baseline_version: Optional[str] = None,
        policy: Optional[CanaryPolicy] = None,
    ):
        self.client = client
        self.flow_id = flow_id
        self.candidate_version = candidate_version
        self.policy = policy or CanaryPolicy()
        self.records: List[Dict[str, Any]] = []
        if baseline_version is None:
            detail = client.get_flow(flow_id)
            published = [v for v in detail.get("versions", []) if str(v.get("status")) == "published"]
            if not published:
                raise ConsoleClientError(f"flow {flow_id!r} has no published version to protect")
            self.baseline_version = str(published[-1]["version"])
        else:
            self.baseline_version = baseline_version
        # candidate definition cache for shadow dry-runs
        self._candidate_definition: Optional[str] = None

    # -- state ------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "flow_id": self.flow_id,
            "baseline_version": self.baseline_version,
            "candidate_version": self.candidate_version,
            "policy": {"mode": self.policy.mode, "ratio": self.policy.ratio, "sticky": self.policy.sticky,
                       "key_field": self.policy.key_field},
            "records": self.records,
        }

    @classmethod
    def from_dict(cls, client: ConsoleClient, state: Dict[str, Any]) -> "CanaryRun":
        policy = CanaryPolicy(
            mode=state.get("policy", {}).get("mode", "split"),
            ratio=float(state.get("policy", {}).get("ratio", 0.1)),
            sticky=bool(state.get("policy", {}).get("sticky", True)),
            key_field=str(state.get("policy", {}).get("key_field", "canary_key")),
        )
        run = cls(client, state["flow_id"], state["candidate_version"],
                  baseline_version=state.get("baseline_version"), policy=policy)
        run.records = list(state.get("records") or [])
        return run

    # -- routing ------------------------------------------------------------

    def route(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Decide the arm for one invocation (split mode)."""
        if self.policy.mode != "split":
            return {"arm": "baseline", "version": self.baseline_version}
        key = str(params.get(self.policy.key_field, ""))
        if self.policy.sticky and key:
            share = _bucket(key)
        else:
            share = _bucket(f"{time.monotonic_ns()}")
        arm = "candidate" if share < self.policy.ratio else "baseline"
        version = self.candidate_version if arm == "candidate" else self.baseline_version
        return {"arm": arm, "version": version}

    # -- invocation ----------------------------------------------------------

    def invoke(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Run one invocation according to the policy and record both arms.

        Returns the caller-facing result: the chosen arm's execution output
        (split), or the baseline output annotated with the shadow outcome
        (shadow).
        """
        if self.policy.mode == "split":
            routed = self.route(params)
            outcome = self._execute(routed["version"], params)
            self._record(routed["arm"], params, outcome)
            return {**outcome, "canary": {"arm": routed["arm"], "version": routed["version"]}}

        # shadow: baseline per policy, candidate always in-process
        if self.policy.baseline_mode == "execution":
            base = self._execute(self.baseline_version, params)
        else:
            base = self._dry_execute(self.baseline_version, params)
        self._record("baseline", params, base)
        shadow: Dict[str, Any] = {}
        try:
            if self._candidate_definition is None:
                self._candidate_definition = self.client.get_version(
                    self.flow_id, self.candidate_version
                ).get("definition") or ""
            dry = self.client.dry_run(self._candidate_definition, input=params)
            shadow = {
                "ok": not dry.get("error"),
                "output": dry.get("result"),
                "error": dry.get("error"),
            }
        except ConsoleClientError as exc:
            shadow = {"ok": False, "output": None, "error": str(exc)}
        self._record("candidate", params, shadow)
        return {
            **base,
            "canary": {
                "arm": "baseline",
                "version": self.baseline_version,
                "shadow_version": self.candidate_version,
                "shadow": shadow,
            },
        }

    # -- reporting ------------------------------------------------------------

    def report(self) -> Dict[str, Any]:
        """Aggregate both arms into the promote/rollback view."""

        def aggregate(arm: str) -> Dict[str, Any]:
            rows = [r for r in self.records if r["arm"] == arm]
            durations = [r["duration_s"] for r in rows if r.get("duration_s") is not None]
            successes = sum(1 for r in rows if r.get("ok"))
            return {
                "count": len(rows),
                "success_rate": round(successes / len(rows), 4) if rows else None,
                "avg_duration_s": round(sum(durations) / len(durations), 3) if durations else None,
            }

        base, cand = aggregate("baseline"), aggregate("candidate")
        deltas: Dict[str, Any] = {}
        if base["success_rate"] is not None and cand["success_rate"] is not None:
            deltas["success_rate_delta"] = round(cand["success_rate"] - base["success_rate"], 4)
        if base["avg_duration_s"] is not None and cand["avg_duration_s"] is not None:
            deltas["avg_duration_s_delta"] = round(cand["avg_duration_s"] - base["avg_duration_s"], 3)
        return {
            "flow_id": self.flow_id,
            "mode": self.policy.mode,
            "baseline_version": self.baseline_version,
            "candidate_version": self.candidate_version,
            "baseline": base,
            "candidate": cand,
            "deltas": deltas,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def verdict(self, min_count: int = 10, tolerance: float = 0.02) -> Dict[str, Any]:
        """Cheap promote/rollback hint from the aggregated arms."""
        rep = self.report()
        base, cand = rep["baseline"], rep["candidate"]
        if not base["count"] or not cand["count"] or base["count"] + cand["count"] < min_count:
            return {"recommendation": "keep_running", "reason": "not enough traffic yet"}
        if cand["success_rate"] is None:
            return {"recommendation": "rollback", "reason": "candidate produced no successful outcome"}
        if base["success_rate"] is not None and cand["success_rate"] < base["success_rate"] - tolerance:
            return {"recommendation": "rollback", "reason": "candidate success rate below baseline tolerance"}
        return {
            "recommendation": "promote",
            "reason": "candidate holds within tolerance on live traffic",
            "promotion_command": f"POST /api/flows/{self.flow_id}/publish "
                                 f'{{"version": "{self.candidate_version}"}}',
        }

    # -- internals --------------------------------------------------------------

    def _dry_execute(self, version: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Run one arm as an in-process console dry-run (no queue needed)."""
        started = time.monotonic()
        definition = self.client.get_version(self.flow_id, version).get("definition") or ""
        dry = self.client.dry_run(definition, input=params)
        return {
            "ok": not dry.get("error"),
            "status": "completed" if not dry.get("error") else "failed",
            "output": dry.get("result"),
            "error": dry.get("error"),
            "duration_s": round(time.monotonic() - started, 3),
        }

    def _execute(self, version: str, params: Dict[str, Any]) -> Dict[str, Any]:
        started = time.monotonic()
        started_at = self.client.start_execution(self.flow_id, version=version, params=params)
        execution_id = str(started_at.get("execution_id") or "")
        if not execution_id:
            raise ConsoleClientError("start_execution returned no execution_id")
        done = self.client.wait_execution(execution_id, timeout_s=self.policy.execution_timeout_s)
        duration = round(time.monotonic() - started, 3)
        ok = str(done.get("status")) in SUCCESS_STATUSES
        return {
            "ok": ok,
            "status": done.get("status"),
            "output": done.get("output"),
            "error": done.get("error"),
            "execution_id": execution_id,
            "duration_s": duration,
        }

    def _record(self, arm: str, params: Dict[str, Any], outcome: Dict[str, Any]) -> None:
        self.records.append(
            {
                "arm": arm,
                "ok": bool(outcome.get("ok")),
                "duration_s": outcome.get("duration_s"),
                "at": datetime.now(timezone.utc).isoformat(),
                "input": params,
            }
        )


def shadow_once(
    client: ConsoleClient,
    flow_id: str,
    candidate_version: str,
    params: Dict[str, Any],
    baseline_version: Optional[str] = None,
    baseline_mode: str = "execution",
) -> Dict[str, Any]:
    """Stateless one-input shadow check: run the baseline for real, dry-run the
    candidate on the same input, and return both side by side."""
    run = CanaryRun(client, flow_id, candidate_version, baseline_version=baseline_version,
                    policy=CanaryPolicy(mode="shadow", baseline_mode=baseline_mode))
    out = run.invoke(params)
    return {
        "flow_id": flow_id,
        "baseline_version": run.baseline_version,
        "candidate_version": candidate_version,
        "baseline": {"ok": out.get("ok"), "output": out.get("output"), "execution_id": out.get("execution_id")},
        "shadow": out.get("canary", {}).get("shadow", {}),
    }
