"""High-level ops over the console data plane — the supervisor's vocabulary.

These functions turn raw console responses into the answers the supervisor
loop (and its MCP tools) actually asks: which versions exist, what changed
between two of them, how is a flow performing, and what is the next version
number to save under.
"""

from __future__ import annotations

import difflib
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from plaita_ai.console_client import ConsoleClient, ConsoleClientError

_SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")

# Statuses that count as a success for flow_metrics purposes.
SUCCESS_STATUSES = frozenset({"completed", "success", "succeeded"})


# -- version helpers ---------------------------------------------------------


def parse_semver(version: str) -> Optional[Tuple[int, int, int]]:
    m = _SEMVER.match(str(version or "").strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def next_patch_version(version: str) -> str:
    """Bump the patch segment of a semver string ("1.2.3" -> "1.2.4")."""
    parsed = parse_semver(version)
    if parsed is None:
        raise ConsoleClientError(f"version {version!r} is not semver (X.Y.Z)")
    return f"{parsed[0]}.{parsed[1]}.{parsed[2] + 1}"


def latest_version(versions: List[Dict[str, Any]]) -> Optional[str]:
    """Highest semver among version dicts (each carrying a "version" key)."""
    best: Optional[Tuple[Tuple[int, int, int], str]] = None
    for entry in versions:
        parsed = parse_semver(str(entry.get("version", "")))
        if parsed is None:
            continue
        key = parsed
        if best is None or key > best[0]:
            best = (key, str(entry["version"]))
    return best[1] if best else None


def published_version(versions: List[Dict[str, Any]]) -> Optional[str]:
    """The current production version: the highest-semver entry carrying a
    published status. The console keeps status="published" on *every* version
    that has ever been published (no current pointer), so recency by semver is
    the resolution convention."""
    published = [
        (parse_semver(str(v.get("version", ""))), str(v.get("version")))
        for v in versions
        if str(v.get("status", "")).lower() in ("published", "production", "active")
    ]
    parsed = [(key, ver) for key, ver in published if key is not None]
    if not parsed:
        return None
    return max(parsed, key=lambda pair: pair[0])[1]


def flow_versions(client: ConsoleClient, flow_id: str) -> Dict[str, Any]:
    """All versions of a flow, semver-sorted, with the published one flagged."""
    detail = client.get_flow(flow_id)
    versions = list(detail.get("versions") or [])
    parsed = [v for v in versions if parse_semver(str(v.get("version", "")))]
    parsed.sort(key=lambda v: parse_semver(str(v["version"])) or (0, 0, 0))
    published = published_version(versions)
    for v in parsed:
        v["is_published"] = str(v.get("version")) == published
    return {
        "flow_id": flow_id,
        "latest": latest_version(versions),
        "published": published,
        "versions": parsed,
    }


# -- diff --------------------------------------------------------------------


def version_diff(
    client: ConsoleClient, flow_id: str, base_version: str, candidate_version: str
) -> Dict[str, Any]:
    """Unified text diff between two version definitions (JSON strings)."""
    base = client.get_version(flow_id, base_version).get("definition") or ""
    candidate = client.get_version(flow_id, candidate_version).get("definition") or ""
    diff_lines = list(
        difflib.unified_diff(
            base.splitlines(),
            candidate.splitlines(),
            fromfile=f"{flow_id}@{base_version}",
            tofile=f"{flow_id}@{candidate_version}",
            lineterm="",
        )
    )
    return {
        "flow_id": flow_id,
        "base": base_version,
        "candidate": candidate_version,
        "changed": bool(diff_lines),
        "unified_diff": "\n".join(diff_lines),
    }


# -- metrics -----------------------------------------------------------------


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _duration_s(execution: Dict[str, Any]) -> Optional[float]:
    start = _parse_dt(execution.get("start_time"))
    end = _parse_dt(execution.get("end_time"))
    if start is None or end is None:
        return None
    return max((end - start).total_seconds(), 0.0)


def flow_metrics(client: ConsoleClient, flow_id: str, limit: int = 50) -> Dict[str, Any]:
    """Aggregate the most recent executions of one flow into health numbers."""
    page = client.list_executions(flow_id=flow_id, page=1, size=max(1, min(limit, 200)))
    executions = list(page.get("executions") or [])

    by_status: Dict[str, int] = {}
    durations: List[float] = []
    recent_failures: List[Dict[str, Any]] = []
    for ex in executions:
        status = str(ex.get("status") or "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        duration = _duration_s(ex)
        if duration is not None:
            durations.append(duration)
        if status in ("failed", "error") and len(recent_failures) < 5:
            recent_failures.append(
                {
                    "execution_id": ex.get("execution_id"),
                    "error": ex.get("error"),
                    "end_time": ex.get("end_time"),
                }
            )

    total = len(executions)
    successes = sum(count for status, count in by_status.items() if status in SUCCESS_STATUSES)
    return {
        "flow_id": flow_id,
        "scanned": total,
        "by_status": by_status,
        "success_rate": round(successes / total, 4) if total else None,
        "avg_duration_s": round(sum(durations) / len(durations), 3) if durations else None,
        "recent_failures": recent_failures,
    }
