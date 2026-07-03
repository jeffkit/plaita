"""Locate and load ``agent-benchmark/tasks.py`` without ``sys.path`` mutation.

``agent-benchmark`` is a sibling directory of ``plaita-ai`` (not an installable
package; its name contains a hyphen so it cannot be imported as a normal
module). We load ``tasks.py`` explicitly via ``importlib`` so the harness and
conftest no longer ``sys.path.insert`` a relative path at import time — that
was fragile (depends on cwd, pollutes global import state, breaks IDE type
resolution). ``tasks.py`` is self-contained (only imports ``typing``).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

_BENCH_TASKS = Path(__file__).resolve().parents[3] / "agent-benchmark" / "tasks.py"

_TASKS_MODULE: Any = None


def _load_tasks_module() -> Any:
    global _TASKS_MODULE
    if _TASKS_MODULE is not None:
        return _TASKS_MODULE
    if not _BENCH_TASKS.exists():
        raise RuntimeError(f"找不到 benchmark 任务集: {_BENCH_TASKS}")
    spec = importlib.util.spec_from_file_location("agent_benchmark_tasks", _BENCH_TASKS)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法为 {_BENCH_TASKS} 构造 import spec")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _TASKS_MODULE = mod
    return mod


def get_tasks() -> Any:
    return _load_tasks_module().TASKS


def get_validators() -> Any:
    return _load_tasks_module().VALIDATORS
