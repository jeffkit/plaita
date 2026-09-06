"""FoT Agent × benchmark tasks (LLM integration, deepseek-v4-flash)."""

from __future__ import annotations

import os

import pytest
pytest.importorskip("langchain", reason="langchain extra not installed: pip install 'plaita-ai[agent]'")

from tests.llm.harness import build_model, run_fot_task, select_tasks

pytestmark = pytest.mark.llm

# Subset for a fast, representative LLM smoke gate. Full run via --difficulty.
_DEFAULT_IDS = ["cond-grade", "str-greet", "map-double", "filter-evens", "router-intent"]


def _tasks():
    ids = set(_DEFAULT_IDS)
    return [t for t in select_tasks() if t["id"] in ids]


@pytest.fixture(scope="module")
def model():
    return build_model()


@pytest.mark.parametrize("task", _tasks(), ids=lambda t: t["id"])
def test_fot_task(task, model):
    score = run_fot_task(task, model)
    # 机械门槛：FoT 必须产出一个可编译可执行的 @flow（agent 闭环跑通）。
    assert score["source"], _fail_msg(score)
    # 质量门槛：默认多数用例通过即可（LLM 有抖动）；PLAITA_LLM_STRICT=1 要求全过。
    threshold = 1.0 if os.environ.get("PLAITA_LLM_STRICT") else 0.6
    assert score["pass_rate"] >= threshold, _fail_msg(score)


def _fail_msg(score) -> str:
    fails = [c for c in score["cases"] if not c["pass"]]
    lines = [f"FoT {score['task_id']}: {score['passed']}/{score['total']}"]
    if score.get("first_error"):
        lines.append(f"first run_error: {score['first_error']}")
    if score.get("compile_errors"):
        lines.append(f"compile_errors: {score['compile_errors']}")
    for c in fails[:3]:
        lines.append(f"  in={c['input']} exp={c['expected']!r} got={c['actual']!r} err={c['run_error']}")
    return "\n".join(lines)
