"""#32 dry-run 不得让业务节点在 console 进程内真实执行副作用。

历史：``dry_run`` 只用名黑名单拦 code / python / javascript / js；经
``PLAITA_CONSOLE_NODE_MODULES`` 注册的业务节点（gate / capture / agentrun）
不在列，且试跑未置 ``globalContext.dry_run``——这些节点各自支持的 dry_run
旗标永不触发，于是在 console backend 进程内真实 spawn 子进程 / 调 agent CLI。

本组断言 dry_run() 统一注入 ``globalContext.dry_run=True``：按契约自守的业务
节点（此处以内联 stub 复刻 gate/capture 的 ``get_global_variable("dry_run")``
判定）不执行任何副作用；流程显式声明 ``dry_run=false`` 也被强制覆盖。

对照（control）：同一 flow 直接经 ``FlowExecution().run`` 执行（不经试跑）时
探针确实执行——证明探针能区分「真跑」与「试跑」，断言非空转。
"""
import json
import sys
from pathlib import Path
from typing import Any, ClassVar, Optional

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import dryrun as dryrun_api  # noqa: E402
from services import dryrun as dryrun_svc  # noqa: E402
from services import flow_store  # noqa: E402

from plaita.core.executor import FlowExecution  # noqa: E402
from plaita.core.flow import Flow  # noqa: E402
from plaita.node import Node, get_default_registry  # noqa: E402

_PROBE_TYPE = "issue32_side_effect_probe"


class _SideEffectProbe(Node):
    """复刻 gate/capture 的 dry-run 契约：``$GLOBAL.dry_run`` 为真则不执行。

    真实分支写哨兵文件（等价于 spawn 子进程的副作用），供测试断言「未执行」。
    """

    node_type: ClassVar[str] = _PROBE_TYPE
    node_name: ClassVar[str] = "issue32 副作用探针"

    sentinel: Optional[str] = None

    def execute(self, execution: Any) -> dict:
        dry = bool(execution.get_global_variable("dry_run", False))
        marker = execution.get_global_variable("marker", None)
        if dry:
            return {"dry_run": True, "executed": False, "marker": marker}
        Path(str(self.sentinel)).write_text("executed", encoding="utf-8")
        return {"dry_run": False, "executed": True, "marker": marker}


@pytest.fixture()
def probe_registered():
    reg = get_default_registry()
    reg.register(_SideEffectProbe)
    yield
    reg.unregister(_PROBE_TYPE)


@pytest.fixture()
def client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path / 'issue32.db'}")
    app = FastAPI()
    app.include_router(dryrun_api.router, prefix="/api")
    return TestClient(app)


def _flow_json(sentinel: Path, *, global_context: Optional[dict] = None) -> str:
    data = {
        "flow_id": "issue32",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "probe"},
            {"type": _PROBE_TYPE, "id": "probe", "sentinel": str(sentinel), "next": "end"},
            {"type": "end", "id": "end", "output": "$NODE.probe", "resultType": "success"},
        ],
    }
    if global_context is not None:
        data["globalContext"] = global_context
    return json.dumps(data)


def test_control_probe_executes_without_dry_run(tmp_path, probe_registered):
    """对照：不经试跑的直跑确实执行（探针有效，非恒真/恒假的空转断言）。"""
    sentinel = tmp_path / "control.sentinel"
    flow = Flow.model_validate(json.loads(_flow_json(sentinel)))
    out = FlowExecution().run(flow, params={})
    assert out == {"dry_run": False, "executed": True, "marker": None}
    assert sentinel.read_text(encoding="utf-8") == "executed"


def test_business_node_self_guards_on_injected_global_dry_run(tmp_path, probe_registered, client):
    """editor 试跑含副作用节点的 flow：命令（哨兵）未被执行，节点走 dry 分支。"""
    sentinel = tmp_path / "probe.sentinel"
    r = client.post("/api/flows/dry-run", json={"flowJson": _flow_json(sentinel), "input": {}})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["error"] is None, body["error"]
    assert body["result"] == {"dry_run": True, "executed": False, "marker": None}
    assert not sentinel.exists(), "试跑下副作用节点不得真实执行"


def test_flow_declared_dry_run_false_is_force_overridden(tmp_path, probe_registered):
    """流程显式声明 globalContext.dry_run=false 也强制覆盖：试跑语义下无副作用。"""
    sentinel = tmp_path / "override.sentinel"
    out = dryrun_svc.dry_run(
        _flow_json(sentinel, global_context={"dry_run": False, "marker": "keep"}),
        {},
    )
    assert out["error"] is None, out["error"]
    assert out["result"] == {"dry_run": True, "executed": False, "marker": "keep"}
    assert not sentinel.exists()


def test_injection_preserves_other_global_keys(tmp_path, probe_registered):
    """注入只补 dry_run，不吞流程已有的其它 globalContext 键。"""
    sentinel = tmp_path / "preserve.sentinel"
    out = dryrun_svc.dry_run(_flow_json(sentinel, global_context={"marker": "kept"}), {})
    assert out["error"] is None, out["error"]
    assert out["result"]["marker"] == "kept"


def test_child_flow_declaring_dry_run_false_cannot_rearm_side_effects(tmp_path, probe_registered):
    """历史绕过面：子流程自带 ``globalContext.dry_run=false`` 时，子级 context
    优先读自己的 $GLOBAL（缺失才回退父级）——只注入根流程的旗标会被盖掉，副作用
    重新打开。改正：读点由 ``_DryRunExecution`` 钉死，声明层改不回去。"""
    sentinel = tmp_path / "child-override.sentinel"
    flow = {
        "flow_id": "issue32-child-override",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c1"},
            {
                "type": "child",
                "id": "c1",
                "childFlow": {
                    "globalContext": {"dry_run": False},
                    "nodes": [
                        {"type": "start", "id": "cs", "next": "probe"},
                        {"type": _PROBE_TYPE, "id": "probe", "sentinel": str(sentinel), "next": "ce"},
                        {"type": "end", "id": "ce", "output": "$NODE.probe", "resultType": "success"},
                    ],
                },
                "next": "end",
            },
            {"type": "end", "id": "end", "output": "$NODE.c1", "resultType": "success"},
        ],
    }
    out = dryrun_svc.dry_run(json.dumps(flow), {})
    assert out["error"] is None, out["error"]
    assert out["result"] == {"dry_run": True, "executed": False, "marker": None}
    assert not sentinel.exists(), "子流程声明 dry_run=false 不得重新打开副作用"


def test_branch_flow_declaring_dry_run_false_cannot_rearm_side_effects(tmp_path, probe_registered):
    """同上，parallel 分支流（thread 池里的子执行）同样被读点钉死覆盖。"""
    sentinel = tmp_path / "branch-override.sentinel"
    flow = {
        "flow_id": "issue32-branch-override",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "p"},
            {
                "type": "parallel",
                "id": "p",
                "branches": [
                    {"name": "b1", "flow": {
                        "globalContext": {"dry_run": False},
                        "nodes": [
                            {"type": "start", "id": "b1s", "next": "probe"},
                            {"type": _PROBE_TYPE, "id": "probe", "sentinel": str(sentinel), "next": "b1e"},
                            {"type": "end", "id": "b1e", "output": "$NODE.probe", "resultType": "success"},
                        ],
                    }},
                ],
                "next": "end",
            },
            {"type": "end", "id": "end", "output": "$NODE.p", "resultType": "success"},
        ],
    }
    out = dryrun_svc.dry_run(json.dumps(flow), {})
    assert out["error"] is None, out["error"]
    probe = next(n for n in out["nodes"] if n["id"] == "probe")
    assert probe["output"] == {"dry_run": True, "executed": False, "marker": None}
    assert not sentinel.exists(), "分支流声明 dry_run=false 不得重新打开副作用"


def test_probe_inside_inline_child_flow_is_disarmed(tmp_path, probe_registered):
    """注入随执行上下文下发子流程：inline child 内的副作用节点同样不执行
    （子级 ExecutionContext.get_global_variable 回退父级）。"""
    sentinel = tmp_path / "child.sentinel"
    flow = {
        "flow_id": "issue32-child",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "c1"},
            {
                "type": "child",
                "id": "c1",
                "childFlow": {"nodes": [
                    {"type": "start", "id": "cs", "next": "probe"},
                    {"type": _PROBE_TYPE, "id": "probe", "sentinel": str(sentinel), "next": "ce"},
                    {"type": "end", "id": "ce", "output": "$NODE.probe", "resultType": "success"},
                ]},
                "next": "end",
            },
            {"type": "end", "id": "end", "output": "$NODE.c1", "resultType": "success"},
        ],
    }
    out = dryrun_svc.dry_run(json.dumps(flow), {})
    assert out["error"] is None, out["error"]
    assert out["result"] == {"dry_run": True, "executed": False, "marker": None}
    assert not sentinel.exists()
