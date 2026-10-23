"""plaita-ai 全仓解析守卫统一收口（A2R51-02/A2R52-05/A2R52-06/A2R53-04/R49-03）。

负控判据（A2 r53 台账）：对每个解析点喂 ~12k 层嵌套串（~24KB）——修后必须
单点具名降级（GuardedJsonError / ok:false 信封 / unavailable case），任何
异常不得穿出工具体或入口。judges.py 三段 ``except Exception`` 是已达标参照
形（A2R45-02/A2R46-01），不在此重复覆盖。

每个缺陷面都带对照格：普通坏 JSON / 合法 JSON 的既有行为不变（守恒）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from plaita_ai.console_client import ConsoleClientError
from plaita_ai.evals import Dataset, evaluate
from plaita_ai.flow_runner import RunResult, result_json
from plaita_ai.json_guard import GuardedJsonError, guarded_json_loads

# ~12k 层嵌套数组 ≈ 24KB——远小于常见载荷上限，json.loads 必抛 RecursionError
# （CPython C scanner 递归上限 ~1000）。
DEEP_JSON = "[" * 12000 + "]" * 12000

SIMPLE_FLOW = """
@flow("t")
def t(INPUT):
    return 1
"""


def deep_struct(depth: int = 12000) -> dict:
    node: dict = {}
    cur = node
    for _ in range(depth):
        cur["n"] = {}
        cur = cur["n"]
    return node


# -- helper 单点 ---------------------------------------------------------------


def test_helper_valid_json_passes():
    assert guarded_json_loads('{"a": 1}', what="x") == {"a": 1}


def test_helper_bad_syntax_named():
    with pytest.raises(GuardedJsonError) as ei:
        guarded_json_loads("{oops", what="field_x")
    assert "field_x is not valid JSON" in str(ei.value)


def test_helper_deep_nesting_named_not_recursionerror():
    with pytest.raises(GuardedJsonError) as ei:
        guarded_json_loads(DEEP_JSON, what="field_x")
    assert "RecursionError" in str(ei.value)


def test_helper_non_str_input_named():
    with pytest.raises(GuardedJsonError):
        guarded_json_loads(123, what="field_x")


def test_helper_error_is_valueerror_subclass():
    # 迁移期守恒：既有 `except ValueError` 调用方仍能接住。
    assert issubclass(GuardedJsonError, ValueError)


# -- mcp/server.py flow_run 双面（A2R51-02 面1） ---------------------------------


def test_mcp_flow_run_deep_inputs_json_named_degrade():
    from plaita_ai.mcp.server import flow_run

    payload = json.loads(flow_run(SIMPLE_FLOW, inputs_json=DEEP_JSON))
    assert payload["ok"] is False
    assert payload["error_type"] == "GuardedJsonError"
    assert "inputs_json is not valid JSON" in payload["error"]


def test_mcp_flow_run_deep_globals_json_named_degrade():
    from plaita_ai.mcp.server import flow_run

    payload = json.loads(flow_run(SIMPLE_FLOW, globals_json=DEEP_JSON))
    assert payload["ok"] is False
    assert payload["error_type"] == "GuardedJsonError"
    assert "globals_json is not valid JSON" in payload["error"]


def test_mcp_flow_run_control_valid_inputs_still_runs():
    from plaita_ai.mcp.server import flow_run

    payload = json.loads(flow_run(SIMPLE_FLOW, inputs_json='{"a": 1}'))
    assert payload["ok"] is True


# -- mcp/server.py flow_from_json definition 面（A2R52-06 扩展面） ----------------


def test_mcp_flow_from_json_deep_definition_named_degrade():
    from plaita_ai.mcp.server import flow_from_json

    payload = json.loads(flow_from_json(DEEP_JSON))
    assert payload["ok"] is False
    assert "definition is not valid JSON" in payload["error"]
    assert "RecursionError" in payload["error"]


# -- flow_runner.result_json dumps 面（A2R51-02 面2） ----------------------------


def test_result_json_deep_result_named_envelope():
    from types import SimpleNamespace

    rr = SimpleNamespace(to_dict=lambda: {"ok": True, "result": deep_struct()})
    payload = json.loads(result_json(rr))
    assert payload["ok"] is False
    assert payload["error_type"] == "RecursionError"
    assert "not JSON-serializable" in payload["error"]


def test_result_json_circular_result_named_envelope():
    from types import SimpleNamespace

    circular: dict = {}
    circular["self"] = circular
    rr = SimpleNamespace(to_dict=lambda: {"ok": True, "result": circular})
    payload = json.loads(result_json(rr))
    assert payload["ok"] is False
    assert payload["error_type"] == "ValueError"


def test_result_json_control_normal_payload_unchanged():
    payload = json.loads(result_json(RunResult(ok=True, result=42)))
    assert payload == {"ok": True, "result": 42, "error": None, "error_type": None}


# -- agent/react/tools.py plaita_run_flow 双面（A2R52-05） ------------------------

def test_react_run_flow_deep_inputs_json_named_degrade():
    pytest.importorskip("langchain", reason="langchain extra not installed")
    from plaita_ai.agent.react.tools import build_plaita_tools

    (run_flow_tool,) = [
        t for t in build_plaita_tools() if t.name == "plaita_run_flow"
    ]
    payload = json.loads(run_flow_tool.func(SIMPLE_FLOW, inputs_json=DEEP_JSON))
    assert payload["ok"] is False
    assert payload["error_type"] == "GuardedJsonError"


def test_react_run_flow_deep_globals_json_named_degrade():
    pytest.importorskip("langchain", reason="langchain extra not installed")
    from plaita_ai.agent.react.tools import build_plaita_tools

    (run_flow_tool,) = [
        t for t in build_plaita_tools() if t.name == "plaita_run_flow"
    ]
    payload = json.loads(run_flow_tool.func(SIMPLE_FLOW, globals_json=DEEP_JSON))
    assert payload["ok"] is False
    assert payload["error_type"] == "GuardedJsonError"


# -- supervisor_flow.py：sup_propose / sup_evaluate（A2R52-05 / A2R53-04①） -------

class _StubClient:
    pass


class _StubDataset:
    def to_dict(self):
        return {}


_UNSET = object()


def _tools_module(proposer=_UNSET):
    from plaita_ai.supervisor import StaticProposer
    from plaita_ai.supervisor_flow import make_tools_module

    return make_tools_module(
        _StubClient(), _StubDataset(),
        proposer=StaticProposer([{"definition": "x"}]) if proposer is _UNSET else proposer,
    )


def test_sup_propose_deep_context_json_named_error_reply():
    mod = _tools_module()
    reply = json.loads(mod.sup_propose(context_json=DEEP_JSON))
    assert "error" in reply
    assert "context_json is not valid JSON" in reply["error"]


def test_sup_propose_control_bad_json_now_named_reply():
    mod = _tools_module()
    reply = json.loads(mod.sup_propose(context_json="{oops"))
    assert "error" in reply


def test_sup_propose_no_proposer_early_exit_before_parse():
    # A2R52-05 次序格：未配置 proposer 时不得先炸解析（原实现在解析之后早退）。
    mod = _tools_module(proposer=None)
    reply = json.loads(mod.sup_propose(context_json="{oops"))
    assert reply == {"error": "no proposer configured"}


def test_sup_propose_control_valid_context_flows_to_proposer():
    mod = _tools_module()
    reply = json.loads(mod.sup_propose(context_json='{"k": 1}'))
    assert reply == {"definition": "x"}


def test_sup_evaluate_deep_proposal_json_no_proposal():
    mod = _tools_module()
    verdict = json.loads(mod.sup_evaluate(flow_id="f", proposal_json=DEEP_JSON))
    assert verdict["status"] == "no_proposal"


def test_sup_evaluate_control_bad_json_no_proposal():
    mod = _tools_module()
    verdict = json.loads(mod.sup_evaluate(flow_id="f", proposal_json="{oops"))
    assert verdict["status"] == "no_proposal"


def test_run_supervisor_flow_deep_verdict_string_flow_error():
    # run_supervisor_flow 出口解析同族收口：深嵌套 verdict 串 → flow_error，
    # 原 (ValueError, TypeError) 窄口径对 RecursionError 失守。
    from plaita_ai import supervisor_flow as sf

    original = sf.run_flow
    sf.run_flow = lambda *a, **kw: RunResult(ok=True, result=DEEP_JSON)
    try:
        out = sf.run_supervisor_flow("f", tools=_tools_module())
    finally:
        sf.run_flow = original
    assert out["status"] == "flow_error"


# -- supervisor.py proposer 族三点（A2R53-04②③④） --------------------------------


def test_parse_legacy_json_deep_reply_none():
    from plaita_ai.supervisor import PromptProposer

    assert PromptProposer._parse_legacy_json(DEEP_JSON) is None


def test_parse_legacy_json_control_valid_reply():
    from plaita_ai.supervisor import PromptProposer

    out = PromptProposer._parse_legacy_json('{"definition": "d", "rationale": "r"}')
    assert out == {"definition": "d", "rationale": "r"}


def test_emit_current_source_deep_json_empty_string():
    from plaita_ai.supervisor import PromptProposer

    # docstring 承诺「任何失败都降级为空串」——深嵌套（RecursionError）也兑现。
    assert PromptProposer._emit_current_source(DEEP_JSON) == ""


def test_emit_current_source_deep_literal_empty_string():
    from plaita_ai.supervisor import PromptProposer

    deep_lit = "{" * 3000 + "}" * 3000  # json 腿死 → ast.literal_eval 兜底腿也深
    assert PromptProposer._emit_current_source(deep_lit) == ""


def test_emit_current_source_control_valid_definition_emits():
    from plaita_ai.supervisor import PromptProposer

    definition = json.dumps({
        "runtime": "python", "flow_id": "legacy", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "e"},
            {"type": "end", "id": "e", "output": "$INPUT.x", "resultType": "success"},
        ],
    })
    # JSON 腿与 dict 形态都走到 emit_source（守恒 test_supervisor.py 同款断言）
    assert "def legacy(INPUT):" in PromptProposer._emit_current_source(definition)
    assert "def legacy(INPUT):" in PromptProposer._emit_current_source(json.loads(definition))


def test_extract_rationale_deep_block_empty_string(monkeypatch):
    from plaita_ai.supervisor import FlowSourceProposer

    monkeypatch.setenv("PLAITA_AI_PROPOSER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("PLAITA_AI_PROPOSER_MODEL", "stub")
    monkeypatch.setenv("PLAITA_AI_PROPOSER_API_KEY", "stub")
    fp = FlowSourceProposer(flow_id="f")
    content = "```json\n" + ('{"a":' * 12000) + "1" + ("}" * 12000) + "\n```"
    assert fp._extract_rationale(content) == ""


# -- ops_mcp.py：6 解析点 + _guarded 兜底（A2R52-06） ------------------------------


class _StubMCP:
    def __init__(self):
        self.registered = {}

    def tool(self):
        def deco(fn):
            self.registered[fn.__name__] = fn
            return fn

        return deco


@pytest.fixture()
def ops_tools():
    from plaita_ai import ops_mcp

    stub = _StubMCP()
    ops_mcp.register(stub)
    yield stub.registered
    ops_mcp.set_client(None)


DEEP_OBJ = '{"a":' * 12000 + "1" + "}" * 12000


def test_ops_console_run_start_deep_params_named(ops_tools):
    payload = json.loads(ops_tools["console_run_start"](flow_id="f", params_json=DEEP_OBJ))
    assert payload["ok"] is False
    assert "params_json is not valid JSON" in payload["error"]
    assert "RecursionError" in payload["error"]


def test_ops_console_dry_run_deep_input_named(ops_tools):
    payload = json.loads(ops_tools["console_dry_run"](flow_json="{}", input_json=DEEP_OBJ))
    assert payload["ok"] is False
    assert "input_json is not valid JSON" in payload["error"]


def test_ops_eval_compare_deep_base_named(ops_tools):
    payload = json.loads(ops_tools["eval_compare"](base_json=DEEP_OBJ, candidate_json="{}"))
    assert payload["ok"] is False
    assert "base_json is not valid JSON" in payload["error"]


def test_ops_eval_compare_deep_candidate_named(ops_tools):
    payload = json.loads(ops_tools["eval_compare"](base_json="{}", candidate_json=DEEP_OBJ))
    assert payload["ok"] is False
    assert "candidate_json is not valid JSON" in payload["error"]


def test_ops_canary_shadow_once_deep_params_named(ops_tools):
    payload = json.loads(
        ops_tools["canary_shadow_once"](flow_id="f", candidate_version="v2", params_json=DEEP_OBJ)
    )
    assert payload["ok"] is False
    assert "params_json is not valid JSON" in payload["error"]


def test_ops_canary_verdict_deep_state_named(ops_tools):
    payload = json.loads(ops_tools["canary_verdict"](state_json=DEEP_OBJ))
    assert payload["ok"] is False
    assert "state_json is not valid JSON" in payload["error"]


def test_ops_guarded_widened_catches_recursionerror():
    # _guarded 兜底口径 = judges.py 的 except Exception：解析点之外的意外
    # 异常（含 RuntimeError 族）同样具名 ok:false，不穿出工具体。
    from plaita_ai import ops_mcp

    @ops_mcp._guarded
    def _raiser() -> str:
        raise RecursionError("boom")

    payload = json.loads(_raiser())
    assert payload["ok"] is False
    assert payload["error"] == "RecursionError: boom"


# -- evals.py：数据集文件面 + dry-run error 载荷面（清单行 11 / R49-03） ------------


def test_dataset_deep_file_named_console_error(tmp_path):
    file = tmp_path / "deep.json"
    file.write_text(DEEP_JSON, encoding="utf-8")
    with pytest.raises(ConsoleClientError) as ei:
        Dataset.load(str(file))
    assert "cannot read dataset file" in str(ei.value)
    assert "RecursionError" in str(ei.value)


def test_dataset_control_valid_file_loads(tmp_path):
    file = tmp_path / "ok.json"
    file.write_text(json.dumps({"cases": [{"id": "c1", "input": {}}]}), encoding="utf-8")
    assert [c.id for c in Dataset.load(str(file)).cases] == ["c1"]


class _DeepErrorConsole:
    """dry-run 返回候选自控的深嵌套 error 载荷（R49-03 面）。"""

    def get_version(self, flow_id, version):
        return {"definition": "{}"}

    def dry_run(self, definition, input):
        return {"error": deep_struct()}


def test_evaluate_dry_run_deep_error_payload_case_unavailable():
    report = evaluate(_DeepErrorConsole(), "f", "v1", Dataset([{"id": "c1", "input": {}}]))
    (entry,) = report["cases"]
    assert entry.get("unavailable") is True
    assert "RecursionError" in entry["error"]
    assert report["unavailable_cases"] == 1
    # 降级 case 剔出均值：pass_rate/avg_score 不因它产生伪判定
    assert report["pass_rate"] is None and report["avg_score"] is None


# -- cli/main.py _load_json_arg（cmd_run --input/--globals 顺带面） ----------------


def test_cli_load_json_arg_deep_named_exit():
    from plaita_ai.cli.main import _load_json_arg

    with pytest.raises(SystemExit) as ei:
        _load_json_arg(DEEP_JSON, {}, what="--input")
    assert "--input is not valid JSON" in str(ei.value)
    assert "RecursionError" in str(ei.value)


def test_cli_load_json_arg_control_valid_and_default():
    from plaita_ai.cli.main import _load_json_arg

    assert _load_json_arg('{"a": 1}', {}, what="--input") == {"a": 1}
    sentinel = {"default": True}
    assert _load_json_arg(None, sentinel, what="--input") is sentinel


def test_cli_cmd_run_deep_input_exits_named(tmp_path):
    """端到端：`plaita-ai run <flow> --input <deep>` 具名退出，非裸 traceback。"""
    from plaita_ai.cli.main import build_parser, cmd_run

    source = tmp_path / "flow.py"
    source.write_text(SIMPLE_FLOW, encoding="utf-8")
    args = build_parser().parse_args(["run", str(source), "--input", DEEP_JSON])
    with pytest.raises(SystemExit) as ei:
        cmd_run(args)
    assert "--input is not valid JSON" in str(ei.value)
