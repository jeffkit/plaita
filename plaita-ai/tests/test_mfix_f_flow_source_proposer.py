"""MF2: FlowSourceProposer — compile-gated @flow proposals, IR stored.

编排单轨口径：supervisor 的 LLM 提案改为 @flow 源码 → compile_flow 编译门
（错误回喂重试）→ 编译得 IR → definition 字段存 IR JSON 字符串。数据面契约
不变（console 的 definition 仍是 flow 定义 JSON，dry-run/evaluate 照常消费），
坏提案不再烧迭代和版本号。PromptProposer 保留为 legacy 显式可选。
"""

from __future__ import annotations

import json

import pytest
pytest.importorskip("langchain", reason="langchain extra not installed: pip install 'plaita-ai[agent]'")
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from plaita_ai import ops_mcp
from plaita_ai.console_client import ConsoleClient, ConsoleClientError, ConsoleConfig
from plaita_ai.evals import Dataset
from plaita_ai.supervisor import FlowSourceProposer, Supervisor, SupervisorPolicy

from ._console_fake import FakeConsole
from .test_ops_mcp import _FakeMCP, make_wired


# 提案：合法 @flow 源码（含可选 rationale JSON 块）
GOOD_RESPONSE = (
    "改进后的 flow：\n"
    "```python\n"
    '@flow("demo")\n'
    "def demo(INPUT):\n"
    '    return "good output"\n'
    "```\n"
    '```json\n{"rationale": "fix failing case"}\n```\n'
)

# 提案：f-string —— 能提取出 @flow 源码但编译必败（回喂路径）
BAD_RESPONSE = '''```python
@flow("demo")
def demo(INPUT):
    return f"hi {INPUT.name}"
```'''

PURE_NOISE = "这个流程我没法改进。"


class _Recording:
    """Duck-typed chat-model wrapper: counts calls and records prompts."""

    def __init__(self, inner):
        self.inner = inner
        self.calls = 0
        self.prompts = []

    def invoke(self, messages, *args, **kwargs):
        self.calls += 1
        self.prompts.append(messages)
        return self.inner.invoke(messages, *args, **kwargs)


def make_client(fake: FakeConsole) -> ConsoleClient:
    return ConsoleClient(
        ConsoleConfig(base_url="http://fake", admin_api_key="test-key"), transport=fake.transport()
    )


def make_dataset(cases: int = 2) -> Dataset:
    dataset = Dataset([{"id": f"c{i}", "input": {"i": i}} for i in range(cases)])  # type: ignore[list-item]
    for case in dataset.cases:
        case.expect = {"contains": "good"}
    return dataset


def seed(fake: FakeConsole) -> FakeConsole:
    fake.seed_flow("demo", {"1.0.0": '{"v": 1}'}, published="1.0.0")

    def handler(flow_json, input_data):
        # 候选（IR JSON）里带着源码中的字面量 "good output"；基线定义没有。
        out = {"text": "good output"} if "good output" in flow_json else {"text": "bad output"}
        return {"result": out, "nodes": [], "error": None}

    fake.dry_run_handler = handler
    return fake


# -- FlowSourceProposer ------------------------------------------------------------


def test_good_proposal_compiles_to_ir_and_passes_gate():
    """合法 @flow 提案 → 编译门通过 → definition 是 IR JSON 字符串。"""
    fake = seed(FakeConsole())
    recorded = _Recording(FakeListChatModel(responses=[GOOD_RESPONSE]))
    proposer = FlowSourceProposer(model=recorded)

    proposal = proposer.propose(
        {"flow_id": "demo", "baseline_version": "1.0.0", "current_definition": '{"v": 1}',
         "baseline": {"avg_score": 0.0}, "dataset": {"cases": []}}
    )

    assert proposal is not None
    definition = proposal["definition"]
    assert "@flow" not in definition  # 存的是 IR，不是源码
    ir = json.loads(definition)
    assert ir["flow_id"] == "demo"
    assert isinstance(ir.get("nodes"), list)
    assert proposal["rationale"] == "fix failing case"
    assert proposer.last_source and "@flow" in proposer.last_source
    assert recorded.calls == 1


def test_good_proposal_flows_through_supervisor_loop():
    """端到端：propose → save_version(IR) → evaluate → promotion ticket。"""
    fake = seed(FakeConsole())
    client = make_client(fake)
    proposer = FlowSourceProposer(model=FakeListChatModel(responses=[GOOD_RESPONSE]))
    sup = Supervisor(
        client, SupervisorPolicy(promote_gate="manual", min_improvement=0.02), proposer
    )

    outcome = sup.iterate("demo", make_dataset())
    assert outcome["status"] == "improved"
    assert outcome["candidate_version"] == "1.0.1"

    saved = client.get_version("demo", "1.0.1").get("definition") or ""
    ir = json.loads(saved)
    assert ir["flow_id"] == "demo"  # 数据面契约：definition 仍是 flow 定义 JSON
    assert fake.published["demo"] == "1.0.0"  # 不自动发布


def test_compile_failure_feeds_errors_back_then_succeeds():
    """首稿编译失败 → 错误回喂 → 第二稿编译通过。"""
    fake = seed(FakeConsole())
    recorded = _Recording(FakeListChatModel(responses=[BAD_RESPONSE, GOOD_RESPONSE]))
    proposer = FlowSourceProposer(model=recorded)

    proposal = proposer.propose(
        {"flow_id": "demo", "baseline_version": "1.0.0", "current_definition": '{"v": 1}',
         "baseline": {}, "dataset": {"cases": []}}
    )

    assert proposal is not None
    assert json.loads(proposal["definition"])["flow_id"] == "demo"
    assert recorded.calls == 2
    second_prompt = "\n".join(
        str(getattr(m, "content", m)) for m in recorded.prompts[1]
    )
    assert "f-string" in second_prompt  # 编译错误原文回喂
    assert "FAILED TO COMPILE" in second_prompt  # review 口径
    assert proposer.last_error is None


def test_feedback_budget_exhausted_returns_none_writes_no_version():
    """回喂耗尽 → propose 返回 None、last_error 记录；Supervisor 按停，不写版本。"""
    fake = seed(FakeConsole())
    client = make_client(fake)
    recorded = _Recording(FakeListChatModel(responses=[BAD_RESPONSE]))
    proposer = FlowSourceProposer(model=recorded, max_compile_attempts=3)

    proposal = proposer.propose(
        {"flow_id": "demo", "baseline_version": "1.0.0", "current_definition": "{}",
         "baseline": {}, "dataset": {"cases": []}}
    )
    assert proposal is None
    assert proposer.last_error is not None and "f-string" in proposer.last_error
    assert recorded.calls == 3  # 默认/显式 3 次尝试全部烧完

    # 整个迭代安全降级为 no_proposal，console 不新增版本
    sup = Supervisor(client, SupervisorPolicy(max_iterations=1), proposer)
    outcome = sup.iterate("demo", make_dataset())
    assert outcome["status"] == "no_proposal"
    versions = [v["version"] for v in client.get_flow("demo")["versions"]]
    assert versions == ["1.0.0"]


def test_parse_failure_exhaustion_also_structured():
    """模型全程不给 @flow 源码：与编译失败同等对待，结构化失败不抛异常。"""
    recorded = _Recording(FakeListChatModel(responses=[PURE_NOISE]))
    proposer = FlowSourceProposer(model=recorded, max_compile_attempts=2)
    proposal = proposer.propose({"flow_id": "demo", "baseline_version": "1.0.0",
                                 "current_definition": "{}", "baseline": {}, "dataset": {}})
    assert proposal is None
    assert recorded.calls == 2
    assert "@flow" in str(proposer.last_error)


def test_flow_source_proposer_requires_config_without_model():
    with pytest.raises(ConsoleClientError, match="PLAITA_AI_PROPOSER"):
        FlowSourceProposer(env={})


# -- ops_mcp wiring ----------------------------------------------------------------


def teardown_function(_):
    ops_mcp.set_client(None)


def test_supervisor_iterate_default_is_flow_and_validates_choice(monkeypatch):
    """默认 proposer=flow；未知值结构化报错；缺 proposer 配置时报可操作错误。"""
    fake = seed(FakeConsole())
    fake.seed_flow("demo2", {"1.0.0": '{"v": 1}'}, published="1.0.0")
    tools = make_wired(fake)

    # 未知 proposer → _guarded 捕获 ValueError → ok:false
    out = json.loads(tools["supervisor_iterate"]("demo", "whatever", proposer="yolo"))
    assert out["ok"] is False
    assert "unknown proposer" in out["error"]

    # 默认值 = "flow"：未配置 PLAITA_AI_PROPOSER_* env → 结构化报错（不炸穿）
    for var in ("PLAITA_AI_PROPOSER_BASE_URL", "PLAITA_AI_PROPOSER_MODEL"):
        monkeypatch.delenv(var, raising=False)
    import inspect

    assert inspect.signature(tools["supervisor_iterate"]).parameters["proposer"].default == "flow"
    out = json.loads(tools["supervisor_iterate"]("demo", "whatever"))
    assert out["ok"] is False
    assert "PLAITA_AI_PROPOSER" in out["error"]
