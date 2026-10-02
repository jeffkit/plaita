"""W3 修复验证：FoT planner 参考注入的时效性与可观测性。

背景（上轮评审 AI-6/AI-7）：
- 参考文档原在模块 import 时读一次，MCP server 等常驻进程永不更新，
  且 FileNotFoundError 静默吞掉（无日志的质量劣化）。修复后改为每次
  plan/review 调用现读 + warning 可观测。
- authoring-spec.md 的 §0「30 秒自检清单」属于注入正文（随全文注入），
  本文件钉住清单关键句必须出现在 COMPOSE/REVIEW 系统提示词里。

离线测试（duck-type 录制模型，无 API key）。
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

pytest.importorskip("langchain", reason="langchain extra not installed: pip install 'plaita-ai[agent]'")
pytest.importorskip("plaita", reason="plaita runtime not installed")

import plaita_ai.agent.fot.planner as planner_module
from plaita_ai.agent.fot.planner import plan_flow_source, review_flow_source


class _RecordingModel:
    """录制 invoke 入参的 duck-type 模型（planner 只用 invoke + .content）。"""

    def __init__(self, content: str = '```python\n@flow("x")\ndef x(INPUT):\n    return 1\n```'):
        self.content = content
        self.calls = []

    def invoke(self, messages, *args, **kwargs):
        self.calls.append(list(messages))
        return SimpleNamespace(content=self.content)


def _system_of(model: _RecordingModel) -> str:
    assert model.calls, "model 未被调用"
    return model.calls[0][0].content


def test_prompt_carries_authoring_checklist_key_phrases():
    """注入段含 authoring-spec §0「30 秒自检清单」关键句（真实参考文件，不打桩）。"""
    model = _RecordingModel()
    plan_flow_source(model, task="测试任务")
    system = _system_of(model)
    assert "30 秒自检清单" in system
    # 清单关键硬约束（跨分支同名赋值 / 保留 kwargs timeout）必须随注入到达模型
    assert "同名赋值" in system
    assert "timeout=" in system
    model2 = _RecordingModel()
    review_flow_source(
        model2, task="t", source='```python\n@flow("x")\ndef x(INPUT):\n    return 1\n```', errors=[]
    )
    assert "30 秒自检清单" in _system_of(model2)


def test_missing_reference_warns_but_does_not_raise(monkeypatch, caplog):
    """参考文件缺失：logger.warning（含路径与修复提示）且 plan/review 不抛。"""

    def _missing(skill_name: str, ref_name: str) -> str:
        raise FileNotFoundError(f"skill reference not found: {skill_name}/{ref_name}")

    monkeypatch.setattr(planner_module, "get_skill_reference", _missing)
    with caplog.at_level(logging.WARNING, logger="plaita_ai.agent.fot.planner"):
        model = _RecordingModel()
        source = plan_flow_source(model, task="测试任务")
    assert source.startswith("@flow")  # 未抛异常，正常出源码
    system = _system_of(model)
    # 双双降级为占位段
    assert "未加载到 DSL 参考" in system
    assert "编写规范文档未加载" in system
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    texts = [r.getMessage() for r in warnings]
    assert any("codeflow-reference.md" in t and "references" in t for t in texts), texts
    assert any("authoring-spec.md" in t and "references" in t for t in texts), texts


def test_reference_edits_between_calls_are_reflected(tmp_path, monkeypatch):
    """两次调用之间修改参考文件内容，注入随之更新（每次调用现读，无模块缓存）。"""
    spec_file = tmp_path / "authoring-spec.md"
    spec_file.write_text("SPEC-VERSION-ONE 唯一标记一", encoding="utf-8")

    def _fake_loader(skill_name: str, ref_name: str) -> str:
        path = tmp_path / ref_name
        if not path.is_file():
            raise FileNotFoundError(f"skill reference not found: {skill_name}/{ref_name}")
        return path.read_text(encoding="utf-8")

    monkeypatch.setattr(planner_module, "get_skill_reference", _fake_loader)

    model = _RecordingModel()
    plan_flow_source(model, task="第一轮")
    assert "SPEC-VERSION-ONE" in _system_of(model)

    spec_file.write_text("SPEC-VERSION-TWO 唯一标记二", encoding="utf-8")
    model2 = _RecordingModel()
    plan_flow_source(model2, task="第二轮")
    system2 = _system_of(model2)
    assert "SPEC-VERSION-TWO" in system2
    assert "SPEC-VERSION-ONE" not in system2
