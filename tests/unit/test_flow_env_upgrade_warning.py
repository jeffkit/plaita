"""P0-5 回归: ``$ENV`` 升级静默失败告警。

0.5.0 起 ``$ENV`` 默认空 (allowlist 模型)。旧 flow 升级后 ``$ENV.HOME`` 会静默
解析为空——下游拿到空字符串继续跑, 不报错。``Flow.model_validate`` 现在扫一次
节点表达式, 检测到 ``$ENV.`` 引用但 ``expose_env`` 为空时 ``logger.warning``
列出 key 名与修复指引, 让沉默变可见。不报错 (保持兼容)。
"""
from __future__ import annotations

import logging

import pytest

from plaita.core.flow import Flow


def _flow_json_with_env(ref: str, expose_env=None):
    data = {
        "flow_id": "env-flow",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "output": ref, "resultType": "success"},
        ],
    }
    if expose_env is not None:
        data["exposeEnv"] = expose_env
    return data


class TestEnvUpgradeWarning:
    def test_warns_when_env_ref_but_expose_env_empty(self, caplog):
        with caplog.at_level(logging.WARNING, logger="plaita.logger"):
            Flow.model_validate(_flow_json_with_env("$ENV.HOME"))
        assert any("$ENV" in r.message and "HOME" in r.message and "expose_env" in r.message
                   for r in caplog.records)

    def test_no_warn_when_expose_env_covers_ref(self, caplog):
        with caplog.at_level(logging.WARNING, logger="plaita.logger"):
            Flow.model_validate(_flow_json_with_env("$ENV.HOME", expose_env=["HOME"]))
        assert not any("$ENV" in r.message for r in caplog.records)

    def test_no_warn_when_no_env_ref(self, caplog):
        with caplog.at_level(logging.WARNING, logger="plaita.logger"):
            Flow.model_validate(_flow_json_with_env("$INPUT.name"))
        assert not any("$ENV" in r.message for r in caplog.records)

    def test_collects_multiple_keys(self, caplog):
        data = {
            "flow_id": "multi",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e",
                 "output": "$ENV.API_BASE and $ENV.TOKEN",
                 "resultType": "success"},
            ],
        }
        with caplog.at_level(logging.WARNING, logger="plaita.logger"):
            Flow.model_validate(data)
        msg = "\n".join(r.message for r in caplog.records if "$ENV" in r.message)
        assert "API_BASE" in msg
        assert "TOKEN" in msg

    def test_warning_does_not_block_construction(self):
        # 告警不应阻止 Flow 构造与执行 (保持兼容)
        flow = Flow.model_validate(_flow_json_with_env("$ENV.HOME"))
        assert flow.flow_id == "env-flow"
        # 执行不报错; $ENV.HOME 解析为空 (HOME 未暴露)
        result = flow.run(name="x")
        assert result in (None, "", "None")
