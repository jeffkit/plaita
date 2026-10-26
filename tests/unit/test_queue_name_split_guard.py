"""队列名分裂闸（2026-10-26，A56-02 警告的前移+升格）。

同主机 env 同时设 console 侧（PLAITA_CONSOLE_TASK_QUEUE）与 worker 侧
（PLAITA_QUEUE_NAME/QUEUE_NAME）任务流且值不同 = 进程组对「消息在哪个
Stream」各执一词——console/调度派发与 worker/event_filter 消费落在不同
Stream，消息进入无人消费的队列（静默搁浅）。preflight 启动即拒 + 迁移
提示；合法多池共存拓扑（:go/:ctrl 池与主池同机）经
PLAITA_ALLOW_QUEUE_SPLIT=1 显式放行（降 WARNING）。

解析语义零变化：本闸只做启动期检测，resolve_queue_name /
resolve_dispatch_queue_name 行为不动（A56-02 的运行期警告保留——它覆盖
跨主机与运行中途改 env 的窗口）。
"""
import unittest

import pytest

from plaita.runtime_config import (
    ConfigError,
    _reset_warnings_for_tests,
    preflight_security_and_limits,
)


def _env(**kw):
    return {k: v for k, v in kw.items() if v is not None}


@pytest.fixture(autouse=True)
def _clean_warned():
    _reset_warnings_for_tests()
    yield
    _reset_warnings_for_tests()


class TestQueueNameSplitGate(unittest.TestCase):
    def test_split_is_fatal_with_migration_hint(self):
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue",
            PLAITA_QUEUE_NAME="plaita:flow:queue:v2",
        )
        with pytest.raises(ConfigError) as ei:
            preflight_security_and_limits(env)
        msg = str(ei.value)
        assert "队列名分裂" in msg
        assert "PLAITA_CONSOLE_TASK_QUEUE" in msg
        assert "PLAITA_QUEUE_NAME" in msg
        assert "PLAITA_ALLOW_QUEUE_SPLIT" in msg  # 逃生门必须随错误提示

    def test_split_via_legacy_alias_is_fatal(self):
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue",
            QUEUE_NAME="plaita:flow:queue:v2",
        )
        with pytest.raises(ConfigError) as ei:
            preflight_security_and_limits(env)
        assert "QUEUE_NAME" in str(ei.value)

    def test_plaita_queue_name_wins_over_legacy_alias(self):
        """worker 侧解析序 PLAITA_QUEUE_NAME > QUEUE_NAME：与 console 一致即不分裂。"""
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue",
            PLAITA_QUEUE_NAME="plaita:flow:queue",
            QUEUE_NAME="plaita:flow:queue:ignored-legacy",
        )
        assert preflight_security_and_limits(env) == []

    def test_split_with_knob_on_downgrades_to_warning(self):
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue",
            PLAITA_QUEUE_NAME="plaita:flow:queue:go",
            PLAITA_ALLOW_QUEUE_SPLIT="1",
        )
        warnings = preflight_security_and_limits(env)
        assert any("队列名分裂" in w for w in warnings)

    def test_split_with_knob_false_stays_fatal(self):
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue",
            PLAITA_QUEUE_NAME="plaita:flow:queue:go",
            PLAITA_ALLOW_QUEUE_SPLIT="0",
        )
        with pytest.raises(ConfigError):
            preflight_security_and_limits(env)

    def test_knob_garbage_is_named_error(self):
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue",
            PLAITA_QUEUE_NAME="plaita:flow:queue:go",
            PLAITA_ALLOW_QUEUE_SPLIT="maybe",
        )
        with pytest.raises(ConfigError) as ei:
            preflight_security_and_limits(env)
        assert "PLAITA_ALLOW_QUEUE_SPLIT" in str(ei.value)

    def test_same_value_both_sides_is_clean(self):
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue",
            PLAITA_QUEUE_NAME="plaita:flow:queue",
        )
        assert preflight_security_and_limits(env) == []

    def test_single_side_set_is_clean(self):
        one = _env(PLAITA_CONSOLE_TASK_QUEUE="plaita:flow:queue")
        other = _env(PLAITA_QUEUE_NAME="plaita:flow:queue:v9")
        assert preflight_security_and_limits(one) == []
        assert preflight_security_and_limits(other) == []

    def test_empty_side_falls_into_existing_identity_gate(self):
        """空值走既有 identity 闸（非本闸职责），不误报成分裂。"""
        env = _env(
            PLAITA_CONSOLE_TASK_QUEUE="",
            PLAITA_QUEUE_NAME="plaita:flow:queue",
        )
        with pytest.raises(ConfigError) as ei:
            preflight_security_and_limits(env)
        assert "队列名分裂" not in str(ei.value)
        assert "empty value" in str(ei.value)


if __name__ == "__main__":
    unittest.main()
