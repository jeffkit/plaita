"""第七十五轮：确定性错误集对齐（plaita-go#43 收口 · Python 侧）。

owner 裁决（2026-10-22）：确定性错误集 = Python 采纳完整集
``{ValueError, TypeError, NameError, SyntaxError}``（及子类），Go 不放松——
plaita-go r69（``b8fb5a6``）的 sidecar 行为为终态，两侧 node-F2 同一口径。

两个面（B3 r70 实测的分裂就此收敛）：

1. **类型面**（in-process unsafe/restricted 后端，用户异常原样穿出）：
   ``is_retryable_error`` 对 TypeError/NameError/SyntaxError 及子类判不可
   重试，与 ValueError 同列；#219 输出校验钩子标记对整个确定性家族一致
   豁免（校验异常仍走 retryTimes）。
2. **信封面**（subprocess/docker 后端把 ``ok=false`` 信封包成 RuntimeError，
   r70 实测 ``is_retryable_error(RuntimeError("ValueError: boom")) == True``
   全可重）：``SandboxEnvelopeError``（``RuntimeError`` 子类，文本逐字不变）
   把信封保留的内层类型名挂在 ``origin_type`` 穿透分类——按属性分类、不做
   文本嗅探；基础设施级失败（无输出/坏 JSON/无信封非零退出）与 JS 信封
   维持历史可重试缺省（与 Go ``decodePythonRunnerOutput`` 注释口径一致）。

负控（先红后绿，验收时手工执行还原变异、勿提交改动）：

1. 类型面还原——把 ``plaita.core.runner._DETERMINISTIC_ERROR_CLASSES`` 收窄
   回 ``(ValueError,)`` 且删除 ``is_retryable_error`` 的 ``origin_type``
   分支：``TestDeterministicTypeSetClassification`` 的 TypeError/NameError/
   SyntaxError 用例、``TestLiveBackendsSingleExecution`` 的四类×两后端实弹
   探针（执行次数 1 → retryTimes+1=4）全红；ValueError/1002/校验钩子守恒
   用例必须仍绿；
2. 信封面还原——把 ``plaita.node.code._decode_runner_output`` 的
   ``origin_typed`` 分支删掉（恒走裸 ``RuntimeError``）：
   ``TestSandboxEnvelopePenetration``/``TestWorkerParityFullChain`` 的
   信封用例全红，基础设施级失败用例仍绿。
"""
from __future__ import annotations

import json
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    DETERMINISTIC_ERROR_TYPES,
    ErrorStrategy,
    FlowErrorException,
    NodeException,
    NodeExecutionError,
    RecoverableErrorHandler,
    mark_output_validation_retryable,
)
from plaita.core.runner import NodeRunner, is_retryable_error
from plaita.node.code import CodeNode, SandboxEnvelopeError, _decode_runner_output
from plaita.server.flow_worker import _is_retryable_node_failure

DETERMINISTIC_NAMES = ("ValueError", "TypeError", "NameError", "SyntaxError")


class TestDeterministicTypeSetClassification:
    """类型面：四类确定性异常（含子类）不重试；#219 校验钩子豁免全家族一致。"""

    def test_set_matches_go_r69_adjudication(self):
        """与 plaita-go ``pyDeterministicEnvelopeTypes`` 逐字同集（owner 裁决）。"""
        assert DETERMINISTIC_ERROR_TYPES == frozenset(DETERMINISTIC_NAMES)

    @pytest.mark.parametrize("exc_type", [ValueError, TypeError, NameError, SyntaxError])
    def test_deterministic_types_are_not_retryable(self, exc_type):
        assert is_retryable_error(exc_type("boom")) is False

    def test_deterministic_subclasses_are_not_retryable(self):
        class MyTypeError(TypeError):
            pass

        class MySyntaxError(SyntaxError):
            pass

        assert is_retryable_error(MyTypeError("boom")) is False
        assert is_retryable_error(MySyntaxError("boom")) is False

    def test_output_validation_marker_exempts_whole_family(self):
        """#219 契约：校验钩子异常仍走 retryTimes——豁免不只护 ValueError。"""
        for exc_type in (ValueError, TypeError, NameError, SyntaxError):
            exc = exc_type("invalid output")
            mark_output_validation_retryable(exc)
            assert is_retryable_error(exc) is True, exc_type

    def test_non_deterministic_keeps_historical_retry(self):
        """集外异常维持历史可重试；1001/1002 分类不受本轮影响（守恒）。"""
        assert is_retryable_error(RuntimeError("boom")) is True
        assert is_retryable_error(KeyError("k")) is True
        assert is_retryable_error(NodeException(1002, "connection reset")) is True
        assert is_retryable_error(NodeException(9999, "future code")) is True
        assert is_retryable_error(NodeException(1001, "bad config")) is False


class TestSandboxEnvelopePenetration:
    """信封面：内层确定性类型经 origin_type 穿透；基础设施/JS 维持历史。"""

    @staticmethod
    def _decode_failure(payload: str) -> BaseException:
        with pytest.raises(RuntimeError) as ctx:
            _decode_runner_output(payload, "", origin_typed=True)
        return ctx.value

    def test_python_envelope_raises_named_error_with_origin_type(self):
        err = self._decode_failure(
            json.dumps({"ok": False, "error": "boom", "type": "TypeError"})
        )
        assert isinstance(err, SandboxEnvelopeError)
        assert str(err) == "TypeError: boom"
        assert err.origin_type == "TypeError"

    @pytest.mark.parametrize("name", DETERMINISTIC_NAMES)
    def test_python_envelope_deterministic_types_not_retryable(self, name):
        err = self._decode_failure(json.dumps({"ok": False, "error": "b", "type": name}))
        assert is_retryable_error(err) is False, name

    def test_python_envelope_missing_type_stays_retryable(self):
        """缺型（→``Error``）与集外类型保持历史可重试缺省（Go 同口径）。"""
        for payload in (
            json.dumps({"ok": False, "error": "b"}),
            json.dumps({"ok": False, "error": "k", "type": "KeyError"}),
        ):
            err = self._decode_failure(payload)
            assert is_retryable_error(err) is True, payload

    def test_bare_runtime_error_text_is_not_sniffed(self):
        """分类按 origin_type 属性、不做文本嗅探：裸 RuntimeError 仍可重试。"""
        assert is_retryable_error(RuntimeError("TypeError: boom")) is True

    def test_infra_failures_have_no_origin_type_and_stay_retryable(self):
        """无输出/坏 JSON 是基础设施级失败：无 origin_type、可重试（Go 同）。"""
        with pytest.raises(RuntimeError) as empty:
            _decode_runner_output("", "Traceback (most recent call last):")
        assert not hasattr(empty.value, "origin_type")
        assert is_retryable_error(empty.value) is True

        with pytest.raises(RuntimeError) as bad:
            _decode_runner_output("not json", "")
        assert not hasattr(bad.value, "origin_type")
        assert is_retryable_error(bad.value) is True

    def test_js_envelope_keeps_plain_runtime_error(self):
        """JS 信封不在裁决集内：维持裸 RuntimeError 历史行为（可重试）。"""
        with pytest.raises(RuntimeError) as ctx:
            _decode_runner_output(
                json.dumps({"ok": False, "error": "b", "type": "TypeError"}), ""
            )
        assert not isinstance(ctx.value, SandboxEnvelopeError)
        assert not hasattr(ctx.value, "origin_type")
        assert is_retryable_error(ctx.value) is True


class TestWorkerParityFullChain:
    """A50-03 链路：worker 重投判据沿 __cause__ 复用同一分类，信封不例外。"""

    @staticmethod
    def _distributed_chain(original: BaseException) -> FlowErrorException:
        """真实分布式形态：FlowErrorException → NodeExecutionError → 原始异常。"""
        wrapped = NodeExecutionError(f"执行节点a出错了: {original}", node="a")
        wrapped.__cause__ = original
        outer = FlowErrorException(str(wrapped))
        outer.__cause__ = wrapped
        return outer

    @pytest.mark.parametrize("name", DETERMINISTIC_NAMES)
    def test_deterministic_envelope_chain_not_redispatched(self, name):
        err = SandboxEnvelopeError(name, "boom")
        assert _is_retryable_node_failure(self._distributed_chain(err)) is False, name

    def test_in_process_typeerror_chain_not_redispatched(self):
        assert (
            _is_retryable_node_failure(self._distributed_chain(TypeError("boom")))
            is False
        )

    def test_transient_envelope_chain_still_redispatched(self):
        """集外信封（KeyError）保持 worker 重投语义（守恒）。"""
        err = SandboxEnvelopeError("KeyError", "k")
        assert _is_retryable_node_failure(self._distributed_chain(err)) is True


class _ProbeCodeNode(CodeNode):
    """实弹探针：用户代码每次执行向探针文件追加一个 ``x`` 后抛指定异常。"""

    _PROBE_TEMPLATE: ClassVar[str] = (
        "def run(input):\n"
        "    with open({path!r}, 'a') as _f:\n"
        "        _f.write('x')\n"
        "    raise {exc}('boom')\n"
    )

    @classmethod
    def build(cls, probe_path, exc: str, backend: str) -> "_ProbeCodeNode":
        return cls(
            id="probe",
            code=cls._PROBE_TEMPLATE.format(path=str(probe_path), exc=exc),
            input=None,
            sandbox_backend=backend,
            errorHandler={
                "strategy": ErrorStrategy.ABORT.value,
                "retryTimes": 3,
            },
        )


class TestLiveBackendsSingleExecution:
    """实弹（先红后绿）：四类确定性错误 × 两后端，pre 重试 4 次 → post 单次
    执行 + 具名终态（NodeExecutionError 内嵌 ``{类型}: boom``）。"""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("backend", ["unsafe", "subprocess"])
    @pytest.mark.parametrize("exc", DETERMINISTIC_NAMES)
    async def test_user_code_runs_once_and_terminalizes(self, tmp_path, backend, exc):
        probe = tmp_path / "probe.txt"
        node = _ProbeCodeNode.build(probe, exc, backend)

        with pytest.raises(NodeExecutionError) as ctx:
            await NodeRunner(ExecutionContext())._execute_with_retry(
                MagicMock(), node, None
            )

        assert f"{exc}: boom" in str(ctx.value), (
            f"具名终态须内嵌原始错误文本（backend={backend}, exc={exc}）：{ctx.value}"
        )
        count = probe.read_text().count("x")
        assert count == 1, (
            f"{exc} 是确定性错误（backend={backend}），retryTimes=3 也不得重试；"
            f"用户代码实际执行 {count} 次"
        )
