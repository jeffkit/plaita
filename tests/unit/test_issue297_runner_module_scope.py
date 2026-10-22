"""plaita#297：runner 信封必须覆盖模块级用户代码。

#43/r75 的信封穿透只包住 ``run()`` 函数体（runner 模板的 ``try`` 从
``_result = run(_input)`` 才开始）。用户代码**模块级**的确定性错误发生在
try 之外 → runner 非零退出且 stdout 空 → 父进程按「无信封非零退出」的
基础设施失败判可重试，烧满 ``retryTimes``（每次重试都是一次真实 spawn，
顶层副作用重复执行）；unsafe 档同一代码 1 次即具名终态。顶层 ``print``
后抛异常更糟：stdout 非空但无信封，诊断退化成「not valid JSON」。

两个面：

1. **runner 模板**（subprocess/docker 共用）：模块级未捕获异常经 excepthook
   写成与 run() 函数体同形的 ``ok=false`` 信封（内层类型名随 ``type`` 字段
   穿透），两档终态形状一致；
2. **解码**（``_decode_runner_output``）：信封前的用户 stdout（print）不再
   让信封不可解析——只接受**末尾**的合法信封，脏输出仍按基础设施级失败。

负控（先红后绿，验收时手工还原、勿提交改动）：

- 把 ``_RUNNER_TEMPLATE`` 还原为 r75 版本（excepthook 块删除）：模块级
  TypeError 用例 pre-fix 执行 4 次（retryTimes=3）/终态
  ``RuntimeError: Subprocess exited...``；顶层 print + raise 用例 pre-fix
  终态 ``not valid JSON``。post-fix 全部 1 次 + 具名。
- 单独还原 ``_decode_runner_output`` 的末尾信封分支：顶层 print 用例与
  run() 内 print 用例红，无脏输出的信封用例仍绿。
"""
from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

from plaita.core.context import ExecutionContext
from plaita.core.errors import ErrorStrategy, NodeExecutionError
from plaita.core.runner import NodeRunner, is_retryable_error
from plaita.node.code import (
    CodeNode,
    SandboxEnvelopeError,
    _decode_runner_output,
    run_python_docker,
)

try:
    _docker_check = subprocess.run(
        ["docker", "images", "python:3.12-slim", "--format", "{{.Repository}}"],
        capture_output=True,
        timeout=5,
    )
    _DOCKER_AVAILABLE = (
        _docker_check.returncode == 0 and b"python" in _docker_check.stdout
    )
except Exception:  # noqa: BLE001 - 探测失败即视为不可用
    _DOCKER_AVAILABLE = False


class _ModuleProbeCodeNode(CodeNode):
    """顶层代码每次执行向探针文件追加 ``x``，随后抛指定异常。"""

    @classmethod
    def build(cls, probe_path, top: str, backend: str = "subprocess") -> "_ModuleProbeCodeNode":
        code = (
            "with open({path!r}, 'a') as _f:\n"
            "    _f.write('x')\n"
            "{top}\n"
            "def run(input):\n"
            "    return 1\n"
        ).format(path=str(probe_path), top=top)
        return cls(
            id="module-probe",
            code=code,
            input=None,
            sandbox_backend=backend,
            errorHandler={
                "strategy": ErrorStrategy.ABORT.value,
                "retryTimes": 3,
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", ["ValueError", "TypeError", "NameError"])
async def test_module_level_deterministic_error_runs_once(tmp_path, exc):
    """模块级确定性错误：retryTimes=3 也只真实执行 1 次（pre-fix 4 次）。"""
    probe = tmp_path / "probe.txt"
    node = _ModuleProbeCodeNode.build(probe, f"raise {exc}('module-level')")

    with pytest.raises(NodeExecutionError) as ctx:
        await NodeRunner(ExecutionContext())._execute_with_retry(
            MagicMock(), node, None
        )

    assert f"{exc}: module-level" in str(ctx.value)
    cause = ctx.value.__cause__
    assert isinstance(cause, SandboxEnvelopeError)
    assert cause.origin_type == exc
    assert is_retryable_error(cause) is False
    assert probe.read_text().count("x") == 1, (
        f"模块级 {exc} 是确定性错误，retryTimes=3 也不得重试"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["unsafe", "subprocess"])
async def test_module_level_terminal_shape_matches_across_backends(tmp_path, backend):
    """两档终态形状一致：同一顶层 TypeError 都具名、都只执行 1 次。"""
    probe = tmp_path / "probe.txt"
    node = _ModuleProbeCodeNode.build(
        probe, "raise TypeError('module-level')", backend=backend
    )

    with pytest.raises(NodeExecutionError) as ctx:
        await NodeRunner(ExecutionContext())._execute_with_retry(
            MagicMock(), node, None
        )

    assert "TypeError: module-level" in str(ctx.value)
    assert probe.read_text().count("x") == 1


@pytest.mark.asyncio
async def test_module_level_error_after_print_still_decodes_named_envelope(tmp_path):
    """顶层 print 污染 stdout 后抛异常：仍取到末尾信封，不退化成 not valid JSON。"""
    probe = tmp_path / "probe.txt"
    node = _ModuleProbeCodeNode.build(
        probe, "print('hello')\nraise TypeError('after-print')"
    )

    with pytest.raises(NodeExecutionError) as ctx:
        await NodeRunner(ExecutionContext())._execute_with_retry(
            MagicMock(), node, None
        )

    assert "TypeError: after-print" in str(ctx.value)
    assert "not valid JSON" not in str(ctx.value)
    assert probe.read_text().count("x") == 1


@pytest.mark.asyncio
async def test_module_level_expression_and_name_errors_run_once(tmp_path):
    """顶层表达式 TypeError / NameError 同属裁决集，执行一次即停。"""
    for top, expected in (
        ("C = 1 + 'a'", "TypeError"),
        ("print(UNDEFINED_TOP)", "NameError"),
    ):
        probe = tmp_path / f"{expected}.txt"
        node = _ModuleProbeCodeNode.build(probe, top)
        with pytest.raises(NodeExecutionError) as ctx:
            await NodeRunner(ExecutionContext())._execute_with_retry(
                MagicMock(), node, None
            )
        assert expected in str(ctx.value), (top, ctx.value)
        assert probe.read_text().count("x") == 1, top


def test_decode_accepts_run_body_print_before_envelope():
    """run() 内 print 也不再让信封不可解析（信封是最后一段输出）。"""
    assert _decode_runner_output('hello\n{"ok": true, "result": 7}', "") == 7
    assert _decode_runner_output('hello {"ok": true, "result": 8}', "") == 8


def test_decode_leading_stdout_envelope_keeps_origin_type():
    """带前置输出的 ok=false 信封仍具名穿透（不依赖 json.loads 整段）。"""
    with pytest.raises(SandboxEnvelopeError) as ctx:
        _decode_runner_output(
            'hello\n{"ok": false, "error": "boom", "type": "TypeError"}',
            "",
            origin_typed=True,
        )
    assert ctx.value.origin_type == "TypeError"
    assert is_retryable_error(ctx.value) is False


def test_decode_rejects_non_terminal_or_junk_envelopes():
    """只有末尾信封算信封；尾部残余/纯脏输出仍是基础设施级失败。"""
    with pytest.raises(RuntimeError) as trailing:
        _decode_runner_output('{"ok": true, "result": 1}\ntrailing', "")
    assert "not valid JSON" in str(trailing.value)
    assert not hasattr(trailing.value, "origin_type")

    with pytest.raises(RuntimeError) as junk:
        _decode_runner_output("hello\nnot json", "")
    assert not hasattr(junk.value, "origin_type")
    assert is_retryable_error(junk.value) is True


@pytest.mark.skipif(
    not _DOCKER_AVAILABLE, reason="python:3.12-slim image not available locally"
)
def test_docker_module_level_envelope_when_available():
    """docker 后端（与 subprocess 共用模板）：顶层 TypeError 同样具名穿透。"""
    with pytest.raises(SandboxEnvelopeError) as ctx:
        run_python_docker(
            "raise TypeError('module-level')\ndef run(i):\n    return 1", None
        )
    assert ctx.value.origin_type == "TypeError"
    assert is_retryable_error(ctx.value) is False
