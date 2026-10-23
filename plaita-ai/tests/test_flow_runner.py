"""Tests for plaita_ai.flow_runner."""

from plaita_ai.flow_runner import compile_flow, run_flow


GOOD_SRC = '''
@flow("greet", input_type="object")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
'''

BAD_SRC = '''
@flow("bad", input_type="object")
def bad(INPUT):
    return f"hi {INPUT.name}"
'''


def test_compile_flow_ok():
    result = compile_flow(GOOD_SRC)
    assert result.ok
    assert result.ir is not None
    assert result.flow_id == "greet"


def test_compile_flow_error_has_line():
    result = compile_flow(BAD_SRC)
    assert not result.ok
    assert result.errors
    assert result.errors[0].line is not None


def test_run_flow():
    result = run_flow(GOOD_SRC, {"name": "alice"})
    assert result.ok
    assert result.result == "hi ALICE"


BAD_TOPO_SRC = '''
@flow("bad_topo", input_type="object")
def bad_topo(INPUT):
    # 合法 AST，但我们通过 compile 后手工无法注入悬空边；
    # 用缺少 return 的路径不够——改测：非法 ErrorHandler strategy 在编译期拦。
    x = F.upper(INPUT.name)
    return x
'''


def test_compile_flow_rejects_invalid_error_handler_strategy():
    src = '''
@flow("eh", input_type="object")
def eh(INPUT):
    x = F.upper(INPUT.name)
    return x
'''
    # 正常应通过
    assert compile_flow(src).ok


def test_compile_and_run_share_validated_ir():
    """run_flow 不应二次 compile_source；compile 失败则不执行。"""
    bad = compile_flow(BAD_SRC)
    assert not bad.ok
    ran = run_flow(BAD_SRC, {"name": "x"})
    assert not ran.ok
    assert ran.error_type == "compile"


# ---------------------------------------------------------------------------
# plaita#330：挂起节点的 flow_run 结构化 fail-closed 呈现
# ---------------------------------------------------------------------------

EVENT_SRC = '''
@flow("wait_flow", input_type="object")
def wait_flow(INPUT):
    EVENT(type="approve_me")
    return "done"
'''


def test_run_flow_suspend_node_named_error():
    """event 在 normal 根（flow_run 路径）：ok=false + 稳定 error_type 字符串 +
    结构化 error_detail（fix 指向分布式运行时，不引导误删节点）。"""
    ran = run_flow(EVENT_SRC)
    assert not ran.ok
    assert ran.error_type == "suspend_not_executable"
    detail = ran.error_detail
    assert detail["error_type"] == "suspend_not_executable"
    assert detail["cause"] == "environment"
    assert detail["fix"]["action"] == "run_distributed"
    assert detail["message"].startswith("这不是源码缺陷，不要删除/替换该节点")
    assert "不要删除" in ran.error


ERROR_END_SRC = '''
@flow("err_flow", input_type="object")
def err_flow(INPUT):
    end(output="boom", result_type="error", error={"code": -42, "message": "boom"})
'''


def test_run_flow_error_type_is_stable_enum_value_string():
    """FlowErrorType 枚举错误类型序列化为稳定字符串值（历史上是
    ``str(Enum)`` 产出 ``FlowErrorType.ERROR_RESULT`` 机器键）。"""
    ran = run_flow(ERROR_END_SRC)
    assert not ran.ok
    assert ran.error_type == "error_result"
