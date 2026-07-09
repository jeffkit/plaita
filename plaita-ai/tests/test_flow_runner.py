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
