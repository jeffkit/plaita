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
