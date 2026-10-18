"""plaita#220 高 3 面之 L3：sexpr 往返丢失 ``CodeNode.sandbox_backend``。

``sandbox_backend`` 是逐节点的沙箱档位（``plaita/node/code.py``）：
``CodeNode.execute`` 直接读该属性选隔离实现，字段缺席时回退进程级默认
（0.5.0 起 ``docker``）。sexpr 的读写两侧此前都不认这个字段 ⇒ 任何
``flow_to_sexpr`` / ``compile_sexpr`` 往返都把 ``subprocess`` / ``restricted``
/ ``unsafe`` **静默洗成默认档位**——安全配置降级，不是普通字段丢失。
``@flow`` 侧原本就保留（见 ``_emit.py::_call_code``）。

验收（#220-L3）：
- 全字段 code IR → ``flow_to_sexpr`` → ``compile_sexpr`` 逐字段相等；
- camel / snake 两种 IR 拼写都发射（#200/#207 同口径）；
- 读侧关键字别名 ``:sandbox-backend`` / ``:sandbox_backend`` / ``:sandboxBackend``；
- 字段缺席时不发明字节（保留「不设 = 用进程默认」语义）；
- 不可忠实表达的值（非字符串 / 空串 / 容器）写、读两侧 fail-closed；
- 编译出的 IR 交给 ``CodeNode`` 后档位真实生效（不只是留在 dict 里）；
- ``@flow`` → sexpr 跨前端链路同样保真。
"""
from __future__ import annotations

import pytest

from plaita.dsl.codeflow import compile_source, emit_source
from plaita.dsl.sexpr import (
    SexprEmitError,
    compile_sexpr,
    flow_to_sexpr,
)
from plaita.node.code import CodeNode

CODE_SRC = "def run(x): return x"


def _code_node(
    backend: object = "subprocess",
    *,
    key: str = "sandbox_backend",
    include: bool = True,
) -> dict:
    node = {
        "type": "code",
        "id": "n",
        "language": "python",
        "code": CODE_SRC,
        "input": "$INPUT.x",
        "next": "e",
    }
    if include:
        node[key] = backend
    return node


def _ir(
    backend: object = "subprocess",
    *,
    key: str = "sandbox_backend",
    include: bool = True,
) -> dict:
    return {
        "flow_id": "f-l3",
        "nodes": [
            {"type": "start", "id": "s", "next": "n"},
            _code_node(backend, key=key, include=include),
            {"type": "end", "id": "e", "resultType": "success"},
        ],
    }


def _pick(nodes):
    return [n for n in nodes if n.get("id") == "n"][0]


def _sexpr_src(fragment: str) -> str:
    return (
        "(flow f (start :id s -> n) "
        f'(code :id n :lang python :code "{CODE_SRC}" {fragment} -> e) '
        "(end :id e))"
    )


# ---------------------------------------------------------------------------
# 写侧：往返保真
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend", ["docker", "subprocess", "restricted", "unsafe"])
def test_sexpr_roundtrip_preserves_sandbox_backend(backend):
    """#220-L3 主症：档位往返后逐字段相等，且其余 code 字段不回归。"""
    src = flow_to_sexpr(_ir(backend))
    assert f':sandbox-backend "{backend}"' in src
    node = _pick(compile_sexpr(src)["nodes"])
    assert node["sandbox_backend"] == backend
    assert node["language"] == "python"
    assert node["code"] == CODE_SRC
    assert node["input"] == "$INPUT.x"
    assert node["next"] == "e"
    # 语义面：不是只留在 IR dict 里，CodeNode 解析后档位真实生效。
    assert CodeNode(**node).sandbox_backend == backend


@pytest.mark.parametrize("backend", ["1", "true", "42", "nil"])
def test_sexpr_roundtrip_keeps_string_typed_backend(backend):
    """数字样 / 原子样的档位名必须仍读回字符串（不得退化成 int/bool/None）。"""
    src = flow_to_sexpr(_ir(backend))
    value = _pick(compile_sexpr(src)["nodes"])["sandbox_backend"]
    assert value == backend
    assert isinstance(value, str)


def test_sexpr_writer_reads_camel_ir_spelling():
    """手写 camel IR（``sandboxBackend``）同样发射，与 #200/#207 双读口径一致。"""
    src = flow_to_sexpr(_ir("subprocess", key="sandboxBackend"))
    assert ':sandbox-backend "subprocess"' in src
    assert _pick(compile_sexpr(src)["nodes"])["sandbox_backend"] == "subprocess"


def test_sexpr_absent_backend_emits_no_bytes():
    """字段缺席时不发明 ``:sandbox-backend``——保留运行期默认档位语义。"""
    src = flow_to_sexpr(_ir(include=False))
    assert ":sandbox-backend" not in src
    node = _pick(compile_sexpr(src)["nodes"])
    assert "sandbox_backend" not in node


def test_sexpr_cross_front_flow_to_sexpr_to_flow():
    """``@flow`` 产出的 IR → sexpr → 再编译：两前端对同一档位口径一致。"""
    flow_src = emit_source(_ir("restricted"))
    assert "sandbox_backend='restricted'" in flow_src
    flow_ir = compile_source(flow_src)
    assert _pick(flow_ir["nodes"])["sandbox_backend"] == "restricted"
    sexpr_src = flow_to_sexpr(flow_ir)
    assert _pick(compile_sexpr(sexpr_src)["nodes"])["sandbox_backend"] == "restricted"


# ---------------------------------------------------------------------------
# 读侧：关键字别名 + 保真
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "keyword", ["sandbox-backend", "sandbox_backend", "sandboxBackend"])
def test_sexpr_reader_accepts_keyword_aliases(keyword):
    src = _sexpr_src(f':{keyword} "subprocess"')
    assert _pick(compile_sexpr(src)["nodes"])["sandbox_backend"] == "subprocess"


# ---------------------------------------------------------------------------
# 负控面：不可忠实表达的值两侧 fail-closed
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", [123, True, ["subprocess"], {"b": "subprocess"}, ""])
def test_sexpr_writer_fail_closed_on_unrepresentable_backend(value):
    """写侧不静默丢：非字符串 / 空串无法忠实回读，必须响亮拒绝。"""
    with pytest.raises(SexprEmitError, match="sandbox_backend"):
        flow_to_sexpr(_ir(value))


@pytest.mark.parametrize(
    "fragment, message",
    [
        (":sandbox-backend 1", "非空字符串"),
        (":sandbox-backend true", "非空字符串"),
        (':sandbox-backend ""', "非空字符串"),
        (':sandbox-backend (list "subprocess")', "非空字符串"),
        (":sandbox-backend (dict :b unsafeprofile)", "非空字符串"),
    ],
)
def test_sexpr_reader_fail_closed_on_unrepresentable_backend(fragment, message):
    """读侧不把非法形状留到运行期：解析期报 SyntaxError。"""
    with pytest.raises(SyntaxError, match=message):
        compile_sexpr(_sexpr_src(fragment))
