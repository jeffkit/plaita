"""plaita#133（安全）：属性遍历只允许数据——代码对象按**真实类型**具名拒绝。

修复前 ``_get_attr`` 只拦 ``_`` 前缀；公有属性可以把模块 / 函数 / 类 / frame
等挂在节点返回值上，``$NODE.h.os.environ`` 因而能读到 worker 进程环境变量。
本文件锁定 #133 落地后的口径：

* **入口闸**：``_get_attr`` 在任何属性探针之前先看 ``type(obj).__mro__``，命中
  module / function / builtin function / method / code / frame / traceback /
  generator / coroutine / async generator / class（``type``）即抛具名
  ``ValueError``（含 ``plaita#133``、路径与真实类型名）；
* **结果闸**：属性路径解析出的**值**若是代码对象同样拒绝——``$NODE.h.os``
  本身即抛错，不会先返回模块再等下一段；
* **数据面不变**：dataclass / 普通类 / Pydantic / SimpleNamespace 的数据字段、
  dict 键、列表索引、``$F.*`` 映射辅助与 ``$ENV`` allowlist 语义都不变；
* **映射 / 索引路径**：键与容器按原语义读取（``os.environ`` 作为**容器**仍是
  mapping 数据面；本闸门只针对属性遍历）。

测试是性质断言：每种代码对象都单独跑「值位置 + 基座位置」，并有一个负控脚本
（docs/proofs/data/133-negative-control.txt）证明把闸门关掉后这些断言会红。
"""
from __future__ import annotations

import asyncio
import os
import sys
import types
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest
from pydantic import BaseModel

import plaita.node as pn
from plaita import Node
from plaita.core.errors import NodeExecutionError
from plaita.core.expression_parser import (
    ExpressionParser,
    _get_attr,
    _is_code_object,
)
from plaita.core.flow import Flow
from plaita.core.state import CheckpointState

REPO_ROOT = Path(__file__).resolve().parents[2]

# 运行时文案的**逐字**副本：代码/文档/测试三处必须一致。刻意手写而非 import
# 模板，改动代码文案时本测试必须随之失败。
ISSUE133_MESSAGE = (
    "expression path '$NODE.h.os' traverses a non-data object of type 'module': "
    "attribute traversal is data-only (plaita#133), so modules, functions, "
    "builtin functions, methods, classes, frames, tracebacks, generators, "
    "coroutines, async generators and code objects are refused by name. Read a "
    "data field instead; mapping keys and numeric indices are unchanged."
)


class WithMethod:
    def method(self):
        return 1


def _make_traceback():
    try:
        raise RuntimeError("issue133 traceback probe")
    except RuntimeError:
        return sys.exc_info()[2]


async def _coroutine_body():
    return 1


async def _async_generator_body():
    yield 1


def _make_coroutine():
    return _coroutine_body()


def _make_async_generator():
    return _async_generator_body()


#: 每种被拒对象一个工厂：参数化测试各拿一份新实例，避免共享可消耗对象
#: （generator / coroutine / async generator）。
REJECTED_FACTORIES = [
    ("module", lambda: os),
    ("function", lambda: (lambda: 1)),
    ("builtin_function", lambda: len),
    ("builtin_method", lambda: "x".upper),
    ("bound_method", lambda: WithMethod().method),
    ("class", lambda: WithMethod),
    ("frame", lambda: sys._getframe()),
    ("traceback", _make_traceback),
    ("generator", lambda: (i for i in range(1))),
    ("coroutine", _make_coroutine),
    ("async_generator", _make_async_generator),
    ("code", lambda: (lambda: 1).__code__),
]


def _close(obj) -> None:
    """Close consumable objects so pytest does not warn about un-awaited ones."""
    if isinstance(obj, types.GeneratorType):
        obj.close()
    elif isinstance(obj, types.CoroutineType):
        obj.close()
    elif isinstance(obj, types.AsyncGeneratorType):
        asyncio.run(obj.aclose())


@pytest.fixture()
def parser():
    return ExpressionParser.for_prefix("$")


@pytest.mark.parametrize("label,factory", REJECTED_FACTORIES)
def test_rejected_type_is_recognized(label, factory):
    obj = factory()
    try:
        assert _is_code_object(obj), label
    finally:
        _close(obj)


@pytest.mark.parametrize("label,factory", REJECTED_FACTORIES)
def test_get_attr_refuses_code_object_as_base(label, factory):
    """入口闸：代码对象作为**基座**时，任何属性名都具名拒绝。"""
    obj = factory()
    try:
        with pytest.raises(ValueError) as excinfo:
            _get_attr(obj, "public_name")
        message = str(excinfo.value)
        assert "plaita#133" in message, label
        assert "public_name" in message, label
        assert type(obj).__name__ in message, label
        assert "refused by name" in message, label
    finally:
        _close(obj)


@pytest.mark.parametrize("label,factory", REJECTED_FACTORIES)
def test_parser_refuses_code_object_as_attribute_value(parser, label, factory):
    """结果闸：公有属性解析出的**值**是代码对象时，该字段本身即拒绝。"""
    obj = factory()
    try:
        ctx = {"$NODE": {"h": SimpleNamespace(v=obj)}}
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate("$NODE.h.v", ctx)
        message = str(excinfo.value)
        assert "plaita#133" in message, label
        assert "$NODE.h.v" in message, label
        assert type(obj).__name__ in message, label
    finally:
        _close(obj)


@pytest.mark.parametrize("label,factory", REJECTED_FACTORIES)
def test_parser_refuses_code_object_as_base(parser, label, factory):
    """入口闸从表达式面可达：映射取出的代码对象再走一段属性即拒绝。"""
    obj = factory()
    try:
        ctx = {"$NODE": {"v": obj}}
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate("$NODE.v.public_name", ctx)
        message = str(excinfo.value)
        assert "plaita#133" in message, label
        assert "$NODE.v.public_name" in message, label
        assert type(obj).__name__ in message, label
    finally:
        _close(obj)


@pytest.mark.parametrize(
    "value",
    [
        {}, [], (), set(), frozenset(), "", "text", 0, 1.5, True, None,
        b"bytes", bytearray(b"ba"), SimpleNamespace(x=1),
        Decimal("1.5"), datetime.now(),
    ],
)
def test_data_values_are_not_code_objects(value):
    """白名单的否定面：普通数据不能被入口/结果闸误伤。"""
    assert not _is_code_object(value)


@dataclass
class DataPayload:
    name: str = "alice"
    count: int = 2


class PlainPayload:
    def __init__(self):
        self.name = "alice"
        self.count = 2


class ModelPayload(BaseModel):
    name: str = "alice"
    count: int = 2


def test_public_data_attributes_stay_readable(parser):
    """三类合法用法（issue 验收第 2 条）：dataclass / 普通类 / Pydantic。"""
    ctx = {
        "$NODE": {
            "d": DataPayload(),
            "p": PlainPayload(),
            "m": ModelPayload(),
            "ns": SimpleNamespace(name="alice", count=2),
        },
        "$INPUT": {"d": DataPayload()},
    }
    assert parser.evaluate("$NODE.d.name", ctx) == "alice"
    assert parser.evaluate("$NODE.d.count", ctx) == 2
    assert parser.evaluate("$NODE.p.name", ctx) == "alice"
    assert parser.evaluate("$NODE.m.name", ctx) == "alice"
    assert parser.evaluate("$NODE.m.count", ctx) == 2
    assert parser.evaluate("$NODE.ns.name", ctx) == "alice"
    assert parser.evaluate("$INPUT.d.name", ctx) == "alice"


def test_mapping_and_index_paths_are_unchanged(parser):
    """issue Boundary：映射键 / 索引路径不改；显式映射辅助也不改。"""
    ctx = {
        "$NODE": {
            "d": {"x": 1, "_k": "kept", "__class__": "data-value"},
            "lst": [{"x": 2}],
        }
    }
    assert parser.evaluate("$NODE.d.x", ctx) == 1
    assert parser.evaluate("$NODE.d._k", ctx) == "kept"
    assert parser.evaluate("$NODE.lst[0].x", ctx) == 2
    assert parser.evaluate("$F.get($NODE.d, 'x')", ctx) == 1
    # 键是数据、不是属性：dunder 键仍可经显式映射辅助读出（#55 的刻意不对称）
    assert parser.evaluate("$F.get($NODE.d, '__class__')", ctx) == "data-value"
    assert sorted(parser.evaluate("$F.keys($NODE.d)", ctx)) == [
        "__class__", "_k", "x",
    ]


def test_all_roots_are_gated(parser):
    """$NODE / $INPUT / $GLOBAL / $PARENT.$INPUT 四个根对称（issue 验收第 1 条）。"""
    ctx = {
        "$NODE": {"h": SimpleNamespace(os=os)},
        "$INPUT": {"h": SimpleNamespace(os=os)},
        "$GLOBAL": {"h": SimpleNamespace(os=os)},
    }
    for expr in ("$NODE.h.os", "$INPUT.h.os", "$GLOBAL.h.os"):
        with pytest.raises(ValueError, match="plaita#133"):
            parser.evaluate(expr, ctx)

    parent = CheckpointState()
    parent["$INPUT"] = {"h": SimpleNamespace(os=os)}
    child = CheckpointState()
    child["$PARENT"] = parent
    with pytest.raises(ValueError, match="plaita#133"):
        parser.evaluate("$PARENT.$INPUT.h.os", child)


def test_error_names_full_path_and_real_type(parser):
    ctx = {"$NODE": {"h": SimpleNamespace(os=os)}}
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$NODE.h.os.environ", ctx)
    message = str(excinfo.value)
    assert "$NODE.h.os" in message
    assert "'module'" in message
    # 拒绝发生在结果闸（.os），环境变量根本没有被读取
    assert "environ(" not in message


def test_runtime_message_is_exact(parser):
    """运行时文案逐字等于本文件常量（代码/文档/测试三处同步）。"""
    ctx = {"$NODE": {"h": SimpleNamespace(os=os)}}
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$NODE.h.os", ctx)
    assert str(excinfo.value) == ISSUE133_MESSAGE


def test_docs_quote_issue133_message_verbatim():
    """MIGRATION.md 与表达式指南必须逐字引用同一串。"""
    targets = [
        REPO_ROOT / "MIGRATION.md",
        REPO_ROOT / "docs-site/docs/guide/expressions.md",
    ]
    missing = [str(p) for p in targets if not p.exists()]
    if missing:
        pytest.skip(f"文档不在当前树内（mutants/ 副本）：{missing}")
    for path in targets:
        text = path.read_text(encoding="utf-8")
        assert ISSUE133_MESSAGE in text, (
            f"{path.name} 未逐字引用 #133 运行时文案"
        )


def test_rejection_message_makes_no_sandbox_promise(parser):
    """错误文案不得把"改用公有属性"写成安全承诺。"""
    ctx = {"$NODE": {"h": SimpleNamespace(os=os)}}
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$NODE.h.os", ctx)
    message = str(excinfo.value)
    assert "Use a public attribute name" not in message
    assert "stay readable by design" not in message
    assert "Only this prefix is gated" not in message


def test_error_does_not_echo_values(monkeypatch, parser):
    """消息只给类型名，不回显对象内容（可能含凭据）。"""
    monkeypatch.setenv("PLAITA_ISSUE133_SECRET", "sk-DUMMY-133-SECRET")
    ctx = {"$NODE": {"h": SimpleNamespace(os=os)}}
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$NODE.h.os.environ", ctx)
    assert "sk-DUMMY-133-SECRET" not in str(excinfo.value)


class HostileClass:
    @property
    def __class__(self):  # pragma: no cover - must never be called by the predicate
        raise RuntimeError("hostile __class__")


def test_predicate_uses_real_type_not_dunder_class():
    """#163 纪律：判据走 type(obj).__mro__，不读对象自己的 __class__。"""
    assert _is_code_object(HostileClass()) is False


class LyingMeta(type):
    def __eq__(cls, other):
        return other is type

    __hash__ = type.__hash__


class LyingData(metaclass=LyingMeta):
    pass


def test_predicate_uses_identity_not_metaclass_eq():
    """成员判定必须是 ``is``：元类 __eq__ 谎称自己是 type 不得误拒数据实例。"""
    assert _is_code_object(LyingData()) is False


class Issue133PayloadNode(Node):
    node_type: ClassVar[str] = "issue133_payload_probe"

    def execute(self, execution):
        return SimpleNamespace(os=os, text="hello")


def _probe_flow(output: str) -> Flow:
    # 方法级注册：其他测试会重置/替换默认 registry 实例（全量套件里模块级
    # 注册会被清掉），方法级注册保证本文件在任意执行顺序下可用。
    pn.get_default_registry().register(Issue133PayloadNode)
    return Flow.model_validate({
        "flow_id": "issue133-probe",
        "name": "issue133-probe",
        "nodes": [
            {"id": "s", "type": "start", "next": "n1"},
            {"id": "n1", "type": "issue133_payload_probe", "next": "end"},
            {"id": "end", "type": "end", "output": output},
        ],
    })


def test_flow_output_chain_raises_and_never_leaks_env(monkeypatch):
    """端到端：节点返回 SimpleNamespace(os=os)，end output 走攻击链 ⇒ 节点错误。"""
    monkeypatch.setenv("PLAITA_ISSUE133_SECRET", "sk-DUMMY-133-SECRET")
    flow = _probe_flow("{% $NODE.n1.os.environ.HOME %}")
    with pytest.raises(NodeExecutionError) as excinfo:
        flow.run({})
    message = str(excinfo.value)
    assert "plaita#133" in message
    assert "sk-DUMMY-133-SECRET" not in message
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_flow_data_attribute_still_works():
    """端到端正控：同一节点的数据字段照旧可读。"""
    flow = _probe_flow("$NODE.n1.text")
    assert flow.run({}) == "hello"
