"""plaita#203（安全）：映射容器只允许数据——host mapping-like 在读取前具名拒绝。

#133 只关闭了**属性**遍历（``$NODE.h.os.environ``）。映射面仍有两条路：

* **容器作值**：``$NODE.h.env`` / ``$NODE.d.env`` 把 live ``os._Environ``
  原样返回；随后的 ``.HOME`` 走映射键读取；
* **``$F.get`` 旁路**：``$F.get($NODE.h.env, 'HOME')`` 直接调 ``_Environ.get``。

本文件锁定 #203 落地后的口径：

* **容器**：真实 ``dict``（含子类，读 ``dict`` 的 C 槽）与引擎的
  ``CheckpointState`` 是数据；``os._Environ`` / ``ChainMap`` / ``UserDict`` /
  ``MappingProxyType`` / 自定义 ``Mapping`` / duck-typed ``__getitem__`` + ``get``
  一律**具名 ``ValueError``（plaita#203）**，在入口 / 结果值 / 索引结果 / 根值
  四个位置都拒；
* **``$F.get`` / ``$F.getDictValue``**：第一个实参是 host mapping-like 时在
  ``#68`` 输入测量和 callee **之前**拒绝；返回值是 host mapping-like 时也拒绝；
  真实 dict 走 ``dict.get`` C 槽（覆写的 ``get``/``__getitem__`` 零调用），
  ``CheckpointState`` 走引擎 ``get``；
* **数据面不变**：plain dict、只加方法的 dict 子类、``CheckpointState``、
  dataclass / Pydantic / SimpleNamespace、``$ENV`` allowlist 语义都不变；
* **零调用**：poisoned ``Mapping`` 的 ``__getitem__`` / ``get`` 计数器在
  ``$NODE.pm.KEY`` / ``$NODE.d.pm`` / ``$F.get($NODE.d, 'pm')`` 上必须为 **0**。

负控（关闸即红）见 ``docs/proofs/203-data-mapping-traversal.md`` 与
``docs/proofs/data/203-negative-control.txt``。
"""
from __future__ import annotations

import os
from collections import ChainMap, UserDict
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import ClassVar

import pytest
from pydantic import BaseModel

import plaita.node as pn
from plaita import Node
from plaita.core._data_mapping import is_data_mapping, is_mapping_like
from plaita.core.errors import NodeExecutionError
from plaita.core.expression import (
    ExpressionRegistry,
    FunctionCategory,
    _fn_get,
    _fn_getDictValue,
)
from plaita.core.expression_parser import ExpressionParser
from plaita.core.flow import Flow
from plaita.core.state import CheckpointState

REPO_ROOT = Path(__file__).resolve().parents[2]

# 运行时文案的**逐字**副本：代码 / 文档 / 测试三处必须一致。刻意手写而非
# import 模板，改动代码文案时本测试必须随之失败。
ISSUE203_MESSAGE = (
    "expression path '$NODE.h.env' read a non-data mapping container of type "
    "'_Environ': only plain dict (including subclasses, read through dict's C "
    "slots) and the engine's CheckpointState may be traversed as a mapping "
    "(plaita#203). Host mapping-like objects such as os.environ / ChainMap / "
    "UserDict / custom Mapping are refused by name before any key or value is "
    "read; convert the data to a plain dict first."
)


class PoisonMapping(Mapping):
    """非 dict 的 host mapping：计数 ``__getitem__`` / ``get`` 的调用。"""

    def __init__(self, data):
        self._data = dict(data)
        self.reads = 0

    def __getitem__(self, key):
        self.reads += 1
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def get(self, key, default=None):
        self.reads += 1
        return self._data.get(key, default)


class DuckMapping:
    """duck-typed ``__getitem__`` + ``get``，但不注册 ``Mapping``。"""

    def __init__(self, data):
        self._data = dict(data)
        self.reads = 0

    def __getitem__(self, key):
        self.reads += 1
        return self._data[key]

    def get(self, key, default=None):
        self.reads += 1
        return self._data.get(key, default)


class DataDict(dict):
    """只加方法的 dict 子类：数据面（#203 兼容性正控）。"""

    def helper(self):
        return "helper"


class OverrideDict(dict):
    """dict 子类覆写 ``get`` / ``__getitem__``：引擎必须读真实存储。"""

    def __init__(self, data):
        super().__init__(data)
        self.override_reads = 0

    def get(self, key, default=None):
        self.override_reads += 1
        return "OVERRIDE"

    def __getitem__(self, key):
        self.override_reads += 1
        return "OVERRIDE"


@dataclass
class DataPayload:
    name: str = "alice"


class PlainPayload:
    def __init__(self):
        self.name = "alice"


class ModelPayload(BaseModel):
    name: str = "alice"


@pytest.fixture()
def parser():
    return ExpressionParser.for_prefix("$")


def _host_mappings():
    return [
        ("environ", os.environ),
        ("chainmap", ChainMap({"SECRET": "s1"})),
        ("userdict", UserDict({"SECRET": "s2"})),
        ("mappingproxy", MappingProxyType({"SECRET": "s3"})),
        ("custom", PoisonMapping({"SECRET": "s4"})),
        ("duck", DuckMapping({"SECRET": "s5"})),
    ]


def test_container_as_value_and_base_is_rejected(parser):
    """验收 #1：容器作值、作基座都拒绝；数据值不受影响。"""
    ctx = {
        "$NODE": {
            "h": SimpleNamespace(env=os.environ, text="hello"),
            "d": {"env": os.environ, "k": "v"},
        }
    }
    for expr in ("$NODE.h.env", "$NODE.h.env.HOME",
                 "$NODE.d.env", "$NODE.d.env.HOME"):
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate(expr, ctx)
        message = str(excinfo.value)
        assert "plaita#203" in message, expr
        assert "_Environ" in message, expr
        assert "environ(" not in message, expr
    assert parser.evaluate("$NODE.h.text", ctx) == "hello"
    assert parser.evaluate("$NODE.d.k", ctx) == "v"


def test_host_mapping_variants_are_rejected(parser):
    """验收 #3：os.environ / ChainMap / UserDict / MappingProxyType / 自定义 Mapping。"""
    for label, value in _host_mappings():
        ctx = {"$NODE": {"h": SimpleNamespace(v=value), "d": {"v": value}}}
        # The container gate rejects before the key is read, so the key need
        # not exist in os.environ.
        for expr in ("$NODE.h.v.SECRET",
                     "$NODE.d.v.SECRET",
                     "$NODE.h.v"):
            with pytest.raises(ValueError) as excinfo:
                parser.evaluate(expr, ctx)
            message = str(excinfo.value)
            assert "plaita#203" in message, (label, expr)
            assert type(value).__name__ in message, (label, expr)


def test_poisoned_mapping_has_zero_reads(parser):
    """验收 #2：拒绝发生在读键之前——poisoned 计数必须为 0。"""
    pm = PoisonMapping({"SECRET": "s"})
    ctx = {"$NODE": {"pm": pm, "d": {"pm": pm}}}
    for expr in ("$NODE.pm.SECRET", "$NODE.d.pm", "$NODE.d.pm.SECRET",
                 "$F.get($NODE.d, 'pm')", "$F.getDictValue($NODE.d, 'pm')"):
        with pytest.raises(ValueError, match="plaita#203"):
            parser.evaluate(expr, ctx)
        assert pm.reads == 0, expr


def test_dollar_f_get_bypass_is_rejected(parser):
    """验收 #3：`$F.get($NODE.h.env, 'HOME')` 旁路必须拒。"""
    ctx = {"$NODE": {"h": SimpleNamespace(env=os.environ)}}
    for expr in ("$F.get($NODE.h.env, 'HOME')",
                 "$F.getDictValue($NODE.h.env, 'HOME')"):
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate(expr, ctx)
        message = str(excinfo.value)
        assert "plaita#203" in message, expr
        assert "_Environ" in message, expr
        assert "environ(" not in message, expr


def test_dollar_f_get_result_value_is_rejected(parser):
    """数据 dict 里挂 host mapping：`$F.get` 的返回值同样是值位置。"""
    ctx = {"$NODE": {"d": {"env": os.environ}}}
    for expr in ("$F.get($NODE.d, 'env')",
                 "$F.getDictValue($NODE.d, 'env')"):
        with pytest.raises(ValueError, match="plaita#203"):
            parser.evaluate(expr, ctx)


def test_dollar_f_get_function_result_is_rejected(parser):
    """`$F.get` 预检：host 函数返回 mapping-like 时在测量/callee 之前拒绝。"""
    registry = ExpressionRegistry()
    registry.register("probe_env", lambda: os.environ, FunctionCategory.DICT)
    registry.register("get", _fn_get, FunctionCategory.DICT)
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$F.get($F.probe_env(), 'HOME')", {}, registry)
    message = str(excinfo.value)
    assert "plaita#203" in message
    assert "$F.get(...)" in message
    assert "_Environ" in message


def test_dollar_f_get_direct_call_is_gated():
    """`_fn_get` 自身也判（不只依赖解析器预检）。"""
    with pytest.raises(ValueError, match="plaita#203"):
        _fn_get(os.environ, "HOME")
    with pytest.raises(ValueError, match="plaita#203"):
        _fn_getDictValue(ChainMap({"SECRET": "s"}), "SECRET")


def test_index_result_is_rejected(parser):
    ctx = {"$NODE": {"lst": [os.environ]}}
    for expr in ("$NODE.lst[0]", "$NODE.lst[0].HOME"):
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate(expr, ctx)
        message = str(excinfo.value)
        assert "plaita#203" in message, expr
        assert "$NODE.lst[0]" in message, expr
        assert "_Environ" in message, expr


def test_root_host_mapping_is_rejected(parser):
    ctx = {"$INPUT": os.environ}
    for expr in ("$INPUT", "$INPUT.HOME"):
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate(expr, ctx)
        message = str(excinfo.value)
        assert "plaita#203" in message, expr
        assert "$INPUT" in message, expr
        assert "_Environ" in message, expr


def test_data_mappings_stay_readable(parser):
    """验收 #4：plain dict / 只加方法的 dict 子类 / CheckpointState / 三类对象。"""
    ctx = {
        "$NODE": {
            "d": {"k": "v"},
            "ds": DataDict({"k": "sub"}),
            "dc": DataPayload(),
            "p": PlainPayload(),
            "m": ModelPayload(),
            "ns": SimpleNamespace(name="alice"),
        },
        "$INPUT": {"x": "input-x"},
    }
    assert parser.evaluate("$NODE.d.k", ctx) == "v"
    assert parser.evaluate("$NODE.ds.k", ctx) == "sub"
    assert parser.evaluate("$NODE.dc.name", ctx) == "alice"
    assert parser.evaluate("$NODE.p.name", ctx) == "alice"
    assert parser.evaluate("$NODE.m.name", ctx) == "alice"
    assert parser.evaluate("$NODE.ns.name", ctx) == "alice"
    assert parser.evaluate("$INPUT.x", ctx) == "input-x"
    assert parser.evaluate("$F.get($NODE.d, 'k')", ctx) == "v"
    assert parser.evaluate("$F.get($NODE.ds, 'k')", ctx) == "sub"
    assert parser.evaluate("$F.getDictValue($NODE.d, 'k')", ctx) == "v"


def test_checkpoint_state_backed_input_stays_readable(parser):
    """验收 #4：CheckpointState 背书的 $INPUT 与 `$F.get` 都不回归。"""
    state = CheckpointState()
    state["$INPUT"] = {"x": "input-x"}
    assert parser.evaluate("$INPUT.x", state) == "input-x"
    assert parser.evaluate("$F.get($INPUT, 'x')", state) == "input-x"
    assert parser.evaluate("$F.getDictValue($INPUT, 'x')", state) == "input-x"


def test_dict_subclass_overrides_are_not_executed(parser):
    """兼容性：dict 子类走 C 槽；覆写的 get/__getitem__ 零调用。"""
    od = OverrideDict({"k": "real"})
    ctx = {"$NODE": {"od": od}}
    assert parser.evaluate("$NODE.od.k", ctx) == "real"
    assert parser.evaluate("$F.get($NODE.od, 'k')", ctx) == "real"
    assert parser.evaluate("$F.getDictValue($NODE.od, 'k')", ctx) == "real"
    assert od.override_reads == 0
    assert parser.evaluate("$F.get($NODE.od, 'missing')", ctx) is None


def test_env_allowlist_semantics_unchanged(parser):
    """验收 #5：$ENV 的显式暴露语义不变。"""
    assert parser.evaluate("$ENV.HOME", {"$ENV": {}}) is None
    assert parser.evaluate("$ENV.HOME", {"$ENV": {"HOME": "h"}}) == "h"
    assert parser.evaluate("$F.get($ENV, 'HOME')",
                           {"$ENV": {"HOME": "h"}}) == "h"


def test_predicate_split():
    assert is_data_mapping({})
    assert is_data_mapping(DataDict({"k": "v"}))
    assert is_data_mapping(CheckpointState())
    for _, value in _host_mappings():
        assert not is_data_mapping(value)
        assert is_mapping_like(value)
    for value in ([], (), "text", 42, None, DataPayload(), ModelPayload(),
                  SimpleNamespace()):
        assert not is_mapping_like(value)


def test_runtime_message_is_exact(parser):
    ctx = {"$NODE": {"h": SimpleNamespace(env=os.environ)}}
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$NODE.h.env", ctx)
    assert str(excinfo.value) == ISSUE203_MESSAGE


def test_docs_quote_issue203_message_verbatim():
    targets = [
        REPO_ROOT / "MIGRATION.md",
        REPO_ROOT / "docs-site/docs/guide/expressions.md",
    ]
    missing = [str(p) for p in targets if not p.exists()]
    if missing:
        pytest.skip(f"文档不在当前树内（mutants/ 副本）：{missing}")
    for path in targets:
        text = path.read_text(encoding="utf-8")
        assert ISSUE203_MESSAGE in text, f"{path.name} 未逐字引用 #203 运行时文案"


def test_error_does_not_echo_values(monkeypatch, parser):
    monkeypatch.setenv("PLAITA_ISSUE203_SECRET", "sk-DUMMY-203-SECRET")
    ctx = {"$NODE": {"h": SimpleNamespace(env=os.environ)}}
    for expr in ("$NODE.h.env.HOME", "$F.get($NODE.h.env, 'HOME')"):
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate(expr, ctx)
        assert "sk-DUMMY-203-SECRET" not in str(excinfo.value), expr


class Issue203PayloadNode(Node):
    node_type: ClassVar[str] = "issue203_payload_probe"

    def execute(self, execution):
        return SimpleNamespace(env=os.environ, text="hello")


def _probe_flow(output: str) -> Flow:
    # 方法级注册：全量套件里其他用例会重置默认 registry。
    pn.get_default_registry().register(Issue203PayloadNode)
    return Flow.model_validate({
        "flow_id": "issue203-probe",
        "name": "issue203-probe",
        "nodes": [
            {"id": "s", "type": "start", "next": "n1"},
            {"id": "n1", "type": "issue203_payload_probe", "next": "end"},
            {"id": "end", "type": "end", "output": output},
        ],
    })


def test_flow_mapping_chain_raises_and_never_leaks_env(monkeypatch):
    monkeypatch.setenv("PLAITA_ISSUE203_SECRET", "sk-DUMMY-203-SECRET")
    for output in ("{% $NODE.n1.env.HOME %}",
                   "{% $F.get($NODE.n1.env, 'HOME') %}"):
        flow = _probe_flow(output)
        with pytest.raises(NodeExecutionError) as excinfo:
            flow.run({})
        message = str(excinfo.value)
        assert "plaita#203" in message, output
        assert "sk-DUMMY-203-SECRET" not in message, output
        assert isinstance(excinfo.value.__cause__, ValueError), output


def test_flow_data_attribute_still_works():
    flow = _probe_flow("$NODE.n1.text")
    assert flow.run({}) == "hello"
