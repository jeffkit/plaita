"""plaita#55（安全）：表达式字段段不得直通 ``__class__`` / ``__globals__``。

修复前 ``$ROOT.field.field…`` 的每个非索引段都无条件交给 ``_get_attr``，而它
对带 ``__dict__`` 的对象直接 ``getattr``（无下划线过滤），段名字符集又把 ``_``
列为合法字符。于是节点 ``execute`` 返回的任意带 ``__dict__`` 的对象
（dataclass / pydantic / 普通实例）都能被
``$NODE.n1.__class__.__init__.__globals__.os.environ`` 走到 worker 进程环境变量，
完全绕开 ``$ENV`` 的 expose allowlist（``plaita.core.context._safe_environment``）。

本文件锁定修复后的口径。检查只在唯一收口点 ``_get_attr`` 做一次，分两条路径：

1. **属性路径**（``getattr`` 回退）：任何 ``_`` 开头的段名一律抛 ``ValueError``
   ——硬报错，不返回 ``None``（返回 ``None`` 会把越权尝试伪装成"键不存在"）；
2. **映射路径**（``dict`` / ``__getitem__`` + ``get``）：段名是**键**不是属性，
   单下划线键保持可读（``$NODE._n2`` 这类 builder 生成的节点 id、刻意存
   ``_foo`` 键的 dict）；``__`` 开头的键（dunder）同样拒绝——它对任何流程都
   不是合法数据键，静默 ``None`` 只会再造一个"看起来被挡住了"的假象。

异常类型取 ``ValueError``：issue 明确建议，且表达式层已有同类
（``ExpressionRegistry.register``）。消息只点名被拒段名与原因，不回显对象。
"""
from dataclasses import dataclass
from types import SimpleNamespace
from typing import ClassVar

import pytest

import plaita.node as pn
from plaita import Node
from plaita.core.errors import NodeExecutionError
from plaita.core.expression_parser import ExpressionParser, _get_attr
from plaita.core.flow import Flow
from plaita.core.state import CheckpointState

# issue 点名的枚举面：dunder / 私有名，逐个在第一、第二段位置尝试。
DUNDERS = [
    "__class__",
    "__init__",
    "__globals__",
    "__subclasses__",
    "__bases__",
    "__mro__",
    "__getattribute__",
    "__getattr__",
    "__reduce__",
    "__builtins__",
    "__code__",
    "__func__",
    "__self__",
    "__module__",
    "__loader__",
    "__spec__",
]
PRIVATE_NAMES = ["_private"]


@dataclass
class Payload:
    text: str = "hello"
    _private: str = "internal"


class Plain:
    def __init__(self):
        self.text = "hello"
        self._private = "internal"


class LoudRepr:
    """消息回显探针：``repr`` 自带哨兵，断言异常消息不回显对象内容。"""

    def __repr__(self):  # pragma: no cover - asserted indirectly
        return "<SENTINEL-INTERNAL-OBJECT>"


class MappingLike:
    """非 dict 的映射（``__getitem__`` + ``get``）——数据路径，键不是属性。"""

    def __init__(self, data):
        self._data = dict(data)

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)


def _context():
    return {
        "$NODE": {
            "a": Payload(),
            "p": Plain(),
            "ns": SimpleNamespace(text="hi", _private="internal"),
            "loud": LoudRepr(),
            "lst": [Payload()],
            "d": {"x": 1, "_k": "kept"},
            "map": MappingLike({"_k": "kept", "x": 1}),
            "_n2": "node-id-value",
        },
        "$INPUT": {"_k": "kept", "x": 1},
    }


@pytest.fixture()
def parser():
    return ExpressionParser.for_prefix("$")


# ---------------------------------------------------------------------------
# 完整的攻击链（issue 原文 repro + 逐段前缀）
# ---------------------------------------------------------------------------

ATTACK_CHAIN = [
    "$NODE.a.__class__",
    "$NODE.a.__dict__",
    "$NODE.a.__class__.__init__",
    "$NODE.a.__class__.__init__.__globals__",
    "$NODE.a.__class__.__init__.__globals__.os.environ",
    "$NODE.a.__class__.__init__.__globals__.__builtins__",
    "$NODE.p.__class__.__mro__",
    "$NODE.lst[0].__class__",
]


@pytest.mark.parametrize("expr", ATTACK_CHAIN)
def test_attack_chain_raises_at_first_rejected_segment(parser, expr):
    """issue 的整条链路（含两/三段深）必须抛错，且点名被拒段。"""
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate(expr, _context())
    message = str(excinfo.value)
    assert "'__class__'" in message or "'__dict__'" in message
    assert "is not allowed" in message


def test_globals_chain_stops_at_the_first_reserved_segment(parser):
    """拒绝发生在链路第一步（``__class__``），模块 globals 根本不会被取到。

    被拒段名 = 第一个 ``_`` 开头的段：若求值曾推进到 ``__init__`` /
    ``__globals__``，消息里的段名就会是它们——以此证明是**早停**而非事后兜底。
    """
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$NODE.a.__class__.__init__.__globals__", _context())
    message = str(excinfo.value)
    assert "'__class__'" in message
    assert "'__globals__'" not in message
    assert "'__init__'" not in message


# ---------------------------------------------------------------------------
# 枚举：每个 dunder / 私有名，第一、第二、索引后段位置
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", DUNDERS)
def test_dunder_rejected_at_every_segment_position(parser, name):
    """dunder 在映射路径与属性路径都拒绝：段名出现在消息里，绝不静默 None。"""
    ctx = _context()
    expressions = [
        f"$NODE.a.{name}",        # 第 2 段：节点结果是对象 → 属性路径
        f"$NODE.p.{name}",        # 第 2 段：普通类实例
        f"$NODE.ns.{name}",       # 第 2 段：SimpleNamespace
        f"$NODE.lst[0].{name}",   # 索引段之后
        f"$NODE.{name}",          # 第 1 段：$NODE 是 dict → 映射路径
        f"$INPUT.{name}",         # 第 1 段：$INPUT 是 dict → 映射路径
    ]
    for expr in expressions:
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate(expr, ctx)
        message = str(excinfo.value)
        assert repr(name) in message, expr
        assert "is not allowed" in message, expr
        assert "_" in message, expr


@pytest.mark.parametrize("name", PRIVATE_NAMES)
def test_private_name_rejected_on_attribute_path(parser, name):
    """单下划线私有**属性**同样拒绝（getattr 分支），且点名段名。"""
    ctx = _context()
    for expr in (f"$NODE.a.{name}", f"$NODE.p.{name}", f"$NODE.ns.{name}",
                 f"$NODE.lst[0].{name}"):
        with pytest.raises(ValueError) as excinfo:
            parser.evaluate(expr, ctx)
        assert repr(name) in str(excinfo.value), expr


def test_private_key_stays_readable_on_mapping_path(parser):
    """单下划线**键**是数据路径，必须继续可读（口径边界，非漏洞）。"""
    ctx = _context()
    assert parser.evaluate("$NODE.d._k", ctx) == "kept"
    assert parser.evaluate("$INPUT._k", ctx) == "kept"
    assert parser.evaluate("$NODE.map._k", ctx) == "kept"
    # 与既有 builder 节点 id 同形：`$NODE._n2`（test_dsl.py 的自动 id）
    assert parser.evaluate("$NODE._n2", ctx) == "node-id-value"
    # 单下划线键缺失时仍走数据路径语义（None），与 `$NODE.d.nope` 一致
    assert parser.evaluate("$NODE._missing_key", ctx) is None


def test_dunder_key_on_mapping_path_is_rejected_not_silent(parser):
    """dict 里的 dunder 键同样拒绝——返回 None 会再造"看起来被挡住"的假象。"""
    ctx = {"$NODE": {"d": {"__class__": "not-an-attribute"}}}
    with pytest.raises(ValueError) as excinfo:
        parser.evaluate("$NODE.d.__class__", ctx)
    assert "'__class__'" in str(excinfo.value)


# ---------------------------------------------------------------------------
# 拒绝的其它入口形态：函数参数、模板、raw 访问器、自定义前缀
# ---------------------------------------------------------------------------


def test_rejected_segment_raises_inside_function_args(parser):
    with pytest.raises(ValueError, match="__class__"):
        parser.evaluate("$F.concat($NODE.a.__class__, '!')", _context())


def test_rejected_segment_raises_inside_template(parser):
    with pytest.raises(ValueError, match="__class__"):
        parser.evaluate("{% $NODE.a.__class__ %}", _context())


def test_raw_accessor_does_not_disable_the_gate(parser):
    """``$NODE.raw.*`` 只关掉字符串二次求值，不关掉遍历闸门（#130 × #55）。"""
    ctx = {"$NODE": {"a": Payload()}}
    assert parser.evaluate("$NODE.raw.a.text", ctx) == "hello"
    with pytest.raises(ValueError, match="__class__"):
        parser.evaluate("$NODE.raw.a.__class__", ctx)


def test_custom_prefix_gets_the_same_gate():
    """``ExpressionParser.for_prefix("@")`` 口径一致（自定义前缀流程不回归）。"""
    custom = ExpressionParser.for_prefix("@")
    ctx = {"@NODE": {"a": Payload(), "d": {"_k": "kept"}}, "@INPUT": {"x": 1}}
    assert custom.evaluate("@NODE.a.text", ctx) == "hello"
    assert custom.evaluate("@NODE.d._k", ctx) == "kept"
    with pytest.raises(ValueError, match="__class__"):
        custom.evaluate("@NODE.a.__class__", ctx)
    with pytest.raises(ValueError, match="__globals__"):
        custom.evaluate("@NODE.a.__class__.__globals__", ctx)


# ---------------------------------------------------------------------------
# 阳性对照：正常属性 / 键 / 索引 / 模板 / $PARENT.$INPUT
# ---------------------------------------------------------------------------


def test_normal_attribute_access_still_works(parser):
    ctx = _context()
    assert parser.evaluate("$NODE.a.text", ctx) == "hello"
    assert parser.evaluate("$NODE.p.text", ctx) == "hello"
    assert parser.evaluate("$NODE.ns.text", ctx) == "hi"


def test_missing_public_attribute_still_returns_none(parser):
    """公开名缺失的历史语义（None）不变——只有下划线名硬报错。"""
    ctx = _context()
    assert parser.evaluate("$NODE.a.missing", ctx) is None
    assert parser.evaluate("$NODE.d.nope", ctx) is None
    assert parser.evaluate("$NODE.p.missing", ctx) is None


def test_dict_keys_and_indices_still_work(parser):
    ctx = _context()
    assert parser.evaluate("$NODE.d.x", ctx) == 1
    assert parser.evaluate("$NODE.d._k", ctx) == "kept"
    assert parser.evaluate("$NODE.lst[0].text", ctx) == "hello"
    assert parser.evaluate("$NODE.lst.0.text", ctx) == "hello"
    assert parser.evaluate("$INPUT._k", ctx) == "kept"


def test_templates_and_functions_still_work(parser):
    ctx = _context()
    assert parser.evaluate("{% $F.concat('hello ', 'world') %}", ctx) == "hello world"
    assert parser.evaluate("$F.concat($NODE.a.text, '!')", ctx) == "hello!"
    assert parser.evaluate("{% $NODE.a.text %}", ctx) == "hello"


def test_parent_input_segment_handling_not_regressed(parser):
    """#104/#130 强制回归：``$PARENT.$INPUT.name`` 在 live CheckpointState 上仍可解析。"""
    parent = CheckpointState()
    parent["$INPUT"] = {"name": "alice"}
    child = CheckpointState()
    child["$PARENT"] = parent
    assert parser.evaluate("$PARENT.$INPUT.name", child) == "alice"
    with pytest.raises(ValueError, match="__class__"):
        parser.evaluate("$PARENT.$INPUT.__class__", child)


def test_builder_generated_underscore_node_id_still_evaluates():
    """``$NODE._n2``（builder 自动 id）端到端仍可求值——映射路径不被误伤。"""
    from plaita.dsl import linear

    flow = (
        linear("issue55-ids", input_type="object")
        .start()
        .assignment(output="$F.upper($INPUT.x)")
        .end(output="$NODE._n2")
    ).build()
    assert flow.run(x="hi") == "HI"


# ---------------------------------------------------------------------------
# 收口点单测：`_get_attr` 两条路径的边界
# ---------------------------------------------------------------------------


def test_get_attr_mapping_path_allows_single_underscore_key():
    assert _get_attr({"k": "v"}, "k") == "v"
    assert _get_attr({"k": "v"}, "missing") is None
    assert _get_attr({"k": "v"}, "_k") is None          # 键缺失→None，不是拒绝
    assert _get_attr({"_k": "v"}, "_k") == "v"
    assert _get_attr(MappingLike({"_k": "v"}), "_k") == "v"


@pytest.mark.parametrize("name", DUNDERS)
def test_get_attr_mapping_path_rejects_dunder_key(name):
    with pytest.raises(ValueError) as excinfo:
        _get_attr({"x": 1}, name)
    assert repr(name) in str(excinfo.value)
    with pytest.raises(ValueError):
        _get_attr(MappingLike({"x": 1}), name)


@pytest.mark.parametrize("name", DUNDERS + PRIVATE_NAMES)
def test_get_attr_attribute_path_rejects_underscore(name):
    with pytest.raises(ValueError) as excinfo:
        _get_attr(Payload(), name)
    assert repr(name) in str(excinfo.value)
    with pytest.raises(ValueError):
        _get_attr(Plain(), name)


def test_get_attr_attribute_path_keeps_public_names():
    assert _get_attr(Payload(), "text") == "hello"
    assert _get_attr(Plain(), "text") == "hello"
    assert _get_attr(Payload(), "missing") is None
    assert _get_attr(SimpleNamespace(text="hi"), "text") == "hi"


def test_get_attr_primitives_reject_underscore_names():
    """无 ``__dict__`` 的对象也不静默：下划线名一律拒绝，公开名仍是 None。"""
    for primitive in (42, None, [1, 2, 3], "text"):
        assert _get_attr(primitive, "anything") is None
        with pytest.raises(ValueError):
            _get_attr(primitive, "__class__")
        with pytest.raises(ValueError):
            _get_attr(primitive, "_private")


def test_rejection_message_names_segment_and_does_not_echo_object():
    with pytest.raises(ValueError) as excinfo:
        _get_attr(LoudRepr(), "__class__")
    message = str(excinfo.value)
    assert "'__class__'" in message
    assert "SENTINEL-INTERNAL-OBJECT" not in message
    assert "reserved for Python internals" in message
    assert "$ENV" in message


def test_rejection_is_not_swallowed_by_parse_function():
    """``parse_function`` 只吞 ParseException；越权拒绝必须穿透。"""
    with pytest.raises(ValueError):
        ExpressionParser.for_prefix("$").parse_function(
            "$F.concat($NODE.a.__class__)", _context()
        )


# ---------------------------------------------------------------------------
# 端到端：issue 原文 repro（节点返回 dataclass → end output 走攻击链）
# ---------------------------------------------------------------------------


class Issue55PayloadNode(Node):
    node_type: ClassVar[str] = "issue55_payload_probe"

    def execute(self, execution):
        return Payload()


def _probe_flow(output: str) -> Flow:
    # 注册在调用时刻做：其他测试会重置/替换默认 registry 实例（全量套件里
    # 模块级注册会被清掉），方法级注册保证本文件在任意执行顺序下可用。
    pn.get_default_registry().register(Issue55PayloadNode)
    return Flow.model_validate({
        "flow_id": "issue55-probe",
        "name": "issue55-probe",
        "nodes": [
            {"id": "s", "type": "start", "next": "n1"},
            {"id": "n1", "type": "issue55_payload_probe", "next": "end"},
            {"id": "end", "type": "end", "output": output},
        ],
    })


def test_issue_repro_now_raises_and_never_leaks_env(monkeypatch):
    """issue 原文脚本：``leaked secret: True`` → 抛错，且链上是 ValueError。"""
    monkeypatch.setenv("PLAITA_ISSUE55_SECRET", "sk-DUMMY-SECRET-123")
    flow = _probe_flow("{% $NODE.n1.__class__.__init__.__globals__.os.environ %}")
    with pytest.raises(NodeExecutionError) as excinfo:
        flow.run({})
    message = str(excinfo.value)
    assert "'__class__'" in message
    assert "is not allowed" in message
    assert "sk-DUMMY-SECRET-123" not in message
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_env_allowlist_semantics_unchanged(monkeypatch):
    """边界：不改 ``$ENV`` allowlist——默认仍是空字典，显式 expose 才可见。"""
    monkeypatch.setenv("PLAITA_ISSUE55_SECRET", "sk-DUMMY-SECRET-123")
    assert _probe_flow("$ENV.PLAITA_ISSUE55_SECRET").run({}) is None

    exposed = Flow.model_validate({
        "flow_id": "issue55-env",
        "name": "issue55-env",
        "expose_env": ["PLAITA_ISSUE55_SECRET"],
        "nodes": [
            {"id": "s", "type": "start", "next": "end"},
            {"id": "end", "type": "end", "output": "$ENV.PLAITA_ISSUE55_SECRET"},
        ],
    })
    assert exposed.run({}) == "sk-DUMMY-SECRET-123"
