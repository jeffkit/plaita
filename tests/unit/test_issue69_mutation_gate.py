"""plaita#69 — 内建就地 mutate 函数对 checkpoint 持久化根的**具名硬报错**门。

owner 的三条答复（issue #69 评论）钉成这里的契约：

* **确认 (b)，不做 (a)**：保留就地语义（``test_expression.py`` 断言调用方自己的
  对象被改；``test_loop.py``/``test_mfix_l_loop_context.py`` 钉住循环条件是唯一
  合法 in-place 现场）。把 mutate 改成 copy-on-write 会让有文档、有测试的语义
  静默变 no-op —— 那是改契约，不是修缺陷。
* **五根全包**：``$INPUT`` / ``$NODE`` / ``$GLOBAL`` / ``$PARENT`` / ``$ENV``
  （``CheckpointState._field_to_key`` 的全部 5 个持久化字段），**含别名**。
* **报错指名路径**（如 ``$INPUT.cfg``），照 #55 ``_reject_reserved_field`` 先例。
* **静态白名单 + ``register(..., fresh_producer=True)`` 声明，二者都要**；声明
  缺省 ``False`` ⇒ 忘记声明时**挡住**而不是放行（失败方向选安全侧）。
* 合法用法（普通 ``$VAR``、白名单/声明的 fresh producer、循环条件）**不得误拒**。

基线（``origin/main``）：本文件应当**红** —— 门不存在，写入静默成功并落盘。
"""

from __future__ import annotations

import copy
import functools
import unittest

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution
from plaita.core.expression import (
    ExpressionRegistry,
    FunctionCategory as _FunctionCategory,
    get_default_expression_registry,
)
import plaita.core.expression_parser as _parser_mod
from plaita.core.expression_parser import ExpressionParser, freshness_scope

# plaita#69 gate internals — accessed via getattr so this file still *collects* on
# origin/main (baseline must be red at the assertion level, not at import time:
# the mutation must die by assertion, never SyntaxError/ImportError).
_DEFAULT_MUTATOR_NAMES = getattr(_parser_mod, "_DEFAULT_MUTATOR_NAMES", frozenset())
_FRESH_PRODUCER_FUNCTIONS = getattr(_parser_mod, "_FRESH_PRODUCER_FUNCTIONS", frozenset())
_DEEP_PRODUCER_FUNCTIONS = getattr(_parser_mod, "_DEEP_PRODUCER_FUNCTIONS", frozenset())
_SHALLOW_PRODUCER_FUNCTIONS = getattr(_parser_mod, "_SHALLOW_PRODUCER_FUNCTIONS", frozenset())
_LIVE_REFERENCE_FUNCTIONS = getattr(_parser_mod, "_LIVE_REFERENCE_FUNCTIONS", frozenset())
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.decide import CONDITION_OP_EQ, Condition
from plaita.node.loop import Loop


class _FlowCfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "f69"


ROOT_SHAPES = {
    "$INPUT": "$INPUT",
    "$GLOBAL": "$GLOBAL",
    "$PARENT": "$PARENT",
    "$ENV": "$ENV",
    "$NODE": "$NODE.n1",
}
MUTATOR_SHAPES = {
    "set-root": "$F.set({target},'k','v')",
    "set-child": "$F.set({target}.cfg,'k','v')",
    "pop-list": "$F.pop({target}.items,0)",
    "setListItem": "$F.setListItem({target}.lst,0,99)",
    "clear-child": "$F.clear({target}.cfg)",
    "set-nested": "$F.set({target}.nested.d,'k','v')",
    "pop-nested": "$F.pop({target}.nested.lst,0)",
}


def _payload() -> dict:
    return {
        "items": [1, 2, 3],
        "lst": [1, 2, 3],
        "cfg": {"a": 1},
        "nested": {"lst": [1, 2, 3], "d": {"a": 1}},
    }


def _engine_value(ctx: ExecutionContext, value):
    """Materialise *value* **through the engine** so it is provably fresh (r5).

    r5 的 default-deny 判据是"这个对象是本次求值新建的"。``ctx.evaluate`` 对
    list/dict 字段**重建容器**并登记 freshness；直接 ``set_state("$VAR", {...})``
    交给引擎的是宿主对象，其来源无法证明 ⇒ 写它会被具名拒绝（见
    ``test_host_bound_non_root_key_is_refused_by_provenance``，owner 决策边界）。
    """
    return ctx.evaluate(value)


def _context(**kwargs) -> ExecutionContext:
    ctx = ExecutionContext(**kwargs)
    ctx.setup_flow(_FlowCfg(), ({},), {})
    for key in ROOT_SHAPES:
        if key == "$NODE":
            ctx.set_state("$NODE", {"n1": _payload()})
        else:
            ctx.set_state(key, _payload())
    ctx.set_state("$VAR", _engine_value(ctx, _payload()))
    return ctx


def _snapshot(ctx: ExecutionContext, keys) -> dict:
    return {k: copy.deepcopy(ctx.get_state(k)) for k in keys}


#: #69 r3 F1：元素是**对象**的载荷 —— 浅 producer 的别名只有在元素是可变容器时才可
#: 通过后续普通表达式写穿（标量元素没有第二条表达式可以就地改写）。
def _alias_payload() -> dict:
    return {
        "items": [{"n": 1}, {"n": 2}],
        "lst": [{"n": 1}, {"n": 2}],
        "cfg": {"a": {"deep": 1}, "b": {"deep": 2}},
        "nested": {"lst": [{"n": 1}], "d": {"a": 1}},
    }


def _alias_context(**kwargs) -> ExecutionContext:
    ctx = ExecutionContext(**kwargs)
    ctx.setup_flow(_FlowCfg(), ({},), {})
    for key in ROOT_SHAPES:
        if key == "$NODE":
            ctx.set_state("$NODE", {"n1": _alias_payload()})
        else:
            ctx.set_state(key, _alias_payload())
    ctx.set_state("$VAR", _engine_value(ctx, {}))
    return ctx


def _mutable_ids(obj, out: set) -> None:
    """Collect the ids of every **mutable container** reachable from *obj*.

    Scalars (str/int/bool/None) are ignored on purpose: they are immutable, so an
    interned string shared with a root is not an alias channel.
    """
    if isinstance(obj, dict):
        out.add(id(obj))
        for key, value in obj.items():
            _mutable_ids(key, out)
            _mutable_ids(value, out)
    elif isinstance(obj, (list, set)):
        out.add(id(obj))
        for value in obj:
            _mutable_ids(value, out)
    elif isinstance(obj, (tuple, frozenset)):
        for value in obj:
            _mutable_ids(value, out)


def _shares_mutable_object(a, b) -> bool:
    ids_a: set = set()
    ids_b: set = set()
    _mutable_ids(a, ids_a)
    _mutable_ids(b, ids_b)
    return bool(ids_a & ids_b)


def _root_value(ctx: ExecutionContext, root: str):
    return ctx.get_state("$NODE")["n1"] if root == "$NODE" else ctx.get_state(root)


def _allowed(testcase, parser, expr: str, ctx: ExecutionContext, registry=None):
    """Evaluate an expression that must be allowed; a gate refusal becomes an assertion.

    Mutants that wrongly refuse a legitimate expression must die by AssertionError
    (an uncaught ``ValueError`` would be an illegal mutant death).
    """
    try:
        return parser.evaluate(expr, ctx.context, registry)
    except ValueError as exc:
        testcase.fail(f"legitimate expression refused: {expr} :: {exc}")


def _one_evaluation(method):
    """r6：把整个测试体放进**一次**逻辑求值。

    ``fresh`` 在 r6 收紧为"**本次**求值由引擎新建"；``_context()`` 里用
    ``ctx.evaluate`` 物化的 ``$VAR`` 因而只在**同一次**逻辑求值内可写。真实用法
    （节点字段一次求值里物化容器、随即就地改写它）正是这样，所以这些"合法用法不得
    误拒"的用例在作用域内跑。反面性质——**跨求值**必须被拒——由
    ``test_created_in_one_evaluation_is_refused_in_the_next`` 用**不带**本装饰器
    的裸调用钉住。
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with freshness_scope():
            return method(self, *args, **kwargs)
    return wrapper


def _run_flow(execution: FlowExecution, flow: Flow, testcase) -> object:
    """Run a flow, turning any SUT exception into an **assertion** (mutant hygiene).

    A mutant that wrongly fires the gate inside the isolated loop-condition context
    makes the engine raise ``ValueError``→``NodeExecutionError``; without this
    wrapper that death would be an uncaught exception (an illegal mutant death),
    not an assertion.
    """
    try:
        return execution.run_compatible(flow, False, {})
    except Exception as exc:                  # noqa: BLE001 - re-surfaced as assert
        testcase.fail(f"flow must not raise: {type(exc).__name__}: {exc}")


class TestMutatorGateRefusesPersistedRoots(unittest.TestCase):
    """五根 × 全部就地形状：具名 ValueError + 零写入（live / checkpoint / 调用方）。"""

    def setUp(self) -> None:
        self.parser = ExpressionParser.for_prefix("$")

    def test_every_root_shape_is_refused_by_name(self):
        keys = list(ROOT_SHAPES) + ["$VAR"]
        for root, target in ROOT_SHAPES.items():
            for shape, tpl in MUTATOR_SHAPES.items():
                with self.subTest(root=root, shape=shape):
                    ctx = _context()
                    before = _snapshot(ctx, keys)
                    expr = tpl.format(target=target)
                    with self.assertRaises(ValueError) as cm:
                        self.parser.evaluate(expr, ctx.context)
                    message = str(cm.exception)
                    self.assertIn("plaita#69", message)
                    self.assertIn(target, message)          # 具名路径（owner 硬要求 ①）
                    self.assertIn(root, message)
                    # 零写入：live、checkpoint、以及最初传入的调用方对象
                    self.assertEqual(before, _snapshot(ctx, keys), "live context changed")
                    self.assertEqual(before, {k: ctx.to_dict()[k] for k in before},
                                     "checkpoint changed")

    def test_caller_owned_object_is_untouched(self):
        ctx = _context()
        caller = ctx.get_state("$INPUT")
        caller_copy = copy.deepcopy(caller)
        with self.assertRaises(ValueError):
            self.parser.evaluate("$F.pop($INPUT.items,0)", ctx.context)
        self.assertEqual(caller_copy, caller)               # 调用方对象未被就地改写

    def test_alias_roots_are_covered(self):
        ctx = _context(express_input_name="IN", express_global_name="G",
                       express_node_name="N", express_parent_name="P")
        ctx.set_state("$IN", _payload())
        ctx.set_state("$G", _payload())
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.set($IN.cfg,'k','v')", ctx.context)
        self.assertIn("$IN.cfg", str(cm.exception))
        with self.assertRaises(ValueError) as cm2:
            self.parser.evaluate("$F.clear($G.cfg)", ctx.context)
        self.assertIn("$G.cfg", str(cm2.exception))


class TestMutatorGateAllowsLegitimateTargets(unittest.TestCase):
    """收紧只落在攻击面：非根本体、fresh producer、循环条件不得被误拒。"""

    def setUp(self) -> None:
        self.parser = ExpressionParser.for_prefix("$")

    @_one_evaluation
    def test_non_root_variable_keeps_in_place_semantics(self):
        ctx = _context()
        before = copy.deepcopy(ctx.get_state("$VAR"))
        self.parser.evaluate("$F.set($VAR,'k','v')", ctx.context)
        self.assertNotEqual(before, ctx.get_state("$VAR"))
        self.assertEqual("v", ctx.get_state("$VAR")["k"])

    def test_fresh_producers_are_allowed(self):
        ctx = _context()
        ctx.set_state("$INPUT", {"items": [1, 2, 3], "raw": '{"q":0}'})
        before = copy.deepcopy(ctx.get_state("$INPUT"))
        for expr, expected in (
            ("$F.set($F.json_loads($INPUT.raw),'q',1)", None),
            ("$F.pop($F.slice($INPUT.items,0,3),0)", 1),
            ("$F.setListItem($F.reverse($INPUT.items),0,9)", None),
            ("$F.set($F.json_loads('{\"a\": 1}'),'k','v')", None),
        ):
            with self.subTest(expr=expr):
                self.assertEqual(expected, _allowed(self, self.parser, expr, ctx))
        self.assertEqual(before, ctx.get_state("$INPUT"), "fresh-producer write leaked")

    def test_loop_condition_keeps_isolated_copy_contract(self):
        """循环条件是唯一合法 in-place 现场：条件里的 mutate 只改副本（不得报错）。"""
        cond = Condition(field="$F.set($GLOBAL, 'counter', $LOOP-RESULT)",
                         operator=CONDITION_OP_EQ, value=0)
        flow = Flow(
            flow_id="iso69", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT),
            global_context={"counter": 0, "nested": {"deep": {"x": 1}}},
            nodes=[
                Start(id="start", next="l"),
                Loop(id="l", collection=[1, 2, 3], child_flow=self._echo_child_flow(),
                     condition=cond, next="end"),
                End(id="end", resultType="success", output="$NODE.l"),
            ],
        )
        execution = FlowExecution()
        result = _run_flow(execution, flow, self)
        self.assertEqual(1, result)
        self.assertEqual(0, execution.context.get("$GLOBAL", {}).get("counter"))
        self.assertEqual({"deep": {"x": 1}}, execution.context.get("$GLOBAL", {}).get("nested"))

    @staticmethod
    def _echo_child_flow() -> Flow:
        return Flow(
            flow_id="child-echo69", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT, children={
                "item": Property(data_type=types.ANY),
                "index": Property(data_type=types.INTEGER)}),
            output_type=Property(data_type=types.ANY),
            nodes=[Start(id="s", next="e"),
                   End(id="e", resultType="success", output="$INPUT.item")],
        )


class TestMutatorGateEveryArgumentPosition(unittest.TestCase):
    """A1（r2）：**任意实参位**静态解析为持久根路径 ⇒ 拒（不只是改写目标）。

    验收者的 r2 反例（r1 上实测写穿 + 落盘）：
        p.evaluate("$F.set($VAR,'k',$INPUT)", ctx.context)   # ① 把活根存进 $VAR
        p.evaluate("$F.set($VAR.k,'x','y')", ctx.context)    # ② 经别名就地写穿
    """

    def setUp(self) -> None:
        self.parser = ExpressionParser.for_prefix("$")

    def test_live_root_as_value_argument_is_refused_by_position(self):
        # 值参位（含 3 参函数的第 2/3 位、列表/字典赋值的值参）逐形状
        shapes = (
            ("set-arg2", "$F.set($VAR,'k',{p})"),
            ("setListItem-arg3", "$F.setListItem($VAR.lst,0,{p})"),
            ("setDictValue-arg3", "$F.setDictValue($VAR.cfg,'k',{p})"),
        )
        for label, tpl in shapes:
            for root, path in (("$INPUT", "$INPUT"), ("$GLOBAL", "$GLOBAL"),
                               ("$INPUT", "$INPUT.cfg"), ("$NODE", "$NODE.n1.items")):
                with self.subTest(shape=label, root=root, path=path):
                    ctx = _context()
                    before = copy.deepcopy(ctx.get_state(root))
                    with self.assertRaises(ValueError) as cm:
                        self.parser.evaluate(tpl.format(p=path), ctx.context)
                    message = str(cm.exception)
                    self.assertIn("plaita#69", message)
                    self.assertIn(path, message)              # 完整路径
                    self.assertIn(root, message)              # 根
                    self.assertIn("argument #", message)      # 实参位
                    self.assertEqual(before, ctx.get_state(root), "live write leaked")

    def test_root_as_target_still_uses_the_target_wording(self):
        ctx = _context()
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.set($INPUT,'k',$VAR)", ctx.context)
        message = str(cm.exception)
        self.assertIn("cannot be the target", message)
        self.assertIn("$INPUT", message)

    def test_live_reference_value_argument_is_refused_by_position(self):
        ctx = _context()
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.set($VAR,'k',$F.get($INPUT,'cfg'))", ctx.context)
        message = str(cm.exception)
        self.assertIn("plaita#69", message)
        self.assertIn("$F.get", message)
        self.assertIn("argument #3", message)

    def test_acceptance_counter_example_sequence(self):
        """①+② 两步：① 现在被拒，于是 `$VAR.k` 永远不会是活根对象。"""
        ctx = _context()
        ctx.set_state("$VAR", _engine_value(ctx, {}))
        before_input = copy.deepcopy(ctx.get_state("$INPUT"))
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.set($VAR,'k',$INPUT)", ctx.context)   # ①
        self.assertIn("argument #3", str(cm.exception))
        # ① 未执行 ⇒ $VAR 里没有 $INPUT 的别名
        self.assertEqual({}, ctx.get_state("$VAR"))
        # ② 目标根是 $VAR（非根）⇒ 规则允许；但它此刻会因缺键原生失败，且 $INPUT 未被触及
        try:
            self.parser.evaluate("$F.set($VAR.k,'x','y')", ctx.context)
        except (AttributeError, TypeError, KeyError):
            pass
        self.assertEqual(before_input, ctx.get_state("$INPUT"))

    @_one_evaluation
    def test_deep_producer_wrapped_root_is_allowed_and_aliases_nothing(self):
        ctx = _context()
        before = copy.deepcopy(ctx.get_state("$INPUT"))
        self.assertIsNone(_allowed(self, self.parser,
            "$F.set($VAR,'k',$F.json_loads($F.json_dumps($INPUT)))", ctx))
        self.assertEqual(before, ctx.get_state("$INPUT"))
        self.assertFalse(_shares_mutable_object(ctx.get_state("$VAR"), ctx.get_state("$INPUT")))

    @_one_evaluation
    def test_shallow_producer_value_is_detached_not_aliased(self):
        """r3 F1：`$F.slice(...)` 作为值参仍然可用，但存进 `$VAR` 的必须是**去别名副本**，
        step2 的普通就地写不得再写穿 `$INPUT`。"""
        ctx = _alias_context()
        ctx.set_state("$VAR", _engine_value(ctx, {}))
        before_input = copy.deepcopy(ctx.get_state("$INPUT"))
        self.assertIsNone(self.parser.evaluate(
            "$F.set($VAR,'k',$F.slice($INPUT.items,0,1))", ctx.context))       # step1
        stored = ctx.get_state("$VAR")["k"]
        self.assertEqual([{"n": 1}], stored)
        self.assertFalse(_shares_mutable_object(stored, ctx.get_state("$INPUT")))
        self.assertIsNot(stored[0], ctx.get_state("$INPUT")["items"][0])
        self.assertIsNone(self.parser.evaluate(
            "$F.set($VAR.k[0],'x',9)", ctx.context))                            # step2
        self.assertEqual({"n": 1, "x": 9}, ctx.get_state("$VAR")["k"][0])
        self.assertEqual(before_input, ctx.get_state("$INPUT"))
        self.assertEqual(before_input, ctx.to_dict()["$INPUT"])

    @_one_evaluation
    def test_non_root_value_arguments_stay_allowed(self):
        ctx = _context()
        for expr in ("$F.set($VAR,'k','literal')",
                     "$F.set($VAR,'k',123)",
                     "$F.set($VAR,'k',$VAR.cfg)",
                     "$F.set($VAR,'k',$F.concat('a','b'))"):
            with self.subTest(expr=expr):
                self.assertIsNone(_allowed(self, self.parser, expr, ctx))


class TestMutatorGateUndeclaredProducers(unittest.TestCase):
    """fail-closed：live-reference 函数与未声明的 producer 一律挡住并点名 callee。"""

    def setUp(self) -> None:
        self.parser = ExpressionParser.for_prefix("$")

    def test_live_reference_functions_are_refused(self):
        for callee, expr in (("get", "$F.set($F.get($INPUT,'cfg'),'k','v')"),
                             ("getDictValue", "$F.set($F.getDictValue($INPUT,'cfg'),'k','v')"),
                             ("getListItem", "$F.set($F.getListItem($INPUT.items,0),'k','v')"),
                             ("ifelse", "$F.set($F.ifelse(1,$INPUT.cfg,$INPUT.cfg),'k','v')"),
                             ("and", "$F.set($F.and($INPUT.cfg,$INPUT.cfg),'k','v')")):
            with self.subTest(callee=callee):
                ctx = _context()
                with self.assertRaises(ValueError) as cm:
                    self.parser.evaluate(expr, ctx.context)
                message = str(cm.exception)
                self.assertIn("plaita#69", message)
                self.assertIn(callee, message)
                self.assertIn("live reference", message)
                self.assertIn("$INPUT", message)

    def test_custom_registry_producer_requires_declaration(self):
        from plaita.core.expression import FunctionCategory

        def _make(fresh: bool) -> ExpressionRegistry:
            reg = ExpressionRegistry()
            reg.register("set", lambda a, b, c: a.update({b: c}),
                         FunctionCategory.DICT, has_side_effects=True)
            reg.register("temp_dict", lambda: {"a": 1}, FunctionCategory.DICT,
                         fresh_producer=fresh)
            return reg

        ctx = _context()
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.set($F.temp_dict(),'k','v')", ctx.context, _make(False))
        self.assertIn("temp_dict", str(cm.exception))
        self.assertIsNone(_allowed(self, self.parser,
            "$F.set($F.temp_dict(),'k','v')", ctx, _make(True)))

    def test_has_side_effects_flag_has_a_runtime_consumer(self):
        """自定义 registry 的 mutator 也受门控 —— 证明该 flag 不是装饰性标志。"""
        from plaita.core.expression import FunctionCategory
        registry = ExpressionRegistry()
        registry.register("sneaky", lambda a, b: a.update({b: 1}),
                          FunctionCategory.DICT, has_side_effects=True)
        ctx = _context()
        before = copy.deepcopy(ctx.get_state("$GLOBAL"))
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.sneaky($GLOBAL,'k')", ctx.context, registry)
        self.assertIn("plaita#69", str(cm.exception))
        self.assertEqual(before, ctx.get_state("$GLOBAL"))


class TestRegisterKwargKeepsExistingRegistrations(unittest.TestCase):
    """r2 ⑤：`register(..., fresh_producer=...)` 新参数不得破坏既有注册面。"""

    LEGACY_HELPERS = (
        "pop", "set", "clear", "setListItem", "delListItem", "setDictValue",
        "delDictValue", "clearDict", "slice", "json_loads", "json_dumps", "get",
        "getListItem", "concat", "extend", "sort", "reverse", "keys", "values",
        "items", "len", "index", "ifelse", "and", "or", "not", "now", "today",
    )

    def test_default_registry_builds_and_legacy_helpers_are_registered(self):
        registry = get_default_expression_registry()
        self.assertGreaterEqual(len(registry.names()), 70)
        for name in self.LEGACY_HELPERS:
            with self.subTest(helper=name):
                desc = registry.get(name)
                self.assertIsNotNone(desc)
                self.assertTrue(callable(desc.func))
        # 既有 helper 仍可直接调用（注册面未因新参数变形）
        self.assertEqual(1, registry.get_callable("pop")([1, 2, 3], 0))
        self.assertEqual({"a": 1}, registry.get_callable("json_loads")('{"a": 1}'))

    def test_register_default_fresh_producer_is_false(self):
        registry = ExpressionRegistry()
        registry.register("plain_helper", lambda: {"a": 1}, _FunctionCategory.DICT)
        self.assertFalse(registry.get("plain_helper").fresh_producer)
        registry.register("declared", lambda: {"a": 1}, _FunctionCategory.DICT,
                          fresh_producer=True)
        self.assertTrue(registry.get("declared").fresh_producer)


class TestFreshProducerRegistryGuard(unittest.TestCase):
    """清单守卫：默认注册表每新增一个函数，必须被分类，否则本测试红。"""

    def test_default_registry_is_fully_classified(self):
        registry = get_default_expression_registry()
        names = set(registry.names())
        classified = (_DEEP_PRODUCER_FUNCTIONS | _SHALLOW_PRODUCER_FUNCTIONS
                      | _LIVE_REFERENCE_FUNCTIONS | _DEFAULT_MUTATOR_NAMES)
        self.assertEqual(names, classified)
        self.assertEqual(sorted(_DEFAULT_MUTATOR_NAMES),
                         sorted(d.name for d in registry.side_effect_functions()))
        # 四个桶两两互斥
        buckets = (_DEEP_PRODUCER_FUNCTIONS, _SHALLOW_PRODUCER_FUNCTIONS,
                   _LIVE_REFERENCE_FUNCTIONS, _DEFAULT_MUTATOR_NAMES)
        for index, left in enumerate(buckets):
            for right in buckets[index + 1:]:
                self.assertFalse(left & right)
        # 兼容伞集合 = 深 + 浅
        self.assertEqual(_FRESH_PRODUCER_FUNCTIONS,
                         _DEEP_PRODUCER_FUNCTIONS | _SHALLOW_PRODUCER_FUNCTIONS)

    def test_shallow_class_covers_every_aliasing_container_producer(self):
        """verifier 点名的 8 个 + 其余列表拼接族都必须在 SHALLOW（不是 DEEP）。"""
        self.assertTrue(
            {"slice", "reverse", "append", "sort", "values", "items", "keys",
             "getDictValues"} <= _SHALLOW_PRODUCER_FUNCTIONS)
        self.assertTrue(
            {"add", "mul", "extend", "insert", "remove", "addListItem",
             "insertListItem", "getDictKeys"} <= _SHALLOW_PRODUCER_FUNCTIONS)
        self.assertFalse(_SHALLOW_PRODUCER_FUNCTIONS & _DEEP_PRODUCER_FUNCTIONS)
        # 深桶里不能有会产生容器元素别名的函数
        self.assertFalse({"slice", "reverse", "values", "items", "add", "mul"}
                         & _DEEP_PRODUCER_FUNCTIONS)


class TestMutatorGateShallowProducerValues(unittest.TestCase):
    """r3 F1（结构性）：浅 producer（新容器、元素别名）作**值参**时去别名。

    验收者最小反例（r2 实测写穿 + 落盘）：

        $F.set($VAR,'k',$F.slice($INPUT.items,0,1))   # 允许（逃生口）
        $F.set($VAR.k[0],'x',9)                        # 允许（根 $VAR）
        ⇒ $VAR.k[0] is $INPUT.items[0]；$INPUT 被就地改写

    现在：值被深拷贝，两步之后 `$INPUT` 与 checkpoint 都不变、且不共享任何可变对象。
    """

    #: 每个浅 producer 一条：值参表达式 + 存进 `$VAR.k` 后**仍应有元素别名**的路径
    SHALLOW_VALUE_CASES = (
        ("slice", "$F.set($VAR,'k',$F.slice($INPUT.items,0,1))"),
        ("reverse", "$F.set($VAR,'k',$F.reverse($INPUT.items))"),
        ("append", "$F.set($VAR,'k',$F.append($INPUT.items,$F.json_loads('{\"n\": 3}')))"),
        ("extend", "$F.set($VAR,'k',$F.extend($INPUT.items,$F.json_loads('[]')))"),
        ("insert", "$F.set($VAR,'k',$F.insert($INPUT.items,1,$F.json_loads('{\"n\": 3}')))"),
        ("add", "$F.set($VAR,'k',$F.add($INPUT.items,$F.json_loads('[]')))"),
        ("mul", "$F.set($VAR,'k',$F.mul($INPUT.items,1))"),
        ("addListItem", "$F.set($VAR,'k',$F.addListItem($INPUT.items,$F.json_loads('{\"n\": 3}')))"),
        ("insertListItem", "$F.set($VAR,'k',$F.insertListItem($INPUT.items,1,$F.json_loads('{\"n\": 3}')))"),
        ("sort", "$F.set($VAR,'k',$F.sort($INPUT.items,'n'))"),
        ("remove", "$F.set($VAR,'k',$F.remove($INPUT.items,$INPUT.items[0]))"),
        ("keys", "$F.set($VAR,'k',$F.keys($INPUT.cfg))"),
        ("values", "$F.set($VAR,'k',$F.values($INPUT.cfg))"),
        ("items", "$F.set($VAR,'k',$F.items($INPUT.cfg))"),
        ("getDictKeys", "$F.set($VAR,'k',$F.getDictKeys($INPUT.cfg))"),
        ("getDictValues", "$F.set($VAR,'k',$F.getDictValues($INPUT.cfg))"),
    )

    def setUp(self) -> None:
        self.parser = ExpressionParser.for_prefix("$")

    @_one_evaluation
    def test_every_shallow_producer_value_is_detached(self):
        for callee, expr in self.SHALLOW_VALUE_CASES:
            with self.subTest(callee=callee):
                ctx = _alias_context()
                ctx.set_state("$VAR", _engine_value(ctx, {}))
                before_input = copy.deepcopy(ctx.get_state("$INPUT"))
                try:
                    self.parser.evaluate(expr, ctx.context)
                except ValueError as exc:
                    self.fail(f"shallow producer {callee!r} value must not be refused: {exc}")
                stored = ctx.get_state("$VAR")["k"]
                self.assertFalse(
                    _shares_mutable_object(stored, ctx.get_state("$INPUT")),
                    f"$VAR.k shares a mutable object with $INPUT after $F.{callee}")
                # step2：普通就地写只能改到副本
                try:
                    self.parser.evaluate("$F.set($VAR.k[0],'x',9)", ctx.context)
                except (AttributeError, TypeError, KeyError, IndexError):
                    pass
                self.assertEqual(before_input, ctx.get_state("$INPUT"))
                self.assertEqual(before_input, ctx.to_dict()["$INPUT"])

    @_one_evaluation
    def test_shallow_producer_non_root_value_keeps_in_place_alias(self):
        """收紧只落在持久根：`$F.slice($VAR.items,…)` 仍与 `$VAR` 共享元素（就地语义）。"""
        ctx = _alias_context()
        ctx.set_state("$VAR", _engine_value(ctx, {"items": [{"n": 1}, {"n": 2}]}))
        self.assertIsNone(_allowed(self, self.parser,
            "$F.set($VAR,'k',$F.slice($VAR.items,0,1))", ctx))
        self.assertIs(ctx.get_state("$VAR")["k"][0], ctx.get_state("$VAR")["items"][0])

    def test_shallow_producer_target_is_still_allowed(self):
        """目标位是**新容器**：9 个 mutator 只改新容器自己的条目，不碰根 ⇒ 允许。"""
        cases = (
            "$F.pop($F.slice($INPUT.items,0,2),0)",
            "$F.setListItem($F.reverse($INPUT.items),0,'X')",
            "$F.delListItem($F.append($INPUT.items,$F.json_loads('{\"n\": 9}')),0)",
        )
        for expr in cases:
            with self.subTest(expr=expr):
                ctx = _alias_context()
                before = copy.deepcopy(ctx.get_state("$INPUT"))
                outcome = self._evaluate_outcome(expr, ctx)
                self.assertNotIn("refuse", outcome, f"target-position producer refused: {expr}")
                self.assertEqual(before, ctx.get_state("$INPUT"))

    @_one_evaluation
    def test_scalar_arithmetic_over_root_stays_usable(self):
        """`$F.add($INPUT.n,1)` 不能在值参位被误拒（标量累加是合法用法）。"""
        ctx = _alias_context()
        ctx.set_state("$INPUT", {"n": 1, "items": [{"n": 1}]})
        self.assertIsNone(_allowed(self, self.parser,
            "$F.set($VAR,'total',$F.add($INPUT.n,1))", ctx))
        self.assertEqual(2, ctx.get_state("$VAR")["total"])
        self.assertEqual(1, ctx.get_state("$INPUT")["n"])

    def _evaluate_outcome(self, expr: str, ctx: ExecutionContext) -> str:
        try:
            self.parser.evaluate(expr, ctx.context)
        except ValueError as exc:
            return f"refuse: {exc}"
        except (NameError, AttributeError, TypeError, KeyError) as exc:
            return f"native-error: {type(exc).__name__}"
        return "allow"


class TestMutatorGateCalleeArgumentReach(unittest.TestCase):
    """r3 F2：callee 子句只在**它自己的实参**能静态触达持久根时才拒绝。

    验收者反例（r2 误拒、base 允许且就地生效）：

        $F.set($VAR,'k',$F.get($VAR,'x'))     # 表达式里没有任何持久根
        $F.set($F.get($VAR,'x'),'k','v')
    """

    ALLOW_CASES = (
        "$F.set($VAR,'k',$F.get($VAR,'x'))",
        "$F.set($F.get($VAR,'x'),'k','v')",
        "$F.set($VAR,'k',$F.getDictValue($VAR,'x'))",
        "$F.set($VAR,'k',$F.getListItem($VAR.lst,0))",
        "$F.set($VAR,'k',$F.ifelse(1,$VAR.x,$VAR.cfg))",
        "$F.set($VAR,'k',$F.and($VAR.cfg,$VAR.cfg))",
        "$F.set($VAR,'k',$F.or($VAR.cfg,$VAR.cfg))",
        "$F.set($VAR,'k',$F.add($INPUT.n,1))",
        "$F.set($VAR,'k',$F.slice($VAR.lst,0,1))",
        "$F.set($VAR,'k',$F.values($VAR.cfg))",
        "$F.set($VAR,'k',$F.get($F.json_loads($INPUT.raw),'q'))",
    )

    def setUp(self) -> None:
        self.parser = ExpressionParser.for_prefix("$")

    def _ctx(self) -> ExecutionContext:
        ctx = _alias_context()
        ctx.set_state("$VAR", _engine_value(ctx, {"cfg": {"a": 1}, "lst": [{"n": 1}],
                                                  "x": {"n": 1}}))
        ctx.set_state("$INPUT", {**_alias_payload(), "raw": '{"q": 0}', "n": 1})
        return ctx

    @_one_evaluation
    def test_non_root_callee_arguments_are_allowed(self):
        for expr in self.ALLOW_CASES:
            with self.subTest(expr=expr):
                ctx = self._ctx()
                try:
                    self.parser.evaluate(expr, ctx.context)
                except ValueError as exc:
                    self.fail(f"legitimate non-root callee refused: {expr} :: {exc}")

    def test_persisted_root_in_callee_arguments_is_still_refused(self):
        for expr in ("$F.set($VAR,'k',$F.get($INPUT,'cfg'))",
                     "$F.set($F.get($INPUT,'cfg'),'k','v')",
                     "$F.set($VAR,'k',$F.setListItem($INPUT.lst,0,9))",
                     "$F.set($VAR,'k',$F.and($INPUT.cfg,$INPUT.cfg))"):
            with self.subTest(expr=expr):
                ctx = self._ctx()
                before_input = copy.deepcopy(ctx.get_state("$INPUT"))
                with self.assertRaises(ValueError) as cm:
                    self.parser.evaluate(expr, ctx.context)
                self.assertIn("plaita#69", str(cm.exception))
                self.assertEqual(before_input, ctx.get_state("$INPUT"))

    def test_undeclared_zero_arg_callee_stays_fail_closed(self):
        """无实参的未声明 producer 无法从实参证明无别名 ⇒ fail-closed（owner 口径）。"""
        registry = ExpressionRegistry()
        registry.register("set", lambda a, b, c: a.update({b: c}),
                          _FunctionCategory.DICT, has_side_effects=True)
        registry.register("opaque", lambda: {"a": 1}, _FunctionCategory.DICT)
        ctx = self._ctx()
        with self.assertRaises(ValueError) as cm:
            self.parser.evaluate("$F.set($F.opaque(),'k','v')", ctx.context, registry)
        self.assertIn("opaque", str(cm.exception))
        self.assertIn("plaita#69", str(cm.exception))


class TestMutatorGateEngineOwnedCopier(unittest.TestCase):
    """r6 C1 反例：``_detach_alias`` 再也不能把**活对象**登记成 fresh（r7 统一）。

    r5 的实现把 ``copy.deepcopy`` 的**结果整体**登记（``note_fresh_closure(detached)``）。
    宿主只要让载荷里的某个对象实现 ``__deepcopy__`` 返回 ``self``，深拷贝就不再"去
    别名"——活对象本身落进 freshness registry：

        class Evil(dict):
            def __deepcopy__(self, memo): return self
        ctx.set_state("$INPUT", {"items": [Evil({"n": 1})], "cfg": {"a": 1}})
        ctx.set_state("$VAR", ctx.evaluate({}))
        parser.evaluate("$F.set($VAR,'k',$F.slice($INPUT.items,0,1))", ctx.context)
        parser.evaluate("$F.set($VAR.k[0],'z',9)", ctx.context)   # r5：就地写穿

    r7 的 ``_engine_copy`` 是**唯一**的数据模型拷贝入口：容器子类用**基类协议**重建
    （``dict.items(sub)`` / ``list.__iter__(sub)`` …，绝不触发 ``__deepcopy__`` /
    ``__copy__`` / 覆写的 ``items``/``__iter__``/``__getitem__``），标量子类按值归一化；
    每个返回的容器都在拷贝器里现造，登记它不可能登记到活对象。
    """

    class Evil(dict):
        def __deepcopy__(self, memo):
            return self

        def __copy__(self):
            return self

    def setUp(self) -> None:
        self.parser = ExpressionParser.for_prefix("$")

    def _alias_context_with_evil(self) -> ExecutionContext:
        ctx = _alias_context()
        ctx.set_state("$INPUT", {"items": [self.Evil({"n": 1})], "cfg": {"a": 1}})
        return ctx

    def test_c1_evil_payload_is_normalised_and_detached(self):
        """r7：``__deepcopy__`` 返回 ``self`` 的 ``dict`` 子类被**归一化**成 plain dict。"""
        ctx = self._alias_context_with_evil()
        live_evil = ctx.get_state("$INPUT")["items"][0]
        with freshness_scope():
            ctx.set_state("$VAR", ctx.evaluate({}))
            self.assertIsNone(self.parser.evaluate(
                "$F.set($VAR,'k',$F.slice($INPUT.items,0,1))", ctx.context))
            stored = ctx.get_state("$VAR")["k"]
            self.assertEqual([{"n": 1}], stored)
            self.assertIs(type(stored[0]), dict)              # 宿主类被丢掉
            self.assertIsNot(stored[0], live_evil)
            self.assertFalse(_shares_mutable_object(stored, ctx.get_state("$INPUT")))
            # 同一次求值里对副本的就地写只改副本
            self.assertIsNone(self.parser.evaluate("$F.set($VAR.k[0],'x',9)", ctx.context))
            self.assertEqual({"n": 1, "x": 9}, ctx.get_state("$VAR")["k"][0])
        self.assertIs(live_evil, ctx.get_state("$INPUT")["items"][0])
        self.assertEqual({"n": 1}, dict(live_evil))            # 活对象零变化
        self.assertEqual({"n": 1}, ctx.to_dict()["$INPUT"]["items"][0])

    def test_pure_json_payload_is_genuinely_detached(self):
        """同一形状的纯 JSON 载荷：不拒、且存下来的是与根零共享的引擎副本。"""
        ctx = _alias_context()
        with freshness_scope():
            ctx.set_state("$VAR", ctx.evaluate({}))
            self.assertIsNone(self.parser.evaluate(
                "$F.set($VAR,'k',$F.slice($INPUT.items,0,1))", ctx.context))
            stored = ctx.get_state("$VAR")["k"]
            self.assertEqual([{"n": 1}], stored)
            self.assertFalse(_shares_mutable_object(stored, ctx.get_state("$INPUT")))
            self.assertIsNot(stored[0], ctx.get_state("$INPUT")["items"][0])
            # 同一次求值里对副本的就地写只改副本
            self.assertIsNone(self.parser.evaluate("$F.set($VAR.k[0],'x',9)", ctx.context))
            self.assertEqual({"n": 1, "x": 9}, ctx.get_state("$VAR")["k"][0])
        self.assertEqual({"n": 1}, ctx.get_state("$INPUT")["items"][0])
        self.assertEqual({"n": 1}, ctx.to_dict()["$INPUT"]["items"][0])

    def test_negative_control_error_path_is_not_unconditional(self):
        """负控：同一条表达式对纯 JSON 载荷**不**报错 ⇒ 具名错误来自类型判据本身。"""
        ctx = _alias_context()
        with freshness_scope():
            ctx.set_state("$VAR", ctx.evaluate({}))
            self.parser.evaluate("$F.set($VAR,'k',$F.slice($INPUT.items,0,1))", ctx.context)
            self.assertIn("k", ctx.get_state("$VAR"))

    def test_engine_copy_accepts_the_documented_data_model(self):
        """``native_types`` 的每个精确类型都必须被接受（Finding 2 的表驱动半边）。"""
        from datetime import date, datetime
        from decimal import Decimal
        import plaita.core.expression_parser as pm
        accepted = (
            {"a": 1}, [1, 2], (1, 2), {1, 2}, frozenset([1]),
            "s", b"b", 1, 1.5, True, 3 + 4j, None,
            Decimal("1.5"), datetime(2020, 1, 2, 3, 4, 5), date(2020, 1, 2),
        )
        for value in accepted:
            with self.subTest(value=repr(value)[:28]):
                copied = pm._engine_copy(value)
                self.assertEqual(value, copied)
                if isinstance(value, (dict, list, set)):
                    self.assertIsNot(value, copied)

    def test_engine_copy_normalises_container_and_scalar_subclasses(self):
        """容器/标量子类被**归一化**成普通基类（不拒绝、不按引用透传）。"""
        from collections import Counter, OrderedDict, defaultdict, namedtuple
        from decimal import Decimal
        from enum import IntEnum
        import plaita.core.expression_parser as pm

        class EvilDict(dict):
            calls = []

            def __deepcopy__(self, memo):
                EvilDict.calls.append("__deepcopy__")
                return self

            def __copy__(self):
                EvilDict.calls.append("__copy__")
                return self

            def items(self):                       # 覆写：绝不能被调用
                raise AssertionError("overridden items() was called")

            def __getitem__(self, key):
                raise AssertionError("overridden __getitem__ was called")

        class EvilList(list):
            def __iter__(self):
                raise AssertionError("overridden __iter__ was called")

            def __getitem__(self, index):
                raise AssertionError("overridden __getitem__ was called")

        class Level(IntEnum):
            HIGH = 2

        NT = namedtuple("NT", "a b")
        cases = (
            (OrderedDict([("a", 1)]), dict, {"a": 1}),
            (defaultdict(int, a=1), dict, {"a": 1}),
            (Counter("aab"), dict, {"a": 2, "b": 1}),
            (NT(1, 2), tuple, (1, 2)),
            (Level.HIGH, int, 2),
            (EvilDict({"n": 1}), dict, {"n": 1}),
            (EvilList([1, 2]), list, [1, 2]),
        )
        for value, expected_type, expected_value in cases:
            with self.subTest(value=type(value).__name__):
                copied = pm._engine_copy(value)
                self.assertIs(type(copied), expected_type)
                self.assertEqual(expected_value, copied)
                self.assertIsNot(copied, value)
        self.assertEqual([], EvilDict.calls,
                         "copy module hooks were invoked while normalising a subclass")

        class MyStr(str):
            pass

        self.assertIs(type(pm._engine_copy(MyStr("x"))), str)
        self.assertIs(type(pm._engine_copy(Decimal("1.5"))), Decimal)

    def test_engine_copy_refuses_genuine_non_data_model_values(self):
        """真·非数据模型：自定义 ``MutableMapping`` / ``bytearray`` / 不透明对象 / dict view。"""
        from collections.abc import MutableMapping
        import plaita.core.expression_parser as pm

        class Opaque:
            def __getitem__(self, item):
                raise AssertionError("side effect")

        class MyMapping(MutableMapping):
            def __init__(self):
                self._d = {}

            def __getitem__(self, key):
                return self._d[key]

            def __setitem__(self, key, value):
                self._d[key] = value

            def __delitem__(self, key):
                del self._d[key]

            def __iter__(self):
                return iter(self._d)

            def __len__(self):
                return len(self._d)

        for value in (Opaque(), MyMapping(), bytearray(b"x"), {1: 2}.keys()):
            with self.subTest(value=type(value).__name__):
                with self.assertRaises(pm._EngineCopyRefused) as cm:
                    pm._engine_copy(value)
                self.assertEqual(type(value).__name__, cm.exception.type_name)

    def test_engine_copy_handles_cycles(self):
        import plaita.core.expression_parser as pm
        cyclic: dict = {}
        cyclic["self"] = cyclic
        copied = pm._engine_copy(cyclic)
        self.assertIs(copied, copied["self"])
        self.assertIsNot(copied, cyclic)

    def test_deepcopy_hooks_are_never_invoked_on_the_dsl_path(self):
        """白盒：``__deepcopy__`` 计数器证明拷贝路径不调用 copy 协议钩子。"""
        import plaita.core.expression_parser as pm
        calls = []

        class Traced(dict):
            def __deepcopy__(self, memo):
                calls.append(self)
                return self

        class TracedList(list):
            def __deepcopy__(self, memo):
                calls.append(self)
                return self

        copied = pm._engine_copy(TracedList([Traced({"n": 1})]))
        self.assertEqual([{"n": 1}], copied)
        self.assertIs(type(copied), list)
        self.assertIs(type(copied[0]), dict)
        self.assertEqual([], calls, "copy protocol hooks were called on the DSL data path")

    def test_copier_and_registrar_share_one_exact_type_predicate(self):
        """单一事实来源：拷贝器与登记面消费**同一个**精确类型元组对象。"""
        import plaita.core.expression_parser as pm

        self.assertIs(pm._ENGINE_COPY_CONTAINER_TYPES, pm._FRESH_WALKABLE)
        self.assertIs(pm._FRESH_WALKABLE, types.PERSISTABLE_CONTAINER_TYPES)
        self.assertIs(pm._FRESH_MUTABLE, types.PERSISTABLE_MUTABLE_TYPES)
        self.assertIs(pm._IMMUTABLE_TARGET_TYPES, types.PERSISTABLE_IMMUTABLE_TYPES)
        for kind in types.PERSISTABLE_CONTAINER_TYPES:
            with self.subTest(kind=kind.__name__):
                self.assertTrue(pm._is_walkable_container(kind()))
        # 精确类型判据：子类**不是**数据模型容器
        evil = self.Evil({"n": 1})
        self.assertFalse(pm._is_walkable_container(evil))
        self.assertFalse(pm._is_mutable_container(evil))
        with freshness_scope():
            pm.note_fresh_closure({"a": evil})
            self.assertFalse(pm._fresh_has(evil),
                             "a dict subclass must never be registered as fresh")
            exact = {"n": 1}
            pm.note_fresh_closure({"a": exact})
            self.assertTrue(pm._fresh_has(exact))
            # 行为半边：闭包里挂了宿主 dict 子类之后，它仍然不能作为就地改写目标。
            # （把 `_is_walkable_container` 改回 isinstance 的变异体会在这里放行并
            # 改写活宿主对象 ⇒ 本断言红。）
            with self.assertRaises(ValueError) as cm:
                self.parser.evaluate("$F.set($V,'k','v')", {"$V": evil})
            self.assertIn("provenance could not be proven fresh", str(cm.exception))
        self.assertEqual({"n": 1}, dict(evil), "host object was mutated in place")


if __name__ == "__main__":
    unittest.main()
