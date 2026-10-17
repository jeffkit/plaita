"""plaita#164 —— 显式 scratch 捐献 API：``ctx.set_scratch(key, value)``。

背景（``#69`` 交付随附的后续单）：``#69`` 把就地改写的判据收紧成"只有**本次求值中
由引擎新建**的对象才允许被就地改写"（default-deny，有界登记取代无界证明）。方向是对的，
但宿主最自然的写法被收紧了::

    ctx.set_state("$V", {"a": 1})                        # 宿主绑定
    parser.evaluate("$F.set($V,'k','v')", ctx.context)
    # → ValueError: ... provenance could not be proven fresh（零写入）

``set_scratch`` 给宿主一个**显式声明**入口，语义 = "这个对象只归本次执行所有、可以被
就地改写"。它的实现**不引入任何"证明对象是活的"谓词**：声明只把对象登记进**读取它的
那一次逻辑求值**的 freshness 作用域（``#69`` r6 的作用域语义原样保留），因此

* **一次求值有效**——第二次求值里同一个宿主对象仍被拒（作用域语义）；
* **只登记被声明的那个对象**（不登记它的图）——声明不能把活根"洗白"成可写目标；
* **不进 checkpoint**——scratch 是本次执行的私有绑定。

本文件同时钉住 ``#69`` 的不变量在 scratch 面前**没有松动**：五根 × 就地形状仍全部
具名拒绝、零写穿。
"""

from __future__ import annotations

import copy
import unittest

from plaita.core.context import ExecutionContext
from plaita.core.expression_parser import ExpressionParser


class _FlowCfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "f164"


PARSER = ExpressionParser.for_prefix("$")

#: ``#69`` 的五个持久化根（``CheckpointState._field_to_key`` 的全部持久化字段）。
ROOT_SHAPES = {
    "$INPUT": "$INPUT",
    "$GLOBAL": "$GLOBAL",
    "$PARENT": "$PARENT",
    "$ENV": "$ENV",
    "$NODE": "$NODE.n1",
}

#: ``#69`` 的就地形状（与 ``test_issue69_mutation_gate.MUTATOR_SHAPES`` 同形）。
MUTATOR_SHAPES = {
    "set-root": "$F.set({target},'k','v')",
    "set-child": "$F.set({target}.cfg,'k','v')",
    "pop-list": "$F.pop({target}.items,0)",
    "setListItem": "$F.setListItem({target}.lst,0,99)",
    "clear-child": "$F.clear({target}.cfg)",
}


def _payload() -> dict:
    return {"items": [1, 2, 3], "lst": [1, 2, 3], "cfg": {"a": 1}}


def _context() -> ExecutionContext:
    ctx = ExecutionContext()
    ctx.setup_flow(_FlowCfg(), ({},), {})
    for key in ROOT_SHAPES:
        if key == "$NODE":
            ctx.set_state("$NODE", {"n1": _payload()})
        else:
            ctx.set_state(key, _payload())
    return ctx


def _snapshot(ctx: ExecutionContext, keys) -> dict:
    return {k: copy.deepcopy(ctx.get_state(k)) for k in keys}


class TestScratchDeclarationIsMutableInPlace(unittest.TestCase):
    """验收 1：``set_scratch`` 绑定的对象可被就地改写（且就地语义未变）。"""

    def test_declared_scratch_object_is_mutable_in_place(self):
        """变异 killer（普通 ``FAILED`` 行）：撤销登记 ⇒ 本条红。"""
        ctx = _context()
        payload = {"a": 1}
        ctx.set_scratch("$V", payload)
        result = PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
        self.assertIsNone(result)
        self.assertEqual({"a": 1, "k": "v"}, ctx.get_state("$V"))
        # 就地语义（#69 答复 (b)）：调用方自己的对象被改，不是 copy-on-write。
        self.assertIs(payload, ctx.get_state("$V"))
        self.assertEqual({"a": 1, "k": "v"}, payload)

    def test_scratch_root_is_visible_to_expressions(self):
        ctx = _context()
        ctx.set_scratch("$V", {"a": 1, "cfg": {"b": 2}})
        self.assertEqual(1, PARSER.evaluate("$V.a", ctx.context))
        self.assertTrue(PARSER.evaluate("$F.contains($V,'cfg')", ctx.context))

    def test_both_evaluation_entry_points_see_the_declaration(self):
        for entry in ("parser", "context"):
            with self.subTest(entry=entry):
                ctx = _context()
                ctx.set_scratch("$V", {"a": 1})
                if entry == "parser":
                    PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
                else:
                    ctx.evaluate("$F.set($V,'k','v')")
                self.assertEqual("v", ctx.get_state("$V")["k"])

    def test_scratch_values_can_nest_the_engine_rebuilt_kinds(self):
        """list/dict 值都走同一条路（不是只对 dict 特判）。"""
        ctx = _context()
        ctx.set_scratch("$L", [1, 2, 3])
        PARSER.evaluate("$F.setListItem($L,0,99)", ctx.context)
        self.assertEqual([99, 2, 3], ctx.get_state("$L"))


class TestScratchIsPrivateAndNotCheckpointed(unittest.TestCase):
    """验收 1（后半）：**不进 checkpoint**（私有副本语义）。"""

    def test_scratch_binding_does_not_enter_the_checkpoint(self):
        """变异 killer（普通 ``FAILED`` 行）：scratch 漏进 checkpoint 键集 ⇒ 本条红。"""
        ctx = _context()
        ctx.set_scratch("$V", {"a": 1})
        checkpoint = ctx.to_dict()
        self.assertNotIn("$V", checkpoint)
        self.assertNotIn("$V", list(ctx.context))
        self.assertEqual(["$V"], ctx.scratch_keys())
        self.assertNotIn("$V", list(ExecutionContext.from_dict(checkpoint).context))

    def test_scratch_storage_is_invisible_to_the_checkpoint_view(self):
        ctx = _context()
        before_keys = list(ctx.context)
        before_len = len(ctx.context)
        ctx.set_scratch("$V", {"a": 1})
        self.assertEqual(before_keys, list(ctx.context))
        self.assertEqual(before_len, len(ctx.context))
        # 表达式可见面仍然认这个键。
        self.assertIn("$V", ctx.context)
        self.assertEqual({"a": 1}, ctx.context["$V"])

    def test_scratch_does_not_change_state_equality_semantics(self):
        ctx = _context()
        plain = {k: ctx.get_state(k) for k in ctx.context}
        ctx.set_scratch("$V", {"a": 1})
        self.assertEqual(ctx.context, plain)

    def test_clean_drops_scratch_bindings(self):
        ctx = _context()
        ctx.set_scratch("$V", {"a": 1})
        ctx.clean()
        self.assertEqual([], ctx.scratch_keys())
        with self.assertRaises(KeyError):
            ctx.context["$V"]

    def test_clear_scratch_drops_only_the_named_key(self):
        ctx = _context()
        ctx.set_scratch("$A", {"a": 1})
        ctx.set_scratch("$B", {"b": 2})
        ctx.clear_scratch("$A")
        self.assertEqual(["$B"], ctx.scratch_keys())


class TestScratchScopeSemantics(unittest.TestCase):
    """验收 3：**跨求值不新鲜**（作用域语义原样保留）。"""

    def test_second_evaluation_refuses_the_same_host_object(self):
        """变异 killer 候选：声明被当成长期"活的证明" ⇒ 本条红。"""
        ctx = _context()
        payload = {"a": 1}
        ctx.set_scratch("$V", payload)
        PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
        self.assertEqual("v", ctx.get_state("$V")["k"])
        before = copy.deepcopy(ctx.get_state("$V"))
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($V,'k2','v2')", ctx.context)
        self.assertIn("plaita#69", str(cm.exception))
        self.assertEqual(before, ctx.get_state("$V"), "第二次求值必须零写入")

    def test_re_declaring_arms_exactly_the_next_evaluation(self):
        ctx = _context()
        payload = {"a": 1}
        ctx.set_scratch("$V", payload)
        ctx.set_scratch("$V", payload)          # 重新声明
        PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
        self.assertEqual("v", ctx.get_state("$V")["k"])
        with self.assertRaises(ValueError):
            PARSER.evaluate("$F.set($V,'k2','v2')", ctx.context)

    def test_declaration_is_consumed_by_the_read_not_by_the_clock(self):
        """声明只被"读到这个键的那次求值"消费；无关求值不消费它。"""
        ctx = _context()
        ctx.set_scratch("$V", {"a": 1})
        PARSER.evaluate("$INPUT.items[0]", ctx.context)      # 无关求值
        PARSER.evaluate("$F.set($V,'k','v')", ctx.context)   # 声明仍在
        self.assertEqual("v", ctx.get_state("$V")["k"])

    def test_engine_materialised_values_keep_their_own_scope_semantics(self):
        """scratch 不改变 `ctx.evaluate({...})` 的既有口径（#69 r6）。"""
        ctx = _context()
        ctx.set_state("$E", ctx.evaluate({"a": 1}))
        with self.assertRaises(ValueError):
            PARSER.evaluate("$F.set($E,'k','v')", ctx.context)


class TestScratchBoundariesAreNegativeControls(unittest.TestCase):
    """边界：每一条都**必须仍被拒**，否则 scratch 就是新的写穿通道。"""

    def test_set_state_alone_is_still_refused_by_provenance(self):
        """显式 API 是唯一入口：普通宿主绑定照旧 default-deny。"""
        ctx = _context()
        ctx.set_state("$V", {"a": 1})
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
        self.assertIn("plaita#69", str(cm.exception))
        self.assertEqual({"a": 1}, ctx.get_state("$V"))

    def test_nested_container_inside_a_scratch_value_is_still_refused(self):
        """只登记被声明的对象，不登记它的图（否则声明就是一次图级洗白）。"""
        ctx = _context()
        ctx.set_scratch("$V", {"cfg": {"a": 1}})
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($V.cfg,'k','v')", ctx.context)
        self.assertIn("plaita#69", str(cm.exception))
        self.assertEqual({"a": 1}, ctx.get_state("$V")["cfg"])

    def test_nesting_a_live_persisted_object_inside_scratch_writes_nothing(self):
        """把活根塞进 scratch 的图里**不能**改写它：嵌套目标仍被拒、$INPUT 零写入。"""
        ctx = _context()
        live = ctx.get_state("$INPUT")
        before = copy.deepcopy(ctx.get_state("$INPUT"))
        ctx.set_scratch("$V", {"alias": live, "own": {"a": 1}})
        with self.assertRaises(ValueError):
            PARSER.evaluate("$F.set($V.alias,'x','y')", ctx.context)
        self.assertEqual(before, ctx.get_state("$INPUT"))
        # 外层仍是**被声明的那个对象** ⇒ 重新声明后可写。这正好说明边界在
        # "对象身份"（只登记被声明的对象），不在"图"——声明一张含活根的图，
        # 图里的活根照旧不是 fresh。
        ctx.set_scratch("$V", ctx.get_state("$V"))
        PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
        self.assertEqual("v", ctx.get_state("$V")["k"])
        self.assertEqual(before, ctx.get_state("$INPUT"))
        self.assertEqual(live, ctx.get_state("$INPUT"))

    def test_declaring_a_live_persisted_object_is_refused_by_name(self):
        """声明对象**就是**活持久化对象 ⇒ 具名拒绝（登记它等于开一条写穿通道）。"""
        ctx = _context()
        ctx.set_scratch("$V", ctx.get_state("$INPUT"))
        before = copy.deepcopy(ctx.get_state("$INPUT"))
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
        message = str(cm.exception)
        self.assertIn("plaita#164", message)
        self.assertIn("$INPUT", message)
        self.assertEqual(before, ctx.get_state("$INPUT"))
        self.assertEqual(before, ctx.get_state("$V"))

    def test_checkpoint_schema_keys_cannot_be_declared_as_scratch(self):
        ctx = _context()
        for key in ("$INPUT", "$NODE", "$GLOBAL", "$PARENT", "$ENV",
                    "$FLOW_ID", "$EXECUTION_ID", "EXPRESS_PREFIX"):
            with self.subTest(key=key):
                with self.assertRaises(ValueError) as cm:
                    ctx.set_scratch(key, {})
                self.assertIn("schema key", str(cm.exception))

    def test_existing_checkpoint_binding_cannot_be_shadowed_by_scratch(self):
        ctx = _context()
        ctx.set_state("$V", {"a": 1})
        with self.assertRaises(ValueError) as cm:
            ctx.set_scratch("$V", {"b": 2})
        self.assertIn("already holds a checkpointed binding", str(cm.exception))

    def test_set_state_supersedes_a_scratch_binding(self):
        ctx = _context()
        ctx.set_scratch("$V", {"a": 1})
        ctx.set_state("$V", {"b": 2})
        self.assertEqual([], ctx.scratch_keys())
        self.assertEqual({"b": 2}, ctx.get_state("$V"))
        with self.assertRaises(ValueError):
            PARSER.evaluate("$F.set($V,'k','v')", ctx.context)

    def test_blank_and_non_string_keys_are_refused(self):
        ctx = _context()
        for key in ("", None, 7):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    ctx.set_scratch(key, {})


class TestIssue69InvariantsSurviveScratch(unittest.TestCase):
    """验收 2：``#69`` 的不变量在 scratch 存在时**没有松动**（含 0 写穿）。"""

    def test_five_roots_and_every_shape_still_refused_with_scratch_armed(self):
        keys = list(ROOT_SHAPES) + ["$V"]
        root_keys = list(ROOT_SHAPES)
        for root, target in ROOT_SHAPES.items():
            for shape, template in MUTATOR_SHAPES.items():
                with self.subTest(root=root, shape=shape):
                    ctx = _context()
                    ctx.set_scratch("$V", _payload())      # 门开着，但根不吃这套
                    before = _snapshot(ctx, keys)
                    with self.assertRaises(ValueError) as cm:
                        PARSER.evaluate(template.format(target=target), ctx.context)
                    self.assertIn("plaita#69", str(cm.exception))
                    self.assertIn(target, str(cm.exception))
                    self.assertEqual(before, _snapshot(ctx, keys), "live context changed")
                    # scratch 键**有意**不在 checkpoint 里：checkpoint 面只比对五根。
                    self.assertEqual(
                        {k: before[k] for k in root_keys},
                        {k: ctx.to_dict()[k] for k in root_keys},
                        "checkpoint changed")

    def test_scratch_declaration_does_not_leak_into_neighbouring_contexts(self):
        armed = _context()
        armed.set_scratch("$V", {"a": 1})
        other = _context()
        other.set_state("$V", {"a": 1})
        with self.assertRaises(ValueError):
            PARSER.evaluate("$F.set($V,'k','v')", other.context)
        self.assertEqual({"a": 1}, other.get_state("$V"))

    def test_scratch_key_alone_cannot_reach_the_persisted_roots(self):
        """``$F.set($V,'k',$INPUT)``：目标 fresh 了，但值参位的活根仍被拒、零写穿。"""
        ctx = _context()
        ctx.set_scratch("$V", {})
        before = copy.deepcopy(ctx.get_state("$INPUT"))
        with self.assertRaises(ValueError):
            PARSER.evaluate("$F.set($V,'k',$INPUT)", ctx.context)
        self.assertEqual({}, ctx.get_state("$V"))
        self.assertEqual(before, ctx.get_state("$INPUT"))

    def test_scratch_object_is_not_pickled_into_the_checkpoint_channel(self):
        """私有副本语义的可观测面：checkpoint 往返里没有 scratch 对象。"""
        ctx = _context()
        payload = {"a": 1}
        ctx.set_scratch("$V", payload)
        restored = ExecutionContext.from_dict(ctx.to_dict())
        self.assertNotIn("$V", list(restored.context))


if __name__ == "__main__":
    unittest.main()
