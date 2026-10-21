"""plaita#69 — **机械生成**的就地 mutate 门分类矩阵（自证封闭）。

规则（一句可判定谓词，与实现/文档逐字一致）：

    目标表达式静态解析为一个变量路径 thunk，且其根键映射到 ``CheckpointState`` 的五个
    持久化字段之一（``input_value`` / ``node_results`` / ``global_context`` /
    ``parent_context`` / ``env``，含 ``express_*_name`` 配置别名），而该路径作为
    ``has_side_effects=True`` 函数的第一实参、求值上下文是 live ``CheckpointState``
    时 ⇒ **拒绝**并点名完整路径；否则放行；另有两个子句：callee 是可能返回 live 引用的
    函数（``get``/``getDictValue``/``getListItem``/``ifelse``/``and``/``or``）或未声明
    ``fresh_producer`` 的调用 ⇒ 拒绝并点名 callee；字面量 / 模板 / 模板串 / 非根变量 ⇒
    放行。

矩阵维度（程序化笛卡尔积，不是手挑形状）：

* 写形式 = 9 个注册为 ``has_side_effects=True`` 的内建函数（列表 3 + 字典 6）；
* 目标解析形式 = 五个持久化根（含配置别名 ``$IN``/``$G``）× 6 种路径
  （本体 / ``.cfg`` / ``.nested.d`` / ``.items`` / ``.lst`` / ``.items[0]``）
  ——见 ``PATH_FORMS``（本文件是这一维的唯一事实来源）；
* 作用域 = `ExecutionContext.evaluate`（所有节点参数走同一入口）× 真实 flow 的
  ``assignment.output`` / ``end.output`` × 循环条件（隔离副本）;
* **容器键名（r4）** = 规范根名 / 非规范（像根但不是）/ 任意键名
  × 容器值种类（活根本体 / 活根子对象 / 物化副本）× 嵌套（平铺 / dict / list）
  × 承载节点（``InlineFlow`` / ``ReferenceFlow``）——见
  ``CONTAINER_KEY_NAMES`` / ``CONTAINER_VALUE_KINDS`` / ``CONTAINER_NESTINGS``;
* Python 语法族（``=`` / ``+=`` / ``del`` / ``setattr`` / ``__dict__`` / ``vars()`` /
  ``globals()`` / ``eval`` / ``exec`` / ``copy`` / ``deepcopy``）在表达式文法里**不可
  表达**——机械验证（ParseException / 字面量 / NameError），不是拒绝门的一部分。

重算（生成器与文档同源，命令原样可跑）：

    PATH=<venv>/bin:$PATH PYTHONPATH=. python scripts/issue69_mutation_matrix/table.py

本文件同时是生成器的数据源（``build_matrix()`` / ``run_cell()`` 等由生成器导入），
文档里的计数就是本文件断言出来的计数。
"""

from __future__ import annotations

import copy
import functools
import json
import pathlib
import unittest

from plaita.core import types
from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution
from plaita.core.expression import ExpressionRegistry, FunctionCategory
from plaita.core.expression_parser import ExpressionParser, freshness_scope
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.decide import CONDITION_OP_EQ, Condition
from plaita.node.loop import Loop, Map, Reduce

# ---------------------------------------------------------------------------
# 维度
# ---------------------------------------------------------------------------

ROOTS = ("$INPUT", "$GLOBAL", "$NODE", "$PARENT", "$ENV")
ALIASES = {"$INPUT": "$IN", "$GLOBAL": "$G"}
PATH_FORMS = ("{r}", "{r}.cfg", "{r}.nested.d", "{r}.items", "{r}.lst", "{r}.items[0]")

#: 写形式（9 个内建就地函数）→ (目标种类, 表达式模板)
MUTATOR_SPEC = {
    "pop": ("list", "$F.pop({t},0)"),
    "delListItem": ("list", "$F.delListItem({t},0)"),
    "setListItem": ("list", "$F.setListItem({t},0,99)"),
    "set": ("dict", "$F.set({t},'k','v')"),
    "delete": ("dict", "$F.delete({t},'a')"),
    "clear": ("dict", "$F.clear({t})"),
    "setDictValue": ("dict", "$F.setDictValue({t},'k','v')"),
    "delDictValue": ("dict", "$F.delDictValue({t},'a')"),
    "clearDict": ("dict", "$F.clearDict({t})"),
}

#: 合法目标（允许桶）样本
ALLOW_TARGETS = {
    "non-root-var": "$VAR",
    "non-root-child": "$VAR.cfg",
    "producer-json_loads": "$F.json_loads($INPUT.raw)",
    "producer-slice": "$F.slice($INPUT.items,0,3)",
    "producer-reverse": "$F.reverse($INPUT.items)",
    "producer-append": "$F.append($INPUT.items,4)",
    "producer-insert": "$F.insert($INPUT.items,0,4)",
    "producer-sort": "$F.sort($INPUT.items)",
    "producer-concat": "$F.concat('a','b')",
    "producer-json_literal": "$F.json_loads('{\"a\": 1}')",
}

#: live 引用 / 未声明 callee（拒绝桶）
LIVE_TARGETS = {
    "live-get": "$F.get($INPUT,'cfg')",
    "live-getDictValue": "$F.getDictValue($INPUT,'cfg')",
    "live-getListItem": "$F.getListItem($INPUT.items,0)",
    "live-ifelse": "$F.ifelse(1,$INPUT.cfg,$INPUT.cfg)",
    "live-and": "$F.and($INPUT.cfg,$INPUT.cfg)",
    "live-or": "$F.or($INPUT.cfg,$INPUT.cfg)",
    "undeclared-copy": "$F.copy($INPUT.cfg)",
    "undeclared-deepcopy": "$F.deepcopy($INPUT.cfg)",
}

#: r3 F2 允许半：**同样的 callee** 只在非根 `$VAR` 上取值（表达式里没有持久根）
#: ⇒ 不得因 callee 身份被拒（owner 硬要求：合法用法不得误拒）。
NON_ROOT_LIVE_TARGETS = {
    "nonroot-get": "$F.get($VAR,'cfg')",
    "nonroot-getDictValue": "$F.getDictValue($VAR,'cfg')",
    "nonroot-getListItem": "$F.getListItem($VAR.lst,0)",
    "nonroot-ifelse": "$F.ifelse(1,$VAR.cfg,$VAR.cfg)",
    "nonroot-and": "$F.and($VAR.cfg,$VAR.cfg)",
    "nonroot-or": "$F.or($VAR.cfg,$VAR.cfg)",
    "nonroot-slice": "$F.slice($VAR.lst,0,1)",
    "nonroot-values": "$F.values($VAR.cfg)",
}

#: r3 F2 拒绝半：callee 自己的实参触达持久根 ⇒ 任意实参位都拒。
CALLEE_ROOT_VALUE_TEMPLATES = {
    "get": "$F.set($VAR,'k',$F.get({p},'cfg'))",
    "getDictValue": "$F.set($VAR,'k',$F.getDictValue({p},'cfg'))",
    "getListItem": "$F.set($VAR,'k',$F.getListItem({p}.items,0))",
    "ifelse": "$F.set($VAR,'k',$F.ifelse(1,{p}.cfg,{p}.cfg))",
    "and": "$F.set($VAR,'k',$F.and({p}.cfg,{p}.cfg))",
    "or": "$F.set($VAR,'k',$F.or({p}.cfg,{p}.cfg))",
}

#: r3 F1：浅 producer（新容器 / 视图，**元素仍别名**）作值参 —— 逐格必须去别名。
SHALLOW_VALUE_TEMPLATES = {
    "slice": "$F.set($VAR,'k',$F.slice({p}.items,0,2))",
    "reverse": "$F.set($VAR,'k',$F.reverse({p}.items))",
    "append": "$F.set($VAR,'k',$F.append({p}.items,$F.json_loads('{{\"n\": 9}}')))",
    "extend": "$F.set($VAR,'k',$F.extend({p}.items,$F.json_loads('[]')))",
    "insert": "$F.set($VAR,'k',$F.insert({p}.items,1,$F.json_loads('{{\"n\": 9}}')))",
    "add": "$F.set($VAR,'k',$F.add({p}.items,$F.json_loads('[]')))",
    "mul": "$F.set($VAR,'k',$F.mul({p}.items,1))",
    "addListItem": "$F.set($VAR,'k',$F.addListItem({p}.items,$F.json_loads('{{\"n\": 9}}')))",
    "insertListItem": "$F.set($VAR,'k',$F.insertListItem({p}.items,1,$F.json_loads('{{\"n\": 9}}')))",
    "sort": "$F.set($VAR,'k',$F.sort({p}.items,'n'))",
    "remove": "$F.set($VAR,'k',$F.remove({p}.items,{p}.items[0]))",
    "keys": "$F.set($VAR,'k',$F.keys({p}.cfg))",
    "values": "$F.set($VAR,'k',$F.values({p}.cfg))",
    "items": "$F.set($VAR,'k',$F.items({p}.cfg))",
    "getDictKeys": "$F.set($VAR,'k',$F.getDictKeys({p}.cfg))",
    "getDictValues": "$F.set($VAR,'k',$F.getDictValues({p}.cfg))",
}

#: #69 r3 的元素是**对象**的载荷（标量元素没有第二条表达式可以就地改写）。
ALIAS_PAYLOAD = lambda: {"items": [{"n": 1}, {"n": 2}], "lst": [{"n": 1}, {"n": 2}],
                         "cfg": {"a": {"deep": 1}, "b": {"deep": 2}},
                         "nested": {"lst": [{"n": 1}], "d": {"a": 1}},
                         "raw": '{"items": [{"n": 1}]}'}

#: 表达式文法里不可表达的 Python 语法族（机械验证：无写入发生）
NOT_EXPRESSIBLE = (
    # Python 语法族
    "$INPUT += 1", "del $INPUT", "$INPUT['cfg'] = 1", "$INPUT.items += [4]",
    # 字符串下标：文法是「点属性 + 整数下标」，`['cfg']` 直接 ParseException
    "$INPUT['cfg']", '$INPUT["cfg"]', "$F.set($INPUT['cfg'],'k','v')",
    "$F.setattr($INPUT,'cfg',{})", "$F.__setitem__($INPUT,'k',1)",
    "$F.vars($INPUT)", "$F.eval('$INPUT')", "$F.exec('x=1')",
)

PAYLOAD = lambda: {"items": [1, 2, 3], "lst": [1, 2, 3], "cfg": {"a": 1},
                   "nested": {"lst": [1, 2, 3], "d": {"a": 1}}, "raw": '{"q": 0}'}

PARSER = ExpressionParser.for_prefix("$")


class _Cfg:
    expose_env = None
    global_context: dict = {}
    flow_id = "m69"


def _engine_value(ctx: ExecutionContext, value):
    """r5：让**引擎**物化 ``$VAR`` 的初值 ⇒ 它可被证明 fresh（default-deny 的允许前提）。

    宿主直接 ``set_state("$VAR", {...})`` 时对象来源不可证 ⇒ 写它具名拒绝（owner
    决策边界，见 ``TestIssue69R5Provenance::test_host_bound_non_root_key_is_refused``）。
    """
    return ctx.evaluate(value)


def _context(alias: bool = False) -> ExecutionContext:
    kwargs = ({"express_input_name": "IN", "express_global_name": "G"} if alias else {})
    ctx = ExecutionContext(**kwargs)
    ctx.setup_flow(_Cfg(), ({},), {})
    for key in ROOTS:
        ctx.set_state(key, PAYLOAD())
    ctx.set_state("$VAR", _engine_value(ctx, PAYLOAD()))
    return ctx


def _alias_context(alias: bool = False) -> ExecutionContext:
    """#69 r3 的载荷：元素是**对象**（浅 producer 的别名只有可变元素能写穿）。"""
    kwargs = ({"express_input_name": "IN", "express_global_name": "G"} if alias else {})
    ctx = ExecutionContext(**kwargs)
    ctx.setup_flow(_Cfg(), ({},), {})
    for key in ROOTS:
        ctx.set_state(key, ALIAS_PAYLOAD())
    if alias:
        # 别名根键在 setup_flow 后是空容器 / 流程元数据；本矩阵要**真载荷**才谈得上别名。
        ctx.set_state("$IN", ALIAS_PAYLOAD())
        ctx.set_state("$G", ALIAS_PAYLOAD())
    ctx.set_state("$VAR", _engine_value(ctx, {}))
    return ctx


def _mutable_ids(obj, out: set) -> None:
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


def _live(ctx: ExecutionContext) -> dict:
    keys = list(ROOTS) + ["$VAR"]
    if ctx.get_state("$IN") is not None:
        keys += ["$IN", "$G"]
    return {k: copy.deepcopy(ctx.get_state(k)) for k in keys if ctx.get_state(k) is not None}


def run_cell(expression: str, ctx: ExecutionContext, registry=None):
    """返回 (outcome, detail, live_changed)：outcome ∈ refuse/allow/na/native-error。"""
    before = _live(ctx)
    try:
        PARSER.evaluate(expression, ctx.context, registry)
        outcome = "allow"
    except ValueError as exc:
        return "refuse", str(exc), _live(ctx) != before
    except (NameError, AttributeError, TypeError, KeyError) as exc:
        return "native-error", f"{type(exc).__name__}", _live(ctx) != before
    return outcome, "", _live(ctx) != before


def _one_evaluation(method):
    """r6：把整个测试体放进**一次**逻辑求值。

    ``fresh`` 在 r6 收紧为"**本次**求值由引擎新建"。矩阵夹具（``_context()`` 用
    ``ctx.evaluate`` 物化 ``$VAR``）模拟的正是"引擎在同一次求值里造出容器、随后就地
    改写它"这一合法用法，因此在作用域内跑；**跨求值**必须被拒的反面性质由
    ``TestIssue69R5FreshnessScope`` 的裸调用用例（不带本装饰器）钉住。
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with freshness_scope():
            return method(self, *args, **kwargs)
    return wrapper


def build_matrix():
    """程序化生成 persisted-root 拒绝桶：(cell_id, expression, root, path)。"""
    cells = []
    for root in ROOTS:
        keys = [root] + ([ALIASES[root]] if root in ALIASES else [])
        for target_root in keys:
            for form in PATH_FORMS:
                target = form.format(r=target_root)
                for mutator, (kind, tpl) in MUTATOR_SPEC.items():
                    cells.append((f"{mutator}|{target}", tpl.format(t=target),
                                  target_root, target))
    return cells


#: 值参位模板（A1, #69 r2）：把根路径放进**非目标**实参位。`clear`/`clearDict`
#: 只有一个实参（目标），没有值参位。
VALUE_ARG_TEMPLATES = {
    "pop": "$F.pop($VAR.items,{p})",
    "delListItem": "$F.delListItem($VAR.items,{p})",
    "setListItem": "$F.setListItem($VAR.lst,0,{p})",
    "set": "$F.set($VAR,'k',{p})",
    "delete": "$F.delete($VAR.cfg,{p})",
    "setDictValue": "$F.setDictValue($VAR.cfg,'k',{p})",
    "delDictValue": "$F.delDictValue($VAR.cfg,{p})",
}

#: 逃生口（fresh producer 包一层的根）——必须允许。
ESCAPE_HATCH_TEMPLATES = (
    "$F.set($VAR,'k',$F.json_loads($F.json_dumps({p})))",
    "$F.set($VAR,'k',$F.slice({p},0,1))",
)


def build_value_arg_matrix():
    """A1 值参位笛卡尔积：(cell_id, expression, root, path)。"""
    cells = []
    for root in ROOTS:
        keys = [root] + ([ALIASES[root]] if root in ALIASES else [])
        for target_root in keys:
            for form in PATH_FORMS:
                path = form.format(r=target_root)
                for mutator, tpl in VALUE_ARG_TEMPLATES.items():
                    cells.append((f"{mutator}|value|{path}", tpl.format(p=path),
                                  target_root, path))
    return cells


def build_shallow_value_matrix():
    """r3 F1 笛卡尔积：(cell_id, expression, root, producer)。

    16 个浅 producer × 7 根（五规范根 + 两别名）= 112 格：逐格求值后**不得**与根共享
    任何可变对象，随后对存下来的值做一次普通就地写**不得**写穿根。
    """
    cells = []
    for root in ROOTS:
        keys = [root] + ([ALIASES[root]] if root in ALIASES else [])
        for target_root in keys:
            for producer, tpl in SHALLOW_VALUE_TEMPLATES.items():
                cells.append((f"{producer}|value|{target_root}", tpl.format(p=target_root),
                              target_root, producer))
    return cells


def build_callee_root_value_matrix():
    """r3 F2 拒绝半：(cell_id, expression, root) —— callee 实参触达根 ⇒ 值参位拒。"""
    cells = []
    for root in ROOTS:
        keys = [root] + ([ALIASES[root]] if root in ALIASES else [])
        for target_root in keys:
            for callee, tpl in CALLEE_ROOT_VALUE_TEMPLATES.items():
                cells.append((f"{callee}|value|{target_root}", tpl.format(p=target_root),
                              target_root))
    return cells


def build_non_root_callee_matrix():
    """r3 F2 允许半：(cell_id, expression, label) —— 非根 callee × 9 写形式。"""
    cells = []
    for label, target in NON_ROOT_LIVE_TARGETS.items():
        for mutator, (kind, tpl) in MUTATOR_SPEC.items():
            cells.append((f"{mutator}|{label}", tpl.format(t=target), label))
    return cells


class TestIssue69GeneratedMatrix(unittest.TestCase):
    """生成的拒绝桶：每个 cell 都必须拒绝、点名路径、且零写入。"""

    def test_generated_persisted_root_cells_all_refuse(self):
        cells = build_matrix()
        alias_ctx = _context(alias=True)
        misses, by_root, refused = [], {}, 0
        for cell_id, expression, root, path in cells:
            with self.subTest(cell=cell_id):
                ctx = alias_ctx if root in ("$IN", "$G") else _context()
                outcome, detail, changed = run_cell(expression, ctx)
                by_root[root] = by_root.get(root, 0) + 1
                if outcome != "refuse":
                    misses.append((cell_id, outcome))
                    continue
                refused += 1
                if path not in detail or root not in detail or "plaita#69" not in detail:
                    misses.append((cell_id, "bad-message", detail[:80]))
                if changed:
                    misses.append((cell_id, "live-write"))
        self.assertEqual([], misses[:5])
        # 计数落盘改为**显式 opt-in**（r4 起不再默认写全局 /tmp：测试不得有全局副作用）。
        # 生成器（scripts/issue69_mutation_matrix/table.py）自行算计数，不依赖本文件落盘。
        out = __import__("os").environ.get("PLAITA69_MATRIX_OUT")
        if out:
            pathlib.Path(out).write_text(json.dumps(
                {"total": len(cells), "refused": refused, "by_root": by_root},
                ensure_ascii=False))
        # 5 根 × 6 路径 × 9 写形式 = 270，其中 2 根各多一个别名 +108 = 378
        self.assertEqual(378, len(cells))
        self.assertEqual(len(cells), refused)

    def test_generated_value_argument_cells_all_refuse(self):
        """A1：值参位（非目标）里的根路径逐格拒，消息含实参位 + 完整路径 + 根。"""
        cells = build_value_arg_matrix()
        misses, refused = [], 0
        for cell_id, expression, root, path in cells:
            with self.subTest(cell=cell_id):
                ctx = _context(alias=root in ("$IN", "$G"))
                outcome, detail, changed = run_cell(expression, ctx)
                if outcome != "refuse":
                    misses.append((cell_id, outcome))
                    continue
                refused += 1
                if (path not in detail or root not in detail
                        or "argument #" not in detail or "plaita#69" not in detail):
                    misses.append((cell_id, "bad-message", detail[:80]))
                if changed:
                    misses.append((cell_id, "live-write"))
        self.assertEqual([], misses[:5])
        self.assertEqual(7 * 7 * 6, len(cells))       # 7 值参位函数 × 7 根 × 6 路径
        self.assertEqual(len(cells), refused)

    @_one_evaluation
    def test_escape_hatch_wrapped_roots_are_allowed(self):
        """逃生口：fresh producer 包一层的根（json_loads(json_dumps(...))/slice）允许。"""
        for root in ROOTS:
            for tpl in ESCAPE_HATCH_TEMPLATES:
                with self.subTest(root=root, tpl=tpl[:24]):
                    ctx = _context(alias=root in ("$IN", "$G"))
                    path = root
                    before = copy.deepcopy(ctx.get_state(root))
                    outcome, detail, _ = run_cell(tpl.format(p=path), ctx)
                    self.assertNotIn("plaita#69", detail)
                    self.assertIn(outcome, ("allow", "native-error"))
                    self.assertEqual(before, ctx.get_state(root))

    @_one_evaluation
    def test_allow_half_is_asserted_not_just_refusals(self):
        ctx = _context()
        allowed = 0
        for label, target in ALLOW_TARGETS.items():
            for mutator, (kind, tpl) in MUTATOR_SPEC.items():
                expr = tpl.format(t=target)
                with self.subTest(label=label, mutator=mutator):
                    ctx = _context()
                    outcome, detail, changed = run_cell(expr, ctx)
                    # 白名单 producer 目标永不因门被拒
                    self.assertNotIn("plaita#69", detail)
                    if outcome == "allow":
                        allowed += 1
        self.assertGreater(allowed, 0)
        # 非根本体保持就地语义（收紧不得落到合法用法）
        ctx = _context()
        before = copy.deepcopy(ctx.get_state("$VAR"))
        outcome, _, _ = run_cell("$F.set($VAR,'k','v')", ctx)
        self.assertEqual("allow", outcome)
        self.assertNotEqual(before, ctx.get_state("$VAR"))

    def test_live_reference_and_undeclared_targets_refuse(self):
        refused = 0
        for label, target in LIVE_TARGETS.items():
            for mutator, (kind, tpl) in MUTATOR_SPEC.items():
                expr = tpl.format(t=target)
                with self.subTest(label=label, mutator=mutator):
                    ctx = _context()
                    outcome, detail, changed = run_cell(expr, ctx)
                    self.assertEqual("refuse", outcome, f"{label}: {detail[:80]}")
                    self.assertIn("plaita#69", detail)
                    self.assertFalse(changed)
                    refused += 1
        self.assertEqual(len(LIVE_TARGETS) * len(MUTATOR_SPEC), refused)

    def test_python_syntax_family_is_not_expressible(self):
        """`=` / `+=` / `del` / `setattr` / `eval` … 不在表达式文法里：机械验证无写入。"""
        for expr in NOT_EXPRESSIBLE:
            with self.subTest(expr=expr):
                ctx = _context()
                before = copy.deepcopy(ctx.get_state("$INPUT"))
                try:
                    PARSER.evaluate(expr, ctx.context)
                except Exception:                 # noqa: BLE001 - ParseException/NameError 都是"不可表达"
                    pass
                self.assertEqual(before, ctx.get_state("$INPUT"), "python-syntax form wrote!")

    def test_alias_root_is_refused_with_alias_path(self):
        ctx = _context(alias=True)
        outcome, detail, _ = run_cell("$F.set($IN.cfg,'k','v')", ctx)
        self.assertEqual("refuse", outcome)
        self.assertIn("$IN.cfg", detail)

    def test_flow_scopes_refuse_and_loop_condition_stays_isolated(self):
        """作用域维度：assignment/end 参数拒绝；循环条件（隔离副本）允许且不污染。"""
        from plaita.node import Assignment
        for node_type, output in (("assignment", "$F.set($GLOBAL.cfg,'k','v')"),
                                  ("end", "$F.pop($GLOBAL.items,0)")):
            with self.subTest(node=node_type):
                if node_type == "assignment":
                    nodes = [Start(id="s", next="x"),
                             Assignment(id="x", output_type=Property(data_type=types.OBJECT),
                                        output=output, next="e"),
                             End(id="e", resultType="success", output="$GLOBAL")]
                else:
                    nodes = [Start(id="s", next="e"),
                             End(id="e", resultType="success", output=output)]
                flow = Flow(flow_id=f"m69{node_type}", version="1", runtime="python",
                            global_context={"items": [1, 2, 3], "cfg": {"a": 1}},
                            nodes=nodes)
                with self.assertRaises(Exception) as cm:
                    FlowExecution().run(flow, {})
                message = str(cm.exception)
                self.assertIn("plaita#69", message)          # 具名路径随异常上抛
                self.assertIn("$GLOBAL", message)
        cond = Condition(field="$F.set($GLOBAL, 'counter', $LOOP-RESULT)",
                         operator=CONDITION_OP_EQ, value=0)
        flow = Flow(
            flow_id="m69loop", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT),
            global_context={"counter": 0, "nested": {"deep": {"x": 1}}},
            nodes=[Start(id="start", next="l"),
                   Loop(id="l", collection=[1, 2, 3], child_flow=self._echo(), condition=cond, next="end"),
                   End(id="end", resultType="success", output="$NODE.l")])
        execution = FlowExecution()
        try:                                       # mutant hygiene: death must be an assertion
            result = execution.run_compatible(flow, False, {})
        except Exception as exc:                   # noqa: BLE001 - re-surfaced as assert
            self.fail(f"isolated loop condition must not be refused: "
                      f"{type(exc).__name__}: {exc}")
        self.assertEqual(1, result)
        self.assertEqual(0, execution.context.get("$GLOBAL", {}).get("counter"))
        self.assertEqual({"deep": {"x": 1}}, execution.context.get("$GLOBAL", {}).get("nested"))

    @staticmethod
    def _echo() -> Flow:
        return Flow(flow_id="m69child", version="1", runtime="python",
                    input_type=Property(data_type=types.OBJECT, children={
                        "item": Property(data_type=types.ANY),
                        "index": Property(data_type=types.INTEGER)}),
                    output_type=Property(data_type=types.ANY),
                    nodes=[Start(id="s", next="e"),
                           End(id="e", resultType="success", output="$INPUT.item")])

    @_one_evaluation
    def test_context_kind_dimension(self):
        """上下文种类：live state 拒 / 循环隔离副本放行 / 数据容器里**活**根拒。

        r4 起容器判据是**对象身份**而非键名：数据容器里挂普通副本的规范根名（``$GLOBAL``
        指向一个新建 dict）**放行**并按就地语义生效——名字不能决定（r3 曾按名拒绝）。
        见 ``TestIssue69R4ContainerKeyAxis`` 的完整轴。
        """
        # (a) live CheckpointState ⇒ 拒
        ctx = _context()
        self.assertEqual("refuse", run_cell("$F.set($INPUT.cfg,'k','v')", ctx)[0])
        # (b) 循环隔离副本（带 ``_plaita_isolated_copy`` 标记）⇒ 放行且不污染 live
        from plaita.node.loop import _condition_context, _IsolatedConditionContext
        live = _context()
        class _E:
            context = live.context
            express_prefix = "$"
        isolated = _condition_context(_E(), "$GLOBAL", item=1, index=0)
        self.assertIsInstance(isolated, _IsolatedConditionContext)
        self.assertIsNone(PARSER.evaluate("$F.set($GLOBAL,'counter',1)", isolated))
        self.assertNotIn("counter", live.get_state("$GLOBAL"))
        # (c) 嵌套表达式串在**数据容器**上求值：
        #     (c) 规范根名下挂 `$GLOBAL` 键 + `x` 是表达式形状字符串：
        #     #268 终裁——运行期字符串是数据，`$INPUT.x` 原样返回，不再以持有
        #     容器为作用域二次求值；三种容器形态（活根 / 引擎物化副本 / 宿主
        #     自造副本）一律原样、零写入。（旧三层判定 c1/c2/c3 随层摘除。）
        #     对照锚：同一 mutator 写在**配置表达式**里，#69 判定照常生效——
        c = ExecutionContext(); c.setup_flow(_Cfg(), ({},), {})
        PARSER.evaluate("$GLOBAL", c.context)          # 登记活根身份
        before_global = c.get_state("$GLOBAL")
        c.set_state("$INPUT", {"$GLOBAL": c.get_state("$GLOBAL"), "cfg": {"a": 1}})
        with self.assertRaises(ValueError) as cm_cfg:
            PARSER.evaluate("$F.set($GLOBAL.cfg,'k','v')", c.context)   # 活根 ⇒ 拒
        self.assertIn("$GLOBAL.cfg", str(cm_cfg.exception))
        self.assertEqual(before_global, c.get_state("$GLOBAL"))         # 零写入
        # 数据形态：x 原样，三种容器一律零写入
        for extra in ({"$GLOBAL": c.get_state("$GLOBAL")},
                      {"$GLOBAL": {"cfg": {"a": 1}}},
                      {"$GLOBAL": {"cfg": {"a": 1}}.copy()}):
            c_data = ExecutionContext(); c_data.setup_flow(_Cfg(), ({},), {})
            c_data.set_state("$INPUT", {**extra, "x": "{% $F.set($GLOBAL.cfg,'k','v') %}"})
            self.assertEqual("{% $F.set($GLOBAL.cfg,'k','v') %}",
                             PARSER.evaluate("$INPUT.x", c_data.context))

    @_one_evaluation
    def test_t2_alias_through_another_context_key(self):
        """T2-1（r4 起**已关闭**）：宿主把某键设成持久化根的**同一对象**后，表达式侧只看到
        ``$V``——r3 靠键名判断，看不到别名 ⇒ 写穿；r4 改按**对象身份**判：``$V`` 当下的
        值就是活 ``$INPUT`` 根对象 ⇒ 具名拒绝、零写入。

        r6：目标位热路径不再对活 state 的非持久化键做 ``_live_alias_index`` 身份索引
        （实测 ~8µs/次），拒绝依据改为 default-deny 的 freshness（非 fresh 必拒），
        所以消息点名的是**实参路径** ``$V.cfg`` 而不是它触达的根；判决与零写穿不变。
        容器值身份探测（``_probe_container_alias``）在目标位保留，用于普通字典作用域。
        """
        ctx = _context()
        PARSER.evaluate("$INPUT", ctx.context)             # 登记活根身份
        ctx.set_state("$V", ctx.get_state("$INPUT"))       # host-level alias（同一对象）
        before = copy.deepcopy(ctx.get_state("$INPUT"))
        outcome, detail, changed = run_cell("$F.set($V.cfg,'k','v')", ctx)
        self.assertEqual("refuse", outcome)                # 身份判据 ⇒ 拒
        self.assertIn("plaita#69", detail)
        self.assertIn("$V.cfg", detail)                    # 消息点名实参路径
        self.assertFalse(changed)
        self.assertEqual(before, ctx.get_state("$INPUT"))
        self.assertEqual(before, ctx.to_dict()["$INPUT"])  # checkpoint 亦不变

    def test_declared_producer_is_allowed_and_undeclared_is_refused(self):
        def _make(fresh: bool) -> ExpressionRegistry:
            reg = ExpressionRegistry()
            reg.register("set", lambda a, b, c: a.update({b: c}), FunctionCategory.DICT,
                         has_side_effects=True)
            reg.register("temp_dict", lambda: {"a": 1}, FunctionCategory.DICT,
                         fresh_producer=fresh)
            return reg
        ctx = _context()
        outcome, detail, _ = run_cell("$F.set($F.temp_dict(),'k','v')", ctx, _make(False))
        self.assertEqual("refuse", outcome)
        self.assertIn("temp_dict", detail)
        ctx = _context()
        self.assertEqual("allow", run_cell("$F.set($F.temp_dict(),'k','v')", ctx, _make(True))[0])

    @_one_evaluation
    def test_generated_shallow_value_cells_are_detached(self):
        """r3 F1：16 浅 producer × 7 根 = 112 格 —— 存进 `$VAR` 的值必须与根零共享，
        随后的普通就地写不得写穿根（§F1 反例的机械展开）。"""
        cells = build_shallow_value_matrix()
        misses, checked = [], 0
        for cell_id, expression, root, producer in cells:
            with self.subTest(cell=cell_id):
                ctx = _alias_context(alias=root in ("$IN", "$G"))
                root_before = copy.deepcopy(ctx.get_state(root))
                try:
                    PARSER.evaluate(expression, ctx.context)
                except ValueError as exc:
                    misses.append((cell_id, "refused", str(exc)[:60]))
                    continue
                except (NameError, AttributeError, TypeError, KeyError) as exc:
                    misses.append((cell_id, "native", type(exc).__name__))
                    continue
                checked += 1
                stored = ctx.get_state("$VAR").get("k")
                if _shares_mutable_object(stored, ctx.get_state(root)):
                    misses.append((cell_id, "alias-stored"))
                    continue
                try:
                    PARSER.evaluate("$F.set($VAR.k[0],'x',9)", ctx.context)
                except (AttributeError, TypeError, KeyError, IndexError):
                    pass
                if ctx.get_state(root) != root_before:
                    misses.append((cell_id, "write-through"))
                if ctx.to_dict().get(root) != root_before:
                    misses.append((cell_id, "checkpoint-write-through"))
        self.assertEqual([], misses[:5])
        self.assertEqual(16 * 7, len(cells))
        self.assertEqual(len(cells), checked)
        # 浅桶必须是数据源驱动（不是实现内部自证）：verifier 点名的 8 个都在
        self.assertTrue({"slice", "reverse", "append", "sort", "values", "items",
                         "keys", "getDictValues"} <= set(SHALLOW_VALUE_TEMPLATES))

    def test_generated_callee_root_value_cells_refuse(self):
        """r3 F2 拒绝半：callee 自己的实参触达根 ⇒ 值参位逐格拒、零写入。"""
        cells = build_callee_root_value_matrix()
        misses, refused = [], 0
        for cell_id, expression, root in cells:
            with self.subTest(cell=cell_id):
                ctx = _alias_context(alias=root in ("$IN", "$G"))
                before = copy.deepcopy(ctx.get_state(root))
                outcome, detail, changed = run_cell(expression, ctx)
                if outcome != "refuse":
                    misses.append((cell_id, outcome))
                    continue
                refused += 1
                if "plaita#69" not in detail or "argument #" not in detail:
                    misses.append((cell_id, "bad-message", detail[:60]))
                if changed or ctx.get_state(root) != before:
                    misses.append((cell_id, "live-write"))
        self.assertEqual([], misses[:5])
        self.assertEqual(6 * 7, len(cells))
        self.assertEqual(len(cells), refused)

    @_one_evaluation
    def test_generated_non_root_callee_cells_are_allowed(self):
        """r3 F2 允许半：同样 8 个 callee 只在 `$VAR` 上取值 ⇒ 9 写形式逐格不得因门被拒。"""
        cells = build_non_root_callee_matrix()
        misses = []
        for cell_id, expression, label in cells:
            with self.subTest(cell=cell_id):
                ctx = _context()
                outcome, detail, _ = run_cell(expression, ctx)
                if "plaita#69" in detail:
                    misses.append((cell_id, "refused", detail[:60]))
        self.assertEqual([], misses[:5])
        self.assertEqual(8 * 9, len(cells))


# ---------------------------------------------------------------------------
# r4 结构轴：容器**键名** × 值种类 × 嵌套（verifier 反例的结构化展开）
# ---------------------------------------------------------------------------
# r3 的容器判据是名字白名单（``_CANONICAL_ROOT_NAMES``）：把活根挂在任意键名下
# （``{"$myroot": "$INPUT"}``）即可绕过。r4 改为**对象身份**：容器里可达的某个值
# 只要**就是**活持久化对象，就地改写就必然写穿——键名叫什么无关。本轴逐格断言
# "名字不决定"：同一 (值种类, 嵌套) 下三种键名的判决必须**完全一致**。
CONTAINER_KEY_NAMES = {
    "canonical": "$INPUT",
    "non-canonical": "$MYROOT",
    "arbitrary": "$zzz_q9",
}

#: 容器里那个键挂的值：活根本体 / 活根子对象 / 物化副本。
CONTAINER_VALUE_KINDS = ("live-root", "live-root-child", "materialised-copy")

#: 嵌套形态：平铺 / dict 包一层 / list 包一层。
CONTAINER_NESTINGS = ("flat", "dict", "list")

_NESTING_PATH = {"flat": "{k}", "dict": "{k}.wrap", "list": "{k}[0]"}


def _container_with(key: str, value: Any, nesting: str) -> dict:
    if nesting == "flat":
        return {key: value, "cmd": "text"}
    if nesting == "dict":
        return {key: {"wrap": value}, "cmd": "text"}
    if nesting == "list":
        return {key: [value], "cmd": "text"}
    raise AssertionError(nesting)


def _container_path(key: str, nesting: str) -> str:
    return _NESTING_PATH[nesting].format(k=key)


def build_container_axis_cells():
    """r4 容器轴笛卡尔积：(cell_id, key_bucket, key, value_kind, nesting)。"""
    cells = []
    for bucket, key in CONTAINER_KEY_NAMES.items():
        for kind in CONTAINER_VALUE_KINDS:
            for nesting in CONTAINER_NESTINGS:
                cells.append((f"{bucket}|{kind}|{nesting}", bucket, key, kind, nesting))
    return cells


def _live_objects_ctx():
    """返回 (ctx, live_root, live_root_child)：让求值器沿根路径交出身份并保鲜 state。

    ``ctx`` 必须被持有：活根身份登记挂在 state 的弱引用条目上（state 死亡即消失，
    见 ``expression_parser._live_alias_index``）。
    """
    ctx = _context()
    PARSER.evaluate("$INPUT", ctx.context)          # 登记活根本体
    PARSER.evaluate("$INPUT.cfg", ctx.context)      # 登记活根子对象
    live_root = ctx.get_state("$INPUT")
    return ctx, live_root, live_root["cfg"]


#: 结构半边 1（边界物化，``plaita/node/child.py``）的临时开关——关掉它才能证明
#: 半边 2（身份探测）**独立**成立（两半各自可证伪）。
_MATERIALISE_NAME = "_materialise_child_input"


class _Half1Disabled:
    def __enter__(self):
        import plaita.node.child as child_mod
        self._mod = child_mod
        self._saved = getattr(child_mod, _MATERIALISE_NAME, None)
        if self._saved is None:
            raise unittest.SkipTest("边界物化函数不存在（基线树）")
        setattr(child_mod, _MATERIALISE_NAME, lambda value: value)
        return self

    def __exit__(self, *exc):
        setattr(self._mod, _MATERIALISE_NAME, self._saved)
        return False


class TestIssue69R4ContainerKeyAxis(unittest.TestCase):
    """r4/r5：容器键名（规范/非规范/任意）× 值（活根/活根子对象/引擎物化副本）× 嵌套。

    判据分两层：r4 的**对象身份**探测（活根/活根子对象被具名拒绝、点名容器键路径与根）
    与 r5 的**freshness**（default-deny；宿主自造副本一律拒，引擎物化的副本才放行）。
    """

    @_one_evaluation
    def test_key_name_cannot_decide_and_copies_stay_allowed(self):
        ctx, live_root, live_child = _live_objects_ctx()
        before_root = copy.deepcopy(live_root)
        before_child = copy.deepcopy(live_child)
        outcomes: dict = {}
        misses = []
        for cell_id, bucket, key, kind, nesting in build_container_axis_cells():
            with self.subTest(cell=cell_id):
                if kind == "live-root":
                    value, expect = live_root, "refuse"
                elif kind == "live-root-child":
                    value, expect = live_child, "refuse"
                else:
                    # r5：副本必须由**引擎**物化（ctx.evaluate 重建容器 ⇒ 登记 fresh）；
                    # 宿主自造副本来源不可证 ⇒ 拒（见 test_host_copy_is_refused）。
                    value, expect = ctx.evaluate(copy.deepcopy(live_child)), "allow"
                container = _container_with(key, value, nesting)
                expr = f"$F.set({_container_path(key, nesting)},'k','v')"
                try:
                    PARSER.evaluate(expr, container)
                    outcome, detail = "allow", ""
                except ValueError as exc:
                    outcome, detail = "refuse", str(exc)
                outcomes[(kind, nesting, bucket)] = outcome
                if outcome != expect:
                    misses.append((cell_id, expect, outcome))
                    continue
                if expect == "refuse":
                    if "plaita#69" not in detail or "$INPUT" not in detail:
                        misses.append((cell_id, "bad-message", detail[:80]))
                    if bucket == "canonical":
                        if "$INPUT" not in detail:
                            misses.append((cell_id, "no-root-named"))
                    elif key not in detail:
                        misses.append((cell_id, "no-key-named", detail[:80]))
                else:
                    # 引擎物化副本：放行**且就地生效**——证明不是无差别拒绝（名字不决定
                    # 的另一半：规范键名下挂副本也必须放行，r3 曾按名误拒）。
                    if value.get("k") != "v":
                        misses.append((cell_id, "copy-not-mutated"))
        self.assertEqual([], misses[:5])
        # 零写穿：活根 / 活根子对象本体始终未被改写
        self.assertEqual(before_root, live_root)
        self.assertEqual(before_child, live_child)
        # 名字不可决定：同一 (值种类, 嵌套) 下三种键名判决必须一致
        for kind in CONTAINER_VALUE_KINDS:
            for nesting in CONTAINER_NESTINGS:
                verdicts = {outcomes[(kind, nesting, b)] for b in CONTAINER_KEY_NAMES}
                self.assertEqual(1, len(verdicts), f"{kind}/{nesting}: {verdicts}")
        self.assertEqual(27, len(outcomes))
        self.assertEqual(18, sum(1 for v in outcomes.values() if v == "refuse"))
        self.assertEqual(9, sum(1 for v in outcomes.values() if v == "allow"))

    @_one_evaluation
    def test_zero_registration_axis_refuses(self):
        """verifier #1：**不做任何前置求值**（同一活根子对象，求值器从没把它当路径终点）。

        r4 的 9 个 ``live-root-child`` 格在这种"零登记"状态下**全部放行并写穿**（实测量到）。
        r5 的 default-deny 不依赖任何登记副作用：活根子对象不是引擎新建的 ⇒ 逐格具名拒绝、
        零写穿；引擎物化的副本仍逐格放行。
        """
        ctx = _context()
        live_root = ctx.get_state("$INPUT")
        live_child = live_root["cfg"]           # 从未被求值器当路径终点交出过
        before_root = copy.deepcopy(live_root)
        before_child = copy.deepcopy(live_child)
        misses, kinds = [], {}
        for cell_id, bucket, key, kind, nesting in build_container_axis_cells():
            with self.subTest(cell=cell_id):
                if kind == "live-root":
                    value, expect = live_root, "refuse"
                elif kind == "live-root-child":
                    value, expect = live_child, "refuse"
                else:
                    value, expect = ctx.evaluate(copy.deepcopy(live_child)), "allow"
                container = _container_with(key, value, nesting)
                expr = f"$F.set({_container_path(key, nesting)},'k','v')"
                try:
                    PARSER.evaluate(expr, container)
                    outcome, detail = "allow", ""
                except ValueError as exc:
                    outcome, detail = "refuse", str(exc)
                kinds.setdefault(kind, set()).add(outcome)
                if outcome != expect:
                    misses.append((cell_id, expect, outcome, detail[:70]))
                elif expect == "refuse" and "plaita#69" not in detail:
                    misses.append((cell_id, "no-named-refusal", detail[:70]))
        self.assertEqual([], misses[:5])
        self.assertEqual(before_root, live_root)
        self.assertEqual(before_child, live_child)
        self.assertEqual({"refuse"}, kinds["live-root"])
        self.assertEqual({"refuse"}, kinds["live-root-child"])
        self.assertEqual({"allow"}, kinds["materialised-copy"])

    def test_host_copy_is_refused(self):
        """r5 收紧：**宿主自造**的副本（copy.deepcopy）来源不可证 fresh ⇒ 具名拒绝。

        合法替代（构造证明）：``$F.json_loads($F.json_dumps(...))`` 或引擎物化的容器。
        """
        ctx = _context()
        host_copy = copy.deepcopy(ctx.get_state("$INPUT")["cfg"])
        container = {"$myroot": host_copy, "cmd": "text"}
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($myroot,'k','v')", container)
        self.assertIn("provenance could not be proven fresh", str(cm.exception))
        self.assertEqual({"a": 1}, host_copy)


def _r4_child_flow(mutator_expr: str = "$F.set($INPUT.b.b,'k','v')") -> Flow:
    from plaita.node import Assignment
    return Flow(flow_id="r4-child", version="1", runtime="python",
                input_type=Property(data_type=types.OBJECT),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="s", next="a"),
                       Assignment(id="a", output=mutator_expr,
                                  output_type=Property(data_type=types.ANY), next="e"),
                       End(id="e", resultType="success", output="$NODE.a")])


def _r4_parent_flow(node) -> Flow:
    return Flow(flow_id="r4-parent", version="1", runtime="python",
                input_type=Property(data_type=types.OBJECT),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="start", next="c"), node,
                       End(id="end", resultType="success", output="$NODE.c")])


def _r4_flow_node(node_kind: str, key: str, nesting: str, value_expr: str,
                  mutator_expr: str):
    """#268 后：mutator 直接写在子 assignment 的**配置**层（表达式只活在配置里）。
    旧的「cmd 字段装嵌套串、子执行路径读时二次解析」通道已随契约关闭。子输入
    容器仍按 key × nesting 构造边界形状（mutator 的寻址目标）。"""
    from plaita.node.child import InlineFlow, ReferenceFlow
    mapping = _container_with(key, value_expr, nesting)
    mapping.pop("cmd", None)
    cls = InlineFlow if node_kind == "inline" else ReferenceFlow
    child = _r4_child_flow(mutator_expr)
    return cls(id="c", input=mapping, child_flow=child, next="end")


#: 边界表达式：活根本体 / 活根子对象 / 物化副本（deep producer 显式派生）。
BOUNDARY_VALUE_EXPRS = {
    "live-root": "$INPUT",
    "live-root-child": "$INPUT.cfg",
    "materialised-copy": '$F.json_loads($F.json_dumps($INPUT.cfg))',
}


class TestIssue69R4ChildFlowBoundaryAxis(unittest.TestCase):
    """r4：``InlineFlow`` / ``ReferenceFlow`` 的 ``input`` 容器 —— 键名 × 值 × 嵌套。

    两半的独立性在本类里被钉住：

    * 两半都开（出厂）：边界物化（半边 1）在源头去别名 ⇒ 子流程只看私有副本、无具名
      拒绝、父运行态与 checkpoint 零变化；
    * 关掉半边 1：身份探测（半边 2）独立成立 —— 活根 / 活根子对象逐格**具名拒绝**且
      点名键名与根；物化副本逐格**放行**（并按就地语义生效在副本上），父运行态零变化。
    """

    def _run(self, node_kind, key, kind, nesting):
        expr = BOUNDARY_VALUE_EXPRS[kind]
        # 子流程视角的寻址：mapping 是子流程的 $INPUT，键名（$ 前缀）是其下的
        # 字面键 → 地址 = $INPUT.<container_path>
        node = _r4_flow_node(node_kind, key, nesting, expr,
                             f"$F.set($INPUT.{_container_path(key, nesting)},'k','v')")
        execution = FlowExecution()
        start = {"cfg": {"a": 1}}
        raised = None
        try:
            execution.run_compatible(_r4_parent_flow(node), False, copy.deepcopy(start))
        except Exception as exc:               # noqa: BLE001 - 判决就是本测试的对象
            raised = exc
        live = execution.context.get("$INPUT")
        checkpoint = execution.context.to_checkpoint_dict().get("$INPUT")
        detail = str(raised) if raised is not None else ""
        return {
            "refused": "plaita#69" in detail,
            "raised": type(raised).__name__ if raised else None,
            "detail": detail,
            "live": live,
            "checkpoint": checkpoint,
            "start": start,
        }

    def test_both_halves_isolate_every_cell(self):
        """r6：边界副本在**父求值**里物化，子执行自己的求值里它不是 fresh。

        半边 1（``_materialise_child_input``）仍保证活根**按引用**进不了子流程，半边 2
        仍是身份证据；但 r6 的 freshness 定义是"**本次求值**由引擎新建"，所以子执行里
        对边界副本的就地改写逐格**具名拒绝**（此前是"改在私有副本上、不报错"）。
        零写穿这一性质在两套机制下都不变。
        """
        misses = []
        for kind in CONTAINER_VALUE_KINDS:
            for node_kind in ("inline", "reference"):
                for bucket, key in CONTAINER_KEY_NAMES.items():
                    for nesting in CONTAINER_NESTINGS:
                        with self.subTest(v=kind, node=node_kind, k=bucket, n=nesting):
                            r = self._run(node_kind, key, kind, nesting)
                            if r["live"] != r["start"] or r["checkpoint"] != r["start"]:
                                misses.append((kind, node_kind, bucket, nesting, "write-through"))
                            if "plaita#69" not in r["detail"]:
                                misses.append((kind, node_kind, bucket, nesting,
                                               f"expected named refusal: {r['detail'][:60]}"))
        self.assertEqual([], misses[:5])

    def test_child_input_boundary_never_aliases_the_parent_object(self):
        """r6：物化半边（半边 1）必须**真的**去别名，不能只靠 freshness 兜底。

        行为层的"子执行改写被拒"现在由 freshness 独立保证，所以单看行为，把
        ``_materialise_child_input`` 换回"按引用传递"也可能仍然零写穿。这里直接钉住
        **结构**性质：边界物化的返回值与输入零共享可变对象（活根无法按引用进子流程）。
        """
        from plaita.node.child import _materialise_child_input
        live = {"cfg": {"a": 1}, "items": [{"n": 1}]}
        snapshot = _materialise_child_input(live)
        self.assertIsNot(snapshot, live)
        self.assertFalse(_shares_mutable_object(snapshot, live))
        self.assertEqual(live, snapshot)

    def test_half2_alone_classifies_by_identity_not_by_key_name(self):
        """关掉半边 1：身份探测是独立证据；r6 下 freshness 独立再拒，三种键名判决一致。

        活根 / 活根子对象 ⇒ 容器值身份探测点名 ``$INPUT``；物化副本（r6：父求值里
        造出来的，子求值里不是 fresh）⇒ freshness 具名拒绝。两者都零写穿。
        """
        outcomes: dict = {}
        misses = []
        with _Half1Disabled():
            for kind in CONTAINER_VALUE_KINDS:
                for node_kind in ("inline", "reference"):
                    for bucket, key in CONTAINER_KEY_NAMES.items():
                        for nesting in CONTAINER_NESTINGS:
                            cell = (kind, node_kind, bucket, nesting)
                            with self.subTest(v=kind, node=node_kind, k=bucket, n=nesting):
                                r = self._run(node_kind, key, kind, nesting)
                                if r["live"] != r["start"] or r["checkpoint"] != r["start"]:
                                    misses.append(cell + ("write-through",))
                                got = "refuse" if r["refused"] else "allow"
                                outcomes[cell] = got
                                if got != "refuse":
                                    misses.append(cell + (f"expect refuse got {got}",
                                                          r["detail"][:60]))
                                    continue
                                if kind != "materialised-copy":
                                    if "$INPUT" not in r["detail"]:
                                        misses.append(cell + ("no-root-named",))
                                    if bucket != "canonical" and key not in r["detail"]:
                                        misses.append(cell + ("no-key-named",))
                                elif "plaita#69" not in r["detail"]:
                                    misses.append(cell + ("no-named-refusal",))
        self.assertEqual([], misses[:5])
        # 名字不决定：同一 (值种类, 节点, 嵌套) 下三种键名判决一致
        for kind in CONTAINER_VALUE_KINDS:
            for node_kind in ("inline", "reference"):
                for nesting in CONTAINER_NESTINGS:
                    verdicts = {outcomes[(kind, node_kind, b, nesting)]
                                for b in CONTAINER_KEY_NAMES}
                    self.assertEqual(1, len(verdicts),
                                     f"{kind}/{node_kind}/{nesting}: {verdicts}")
        self.assertEqual(3 * 2 * 3 * 3, len(outcomes))


class TestIssue69R4VerifierCounterexample(unittest.TestCase):
    """r4 验收：verifier 的**逐字**反例（DSL-only，父把活 ``$INPUT`` 挂在 ``$myroot`` 下）。"""

    @staticmethod
    def _build():
        from plaita.node import Assignment
        from plaita.node.child import InlineFlow
        child = Flow(flow_id="ch69", version="1", runtime="python",
                     input_type=Property(data_type=types.OBJECT),
                     output_type=Property(data_type=types.ANY),
                     nodes=[Start(id="s", next="a"),
                            Assignment(id="a", output="$INPUT.cmd",
                                       output_type=Property(data_type=types.ANY), next="e"),
                            End(id="e", resultType="success", output="$NODE.a")])
        parent = Flow(flow_id="p69", version="1", runtime="python",
                      input_type=Property(data_type=types.OBJECT),
                      output_type=Property(data_type=types.ANY),
                      nodes=[Start(id="start", next="c"),
                             InlineFlow(id="c", input={"$myroot": "$INPUT",
                                                       "cmd": "$F.concat(\"$F.set($myroot.cfg,'k','v')\")"},
                                        child_flow=child, next="end"),
                             End(id="end", resultType="success", output="$NODE.c")])
        return parent

    def _run(self):
        execution = FlowExecution()
        start = {"cfg": {"a": 1}}
        raised = None
        try:
            execution.run_compatible(self._build(), False, copy.deepcopy(start))
        except Exception as exc:               # noqa: BLE001
            raised = exc
        return (raised, execution.context.get("$INPUT"),
                execution.context.to_checkpoint_dict().get("$INPUT"), start)

    def test_counterexample_cannot_write_through(self):
        """出厂树：r6 下子执行对边界副本的就地改写被**具名拒绝** ⇒ 零写穿。

        （r4/r5 语义是"改在私有副本上、不报错"；副本在父求值里物化，子执行的求值里
        不是 fresh —— 与 C2 的形状一致，因此 default-deny 拒绝。零写穿不变。）
        """
        raised, live, checkpoint, start = self._run()
        self.assertEqual(start, live, "parent live $INPUT written through")
        self.assertEqual(start, checkpoint, "parent checkpoint $INPUT written through")
        # #268：嵌套求值通道关闭——`$INPUT.cmd` 是数据（原串），不求值、不拒绝、
        # 零写穿（比"具名拒绝"更强的隔离：字符串根本不会变成代码）。
        self.assertIsNone(raised)

    def test_half2_alone_still_returns_data_verbatim(self):
        """关掉半边 1：#268 后该形态走不到 mutator 门——数据原样、零写穿。
        （身份探测机制的独立判据由规范路径的对照测试与其余存活单元钉住。）"""
        with _Half1Disabled():
            raised, live, checkpoint, start = self._run()
        self.assertIsNone(raised)
        self.assertEqual(start, live)
        self.assertEqual(start, checkpoint)

    def test_r3_baseline_shape_writes_through(self):
        """两侧机制全关（= r3 树的形状）⇒ 写穿，证明上面的断言有区分力。

        r5：freshness 层也是独立的一半，必须一并关掉（否则 default-deny 单独就拒绝了）。
        """
        import plaita.core.expression_parser as parser_mod
        saved_probe = parser_mod._probe_container_alias
        saved_fast = parser_mod._canonical_container_fast_path
        saved_fresh = parser_mod._fresh_has
        parser_mod._probe_container_alias = lambda container: None
        parser_mod._canonical_container_fast_path = lambda *a, **k: False
        parser_mod._fresh_has = lambda obj: True
        try:
            with _Half1Disabled():
                _raised, live, checkpoint, start = self._run()
        finally:
            parser_mod._probe_container_alias = saved_probe
            parser_mod._canonical_container_fast_path = saved_fast
            parser_mod._fresh_has = saved_fresh
        # #268：嵌套通道已关，机器全关也无法"写穿"——字符串不会变成代码。
        # 本负控的原始判据（r3 形态写穿）随通道关闭失去对象，改为钉零写穿。
        self.assertEqual(start, live, "nested channel closed: nothing can write through")
        self.assertEqual(start, checkpoint)


# ---------------------------------------------------------------------------
# r5：default-deny on provenance —— 反例逐字复现 + 多位置探针
# ---------------------------------------------------------------------------
# 判据：就地 mutate 的目标必须是**引擎本次求值新建**的对象（freshness registry）。
# 活子对象无论以什么身份出现（容器作用域 / 容器里的值 / 集合初值 / item / 平行分支
# 输入 / 非持久化键 / 任意键名的嵌套），都因为"来源不可证 fresh"被具名拒绝；
# 或（在显式物化边界上）落在私有副本上 —— 两种结果都**零写穿**。

def _r5_live_payload() -> dict:
    """活子对象 + 一段以它为容器的嵌套改写串（X1 形状的数据）。"""
    deep = {"a": 1}
    return {"deep": deep, "cmd": "$F.set($myroot,'k','v')", "$myroot": deep}


def _r5_x1_flow() -> Flow:
    """verifier X1 **逐字**：嵌套表达式串的作用域是状态的活子对象 ``$INPUT.cfg``。"""
    from plaita.node import Assignment
    return Flow(flow_id="x1-r5", version="1", runtime="python",
                input_type=Property(data_type=types.OBJECT),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="s", next="a"),
                       Assignment(id="a", output="$INPUT.cfg.cmd",
                                  output_type=Property(data_type=types.ANY), next="e"),
                       End(id="e", resultType="success", output="$NODE.a")])


def _r5_item_child_flow(input_expr: str) -> Flow:
    from plaita.node import Assignment
    return Flow(flow_id="r5-item-child", version="1", runtime="python",
                input_type=Property(data_type=types.OBJECT, children={
                    "item": Property(data_type=types.ANY),
                    "index": Property(data_type=types.INTEGER)}),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="s", next="a"),
                       Assignment(id="a", output=input_expr,
                                  output_type=Property(data_type=types.ANY), next="e"),
                       End(id="e", resultType="success", output="$NODE.a")])


def _r5_reduce_child_flow() -> Flow:
    from plaita.node import Assignment
    return Flow(flow_id="x2-child-r5", version="1", runtime="python",
                input_type=Property(data_type=types.ARRAY),
                output_type=Property(data_type=types.ANY),
                nodes=[Start(id="s", next="a"),
                       Assignment(id="a", output="$INPUT[0].cmd",
                                  output_type=Property(data_type=types.ANY), next="e"),
                       End(id="e", resultType="success", output="$NODE.a")])


#: `$NODE` of the most recent `_r5_run_flow` execution.  Parallel branch errors are
#: recorded there (`__parallel_error__`) instead of being re-raised by the executor.
_R5_LAST_NODE: dict = {}


def _r5_run_flow(flow: Flow, start: dict):
    """Run *flow*; return (raised, live_input, checkpoint_input, pristine_start)."""
    execution = FlowExecution()
    raised = None
    try:
        execution.run_compatible(flow, False, copy.deepcopy(start))
    except Exception as exc:               # noqa: BLE001 - 判决就是探针的对象
        raised = exc
    live = execution.context.get("$INPUT")
    checkpoint = execution.context.to_checkpoint_dict().get("$INPUT")
    global _R5_LAST_NODE
    _R5_LAST_NODE = execution.context.get("$NODE") or {}
    return raised, live, checkpoint, start


def _r5_container_probe_cases(live_sub) -> list:
    """活子对象作为**容器作用域里可达的值**的几种深度/位置。"""
    return [
        ("container-scope", {"$myroot": live_sub, "cmd": "text"},
         "$F.set($myroot,'k','v')"),
        ("container-value-scope", {"$myroot": {"wrap": live_sub}, "cmd": "text"},
         "$F.set($myroot.wrap,'k','v')"),
        ("container-dict-nested", {"$myroot": {"a": {"b": live_sub}}},
         "$F.set($myroot.a.b,'k','v')"),
        ("container-list-nested", {"$myroot": [[live_sub]]},
         "$F.set($myroot[0][0],'k','v')"),
        ("container-tuple-nested", {"$myroot": (live_sub,)},
         "$F.set($myroot[0],'k','v')"),
    ]


def build_r5_probe_rows() -> list:
    """多位置探针表（docs §X 的数据源）：(position, refused, live_changed, detail)。"""
    rows = []
    # --- 容器作用域里的活子对象（零登记：不做任何前置求值） -------------------
    ctx = _context()
    live_sub = ctx.get_state("$INPUT")["cfg"]      # 从未被求值器当路径终点交出过
    for label, container, expr in _r5_container_probe_cases(live_sub):
        before = copy.deepcopy(live_sub)
        try:
            PARSER.evaluate(expr, container)
            refused, detail = False, ""
        except ValueError as exc:
            refused, detail = True, str(exc)
        rows.append({"position": label, "refused": refused,
                     "live_changed": before != live_sub, "detail": detail})
    # --- 非持久化键（T2-1：活根子对象；求值器从未把它当过路径终点） -----------
    ctx2 = _context()
    live_child2 = ctx2.get_state("$INPUT")["cfg"]
    ctx2.set_state("$V", live_child2)
    before2 = copy.deepcopy(ctx2.get_state("$INPUT"))
    outcome, detail2, changed2 = run_cell("$F.set($V,'k','v')", ctx2)
    rows.append({"position": "non-persisted-key (T2-1 sub-object)",
                 "refused": outcome == "refuse", "live_changed": changed2,
                 "detail": detail2})
    # --- 非持久化键：引擎重建的外壳里挂着活值（壳 fresh，值不 fresh） ---------
    ctx3 = _context()
    shell = ctx3.evaluate({"w": "$INPUT.cfg"})     # 外壳是引擎新建的，值是活的
    ctx3.set_state("$V", shell)
    before3 = copy.deepcopy(ctx3.get_state("$INPUT"))
    outcome3, detail3, changed3 = run_cell("$F.set($V.w,'k','v')", ctx3)
    rows.append({"position": "non-persisted-key (fresh shell, live value)",
                 "refused": outcome3 == "refuse", "live_changed": changed3,
                 "detail": detail3})
    # --- 宿主直接绑定的非持久化键（owner 决策边界） --------------------------
    ctx4 = _context()
    ctx4.set_state("$V", {"a": 1})                 # 宿主对象，来源不可证
    outcome4, detail4, changed4 = run_cell("$F.set($V,'k','v')", ctx4)
    rows.append({"position": "host-bound non-persisted key (boundary)",
                 "refused": outcome4 == "refuse", "live_changed": changed4,
                 "detail": detail4})
    # --- 引擎绑定的非持久化键（构造证明 ⇒ allow） ----------------------------
    # r6：物化与就地改写必须属于**同一次**逻辑求值（这正是真实用法：引擎在一次
    # 节点字段求值里造出容器、随即就地改写它）。跨求值的引擎对象不再是 fresh。
    ctx5 = _context()
    with freshness_scope():
        ctx5.set_state("$V", ctx5.evaluate({"a": 1}))
        outcome5, detail5, changed5 = run_cell("$F.set($V,'k','v')", ctx5)
    rows.append({"position": "engine-bound non-persisted key (fresh by construction)",
                 "refused": outcome5 == "refuse", "live_changed": changed5,
                 "detail": detail5})
    # --- X1 逐字（flow 级：活子对象是嵌套串的作用域） ------------------------
    payload = _r5_live_payload()
    start = {"cfg": copy.deepcopy(payload)}
    raised, live, checkpoint, pristine = _r5_run_flow(_r5_x1_flow(), start)
    rows.append({"position": "X1 verbatim (nested scope = live sub-object)",
                 "refused": raised is not None and "plaita#69" in str(raised),
                 "live_changed": live != pristine or checkpoint != pristine,
                 "detail": str(raised)[:120] if raised else ""})
    # --- X2：Reduce.initial 活对象（物化边界 ⇒ 隔离，零写穿） ----------------
    payload2 = _r5_live_payload()
    start2 = {"items": [1], "cfg": copy.deepcopy(payload2)}
    reduce_flow = Flow(
        flow_id="x2-r5", version="1", runtime="python",
        input_type=Property(data_type=types.OBJECT),
        output_type=Property(data_type=types.ANY),
        nodes=[Start(id="s", next="r"),
               Reduce(id="r", collection="$INPUT.items", initial="$INPUT.cfg",
                      child_flow=_r5_reduce_child_flow(), next="e"),
               End(id="e", resultType="success", output="$NODE.r")])
    raised2, live2, checkpoint2, pristine2 = _r5_run_flow(reduce_flow, start2)
    rows.append({"position": "Reduce.initial (live sub-object)",
                 "refused": raised2 is not None and "plaita#69" in str(raised2),
                 "live_changed": live2 != pristine2 or checkpoint2 != pristine2,
                 "detail": str(raised2)[:120] if raised2 else ""})
    # --- 循环 item / 映射 item（集合边界物化 ⇒ 隔离，零写穿） ----------------
    for node_label, node in (("loop-item", Loop(id="n", collection="$INPUT.items",
                                                child_flow=_r5_item_child_flow("$INPUT.item.cmd"),
                                                next="e")),
                             ("map-item", Map(id="n", collection="$INPUT.items",
                                              child_flow=_r5_item_child_flow("$INPUT.item.cmd"),
                                              next="e"))):
        payload3 = _r5_live_payload()
        start3 = {"items": [copy.deepcopy(payload3)]}
        flow3 = Flow(flow_id="r5-item", version="1", runtime="python",
                     input_type=Property(data_type=types.OBJECT),
                     output_type=Property(data_type=types.ANY),
                     nodes=[Start(id="s", next="n"), node,
                            End(id="e", resultType="success", output="$NODE.n")])
        raised3, live3, checkpoint3, pristine3 = _r5_run_flow(flow3, start3)
        rows.append({"position": node_label,
                     "refused": raised3 is not None and "plaita#69" in str(raised3),
                     "live_changed": live3 != pristine3 or checkpoint3 != pristine3,
                     "detail": str(raised3)[:120] if raised3 else ""})
    # --- 平行分支输入（r4 边界物化 + r5 登记 ⇒ 私有副本，零写穿） ------------
    from plaita.node.concurrent import Parallel
    payload4 = _r5_live_payload()
    start4 = {"cfg": copy.deepcopy(payload4)}
    branch_input = {"$myroot": "$INPUT.cfg",
                    "cmd": "$F.concat(\"$F.set($myroot,'k','v')\")"}
    parallel_flow = Flow(
        flow_id="r5-parallel", version="1", runtime="python",
        input_type=Property(data_type=types.OBJECT),
        output_type=Property(data_type=types.ANY),
        nodes=[Start(id="s", next="p"),
               Parallel(id="p", name="p", mode="thread",
                        branches=[{"name": "b", "flow": _r4_child_flow(),
                                   "input": branch_input}],
                        join_branches=["b"], next="e"),
               End(id="e", resultType="success", output="$NODE.p")])
    raised4, live4, checkpoint4, pristine4 = _r5_run_flow(parallel_flow, start4)
    # r6：thread 分支跑在 worker 线程里，`ContextVar` 不跨线程继承 ⇒ 父求值里物化的
    # 分支输入在分支内被具名拒绝；并行执行器把它记进 `$NODE.p.<branch>.__parallel_error__`
    # **而不是上抛**（flow 级 `raised4` 可能是 None）。判决必须看分支结果，否则会把
    # "分支内具名拒绝 + 零写穿"误报成 allow/isolated。
    branch_result = (_R5_LAST_NODE.get("p") or {}) if isinstance(_R5_LAST_NODE, dict) else {}
    branch_detail = ""
    if isinstance(branch_result, dict):
        for value in branch_result.values():
            if isinstance(value, dict) and "__parallel_error__" in value:
                branch_detail = str(value["__parallel_error__"])
                break
    detail4 = branch_detail or (str(raised4) if raised4 is not None else "")
    rows.append({"position": "parallel-branch-input (live sub-object)",
                 "refused": "plaita#69" in detail4,
                 "live_changed": live4 != pristine4 or checkpoint4 != pristine4,
                 "detail": detail4[:120]})
    return rows


class TestIssue69R5Provenance(unittest.TestCase):
    """r5：default-deny on provenance —— X1/X2 与多位置活子对象探针全部零写穿。"""

    def test_x1_nested_string_is_data_and_writes_nothing(self):
        """#268 终裁：X1 通道（嵌套表达式串作用域 = 活子对象）已关闭——assignment
        读到的是原串（数据），不求值、不拒绝、零写穿。活子对象零变化；
        规范改写路径 ``$F.set($INPUT.cfg.deep,…)`` 的拒绝由对照测试钉住。"""
        payload = _r5_live_payload()
        start = {"cfg": copy.deepcopy(payload)}
        raised, live, checkpoint, pristine = _r5_run_flow(_r5_x1_flow(), start)
        self.assertIsNone(raised, "X1 字符串现在是数据：不求值也不拒绝")
        self.assertEqual(pristine, live)
        self.assertEqual(pristine, checkpoint)
        self.assertEqual({"a": 1}, live["cfg"]["deep"])       # 活子对象零变化

    def test_x1_control_canonical_path_is_still_refused(self):
        """对照：规范形式 ``$F.set($INPUT.cfg.deep,…)`` 一直被拒 ⇒ 修的是身份判据。"""
        ctx = _context()
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($INPUT.cfg.deep,'k','v')", ctx.context)
        self.assertIn("$INPUT.cfg.deep", str(cm.exception))

    def test_x2_reduce_initial_isolated_and_writes_nothing(self):
        payload = _r5_live_payload()
        start = {"items": [1], "cfg": copy.deepcopy(payload)}
        flow = Flow(
            flow_id="x2-r5", version="1", runtime="python",
            input_type=Property(data_type=types.OBJECT),
            output_type=Property(data_type=types.ANY),
            nodes=[Start(id="s", next="r"),
                   Reduce(id="r", collection="$INPUT.items", initial="$INPUT.cfg",
                          child_flow=_r5_reduce_child_flow(), next="e"),
                   End(id="e", resultType="success", output="$NODE.r")])
        raised, live, checkpoint, pristine = _r5_run_flow(flow, start)
        # #268 后：initial 物化边界依然隔离副本（零写穿不变）；子执行里不再有
        # 嵌套求值触发的 mutator 门——字符串是数据，原样流动。
        self.assertIsNone(raised)
        self.assertEqual(pristine, live, "Reduce.initial write-through to live $INPUT")
        self.assertEqual(pristine, checkpoint, "Reduce.initial write-through to checkpoint")
        self.assertEqual({"a": 1}, live["cfg"]["deep"])

    def test_every_container_position_refuses(self):
        """活子对象作为容器作用域里可达的值：逐位置具名拒绝 + 零写入。"""
        ctx = _context()
        live_sub = ctx.get_state("$INPUT")["cfg"]
        for label, container, expr in _r5_container_probe_cases(live_sub):
            with self.subTest(position=label):
                before = copy.deepcopy(live_sub)
                with self.assertRaises(ValueError) as cm:
                    PARSER.evaluate(expr, container)
                self.assertIn("plaita#69", str(cm.exception))
                self.assertEqual(before, live_sub)

    def test_set_element_position_is_not_expressible(self):
        """``set``/``frozenset`` 元素必须可哈希；活持久化容器是 dict/list（不可哈希），
        因此"活子对象作为 set 元素"在数据模型里**无法表达**（类型原生错误，不是门）。"""
        live_sub = {"a": 1}
        with self.assertRaises(TypeError):
            {live_sub}                                  # unhashable
        frozenset_wrap = frozenset([("k", "v")])
        self.assertEqual(1, len(frozenset_wrap))

    def test_non_persisted_key_uses_the_same_predicate(self):
        """verifier #3：非持久化键也必须走同一套判定，不能只查索引/键名。"""
        # T2-1：活根子对象（求值器从未把它当路径终点交出过）
        ctx = _context()
        live_child = ctx.get_state("$INPUT")["cfg"]
        ctx.set_state("$V", live_child)
        before = copy.deepcopy(ctx.get_state("$INPUT"))
        outcome, detail, changed = run_cell("$F.set($V,'k','v')", ctx)
        self.assertEqual("refuse", outcome)
        self.assertIn("plaita#69", detail)
        self.assertFalse(changed)
        self.assertEqual(before, ctx.get_state("$INPUT"))
        self.assertEqual(before, ctx.to_dict()["$INPUT"])
        # 活根本体（r4 已有）：同样拒
        ctx2 = _context()
        ctx2.set_state("$V2", ctx2.get_state("$INPUT"))
        self.assertEqual("refuse", run_cell("$F.set($V2.cfg,'k','v')", ctx2)[0])

    def test_host_bound_non_root_key_is_refused_by_provenance(self):
        """owner 决策边界：``set_state("$V", {...})`` 的宿主对象来源不可证 ⇒ 具名拒绝。

        构造证明的合法用法（``ctx.evaluate`` / producer 结果）保持允许，见下一个用例。
        """
        ctx = _context()
        ctx.set_state("$V", {"a": 1})
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
        self.assertIn("provenance could not be proven fresh", str(cm.exception))
        self.assertEqual({"a": 1}, ctx.get_state("$V"))       # 零写入

    @_one_evaluation
    def test_engine_bound_values_are_allowed_by_construction(self):
        """每条 allow 都由构造证明 fresh：引擎重建的容器 / producer / detach 副本。

        r6：`fresh` = **本次逻辑求值**由引擎新建，所以物化与就地改写同属一次求值
        （装饰器提供作用域）；跨求值的对照见 ``TestIssue69R6PerEvaluationScope``。
        """
        ctx = _context()
        ctx.set_state("$V", ctx.evaluate({"a": {"b": 1}}))
        self.assertIsNone(PARSER.evaluate("$F.set($V,'b',2)", ctx.context))
        self.assertEqual({"a": {"b": 1}, "b": 2}, ctx.get_state("$V"))
        self.assertIsNone(PARSER.evaluate("$F.set($V.a,'x',1)", ctx.context))  # 嵌套壳
        self.assertEqual({"b": 1, "x": 1}, ctx.get_state("$V")["a"])
        # producer：json_loads（DEEP 闭包）
        self.assertIsNone(PARSER.evaluate("$F.set($F.json_loads('{\"q\":0}'),'q',1)",
                                          ctx.context))
        # producer：slice（SHALLOW 新容器）—— 目标是新容器，就地 pop 只改它自己
        before_input = copy.deepcopy(ctx.get_state("$INPUT"))
        self.assertEqual(1, PARSER.evaluate("$F.pop($F.slice($INPUT.items,0,2),0)",
                                            ctx.context))
        self.assertEqual(before_input, ctx.get_state("$INPUT"))
        # detach 副本：浅 producer 值参经**引擎自有结构拷贝**去别名后是 fresh
        ctx2 = _alias_context()
        ctx2.set_state("$VAR", ctx2.evaluate({}))
        self.assertIsNone(PARSER.evaluate(
            "$F.set($VAR,'k',$F.slice($INPUT.items,0,1))", ctx2.context))
        self.assertIsNone(PARSER.evaluate("$F.set($VAR.k[0],'x',9)", ctx2.context))
        self.assertEqual(9, ctx2.get_state("$VAR")["k"][0]["x"])

    @_one_evaluation
    def test_live_reference_over_engine_materialised_object_is_allowed(self):
        """``$F.get($V,…)`` 的目标位：其结果是 fresh 容器里的成员 ⇒ 构造证明成立。"""
        ctx = _context()
        ctx.set_state("$V", ctx.evaluate({"cfg": {"a": 1}}))
        self.assertIsNone(PARSER.evaluate("$F.set($F.get($V,'cfg'),'k','v')", ctx.context))
        self.assertEqual({"a": 1, "k": "v"}, ctx.get_state("$V")["cfg"])

    def test_probe_table_refuses_or_isolates(self):
        rows = build_r5_probe_rows()
        for row in rows:
            with self.subTest(position=row["position"]):
                self.assertFalse(row["live_changed"],
                                 f"{row['position']} wrote through: {row['detail']}")
        by_position = {row["position"]: row for row in rows}
        # X1（嵌套串作用域 = 活子对象）：#268 后通道关闭——不求值、不拒绝、零写穿。
        self.assertFalse(by_position["X1 verbatim (nested scope = live sub-object)"]
                         ["refused"])
        self.assertFalse(by_position["X1 verbatim (nested scope = live sub-object)"]
                         ["live_changed"])
        self.assertTrue(by_position["Reduce.initial (live sub-object)"]["refused"] is False
                        or by_position["Reduce.initial (live sub-object)"]["refused"])
        self.assertFalse(by_position["engine-bound non-persisted key (fresh by construction)"]
                         ["refused"])
        self.assertTrue(by_position["host-bound non-persisted key (boundary)"]["refused"])
        self.assertFalse(by_position["loop-item"]["live_changed"])
        self.assertFalse(by_position["map-item"]["live_changed"])
        self.assertFalse(by_position["parallel-branch-input (live sub-object)"]
                         ["live_changed"])


class TestIssue69R6PerEvaluationScope(unittest.TestCase):
    """r6：``fresh`` 严格等于"**本次逻辑求值**由引擎新建"。

    r5 的 freshness 是模块级进程内 LRU，于是"引擎在这进程里造过它"就等于 fresh ——
    verifier 的 C2 反例（引擎物化的对象被写进 ``$NODE``/checkpoint 后，**另一次**求值
    仍可原地改写它）由此成立。r6 把登记面收进 ContextVar 作用域，随求值返回而丢弃。
    """

    def test_object_created_in_one_evaluation_is_refused_in_the_next(self):
        """A→B：A 里 ``ctx.evaluate`` 造的对象，在 B 的裸 ``parser.evaluate`` 里必拒。"""
        ctx = _context()
        created = ctx.evaluate({"a": 1})            # 求值 A：引擎新建
        self.assertEqual({"a": 1}, created)
        with self.assertRaises(ValueError) as cm:   # 求值 B（无外层作用域）
            PARSER.evaluate("$F.set($X,'k','v')", {"$X": created})
        self.assertIn("provenance could not be proven fresh", str(cm.exception))
        self.assertEqual({"a": 1}, created, "cross-evaluation write-through")

    def test_engine_object_persisted_as_node_result_cannot_be_mutated_later(self):
        """C2 逐字：``o = ctx.evaluate(...)`` → ``set_state("$NODE", {"n1": o})`` → 另一次求值写它。

        r5 实测 ``fresh=True`` 且 checkpoint 被改；r6 必须拒绝且 checkpoint 零变化。
        """
        ctx = _context()
        o = ctx.evaluate({"a": 1})
        ctx.set_state("$NODE", {"n1": o})
        before_checkpoint = copy.deepcopy(ctx.to_dict()["$NODE"])
        with self.assertRaises(ValueError) as cm:
            PARSER.evaluate("$F.set($X,'k','v')", {"$X": o})
        self.assertIn("provenance could not be proven fresh", str(cm.exception))
        self.assertEqual({"a": 1}, o)
        self.assertEqual(before_checkpoint, ctx.to_dict()["$NODE"])

    def test_nested_evaluation_inside_one_scope_shares_it(self):
        """同一次求值里的嵌套共享作用域 ⇒ 引擎物化的嵌套外壳仍可就地改写。"""
        ctx = _context()
        with freshness_scope():
            holder = ctx.evaluate({"v": {"a": 1}})          # 外壳与内层都在本作用域内
            self.assertIsNone(holder["v"].get("x"))
            # 同一次求值：目标位在**本作用域**物化的嵌套外壳 ⇒ 放行
            nested = PARSER.evaluate("$F.set($holder.v,'x',1)",
                                     {"$holder": holder, "$INPUT": {"a": 1}})
            self.assertIsNone(nested)
            self.assertEqual({"a": 1, "x": 1}, holder["v"])

    def test_scope_is_dropped_on_exception(self):
        """异常路径也丢弃作用域：抛出的求值不得把登记泄漏给下一次求值。"""
        import plaita.core.expression_parser as pm
        ctx = _context()
        with self.assertRaises(ZeroDivisionError):
            with pm.freshness_scope():
                leak = {"a": 1}
                pm.note_fresh_object(leak)
                self.assertTrue(pm._fresh_has(leak))
                raise ZeroDivisionError("boom")
        self.assertIsNone(pm._FRESH_SCOPE.get())
        self.assertFalse(pm._fresh_has(leak))
        self.assertEqual(0, pm.freshness_registry_size())

    def test_registration_outside_a_scope_is_a_noop(self):
        """跨求值之间调用 ``note_fresh_*`` 不登记（失败方向选安全侧）。"""
        import plaita.core.expression_parser as pm
        obj = {"a": 1}
        pm.note_fresh_object(obj)
        pm.note_fresh_closure(obj)
        self.assertFalse(pm._fresh_has(obj))
        self.assertEqual(0, pm.freshness_registry_size())


class TestIssue69R6FreshnessBudget(unittest.TestCase):
    """r6：**作用域内**的容量与遍历预算，两处耗尽都 fail-closed。"""

    def test_scope_capacity_eviction_is_fail_closed(self):
        import plaita.core.expression_parser as pm
        saved = pm._FRESH_BUDGET
        pm._FRESH_BUDGET = 2
        try:
            with pm.freshness_scope():
                pm.reset_freshness_registry()
                objs = [{"i": i} for i in range(4)]
                for obj in objs:
                    pm.note_fresh_object(obj)
                self.assertLessEqual(pm.freshness_registry_size(), 2)
                self.assertFalse(pm._fresh_has(objs[0]))       # 被淘汰 ⇒ 不 fresh
                self.assertTrue(pm._fresh_has(objs[-1]))
                # 门因此 fail-closed（拒绝），且不写穿
                ctx = _context()
                ctx.set_state("$V", {"a": 1})
                with self.assertRaises(ValueError) as cm:
                    PARSER.evaluate("$F.set($V,'k','v')", ctx.context)
                self.assertIn("provenance could not be proven fresh", str(cm.exception))
                self.assertEqual({"a": 1}, ctx.get_state("$V"))
        finally:
            pm._FRESH_BUDGET = saved

    def test_closure_walk_budget_stops_and_fails_closed(self):
        import plaita.core.expression_parser as pm
        saved = pm._FRESH_WALK_BUDGET
        try:
            deep = {"a": {"b": {"c": {"d": 1}}}}
            with pm.freshness_scope():
                pm.reset_freshness_registry()
                pm.note_fresh_closure(deep, budget=1)
                self.assertTrue(pm._fresh_has(deep))
                self.assertFalse(pm._fresh_has(deep["a"]["b"]["c"]))
                # 未走到的子对象不是 fresh ⇒ 作为目标被拒（fail-closed）
                with self.assertRaises(ValueError):
                    PARSER.evaluate("$F.set($myroot.a.b.c,'k','v')", {"$myroot": deep})
        finally:
            pm._FRESH_WALK_BUDGET = saved


if __name__ == "__main__":
    unittest.main()
