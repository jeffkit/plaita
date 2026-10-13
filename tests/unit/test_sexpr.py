"""Tests for plaita.dsl.sexpr — S-表达式前端。"""

import ast
import re
import unittest
from pathlib import Path

import plaita.dsl.sexpr as sexpr_mod
from plaita.dsl.sexpr import parse_sexpr, compile_sexpr, flow_to_sexpr


class TestSexprBasic(unittest.TestCase):
    def test_if_branch(self):
        src = """
        (flow adult_check
          :input-type object
          :desc "判断成年"
          (start -> check_age)
          (if :id check_age (cond "$INPUT.age" >= 18) -> end_adult :else end_minor)
          (end end_adult :output "成年")
          (end end_minor :output "未成年"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(age=20), "成年")
        self.assertEqual(f.run(age=15), "未成年")

    def test_compile_to_dict_shape(self):
        src = """
        (flow x :input-type object
          (start -> e)
          (end e :output "$INPUT.x"))
        """
        d = compile_sexpr(src)
        self.assertEqual(d["flow_id"], "x")
        self.assertEqual(d["inputType"], {"dataType": "object"})
        self.assertEqual(d["nodes"][0]["type"], "start")
        self.assertEqual(d["nodes"][1]["resultType"], "success")

    def test_roundtrip_through_flow_to_sexpr(self):
        src = """
        (flow adult_check :input-type object
          (start -> c)
          (if :id c (cond "$INPUT.age" >= 18) -> a :else b)
          (end a :output "成年")
          (end b :output "未成年"))
        """
        d = compile_sexpr(src)
        back = flow_to_sexpr(d)
        f = parse_sexpr(back)
        self.assertEqual(f.run(age=20), "成年")


class TestSexprConditions(unittest.TestCase):
    def test_and_group(self):
        src = """
        (flow g :input-type object
          (start -> c)
          (if :id c (and (cond "$INPUT.age" >= 18) (cond "$INPUT.vip" == true))
            -> ok :else no)
          (end ok :output "通过")
          (end no :output "拒绝"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(age=20, vip=True), "通过")
        self.assertEqual(f.run(age=20, vip=False), "拒绝")
        self.assertEqual(f.run(age=15, vip=True), "拒绝")

    def test_not_negates_compare(self):
        src = """
        (flow g :input-type object
          (start -> c)
          (if :id c (not (cond "$INPUT.role" == "blocked")) -> ok :else no)
          (end ok :output "通过")
          (end no :output "拒绝"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(role="normal"), "通过")
        self.assertEqual(f.run(role="blocked"), "拒绝")

    def test_operator_alias_normalized(self):
        src = """
        (flow g :input-type object
          (start -> c)
          (if :id c (cond "$INPUT.n" != 0) -> ok :else no)
          (end ok :output "非零")
          (end no :output "零"))
        """
        d = compile_sexpr(src)
        cond = d["nodes"][1]["condition"]
        self.assertEqual(cond["operator"], "ne")
        self.assertEqual(parse_sexpr(src).run(n=5), "非零")


class TestSexprCollections(unittest.TestCase):
    def test_map(self):
        src = """
        (flow double :input-type object
          (start -> m)
          (map :id m :collection "$INPUT.numbers"
            :child (childflow :input-type object
              (start -> e)
              (end e :output "$F.mul($INPUT.item, 2)"))
            -> end)
          (end end :output "$NODE.m"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(numbers=[1, 2, 3, 4]), [2, 4, 6, 8])

    def test_filter_with_if_returning_bool(self):
        src = """
        (flow evens :input-type object
          (start -> f)
          (filter :id f :collection "$INPUT.nums"
            :child (childflow :input-type object
              (start -> c)
              (if :id c (cond "$F.mod($INPUT.item, 2)" == 0) -> y :else n)
              (end y :output true)
              (end n :output false))
            -> end)
          (end end :output "$NODE.f"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(nums=[1, 2, 3, 4, 6]), [2, 4, 6])


class TestSexprSwitchCase(unittest.TestCase):
    def test_switch_with_default(self):
        src = """
        (flow r :input-type object
          (start -> s)
          (switch :id s
            (branch a -> ea :when (cond "$INPUT.type" == "A"))
            (branch b -> eb :when (cond "$INPUT.type" == "B"))
            (branch dft -> ed :default true))
          (end ea :output "A")
          (end eb :output "B")
          (end ed :output "other"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(type="A"), "A")
        self.assertEqual(f.run(type="B"), "B")
        self.assertEqual(f.run(type="Z"), "other")

    def test_case_equal_match(self):
        src = """
        (flow r :input-type object
          (start -> s)
          (case :id s :target "$INPUT.n"
            (match 1 e1) (match 2 e2) :default ed)
          (end e1 :output "一")
          (end e2 :output "二")
          (end ed :output "其它"))
        """
        f = parse_sexpr(src)
        self.assertEqual(f.run(n=1), "一")
        self.assertEqual(f.run(n=2), "二")
        self.assertEqual(f.run(n=99), "其它")


class TestSexprHttpCompile(unittest.TestCase):
    def test_http_compiles_to_ir(self):
        # 不执行（需要 http extra），只验证编译到 IR
        src = """
        (flow call :input-type object
          (start -> h)
          (http :id h :method POST :url "https://api.example.com/users"
            :headers (dict :Content-Type "application/json")
            :body (dict :name "$INPUT.name")
            :timeout "PT5S"
            :on-error (on-error continue_with :default (dict :data nil))
            -> end)
          (end end :output "$NODE.h.data"))
        """
        d = compile_sexpr(src)
        h = d["nodes"][1]
        self.assertEqual(h["type"], "http")
        self.assertEqual(h["method"], "POST")
        self.assertEqual(h["url"], "https://api.example.com/users")
        self.assertEqual(h["timeout"], "PT5S")
        self.assertEqual(h["errorHandler"]["strategy"], "continue_with")


class TestSexprValidation(unittest.TestCase):
    def test_dangling_next_raises(self):
        src = """
        (flow bad :input-type object
          (start -> nope)
          (end end :output "x"))
        """
        with self.assertRaises(Exception):
            parse_sexpr(src)

    def test_unknown_node_type_raises(self):
        src = """
        (flow bad :input-type object
          (start -> e)
          (bogus :id e :output "x")
          (end e2 :output "y"))
        """
        with self.assertRaises(SyntaxError):
            compile_sexpr(src)


class TestSexprChildFlowId(unittest.TestCase):
    """#149：内联子流程自身 id 的 sexpr 读写（拼写 ``:flow-id``，维护者录定）。

    ``:id`` 已被节点自身占用（``(child :id c …)`` 里的 ``c``），故不采纳；
    读侧此前对 ``:flow-id``/``:flow_id``/``:id`` 三种写法**全部静默无效**，
    即 sexpr 路径没有表达内联子流程 id 的手段，而不是"发射侧漏写"。
    这里把选定拼写**双向**钉死，并钉住"不给 id 就保持缺失（不凭空造名）"。
    """

    #: 子流程带 id（读/写/往返三条主用例共用）
    _WITH_ID = """
    (flow parent :input-type object
      (start -> c)
      (child :id c :input "$INPUT.payload"
        :child (childflow :flow-id "cf" :input-type object
          (start -> ce)
          (end ce :output "$INPUT.item"))
        -> end)
      (end end :output "$NODE.c"))
    """

    #: 同一形状但不给 id（负向：必须保持缺失）
    _NO_ID = _WITH_ID.replace(' :flow-id "cf"', "")

    @staticmethod
    def _child(ir):
        for n in ir["nodes"]:
            c = n.get("childFlow") or n.get("child_flow")
            if isinstance(c, dict):
                return c
        raise AssertionError("IR 里没有内联子流程")

    # --- 读：source -> IR ---------------------------------------------------
    def test_reader_populates_flow_id(self):
        self.assertEqual(self._child(compile_sexpr(self._WITH_ID))["flow_id"], "cf")

    def test_reader_accepts_bare_symbol_flow_id(self):
        """``:flow-id cf``（不带引号）与 ``:flow-id "cf"`` 等价。"""
        child = self._child(compile_sexpr(self._WITH_ID.replace(':flow-id "cf"', ":flow-id cf")))
        self.assertEqual(child["flow_id"], "cf")

    def test_reader_flow_id_coexists_with_desc_and_types(self):
        """id 与 ``desc``/``input-type``/``output-type`` 同处一形时 id 仍进 IR。"""
        src = self._WITH_ID.replace(
            ':flow-id "cf"', ':flow-id "cf" :desc "子流程" :output-type string'
        )
        child = self._child(compile_sexpr(src))
        self.assertEqual(child["flow_id"], "cf")
        self.assertEqual(child["desc"], "子流程")
        self.assertEqual(child["outputType"], {"dataType": "string"})

    def test_flow_object_layer_carries_flow_id(self):
        flow = parse_sexpr(self._WITH_ID)
        ids = [
            n.child_flow.flow_id
            for n in flow.nodes
            if getattr(n, "child_flow", None) is not None
        ]
        self.assertEqual(ids, ["cf"])

    # --- 写：IR -> source ---------------------------------------------------
    def test_writer_emits_flow_id(self):
        emitted = flow_to_sexpr(compile_sexpr(self._WITH_ID))
        self.assertIn(':flow-id "cf"', emitted)

    def test_writer_quotes_and_escapes_special_flow_id(self):
        """含引号/空格/unicode 的 id 走既有转义约定并往返。"""
        ir = compile_sexpr(self._WITH_ID)
        self._child(ir)["flow_id"] = 'cf" qé'
        emitted = flow_to_sexpr(ir)
        self.assertIn(':flow-id "cf\\" qé"', emitted)
        self.assertEqual(self._child(compile_sexpr(emitted))["flow_id"], 'cf" qé')

    def test_writer_quotes_atom_like_flow_ids(self):
        """``nil``/``true``/``:x`` 这类裸写会被读成语义不同的原子，必须带引号。

        实测裸写：``nil`` → ``None``（整键丢失）、``true`` → ``'True'``、
        ``:x`` → ``'x'``。id 是标识符而非表达式，故一律加引号。
        """
        for fid in ("nil", "true", "false", "None", ":x", "123", "-1.5", ""):
            ir = compile_sexpr(self._WITH_ID)
            self._child(ir)["flow_id"] = fid
            emitted = flow_to_sexpr(ir)
            self.assertIn(f':flow-id "{fid}"', emitted)
            self.assertEqual(self._child(compile_sexpr(emitted))["flow_id"], fid)

    # --- 往返 ---------------------------------------------------------------
    def test_roundtrip_preserves_flow_id(self):
        once = compile_sexpr(self._WITH_ID)
        twice = compile_sexpr(flow_to_sexpr(once))
        self.assertEqual(self._child(once)["flow_id"], "cf")
        self.assertEqual(self._child(twice)["flow_id"], "cf")

    # --- 负向：不给 id 就必须缺失 ------------------------------------------
    def test_no_id_is_absent_on_read(self):
        child = self._child(compile_sexpr(self._NO_ID))
        self.assertIsNone(child.get("flow_id"))
        self.assertNotIn("flow_id", child)

    def test_no_id_is_absent_on_write_and_roundtrip(self):
        for src in (self._NO_ID, self._WITH_ID.replace(' :flow-id "cf"', "")):
            emitted = flow_to_sexpr(compile_sexpr(src))
            self.assertNotIn(":flow-id", emitted)
            child = self._child(compile_sexpr(emitted))
            self.assertIsNone(child.get("flow_id"))
            self.assertNotIn("flow_id", child)

    def test_no_id_emission_is_byte_identical_to_shapeless_child(self):
        """不给 id 的子流程发射结果里，``childflow`` 形逐字等于改动前的形状。

        钉死"不写 ``:flow-id nil`` / 不凭空造名"：任何多写的键都会让这条断言失败。
        """
        emitted = flow_to_sexpr(compile_sexpr(self._NO_ID))
        self.assertIn(
            "(childflow :input-type object "
            "(start :id _n1 -> ce) (end :id ce :output $INPUT.item))",
            emitted,
        )

    # --- 拼写边界：选定的只有 :flow-id -------------------------------------
    def test_other_candidate_spellings_stay_ignored(self):
        """``:flow_id`` / ``:flowId`` / ``:id`` 不采纳，未知键仍静默忽略。

        维护者的决策面只有 ``:flow-id``；此用例钉住"不悄悄扩大决策面"。
        """
        for kw in (":flow_id", ":flowId", ":bogus-key"):
            src = self._WITH_ID.replace(":flow-id", kw, 1)
            child = self._child(compile_sexpr(src))
            self.assertIsNone(child.get("flow_id"), kw)
        # `:id` 写在 childflow 形内是未知键（节点自身的 :id 另有其位），
        # 既不得变成子流程 id，也不得被 childflow 吃成自己的 id 键
        node = next(
            n
            for n in compile_sexpr(self._WITH_ID.replace(':flow-id "cf"', ':id "cf"'))["nodes"]
            if n.get("childFlow")
        )
        self.assertIsNone(node["childFlow"].get("flow_id"))
        self.assertNotIn("id", node["childFlow"])
        self.assertEqual(node["id"], "c")


# ---------------------------------------------------------------------------
# #147: flow 级字段读得到 → 必须发得出（root 与内联子流程）
# ---------------------------------------------------------------------------

#: ``(flow …)`` 顶层接受、但 base 不发射的字段：
#: (字段名, 源片段, IR 键, 期望 IR 值)
_ROOT_CASES = (
    ("outputType", ":output-type string", "outputType", {"dataType": "string"}),
    ("version", ':version "1.2"', "version", "1.2"),
    ("author", ':author "me"', "author", "me"),
    ("timeout", ":timeout 30", "timeout", 30),
    ("globalContext", ':global-context (dict :key "val")', "globalContext",
     ["dict", "key", "val"]),
    ("metadata", ':metadata (dict :env "prod")', "metadata", ["dict", "env", "prod"]),
)

#: ``(childflow …)`` 接受、但 base 不发射的字段（``#149`` 已补 ``flow-id``）。
_CHILD_CASES = (
    ("outputType", ":output-type string", "outputType", {"dataType": "string"}),
    ("desc", ':desc "子流程"', "desc", "子流程"),
)

_ROOT_SHELL = "(flow g {frag} (start :id s -> e) (end :id e :output x))"
_CHILD_SHELL = (
    '(flow parent :input-type object (start -> c)'
    ' (child :id c :input "$INPUT.payload"'
    '  :child (childflow {frag} (start :id cs -> ce) (end :id ce :output "$INPUT.item"))'
    '  -> end)'
    ' (end :id end :output "$NODE.c"))'
)


def _norm_ir(x):
    """把 ``Symbol``/``Keyword``（str 子类）归一成 ``str``，便于整树比较。"""
    if isinstance(x, str):
        return str(x)
    if isinstance(x, list):
        return [_norm_ir(i) for i in x]
    if isinstance(x, dict):
        return {_norm_ir(k): _norm_ir(v) for k, v in x.items()}
    return x


def _child_ir(ir):
    for n in ir["nodes"]:
        c = n.get("childFlow") or n.get("child_flow")
        if isinstance(c, dict):
            return c
    raise AssertionError(f"IR 里没有内联子流程：{ir['nodes']}")


class TestFlowLevelFieldsRoundTrip(unittest.TestCase):
    """#147：``_compile_flow`` / ``_compile_childflow`` 读得到的 flow 级字段，
    ``flow_to_sexpr`` 必须发射，且 source→IR→sexpr→IR 整树相等。"""

    # --- 写：字段确实出现在发射结果里 ------------------------------------
    def test_writer_emits_every_root_flow_level_field(self):
        for label, frag, _ir_key, _expected in _ROOT_CASES:
            with self.subTest(field=label):
                emitted = flow_to_sexpr(compile_sexpr(_ROOT_SHELL.format(frag=frag)))
                self.assertIn(f":{_kebab(label)}", emitted, emitted)

    def test_writer_emits_every_child_flow_level_field(self):
        for label, frag, _ir_key, _expected in _CHILD_CASES:
            with self.subTest(field=label):
                emitted = flow_to_sexpr(compile_sexpr(_CHILD_SHELL.format(frag=frag)))
                self.assertIn(f":{_kebab(label)}", emitted, emitted)

    # --- 往返：单字段与整树 ------------------------------------------------
    def test_root_flow_level_fields_round_trip(self):
        for label, frag, ir_key, expected in _ROOT_CASES + (
            ("inputType", ":input-type object", "inputType", {"dataType": "object"}),
            ("desc", ':desc "hello"', "desc", "hello"),
        ):
            with self.subTest(field=label):
                first = compile_sexpr(_ROOT_SHELL.format(frag=frag))
                self.assertEqual(_norm_ir(first.get(ir_key)), _norm_ir(expected))
                second = compile_sexpr(flow_to_sexpr(first))
                self.assertEqual(_norm_ir(second.get(ir_key)), _norm_ir(expected))
                self.assertEqual(_norm_ir(first), _norm_ir(second),
                                 flow_to_sexpr(first))

    def test_child_flow_level_fields_round_trip(self):
        for label, frag, ir_key, expected in _CHILD_CASES + (
            ("flow_id", ':flow-id "cf"', "flow_id", "cf"),
            ("inputType", ":input-type object", "inputType", {"dataType": "object"}),
        ):
            with self.subTest(field=label):
                first = compile_sexpr(_CHILD_SHELL.format(frag=frag))
                self.assertEqual(_norm_ir(_child_ir(first).get(ir_key)), _norm_ir(expected))
                second = compile_sexpr(flow_to_sexpr(first))
                self.assertEqual(_norm_ir(_child_ir(second).get(ir_key)), _norm_ir(expected))
                self.assertEqual(_norm_ir(first), _norm_ir(second),
                                 flow_to_sexpr(first))

    # --- #147 报的两处实测缺口，逐一显式钉住 ------------------------------
    def test_nested_child_desc_and_output_type_survive(self):
        """#149 只补了 ``flow-id``；``:desc`` / ``:output-type`` 仍被丢弃。"""
        frag = ':flow-id "cf" :desc "子流程" :output-type string'
        first = compile_sexpr(_CHILD_SHELL.format(frag=frag))
        child = _child_ir(first)
        self.assertEqual(child["desc"], "子流程")
        self.assertEqual(child["outputType"], {"dataType": "string"})

        emitted = flow_to_sexpr(first)
        self.assertIn(':flow-id "cf"', emitted)
        self.assertIn(":desc", emitted)
        self.assertIn("子流程", emitted)
        self.assertIn(":output-type string", emitted)

        back = _child_ir(compile_sexpr(emitted))
        self.assertEqual(back["flow_id"], "cf")
        self.assertEqual(back["desc"], "子流程")
        self.assertEqual(back["outputType"], {"dataType": "string"})

    def test_root_output_type_survives(self):
        """root 的 ``:output-type`` 原本读得到、发不出。"""
        first = compile_sexpr(_ROOT_SHELL.format(frag=":output-type string"))
        self.assertEqual(first["outputType"], {"dataType": "string"})
        emitted = flow_to_sexpr(first)
        self.assertIn(":output-type string", emitted)
        self.assertEqual(compile_sexpr(emitted)["outputType"], {"dataType": "string"})

    def test_quoted_string_fields_do_not_collapse_to_other_atoms(self):
        """``version``/``author`` 若裸写会被重读成 float/bool/None。"""
        first = compile_sexpr(_ROOT_SHELL.format(frag=':version "1.2" :author "true"'))
        self.assertEqual(first["version"], "1.2")
        self.assertEqual(first["author"], "true")
        emitted = flow_to_sexpr(first)
        self.assertIn(':version "1.2"', emitted)
        self.assertIn(':author "true"', emitted)
        back = compile_sexpr(emitted)
        self.assertEqual(back["version"], "1.2")
        self.assertEqual(back["author"], "true")

    # --- 缺席行为：不发、不造值，且与 base 逐字节相同 ---------------------
    #: 下面几条钉住"缺席字段零新增字节"：这些整串取自 base 的实测发射结果。
    _ROOT_MINIMAL = "(flow g (start :id s -> e) (end :id e :output x))"
    _CHILD_MINIMAL = (
        "(flow parent :input-type object (start :id _n1 -> c)"
        " (child :id c :input $INPUT.payload"
        " :child (childflow (start :id cs -> ce) (end :id ce :output $INPUT.item))"
        " -> end) (end :id end :output $NODE.c))"
    )

    def test_absent_root_fields_emit_nothing_and_are_byte_identical_to_base(self):
        emitted = flow_to_sexpr(compile_sexpr(_ROOT_SHELL.format(frag="")))
        self.assertEqual(emitted, self._ROOT_MINIMAL)
        for key in ("output-type", "version", "author", "timeout",
                    "global-context", "metadata", "desc", "nil"):
            self.assertNotIn(f":{key}", emitted, emitted)

    def test_absent_child_fields_emit_nothing_and_are_byte_identical_to_base(self):
        """也覆盖 ``#149`` 的"无 id 子流程"形状：``:flow-id`` 不得凭空出现。"""
        emitted = flow_to_sexpr(compile_sexpr(_CHILD_SHELL.format(frag="")))
        self.assertEqual(emitted, self._CHILD_MINIMAL)
        self.assertIn("(childflow (start :id cs -> ce)", emitted)
        for key in ("flow-id", "output-type", "desc", "nil"):
            self.assertNotIn(f":{key}", emitted, emitted)

    def test_base_correct_emissions_stay_byte_identical(self):
        """base 已正确的字段（root desc/inputType、子流程 flow-id/inputType）不变。"""
        self.assertEqual(
            flow_to_sexpr(compile_sexpr(_ROOT_SHELL.format(frag=":input-type object"))),
            "(flow g :input-type object (start :id s -> e) (end :id e :output x))",
        )
        self.assertEqual(
            flow_to_sexpr(compile_sexpr(_ROOT_SHELL.format(
                frag=":input-type object :desc hello"))),
            "(flow g :input-type object :desc hello"
            " (start :id s -> e) (end :id e :output x))",
        )
        self.assertIn(
            '(childflow :flow-id "cf" :input-type object (start :id cs -> ce)',
            flow_to_sexpr(compile_sexpr(_CHILD_SHELL.format(
                frag=':flow-id "cf" :input-type object'))),
        )


def _sexpr_module_source() -> str:
    return Path(sexpr_mod.__file__).read_text(encoding="utf-8")


def _function_node(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"未找到函数 {name}")


def _kebab(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower()


def _reader_opt_spellings(func_name: str):
    """AST 收集某读侧函数内 ``_opt(kw, …)`` 接受的拼写名。

    ``_compile_flow`` 用 ``for fld, key in ((...), ...)`` 间接传 ``fld``，故先
    解析该函数内的 for 循环字面量元组，把循环变量还原成常量集合。
    """
    fn = _function_node(ast.parse(_sexpr_module_source()), func_name)
    loop_vars = {}
    for node in ast.walk(fn):
        if isinstance(node, ast.For):
            targets = (node.target.elts
                       if isinstance(node.target, (ast.Tuple, ast.List))
                       else [node.target])
            names = [t.id for t in targets if isinstance(t, ast.Name)]
            for entry in getattr(node.iter, "elts", []):
                if not isinstance(entry, (ast.Tuple, ast.List)):
                    continue
                for nm, val in zip(names, entry.elts):
                    if isinstance(val, ast.Constant) and isinstance(val.value, str):
                        loop_vars.setdefault(nm, set()).add(val.value)
    out = set()
    for node in ast.walk(fn):
        if not (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name) and node.func.id == "_opt"):
            continue
        for arg in node.args[1:]:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                out.add(arg.value)
            elif isinstance(arg, ast.Name):
                out.update(loop_vars.get(arg.id, ()))
    return out


class TestFlowLevelFieldSymmetry(unittest.TestCase):
    """#147 结构性对称（#150 同族缺口的守卫）。

    读侧 ``_compile_flow`` / ``_compile_childflow`` 接受的 flow 级拼写集合，
    必须与写侧发射表逐一对齐：将来读侧加一个字段而写侧忘了发，这里直接转红，
    不必等往返用例才发现。camelCase 别名按其 kebab 拼写核对。
    """

    def _assert_symmetric(self, reader_func: str, table) -> None:
        reader = _reader_opt_spellings(reader_func)
        writer = {sexpr_key for _, sexpr_key in table}
        self.assertEqual(
            writer - reader, set(),
            f"{reader_func}: 写侧发射了读侧不接受的键 {sorted(writer - reader)}")
        missing = {k for k in reader if _kebab(k) not in writer}
        self.assertEqual(
            missing, set(),
            f"{reader_func}: 读侧接受但写侧不发射 {sorted(missing)}")

    def test_root_flow_level_keys_agree(self):
        self._assert_symmetric("_compile_flow", sexpr_mod._FLOW_LEVEL_FIELDS)

    def test_child_flow_level_keys_agree(self):
        self._assert_symmetric("_compile_childflow", sexpr_mod._CHILD_FLOW_LEVEL_FIELDS)

    def test_writer_actually_consumes_the_tables(self):
        """写侧必须真的引用这两张表，否则上面的对称断言会空转。"""
        tree = ast.parse(_sexpr_module_source())
        for func_name, table_name in (
            ("flow_to_sexpr", "_FLOW_LEVEL_FIELDS"),
            ("_flow_inner_to_src", "_CHILD_FLOW_LEVEL_FIELDS"),
        ):
            with self.subTest(func=func_name, table=table_name):
                used = any(
                    isinstance(n, ast.Name) and n.id == table_name
                    for n in ast.walk(_function_node(tree, func_name))
                )
                self.assertTrue(used, f"{func_name} 未引用 {table_name}")


# ---------------------------------------------------------------------------
# #148: dict 键的发射拼写——IR → sexpr → IR 必须逐字忠实
# ---------------------------------------------------------------------------
#
# 病根：``_dict_to_sexpr`` 把键无条件写成裸 ``:{k}``，凡键含空白 / ``()`` /
# ``"`` / ``;`` 即产出**读不回来**的源码（#76 原则：发射器不得产出无法重
# 编译的源码）。下面按 issue 的验收条款逐条钉住：键语料整树往返、发射源码必
# 须能再解析、简单键逐字节不变、无法表达的键 fail closed。

#: #148 的键语料。前两条是 #147 交付时实测的两条 hazard（引号键 ⇒ 词法错误；
#: 空格键 ⇒ 被读成两条 token、形状静默改变），故显式置于最前。
_DICT_KEY_CORPUS = (
    ("#147-quote", 'q"x'),
    ("#147-space", "a b"),
    ("plain", "plain"),
    ("open-paren", "(paren"),
    ("close-paren", "pa)ren"),
    ("quote-only", '"'),
    ("squote", "it's"),
    ("backslash", "a\\b"),
    ("semicolon", "semi;colon"),
    ("tab", "tab\there"),
    ("newline", "line\nbreak"),
    ("unicode", "中文"),
    ("emoji", "emoji😀"),
    ("empty", ""),
    ("nil-like", "nil"),
    ("true-like", "true"),
    ("false-like", "false"),
    ("None-like", "None"),
    ("int-like", "1"),
    ("float-like", "1.2"),
    ("neg-int-like", "-1"),
    ("leading-colon", ":lead"),
    ("colon-inside", "a:b"),
    ("long", "long_" + "x" * 300),
)

#: 承载 dict 的（标签, 节点类型, 字段 IR 键, 源壳）。``end.error`` 是 #148 点名
#: 的路径；其余三个是同样经 ``_value()`` 编译 dict 的既有通道，保证不是只修一条路。
_DICT_CARRIERS = (
    ("end.error", "end", "error",
     "(flow g (start :id s -> e) (end :id e :output x))"),
    ("http.headers", "http", "headers",
     '(flow g (start :id s -> h) (http :id h :method GET :url "http://x" -> e)'
     " (end :id e :output x))"),
    ("http.body", "http", "body",
     '(flow g (start :id s -> h) (http :id h :method GET :url "http://x" -> e)'
     " (end :id e :output x))"),
    ("event.eventFilter", "event", "eventFilter",
     "(flow g (start :id s -> ev) (event :id ev :type t -> e)"
     " (end :id e :output x))"),
)

_BARE_KEY_RE = re.compile(r'[^\s()";]+')

#: 每种节点类型一条源壳：类级不变量「任何 ``flow_to_sexpr`` 输出都必须能再解析」
#: 的语料。``parallel`` / ``code`` 存在**既有**写侧覆盖缺口（见下方断言注释），
#: 它们同样必须能再解析。
_NODE_CORPUS_SRC = (
    "(flow g (start :id s -> e) (end :id e :output x))",
    '(flow g (start :id s -> a) (assignment :id a "$INPUT.x" -> e)'
    ' (end :id e :output "$NODE.a"))',
    '(flow g (start :id s -> i) (if :id i (cond "$INPUT.age" >= 18) -> a :else b)'
    " (end :id a :output y) (end :id b :output n))",
    '(flow g (start :id s -> sw) (switch :id sw (branch "b1" -> e'
    ' :when (cond "$INPUT.x" == 1))) (end :id e :output x))',
    '(flow g (start :id s -> c) (case :id c :target "$INPUT.x" (match 1 e))'
    " (end :id e :output x))",
    '(flow g (start :id s -> l) (loop :id l :collection "$INPUT.xs"'
    " :child (childflow (start :id cs -> ce) (end :id ce :output \"$INPUT.item\"))"
    " -> e) (end :id e :output x))",
    '(flow g (start :id s -> m) (map :id m :collection "$INPUT.xs" :concurrent true'
    " :child (childflow (start :id cs -> ce) (end :id ce :output \"$INPUT.item\"))"
    " -> e) (end :id e :output x))",
    '(flow g (start :id s -> f) (filter :id f :collection "$INPUT.xs"'
    " :child (childflow (start :id cs -> ce) (end :id ce :output \"$INPUT.item\"))"
    " -> e) (end :id e :output x))",
    '(flow g (start :id s -> fd) (find :id fd :collection "$INPUT.xs"'
    " :child (childflow (start :id cs -> ce) (end :id ce :output \"$INPUT.item\"))"
    " -> e) (end :id e :output x))",
    '(flow g (start :id s -> r) (reduce :id r :collection "$INPUT.xs" :initial 0'
    " :child (childflow (start :id cs -> ce) (end :id ce :output \"$INPUT.item\"))"
    " -> e) (end :id e :output x))",
    '(flow g (start :id s -> c) (child :id c :input "$INPUT.x"'
    " :child (childflow (start :id cs -> ce) (end :id ce :output \"$INPUT.item\"))"
    " -> e) (end :id e :output x))",
    '(flow g (start :id s -> c) (reference :id c :input "$INPUT.x"'
    ' :child (childflow :flow-id "cf" (start :id cs -> ce)'
    ' (end :id ce :output "$INPUT.item")) -> e) (end :id e :output x))',
    '(flow g (start :id s -> p) (parallel :id p (pbranch "b1"'
    " :flow (childflow (start :id cs -> ce) (end :id ce :output 1))))"
    " (end :id e :output x))",
    '(flow g (start :id s -> cd) (code :id cd :lang python :code "1" -> e)'
    " (end :id e :output x))",
    '(flow g (start :id s -> h) (http :id h :method GET :url "http://x" -> e)'
    " (end :id e :output x))",
    "(flow g (start :id s -> ev) (event :id ev :type t -> e)"
    " (end :id e :output x))",
)


def _inject_dict(shell, node_type, field, payload):
    """把 ``payload`` 作为 dict-origin IR 注入源壳编译结果里指定节点的字段。"""
    ir = compile_sexpr(shell)
    for n in ir["nodes"]:
        if n.get("type") == node_type:
            n[field] = payload
            return ir
    raise AssertionError(f"源壳里没有 {node_type} 节点：{shell}")


class TestDictKeySpellingRoundTrip(unittest.TestCase):
    """#148：dict 键（含 :error 载荷路径）必须 IR → sexpr → IR 忠实往返。"""

    # --- 主性质：每个字符串键、每个 dict 通道，整树往返相等 -----------------
    def test_every_string_key_round_trips(self):
        for label, key in _DICT_KEY_CORPUS:
            for cname, ntype, field, shell in _DICT_CARRIERS:
                with self.subTest(key=label, carrier=cname):
                    ir = _inject_dict(shell, ntype, field, {key: 1})
                    emitted = flow_to_sexpr(ir)
                    # (a) 发射出的源码必须能重新解析——#76/#148 的类级不变量
                    sexpr_mod.read_forms(emitted)
                    # (b) 整树相等（_norm_ir 归一 Symbol/Keyword 子类）
                    back = compile_sexpr(emitted)
                    self.assertEqual(_norm_ir(back), _norm_ir(ir), emitted)
                    got = next(n for n in back["nodes"]
                               if n.get("type") == ntype)[field]
                    self.assertEqual(list(got), [key], emitted)
                    self.assertIs(type(list(got)[0]), str, emitted)

    # --- 拼写边界：安全裸关键字 `:k v`，否则元组条目 `("k" v)` -------------
    def test_spelling_boundary_matches_bare_atom_rule(self):
        shell = _DICT_CARRIERS[0][3]
        for label, key in _DICT_KEY_CORPUS:
            with self.subTest(key=label):
                emitted = flow_to_sexpr(
                    _inject_dict(shell, "end", "error", {key: 1}))
                if key and _BARE_KEY_RE.fullmatch(key):
                    self.assertIn(f":{key} 1)", emitted)
                    self.assertNotIn(f'("{key}"', emitted)
                else:
                    self.assertIn(
                        f"({sexpr_mod._string_to_src(key)} 1)", emitted)

    # --- 简单键逐字节不变：既有语料零改动 ---------------------------------
    def test_simple_keys_keep_base_spelling_byte_identical(self):
        # 整串取自 base 实测（值侧 ``application/json`` 裸写是既有的
        # ``_expr_to_src`` 行为，本单**未动**；这里钉住键侧零改动）。
        self.assertEqual(
            sexpr_mod._dict_to_sexpr(
                {"Content-Type": "application/json", "name": "$INPUT.n"}),
            "(dict :Content-Type application/json :name $INPUT.n)",
        )
        # #127 已钉的 `(dict :a 1)` 形状不变
        self.assertEqual(sexpr_mod._dict_to_sexpr({"a": 1}), "(dict :a 1)")
        self.assertEqual(sexpr_mod._dict_to_sexpr({}), "(dict)")
        # 数字样 / 布尔样 / nil 样键在「关键字位」裸写本就是安全的（读侧取冒号
        # 后的原文，不像**值**位会被重读成 int/bool/None），保持 base 逐字节。
        self.assertEqual(
            sexpr_mod._dict_to_sexpr({"1": 1, "nil": 1, "true": 1}),
            "(dict :1 1 :nil 1 :true 1)",
        )

    # --- 非 str 标量键：可忠实回读者往返且类型不变 -------------------------
    def test_scalar_keys_round_trip_with_type_intact(self):
        shell = _DICT_CARRIERS[0][3]
        for key in (1, -3, 1.5, True, False, None):
            with self.subTest(key=key):
                ir = _inject_dict(shell, "end", "error", {key: 1})
                back = compile_sexpr(flow_to_sexpr(ir))
                got = next(n for n in back["nodes"]
                           if n.get("type") == "end")["error"]
                self.assertEqual(got, {key: 1})
                self.assertIs(type(list(got)[0]), type(key))

    # --- fail closed：确实无法忠实回读的键在产出文本前拒绝 -----------------
    def test_unrepresentable_keys_fail_closed_before_writing(self):
        shell = _DICT_CARRIERS[0][3]
        for key in ((1, 2), frozenset({1}), b"bytes"):
            with self.subTest(key=key):
                with self.assertRaises(sexpr_mod.SexprEmitError) as cm:
                    flow_to_sexpr(
                        _inject_dict(shell, "end", "error", {key: 1}))
                self.assertIn("dict 键", str(cm.exception))
        # 非有限 / 指数 float 的 str() 不是读侧数字字面量（会落成 Symbol），
        # 类型必变，故同样拒绝而不是静默改型。
        for key in (float("inf"), float("nan"), 1e20):
            with self.subTest(key=key):
                with self.assertRaises(sexpr_mod.SexprEmitError):
                    sexpr_mod._dict_to_sexpr({key: 1})

    def test_reader_rejects_compound_tuple_key_with_named_error(self):
        src = ("(flow g (start :id s -> e)"
               " (end :id e :output x :error (dict ((1 2) v))))")
        with self.assertRaises(SyntaxError) as cm:
            compile_sexpr(src)
        self.assertIn("dict 键", str(cm.exception))

    # --- #147 实测的两条 metadata hazard：必须能再解析且键不被拆散 ---------
    def test_147_metadata_hazard_cases_reparse_and_keep_key_whole(self):
        """#147 交付新暴露的两条 hazard（dict-origin metadata）。

        base：``{'q"x': 1}`` → ``(dict :q"x 1)`` **读不回来**（词法错误）；
        ``{"a b": 1}`` → ``(dict :a b 1)`` 被再读成 ``['dict','a','b',1]``
        （形状静默改变）。本单后：源码可再解析，键作为**单个**元素保留。

        整树回到 dict 仍受 #147 记录的读侧 ``_value()`` 缺口限制（``_compile_flow``
        不对 ``:metadata`` 走 ``_value``），故这里断言「可再解析 + 键完整」，而不是
        dict 相等——那属 #147 的既有残留项，不在 #148 边界内。
        """
        for key in ('q"x', "a b"):
            with self.subTest(key=key):
                ir = {"runtime": "python", "flow_id": "g", "metadata": {key: 1},
                      "nodes": [{"type": "start", "id": "s", "next": "e"},
                                {"type": "end", "id": "e",
                                 "resultType": "success", "output": "x"}]}
                emitted = flow_to_sexpr(ir)
                sexpr_mod.read_forms(emitted)      # 不再抛词法/语法错
                back = compile_sexpr(emitted)      # 不再抛
                self.assertEqual(back["metadata"], ["dict", [key, 1]], emitted)

    # --- 类级不变量：任何发射结果都必须能再解析 ----------------------------
    def test_every_emitted_flow_reparses_over_node_corpus(self):
        for src in _NODE_CORPUS_SRC:
            with self.subTest(src=src[:48]):
                emitted = flow_to_sexpr(compile_sexpr(src))
                sexpr_mod.read_forms(emitted)

    # --- 结构性双侧守卫：写侧/读侧必须共用元组键拼写 ----------------------
    def test_reader_and_writer_share_the_tuple_key_spelling(self):
        """防止将来任一侧单方面改回 ``:key`` 直拼而另一侧不跟。"""
        tree = ast.parse(_sexpr_module_source())
        writer_names = {n.id for n in ast.walk(
            _function_node(tree, "_dict_to_sexpr")) if isinstance(n, ast.Name)}
        self.assertIn("_dict_entry_to_src", writer_names)
        reader_names = {n.id for n in ast.walk(
            _function_node(tree, "_compile_dict_literal"))
            if isinstance(n, ast.Name)}
        self.assertIn("_dict_entry_key", reader_names)


if __name__ == "__main__":
    unittest.main()
