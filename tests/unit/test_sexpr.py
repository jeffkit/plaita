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
#: 的语料。#150 后**每个 reader 支持的节点类型**都在此列（含别名 ``assign``），
#: 且 ``parallel`` 带分支/join/conditional/next——base 曾把它发成空壳
#: ``(parallel :id p)``，而空壳**恰好仍能再解析**，所以这条不变量此前对它无感，
#: 真正的守卫是 ``TestWriterNodeTypeSymmetry`` 与 ``TestNodeTypeRoundTripCorpus``。
_NODE_CORPUS_SRC = (
    "(flow g (start :id s -> e) (end :id e :output x))",
    '(flow g (start :id s -> a) (assignment :id a "$INPUT.x" -> e)'
    ' (end :id e :output "$NODE.a"))',
    '(flow g (start :id s -> a) (assign :id a "$INPUT.x" -> e)'
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
    '(flow g (start :id s -> p) (parallel :id p :mode thread :join-branches (b1)'
    ' :is-conditional true'
    ' (pbranch "b1" :flow (childflow (start :id cs -> ce) (end :id ce :output 1))'
    ' :input "$INPUT.x" :when (cond "$INPUT.x" eq 1)) -> e)'
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


# ---------------------------------------------------------------------------
# #150: 写侧节点类型覆盖——结构性对称守卫 + 逐类型往返 + base 字节不变
# ---------------------------------------------------------------------------
#
# 病根不是"漏了 `parallel`"，而是**写侧的类型集合可以悄悄落后于读侧**：读侧
# `_NODE_COMPILERS` 加了类型、写侧 `_node_to_src` 忘了发，没有任何东西会因此
# 变红——节点**类型**甚至还能"往返"（base 把 parallel 发成 `(parallel :id p)`，
# 它仍能再解析），损失发生在节点**内部**。故下面直接从源码 AST 抽两侧的类型
# 集合做包含断言，而不是逐类型写死预期。

#: issue #150 正文列出的 21 个"reader 接受的类型"。逐条核对代码后：其中 18 个
#: 是 ``_NODE_COMPILERS`` 的 key，另 3 个**根本不是节点类型**，读侧没有它们的
#: dispatch 分支——单列于此，以免再把计数报成 21：
#:
#: - ``flow``：**顶层 form 名**。``_compile_flow`` 只接受 ``(flow <id> …)``
#:   （``str(form[0]) != "flow"`` 直接报错），是文档结构不是节点；
#: - ``runtime``：**子流程 IR 字段**。``_compile_childflow`` 写死
#:   ``data = {"runtime": "python"}``，从不是可读的 form 头；
#: - ``flow_id``：**flow 级字段名**（``#149`` 定下的子流程 id 拼写），出现在
#:   ``_FLOW_LEVEL_FIELDS`` / ``_compile_flow``，由位置参数读入，不是节点类型。
_NON_NODE_READER_KEYS = frozenset({"flow", "flow_id", "runtime"})

#: issue #150 正文的 21 项原文，用于钉住"18 个 reader key + 3 个非节点键"的分解。
_ISSUE_150_READER_TYPES = frozenset((
    "assign", "assignment", "case", "child", "code", "end", "event", "filter",
    "find", "flow", "flow_id", "http", "if", "loop", "map", "parallel",
    "reduce", "reference", "runtime", "start", "switch",
))

#: 写侧豁免表：reader 的某个节点实现若**故意**不由 ``_node_to_src`` 发射，必须
#: 在此登记并写明理由，否则 :meth:`TestWriterNodeTypeSymmetry
#: .test_writer_dispatch_covers_every_reader_node_implementation` 判红。
#: 当前为**空**——18 个 reader key（17 个实现，``assign`` 是 ``assignment`` 的
#: 拼写别名）现在全部有写侧分支；空表本身也有断言
#: (:meth:`…test_no_undocumented_writer_exclusions`)，避免豁免被悄悄加进来。
_WRITER_NODE_TYPE_EXCLUSIONS = {}


def _reader_node_compilers():
    """AST 抽取 ``_NODE_COMPILERS`` 的 ``{节点类型: handler 源码}`` 映射。"""
    tree = ast.parse(_sexpr_module_source())
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "_NODE_COMPILERS"):
            return {k.value: ast.unparse(v)
                    for k, v in zip(node.value.keys, node.value.values)}
    raise AssertionError("未在 sexpr.py 中找到 _NODE_COMPILERS 字面量")


def _writer_node_types():
    """AST 抽取 ``_node_to_src`` 里以 ``t`` 为判别式发射的类型集合。

    只认 ``t == "x"`` / ``t in ("x", …)``：``left`` 必须是 ``Name("t")``。
    这样 ``n["resultType"] != "success"`` 之类的字段比较不会被误收。
    """
    fn = _function_node(ast.parse(_sexpr_module_source()), "_node_to_src")
    out = set()
    for node in ast.walk(fn):
        if not (isinstance(node, ast.Compare) and len(node.ops) == 1
                and isinstance(node.left, ast.Name) and node.left.id == "t"):
            continue
        op, other = node.ops[0], node.comparators[0]
        if isinstance(op, ast.Eq) and isinstance(other, ast.Constant):
            if isinstance(other.value, str):
                out.add(other.value)
        elif isinstance(op, ast.In) and isinstance(other, (ast.Tuple, ast.List)):
            for elt in other.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    out.add(elt.value)
    return out


class TestWriterNodeTypeSymmetry(unittest.TestCase):
    """#150 验收 2：写侧类型集合 ⊇ 读侧 ``_NODE_COMPILERS`` 的实现集合。

    读侧按 **handler 实现**分组：``assign`` / ``assignment`` 指向同一个
    ``_c_assignment``，属**拼写别名**而非两个节点类型，因此只要实现被覆盖即可。
    """

    def test_writer_dispatch_covers_every_reader_node_implementation(self):
        reader = _reader_node_compilers()
        writer = _writer_node_types()
        groups = {}
        for ntype, handler in reader.items():
            groups.setdefault(handler, set()).add(ntype)
        uncovered = {
            "/".join(sorted(g)): reader[next(iter(g))]
            for g in groups.values()
            if not (g & writer) and not (g & set(_WRITER_NODE_TYPE_EXCLUSIONS))
        }
        self.assertEqual(
            uncovered, {},
            f"读侧接受、写侧 _node_to_src 不发射的节点实现: {uncovered}")

    def test_reader_alias_groups_are_only_assign_assignment(self):
        """别名只允许 ``assign``/``assignment`` 这一组（即只允许同实现拼写）。"""
        reader = _reader_node_compilers()
        groups = {}
        for ntype, handler in reader.items():
            groups.setdefault(handler, set()).add(ntype)
        aliases = {tuple(sorted(g)) for g in groups.values() if len(g) > 1}
        self.assertEqual(aliases, {("assign", "assignment")})
        self.assertEqual(len(groups), 17, "reader 节点实现数应为 17")

    def test_writer_emits_no_node_type_the_reader_rejects(self):
        """反向：写侧不得发射读侧不认识的类型（防单方面扩写侧）。"""
        extra = _writer_node_types() - set(_reader_node_compilers())
        self.assertEqual(extra, set(), f"写侧发射了读侧不接受的类型: {sorted(extra)}")

    def test_issue_150_count_is_18_reader_keys_plus_3_non_nodes(self):
        reader = set(_reader_node_compilers())
        self.assertEqual(reader | _NON_NODE_READER_KEYS, _ISSUE_150_READER_TYPES)
        self.assertEqual(reader & _NON_NODE_READER_KEYS, set())
        self.assertEqual(len(reader), 18)
        self.assertEqual(len(_ISSUE_150_READER_TYPES), 21)

    def test_no_undocumented_writer_exclusions(self):
        self.assertEqual(_WRITER_NODE_TYPE_EXCLUSIONS, {})

    def test_guard_reads_the_real_discriminant(self):
        """守卫靠 ``t`` 找判别式；钉住 ``t`` 确实来自 ``n["type"]``，否则空转。"""
        fn = _function_node(ast.parse(_sexpr_module_source()), "_node_to_src")
        self.assertEqual(ast.unparse(fn.body[0]), "t = n.get('type')")
        self.assertIn("parallel", _writer_node_types())


#: 往返语料里内联子流程的统一壳（带 flow-id/type/desc，顺带覆盖 #147 的字段）。
_RT_CHILD = ('(childflow :flow-id cf :input-type object :desc 子 :output-type string'
             ' (start -> ce) (end ce :output 1))')

#: #150 验收 3 的往返语料：**每个 reader 节点实现**一条，且把该类型读侧能读的
#: 字段**全部填满**——最小可编译形状恰好会漏掉"节点内部丢字段"（base 上
#: parallel 就是空壳）。值一律选用 ``_expr_to_src`` 能忠实回读的拼写：数字样
#: **字符串**值（``(code :code "1"`` → ``1`` → int）是 #150 边界外的既有
#: ``_expr_to_src`` 残留，故 ``code`` 行用 ``"return 1"``；该残留另有独测钉住。
_NODE_TYPE_ROUNDTRIP_CORPUS = (
    ("start", '(flow g :input-type object (start :id n0 :timeout 5 -> n1)'
              ' (end :id n1 :output 1))'),
    ("end", '(flow g :input-type object (start :id n0 -> e)'
            ' (end :id e :timeout 5 :output 1 :result-type error'
            ' :error (dict :code 4) -> zz) (end :id zz :output 9))'),
    ("assignment", '(flow g :input-type object (start :id n0 -> a)'
                   ' (assignment :id a 1 :output-type object :timeout 5 -> e)'
                   ' (end :id e :output 9))'),
    ("if", '(flow g :input-type object (start :id n0 -> i)'
           ' (if :id i (cond "$INPUT.x" eq 1) -> e :else a :timeout 5)'
           ' (end :id e :output 9))'),
    ("switch", '(flow g :input-type object (start :id n0 -> sw)'
               ' (switch :id sw :timeout 5 (branch b1 -> e'
               ' :when (cond "$INPUT.x" eq 1) :priority 2)'
               ' (branch b2 -> e :default true) -> e) (end :id e :output 9))'),
    ("case", '(flow g :input-type object (start :id n0 -> c)'
             ' (case :id c :target "$INPUT.x" :timeout 5 (match 1 e :name m1)'
             ' :default e) (end :id e :output 9))'),
    ("loop", '(flow g :input-type object (start :id n0 -> l)'
             ' (loop :id l :collection "$INPUT.xs" :child %s'
             ' :when (cond "$INPUT.i" eq 1) :timeout 5 -> e)'
             ' (end :id e :output 9))' % _RT_CHILD),
    ("map", '(flow g :input-type object (start :id n0 -> m)'
            ' (map :id m :collection "$INPUT.xs" :child %s :concurrent true'
            ' :max-concurrent 3 :timeout 5 -> e) (end :id e :output 9))' % _RT_CHILD),
    ("filter", '(flow g :input-type object (start :id n0 -> f)'
               ' (filter :id f :collection "$INPUT.xs" :child %s :timeout 5 -> e)'
               ' (end :id e :output 9))' % _RT_CHILD),
    ("find", '(flow g :input-type object (start :id n0 -> fd)'
             ' (find :id fd :collection "$INPUT.xs" :child %s :timeout 5 -> e)'
             ' (end :id e :output 9))' % _RT_CHILD),
    ("reduce", '(flow g :input-type object (start :id n0 -> r)'
               ' (reduce :id r :collection "$INPUT.xs" :child %s :initial 0'
               ' :timeout 5 -> e) (end :id e :output 9))' % _RT_CHILD),
    ("child", '(flow g :input-type object (start :id n0 -> ch)'
              ' (child :id ch :input "$INPUT.x" :child %s :timeout 5 -> e)'
              ' (end :id e :output 9))' % _RT_CHILD),
    ("reference", '(flow g :input-type object (start :id n0 -> rf)'
                  ' (reference :id rf :input "$INPUT.x" :child %s :timeout 5 -> e)'
                  ' (end :id e :output 9))' % _RT_CHILD),
    ("parallel", '(flow g :input-type object (start :id n0 -> p)'
                 ' (parallel :id p :mode thread :join-branches (b1 b2)'
                 ' :is-conditional true :timeout 5'
                 ' (pbranch b1 :flow %s :input "$INPUT.x"'
                 ' :when (cond "$INPUT.x" eq 1)) (pbranch b2 :flow %s) -> e)'
                 ' (end :id e :output 9))' % (_RT_CHILD, _RT_CHILD)),
    ("code", '(flow g :input-type object (start :id n0 -> cd)'
             ' (code :id cd :lang python :code "return 1" :input "$INPUT.x"'
             ' :timeout 5 -> e) (end :id e :output 9))'),
    ("http", '(flow g :input-type object (start :id n0 -> h)'
             ' (http :id h :method POST :url "http://x" :headers (dict :a 1)'
             ' :body "b" :input "$INPUT.x" :timeout 5 -> e)'
             ' (end :id e :output 9))'),
    ("event", '(flow g :input-type object (start :id n0 -> ev)'
              ' (event :id ev :type tick :filter (dict :a 1) :timeout 5 -> e)'
              ' (end :id e :output 9))'),
)


class TestNodeTypeRoundTripCorpus(unittest.TestCase):
    """#150 验收 3：每个 reader 节点类型 emit → re-read → **整树 IR 相等**。"""

    def test_corpus_covers_every_reader_node_implementation(self):
        reader = _reader_node_compilers()
        groups = {}
        for ntype, handler in reader.items():
            groups.setdefault(handler, set()).add(ntype)
        labels = {label for label, _ in _NODE_TYPE_ROUNDTRIP_CORPUS}
        uncovered = sorted("/".join(sorted(g)) for g in groups.values()
                           if not (g & labels))
        self.assertEqual(uncovered, [], f"语料未覆盖的 reader 节点实现: {uncovered}")

    def test_all_fields_populated_corpus_round_trips(self):
        for label, src in _NODE_TYPE_ROUNDTRIP_CORPUS:
            with self.subTest(node_type=label):
                ir = compile_sexpr(src)
                emitted = flow_to_sexpr(ir)
                # (a) #76 类级不变量：发射结果必须能再解析
                sexpr_mod.read_forms(emitted)
                # (b) 整树相等（含节点内所有字段）
                back = compile_sexpr(emitted)
                self.assertEqual(_norm_ir(back), _norm_ir(ir), emitted)

    def test_parallel_node_survives_with_all_of_its_parts(self):
        """#150 的点名形状：base 发 `(parallel :id p)`，分支/汇合/条件/后继全丢。"""
        src = dict(_NODE_TYPE_ROUNDTRIP_CORPUS)["parallel"]
        emitted = flow_to_sexpr(compile_sexpr(src))
        for token in ("(pbranch b1 ", "(pbranch b2 ",
                      ":join-branches (b1 b2)", ":is-conditional true",
                      ":mode thread", ":input $INPUT.x",
                      ":when (cond $INPUT.x eq 1)", "-> e)"):
            self.assertIn(token, emitted)
        par = next(n for n in compile_sexpr(emitted)["nodes"]
                   if n["type"] == "parallel")
        self.assertEqual([b["name"] for b in par["branches"]], ["b1", "b2"])
        self.assertEqual(par["joinBranches"], ["b1", "b2"])
        self.assertTrue(par["isConditional"])
        self.assertEqual(par["mode"], "thread")
        self.assertEqual(par["next"], "e")
        self.assertEqual(par["branches"][0]["input"], "$INPUT.x")
        self.assertIn("condition", par["branches"][0])


class TestNodeTypeWriterByteIdentity(unittest.TestCase):
    """#150 验收 4：base **已发射正确**的形状必须逐字节不变。

    期望值取自 base ``0ce8549`` 上的实测发射（``git archive`` 导出、断言 import
    路径）；下面每个形状在 base 上都不含本单新增的字段，故必须逐字节相等。
    """

    _BASE_BYTE_IDENTICAL = (
        ("plain",
         "(flow g (start :id s -> e) (end :id e :output x))",
         "(flow g (start :id s -> e) (end :id e :output x))"),
        ("assignment-no-output-type",
         '(flow g (start :id s -> a) (assignment :id a "$INPUT.x" -> e)'
         ' (end :id e :output "$NODE.a"))',
         '(flow g (start :id s -> a) (assignment :id a $INPUT.x -> e)'
         ' (end :id e :output $NODE.a))'),
        ("switch-no-priority",
         '(flow g (start :id s -> sw) (switch :id sw (branch b1 -> e'
         ' :when (cond "$INPUT.x" == 1))) (end :id e :output x))',
         '(flow g (start :id s -> sw) (switch :id sw (branch b1 -> e'
         ' :when (cond $INPUT.x eq 1))) (end :id e :output x))'),
        ("case-default-name",
         '(flow g (start :id s -> c) (case :id c :target "$INPUT.x" (match 1 e))'
         " (end :id e :output x))",
         '(flow g (start :id s -> c) (case :id c :target $INPUT.x (match 1 e))'
         " (end :id e :output x))"),
        ("map-no-max-concurrent",
         '(flow g (start :id s -> m) (map :id m :collection "$INPUT.xs"'
         ' :concurrent true :child (childflow (start :id cs -> ce)'
         ' (end :id ce :output 1)) -> e) (end :id e :output x))',
         '(flow g (start :id s -> m) (map :id m :collection $INPUT.xs'
         ' :child (childflow (start :id cs -> ce) (end :id ce :output 1))'
         ' :concurrent true -> e) (end :id e :output x))'),
        ("child-input-not-affected",
         '(flow g (start :id s -> c) (child :id c :input "$INPUT.x"'
         ' :child (childflow (start :id cs -> ce) (end :id ce :output 1)) -> e)'
         " (end :id e :output x))",
         '(flow g (start :id s -> c) (child :id c :input $INPUT.x'
         ' :child (childflow (start :id cs -> ce) (end :id ce :output 1)) -> e)'
         " (end :id e :output x))"),
    )

    def test_base_correct_shapes_stay_byte_identical(self):
        for label, src, expected in self._BASE_BYTE_IDENTICAL:
            with self.subTest(shape=label):
                self.assertEqual(flow_to_sexpr(compile_sexpr(src)), expected)

    def test_previously_dropped_fields_are_now_emitted(self):
        """逐条钉住 base 静默丢弃、本单补发的字段。

        每行给的是**同一份 src 在 base ``0ce8549`` 上的实测发射**（``git archive``
        导出、断言 import 路径）：它既不含新 token，其回读 IR 也不等于 src 的 IR
        ——这正是"base 丢字段"的双重证据，也让本断言具备区分力（不是自证）。
        """
        cases = (
            ("parallel", ":input $INPUT.x",
             "(flow g (start :id s -> p) (parallel :id p) (end :id e :output x))",
             '(flow g (start :id s -> p) (parallel :id p :mode thread'
             ' :join-branches (b1 b2) :is-conditional true'
             ' (pbranch b1 :flow (childflow (start :id cs -> ce)'
             ' (end :id ce :output 1)) :input "$INPUT.x"'
             ' :when (cond "$INPUT.x" eq 1))'
             ' (pbranch b2 :flow (childflow (start :id cs -> ce)'
             ' (end :id ce :output 2))) -> e) (end :id e :output x))'),
            ("switch.branch.priority", ":priority 2",
             "(flow g (start :id s -> sw) (switch :id sw (branch b1 -> e"
             " :when (cond $INPUT.x eq 1)) (branch b2 -> e :default true))"
             " (end :id e :output x))",
             '(flow g (start :id s -> sw) (switch :id sw (branch b1 -> e'
             ' :when (cond "$INPUT.x" == 1) :priority 2)'
             ' (branch b2 -> e :default true)) (end :id e :output x))'),
            ("switch.next", "-> x",
             "(flow g (start :id s -> sw) (switch :id sw (branch b1 -> e"
             " :when (cond $INPUT.x eq 1))) (end :id e :output x)"
             " (end :id x :output y))",
             '(flow g (start :id s -> sw) (switch :id sw (branch b1 -> e'
             ' :when (cond "$INPUT.x" == 1)) -> x) (end :id e :output x)'
             ' (end :id x :output y))'),
            ("case.name", ':name "m1"',
             "(flow g (start :id s -> c) (case :id c :target $INPUT.x"
             " (match 1 e) :default e) (end :id e :output x))",
             '(flow g (start :id s -> c) (case :id c :target "$INPUT.x"'
             ' (match 1 e :name m1) :default e) (end :id e :output x))'),
            ("assignment.outputType", ":output-type string",
             "(flow g (start :id s -> a) (assignment :id a $INPUT.x -> e)"
             " (end :id e :output x))",
             '(flow g (start :id s -> a) (assignment :id a "$INPUT.x"'
             ' :output-type string -> e) (end :id e :output x))'),
            ("map.maxConcurrent", ":max-concurrent 3",
             "(flow g (start :id s -> m) (map :id m :collection $INPUT.xs"
             " :child (childflow (start :id cs -> ce) (end :id ce :output 1))"
             " :concurrent true -> e) (end :id e :output x))",
             '(flow g (start :id s -> m) (map :id m :collection "$INPUT.xs"'
             ' :concurrent true :max-concurrent 3 :child (childflow'
             ' (start :id cs -> ce) (end :id ce :output 1)) -> e)'
             " (end :id e :output x))"),
            ("code.input", ":input $INPUT.x",
             '(flow g (start :id s -> c) (code :id c :lang python'
             ' :code "return 1" -> e) (end :id e :output x))',
             '(flow g (start :id s -> c) (code :id c :lang python'
             ' :code "return 1" :input "$INPUT.x" -> e)'
             " (end :id e :output x))"),
            ("http.input", ":input $INPUT.x",
             '(flow g (start :id s -> h) (http :id h :method GET'
             ' :url http://x -> e) (end :id e :output x))',
             '(flow g (start :id s -> h) (http :id h :method GET'
             ' :url "http://x" :input "$INPUT.x" -> e) (end :id e :output x))'),
            ("end.next", "-> x",
             "(flow g (start :id s -> e) (end :id e :output 1)"
             " (end :id x :output 2))",
             '(flow g (start :id s -> e) (end :id e :output 1 -> x)'
             " (end :id x :output 2))"),
        )
        for label, new_token, base_emission, src in cases:
            with self.subTest(field=label):
                emitted = flow_to_sexpr(compile_sexpr(src))
                # 新字段确实进了发射文本与回读 IR
                self.assertIn(new_token, emitted)
                self.assertEqual(_norm_ir(compile_sexpr(emitted)),
                                 _norm_ir(compile_sexpr(src)), emitted)
                # base 的实测发射缺这个字段，且其回读 IR 与真实 IR 不等
                self.assertNotIn(new_token, base_emission)
                self.assertNotEqual(_norm_ir(compile_sexpr(base_emission)),
                                    _norm_ir(compile_sexpr(src)),
                                    f"{label}: base 发射的回读 IR 竟然等价")


if __name__ == "__main__":
    unittest.main()
