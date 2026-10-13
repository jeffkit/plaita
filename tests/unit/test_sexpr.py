"""Tests for plaita.dsl.sexpr — S-表达式前端。"""

import unittest

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


if __name__ == "__main__":
    unittest.main()
