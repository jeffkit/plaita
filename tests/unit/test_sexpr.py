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


if __name__ == "__main__":
    unittest.main()
