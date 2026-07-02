"""Tests for plaita.dsl — Python Builder DSL。"""

import unittest

from plaita.dsl import (
    build,
    linear,
    child_flow,
    cond,
    cond_group,
    error_handler,
    start,
    end,
    assignment,
    if_,
    switch,
    branch,
    case,
    map,
    filter,
    find,
    reduce,
    loop,
    child,
    parallel,
    parallel_branch,
    http,
)


class TestBasicFlow(unittest.TestCase):
    def test_if_branch_flow(self):
        flow = (
            build("adult_check", input_type="object", desc="判断成年")
            .add(start(next="check_age"))
            .add(if_(
                id="check_age",
                condition=cond("$INPUT.age", ">=", 18),
                next="end_adult",
                else_next="end_minor",
            ))
            .add(end("end_adult", output="成年"))
            .add(end("end_minor", output="未成年"))
            .build()
        )
        self.assertEqual(flow.run(age=20), "成年")
        self.assertEqual(flow.run(age=15), "未成年")

    def test_to_dict_preserves_camel_case_fields(self):
        builder = (
            build("x", input_type="object")
            .add(start(next="e"))
            .add(end("e", output="$INPUT.x"))
        )
        data = builder.to_dict()
        self.assertEqual(data["flow_id"], "x")
        self.assertEqual(data["inputType"], {"dataType": "object"})
        self.assertEqual(data["nodes"][0]["type"], "start")
        self.assertEqual(data["nodes"][1]["resultType"], "success")

    def test_to_json_roundtrips_through_flow(self):
        builder = (
            build("echo", input_type="object")
            .add(start(next="e"))
            .add(end("e", output="$INPUT.x"))
        )
        flow = builder.build()
        self.assertEqual(flow.run(x="hi"), "hi")
        # JSON 可被 Flow.from_string 重新解析
        from plaita.core.flow import Flow
        flow2 = Flow.from_string(builder.to_json())
        self.assertEqual(flow2.run(x="hi"), "hi")


class TestCondHelpers(unittest.TestCase):
    def test_cond_normalizes_symbol_operator(self):
        self.assertEqual(cond("$INPUT.x", ">=", 18)["operator"], "gte")
        self.assertEqual(cond("$INPUT.x", "==", 1)["operator"], "eq")
        self.assertEqual(cond("$INPUT.x", "!=", 1)["operator"], "ne")

    def test_cond_keeps_canonical_operator(self):
        self.assertEqual(cond("$INPUT.x", "gte", 18)["operator"], "gte")

    def test_cond_group_validates_relation(self):
        g = cond_group("and", [cond("$INPUT.a", ">", 1), cond("$INPUT.b", "<", 2)])
        self.assertEqual(g["relation"], "and")
        self.assertEqual(len(g["conditions"]), 2)
        with self.assertRaises(ValueError):
            cond_group("xor", [])

    def test_error_handler_validates_strategy(self):
        self.assertEqual(error_handler("continue_with", default_value=0)["strategy"], "continue_with")
        with self.assertRaises(ValueError):
            error_handler("bogus")


class TestCollectionNodes(unittest.TestCase):
    def test_map_with_child_flow_decorator(self):
        @child_flow(input_type="object")
        def double_each(c):
            c.add(start(next="e"))
            c.add(end("e", output="$F.mul($INPUT.item, 2)"))

        # 拼接在 builder 上
        f = (
            build("double", input_type="object")
            .add(start(next="double_all"))
            .add(map(id="double_all", collection="$INPUT.numbers",
                     child_flow=double_each, next="end"))
            .add(end("end", output="$NODE.double_all"))
            .build()
        )
        self.assertEqual(f.run(numbers=[1, 2, 3, 4]), [2, 4, 6, 8])

    def test_filter_and_find(self):
        # 子流程：用 if 分支到输出 True/False 的两个 end，返回 bool
        @child_flow(input_type="object")
        def is_even(c):
            c.add(start(next="check"))
            c.add(if_(id="check",
                      condition=cond("$F.mod($INPUT.item, 2)", "==", 0),
                      next="yes", else_next="no"))
            c.add(end("yes", output=True))
            c.add(end("no", output=False))

        f = (
            build("evens", input_type="object")
            .add(start(next="flt"))
            .add(filter(id="flt", collection="$INPUT.nums", child_flow=is_even, next="end"))
            .add(end("end", output="$NODE.flt"))
            .build()
        )
        self.assertEqual(f.run(nums=[1, 2, 3, 4, 6]), [2, 4, 6])

        f2 = (
            build("first_even", input_type="object")
            .add(start(next="fd"))
            .add(find(id="fd", collection="$INPUT.nums", child_flow=is_even, next="end"))
            .add(end("end", output="$NODE.fd"))
            .build()
        )
        self.assertEqual(f2.run(nums=[1, 3, 4, 6]), 4)

    def test_loop_with_break_condition(self):
        @child_flow(input_type="object")
        def echo(c):
            c.add(start(next="e"))
            c.add(end("e", output="$INPUT.item"))

        # 循环条件引用 $LOOP-ITEM：元素 < 3 时继续，遇到 3 中止
        f = (
            build("loop_until", input_type="object")
            .add(start(next="lp"))
            .add(loop(id="lp", collection="$INPUT.nums", child_flow=echo,
                      condition=cond("$LOOP-ITEM", "<", 3), next="end"))
            .add(end("end", output="$NODE.lp"))
            .build()
        )
        # 循环条件引用 $LOOP-ITEM：每处理完一个元素后检查
        # 1,2 满足继续；处理 3 后 3<3 为假 -> 中止，返回最后一次结果 3
        self.assertEqual(f.run(nums=[1, 2, 3, 4]), 3)


class TestSwitchAndCase(unittest.TestCase):
    def test_switch_routes_by_priority(self):
        flow = (
            build("router", input_type="object")
            .add(start(next="route"))
            .add(switch(id="route", branches=[
                branch(name="a", next="end_a", condition=cond("$INPUT.type", "==", "A"), priority=0),
                branch(name="b", next="end_b", condition=cond("$INPUT.type", "==", "B")),
                branch(name="dft", next="end_dft", is_default=True),
            ]))
            .add(end("end_a", output="A"))
            .add(end("end_b", output="B"))
            .add(end("end_dft", output="other"))
            .build()
        )
        self.assertEqual(flow.run(type="A"), "A")
        self.assertEqual(flow.run(type="B"), "B")
        self.assertEqual(flow.run(type="Z"), "other")

    def test_case_equal_match(self):
        flow = (
            build("case_router", input_type="object")
            .add(start(next="route"))
            .add(case(id="route", target="$INPUT.n", cases=[
                {"name": "one", "value": 1, "next": "end1"},
                {"name": "two", "value": 2, "next": "end2"},
            ], default="endd"))
            .add(end("end1", output="一"))
            .add(end("end2", output="二"))
            .add(end("endd", output="其它"))
            .build()
        )
        self.assertEqual(flow.run(n=1), "一")
        self.assertEqual(flow.run(n=2), "二")
        self.assertEqual(flow.run(n=99), "其它")


class TestBuildTimeValidation(unittest.TestCase):
    def test_duplicate_ids_rejected(self):
        with self.assertRaises(ValueError):
            (
                build("dup", input_type="object")
                .add(start(id="s", next="e"))
                .add(end(id="e"))
                .add(end(id="e", output="x"))
                .build()
            )

    def test_dangling_next_rejected(self):
        with self.assertRaises(ValueError):
            (
                build("bad", input_type="object")
                .add(start(next="nope"))
                .add(end("end"))
                .build()
            )

    def test_if_missing_else_next_target_rejected(self):
        with self.assertRaises(ValueError):
            (
                build("bad_if", input_type="object")
                .add(start(next="c"))
                .add(if_(id="c", condition=cond("$INPUT.x", ">", 1),
                         next="end", else_next="ghost"))
                .add(end("end"))
                .build()
            )

    def test_switch_without_default_rejected(self):
        with self.assertRaises(ValueError):
            (
                build("bad_switch", input_type="object")
                .add(start(next="s"))
                .add(switch(id="s", branches=[
                    branch(name="a", next="end", condition=cond("$INPUT.x", "==", 1)),
                ]))
                .add(end("end"))
                .build()
            )

    def test_valid_flow_passes_validation(self):
        # 不抛错即通过
        (
            build("ok", input_type="object")
            .add(start(next="e"))
            .add(end("e"))
            .validate()
        )


class TestSerialization(unittest.TestCase):
    def setUp(self):
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("PyYAML 未安装")

    def test_to_yaml_roundtrips(self):
        from plaita.core.flow import Flow

        builder = (
            build("adult_check", input_type="object", desc="判断成年")
            .add(start(next="check_age"))
            .add(if_(id="check_age", condition=cond("$INPUT.age", ">=", 18),
                     next="end_adult", else_next="end_minor"))
            .add(end("end_adult", output="成年"))
            .add(end("end_minor", output="未成年"))
        )
        flow = Flow.from_string(builder.to_yaml())
        self.assertEqual(flow.run(age=20), "成年")


class TestLinearBuilder(unittest.TestCase):
    """隐式 next 的 LinearBuilder。"""

    def test_if_branch_no_explicit_next(self):
        flow = (
            linear("adult_check", input_type="object", desc="判断成年")
            .start()
            .if_(condition=cond("$INPUT.age", ">=", 18),
                 then="adult", else_="minor")
            .end("adult", output="成年")
            .end("minor", output="未成年")
            .build()
        )
        self.assertEqual(flow.run(age=20), "成年")
        self.assertEqual(flow.run(age=15), "未成年")

    def test_auto_chain_sets_next_in_declaration_order(self):
        b = (
            linear("pipe", input_type="object")
            .start()
            .assignment(output="$F.upper($INPUT.x)")
            .end(output="$NODE._n2")
        )
        data = b.to_dict()
        nodes = data["nodes"]
        self.assertEqual(nodes[0]["next"], "_n2")  # start -> assignment
        self.assertEqual(nodes[1]["next"], "_n3")  # assignment -> end
        self.assertIsNone(nodes[2].get("next"))    # end: 无 next
        self.assertEqual(b.build().run(x="hi"), "HI")

    def test_if_then_defaults_to_next_declared(self):
        # then 省略 → 默认走下一个声明节点；else_ 显式
        flow = (
            linear("guard", input_type="object")
            .start()
            .if_(condition=cond("$INPUT.ok", "==", True), else_="bad")
            .end("ok_end", output="ok")
            .end("bad", output="bad")
            .build()
        )
        self.assertEqual(flow.run(ok=True), "ok")
        self.assertEqual(flow.run(ok=False), "bad")

    def test_auto_ids_unique_and_claimed(self):
        b = (
            linear("ids", input_type="object")
            .start()
            .assignment(output="$INPUT.x")
            .end(output="$NODE._n2")
        )
        ids = [n["id"] for n in b.to_dict()["nodes"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(ids[0], "_n1")

    def test_explicit_id_collision_rejected(self):
        with self.assertRaises(ValueError):
            (
                linear("collide", input_type="object")
                .start(id="dup")
                .end(id="dup")
                .build()
            )

    def test_map_with_child_flow(self):
        from plaita.dsl import child_flow, start, end

        @child_flow(input_type="object")
        def double_each(c):
            c.add(start(next="e"))
            c.add(end("e", output="$F.mul($INPUT.item, 2)"))

        flow = (
            linear("double", input_type="object")
            .start()
            .map(collection="$INPUT.numbers", child_flow=double_each, id="double_all")
            .end(output="$NODE.double_all")
            .build()
        )
        self.assertEqual(flow.run(numbers=[1, 2, 3]), [2, 4, 6])

    def test_switch_with_labels(self):
        flow = (
            linear("router", input_type="object")
            .start()
            .switch(branches=[
                branch(name="a", next="end_a", condition=cond("$INPUT.type", "==", "A")),
                branch(name="b", next="end_b", condition=cond("$INPUT.type", "==", "B")),
                branch(name="dft", next="end_dft", is_default=True),
            ])
            .end("end_a", output="A")
            .end("end_b", output="B")
            .end("end_dft", output="other")
            .build()
        )
        self.assertEqual(flow.run(type="A"), "A")
        self.assertEqual(flow.run(type="Z"), "other")

    def test_to_yaml_roundtrip(self):
        from plaita.core.flow import Flow

        b = (
            linear("adult_check", input_type="object")
            .start()
            .if_(condition=cond("$INPUT.age", ">=", 18),
                 then="adult", else_="minor")
            .end("adult", output="成年")
            .end("minor", output="未成年")
        )
        flow = Flow.from_string(b.to_yaml())
        self.assertEqual(flow.run(age=20), "成年")


class TestBuilderMutation(unittest.TestCase):
    """Tests for FlowBuilder mutation methods: remove_node / update_node / reroute / from_dict."""

    def _simple_builder(self):
        """echo flow: start -> greet -> end"""
        return (
            build("echo", input_type="object")
            .add(start(id="start", next="greet"))
            .add(assignment(id="greet", output="$F.concat('hi ', $INPUT.name)", next="end"))
            .add(end("end", output="$NODE.greet"))
        )

    def test_remove_node(self):
        b = self._simple_builder()
        b.remove_node("greet")
        ids = [n["id"] for n in b._nodes]
        self.assertNotIn("greet", ids)
        self.assertEqual(len(ids), 2)

    def test_remove_node_missing_raises(self):
        b = self._simple_builder()
        with self.assertRaises(KeyError):
            b.remove_node("nonexistent")

    def test_update_node(self):
        b = self._simple_builder()
        b.update_node("greet", output="$F.concat('hello ', $INPUT.name)")
        node = next(n for n in b._nodes if n["id"] == "greet")
        self.assertEqual(node["output"], "$F.concat('hello ', $INPUT.name)")

    def test_update_node_missing_raises(self):
        b = self._simple_builder()
        with self.assertRaises(KeyError):
            b.update_node("ghost", output="x")

    def test_reroute_next(self):
        b = self._simple_builder()
        b.reroute("start", next="end")
        node = next(n for n in b._nodes if n["id"] == "start")
        self.assertEqual(node["next"], "end")

    def test_reroute_missing_raises(self):
        b = self._simple_builder()
        with self.assertRaises(KeyError):
            b.reroute("missing", next="end")

    def test_from_dict_roundtrip(self):
        from plaita.dsl import FlowBuilder
        builder = self._simple_builder()
        # from_dict 使用 to_dict() 产出的格式（与 JSON 格式一致）
        raw = builder.to_dict()
        rebuilt = FlowBuilder.from_dict(raw).build()
        self.assertEqual(rebuilt.run(name="alice"), "hi alice")

    def test_mutation_then_build(self):
        """remove greet node, reroute start to end directly"""
        from plaita.dsl import FlowBuilder
        b = self._simple_builder()
        b.remove_node("greet")
        b.reroute("start", next="end")
        b.update_node("end", output="$INPUT.name")
        flow = b.build()
        self.assertEqual(flow.run(name="bob"), "bob")


class TestReduceNode(unittest.TestCase):
    """Tests for the reduce node (bug fix verification)."""

    def _make_sum_child(self):
        """子流程：接收 first + second，返回 first + second"""
        return (
            build(input_type="object")
            .add(start(next="sum"))
            .add(end("sum", output="$F.add($INPUT.first, $INPUT.second)"))
        ).to_dict()

    def test_reduce_sum(self):
        import json
        child = self._make_sum_child()
        flow_def = {
            "flow_id": "sum_test",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "reduce"},
                {
                    "type": "reduce",
                    "id": "reduce",
                    "collection": "$INPUT.nums",
                    "childFlow": child,
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "$NODE.reduce", "resultType": "success"},
            ],
        }
        from plaita.core.flow import Flow
        flow = Flow.model_validate(flow_def)
        result = flow.run(nums=[1, 2, 3, 4])
        self.assertEqual(result, 10)

    def test_reduce_with_initial(self):
        child = self._make_sum_child()
        flow_def = {
            "flow_id": "sum_initial",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "reduce"},
                {
                    "type": "reduce",
                    "id": "reduce",
                    "collection": "$INPUT.nums",
                    "initial": 100,
                    "childFlow": child,
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "$NODE.reduce", "resultType": "success"},
            ],
        }
        from plaita.core.flow import Flow
        flow = Flow.model_validate(flow_def)
        result = flow.run(nums=[1, 2, 3])
        self.assertEqual(result, 106)

    def test_reduce_initial_zero_is_valid(self):
        """initial=0 是有效初始值，不应被误判为 falsy"""
        child = self._make_sum_child()
        flow_def = {
            "flow_id": "sum_zero_initial",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "reduce"},
                {
                    "type": "reduce",
                    "id": "reduce",
                    "collection": "$INPUT.nums",
                    "initial": 0,
                    "childFlow": child,
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "$NODE.reduce", "resultType": "success"},
            ],
        }
        from plaita.core.flow import Flow
        flow = Flow.model_validate(flow_def)
        result = flow.run(nums=[5, 3, 2])
        self.assertEqual(result, 10)


if __name__ == "__main__":
    unittest.main()
