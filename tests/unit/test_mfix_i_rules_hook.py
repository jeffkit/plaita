"""M 修复包 I：validate_flow_ir 规则钩子 + 内置规则（MI1 / MI2）。

每条规则的复核依据（运行期实证）见各测试 docstring；规则实现位于
``plaita/dsl/ir_validate.py``。
"""
from __future__ import annotations

import unittest

from plaita.dsl.ir_validate import (
    DEFAULT_RULES,
    FlowIRGraph,
    FlowIRValidationError,
    build_flow,
    check_expression_functions,
    forbid_node_types_in_childflow,
    unknown_expression_functions,
    validate_flow_ir,
)


def _flow(nodes, **extra):
    data = {"runtime": "python", "flow_id": "t", "nodes": nodes}
    data.update(extra)
    return data


_START_MOCK_END = [
    {"type": "start", "id": "s", "next": "a"},
    {"type": "mock", "id": "a", "output": "x", "next": "e"},
    {"type": "end", "id": "e", "output": "$NODE.a"},
]


class TestRulesHook(unittest.TestCase):
    """MI1：rules 参数的回调协议与组合语义。"""

    def test_default_rules_applied_when_none(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(_flow(_START_MOCK_END[1:]))  # 缺 start
        self.assertIn("start", str(cm.exception))

    def test_empty_rules_disables_builtin_rules_only(self):
        # rules=[] 关掉全部内置规则，硬编码检查仍在
        validate_flow_ir(_flow(_START_MOCK_END[1:]), rules=[])
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([{"type": "start", "id": "s", "next": "ghost"}]), rules=[]
            )
        self.assertIn("ghost", str(cm.exception))

    def test_custom_rule_receives_node_path_graph(self):
        seen = []

        def rule(node, path, graph):
            seen.append((node.get("id"), path, graph))
            return "boom" if node.get("id") == "a" else None

        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(_flow(_START_MOCK_END), rules=[rule])
        self.assertEqual(cm.exception.path, "nodes[a]")
        ids = [s[0] for s in seen]
        self.assertIn("s", ids)
        self.assertIn("a", ids)
        # graph 上下文携带 ids / host / ancestors
        _, _, graph = seen[0]
        self.assertIsInstance(graph, FlowIRGraph)
        self.assertEqual(graph.ids, {"s", "a", "e"})
        self.assertIsNone(graph.host)
        self.assertEqual(graph.ancestors, ())

    def test_first_error_wins_and_path_is_node_scoped(self):
        def rule(node, path, graph):
            return f"rule hit {node.get('id')}"

        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(_flow(_START_MOCK_END), rules=[rule])
        self.assertEqual(cm.exception.path, "nodes[s]")  # 首个节点即失败

    def test_graph_context_for_childflow(self):
        graphs = []

        def rule(node, path, graph):
            if not graphs or graphs[-1] is not graph:
                graphs.append(graph)
            return None

        validate_flow_ir(
            _flow(
                [
                    {"type": "start", "id": "s", "next": "c"},
                    {
                        "type": "child", "id": "c", "input": {},
                        "childFlow": {"nodes": [
                            {"type": "start", "id": "cs", "next": "ce"},
                            {"type": "end", "id": "ce"},
                        ]},
                        "next": "e",
                    },
                    {"type": "end", "id": "e"},
                ]
            ),
            rules=[rule],
        )
        self.assertEqual(len(graphs), 2)
        root, child = graphs
        self.assertIsNone(root.host)
        self.assertEqual(child.host_type, "child")
        self.assertEqual(child.path, "nodes[c].childFlow")
        self.assertEqual([a.get("id") for a in child.ancestors], ["c"])
        self.assertTrue(child.in_childflow_subtree())
        self.assertFalse(root.in_childflow_subtree())

    def test_build_flow_passes_rules_through(self):
        def rule(node, path, graph):
            return "nope"

        with self.assertRaises(FlowIRValidationError) as cm:
            build_flow(_flow(_START_MOCK_END), rules=[rule])
        self.assertIn("nope", str(cm.exception))


class TestR1StartAndReachability(unittest.TestCase):
    """R1：缺 start / 不可达节点。

    运行期实证：缺 start 在解析/运行期抛 FlowStartMissingError；
    不可达节点运行期静默跳过（run 正常返回、无人执行）。
    """

    def test_missing_start(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(_flow(_START_MOCK_END[1:]))
        self.assertEqual(cm.exception.path, "nodes")
        self.assertIn("start", cm.exception.message)

    def test_unreachable_nodes_reported(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow(_START_MOCK_END + [{"type": "mock", "id": "ghost"}])
            )
        self.assertIn("ghost", cm.exception.message)

    def test_second_start_is_unreachable_dead_node(self):
        # 运行期 start_node 取首个 start，第二个 start 是死节点
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow(_START_MOCK_END + [{"type": "start", "id": "s2"}])
            )
        self.assertIn("s2", cm.exception.message)

    def test_switch_name_as_target_counts_as_edge(self):
        # Switch/Bool 的 branch.name-as-target 回退契约：name 即目标 id
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "sw"},
                {"type": "switch", "id": "sw", "branches": [
                    {"name": "a", "next": "a", "isDefault": True},
                ], "next": None},
                {"type": "mock", "id": "a", "next": "e"},
                {"type": "end", "id": "e"},
            ])
        )

    def test_case_targets_count_as_edges(self):
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "c"},
                {"type": "case", "id": "c", "target": "$INPUT.k",
                 "cases": [{"id": "a", "value": 1}], "default": "e"},
                {"type": "mock", "id": "a", "next": "e"},
                {"type": "end", "id": "e"},
            ])
        )

    def test_childflow_start_missing_anchored_at_child_path(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "mock", "id": "m", "next": "ce"},
                         {"type": "end", "id": "ce"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )
        self.assertIn("childFlow", cm.exception.path)


class TestR2ParallelJoinBranches(unittest.TestCase):
    """R2：joinBranches ⊆ branches[].name。

    运行期实证：``Parallel._split_branches`` 按名字过滤，拼错分支名静默
    降级 fire-and-forget，join 结果无声丢失（实证结果为 ``{}``）。
    """

    def _par(self, join):
        return [
            {"type": "start", "id": "s", "next": "p"},
            {"type": "parallel", "id": "p", "joinBranches": join, "branches": [
                {"name": "main", "flow": {"nodes": [
                    {"type": "start", "id": "cs", "next": "ce"},
                    {"type": "end", "id": "ce"},
                ]}},
            ], "next": "e"},
            {"type": "end", "id": "e"},
        ]

    def test_typo_branch_name_rejected(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(_flow(self._par(["mian"])))
        self.assertIn("mian", cm.exception.message)
        self.assertIn("main", cm.exception.message)  # difflib 近似提示
        self.assertEqual(cm.exception.path, "nodes[p]")

    def test_valid_join_passes(self):
        validate_flow_ir(_flow(self._par(["main"])))

    def test_join_branches_snake_case_alias_checked(self):
        nodes = self._par(["typo"])
        nodes[1]["join_branches"] = nodes[1].pop("joinBranches")
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(_flow(nodes))
        self.assertIn("typo", cm.exception.message)


class TestR3ChildflowReachableEnd(unittest.TestCase):
    """R3：子图必须有从 start 可达的 end。

    运行期实证：子流程尾节点缺 next 且非 End 抛 FlowExecutionException；
    codeflow 前端已保证，sexpr/builder/JSON 不保证。根图不查（运行期根图
    收尾语义另有 graceful 路径，超出本批次范围）。
    """

    def test_childflow_without_end_rejected(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "start", "id": "cs", "next": "m"},
                         {"type": "mock", "id": "m", "output": "x"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )
        self.assertEqual(cm.exception.path, "nodes[c].childFlow")

    def test_parallel_branch_without_end_rejected(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "p"},
                    {"type": "parallel", "id": "p", "branches": [
                        {"name": "b", "flow": {"nodes": [
                            {"type": "start", "id": "bs", "next": "bm"},
                            {"type": "mock", "id": "bm", "output": "x"},
                        ]}},
                    ], "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )
        self.assertIn("branches[0].flow", cm.exception.path)

    def test_unreachable_end_still_fails(self):
        # end 存在但不可达同样不收尾
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "start", "id": "cs", "next": "m"},
                         {"type": "mock", "id": "m", "output": "x"},
                         {"type": "end", "id": "ce"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )
        self.assertIn("childFlow", cm.exception.path)

    def test_root_graph_not_checked(self):
        # 根图无 end 不在本规则范围
        validate_flow_ir(_flow([
            {"type": "start", "id": "s", "next": "m"},
            {"type": "mock", "id": "m", "output": "x"},
        ]))

    def test_child_without_start_defers_to_r1(self):
        # 缺 start 时 R3 让位，不重复报
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "mock", "id": "m", "next": "ce"},
                         {"type": "end", "id": "ce"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )
        self.assertIn("start", cm.exception.message)


class TestR4ForbidNodeTypesInChildflow(unittest.TestCase):
    """R4：规则工厂，默认不启用（部署方约定）。"""

    def test_not_in_default_rules(self):
        # 部署方约定类规则不进默认集；$F 内置表校验在默认集里
        self.assertNotIn(forbid_node_types_in_childflow({"http"}), DEFAULT_RULES)
        self.assertIn(check_expression_functions, DEFAULT_RULES)

    def test_forbidden_type_inside_childflow(self):
        rule = forbid_node_types_in_childflow({"http"})
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "start", "id": "cs", "next": "h"},
                         {"type": "http", "id": "h", "url": "http://x", "next": "ce"},
                         {"type": "end", "id": "ce"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ]),
                rules=[rule],
            )
        self.assertIn("http", cm.exception.message)
        self.assertIn("childFlow", cm.exception.path)

    def test_same_type_at_root_allowed(self):
        rule = forbid_node_types_in_childflow({"http"})
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "h"},
                {"type": "http", "id": "h", "url": "http://x", "next": "e"},
                {"type": "end", "id": "e"},
            ]),
            rules=[rule],
        )

    def test_nested_parallel_branch_inside_childflow_is_in_subtree(self):
        rule = forbid_node_types_in_childflow({"http"})
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "start", "id": "cs", "next": "p"},
                         {"type": "parallel", "id": "p", "branches": [
                             {"name": "b", "flow": {"nodes": [
                                 {"type": "start", "id": "bs", "next": "h"},
                                 {"type": "http", "id": "h", "url": "http://x",
                                  "next": "be"},
                                 {"type": "end", "id": "be"},
                             ]}},
                         ], "next": "ce"},
                         {"type": "end", "id": "ce"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ]),
                rules=[rule],
            )
        self.assertIn("branches", cm.exception.path)

    def test_custom_message(self):
        rule = forbid_node_types_in_childflow({"http"}, message="no http here")
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "start", "id": "cs", "next": "h"},
                         {"type": "http", "id": "h", "url": "http://x", "next": "ce"},
                         {"type": "end", "id": "ce"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ]),
                rules=[rule],
            )
        self.assertEqual(cm.exception.message, "no http here")


class TestR5NodeRefs(unittest.TestCase):
    """R5：$NODE.<id> 引用须存在于本（子）图。

    运行期实证：未知 id 静默求值为 None；子图内引用父图 id 同样为 None
    （$NODE 按执行作用域解析）。
    """

    def test_unknown_ref_with_close_match_hint(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "a"},
                    {"type": "mock", "id": "a", "output": "x", "next": "e"},
                    {"type": "end", "id": "e", "output": "$NODE.ab"},
                ])
            )
        self.assertIn("$NODE.ab", cm.exception.message)
        self.assertIn("'a'", cm.exception.message)  # difflib 近似提示
        self.assertEqual(cm.exception.path, "nodes[e]")

    def test_end_node_output_is_scanned(self):
        with self.assertRaises(FlowIRValidationError):
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "output": "$NODE.ghost"},
                ])
            )

    def test_nested_childflow_refs_checked_against_child_ids(self):
        # 子图内引用子图节点：通过；引用父图节点 id：拦截
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "c"},
                {"type": "child", "id": "c", "input": {},
                 "childFlow": {"nodes": [
                     {"type": "start", "id": "cs", "next": "m"},
                     {"type": "mock", "id": "m", "output": "x", "next": "ce"},
                     {"type": "end", "id": "ce", "output": "$NODE.m"},
                 ]}, "next": "e"},
                {"type": "end", "id": "e"},
            ])
        )
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "c"},
                    {"type": "child", "id": "c", "input": {},
                     "childFlow": {"nodes": [
                         {"type": "start", "id": "cs", "next": "ce"},
                         {"type": "end", "id": "ce", "output": "$NODE.s"},
                     ]}, "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )
        self.assertIn("childFlow", cm.exception.path)
        self.assertIn("$NODE.s", cm.exception.message)

    def test_code_field_not_scanned(self):
        # CODE 节点体是 Python 源码，$NODE./$F. 出现不算引用
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "c"},
                {"type": "code", "id": "c", "language": "python",
                 "code": "def run(input):\n    return $NODE.ghost + $F.nothere(1)\n",
                 "next": "e"},
                {"type": "end", "id": "e"},
            ])
        )

    def test_desc_and_parent_prefixed_refs_not_scanned(self):
        # 文档字段散文引用、$PARENT.$NODE 跨作用域形式、动态 id 段不匹配
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "a"},
                {"type": "mock", "id": "a", "output": "x", "next": "d"},
                {"type": "mock", "id": "d", "desc": "see $NODE.ghost docs", "next": "e"},
                {"type": "end", "id": "e", "output": "$PARENT.$NODE.a"},
            ])
        )
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$NODE.$INPUT.key"},
            ])
        )


class TestR6ExpressionFunctions(unittest.TestCase):
    """R6：$F.<fn> 须已注册。

    运行期实证：未注册函数求值为字符串 'undefined'（静默伪值）。
    """

    def test_unknown_function_rejected(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "output": "$F.no_such_fn(1)"},
                ])
            )
        self.assertIn("no_such_fn", cm.exception.message)
        self.assertEqual(cm.exception.path, "nodes[e]")

    def test_registered_builtin_passes(self):
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$F.upper($NODE.s)"},
            ])
        )

    def test_scoped_registry_via_factory(self):
        rule = unknown_expression_functions(frozenset({"add", "sub"}))
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$F.add(1, 2)"},
            ]),
            rules=[rule],
        )
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "output": "$F.upper('x')"},
                ]),
                rules=[rule],
            )
        self.assertIn("upper", cm.exception.message)

    def test_replace_default_rule_composition(self):
        # scoped registry 替换默认 $F 规则的官方组合姿势
        rules = [
            r for r in DEFAULT_RULES if r is not check_expression_functions
        ]
        rules.append(unknown_expression_functions(frozenset({"add"})))
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$F.add(1, 2)"},
            ]),
            rules=rules,
        )


class TestR7OutputTypeLiteral(unittest.TestCase):
    """R7：output 字面量与声明的 outputType 预检。

    运行期实证：Assignment.execute 对求值结果做 io.match，不符抛
    ValueError；字面量 output 求值结果即字面量本身，可前移。表达式
    output 不检（诚实边界）。
    """

    def test_literal_mismatch_rejected(self):
        with self.assertRaises(FlowIRValidationError) as cm:
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "asg"},
                    {"type": "assignment", "id": "asg", "output": "abc",
                     "outputType": {"dataType": "integer"}, "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )
        self.assertIn("outputType", cm.exception.message)
        self.assertEqual(cm.exception.path, "nodes[asg]")

    def test_literal_match_passes(self):
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "asg"},
                {"type": "assignment", "id": "asg", "output": 42,
                 "outputType": {"dataType": "integer"}, "next": "e"},
                {"type": "end", "id": "e"},
            ])
        )

    def test_expression_output_not_checked(self):
        # 表达式/模板 output 运行期才求值，静态不检（诚实边界）
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "asg"},
                {"type": "assignment", "id": "asg", "output": "$F.add(1, 2)",
                 "outputType": {"dataType": "integer"}, "next": "e"},
                {"type": "end", "id": "e"},
            ])
        )

    def test_no_output_type_no_check(self):
        validate_flow_ir(
            _flow([
                {"type": "start", "id": "s", "next": "asg"},
                {"type": "assignment", "id": "asg", "output": "abc", "next": "e"},
                {"type": "end", "id": "e"},
            ])
        )

    def test_invalid_output_type_defers_to_model_validate(self):
        # outputType 缺 dataType：Property.model_validate 在规则内失败时规则
        # 放行（debug 留痕），由节点 model_validate 报权威错误
        data = _flow([
            {"type": "start", "id": "s", "next": "asg"},
            {"type": "assignment", "id": "asg", "output": "abc",
             "outputType": {"no_data_type": 1}, "next": "e"},
            {"type": "end", "id": "e"},
        ])
        validate_flow_ir(data)  # 规则层放行
        import pydantic
        with self.assertRaises((ValueError, pydantic.ValidationError)):
            from plaita.core.flow import Flow
            Flow.model_validate(data)

    def test_unknown_data_type_matches_never_prechecked_as_mismatch(self):
        # data_type 非法但可解析：io.match 对任何值都 False——规则拦截与
        # 运行期 Assignment.execute 抛 ValueError 行为一致，不算误报
        with self.assertRaises(FlowIRValidationError):
            validate_flow_ir(
                _flow([
                    {"type": "start", "id": "s", "next": "asg"},
                    {"type": "assignment", "id": "asg", "output": "abc",
                     "outputType": {"dataType": "not-a-type"}, "next": "e"},
                    {"type": "end", "id": "e"},
                ])
            )


if __name__ == "__main__":
    unittest.main()
