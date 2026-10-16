"""plaita#146：``mock`` 不在 sexpr 节点表里——含 mock 节点的 flow 无法往返。

基线（``1503553``）实测：``_NODE_COMPILERS`` 没有 ``mock``，``_node_to_src`` 也
没有对应分支 ⇒ 写侧穿过所有 ``elif`` 只发出空壳 ``(mock :id m)``（``value`` /
``output`` / ``next`` 静默丢失），读侧再解析直接 ``SyntaxError: 未知节点类型
'mock'``。这是**节点表覆盖缺口**（``#136`` 同族、不同根）：损失发生在"整条类型
都没有"，而不是"类型在、字段丢"。

选定的行为：**把 ``mock`` 补进节点表**（emit + parse，字段往返）。测试语料按
issue 验收条款带上 ``7d0add9`` 的关键字序规则（``:kw value`` 一律排在
``-> next`` 之前）与 ``87e4dd7`` 的 dict 字面量规则（``(dict …)`` 必须经
``_value()`` 编译成真 dict，而不是留成 list）。
"""

import pytest

from plaita.dsl import sexpr as sexpr_mod
from plaita.dsl.ir_validate import FlowIRValidationError, build_flow
from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr, parse_sexpr


def _mock_node(ir, node_id="m"):
    for n in ir["nodes"]:
        if n.get("id") == node_id:
            return n
    raise AssertionError(f"IR 里没有节点 {node_id!r}: {ir['nodes']}")


def _mock_segment(text):
    """平衡括号截出 ``(mock …)`` 片段（``text.index(")")`` 会停在嵌套的 ``(dict``）。"""
    start = text.index("(mock")
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    raise AssertionError(f"括号不平衡: {text[start:]}")


def _mock_ir(value=None, *, include_value=True, output=None, next_id="e",
             end_output="$NODE.m"):
    """一份含 mock 节点的完整 flow IR（``end`` 的字段取读侧产出的规范形状）。

    ``_c_end`` 无条件写 ``resultType``（缺省 ``"success"``）与可选 ``output``，
    故这里的 end 节点按**读侧规范**给值，使 ``compile_sexpr`` 的往返比较是精确的
    整树相等（与 ``test_sexpr.py`` 的 #150 语料同一口径）。
    """
    node = {"type": "mock", "id": "m"}
    if include_value:
        node["value"] = value
    if output is not None:
        node["output"] = output
    if next_id is not None:
        node["next"] = next_id
    end = {"type": "end", "id": "e", "resultType": "success"}
    if end_output is not None:
        end["output"] = end_output
    return {
        "runtime": "python", "flow_id": "f", "version": "1",
        "nodes": [
            {"type": "start", "id": "s", "next": "m"},
            node,
            end,
        ],
    }


# ===========================================================================
# 1. 缺陷本体：base 上无法往返
# ===========================================================================

class TestMockRoundTrip:
    """issue 实测形状：``value`` 是 dict（dryrun pin 的典型载荷）。"""

    def test_dict_value_mock_node_round_trips(self):
        ir = _mock_ir({"a": 1, "b": "$INPUT.x"})
        emitted = flow_to_sexpr(ir)
        back = compile_sexpr(emitted)
        assert back == ir, emitted

    def test_mock_node_is_emitted_with_its_fields_not_a_hollow_shell(self):
        """base 发射 ``(mock :id m)``——这里钉住三个必然会丢的 token。"""
        emitted = flow_to_sexpr(_mock_ir({"a": 1}, output="$NODE.m"))
        segment = _mock_segment(emitted)
        assert ":value (dict :a 1)" in segment
        assert ":output $NODE.m" in segment
        assert segment.endswith("-> e)")

    def test_mock_survives_parse_sexpr_and_still_returns_its_value(self):
        """不只是 IR 往返：建 Flow 之后 mock 仍然"原样返回 value"。"""
        src = '(flow g (start :id s -> m) (mock :id m :value (dict :a 1) -> e)' \
              ' (end :id e :output "$NODE.m"))'
        flow = parse_sexpr(src)
        assert flow.run() == {"a": 1}

    def test_mock_with_only_common_fields_round_trips(self):
        """没有自有字段的 mock（``value`` 缺席 = ``Mock.value`` 默认 None）。"""
        ir = _mock_ir(include_value=False)
        assert compile_sexpr(flow_to_sexpr(ir)) == ir


# ===========================================================================
# 2. 字段矩阵 / #150 语料级往返
# ===========================================================================

class TestMockFieldMatrix:
    """``value`` 的各形状 + 继承自 ``Node`` 的 ``output``。"""

    @pytest.mark.parametrize("value", [
        42, 0, 1.5, True, False, "", "plain", "$INPUT.x",
        [1, 2], [], {}, {"a": 1, "nested": {"b": [1, "$INPUT.x"]}},
    ])
    def test_value_shapes_round_trip(self, value):
        ir = _mock_ir(value)
        emitted = flow_to_sexpr(ir)
        assert compile_sexpr(emitted) == ir, emitted

    def test_falsy_values_are_not_dropped(self):
        """发射用 ``is not None`` 而非真值判断——``0``/``false``/``""`` 是数据。"""
        for value, token in ((0, ":value 0"), (False, ":value false"), ("", ':value ""')):
            emitted = flow_to_sexpr(_mock_ir(value))
            assert token in emitted
            assert compile_sexpr(emitted)[
                "nodes"][1]["value"] == value

    def test_none_value_stays_absent_and_is_semantically_equal(self):
        """``:value nil`` 读回缺席 = ``Mock.value`` 默认 None，二者等价。

        与 ``_flow_level_fields_to_src`` 的"None 一律不发"同一口径。
        """
        ir = _mock_ir(None)
        del ir["nodes"][1]["value"]
        assert compile_sexpr(flow_to_sexpr(ir)) == ir
        # 显式 nil 读侧落成"缺席"，不落成字符串 "None"
        explicit = compile_sexpr(
            '(flow g (start :id s -> m) (mock :id m :value nil -> e) (end :id e))')
        assert "value" not in _mock_node(explicit)

    def test_inherited_output_round_trips(self):
        """dryrun 把被 pin 的节点换成 mock 时**保留原节点的 output**（见
        ``plaita-console/backend/services/dryrun.py``），漏发即往返丢字段。"""
        ir = _mock_ir({"a": 1}, output="$NODE.m")
        assert compile_sexpr(flow_to_sexpr(ir)) == ir


# ===========================================================================
# 3. 语料规则：关键字序（7d0add9）与 dict 字面量（87e4dd7）
# ===========================================================================

class TestCorpusRulesApplyToMock:
    """issue 验收条款点名要把这两条既有规则纳入 mock 语料。"""

    def test_dict_literal_value_compiles_to_a_real_dict(self):
        """``87e4dd7``：不经 ``_value()`` 时 ``(dict :a 1)`` 会留成 list，
        发射侧再按 list 分支丢冒号 ⇒ 往返后不再是 dict。"""
        ir = compile_sexpr(
            '(flow g (start :id s -> m)'
            ' (mock :id m :value (dict :a 1 :b "$INPUT.x") -> e) (end :id e))')
        assert _mock_node(ir)["value"] == {"a": 1, "b": "$INPUT.x"}

    def test_writer_puts_every_keyword_before_the_arrow(self):
        """``7d0add9``：写入侧统一按"关键字先于后继"的形状发射。

        这是**形状约定**而非读侧要求——``_split_args`` 平扫，箭头之后的平铺
        ``:kw value`` 照样能读（见下一个用例的对照）；退化的是**不在关键字槽位上**
        的嵌套 list form（``#136`` 的尾随 ``(on-error …)``）。守卫钉住发射形状。
        """
        emitted = flow_to_sexpr(_mock_ir({"a": 1}, output="$NODE.m"))
        segment = _mock_segment(emitted)
        assert segment.index(":value") < segment.index("->")
        assert segment.index(":output") < segment.index("->")

    def test_reader_still_reads_value_written_before_the_arrow(self):
        """关键字序规则的可读性对照：位置合规时字段确实被读入。"""
        ir = compile_sexpr(
            '(flow g (start :id s -> m) (mock :id m :value (dict :a 1) -> e)'
            ' (end :id e))')
        assert _mock_node(ir)["value"] == {"a": 1}

    def test_flat_keyword_after_the_arrow_is_still_read(self):
        """实测记录：关键字序是**形状约定**，不是读侧要求。

        ``_split_args`` 平扫整个参数表，箭头之后的平铺 ``:kw value``（含嵌套
        ``(dict …)`` 值）照样被读成关键字。把这条钉住，免得后人把"关键字必须在
        ``->`` 之前"误当成读侧语义（``_common_kw_to_src`` 里有同款实测更正）。
        """
        for src, expected in (
            ('(flow g (start :id s -> m) (mock :id m -> e :value 42)'
             ' (end :id e))', 42),
            ('(flow g (start :id s -> m) (mock :id m -> e :value (dict :a 1))'
             ' (end :id e))', {"a": 1}),
        ):
            assert _mock_node(compile_sexpr(src))["value"] == expected


# ===========================================================================
# 4. 负向对照：不收编未知类型，也不静默吞位置参数
# ===========================================================================

class TestNegativeControls:

    def test_unknown_node_type_is_still_rejected(self):
        """补 ``mock`` 不等于放宽节点表——别的未知类型照旧具名报错。"""
        with pytest.raises(SyntaxError, match="未知节点类型 'mockish'"):
            compile_sexpr('(flow g (start :id s -> m) (mockish :id m) (end :id e))')

    def test_extra_positional_value_is_a_named_error_not_silently_dropped(self):
        """位置写法等价于 ``:value``；多余位置参数必须响亮报错
        （同 ``_c_assignment``：静默丢弃曾造成错编译）。"""
        with pytest.raises(SyntaxError, match="mock 最多接受一个位置值"):
            compile_sexpr('(flow g (start :id s -> m) (mock :id m 1 2 -> e)'
                          ' (end :id e))')

    def test_positional_value_is_an_alias_of_keyword_value(self):
        positional = compile_sexpr(
            '(flow g (start :id s -> m) (mock :id m 42 -> e) (end :id e))')
        keyword = compile_sexpr(
            '(flow g (start :id s -> m) (mock :id m :value 42 -> e) (end :id e))')
        assert positional == keyword

    def test_base_hollow_emission_would_have_lost_the_fields(self):
        """自证有区分力：把 base 的空壳 ``(mock :id m)`` 再解析一次，它确实
        不含 value/next——本单的往返断言在 base 上必然红。"""
        hollow = compile_sexpr(
            '(flow g (start :id s -> m) (mock :id m -> e) (end :id e))')
        assert "value" not in _mock_node(hollow)
        # 而"带 value 的 IR"发射出来**不是**那个空壳
        assert flow_to_sexpr(_mock_ir(1)) != flow_to_sexpr(hollow)

    def test_validation_still_runs_for_mock_nodes(self):
        """mock 进了节点表≠绕开共享校验：``:value`` 里的悬空 ``$NODE`` 照旧被拒
        （顺带钉住 mock 的 ``value`` 与其它节点字段一样进 R5 引用扫描）。"""
        with pytest.raises(FlowIRValidationError) as ei:
            parse_sexpr('(flow g (start :id s -> m)'
                        ' (mock :id m :value "$NODE.ghost" -> e)'
                        ' (end :id e :output "$NODE.m"))')
        assert "$NODE.ghost" in str(ei.value)

    def test_dangling_next_on_mock_is_still_rejected(self):
        with pytest.raises(FlowIRValidationError):
            parse_sexpr('(flow g (start :id s -> m)'
                        ' (mock :id m :value 1 -> nowhere) (end :id e))')


# ===========================================================================
# 5. 结构守卫：节点表两侧对称（#150 的结构守卫也覆盖 mock）
# ===========================================================================

class TestNodeTableSymmetry:

    def test_mock_is_in_the_reader_table(self):
        assert "mock" in sexpr_mod._NODE_COMPILERS

    def test_mock_compiles_and_emits_with_the_same_type_name(self):
        ir = compile_sexpr(
            '(flow g (start :id s -> m) (mock :id m :value 1 -> e) (end :id e))')
        assert _mock_node(ir)["type"] == "mock"
        assert "(mock " in flow_to_sexpr(ir)

    def test_mock_ir_is_accepted_by_the_flow_model(self):
        """IR 层之外：``Mock`` 是真实节点类型，``build_flow`` 接受该 IR。"""
        flow = build_flow(_mock_ir({"a": 1}))
        assert flow.run() == {"a": 1}
