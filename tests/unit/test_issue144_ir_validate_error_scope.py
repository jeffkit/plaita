"""plaita#144：``ir_validate`` 的 ``error`` 免扫描口径必须收窄到**节点级**。

基线（``87e4dd7``）把 ``"error"`` 放进 ``ir_validate._UNSCANNED_KEYS``，而
``_iter_strings`` 是"任意深度上精确键名匹配"——于是**任何**恰好叫 ``error`` 的
普通字段下的 ``$NODE``/``$F`` 悬空引用都被放行。免扫的理由只对 ``end`` 节点的
失败载荷成立（``End.execute`` 直接 ``self.error.get(...)``，从不求值），爆炸半径
远大于理由。

本文件的断言分三组：

1. **免扫按理由生效**（必须继续免扫，不许误报）：节点级 ``error`` 载荷里的
   ``$NODE``/``$F`` 文本，无论嵌套多深，都不是引用；
2. **免扫不再越界**（本单修的缺陷）：嵌套层同名字段里的悬空引用必须被拒；
3. **负向对照**：与第 2 组同形、只把键名换掉的悬空引用同样被拒——证明第 2 组
   拒的是"悬空引用"而不是"带 error 键的字段"本身；且**合法**引用放在嵌套
   ``error`` 下不产生误报。
"""

import pytest

from plaita.dsl import ir_validate as iv
from plaita.dsl.ir_validate import FlowIRValidationError, validate_flow_ir


def _ir(nodes):
    return {"runtime": "python", "flow_id": "f", "nodes": nodes}


def _assignment(**extra):
    node = {"type": "assignment", "id": "a", "output": "1", "next": "e"}
    node.update(extra)
    return node


def _validated(nodes):
    validate_flow_ir(_ir(nodes), recursive=True)


def _rejected(nodes):
    with pytest.raises(FlowIRValidationError) as ei:
        _validated(nodes)
    return str(ei.value)


#: ``start -> a -> e`` 的骨架，节点 ``a`` 上挂待测字段。
def _graph(**extra):
    return [
        {"type": "start", "id": "s", "next": "a"},
        _assignment(**extra),
        {"type": "end", "id": "e"},
    ]


# ===========================================================================
# 1. 节点级 error 免扫描：本意，必须继续免扫（边界/防误报）
# ===========================================================================

class TestNodeLevelErrorStaysExempt:
    """节点自己的 ``error`` 字段是纯字面数据——照旧免扫。"""

    def test_end_error_payload_with_dangling_node_ref_is_accepted(self):
        """载荷引用 end 自身 id：emit→compile 后 id 会被语义 slug 重命名，
        这正是 ``5e376fd`` 当初加免扫要挡的误报。"""
        _validated([
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "error": {"message": "$NODE.ghost"}},
        ])

    def test_end_error_payload_with_dangling_function_ref_is_accepted(self):
        _validated([
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e",
             "error": {"code": "$F.nosuchfn(1)", "message": "$F.other(2)"}},
        ])

    def test_whole_node_level_error_subtree_is_skipped(self):
        """免扫是**整棵子树**跳过——节点级 error 内部再嵌同名字典也不扫。"""
        _validated([
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e",
             "error": {"a": {"b": [{"c": "$NODE.ghost"}]}}},
        ])


# ===========================================================================
# 2. 嵌套层同名字段：缺陷本体，必须被拒
# ===========================================================================

class TestNestedErrorKeyIsScanned:
    """任意深度上恰好叫 ``error`` 的普通字段不是失败载荷——里面的引用照扫。"""

    def test_error_key_nested_three_levels_deep_is_rejected(self):
        """issue 正文实测的第一条：``error`` 嵌套 3 层仍被免扫。"""
        msg = _rejected(_graph(output={"lvl1": {"lvl2": {"error": {"m": "$NODE.ghost"}}}}))
        assert "$NODE.ghost" in msg

    def test_error_key_inside_list_element_is_rejected(self):
        """issue 正文实测的第二条：list 元素里的 ``error`` 键。"""
        msg = _rejected(_graph(output=[{"error": {"m": "$NODE.ghost"}}]))
        assert "$NODE.ghost" in msg

    def test_error_key_under_a_plain_sibling_field_is_rejected(self):
        """不经 ``output``：挂在任意普通字段上的 ``error`` 同样不再免扫。"""
        msg = _rejected(_graph(body={"payload": {"error": {"m": "$NODE.ghost"}}}))
        assert "$NODE.ghost" in msg

    def test_nested_error_dangling_function_ref_is_rejected(self):
        """$F 走同一条免扫通道，一并收窄。"""
        msg = _rejected(_graph(output={"error": {"m": "$F.nosuchfn(1)"}}))
        assert "$F.nosuchfn" in msg

    def test_error_key_on_a_node_inside_childflow_is_rejected(self):
        """issue 正文实测的第三条：子流程节点上的嵌套 ``error``。

        子图按自己的 ``graph.ids`` 递归校验，父节点的免扫集合不得顺带豁免子图。
        """
        child = {
            "runtime": "python", "flow_id": "cf",
            "nodes": [
                {"type": "start", "id": "cs", "next": "ca"},
                {"type": "assignment", "id": "ca",
                 "output": {"error": {"m": "$NODE.ghost"}}, "next": "ce"},
                {"type": "end", "id": "ce"},
            ],
        }
        nodes = [
            {"type": "start", "id": "s", "next": "c"},
            {"type": "child", "id": "c", "input": "$INPUT", "next": "e",
             "childFlow": child},
            {"type": "end", "id": "e"},
        ]
        msg = _rejected(nodes)
        assert "$NODE.ghost" in msg


# ===========================================================================
# 3. 负向对照 + 防误报
# ===========================================================================

class TestNegativeControls:
    """对照组：证明第 2 组拒的是"悬空引用"，不是"带 error 键的字段"。"""

    def test_same_shape_without_error_key_is_rejected(self):
        """与第 2 组逐字同形，只把键名 ``error`` 换成 ``note`` —— 同样被拒。

        base 与修复后都必须拒；它钉住"悬空引用本身"这条基线，使第 2 组具有
        区分力（若第 2 组的拒绝来自别的规则，这里会一起红/一起绿）。
        """
        msg = _rejected(_graph(output={"lvl1": {"lvl2": {"note": {"m": "$NODE.ghost"}}}}
                               ))
        assert "$NODE.ghost" in msg

    def test_dangling_ref_on_plain_output_is_still_rejected(self):
        """#144 之前就被正确拒绝的案例，不许被本次改动放松。"""
        msg = _rejected(_graph(output="$NODE.ghost"))
        assert "$NODE.ghost" in msg

    def test_known_ref_under_nested_error_key_is_not_a_false_positive(self):
        """嵌套 ``error`` 下引用**本图真实存在**的节点——必须放行。

        收窄免扫的风险是"一刀切拒绝带 error 的字段"；这条钉住我只拒悬空引用。
        """
        _validated(_graph(output={"error": {"m": "$NODE.a", "n": "$NODE.s"}}))

    def test_error_handler_fields_are_still_scanned(self):
        """``errorHandler`` / ``timeoutHandler`` 的求值字段照旧被扫（回归护栏）。"""
        msg = _rejected(_graph(errorHandler={"defaultValue": "$NODE.ghost"}))
        assert "$NODE.ghost" in msg
        msg = _rejected(_graph(errorHandler={"errorMessage": "$NODE.ghost"}))
        assert "$NODE.ghost" in msg
        msg = _rejected(_graph(timeoutHandler={"defaultValue": "$NODE.ghost"}))
        assert "$NODE.ghost" in msg


# ===========================================================================
# 4. 结构守卫：两级免扫集合的拆分本身被钉住
# ===========================================================================

class TestUnscannedKeyTables:
    """免扫口径是"节点级 / 嵌套级"两张表——防止将来又被并回一张。"""

    def test_error_is_node_level_only(self):
        assert "error" in iv._UNSCANNED_KEYS
        assert "error" in iv._UNSCANNED_NODE_ONLY_KEYS
        assert "error" not in iv._UNSCANNED_NESTED_KEYS

    def test_nested_table_is_the_node_table_minus_the_node_only_keys(self):
        assert iv._UNSCANNED_NESTED_KEYS == (
            iv._UNSCANNED_KEYS - iv._UNSCANNED_NODE_ONLY_KEYS)
        # 只有 error 是"仅节点级"；别把别的键顺手挪进这张表
        assert iv._UNSCANNED_NODE_ONLY_KEYS == frozenset({"error"})

    def test_documented_unscanned_keys_survive_the_split(self):
        """base 已登记的其余免扫键（code/childFlow/desc…）仍免扫。"""
        for key in ("code", "childFlow", "child_flow", "flow", "desc", "name"):
            assert key in iv._UNSCANNED_NESTED_KEYS
