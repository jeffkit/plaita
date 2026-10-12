"""plaita#104 方案 A 的白盒单测：根键扫描 + 父容器根键探针 + 路径渲染。

行为级验收（`$NODE.<id>.<field>` 数据文本原样返回、归因 KeyError）见
``tests/unit/test_expression_parser_path_recursion.py``；本文件直接锁三个新私有
helper 的判据与分支——尤其是非 dict 容器（``__contains__`` / ``get`` /
``__dict__``）与"哪些 ``$`` token 不算根引用"，这些分支在行为测试里不易走到，
而 ``mutate_only_covered_lines`` 下未覆盖的分支不会产生变异点（分数会虚高）。
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

from plaita.core.expression_parser import (
    ExpressionParser,
    _container_has_root,
    _render_path,
    _scan_root_refs,
)


class TestScanRootRefs:
    """只认六个文档化根键；字面量/函数命名空间/段名位置一律不算。"""

    def test_empty_text(self):
        assert _scan_root_refs("$", "") == frozenset()

    def test_all_documented_roots(self):
        text = "{% $INPUT.a %}{% $NODE.b %}{% $GLOBAL.c %}{% $PARENT.d %}{% $ENV.e %}"
        assert _scan_root_refs("$", text) == frozenset(
            {"$INPUT", "$NODE", "$GLOBAL", "$PARENT", "$ENV"}
        )

    def test_flow_alias_is_flow_id(self):
        assert _scan_root_refs("$", "{% $FLOW %}") == frozenset({"$FLOW_ID"})
        assert _scan_root_refs("$", "{% $FLOW_ID %}") == frozenset({"$FLOW_ID"})

    def test_function_namespace_and_unknown_roots_are_not_roots(self):
        assert _scan_root_refs("$", "{% $F.eq(1,1) %}") == frozenset()
        assert _scan_root_refs("$", "$FOO.bar") == frozenset()
        # 最大吞并：``$INPUT-x`` 是未知根 ``$INPUT-x``，不是 ``$INPUT``
        assert _scan_root_refs("$", "$INPUT-x") == frozenset()
        # 同理 ``$INPUTlower`` 是未知根；不能只看前缀就当 ``$INPUT``（大小写敏感）
        assert _scan_root_refs("$", "$INPUTlower") == frozenset()

    def test_bare_dollar_is_skipped(self):
        assert _scan_root_refs("$", "$") == frozenset()
        assert _scan_root_refs("$", "$.not_exist") == frozenset()
        # 裸 ``$`` 只跳过自身（continue 而非 break）：后面的根引用仍要被发现
        assert _scan_root_refs("$", "$.a $INPUT.b") == frozenset({"$INPUT"})

    def test_quoted_literals_are_data(self):
        assert _scan_root_refs("$", "$F.concat('$INPUT', \"$NODE\")") == frozenset()
        assert _scan_root_refs("$", "$F.concat('$INPUT', $NODE.x)") == frozenset({"$NODE"})

    def test_dollar_in_field_position_is_not_a_root(self):
        assert _scan_root_refs("$", "$PARENT.$INPUT.name") == frozenset({"$PARENT"})
        # 段名位置的 ``$`` 只跳过自身：后面的根引用仍要被发现
        assert _scan_root_refs("$", "$PARENT.$INPUT $NODE.x") == frozenset(
            {"$PARENT", "$NODE"}
        )

    def test_field_position_at_string_boundaries(self):
        # 首个 token 就在文本下标 0 / 1 时，"." 检查既不越界也不误判
        assert _scan_root_refs("$", "$INPUT.") == frozenset({"$INPUT"})
        assert _scan_root_refs("$", ".$INPUT") == frozenset()

    def test_custom_prefix(self):
        assert _scan_root_refs("#", "#INPUT.x") == frozenset({"#INPUT"})
        assert _scan_root_refs("#", "$INPUT.x") == frozenset()


class TestContainerHasRoot:
    """父容器探针：与嵌套求值实际使用的 ``obj[root_key]`` 语义一致。"""

    def test_dict(self):
        assert _container_has_root({"$INPUT": 1}, "$INPUT") is True
        assert _container_has_root({"$INPUT": 1}, "$NODE") is False

    def test_dict_like_contains(self):
        class _Contains:
            def __contains__(self, key):
                return key == "$GLOBAL"

        assert _container_has_root(_Contains(), "$GLOBAL") is True
        assert _container_has_root(_Contains(), "$INPUT") is False

    def test_get_fallback_when_contains_explodes(self, caplog):
        class _HostileContains:
            def __contains__(self, key):
                raise TypeError("probe not supported")

            def get(self, key, default=None):
                return "hit" if key == "$NODE" else default

        with caplog.at_level(logging.DEBUG, logger="plaita"):
            assert _container_has_root(_HostileContains(), "$NODE") is True
            assert _container_has_root(_HostileContains(), "$INPUT") is False
        # 探针异常必须留痕（逐条精确比对：既锁可见性，也锁参数不被吞掉/改写）
        messages = [record.getMessage() for record in caplog.records]
        assert (
            "contains probe failed for _HostileContains root '$NODE': probe not supported"
            in messages
        )

    def test_get_probe_failure_falls_back_to_attributes(self, caplog):
        class _BadGet:
            def get(self, key, default=None):
                raise RuntimeError("storage down")

        with caplog.at_level(logging.DEBUG, logger="plaita"):
            assert _container_has_root(_BadGet(), "$INPUT") is False
        messages = [record.getMessage() for record in caplog.records]
        assert "get probe failed for _BadGet root '$INPUT': storage down" in messages

    def test_attribute_style_container(self):
        obj = SimpleNamespace(**{"$NODE": {"a": 1}})
        assert _container_has_root(obj, "$NODE") is True
        assert _container_has_root(obj, "$INPUT") is False

    def test_opaque_object_has_no_roots(self):
        assert _container_has_root(object(), "$INPUT") is False
        assert _container_has_root(None, "$INPUT") is False


class TestRenderPath:
    def test_field_and_index_segments(self):
        walked = [("field", "llm", "llm"), ("index", 0, "0"), ("field", "text", "text")]
        assert _render_path("$NODE", walked) == "$NODE.llm[0].text"

    def test_root_only(self):
        assert _render_path("$NODE", []) == "$NODE"


def test_root_scan_cache_is_prefix_aware():
    """同一 string 在不同 prefix 下不得串扰（缓存键含 prefix）。"""
    parser = ExpressionParser.for_prefix("$")
    assert _scan_root_refs("$", "{% $INPUT.x %}") == frozenset({"$INPUT"})
    assert _scan_root_refs("#", "{% $INPUT.x %}") == frozenset()
    # parser.prefix 是 thunk 里实际用于扫描的 prefix
    assert parser.prefix == "$"
