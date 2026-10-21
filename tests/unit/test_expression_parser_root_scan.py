"""`_container_has_root` / `_render_path` 的白盒单测。

``_container_has_root`` 在 #268（摘除运行期字符串二次求值）后仅服务
`$NODE.raw` 修饰符的碰撞检查——"真有节点叫 raw"时修饰符不生效。原
``_scan_root_refs`` 根扫描 helper 已随嵌套求值一并移除，其行为级测试由
``test_expression_parser_path_recursion.py`` 的数据契约锚取代。
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

from plaita.core.expression_parser import (
    ExpressionParser,
    _container_has_root,
    _render_path,
)


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
