"""plaita#132 回归：`docs/DOC_CODE_MAP.md` 必须登记用户可见的表达式文档。

`DOC_CODE_MAP.md` 是"改了代码 → 哪些文档必须回看"的索引。缺行意味着
**改 `expression_parser.py` 时用户文档 `guide/expressions.md` 不在评审集里**——
#104（数据文本不再二次求值）与 #130（`$NODE.raw.<id>` 访问器）都改的是
**用户可见的表达式行为**，却只能靠人工记得去改那一页。

本文件把"映射存在 + 触发条件点名三类变化"钉成测试。
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
MAP = REPO / "docs/DOC_CODE_MAP.md"
EXPR_DOC = "docs-site/docs/guide/expressions.md"


@pytest.fixture(scope="module")
def table() -> str:
    return MAP.read_text(encoding="utf-8")


def _row_for(table: str, doc_path: str) -> str:
    for line in table.splitlines():
        if line.startswith("|") and doc_path in line:
            return line
    return ""


class TestExpressionDocIsMapped:
    def test_map_lists_expressions_doc(self, table):
        assert _row_for(table, EXPR_DOC), (
            f"DOC_CODE_MAP.md 缺少 `{EXPR_DOC}` 的映射行——改 expression_parser.py 时"
            "用户文档不会进入评审集（plaita#132）"
        )

    def test_row_names_the_parser_and_root_tables(self, table):
        row = _row_for(table, EXPR_DOC)
        assert "plaita/core/expression_parser.py" in row, (
            "该行必须点名 expression_parser.py（改它就应触发本页同步）"
        )
        assert "plaita/core/context.py" in row, (
            "根表（$NODE/$INPUT/...）定义在 context.py，也应登记"
        )

    @pytest.mark.parametrize("phrase", ["语法", "语义", "措辞"])
    def test_trigger_covers_three_change_classes(self, table, phrase):
        """触发条件必须点名 语法/语义/措辞 三类，而不是笼统的"表达式变更"。"""
        row = _row_for(table, EXPR_DOC)
        assert phrase in row, (
            f"触发条件未点名「{phrase}」类变化；笼统措辞会让"
            "「只改了报错文案」或「只改了求值语义」被漏掉"
        )

    def test_row_mentions_accessor_and_reevaluation_examples(self, table):
        """点名近两次真实触发（#104 的二次求值、#130 的 raw 访问器）作为锚例。"""
        row = _row_for(table, EXPR_DOC)
        assert "raw" in row, "应举例 #130 的 `$NODE.raw.<id>` 访问器"
        assert "二次求值" in row, "应举例 #104 的「数据文本是否被二次求值」语义变化"


class TestMapHeaderStaysHonest:
    def test_last_updated_is_recent(self, table):
        m = re.search(r"最后更新：(\d{4})-(\d{2})-(\d{2})", table)
        assert m, "DOC_CODE_MAP.md 应有「最后更新」行"
        # 只要求存在合法日期且不早于首次引入映射的日期
        assert (int(m.group(1)), int(m.group(2)), int(m.group(3))) >= (2026, 10, 12), (
            "新增映射行后应 bump「最后更新」"
        )

    def test_scope_note_clarifies_it_is_not_exhaustive(self, table):
        """范围说明必须写明"未列出 ≠ 不需要维护"，避免被误读为全量清单。"""
        assert "定向索引" in table, "应说明本表是定向索引而非全量清单"
        assert "guide/" in table, "应提示 guide/ 下仍有未建立代码绑定的页面"
