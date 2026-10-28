"""skill↔compiler 同步 CI（plaita#351 P2-1）。

抽取 flow-coder skill（SKILL.md + references/*.md）的 python 代码块，按声明
编译——编译器行为一变，文档块立即红，skill 不再与编译器脱节：

- 普通 ```python 块中「纯 @flow 源码」（无 import / run / 类定义 / `<占位>` 骨架）
  必须 ``compile_source`` 通过；
- ```python expect-error 块首行 ``# 期望报错: <子串>`` 必须真的以该子串编译失败，
  且测试输出带上文件+块序号，CI 红能定位到具体块。

负向断言的完整矩阵（行号/错误码/修复后运行）在 tests/test_skill_repair_evals.py；
本文件只守「文档里写的代码块 == 编译器真实行为」这一层。
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)

from plaita.dsl.codeflow import compile_source

_SKILL_DIR = Path(_PLAITA_AI) / "plaita_ai" / "skills" / "flow-coder"
_MARKDOWN_FILES = [_SKILL_DIR / "SKILL.md"] + sorted((_SKILL_DIR / "references").glob("*.md"))

_FENCE_RE = re.compile(r"^(`{3,})[ \t]*([^\n]*)\n(.*?)^\1[ \t]*$", re.MULTILINE | re.DOTALL)
_EXPECTED_ERR_RE = re.compile(r"^\s*#\s*期望报错[:：]\s*(.+)$", re.MULTILINE)
# 骨架块里的 <name>/<expr> 等占位符（小写标识符形态；比较运算 `a < b` 带空格不匹配）
_PLACEHOLDER_RE = re.compile(r"<[a-z_]+>")
# 非「纯 @flow 源码」的块：harness 代码（import/构造/运行/类定义）不做编译断言
_NOT_PURE_SOURCE_RE = re.compile(
    r"^\s*(from|import)\s|\.run\(|compile_source\(|flow_from_source\(|^\s*class\s",
    re.MULTILINE,
)


def _is_python_fence(info: str) -> bool:
    tokens = info.split()
    return not tokens or tokens[0].startswith("python")


def _iter_blocks():
    for md_path in _MARKDOWN_FILES:
        text = md_path.read_text(encoding="utf-8")
        for i, match in enumerate(_FENCE_RE.finditer(text)):
            info = (match.group(2) or "").strip()
            body = match.group(3)
            if not _is_python_fence(info):
                continue
            label = f"{md_path.name}#block-{i}"
            if "expect-error" in info:
                expected = _EXPECTED_ERR_RE.search(body)
                assert expected is not None, f"{label}: expect-error 块缺 `# 期望报错:` 行"
                yield label, "error", body, expected.group(1).strip()
            elif "@flow(" in body and not _PLACEHOLDER_RE.search(body) and not _NOT_PURE_SOURCE_RE.search(body):
                yield label, "good", body, None


def _collect(kind):
    blocks = list(_iter_blocks())
    chosen = [b for b in blocks if b[1] == kind]
    # 下限守卫：正则腐烂导致静默零抽取时，测试本身要先红
    assert blocks, "no python blocks extracted — fence regex likely broken"
    assert len(chosen) >= (3 if kind == "error" else 10), (
        f"only {len(chosen)} {kind} blocks extracted — extraction regex may have rotted"
    )
    return chosen


def test_good_examples_compile():
    for label, _kind, body, _expected in _collect("good"):
        try:
            compile_source(body)
        except Exception as exc:  # noqa: BLE001 — 断言本体：文档块必须编译通过
            pytest.fail(f"{label}: documented @flow example fails to compile: {exc}")


def test_expect_error_examples_fail_with_declared_substring():
    for label, _kind, body, expected in _collect("error"):
        try:
            compile_source(body)
        except Exception as exc:  # noqa: BLE001 — 断言本体：必须失败且命中声明子串
            assert expected in str(exc), (
                f"{label}: expected error substring {expected!r}, got: {exc}"
            )
        else:
            pytest.fail(f"{label}: expect-error block compiled OK (compiler no longer rejects it)")


def test_no_stale_compile_time_claims_in_skill_text():
    """P0-1 验收：不再出现「编译期不拦」式误述（跨分支同名赋值等已编译期拒绝）。"""
    for md_path in _MARKDOWN_FILES:
        text = md_path.read_text(encoding="utf-8")
        assert "多数编译期不拦" not in text, md_path.name
        assert "编译器不会替你合并" not in text, md_path.name
