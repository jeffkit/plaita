"""文档漂移守卫：`docs/` 里 `from plaita.event import ...` 的名字必须真实存在。

背景（2026-10-14，plaita#153/r6 复审）：`docs/event-USER_GUIDE.md` 长期教用户

* ``from plaita.event import FlowEventHandler``
* ``from plaita.event import MixedEventBus``

而 `plaita.event` **从来没有**这两个名字——`FlowEventHandler` 甚至是
`plaita/event/__init__.py` 的 ``__all__`` 里挂了不知多久的空名（`from plaita.event
import *` 必炸 AttributeError），直到 r6 才摘掉。也就是说：文档教了一个必然
`ImportError` 的 API，而没有任何测试拦得住。

本模块把「文档里出现的公开名」钉在实现上：任何臆造/已删除的导出都会立刻变红。
只检查**存在性**（`hasattr`），不检查签名——参数名/关键字漂移不在本守卫射程内。
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import plaita.event

ROOT = Path(__file__).resolve().parents[2]

#: 纳入守卫的文档。`docs/event-USER_GUIDE.md` 是 #153/r6 出事的那份；
#: `docs/event-ARCHITECTURE.md` 抄了同一批 MixedEventBus 示例，一并纳入。
DOC_FILES = (
    ROOT / "docs" / "event-USER_GUIDE.md",
    ROOT / "docs" / "event-ARCHITECTURE.md",
)

#: `from plaita.event import a, b` / 括号换行形式（`#` 之后是注释，切掉）
_IMPORT_RE = re.compile(
    r"^[ \t]*from[ \t]+plaita\.event[ \t]+import[ \t]+(?P<body>\([^)]*\)|[^\n#]+)",
    re.MULTILINE,
)

#: `plaita.event.<name>` 属性引用
_ATTR_RE = re.compile(r"\bplaita\.event\.([A-Za-z_]\w*)")


def _doc_names(text: str) -> set[str]:
    """抽出文档里引用的 `plaita.event` 公开名（import 形式 + 属性形式）。"""
    names: set[str] = set()
    for match in _IMPORT_RE.finditer(text):
        body = match.group("body").strip()
        if body.startswith("("):
            body = body[1:-1]
        for raw in body.replace("\n", " ").split(","):
            name = raw.strip()
            # `import *` / 空片段 / 非标识符（例如带括号的残留）一律不算名字
            if name and name.isidentifier():
                names.add(name)
    names.update(_ATTR_RE.findall(text))
    return names


def _conditional_names() -> dict[str, str]:
    """`plaita/event/__init__.py` 里只有装了 optional extra 才挂上的名字 → 其 flag。

    真相来源是**实现本身**，不是另立一张会漂移的白名单：`__init__.py` 在
    ``if HAS_REDIS:`` / ``if HAS_SQLALCHEMY:`` 分支里 ``__all__.extend([...])``
    的名字，只有对应 flag 为真时才出现在包上。缺 extra 时这些名字缺席是**合法**的，
    不算文档漂移；本次解析只在 flag 当前为假时启用豁免（本机 redis/sqlalchemy
    都在，豁免集为空 ⇒ 断言是全严格的）。
    """
    source = (ROOT / "plaita" / "event" / "__init__.py").read_text(encoding="utf-8")
    exempt: dict[str, str] = {}
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.If):
            continue
        missing_flag = next(
            (t.id for t in ast.walk(node.test)
             if isinstance(t, ast.Name) and t.id.startswith("HAS_")
             and not getattr(plaita.event, t.id, False)),
            None,
        )
        if missing_flag is None:
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                exempt.setdefault(sub.value, missing_flag)
    return exempt


def _missing_names(text: str) -> list[str]:
    """文档里引用了、但 `plaita.event` 上不存在的名字（排序后返回）。"""
    exempt = _conditional_names()
    return sorted(
        name for name in _doc_names(text)
        if not hasattr(plaita.event, name) and name not in exempt
    )


class TestDocsReferenceOnlyRealPlaitaEventNames:
    def test_no_doc_teaches_a_name_that_does_not_exist(self):
        """`docs/` 里每个 `plaita.event` 公开名都必须真的存在。"""
        offenders = {
            doc.name: _missing_names(doc.read_text(encoding="utf-8"))
            for doc in DOC_FILES
        }
        offenders = {name: missing for name, missing in offenders.items() if missing}
        assert offenders == {}, (
            "文档引用了 plaita.event 上不存在的名字（臆造 API 或已删的导出）: "
            + "; ".join(f"{doc}: {missing}" for doc, missing in sorted(offenders.items()))
        )

    def test_extractor_is_not_vacuous_and_can_flag_a_fake_name(self):
        """抽取器自证：真的抽到了文档名，且对假名字有区分力（非空转）。"""
        names = _doc_names(DOC_FILES[0].read_text(encoding="utf-8"))
        assert "InMemoryEventBus" in names, f"抽取器没抽到文档里的名字: {sorted(names)}"
        assert "_doc_names" not in names

        fake = "__no_such_plaita_event_name__"
        assert not hasattr(plaita.event, fake)
        assert _doc_names(f"from plaita.event import {fake}") == {fake}
        assert _missing_names(f"from plaita.event import {fake}") == [fake]
        # 真名字不会被误判
        assert _missing_names("from plaita.event import InMemoryEventBus") == []
