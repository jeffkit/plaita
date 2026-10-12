"""plaita#127 回归：sexpr 往返不得静默丢 `event` 的字段。

`plaita/dsl/sexpr.py` 的 `_node_to_src`（`flow_to_sexpr` 用的节点发射器）原本在
`elif t == "event"` 分支里**只发 `:type`**，而解析侧（`_c_event` → `_common_fields`
+ 对 `:filter` 的处理）**是读这些字段的**。属单边缺失：**解析支持、发射不支持**
⇒ `sexpr → IR → sexpr` 无告警、无异常地丢数据。

本文件钉住两处同根缺口（`next` 是 issue 报的，`eventFilter` 是我在同一次核查中发现的）：

* 丢 `next` ⇒ 运行期 `Node 'ev' (event) has no 'next'`，或配 `errorHandler` 后
  退化成"告警并提前结束"；
* 丢 `eventFilter` ⇒ **更隐蔽**：事件仍会触发，只是不再按条件过滤（不该触发的也触发）。
"""

import pytest

from plaita.dsl.sexpr import compile_sexpr, flow_to_sexpr

_EVENT_NEXT_SRC = (
    "(flow ev (start :id s -> ev) "
    "(event :id ev :type approval -> done) (end :id done))"
)


def _node(ir, node_id):
    for n in ir["nodes"]:
        if n.get("id") == node_id:
            return n
    raise AssertionError(f"IR 里没有节点 {node_id}：{ir['nodes']}")


def _roundtrip(src):
    """sexpr → IR → sexpr → IR，返回 (首次 IR, 往返后 IR, 中间 sexpr 文本)。"""
    ir = compile_sexpr(src)
    text = flow_to_sexpr(ir)
    back = compile_sexpr(text)
    return ir, back, text


class TestEventNextSurvivesRoundTrip:
    """issue 报的主缺口。"""

    def test_event_emits_successor(self):
        ir = compile_sexpr(_EVENT_NEXT_SRC)
        text = flow_to_sexpr(ir)
        assert "-> done" in text, f"event 分支必须发射后继：{text}"

    def test_event_next_survives(self):
        ir, back, text = _roundtrip(_EVENT_NEXT_SRC)
        assert _node(ir, "ev")["next"] == "done"
        assert _node(back, "ev")["next"] == "done", (
            f"往返后 event.next 被丢失：{_node(back, 'ev')}（中间文本 {text}）"
        )

    def test_emitter_is_idempotent(self):
        """再走一遍不得继续漂移。"""
        ir, back, text = _roundtrip(_EVENT_NEXT_SRC)
        assert flow_to_sexpr(back) == text

    def test_event_without_next_is_unchanged(self):
        """没有 next 的 event 不得凭空多出后继。"""
        text = flow_to_sexpr(compile_sexpr("(flow f (start :id s) (event :id ev :type approval))"))
        assert "->" not in text, f"缺席的 next 不应被编造：{text}"


class TestEventFilterSurvivesRoundTrip:
    """同根第二处：`eventFilter` 原本也只在解析侧存在。"""

    SRC = ('(flow f (start :id s -> ev) '
           '(event :id ev :type approval :filter "$INPUT.x" -> e) (end :id e))')

    def test_event_emits_filter(self):
        text = flow_to_sexpr(compile_sexpr(self.SRC))
        assert ":filter" in text, f"event 的过滤条件必须发射：{text}"

    def test_event_filter_survives(self):
        ir, back, text = _roundtrip(self.SRC)
        assert _node(ir, "ev")["eventFilter"] == "$INPUT.x"
        assert _node(back, "ev").get("eventFilter") == "$INPUT.x", (
            f"往返后 eventFilter 被丢失：{_node(back, 'ev')}（中间文本 {text}）"
        )

    def test_both_fields_together(self):
        ir, back, text = _roundtrip(self.SRC)
        assert _node(ir, "ev") == _node(back, "ev"), f"往返不保真：{text}"


class TestFullNodeEquality:
    """整节点逐字段相等——比只比对个别键更能抓住未来新增字段的同类遗漏。"""

    @pytest.mark.parametrize("src,node_id", [
        (_EVENT_NEXT_SRC, "ev"),
        ('(flow f (start :id s -> ev) (event :id ev :type approval :filter "$INPUT.x" -> e) (end :id e))', "ev"),
        # 对照：非 event 类型的 next 本来就幸存
        ('(flow h (start :id s -> h1) '
         '(http :id h1 :method GET :url "http://x" -> e) (end :id e))', "h1"),
    ])
    def test_node_roundtrips_identically(self, src, node_id):
        ir, back, text = _roundtrip(src)
        assert _node(ir, node_id) == _node(back, node_id), (
            f"节点 {node_id} 往返不保真\n  前: {_node(ir, node_id)}\n  后: {_node(back, node_id)}\n  文本: {text}"
        )


class TestPreExistingCodeQuirkIsSeparate:
    """记录一个**既有**、与本单同族但不同点的现象：`code` 的 `:code` 往返会被规整化。

    `:code "1"` 解析为字符串 `'1'`，发射时变成裸 `1`，往返后成了 int `1`。
    **基线（`cf31307` 之前）行为完全相同**，故非 #127 引入，也不在本单范围内
    （本单只修 `event` 分支的字段缺失）。钉住它，免得日后被误判成 #127 的回归。
    """

    SRC = '(flow c (start :id s -> code1) (code :id code1 :lang python :code "1" -> e) (end :id e))'

    def test_code_value_normalization_is_pinned(self):
        ir = compile_sexpr(self.SRC)
        assert _node(ir, "code1")["code"] == "1"          # 解析得到字符串
        back = compile_sexpr(flow_to_sexpr(ir))
        assert _node(back, "code1")["code"] == 1          # 往返后规整化为 int（既有行为）

    def test_code_next_itself_survives(self):
        """本单关心的那一半：`code` 的 next 是幸存的（issue 里给的 control）。"""
        ir, back, _ = _roundtrip(self.SRC)
        assert _node(back, "code1")["next"] == "e"


class TestKeywordOrderingBeforeNext:
    """`-> next` **终止关键字列表**，所以所有关键字必须排在它之前。

    plaita#136 的核查中发现（交付方指出，我复现确认）：我最初的 #127 修复把
    `:filter` 追加在 `-> next` **之后**，于是它退化成**位置参数**。
    字符串 filter 侥幸幸存，但**关键字语义已坏**——这也是 `#136` 里
    `errorHandler` 被 `output` 吞掉（`output = ['on-error','abort','retry',2]`）的同一机制。

    本测试钉住发射顺序：`:filter` **必须**出现在 `-> next` 之前。
    """

    def test_filter_emitted_before_arrow(self):
        ir = compile_sexpr(
            '(flow f (start :id s -> ev) '
            '(event :id ev :type approval :filter $INPUT.x -> e) (end :id e))'
        )
        text = flow_to_sexpr(ir)
        # 只在 **event 子串**内比较顺序——整串里第一个 "-> e" 属于 start 节点。
        ev_part = text[text.index("(event"):]
        ev_part = ev_part[: ev_part.index(")") + 1]
        assert ":filter" in ev_part and "-> e" in ev_part, f"event 子串异常：{ev_part}"
        assert ev_part.index(":filter") < ev_part.index("-> e"), (
            f"`:filter` 必须排在 `-> next` 之前（后者终止关键字列表）：{ev_part}"
        )

    def test_no_keyword_after_arrow_for_any_node_type(self):
        """通用护栏：任何节点的 `-> x` 之后都不得再出现 `:keyword`。"""
        import re

        srcs = [
            '(flow f (start :id s -> ev) (event :id ev :type approval :filter $INPUT.x -> e) (end :id e))',
            '(flow f (start :id s -> h) (http :id h :method GET :url "http://x" -> e) (end :id e))',
            '(flow f (start :id s -> a) (assign :id a "1" -> e) (end :id e))',
        ]
        for src in srcs:
            text = flow_to_sexpr(compile_sexpr(src))
            # 逐节点检查：取每个 "(type " 起的括号段，段内 `-> x` 之后不得再有 `:kw`
            for seg in re.findall(r"\((?:start|end|event|http|assignment|code)\b[^()]*\)", text):
                m = re.search(r"-> \S+", seg)
                if not m:
                    continue
                after = seg[m.end():]
                assert not after.lstrip().startswith(":"), (
                    f"`->` 之后出现了关键字（会被当位置参数吞掉）：{seg}"
                )

    def test_next_still_survives_with_filter_present(self):
        """顺序修正后，next 与 filter 必须**同时**保真。"""
        ir = compile_sexpr(
            '(flow f (start :id s -> ev) '
            '(event :id ev :type approval :filter $INPUT.x -> e) (end :id e))'
        )
        back = compile_sexpr(flow_to_sexpr(ir))
        ev = [n for n in back["nodes"] if n.get("id") == "ev"][0]
        assert ev["next"] == "e"
        assert ev["eventFilter"] == "$INPUT.x"


class TestEventFilterValuePreserved:
    """plaita#136：`:filter` 的**值**也必须保真（不只是位置）。

    交付方在 `#136` 的复测里指出、我独立定位到**两个叠加缺陷**：

    1. `_c_event` 存 `:filter` 时**没过 `_value()`** ⇒ `(dict :a 1)` 原样留成列表
       `['dict','a',1]` ⇒ 发射走 list 分支（**丢冒号**）⇒ 再解析仍是 list；
    2. `EventNode.event_filter` **漏了 camelCase 别名**（紧邻的 `event_type` /
       `subscription_timeout` 都有 `AliasChoices`）⇒ pydantic 静默忽略
       `eventFilter`、落回默认 `{}`。

    合并后果：**事件静默失去过滤条件、不再按条件触发**（比丢字段危险——它照常触发）。
    """

    SRC = ('(flow f (start :id s -> ev) '
           '(event :id ev :type t :filter (dict :a 1) -> e) (end :id e))')

    def test_dict_literal_compiles_to_dict(self):
        """缺陷 1：`(dict :a 1)` 必须编译成 dict，而不是留着 list。"""
        ir = compile_sexpr(self.SRC)
        ev = [n for n in ir["nodes"] if n.get("id") == "ev"][0]
        assert ev["eventFilter"] == {"a": 1}, f"应是 dict，实得 {ev['eventFilter']!r}"

    def test_emitted_with_colons(self):
        """发射必须带冒号（`(dict :a 1)`），否则再解析仍是 list。"""
        text = flow_to_sexpr(compile_sexpr(self.SRC))
        assert "(dict :a 1)" in text, f"dict 字面量丢了冒号：{text}"

    def test_survives_model_validation(self):
        """缺陷 2：经 `Flow.model_validate` 后 filter 不得变成 `{}`。"""
        from plaita.core.flow import Flow

        node = [x for x in Flow.model_validate(compile_sexpr(self.SRC)).nodes
                if getattr(x, "id", None) == "ev"][0]
        assert node.event_filter == {"a": 1}, (
            f"过滤条件在模型校验后被丢弃：{node.event_filter!r} ⇒ 事件将不再按条件触发"
        )
        assert node.next == "e"

    def test_camelcase_alias_accepted(self):
        """`eventFilter`（camelCase）必须被接受——codeflow/console 都发这种。"""
        from plaita.core.flow import Flow

        ir = {"flow_id": "f", "runtime": "python", "nodes": [
            {"id": "s", "type": "start", "next": "ev"},
            {"id": "ev", "type": "event", "eventType": "t",
             "eventFilter": {"a": 1}, "next": "e"},
            {"id": "e", "type": "end", "resultType": "success"},
        ]}
        node = [x for x in Flow.model_validate(ir).nodes if getattr(x, "id", None) == "ev"][0]
        assert node.event_filter == {"a": 1}

    def test_snake_case_still_accepted(self):
        """snake_case 也必须继续可用（存量 JSON flow 用这个）。"""
        from plaita.core.flow import Flow

        ir = {"flow_id": "f", "runtime": "python", "nodes": [
            {"id": "s", "type": "start", "next": "ev"},
            {"id": "ev", "type": "event", "event_type": "t",
             "event_filter": {"b": 2}, "next": "e"},
            {"id": "e", "type": "end", "resultType": "success"},
        ]}
        node = [x for x in Flow.model_validate(ir).nodes if getattr(x, "id", None) == "ev"][0]
        assert node.event_filter == {"b": 2}
