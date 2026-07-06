"""变异测试专项断言 — plaita.dsl.builder

针对 mutmut 扫描出的 199 个 survived 变异（+ 156 个假阳性 timeout），
按方法/函数分组写精准杀灭测试。

survived 主要分类：
  - cond_group validation (3)
  - error_handler validation/default (5)
  - assignment/if_/switch/case **extra forwarding (several)
  - branch key names & default priority (5)
  - case c.get() key mutation (3) + error message (1)
  - _collection_node error message (1)
  - loop/map/filter/find/reduce next/initial/concurrent args (many)
  - code/http/event type string & key fields (many)
  - from_dict key strings (58)
  - build/child_flow kwargs (14+6)
  - to_json indent default (2)
  - FlowBuilder.run *args (1)
  - LinearBuilder methods forwarding (many)
"""
from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any, Dict

from plaita.dsl.builder import (
    FlowBuilder,
    LinearBuilder,
    NodeSpec,
    assignment,
    branch,
    build,
    case,
    child as child_node,
    child_flow,
    code,
    cond,
    cond_group,
    end,
    error_handler,
    event,
    filter as filter_node,
    find,
    http,
    if_,
    linear,
    loop,
    map as map_node,
    parallel,
    parallel_branch,
    reduce,
    reference,
    start,
    switch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _echo_child() -> FlowBuilder:
    return (
        FlowBuilder()
        .add(start())
        .add(end(output="$INPUT"))
    )


def _simple_fb() -> FlowBuilder:
    return FlowBuilder().add(start()).add(end(output="ok"))


def _simple_flow():
    return _simple_fb().build()


# ---------------------------------------------------------------------------
# cond_group validation
#
# _4: "or" → "XXorXX"  — validation check accepts wrong string
# _5: "or" → "OR"      — uppercase not accepted
# _6: similar
# ---------------------------------------------------------------------------

class TestCondGroup(unittest.TestCase):

    def test_or_relation_is_valid(self):
        """_4,_5: 'or' 应被接受（不应报错）."""
        result = cond_group("or", [cond("x", "==", 1)])
        self.assertEqual(result["relation"], "or")

    def test_and_relation_is_valid(self):
        """Baseline: 'and' 应被接受."""
        result = cond_group("and", [cond("x", "==", 1)])
        self.assertEqual(result["relation"], "and")

    def test_invalid_relation_raises(self):
        """_4,_5: 若 'or' 被替换为 'XXorXX' 或 'OR'，则校验通过——导致 'or' 不会报错。
        通过验证无效值确实报错，侧面证明合法值集合正确。
        """
        with self.assertRaises(ValueError):
            cond_group("OR", [cond("x", "==", 1)])

    def test_conditions_are_stored(self):
        """_6: conditions 列表应被正确保存."""
        conditions = [cond("x", "==", 1), cond("y", ">", 0)]
        result = cond_group("and", conditions)
        self.assertEqual(len(result["conditions"]), 2)


# ---------------------------------------------------------------------------
# error_handler validation & default
#
# _1:  strategy default "XXabortXX"
# _2:  similar
# _6:  "continue" → "XXcontinueXX" in validation
# _7:  "continue_with" string mutation
# _10: similar
# ---------------------------------------------------------------------------

class TestErrorHandler(unittest.TestCase):

    def test_default_strategy_is_abort(self):
        """_1,_2: 默认 strategy 应为 'abort'（不是 'XXabortXX'）."""
        spec = error_handler()
        self.assertEqual(spec["strategy"], "abort")

    def test_continue_strategy_is_accepted(self):
        """_6: 'continue' 应被接受（不应报错）."""
        spec = error_handler(strategy="continue")
        self.assertEqual(spec["strategy"], "continue")

    def test_continue_with_strategy_is_accepted(self):
        """_7: 'continue_with' 应被接受."""
        spec = error_handler(strategy="continue_with")
        self.assertEqual(spec["strategy"], "continue_with")

    def test_invalid_strategy_raises(self):
        """_6,_7,_10: 若 'continue' 被替换成 'XXcontinueXX'，则它不被校验通过。
        侧面验证：真正无效的字符串应该报错。
        """
        with self.assertRaises(ValueError):
            error_handler(strategy="XXcontinueXX")

    def test_retry_times_stored(self):
        """Baseline: retry_times 应存入 retryTimes."""
        spec = error_handler(retry_times=3)
        self.assertEqual(spec["retryTimes"], 3)


# ---------------------------------------------------------------------------
# assignment — **extra forwarding
#
# _10: **extra 被省略
# ---------------------------------------------------------------------------

class TestAssignmentFactory(unittest.TestCase):

    def test_extra_kwargs_forwarded(self):
        """_10: **extra 被省略 → 额外字段丢失."""
        spec = assignment("n1", output="$INPUT.x", timeout="1s")
        self.assertEqual(spec["timeout"], "1s",
                         "extra kwarg 'timeout' 应出现在 NodeSpec 中")


# ---------------------------------------------------------------------------
# if_ — **extra forwarding
#
# _15: **extra 被省略
# ---------------------------------------------------------------------------

class TestIfFactory(unittest.TestCase):

    def test_extra_kwargs_forwarded(self):
        """_15: **extra 被省略 → 额外字段丢失."""
        spec = if_("n1", cond("x", "==", 1), next="n2", else_next="n3", errorHandler={"strategy": "abort"})
        self.assertIn("errorHandler", spec,
                      "extra kwarg 'errorHandler' 应出现在 NodeSpec 中")


# ---------------------------------------------------------------------------
# branch — key names & default priority
#
# _1: priority default 1 → 0
# _4: "name" → "XXnameXX"
# _5: "name" → "NAME"
# _8: "priority" → "XXpriorityXX"
# _9: "priority" → "PRIORITY"
# ---------------------------------------------------------------------------

class TestBranchFactory(unittest.TestCase):

    def test_name_key_is_lowercase(self):
        """_4,_5: 'name' 键应为小写，不是 'XXnameXX' 或 'NAME'."""
        spec = branch("mybranch", "target")
        self.assertIn("name", spec)
        self.assertEqual(spec["name"], "mybranch")
        self.assertNotIn("XXnameXX", spec)
        self.assertNotIn("NAME", spec)

    def test_next_key_is_lowercase(self):
        """_4 category: 'next' 键应为小写."""
        spec = branch("b", "target_node")
        self.assertIn("next", spec)
        self.assertEqual(spec["next"], "target_node")

    def test_priority_key_is_lowercase(self):
        """_8,_9: 'priority' 键应为小写，不是 'XXpriorityXX' 或 'PRIORITY'."""
        spec = branch("b", "t", priority=5)
        self.assertIn("priority", spec)
        self.assertEqual(spec["priority"], 5)
        self.assertNotIn("PRIORITY", spec)
        self.assertNotIn("XXpriorityXX", spec)

    def test_default_priority_is_zero(self):
        """_1: 默认 priority 应为 0，不是 1."""
        spec = branch("b", "t")
        self.assertEqual(spec["priority"], 0)

    def test_is_default_not_set_by_default(self):
        """Baseline: is_default=False (default) 不应在 spec 中出现 isDefault."""
        spec = branch("b", "t")
        self.assertNotIn("isDefault", spec)

    def test_is_default_true_sets_key(self):
        """Baseline: is_default=True 应设置 isDefault."""
        spec = branch("b", "t", is_default=True)
        self.assertTrue(spec.get("isDefault"))


# ---------------------------------------------------------------------------
# switch — **extra forwarding
#
# _7: **extra 被省略
# ---------------------------------------------------------------------------

class TestSwitchFactory(unittest.TestCase):

    def test_extra_kwargs_forwarded(self):
        """_7: **extra 被省略 → 额外字段丢失."""
        branches_list = [branch("b1", "n1", is_default=True)]
        spec = switch("sw", branches_list, errorHandler={"strategy": "abort"})
        self.assertIn("errorHandler", spec)

    def test_branches_stored(self):
        """Baseline: branches 应以 list 形式存入."""
        branches_list = [branch("b1", "n1", is_default=True)]
        spec = switch("sw", branches_list)
        self.assertIsInstance(spec["branches"], list)
        self.assertEqual(len(spec["branches"]), 1)


# ---------------------------------------------------------------------------
# case — id normalization & error message
#
# _5:  c.get(None) 代替 c.get("id")
# _6:  c.get("id") 相关变异
# _7:  相关变异
# _14: ValueError message XX 前缀
# _29: 其他变异
# ---------------------------------------------------------------------------

class TestCaseFactory(unittest.TestCase):

    def test_case_with_next_uses_next_as_id(self):
        """_5,_6,_7: 无显式 id 时，c.get("id") 应回退到 c.get("next").
        若 c.get(None)，则 id 始终为 None，回退逻辑失效。
        """
        spec = case("cas", target="$x", cases=[
            {"value": "a", "next": "n_a"},
        ])
        # 归一化后 case["id"] 应为 "n_a"
        self.assertEqual(spec["cases"][0].get("id"), "n_a")

    def test_case_with_explicit_id_uses_id(self):
        """Baseline: 有显式 id 时，保持原 id."""
        spec = case("cas", target="$x", cases=[
            {"id": "case_a", "value": "a", "next": "n_a"},
        ])
        self.assertEqual(spec["cases"][0]["id"], "case_a")

    def test_case_missing_next_raises(self):
        """_14: ValueError 消息不应含 'XX' 前缀/后缀."""
        with self.assertRaises(ValueError) as ctx:
            case("cas", target="$x", cases=[{"value": "a"}])
        msg = str(ctx.exception)
        self.assertNotIn("XX", msg,
                         "错误消息不应含 XX 前缀/后缀")
        self.assertIn("next", msg.lower())


# ---------------------------------------------------------------------------
# _collection_node — error message
#
# _2: ValueError(None) 代替正确消息
# _13: 相关变异
# ---------------------------------------------------------------------------

class TestCollectionNodeError(unittest.TestCase):

    def test_missing_child_flow_raises_valueerror_with_message(self):
        """_2: ValueError(None) → 错误消息为 None/空。
        验证消息不为 None 且包含实际内容。
        """
        with self.assertRaises((ValueError, TypeError)) as ctx:
            loop("lp", "$items", child_flow=None, next="n1")
        exc = ctx.exception
        msg = str(exc)
        self.assertNotEqual(msg, "None",
                            "错误消息不应为字符串 'None'")
        self.assertTrue(len(msg) > 0)


# ---------------------------------------------------------------------------
# loop/map/filter/find/reduce — argument forwarding
#
# loop__mutmut_12:   next=None
# map__mutmut_1:     concurrent default True
# map__mutmut_13:    max_concurrent missing
# filter__mutmut_11: next=None
# find__mutmut_11:   next=None
# reduce__mutmut_6:  next=None
# reduce__mutmut_11: initial=None
# reduce__mutmut_12: initial missing
# ---------------------------------------------------------------------------

class TestCollectionFactories(unittest.TestCase):

    def test_loop_next_forwarded(self):
        """loop__mutmut_12: next 应被设置，不是 None."""
        spec = loop("lp", "$items", _echo_child(), next="n1")
        self.assertEqual(spec.get("next"), "n1")

    def test_map_concurrent_default_false(self):
        """map__mutmut_1: 默认 concurrent 应为 False（不设置 concurrent 字段）."""
        spec = map_node("mp", "$items", _echo_child())
        self.assertNotEqual(spec.get("concurrent"), True,
                            "默认 concurrent 不应为 True")

    def test_map_concurrent_true_when_set(self):
        """Baseline: concurrent=True 时正确设置."""
        spec = map_node("mp", "$items", _echo_child(), concurrent=True)
        self.assertTrue(spec.get("concurrent"))

    def test_map_max_concurrent_forwarded(self):
        """map__mutmut_13: max_concurrent 应被设置."""
        spec = map_node("mp", "$items", _echo_child(), concurrent=True, max_concurrent=5)
        self.assertEqual(spec.get("maxConcurrent"), 5)

    def test_filter_next_forwarded(self):
        """filter__mutmut_11: next 不应为 None."""
        spec = filter_node("ft", "$items", _echo_child(), next="n1")
        self.assertEqual(spec.get("next"), "n1")

    def test_find_next_forwarded(self):
        """find__mutmut_11: next 不应为 None."""
        spec = find("fd", "$items", _echo_child(), next="n1")
        self.assertEqual(spec.get("next"), "n1")

    def test_reduce_next_forwarded(self):
        """reduce__mutmut_6: next 不应为 None."""
        spec = reduce("rd", "$items", _echo_child(), next="n1")
        self.assertEqual(spec.get("next"), "n1")

    def test_reduce_initial_forwarded(self):
        """reduce__mutmut_11,12: initial 不应为 None 或丢失."""
        spec = reduce("rd", "$items", _echo_child(), initial=0)
        self.assertEqual(spec.get("initial"), 0)


# ---------------------------------------------------------------------------
# child / reference — argument forwarding
#
# child__mutmut_5,10,11: 各种参数变异
# reference__mutmut_2,4,5,7,9,10,11: 各种参数变异
# ---------------------------------------------------------------------------

class TestChildReferenceFactories(unittest.TestCase):

    def test_child_node_type_is_child(self):
        """child__mutmut_5: type 应为 'child'."""
        spec = child_node("c1", "$INPUT", _echo_child())
        self.assertEqual(spec["type"], "child")

    def test_child_input_forwarded(self):
        """child__mutmut_10: input 应被设置."""
        spec = child_node("c1", "$INPUT.data", _echo_child())
        self.assertEqual(spec["input"], "$INPUT.data")

    def test_child_next_forwarded(self):
        """child__mutmut_11: next 应被设置."""
        spec = child_node("c1", "$INPUT", _echo_child(), next="n2")
        self.assertEqual(spec.get("next"), "n2")

    def test_reference_type_is_reference(self):
        """reference__mutmut_2: type 应为 'reference'."""
        spec = reference("r1", "$INPUT", _echo_child())
        self.assertEqual(spec["type"], "reference")

    def test_reference_id_forwarded(self):
        """reference__mutmut_4: id 应被设置."""
        spec = reference("ref_id", "$INPUT", _echo_child())
        self.assertEqual(spec["id"], "ref_id")

    def test_reference_input_forwarded(self):
        """reference__mutmut_5: input 应被设置."""
        spec = reference("r1", "$INPUT.val", _echo_child())
        self.assertEqual(spec["input"], "$INPUT.val")

    def test_reference_next_forwarded(self):
        """reference__mutmut_7: next 应被设置."""
        spec = reference("r1", "$INPUT", _echo_child(), next="n2")
        self.assertEqual(spec.get("next"), "n2")


# ---------------------------------------------------------------------------
# code / http / event — type string & key fields
#
# code__mutmut_2:  "code" → None
# code__mutmut_3:  id=None
# code__mutmut_5:  code=None
# code__mutmut_9:  language kwarg 缺失
# http__mutmut_2:  "http" → None
# http__mutmut_5:  url=None
# http__mutmut_9:  method kwarg 缺失
# event__mutmut_2: "event" → None
# event__mutmut_6: id kwarg 缺失
# ---------------------------------------------------------------------------

class TestCodeFactory(unittest.TestCase):

    def test_type_is_code(self):
        """_2: type 应为 'code'，不是 None."""
        spec = code("n1", language="python", code="return 1")
        self.assertEqual(spec["type"], "code")

    def test_id_forwarded(self):
        """_3: id 应被设置，不是 None."""
        spec = code("my_code", language="python", code="return 1")
        self.assertEqual(spec["id"], "my_code")

    def test_code_body_forwarded(self):
        """_5: code 字段应被设置，不是 None."""
        spec = code("n1", language="python", code="x = 1\nreturn x")
        self.assertEqual(spec["code"], "x = 1\nreturn x")

    def test_language_forwarded(self):
        """_9: language 应存在."""
        spec = code("n1", language="javascript", code="return 1;")
        self.assertEqual(spec["language"], "javascript")

    def test_extra_kwargs_forwarded(self):
        """Various: **extra 应转发."""
        spec = code("n1", language="python", code="return 1", timeout="5s")
        self.assertEqual(spec["timeout"], "5s")


class TestHttpFactory(unittest.TestCase):

    def test_type_is_http(self):
        """_2: type 应为 'http'."""
        spec = http("h1", method="GET", url="https://example.com")
        self.assertEqual(spec["type"], "http")

    def test_url_forwarded(self):
        """_5: url 应被设置，不是 None."""
        spec = http("h1", method="GET", url="https://api.example.com/data")
        self.assertEqual(spec["url"], "https://api.example.com/data")

    def test_method_forwarded(self):
        """_9: method 应存在."""
        spec = http("h1", method="POST", url="https://example.com")
        self.assertEqual(spec["method"], "POST")

    def test_headers_forwarded(self):
        """Various: headers 应被设置."""
        spec = http("h1", method="GET", url="https://x.com", headers={"X-Auth": "token"})
        self.assertEqual(spec["headers"], {"X-Auth": "token"})

    def test_body_forwarded(self):
        """Various: body 应被设置."""
        spec = http("h1", method="POST", url="https://x.com", body={"k": "v"})
        self.assertEqual(spec["body"], {"k": "v"})


class TestEventFactory(unittest.TestCase):

    def test_type_is_event(self):
        """_2: type 应为 'event'."""
        spec = event("ev1", event_type="user.created")
        self.assertEqual(spec["type"], "event")

    def test_id_forwarded(self):
        """_6: id 应被设置."""
        spec = event("my_event", event_type="user.created")
        self.assertEqual(spec["id"], "my_event")

    def test_event_type_forwarded(self):
        """Various: eventType 应被设置."""
        spec = event("ev1", event_type="order.placed")
        self.assertEqual(spec["eventType"], "order.placed")

    def test_event_filter_forwarded(self):
        """Various: event_filter 应被设置."""
        spec = event("ev1", event_type="user.created",
                     event_filter=cond("user.age", ">=", 18))
        self.assertIn("eventFilter", spec)


# ---------------------------------------------------------------------------
# FlowBuilder.from_dict — key string mutations (58 mutations)
#
# All fields should be correctly loaded from the data dict.
# ---------------------------------------------------------------------------

class TestFlowBuilderFromDict(unittest.TestCase):

    def _make_full_data(self) -> Dict:
        return {
            "flow_id": "test_flow",
            "inputType": {"dataType": "object"},
            "outputType": {"dataType": "string"},
            "desc": "A test flow",
            "version": "2.0",
            "author": "tester",
            "timeout": "PT30S",
            "globalContext": {"key": "val"},
            "metadata": {"tag": "test"},
            "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "ok"},
            ],
        }

    def test_flow_id_loaded(self):
        """mutmut_2: flow_id=None 代替 data.get("flow_id")."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertEqual(fb.flow_id, "test_flow")

    def test_input_type_loaded(self):
        """mutmut_3,25: input_type 变异."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertIsNotNone(fb.input_type)
        self.assertEqual(fb.input_type, {"dataType": "object"})

    def test_output_type_loaded(self):
        """Various: output_type 应被加载."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertIsNotNone(fb.output_type)
        self.assertEqual(fb.output_type, {"dataType": "string"})

    def test_desc_loaded(self):
        """mutmut_5,15,35: desc 变异."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertEqual(fb.desc, "A test flow")

    def test_version_loaded(self):
        """Various: version 应被加载."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertEqual(fb.version, "2.0")

    def test_author_loaded(self):
        """Various: author 应被加载."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertEqual(fb.author, "tester")

    def test_timeout_loaded(self):
        """Various: timeout 应被加载."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertEqual(fb.timeout, "PT30S")

    def test_global_context_loaded(self):
        """mutmut_10,45: global_context 变异."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertIsNotNone(fb.global_context)
        self.assertEqual(fb.global_context.get("key"), "val")

    def test_metadata_loaded(self):
        """mutmut_10: metadata 变异."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertIsNotNone(fb.metadata)
        self.assertEqual(fb.metadata.get("tag"), "test")

    def test_runtime_loaded(self):
        """mutmut_55: runtime 默认 "python"，从 data 加载."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertEqual(fb.runtime, "python")

    def test_runtime_default_is_python(self):
        """mutmut_55: data 无 runtime 键时默认 "python"."""
        data = self._make_full_data()
        del data["runtime"]
        fb = FlowBuilder.from_dict(data)
        self.assertEqual(fb.runtime, "python")

    def test_nodes_loaded(self):
        """Various: nodes 应被加载."""
        fb = FlowBuilder.from_dict(self._make_full_data())
        self.assertEqual(len(fb._nodes), 2)

    def test_roundtrip_preserves_fields(self):
        """综合测试：from_dict → to_dict → 字段应保留."""
        data = self._make_full_data()
        fb = FlowBuilder.from_dict(data)
        result = fb.to_dict()
        self.assertEqual(result.get("flow_id"), "test_flow")
        self.assertEqual(result.get("desc"), "A test flow")
        self.assertEqual(result.get("version"), "2.0")
        self.assertEqual(result.get("author"), "tester")


# ---------------------------------------------------------------------------
# build() top-level function — kwargs forwarding
#
# build__mutmut_3: output_type=None
# build__mutmut_4: desc=None
# build__mutmut_7: timeout=None
# build__mutmut_12: output_type 缺失
# build__mutmut_13,14: 其他 None 替换
# build__mutmut_15-18: 其他 kwargs 变异
# ---------------------------------------------------------------------------

class TestBuildFunction(unittest.TestCase):

    def test_output_type_forwarded(self):
        """mutmut_3,12: output_type 应被设置在 builder 中."""
        output_t = {"dataType": "string"}
        fb = build("myflow", output_type=output_t)
        self.assertEqual(fb.output_type, output_t)

    def test_desc_forwarded(self):
        """mutmut_4: desc 应被设置."""
        fb = build("myflow", desc="My Flow")
        self.assertEqual(fb.desc, "My Flow")

    def test_timeout_forwarded(self):
        """mutmut_7: timeout 应被设置."""
        fb = build("myflow", timeout="PT30S")
        self.assertEqual(fb.timeout, "PT30S")

    def test_input_type_forwarded(self):
        """Various: input_type 应被设置."""
        it = {"dataType": "object"}
        fb = build("myflow", input_type=it)
        self.assertEqual(fb.input_type, it)

    def test_version_forwarded(self):
        """Various: version 应被设置."""
        fb = build("myflow", version="1.5.0")
        self.assertEqual(fb.version, "1.5.0")

    def test_author_forwarded(self):
        """Various: author 应被设置."""
        fb = build("myflow", author="alice")
        self.assertEqual(fb.author, "alice")

    def test_global_context_forwarded(self):
        """Various: global_context 应被设置."""
        fb = build("myflow", global_context={"gk": "gv"})
        self.assertEqual(fb.global_context, {"gk": "gv"})

    def test_metadata_forwarded(self):
        """Various: metadata 应被设置."""
        fb = build("myflow", metadata={"env": "prod"})
        self.assertEqual(fb.metadata, {"env": "prod"})


# ---------------------------------------------------------------------------
# child_flow() function — kwargs forwarding
#
# child_flow__mutmut_2: input_type=None
# child_flow__mutmut_3: output_type=None
# child_flow__mutmut_5: input_type kwarg 缺失
# ---------------------------------------------------------------------------

class TestChildFlowFunction(unittest.TestCase):
    """child_flow 是装饰器工厂：child_flow(...)(...) → FlowBuilder。"""

    def _apply(self, **kwargs) -> FlowBuilder:
        """将 child_flow 装饰器应用到一个空函数，返回 FlowBuilder。"""
        @child_flow(**kwargs)
        def _inner(b: FlowBuilder) -> None:
            b.add(start())
            b.add(end(output="ok"))
        return _inner  # type: ignore[return-value]

    def test_input_type_forwarded(self):
        """mutmut_2,5: input_type 应被传给 FlowBuilder。"""
        it = {"dataType": "object"}
        fb = self._apply(input_type=it, output_type=None)
        self.assertEqual(fb.input_type, it)

    def test_output_type_forwarded(self):
        """mutmut_3: output_type 应被传给 FlowBuilder。"""
        ot = {"dataType": "string"}
        fb = self._apply(input_type=None, output_type=ot)
        self.assertEqual(fb.output_type, ot)

    def test_desc_forwarded(self):
        """mutmut_4,6: desc 应被传给 FlowBuilder。"""
        fb = self._apply(input_type=None, output_type=None, desc="Child flow")
        self.assertEqual(fb.desc, "Child flow")


# ---------------------------------------------------------------------------
# FlowBuilder.to_json — default indent
#
# to_json__mutmut_1: indent default 3 → 2
# to_json__mutmut_3,4,6,7,8: 其他参数变异
# ---------------------------------------------------------------------------

class TestFlowBuilderToJson(unittest.TestCase):

    def test_default_indent_is_2(self):
        """to_json__mutmut_1: 默认 indent 应为 2，不是 3."""
        fb = build("t").add(start()).add(end(output="ok"))
        default_json = fb.to_json()
        indent2_json = fb.to_json(indent=2)
        self.assertEqual(default_json, indent2_json,
                         "to_json() 应等价于 to_json(indent=2)")

    def test_to_json_produces_valid_json(self):
        """Baseline: to_json 输出应是合法 JSON."""
        fb = build("t").add(start()).add(end(output="ok"))
        parsed = json.loads(fb.to_json())
        self.assertIsInstance(parsed, dict)


# ---------------------------------------------------------------------------
# FlowBuilder.run — *args forwarding
#
# FlowBuilderǁrun__mutmut_1: run(**kwargs) 代替 run(*args, **kwargs)
# ---------------------------------------------------------------------------

class TestFlowBuilderRun(unittest.TestCase):

    def test_run_with_kwargs_returns_correct_result(self):
        """run__mutmut_1: run() 应正确转发 kwargs 至 Flow.run."""
        fb = (FlowBuilder()
              .add(start("s", next="e"))
              .add(end("e", output="static_result")))
        result = fb.run()
        self.assertEqual(result, "static_result")

    def test_run_with_input_kwargs(self):
        """run__mutmut_1: 使用 keyword 参数调用，结果应包含输入字段."""
        fb = (FlowBuilder()
              .add(start("s", next="e"))
              .add(end("e", output="$INPUT")))
        result = fb.run(greeting="hello_builder")
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("greeting"), "hello_builder")

    def test_run_with_positional_dict_input(self):
        """FlowBuilderǁrun__mutmut_1: *args 被省略 → 位置参数字典输入被丢弃。
        突变: self.build().run(**kwargs) 删除了 *args。
        测试: 以位置参数传入 dict，结果应反映该输入。
        """
        fb = (FlowBuilder()
              .add(start("s", next="e"))
              .add(end("e", output="$INPUT")))
        result = fb.run({"positional_key": "expected_val"})
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("positional_key"), "expected_val")


# ---------------------------------------------------------------------------
# LinearBuilder — method argument forwarding
#
# assignment__mutmut_4,7,8,9: output_type 变异
# if___mutmut_4,5,6,10,11,12,14,15: next/else_next/**extra 变异
# switch__mutmut_6,7: **extra 变异
# case__mutmut_10,11: default/**extra 变异
# _collection__mutmut_8: 内部调用参数变异
# loop__mutmut_5,10,11: condition/next 变异
# map__mutmut_1,6,7,12,13,14: concurrent/max_concurrent 变异
# filter__mutmut_9: next 变异
# find__mutmut_9: next 变异
# reduce__mutmut_5,10,11: initial/next 变异
# child__mutmut_8: **extra 变异
# add__mutmut_2,3: 错误消息
# to_json__mutmut_1,2: indent 变异
# run__mutmut_1: *args 变异
# arun__mutmut_1: *args 变异
# ---------------------------------------------------------------------------

class TestLinearBuilderForwarding(unittest.TestCase):

    def test_assignment_output_type_forwarded(self):
        """assignment__mutmut_4,7,8: output_type 应被设置."""
        ot = {"dataType": "string"}
        lb = linear("f").start().assignment("$x", output_type=ot)
        node = lb._builder._nodes[-1]
        self.assertEqual(node.get("outputType"), ot)

    def test_if_else_next_forwarded(self):
        """if___mutmut_5: else_next 不应为 None."""
        lb = (linear("f").start()
              .if_(cond("x", "==", 1), then="yes", else_="no", id="if1")
              .end(id="yes", output="y")
              .end(id="no", output="n"))
        if_node = lb._builder._nodes[1]
        self.assertEqual(if_node.get("else_next"), "no")

    def test_if_extra_kwargs_forwarded(self):
        """if___mutmut_4,14,15: **extra 应被转发."""
        lb = (linear("f").start()
              .if_(cond("x", "==", 1), then="e", else_="e", timeout="2s", id="i1")
              .end(id="e", output="ok"))
        if_node = lb._builder._nodes[1]
        self.assertEqual(if_node.get("timeout"), "2s")

    def test_switch_extra_kwargs_forwarded(self):
        """switch__mutmut_6,7: **extra 应被转发."""
        lb = (linear("f")
              .start()
              .switch([branch("b1", "e", is_default=True)], timeout="1s")
              .end(id="e", output="ok"))
        sw_node = lb._builder._nodes[1]
        self.assertEqual(sw_node.get("timeout"), "1s")

    def test_case_default_forwarded(self):
        """case__mutmut_10: default 应被设置."""
        lb = (linear("f")
              .start()
              .case(target="$x", cases=[{"value": "a", "next": "ea"}],
                    default="eb")
              .end(id="ea", output="a")
              .end(id="eb", output="b"))
        case_nd = lb._builder._nodes[1]
        self.assertEqual(case_nd.get("default"), "eb")

    def test_loop_condition_forwarded(self):
        """loop__mutmut_5: condition 应被设置."""
        lb = (linear("f")
              .start()
              .loop("$items", _echo_child(), condition=cond("$ITEM", "!=", None))
              .end(output="ok"))
        loop_nd = lb._builder._nodes[1]
        self.assertIn("condition", loop_nd)

    def test_map_concurrent_forwarded(self):
        """map__mutmut_1,6,7: concurrent 应被设置."""
        lb = (linear("f")
              .start()
              .map("$items", _echo_child(), concurrent=True)
              .end(output="ok"))
        map_nd = lb._builder._nodes[1]
        self.assertTrue(map_nd.get("concurrent"))

    def test_map_max_concurrent_forwarded(self):
        """map__mutmut_13: max_concurrent 应被设置."""
        lb = (linear("f")
              .start()
              .map("$items", _echo_child(), concurrent=True, max_concurrent=4)
              .end(output="ok"))
        map_nd = lb._builder._nodes[1]
        self.assertEqual(map_nd.get("maxConcurrent"), 4)

    def test_reduce_initial_forwarded(self):
        """reduce__mutmut_5: initial 应被设置."""
        lb = (linear("f")
              .start()
              .reduce("$items", _echo_child(), initial=0)
              .end(output="ok"))
        reduce_nd = lb._builder._nodes[1]
        self.assertEqual(reduce_nd.get("initial"), 0)

    def test_add_error_message_not_none(self):
        """add__mutmut_2,3: TypeError 消息不应为 None 或 NoneType."""
        lb = linear("f").start()
        with self.assertRaises(TypeError) as ctx:
            lb.add(42)  # not a NodeSpec/dict
        msg = str(ctx.exception)
        self.assertNotEqual(msg, "None")
        self.assertNotIn("NoneType.__name__", msg)

    def test_add_error_mentions_actual_type(self):
        """add__mutmut_3: 消息应包含实际类型名称（不是 None.__class__.__name__='NoneType'）."""
        lb = linear("f").start()
        with self.assertRaises(TypeError) as ctx:
            lb.add("not_a_node")
        msg = str(ctx.exception)
        self.assertIn("str", msg,
                      "错误消息应包含实际类型名称 'str'")

    def test_linear_to_json_default_indent(self):
        """LinearBuilder.to_json__mutmut_1,2: 默认 indent=2."""
        lb = linear("f").start().end(output="ok")
        default_j = lb.to_json()
        indent2_j = lb.to_json(indent=2)
        self.assertEqual(default_j, indent2_j)

    def test_linear_run_with_params(self):
        """LinearBuilder.run__mutmut_1: *args 被省略 → keyword params 仍可工作."""
        lb = linear().start().assignment("$INPUT.x").end(output="$NODE._n2")
        # Simple run with keyword params
        result = lb.run(params={})
        self.assertIsNone(result)

    def test_linear_arun(self):
        """LinearBuilder.arun__mutmut_1: async run 应工作."""
        lb = linear().start().end(output="ok_async")

        async def _run():
            return await lb.arun(params={})

        result = asyncio.run(_run())
        self.assertEqual(result, "ok_async")

    def test_linear_run_with_positional_dict(self):
        """LinearBuilder.run__mutmut_1: *args 被省略 → 位置参数字典被丢弃。"""
        lb = linear().start().end(output="$INPUT")
        result = lb.run({"key": "lrun_val"})
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("key"), "lrun_val")

    def test_linear_arun_with_positional_dict(self):
        """LinearBuilder.arun__mutmut_1: *args 被省略。"""
        lb = linear().start().end(output="$INPUT")

        async def _run():
            return await lb.arun({"key": "larun_val"})

        result = asyncio.run(_run())
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("key"), "larun_val")


# ---------------------------------------------------------------------------
# FlowBuilder.remove_node / update_node / reroute — survived 变异
#
# FlowBuilderǁremove_node__mutmut_8: 返回值变异
# FlowBuilderǁupdate_node__mutmut_6: 参数变异
# FlowBuilderǁreroute__mutmut_13: 参数变异
# ---------------------------------------------------------------------------

class TestFlowBuilderMutations(unittest.TestCase):

    def _fb_with_nodes(self) -> FlowBuilder:
        fb = FlowBuilder()
        fb.add(start("s", next="m"))
        fb.add(assignment("m", output="42", next="e"))
        fb.add(end("e", output="$NODE.m"))
        return fb

    def test_remove_node_returns_self(self):
        """remove_node__mutmut_8: 方法应返回 self（FlowBuilder）."""
        fb = self._fb_with_nodes()
        result = fb.remove_node("m")
        self.assertIs(result, fb,
                      "remove_node 应返回 self 以支持链式调用")

    def test_update_node_changes_field(self):
        """update_node__mutmut_6: 指定节点字段应被更新."""
        fb = self._fb_with_nodes()
        fb.update_node("m", output="99")
        m_node = next(n for n in fb._nodes if n.get("id") == "m")
        self.assertEqual(m_node["output"], "99")

    def test_reroute_changes_next(self):
        """reroute__mutmut_13: 节点的 next 应被更新."""
        fb = self._fb_with_nodes()
        fb.add(end("e2", output="bye"))
        fb.reroute("s", next="e2")
        s_node = next(n for n in fb._nodes if n.get("id") == "s")
        self.assertEqual(s_node.get("next"), "e2")


# ---------------------------------------------------------------------------
# 补充测试：第二轮 survived（58 个剩余变异）
# ---------------------------------------------------------------------------

# --- 错误消息内容（不应为 None）---

class TestErrorMessagesSupplement(unittest.TestCase):

    def test_cond_group_invalid_relation_message_not_none(self):
        """cond_group__mutmut_6: ValueError(None) → 错误消息不应为字符串 'None'."""
        with self.assertRaises(ValueError) as ctx:
            cond_group("AND", [])
        msg = str(ctx.exception)
        self.assertNotEqual(msg, "None")
        self.assertTrue(len(msg) > 2)

    def test_error_handler_invalid_strategy_message_not_none(self):
        """error_handler__mutmut_10: ValueError(None)."""
        with self.assertRaises(ValueError) as ctx:
            error_handler(strategy="unknown")
        msg = str(ctx.exception)
        self.assertNotEqual(msg, "None")

    def test_remove_node_missing_id_message_not_none(self):
        """remove_node__mutmut_8: KeyError(None) → 消息应包含节点 id."""
        fb = FlowBuilder().add(start("s")).add(end("e"))
        with self.assertRaises(KeyError) as ctx:
            fb.remove_node("nonexistent")
        msg = str(ctx.exception)
        self.assertNotEqual(msg, "None")

    def test_update_node_missing_id_message_not_none(self):
        """update_node__mutmut_6: KeyError(None)."""
        fb = FlowBuilder().add(start("s")).add(end("e"))
        with self.assertRaises(KeyError) as ctx:
            fb.update_node("nonexistent", output="x")
        msg = str(ctx.exception)
        self.assertNotEqual(msg, "None")

    def test_reroute_missing_id_message_not_none(self):
        """reroute__mutmut_13: KeyError(None)."""
        fb = FlowBuilder().add(start("s")).add(end("e"))
        with self.assertRaises(KeyError) as ctx:
            fb.reroute("nonexistent", next="e")
        msg = str(ctx.exception)
        self.assertNotEqual(msg, "None")


# --- **extra forwarding in collection/child/reference/code/http/event ---

class TestExtraKwargsForwardingSupplement(unittest.TestCase):
    """验证所有节点工厂都能将 **extra kwargs 转发到 NodeSpec。"""

    def _cf(self):
        return _echo_child()

    def test_collection_node_extra(self):
        """_collection_node__mutmut_13: **extra 丢失."""
        spec = loop("lp", "$items", self._cf(), custom_field="x")
        self.assertEqual(spec.get("custom_field"), "x")

    def test_loop_extra_forwarded(self):
        """loop__mutmut_12: **extra 丢失."""
        spec = loop("lp", "$items", self._cf(), next="n1", tag="loop_extra")
        self.assertEqual(spec.get("tag"), "loop_extra")

    def test_map_extra_forwarded(self):
        """map__mutmut_13: **extra 丢失."""
        spec = map_node("mp", "$items", self._cf(), next="n1", tag="map_extra")
        self.assertEqual(spec.get("tag"), "map_extra")

    def test_reduce_extra_forwarded(self):
        """reduce__mutmut_12: **extra 丢失."""
        spec = reduce("rd", "$items", self._cf(), next="n1", tag="reduce_extra")
        self.assertEqual(spec.get("tag"), "reduce_extra")

    def test_filter_extra_forwarded(self):
        """filter__mutmut_11: **extra 丢失."""
        spec = filter_node("ft", "$items", self._cf(), next="n1", tag="filter_extra")
        self.assertEqual(spec.get("tag"), "filter_extra")

    def test_find_extra_forwarded(self):
        """find__mutmut_11: **extra 丢失."""
        spec = find("fd", "$items", self._cf(), next="n1", tag="find_extra")
        self.assertEqual(spec.get("tag"), "find_extra")

    def test_child_node_extra_forwarded(self):
        """child__mutmut_11: **extra 丢失."""
        spec = child_node("c1", "$INPUT", self._cf(), tag="child_extra")
        self.assertEqual(spec.get("tag"), "child_extra")

    def test_reference_extra_forwarded(self):
        """reference__mutmut_11: **extra 丢失."""
        spec = reference("r1", "$INPUT", self._cf(), tag="ref_extra")
        self.assertEqual(spec.get("tag"), "ref_extra")

    def test_reference_child_flow_field(self):
        """reference__mutmut_4,9: childFlow 应被设置，不是 None 或缺失."""
        cf = self._cf()
        spec = reference("r1", "$INPUT", cf, next="n1")
        self.assertIn("childFlow", spec,
                      "reference NodeSpec 应包含 childFlow 字段")
        self.assertIsNotNone(spec.get("childFlow"))

    def test_code_extra_forwarded(self):
        """code__mutmut_11: next kwarg 存在但被丢弃."""
        spec = code("c1", language="python", code="return 1", next="n1")
        self.assertEqual(spec.get("next"), "n1")

    def test_http_extra_forwarded(self):
        """http__mutmut_12: **extra 丢失."""
        spec = http("h1", method="GET", url="https://x.com", tag="http_extra")
        self.assertEqual(spec.get("tag"), "http_extra")

    def test_event_extra_forwarded(self):
        """event__mutmut_8: **extra 丢失."""
        spec = event("ev1", event_type="user.login", tag="event_extra")
        self.assertEqual(spec.get("tag"), "event_extra")

    def test_case_extra_forwarded(self):
        """case__mutmut_29: **extra 丢失."""
        spec = case("cas", target="$x",
                    cases=[{"value": "a", "next": "n_a"}],
                    tag="case_extra")
        self.assertEqual(spec.get("tag"), "case_extra")

    def test_http_next_forwarded(self):
        """http__mutmut_6,11: next=None 或 next 缺失."""
        spec = http("h1", method="GET", url="https://x.com", next="n1")
        self.assertEqual(spec.get("next"), "n1")

    def test_http_id_forwarded(self):
        """http__mutmut_3,8: id=None 或 id 缺失."""
        spec = http("my_http", method="GET", url="https://x.com")
        self.assertEqual(spec.get("id"), "my_http")


# --- from_dict runtime key ---

class TestFromDictRuntimeSupplement(unittest.TestCase):

    def test_runtime_key_lowercase_runtime(self):
        """from_dict__mutmut_52,56,57: 'runtime' 键大小写正确."""
        data = {
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "ok"},
            ],
            "runtime": "python",
        }
        fb = FlowBuilder.from_dict(data)
        self.assertEqual(fb.runtime, "python")

    def test_runtime_not_loaded_from_wrong_key(self):
        """from_dict__mutmut_56,57: 错误 key ('XXruntimeXX'/'RUNTIME') 不应被读取."""
        data = {
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "ok"},
            ],
            "XXruntimeXX": "javascript",
            "RUNTIME": "go",
        }
        fb = FlowBuilder.from_dict(data)
        # Should default to "python", not read wrong keys
        self.assertEqual(fb.runtime, "python")

    def test_non_default_runtime_is_loaded(self):
        """from_dict__mutmut_21,52: 非默认 runtime 应被正确加载。
        突变: runtime=data.get(None, "python") → 总是返回 "python"。
        突变: runtime= 行被删除 → 使用 __init__ 默认值 "python"。
        测试: 传入 "javascript" 应被加载到 builder.runtime。
        """
        data = {
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "ok"},
            ],
            "runtime": "javascript",
        }
        fb = FlowBuilder.from_dict(data)
        self.assertEqual(fb.runtime, "javascript",
                         "非默认 runtime 值应被正确从 'runtime' 键加载")

    def test_nodes_absent_defaults_to_empty_list(self):
        """from_dict__mutmut_61,63: 'nodes' 缺失时应默认空列表，不是 None."""
        data = {"flow_id": "no_nodes"}
        fb = FlowBuilder.from_dict(data)
        self.assertEqual(len(fb._nodes), 0)


# --- to_json ensure_ascii ---

class TestToJsonEnsureAscii(unittest.TestCase):
    """验证 to_json 的 ensure_ascii=False（中文等非 ASCII 字符不被转义）。"""

    def _build_with_chinese(self) -> FlowBuilder:
        fb = FlowBuilder(flow_id="中文流程", desc="一个测试流程")
        fb.add(start("开始", next="结束"))
        fb.add(end("结束", output="完成"))
        return fb

    def test_ensure_ascii_false_preserves_chinese(self):
        """to_json__mutmut_3,6,8: ensure_ascii 为 False → 中文不被转义."""
        fb = self._build_with_chinese()
        json_str = fb.to_json()
        self.assertIn("中文流程", json_str,
                      "to_json 默认应保留中文字符（ensure_ascii=False）")
        self.assertNotIn("\\u4e2d", json_str,
                         "中文不应被 Unicode 转义")

    def test_to_json_with_indent_argument(self):
        """to_json__mutmut_4,7: indent 参数应被使用."""
        fb = FlowBuilder("test").add(start("s")).add(end("e", output="ok"))
        json_str_2 = fb.to_json(indent=2)
        json_str_4 = fb.to_json(indent=4)
        # 4-space indent will have more leading spaces than 2-space
        # Find a consistently indented key
        parsed_2 = json.loads(json_str_2)
        parsed_4 = json.loads(json_str_4)
        self.assertEqual(parsed_2, parsed_4,
                         "两个 indent 版本解析结果应相同")
        self.assertGreater(len(json_str_4), len(json_str_2),
                           "indent=4 产生更长的 JSON")

    def test_linear_to_json_ensure_ascii(self):
        """LinearBuilder.to_json__mutmut_2: ensure_ascii 应保持中文."""
        lb = linear("流程").start().assignment("'结果'").end(output="ok")
        json_str = lb.to_json()
        self.assertIn("流程", json_str,
                      "LinearBuilder.to_json 应保留中文字符")

    def test_linear_to_json_respects_indent_argument(self):
        """LinearBuilder.to_json__mutmut_2: indent=None 忽略了实际传入的 indent。
        突变: return self._builder.to_json(indent=None) 无论传什么都用 None。
        测试策略: to_json(indent=4) 应产生比 to_json(indent=1) 更长的输出。
        """
        lb = linear("f").start().end(output="ok")
        json_1 = lb.to_json(indent=1)
        json_4 = lb.to_json(indent=4)
        self.assertGreater(len(json_4), len(json_1),
                           "indent=4 的 JSON 应长于 indent=1")


# --- LinearBuilder _ensure_id(None) mutations ---

class TestLinearBuilderEnsureId(unittest.TestCase):
    """验证 LinearBuilder 方法在显式指定 id 时，id 应被正确使用。"""

    def test_assignment_explicit_id_preserved(self):
        """assignment__mutmut_9: _ensure_id(None) → 生成自动 id 而非使用显式 id."""
        lb = linear("f").start().assignment("$x", id="my_assign")
        node = lb._builder._nodes[-1]
        self.assertEqual(node.get("id"), "my_assign")

    def test_if_explicit_id_preserved(self):
        """if___mutmut_15: _ensure_id(None) 变异."""
        lb = (linear("f").start()
              .if_(cond("x", "==", 1), id="my_if", then="e", else_="e")
              .end(id="e", output="ok"))
        if_node = lb._builder._nodes[1]
        self.assertEqual(if_node.get("id"), "my_if")

    def test_switch_explicit_id_preserved(self):
        """switch__mutmut_7: _ensure_id(None) 变异."""
        lb = (linear("f").start()
              .switch([branch("b1", "e", is_default=True)], id="my_switch")
              .end(id="e", output="ok"))
        sw_node = lb._builder._nodes[1]
        self.assertEqual(sw_node.get("id"), "my_switch")

    def test_case_explicit_id_preserved(self):
        """case__mutmut_11: _ensure_id(None) 变异."""
        lb = (linear("f").start()
              .case(target="$x",
                    cases=[{"value": "a", "next": "ea"}],
                    id="my_case")
              .end(id="ea", output="a")
              .end(id="eb", output="b"))
        case_nd = lb._builder._nodes[1]
        self.assertEqual(case_nd.get("id"), "my_case")

    def test_assignment_output_type_line_mutation(self):
        """assignment__mutmut_8: output_type 整行被删除."""
        ot = {"dataType": "number"}
        lb = linear("f").start().assignment("$x", output_type=ot)
        node = lb._builder._nodes[-1]
        self.assertEqual(node.get("outputType"), ot)

    def test_assignment_extra_kwargs_in_linear(self):
        """assignment__mutmut_8: **extra 被删除（output_type 仍在，但 **extra 丢失）.
        突变: assignment(..., output_type=ot, ) → 丢失 **extra。
        """
        lb = linear("f").start().assignment("$x", my_extra_field="extra_val")
        node = lb._builder._nodes[-1]
        self.assertEqual(node.get("my_extra_field"), "extra_val",
                         "LinearBuilder.assignment 的 **extra 应被转发到节点")


# --- LinearBuilder if_ branch args ---

class TestLinearBuilderIfBranches(unittest.TestCase):
    """验证 LinearBuilder.if_() 的 next/else_next/then/else_ 参数传递。"""

    def _lb_with_if(self, **kwargs) -> "LinearBuilder":
        return (linear("f").start(id="s")
                .if_(cond("x", "==", 1), **kwargs)
                .end(id="yes", output="y")
                .end(id="no", output="n"))

    def test_if_next_forwarded(self):
        """if___mutmut_4: next=None."""
        lb = self._lb_with_if(id="i1", next="yes", else_next="no")
        if_nd = lb._builder._nodes[1]
        self.assertEqual(if_nd.get("next"), "yes")

    def test_if_else_next_forwarded_explicit(self):
        """if___mutmut_5: else_next=None."""
        lb = self._lb_with_if(id="i1", next="yes", else_next="no")
        if_nd = lb._builder._nodes[1]
        self.assertEqual(if_nd.get("else_next"), "no")

    def test_if_then_alias_forwarded(self):
        """if___mutmut_6: then=None 变异."""
        lb = self._lb_with_if(id="i1", then="yes", else_="no")
        if_nd = lb._builder._nodes[1]
        # then 被应用后 next 应为 "yes"
        next_val = if_nd.get("next") or if_nd.get("then")
        self.assertIn("yes", [next_val])

    def test_if_else_kwarg_kept(self):
        """if___mutmut_11,12: else_next 或 then kwarg 被删除."""
        lb = self._lb_with_if(id="i1", then="yes", else_="no")
        if_nd = lb._builder._nodes[1]
        # else_next 或 else_ 应被设置
        else_val = if_nd.get("else_next") or if_nd.get("else_")
        self.assertIn("no", [else_val])

    def test_if_extra_kwargs_in_linear(self):
        """if___mutmut_4-15: **extra 也需要被转发."""
        lb = (linear("f").start(id="s")
              .if_(cond("x", "==", 1), id="i1", then="e", else_="e",
                   errorHandler={"strategy": "abort"})
              .end(id="e", output="ok"))
        if_nd = lb._builder._nodes[1]
        self.assertIn("errorHandler", if_nd)


# --- LinearBuilder collection extra/args ---

class TestLinearBuilderCollectionSupplement(unittest.TestCase):

    def test_loop_extra_forwarded_in_linear(self):
        """loop__mutmut_11: **extra 丢失."""
        lb = (linear("f").start()
              .loop("$items", _echo_child(), tag="loop_tag")
              .end(output="ok"))
        lp_nd = lb._builder._nodes[1]
        self.assertEqual(lp_nd.get("tag"), "loop_tag")

    def test_map_concurrent_default_in_linear(self):
        """LinearBuilder.map__mutmut_1: concurrent default True."""
        lb = (linear("f").start()
              .map("$items", _echo_child())
              .end(output="ok"))
        mp_nd = lb._builder._nodes[1]
        self.assertNotEqual(mp_nd.get("concurrent"), True,
                            "LinearBuilder.map 默认 concurrent 不应为 True")

    def test_map_extra_forwarded_in_linear(self):
        """LinearBuilder.map__mutmut_14: **extra 丢失."""
        lb = (linear("f").start()
              .map("$items", _echo_child(), tag="map_tag")
              .end(output="ok"))
        mp_nd = lb._builder._nodes[1]
        self.assertEqual(mp_nd.get("tag"), "map_tag")

    def test_filter_extra_forwarded_in_linear(self):
        """filter__mutmut_9: **extra 丢失."""
        lb = (linear("f").start()
              .filter("$items", _echo_child(), tag="filter_tag")
              .end(output="ok"))
        ft_nd = lb._builder._nodes[1]
        self.assertEqual(ft_nd.get("tag"), "filter_tag")

    def test_find_extra_forwarded_in_linear(self):
        """find__mutmut_9: **extra 丢失."""
        lb = (linear("f").start()
              .find("$items", _echo_child(), tag="find_tag")
              .end(output="ok"))
        fd_nd = lb._builder._nodes[1]
        self.assertEqual(fd_nd.get("tag"), "find_tag")

    def test_reduce_extra_forwarded_in_linear(self):
        """reduce__mutmut_11: **extra 丢失."""
        lb = (linear("f").start()
              .reduce("$items", _echo_child(), tag="reduce_tag")
              .end(output="ok"))
        rd_nd = lb._builder._nodes[1]
        self.assertEqual(rd_nd.get("tag"), "reduce_tag")

    def test_child_extra_forwarded_in_linear(self):
        """LinearBuilder.child__mutmut_8: **extra 丢失."""
        lb = (linear("f").start()
              .child("$INPUT", _echo_child(), tag="child_tag")
              .end(output="ok"))
        ch_nd = lb._builder._nodes[1]
        self.assertEqual(ch_nd.get("tag"), "child_tag")

    def test_switch_extra_id_preserved(self):
        """LinearBuilder.switch__mutmut_7: _ensure_id(None)."""
        lb = (linear("f").start()
              .switch([branch("b1", "e", is_default=True)], id="sw1")
              .end(id="e", output="ok"))
        sw_nd = lb._builder._nodes[1]
        self.assertEqual(sw_nd.get("id"), "sw1")

    def test_case_extra_forwarded_in_linear(self):
        """LinearBuilder.case__mutmut_10: default/**extra 丢失."""
        lb = (linear("f").start()
              .case(target="$x",
                    cases=[{"value": "a", "next": "ea"}],
                    default="eb", tag="case_tag")
              .end(id="ea", output="a")
              .end(id="eb", output="b"))
        case_nd = lb._builder._nodes[1]
        self.assertEqual(case_nd.get("tag"), "case_tag")


if __name__ == "__main__":
    unittest.main()
