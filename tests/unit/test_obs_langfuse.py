"""LangfuseCallback：fake SDK 单测 + 真实 Flow 集成 + Distributed 续写契约。

langfuse SDK 从不真装：fake 模块注入 ``sys.modules["langfuse"]``，
构造路径（``client=None``）经惰性 import 命中 fake；注入 client 的路径
则完全绕开 SDK。断言面是"发给 Langfuse 的对象形状"（SDK v4 API：
create_trace_id / start_observation / OTel 根属性）。

错误语义与内核对齐（runner 只在成功路径发 on_node_end）：abort 策略下
错误节点与整条 run 的回调都不再发——span 级 ERROR 标记只在直调
on_node_end(error=...) 时生效，契约保留给未来内核补发 / 手动触发场景。
"""
from __future__ import annotations

import hashlib
import json
import sys
import unittest
from unittest import mock
from typing import Any, Dict, List, Optional

from plaita import Flow, FlowExecution, Node, NodeRegistry
from plaita.core.errors import NodeExecutionError
from plaita.event.memory import InMemoryEventBus
from plaita.obs import LangfuseCallback, map_openai_usage

# Langfuse v4 trace 级 OTel 属性名（字面量，与 SDK LangfuseOtelSpanAttributes
# 对齐——CI 环境不装真 SDK，字面量同时锁死契约）
TRACE_NAME = "langfuse.trace.name"
TRACE_TAGS = "langfuse.trace.tags"
TRACE_METADATA = "langfuse.trace.metadata"
TRACE_SESSION_ID = "session.id"
TRACE_USER_ID = "user.id"


# ---------------------------------------------------------------- fake SDK

class FakeRawSpan:
    """根 span 的 OTel 载体：记录 set_attributes（trace 级身份）。"""

    def __init__(self, recorder: "FakeRecorder"):
        self.recorder = recorder
        self.attrs: Dict[str, Any] = {}

    def set_attributes(self, attrs):
        self.attrs.update(attrs)
        self.recorder.root_attrs.append(dict(attrs))


class FakeObservation:
    def __init__(self, recorder: "FakeRecorder", kind: str, **kwargs):
        self.kind = kind
        self.name = kwargs.get("name", "")
        self.kwargs = kwargs
        self.updates: List[Dict[str, Any]] = []
        self.ended = False
        self._otel_span = FakeRawSpan(recorder)
        recorder.observations.append(self)

    def update(self, **kwargs):
        self.updates.append(kwargs)

    def end(self, **kwargs):
        self.ended = True

    def start_observation(self, **kwargs):
        recorder = self._otel_span.recorder
        kind = kwargs.get("as_type", "span")
        return FakeObservation(recorder, kind, _recorder=recorder, **kwargs)


class FakeRecorder:
    """fake client（SDK v4 形状）：create_trace_id/start_observation/flush。"""

    def __init__(self):
        self.observations: List[FakeObservation] = []
        self.roots: List[Dict[str, Any]] = []
        self.root_attrs: List[Dict[str, Any]] = []
        self.trace_id_seeds: List[str] = []
        self.flushes = 0

    def create_trace_id(self, seed: Optional[str] = None) -> str:
        self.trace_id_seeds.append(str(seed))
        # 32 位 hex、由种子确定——与真 SDK 的派生契约对齐
        return hashlib.md5(str(seed).encode()).hexdigest()

    def start_observation(self, **kwargs):
        self.roots.append(kwargs)
        return FakeObservation(self, kwargs.get("as_type", "span"),
                               _recorder=self, **kwargs)

    def flush(self):
        self.flushes += 1


class FakeLangfuseClient:
    """替换 langfuse.Langfuse 构造目标，捕获 ctor kwargs。"""

    last_kwargs: Dict[str, Any] = {}

    def __init__(self, **kwargs):
        self.recorder = FakeRecorder()
        FakeLangfuseClient.last_kwargs = kwargs

    def create_trace_id(self, seed: Optional[str] = None) -> str:
        return self.recorder.create_trace_id(seed=seed)

    def start_observation(self, **kwargs):
        return self.recorder.start_observation(**kwargs)

    def flush(self):
        self.recorder.flushes += 1


class FakeLangfuseModule:
    Langfuse = FakeLangfuseClient


# ------------------------------------------------------------ 被测工具函数

class TestMapOpenaiUsage(unittest.TestCase):
    def test_openai_shape_translated(self):
        out = map_openai_usage({"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15})
        self.assertEqual(out, {"input": 10, "output": 5, "total": 15})

    def test_partial_openai_keys_kept(self):
        out = map_openai_usage({"total_tokens": 7})
        self.assertEqual(out, {"total": 7})

    def test_non_int_values_dropped(self):
        out = map_openai_usage({"prompt_tokens": 10, "unit": "TOKENS"})
        self.assertEqual(out, {"input": 10})

    def test_anthropic_shape_with_cache_and_total_fallback(self):
        """Anthropic 形状（recursive 输出）：缓存键映射 + total 由 input+output 兜底。"""
        out = map_openai_usage({"input_tokens": 36, "output_tokens": 3,
                                "cache_read_input_tokens": 7680,
                                "cache_creation_input_tokens": 36})
        self.assertEqual(out, {"input": 36, "output": 3, "input_cached": 7680,
                               "input_cache_creation": 36, "total": 39})

    def test_agent_cli_shape_translated(self):
        """Agent CLI 常见的 input_tokens/output_tokens 形状（agentrun 输出）。"""
        out = map_openai_usage({"input_tokens": 120, "output_tokens": 30,
                                "total_tokens": 150})
        self.assertEqual(out, {"input": 120, "output": 30, "total": 150})

    def test_non_dict_returns_none(self):
        self.assertIsNone(map_openai_usage(None))
        self.assertIsNone(map_openai_usage(42))


# ------------------------------------------------------------ 构造与 extra

class TestConstruction(unittest.TestCase):
    def test_missing_extra_raises_actionable(self):
        with mock.patch("builtins.__import__",
                        side_effect=ImportError("No module named 'langfuse'")):
            with self.assertRaises(ImportError) as ctx:
                LangfuseCallback()
        self.assertIn("pip install plaita[langfuse]", str(ctx.exception))

    def test_lazy_import_path_uses_sys_modules(self):
        with mock.patch.dict(sys.modules, {"langfuse": FakeLangfuseModule}):
            cb = LangfuseCallback()
        self.assertIsInstance(cb._client, FakeLangfuseClient)

    def test_injected_client_skips_import(self):
        recorder = FakeRecorder()
        cb = LangfuseCallback(client=recorder)
        self.assertIs(cb._client, recorder)

    def test_top_level_gated_export(self):
        with mock.patch.dict(sys.modules, {"langfuse": FakeLangfuseModule}):
            from plaita import LangfuseCallback as TopLevel
        self.assertIs(TopLevel, LangfuseCallback)

    def test_ctor_kwargs_passthrough(self):
        with mock.patch.dict(sys.modules, {"langfuse": FakeLangfuseModule}):
            LangfuseCallback(public_key="pk", secret_key="sk", host="http://lf",
                             client_kwargs={"release": "r1"})
        self.assertEqual(FakeLangfuseClient.last_kwargs,
                         {"public_key": "pk", "secret_key": "sk",
                          "host": "http://lf", "release": "r1"})


# ------------------------------------------------------------ 真实 Flow 集成

LLM_SHAPE = {"text": "生成的文本", "model": "glm-5",
             "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
             "dry_run": False}


class LlmShapedNode(Node):
    """返回 llm 节点输出形状的测试节点（不依赖 plaita-nodes）。"""

    node_type = "llmShaped"
    node_name = "LLM 形状节点"

    def execute(self, execution=None):
        return dict(LLM_SHAPE)


class BoomNode(Node):
    node_type = "boom"
    node_name = "会炸节点"

    def execute(self, execution=None):
        raise RuntimeError("节点炸了")


def _registry(*node_classes) -> NodeRegistry:
    reg = NodeRegistry()
    for cls in node_classes:
        reg.register(cls)
    return reg


def _run(flow_json: dict, cb: LangfuseCallback, registry: NodeRegistry):
    flow = Flow.from_string(json.dumps(flow_json), registry=registry)
    execution = FlowExecution(callback_handlers=[cb])
    return execution.run_compatible(flow, False)


class TestRealFlowIntegration(unittest.TestCase):
    def setUp(self):
        self.recorder = FakeRecorder()
        self.cb = LangfuseCallback(client=self.recorder)
        self.flow_json = {
            "flow_id": "obs-flow",
            "inputType": {"dataType": "object"},
            "globalContext": {
                "langfuse_trace_id": "tr-fixed-1",
                "langfuse_session_id": "sess-9",
                "langfuse_user_id": "u-7",
            },
            "nodes": [
                {"type": "start", "id": "start", "next": "call"},
                {"type": "llmShaped", "id": "call", "next": "end"},
                {"type": "end", "id": "end", "output": "$NODE.call.text", "resultType": "success"},
            ],
        }
        self.registry = _registry(LlmShapedNode)

    def test_trace_identity_from_global_context(self):
        _run(self.flow_json, self.cb, self.registry)
        # 语义 id 经 create_trace_id(seed=) 确定性派生
        self.assertEqual(self.recorder.trace_id_seeds, ["tr-fixed-1"])
        root = self.recorder.roots[0]
        self.assertEqual(root["name"], "obs-flow")
        self.assertRegex(root["trace_context"]["trace_id"], r"^[0-9a-f]{32}$")
        # trace 级身份走根 span 的 OTel 属性
        attrs = self.recorder.root_attrs[0]
        self.assertEqual(attrs[TRACE_NAME], "obs-flow")
        self.assertIn("flow:obs-flow", attrs[TRACE_TAGS])
        self.assertEqual(attrs[TRACE_SESSION_ID], "sess-9")
        self.assertEqual(attrs[TRACE_USER_ID], "u-7")
        self.assertIn("flow_id", json.loads(attrs[TRACE_METADATA]))

    def test_spans_and_generation_shape(self):
        result = _run(self.flow_json, self.cb, self.registry)
        self.assertEqual(result, "生成的文本")
        spans = {o.name: o for o in self.recorder.observations if o.kind == "span"}
        gens = [o for o in self.recorder.observations if o.kind == "generation"]
        # 根 span（流程）+ start/call/end 三个节点 span，全部收口（v4 根是
        # 真 OTel span，on_flow_end 必须 end 它才导出）
        self.assertEqual(set(spans), {"obs-flow", "start", "call", "end"})
        self.assertTrue(all(s.ended for s in spans.values()))
        # llm 形状输出 → 一条 generation，usage 换算为 v4 usage_details
        self.assertEqual(len(gens), 1)
        gen = gens[0]
        self.assertEqual(gen.name, "call.generation")
        self.assertEqual(gen.kwargs["model"], "glm-5")
        self.assertEqual(gen.kwargs["usage_details"],
                         {"input": 10, "output": 5, "total": 15})
        self.assertEqual(gen.kwargs["output"], "生成的文本")
        self.assertTrue(gen.ended)

    def test_flush_on_flow_end(self):
        _run(self.flow_json, self.cb, self.registry)
        self.assertGreaterEqual(self.recorder.flushes, 1)

    def test_random_trace_id_without_key(self):
        self.flow_json["globalContext"] = {}
        _run(self.flow_json, self.cb, self.registry)
        seed = self.recorder.trace_id_seeds[0]
        self.assertTrue(seed.startswith("obs-flow-"))

    def test_abort_leaves_trace_open(self):
        """abort 策略（内核现状）：错误节点与整条 run 的回调都不再发——
        on_node_end 只走成功路径，NodeExecutionError 直接穿透不触发
        on_flow_end。span 保持 open，由宿主收尾（Langfuse TTL 兜底）。"""
        flow_json = {
            "flow_id": "obs-boom",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "boom"},
                {"type": "boom", "id": "boom", "next": "end"},
                {"type": "end", "id": "end", "output": "never", "resultType": "success"},
            ],
        }
        with self.assertRaises(NodeExecutionError):
            _run(flow_json, self.cb, _registry(BoomNode))
        spans = {o.name: o for o in self.recorder.observations if o.kind == "span"}
        self.assertIn("boom", spans)
        self.assertFalse(spans["boom"].ended)

    def test_content_clip(self):
        big_node = type("BigNode", (Node,), {
            "node_type": "bigNode", "node_name": "大输出",
            "execute": lambda self, execution=None: {"text": "x" * 50_000},
        })
        flow_json = {
            "flow_id": "obs-big",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "big"},
                {"type": "bigNode", "id": "big", "next": "end"},
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }
        _run(flow_json, self.cb, _registry(big_node))
        span = [o for o in self.recorder.observations
                if o.kind == "span" and o.name == "big"][0]
        output = span.updates[0]["output"]
        self.assertLessEqual(len(output["text"]), 10_100)
        self.assertTrue(output["text"].endswith(f"…[{50_000} chars]"))

    def test_sdk_failure_does_not_break_flow(self):
        class ExplodingClient:
            def create_trace_id(self, seed=None):
                raise RuntimeError("sdk down")

            def flush(self):
                raise RuntimeError("sdk down")

        cb = LangfuseCallback(client=ExplodingClient())
        result = _run(self.flow_json, cb, self.registry)
        self.assertEqual(result, "生成的文本")


class TestDirectErrorHooks(unittest.TestCase):
    """直调 on_node_end(error=...) 的 span ERROR 标记（内核目前不发 error 回调，
    该路径为契约完整性保留，也可被自定义节点手动触发）。"""

    def test_mark_error_on_open_span(self):
        recorder = FakeRecorder()
        cb = LangfuseCallback(client=recorder)

        class F:
            flow_id = "f"

        class N:
            id = "n1"
            node_type = "t"

        cb.on_node_start(F(), N())
        cb.on_node_end(F(), N(), error="炸了")
        span = [o for o in recorder.observations
                if o.kind == "span" and o.name == "n1"][0]
        self.assertEqual(span.updates[0]["level"], "ERROR")
        self.assertEqual(span.updates[0]["status_message"], "炸了")
        self.assertTrue(span.ended)

    def test_unmatched_node_end_is_noop(self):
        recorder = FakeRecorder()
        cb = LangfuseCallback(client=recorder)

        class F:
            flow_id = "f"

        class N:
            id = "ghost"
            node_type = "t"

        cb.on_node_end(F(), N(), result=None)  # 没有对应 on_node_start，不抛错
        self.assertEqual(recorder.observations, [])


class TestBoundExecutionTraceId(unittest.TestCase):
    """bind_execution：语义 trace id 解析链第 2 级——运行时 $EXECUTION_ID。"""

    def setUp(self):
        self.recorder = FakeRecorder()
        self.cb = LangfuseCallback(client=self.recorder)
        self.flow_json = {
            "flow_id": "obs-bind",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }

    def _run_bound(self) -> str:
        """跑一次真实 Flow；运行时在 clean() 阶段生成 $EXECUTION_ID，
        on_flow_start 时已就绪——返回运行时真实生成的 id 供断言。"""
        flow = Flow.from_string(json.dumps(self.flow_json))
        execution = FlowExecution(callback_handlers=[self.cb])
        self.cb.bind_execution(execution)
        execution.run_compatible(flow, False)
        return str(execution.get_state("$EXECUTION_ID"))

    def test_trace_seed_from_execution_state(self):
        exec_id = self._run_bound()
        self.assertTrue(exec_id)
        self.assertEqual(self.recorder.trace_id_seeds, [exec_id])
        self.assertRegex(self.recorder.roots[0]["trace_context"]["trace_id"],
                         r"^[0-9a-f]{32}$")

    def test_rebind_resets_run_state(self):
        """常驻宿主复用回调实例：换 execution 绑定必须换 trace。"""
        first = self._run_bound()
        second = self._run_bound()
        self.assertNotEqual(first, second)
        self.assertEqual(self.recorder.trace_id_seeds, [first, second])

    def test_global_context_key_overrides_execution_state(self):
        self.flow_json["globalContext"] = {"langfuse_trace_id": "tr-explicit"}
        flow = Flow.from_string(json.dumps(self.flow_json))
        execution = FlowExecution(callback_handlers=[self.cb])
        self.cb.bind_execution(execution)
        execution.run_compatible(flow, False)
        self.assertEqual(self.recorder.trace_id_seeds, ["tr-explicit"])

    def test_unbound_falls_back_to_random(self):
        flow = Flow.from_string(json.dumps(self.flow_json))
        FlowExecution(callback_handlers=[self.cb]).run_compatible(flow, False)
        self.assertTrue(self.recorder.trace_id_seeds[0].startswith("obs-bind-"))


class TestRunIsolation(unittest.TestCase):
    """同一 callback 实例跨多次 run 不串状态。"""

    def test_sequential_runs_get_fresh_traces(self):
        recorder = FakeRecorder()
        cb = LangfuseCallback(client=recorder)
        registry = _registry(LlmShapedNode)
        flow_json = {
            "flow_id": "obs-multi",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }
        _run(flow_json, cb, registry)
        _run(flow_json, cb, registry)
        self.assertEqual(len(recorder.roots), 2)
        self.assertNotEqual(recorder.trace_id_seeds[0], recorder.trace_id_seeds[1])


class TestDistributedContract(unittest.TestCase):
    """挂起 flush + 跨进程（新 Flow 对象 + 新执行实例）按同一语义 id 续写。"""

    def test_suspend_flush_and_cross_process_trace_id(self):
        recorder = FakeRecorder()
        flow_json = {
            "flow_id": "obs-dist",
            "inputType": {"dataType": "object"},
            "globalContext": {"langfuse_trace_id": "tr-dist-1"},
            "nodes": [
                {"type": "start", "id": "start", "next": "wait"},
                {"type": "event", "id": "wait", "event_type": "approve", "next": "end"},
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }
        flow = Flow.from_string(json.dumps(flow_json))
        cb = LangfuseCallback(client=recorder)
        execution = FlowExecution(event_bus=InMemoryEventBus(), callback_handlers=[cb])
        step = execution.run_distributed(flow, {})
        self.assertTrue(step["is_suspend"])
        self.assertGreaterEqual(recorder.flushes, 1)  # 挂起即 flush

        # 模拟另一进程：全新 Flow 对象 + 全新执行实例，global_context 注入同一 key。
        # 挂起的 event 节点必须以 resume_type="event" 解决（continue 会被内核拒绝）。
        flow2 = Flow.from_string(json.dumps(flow_json))
        cb2 = LangfuseCallback(client=recorder)
        execution2 = FlowExecution(event_bus=InMemoryEventBus(), callback_handlers=[cb2])
        step2 = execution2.run_distributed(
            flow2, None, saved_context=step["context"],
            resume_type="event", resume_data={"type": "approve", "approved": True})
        while not step2["is_end"]:
            step2 = execution2.run_distributed(
                flow2, None, saved_context=step2["context"], resume_type="continue")
        # 两个进程的语义 id 同源 → 派生 trace id 相同
        self.assertEqual(recorder.trace_id_seeds, ["tr-dist-1", "tr-dist-1"])
        # 宿主终态调用 finalize：收口 root（distributed 不发 on_flow_end）
        cb2.finalize()
        roots = [o for o in recorder.observations if o.kind == "span"
                 and o.name == "obs-dist"]
        self.assertEqual(len(roots), 2)  # 两个进程各一个虚拟根
        self.assertTrue(roots[-1].ended)  # 进程 2 的根被 finalize 收口
        self.assertFalse(roots[0].ended)  # 进程 1 挂起未终态，root 保持 open

    def test_finalize_idempotent_and_closes_root(self):
        """finalize：无 on_flow_end 场景收口 root；重复调用不炸。"""
        recorder = FakeRecorder()
        cb = LangfuseCallback(client=recorder)
        flow_json = {
            "flow_id": "obs-fin",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }
        flow = Flow.from_string(json.dumps(flow_json))
        execution = FlowExecution(event_bus=InMemoryEventBus(), callback_handlers=[cb])
        cb.bind_execution(execution)
        step = execution.run_distributed(flow, {})
        while not step["is_end"]:
            step = execution.run_distributed(flow, None, saved_context=step["context"],
                                             resume_type="continue")
        root = [o for o in recorder.observations
                if o.kind == "span" and o.name == "obs-fin"][0]
        self.assertFalse(root.ended)
        cb.finalize()
        self.assertTrue(root.ended)
        cb.finalize()  # 幂等
        self.assertGreaterEqual(recorder.flushes, 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
