"""0.5.0 用户评审回归修复的契约测试（fix/user-review-round1）。

每条测试对应一个已确认的用户侧问题, 防止行为回退：
- ``python -m plaita`` 入口与 ``help(plaita)`` 可用性
- End 节点 resultType 默认值 / 未知值告警
- Flow.from_string/from_file 注入自定义 NodeRegistry
- Generator 模式 End 步 is_end=True 与流程级超时
- ExecutionMode.from_string 报错可读
- 分布式挂起节点被 continue 绕过的防御
- 集合节点字符串 collection 的数组字面量解析
- 嵌套 Parallel 共享线程池死锁防护
- cancel_event 跨节点污染清理
- DSL 层 continue-with 拼写 / FlowBuilder.from_dict roundtrip
- NodeRegistry 重复注册告警
- PlaitaClient input_data=None / clear_cache 部分语义
"""
import asyncio
import io
import json
import logging
import unittest
from contextlib import redirect_stdout
from unittest.mock import AsyncMock, MagicMock

from pydantic import ValidationError

from plaita import Flow, FlowExecution, Node, NodeRegistry
from plaita.core.errors import ResumeError
from plaita.core.strategies import ExecutionMode
from plaita.event.memory import InMemoryEventBus


def _echo_flow(output: str = "hello") -> str:
    return json.dumps({
        "flow_id": "t",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "output": output},
        ],
    })


class TestPackageEntry(unittest.TestCase):
    def test_python_dash_m_plaita_exists(self):
        """``python -m plaita`` 有可执行入口（README 验证安装的标准姿势）。"""
        import importlib.util
        spec = importlib.util.find_spec("plaita.__main__")
        self.assertIsNotNone(spec)

    def test_help_plaita_does_not_die_on_missing_extra(self):
        """help(plaita) 不能被缺 extra 的 ImportError 打断（回归: pydoc 逐个
        getattr __dir__ 名字, CodeNode/HTTP 抛 ImportError 后整个 help 只剩
        一行报错）。"""
        import plaita
        buf = io.StringIO()
        with redirect_stdout(buf):
            help(plaita)
        self.assertGreater(len(buf.getvalue()), 3000)

    def test_star_import_non_empty(self):
        """``from plaita import *`` 不再得到空集。"""
        ns: dict = {}
        exec("from plaita import *", ns)
        public = [k for k in ns if not k.startswith("_")]
        self.assertIn("Flow", public)
        self.assertIn("NodeRegistry", public)
        self.assertGreater(len(public), 30)


class TestEndNodeDefaults(unittest.TestCase):
    def test_missing_result_type_defaults_to_success(self):
        """End 节点不写 resultType 时默认 success——历史上静默返回 None。"""
        flow = Flow.from_string(_echo_flow("hello"))
        self.assertEqual(flow.run(), "hello")

    def test_nop_still_returns_none(self):
        data = json.loads(_echo_flow("hello"))
        data["nodes"][1]["resultType"] = "nop"
        self.assertIsNone(Flow.model_validate(data).run())

    def test_unknown_result_type_warns_and_treats_as_success(self):
        data = json.loads(_echo_flow("hello"))
        data["nodes"][1]["resultType"] = "sucess"  # 拼写错误
        with self.assertLogs("plaita.node.end", level="WARNING"):
            self.assertEqual(Flow.model_validate(data).run(), "hello")


class _MyNode(Node):
    node_type = "myReviewNode"
    node_name = "评审自定义节点"

    def execute(self, execution=None):
        return {"result": "ok"}


class TestRegistryInjection(unittest.TestCase):
    def test_from_string_accepts_registry(self):
        """from_string 支持 registry 注入（与 model_validate 对齐）。"""
        reg = NodeRegistry()
        reg.register(_MyNode)
        flow = Flow.from_string(
            '{"flow_id":"r","nodes":[{"type":"start","id":"s","next":"m"},'
            '{"type":"myReviewNode","id":"m","next":"e"},'
            '{"type":"end","id":"e","output":"1"}]}',
            registry=reg,
        )
        self.assertIsNotNone(flow.run())

    def test_from_string_rejects_unregistered_custom_node(self):
        with self.assertRaises(RuntimeError):
            Flow.from_string(
                '{"flow_id":"r2","nodes":[{"type":"start","id":"s","next":"m"},'
                '{"type":"myReviewNode","id":"m","next":"e"},'
                '{"type":"end","id":"e","output":"1"}]}'
            )


class TestGeneratorMode(unittest.TestCase):
    def test_end_step_reports_is_end_true(self):
        """Generator 模式 End 节点步骤 is_end=True（历史恒 False, 完成判断失效）。"""
        flow = Flow.from_string(_echo_flow("x"))
        steps = list(flow.debug())
        self.assertTrue(steps[-1]["is_end"])
        self.assertFalse(steps[0]["is_end"])

    def test_flow_level_timeout_enforced(self):
        """Generator 模式下流程级超时生效（历史上被完全忽略）。"""
        from plaita.node import get_default_registry
        import time as _time

        class _SlowNode(Node):
            node_type = "slowReviewNode"

            def execute(self, execution=None):
                _time.sleep(0.5)
                return "ok"

        get_default_registry().register(_SlowNode)
        data = json.dumps({
            "flow_id": "slow",
            "timeout": "PT0.3S",
            "nodes": [
                {"type": "start", "id": "s", "next": "a"},
                {"type": "slowReviewNode", "id": "a", "next": "b"},
                {"type": "slowReviewNode", "id": "b", "next": "e"},
                {"type": "end", "id": "e", "output": "1"},
            ],
        })
        flow = Flow.from_string(data)

        from plaita.core.errors import FlowTimeoutError, NodeTimeoutError
        with self.assertRaises((FlowTimeoutError, NodeTimeoutError)):
            list(flow.debug())


class TestExecutionModeErrors(unittest.TestCase):
    def test_invalid_mode_lists_valid_values(self):
        with self.assertRaises(ValueError) as cm:
            ExecutionMode.from_string("generater")
        msg = str(cm.exception)
        for m in ("normal", "generator", "distributed"):
            self.assertIn(m, msg)


class TestDistributedContinueGuard(unittest.TestCase):
    def _event_flow(self):
        return Flow.from_string(json.dumps({
            "flow_id": "d",
            "nodes": [
                {"type": "start", "id": "s", "next": "ev"},
                {"type": "event", "id": "ev", "event_type": "approval.next", "next": "e"},
                {"type": "end", "id": "e", "output": "1"},
            ],
        }))

    def test_continue_over_pending_event_is_rejected(self):
        """挂起中的事件节点不允许 resume_type=continue 绕过（历史上静默完成）。"""
        execution = FlowExecution(event_bus=InMemoryEventBus())
        step = execution.run_distributed(self._event_flow(), {})
        self.assertTrue(step["is_suspend"])
        # distributed 外部契约把所有异常归一为 FlowErrorException (_error_normalization),
        # 但消息保留 ResumeError 的指引内容
        from plaita.core.errors import FlowErrorException
        with self.assertRaises(FlowErrorException) as cm:
            execution.run_distributed(
                self._event_flow(), None,
                saved_context=step["context"], resume_type="continue",
            )
        self.assertIn("pending", str(cm.exception))


class TestStringCollection(unittest.TestCase):
    def _map_flow(self, collection: str):
        return Flow.from_string(json.dumps({
            "flow_id": "m",
            "nodes": [
                {"type": "start", "id": "s", "next": "map"},
                {"type": "map", "id": "map", "collection": collection, "next": "e",
                 "childFlow": {"flow_id": "c", "nodes": [
                     {"type": "start", "id": "cs", "next": "ce"},
                     {"type": "end", "id": "ce", "output": "$INPUT.item"}]}},
                {"type": "end", "id": "e", "output": "$NODE.map", "resultType": "success"},
            ],
        }))

    def test_json_array_literal_not_char_iterated(self):
        """字符串形式的数组字面量按元素执行（历史上被 list() 拆成单字符）。"""
        result = self._map_flow("[1, 2, 3]").run()
        self.assertEqual(result, [1, 2, 3])

    def test_plain_string_treated_as_single_item(self):
        with self.assertLogs("plaita.node.loop", level="WARNING"):
            result = self._map_flow('"abc"').run()
        # 表达式引擎把带引号字面量原样求值为含引号字符串; 关键契约是不再逐字符迭代
        self.assertEqual(len(result), 1)


class TestNestedParallelNoDeadlock(unittest.TestCase):
    def test_wide_nested_parallel_completes(self):
        """分支数 > 共享池 max_workers 的嵌套 Parallel 不再永久死锁。"""
        inner = {"flow_id": "inner", "nodes": [
            {"type": "start", "id": "is", "next": "ip"},
            {"type": "parallel", "id": "ip", "mode": "thread", "next": "ie", "branches": [
                {"name": "x1", "flow": {"flow_id": "leaf", "nodes": [
                    {"type": "start", "id": "ls", "next": "le"},
                    {"type": "end", "id": "le", "output": "1"}]}},
                {"name": "x2", "flow": {"flow_id": "leaf2", "nodes": [
                    {"type": "start", "id": "ls", "next": "le"},
                    {"type": "end", "id": "le", "output": "2"}]}},
            ]},
            {"type": "end", "id": "ie", "output": "$NODE.ip"},
        ]}
        branch_names = [f"b{i}" for i in range(12)]
        branches = [
            {"name": name, "flow": inner} for name in branch_names
        ]
        flow = Flow.model_validate({
            "flow_id": "outer",
            "nodes": [
                {"type": "start", "id": "s", "next": "p"},
                {"type": "parallel", "id": "p", "mode": "thread", "next": "e",
                 "branches": branches, "joinBranches": branch_names},
                {"type": "end", "id": "e", "output": "$NODE.p", "resultType": "success"},
            ],
        })

        async def run_with_watchdog():
            return await asyncio.wait_for(flow.arun(), timeout=60)

        result = asyncio.run(run_with_watchdog())
        self.assertEqual(len(result), 12)
        self.assertTrue(all("b" in k for k in result))


class TestCancelEventScope(unittest.TestCase):
    def test_cancel_event_reset_between_nodes(self):
        """上一个节点超时遗留的 cancel_event 在新节点执行前复位。"""
        from plaita.core.runner import NodeRunner
        from plaita.core.context import ExecutionContext

        ctx = ExecutionContext()
        ctx.clean()
        runner = NodeRunner(ctx)
        node = MagicMock()
        node.id = "n1"
        node.timeout = None
        node.error_handler = None
        node.run = MagicMock(return_value="ok")
        flow = MagicMock()
        ctx.cancel_event.set()
        result = asyncio.run(runner._execute_with_retry(flow, node, None))
        self.assertEqual(result, "ok")
        self.assertFalse(ctx.cancel_event.is_set())


class TestDslConsistency(unittest.TestCase):
    def test_error_handler_accepts_enum_value_spelling(self):
        """DSL 层接受规范连字符 ``continue-with``（enum .value 可直接回填）。"""
        from plaita.dsl.builder import error_handler
        spec = error_handler(strategy="continue-with", default_value={"v": 1})
        self.assertEqual(spec["strategy"], "continue-with")

    def test_builder_from_dict_roundtrip_keeps_types(self):
        """from_dict(Flow.model_dump()) 不再静默丢 inputType/globalContext。"""
        from plaita.dsl import FlowBuilder
        flow = Flow.from_string(json.dumps({
            "flow_id": "r2",
            "inputType": {"dataType": "object"},
            "globalContext": {"g": 1},
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "1"},
            ],
        }))
        data = FlowBuilder.from_dict(flow.model_dump()).to_dict()
        self.assertIsNotNone(data.get("inputType"))
        self.assertIsNotNone(data.get("globalContext"))


class TestRegistryOverrideWarning(unittest.TestCase):
    def test_overriding_existing_node_type_warns(self):
        reg = NodeRegistry()

        class FakeStart(Node):
            node_type = "start"

        logger = "plaita.node"
        with self.assertLogs(logger, level="WARNING"):
            reg.register(FakeStart)


class TestPlaitaClientContracts(unittest.TestCase):
    def _client(self):
        from plaita.client import PlaitaClient
        return PlaitaClient("id", "key")

    def test_run_flow_none_input_becomes_empty_dict(self):
        """run_flow 不传 input_data 不再 TypeError（Optional 契约）。"""
        client = self._client()
        captured = {}

        class _FakeFlow:
            def run(self, *args, **kwargs):
                captured["args"] = args
                return "done"

        client.get_flow = MagicMock(return_value=_FakeFlow())
        self.assertEqual(client.run_flow("f", "1.0.0"), "done")
        self.assertEqual(captured["args"], ({},))

    def test_clear_cache_single_flow_keeps_other_flows(self):
        """clear_cache(flow_id) 只清该 flow 的版本, 不再清空全部。"""
        client = self._client()
        client.memory_cache["flow:a:1.0.0"] = object()
        client.memory_cache["flow:a:2.0.0"] = object()
        client.memory_cache["flow:b:1.0.0"] = object()
        cleared = client.clear_cache(flow_id="a")
        self.assertEqual(cleared, 2)
        self.assertIn("flow:b:1.0.0", client.memory_cache)

    def test_error_types_exist(self):
        from plaita.client import (
            PlaitaClientError,
            PlaitaClientNetworkError,
            PlaitaClientResponseError,
        )
        self.assertTrue(issubclass(PlaitaClientNetworkError, PlaitaClientError))
        self.assertTrue(issubclass(PlaitaClientResponseError, PlaitaClientError))
        self.assertTrue(issubclass(PlaitaClientError, Exception))


if __name__ == "__main__":
    unittest.main()


class TestR2UnknownKeyWarning(unittest.TestCase):
    """未知键告警：把"沉默地配置错"变成可见（只告警不报错）。"""

    def test_node_typo_key_warns(self):
        data = json.loads(_echo_flow("x"))
        data["nodes"][1]["resutlType"] = "error"  # 拼错——历史上被 extra=ignore 静默吞
        with self.assertLogs("plaita.node.schema", level="WARNING") as cm:
            Flow.model_validate(data)
        self.assertTrue(any("resutlType" in line for line in cm.output))

    def test_legacy_camel_keys_do_not_warn(self):
        data = json.loads(_echo_flow("x"))
        data["nodes"][1]["resultType"] = "success"  # 合法遗留别名
        flow = Flow.model_validate(data)
        self.assertEqual(flow.run(), "x")

    def test_switch_typo_branches_key_warns_and_validate_flags(self):
        flow_json = json.dumps({
            "flow_id": "sw",
            "nodes": [
                {"type": "start", "id": "s", "next": "sw"},
                {"type": "switch", "id": "sw", "branchs": [  # 拼错 branches
                    {"name": "a", "next": "e", "condition": {"field": "$INPUT.x", "operator": "eq", "value": 1}},
                ]},
                {"type": "end", "id": "e", "output": "1"},
            ],
        })
        with self.assertLogs("plaita.node.schema", level="WARNING"):
            Flow.from_string(flow_json)


class TestR2SwitchNoMatch(unittest.TestCase):
    def _flow(self, extra_node_cfg=None):
        node = {"type": "switch", "id": "sw", "next": "e", "branches": [
            {"name": "adult", "next": "e", "condition": {"field": "$INPUT.age", "operator": "gte", "value": 18}},
        ]}
        if extra_node_cfg:
            node.update(extra_node_cfg)
        return Flow.from_string(json.dumps({
            "flow_id": "sw",
            "nodes": [
                {"type": "start", "id": "s", "next": "sw"},
                node,
                {"type": "end", "id": "e", "output": "1"},
            ],
        }))

    def test_no_match_no_default_raises(self):
        """分支未命中且无 default：不再把 $NODE 中间态当结果"成功"返回。"""
        from plaita.core.errors import FlowExecutionException
        with self.assertRaises(FlowExecutionException) as cm:
            self._flow().run(age=5)
        self.assertIn("no branch", str(cm.exception))

    def test_no_match_with_continue_strategy_keeps_legacy_behavior(self):
        """显式 errorHandler.strategy=continue 保留旧"跳过"逃生口。"""
        result = self._flow({"errorHandler": {"strategy": "continue"}}).run(age=5)
        self.assertIsNotNone(result)  # 走到流程收尾（$NODE 表），不再抛错


class TestR2SubscriptionCleanup(unittest.TestCase):
    def test_resume_unregisters_subscription(self):
        """resume 完成后注销挂起期订阅（历史死订阅持续匹配后续事件）。"""
        from plaita.node import EventNode  # noqa: F401 - 确保 event 节点已注册

        flow = Flow.from_string(json.dumps({
            "flow_id": "d",
            "nodes": [
                {"type": "start", "id": "s", "next": "ev"},
                {"type": "event", "id": "ev", "event_type": "approval.next", "next": "e"},
                {"type": "end", "id": "e", "output": "1"},
            ],
        }))
        bus = InMemoryEventBus()
        execution = FlowExecution(event_bus=bus)
        step = execution.run_distributed(flow, {})
        self.assertTrue(step["is_suspend"])
        step2 = execution.run_distributed(
            flow, None, saved_context=step["context"],
            resume_type="event", resume_data={"approved": True},
        )
        self.assertFalse(step2["is_suspend"])
        # 订阅应已被清理
        subs = list(bus.subscription_storage.subscriptions.values())
        self.assertEqual(len(subs), 0)


class TestR2RunnerContracts(unittest.TestCase):
    def test_http_error_strategy_continue_with_returns_default(self):
        """http 失败现在走 errorHandler（continue_with 返回 defaultValue）。

        历史上 NodeException 被当返回值传回，errorHandler 永不生效。 unroutable
        端口保证快速连接失败。
        """
        flow = Flow.from_string(json.dumps({
            "flow_id": "h",
            "nodes": [
                {"type": "start", "id": "s", "next": "h"},
                {"type": "http", "id": "h", "method": "GET",
                 "url": "http://127.0.0.1:1/nope", "next": "e",
                 "errorHandler": {"strategy": "continue_with", "defaultValue": "unknown"}},
                {"type": "end", "id": "e", "output": "$NODE.h", "resultType": "success"},
            ],
        }))
        self.assertEqual(flow.run(), "unknown")

    def test_invalid_timeout_message_lists_formats(self):
        from plaita.core.runner import NodeRunner
        with self.assertRaises(ValueError) as cm:
            NodeRunner._parse_timeout("100ms")
        self.assertIn("ISO 8601", str(cm.exception))

    def test_execution_reentry_guard(self):
        ex = FlowExecution()
        ex._begin_run()
        try:
            with self.assertRaises(Exception) as cm:
                ex._begin_run()
            self.assertIn("already running", str(cm.exception))
        finally:
            ex._running = False


class TestR2PlaitaClientContract(unittest.TestCase):
    def test_get_flow_parses_server_flow_string(self):
        """契约接口 data.flow 是 JSON 字符串——历史上被双重解析必崩。"""
        from unittest.mock import patch
        import plaita.client as client_mod
        from plaita.client import PlaitaClient

        flow_str = json.dumps({
            "flow_id": "remote",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$INPUT.name", "resultType": "success"},
            ],
        })
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"code": 0, "data": {"flow": flow_str}}
        with patch.object(client_mod.requests, "post", return_value=response):
            client = PlaitaClient("id", "key")
            flow = client.get_flow("259", "0.0.2")
            self.assertEqual(flow.run(name="kongjie"), "kongjie")


class TestP2Polish(unittest.TestCase):
    """P2 打磨轮：日志噪音治理 / 表达式前缀统一 / executor 参数校验 / @flow REPL。"""

    def test_flow_root_alias(self):
        """``$FLOW`` 是 ``$FLOW_ID`` 的别名（历史上 KeyError 崩）。"""
        flow = Flow.from_string(json.dumps({
            "flow_id": "probe",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "output": "$FLOW", "resultType": "success"},
            ],
        }))
        self.assertEqual(flow.run(), "probe")

    def test_handled_node_error_logs_concise_no_traceback(self):
        """被 errorHandler 兜住的节点错误只打一行简明告警，不再双份 traceback。"""
        from plaita.node import get_default_registry

        class Boom(Node):
            node_type = "boomP2"

            def execute(self, execution=None):
                raise RuntimeError("boom-cause")

        get_default_registry().register(Boom)
        flow = Flow.from_string(json.dumps({
            "flow_id": "he",
            "nodes": [
                {"type": "start", "id": "s", "next": "b"},
                {"type": "boomP2", "id": "b", "next": "e",
                 "errorHandler": {"strategy": "continue_with", "defaultValue": "d"}},
                {"type": "end", "id": "e", "output": "'x'", "resultType": "success"},
            ],
        }))
        with self.assertLogs("plaita.core.runner", level="WARNING") as cm:
            flow.run()
        self.assertTrue(any("handled (errorHandler.strategy=continue-with)" in line
                            and "RuntimeError: boom-cause" in line
                            for line in cm.output))
        self.assertTrue(all("Traceback" not in line for line in cm.output))

    def test_run_respects_timeout_zero(self):
        """``FlowExecution.run(timeout=0)`` 不再被 ``or`` 当 falsy 丢弃。"""
        from unittest.mock import patch
        from plaita.core.executor import FlowExecution

        captured = {}

        def spy(self, flow, params=None, **kw):
            captured["timeout"] = self.timeout
            raise KeyboardInterrupt

        flow = Flow.from_string(_echo_flow("1"))
        with patch.object(FlowExecution, "run_distributed", spy):
            with self.assertRaises(KeyboardInterrupt):
                FlowExecution.run(flow, timeout=0, mode="distributed")
        self.assertEqual(captured["timeout"], 0)

    def test_run_warns_on_unknown_options(self):
        from unittest.mock import patch
        from plaita.core.executor import FlowExecution

        def spy(self, flow, params=None, **kw):
            raise KeyboardInterrupt

        flow = Flow.from_string(_echo_flow("1"))
        with self.assertLogs("plaita.core.executor", level="WARNING") as cm:
            with patch.object(FlowExecution, "run_distributed", spy):
                with self.assertRaises(KeyboardInterrupt):
                    FlowExecution.run(flow, mode="distributed", express_prefx="$!")
        self.assertTrue(any("express_prefx" in line for line in cm.output))

    def test_flow_decorator_repl_friendly_error(self):
        """REPL/Jupyter 里用 @flow：报错指路 .py 文件 / flow_from_source。"""
        src = (
            "from plaita.dsl.codeflow import flow\n"
            "@flow('repl_probe')\n"
            "def repl_probe(x):\n"
            "    return {'v': x}\n"
        )
        with self.assertRaises(Exception) as cm:
            exec(src, {})  # noqa: S102 - 刻意在 exec 中复现"无源文件"场景
        self.assertIn("flow_from_source", str(cm.exception))

    def test_event_timeout_mentions_no_replay(self):
        from plaita.event.exceptions import EventTimeoutError
        err = EventTimeoutError("a.b", 1.0)
        self.assertIn("不回放", str(err))


class TestBgStateBounded(unittest.TestCase):
    """并行分支登记表 _BG_STATE 必须有界（长时进程泄漏防护）。"""

    def test_bg_state_evicts_oldest_beyond_capacity(self):
        import plaita.node.concurrent as conc

        executions = []
        original_max = conc._BG_STATE_MAX
        try:
            conc._BG_STATE_MAX = 8
            conc._BG_STATE.clear()
            for i in range(32):
                ex = MagicMock()
                ex.execution_id = f"exec-{i}"
                executions.append(ex)
                conc._get_bg_state(ex)
            self.assertLessEqual(len(conc._BG_STATE), 8)
            # LRU：最新的在，最老的被淘汰
            self.assertIn("exec-31", conc._BG_STATE)
            self.assertNotIn("exec-0", conc._BG_STATE)
        finally:
            conc._BG_STATE_MAX = original_max
            conc._BG_STATE.clear()

    def test_done_future_removed_from_state(self):
        """完成的 future 从登记表摘除，不再钉住分支结果。"""
        import concurrent.futures as cf

        import plaita.node.concurrent as conc

        conc._BG_STATE.clear()
        ex = MagicMock()
        ex.execution_id = "exec-done-test"
        state = conc._get_bg_state(ex)
        pool = cf.ThreadPoolExecutor(max_workers=1)
        try:
            node = MagicMock()
            node.id = "p"
            node.match_condition_branches.return_value = []
            p_node = conc.Parallel.model_validate({
                "id": "p", "branches": [], "joinBranches": [],
            })
            fut = pool.submit(lambda: {"payload": "x" * 100})
            state["futures"].append(fut)
            errors = state["errors"]
            cb = p_node._make_background_done_callback(
                MagicMock(name="b0"), errors, state,
            )
            fut.add_done_callback(cb)
            fut.result()
            cf.wait([fut])
            # done_callback 已把 future 摘除
            self.assertEqual(state["futures"], [])
            self.assertEqual(errors, [])
        finally:
            pool.shutdown(wait=False)
            conc._BG_STATE.clear()
