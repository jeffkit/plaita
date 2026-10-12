"""plaita#129 回归：http 节点的响应字段必须**真正可寻址**。

历史缺陷：`http` 节点用 `execution.set_state("$NODE.<id>.RESPONSE", …)` 写响应上下文，
而 `ExecutionContext.set_state` 当时是扁平写入（`self._state[key] = value`），
表达式寻址 `$NODE.<id>.RESPONSE` 走嵌套路径 ⇒ **永远取不到**：静默 `None`，
或根不存在时抛误导性的 `KeyError: '$NODE' not found`。

掩盖它的原因：`tests/integration/test_http.py` 用的是**带 `set_node_context` 的 mock
execution**，且只断言 `set_state` **被调用过**，从未对**真实** `ExecutionContext`
求值过文档里的表达式。

本文件按 issue 的验收条件覆盖**三类彼此独立的断言**（不要用一条断言兼职）：

1. **字段可达**：文档承诺的 `$NODE.<id>.data` / `.status` / `.headers` 解析到真实值；
2. **返回值契约**：节点的**返回值**（经 `next` 链路，不经 `$NODE.<id>`）仍是响应体；
3. **作用域**：未声明结果字段的节点，`$NODE.<id>` 形状**完全不变**（str/list/None）。
"""

import json
import re
import threading
import http.server
from pathlib import Path

import pytest

from plaita.core.context import ExecutionContext

REPO = Path(__file__).resolve().parents[2]
DOC_FIELDS = ("data", "status", "headers")


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        body = json.dumps({"hello": "world"}).encode()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Probe", "p129")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # noqa: D102
        pass


@pytest.fixture(scope="module")
def live_url():
    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


def _run(url, http_extra, end_output):
    from plaita.core.flow import Flow

    node = {"type": "http", "id": "fetch", "method": "GET", "url": url, "next": "e"}
    node.update(http_extra)
    return Flow.from_string(json.dumps({
        "flow_id": "t129",
        "nodes": [
            {"type": "start", "id": "s", "next": "fetch"},
            node,
            {"type": "end", "id": "e", "output": end_output, "resultType": "success"},
        ],
    })).run()


class TestFieldsReachable:
    """验收①：文档承诺的字段必须解析到真实值（真实 HTTP + 真实 ExecutionContext）。"""

    def test_documented_fields_resolve(self, live_url):
        assert _run(live_url, {}, "$NODE.fetch.data") == {"hello": "world"}
        assert _run(live_url, {}, "$NODE.fetch.status") == 202
        assert _run(live_url, {}, "$NODE.fetch.headers").get("X-Probe") == "p129"

    def test_all_fields_present_on_node_entry(self, live_url):
        entry = _run(live_url, {}, "$NODE.fetch")
        assert set(DOC_FIELDS) <= set(entry), f"节点结果缺少文档字段：{sorted(entry)}"
        assert entry["statusText"]  # 保留既有附带字段


class TestReturnValueContract:
    """验收②：节点**返回值**不变——经 `next` 链路观察，**不经 `$NODE.<id>`**。"""

    def test_downstream_next_receives_the_body(self, live_url):
        """直接观察 http 节点的**返回值**（不经 `$NODE.<id>`）。

        用真实 `ExecutionContext` 跑节点、拿它 `execute()` 的返回——这就是 next 链路
        与节点级消费方实际拿到的东西。断言它是**响应体**，而不是整个结果结构。
        """
        from plaita.node.http import HTTP

        ctx = ExecutionContext()
        node = HTTP(id="fetch", url=live_url, method="GET")
        result = node.execute(ctx)

        assert result == {"hello": "world"}, (
            f"节点返回值不再是响应体（next 链路会拿到结构体）：{result!r}"
        )

    def test_stored_entry_is_the_structured_result(self, live_url):
        """同一节点：返回值是 body，而 `$NODE.<id>` 是结果结构——两者分工明确。"""
        from plaita.node.http import HTTP

        ctx = ExecutionContext()
        node = HTTP(id="fetch", url=live_url, method="GET")
        returned = node.execute(ctx)
        ctx.update_node_result(node, returned)

        entry = ctx.evaluate("$NODE.fetch")
        assert isinstance(entry, dict)
        assert entry["data"] == {"hello": "world"}
        assert entry["status"] == 202

    def test_output_expression_still_wins_for_node_value(self, live_url):
        """设了 output 表达式时，节点值 = 表达式结果（既有语义不变）。

        注意 `output` 走表达式求值：JSON 里的 `"999"` 是**字符串字面量**，
        求值结果即 `'999'`（不是 int）。这里要钉的是"**节点值是 output 的结果**、
        而不是原始响应体"。
        """
        assert _run(live_url, {"output": "999"}, "$NODE.fetch.data") == "999"
        # 原始响应体另存 `.body`，不与 `data` 混用
        assert _run(live_url, {"output": "999"}, "$NODE.fetch.body") == {"hello": "world"}


class TestScopeUnchanged:
    """验收③：合并**只在节点声明结果字段时**发生，其它节点形状不变。"""

    @pytest.mark.parametrize("value", ["bare-string", [1, 2, 3], None, {"k": 1}, 0, False])
    def test_plain_node_shape_unchanged(self, value):
        class _Plain:
            id = "p"

        ctx = ExecutionContext()
        ctx.update_node_result(_Plain(), value)
        assert ctx.evaluate("$NODE.p") == value

    def test_node_without_payload_method_is_untouched(self):
        """没有 node_result_payload 的节点绝不进入合并分支。"""

        class _NoPayload:
            id = "n"

            def node_result_payload(self):  # pragma: no cover - 不应被当作 dict
                return None

        ctx = ExecutionContext()
        ctx.update_node_result(_NoPayload(), "x")
        assert ctx.evaluate("$NODE.n") == "x"

    def test_payload_raises_does_not_break_recording(self):
        class _Boom:
            id = "b"

            def node_result_payload(self):
                raise RuntimeError("boom")

        ctx = ExecutionContext()
        ctx.update_node_result(_Boom(), {"ok": 1})
        assert ctx.evaluate("$NODE.b") == {"ok": 1}


class TestCheckpointRoundTrip:
    """序列化往返后仍是 dict，且字段仍可点取。"""

    def test_roundtrip_preserves_nested_fields(self, live_url):
        from plaita.core.flow import Flow

        flow = Flow.from_string(json.dumps({
            "flow_id": "t129_rt",
            "nodes": [
                {"type": "start", "id": "s", "next": "fetch"},
                {"type": "http", "id": "fetch", "method": "GET",
                 "url": live_url, "next": "e"},
                {"type": "end", "id": "e", "output": "$NODE.fetch.status",
                 "resultType": "success"},
            ],
        }))
        assert flow.run() == 202

        # 用 to_dict/context 往返，再在**恢复后**的上下文里求值。
        # 注意：裸 ExecutionContext 未建 `$NODE` 根，故这里直接读状态字典 +
        # 用 from_checkpoint_dict 走一遍真实序列化路径。
        from plaita.core.state import CheckpointState

        state = CheckpointState()
        state.update_node_result("h", {"data": {"a": 1}, "status": 200})
        data = state.to_checkpoint_dict()
        restored = CheckpointState.from_checkpoint_dict(data)
        entry = restored.get("$NODE")["h"]
        assert entry["data"] == {"a": 1}
        assert entry["status"] == 200


class TestFieldNamesMatchDocs:
    """验收②（命名）：代码与文档同一份字段名，且**只有一套**权威命名。"""

    def test_builtin_doc_promises_these_names(self):
        doc = (REPO / "docs-site/docs/nodes/builtin.md").read_text(encoding="utf-8")
        section = doc.split("## http", 1)
        assert len(section) == 2
        m = re.search(r"\$NODE\.http_id\.([a-z]+)`?\s*/\s*`?\.([a-z]+)`?\s*/\s*`?\.([a-z]+)",
                      section[1][:400])
        assert m, "builtin.md 的 http 小节未声明字段名"
        assert tuple(m.groups()) == DOC_FIELDS

    def test_expressions_doc_states_node_field_model(self):
        doc = (REPO / "docs-site/docs/guide/expressions.md").read_text(encoding="utf-8")
        assert "$NODE.assign.field" in doc, "文档应声明 `$NODE.<id>.field` 模型"

    def test_no_flat_node_key_write_remains(self):
        """AC③：不得再有"写 `$NODE.<id>.<FIELD>` 而状态层扁平存储"的错配。"""
        src = (REPO / "plaita/node/http.py").read_text(encoding="utf-8")
        # 注释里可以提到历史写法；不能有**活的** set_state 调用
        live = []
        for ln in src.splitlines():
            code = ln.split("#", 1)[0]          # 去掉行内注释再判断
            if "execution.set_state(" in code and "NODE." in code:
                live.append(ln.strip())
        assert not live, f"仍有活的 $NODE 扁平 set_state 调用：{live}"

    def test_console_example_uses_data(self):
        """会 seed 给用户的示例必须跟着改，否则是用户可见的破坏。"""
        src = (REPO / "plaita-console/backend/services/examples.py").read_text(encoding="utf-8")
        assert '"output": "$NODE.fetch.data"' in src, (
            "console 的 http-echo 示例仍用 $NODE.fetch（#129 后返回结构而非 body）"
        )
