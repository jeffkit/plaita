"""plaita#200：sexpr / ``@flow`` 往返静默丢弃 HTTP 访问策略与 EVENT 订阅超时。

HTTP 的访问策略字段（``allowedHosts`` / ``deniedHosts`` /
``blockPrivateNetworks`` / ``requestTimeout`` / ``maxRedirects``）是 SSRF 防护面
（``plaita/node/http.py``）；``EventNode.subscription_timeout`` 决定挂起等待的
上限。两条路径此前读写两侧都没有表达面 ⇒ 任何 sexpr / ``@flow`` 往返都把
安全配置**静默清空**（不是普通字段丢失，是安全默认降级）。

验收（#200）：
- ``flow_to_sexpr`` → ``compile_sexpr`` 后 5 个 HTTP 策略字段逐字段相等；
- ``emit_source`` → ``compile_source`` 后同样相等；
- EVENT ``subscriptionTimeout`` 两条路径往返相等；
- camel / snake 两种 IR 拼写都能发射（#207 同口径的对称性）。
"""
from __future__ import annotations

import pytest

from plaita.dsl.codeflow import compile_source, emit_source, flow_from_source
from plaita.dsl.sexpr import SexprEmitError, compile_sexpr, flow_to_sexpr, parse_sexpr

HTTP_POLICY = (
    ("allowedHosts", "allowed_hosts"),
    ("deniedHosts", "denied_hosts"),
    ("blockPrivateNetworks", "block_private_networks"),
    ("requestTimeout", "request_timeout"),
    ("maxRedirects", "max_redirects"),
)
POLICY_VALUES = {
    "allowedHosts": ["example.com", "*.internal.test"],
    "deniedHosts": ["evil.example.com"],
    "blockPrivateNetworks": True,
    "requestTimeout": 5,
    "maxRedirects": 2,
}
SUBSCRIPTION_TIMEOUT = 120


def _http_node(camel: bool = True) -> dict:
    node = {"type": "http", "id": "h", "method": "GET",
            "url": "http://example.com", "next": "w"}
    for camel_key, snake_key in HTTP_POLICY:
        node[camel_key if camel else snake_key] = POLICY_VALUES[camel_key]
    return node


def _ir(camel: bool = True) -> dict:
    return {"flow_id": "f-policy", "nodes": [
        {"type": "start", "id": "s", "next": "h"},
        _http_node(camel),
        {"type": "event", "id": "w", "eventType": "quote.ready",
         "subscriptionTimeout": SUBSCRIPTION_TIMEOUT, "next": "e"},
        {"type": "end", "id": "e", "resultType": "success"},
    ]}


def _pick_http(nodes, camel: bool = True):
    node = [n for n in nodes if n.get("id") == "h"][0]
    return {camel_key if camel else snake_key: node.get(camel_key if camel else snake_key)
            for camel_key, snake_key in HTTP_POLICY}


def _pick_sub_timeout(nodes):
    node = [n for n in nodes if n.get("id") == "w"][0]
    return node.get("subscriptionTimeout", node.get("subscription_timeout"))


# ---------------------------------------------------------------------------
# sexpr
# ---------------------------------------------------------------------------

def test_sexpr_roundtrip_http_policy_fields():
    """#200 主症：5 个访问策略字段逐字段往返相等。"""
    src = flow_to_sexpr(_ir())
    for kw in (":allowed-hosts", ":denied-hosts",
               ":block-private-networks", ":request-timeout", ":max-redirects"):
        assert kw in src
    assert _pick_http(compile_sexpr(src)["nodes"]) == POLICY_VALUES


def test_sexpr_roundtrip_event_subscription_timeout():
    """#200：EVENT 的 ``subscriptionTimeout`` 往返相等。"""
    src = flow_to_sexpr(_ir())
    assert ":subscription-timeout 120" in src
    assert _pick_sub_timeout(compile_sexpr(src)["nodes"]) == SUBSCRIPTION_TIMEOUT


def test_sexpr_roundtrip_snake_case_policy_fields():
    """写侧双读：snake IR（``Flow.model_dump()`` 形态）同样不得丢字段。"""
    src = flow_to_sexpr(_ir(camel=False))
    assert ":allowed-hosts" in src
    assert _pick_http(compile_sexpr(src)["nodes"]) == POLICY_VALUES


def test_sexpr_roundtrip_builds_a_flow_with_policy_intact():
    """发射的源码必须能通过 ``Flow.model_validate``，且策略进到节点字段。"""
    flow = parse_sexpr(flow_to_sexpr(_ir()))
    http = [n for n in flow.nodes if n.id == "h"][0]
    assert http.allowed_hosts == POLICY_VALUES["allowedHosts"]
    assert http.denied_hosts == POLICY_VALUES["deniedHosts"]
    assert http.block_private_networks is True
    assert http.request_timeout == 5
    assert http.max_redirects == 2
    event = [n for n in flow.nodes if n.id == "w"][0]
    assert event.subscription_timeout == SUBSCRIPTION_TIMEOUT


def test_sexpr_without_policy_keeps_base_bytes():
    """没有策略字段的 HTTP 节点不新增任何关键字（存量语料零改动）。"""
    ir = {"flow_id": "f", "nodes": [
        {"type": "start", "id": "s", "next": "h"},
        {"type": "http", "id": "h", "method": "GET", "url": "http://example.com",
         "next": "e"},
        {"type": "end", "id": "e"},
    ]}
    src = flow_to_sexpr(ir)
    assert src == ('(flow f (start :id s -> h) '
                   '(http :id h :method GET :url http://example.com -> e) '
                   '(end :id e))')


# ---------------------------------------------------------------------------
# 读侧的响亮拒绝（不许把笔误静默压成默认值）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fragment, message", [
    (':block-private-networks "yes"', "需要 true/false"),
    (':allowed-hosts 42', "需要主机字符串"),
    (':request-timeout 0', "需要正数"),
    (':max-redirects -1', "需要非负整数"),
])
def test_sexpr_reader_rejects_malformed_policy(fragment, message):
    src = ('(flow f (start :id s -> h) '
           f'(http :id h :method GET :url "http://example.com" {fragment} -> e) '
           '(end :id e))')
    with pytest.raises(SyntaxError, match=message):
        compile_sexpr(src)


def test_sexpr_writer_rejects_non_bool_block_private_networks():
    ir = _ir()
    ir["nodes"][1]["blockPrivateNetworks"] = "true"
    with pytest.raises(SexprEmitError, match="blockPrivateNetworks"):
        flow_to_sexpr(ir)


# ---------------------------------------------------------------------------
# @flow（codeflow）
# ---------------------------------------------------------------------------

def test_codeflow_roundtrip_http_policy_and_event_timeout():
    """``emit_source`` → ``compile_source`` 后策略字段与订阅超时逐字段相等。"""
    src = emit_source(_ir())
    for kw in ("allowed_hosts=", "denied_hosts=", "block_private_networks=",
               "request_timeout=", "max_redirects=", "subscription_timeout="):
        assert kw in src
    back = compile_source(src)
    assert _pick_http(back["nodes"]) == POLICY_VALUES
    assert _pick_sub_timeout(back["nodes"]) == SUBSCRIPTION_TIMEOUT


def test_codeflow_roundtrip_snake_case_policy_fields():
    """发射器对 snake IR（``Flow.model_dump()`` 形态）同样双读。"""
    src = emit_source(_ir(camel=False))
    assert "allowed_hosts=" in src
    back = compile_source(src)
    assert _pick_http(back["nodes"]) == POLICY_VALUES


def test_codeflow_recompiled_flow_keeps_policy():
    """重编译成 ``Flow`` 后节点字段仍带着策略（不只是中间 IR）。"""
    flow = flow_from_source(emit_source(_ir()))
    http = [n for n in flow.nodes if n.id == "h"][0]
    assert http.allowed_hosts == POLICY_VALUES["allowedHosts"]
    assert http.block_private_networks is True
    assert http.request_timeout == 5
    assert http.max_redirects == 2
    event = [n for n in flow.nodes if n.id == "w"][0]
    assert event.subscription_timeout == SUBSCRIPTION_TIMEOUT


def test_codeflow_without_policy_keeps_base_source():
    """没有策略字段时不新增 kwarg（既有 @flow 源码输出零变化）。"""
    ir = {"flow_id": "f", "nodes": [
        {"type": "start", "id": "s", "next": "h"},
        {"type": "http", "id": "h", "method": "GET", "url": "http://example.com",
         "next": "w"},
        {"type": "event", "id": "w", "eventType": "evt", "next": "e"},
        {"type": "end", "id": "e", "resultType": "success"},
    ]}
    src = emit_source(ir)
    assert "allowed_hosts" not in src
    assert "subscription_timeout" not in src
