"""#83：allowedHosts 的 CIDR 条目在未同时开 deniedHosts/blockPrivateNetworks 时被忽略。

修复前：``_host_allowed`` 的解析门是 ``if denied_hosts or block_private_networks``
（plaita/node/http.py:307-311，基线 65f48f2 行号），而 CIDR 条目（字段文档承诺
「按解析后 IP 匹配」）依赖 ``resolved_ips``——只配 ``allowedHosts``（CIDR）时
恒不匹配、**全部请求必拒**，且报错文案误导（读起来像目标不在网段内）。

修复后语义：
1. 解析门在 ``allowed_hosts`` 存在含 ``/`` 的条目时也打开；
2. 纯精确域名/通配白名单**不**多付一次 DNS 解析；
3. 解析失败（resolver 返回 None）且命中判定需要 CIDR 时显式报「无法解析」，
   不退化成误导性的 ``not in allowed_hosts`` 文案。
"""
import pytest

from plaita.node.http import URLPolicyError, _host_allowed


def _resolver(ips):
    """构造打点 resolver：记录 (hostname, port) 调用，返回 ips（None 模拟解析失败）。"""
    calls = []

    def resolve(hostname, port):
        calls.append((hostname, port))
        return list(ips) if ips is not None else None

    return resolve, calls


class TestCidrAllowlistWithoutOtherPolicies:
    """只配 allowedHosts（CIDR）：解析门必须为 CIDR 打开（#83 复现面）。"""

    def test_cidr_entry_matches_resolved_ip(self):
        """文档承诺的主场景：``10.0.0.0/8`` 白名单放行 ``10.1.2.3``。"""
        resolve, calls = _resolver(["10.1.2.3"])
        _host_allowed("http://10.1.2.3/x", ["10.0.0.0/8"], None, False, resolver=resolve)
        assert calls == [("10.1.2.3", 80)], "CIDR 条目必须触发一次解析（#83 前为 0 次）"

    def test_cidr_entry_still_rejects_ip_outside_network(self):
        """负向对照：网段外的 IP 必须仍被拒（fail-closed 不松动）。"""
        resolve, _ = _resolver(["192.168.1.1"])
        with pytest.raises(URLPolicyError):
            _host_allowed("http://192.168.1.1/x", ["10.0.0.0/8"], None, False, resolver=resolve)

    def test_exact_only_allowlist_does_not_resolve(self):
        """纯精确域名白名单不得引入额外 DNS 解析（issue 建议的「对齐需求」口径）。"""
        resolve, calls = _resolver(["10.1.2.3"])
        _host_allowed("http://api.example.com/x", ["api.example.com"], None, False, resolver=resolve)
        assert calls == []

    def test_resolution_failure_is_explicit_not_misleading(self):
        """解析失败 + 需要校验 CIDR 条目 ⇒ 显式报「无法解析」，非误导性 not-in-list。"""
        resolve, calls = _resolver(None)
        with pytest.raises(URLPolicyError) as ei:
            _host_allowed(
                "http://no-dns.example.com/x", ["10.0.0.0/8"], None, False, resolver=resolve
            )
        assert calls, "需要校验 CIDR 条目就必须尝试解析（#83 前门条件根本不开）"
        assert "resolve" in str(ei.value).lower(), str(ei.value)

    def test_mixed_list_exact_match_survives_resolution_failure(self):
        """混写列表：hostname 命中精确条目即放行，不因 CIDR 条目解析失败而误拒。"""
        resolve, _ = _resolver(None)
        _host_allowed(
            "http://api.example.com/x", ["api.example.com", "10.0.0.0/8"], None, False,
            resolver=resolve,
        )

    def test_other_policies_unchanged_denied_still_gates(self):
        """既有语义回归：deniedHosts 命中即拒，与 allowed 无关。"""
        resolve, _ = _resolver(["10.1.2.3"])
        with pytest.raises(URLPolicyError):
            _host_allowed(
                "http://10.1.2.3/x", None, ["10.0.0.0/8"], False, resolver=resolve
            )
