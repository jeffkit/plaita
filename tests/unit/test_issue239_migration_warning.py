"""plaita#239 phase 2：Redis 写站点租户来源 fail-closed（owner 2026-10-18）。

原 #239② 的「隐式 default 首告 WARNING + 探针异常不阻断」被 owner 裁决取代：
- 来源 ``default``（从未显式/派生设置）⇒ 抛具名
  ``TenantContextMissingError``，**不触达写**；探针仍计数（含被拒尝试）；
- ``explicit`` / ``derived`` 照常写；
- 单租户部署由服务入口显式声明 default（flow_worker/console 启动行）。
"""

from contextvars import Context

import pytest

from plaita import tenant_context as tc
from plaita.core.parallel_executor import ThreadParallelExecutor
from plaita.storage.redis import _tenant_write_observability


@pytest.fixture(autouse=True)
def _clean_probe_state():
    tc.reset_write_probe()
    yield
    tc.reset_write_probe()


def test_default_source_write_raises_named_error_and_counts():
    def _run():
        for _ in range(3):
            with pytest.raises(tc.TenantContextMissingError):
                _tenant_write_observability("redis.save_execution_state", "e")
        return tc.write_probe_snapshot()

    snapshot = Context().run(_run)
    assert snapshot[("redis.save_execution_state", "default")] == 3


def test_default_error_names_site_and_source():
    def _run():
        with pytest.raises(tc.TenantContextMissingError) as ei:
            _tenant_write_observability("redis.save_flow", "f")
        return str(ei.value)

    message = Context().run(_run)
    assert "redis.save_flow" in message
    assert "source='default'" in message


def test_explicit_source_write_succeeds_and_counts():
    token = tc.set_current_tenant("acme", source="explicit")
    try:
        _tenant_write_observability("redis.save_flow", "f1")
        _tenant_write_observability("redis.save_flow", "f2")
        snapshot = tc.write_probe_snapshot()
    finally:
        tc.reset_current_tenant(token)
    assert snapshot[("redis.save_flow", "explicit")] == 2


def test_derived_source_write_succeeds_and_counts():
    token = tc.set_current_tenant("acme", source="explicit")
    try:
        ThreadParallelExecutor(max_workers=1).submit(
            _tenant_write_observability, "redis.save_flow", "f").result()
        snapshot = tc.write_probe_snapshot()
    finally:
        tc.reset_current_tenant(token)
    assert snapshot[("redis.save_flow", "derived")] == 1
