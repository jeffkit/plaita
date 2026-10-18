"""plaita#239②：迁移期观测——写站点接线探针 + 隐式 default 首告 WARNING。

口径：
- default 来源写 ⇒ WARNING **每站点进程内一次**（站点闩）+ 探针计数累加；
- explicit/derived 来源写 ⇒ 不告警；
- 探针自身异常绝不阻断写路径。
"""

import logging

import pytest

from plaita import tenant_context as tc
from plaita.storage.redis import (
    _IMPLICIT_DEFAULT_WARNED,
    _tenant_write_observability,
)


@pytest.fixture(autouse=True)
def _clean_probe_state():
    tc.reset_write_probe()
    _IMPLICIT_DEFAULT_WARNED.clear()
    token = None
    try:
        yield
    finally:
        tc.reset_write_probe()
        _IMPLICIT_DEFAULT_WARNED.clear()
        if token is None:
            # 复位租户上下文（前测的 explicit 设置不得泄漏）
            try:
                tc.set_current_tenant(None, source="default")
            except TypeError:
                pass


def test_default_source_write_warns_once_and_counts(caplog):
    with caplog.at_level(logging.WARNING, logger="plaita"):
        _tenant_write_observability("redis.save_execution_state", "e1")
        _tenant_write_observability("redis.save_execution_state", "e2")
        _tenant_write_observability("redis.save_execution_state", "e3")
    warns = [r for r in caplog.records if "#239 迁移期" in r.message]
    assert len(warns) == 1, f"站点闩失效：{len(warns)} 条"
    snap = tc.write_probe_snapshot()
    assert snap[("redis.save_execution_state", "default")] == 3


def test_explicit_source_write_does_not_warn(caplog):
    tc.set_current_tenant("acme", source="explicit")
    with caplog.at_level(logging.WARNING, logger="plaita"):
        _tenant_write_observability("redis.save_flow", "f1")
        _tenant_write_observability("redis.save_flow", "f2")
    assert not [r for r in caplog.records if "#239 迁移期" in r.message]
    snap = tc.write_probe_snapshot()
    assert snap[("redis.save_flow", "explicit")] == 2


def test_sites_latch_independently(caplog):
    with caplog.at_level(logging.WARNING, logger="plaita"):
        _tenant_write_observability("redis.save_execution_state", "e1")
        _tenant_write_observability("redis.delete_flow", "d1")
        _tenant_write_observability("redis.save_flow", "f1")
        _tenant_write_observability("redis.delete_execution_state", "x1")
    warns = [r for r in caplog.records if "#239 迁移期" in r.message]
    assert len(warns) == 4, f"四站点各一告：{len(warns)} 条"


def test_probe_failure_never_blocks(caplog, monkeypatch):
    def boom(site):
        raise RuntimeError("probe down")
    monkeypatch.setattr(tc, "record_write_probe", boom)
    with caplog.at_level(logging.DEBUG, logger="plaita"):
        _tenant_write_observability("redis.save_execution_state", "e1")
    debugs = [r for r in caplog.records if "观测探针异常" in r.message]
    assert debugs, "探针异常应有 debug 痕迹（可观测纪律）"
