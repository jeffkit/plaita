"""plaita#237：租户状态闸 fail-open 必须 WARNING 计数（owner 裁决）。

语义不变：缺失记录 = active（default 单租部署兼容）；但每次命中 fail-open
（无客户端 / 读失败 / 无记录）必须 WARNING 并累加进程内计数，避免闸门静默
失效。停用租户命中 disabled 走正常拒绝，不计数。
"""
from __future__ import annotations

import logging

import pytest

pytest.importorskip("fakeredis")

from plaita.server import tenant_status  # noqa: E402


@pytest.fixture()
def redis_client():
    import fakeredis

    return fakeredis.FakeRedis(decode_responses=True)


def test_missing_record_is_fail_open_with_warning_and_count(redis_client, caplog):
    before = tenant_status.tenant_fail_open_total()
    with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_status"):
        assert tenant_status.tenant_is_active(redis_client, "tenant-x") is True
    assert tenant_status.tenant_fail_open_total() == before + 1
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "无状态记录" in text and "tenant-x" in text


def test_disabled_record_blocks_without_fail_open(redis_client, caplog):
    tenant_status.publish_tenant_status(redis_client, "tenant-x", "disabled")
    before = tenant_status.tenant_fail_open_total()
    with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_status"):
        assert tenant_status.tenant_is_active(redis_client, "tenant-x") is False
    assert tenant_status.tenant_fail_open_total() == before


def test_active_record_does_not_count(redis_client):
    tenant_status.publish_tenant_status(redis_client, "tenant-x", "active")
    before = tenant_status.tenant_fail_open_total()
    assert tenant_status.tenant_is_active(redis_client, "tenant-x") is True
    assert tenant_status.tenant_fail_open_total() == before


def test_no_client_is_fail_open_with_warning_and_count(caplog):
    before = tenant_status.tenant_fail_open_total()
    with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_status"):
        assert tenant_status.tenant_is_active(None, "tenant-x") is True
    assert tenant_status.tenant_fail_open_total() == before + 1
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "无 Redis 客户端" in text and "tenant-x" in text


def test_falsy_tenant_id_does_not_count(caplog):
    before = tenant_status.tenant_fail_open_total()
    with caplog.at_level(logging.WARNING, logger="plaita.server.tenant_status"):
        assert tenant_status.tenant_is_active(None, None) is True
    assert tenant_status.tenant_fail_open_total() == before
    assert not [r for r in caplog.records if "fail-open" in r.getMessage()]
