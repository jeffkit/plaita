"""执行载体对账器（``scripts/carrier_audit.py``，plaita#316 Stage 0）负控。

覆盖面（对照 owner #316 + A2 r59 评审增补）：

- **五失联格**：mode 1（ack 后崩溃）/ mode 2（租约冲突 ack 唯一载体——
  租约键**不是**载体）/ mode 3（挂起提交窗口）/ mode 4（事件先于订阅）/
  第六类（幽灵 queued 占位行，schedule 入队失败残留）。mode 5 出圈不设格
  （A2：ack 全走 enqueue→ack 安全序，卡死时消息留 PEL ⇒ carrier=pel，
  检测面归 reap_zombie，Stage 1 reconciler 再收）。每格：制造失联态 →
  gauge 命中具名维度 → 修复态 → 归零。
- **四存放处正控锁**：queued×stream 本体消息 = pel 非 none（A2 修正①的
  回归锁）、DLQ / 租户隔离流（第四存放处）、订阅、timer、multiple。
- **违例**：duplicate / orphan_terminal / unbound_subscription。
- **故障注入**：整源读失败 → unknown 不误报 none 且下一轮自愈；损坏状态
  行 → unknown cell + check_errors；不可归属 payload 跳过。
- **只读纪律**：白名单外命令拒；扫描不写任何 Redis 键；落盘 tmp+rename
  原子写、轮失败不覆写产物；配置缺失 fail-visible。

基建：fakeredis；执行行走真实 ``RedisExecutionStorage.save_execution_state``
（行形状与生产同源，不手拼 JSON）。
"""
import importlib.util
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.storage.base import ExecutionState
from plaita.storage.redis import (
    RedisExecutionStorage,
    execution_index_key,
    execution_lease_key,
)
from plaita.tenant_context import (
    DEFAULT_TENANT_ID,
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "carrier_audit", REPO_ROOT / "scripts" / "carrier_audit.py"
)
carrier_audit = importlib.util.module_from_spec(_spec)
sys.modules["carrier_audit"] = carrier_audit
_spec.loader.exec_module(carrier_audit)

NS_DEFAULT = "plaita"  # tenant_namespace(DEFAULT_TENANT_ID)
STREAM = "plaita:flow:queue"
NOW = datetime(2026, 10, 23, 12, 0, 0, tzinfo=timezone.utc).timestamp()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _storage(client, namespace=NS_DEFAULT):
    return RedisExecutionStorage(client=client, namespace=namespace)


def _seed(client, execution_id, status, *, age=300, tenant=DEFAULT_TENANT_ID):
    """经真实存储种一行执行状态（last_update = 启动后 age/2）。"""
    token = set_current_tenant(tenant)
    try:
        store = _storage(client, tenant_namespace(tenant))
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            tenant_id=tenant,
            context={},
            status=status,
            start_time=_iso(NOW - age),
            last_update_time=_iso(NOW - age / 2),
        )
        assert store.save_execution_state(execution_id, state) is True
    finally:
        reset_current_tenant(token)


def _config(tmp_path, tenants="default", streams=STREAM):
    return carrier_audit.load_config(
        redis_url="redis://localhost:6379/9",
        tenants=tenants,
        streams=streams,
        timer_queues="plaita:delay:queue",
        output_dir=str(tmp_path),
    )


def _scan(client, config):
    return carrier_audit.scan_round(
        carrier_audit.ReadOnlyRedis(client), config, now=NOW
    )


def _cell(result, status, carrier, tenant="default"):
    return result.cells.get((tenant, status, carrier), 0)


def _terminalize(client, execution_id, *, tenant=DEFAULT_TENANT_ID):
    """修复态：把执行行终态化（completed），载体四存放处本就为空。"""
    token = set_current_tenant(tenant)
    try:
        store = _storage(client, tenant_namespace(tenant))
        ns = tenant_namespace(tenant)
        raw = client.get(f"{ns}:execution:{execution_id}")
        data = json.loads(raw)
        data["status"] = "completed"
        data["last_update_time"] = _iso(NOW)
        store.save_execution_state(execution_id, ExecutionState.model_validate(data))
    finally:
        reset_current_tenant(token)


class _FlakyRange:
    """只对指定键的 xrange 抛错、其余命令透传——整源读失败注入探针。"""

    def __init__(self, inner, bad_keys):
        self._inner = inner
        self._bad_keys = set(bad_keys)

    def xrange(self, key, **kwargs):
        if key in self._bad_keys:
            raise RuntimeError("synthetic redis read failure")
        return self._inner.xrange(key, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class TestFiveLostCarriers:
    """五失联格：制造 → gauge 命中具名维度 → 修复态 → 归零。"""

    def test_m1_ack_crash_running_none(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m1", "running")
        config = _config(tmp_path)

        result = _scan(client, config)
        assert _cell(result, "running", "none") == 1
        assert result.violations["missing"] == 1
        assert result.missing_samples[0]["execution_id"] == "m1"
        assert result.ages[("default", "running")] == pytest.approx(150.0, abs=1.0)

        _terminalize(client, "m1")
        repaired = _scan(client, config)
        assert _cell(repaired, "running", "none") == 0
        assert repaired.violations["missing"] == 0

    def test_m2_lease_conflict_ack_lease_is_not_carrier(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m2", "running")
        # mode 2 现场：重复消息已被 ack，唯一遗留线索是租约键（持有者已死）。
        # 租约是推进的阻碍物不是载体——有它也不改判 none（A2 判据锁）。
        client.set(execution_lease_key(NS_DEFAULT, "m2"), "dead-holder")
        config = _config(tmp_path)

        result = _scan(client, config)
        assert _cell(result, "running", "none") == 1
        assert result.violations["missing"] == 1

    def test_m3_suspend_commit_window_suspended_none(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m3", "suspended")
        # mode 3 现场：suspended 落盘了，但订阅注册 / 服务 rpush 都没发生。
        config = _config(tmp_path)

        result = _scan(client, config)
        assert _cell(result, "suspended", "none") == 1
        assert result.violations["missing"] == 1

        _terminalize(client, "m3")
        assert _cell(_scan(client, config), "suspended", "none") == 0

    def test_m4_event_before_subscription_suspended_none(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m4", "suspended")
        # mode 4 现场：事件先于订阅到达被静默丢弃——对账面与 mode 3 同形
        # （suspended×0 载体），游标不可回溯是 worker 判据，审计不复制。
        config = _config(tmp_path)

        result = _scan(client, config)
        assert _cell(result, "suspended", "none") == 1
        assert result.violations["missing"] == 1

        # 订阅晚到（另一进程补注册）→ 载体在场，none 熄灭。
        client.set(
            "plaita:subscription:data:s-m4",
            json.dumps(
                {"subscription_id": "s-m4", "event_type": "e", "correlation_id": "m4"}
            ),
        )
        recovered = _scan(client, config)
        assert _cell(recovered, "suspended", "none") == 0
        assert _cell(recovered, "suspended", "subscription") == 1

    def test_m6_ghost_queued_placeholder(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m6", "queued")
        # 第六类现场：fire_schedule 落了 queued 占位行、入队消息被删
        #（入队失败 + _discard_queued_placeholder 清理失败）——A2 验收
        # 增补点名「删占位行的入队消息，留 queued 行」。
        config = _config(tmp_path)

        result = _scan(client, config)
        assert _cell(result, "queued", "none") == 1
        assert result.violations["missing"] == 1

        # 修复态 = 人工清掉幽灵行（#308 删除路径连索引成员一起清）。
        token = set_current_tenant(DEFAULT_TENANT_ID)
        try:
            assert _storage(client).delete_execution_state("m6") is True
        finally:
            reset_current_tenant(token)
        repaired = _scan(client, config)
        assert _cell(repaired, "queued", "none") == 0
        assert repaired.violations["missing"] == 0


class TestCarrierKinds:
    """四存放处正控：在场即命中具名 carrier，绝不误报 none。"""

    def test_queued_with_stream_message_is_pel_not_none(self, tmp_path):
        # A2 修正①回归锁：占位行落盘后消息在 stream 本体等首投（未进 PEL）
        # ——只查 PEL 会把「刚 tick 的调度执行 + 消费积压」误报 none。
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "b1", "queued")
        client.xadd(
            STREAM,
            {"payload": json.dumps({"execution_id": "b1"}), "schema_version": "1"},
        )

        result = _scan(client, _config(tmp_path))
        assert _cell(result, "queued", "pel") == 1
        assert _cell(result, "queued", "none") == 0
        assert result.violations["missing"] == 0

    def test_dlq_only_carrier(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "f1", "suspended")
        client.xadd(
            f"{STREAM}:dlq",
            {"payload": json.dumps({"reason": "r", "payload": {"execution_id": "f1"}})},
        )

        result = _scan(client, _config(tmp_path))
        assert _cell(result, "suspended", "dlq") == 1
        assert _cell(result, "suspended", "none") == 0

    def test_quarantine_stream_carrier_per_tenant(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "q1", "suspended", tenant="acme")
        dlq = f"{STREAM}:dlq"
        client.xadd(
            f"{dlq}:quarantine:acme",
            {"payload": json.dumps({"reason": "r", "payload": {"execution_id": "q1"}})},
        )

        result = _scan(client, _config(tmp_path, tenants="default,acme"))
        assert _cell(result, "suspended", "dlq", tenant="acme") == 1
        assert _cell(result, "suspended", "none", tenant="acme") == 0

    def test_subscription_timer_multiple(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "c1", "suspended")
        client.set(
            "plaita:subscription:data:s1",
            json.dumps({"subscription_id": "s1", "event_type": "e", "correlation_id": "c1"}),
        )
        _seed(client, "d1", "suspended")
        client.rpush("plaita:delay:queue", json.dumps({"execution_id": "d1"}))
        _seed(client, "g1", "suspended")
        client.set(
            "plaita:subscription:data:s2",
            json.dumps({"subscription_id": "s2", "event_type": "e", "correlation_id": "g1"}),
        )
        client.rpush("plaita:delay:queue", json.dumps({"execution_id": "g1"}))

        result = _scan(client, _config(tmp_path))
        assert _cell(result, "suspended", "subscription") == 1
        assert _cell(result, "suspended", "timer") == 1
        assert _cell(result, "suspended", "multiple") == 1
        assert _cell(result, "suspended", "none") == 0

    def test_duplicate_violation(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "dup", "running")
        for _ in range(2):
            client.xadd(
                STREAM,
                {"payload": json.dumps({"execution_id": "dup"}), "schema_version": "1"},
            )

        result = _scan(client, _config(tmp_path))
        assert _cell(result, "running", "pel") == 1
        assert result.violations["duplicate"] == 1
        assert result.violations["missing"] == 0

    def test_orphan_terminal_violation(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "done", "completed")
        client.set(
            "plaita:subscription:data:s3",
            json.dumps(
                {"subscription_id": "s3", "event_type": "e", "correlation_id": "done"}
            ),
        )

        result = _scan(client, _config(tmp_path))
        assert result.violations["orphan_terminal"] == 1
        assert result.violations["missing"] == 0

    def test_unbound_subscription_violation(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        client.set(
            "plaita:subscription:data:s4",
            json.dumps(
                {
                    "subscription_id": "s4",
                    "event_type": "e",
                    "correlation_id": "no-such-execution",
                }
            ),
        )

        result = _scan(client, _config(tmp_path))
        assert result.violations["unbound_subscription"] == 1

    def test_acked_residue_documented_limit(self, tmp_path):
        # 已知边界（模块 docstring）：ack 后 XDEL 失败的残留仍在 stream 本体，
        # 按消息在场记 pel（Stage 0 不复制 residue sweeper 的判据——纪律 3）。
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "res", "running")
        client.xadd(
            STREAM,
            {"payload": json.dumps({"execution_id": "res"}), "schema_version": "1"},
        )

        result = _scan(client, _config(tmp_path))
        assert _cell(result, "running", "pel") == 1


class TestTenants:
    def test_two_tenant_isolation(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "t-default", "running", tenant=DEFAULT_TENANT_ID)
        _seed(client, "t-acme", "queued", tenant="acme")

        result = _scan(client, _config(tmp_path, tenants="default,acme"))
        assert _cell(result, "running", "none", tenant="default") == 1
        assert _cell(result, "queued", "none", tenant="acme") == 1
        # 租户 ns 互不可见：default 的扫描看不到 acme 的行，反之亦然。
        assert _cell(result, "running", "none", tenant="acme") == 0
        assert _cell(result, "queued", "none", tenant="default") == 0
        assert {s["tenant"] for s in result.missing_samples} == {"default", "acme"}


class TestFailureInjection:
    def test_queue_source_failure_yields_unknown_not_none(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m1", "running")
        healthy = carrier_audit.ReadOnlyRedis(client)
        flaky = _FlakyRange(healthy, [STREAM])

        config = _config(tmp_path)
        result = carrier_audit.scan_round(flaky, config, now=NOW)
        # 工具故障绝不翻页成 none 风暴（A2）：unknown 记账、missing 恒 0。
        assert _cell(result, "running", "unknown") == 1
        assert _cell(result, "running", "none") == 0
        assert result.violations["missing"] == 0
        assert result.sources_failed == ["queue"]
        assert result.check_errors >= 1

        # 主循环不受影响：下一轮健康源即恢复具名判定。
        recovered = carrier_audit.scan_round(healthy, config, now=NOW)
        assert _cell(recovered, "running", "none") == 1
        assert recovered.sources_failed == []

    def test_corrupt_state_row_unknown_cell_and_check_error(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        client.zadd(execution_index_key(NS_DEFAULT), {"corrupt1": NOW})
        client.set(f"{NS_DEFAULT}:execution:corrupt1", "{not-json")

        result = _scan(client, _config(tmp_path))
        assert _cell(result, "unknown", "unknown") == 1
        assert result.check_errors >= 1
        assert result.violations["missing"] == 0

    def test_unattributable_payload_skipped(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m1", "running")
        client.xadd(STREAM, {"payload": "not-json", "schema_version": "1"})
        client.xadd(STREAM, {"schema_version": "1"})  # 缺 payload 字段

        result = _scan(client, _config(tmp_path))
        # 畸形消息不归属任何执行：执行本身仍 none（消息不算它的载体）。
        assert _cell(result, "running", "none") == 1
        assert result.check_errors == 0


class TestReadOnlyDiscipline:
    def test_whitelist_rejects_write_commands(self):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        readonly = carrier_audit.ReadOnlyRedis(client)
        for command, args in [
            ("set", ("k", "v")),
            ("xadd", ("s", {"f": "v"})),
            ("delete", ("k",)),
            ("rpush", ("l", "v")),
            ("incr", ("c",)),
            ("xack", ("s", "g", "0-1")),
            ("xdel", ("s", "0-1")),
            ("eval", ("return 1", 0)),
        ]:
            with pytest.raises(carrier_audit.AuditCommandNotAllowed):
                getattr(readonly, command)(*args)
        # 白名单内正常透传。
        assert readonly.exists("nothing") == 0

    def test_scan_writes_no_redis_keys(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m1", "running")
        before = sorted(client.scan_iter(match="*"))

        _scan(client, _config(tmp_path))

        assert sorted(client.scan_iter(match="*")) == before

    def test_flush_atomic_snapshot_and_prom(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m1", "running")
        config = _config(tmp_path)
        result = _scan(client, config)

        carrier_audit.flush(result, config)
        prom = (config.output_dir / "carrier_metrics.prom").read_text(encoding="utf-8")
        snapshot = json.loads(
            (config.output_dir / "carrier_snapshot.json").read_text(encoding="utf-8")
        )
        assert "# TYPE plaita_execution_carrier gauge" in prom
        assert 'plaita_carrier_violations{reason="missing"} 1' in prom
        assert "_total" not in "".join(
            line for line in prom.splitlines() if not line.startswith("#")
        )
        assert snapshot["violations"]["missing"] == 1
        assert snapshot["config"]["tenants"] == ["default"]
        # tmp+rename：不留临时件。
        assert not list(config.output_dir.glob("*.tmp"))

    def test_round_failure_does_not_overwrite_outputs(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "m1", "running")
        config = _config(tmp_path)
        carrier_audit.flush(_scan(client, config), config)
        prom_before = (config.output_dir / "carrier_metrics.prom").read_text(
            encoding="utf-8"
        )

        # 轮级失败（收口落盘不可用）：异常上抛、产物保持上一轮内容。
        with patch.object(
            carrier_audit, "flush", side_effect=OSError("disk full")
        ):
            with pytest.raises(OSError):
                carrier_audit.run_round(config)
        assert (
            config.output_dir / "carrier_metrics.prom"
        ).read_text(encoding="utf-8") == prom_before

    def test_config_missing_fails_visible(self, monkeypatch, tmp_path):
        for name in (
            carrier_audit.ENV_REDIS_URL,
            carrier_audit.ENV_TENANTS,
            carrier_audit.ENV_STREAMS,
        ):
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(carrier_audit.AuditConfigError):
            carrier_audit.load_config()
        with pytest.raises(carrier_audit.AuditConfigError):
            carrier_audit.load_config(redis_url="redis://x", tenants="default")

    def test_main_exit_codes(self, monkeypatch, tmp_path):
        for name in (
            carrier_audit.ENV_REDIS_URL,
            carrier_audit.ENV_TENANTS,
            carrier_audit.ENV_STREAMS,
            carrier_audit.ENV_OUTPUT_DIR,
            carrier_audit.ENV_INTERVAL,
        ):
            monkeypatch.delenv(name, raising=False)
        # 配置缺失 → 2（fail-visible，绝不静默扫空）。
        assert carrier_audit.main([]) == 2
        # 轮级失败 → 1。
        monkeypatch.setenv(carrier_audit.ENV_REDIS_URL, "redis://localhost:6379/9")
        monkeypatch.setenv(carrier_audit.ENV_TENANTS, "default")
        monkeypatch.setenv(carrier_audit.ENV_STREAMS, STREAM)
        monkeypatch.setenv(carrier_audit.ENV_OUTPUT_DIR, str(tmp_path))
        with patch.object(
            carrier_audit, "run_round", side_effect=RuntimeError("boom")
        ):
            assert carrier_audit.main(["--once"]) == 1


class TestPromRendering:
    def test_zero_round_emits_full_cell_grid(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        result = _scan(client, _config(tmp_path, tenants="default,acme"))
        prom = carrier_audit.render_prom(result)

        for tenant in ("default", "acme"):
            for status in ("queued", "running", "suspended"):
                for carrier in (
                    "pel",
                    "subscription",
                    "timer",
                    "dlq",
                    "multiple",
                    "none",
                    "unknown",
                ):
                    line = (
                        "plaita_execution_carrier"
                        f'{{status="{status}",carrier="{carrier}",tenant="{tenant}"}} 0'
                    )
                    assert line in prom
        assert 'plaita_carrier_violations{reason="missing"} 0' in prom
        assert "plaita_carrier_audit_sources_failed 0" in prom
        assert "# TYPE plaita_carrier_audit_last_success_timestamp gauge" in prom

    def test_label_escaping(self):
        assert (
            carrier_audit._escape_label('a"b\\c\nd')
            == 'a\\"b\\\\c\\nd'
        )

    def test_age_max_per_cell(self, tmp_path):
        client = fakeredis.FakeStrictRedis(decode_responses=True)
        _seed(client, "old", "running", age=3600)
        _seed(client, "new", "running", age=60)

        result = _scan(client, _config(tmp_path))
        assert result.ages[("default", "running")] == pytest.approx(1800.0, abs=1.0)
        prom = carrier_audit.render_prom(result)
        assert 'plaita_execution_carrier_age_seconds{status="running",tenant="default"}' in prom
