"""#40 存储面缺口回归。

1. sqlalchemy ``list_executions`` 重建 state_dict 缺 ``tenant_id``/``flow_hash``
   （load 路径齐全）→ 基于 list 的巡检/报表无法按租户归因、flow_hash 读不到；
   存量库缺列靠注释指引手工 ``ALTER TABLE`` → 落地最小版本化 DDL 迁移。
2. redis ``list_executions`` 全命名空间 ``scan_iter`` + 逐键 GET + 完整
   ``model_validate``（含整个 context），排序分页在 Python 内存做 →
   ``{ns}:execution:index`` ZSET（score=start_time、member=execution_id）
   承担列表/分页/过滤。
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta

import pytest

from plaita.storage.base import ExecutionState


def _state(exec_id="e1", start_time=None, **extra):
    return ExecutionState(
        execution_id=exec_id,
        flow_id="f1",
        tenant_id="acme",
        flow_hash="hash-" + exec_id,
        context={"k": "v"},
        status="running",
        start_time=start_time,
        **extra,
    )


# ── 1. redis 执行列表索引 ─────────────────────────────────────────────

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis  # noqa: E402

from plaita.storage.redis import (  # noqa: E402
    RedisExecutionStorage,
    execution_index_ready_key,
    execution_start_time_score,
)

INDEX = "plaita:execution:index"


class TestRedisExecutionIndexMaintenance(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.storage = RedisExecutionStorage(client=self.redis, namespace="plaita")

    def test_save_adds_index_member_with_start_time_score(self):
        self.storage.save_execution_state("e1", _state(start_time="2026-09-01T00:00:00"))
        expected = execution_start_time_score({"start_time": "2026-09-01T00:00:00"})
        self.assertAlmostEqual(self.redis.zscore(INDEX, "e1"), expected, places=3)

    def test_save_without_start_time_keeps_member_at_zero(self):
        self.storage.save_execution_state("e1", _state(start_time=None))
        self.assertEqual(self.redis.zscore(INDEX, "e1"), 0.0)

    def test_save_overwrite_updates_score(self):
        self.storage.save_execution_state("e1", _state(start_time="2026-09-01T00:00:00"))
        self.storage.save_execution_state("e1", _state(start_time="2026-09-05T00:00:00"))
        self.assertAlmostEqual(
            self.redis.zscore(INDEX, "e1"),
            execution_start_time_score({"start_time": "2026-09-05T00:00:00"}),
            places=3,
        )

    def test_delete_removes_index_member(self):
        self.storage.save_execution_state("e1", _state())
        self.storage.delete_execution_state("e1")
        self.assertIsNone(self.redis.zscore(INDEX, "e1"))


class TestRedisListViaIndex(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.storage = RedisExecutionStorage(client=self.redis, namespace="plaita")
        for i in range(5):
            self.storage.save_execution_state(
                f"e{i}", _state(exec_id=f"e{i}", start_time=f"2026-09-0{i + 1}T00:00:00")
            )

    def _no_scan(self):
        def _boom(*a, **kw):
            raise AssertionError("索引就绪后 list 不应再 SCAN 全命名空间")

        self.redis.scan_iter = _boom

    def test_pagination_pushes_down_only_page_is_deserialized(self):
        self.storage.list_executions()  # 首次触发回填，置就绪标记
        self._no_scan()
        loaded = []
        orig_mget = self.redis.mget

        def spy_mget(keys):
            loaded.append(len(keys))
            return orig_mget(keys)

        self.redis.mget = spy_mget
        rows = self.storage.list_executions(limit=2, offset=1)
        self.assertEqual(sum(loaded), 2, "只应反序列化本页 2 条，而非全部 5 条")
        self.assertEqual([r.execution_id for r in rows], ["e1", "e2"])

    def test_order_by_start_time_desc_uses_zrevrange(self):
        self.storage.list_executions()
        self._no_scan()
        rows = self.storage.list_executions(order_by="-start_time", limit=2)
        self.assertEqual([r.execution_id for r in rows], ["e4", "e3"])

    def test_order_by_other_field_still_works(self):
        self.storage.list_executions()
        self._no_scan()
        rows = self.storage.list_executions(order_by="-execution_id")
        self.assertEqual([r.execution_id for r in rows], ["e4", "e3", "e2", "e1", "e0"])

    def test_query_filter_via_index(self):
        self.storage.list_executions()
        self._no_scan()
        rows = self.storage.list_executions(query={"flow_id": "f1"})
        self.assertEqual(len(rows), 5)
        rows = self.storage.list_executions(query={"flow_id": "nope"})
        self.assertEqual(rows, [])
        rows = self.storage.list_executions(query={"tenant_id": "acme"}, limit=2)
        self.assertEqual([r.execution_id for r in rows], ["e0", "e1"])

    def test_list_returns_full_state_including_context(self):
        rows = self.storage.list_executions()
        self.assertTrue(all(r.context == {"k": "v"} for r in rows))
        self.assertTrue(all(r.tenant_id == "acme" for r in rows))
        self.assertTrue(all(r.flow_hash.startswith("hash-") for r in rows))

    def test_page_with_stale_members_refills(self):
        """索引成员悬空（状态键过期）时本页仍应填满，不出现空格。"""
        self.storage.list_executions()  # 置就绪标记
        self.redis.delete("plaita:execution:e0")
        self.redis.delete("plaita:execution:e1")
        rows = self.storage.list_executions(limit=2, offset=0)
        self.assertEqual([r.execution_id for r in rows], ["e2", "e3"])

    def test_stale_backlog_beyond_one_page_still_lists_live_rows(self):
        """头部悬空成员积压超过两页时仍须返回实存执行——不得返回空列表。

        重取一次只剪 ≤ 2×limit 个成员（剪枝只发生在本页，而默认/``offset=0``
        的读总从头部开始）：终态 TTL 批量过期或外部批量删键（console
        ``DELETE /executions/{id}``、运维脚本）会在头部堆出多页悬空，单次重取
        会让列表在**有活执行**时返回 []（旧 SCAN 实现不会）。
        """
        self.storage.list_executions()  # 置就绪标记
        self.redis.zadd(INDEX, {f"stale{i:03d}": 0.0 for i in range(250)})
        rows = self.storage.list_executions()
        self.assertEqual([r.execution_id for r in rows], ["e0", "e1", "e2", "e3", "e4"])
        self.assertEqual(self.redis.zcard(INDEX), 5, "悬空成员须全部被剪掉")

    def test_pager_enumerates_every_execution_after_stale_backlog(self):
        """reaper 的翻页循环（``offset += len(page)``，空页停）不被短页截断。

        短页被当成数据结束 = 巡检静默截断成 no-op（300 个执行只看到 50/0 个）。
        """
        storage = RedisExecutionStorage(client=self.redis, namespace="rig")
        base = datetime(2026, 1, 1)
        for i in range(300):
            storage.save_execution_state(
                f"live{i:03d}",
                _state(
                    exec_id=f"live{i:03d}",
                    start_time=(base + timedelta(seconds=i)).isoformat(),
                ),
            )
        storage.list_executions()  # 置就绪标记
        self.redis.zadd("rig:execution:index", {f"stale{i:03d}": 0.0 for i in range(250)})

        seen = []
        offset = 0
        while True:
            page = storage.list_executions(limit=100, offset=offset)
            if not page:
                break
            seen.extend(r.execution_id for r in page)
            offset += len(page)

        self.assertEqual(seen, [f"live{i:03d}" for i in range(300)])

    def test_unparseable_state_skipped_not_pruned(self):
        """状态键还在但反序列化失败（跨版本 schema）→ 跳过该行，不剪索引成员。

        剪掉等于让这行从列表永久消失（且降级回旧版本也读不到），比列表少一行糟。
        """
        self.storage.list_executions()  # 置就绪标记
        self.redis.set("plaita:execution:broken", "{not-json")
        self.redis.zadd(INDEX, {"broken": 0.5})
        rows = self.storage.list_executions()
        self.assertNotIn("broken", [r.execution_id for r in rows])
        self.assertEqual(len(rows), 5)
        self.assertIsNotNone(self.redis.zscore(INDEX, "broken"), "反序列化失败不得剪索引")

    def test_dangling_index_member_pruned_on_read(self):
        self.storage.list_executions()
        # 状态键被 TTL/外部删除 → 索引成员悬空
        self.redis.set(execution_index_ready_key("plaita"), "1")
        self.redis.zadd(INDEX, {"ghost": 1.0})
        rows = self.storage.list_executions()
        self.assertNotIn("ghost", [r.execution_id for r in rows])
        self.assertIsNone(self.redis.zscore(INDEX, "ghost"))


class TestRedisIndexBackfill(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.storage = RedisExecutionStorage(client=self.redis, namespace="plaita")

    def test_legacy_keys_backfilled_once(self):
        """升级前写入的执行（无索引成员）不应从列表静默消失。"""
        legacy = _state(exec_id="legacy1", start_time="2026-08-01T00:00:00")
        self.redis.set(
            "plaita:execution:legacy1", self.storage.serialize_state(legacy.model_dump())
        )
        self.assertIsNone(self.redis.zscore(INDEX, "legacy1"))

        rows = self.storage.list_executions()
        self.assertEqual([r.execution_id for r in rows], ["legacy1"])
        self.assertIsNotNone(self.redis.zscore(INDEX, "legacy1"))
        self.assertTrue(self.redis.exists(execution_index_ready_key("plaita")))

    def test_backfill_ignores_mechanism_and_index_keys(self):
        self.redis.set("plaita:execution:lease:x", "holder:1")
        self.redis.set("plaita:execution:fence:x", "1")
        self.redis.set("plaita:execution:cancel:x", "1")
        self.storage.save_execution_state("e1", _state())
        rows = self.storage.list_executions()
        self.assertEqual([r.execution_id for r in rows], ["e1"])

    def test_index_and_ready_keys_are_not_members(self):
        """索引/就绪键本身在 ``{ns}:execution:*`` 扫描范围内，必须先排除。

        否则回填对 ZSET 键发 GET → WRONGTYPE，整批 ZADD 作废、就绪标记不置位
        （#40 review：list 每次全扫 + legacy 执行从列表消失）。
        """
        self.assertIsNone(self.storage._index_member_id(INDEX))
        self.assertIsNone(self.storage._index_member_id(execution_index_ready_key("plaita")))
        self.assertEqual(self.storage._index_member_id("plaita:execution:e1"), "e1")

    def test_legacy_key_backfilled_after_post_upgrade_save(self):
        """生产时序：worker 先 save（索引 ZSET 已存在）→ 首个 list 才回填存量键。

        save 先于 list 是常态（有 worker 在跑）；此时索引键已存在，回填若不排除
        它就会 WRONGTYPE 整批作废，存量执行永远进不了列表。
        """
        self.storage.save_execution_state(
            "new1", _state(exec_id="new1", start_time="2026-09-01T00:00:00")
        )
        legacy = _state(exec_id="legacy1", start_time="2026-08-01T00:00:00")
        self.redis.set(
            "plaita:execution:legacy1", self.storage.serialize_state(legacy.model_dump())
        )

        rows = self.storage.list_executions()
        self.assertEqual([r.execution_id for r in rows], ["legacy1", "new1"])
        self.assertTrue(self.redis.exists(execution_index_ready_key("plaita")))

    def test_unreadable_state_key_does_not_discard_backfill_batch(self):
        """单个键 GET 抛错只跳过该键——其余存量键仍要进索引、就绪标记仍要置位。"""
        for eid in ("legacy1", "legacy2"):
            legacy = _state(exec_id=eid, start_time="2026-08-01T00:00:00")
            self.redis.set(
                f"plaita:execution:{eid}", self.storage.serialize_state(legacy.model_dump())
            )
        self.storage.save_execution_state(
            "new1", _state(exec_id="new1", start_time="2026-09-01T00:00:00")
        )

        orig_get = self.redis.get

        def flaky_get(key):
            if key == "plaita:execution:legacy2":
                raise RuntimeError("boom")
            return orig_get(key)

        self.redis.get = flaky_get
        rows = self.storage.list_executions()
        self.assertEqual([r.execution_id for r in rows], ["legacy1", "new1"])
        self.assertTrue(self.redis.exists(execution_index_ready_key("plaita")))

    def test_ready_flag_skips_backfill_scan(self):
        """回填置位后 list 不再 SCAN（回填缺陷曾让就绪标记永远写不上）。"""
        self.redis.set("plaita:execution:lease:x", "holder:1")
        self.storage.save_execution_state(
            "new1", _state(exec_id="new1", start_time="2026-09-01T00:00:00")
        )
        self.storage.list_executions()
        self.assertTrue(self.redis.exists(execution_index_ready_key("plaita")))

        def _boom(*a, **kw):
            raise AssertionError("就绪后 list 不应再 SCAN 全命名空间")

        self.redis.scan_iter = _boom
        self.assertEqual(
            [r.execution_id for r in self.storage.list_executions()], ["new1"]
        )


class TestFencedSaveMaintainsIndex(unittest.TestCase):
    def setUp(self):
        pytest.importorskip("lupa")
        self.redis = fakeredis.FakeStrictRedis(decode_responses=True)

    def test_fenced_lua_zadds_index_member(self):
        """fenced CAS Lua 直写状态键，必须在脚本内同步维护索引。"""
        from plaita.server.execution_lease import RedisExecutionLease
        from plaita.storage.fenced import (
            FencedExecutionStorage,
            reset_current_fence_token,
            set_current_fence_token,
        )

        storage = FencedExecutionStorage(RedisExecutionStorage(client=self.redis))
        lease = RedisExecutionLease(self.redis)
        gen = lease.try_acquire_fenced("e1", "h1", 60)
        assert gen is not None
        token = set_current_fence_token(gen)
        try:
            assert storage.save_execution_state(
                "e1", _state(start_time="2026-09-01T00:00:00")
            ) is True
        finally:
            reset_current_fence_token(token)

        self.assertAlmostEqual(
            self.redis.zscore(INDEX, "e1"),
            execution_start_time_score({"start_time": "2026-09-01T00:00:00"}),
            places=3,
        )

    def test_fenced_refused_write_does_not_touch_index(self):
        from plaita.storage.fenced import (
            FencedExecutionStorage,
            reset_current_fence_token,
            set_current_fence_token,
        )

        storage = FencedExecutionStorage(RedisExecutionStorage(client=self.redis))
        token = set_current_fence_token(7)  # fence 键缺失 → CAS 拒绝
        try:
            with pytest.raises(Exception):
                storage.save_execution_state("e1", _state())
        finally:
            reset_current_fence_token(token)
        self.assertIsNone(self.redis.zscore(INDEX, "e1"))


class TestConsoleMechanismIndexKeys(unittest.TestCase):
    def test_is_mechanism_key_excludes_index_keys(self):
        from tests.unit.test_console_mechanism_keys import get_is_mechanism_key

        f = get_is_mechanism_key()
        self.assertTrue(f("plaita:execution:index"))
        self.assertTrue(f("plaita:execution:index:ready"))
        self.assertTrue(f("plaita:tenant-a:execution:index"))
        self.assertFalse(f("plaita:execution:abcdef012345"))


# ── 2. sqlalchemy list 字段 + 版本化 DDL 迁移 ─────────────────────────

OLD_EXECUTION_STATES_DDL = """
CREATE TABLE execution_states (
    execution_id VARCHAR(50) PRIMARY KEY,
    flow_id VARCHAR(100),
    flow_name VARCHAR(100),
    flow_version VARCHAR(50),
    context JSON NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time VARCHAR(50),
    last_update_time VARCHAR(50),
    end_time VARCHAR(50),
    error JSON,
    invoker VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME
)
"""


class TestSqlalchemyListFieldsAndMigration(unittest.TestCase):
    """需要 sqlalchemy + aiosqlite；缺依赖自动跳过。"""

    def setUp(self):
        pytest.importorskip("sqlalchemy")
        pytest.importorskip("aiosqlite")
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        from plaita.core.async_utils import run_async_from_sync

        self._text = text
        self._run = run_async_from_sync
        self.engine = create_async_engine("sqlite+aiosqlite://")

        async def _seed():
            async with self.engine.begin() as conn:
                await conn.execute(text(OLD_EXECUTION_STATES_DDL))

        run_async_from_sync(_seed())

    def tearDown(self):
        self._run(self.engine.dispose())

    def _storage(self):
        from plaita.storage.sqlalchemy import SqlalchemyExecutionStorage

        return SqlalchemyExecutionStorage(engine=self.engine)

    def test_migration_adds_missing_columns_and_records_version(self):
        storage = self._storage()

        async def _check():
            async with self.engine.begin() as conn:
                res = await conn.execute(self._text("PRAGMA table_info(execution_states)"))
                cols = {row[1] for row in res}
                versions = [
                    row[0]
                    for row in await conn.execute(
                        self._text("SELECT version FROM schema_migrations")
                    )
                ]
            assert {"flow_hash", "tenant_id", "usage"} <= cols
            assert versions == [1, 2]
            # 迁移后 INSERT 路径可用（老库此前 INSERT 必炸）
            ok = await storage.save_execution_state("e1", _state(start_time=None))
            assert ok is True

        self._run(_check())

    def test_migration_is_idempotent(self):
        self._storage()
        self._storage()  # 二次构造：迁移已记录，不重复执行

        async def _check():
            async with self.engine.begin() as conn:
                versions = [
                    row[0]
                    for row in await conn.execute(
                        self._text("SELECT version FROM schema_migrations")
                    )
                ]
            assert versions == [1, 2]

        self._run(_check())

    def test_list_executions_carries_tenant_id_and_flow_hash(self):
        storage = self._storage()

        async def _check():
            await storage.save_execution_state("e1", _state(exec_id="e1"))
            await storage.save_execution_state("e2", _state(exec_id="e2"))
            rows = await storage.list_executions(order_by="execution_id")
            got = {r.execution_id: (r.tenant_id, r.flow_hash) for r in rows}
            assert got == {"e1": ("acme", "hash-e1"), "e2": ("acme", "hash-e2")}

        self._run(_check())


if __name__ == "__main__":
    unittest.main()
