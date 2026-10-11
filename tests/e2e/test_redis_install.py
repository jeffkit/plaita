"""
E2E test: redis install + storage round-trip.
"""
import pytest


class TestRedisInstall:
    """Verify redis extras install and storage import works."""

    def test_redis_storage_import(self):
        """plaita.storage.redis 必须导出两个真实存储类。

        不设 extra 守卫：``redis`` 是惰性可选依赖，本模块缺 extra 时也能导入——
        符号名写错必须直接失败，不能被「extra 未装」的 skip 吞掉（#62）。
        """
        from plaita.storage.redis import RedisExecutionStorage, RedisFlowStorage

        assert RedisExecutionStorage is not None
        assert RedisFlowStorage is not None

    def test_top_level_storage_exports(self):
        """顶层 lazy 导出表指向真实类（#62：曾声明不存在的 RedisStorage）。"""
        pytest.importorskip("redis")
        from plaita import RedisExecutionStorage, RedisFlowStorage
        from plaita.storage.redis import (
            RedisExecutionStorage as CanonicalExecution,
            RedisFlowStorage as CanonicalFlow,
        )

        assert RedisExecutionStorage is CanonicalExecution
        assert RedisFlowStorage is CanonicalFlow

    def test_redis_storage_instantiation(self):
        """存储类可构造（注入 fakeredis 客户端，不连真 Redis）。"""
        fakeredis = pytest.importorskip("fakeredis")
        pytest.importorskip("redis")
        from plaita.storage.redis import RedisExecutionStorage

        storage = RedisExecutionStorage(
            client=fakeredis.FakeStrictRedis(decode_responses=True),
            namespace="plaita-e2e",
        )
        assert storage.namespace == "plaita-e2e"

    def test_fakeredis_round_trip(self):
        """fakeredis 后端执行状态存/取往返。"""
        fakeredis = pytest.importorskip("fakeredis")
        pytest.importorskip("redis")
        from plaita.storage.base import ExecutionState
        from plaita.storage.redis import RedisExecutionStorage

        storage = RedisExecutionStorage(
            client=fakeredis.FakeStrictRedis(decode_responses=True),
            namespace="plaita-e2e",
        )
        state = ExecutionState(execution_id="exec-e2e", context={"key": "value"})
        assert storage.save_execution_state("exec-e2e", state) is True

        loaded = storage.load_execution_state("exec-e2e")
        assert loaded is not None
        assert loaded.execution_id == "exec-e2e"
        assert loaded.context == {"key": "value"}
