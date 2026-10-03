"""SQLAlchemy 执行状态存储回归（2026-10-03 遗留小账清偿）。

基线缺陷（第一波 Track P1 回报、本轮修）：``save_execution_state`` 的
INSERT 路径 ``ExecutionStateModel(execution_id=…, **state_dict)`` 双重传参
（state_dict 里本就有 execution_id）→ TypeError；且模型缺 ``tenant_id`` 列
（state_dict 含之）→ 即便不重复传参也炸。净效果：INSERT 恒失败 → save
返回 False →（波次一后）StatePersistError。UPDATE 路径靠 setattr 对未知
字段静默忽略，掩住了问题。

顺带澄清：``task_queue._reclaim_one`` 的 delivery_count 不做 +1——上报值
= 前一次投递序号，配两侧 ``>= max_deliveries`` 检查净值恰为「N 次处理后
死信」，语义正确（详见该函数注释），本轮只修了误导性注释。
"""
import unittest

import pytest

pytest.importorskip("sqlalchemy")

from plaita.storage.base import ExecutionState


def _state(execution_id="exec-sq-1", status="running", **kw):
    kw.setdefault("context", {"$LAST_NODE": "start", "$NODE": {}})
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        tenant_id="acme",
        status=status,
        **kw,
    )


class TestSqlalchemyExecutionStateModel(unittest.TestCase):
    def test_model_has_nullable_tenant_column(self):
        from plaita.storage.sqlalchemy import ExecutionStateModel

        col = ExecutionStateModel.__table__.columns["tenant_id"]
        assert col.nullable is True


class TestSqlalchemyExecutionStateRoundTrip(unittest.TestCase):
    def _storage(self):
        pytest.importorskip("aiosqlite")
        from sqlalchemy.ext.asyncio import create_async_engine

        from plaita.storage.sqlalchemy import SqlalchemyExecutionStorage

        # 构造须在同步段：create_tables=True 时存储自建表（内部 asyncio.run，
        # 在 run_async_from_sync 的 loop 里构造会嵌套炸）；构造器是
        # (database_url=None, engine=None) 关键字签名——位置传参会把 engine
        # 落进 database_url 炸 ArgumentError
        engine = create_async_engine("sqlite+aiosqlite://")
        return SqlalchemyExecutionStorage(engine=engine), engine

    def test_insert_then_load_roundtrip(self):
        """INSERT 路径此前恒 TypeError（重复 execution_id + 未知列）——回归钉。"""
        storage, engine = self._storage()

        async def _test():
            ok = await storage.save_execution_state("exec-sq-1", _state())
            assert ok is True
            got = await storage.load_execution_state("exec-sq-1")
            assert got is not None
            assert got.tenant_id == "acme"
            assert got.flow_id == "f1"
            assert got.status == "running"
            assert got.context == {"$LAST_NODE": "start", "$NODE": {}}

        from plaita.core.async_utils import run_async_from_sync

        try:
            run_async_from_sync(_test())
        finally:
            run_async_from_sync(engine.dispose())

    def test_update_existing_row(self):
        """UPDATE 路径同走列过滤——二次 save 覆盖字段且不炸。"""
        storage, engine = self._storage()

        async def _test():
            assert await storage.save_execution_state("exec-sq-2", _state("exec-sq-2")) is True
            assert await storage.save_execution_state(
                "exec-sq-2", _state("exec-sq-2", status="completed", context={"done": True})
            ) is True
            got = await storage.load_execution_state("exec-sq-2")
            assert got.status == "completed"
            assert got.context == {"done": True}
            assert got.tenant_id == "acme"

        from plaita.core.async_utils import run_async_from_sync

        try:
            run_async_from_sync(_test())
        finally:
            run_async_from_sync(engine.dispose())


if __name__ == "__main__":
    unittest.main()
