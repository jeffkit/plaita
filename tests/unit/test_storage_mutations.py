"""变异测试专项断言 — plaita.storage (memory + base)

针对 storage/memory.py 的 42 个 survived 变异精准杀灭。
主要变异类别：
  1. save_execution_state: return True → False
  2. list_executions: 各种条件/比较变化
  3. save_flow: version 默认值 "latest" → None
  4. get_flow: "latest" key name, numeric version sorting
"""
from __future__ import annotations
import unittest

from plaita.storage.base import ExecutionState, ExecutionStorage
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _make_state(execution_id="ex1", flow_id="flow1", status="running", context=None):
    return ExecutionState(
        execution_id=execution_id,
        flow_id=flow_id,
        status=status,
        context=context or {},
    )


class TestMemoryExecutionStorageSave(unittest.TestCase):

    def test_save_returns_true(self):
        """_2: save_execution_state return True → False。"""
        storage = MemoryExecutionStorage()
        state = _make_state()
        result = storage.save_execution_state("ex1", state)
        self.assertTrue(result)

    def test_save_persists_state(self):
        """保存后能 load 回来。"""
        storage = MemoryExecutionStorage()
        state = _make_state(execution_id="ex1", flow_id="f1", status="done")
        storage.save_execution_state("ex1", state)
        loaded = storage.load_execution_state("ex1")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.execution_id, "ex1")
        self.assertEqual(loaded.flow_id, "f1")
        self.assertEqual(loaded.status, "done")

    def test_save_overwrite(self):
        storage = MemoryExecutionStorage()
        s1 = _make_state(status="running")
        s2 = _make_state(status="done")
        storage.save_execution_state("ex1", s1)
        storage.save_execution_state("ex1", s2)
        loaded = storage.load_execution_state("ex1")
        self.assertEqual(loaded.status, "done")

    def test_load_missing_returns_none(self):
        storage = MemoryExecutionStorage()
        self.assertIsNone(storage.load_execution_state("nonexistent"))

    def test_delete_existing(self):
        storage = MemoryExecutionStorage()
        storage.save_execution_state("ex1", _make_state())
        result = storage.delete_execution_state("ex1")
        self.assertTrue(result)
        self.assertIsNone(storage.load_execution_state("ex1"))

    def test_delete_nonexistent(self):
        storage = MemoryExecutionStorage()
        result = storage.delete_execution_state("not_there")
        self.assertFalse(result)


class TestMemoryExecutionStorageList(unittest.TestCase):

    def setUp(self):
        self.storage = MemoryExecutionStorage()
        for i in range(5):
            state = _make_state(
                execution_id=f"ex{i}",
                flow_id=f"flow{i % 2}",
                status="done" if i % 2 == 0 else "running",
            )
            self.storage.save_execution_state(f"ex{i}", state)

    def test_list_all(self):
        """_1: list_executions 基本功能。"""
        results = self.storage.list_executions()
        self.assertEqual(len(results), 5)

    def test_list_with_query(self):
        """_11: query 过滤功能——只返回匹配 flow_id 的条目。"""
        results = self.storage.list_executions(query={"flow_id": "flow0"})
        self.assertGreater(len(results), 0)
        for r in results:
            self.assertEqual(r.flow_id, "flow0")

    def test_list_query_no_match(self):
        """_15: query 不匹配时返回空列表。"""
        results = self.storage.list_executions(query={"flow_id": "nonexistent_flow"})
        self.assertEqual(results, [])

    def test_list_with_limit(self):
        """_17: limit 参数应截断结果。"""
        results = self.storage.list_executions(limit=2)
        self.assertLessEqual(len(results), 2)

    def test_list_with_offset(self):
        """offset 参数应跳过前 N 条。"""
        all_results = self.storage.list_executions()
        offset_results = self.storage.list_executions(offset=2)
        self.assertEqual(len(offset_results), len(all_results) - 2)

    def test_list_order_by_field(self):
        """order_by 按字段排序。"""
        results = self.storage.list_executions(order_by="execution_id")
        ids = [r.execution_id for r in results]
        self.assertEqual(ids, sorted(ids))

    def test_list_order_by_desc(self):
        """order_by '-field' 倒序排列。"""
        results = self.storage.list_executions(order_by="-execution_id")
        ids = [r.execution_id for r in results]
        self.assertEqual(ids, sorted(ids, reverse=True))


class TestMemoryFlowStorageSave(unittest.TestCase):

    def test_save_returns_true(self):
        storage = MemoryFlowStorage()
        result = storage.save_flow({"flow_id": "f1", "name": "test"})
        self.assertTrue(result)

    def test_save_without_flow_id_returns_false(self):
        storage = MemoryFlowStorage()
        result = storage.save_flow({"name": "no_id"})
        self.assertFalse(result)

    def test_save_default_version_is_latest(self):
        """_11: version 默认值 "latest" → None。"""
        storage = MemoryFlowStorage()
        storage.save_flow({"flow_id": "f1"})
        # 不传 version，应存入 "latest" key
        self.assertIn("latest", storage.flows["f1"])

    def test_save_with_explicit_version(self):
        """_13: version = flow.get("version", ...) 应读取 version 字段。"""
        storage = MemoryFlowStorage()
        storage.save_flow({"flow_id": "f1", "version": "1.0.0"})
        self.assertIn("1.0.0", storage.flows["f1"])

    def test_save_creates_flow_id_entry(self):
        """_16: flow_id not in self.flows 条件。"""
        storage = MemoryFlowStorage()
        storage.save_flow({"flow_id": "new_flow"})
        self.assertIn("new_flow", storage.flows)

    def test_save_multiple_versions(self):
        """_17: 多版本存储。"""
        storage = MemoryFlowStorage()
        storage.save_flow({"flow_id": "f1", "version": "1.0.0", "v": 1})
        storage.save_flow({"flow_id": "f1", "version": "2.0.0", "v": 2})
        self.assertIn("1.0.0", storage.flows["f1"])
        self.assertIn("2.0.0", storage.flows["f1"])

    def test_save_flow_with_id_key(self):
        """支持 'id' 作为 flow_id 备用键。"""
        storage = MemoryFlowStorage()
        result = storage.save_flow({"id": "f2", "name": "by_id"})
        self.assertTrue(result)
        self.assertIn("f2", storage.flows)


class TestMemoryFlowStorageGet(unittest.TestCase):

    def setUp(self):
        self.storage = MemoryFlowStorage()
        self.storage.save_flow({"flow_id": "f1", "name": "latest_flow"})  # → "latest"
        self.storage.save_flow({"flow_id": "f1", "version": "1.0.0", "name": "v1"})
        self.storage.save_flow({"flow_id": "f1", "version": "2.0.0", "name": "v2"})

    def test_get_missing_flow_returns_none(self):
        result = self.storage.get_flow("nonexistent")
        self.assertIsNone(result)

    def test_get_specific_version(self):
        """_6: version 参数匹配直接返回。"""
        result = self.storage.get_flow("f1", version="1.0.0")
        self.assertEqual(result["name"], "v1")

    def test_get_latest_version_key(self):
        """_7: "latest" in versions 条件。"""
        result = self.storage.get_flow("f1")
        self.assertEqual(result["name"], "latest_flow")

    def test_get_numeric_latest_when_no_latest_key(self):
        """_11-24: 数字版本排序——应返回版本号最大的那个。"""
        storage = MemoryFlowStorage()
        storage.save_flow({"flow_id": "f2", "version": "1.0.0", "name": "v1"})
        storage.save_flow({"flow_id": "f2", "version": "2.0.0", "name": "v2"})
        storage.save_flow({"flow_id": "f2", "version": "10.0.0", "name": "v10"})
        result = storage.get_flow("f2")
        self.assertEqual(result["name"], "v10")

    def test_get_version_not_found_falls_back(self):
        """指定不存在的 version，应回退到默认策略。"""
        result = self.storage.get_flow("f1", version="99.0.0")
        # 不应报错，回退到 latest 或任意版本
        self.assertIsNotNone(result)

    def test_get_any_version_when_no_latest_or_numeric(self):
        """_15-17: 无 latest 且非数字版本时，返回任意一个版本。"""
        storage = MemoryFlowStorage()
        storage.save_flow({"flow_id": "f3", "version": "alpha", "name": "alpha_flow"})
        result = storage.get_flow("f3")
        self.assertEqual(result["name"], "alpha_flow")

    def test_get_none_versions_dict(self):
        """empty versions dict 返回 None。"""
        storage = MemoryFlowStorage()
        storage.flows["f_empty"] = {}
        result = storage.get_flow("f_empty")
        self.assertIsNone(result)


class TestSerializeDeserializeState(unittest.TestCase):
    """ExecutionStorage.serialize_state / deserialize_state 的精确断言。

    覆盖正常路径（json.dumps/loads 透传）+ 异常路径（logger.error 的格式串
    与异常文本均进日志，且原异常被 re-raise）——杀 14 个 logger.error 参数变异。
    """

    def setUp(self):
        self.storage = MemoryExecutionStorage()

    def test_serialize_round_trips_dict(self):
        state = {"execution_id": "ex1", "status": "running", "n": 42}
        s = self.storage.serialize_state(state)
        self.assertEqual(self.storage.deserialize_state(s), state)

    def test_serialize_handles_nested_and_list(self):
        state = {"a": [1, 2, {"b": 3}], "c": None}
        s = self.storage.serialize_state(state)
        self.assertEqual(self.storage.deserialize_state(s), state)

    def test_serialize_returns_str(self):
        s = self.storage.serialize_state({"x": 1})
        self.assertIsInstance(s, str)

    def test_deserialize_returns_dict(self):
        d = self.storage.deserialize_state('{"x": 1}')
        self.assertEqual(d, {"x": 1})

    def test_serialize_raises_and_logs_on_unserializable(self):
        # set 不可 JSON 序列化 → json.dumps 抛 TypeError → logger.error 后 re-raise
        with self.assertRaises(TypeError):
            self.storage.serialize_state({"bad": {1, 2, 3}})

    def test_serialize_log_has_format_string_and_error_text(self):
        # 杀 logger.error(None, e) / logger.error(fmt, None) / logger.error(e) /
        # logger.error(fmt, ) / logger.error("XXfmtXX", e) / logger.error("lower", e) /
        # logger.error("UPPER: %S", e) 等参数变异
        with self.assertLogs("plaita", level="ERROR") as cm:
            with self.assertRaises(TypeError):
                self.storage.serialize_state({"bad": {1, 2}})
        msgs = [m for m in cm.output if "serialize" in m]
        self.assertTrue(len(msgs) >= 1, f"expected serialize error log, got {cm.output}")
        combined = " ".join(msgs)
        self.assertIn("Failed to serialize state", combined)
        # 异常文本应进日志（%s 把异常转成 "Object of type set is not JSON serializable"）
        self.assertIn("not JSON serializable", combined)
        self.assertNotIn("XX", combined)
        self.assertNotIn("%S", combined)

    def test_deserialize_raises_and_logs_on_invalid_json(self):
        with self.assertRaises(Exception):
            self.storage.deserialize_state("{not valid json")

    def test_deserialize_log_has_format_string_and_error_text(self):
        # 杀 logger.error(fmt, None) / logger.error(fmt, ) ——异常文本必须进日志
        with self.assertLogs("plaita", level="ERROR") as cm:
            with self.assertRaises(Exception):
                self.storage.deserialize_state("{not valid json")
        msgs = [m for m in cm.output if "deserialize" in m]
        self.assertTrue(len(msgs) >= 1, f"expected deserialize error log, got {cm.output}")
        combined = " ".join(msgs)
        self.assertIn("Failed to deserialize state", combined)
        # 异常文本应进日志（%s 把 JSONDecodeError 转成 "Expecting property name..."）
        self.assertIn("Expecting", combined)
        self.assertNotIn("XX", combined)
        self.assertNotIn("%S", combined)


if __name__ == "__main__":
    unittest.main()
