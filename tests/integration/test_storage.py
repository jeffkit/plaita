import unittest
import time
import copy  # 添加copy模块
from typing import Dict, Any

from plaita.storage import ExecutionStorage, MemoryExecutionStorage, ExecutionState
from .test_storage_commons import StorageTestMixin, get_standard_test_cases


class TestStateStorageBase(unittest.TestCase):
    """StateStorage基类测试，主要测试序列化方法"""
    
    def setUp(self):
        class TestStorage(ExecutionStorage):
            """用于测试的StateStorage实现"""
            def save_execution_state(self, execution_id, state): pass
            def load_execution_state(self, execution_id): pass
            def delete_execution_state(self, execution_id): pass
            def list_executions(self, query=None, order_by=None, limit=100, offset=0): pass
            
        self.storage = TestStorage()
    
    def test_serialize_deserialize(self):
        """测试状态序列化与反序列化"""
        state = {
            "string": "test",
            "int": 123,
            "float": 45.67,
            "bool": True,
            "list": [1, 2, 3],
            "dict": {"a": 1, "b": 2},
            "none": None
        }
        
        serialized = self.storage.serialize_state(state)
        self.assertIsInstance(serialized, str)
        
        deserialized = self.storage.deserialize_state(serialized)
        self.assertEqual(deserialized, state)


class TestMemoryStateStorage(unittest.TestCase, StorageTestMixin):
    """MemoryStateStorage实现测试（自定义实现）"""
    
    def get_storage_instance(self) -> ExecutionStorage:
        """获取存储实例"""
        if not hasattr(self, '_storage'):
            self._storage = MemoryExecutionStorage()
        return self._storage
    
    def setUp(self):
        """初始化测试"""
        self.storage = self.get_storage_instance()
        # 每次测试前清空存储状态
        self.storage.execution_states = {}
    
    def test_execution_state(self):
        """测试执行状态的保存、加载和删除"""
        # 保存状态
        execution_id = "test-exec-1"
        state = ExecutionState(
            execution_id=execution_id,
            context={"key": "value"},
            status="running"
        )
        
        # 保存状态
        result = self.storage.save_execution_state(execution_id, state)
        self.assertTrue(result)
        
        # 加载状态
        loaded_state = self.storage.load_execution_state(execution_id)
        self.assertIsNotNone(loaded_state)
        self.assertEqual(loaded_state.execution_id, execution_id)
        self.assertEqual(loaded_state.context["key"], "value")
        self.assertEqual(loaded_state.status, "running")
        
        # 删除状态
        result = self.storage.delete_execution_state(execution_id)
        self.assertTrue(result)
        
        loaded_state = self.storage.load_execution_state(execution_id)
        self.assertIsNone(loaded_state)
        
        # 删除不存在的状态
        result = self.storage.delete_execution_state("non-existent")
        self.assertFalse(result)
    
    def test_list_executions(self):
        """测试列出执行状态"""
        # 保存多个执行状态
        execution_ids = [f"exec-{i}" for i in range(5)]
        for exec_id in execution_ids:
            state = ExecutionState(
                execution_id=exec_id,
                context={"id": exec_id},
                status="running"
            )
            self.storage.save_execution_state(exec_id, state)
            
        # 列出全部
        listed = self.storage.list_executions()
        self.assertEqual(len(listed), 5)
        
        # 验证返回的是ExecutionState对象
        for state in listed:
            self.assertIsInstance(state, ExecutionState)
            self.assertIn(state.execution_id, execution_ids)
        
        # 带分页
        listed = self.storage.list_executions(limit=2, offset=1)
        self.assertEqual(len(listed), 2)
    
    def test_execution_state_with_optional_fields(self):
        """测试带有可选字段的执行状态"""
        execution_id = "test-exec-optional"
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="test-flow",
            flow_name="Test Flow",
            flow_version="1.0",
            context={"step": 1},
            status="completed",
            start_time="2023-01-01T00:00:00",
            end_time="2023-01-01T00:01:00",
            invoker="test-user"
        )
        
        # 保存状态
        result = self.storage.save_execution_state(execution_id, state)
        self.assertTrue(result)
        
        # 加载并验证所有字段
        loaded_state = self.storage.load_execution_state(execution_id)
        self.assertIsNotNone(loaded_state)
        self.assertEqual(loaded_state.execution_id, execution_id)
        self.assertEqual(loaded_state.flow_id, "test-flow")
        self.assertEqual(loaded_state.flow_name, "Test Flow")
        self.assertEqual(loaded_state.flow_version, "1.0")
        self.assertEqual(loaded_state.status, "completed")
        self.assertEqual(loaded_state.start_time, "2023-01-01T00:00:00")
        self.assertEqual(loaded_state.end_time, "2023-01-01T00:01:00")
        self.assertEqual(loaded_state.invoker, "test-user")
    
    def test_execution_state_with_error(self):
        """测试带有错误信息的执行状态"""
        execution_id = "test-exec-error"
        error_info = {
            "type": "ValidationError",
            "message": "Invalid input",
            "details": {"field": "name", "value": ""}
        }
        
        state = ExecutionState(
            execution_id=execution_id,
            context={"step": 1},
            status="error",
            error=error_info
        )
        
        # 保存状态
        result = self.storage.save_execution_state(execution_id, state)
        self.assertTrue(result)
        
        # 加载并验证错误信息
        loaded_state = self.storage.load_execution_state(execution_id)
        self.assertIsNotNone(loaded_state)
        self.assertEqual(loaded_state.status, "error")
        self.assertEqual(loaded_state.error, error_info)


if __name__ == '__main__':
    unittest.main() 