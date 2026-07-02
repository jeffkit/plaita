import unittest
import time
from unittest.mock import patch

try:
    import fakeredis
    from plaita.storage import RedisExecutionStorage, ExecutionStorage, ExecutionState
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .test_storage_commons import StorageTestMixin, get_standard_test_cases


@unittest.skipIf(not REDIS_AVAILABLE, "redis依赖未安装，跳过Redis存储测试")
class TestRedisStateStorage(unittest.TestCase, StorageTestMixin):
    """RedisStateStorage实现测试"""
    
    def get_storage_instance(self):
        """获取存储实例"""
        if not hasattr(self, '_storage'):
            # 使用fakeredis创建模拟的Redis客户端
            self.fake_redis = fakeredis.FakeStrictRedis(decode_responses=True)
            self._storage = RedisExecutionStorage(client=self.fake_redis, namespace="test")
        return self._storage
    
    def setUp(self):
        """测试初始化"""
        # 确保有存储实例
        self.storage = self.get_storage_instance()
        
    def tearDown(self):
        """测试清理"""
        # 清空Redis数据
        if hasattr(self, 'fake_redis'):
            self.fake_redis.flushall()
    
    def test_namespace_key(self):
        """测试命名空间键生成"""
        key = self.storage.get_namespace_key("execution", "test-id")
        self.assertEqual(key, "test:execution:test-id")
    
    def test_execution_state(self):
        """测试执行状态的保存、加载和删除"""
        # 保存状态
        execution_id = "test-exec-1"
        state = ExecutionState(
            execution_id=execution_id,
            context={"key": "value"},
            status="running"
        )
        
        result = self.storage.save_execution_state(execution_id, state)
        self.assertTrue(result)
        
        # 检查Redis中是否有对应的键
        key = self.storage.get_namespace_key("execution", execution_id)
        self.assertTrue(self.fake_redis.exists(key))
        
        # 加载状态
        loaded_state = self.storage.load_execution_state(execution_id)
        self.assertIsNotNone(loaded_state)
        self.assertEqual(loaded_state.execution_id, execution_id)
        self.assertEqual(loaded_state.context["key"], "value")
        self.assertEqual(loaded_state.status, "running")
        
        # 删除状态
        result = self.storage.delete_execution_state(execution_id)
        self.assertTrue(result)
        
        # 验证键已被删除
        self.assertFalse(self.fake_redis.exists(key))
        
        # 加载已删除的状态
        loaded_state = self.storage.load_execution_state(execution_id)
        self.assertIsNone(loaded_state)
    
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
    
    def test_execution_state_with_complex_data(self):
        """测试复杂数据的执行状态"""
        execution_id = "test-exec-complex"
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="complex-flow",
            flow_name="Complex Test Flow",
            flow_version="2.0",
            context={
                "nested": {"data": {"deeply": {"nested": "value"}}},
                "list": [1, 2, {"inner": "dict"}],
                "number": 42.5,
                "boolean": True,
                "null_value": None
            },
            status="suspended",
            start_time="2023-01-01T00:00:00",
            last_update_time="2023-01-01T00:30:00",
            invoker="test-system"
        )
        
        # 保存复杂状态
        result = self.storage.save_execution_state(execution_id, state)
        self.assertTrue(result)
        
        # 加载并验证所有字段
        loaded_state = self.storage.load_execution_state(execution_id)
        self.assertIsNotNone(loaded_state)
        self.assertEqual(loaded_state.execution_id, execution_id)
        self.assertEqual(loaded_state.flow_id, "complex-flow")
        self.assertEqual(loaded_state.flow_name, "Complex Test Flow")
        self.assertEqual(loaded_state.flow_version, "2.0")
        self.assertEqual(loaded_state.context["nested"]["data"]["deeply"]["nested"], "value")
        self.assertEqual(loaded_state.context["list"][2]["inner"], "dict")
        self.assertEqual(loaded_state.context["number"], 42.5)
        self.assertEqual(loaded_state.context["boolean"], True)
        self.assertIsNone(loaded_state.context["null_value"])
        self.assertEqual(loaded_state.status, "suspended")
        self.assertEqual(loaded_state.invoker, "test-system")
    
    def test_execution_state_error_scenarios(self):
        """测试执行状态的错误场景"""
        # 加载不存在的执行状态
        result = self.storage.load_execution_state("non-existent")
        self.assertIsNone(result)
        
        # 删除不存在的执行状态
        result = self.storage.delete_execution_state("non-existent")
        self.assertTrue(result)  # Redis的delete操作即使键不存在也返回True（或操作成功）
    
    def test_redis_specific_features(self):
        """测试Redis特定功能"""
        execution_id = "redis-test"
        state = ExecutionState(
            execution_id=execution_id,
            context={"test": "redis"},
            status="running"
        )
        
        # 保存状态
        self.storage.save_execution_state(execution_id, state)
        
        # 直接从Redis检查数据
        key = self.storage.get_namespace_key("execution", execution_id)
        raw_data = self.fake_redis.get(key)
        self.assertIsNotNone(raw_data)
        
        # 反序列化并检查
        deserialized = self.storage.deserialize_state(raw_data)
        self.assertEqual(deserialized["execution_id"], execution_id)
        self.assertEqual(deserialized["context"]["test"], "redis")
    
    def test_concurrent_operations(self):
        """测试并发操作"""
        # 创建多个执行状态模拟并发操作
        execution_ids = [f"concurrent-{i}" for i in range(10)]
        
        # 批量保存
        for exec_id in execution_ids:
            state = ExecutionState(
                execution_id=exec_id,
                context={"index": exec_id.split("-")[1]},
                status="running"
            )
            result = self.storage.save_execution_state(exec_id, state)
            self.assertTrue(result)
        
        # 批量验证
        for exec_id in execution_ids:
            loaded = self.storage.load_execution_state(exec_id)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.execution_id, exec_id)
        
        # 批量删除
        for exec_id in execution_ids:
            result = self.storage.delete_execution_state(exec_id)
            self.assertTrue(result)
        
        # 验证都已删除
        for exec_id in execution_ids:
            loaded = self.storage.load_execution_state(exec_id)
            self.assertIsNone(loaded)


@unittest.skipIf(not REDIS_AVAILABLE, "redis依赖未安装，跳过Redis存储测试")
class StandardRedisStateStorageTests(get_standard_test_cases(RedisExecutionStorage)):
    """使用标准测试用例测试Redis存储"""
    
    def get_storage_instance(self):
        """获取存储实例"""
        if not hasattr(self, '_storage'):
            self.fake_redis = fakeredis.FakeStrictRedis(decode_responses=True)
            self._storage = RedisExecutionStorage(client=self.fake_redis, namespace="std_test")
        return self._storage
    
    def tearDown(self):
        """测试清理"""
        if hasattr(self, 'fake_redis'):
            self.fake_redis.flushall()


if __name__ == '__main__':
    unittest.main() 