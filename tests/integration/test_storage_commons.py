"""
存储模块通用测试工具和方法
"""
import unittest
from typing import Type, Any, Dict, Optional

from plaita.storage import ExecutionStorage, ExecutionState


class StorageTestMixin:
    """存储测试混入类，提供通用的测试方法"""
    
    def get_storage_instance(self) -> ExecutionStorage:
        """获取存储实例，子类必须实现此方法"""
        raise NotImplementedError("子类必须实现get_storage_instance方法")
        
    def create_test_execution_state(self, execution_id: str, context: Optional[Dict[str, Any]] = None) -> ExecutionState:
        """创建测试执行状态数据"""
        if context is None:
            context = {
                "flow_id": "test-flow",
                "input": {"param1": "value1"},
                "output": None,
                "current_nodes": ["node1", "node2"],
                "completed_nodes": [],
                "metadata": {}
            }
        
        state = ExecutionState(
            execution_id=execution_id,
            flow_id="test-flow",
            flow_name="Test Flow",
            context=context,
            status="running",
            start_time="2021-04-01T00:00:00",
            last_update_time="2021-04-01T00:00:00"
        )
        
        storage = self.get_storage_instance()
        storage.save_execution_state(execution_id, state)
        return state


def get_standard_test_cases(storage_class: Type[ExecutionStorage]) -> Type[unittest.TestCase]:
    """
    生成标准测试用例类
    
    Args:
        storage_class: 要测试的存储类
        
    Returns:
        TestCase类
    """
    class StandardStorageTests(unittest.TestCase, StorageTestMixin):
        def get_storage_instance(self) -> ExecutionStorage:
            """获取存储实例"""
            if not hasattr(self, '_storage'):
                self._storage = storage_class()
            return self._storage
            
        def test_execution_state_lifecycle(self):
            """测试执行状态的完整生命周期"""
            storage = self.get_storage_instance()
            execution_id = "lifecycle-test-1"
            
            # 1. 创建状态
            state = self.create_test_execution_state(execution_id)
            
            # 2. 更新状态
            state.status = "completed"
            state.context["output"] = {"result": "success"}
            state.last_update_time = "2021-04-01T01:00:00"
            storage.save_execution_state(execution_id, state)
            
            # 3. 加载状态并验证
            loaded = storage.load_execution_state(execution_id)
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.status, "completed")
            self.assertEqual(loaded.context["output"], {"result": "success"})
            
            # 4. 列出执行状态
            executions = storage.list_executions()
            execution_ids = [state.execution_id for state in executions]
            self.assertIn(execution_id, execution_ids)
            
            # 5. 删除状态
            result = storage.delete_execution_state(execution_id)
            self.assertTrue(result)
            self.assertIsNone(storage.load_execution_state(execution_id))
            
        def test_multiple_execution_states(self):
            """测试多个执行状态的管理"""
            storage = self.get_storage_instance()
            
            # 创建多个执行状态
            execution_ids = [f"multi-test-{i}" for i in range(3)]
            states = []
            
            for exec_id in execution_ids:
                state = self.create_test_execution_state(
                    exec_id, 
                    context={"test_id": exec_id, "step": 1}
                )
                states.append(state)
            
            # 验证都能正确加载
            for exec_id in execution_ids:
                loaded = storage.load_execution_state(exec_id)
                self.assertIsNotNone(loaded)
                self.assertEqual(loaded.execution_id, exec_id)
                self.assertEqual(loaded.context["test_id"], exec_id)
            
            # 列出所有执行状态
            all_executions = storage.list_executions()
            loaded_ids = [state.execution_id for state in all_executions]
            
            for exec_id in execution_ids:
                self.assertIn(exec_id, loaded_ids)
            
            # 清理
            for exec_id in execution_ids:
                storage.delete_execution_state(exec_id)
            
        def test_execution_state_query_and_pagination(self):
            """测试执行状态查询和分页功能"""
            storage = self.get_storage_instance()
            
            # 创建多个执行状态用于测试分页
            execution_ids = [f"page-test-{i}" for i in range(10)]
            
            for exec_id in execution_ids:
                self.create_test_execution_state(exec_id)
            
            # 测试分页
            first_page = storage.list_executions(limit=3, offset=0)
            self.assertEqual(len(first_page), 3)
            
            second_page = storage.list_executions(limit=3, offset=3)
            self.assertEqual(len(second_page), 3)
            
            # 验证返回的都是ExecutionState对象
            for state in first_page + second_page:
                self.assertIsInstance(state, ExecutionState)
            
            # 清理
            for exec_id in execution_ids:
                storage.delete_execution_state(exec_id)
                
        def test_execution_state_error_handling(self):
            """测试执行状态的错误处理"""
            storage = self.get_storage_instance()
            
            # 加载不存在的执行状态
            result = storage.load_execution_state("non-existent")
            self.assertIsNone(result)
            
            # 删除不存在的执行状态
            # 注意：不同的存储实现可能有不同的行为
            # 内存存储返回False，Redis存储可能返回True（这都是正常的）
            result = storage.delete_execution_state("non-existent")
            self.assertIsInstance(result, bool)  # 只要返回布尔值就可以
    
    return StandardStorageTests 