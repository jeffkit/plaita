"""
测试异步流程执行和 parse_function 性能优化
"""
import asyncio
import unittest
import json

from plaita.flow import Flow, FlowExecution


class TestAsyncFlowExecution(unittest.IsolatedAsyncioTestCase):
    """测试异步流程执行的基础功能"""
    
    async def test_async_execution_method_exists(self):
        """验证异步执行方法存在"""
        execution = FlowExecution()
        
        self.assertTrue(hasattr(execution, 'arun_compatible'))
        # Traversal moved to ExecutionStrategy; node internals to NodeRunner
        from plaita.core.executor import NormalStrategy, GeneratorStrategy, DistributedStrategy
        self.assertTrue(hasattr(execution, '_strategies'))
        self.assertIsInstance(execution._strategies['normal'], NormalStrategy)
        self.assertIsInstance(execution._strategies['generator'], GeneratorStrategy)
        self.assertIsInstance(execution._strategies['distributed'], DistributedStrategy)
        # Node execution internals moved to NodeRunner
        self.assertTrue(hasattr(execution._runner, '_execute_with_retry'))
        self.assertTrue(hasattr(execution._runner, '_run_with_timeout'))
    
    async def test_async_execution_with_simple_flow(self):
        """测试异步执行简单流程"""
        flow_json = '''
        {
            "id": "test_async_simple",
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "resultType": "success", "output": "async_result"}
            ]
        }
        '''
        
        flow = Flow.model_validate_json(flow_json)
        execution = FlowExecution()
        
        # 使用异步执行 - 验证方法能正常调用且不抛出异常
        result = await execution.arun_compatible(flow, False)
        
        # 验证结果 - End 节点返回字面量值
        self.assertEqual(result, "async_result")
    
    async def test_async_lazy_mode_returns_generator(self):
        """测试异步懒执行模式返回生成器"""
        flow_json = '''
        {
            "id": "test_async_lazy",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "done"}
            ]
        }
        '''
        
        flow = Flow.model_validate_json(flow_json)
        execution = FlowExecution()
        
        # 懒执行模式应该返回一个异步生成器
        result = await execution.arun_compatible(flow, True)
        
        # 验证返回的是异步生成器
        self.assertTrue(hasattr(result, '__anext__'))


class TestParseFunction(unittest.TestCase):
    """测试 parse_function 性能优化"""
    
    def test_parse_function_performance(self):
        """测试 parse_function 的性能（使用缓存）"""
        from plaita.io import parse_function, _parser_components_cache
        
        context = {"$INPUT": {"value": 10}}
        
        # 清除缓存
        _parser_components_cache.clear()
        
        # 第一次调用（会创建缓存）
        result1 = parse_function("$F.add(1, 2)", context, "$")
        self.assertEqual(result1, 3)
        
        # 验证缓存已创建
        self.assertIn("$", _parser_components_cache)
        
        # 第二次调用（使用缓存）
        result2 = parse_function("$F.mul(3, 4)", context, "$")
        self.assertEqual(result2, 12)
    
    def test_nested_function_calls(self):
        """测试嵌套函数调用"""
        from plaita.io import parse_function
        
        context = {}
        
        # 嵌套函数调用
        result = parse_function("$F.add($F.add(1, 2), $F.mul(3, 4))", context, "$")
        self.assertEqual(result, 15)  # (1 + 2) + (3 * 4) = 3 + 12 = 15
    
    def test_non_function_expression(self):
        """测试非函数表达式快速返回"""
        from plaita.io import parse_function
        
        context = {}
        
        # 非函数表达式应该直接返回原始值
        result = parse_function("$INPUT.value", context, "$")
        self.assertEqual(result, "$INPUT.value")


if __name__ == "__main__":
    unittest.main()

