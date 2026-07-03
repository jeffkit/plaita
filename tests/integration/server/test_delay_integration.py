#!/usr/bin/env python3
"""
延迟节点和延迟服务集成测试程序
验证整个延迟机制的可用性
"""
import asyncio
import json
import time
import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from plaita import Flow, FlowExecution
from plaita.event.core import EventBus
from plaita.event.memory import InMemoryEventBus
from plaita.logger import logger
from plaita.server.services import ServiceManager, DelayService


class DelayIntegrationTest:
    """
    延迟节点集成测试类
    """
    
    def __init__(self):
        # 创建事件总线
        self.event_bus = InMemoryEventBus()
        
        # 创建服务管理器
        self.service_manager = ServiceManager(self.event_bus)
        
        # 测试结果
        self.test_results = {}
        
    async def setup(self):
        """初始化测试环境"""
        logger.info("=== 开始初始化延迟节点测试环境 ===")
        
        # InMemoryEventBus 不需要启动
        logger.info("事件总线已准备就绪")
        
        # 只启动延迟服务
        service_configs = {
            "delay": {"max_workers": 3}
        }
        
        # 手动创建并启动延迟服务
        delay_service = DelayService(self.event_bus, service_configs["delay"])
        if delay_service.start_service():
            self.service_manager.services["delay"] = delay_service
            logger.info("延迟服务启动成功")
        else:
            logger.error("延迟服务启动失败")
            return False
        
        # 监听事件
        await self.setup_event_listeners()
        
        return True
    
    async def setup_event_listeners(self):
        """设置事件监听器"""
        async def on_delay_trigger(event):
            logger.info(f"收到延迟触发事件: {json.dumps(event.data, indent=2, ensure_ascii=False)}")
            self.test_results["delay_event_received"] = True
            self.test_results["delay_event_data"] = event.data
        
        # 注册延迟事件监听器
        handler_id = await self.event_bus.register_handler("delay_trigger", on_delay_trigger)
        logger.info(f"延迟事件监听器已注册，处理器ID: {handler_id}")
    
    async def cleanup(self):
        """清理测试环境"""
        logger.info("=== 开始清理测试环境 ===")
        
        # 停止所有服务
        self.service_manager.stop_all_services(timeout=5)
        
        # InMemoryEventBus 不需要停止
        logger.info("事件总线已清理")
        
        logger.info("测试环境清理完成")
    
    def load_test_flow(self) -> Flow:
        """加载测试流程"""
        flow_file = Path(__file__).parent / "test_delay_flow.json"
        
        if not flow_file.exists():
            raise FileNotFoundError(f"测试流程文件不存在: {flow_file}")
        
        with open(flow_file, 'r', encoding='utf-8') as f:
            flow_content = f.read()
        
        logger.info("测试流程文件加载成功")
        return Flow.from_string(flow_content)
    
    async def test_delay_node_basic(self):
        """测试延迟节点基础功能"""
        logger.info("\n=== 测试延迟节点基础功能 ===")
        
        try:
            from plaita.server.nodes import DelayNode
            
            # 创建延迟节点
            delay_node = DelayNode(
                id="test_delay",
                delay_seconds=2,
                delay_unit="seconds"
            )
            
            # 创建执行上下文
            execution = FlowExecution(event_bus=self.event_bus)
            execution.clean()
            execution.set_state("$FLOW_ID", "test_flow")
            execution.set_state("$EXECUTION_ID", "test_exec_123")
            
            # 执行节点
            result = delay_node.execute(execution)
            
            logger.info(f"延迟节点执行结果:")
            logger.info(json.dumps(result, indent=2, ensure_ascii=False))
            
            # 验证结果
            assert "service_config" in result, "缺少服务配置"
            assert result["event_type"] == "delay_trigger", "事件类型不正确"
            assert result["is_async"] == True, "应该是异步节点"
            
            service_config = result["service_config"]
            assert service_config["type"] == "delay", "服务类型不正确"
            assert service_config["delay_ms"] == 2000, "延迟时间不正确"
            assert "trigger_timestamp" in service_config, "缺少触发时间戳"
            
            self.test_results["basic_node_test"] = True
            logger.info("✓ 延迟节点基础功能测试通过")
            
        except Exception as e:
            logger.error(f"✗ 延迟节点基础功能测试失败: {e}", exc_info=True)
            self.test_results["basic_node_test"] = False
    
    async def test_delay_service_direct(self):
        """直接测试延迟服务"""
        logger.info("\n=== 直接测试延迟服务 ===")
        
        try:
            delay_service = self.service_manager.services.get("delay")
            if not delay_service:
                logger.error("延迟服务未找到")
                return False
            
            # 配置任务参数
            task_config = {
                "type": "delay",
                "delay_ms": 1500,
                "trigger_timestamp": int(time.time() * 1000) + 1500,
                "node_id": "test_node",
                "execution_id": "test_exec",
                "flow_id": "test_flow",
                "event_type": "delay_trigger",
                "event_filter": {},
                "retry_config": {
                    "max_retries": 3,
                    "retry_delay_ms": 1000,
                    "exponential_backoff": True
                }
            }
            
            # 提交延迟任务
            task_id = delay_service.submit_task(task_config)
            logger.info(f"延迟任务已提交，任务ID: {task_id}")
            
            start_time = time.time()
            logger.info(f"延迟任务等待中: {task_config['delay_ms']/1000}秒")
            
            # 等待延迟时间 + 额外缓冲时间
            wait_time = task_config['delay_ms']/1000 + 1.5
            await asyncio.sleep(wait_time)
            
            # 让事件处理器有时间完成
            await asyncio.sleep(0.1)
            
            elapsed = time.time() - start_time
            logger.info(f"总耗时: {elapsed:.2f}秒")
            
            # 检查是否收到事件
            if self.test_results.get("delay_event_received"):
                logger.info("✓ 延迟服务直接测试通过")
                return True
            else:
                logger.error("✗ 未收到延迟事件")
                return False
                
        except Exception as e:
            logger.error(f"延迟服务直接测试失败: {e}", exc_info=True)
            return False
    
    async def test_flow_execution_distributed(self):
        """测试分布式流程执行"""
        logger.info("\n=== 测试分布式流程执行 ===")
        
        try:
            # 加载测试流程
            flow = self.load_test_flow()
            
            # 使用分布式模式执行
            execution = FlowExecution(event_bus=self.event_bus)
            
            # 第一步：开始执行
            input_params = {
                "delay_seconds": 2,
                "message": "分布式测试消息"
            }
            
            result1 = execution._run_distributed(flow, input_params)
            logger.info("第一步执行结果:")
            logger.info(json.dumps(result1, indent=2, ensure_ascii=False, default=str))
            
            # 继续执行直到到达延迟节点
            current_result = result1
            max_steps = 10  # 防止无限循环
            step_count = 0
            
            while not current_result.get("is_end") and step_count < max_steps:
                step_count += 1
                current_node_id = current_result.get("id")
                
                logger.info(f"第{step_count + 1}步：当前节点 {current_node_id}")
                
                # 如果到达延迟节点，跳出循环
                if current_node_id == "delay_step":
                    logger.info("✓ 成功到达延迟节点")
                    break
                
                # 继续执行下一步
                current_result = execution._run_distributed(
                    flow, 
                    input_params, 
                    context=current_result["context"]
                )
                
                logger.info(f"第{step_count + 1}步执行结果:")
                logger.info(json.dumps(current_result, indent=2, ensure_ascii=False, default=str))
            
            # 检查是否到达延迟节点
            if current_result.get("id") == "delay_step":
                logger.info("✓ 成功到达延迟节点")
                
                # 提交延迟任务
                service_config = current_result.get("result", {}).get("service_config")
                if service_config:
                    task_id = self.service_manager.handle_node_config(service_config)
                    logger.info(f"延迟任务已提交: {task_id}")
                    
                    # 等待延迟任务完成并触发事件
                    await asyncio.sleep(3)  # 等待2秒延迟 + 缓冲时间
                    
                    # 让事件处理器有时间完成
                    await asyncio.sleep(0.2)
                    
                    # 检查是否收到延迟事件
                    if self.test_results.get("delay_event_received"):
                        logger.info("✓ 收到延迟事件，继续执行流程")
                        # 继续执行剩余流程
                        # TODO: 在实际应用中，这里会通过事件继续执行
                        self.test_results["distributed_flow_test"] = True
                        return True
                    else:
                        logger.error("✗ 未收到延迟事件，无法继续执行")
                        return False
                else:
                    logger.error("✗ 延迟节点未生成服务配置")
                    self.test_results["distributed_flow_test"] = False
            else:
                logger.error(f"✗ 未到达延迟节点，当前节点: {current_result.get('id')}")
                self.test_results["distributed_flow_test"] = False
                
        except Exception as e:
            logger.error(f"✗ 分布式流程执行测试失败: {e}", exc_info=True)
            self.test_results["distributed_flow_test"] = False
    
    async def run_tests(self):
        """运行所有测试"""
        try:
            # 1. 初始化环境
            if not await self.setup():
                logger.error("测试环境初始化失败")
                return False
            
            # 2. 延迟节点基础功能测试
            self.test_results["basic_node_test"] = self.test_delay_node_basic()
            
            # 3. 延迟服务直接测试
            self.test_results["direct_service_test"] = await self.test_delay_service_direct()
            
            # 4. 分布式流程执行测试
            self.test_results["distributed_flow_test"] = await self.test_flow_execution_distributed()
            
            # 等待所有异步任务完成
            await asyncio.sleep(0.5)
            
            return True
            
        except Exception as e:
            logger.error(f"测试运行失败: {e}", exc_info=True)
            return False
        finally:
            await self.cleanup()
    
    def print_test_summary(self):
        """打印测试结果总结"""
        logger.info("\n" + "="*50)
        logger.info("测试结果总结")
        logger.info("="*50)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for v in self.test_results.values() if v)
        
        for test_name, passed in self.test_results.items():
            status = "✓ 通过" if passed else "✗ 失败"
            logger.info(f"{test_name}: {status}")
        
        logger.info(f"\n总计: {passed_tests}/{total_tests} 个测试通过")
        
        if passed_tests == total_tests:
            logger.info("🎉 所有测试通过！延迟节点和延迟服务集成功能正常")
        else:
            logger.warning("⚠️ 部分测试失败，请检查相关功能")


async def main():
    """主函数"""
    test = DelayIntegrationTest()
    success = await test.run_tests()
    
    # 打印测试结果总结
    test.print_test_summary()
    
    if success and all(test.test_results.values()):
        logger.info("🎉 所有测试通过！延迟节点机制验证成功")
        exit(0)
    else:
        logger.error("延迟节点集成测试存在失败")
        exit(1)


if __name__ == "__main__":
    # 设置日志级别
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 运行测试
    asyncio.run(main()) 