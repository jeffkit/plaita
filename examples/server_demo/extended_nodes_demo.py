"""
扩展节点和外延服务框架演示
展示如何使用延迟节点、队列节点、回调节点和审批节点
"""
import asyncio
import json
import logging
import time
from typing import Dict, Any

from plaita.server.nodes import DelayNode, RedisQueueNode, KafkaQueueNode, HttpCallbackNode, ApprovalNode
from plaita.server.services import ServiceManager, DelayService, HttpCallbackService, ApprovalService
from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution
from plaita.event import InMemoryEventBus, get_default_event_bus
from plaita.logger import logger

# 演示脚本自行配置日志输出（库本身不再强制任何 handler/级别）。
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def create_demo_flow_with_delay_node() -> Dict[str, Any]:
    """
    创建包含延迟节点的演示流程
    """
    return {
        "flow_id": "demo_delay_flow",
        "version": "1.0",
        "runtime": "python",
        "desc": "延迟节点演示流程",
        "nodes": [
            {
                "id": "start",
                "type": "start",
                "name": "开始",
                "next": "delay_5s"
            },
            {
                "id": "delay_5s",
                "type": "delay",
                "name": "延迟5秒",
                "delay_seconds": 5,
                "delay_unit": "seconds",
                "next": "end"
            },
            {
                "id": "end",
                "type": "end",
                "name": "结束"
            }
        ]
    }


def create_demo_flow_with_approval_node() -> Dict[str, Any]:
    """
    创建包含审批节点的演示流程
    """
    return {
        "flow_id": "demo_approval_flow",
        "version": "1.0",
        "runtime": "python",
        "desc": "审批节点演示流程",
        "nodes": [
            {
                "id": "start",
                "type": "start",
                "name": "开始",
                "next": "approval_step"
            },
            {
                "id": "approval_step",
                "type": "approval",
                "name": "审批步骤",
                "event_type": "approval_decision",
                "approval_title": "资源申请审批",
                "approval_content": "请审批以下资源申请：CPU 4核，内存 8GB",
                "approvers": ["manager1", "manager2"],
                "approval_strategy": "any",
                "next": "end"
            },
            {
                "id": "end",
                "type": "end",
                "name": "结束"
            }
        ]
    }


def create_demo_flow_with_redis_node() -> Dict[str, Any]:
    """
    创建包含Redis队列节点的演示流程
    """
    return {
        "flow_id": "demo_redis_flow",
        "version": "1.0",
        "runtime": "python",
        "desc": "Redis队列节点演示流程",
        "nodes": [
            {
                "id": "start",
                "type": "start",
                "name": "开始",
                "next": "redis_listener"
            },
            {
                "id": "redis_listener",
                "type": "redis_queue",
                "name": "监听Redis消息",
                "event_type": "redis_message_trigger",
                "connection_config": {
                    "host": "localhost",
                    "port": 6379,
                    "db": 0
                },
                "queue_name": "demo_queue",
                "next": "end"
            },
            {
                "id": "end",
                "type": "end",
                "name": "结束"
            }
        ]
    }


def create_demo_flow_with_kafka_node() -> Dict[str, Any]:
    """
    创建包含Kafka队列节点的演示流程
    """
    return {
        "flow_id": "demo_kafka_flow",
        "version": "1.0",
        "runtime": "python",
        "desc": "Kafka队列节点演示流程",
        "nodes": [
            {
                "id": "start",
                "type": "start",
                "name": "开始",
                "next": "kafka_listener"
            },
            {
                "id": "kafka_listener",
                "type": "kafka_queue",
                "name": "监听Kafka消息",
                "event_type": "kafka_message_trigger",
                "bootstrap_servers": ["localhost:9092"],
                "topic": "demo_topic",
                "group_id": "demo_group",
                "next": "end"
            },
            {
                "id": "end",
                "type": "end",
                "name": "结束"
            }
        ]
    }


def create_demo_flow_with_http_callback_node() -> Dict[str, Any]:
    """
    创建包含HTTP回调节点的演示流程
    """
    return {
        "flow_id": "demo_http_callback_flow",
        "version": "1.0",
        "runtime": "python",
        "desc": "HTTP回调节点演示流程",
        "nodes": [
            {
                "id": "start",
                "type": "start",
                "name": "开始",
                "next": "http_callback_step"
            },
            {
                "id": "http_callback_step",
                "type": "http_callback",
                "name": "HTTP回调步骤",
                "event_type": "http_callback_trigger",
                "callback_method": "POST",
                "require_auth": False,
                "next": "end"
            },
            {
                "id": "end",
                "type": "end",
                "name": "结束"
            }
        ]
    }


class ExtendedNodesDemo:
    """
    扩展节点演示类
    """
    
    def __init__(self):
        # 创建事件总线 - 使用具体的InMemoryEventBus实现
        self.event_bus = InMemoryEventBus()
        
        # 创建服务管理器
        self.service_manager = ServiceManager(self.event_bus)
        
        # 注册自定义节点类型
        self._register_extended_nodes()
        
        # 节点类型映射
        self.node_classes = {
            "delay": DelayNode,
            "redis_queue": RedisQueueNode,
            "kafka_queue": KafkaQueueNode,
            "http_callback": HttpCallbackNode,
            "approval": ApprovalNode
        }

        # 存储挂起的流程执行信息
        self.suspended_executions = {}
        
        # 注册事件处理器监听所有事件 - 创建任务但不等待
        # 异步初始化任务将在setup方法中完成
        self._event_handler_task = None

    def _register_extended_nodes(self):
        """注册扩展节点类型到节点解析器"""
        extended_nodes = [
            DelayNode,
            RedisQueueNode,
            KafkaQueueNode,
            HttpCallbackNode,
            ApprovalNode
        ]
        
        from plaita.node import get_default_registry
        registry = get_default_registry()
        for node_class in extended_nodes:
            try:
                registry.register(node_class)
                logger.info(f"已注册扩展节点类型: {node_class.node_type}")
            except Exception as e:
                logger.warning(f"注册节点类型 {node_class.node_type} 失败: {e}")
        
    async def setup(self):
        """初始化演示环境"""
        logger.info("开始初始化扩展节点演示环境...")
        
        # 确保事件处理器注册完成
        await self._register_event_handlers()
        
        # InMemoryEventBus不需要显式启动
        logger.info("事件总线初始化完成")
        
        # 启动所有服务
        service_configs = {
            "delay": {"max_workers": 5},
            "http_callback": {"max_workers": 3},
            "approval": {"max_workers": 2},
            "redis_queue": {"max_workers": 3},
            "kafka_queue": {"max_workers": 3}
        }
        
        try:
            success = self.service_manager.start_all_services(service_configs)
            if success:
                logger.info("所有服务启动成功")
            else:
                logger.warning("部分服务启动失败")
        except Exception as e:
            logger.error(f"服务启动过程中出错: {e}")
        
        # 打印服务状态
        try:
            status = self.service_manager.get_all_services_status()
            logger.info(f"服务状态: {json.dumps(status, indent=2, ensure_ascii=False)}")
        except Exception as e:
            logger.warning(f"获取服务状态失败: {e}")
    
    async def cleanup(self):
        """清理演示环境"""
        logger.info("开始清理演示环境...")
        
        try:
            # 等待所有挂起的任务完成或超时
            if hasattr(self, 'suspended_executions') and self.suspended_executions:
                logger.info(f"等待 {len(self.suspended_executions)} 个挂起的流程...")
                await asyncio.sleep(1.0)  # 给一些时间让事件处理完成
                
            # 停止所有服务
            self.service_manager.stop_all_services(timeout=5)
            logger.info("所有服务已停止")
            
            # 等待事件总线处理完所有任务
            logger.info("等待事件总线处理完成...")
            await asyncio.sleep(0.5)
            
            # 清理事件总线
            if hasattr(self, 'event_bus'):
                # 清理所有等待的future和处理器
                self.event_bus.waiting_futures.clear()
                self.event_bus.handlers.clear()
                logger.info("事件总线清理完成")
            
            logger.info("演示环境清理完成")
            
        except Exception as e:
            logger.error(f"清理过程中出错: {e}", exc_info=True)
    
    async def _register_event_handlers(self):
        """注册wildcard事件处理器监听所有完成事件"""
        try:
            # 使用通配符监听所有事件
            await self.event_bus.register_handler(
                event_type="*",
                handler=self._handle_wildcard_event
            )
            
            logger.info("wildcard事件处理器注册完成")
            logger.info(f"事件总线在注册事件处理器后: {self.event_bus}")
        except Exception as e:
            logger.error(f"注册事件处理器失败: {e}")

    async def _handle_wildcard_event(self, event):
        """处理所有类型的事件"""
        try:
            logger.info(f"🎯 收到事件: {event.event_type}, 数据: {json.dumps(event.data, ensure_ascii=False)}")
            
            # 检查是否是我们关心的完成事件 - 更新为实际的事件类型
            completion_event_types = {
                "delay_trigger",           # 延迟事件
                "approval_trigger",        # 审批触发事件
                "approval_decision",       # 审批决策事件  
                "http_callback",           # HTTP回调事件
                "http_callback_trigger",   # HTTP回调触发事件
                "redis_message",           # Redis消息事件
                "redis_message_trigger",   # Redis消息触发事件
                "kafka_message",           # Kafka消息事件
                "kafka_message_trigger"    # Kafka消息触发事件
            }
            
            if event.event_type in completion_event_types:
                # 从事件数据中获取执行ID
                execution_id = event.data.get("execution_id")
                logger.info(f"🔍 检查执行ID: {execution_id}")
                logger.info(f"🔍 当前挂起的执行: {list(self.suspended_executions.keys())}")
                
                if execution_id and execution_id in self.suspended_executions:
                    logger.info(f"✅ 找到匹配的挂起流程，开始恢复: {execution_id}")
                    # 使用await关键字确保异步函数被正确等待
                    await self._resume_flow_execution(execution_id, event.data)
                else:
                    logger.warning(f"❌ 未找到匹配的挂起流程: {execution_id}")
            else:
                logger.debug(f"⏭️  忽略非完成事件: {event.event_type}")
                
        except Exception as e:
            logger.error(f"❌ wildcard事件处理器出错: {e}", exc_info=True)

    async def _resume_flow_execution(self, execution_id: str, event_data: Dict[str, Any]):
        """恢复流程执行"""
        try:
            suspended_info = self.suspended_executions.get(execution_id)
            if not suspended_info:
                logger.warning(f"找不到挂起的执行信息: {execution_id}")
                return
            
            flow = suspended_info["flow"]
            context = suspended_info["context"]
            
            logger.info(f"恢复流程执行: {execution_id}")
            
            # 恢复流程执行
            from plaita import ExecutionMode
            resume_result = FlowExecution.run(
                flow, 
                params={}, 
                context=context,
                mode=ExecutionMode.DISTRIBUTED,
                resume_type="event",
                resume_data=event_data,
                event_bus=self.event_bus
            )
            
            logger.info(f"恢复执行结果: {json.dumps(resume_result['result'], indent=2, ensure_ascii=False)}")
            
            # 检查是否完成
            if resume_result.get("is_end"):
                logger.info(f"✓ 流程 {execution_id} 执行完成！")
                # 清理挂起信息
                del self.suspended_executions[execution_id]
            elif resume_result.get("is_suspend"):
                # 如果又挂起了，更新挂起信息
                self.suspended_executions[execution_id] = {
                    "flow": flow,
                    "context": resume_result.get("context", {})
                }
                # 继续提交任务
                await self._submit_suspended_task(resume_result)
            else:
                # 如果不是挂起也不是结束，可能是继续运行状态 - 也清理挂起信息
                logger.info(f"✓ 流程 {execution_id} 继续运行中，清理挂起状态")
                del self.suspended_executions[execution_id]
            
        except Exception as e:
            logger.error(f"恢复流程执行失败: {e}", exc_info=True)

    async def _submit_suspended_task(self, result: Dict[str, Any]):
        """提交挂起任务到服务管理器"""
        try:
            node_result = result.get("result", {})
            service_config = node_result.get("service_config")
            
            if service_config:
                task_id = self.service_manager.handle_node_config(service_config)
                logger.info(f"挂起任务已重新提交，任务ID: {task_id}")
            else:
                logger.warning("挂起结果中没有找到service_config")
                
        except Exception as e:
            logger.error(f"提交挂起任务失败: {e}")

    async def execute_flow_with_event_node(self, flow_dict: Dict[str, Any], wait_time: int = 10) -> bool:
        """
        通用的事件节点流程执行方法
        
        Args:
            flow_dict: 流程定义字典
            wait_time: 等待事件完成的时间（秒）
            
        Returns:
            bool: 是否执行成功
        """
        logger.info(f"=== 开始执行流程: {flow_dict.get('flow_id')} ===")
        
        try:
            # 创建流程对象
            flow = Flow.model_validate(flow_dict)
            logger.info(f"创建流程成功: {flow.flow_id}")
            
            # 执行流程
            from plaita import ExecutionMode
            result = FlowExecution.run(
                flow, 
                params={}, 
                mode=ExecutionMode.DISTRIBUTED, 
                event_bus=self.event_bus
            )
            
            logger.info(f"流程执行结果: {json.dumps(result['result'], indent=2, ensure_ascii=False)}")
            
            # 如果流程挂起，提交任务给服务管理器
            if result.get("is_suspend"):
                execution_id = result.get("execution_id")
                logger.info(f"检测到流程挂起，执行ID: {execution_id}")
                
                # 保存挂起的执行信息
                self.suspended_executions[execution_id] = {
                    "flow": flow,
                    "context": result.get("context", {})
                }
                
                # 提交任务到服务管理器
                await self._submit_suspended_task(result)
                
                # 等待事件处理器自动恢复执行
                logger.info(f"等待服务完成事件以恢复流程，最大等待时间: {wait_time}秒...")
                
                # 先等待一下让异步任务有时间启动
                await asyncio.sleep(0.1)
                
                start_time = time.time()
                while time.time() - start_time < wait_time:
                    await asyncio.sleep(0.5)  # 更频繁地检查
                    # 检查是否已完成 - 修改检查逻辑
                    if execution_id not in self.suspended_executions:
                        logger.info("✓ 流程已通过事件恢复并完成！")
                        # 再等待一点时间确保所有异步任务完成
                        await asyncio.sleep(1.0)  # 增加等待时间
                        return True
                
                # 超时后最后检查一次
                if execution_id not in self.suspended_executions:
                    logger.info("✓ 流程已完成（超时后确认）！")
                    # 再等待一点时间确保所有异步任务完成
                    await asyncio.sleep(0.5)
                    return True
                else:
                    logger.warning(f"流程 {execution_id} 在 {wait_time} 秒内未完成")
                    logger.info(f"当前挂起的流程: {list(self.suspended_executions.keys())}")
                    
                    # 最后等待一段时间看看异步任务是否能完成
                    logger.info("再等待2秒钟看看异步任务是否能完成...")
                    await asyncio.sleep(2.0)
                    
                    if execution_id not in self.suspended_executions:
                        logger.info("✓ 异步任务延迟完成了！")
                        return True
                    else:
                        return False
                    
            elif result.get("is_end"):
                logger.info("✓ 流程直接执行完成！")
                return True
            else:
                logger.info("流程在运行中...")
                return False
                
        except Exception as e:
            logger.error(f"流程执行出错: {e}", exc_info=True)
            return False
        finally:
            logger.info(f"=== 流程 {flow_dict.get('flow_id')} 执行完成 ===\n")

    async def test_delay_flow(self):
        """测试延迟节点流程"""
        flow_dict = create_demo_flow_with_delay_node()
        return await self.execute_flow_with_event_node(flow_dict, wait_time=8)

    async def test_approval_flow(self):
        """测试审批节点流程"""
        flow_dict = create_demo_flow_with_approval_node()
        
        # 启动流程
        success = await self.execute_flow_with_event_node(flow_dict, wait_time=10)
        
        # 如果流程挂起，需要模拟审批决策
        if not success:
            # 查找挂起的审批流程
            for execution_id, info in self.suspended_executions.items():
                if info["flow"].flow_id == "demo_approval_flow":
                    logger.info(f"发现挂起的审批流程: {execution_id}")
                    
                    # 获取审批服务并模拟决策
                    approval_service = self.service_manager.get_service("approval")
                    if approval_service and isinstance(approval_service, ApprovalService):
                        pending = approval_service.get_pending_approvals()
                        logger.info(f"待审批任务: {json.dumps(pending, indent=2, ensure_ascii=False)}")
                        
                        # 对所有待审批任务提交决策（get_pending_approvals 返回的
                        # 是摘要 dict，不含 execution_id；demo 一次只挂起一个审批）
                        for approval_id in list(pending.keys()):
                            decision_result = await approval_service.submit_approval_decision(
                                approval_id, "manager1", "approve", "同意申请"
                            )
                            logger.info(f"提交审批决策: {json.dumps(decision_result, ensure_ascii=False)}")

                            # 再等待一下看是否完成
                            await asyncio.sleep(3)
                            if execution_id not in self.suspended_executions:
                                logger.info("✓ 审批流程通过决策完成！")
                                return True
                    break
        
        return success

    async def test_redis_flow(self):
        """测试Redis队列节点流程"""
        flow_dict = create_demo_flow_with_redis_node()
        
        # 启动流程（这会让它等待Redis消息）
        task = asyncio.create_task(self.execute_flow_with_event_node(flow_dict, wait_time=15))
        
        # 等待一下让流程启动并挂起
        await asyncio.sleep(2)
        
        # 模拟发送Redis消息
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            
            # 发送测试消息
            test_message = {
                "type": "test",
                "data": "这是一个测试消息",
                "timestamp": int(time.time() * 1000)
            }
            
            r.lpush("demo_queue", json.dumps(test_message, ensure_ascii=False))
            logger.info(f"已发送Redis测试消息到队列: demo_queue")
            
        except Exception as e:
            logger.error(f"发送Redis消息失败: {e}")
        
        # 等待流程完成
        return await task

    async def test_kafka_flow(self):
        """测试Kafka队列节点流程"""
        flow_dict = create_demo_flow_with_kafka_node()
        
        # 启动流程（这会让它等待Kafka消息）
        task = asyncio.create_task(self.execute_flow_with_event_node(flow_dict, wait_time=15))
        
        # 等待一下让流程启动并挂起
        await asyncio.sleep(2)
        
        # 尝试使用实际的Kafka服务，如果失败则会使用模拟模式
        try:
            kafka_service = self.service_manager.get_service("kafka_queue")
            if kafka_service:
                # 获取kafka节点配置中的主题
                topic = flow_dict["nodes"][1]["topic"]
                
                # 创建测试消息
                test_message = {
                    "type": "test",
                    "data": "这是一个实际的Kafka测试消息",
                    "timestamp": int(time.time() * 1000)
                }
                
                # 尝试发送消息
                try:
                    send_result = await kafka_service.send_test_message(topic, test_message)
                    if send_result:
                        logger.info(f"成功发送实际Kafka消息到主题: {topic}")
                    else:
                        logger.warning("发送Kafka消息失败，将使用模拟模式")
                except Exception as e:
                    logger.warning(f"发送Kafka消息出错: {e}")
        except Exception as e:
            logger.error(f"准备Kafka测试时出错: {e}")
        
        # 等待流程完成
        return await task

    async def test_http_callback_flow(self):
        """测试HTTP回调节点流程"""
        flow_dict = create_demo_flow_with_http_callback_node()
        
        # 启动流程（这会让它等待HTTP回调）
        task = asyncio.create_task(self.execute_flow_with_event_node(flow_dict, wait_time=10))
        
        # 等待一下让流程启动并挂起
        await asyncio.sleep(2)
        
        # 模拟HTTP回调请求
        try:
            callback_service = self.service_manager.get_service("http_callback")
            if callback_service and isinstance(callback_service, HttpCallbackService):
                # 查看已注册的回调
                callbacks = callback_service.get_registered_callbacks()
                logger.info(f"已注册的回调: {json.dumps(callbacks, indent=2, ensure_ascii=False)}")
                
                # 找到回调路径并模拟请求
                # （回调记录的 path 在 task_config.callback_config 里）
                for callback_id, callback_info in callbacks.items():
                    callback_path = (callback_info.get("task_config", {})
                                              .get("callback_config", {})
                                              .get("path"))
                    if callback_path:
                        mock_request_data = {
                            "status": "completed",
                            "data": {"result": "success"},
                            "timestamp": int(time.time() * 1000)
                        }
                        
                        response = await callback_service.handle_callback_request(
                            callback_path, mock_request_data
                        )
                        logger.info(f"模拟回调响应: {json.dumps(response, ensure_ascii=False)}")
                        break
            
        except Exception as e:
            logger.error(f"模拟HTTP回调失败: {e}")
        
        # 等待流程完成
        return await task
    
    async def run_all_flow_tests(self):
        """运行所有流程测试"""
        await self.setup()
        
        try:
            logger.info("开始运行所有扩展节点流程测试...")
            
            # 测试延迟节点流程
            logger.info("\n=== 测试延迟节点流程 ===")
            success1 = await self.test_delay_flow()
            logger.info(f"延迟节点流程测试结果: {'成功' if success1 else '失败'}")
            
            await asyncio.sleep(2)
            
            # 测试审批节点流程
            logger.info("\n=== 测试审批节点流程 ===")
            success2 = await self.test_approval_flow()
            logger.info(f"审批节点流程测试结果: {'成功' if success2 else '失败'}")
            
            await asyncio.sleep(2)
            
            # 测试HTTP回调节点流程
            logger.info("\n=== 测试HTTP回调节点流程 ===")
            success3 = await self.test_http_callback_flow()
            logger.info(f"HTTP回调节点流程测试结果: {'成功' if success3 else '失败'}")
            
            await asyncio.sleep(2)
            
            # 测试Redis队列节点流程
            logger.info("\n=== 测试Redis队列节点流程 ===")
            success4 = await self.test_redis_flow()
            logger.info(f"Redis队列节点流程测试结果: {'成功' if success4 else '失败'}")
            
            await asyncio.sleep(2)
            
            # 测试Kafka队列节点流程
            logger.info("\n=== 测试Kafka队列节点流程 ===")
            success5 = await self.test_kafka_flow()
            logger.info(f"Kafka队列节点流程测试结果: {'成功' if success5 else '失败'}")
            
            # 总结
            total_tests = 5
            successful_tests = sum([success1, success2, success3, success4, success5])
            logger.info(f"\n=== 测试总结 ===")
            logger.info(f"总测试数: {total_tests}")
            logger.info(f"成功数: {successful_tests}")
            logger.info(f"失败数: {total_tests - successful_tests}")
            logger.info(f"成功率: {successful_tests/total_tests*100:.1f}%")
            
        except Exception as e:
            logger.error(f"运行流程测试过程中出错: {e}", exc_info=True)
        finally:
            await self.cleanup()


async def main():
    """主函数"""
    logger.info("开始扩展节点和外延服务框架测试")
    
    try:
        demo = ExtendedNodesDemo()
        await demo.run_all_flow_tests()
        logger.info("测试完成")
    except Exception as e:
        logger.error(f"测试程序异常退出: {e}", exc_info=True)


async def simple_demo():
    """简化的演示，专注于核心功能"""
    logger.info("=== 简化演示开始 ===")
    
    try:
        # 创建事件总线和服务管理器
        event_bus = InMemoryEventBus()
        service_manager = ServiceManager(event_bus)
        
        # 存储完成事件
        completion_events = []
        
        # 注册事件处理器监听完成事件
        async def completion_handler(event):
            logger.info(f"✓ 收到完成事件: {event.event_type}, 数据: {json.dumps(event.data, ensure_ascii=False)}")
            completion_events.append(event)
        
        # 监听延迟和审批完成事件 - 使用正确的事件类型
        delay_handler_id = await event_bus.register_handler(event_type="delay_trigger", handler=completion_handler)
        approval_handler_id = await event_bus.register_handler(event_type="approval_decision", handler=completion_handler)
        logger.info(f"事件处理器已注册: delay={delay_handler_id}, approval={approval_handler_id}")
        
        # 注册节点类型
        from plaita.node import get_default_registry
        registry = get_default_registry()
        for node_class in [DelayNode, ApprovalNode, HttpCallbackNode, KafkaQueueNode]:
            registry.register(node_class)
            logger.info(f"已注册节点类型: {node_class.node_type}")
        
        # 启动服务
        service_configs = {
            "delay": {"max_workers": 2},
            "approval": {"max_workers": 1}
        }
        
        success = service_manager.start_all_services(service_configs)
        logger.info(f"服务启动状态: {'成功' if success else '失败'}")
        
        # 演示1: 延迟节点
        logger.info("\n--- 延迟节点演示 ---")
        delay_node = DelayNode(id="test_delay", delay_seconds=2)
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution.set_state("$EXECUTION_ID", "test_001")
        
        result = delay_node.execute(execution)
        logger.info(f"延迟节点状态: {result.get('status')}")
        
        # 提交延迟任务
        service_config = result.get("service_config", {})
        if service_config:
            task_id = service_manager.handle_node_config(service_config)
            logger.info(f"延迟任务ID: {task_id}")
            
            # 等待延迟完成事件
            logger.info("等待延迟完成事件...")
            start_time = time.time()
            delay_event_received = False
            
            # 延长等待时间并增加事件处理的等待
            while time.time() - start_time < 6:  # 6秒超时
                await asyncio.sleep(0.2)  # 更频繁检查
                
                # 给事件处理器一些时间
                await asyncio.sleep(0.1)
                
                for event in completion_events:
                    if event.event_type == "delay_trigger" and event.data.get("execution_id") == "test_001":
                        logger.info("✓ 延迟事件已收到并处理")
                        delay_event_received = True
                        break
                if delay_event_received:
                    break
            
            if not delay_event_received:
                logger.warning("延迟事件超时未收到")
                # 检查是否有其他类型的延迟事件
                for event in completion_events:
                    if event.event_type == "delay_trigger":
                        logger.info(f"发现延迟事件但execution_id不匹配: {event.data}")
        
        # 演示2: 审批节点
        logger.info("\n--- 审批节点演示 ---")
        approval_node = ApprovalNode(
            id="test_approval",
            event_type="approval_decision",  # 使用正确的事件类型
            approval_title="演示审批",
            approval_content="这是一个简单的审批演示",
            approvers=["admin"],
            approval_strategy="any"
        )
        
        execution2 = FlowExecution(event_bus=event_bus)
        execution2.clean()
        execution2.set_state("$EXECUTION_ID", "test_002")
        
        result = approval_node.execute(execution2)
        logger.info(f"审批节点状态: {result.get('status')}")
        
        # 提交审批任务
        service_config = result.get("service_config", {})
        if service_config:
            task_id = service_manager.handle_node_config(service_config)
            logger.info(f"审批任务ID: {task_id}")
            
            # 模拟审批决策
            approval_service = service_manager.get_service("approval")
            if approval_service:
                approval_id = service_config.get("approval_id")
                if approval_id:
                    # 稍等一下让审批任务注册完成
                    await asyncio.sleep(1)
                    
                    decision_result = await approval_service.submit_approval_decision(
                        approval_id, "admin", "approve", "同意"
                    )
                    logger.info(f"审批决策提交: {decision_result.get('decision', '未知')}")
                    
                    # 等待审批完成事件
                    logger.info("等待审批完成事件...")
                    start_time = time.time()
                    approval_event_received = False
                    
                    while time.time() - start_time < 3:  # 3秒超时
                        await asyncio.sleep(0.2)
                        
                        # 给事件处理器一些时间
                        await asyncio.sleep(0.1)
                        
                        for event in completion_events:
                            if event.event_type == "approval_decision" and event.data.get("execution_id") == "test_002":
                                logger.info("✓ 审批事件已收到并处理")
                                approval_event_received = True
                                break
                        if approval_event_received:
                            break
                    
                    if not approval_event_received:
                        logger.warning("审批事件超时未收到")
        
        # 等待一下确保所有异步任务完成
        logger.info("等待异步任务完成...")
        await asyncio.sleep(1)
        
        # 清理事件处理器
        await event_bus.unregister_handler(delay_handler_id)
        await event_bus.unregister_handler(approval_handler_id)
        
        # 清理
        service_manager.stop_all_services(timeout=5)
        logger.info("服务已停止")
        
    except Exception as e:
        logger.error(f"简化演示出错: {e}", exc_info=True)
    
    logger.info("=== 简化演示完成 ===")


async def simple_delay_test():
    """简化的延迟节点测试"""
    logger.info("=== 简化延迟节点测试开始 ===")
    
    try:
        demo = ExtendedNodesDemo()
        await demo.setup()
        
        # 只测试延迟节点
        logger.info("开始延迟节点测试...")
        success = await demo.test_delay_flow()
        logger.info(f"延迟节点测试结果: {'成功' if success else '失败'}")
        
        await demo.cleanup()
        
    except Exception as e:
        logger.error(f"测试出错: {e}", exc_info=True)
    
    logger.info("=== 简化延迟节点测试完成 ===")

async def simple_single_test(test_type: str):
    logger.info(f"=== 简化{test_type}测试开始 ===")
    
    try:
        demo = ExtendedNodesDemo()
        await demo.setup()
        
        # 只测试延迟节点
        logger.info(f"开始{test_type}测试...")
        if test_type == "delay":
            success = await demo.test_delay_flow()
        elif test_type == "approval":
            success = await demo.test_approval_flow()
        elif test_type == "http_callback":
            success = await demo.test_http_callback_flow()
        elif test_type == "redis":
            success = await demo.test_redis_flow()
        elif test_type == "kafka":
            success = await demo.test_kafka_flow() 
        logger.info(f"{test_type}测试结果: {'成功' if success else '失败'}")
        
        await demo.cleanup()
        
    except Exception as e:
        logger.error(f"测试出错: {e}", exc_info=True)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "simple":
            # 运行简化演示
            asyncio.run(simple_demo())
        else:
            asyncio.run(simple_single_test(sys.argv[1]))
            print("可用类型: simple, delay, approval, redis, kafka, http_callback")
    else:
        # 运行完整演示
        asyncio.run(main())