#!/usr/bin/env python3
"""
测试EventFilter的通配符匹配功能
"""
import asyncio
import json
import logging
from unittest.mock import Mock

from plaita.event import InMemoryEventBus, InMemoryEventSubscriptionStorage
from plaita.event.core import Event
from plaita.server.event_filter import EventFilter
from plaita.storage.memory import MemoryExecutionStorage
from plaita.storage.base import ExecutionState

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_event_filter")

class DebugEventFilter(EventFilter):
    """带调试功能的EventFilter"""
    
    async def handle_event(self, event: Event) -> None:
        """处理事件，添加详细的调试信息"""
        try:
            logger.info(f"接收到事件: {event.event_id}, 类型: {event.event_type}, correlation_id: {event.correlation_id}")
            
            # 如果事件没有correlation_id，无法关联到流程执行
            if not event.correlation_id:
                logger.debug(f"事件没有correlation_id，跳过处理: {event.event_id}")
                return
            
            # 使用correlation_id作为execution_id查询执行状态
            execution_id = event.correlation_id
            state = self.execution_storage.load_execution_state(execution_id)
            
            logger.info(f"查询执行状态: execution_id={execution_id}, state={state is not None}")
            
            if not state:
                logger.debug(f"找不到关联的执行状态，跳过处理: {execution_id}")
                return
            
            # 查询与事件匹配的订阅
            logger.info(f"查询匹配的订阅，event_type={event.event_type}, correlation_id={event.correlation_id}")
            
            # 使用基类的find_matching_subscriptions方法
            subscriptions = await self.subscription_storage.find_matching_subscriptions(event, state.context)
            
            logger.info(f"找到匹配的订阅数量: {len(subscriptions)}")
            for sub in subscriptions:
                logger.info(f"匹配订阅: id={sub.subscription_id}, event_type={sub.event_type}, correlation_id={sub.correlation_id}, flow_id={sub.flow_id}")
            
            if not subscriptions:
                logger.debug(f"没有匹配的订阅，跳过处理: {event.event_id}")
                return
            
            # 遍历匹配的订阅，并放入队列
            for subscription in subscriptions:
                logger.info(f"检查订阅关联性: subscription.flow_id={subscription.flow_id}, state.flow_id={state.flow_id}")
                logger.info(f"检查订阅关联性: subscription.correlation_id={subscription.correlation_id}, execution_id={execution_id}")
                
                # 检查订阅是否与当前执行相关(通过flow_id或correlation_id)
                if (subscription.flow_id and subscription.flow_id == state.flow_id) or \
                   (subscription.correlation_id and subscription.correlation_id == execution_id):
                    
                    # 构建resume任务
                    resume_task = {
                        "type": "resume",
                        "flow_id": state.flow_id,
                        "execution_id": execution_id,
                        "resume_type": "event",
                        "data": {
                            "event_id": event.event_id,
                            "event_type": event.event_type,
                            "event_data": event.data,
                            "subscription_id": subscription.subscription_id
                        }
                    }
                    
                    # 将任务放入队列
                    self.redis_client.rpush(
                        self.queue_name,
                        json.dumps(resume_task)
                    )
                    
                    logger.info(f"已将事件 {event.event_id} 添加到队列，关联订阅: {subscription.subscription_id}")
                    
                    # 标记事件为已处理
                    await self.subscription_storage.mark_event_processed(
                        subscription.subscription_id, 
                        event.event_id
                    )
                    
                else:
                    logger.debug(f"订阅与当前执行无关，跳过: {subscription.subscription_id}")
            
        except Exception as e:
            logger.error(f"处理事件出错: {e}", exc_info=True)

class TestEventFilterWildcard:
    """测试EventFilter的通配符功能"""

    __test__ = False  # 手动脚本式测试运行器（见文件末 main()），不让 pytest 采集

    def __init__(self):
        self.event_bus = InMemoryEventBus()
        self.subscription_storage = InMemoryEventSubscriptionStorage()
        self.execution_storage = MemoryExecutionStorage()
        
        # 模拟Redis客户端
        self.mock_redis = Mock()
        self.task_queue = []
        
        # 模拟Redis的rpush方法
        def mock_rpush(queue_name, task_data):
            self.task_queue.append(json.loads(task_data))
            return len(self.task_queue)
        
        self.mock_redis.rpush = mock_rpush
        
        # 创建EventFilter
        self.event_filter = DebugEventFilter(
            execution_storage=self.execution_storage,
            subscription_storage=self.subscription_storage,
            redis_client=self.mock_redis,
            event_bus=self.event_bus
        )
    
    async def setup_test_execution(self, execution_id: str, flow_id: str):
        """设置测试用的执行状态"""
        state = ExecutionState(
            execution_id=execution_id,
            flow_id=flow_id,
            status="running",
            context={
                "$FLOW_ID": flow_id,
                "$EXECUTION_ID": execution_id,
                "$LAST_NODE": "test_node"
            }
        )
        
        # 保存执行状态
        self.execution_storage.save_execution_state(execution_id, state)
        return state
    
    async def test_filter_no_event_type(self):
        """测试EventFilter不指定event_type参数，监听所有事件"""
        logger.info("=== 测试EventFilter监听所有事件 ===")
        
        # 设置测试数据
        execution_id = "test_exec_001"
        flow_id = "test_flow"
        await self.setup_test_execution(execution_id, flow_id)
        
        # 创建订阅 - 使用正确的事件类型和correlation_id
        from plaita.event.core import EventSubscription
        subscription = EventSubscription(
            event_type="user.login",
            correlation_id=execution_id,
            flow_id=flow_id
        )
        await self.subscription_storage.store_subscription(subscription)
        
        # 验证订阅是否正确存储
        stored_subs = await self.subscription_storage.list_subscriptions()
        logger.info(f"存储的订阅数量: {len(stored_subs)}")
        for sub in stored_subs:
            logger.info(f"订阅: event_type={sub.event_type}, correlation_id={sub.correlation_id}, flow_id={sub.flow_id}")
        
        # 启动EventFilter（不指定event_type）
        filter_task = asyncio.create_task(self.event_filter.start())
        await asyncio.sleep(0.1)  # 等待启动完成
        
        # 发布各种类型的事件 - 确保包含correlation_id
        test_events = [
            ("user.login", {"user_id": "123"}),
            ("user.logout", {"user_id": "123"}),
            ("order.create", {"order_id": "456"}),
            ("system.start", {"version": "1.0"})
        ]
        
        for event_type, data in test_events:
            # 手动创建Event对象以确保correlation_id正确设置
            event = Event(
                event_type=event_type,
                data=data,
                correlation_id=execution_id
            )
            await self.event_bus.publish(event)
            await asyncio.sleep(0.1)
        
        # 停止EventFilter
        await self.event_filter.stop()
        filter_task.cancel()
        
        # 调试：打印队列内容
        logger.info(f"队列中的任务数量: {len(self.task_queue)}")
        for i, task in enumerate(self.task_queue):
            logger.info(f"任务 {i}: {task}")
        
        # 验证只有匹配的事件被处理
        assert len(self.task_queue) == 1  # 只有user.login匹配订阅
        task = self.task_queue[0]
        assert task["type"] == "resume"
        assert task["data"]["event_type"] == "user.login"
        
        logger.info("✓ EventFilter监听所有事件测试通过")
    
    async def test_filter_with_specific_event_type(self):
        """测试EventFilter指定特定事件类型"""
        logger.info("=== 测试EventFilter监听特定事件类型 ===")
        
        # 清理之前的任务
        self.task_queue.clear()
        
        # 设置测试数据
        execution_id = "test_exec_002"
        flow_id = "test_flow"
        await self.setup_test_execution(execution_id, flow_id)
        
        # 创建订阅
        from plaita.event.core import EventSubscription
        subscription = EventSubscription(
            event_type="user.login",
            correlation_id=execution_id,
            flow_id=flow_id
        )
        await self.subscription_storage.store_subscription(subscription)
        
        # 启动EventFilter（指定监听user.*事件）
        filter_task = asyncio.create_task(self.event_filter.start("user.*"))
        await asyncio.sleep(0.1)  # 等待启动完成
        
        # 发布各种类型的事件
        test_events = [
            ("user.login", {"user_id": "123"}),
            ("user.logout", {"user_id": "123"}),
            ("order.create", {"order_id": "456"}),  # 这个不会被监听到
            ("admin.login", {"admin_id": "789"})    # 这个也不会被监听到
        ]
        
        for event_type, data in test_events:
            # 手动创建Event对象以确保correlation_id正确设置
            event = Event(
                event_type=event_type,
                data=data,
                correlation_id=execution_id
            )
            await self.event_bus.publish(event)
            await asyncio.sleep(0.1)
        
        # 停止EventFilter
        await self.event_filter.stop()
        filter_task.cancel()
        
        # 验证只有user.login被处理（因为订阅只有这一个，且匹配）
        assert len(self.task_queue) == 1
        task = self.task_queue[0]
        assert task["data"]["event_type"] == "user.login"
        
        logger.info("✓ EventFilter监听特定事件类型测试通过")
    
    async def run_all_tests(self):
        """运行所有测试"""
        logger.info("开始测试EventFilter通配符功能")
        
        tests = [
            self.test_filter_no_event_type,
            self.test_filter_with_specific_event_type
        ]
        
        for test in tests:
            try:
                await test()
            except Exception as e:
                logger.error(f"测试 {test.__name__} 失败: {e}")
                raise
        
        logger.info("所有EventFilter通配符测试通过！")


async def main():
    """主函数"""
    test = TestEventFilterWildcard()
    await test.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main()) 