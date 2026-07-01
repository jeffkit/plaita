#!/usr/bin/env python3
"""
测试事件总线的通配符匹配功能
"""
import asyncio
import logging
from typing import List

from plaita.event import InMemoryEventBus, Event

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_wildcard")

class TestWildcardEvents:
    """测试通配符事件匹配"""
    
    def __init__(self):
        self.event_bus = InMemoryEventBus()
        self.received_events = []
    
    async def test_exact_match(self):
        """测试精确匹配"""
        logger.info("=== 测试精确匹配 ===")
        
        # 注册精确匹配处理器
        await self.event_bus.register_handler(
            event_type="user.login",
            handler=self.make_handler("exact_match")
        )
        
        # 发布匹配的事件
        await self.event_bus.publish("user.login", user_id="123")
        await asyncio.sleep(0.1)
        
        # 发布不匹配的事件
        await self.event_bus.publish("user.logout", user_id="123")
        await asyncio.sleep(0.1)
        
        # 验证结果
        exact_events = [e for e in self.received_events if e["handler"] == "exact_match"]
        assert len(exact_events) == 1
        assert exact_events[0]["event"].event_type == "user.login"
        logger.info("✓ 精确匹配测试通过")
    
    async def test_wildcard_all(self):
        """测试匹配所有事件"""
        logger.info("=== 测试匹配所有事件 ===")
        
        # 注册匹配所有事件的处理器（使用None）
        await self.event_bus.register_handler(
            event_type=None,
            handler=self.make_handler("wildcard_all_none")
        )
        
        # 注册匹配所有事件的处理器（使用*）
        await self.event_bus.register_handler(
            event_type="*",
            handler=self.make_handler("wildcard_all_star")
        )
        
        # 发布多种类型的事件
        test_events = ["user.login", "user.logout", "order.create", "system.start"]
        for event_type in test_events:
            await self.event_bus.publish(event_type, data={"test": True})
            await asyncio.sleep(0.1)
        
        # 验证结果
        none_events = [e for e in self.received_events if e["handler"] == "wildcard_all_none"]
        star_events = [e for e in self.received_events if e["handler"] == "wildcard_all_star"]
        
        assert len(none_events) == len(test_events)
        assert len(star_events) == len(test_events)
        
        # 验证收到了所有类型的事件
        received_types_none = set(e["event"].event_type for e in none_events)
        received_types_star = set(e["event"].event_type for e in star_events)
        assert received_types_none == set(test_events)
        assert received_types_star == set(test_events)
        
        logger.info("✓ 匹配所有事件测试通过")
    
    async def test_prefix_wildcard(self):
        """测试前缀通配符"""
        logger.info("=== 测试前缀通配符 ===")
        
        # 注册前缀匹配处理器
        await self.event_bus.register_handler(
            event_type="user.*",
            handler=self.make_handler("prefix_wildcard")
        )
        
        # 发布匹配的事件
        user_events = ["user.login", "user.logout", "user.register", "user.update"]
        for event_type in user_events:
            await self.event_bus.publish(event_type, user_id="123")
            await asyncio.sleep(0.1)
        
        # 发布不匹配的事件
        non_user_events = ["order.create", "system.start", "admin.login"]
        for event_type in non_user_events:
            await self.event_bus.publish(event_type, data={"test": True})
            await asyncio.sleep(0.1)
        
        # 验证结果
        prefix_events = [e for e in self.received_events if e["handler"] == "prefix_wildcard"]
        received_types = set(e["event"].event_type for e in prefix_events)
        
        assert len(prefix_events) == len(user_events)
        assert received_types == set(user_events)
        
        # 确保不匹配的事件没有被处理
        for event_type in non_user_events:
            assert event_type not in received_types
        
        logger.info("✓ 前缀通配符测试通过")
    
    async def test_suffix_wildcard(self):
        """测试后缀通配符"""
        logger.info("=== 测试后缀通配符 ===")
        
        # 注册后缀匹配处理器
        await self.event_bus.register_handler(
            event_type="*.login",
            handler=self.make_handler("suffix_wildcard")
        )
        
        # 发布匹配的事件
        login_events = ["user.login", "admin.login", "guest.login"]
        for event_type in login_events:
            await self.event_bus.publish(event_type, user_id="123")
            await asyncio.sleep(0.1)
        
        # 发布不匹配的事件
        non_login_events = ["user.logout", "admin.create", "system.start"]
        for event_type in non_login_events:
            await self.event_bus.publish(event_type, data={"test": True})
            await asyncio.sleep(0.1)
        
        # 验证结果
        suffix_events = [e for e in self.received_events if e["handler"] == "suffix_wildcard"]
        received_types = set(e["event"].event_type for e in suffix_events)
        
        assert len(suffix_events) == len(login_events)
        assert received_types == set(login_events)
        
        # 确保不匹配的事件没有被处理
        for event_type in non_login_events:
            assert event_type not in received_types
        
        logger.info("✓ 后缀通配符测试通过")
    
    async def test_middle_wildcard(self):
        """测试中间通配符"""
        logger.info("=== 测试中间通配符 ===")
        
        # 注册中间匹配处理器
        await self.event_bus.register_handler(
            event_type="*.user.*",
            handler=self.make_handler("middle_wildcard")
        )
        
        # 发布匹配的事件
        user_events = ["app.user.login", "sys.user.update", "admin.user.create"]
        for event_type in user_events:
            await self.event_bus.publish(event_type, user_id="123")
            await asyncio.sleep(0.1)
        
        # 发布不匹配的事件
        non_user_events = ["app.order.create", "sys.config.update", "user.login"]
        for event_type in non_user_events:
            await self.event_bus.publish(event_type, data={"test": True})
            await asyncio.sleep(0.1)
        
        # 验证结果
        middle_events = [e for e in self.received_events if e["handler"] == "middle_wildcard"]
        received_types = set(e["event"].event_type for e in middle_events)
        
        assert len(middle_events) == len(user_events)
        assert received_types == set(user_events)
        
        # 确保不匹配的事件没有被处理
        for event_type in non_user_events:
            assert event_type not in received_types
        
        logger.info("✓ 中间通配符测试通过")
    
    def make_handler(self, handler_name: str):
        """创建测试处理器"""
        async def handler(event: Event):
            logger.info(f"Handler '{handler_name}' 收到事件: {event.event_type}")
            self.received_events.append({
                "handler": handler_name,
                "event": event
            })
        return handler
    
    async def run_all_tests(self):
        """运行所有测试"""
        logger.info("开始测试事件总线通配符匹配功能")
        
        tests = [
            self.test_exact_match,
            self.test_wildcard_all,
            self.test_prefix_wildcard,
            self.test_suffix_wildcard,
            self.test_middle_wildcard
        ]
        
        for test in tests:
            # 清理之前的事件
            self.received_events.clear()
            
            try:
                await test()
            except Exception as e:
                logger.error(f"测试 {test.__name__} 失败: {e}")
                raise
        
        logger.info("所有通配符匹配测试通过！")


async def main():
    """主函数"""
    test = TestWildcardEvents()
    await test.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main()) 