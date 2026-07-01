"""
独立的订阅超时检查功能
"""
import asyncio
import logging
import time
from typing import Any, Callable, List, Optional, Dict, Awaitable

from .core import EventSubscription

# 获取logger
logger = logging.getLogger("plaita.event")


class SubscriptionTimeoutChecker:
    """
    独立的订阅超时检查器，可以与任何支持存储订阅的后端一起使用
    """
    
    def __init__(self, subscription_storage, check_interval: float = 10.0):
        """
        初始化超时检查器
        
        Args:
            subscription_storage: 订阅存储接口，需要支持list_subscriptions和delete_subscription方法
            check_interval: 检查间隔时间（秒）
        """
        self.subscription_storage = subscription_storage
        self.check_interval = check_interval
        self._check_task = None
        self._running = False
        self._on_timeout_callbacks = []
    
    def register_timeout_callback(self, callback: Callable[[EventSubscription], Awaitable[None]]) -> None:
        """注册订阅超时回调函数"""
        self._on_timeout_callbacks.append(callback)
    
    async def start(self) -> None:
        """启动超时检查任务"""
        if self._running:
            return
        
        self._running = True
        self._check_task = asyncio.create_task(self._check_loop())
    
    async def stop(self) -> None:
        """停止超时检查任务"""
        self._running = False
        if self._check_task and not self._check_task.done():
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
    
    async def _check_loop(self) -> None:
        """超时检查循环"""
        while self._running:
            try:
                await self._check_timeouts()
            except Exception as e:
                logger.error(f"超时检查出错: {e}")
            
            await asyncio.sleep(self.check_interval)
    
    async def _check_timeouts(self) -> None:
        """检查所有已超时的订阅"""
        current_time = time.time()
        subscriptions = await self.subscription_storage.list_subscriptions()
        
        for subscription in subscriptions:
            # 只检查设置了超时的订阅
            if subscription.timeout is not None:
                # 计算订阅创建后的经过时间
                elapsed_time = current_time - subscription.created_at
                
                # 如果超过了设置的超时时间
                if elapsed_time > subscription.timeout:
                    # 通知所有注册的回调
                    for callback in self._on_timeout_callbacks:
                        try:
                            await callback(subscription)
                        except Exception as e:
                            logger.error(f"超时回调执行出错: {e}") 