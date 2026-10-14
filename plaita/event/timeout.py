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


class TimeoutCallbackError(RuntimeError):
    """一轮超时检查里至少一个回调失败（plaita#153 / R1）。

    聚合异常：单条订阅的回调失败**只记本订阅的账**，本轮其余回调与其余订阅
    （例如 subB 的超时唤醒）照常执行；整轮扫完后再聚合上抛——用「收集 + 末尾
    聚合上抛」替代「第一个异常就中断整轮」，同时保住两件事：

    * **liveness**：坏回调不再饿死同轮其它回调/订阅（不引入轮级中断）；
    * **可观察**：失败仍以上抛形式抵达 ``_check_loop``（记 ERROR 后下一轮重试），
      绝不退回「只剩一行日志」的静默吞咽。

    权衡：上抛类型从「原始异常」变成聚合异常，调用方若要按原始类型捕获需走
    ``failures``；原始异常保留在 :attr:`failures`（并作为 ``__cause__``）。
    """

    def __init__(self, failures: List[tuple]):
        #: ``[(subscription_id, 原始异常), ...]``，按发生顺序
        self.failures = list(failures)
        self.subscription_ids = [sub_id for sub_id, _exc in self.failures]
        super().__init__(
            "本轮超时检查有 %d 个回调失败（订阅: %s）"
            % (len(self.failures), ", ".join(str(s) for s in self.subscription_ids))
        )


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
                # #153/R1：失败轮已由 `_check_timeouts` 逐条记 ERROR（含 sub_id 与
                # exc_info），这里再记一次聚合并进入下一轮重试——循环绝不终止。
                logger.error("超时检查出错: %s", e, exc_info=True)

            await asyncio.sleep(self.check_interval)

    async def _check_timeouts(self) -> None:
        """检查所有已超时的订阅。

        #153/R1：单个回调失败**不得中止本轮**——就地记 ERROR（含 sub_id 与
        ``exc_info``）并把 ``(sub_id, 异常)`` 收进 ``failures``，然后继续跑同
        订阅的其余回调与其余订阅。整轮扫完后若有失败，聚合上抛
        :class:`TimeoutCallbackError`，由 ``_check_loop`` 记日志后下一轮重试。

        （回归背景：r4 把包装改成立即 ``raise`` ⇒ 回调整轮中断，同轮第二个回调与
        后续订阅被饿死——那是本单引入的 liveness 回归，见 tests/unit/
        test_wave4_subscription_timeout.py 的两条多对象用例。）
        """
        current_time = time.time()
        subscriptions = await self.subscription_storage.list_subscriptions()

        failures: List[tuple] = []
        for subscription in subscriptions:
            # 只检查设置了超时的订阅
            if subscription.timeout is None:
                continue
            # 计算订阅创建后的经过时间
            if current_time - subscription.created_at <= subscription.timeout:
                continue
            # 通知所有注册的回调：单个回调失败只记账，不中断本轮
            for callback in self._on_timeout_callbacks:
                try:
                    await callback(subscription)
                except Exception as e:  # noqa: BLE001 — 收集后整轮末尾聚合上抛
                    failures.append((subscription.subscription_id, e))
                    logger.error(
                        "超时回调执行出错（订阅 %s）: %s",
                        subscription.subscription_id, e, exc_info=True,
                    )
        if failures:
            raise TimeoutCallbackError(failures) from failures[0][1]
