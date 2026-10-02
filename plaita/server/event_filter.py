"""
事件过滤器，用于接收事件并过滤出与当前事件相关的订阅
将订阅信息组装成flow worker任务放入队列
"""
import json
import os
import logging
import asyncio
import argparse
import sys
from typing import Optional, Dict, Any, List

from redis import Redis

from plaita.event.core import Event, EventSubscription, EventSubscriptionStorage, EventBus
from plaita.event.timeout import SubscriptionTimeoutChecker
from plaita.storage.base import ExecutionStorage
from plaita.server.task_queue import enqueue_task
from plaita.server.tenant_context import (
    reset_current_tenant,
    set_current_tenant,
)

# 获取logger
logger = logging.getLogger("plaita.server.event_filter")

# 执行终态集合（ReviewFix D2）。必须与 flow_worker.resume_flow 的终态短路
# 集合（completed / error / cancelled）对齐：cancelled 曾被本文件 GC 漏掉——
# console 取消执行后，残留订阅在 TTL（约 7 天）内每个匹配事件都会入队一条
# 注定在 worker 侧被终态短路丢弃的 resume，持续制造无效任务。
# 理想做法是抽成跨文件共享常量（如放 plaita.storage.base 或独立常量模块，
# flow_worker 一并引用）；受本次修复文件白名单约束先就地定义，后续可上移。
TERMINAL_EXECUTION_STATUSES = ("completed", "error", "cancelled")


class EventFilter:
    """
    事件过滤器，接收事件并处理与流程执行相关的订阅
    将匹配的订阅组装成任务放入flow worker队列
    """
    
    def __init__(
        self,
        execution_storage: ExecutionStorage,
        subscription_storage: EventSubscriptionStorage,
        redis_client: Redis,
        event_bus: EventBus,
        queue_name: str = "plaita:flow:queue",
        enable_subscription_timeout_checker: bool = True,
        timeout_check_interval: float = 10.0,
    ):
        """
        初始化事件过滤器

        Args:
            execution_storage: 执行状态存储
            subscription_storage: 事件订阅存储
            redis_client: Redis客户端
            event_bus: 事件总线
            queue_name: 流程工作器队列名称
            enable_subscription_timeout_checker: 是否随本过滤器启动订阅超时检查器
                （波次④回滚开关：置 False 即回到「订阅无限等待」的历史现状）
            timeout_check_interval: 订阅超时检查器的轮询间隔（秒）
        """
        self.execution_storage = execution_storage
        self.subscription_storage = subscription_storage
        self.redis_client = redis_client
        self.event_bus = event_bus
        self.queue_name = queue_name
        self._running = False
        self._subscription_id = None
        # 波次④：EventNode 订阅自动超时的宿主。checker 只消费 subscription.timeout
        # 非空的订阅——存量订阅（timeout=None）即使 checker 在跑也零变化。
        self._timeout_checker: Optional[SubscriptionTimeoutChecker] = None
        if enable_subscription_timeout_checker:
            self._timeout_checker = SubscriptionTimeoutChecker(
                subscription_storage, check_interval=timeout_check_interval
            )
            self._timeout_checker.register_timeout_callback(self._on_subscription_timeout)
    
    async def handle_event(self, event: Event) -> None:
        """
        处理事件，实现EventHandler接口
        
        Args:
            event: 接收到的事件对象
        """
        try:
            logger.info("接收到事件: %s, 类型: %s", event.event_id, event.event_type)
            
            # 如果事件没有correlation_id，无法关联到流程执行
            if not event.correlation_id:
                logger.debug("事件没有correlation_id，跳过处理: %s", event.event_id)
                return
            
            # 使用correlation_id作为execution_id查询执行状态。
            # 多租户：租户取自事件数据（挂起服务透传 / console 发布时写入），
            # 缺省 default（兼容旧事件）；执行状态经租户路由存储读取，
            # resume 消息同样携带租户（worker 据此路由定义/状态/租约）。
            execution_id = event.correlation_id
            event_data = event.data if isinstance(event.data, dict) else {}
            tenant_token = set_current_tenant(event_data.get("tenant_id"))
            try:
                state = self.execution_storage.load_execution_state(execution_id)
            finally:
                reset_current_tenant(tenant_token)

            if not state:
                logger.debug("找不到关联的执行状态，跳过处理: %s", execution_id)
                return
            
            # 查询与事件匹配的订阅
            subscriptions = await self.subscription_storage.find_matching_subscriptions(event, state.context)
            
            if not subscriptions:
                # 升级演练 P1-1：0.4.x 的订阅挂在**未解析表达式**的 type 索引下
                # （如 `type:$INPUT.event_type`），升级后 EventFilter 按解析后的
                # event_type 检索永远查不到——升级前挂起在 event 节点的执行，
                # 事件驱动 resume 全部静默失效。无匹配时检查同 flow 的孤儿订阅
                # 并大声告警。
                try:
                    flow_subs = await self.subscription_storage.list_subscriptions()
                    orphans = [
                        s for s in (flow_subs or [])
                        if getattr(s, "flow_id", None) == state.flow_id
                    ]
                except Exception:  # noqa: BLE001 — 巡检失败不影响主流程
                    orphans = []
                if orphans:
                    logger.warning(
                        "执行 %s（flow %s）没有匹配本事件的订阅，但同 flow 下存在 %d 条"
                        "孤儿订阅（类型: %s）。旧版本订阅可能挂在未解析的 $INPUT.* type "
                        "索引下——事件驱动 resume 不会生效，请手动 resume_flow 或重新"
                        "注册订阅。",
                        execution_id, state.flow_id, len(orphans),
                        sorted({s.event_type for s in orphans}),
                    )
                logger.debug("没有匹配的订阅，跳过处理: %s", event.event_id)
                return
            
            # 终态执行的残留订阅就地回收（2026-09 分布式评审 P2-5）：
            # error 路径泄漏的订阅会被后续同类事件反复匹配并入队注定失败的
            # resume；检测到终态直接注销订阅、跳过入队。没有这步 GC，残留
            # 键只能等 7 天 TTL。
            state_status = getattr(state, "status", "") or ""
            if state_status in TERMINAL_EXECUTION_STATUSES:
                for subscription in subscriptions:
                    try:
                        await self.subscription_storage.unregister_subscription(
                            subscription.subscription_id
                        )
                        logger.info(
                            "执行 %s 已终态(%s)，回收残留订阅 %s",
                            execution_id, state_status, subscription.subscription_id,
                        )
                    except Exception:  # noqa: BLE001 — 回收失败不影响主流程
                        logger.debug(
                            "回收订阅 %s 失败", subscription.subscription_id, exc_info=True
                        )
                return

            for subscription in subscriptions:
                if (subscription.flow_id and subscription.flow_id == state.flow_id) or \
                   (subscription.correlation_id and subscription.correlation_id == execution_id):
                    
                    dedup_key = f"plaita:event_filter:dedup:{event.event_id}:{subscription.subscription_id}"
                    if not self.redis_client.set(dedup_key, "1", nx=True, ex=3600):
                        logger.debug("事件已被其他实例处理，跳过: %s/%s", event.event_id, subscription.subscription_id)
                        continue
                    
                    resume_task = {
                        "type": "resume",
                        "flow_id": state.flow_id,
                        "execution_id": execution_id,
                        "resume_type": "event",
                        "tenant_id": getattr(state, "tenant_id", None)
                        or event_data.get("tenant_id")
                        or "default",
                        "data": {
                            "event_id": event.event_id,
                            "event_type": event.event_type,
                            "event_data": event.data,
                            "subscription_id": subscription.subscription_id
                        }
                    }
                    
                    enqueue_task(self.redis_client, self.queue_name, resume_task)
                    
                    logger.info("已将事件 %s 入队 stream %s，关联订阅: %s", event.event_id, self.queue_name, subscription.subscription_id)
                    
                    await self.subscription_storage.mark_event_processed(
                        subscription.subscription_id, 
                        event.event_id
                    )
                    
                else:
                    logger.debug("订阅与当前执行无关，跳过: %s", subscription.subscription_id)
            
        except Exception as e:
            logger.error("处理事件出错: %s", e, exc_info=True)
    
    async def _on_subscription_timeout(self, subscription: EventSubscription) -> None:
        """SubscriptionTimeoutChecker 回调：订阅超时 → 入队 resume_type=timeout。

        复用 ``handle_event`` 的 resume 任务形状与去重键模式（设计稿 §3.4）：
        worker 侧 ``_handle_resume`` 白名单已放行 timeout，resume 完成后由
        ``_unregister_suspended_subscription`` 注销订阅，checker 自然不再看到它。
        """
        sub_id = subscription.subscription_id
        execution_id = subscription.correlation_id
        if not execution_id:
            # 无法关联执行的订阅无从 resume（正常挂起订阅都带 correlation_id）
            logger.debug("订阅 %s 超时但无 correlation_id，跳过", sub_id)
            return

        # 挂起订阅本身不携带租户（租户随事件数据传递，超时路径没有事件载体），
        # 按 default 命名空间加载执行状态（default/空租户 = 历史前缀 plaita）。
        tenant_token = set_current_tenant(None)
        try:
            state = self.execution_storage.load_execution_state(execution_id)
        finally:
            reset_current_tenant(tenant_token)

        if not state:
            logger.warning(
                "订阅 %s 超时但找不到执行状态 %s，跳过（不注销订阅，待执行状态恢复后可再触发）",
                sub_id, execution_id,
            )
            return

        # 终态执行（含取消）的残留订阅就地回收——与 handle_event 的终态 GC
        # 同语义：否则残留订阅在去重键 TTL 过期后反复触发注定被 worker 终态
        # 短路丢弃的 timeout resume。delete_subscription 是 EventSubscriptionStorage
        # ABC 方法，各后端（redis/memory/sqlalchemy）均实现。
        state_status = getattr(state, "status", "") or ""
        if state_status in TERMINAL_EXECUTION_STATUSES:
            try:
                await self.subscription_storage.delete_subscription(sub_id)
                logger.info(
                    "执行 %s 已终态(%s)，订阅 %s 超时后回收",
                    execution_id, state_status, sub_id,
                )
            except Exception:  # noqa: BLE001 — 回收失败不影响主流程
                logger.debug("回收超时订阅 %s 失败", sub_id, exc_info=True)
            return

        # 去重：多实例 event_filter / 同一订阅重复触发只入队一次（同 handle_event
        # 的 SET NX 模式）。TTL 过期后若订阅仍存在（worker 未成功 resume），
        # checker 可再次触发——at-least-once 而非至多一次。
        dedup_key = f"plaita:event_filter:timeout:{sub_id}"
        if not self.redis_client.set(dedup_key, "1", nx=True, ex=3600):
            logger.debug("订阅超时已被处理（其他实例或先前轮次），跳过: %s", sub_id)
            return

        resume_task = {
            "type": "resume",
            "flow_id": state.flow_id,
            "execution_id": execution_id,
            "resume_type": "timeout",
            "tenant_id": getattr(state, "tenant_id", None) or "default",
            "data": {
                "subscription_id": sub_id,
                "event_type": subscription.event_type,
                "node_id": subscription.node_id,
            },
        }

        enqueue_task(self.redis_client, self.queue_name, resume_task)
        logger.info(
            "订阅 %s 超时，已入队 timeout resume（执行 %s，队列 %s）",
            sub_id, execution_id, self.queue_name,
        )

    async def start(self, event_type: Optional[str] = None):
        """
        启动事件过滤器，开始监听事件

        Args:
            event_type: 要监听的事件类型，默认监听所有事件
        """
        if self._running:
            logger.warning("事件过滤器已经在运行中")
            return

        self._running = True

        try:
            # 订阅事件，如果未指定event_type则监听所有事件
            self._subscription_id = await self.event_bus.register_handler(
                event_type=event_type,  # None表示监听所有事件
                handler=self.handle_event
            )

            event_type_desc = event_type if event_type else "所有事件类型"
            logger.info("事件过滤器已启动，订阅ID: %s, 监听事件类型: %s", self._subscription_id, event_type_desc)

            # 波次④：订阅超时检查器随过滤器同生命周期启停（checker 不启动即回现状）
            if self._timeout_checker is not None:
                await self._timeout_checker.start()

            # 保持运行直到停止
            while self._running:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error("启动事件过滤器时出错: %s", e)
            self._running = False

    async def stop(self):
        """停止事件过滤器"""
        if not self._running:
            return

        self._running = False

        # 先停订阅超时检查器，再注销事件订阅
        if self._timeout_checker is not None:
            try:
                await self._timeout_checker.stop()
            except Exception as e:
                logger.error("停止订阅超时检查器时出错: %s", e)
        
        # 取消事件订阅
        if self._subscription_id and self.event_bus:
            try:
                # 修正：使用unregister_handler而不是unregister_subscription
                if hasattr(self.event_bus, 'unregister_handler'):
                    await self.event_bus.unregister_handler(self._subscription_id)
                else:
                    # 兼容性处理
                    await self.event_bus.unregister_subscription(self._subscription_id)
                logger.info("已取消事件订阅: %s", self._subscription_id)
            except Exception as e:
                logger.error("取消事件订阅时出错: %s", e)
    
    @staticmethod
    def create_event_filter(
        execution_storage: ExecutionStorage,
        subscription_storage: EventSubscriptionStorage,
        event_bus: EventBus,
        redis_url: str,
        queue_name: Optional[str] = None
    ) -> "EventFilter":
        """
        工厂方法：创建事件过滤器实例
        
        Args:
            execution_storage: 执行状态存储
            subscription_storage: 事件订阅存储
            event_bus: 事件总线
            redis_url: Redis连接URL
            queue_name: 队列名称，默认为"plaita:flow:queue"
            
        Returns:
            EventFilter: 事件过滤器实例
        """
        redis_client = Redis.from_url(redis_url)
        return EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=redis_client,
            event_bus=event_bus,
            queue_name=queue_name or "plaita:flow:queue"
        )


from plaita.server.factory import create_storage_component, create_event_bus  # noqa: F401


async def main_async(args):
    """异步主函数"""
    try:
        # 创建执行状态存储
        storage_kwargs = {
            "redis_url": args.redis_url,
            "database_url": args.database_url
        }
        
        # 创建执行状态存储（多租户路由：按事件租户读对应 namespace）
        execution_storage = create_storage_component(
            args.execution_storage_type,
            "execution",
            tenant_routing=True,
            **storage_kwargs
        )
        logger.info("已创建执行状态存储: %s类型（多租户路由）", args.execution_storage_type)
        
        # 创建事件总线（先于订阅存储：优先复用 bus 自带的 subscription_storage，
        # 避免 worker register_subscription 写入与 filter 读取落在不同后端/实例）
        event_bus = create_event_bus(
            args.event_bus_type,
            **storage_kwargs
        )
        logger.info("已创建事件总线: %s类型", args.event_bus_type)

        bus_storage = getattr(event_bus, "subscription_storage", None)
        if bus_storage is not None:
            subscription_storage = bus_storage
            logger.info(
                "使用事件总线自带的订阅存储（与 register_subscription 同实例/同 keyspace）"
            )
        else:
            subscription_storage = create_storage_component(
                args.subscription_storage_type,
                "subscription",
                **storage_kwargs
            )
            logger.info("已创建事件订阅存储: %s类型", args.subscription_storage_type)
        
        # 创建事件过滤器
        event_filter = EventFilter.create_event_filter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            event_bus=event_bus,
            redis_url=args.redis_url,
            queue_name=args.queue_name
        )
        
        # 启动事件过滤器
        logger.info("事件过滤器启动中，队列名称: %s", args.queue_name)
        await event_filter.start(event_type=args.event_type)
        
    except Exception as e:
        logger.error("事件过滤器启动失败: %s", e, exc_info=True)
        sys.exit(1)

def main():
    """命令行入口程序"""
    parser = argparse.ArgumentParser(description="Plaita事件过滤器")
    
    # Redis参数
    parser.add_argument("--redis-url",
                        default=os.environ.get("PLAITA_REDIS_URL", "redis://localhost:6379/0"),
                        help="Redis 连接地址（默认取 PLAITA_REDIS_URL，与 flow_worker 一致）")
    parser.add_argument("--queue-name", default="plaita:flow:queue",
                      help="Redis队列名称")
    
    # 数据库参数
    parser.add_argument("--database-url", default="sqlite:///flow.db",
                      help="数据库连接URL")
    
    # 存储组件类型（execution 仅 memory|redis；subscription 可为 db，调用方为 async）
    parser.add_argument("--execution-storage-type", choices=["memory", "redis"], default="redis",
                      help="执行状态存储类型（memory|redis；db 已下架）")
    parser.add_argument("--subscription-storage-type", choices=["memory", "redis"], default="redis",
                      help="事件订阅存储类型（db/sqlalchemy 为 experimental，见 factory）")
    
    # 事件总线参数
    parser.add_argument("--event-bus-type", choices=["memory", "redis"], default="redis",
                      help="事件总线类型（生产用 redis）")
    
    # 事件类型过滤
    parser.add_argument("--event-type", type=str, default="",
                      help="要监听的事件类型")
    
    args = parser.parse_args()
    
    # 运行异步主函数
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main() 