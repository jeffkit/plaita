"""
延迟服务实现
负责处理延迟任务，在指定时间后触发事件
"""
import asyncio
import json
import threading
import time
from typing import Any, Dict

from .base_service import BaseExtendedService
from ...logger import logger


class DelayService(BaseExtendedService):
    """
    延迟服务
    负责处理延迟任务，在指定时间后触发事件
    """

    def __init__(self, event_bus, service_config=None, redis_client=None):
        # 签名对齐基类：(event_bus, service_config, redis_client)——
        # 位置传参 DelayService(bus, {...}) 时第二位是 service_config
        super().__init__(
            event_bus=event_bus,
            redis_client=redis_client,
            service_config=service_config,
        )
        import os
        self.queue_key = os.environ.get(
            "PLAITA_DELAY_QUEUE",
            (service_config or {}).get("delay_queue", "plaita:delay:queue"),
        )
        self._consumer_thread = None

    def get_service_type(self) -> str:
        """
        获取服务类型

        Returns:
            str: 服务类型
        """
        return "delay"

    def start_service(self) -> bool:
        """
        启动延迟服务

        Returns:
            bool: 启动是否成功
        """
        try:
            self.is_running = True
            # 消费 plaita:delay:queue：worker 挂起时把延迟任务 RPUSH 进来。
            # 历史上没人投递也没人消费，delay 节点的执行会永久挂起。
            if self._redis_client is None or not hasattr(self._redis_client, "blpop"):
                logger.warning("延迟服务无 redis 客户端，队列消费不启动")
                return True
            self._consumer_thread = threading.Thread(
                target=self._consume_queue, name="delay-service-consumer", daemon=True
            )
            self._consumer_thread.start()
            logger.info("延迟服务已启动（队列: %s）", self.queue_key)
            return True
        except Exception as e:
            logger.error("启动延迟服务失败: %s", e, exc_info=True)
            return False

    def stop_service(self) -> bool:
        """
        停止延迟服务

        Returns:
            bool: 停止是否成功
        """
        try:
            self.is_running = False
            logger.info("延迟服务已停止")
            return True
        except Exception as e:
            logger.error("停止延迟服务失败: %s", e, exc_info=True)
            return False

    def _consume_queue(self) -> None:
        """消费延迟任务队列（BLPOP 短超时轮询，保证关闭响应性）。"""
        while not self.is_shutdown_requested():
            try:
                item = self._redis_client.blpop(self.queue_key, timeout=2)
            except Exception as e:
                logger.error("延迟队列消费失败: %s", e, exc_info=True)
                self._shutdown_event.wait(timeout=2)
                continue
            if not item:
                continue
            _key, raw = item
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                task_config = json.loads(raw)
            except json.JSONDecodeError:
                logger.error("延迟任务配置非法: %r", raw[:200])
                continue
            self.submit_task(task_config)

    async def trigger_event(self, event_type: str, event_data: Dict[str, Any]):
        """触发事件：带 correlation_id（=execution_id），EventFilter 才能关联到挂起执行。

        有 Redis 客户端时，直接用同步 redis 客户端发布到引擎 RedisEventBus
        的频道（plaita:events:{type}）。不要走 self.event_bus.publish——
        它的 aioredis 连接绑定在创建时的 event loop 上，而 handle_task
        运行在线程池新开的 loop 里，跨 loop 使用会静默失败。

        无 Redis 客户端（进程内 InMemoryEventBus 场景，如 examples/server_demo）
        时回退到 self.event_bus.publish，否则 publish 必然抛
        AttributeError: 'NoneType' object has no attribute 'publish'，
        事件永远到不了总线，挂起流程无法恢复。
        """
        from ...event.core import Event

        event = Event(
            event_type=event_type,
            data=event_data,
            correlation_id=event_data.get("execution_id"),
        )
        try:
            if self._redis_client is None:
                await self.event_bus.publish(event)
            else:
                self._redis_client.publish(
                    f"plaita:events:{event_type}", event.model_dump_json()
                )
            logger.info(
                "事件已触发: %s (correlation_id=%s)", event_type, event.correlation_id
            )
        except Exception as e:
            logger.error("触发事件失败: %s", e, exc_info=True)

    
    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """
        处理延迟任务
        
        Args:
            task_config: 任务配置
            
        Returns:
            bool: 处理是否成功
        """
        try:
            # 从配置中获取延迟信息
            delay_ms = task_config.get("delay_ms", 0)
            trigger_timestamp = task_config.get("trigger_timestamp")
            node_id = task_config.get("node_id")
            execution_id = task_config.get("execution_id")
            flow_id = task_config.get("flow_id")
            event_type = task_config.get("event_type")
            
            logger.info("开始处理延迟任务: node_id=%s, delay_ms=%s", node_id, delay_ms)
            
            # 计算实际需要等待的时间
            current_time = int(time.time() * 1000)
            if trigger_timestamp:
                # 使用绝对时间戳
                wait_ms = max(0, trigger_timestamp - current_time)
            else:
                # 使用相对延迟时间
                wait_ms = delay_ms
            
            # 如果需要等待的时间太长，可以考虑分段等待
            if wait_ms > 0:
                wait_seconds = wait_ms / 1000.0
                logger.info("延迟任务等待中: %s秒", wait_seconds)
                
                # 分段等待，每次最多等待60秒，以便及时响应关闭请求
                while wait_seconds > 0 and not self.is_shutdown_requested():
                    chunk_wait = min(60, wait_seconds)
                    await asyncio.sleep(chunk_wait)
                    wait_seconds -= chunk_wait
                
                # 检查是否被要求关闭
                if self.is_shutdown_requested():
                    logger.info("延迟任务被中断: node_id=%s", node_id)
                    return False
            
            # 构造事件数据
            event_data = {
                "node_id": node_id,
                "execution_id": execution_id,
                "flow_id": flow_id,
                "trigger_type": "delay_completed",
                "delay_ms": delay_ms,
                "actual_trigger_timestamp": int(time.time() * 1000),
                "planned_trigger_timestamp": trigger_timestamp,
                "success": True
            }
            
            # 触发事件
            await self.trigger_event(event_type, event_data)
            
            logger.info("延迟任务完成: node_id=%s", node_id)
            return True
            
        except Exception as e:
            logger.error("处理延迟任务失败: %s", e, exc_info=True)
            
            # 触发错误事件
            try:
                error_event_data = {
                    "node_id": task_config.get("node_id"),
                    "execution_id": task_config.get("execution_id"),
                    "flow_id": task_config.get("flow_id"),
                    "trigger_type": "delay_error",
                    "error_message": str(e),
                    "success": False
                }
                await self.trigger_event(task_config.get("event_type"), error_event_data)
            except Exception:
                logger.warning("delay error-event trigger failed", exc_info=True)

            return False
    
    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """
        验证延迟任务配置
        
        Args:
            task_config: 任务配置
            
        Returns:
            bool: 配置是否有效
        """
        # 调用父类验证
        if not super().validate_task_config(task_config):
            return False
        
        # 验证延迟特定字段
        delay_ms = task_config.get("delay_ms")
        trigger_timestamp = task_config.get("trigger_timestamp")
        
        if delay_ms is None and trigger_timestamp is None:
            logger.error("延迟任务必须指定 delay_ms 或 trigger_timestamp")
            return False
        
        if delay_ms is not None and delay_ms < 0:
            logger.error("延迟时间不能为负数")
            return False
        
        if trigger_timestamp is not None and trigger_timestamp <= int(time.time() * 1000):
            logger.warning("触发时间戳已过期，将立即触发")
        
        return True
    
    def get_pending_tasks_info(self) -> Dict[str, Any]:
        """
        获取待处理任务信息
        
        Returns:
            Dict[str, Any]: 任务信息
        """
        return {
            "service_type": self.get_service_type(),
            "active_task_count": self.get_active_task_count(),
            "is_running": self.is_running,
            "max_workers": self.get_max_workers()
        } 