"""
延迟服务实现
负责处理延迟任务，在指定时间后触发事件

可靠性（2026-09，plaita#11）：带 Redis 的独立部署形态下，延迟任务出队后
立即落到 ``plaita:delay:pending`` ZSET（score = 触发时刻 epoch ms），由每秒
扫描的 sweeper 到点触发——「已消费、未触发」的任务不再只活在内存线程的
sleep 里。进程重启/被杀后 sweeper 首扫即补触发到期任务，对应执行不再永久
停留在 suspended。触发语义为 at-least-once：先发布事件后 ZREM，崩溃窗口内
的重复触发由 worker 侧幂等 resume 兜住。
"""

import asyncio
import json
import os
import threading
import time
from typing import Any, Dict

from .base_service import BaseExtendedService
from ...logger import logger

# 出队后待触发的延迟任务（ZSET：member=任务 JSON，score=触发时刻 ms）
DELAY_PENDING_KEY = "plaita:delay:pending"
# 触发互斥锁（NX + PX，防多实例双发，同 schedule_service 模式）
DELAY_LOCK_KEY = "plaita:delay:lock:{execution_id}"
DELAY_LOCK_TTL_MS = 30_000


class DelayService(BaseExtendedService):
    """
    延迟服务
    负责处理延迟任务，在指定时间后触发事件

    两种运行形态：
    - 有 Redis（独立 ext-services 进程）：消费 ``plaita:delay:queue`` 后任务
      落 pending ZSET，sweeper 每秒扫描到点触发；重启自动恢复。
    - 无 Redis（进程内 ServiceManager，如 examples/server_demo）：沿用
      handle_task 内存分段 sleep + InMemoryEventBus 发布。
    """

    def __init__(self, event_bus, service_config=None, redis_client=None):
        # 签名对齐基类：(event_bus, service_config, redis_client)——
        # 位置传参 DelayService(bus, {...}) 时第二位是 service_config
        super().__init__(
            event_bus=event_bus,
            redis_client=redis_client,
            service_config=service_config,
        )
        cfg = service_config or {}
        self.queue_key = os.environ.get("PLAITA_DELAY_QUEUE", cfg.get("delay_queue", "plaita:delay:queue"))
        self.pending_key = os.environ.get("PLAITA_DELAY_PENDING", cfg.get("delay_pending", DELAY_PENDING_KEY))
        self.sweep_interval = float(os.environ.get("PLAITA_DELAY_SWEEP_INTERVAL", cfg.get("sweep_interval", 1)))
        self._consumer_thread = None
        self._sweeper_thread = None
        self._recovery_logged = False

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
            if self._redis_client is None or not hasattr(self._redis_client, "blpop"):
                # 进程内形态：无队列可消费，任务经 ServiceManager 直接投递
                logger.info("延迟服务无 redis 客户端，以内存模式启动（无重启恢复）")
                return True
            # 注册 + 心跳：__main__ 启动器只调 start_service，不调
            # start_with_registry，不补这一步注册会在 TTL 30s 后过期，
            # 拓扑/服务列表看不到延迟服务（与 schedule_service 同款补齐）
            if self._enable_registry and not self.register_service():
                logger.warning("延迟服务注册失败，服务仍将启动")
            self._consumer_thread = threading.Thread(
                target=self._consume_queue, name="delay-service-consumer", daemon=True
            )
            self._consumer_thread.start()
            self._sweeper_thread = threading.Thread(target=self._sweep_loop, name="delay-service-sweeper", daemon=True)
            self._sweeper_thread.start()
            logger.info(
                "延迟服务已启动（队列: %s，pending: %s，扫描间隔: %ss）",
                self.queue_key,
                self.pending_key,
                self.sweep_interval,
            )
            return True
        except Exception as e:  # noqa: BLE001 — 服务循环兜底
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
        except Exception as e:  # noqa: BLE001 — 服务循环兜底
            logger.error("停止延迟服务失败: %s", e, exc_info=True)
            return False

    # ---------- 消费：queue → pending ----------

    def _consume_queue(self) -> None:
        """消费延迟任务队列（BLPOP 短超时轮询，保证关闭响应性）。

        出队即落 pending ZSET：此后进程无论怎么死，sweeper 都能把任务补触发；
        这是重启恢复的数据来源，崩溃在 BLPOP 与 ZADD 之间的窗口仍会丢任务
        （微秒级，远小于原实现的整个等待期）。
        """
        while not self.is_shutdown_requested():
            try:
                item = self._redis_client.blpop(self.queue_key, timeout=2)
            except Exception as e:  # noqa: BLE001 — 服务循环兜底
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
            self._enqueue_pending(task_config, raw)

    def _enqueue_pending(self, task_config: Dict[str, Any], raw: str) -> bool:
        """任务落 pending ZSET（score=触发时刻），返回是否入集成功。

        非法配置直接丢弃（与原 submit_task 验证失败的语义一致），不落集。
        """
        if not self.validate_task_config(task_config):
            logger.error("延迟任务配置验证失败，已丢弃: %s", task_config)
            return False
        trigger_timestamp = self._resolve_trigger_timestamp(task_config)
        try:
            # member 存 worker 投递的原始 JSON（保真），score 决定扫描时机
            self._redis_client.zadd(self.pending_key, {raw: trigger_timestamp})
        except Exception as e:  # noqa: BLE001 — 服务循环兜底
            logger.error(
                "延迟任务落 pending 失败 (execution_id=%s): %s",
                task_config.get("execution_id"),
                e,
                exc_info=True,
            )
            return False
        logger.info(
            "延迟任务已入 pending: node_id=%s, execution_id=%s, 触发时刻=%s",
            task_config.get("node_id"),
            task_config.get("execution_id"),
            trigger_timestamp,
        )
        return True

    def _resolve_trigger_timestamp(self, task_config: Dict[str, Any]) -> int:
        """取触发时刻：优先绝对时间戳；缺失时按 delay_ms 从现在起算。"""
        trigger_timestamp = task_config.get("trigger_timestamp")
        if trigger_timestamp:
            return int(trigger_timestamp)
        return int(time.time() * 1000) + int(task_config.get("delay_ms", 0))

    # ---------- sweeper：pending → 触发 ----------

    def _sweep_loop(self) -> None:
        """每秒扫描 pending ZSET，到点补触发（重启恢复即本循环的首扫）。"""
        while not self.is_shutdown_requested():
            try:
                self._sweep_due()
            except Exception as e:  # noqa: BLE001 — 服务循环兜底
                logger.error("延迟任务扫描失败: %s", e, exc_info=True)
            # shutdown_event.wait 兼顾休眠与快速响应停止
            if self._shutdown_event.wait(timeout=self.sweep_interval):
                break

    def _sweep_due(self) -> None:
        now_ms = int(time.time() * 1000)
        due = self._redis_client.zrangebyscore(self.pending_key, "-inf", now_ms)
        if due and not self._recovery_logged:
            logger.info("启动恢复：发现 %d 个待触发延迟任务", len(due))
            self._recovery_logged = True
        for raw in due:
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                task_config = json.loads(raw)
            except json.JSONDecodeError:
                # 无法解析的任务永远到不了「触发后删除」，原地清掉防僵尸
                logger.error("pending 延迟任务非法，已清除: %r", raw[:200])
                self._redis_client.zrem(self.pending_key, raw)
                continue
            self._fire_if_due(task_config, raw, now_ms)

    def _fire_if_due(self, task_config: Dict[str, Any], raw_member: str, now_ms: int) -> None:
        """触发单个到期任务：互斥锁 → 发布事件 → 出集。

        锁（NX + PX 30s）防多实例双发；发布成功才出集，发布失败保留任务，
        锁过期后由下一轮扫描重试（at-least-once）。
        """
        execution_id = task_config.get("execution_id")
        if not execution_id:
            # 无 execution_id 无法关联执行也无法加锁，属于永远发不出去的毒丸
            logger.error("pending 延迟任务缺 execution_id，已清除: %s", task_config)
            self._redis_client.zrem(self.pending_key, raw_member)
            return
        lock_key = DELAY_LOCK_KEY.format(execution_id=execution_id)
        service_info = getattr(self, "_service_info", None)
        lock_holder = service_info.instance_id if service_info else f"delay-{os.getpid()}"
        if not self._redis_client.set(lock_key, lock_holder, nx=True, px=DELAY_LOCK_TTL_MS):
            # 另一实例持有（正在触发或刚触发完），本实例跳过
            return
        release_lock = False
        try:
            # 锁内复查成员还在不在：另一实例可能已触发并出集，本实例的
            # 扫描快照是旧的——不复查会把同一任务发两次
            if self._redis_client.zscore(self.pending_key, raw_member) is None:
                release_lock = True
                return
            if not self._fire_pending_task(task_config):
                # 发布失败：任务与锁都保留——锁 TTL 即 30s 重试退避，
                # 到期后由下一轮扫描（本实例或另一实例）再试
                return
            self._redis_client.zrem(self.pending_key, raw_member)
            release_lock = True
        finally:
            if release_lock:
                # 触发完即释放锁：同一执行的下一个延迟任务不被压满锁 TTL
                try:
                    self._redis_client.delete(lock_key)
                except Exception:  # noqa: BLE001 — 服务循环兜底
                    logger.debug("延迟触发锁释放失败: %s", lock_key, exc_info=True)

    def _fire_pending_task(self, task_config: Dict[str, Any]) -> bool:
        """发布延迟到点事件（同步 redis 发布，与 trigger_event 的 redis 分支同形）。"""
        trigger_timestamp = task_config.get("trigger_timestamp")
        event_data = {
            "node_id": task_config.get("node_id"),
            "execution_id": task_config.get("execution_id"),
            "flow_id": task_config.get("flow_id"),
            "tenant_id": task_config.get("tenant_id") or "default",
            "trigger_type": "delay_completed",
            "delay_ms": task_config.get("delay_ms", 0),
            "actual_trigger_timestamp": int(time.time() * 1000),
            "planned_trigger_timestamp": trigger_timestamp,
            "success": True,
        }
        try:
            self._publish_trigger(task_config.get("event_type"), event_data)
            logger.info(
                "延迟任务已触发（pending 恢复）: node_id=%s, execution_id=%s",
                task_config.get("node_id"),
                task_config.get("execution_id"),
            )
            return True
        except Exception as e:  # noqa: BLE001 — 服务循环兜底
            logger.error(
                "延迟任务触发失败 (execution_id=%s): %s",
                task_config.get("execution_id"),
                e,
                exc_info=True,
            )
            return False

    def _publish_trigger(self, event_type: str, event_data: Dict[str, Any]) -> None:
        """经同步 redis 客户端发布到引擎 RedisEventBus 的频道（plaita:events:{type}）。

        不要走 self.event_bus.publish——它的 aioredis 连接绑定在创建时的
        event loop 上，跨 loop 使用会静默失败。
        """
        from ...event.core import Event

        event = Event(
            event_type=event_type,
            data=event_data,
            correlation_id=event_data.get("execution_id"),
        )
        self._redis_client.publish(f"plaita:events:{event_type}", event.model_dump_json())

    async def trigger_event(self, event_type: str, event_data: Dict[str, Any]):
        """触发事件：带 correlation_id（=execution_id），EventFilter 才能关联到挂起执行。

        有 Redis 客户端时，用同步 redis 客户端发布（见 _publish_trigger）；
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
                self._publish_trigger(event_type, event_data)
            logger.info("事件已触发: %s (correlation_id=%s)", event_type, event.correlation_id)
        except Exception as e:  # noqa: BLE001 — 服务循环兜底
            logger.error("触发事件失败: %s", e, exc_info=True)

    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """
        处理延迟任务（内存模式：进程内 ServiceManager 投递后分段 sleep 到点触发）

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
                "tenant_id": task_config.get("tenant_id") or "default",
                "trigger_type": "delay_completed",
                "delay_ms": delay_ms,
                "actual_trigger_timestamp": int(time.time() * 1000),
                "planned_trigger_timestamp": trigger_timestamp,
                "success": True,
            }

            # 触发事件
            await self.trigger_event(event_type, event_data)

            logger.info("延迟任务完成: node_id=%s", node_id)
            return True

        except Exception as e:  # noqa: BLE001 — 服务循环兜底
            logger.error("处理延迟任务失败: %s", e, exc_info=True)

            # 触发错误事件
            try:
                error_event_data = {
                    "node_id": task_config.get("node_id"),
                    "execution_id": task_config.get("execution_id"),
                    "flow_id": task_config.get("flow_id"),
                    "tenant_id": task_config.get("tenant_id") or "default",
                    "trigger_type": "delay_error",
                    "error_message": str(e),
                    "success": False,
                }
                await self.trigger_event(task_config.get("event_type"), error_event_data)
            except Exception:  # noqa: BLE001 — 服务循环兜底
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
        pending_count = 0
        if self._redis_client is not None and hasattr(self._redis_client, "zcard"):
            try:
                pending_count = int(self._redis_client.zcard(self.pending_key))
            except Exception:  # noqa: BLE001 — 服务循环兜底
                logger.warning("读取 pending 延迟任务数失败", exc_info=True)
        return {
            "service_type": self.get_service_type(),
            "active_task_count": self.get_active_task_count(),
            "pending_task_count": pending_count,
            "is_running": self.is_running,
            "max_workers": self.get_max_workers(),
        }
