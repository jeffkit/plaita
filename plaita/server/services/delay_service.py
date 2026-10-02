"""
延迟服务实现
负责处理延迟任务，在指定时间后触发事件

C4-2 改造（2026-10）：历史实现 BLPOP 出队即提交线程池、handle_task 内睡眠
等待触发点——存在三个问题：
1. 出队后任务只存在于内存，进程崩溃则任务永久丢失（延迟执行永久挂起）；
2. 每个未到期任务占一个线程池坑位睡眠，10 个长延迟即可占满默认池；
3. shutdown 时睡眠中的任务被丢弃，不回队。

现改为「list 传输 + ZSET 排程」两段式：
- 生产者（flow_worker._dispatch_service_task）仍 RPUSH JSON 到 list 键
  （``plaita:delay:queue``，格式不变，生产者侧零改动）；
- 消费线程每周期把 list 条目**搬运**进内部 ZSET ``{queue}:scheduled``
  （member=原始 JSON，score=触发时间戳 ms）。先 ZADD 成功再 LREM：
  崩溃在两步之间只会导致下轮重复搬运（ZADD 按 member 幂等），不丢任务；
- 到期轮询：ZSET 中 score<=now 的任务才提交线程池，处理走完才 ZREM
  （at-least-once：崩溃后任务仍在 ZSET，重启后被下一周期捡起；
  shutdown 中断的任务同样留在 ZSET 等待下次启动恢复）。
"""
import asyncio
import json
import threading
import time
from typing import Any, Dict

from .base_service import BaseExtendedService
from ...logger import logger

# 到期轮询/搬运周期（秒）。service_config.poll_interval 可覆盖。
DELAY_POLL_INTERVAL_SECONDS = 0.5
# 每周期搬运/提交的最大条数（防单周期阻塞过久）。
DELAY_CONVEY_BATCH = 200
DELAY_DUE_BATCH = 50


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
        # C4-2：排程 ZSET（member=任务 JSON，score=触发时间戳 ms）。
        # list 键仍是生产者契约（flow_worker RPUSH），ZSET 是本服务内部的
        # 崩溃可恢复排程态。
        self.scheduled_key = f"{self.queue_key}:scheduled"
        self._poll_interval = float(
            (service_config or {}).get("poll_interval", DELAY_POLL_INTERVAL_SECONDS)
        )
        self._consumer_thread = None
        # in-flight 去重（进程内）：同一 member 在处理期间不重复提交。
        self._inflight: set = set()
        self._inflight_lock = threading.Lock()

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
            # C4-2 后需要 ZSET 原语做排程；list 键由搬运逻辑持续迁入 ZSET。
            if (
                self._redis_client is None
                or not hasattr(self._redis_client, "zadd")
                or not hasattr(self._redis_client, "lrange")
            ):
                logger.warning("延迟服务无 redis 客户端，队列消费不启动")
                return True
            self._consumer_thread = threading.Thread(
                target=self._consume_queue, name="delay-service-consumer", daemon=True
            )
            self._consumer_thread.start()
            logger.info(
                "延迟服务已启动（队列: %s，排程: %s）", self.queue_key, self.scheduled_key
            )
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
        """消费循环：list→ZSET 搬运 + 到期任务提交（无阻塞原语，周期轮询）。

        C4-2：替代历史 BLPOP——出队即内存持有、崩溃即丢。现任务先落 ZSET
        再出 list，且只有到期任务才进线程池。
        """
        while not self.is_shutdown_requested():
            try:
                self._convey_list_to_zset()
                self._submit_due_tasks()
            except Exception as e:
                logger.error("延迟队列轮询失败: %s", e, exc_info=True)
            self._shutdown_event.wait(timeout=self._poll_interval)

    # ---- list → ZSET 搬运 ----

    def _due_score_ms(self, task_config: Dict[str, Any]) -> int:
        """计算触发时间戳（ms）：trigger_timestamp 优先，其次 now+delay_ms。"""
        trigger_ts = task_config.get("trigger_timestamp")
        if trigger_ts:
            return int(trigger_ts)
        delay_ms = task_config.get("delay_ms") or 0
        return int(time.time() * 1000 + float(delay_ms))

    def _convey_list_to_zset(self) -> None:
        """把 list 队列条目搬进排程 ZSET（先 ZADD 后 LREM，崩溃安全）。

        启动时遗留的旧 list 任务由此在同周期迁入——即「一次性迁移」的
        持续化版本（生产者持续 RPUSH list，单次启动迁移不够）。
        """
        raw_items = self._redis_client.lrange(self.queue_key, 0, DELAY_CONVEY_BATCH - 1)
        if not raw_items:
            return
        valid = []
        for raw in raw_items:
            if isinstance(raw, bytes):
                raw = raw.decode()
            try:
                task_config = json.loads(raw)
            except json.JSONDecodeError:
                logger.error("延迟任务配置非法（丢弃）: %r", raw[:200])
                self._lrem_raw(raw)
                continue
            if not isinstance(task_config, dict) or not self.validate_task_config(task_config):
                logger.error("延迟任务配置校验失败（丢弃）: %r", raw[:200])
                self._lrem_raw(raw)
                continue
            valid.append((raw, self._due_score_ms(task_config)))
        if not valid:
            return
        pipe = self._redis_client.pipeline()
        for raw, score in valid:
            pipe.zadd(self.scheduled_key, {raw: score})
        pipe.execute()
        # ZADD 成功后才出 list：中途崩溃 → 下轮重复搬运，ZADD 按 member 幂等。
        pipe = self._redis_client.pipeline()
        for raw, _score in valid:
            pipe.lrem(self.queue_key, 1, raw)
        pipe.execute()
        logger.debug("延迟任务搬运 %d 条 → %s", len(valid), self.scheduled_key)

    def _lrem_raw(self, raw: str) -> None:
        try:
            self._redis_client.lrem(self.queue_key, 1, raw)
        except Exception as e:
            logger.error("延迟队列坏条目移除失败: %s", e)

    # ---- 到期提交 ----

    def _submit_due_tasks(self) -> None:
        """提交已到期且不在处理中的任务（未到期不动，不占线程池坑位）。"""
        now_ms = int(time.time() * 1000)
        due = (
            self._redis_client.zrangebyscore(
                self.scheduled_key, "-inf", now_ms, start=0, num=DELAY_DUE_BATCH
            )
            or []
        )
        for member in due:
            if isinstance(member, bytes):
                member = member.decode()
            with self._inflight_lock:
                if member in self._inflight:
                    continue
                self._inflight.add(member)
            self._start_scheduled_task(member)

    def _start_scheduled_task(self, member: str) -> None:
        try:
            task_config = json.loads(member)
        except json.JSONDecodeError:
            logger.error("排程任务反序列化失败（移除）: %r", member[:200])
            self._finalize_scheduled(member)
            return
        if not isinstance(task_config, dict):
            logger.error("排程任务非法（移除）: %r", member[:200])
            self._finalize_scheduled(member)
            return
        self.thread_pool.submit(self._run_scheduled_task, member, task_config)
        logger.info("到期延迟任务已提交: node_id=%s", task_config.get("node_id"))

    def _run_scheduled_task(self, member: str, task_config: Dict[str, Any]) -> None:
        """线程池内执行到期任务；处理走完才 ZREM（at-least-once）。"""
        task_id = self._generate_task_id(task_config)
        self.active_tasks.add(task_id)
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self.handle_task(task_config))
            finally:
                try:
                    loop.close()
                except Exception:
                    logger.warning("event loop close failed during task cleanup", exc_info=True)
            logger.info("延迟任务 %s 执行完成: %s", task_id, result)
        except Exception as e:
            logger.error("延迟任务 %s 执行失败: %s", task_id, e, exc_info=True)
            self._handle_task_error(task_config, e)
        finally:
            self.active_tasks.discard(task_id)
            if self.is_shutdown_requested():
                # shutdown 中断的任务留在 ZSET，下次启动由到期轮询捡起。
                with self._inflight_lock:
                    self._inflight.discard(member)
            else:
                self._finalize_scheduled(member)

    def _finalize_scheduled(self, member: str) -> None:
        """任务处理走完：ZREM 出排程态并清 in-flight。

        ZREM 失败只导致下次重复触发一次（事件侧 EventFilter 去重幂等）。
        """
        try:
            self._redis_client.zrem(self.scheduled_key, member)
        except Exception as e:
            logger.error("延迟任务完成登记失败: %s", e)
        with self._inflight_lock:
            self._inflight.discard(member)

    async def trigger_event(self, event_type: str, event_data: Dict[str, Any]):
        """触发事件：带 correlation_id（=execution_id），EventFilter 才能关联到挂起执行。

        实现收敛到基类 publish_resume_event（与本文件历史 override 同手法：
        有 redis 直发 plaita:events:{type} 频道绕开 aioredis 跨 loop 问题，
        无 redis 回退 event_bus.publish）。Track P2 起直发前还按
        RedisEventStorage 键格式尽力落盘——直发不经 RedisEventBus.publish
        不落存储，EventReconciler 回扫补偿不到；delay 的 resume 与审批/回调
        同待遇。
        """
        await self.publish_resume_event(event_type, event_data)

    
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
                "tenant_id": task_config.get("tenant_id") or "default",
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
                    "tenant_id": task_config.get("tenant_id") or "default",
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
        scheduled_count = 0
        try:
            if self._redis_client is not None and hasattr(self._redis_client, "zcard"):
                scheduled_count = int(self._redis_client.zcard(self.scheduled_key) or 0)
        except Exception as e:
            logger.debug("zcard(%s) failed: %s", self.scheduled_key, e)
        return {
            "service_type": self.get_service_type(),
            "active_task_count": self.get_active_task_count(),
            "scheduled_task_count": scheduled_count,
            "queue_key": self.queue_key,
            "scheduled_key": self.scheduled_key,
            "is_running": self.is_running,
            "max_workers": self.get_max_workers()
        } 