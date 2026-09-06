from datetime import datetime
import json
import logging
import os
import signal
from typing import Dict, Any, Optional

import argparse
import importlib
import sys

from cachetools import TTLCache
from redis import Redis
from plaita.event.core import EventBus
from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution, ExecutionMode
from plaita.storage.base import ExecutionState, ExecutionStorage, FlowStorage
from plaita.logger import logger
from plaita.server.registry import RegistryMixin, ServiceRegistry, ServiceInfo
from plaita.server.control import ControlMixin, ControlListener
from plaita.server.log_handler import setup_redis_logging
from plaita.server.task_queue import (
    DEFAULT_CLAIM_MIN_IDLE_MS,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_MAX_DELIVERIES,
    RedisStreamTaskQueue,
)
from plaita.server.execution_lease import (
    DEFAULT_LEASE_TTL_SECONDS,
    ExecutionLease,
    ExecutionLeaseError,
    NullExecutionLease,
    RedisExecutionLease,
    new_holder_token,
)

class FlowWorker:
    """
    流程工作器：把 ``run_distributed`` 与 ExecutionStorage / FlowStorage / EventBus 串起来。

    这是 **suspend/resume 编排器**，不是具备至少一次投递或崩溃安全的工作流引擎。
    可靠性边界见 docs-site ``distributed/flow-worker.md`` 与类常量
    ``PERSIST_EVERY_N_STEPS`` / ``RedisFlowWorker.run`` 的队列语义说明。
    """

    # 连续推进时每隔 N 步落一次中间态。挂起 / 结束 / 出错始终立即持久化。
    # 默认 1 = 每步落盘，崩溃不丢步进进度（Wave 3）。
    PERSIST_EVERY_N_STEPS = 1

    def __init__(
        self,
        execution_storage: ExecutionStorage,
        flow_storage: FlowStorage,
        event_bus: EventBus = None,
        cache_size: int = 100,
        cache_ttl: int = 300,
        callback_handlers: Optional[list] = None,
        execution_lease: Optional[ExecutionLease] = None,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
    ):
        """
        初始化流程工作器

        Args:
            execution_storage: 执行状态存储实例
            flow_storage: 流程定义存储实例
            event_bus: 事件总线实例
            cache_size: 缓存大小，默认100
            cache_ttl: 缓存过期时间(秒)，默认300秒
            callback_handlers: 分布式执行期间贯穿所有步骤的回调列表
            execution_lease: resume 租约（默认 NullExecutionLease）；RedisFlowWorker 注入 Redis 实现
            lease_ttl_seconds: resume 租约 TTL（秒），推进过程中会 renew
        """
        self.execution_storage = execution_storage
        self.flow_storage = flow_storage
        self.event_bus = event_bus
        self.callback_handlers = list(callback_handlers) if callback_handlers else []
        # 初始化流程定义缓存，使用TTL缓存
        self.flow_definition_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        self.execution_lease = execution_lease or NullExecutionLease()
        self.lease_ttl_seconds = lease_ttl_seconds
    
    def get_flow_definition(self, flow_id: str, version: Optional[str] = None) -> Flow:
        """
        获取流程定义，支持按ID和版本获取
        
        Args:
            flow_id: 流程ID
            version: 流程版本，如果不指定则获取最新版本
            
        Returns:
            Dict[str, Any]: 流程定义
            
        Raises:
            ValueError: 如果找不到流程定义或版本不匹配
        """
        # 生成缓存键
        cache_key = f"{flow_id}:{version or 'latest'}"
        
        # 尝试从缓存获取
        if cache_key in self.flow_definition_cache:
            logger.info("从缓存获取流程定义: %s", cache_key)
            return self.flow_definition_cache[cache_key]
        
        # 缓存未命中，从存储获取
        logger.info("从存储获取流程定义: %s, 版本: %s", flow_id, version or 'latest')
        flow_definition = self.flow_storage.get_flow(flow_id, version)
        
        if not flow_definition:
            # Delegate diagnostic details to the storage layer via an optional
            # diagnose() method — FlowWorker must not peek at storage internals
            # (e.g. Redis keys / namespaces) directly.
            if hasattr(self.flow_storage, "diagnose_missing_flow"):
                try:
                    self.flow_storage.diagnose_missing_flow(flow_id)
                except Exception:
                    logger.warning("flow_storage.diagnose_missing_flow(%s) failed", flow_id, exc_info=True)

            error_msg = f"找不到流程定义: {flow_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 解析流程定义
        try:
            logger.info("成功获取流程定义: %s, 数据: %s", flow_id, flow_definition.get('name', 'unknown'))
            flow = Flow.model_validate(flow_definition)
        except Exception as e:
            error_msg = f"解析流程定义失败: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.flow_definition_cache[cache_key] = flow
        
        return flow
    
    def start_flow(self, flow_id: str, params: Dict[str, Any], version: Optional[str] = None) -> Dict[str, Any]:
        """
        启动流程执行
        
        Args:
            flow_id: 流程ID
            params: 流程输入参数
            version: 流程版本，如果不指定则使用最新版本
            
        Returns:
            Dict[str, Any]: 流程执行结果
        """
        # 获取流程定义
        flow = self.get_flow_definition(flow_id, version)
        
        
        
        logger.info("开始执行流程: %s, 版本: %s", flow_id, version or '最新版本')
        
        try:
            # 创建流程执行器并执行流程
            # 复用同一个 FlowExecution 贯穿所有分布式步骤, 保留用户回调
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED

            # 执行流程，获取初始结果
            result = execution.run_distributed(flow, params=params)
            execution_id = result.get("execution_id")

            # 创建执行状态对象
            state = ExecutionState(
                execution_id=execution_id,
                flow_id=flow_id,
                flow_version=version,
                context=result.get("context"),
                status="running",
                start_time=datetime.now().isoformat(),
                invoker="worker"
            )

            # 保存执行状态
            success = self.execution_storage.save_execution_state(execution_id, state)
            if not success:
                logger.error("保存执行状态失败: %s", execution_id)
                raise RuntimeError(f"保存执行状态失败: {execution_id}")

            # 处理执行结果
            final_result = self._process_execution_result(flow, result, state, execution)

            return final_result

        except Exception as e:
            logger.error("执行流程出错: %s", e, exc_info=True)
            raise RuntimeError(f"执行流程出错: {e}")
    
    def resume_flow(self, flow_id: str, execution_id: str, resume_type: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        恢复流程执行。

        取得 ``execution_id`` 租约后才推进；另一 worker 已持有租约时抛
        ``ExecutionLeaseError``（RedisFlowWorker 对此**不** XACK，待租约过期后回收）。
        """
        # 加载执行状态
        state = self.execution_storage.load_execution_state(execution_id)
        if not state:
            error_msg = f"找不到执行状态: {execution_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 验证流程ID是否匹配
        stored_flow_id = state.flow_id
        if stored_flow_id and stored_flow_id != flow_id:
            error_msg = f"流程ID不匹配: 期望 {flow_id}, 实际 {stored_flow_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 终态短路（2026-09 分布式评审 P1-1）：at-least-once 下重复投递的
        # resume 任务会命中已完成的执行。历史实现走完正常 resume 再在异常
        # 处理里把终态改写成 error——监控按 status 查询会得出错误结论。
        # 幂等语义：已终态的执行直接原样返回，不再推进。
        state_status = getattr(state, "status", "") or ""
        if state_status in ("completed", "error"):
            logger.info(
                "执行 %s 已是终态 (%s)，跳过重复 resume", execution_id, state_status,
            )
            return {
                "execution_id": execution_id,
                "status": state_status,
                "already_terminal": True,
                "result": getattr(state, "result", None),
                "error": getattr(state, "error", None),
            }
        
        # 获取流程版本
        version = state.flow_version
        
        # 获取流程定义
        flow = self.get_flow_definition(flow_id, version)
        
        # 解析流程定义
        logger.info("恢复流程执行: %s, 执行ID: %s, 恢复类型: %s", flow_id, execution_id, resume_type)

        holder = new_holder_token(prefix="resume")
        if not self.execution_lease.try_acquire(execution_id, holder, self.lease_ttl_seconds):
            raise ExecutionLeaseError(
                f"execution {execution_id} is leased by another worker; refuse concurrent resume"
            )

        try:
            # 复用同一个 FlowExecution 贯穿恢复后的所有分布式步骤
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED

            # 直接使用 run_distributed 恢复执行
            result = execution.run_distributed(
                flow,
                saved_context=state.context,
                resume_type=resume_type,
                resume_data=data,
            )

            # 处理执行结果
            final_result = self._process_execution_result(
                flow,
                result,
                state,
                execution,
                lease_execution_id=execution_id,
                lease_holder=holder,
            )

            return final_result

        except ExecutionLeaseError:
            raise
        except Exception as e:
            logger.error("恢复流程执行出错: %s", e, exc_info=True)

            # 更新执行状态为错误
            state.status = "error"
            state.error = {"message": str(e)}
            state.end_time = datetime.now().isoformat()

            self.execution_storage.save_execution_state(execution_id, state)

            raise RuntimeError(f"恢复流程执行出错: {e}")
        finally:
            self.execution_lease.release(execution_id, holder)

    def _renew_lease_if_held(self, lease_execution_id: Optional[str], lease_holder: Optional[str]) -> None:
        if not lease_execution_id or not lease_holder:
            return
        if not self.execution_lease.renew(lease_execution_id, lease_holder, self.lease_ttl_seconds):
            raise ExecutionLeaseError(
                f"lost lease for execution {lease_execution_id}; aborting resume"
            )

    def _process_execution_result(
        self,
        flow: Flow,
        result: Dict[str, Any],
        state: ExecutionState,
        execution: Optional[FlowExecution] = None,
        lease_execution_id: Optional[str] = None,
        lease_holder: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        处理流程执行结果

        Args:
            flow: 流程对象
            result: 执行结果
            state: 执行状态
            execution: 贯穿本执行全过程的 FlowExecution (复用以保留回调);
                为 None 时按需创建, 仅供向后兼容的简单调用场景使用
            lease_execution_id / lease_holder: resume 租约续期参数

        Returns:
            Dict[str, Any]: 最终的执行结果
        """

        # 初始化状态
        execution_id = result.get("execution_id")
        context = result.get("context")

        if execution is None:
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED

        steps_since_persist = 0

        while True:
            is_end = result.get("is_end", False)
            is_suspend = result.get("is_suspend", False)

            if is_end:
                state.status = "completed"
                state.context = context
                state.end_time = datetime.now().isoformat()
                self.execution_storage.save_execution_state(execution_id, state)
                break
            elif is_suspend:
                state.status = "suspended"
                state.context = context
                self.execution_storage.save_execution_state(execution_id, state)
                self._dispatch_service_task(result, context, execution_id)
                break
            else:
                state.status = "running"
                try:
                    self._renew_lease_if_held(lease_execution_id, lease_holder)
                    result = execution.run_distributed(
                        flow,
                        saved_context=context,
                        resume_type="continue",
                    )

                    context = result.get("context", context)
                    steps_since_persist += 1

                    is_end = result.get("is_end", False)
                    is_suspend = result.get("is_suspend", False)

                    if (
                        not is_end
                        and not is_suspend
                        and steps_since_persist >= self.PERSIST_EVERY_N_STEPS
                    ):
                        state.context = context
                        state.last_update_time = datetime.now().isoformat()
                        self.execution_storage.save_execution_state(execution_id, state)
                        steps_since_persist = 0
                        logger.info("流程步骤执行完成，继续下一步: %s", execution_id)

                except ExecutionLeaseError:
                    raise
                except Exception as e:
                    logger.error("流程执行出错: %s", e, exc_info=True)
                    state.status = "error"
                    state.context = context
                    state.error = {"message": str(e)}
                    state.end_time = datetime.now().isoformat()
                    self.execution_storage.save_execution_state(execution_id, state)
                    break

        return result

    def _dispatch_service_task(
        self, result: Dict[str, Any], context: Dict[str, Any], execution_id: str
    ) -> None:
        """挂起时把扩展节点的 service_config 投递给对应外延服务。

        历史上挂起只落执行状态，service_config 无人消费——
        delay/approval 等节点的任务永远不会被服务接走，执行会永久挂起。
        service_config 通常埋在 ``context.$NODE.{最后节点}.service_config``，
        顶层偶有直接携带；任务按 subtype 投递到 ``plaita:{subtype}:queue``，
        由对应外延服务消费（DelayService 等）。
        无 redis 客户端（单测/纯内存派生类）时跳过投递。
        """
        service_config = result.get("service_config")
        if not isinstance(service_config, dict) or not service_config:
            nodes = (context or {}).get("$NODE") or {}
            last_node = (context or {}).get("$LAST_NODE")
            node_result = nodes.get(last_node) if last_node else None
            if isinstance(node_result, dict):
                service_config = node_result.get("service_config")
        if not isinstance(service_config, dict) or not service_config:
            return
        subtype = str(service_config.get("type") or result.get("node_subtype") or "").strip()
        if not subtype:
            return
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "rpush"):
            logger.warning(
                "挂起任务投递跳过（无 redis 客户端）: %s (execution_id=%s)",
                subtype, execution_id,
            )
            return
        queue_key = f"plaita:{subtype}:queue"
        try:
            task = dict(service_config)
            task.setdefault("execution_id", execution_id)
            redis_client.rpush(queue_key, json.dumps(task, ensure_ascii=False))
            logger.info(
                "挂起任务已投递: %s → %s (execution_id=%s)", subtype, queue_key, execution_id
            )
        except Exception as e:
            logger.error(
                "挂起任务投递失败: %s → %s: %s", subtype, queue_key, e, exc_info=True
            )

class RedisFlowWorker(RegistryMixin, ControlMixin, FlowWorker):
    """
    基于 Redis Stream 队列的流程工作器（服务注册 / 心跳 / 远程控制）。

    任务队列语义为 **at-least-once**：consumer group + ``XACK``；处理成功前
    崩溃则 pending 可被其他 consumer 在 ``claim_min_idle_ms`` 后回收重投。
    resume 通过 Redis execution lease 保证同一 ``execution_id`` 最多一个 worker 推进。
    控制面硬依赖 Redis。
    """

    SERVICE_TYPE = "flow_worker"
    
    def __init__(
        self,
        redis_url: str,
        queue_name: str,
        execution_storage: ExecutionStorage,
        flow_storage: FlowStorage,
        event_bus: EventBus = None,
        cache_size: int = 100,
        cache_ttl: int = 300,
        enable_registry: bool = True,
        registry_ttl: int = ServiceRegistry.DEFAULT_TTL,
        heartbeat_interval: int = ServiceRegistry.DEFAULT_HEARTBEAT_INTERVAL,
        enable_redis_logging: bool = True,
        redis_client: Redis = None,
        callback_handlers=None,
        consumer_group: str = DEFAULT_CONSUMER_GROUP,
        consumer_name: Optional[str] = None,
        claim_min_idle_ms: int = DEFAULT_CLAIM_MIN_IDLE_MS,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
        execution_lease: Optional[ExecutionLease] = None,
        max_deliveries: int = DEFAULT_MAX_DELIVERIES,
        dlq_key: Optional[str] = None,
        read_block_ms: int = 1_000,
    ):
        redis_client = redis_client or Redis.from_url(redis_url)
        super().__init__(
            execution_storage,
            flow_storage,
            event_bus,
            cache_size,
            cache_ttl,
            callback_handlers=callback_handlers,
            execution_lease=execution_lease or RedisExecutionLease(redis_client),
            lease_ttl_seconds=lease_ttl_seconds,
        )
        self.redis_url = redis_url
        self.redis_client = redis_client
        self.queue_name = queue_name
        self._running = False
        # 消费阻塞窗口上限（毫秒）——见 run() 主循环内的分片说明
        self.read_block_ms = max(100, int(read_block_ms))
        self._active_task_count = 0
        self._log_handler = None
        self._consumer_group = consumer_group
        self._claim_min_idle_ms = claim_min_idle_ms
        self._consumer_name = consumer_name
        self._max_deliveries = max_deliveries
        self._dlq_key = dlq_key
        self._task_queue: Optional[RedisStreamTaskQueue] = None
        
        # 服务注册
        self._enable_registry = enable_registry
        if self._enable_registry:
            self.init_registry(
                redis_client=self.redis_client,
                service_type=self.SERVICE_TYPE,
                metadata={
                    "queue_name": queue_name,
                    "redis_url": redis_url,
                    "cache_size": cache_size,
                    "cache_ttl": cache_ttl
                },
                ttl=registry_ttl,
                heartbeat_interval=heartbeat_interval
            )
            
            # 初始化控制监听
            self.init_control(
                redis_client=self.redis_client,
                instance_id=self._service_info.instance_id if self._service_info else "unknown"
            )
        
        # Redis 日志处理器
        self._enable_redis_logging = enable_redis_logging
        if self._enable_redis_logging and self._service_info:
            self._log_handler = setup_redis_logging(
                redis_client=self.redis_client,
                service_type=self.SERVICE_TYPE,
                instance_id=self._service_info.instance_id,
                logger_name="plaita",
                level=logging.INFO
            )

    def _resolve_consumer_name(self) -> str:
        if self._consumer_name:
            return self._consumer_name
        if self._enable_registry and getattr(self, "_service_info", None):
            return self._service_info.instance_id
        return f"worker-{os.getpid()}"

    def _get_task_queue(self) -> RedisStreamTaskQueue:
        if self._task_queue is None:
            self._task_queue = RedisStreamTaskQueue(
                self.redis_client,
                self.queue_name,
                group_name=self._consumer_group,
                consumer_name=self._resolve_consumer_name(),
                claim_min_idle_ms=self._claim_min_idle_ms,
                max_deliveries=self._max_deliveries,
                dlq_key=self._dlq_key,
            )
        return self._task_queue

    def _dispatch_task(self, message_data: Dict[str, Any]) -> None:
        message_type = message_data.get("type")
        if message_type == "start":
            self.start_flow(
                message_data.get("flow_id"),
                message_data.get("params"),
                message_data.get("version"),
            )
        elif message_type == "resume":
            self.resume_flow(
                message_data.get("flow_id"),
                message_data.get("execution_id"),
                message_data.get("resume_type"),
                message_data.get("data"),
            )
        else:
            raise ValueError(f"unknown task type: {message_type!r}")

    def run(self):
        """
        从 Redis Stream consumer group 拉取 ``start`` / ``resume`` 任务。

        成功处理后 ``XACK``；崩溃或未 ack 的消息留在 pending，超时后可被
        其他 consumer ``XCLAIM`` 回收（at-least-once，业务侧应幂等）。
        """
        self._running = True
        queue = self._get_task_queue()
        queue.ensure_group()
        
        # 注册服务
        if self._enable_registry:
            self.register_service()
            # 启动控制监听
            self.start_control_listener()
        
        logger.info(
            "流程工作器已启动，监听 stream: %s (group=%s, consumer=%s)",
            self.queue_name,
            self._consumer_group,
            queue.consumer_name,
        )
        
        try:
            while self._running:
                # 分片阻塞读取（2026-09 分布式评审 P2-2）：XREADGROUP 的
                # BLOCK 无法被信号中断出循环，整块 10s 会让 SIGTERM 后的
                # worker 继续抢任务最长 10s。切成 ≤1s 的窗口，停机延迟
                # 上限 ≈1s，空轮询的 Redis 往返开销可忽略。
                task = queue.read(block_ms=min(self.read_block_ms, 1_000))
                if not task:
                    continue

                self._active_task_count += 1
                if self._enable_registry:
                    self.update_registry_info(active_tasks=self._active_task_count)

                acked = False
                try:
                    self._dispatch_task(task.body)
                    queue.ack(task.message_id)
                    acked = True
                except ExecutionLeaseError as exc:
                    # 另一 worker 持有 resume 租约：不 ack，待租约过期后 reclaim
                    queue.note_lease_conflict()
                    logger.warning(
                        "任务 %s 未取得 execution lease，留在 pending: %s",
                        task.message_id,
                        exc,
                    )
                except ValueError as exc:
                    # 畸形消息：ack 掉避免 poison pill 无限重投
                    logger.error("丢弃无效任务 %s: %s", task.message_id, exc)
                    queue.ack(task.message_id)
                    queue.note_poison()
                    acked = True
                except Exception as exc:
                    queue.note_failed()
                    if task.delivery_count >= queue.max_deliveries:
                        queue.dead_letter(
                            task,
                            reason=f"processing_failed:{type(exc).__name__}:{exc}"[:500],
                        )
                        acked = True
                    else:
                        logger.error(
                            "任务处理失败 %s (delivery=%s/%s)，未 ack（将重投）: %s",
                            task.message_id,
                            task.delivery_count,
                            queue.max_deliveries,
                            exc,
                            exc_info=True,
                        )
                finally:
                    self._active_task_count -= 1
                    if self._enable_registry:
                        self.update_registry_info(active_tasks=self._active_task_count)
                    if not acked:
                        logger.debug("任务 %s 留在 pending 等待回收", task.message_id)
        finally:
            self.stop()
    
    def stop(self):
        """停止流程工作器"""
        logger.info("正在停止流程工作器...")
        self._running = False
        
        # 停止控制监听
        if self._enable_registry:
            self.stop_control_listener()
        
        # 注销服务
        if self._enable_registry:
            self.unregister_service()
        
        # 关闭日志处理器
        if self._log_handler:
            self._log_handler.close()
        
        logger.info("流程工作器已停止")
    
    def _on_stop_command(self, graceful: bool):
        """响应停止命令"""
        logger.info("收到远程停止命令，优雅停止: %s", graceful)
        self.stop()
    
    def _on_status_command(self) -> Dict[str, Any]:
        """响应状态查询命令"""
        status = {
            "status": "running" if self._running else "stopped",
            "active_tasks": self._active_task_count,
            "queue_name": self.queue_name,
            "consumer_group": self._consumer_group,
            "consumer_name": self._resolve_consumer_name(),
        }
        try:
            status["queue"] = self._get_task_queue().stats()
        except Exception as exc:
            status["queue_error"] = str(exc)
        return status

# 新增命令行入口


from plaita.server.factory import create_storage_component, create_event_bus  # noqa: F401


def main():
    """命令行入口程序"""
    # CLI 默认给控制台日志：库内 logger 无 handler，原样跑运维看到的是
    # 零输出（2026-09 分布式评审 P2-6）。--quiet / PLAITA_LOG_LEVEL 可调。
    logging.basicConfig(
        level=os.environ.get("PLAITA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Plaita流程工作器")
    
    # Redis参数（支持环境变量 PLAITA_REDIS_URL / REDIS_URL）
    parser.add_argument("--redis-url",
                      default=os.environ.get("PLAITA_REDIS_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/0")),
                      help="Redis连接URL")
    parser.add_argument("--queue-name",
                      default=os.environ.get("PLAITA_QUEUE_NAME", os.environ.get("QUEUE_NAME", "plaita:flow:queue")),
                      help="Redis Stream 键名（任务队列，需 Redis 5+）")
    parser.add_argument("--consumer-group",
                      default=os.environ.get("PLAITA_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
                      help="Stream consumer group 名称")
    parser.add_argument("--consumer-name",
                      default=os.environ.get("PLAITA_CONSUMER_NAME"),
                      help="本 worker 的 consumer 名称（默认 instance_id 或 worker-pid）")
    parser.add_argument("--claim-min-idle-ms", type=int,
                      default=int(os.environ.get("PLAITA_CLAIM_MIN_IDLE_MS", str(DEFAULT_CLAIM_MIN_IDLE_MS))),
                      help="pending 消息最短空闲毫秒数后才可被其他 consumer 回收")
    parser.add_argument("--lease-ttl-seconds", type=int,
                      default=int(os.environ.get("PLAITA_LEASE_TTL_SECONDS", str(DEFAULT_LEASE_TTL_SECONDS))),
                      help="resume execution lease TTL（秒），推进中会 renew")
    parser.add_argument("--max-deliveries", type=int,
                      default=int(os.environ.get("PLAITA_MAX_DELIVERIES", str(DEFAULT_MAX_DELIVERIES))),
                      help="任务最大投递次数，超过后写入 DLQ 并 ack")
    parser.add_argument("--dlq-key",
                      default=os.environ.get("PLAITA_DLQ_KEY"),
                      help="死信 Stream 键（默认 <queue-name>:dlq）")
    
    # 数据库参数
    parser.add_argument("--database-url", default="sqlite:///flow.db",
                      help="数据库连接URL")
    
    # 存储组件类型（execution/flow 仅 memory|redis；db 与同步 ABC 不兼容，见 factory）
    parser.add_argument("--execution-storage-type", choices=["memory", "redis"], default="redis",
                      help="执行状态存储类型（memory|redis；db 已下架）")
    parser.add_argument("--flow-storage-type", choices=["memory", "redis"], default="redis",
                      help="流程定义存储类型（memory|redis；db 已下架）")
    
    # 事件总线参数（默认启用：分布式挂起/恢复依赖订阅写入；
    # 历史 --use-event-bus 为 opt-in，导致默认部署订阅落内存、EventFilter 读 Redis）
    parser.add_argument("--event-bus-type", choices=["memory", "redis"], default="redis",
                      help="事件总线类型（生产用 redis；db/sqlalchemy 已标 experimental，见 factory）")
    parser.add_argument("--use-event-bus", action="store_true",
                      help=argparse.SUPPRESS)  # 已默认启用，保留兼容旧脚本
    parser.add_argument("--no-event-bus", action="store_true",
                      help="禁用事件总线（仅无挂起节点的纯同步场景）")
    
    # 缓存参数
    parser.add_argument("--cache-size", type=int, default=100,
                      help="缓存大小")
    parser.add_argument("--cache-ttl", type=int, default=300,
                      help="缓存TTL(秒)")
    
    # 服务注册参数
    parser.add_argument("--enable-registry", action="store_true", default=True,
                      help="启用服务注册")
    parser.add_argument("--no-registry", action="store_true",
                      help="禁用服务注册")
    parser.add_argument("--registry-ttl", type=int, default=30,
                      help="服务注册TTL(秒)")
    parser.add_argument("--read-block-ms", type=int, default=1_000,
                        help="XREADGROUP 阻塞窗口上限（毫秒）。默认 1000：让 SIGTERM "
                             "后的停机延迟 ≤1s；调大可略降 Redis 往返次数")
    parser.add_argument("--quiet", action="store_true",
                        help="关闭 INFO 级控制台日志（等价 PLAITA_LOG_LEVEL=WARNING）")
    parser.add_argument("--heartbeat-interval", type=int, default=10,
                      help="心跳间隔(秒)")

    args = parser.parse_args()
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # 外部业务节点模块加载（与 console 的 PLAITA_CONSOLE_NODE_MODULES 约定对齐）：
    # PLAITA_NODE_PATH 冒号分隔追加 sys.path；PLAITA_NODE_MODULES 逗号分隔，
    # 逐个 import 并调用其 register_all()（或 register）。业务仓（如 mediaflow
    # 的 plaita_flows.nodes）由此在 console 拉起的 worker 内生效。
    import importlib

    for extra in [p for p in os.environ.get("PLAITA_NODE_PATH", "").split(os.pathsep) if p]:
        if extra not in sys.path:
            sys.path.insert(0, extra)
    for mod_path in [m.strip() for m in os.environ.get("PLAITA_NODE_MODULES", "").split(",") if m.strip()]:
        try:
            mod = importlib.import_module(mod_path)
            register = getattr(mod, "register_all") or getattr(mod, "register")
            register()
            try:
                from plaita.node import register_code_node

                register_code_node(default_backend="subprocess")
            except ImportError:
                pass
            logger.info("已加载外部节点模块: %s", mod_path)
        except Exception as e:
            logger.error("外部节点模块加载失败 %s: %s", mod_path, e, exc_info=True)
            raise SystemExit(f"外部节点模块加载失败: {mod_path}")
    
    # 处理注册开关
    enable_registry = args.enable_registry and not args.no_registry
    
    try:
        # 创建执行状态存储
        storage_kwargs = {
            "redis_url": args.redis_url,
            "database_url": args.database_url
        }
        
        execution_storage = create_storage_component(
            args.execution_storage_type,
            "execution",
            **storage_kwargs
        )
        logger.info("已创建执行状态存储: %s类型", args.execution_storage_type)
        
        # 创建流程定义存储
        flow_storage = create_storage_component(
            args.flow_storage_type,
            "flow",
            **storage_kwargs
        )
        logger.info("已创建流程定义存储: %s类型", args.flow_storage_type)
        
        # 创建事件总线（默认启用；--no-event-bus 显式关闭）
        event_bus = None
        if not args.no_event_bus:
            event_bus = create_event_bus(
                args.event_bus_type,
                **storage_kwargs
            )
            logger.info("已创建事件总线: %s类型", args.event_bus_type)
        else:
            logger.warning("已禁用事件总线（--no-event-bus）；含挂起节点的流程将无法恢复")
        
        # 创建Redis流程工作器
        worker = RedisFlowWorker(
            redis_url=args.redis_url,
            queue_name=args.queue_name,
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=event_bus,
            cache_size=args.cache_size,
            cache_ttl=args.cache_ttl,
            enable_registry=enable_registry,
            registry_ttl=args.registry_ttl,
            heartbeat_interval=args.heartbeat_interval,
            consumer_group=args.consumer_group,
            consumer_name=args.consumer_name or None,
            claim_min_idle_ms=args.claim_min_idle_ms,
            lease_ttl_seconds=args.lease_ttl_seconds,
            max_deliveries=args.max_deliveries,
            dlq_key=args.dlq_key or None,
            read_block_ms=args.read_block_ms,
        )
        
        # 注册信号处理器以支持优雅关闭
        def signal_handler(signum, frame):
            logger.info("收到信号 %s，正在关闭...", signum)
            worker.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # 启动工作器。历史上这里有 --debug-mode 分支硬编码 flow_id="event_flow_demo"
        # 直接读 Redis + 手动 lrem 队列消息, 是开发期临时脚本——已删除。需要类似
        # 调试请用 Redis CLI 或独立 dev 脚本, 不要留在生产 CLI 入口里。
        logger.info("流程工作器启动成功，监听队列: %s", args.queue_name)
        worker.run()

    except Exception as e:
        logger.error("流程工作器启动失败: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()


