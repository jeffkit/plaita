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

class FlowWorker:
    """
    流程工作器，负责执行流程和处理恢复任务
    """
    
    def __init__(self, execution_storage: ExecutionStorage, flow_storage: FlowStorage, event_bus: EventBus=None, cache_size: int = 100, cache_ttl: int = 300, callback_handlers: Optional[list] = None):
        """
        初始化流程工作器

        Args:
            execution_storage: 执行状态存储实例
            flow_storage: 流程定义存储实例
            event_bus: 事件总线实例
            cache_size: 缓存大小，默认100
            cache_ttl: 缓存过期时间(秒)，默认300秒
            callback_handlers: 分布式执行期间贯穿所有步骤的回调列表
        """
        self.execution_storage = execution_storage
        self.flow_storage = flow_storage
        self.event_bus = event_bus
        self.callback_handlers = list(callback_handlers) if callback_handlers else []
        # 初始化流程定义缓存，使用TTL缓存
        self.flow_definition_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
    
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
            logger.info(f"从缓存获取流程定义: {cache_key}")
            return self.flow_definition_cache[cache_key]
        
        # 缓存未命中，从存储获取
        logger.info(f"从存储获取流程定义: {flow_id}, 版本: {version or 'latest'}")
        flow_definition = self.flow_storage.get_flow(flow_id, version)
        
        if not flow_definition:
            # Delegate diagnostic details to the storage layer via an optional
            # diagnose() method — FlowWorker must not peek at storage internals
            # (e.g. Redis keys / namespaces) directly.
            if hasattr(self.flow_storage, "diagnose_missing_flow"):
                try:
                    self.flow_storage.diagnose_missing_flow(flow_id)
                except Exception:
                    pass

            error_msg = f"找不到流程定义: {flow_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 解析流程定义
        try:
            logger.info(f"成功获取流程定义: {flow_id}, 数据: {flow_definition.get('name', 'unknown')}")
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
        
        
        
        logger.info(f"开始执行流程: {flow_id}, 版本: {version or '最新版本'}")
        
        try:
            # 创建流程执行器并执行流程
            # 复用同一个 FlowExecution 贯穿所有分布式步骤, 保留用户回调
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED.value

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
                logger.error(f"保存执行状态失败: {execution_id}")
                raise RuntimeError(f"保存执行状态失败: {execution_id}")

            # 处理执行结果
            final_result = self._process_execution_result(flow, result, state, execution)

            return final_result

        except Exception as e:
            logger.error("执行流程出错: %s", e, exc_info=True)
            raise RuntimeError(f"执行流程出错: {e}")
    
    def resume_flow(self, flow_id: str, execution_id: str, resume_type: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        恢复流程执行
        
        Args:
            flow_id: 流程ID
            execution_id: 执行ID
            resume_type: 恢复类型，支持continue(继续)、cancel(取消)、timeout(超时)、event(事件)
            data: 恢复数据，当resume_type为event时有效
            
        Returns:
            Dict[str, Any]: 流程执行结果
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
        
        # 获取流程版本
        version = state.flow_version
        
        # 获取流程定义
        flow = self.get_flow_definition(flow_id, version)
        
        # 解析流程定义
        logger.info(f"恢复流程执行: {flow_id}, 执行ID: {execution_id}, 恢复类型: {resume_type}")
        
        try:
            # 复用同一个 FlowExecution 贯穿恢复后的所有分布式步骤
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self.callback_handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED.value

            # 直接使用 run_distributed 恢复执行
            result = execution.run_distributed(
                flow,
                saved_context=state.context,
                resume_type=resume_type,
                resume_data=data,
            )

            # 处理执行结果
            final_result = self._process_execution_result(flow, result, state, execution)

            return final_result

        except Exception as e:
            logger.error("恢复流程执行出错: %s", e, exc_info=True)

            # 更新执行状态为错误
            state.status = "error"
            state.error = {"message": str(e)}
            state.end_time = datetime.now().isoformat()

            self.execution_storage.save_execution_state(execution_id, state)

            raise RuntimeError(f"恢复流程执行出错: {e}")

    def _process_execution_result(self, flow: Flow, result: Dict[str, Any], state: ExecutionState, execution: Optional[FlowExecution] = None) -> Dict[str, Any]:
        """
        处理流程执行结果

        Args:
            flow: 流程对象
            result: 执行结果
            state: 执行状态
            execution: 贯穿本执行全过程的 FlowExecution (复用以保留回调);
                为 None 时按需创建, 仅供向后兼容的简单调用场景使用

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
            execution.mode = ExecutionMode.DISTRIBUTED.value

        PERSIST_EVERY = 5
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
                break
            else:
                state.status = "running"
                try:
                    result = execution.run_distributed(
                        flow,
                        saved_context=context,
                        resume_type="continue",
                    )

                    context = result.get("context", context)
                    steps_since_persist += 1

                    is_end = result.get("is_end", False)
                    is_suspend = result.get("is_suspend", False)

                    if not is_end and not is_suspend and steps_since_persist >= PERSIST_EVERY:
                        state.context = context
                        state.last_update_time = datetime.now().isoformat()
                        self.execution_storage.save_execution_state(execution_id, state)
                        steps_since_persist = 0
                        logger.info(f"流程步骤执行完成，继续下一步: {execution_id}")

                except Exception as e:
                    logger.error("流程执行出错: %s", e, exc_info=True)
                    state.status = "error"
                    state.context = context
                    state.error = {"message": str(e)}
                    state.end_time = datetime.now().isoformat()
                    self.execution_storage.save_execution_state(execution_id, state)
                    break

        return result
    
class RedisFlowWorker(RegistryMixin, ControlMixin, FlowWorker):
    """
    基于Redis的流程工作器
    
    支持服务注册、心跳机制和远程控制
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
    ):
        super().__init__(execution_storage, flow_storage, event_bus, cache_size, cache_ttl, callback_handlers=callback_handlers)
        self.redis_url = redis_url
        self.redis_client = redis_client or Redis.from_url(redis_url)
        self.queue_name = queue_name
        self._running = False
        self._active_task_count = 0
        self._log_handler = None
        
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

    def run(self):
        """
        运行流程工作器, 从redis队列中获取消息，消息有start、resume 两种类型，
        消息体的payload和FlowWorker.start_flow、FlowWorker.resume_flow的入参一致.
        """
        self._running = True
        
        # 注册服务
        if self._enable_registry:
            self.register_service()
            # 启动控制监听
            self.start_control_listener()
        
        logger.info(f"流程工作器已启动，监听队列: {self.queue_name}")
        
        try:
            while self._running:
                # 从Redis队列中获取任务
                message = self.redis_client.blpop(self.queue_name, timeout=10)
                if message:
                    self._active_task_count += 1
                    if self._enable_registry:
                        self.update_registry_info(active_tasks=self._active_task_count)
                    
                    try:
                        message_data = json.loads(message[1])
                        message_type = message_data.get("type")
                        if message_type == "start":
                            self.start_flow(
                                message_data.get("flow_id"), 
                                message_data.get("params"), 
                                message_data.get("version")
                            )
                        elif message_type == "resume":
                            self.resume_flow(
                                message_data.get("flow_id"), 
                                message_data.get("execution_id"), 
                                message_data.get("resume_type"), 
                                message_data.get("data")
                            )
                    finally:
                        self._active_task_count -= 1
                        if self._enable_registry:
                            self.update_registry_info(active_tasks=self._active_task_count)
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
        logger.info(f"收到远程停止命令，优雅停止: {graceful}")
        self.stop()
    
    def _on_status_command(self) -> Dict[str, Any]:
        """响应状态查询命令"""
        return {
            "status": "running" if self._running else "stopped",
            "active_tasks": self._active_task_count,
            "queue_name": self.queue_name
        }

# 新增命令行入口


from plaita.server.factory import create_storage_component, create_event_bus  # noqa: F401


def main():
    """命令行入口程序"""
    parser = argparse.ArgumentParser(description="Plaita流程工作器")
    
    # Redis参数（支持环境变量 PLAITA_REDIS_URL / REDIS_URL）
    parser.add_argument("--redis-url",
                      default=os.environ.get("PLAITA_REDIS_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/0")),
                      help="Redis连接URL")
    parser.add_argument("--queue-name",
                      default=os.environ.get("PLAITA_QUEUE_NAME", os.environ.get("QUEUE_NAME", "plaita:flow:queue")),
                      help="Redis队列名称")
    
    # 数据库参数
    parser.add_argument("--database-url", default="sqlite:///flow.db",
                      help="数据库连接URL")
    
    # 存储组件类型
    parser.add_argument("--execution-storage-type", choices=["memory", "redis", "db"], default="redis",
                      help="执行状态存储类型")
    parser.add_argument("--flow-storage-type", choices=["memory", "redis", "db"], default="redis",
                      help="流程定义存储类型")
    
    # 事件总线参数
    parser.add_argument("--event-bus-type", choices=["memory", "redis", "db"], default="redis",
                      help="事件总线类型")
    parser.add_argument("--use-event-bus", action="store_true",
                      help="是否使用事件总线")
    
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
    parser.add_argument("--heartbeat-interval", type=int, default=10,
                      help="心跳间隔(秒)")

    args = parser.parse_args()
    
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
        logger.info(f"已创建执行状态存储: {args.execution_storage_type}类型")
        
        # 创建流程定义存储
        flow_storage = create_storage_component(
            args.flow_storage_type,
            "flow",
            **storage_kwargs
        )
        logger.info(f"已创建流程定义存储: {args.flow_storage_type}类型")
        
        # 创建事件总线（可选）
        event_bus = None
        if args.use_event_bus:
            event_bus = create_event_bus(
                args.event_bus_type,
                **storage_kwargs
            )
            logger.info(f"已创建事件总线: {args.event_bus_type}类型")
        
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
            heartbeat_interval=args.heartbeat_interval
        )
        
        # 注册信号处理器以支持优雅关闭
        def signal_handler(signum, frame):
            logger.info(f"收到信号 {signum}，正在关闭...")
            worker.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # 启动工作器。历史上这里有 --debug-mode 分支硬编码 flow_id="event_flow_demo"
        # 直接读 Redis + 手动 lrem 队列消息, 是开发期临时脚本——已删除。需要类似
        # 调试请用 Redis CLI 或独立 dev 脚本, 不要留在生产 CLI 入口里。
        logger.info(f"流程工作器启动成功，监听队列: {args.queue_name}")
        worker.run()

    except Exception as e:
        logger.error(f"流程工作器启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


