"""
Redis Stream 日志处理器
将日志发送到 Redis Stream 以支持实时日志查看
"""
import json
import logging
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from redis import Redis


class RedisStreamHandler(logging.Handler):
    """
    将日志发送到 Redis Stream 的处理器
    
    日志存储格式：
    - Key: plaita:logs:{service_type}:{instance_id}
    - 每条日志包含: timestamp, level, message, context
    """
    
    # Stream 最大长度（默认保留 10000 条）
    DEFAULT_MAX_LEN = 10000
    # 发布通道前缀
    STREAM_CHANNEL_PREFIX = "plaita:logs:stream"
    
    def __init__(
        self,
        redis_client: Redis,
        service_type: str,
        instance_id: str,
        max_len: int = DEFAULT_MAX_LEN,
        publish_realtime: bool = True
    ):
        """
        初始化 Redis Stream 日志处理器
        
        Args:
            redis_client: Redis 客户端
            service_type: 服务类型
            instance_id: 实例 ID
            max_len: Stream 最大长度
            publish_realtime: 是否同时发布到 Pub/Sub 通道
        """
        super().__init__()
        self.redis_client = redis_client
        self.service_type = service_type
        self.instance_id = instance_id
        self.max_len = max_len
        self.publish_realtime = publish_realtime
        
        # Stream key
        self.stream_key = f"plaita:logs:{service_type}:{instance_id}"
        # Pub/Sub 通道
        self.pubsub_channel = f"{self.STREAM_CHANNEL_PREFIX}:{service_type}:{instance_id}"
        
        # 异步写入队列和线程
        self._queue: list = []
        self._queue_lock = threading.Lock()
        self._flush_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # 启动异步刷新线程
        self._start_flush_thread()
    
    def _get_stream_key(self) -> str:
        """获取 Stream key"""
        return self.stream_key
    
    def emit(self, record: logging.LogRecord):
        """
        发送日志记录
        
        Args:
            record: 日志记录
        """
        try:
            # 格式化日志
            log_entry = self._format_record(record)
            
            # 添加到队列
            with self._queue_lock:
                self._queue.append(log_entry)
            
        except Exception:
            self.handleError(record)
    
    def _format_record(self, record: logging.LogRecord) -> Dict[str, Any]:
        """
        格式化日志记录
        
        Args:
            record: 日志记录
            
        Returns:
            格式化后的日志字典
        """
        # 提取上下文信息
        context = {}
        if hasattr(record, 'context'):
            context = record.context
        
        # 异常信息
        exc_info = None
        if record.exc_info:
            exc_info = self.formatter.formatException(record.exc_info) if self.formatter else str(record.exc_info)
        
        return {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "service_type": self.service_type,
            "instance_id": self.instance_id,
            "logger": record.name,
            "module": record.module,
            "line": record.lineno,
            "context": json.dumps(context) if context else "{}",
            "exception": exc_info or "",
        }
    
    def _start_flush_thread(self):
        """启动刷新线程"""
        if self._flush_thread and self._flush_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            daemon=True,
            name=f"log-flush-{self.instance_id}"
        )
        self._flush_thread.start()
    
    def _flush_loop(self):
        """刷新循环"""
        while not self._stop_event.is_set():
            self._flush()
            self._stop_event.wait(1.0)  # 每秒刷新一次
    
    def _flush(self):
        """刷新日志到 Redis"""
        with self._queue_lock:
            if not self._queue:
                return
            entries = self._queue[:]
            self._queue.clear()
        
        try:
            pipeline = self.redis_client.pipeline()
            
            for entry in entries:
                # 写入 Stream
                pipeline.xadd(
                    self.stream_key,
                    entry,
                    maxlen=self.max_len,
                    approximate=True
                )
                
                # 发布到 Pub/Sub 通道（实时日志）
                if self.publish_realtime:
                    pipeline.publish(
                        self.pubsub_channel,
                        json.dumps(entry)
                    )
            
            pipeline.execute()
            
        except Exception as e:
            # 重新加入队列
            with self._queue_lock:
                self._queue = entries + self._queue
            print(f"日志刷新失败: {e}")
    
    def close(self):
        """关闭处理器"""
        # 停止刷新线程
        self._stop_event.set()
        if self._flush_thread:
            self._flush_thread.join(timeout=5)
        
        # 最后刷新一次
        self._flush()
        
        super().close()


def setup_redis_logging(
    redis_client: Redis,
    service_type: str,
    instance_id: str,
    logger_name: str = "plaita",
    level: int = logging.INFO,
    max_len: int = RedisStreamHandler.DEFAULT_MAX_LEN
) -> RedisStreamHandler:
    """
    设置 Redis 日志处理器
    
    Args:
        redis_client: Redis 客户端
        service_type: 服务类型
        instance_id: 实例 ID
        logger_name: 日志器名称
        level: 日志级别
        max_len: Stream 最大长度
        
    Returns:
        RedisStreamHandler: 创建的日志处理器
    """
    # 获取日志器
    logger = logging.getLogger(logger_name)
    
    # 创建处理器
    handler = RedisStreamHandler(
        redis_client=redis_client,
        service_type=service_type,
        instance_id=instance_id,
        max_len=max_len
    )
    handler.setLevel(level)
    
    # 设置格式化器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    
    # 添加到日志器
    logger.addHandler(handler)
    
    return handler


class LogContext:
    """
    日志上下文管理器
    用于向日志记录添加上下文信息
    """
    
    _local = threading.local()
    
    @classmethod
    def set(cls, **kwargs):
        """设置上下文"""
        if not hasattr(cls._local, 'context'):
            cls._local.context = {}
        cls._local.context.update(kwargs)
    
    @classmethod
    def get(cls) -> Dict[str, Any]:
        """获取上下文"""
        if not hasattr(cls._local, 'context'):
            cls._local.context = {}
        return cls._local.context
    
    @classmethod
    def clear(cls):
        """清除上下文"""
        cls._local.context = {}
    
    def __init__(self, **kwargs):
        self.context = kwargs
        self.previous_context = {}
    
    def __enter__(self):
        self.previous_context = self.get().copy()
        self.set(**self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._local.context = self.previous_context
        return False


class ContextFilter(logging.Filter):
    """
    添加上下文信息到日志记录的过滤器
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        record.context = LogContext.get()
        return True

