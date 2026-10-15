"""
Redis Stream 日志处理器
将日志发送到 Redis Stream 以支持实时日志查看
"""
import json
import logging
import threading
from collections import deque
from datetime import datetime
from typing import Any, Dict, Optional

from redis import Redis

from .tenant_context import current_tenant

# 审计通道（#84）：丢弃/刷新失败告警**故意**不挂在 `plaita` logger 树上
# （`propagate=False` + 自有 stderr handler），否则告警会被 RedisStreamHandler 自己
# 收进队列（自噬），失败期间队列永远排不空。
_AUDIT_LOGGER_NAME = "plaita.server.log_handler.audit"
_audit_logger = logging.getLogger(_AUDIT_LOGGER_NAME)
_audit_logger.propagate = False
# 审计让告警**独立于 root logger 级别**（#84 复审缺陷）：级别留在 NOTSET 会继承
# root；`PLAITA_LOG_LEVEL=ERROR`（已文档化的开关）或嵌入方抬 root 时，
# `_audit_logger.warning(...)` 会在记录创建前被判为 disabled ⇒ 丢弃/回灌完全静默
# 而 dropped_count 照涨。显式钉在 WARNING，告警只受本 logger 约束。
_audit_logger.setLevel(logging.WARNING)
if not _audit_logger.handlers:
    _audit_handler = logging.StreamHandler()
    _audit_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    _audit_logger.addHandler(_audit_handler)


class RedisStreamHandler(logging.Handler):
    """
    将日志发送到 Redis Stream 的处理器
    
    日志存储格式：
    - Key: plaita:logs:{service_type}:{instance_id}
    - 每条日志包含: timestamp, level, message, context
    """
    
    # Stream 最大长度（默认保留 10000 条）——**只约束 Redis Stream 一侧**
    DEFAULT_MAX_LEN = 10000
    #: 客户端队列上限（#84：与 Stream 侧 `max_len` 无关；超出即丢**最旧**一条）
    DEFAULT_MAX_QUEUE = 10000
    #: 每累计丢弃多少条聚合告警一次（丢弃必须可观测，但不逐条刷屏）
    DROP_WARN_EVERY = 100
    #: 单次 pipeline.execute() 最多提交的日志条数（#84：恢复瞬间不再构造 2N 条命令）
    DEFAULT_CHUNK_SIZE = 500
    # 发布通道前缀
    STREAM_CHANNEL_PREFIX = "plaita:logs:stream"
    
    def __init__(
        self,
        redis_client: Redis,
        service_type: str,
        instance_id: str,
        max_len: int = DEFAULT_MAX_LEN,
        publish_realtime: bool = True,
        max_queue: int = DEFAULT_MAX_QUEUE,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        """
        初始化 Redis Stream 日志处理器
        
        Args:
            redis_client: Redis 客户端
            service_type: 服务类型
            instance_id: 实例 ID
            max_len: Stream 最大长度
            publish_realtime: 是否同时发布到 Pub/Sub 通道
            max_queue: 客户端队列上限（#84）。队列满时丢弃**最旧**一条并计数，
                `DROP_WARN_EVERY` 条聚合告警一次；刷新失败时回灌也受此上限约束。
            chunk_size: 单次 `pipeline.execute()` 提交的条数上限（#84）。积压
                恢复时按此分批提交（每批至多 `2 × chunk_size` 条命令：
                `xadd` + `publish`），避免一次性打回 2N 条命令的洪峰。
        """
        super().__init__()
        self.redis_client = redis_client
        self.service_type = service_type
        self.instance_id = instance_id
        self.max_len = max_len
        self.publish_realtime = publish_realtime
        self.max_queue = max(1, int(max_queue))
        self.chunk_size = max(1, int(chunk_size))
        self._dropped = 0
        
        # Stream key
        self.stream_key = f"plaita:logs:{service_type}:{instance_id}"
        # Pub/Sub 通道
        self.pubsub_channel = f"{self.STREAM_CHANNEL_PREFIX}:{service_type}:{instance_id}"
        
        # 异步写入队列和线程
        # #84（perf）：`deque` 而非 `list`——队列满时 `del list[0]` 是 O(n)，
        # 在 10000 上限 + 持续丢弃下退化成 O(n²)；`popleft()` 常数时间，
        # 丢弃方向/计数/分批提交语义**零变化**。
        self._queue: deque = deque()
        self._queue_lock = threading.Lock()
        self._flush_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # 启动异步刷新线程
        self._start_flush_thread()
    
    @property
    def dropped_count(self) -> int:
        """因队列上限/失败回灌截断而丢弃的日志条数（可观测，#84）。"""
        return self._dropped

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
            
            # 有界入队：满则丢**最旧**一条（并计数 + 聚合响亮告警）
            dropped_total = 0
            with self._queue_lock:
                if len(self._queue) >= self.max_queue:
                    self._queue.popleft()
                    self._dropped += 1
                    dropped_total = self._dropped
                self._queue.append(log_entry)
                queue_len = len(self._queue)
            if dropped_total and (dropped_total == 1
                                  or dropped_total % self.DROP_WARN_EVERY == 0):
                _audit_logger.warning(
                    "RedisStreamHandler 队列已达上限 %d，丢弃最旧日志（累计 %d 条；"
                    "队列 %d/%d，stream=%s）",
                    self.max_queue, dropped_total, queue_len, self.max_queue,
                    self.stream_key,
                )
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
            # 多租户：当前任务租户随条目落库（键保持平台级 plaita:logs:*，
            # console 按条目字段过滤，避免键段错位）
            "tenant_id": current_tenant(),
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
        """刷新日志到 Redis（#84：按 `chunk_size` 分批 `execute()`）"""
        with self._queue_lock:
            if not self._queue:
                return
            entries = list(self._queue)
            self._queue.clear()
        
        chunk = max(1, int(self.chunk_size))
        start = 0
        try:
            # 分批提交（#84）：积压恢复时单次 execute() 的命令数封顶在
            # 2 × chunk_size（每条 entry 一条 xadd + 一条 publish），
            # 不再把整个积压一次打回刚恢复的 Redis（失败→堆积→恢复即洪峰）。
            for start in range(0, len(entries), chunk):
                batch = entries[start:start + chunk]
                pipeline = self.redis_client.pipeline()
                
                for entry in batch:
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
            # 有界回灌（#84）：只回灌**本批起未成功提交**的部分（此前各批已
            # execute 成功，不重复入队）；合并后超出上限的部分整批丢弃
            # **并计数**，绝不静默。
            pending = entries[start:]
            with self._queue_lock:
                merged = list(pending)
                merged.extend(self._queue)      # 合并期间并发追加的条目
                overflow = len(merged) - self.max_queue
                if overflow > 0:
                    del merged[:overflow]
                    self._dropped += overflow
                self._queue = deque(merged)
                queue_len = len(self._queue)
                dropped_total = self._dropped
            _audit_logger.warning(
                "日志刷新失败: %s（本批起回灌 %d 条，丢弃最旧 %d 条，累计丢弃 %d，队列 %d/%d）",
                e, len(pending), max(0, overflow), dropped_total, queue_len,
                self.max_queue,
            )
    
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
    max_len: int = RedisStreamHandler.DEFAULT_MAX_LEN,
    max_queue: int = RedisStreamHandler.DEFAULT_MAX_QUEUE,
    chunk_size: int = RedisStreamHandler.DEFAULT_CHUNK_SIZE,
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
        max_queue: 客户端队列上限（#84，可选覆盖）
        chunk_size: 单次 execute() 提交条数上限（#84，可选覆盖）
        
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
        max_len=max_len,
        max_queue=max_queue,
        chunk_size=chunk_size,
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

