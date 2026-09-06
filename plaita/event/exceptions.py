"""
事件系统相关异常
"""

class EventError(Exception):
    """事件系统基础异常"""
    pass


class EventTimeoutError(EventError):
    """等待事件超时异常"""
    def __init__(self, event_type: str, timeout: float):
        self.event_type = event_type
        self.timeout = timeout
        super().__init__(
            f"等待事件 '{event_type}' 超时，等待时间 {timeout}秒。"
            "注意：内存事件总线不回放——在开始等待之前发布的事件不会送达本等待者"
            "（分布式/持久化总线语义见 docs-site/docs/distributed/event-system.md）"
        )


class EventNotFoundError(EventError):
    """事件未找到异常"""
    def __init__(self, event_id: str):
        self.event_id = event_id
        super().__init__(f"事件ID为 '{event_id}' 的事件未找到")


class EventStorageError(EventError):
    """事件存储异常"""
    pass 