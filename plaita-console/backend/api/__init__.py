"""
API 路由模块
"""
try:
    from . import services, executions, queues, logs
except ImportError:
    from api import services, executions, queues, logs

__all__ = ["services", "executions", "queues", "logs"]

