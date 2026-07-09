"""
统一的存储组件和事件总线工厂
"""
import re
import logging

logger = logging.getLogger("plaita.server.factory")


def _parse_redis_url(redis_url: str):
    """解析 Redis URL 为连接参数"""
    host, port, db, password = "localhost", 6379, 0, None
    if redis_url:
        try:
            pattern = r'redis://(:(.*?)@)?(.*?):(.*?)(/(\d+))?$'
            match = re.match(pattern, redis_url)
            if match:
                password = match.group(2) or None
                host = match.group(3) or host
                port = int(match.group(4)) if match.group(4) else port
                db = int(match.group(6)) if match.group(6) else db
        except Exception as e:
            logger.warning("解析Redis URL失败: %s, 使用默认连接参数", e)
    return host, port, db, password


def _create_async_engine(database_url: str):
    """从 database_url 创建 SQLAlchemy 异步引擎（event/subscription db 后端需要）。"""
    if not database_url:
        raise ValueError("db 后端需要 database_url")
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(database_url)


def create_storage_component(storage_type, component_type, **kwargs):
    """
    创建存储组件实例

    Args:
        storage_type: 存储类型 (memory, redis, db)
        component_type: 组件类型 (execution, flow, subscription)
        **kwargs: 组件初始化参数 (redis_url, database_url)

    Returns:
        创建的存储组件实例
    """
    if storage_type == "memory":
        if component_type == "execution":
            from plaita.storage.memory import MemoryExecutionStorage
            return MemoryExecutionStorage()
        elif component_type == "flow":
            from plaita.storage.memory import MemoryFlowStorage
            return MemoryFlowStorage()
        elif component_type == "subscription":
            from plaita.event.memory import InMemoryEventSubscriptionStorage
            return InMemoryEventSubscriptionStorage()

    elif storage_type == "redis":
        redis_url = kwargs.get("redis_url", "redis://localhost:6379/0")

        if component_type in ("execution", "flow"):
            host, port, db, password = _parse_redis_url(redis_url)
            if component_type == "execution":
                from plaita.storage.redis import RedisExecutionStorage
                return RedisExecutionStorage(host=host, port=port, db=db, password=password)
            else:
                from plaita.storage.redis import RedisFlowStorage
                return RedisFlowStorage(host=host, port=port, db=db, password=password)
        elif component_type == "subscription":
            from plaita.event.redis import RedisEventSubscriptionStorage
            return RedisEventSubscriptionStorage(redis_url=redis_url, key_prefix="plaita:subscription:")

    elif storage_type == "db":
        database_url = kwargs.get("database_url")
        if component_type == "execution":
            from plaita.storage.sqlalchemy import SqlalchemyExecutionStorage
            return SqlalchemyExecutionStorage(database_url=database_url)
        elif component_type == "flow":
            from plaita.storage.sqlalchemy import SqlalchemyFlowStorage
            return SqlalchemyFlowStorage(database_url=database_url)
        elif component_type == "subscription":
            # SqlalchemyEventSubscriptionStorage 构造函数要 engine，不是 database_url
            from plaita.event.sqlalchemy import SqlalchemyEventSubscriptionStorage
            return SqlalchemyEventSubscriptionStorage(engine=_create_async_engine(database_url))
    else:
        raise ValueError(f"不支持的存储类型: {storage_type}")


def create_event_bus(bus_type, **kwargs):
    """
    创建事件总线实例

    Args:
        bus_type: 事件总线类型 (memory, redis, db)
        **kwargs: 组件初始化参数 (redis_url, database_url)

    Returns:
        创建的事件总线实例
    """
    if bus_type == "memory":
        from plaita.event.memory import InMemoryEventBus
        return InMemoryEventBus()
    elif bus_type == "redis":
        from plaita.event.redis import RedisEventBus
        return RedisEventBus(redis_url=kwargs.get("redis_url"))
    elif bus_type == "db":
        # SqlalchemyEventBus 构造函数要 engine，不是 database_url
        from plaita.event.sqlalchemy import SqlalchemyEventBus
        return SqlalchemyEventBus(engine=_create_async_engine(kwargs.get("database_url")))
    else:
        raise ValueError(f"不支持的事件总线类型: {bus_type}")
