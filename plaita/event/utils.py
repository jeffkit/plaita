"""
事件系统工具函数
"""
import asyncio
import inspect
import json
import logging
from typing import Any, Callable, Dict, Optional, Union

logger = logging.getLogger(__name__)


def is_coroutine_function(func: Callable) -> bool:
    """检查函数是否是协程函数"""
    return inspect.iscoroutinefunction(func)


async def execute_callback(callback: Callable, *args, **kwargs) -> Any:
    """执行回调函数，支持同步和异步函数"""
    if is_coroutine_function(callback):
        return await callback(*args, **kwargs)
    else:
        return callback(*args, **kwargs)


def normalize_event(event_type_or_obj, data: Dict[str, Any] = None, **kwargs):
    """
    将多种格式的事件输入标准化
    
    Args:
        event_type_or_obj: 事件类型字符串或事件对象
        data: 事件数据字典
        **kwargs: 其他事件属性
        
    Returns:
        Event: 标准化后的事件对象
    """
    from .core import Event
    
    if isinstance(event_type_or_obj, Event):
        return event_type_or_obj
    elif isinstance(event_type_or_obj, str):
        # 拷一份再合并 kwargs：规范化不得就地改写调用方传入的 data（#224）
        event_data = dict(data) if data else {}
        if kwargs:
            event_data.update(kwargs)
        return Event(event_type=event_type_or_obj, data=event_data)
    elif isinstance(event_type_or_obj, dict):
        if 'event_type' in event_type_or_obj:
            # #224：此前调用 pydantic v2 模型上不存在的 ``Event.from_dict`` ⇒ 公开
            # 导出的 normalize_event 对 dict 形态输入必抛 AttributeError。改走既有
            # 构造路径（``model_validate``），并让 correlation_id 等字段随 dict 保真
            # （EventFilter 依赖它做 correlation 路由）。先拷一份：不写调用方字典。
            fields = dict(event_type_or_obj)
            event_data = fields.get('data', {})
            if isinstance(event_data, str):
                try:
                    event_data = json.loads(event_data)
                except json.JSONDecodeError:
                    event_data = {'value': event_data}
            if event_data is None:
                event_data = {}
            fields['data'] = dict(event_data) if isinstance(event_data, dict) else event_data
            # kwargs 只补 dict 未显式给出的字段（如 publish(**{"correlation_id": ...})）
            for key, value in kwargs.items():
                fields.setdefault(key, value)
            return Event.model_validate(fields)
        else:
            # 假设这是一个没有明确指定event_type的数据字典
            event_type = kwargs.pop('event_type', 'generic.event')
            return Event(event_type=event_type, data=event_type_or_obj)
    else:
        raise TypeError(f"无法从类型 {type(event_type_or_obj)} 创建事件") 