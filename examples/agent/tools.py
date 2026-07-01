"""示例工具函数 —— 注册到 :class:`ToolNode` 供 flow 调用。

参考 edan-backend 的 Action 节点：任意 Python 函数都能注册成"工具"，flow 里的
``tool`` 节点按 ``action`` 名取函数、按 ``params`` 展开参数调用。
"""
from __future__ import annotations

from .nodes import ToolNode


@ToolNode.register("weather")
def weather(city: str) -> str:
    """模拟天气查询工具。"""
    return f"{city}：晴，25°C，东南风 3 级"


@ToolNode.register("calc")
def calc(a: int, b: int) -> str:
    """简单计算工具：返回两数之和。"""
    return f"{a} + {b} = {a + b}"


@ToolNode.register("search")
def search(query: str) -> str:
    """模拟通用搜索工具。"""
    return f"关于「{query}」的 3 条结果：① … ② … ③ …"


def register_tools() -> None:
    """显式触发注册（装饰器在导入时已注册，此函数仅用于占位/可读性）。"""
    # 装饰器已在导入时完成注册，这里无需额外操作。
    return None
