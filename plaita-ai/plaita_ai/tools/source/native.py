"""NativeToolSource — 把已有 Python 函数声明为工具（配置轨入口）。"""

from __future__ import annotations

import importlib
from typing import Any, Callable, ClassVar

from plaita_ai.tools.source.base import BaseToolSource


class NativeToolSource(BaseToolSource):
    """通过 ``module.function`` 引用已有 callable。

    适合复杂业务逻辑留在 Python，仅用配置暴露给 Agent。
    """

    type: ClassVar[str] = "native"

    module: str
    function: str

    def to_callable(self) -> Callable[..., Any]:
        mod = importlib.import_module(self.module)
        func = getattr(mod, self.function, None)
        if func is None or not callable(func):
            raise AttributeError(
                f"NativeToolSource {self.name!r}: "
                f"{self.module}.{self.function} 不是可调用对象"
            )
        if self.description and not (getattr(func, "__doc__", None) or "").strip():
            try:
                func.__doc__ = self.description
            except Exception:
                pass
        return func
