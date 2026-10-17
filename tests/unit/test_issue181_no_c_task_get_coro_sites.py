"""#181 结构性守卫：**生产代码不得调用 C 实现的 ``Task.get_coro()``**。

理由（实证）：``asyncio.Task`` 在 CPython 上是 ``_asyncio.Task``（C 实现、不可 monkeypatch），
其 ``get_coro()`` 在「**已结束且 coro 已回收**」（``_state == 'FINISHED'`` 且 ``_coro is None``）
的 Task 上会 **SIGSEGV**——**进程级信号，Python 层 ``except BaseException`` 结构上够不着**
（实测：Python 3.12 必现 5/5；3.11.16 表现为全量套件「挂起」）。

``plaita/event/redis.py`` 的 3 处调用点已全部改为**读纯 Python 属性** ``_coro`` / 用
``done()``（只读 ``_state``、不解引用 coro，故在坏对象上安全）；本守卫防止同类调用
**再次混入生产代码**——它是「同一认知盲区出现过两处（:2430 与 :2587）」的结构性收口。

口径（避免假阳/假阴）：
- 用 **AST** 扫描（不是 grep）⇒ 注释与字符串里的 ``get_coro`` 不算调用点；
- 覆盖 ``x.get_coro()`` 与 ``Task.get_coro(x)`` 两种写法；
- 只扫 **``plaita/``（生产代码）**；``tests/``、``scripts/`` 不在范围内（测试/工具可用）。
"""

from __future__ import annotations

import ast
import pathlib

PROD_ROOT = pathlib.Path(__file__).resolve().parents[2] / "plaita"


def _get_coro_call_sites(source: str) -> list[int]:
    """返回源码里 ``get_coro`` **调用点**的行号（AST 口径，注释/字符串不算）。"""
    lines: list[int] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # `x.get_coro(...)` 与 `Task.get_coro(x)` 都算
        if isinstance(func, ast.Attribute) and func.attr == "get_coro":
            lines.append(node.lineno)
    return lines


def _production_sites() -> list[str]:
    offenders: list[str] = []
    for path in sorted(PROD_ROOT.rglob("*.py")):
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for lineno in _get_coro_call_sites(source):
            offenders.append(f"{path.relative_to(PROD_ROOT.parent)}:{lineno}")
    return offenders


def test_scan_detects_a_synthetic_call_site() -> None:
    """**正控**：扫描器本身必须能发现调用点（否则下面的零断言是空的）。"""
    assert _get_coro_call_sites("import asyncio\nasyncio.Task.get_coro(t)\n") == [2]
    assert _get_coro_call_sites("t.get_coro()\n") == [1]
    # 注释与字符串不算调用点（AST 口径）
    assert _get_coro_call_sites("# asyncio.Task.get_coro(t)\ns = 'get_coro()'\n") == []


def test_no_get_coro_call_sites_in_production() -> None:
    offenders = _production_sites()
    assert not offenders, (
        "生产代码里出现 ``get_coro()`` 调用点——#181 已实证它是 SIGSEGV 面"
        "（C 实现、`except BaseException` 够不着）。请改用纯 Python 属性读取："
        f"``getattr(task, '_coro', None)``。违规位置：{offenders}"
    )
