"""Single parse guard for untrusted JSON text — plaita-ai 全仓统一解析口.

A2R51-02 / A2R52-05 / A2R52-06 / A2R53-04 的系统性收口：调用方自控的 JSON
文本（~12k 层嵌套只需 ~24KB）能让 ``json.loads`` 抛 ``RecursionError``
（RuntimeError 族），``except ValueError`` 拦不住，裸调用更是直接穿出工具体
（ToolNode 零包裹直穿引擎节点层）或入口。全仓 LLM/工具回调解析点统一走
:func:`guarded_json_loads`：任何解析失败折叠成一个具名 ``GuardedJsonError``，
调用方按自身语义降级——工具体 → case unavailable 式 ``ok:false`` 载荷，
入口 → 具名 4xx/错误退出。唯一不变量：异常不得以裸形状穿出。

口径参照（已达标形）：``evals/judges.py`` 的三段 ``except Exception`` 降级
（A2R45-02/A2R46-01）。
"""

from __future__ import annotations

import json
from typing import Any

__all__ = ["GuardedJsonError", "guarded_json_loads"]


class GuardedJsonError(ValueError):
    """Unparseable JSON text of any shape, named at a single point.

    Covers everything ``json.loads`` can raise: ``JSONDecodeError``（坏语法，
    ValueError 族）、``RecursionError``（深嵌套）、``TypeError``（非 str 输入）。
    Subclasses ``ValueError`` so callers still guarding with
    ``except ValueError`` keep catching during migration.
    """


def guarded_json_loads(text: Any, *, what: str = "payload") -> Any:
    """``json.loads`` with a single named failure mode.

    Args:
        text: The untrusted JSON text (tool argument / LLM reply / file body).
        what: Field label for the error message, e.g. ``"inputs_json"``.

    Raises:
        GuardedJsonError: For every failure ``json.loads`` can produce —
            bad syntax (``JSONDecodeError``), ~12k nested layers in a ~24KB
            string (``RecursionError``), non-str input (``TypeError``).
    """
    try:
        return json.loads(text)
    except Exception as exc:
        raise GuardedJsonError(f"{what} is not valid JSON: {type(exc).__name__}: {exc}") from exc
