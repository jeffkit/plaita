"""
plaita.core.expression — Structured expression engine.

Provides:
- FunctionCategory enum for classifying expression functions.
- FunctionDescriptor frozen dataclass with side-effect metadata.
- ExpressionRegistry for querying functions by category / side-effect.
- ExpressionEvaluator that delegates to plaita.io evaluate/parse_function.
- get_default_expression_registry() factory with all 90+ built-in functions.
"""

from __future__ import annotations

import json
import math
import logging
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable, Dict, List, NamedTuple, Optional

logger = logging.getLogger("plaita.core.expression")


# ---------------------------------------------------------------------------
# FunctionCategory enum
# ---------------------------------------------------------------------------

class FunctionCategory(str, Enum):
    """Classification categories for expression functions."""

    MATH = "math"
    STRING = "string"
    LOGIC = "logic"
    ARRAY = "array"
    DICT = "dict"
    DATETIME = "datetime"
    JSON = "json"
    TYPE = "type"


# ---------------------------------------------------------------------------
# FunctionDescriptor
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FunctionDescriptor:
    """Metadata wrapper around a registered expression function.

    Attributes:
        name: Function name (used in expressions as ``$F.name(...)``).
        func: The underlying callable.
        category: Classification category.
        has_side_effects: ``True`` if the function mutates its input.
            Side-effect functions (e.g. pop, set, delete, clear) are **not
            thread-safe** and must not be used in concurrent execution
            contexts without external synchronisation.  Since plaita#69 the
            evaluator enforces a runtime gate on this flag: **any argument** of
            such a mutator that is a live ``$INPUT``/``$NODE``/``$GLOBAL``/
            ``$PARENT``/``$ENV`` path (or an undeclared producer call) is
            refused with a named error before evaluation; a *shallow* producer
            result stored as a value argument is detached (deep-copied).
        fresh_producer: ``True`` if the function always returns a **new**
            object whose elements cannot alias the caller's data (plaita#69).
            A declared producer is treated as ``DEEP`` by the gate; the
            parser's built-in classification (deep / shallow / live-reference)
            covers the default registry.  The default is ``False`` so a
            forgotten declaration **blocks** the call instead of silently
            allowing a write-through.
        description: Optional human-readable description.
    """

    name: str
    func: Callable
    category: FunctionCategory
    has_side_effects: bool = False
    fresh_producer: bool = False
    description: str = ""


# ---------------------------------------------------------------------------
# ExpressionRegistry
# ---------------------------------------------------------------------------

class ExpressionRegistry:
    """Registry for expression functions with category and side-effect metadata."""

    def __init__(self) -> None:
        self._functions: dict[str, FunctionDescriptor] = {}

    def register(
        self,
        name: str,
        func: Callable,
        category: FunctionCategory,
        *,
        has_side_effects: bool = False,
        fresh_producer: bool = False,
        description: str = "",
        override: bool = False,
    ) -> None:
        """Register an expression function.

        Args:
            name: Function name (used in expressions as ``$F.name(...)``).
            func: The callable to execute.
            category: Classification category.
            has_side_effects: ``True`` if the function mutates its input.
            fresh_producer: ``True`` if the function always returns a new
                object **whose elements cannot alias** the caller's data
                (plaita#69) — i.e. a *deep* producer, not merely a new
                container.  Defaults to ``False``: an undeclared producer used
                as any argument of a mutator is refused (fail-closed).
            description: Human-readable description.
            override: If ``True``, overwrite an existing registration with the
                same name instead of raising ``ValueError``.  Use this for
                intentional re-registration (e.g. backward-compatible
                ``register_function`` / dict-style assignment) rather than
                reaching into ``_functions`` from outside the class.

        Raises:
            ValueError: If *name* is empty, or already registered with
                ``override=False``.
        """
        if not name:
            raise ValueError("Function name must not be empty")
        if name in self._functions and not override:
            raise ValueError(f"Function '{name}' is already registered")
        self._functions[name] = FunctionDescriptor(
            name=name,
            func=func,
            category=category,
            has_side_effects=has_side_effects,
            fresh_producer=fresh_producer,
            description=description,
        )

    def get(self, name: str) -> Optional[FunctionDescriptor]:
        """Look up a function descriptor by name."""
        return self._functions.get(name)

    def unregister(self, name: str) -> None:
        """Remove a function registration. No-op if *name* is not registered."""
        self._functions.pop(name, None)

    def get_callable(self, name: str) -> Optional[Callable]:
        """Look up just the callable by name (for backward compatibility)."""
        desc = self._functions.get(name)
        return desc.func if desc else None

    def names(self) -> list:
        """已注册的函数名清单（供报错提示/补全）。"""
        return list(self._functions.keys())

    def by_category(self, category: FunctionCategory) -> List[FunctionDescriptor]:
        """Return all functions in *category*."""
        return [d for d in self._functions.values() if d.category == category]

    def side_effect_functions(self) -> List[FunctionDescriptor]:
        """Return all functions marked with side effects."""
        return [d for d in self._functions.values() if d.has_side_effects]

    def all_functions(self) -> dict[str, FunctionDescriptor]:
        """Return the full registry as a read-only snapshot."""
        return dict(self._functions)

    def __contains__(self, name: str) -> bool:
        return name in self._functions

    def __len__(self) -> int:
        return len(self._functions)

    def __repr__(self) -> str:
        return f"<ExpressionRegistry functions={len(self._functions)}>"


# ---------------------------------------------------------------------------
# Module-level function definitions (must be picklable for multiprocessing)
# ---------------------------------------------------------------------------

# 第十五轮 owner K1：上限提到"实际流量不可达"量级（≥10^6），并且只作为**发现
# 放大**的有界预检——真正的放大判定交给渲染闸（`_check_result_size`）。
_POW_MAX_EXPONENT = 1_000_000

# 结果规模守卫（2026-10 安全评审 P1；issue #68 独立验收返工）。
#
# 目标不是「限制绝对大小」，而是**识别放大**：危险形态是「小输入 → 巨大输出」，
# 而输入本来就大、输出与输入同量级的拼接是合法流程（把 200 段 1KB 文本拼成
# prompt）。因此判定阈值随输入规模走：
#
#     allowed = min( max(_EXPR_MAX_RESULT_SIZE, _EXPR_GROWTH_FACTOR * input_size),
#                    _EXPR_MAX_RESULT_HARD )
#
#   * floor：小输入时的绝对下限（避免琐碎拒绝）；
#   * factor：允许的单次放大倍数（4 = 输出最多是输入 4 倍）；
#   * hard：单次结果硬上限（输入本来就巨大时，线性拷贝也要有天花板）。
#
# 量级示例（默认 floor=100_000 / factor=4 / hard=100_000_000）：
#   * ``"x" * 10**6``：input=1 → allowed=100_000 ⇒ 拒绝（放大 10^6 倍）；
#   * ``join(200 段 × 1KB)``：input≈204_800 → allowed≈819_200 ⇒ 通过；
#   * ``join(['b'], 's'*100_001)``：n=1 ⇒ 分隔符不进结果 ⇒ 通过；
#   * ``extend([0]*10, [0]*100_000_000)``：projected 超过 hard ⇒ 拒绝。
#
# 三个旋钮都可由环境变量覆盖（沿用本仓 PLAITA_* 运维旋钮的形状；非法/非正值
# 静默回退默认，与 `plaita/node/http.py` 的响应体上限一致）。模块导入时读取
# 一次，改后需重启进程。
# 逐算子结论（"能否从极小输入放大出巨大输出"；2026-10 #68 独立验收要求）：
#
#   无界（已加**分配前**预检）
#     mul      序列重复：str/bytes/bytearray/list/tuple × int（双向）与 int × 序列
#     replace  str/bytes/bytearray 替换：命中数 × 替换串长度（空模式最狠）
#     mod      str/bytes/bytearray 的 % 是 printf 格式化：width/precision
#              （含 %*d 来自参数的宽度）能一步撑到 GB 级
#     join     分隔符被 (元素数-1) 次重复；str/bytes 自身作可迭代对象同理
#   线性（预检只在结果超过 hard 时触发；结果 ≈ 输入）
#     add extend append concat addListItem insertListItem insert remove
#     slice reverse sort   —— 新对象 ≤ 输入 + 1
#   有界（无需预检）
#     upper/lower/capitalize/title  Unicode 大小写映射 ≤ 3× 输入（< factor）
#     split                        元素数 ≤ 输入字符数 + 1（元素计量无线放大；
#                                  对象头约 9× 开销未建模，见文档「残余」）
#     json_dumps/json_loads        线性于结构规模 / 十进制位数（int 位宽）
#     keys/values/items            返回 view（O(1) 分配）
#     sub/div/abs/ceil/floor/round/trunc/sqrt、not、比较类  缩小或 O(1)
#     and/or/ifelse                返回既有对象，不分配
#     now/today/len/index/getListItem/startswith/endswith/isDigit/in/contains  有界
#   另一类（CPU 而非内存）
#     pow（指数上限）、数值 mul（大整数按需分配）—— 数值 mul 的结果 ≈ 操作数
#     位宽之和，线性，故只受 hard 约束
#
# 探针（expression_parser._size_guard_result）是**网**不是闸：它兜住未枚举/
# 自定义函数的返回值，但对未预检函数而言值已经分配出来了——文档必须如实描述。
EXPR_MAX_RESULT_SIZE_ENV = "PLAITA_EXPR_MAX_RESULT_SIZE"
EXPR_GROWTH_FACTOR_ENV = "PLAITA_EXPR_GROWTH_FACTOR"
EXPR_MAX_RESULT_HARD_ENV = "PLAITA_EXPR_MAX_RESULT_HARD"

_DEFAULT_MAX_RESULT_SIZE = 100_000
_DEFAULT_GROWTH_FACTOR = 4
_DEFAULT_MAX_RESULT_HARD = 100_000_000


# 第十二轮 O4：写进日志/错误消息的**作者可控字符串**（类名、异常消息、值名）一律
# 截断到固定长度——`type(x).__name__` 可以被作者设成 3MB（实测 WARNING 3,000,051
# 字节 / 峰值 5.7MB）。截断是渲染量问题，属于本单的规模面。
_LOG_TEXT_LIMIT = 64


def _clip(text: str, limit: int = _LOG_TEXT_LIMIT) -> str:
    """把作者可控字符串截断到 ``limit`` 字符（短字符串逐字不变）。

    只接受 ``str``：调用方传的都是类型名/异常名/已解析文本，函数内不做任何
    ``str()`` 转换（那会变成一个新的渲染点）。
    """
    if not isinstance(text, str):             # pragma: no cover - 防御性
        return ""
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 0)] + "..."


def _type_name(value: Any) -> str:
    """值的类型名（截断）；不触发任何作者代码里的 ``__str__``。"""
    return _clip(_class_name(type(value)) or "")


def _exception_name(exc: BaseException) -> str:
    return _clip(getattr(type(exc), "__name__", "") or "")


_LOG_MESSAGE_LIMIT = 200


def _exception_message(exc: BaseException, limit: int = _LOG_MESSAGE_LIMIT) -> str:
    """异常的**有界**文本：只对"单个精确 str 参数"走 C 槽切片，其余只给类型名。

    第十二轮 O4：`str(exc)` 会调用作者覆写的 ``__str__``（A7 实测 5MB 消息 ⇒
    10,000,111 字节日志 / 14.3MB 峰值）。这里用 ``BaseException.args`` 的 C 槽读
    真实参数；参数是 ``str`` 时按 ``str.__getitem__`` 切片（不整段渲染），否则只
    回类型名——日志量因此有上界。
    """
    try:
        args = BaseException.args.__get__(exc, type(exc))
    except (AttributeError, TypeError):       # 非普通异常对象
        return _exception_name(exc)
    if isinstance(args, tuple) and len(args) == 1 and isinstance(args[0], str):
        text = str.__getitem__(args[0], slice(0, limit))
        return _clip(text, limit)
    return _exception_name(exc)


def _resolve_positive_int(env_name: str, default: int) -> int:
    """读正整数环境变量；缺失/非法/非正一律回退默认值。"""
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("%s=%r 非法，回退默认 %d", env_name, raw, default)
        return default
    if value <= 0:
        logger.warning("%s=%r 非正，回退默认 %d", env_name, raw, default)
        return default
    return value


_EXPR_MAX_RESULT_SIZE = _resolve_positive_int(
    EXPR_MAX_RESULT_SIZE_ENV, _DEFAULT_MAX_RESULT_SIZE)
_EXPR_GROWTH_FACTOR = _resolve_positive_int(
    EXPR_GROWTH_FACTOR_ENV, _DEFAULT_GROWTH_FACTOR)
_EXPR_MAX_RESULT_HARD = _resolve_positive_int(
    EXPR_MAX_RESULT_HARD_ENV, _DEFAULT_MAX_RESULT_HARD)


def _allowed_result_size(input_size: int) -> int:
    """本次允许的结果上界（输入规模 × 放大系数，见上方说明）。"""
    allowed = max(_EXPR_MAX_RESULT_SIZE, _EXPR_GROWTH_FACTOR * input_size)
    return min(allowed, _EXPR_MAX_RESULT_HARD)


def _seq_kind(value: Any) -> Optional[str]:
    """可重复/可拼接容器的类别：str / bytes / bytearray / list / tuple。

    只有这些类型的 ``len`` 与 ``+`` / ``*`` 的结果规模一一对应；其它类型
    （int / float / dict / set / None…）返回 ``None``——它们不参与规模预检，
    类型错误交给 Python 原生抛（错误归因不随规模变化）。
    """
    if isinstance(value, str):
        return "str"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, bytearray):
        return "bytearray"
    if isinstance(value, list):
        return "list"
    if isinstance(value, tuple):
        return "tuple"
    return None


def _kinds_compatible(k1: Optional[str], k2: Optional[str]) -> bool:
    """Python 允许的拼接组合：同种，外加 bytes/bytearray 互相混用。

    ``str+str`` / ``list+list`` / ``tuple+tuple`` / ``bytes+bytearray`` 合法；
    ``list+tuple``、``bytes+str`` 等是原生 TypeError——预检必须据此让路，否则
    同一个表达式会因为规模不同得到 ValueError/TypeError 两种归因。
    """
    if k1 is None or k2 is None:
        return False
    if k1 == k2:
        return True
    return k1 in ("bytes", "bytearray") and k2 in ("bytes", "bytearray")


def _unit_for(value: Any) -> str:
    """结果计量单位：bytes 容器计字节、str 计字符、其余可数容器计元素。"""
    if isinstance(value, str):
        return "chars"
    if isinstance(value, (bytes, bytearray)):
        return "bytes"
    return "elements"


def _measure(value: Any) -> int:
    """O(1) 规模读取：``len()`` 优先，无长度/长度不可用的对象回退 getsizeof。

    第十一轮 H2：**任何**用户异常（`__len__`/`__sizeof__` 抛 `RuntimeError` 等）
    都必须转成"不可界定 ⇒ ValueError"，不得从守卫里漏出去。
    """
    try:
        return len(value)
    except (TypeError, OverflowError):
        pass                              # 正常回退：无 __len__ ⇒ 用 getsizeof
    except ValueError:
        raise
    except Exception as exc:                  # noqa: BLE001 - 敌意 __len__ 不得漏出
        _render_unmeasurable(exc)
        raise _size_refusal(f"$F.* unmeasurable value ({_type_name(value)})",
                            0, "bytes", 0) from exc
    try:
        return sys.getsizeof(value)
    except ValueError:
        raise
    except Exception as exc:                  # noqa: BLE001 - 敌意 __sizeof__ 不得漏出
        _render_unmeasurable(exc)
        raise _size_refusal(f"$F.* unmeasurable value ({_type_name(value)})",
                            0, "bytes", 0) from exc


def _safe_int_text(value: int) -> str:
    """把 ``int``（含子类）安全地转成**有界**文本。

    ``f"{n}"`` 会调用 ``type(n).__str__``——int 子类可以覆写它并返回任意大的串
    （第十二轮 H3 实测：``_fn_mul``/``_fn_pow`` 的错误消息被敌意 ``__str__`` 撑到
    20,000,098 字符、tracemalloc 峰值 40MB，而拒绝恰恰是为了"不分配"）。这里用
    ``int.__repr__``（C 槽，忽略覆写）读**真实**十进制，且只在位数有界时展开；
    超过 64bit 只报位数（不做十进制展开）。
    """
    bits = int.bit_length(value)
    if bits <= 64:
        return int.__repr__(value)
    return f"<int: {bits} bits>"


def _measured_unit(value: Any) -> str:
    """``_measure`` 实际使用的计量单位（无 ``len`` 的对象按字节）。"""
    if isinstance(value, str):
        return "chars"
    if isinstance(value, (bytes, bytearray)):
        return "bytes"
    try:
        len(value)
    except Exception as exc:                  # noqa: BLE001 - H2：任何异常都当无长度
        _render_unmeasurable(exc)
        return "bytes"
    return "elements"


def _value_size(value: Any) -> int:
    """``str(value)`` 会产生的规模上界（且不真的做转换）。

    int 用 ``bit_length() // 3`` 估十进制位数（1 位十进制 ≥ 3.32 bit，除以 3
    是上界），避免为了估算而把 ``10**1000000`` 的十进制展开出来；小整数
    （≤64bit）直接 ``len(str(v))`` 拿到**精确**位数，避免提示文案比真实结果大。
    """
    cls = type(value)
    if cls is str:
        return len(value)
    if isinstance(value, str):
        return str.__len__(value)
    if cls is bytes or cls is bytearray:
        return len(value)
    if isinstance(value, (bytes, bytearray)):
        return (bytes if isinstance(value, bytes) else bytearray).__len__(value)
    if isinstance(value, int):
        # Z4.3：int 子类的 `__int__` 是作者代码 ⇒ 走 `int.__index__` C 槽读真实值。
        value = int.__index__(value)
        if value.bit_length() <= 64:
            return len(str(value))
        return max(sys.getsizeof(value), value.bit_length() // 3 + 1)
    return _measure(value)


# 第十五轮 owner K1：数据闸/返回闸共用的**有界遍历**预算提到 10^6（真实非放大流量
# 不可达）——预算只用于"发现非数据"，纯数据一律通过；耗尽时抛**明确消息**。
_DATA_WALK_MAX_DEPTH = 1_000_000
_CONTENT_MAX_DEPTH = _DATA_WALK_MAX_DEPTH   # 兼容旧名（文档/测试引用）


def _as_memoryview(value: Any) -> Optional[memoryview]:
    """真实缓冲协议 → memoryview；否则 None（**不看** ``itemsize`` 这类自报字段）。"""
    try:
        return memoryview(value)
    except TypeError:
        return None


def _is_one_shot_iterator(value: Any) -> bool:
    """``iter(x) is x``：生成器/``iter(list)``/``map``/自定义迭代器——枚举会消费它。"""
    if isinstance(value, (str, bytes, bytearray, list, tuple, dict, set, frozenset)):
        return False
    # S6：`iter(value) is value` 会跑作者 `__iter__`；只按类型层的 `__next__` 判。
    return _has_raw_slot(type(value), "__next__")


def _looks_like_mapping(value: Any) -> bool:
    """真 ``Mapping`` 或鸭子类型映射（有 ``keys``/``items``/``__getitem__``）。

    ``dict`` 之外的映射（自定义 Mapping、``collections.abc.Mapping`` 子类）此前会
    掉进"未知对象"分支 ⇒ keys/values 全漏（第六轮验收 必修 2）。
    """
    if type(value) is dict:
        return True
    if isinstance(value, Mapping):
        return True
    cls = type(value)
    # S6：类型层判据（读 MRO `__dict__`）——`hasattr(instance, …)` 会触发作者
    # `__getattr__`（炸弹实测 3 次调用，出现在 `$F.not` 路径上）。
    return (_has_raw_slot(cls, "items") and _has_raw_slot(cls, "keys")
            and _has_raw_slot(cls, "__getitem__"))


# X2.2：`_DATA_WALK_MAX_DEPTH` 是**声明**预算（10^6），但实现是递归走查，实际会被
# 解释器递归上限截断（默认 ~1000 帧 ⇒ 约 400-500 层）。走查期间临时抬高到 6000 帧
# （≈2000 层，远超真实流量），走完立刻还原；仍超出的病态嵌套 fail-closed。
_CONTENT_WALK_RECURSION_LIMIT = 6000


def _content_size(value: Any, depth: int = 0, seen: Optional[set] = None) -> int:
    """值能贡献给「结果规模」的**内容量**（守卫与探针共用的"输入规模"）。

    **只信权威协议**（第六轮验收 必修 2）：``str``/``bytes``/``bytearray`` 取长度、
    真实缓冲走 ``memoryview(x).nbytes``、映射/容器**由我们自己枚举**。其它对象
    一律**不信**它自报的 ``itemsize``/``__len__``（说谎缓冲对象会把 allowance
    抬到 2500 万），按 0 计——宁可让事后硬上限兜住，也不放宽额度。

    口径说明：容器/映射按**内容量之和**计，并以**元素数**为下界（已知长度的内建
    容器用元素数豁免"内容量为 0"的退化情形）；``str``/``bytes`` 等按长度计。
    其它规则：``int`` 取十进制位数上界；**单次迭代器按 0 计**（枚举会消费它；
    需要真实内容量的 ``join``/``sort`` 由调用点先物化一次再测量）；容器扫描精确
    到项（只有累计到 ``hard`` 才提前返回）；自引用按 id 路径栈只计一次；嵌套超过
    ``_CONTENT_MAX_DEPTH`` 层保守拒绝；枚举过程中对象自己抛异常 → 同样保守拒绝
    （绝不让用户异常从守卫里漏出去）。
    """
    cls = type(value)
    if cls is str:
        return len(value)
    if cls is int:
        return _value_size(value)
    if cls is bool:
        return 4
    if cls is bytes or cls is bytearray:
        return len(value)
    if cls is float:
        return 8
    if value is None:
        return 4
    previous_limit = sys.getrecursionlimit()
    raised_limit = False
    if previous_limit < _CONTENT_WALK_RECURSION_LIMIT:
        sys.setrecursionlimit(_CONTENT_WALK_RECURSION_LIMIT)
        raised_limit = True
    try:
        return _content_size_inner(value, depth, seen)
    except RecursionError as exc:
        _render_unmeasurable(exc)
        raise ValueError(
            f"$F.* nested value exceeds the interpreter recursion budget: depth "
            f"{depth} (declared budget {_CONTENT_MAX_DEPTH} levels; the walk raises "
            f"the interpreter recursion limit to {_CONTENT_WALK_RECURSION_LIMIT} "
            f"frames while it runs). Refusing to build an unbounded result. "
            f"{_ESCAPE_LIST_HINT}"
        ) from exc
    except ValueError:
        raise
    except Exception as exc:                  # noqa: BLE001 - 用户对象异常不得漏出（F4）
        _render_unmeasurable(exc)
        raise _size_refusal(f"$F.* unmeasurable value ({_type_name(value)})", 0, "chars", 0) from exc
    finally:
        if raised_limit:
            sys.setrecursionlimit(previous_limit)



#: #68×#69 union：数据模型类型的探测序（bool 先于 int）与精确型集合
_DM_TYPES = (bool,) + tuple(t for t in __import__("plaita.core.types",
                            fromlist=["PERSISTABLE_CONTAINER_TYPES"]).PERSISTABLE_CONTAINER_TYPES) + \
    tuple(t for t in __import__("plaita.core.types",
          fromlist=["PERSISTABLE_SCALAR_TYPES"]).PERSISTABLE_SCALAR_TYPES) + (str,)
_DM_EXACT_TYPES = frozenset(_DM_TYPES) | {type(None)}


def _content_size_inner(value: Any, depth: int = 0, seen: Optional[set] = None) -> int:
    # plaita#68×#69 rebase union（2026-10-16，规则级裁决）：**边界接管**——
    # 一个对象若 isinstance 某数据模型类型（可能经 ``__class__`` 伪造）却没有
    # 内建祖先槽可走，它的尺寸**不可信也不可测**：在这里测它只能调用宿主协议
    # （#69 r9 明令禁止：``liar.__len__``/``__iter__`` 一次都不许调）或以守卫的
    # 通用消息拒绝——而那会**抢在 #69 的具名拒绝之前**（r9 bytes 子用例实测被
    # "unmeasurable (Liar_bytes)" 吃掉）。裁决：交给边界——归一化或具名拒绝在
    # 最近一次拷贝/落盘点发生；本守卫按"不可测贡献 0"放行。
    # ★残余窗口（点名，不声称有界）：纯透传算子可把该对象原样带出且尺寸不计；
    # 覆盖它的是数据模型边界在下一拷贝/落盘点的具名拒绝。
    if type(value) not in _DM_EXACT_TYPES:
        _real_base = next((c for c in type(value).__mro__ if c in _DM_EXACT_TYPES),
                          None)
        if _real_base is None and any(isinstance(value, _k) for _k in _DM_TYPES):
            return 0
    # **祖先槽**读真实存储：子类覆写的 __len__ 只能骗到额度、骗不到渲染的 C 槽
    # （第十一轮 H1）。
    cls = type(value)
    # T5：精确 UserString/UserList 的迭代器产出**还是包装类型**（长度 1）⇒ 通用容器
    # 分支会无限递归到预算耗尽。走它们的精确 `.data`（C 槽读；不是精确内建 ⇒ 具名拒绝）。
    if cls is _collections.UserString:
        data = object.__getattribute__(value, "data")
        if type(data) is not str:
            raise ValueError(
                f"$F.* cannot bound 'UserString' with non-exact .data; "
                f"{_ESCAPE_LIST_HINT}")
        return str.__len__(data)
    if cls is _collections.UserList:
        data = object.__getattribute__(value, "data")
        if type(data) is not list:
            raise ValueError(
                f"$F.* cannot bound 'UserList' with non-exact .data; "
                f"{_ESCAPE_LIST_HINT}")
        value = data
    elif isinstance(value, (_collections.UserString, _collections.UserList)):
        # T5：子类可以覆写 __iter__ ⇒ 读它的迭代器就是跑作者代码；在迭代前拒绝。
        raise ValueError(
            f"$F.* cannot bound {_type_name(value)} (UserString/UserList 子类; "
            f"迭代会跑作者 __iter__). {_ESCAPE_LIST_HINT}")
    if cls is str or isinstance(value, str):
        return len(value) if cls is str else str.__len__(value)
    if cls is bytes or isinstance(value, bytes):
        return len(value) if cls is bytes else bytes.__len__(value)
    if cls is bytearray or isinstance(value, bytearray):
        return len(value) if cls is bytearray else bytearray.__len__(value)
    if isinstance(value, bool):
        return 4
    if cls is int:
        return _value_size(value)
    if isinstance(value, int):
        return _value_size(int.__index__(value))
    if isinstance(value, float):
        return 8
    if value is None:
        return 4
    mapping = _looks_like_mapping(value)
    container = mapping or isinstance(value, (list, tuple, set, frozenset))
    if not container:
        view = _as_memoryview(value)
        if view is not None:
            return view.nbytes
        if _is_one_shot_iterator(value):
            return 0                          # 不消费；调用点会物化
        if isinstance(value, type) or not _has_raw_slot(type(value), "__iter__"):
            return 0                          # 未知对象：不信任何自报尺寸（类型层判据）
    if depth >= _CONTENT_MAX_DEPTH:
        raise ValueError(
            f"$F.* nested value exceeds the data-verification budget: depth {depth} "
            f"(limit {_CONTENT_MAX_DEPTH} levels). Refusing to build an unbounded "
            f"result. {_ESCAPE_LIST_HINT}"
        )
    if seen is None:
        seen = set()
    if id(value) in seen:
        return 0                              # 自引用：只计一次
    seen.add(id(value))
    total = 0
    walk_base = _builtin_ancestor(type(value))
    try:
        if walk_base is not None and type(value) is not walk_base:
            # 子类：**祖先槽**枚举真实存储（实例覆写的 __iter__/items 只用于骗上界）
            stream = _ancestor_stream(value, walk_base, mapping=mapping)
            count = 0
            for item in stream:
                if mapping:
                    key, sub = item
                    total += _content_size(key, depth + 1, seen) + _content_size(sub, depth + 1, seen)
                else:
                    total += _content_size(item, depth + 1, seen)
                count += 1
                if total >= _EXPR_MAX_RESULT_HARD:
                    break
            total = max(total, _ancestor_len(value, walk_base) if not mapping else count)
        elif mapping:
            count = 0
            for key, item in value.items():
                total += _content_size(key, depth + 1, seen) + _content_size(item, depth + 1, seen)
                count += 1
                if total >= _EXPR_MAX_RESULT_HARD:
                    break
        else:
            count = 0
            for item in value:
                total += _content_size(item, depth + 1, seen)
                count += 1
                if total >= _EXPR_MAX_RESULT_HARD:
                    break
        # 已知长度的内建容器：**元素数**是内容量的下界（空串/零值容器不能因为
        # "内容量为 0" 被误拒——`sort(['']*200000)`、`join(['']*200000,'x')`；
        # 第七轮验收 必修 2）。方向只会放宽，不会收紧。
        total = max(total, count)
    except ValueError:
        raise                                 # 我们自己的保守拒绝
    except RecursionError as exc:             # T4.2：比声明预算更早触及解释器递归上限
        _render_unmeasurable(exc)
        raise ValueError(
            f"$F.* nested value exceeds the interpreter recursion budget: depth "
            f"{depth} (declared budget {_CONTENT_MAX_DEPTH} levels; the walk raises "
            f"the interpreter recursion limit to {_CONTENT_WALK_RECURSION_LIMIT} "
            f"frames while it runs). Refusing to build an unbounded result. "
            f"{_ESCAPE_LIST_HINT}"
        ) from None
    except Exception as exc:                  # noqa: BLE001 - 用户对象的异常不得漏出
        raise _size_refusal(f"$F.* unmeasurable value ({_exception_name(exc)})",
                            0, "bytes", 0) from None
    finally:
        seen.discard(id(value))
    return total


def _measure_input(args: Tuple[Any, ...]) -> int:
    """**调用前**算好的输入规模（本帧局部量，调用后不再回读实参）。

    第六轮验收 必修 1：被调用者改写自己的可变实参（``lst.clear(); lst.extend([...])``）
    不能抬高额度——因为额度在调用**之前**就算完了，而且作为值传给守卫，
    不存在任何可写的共享槽位。
    """
    total = 0
    for arg in args:
        total += _content_size(arg)
    return total


def _materialize_one_shot(value: Any) -> Any:
    """单次迭代器 → list（物化一次）；其它值原样返回。

    由**调用点**对"会消费可迭代对象的算子名"调用，这样包装/偏应用/绑定方法形态
    也能拿到物化后的实参：判定、输入规模与真正的执行共用同一份数据，迭代器只被
    拉取一遍（第六轮验收 必修 3）。
    """
    if _is_one_shot_iterator(value):
        return list(value)
    return value


def _materialize_for_consuming(value: Any) -> Any:
    """会**消费**可迭代对象的算子：把不可信任的实参**物化一次**再交出去。

    第九轮 D5：守卫（测量）与执行（join/sorted）各迭代一遍，**有状态可迭代对象**
    就能在第二遍"长出来"（实测：第一遍 1000 项、第二遍 40 万项 ⇒ 结果 4000 万字符、
    峰值 43MB）。精确内建可重复容器原样返回；其余（生成器/`zip`/自定义可迭代）
    物化成 list，使**判定、输入规模与执行共用同一次迭代**。
    """
    if type(value) in _REUSABLE_ITERABLE_TYPES:
        return value
    # **只迭代一次**（不能先 _is_one_shot_iterator 探测：那会调用一次 __iter__，
    # 有状态对象第二遍就"长出来"）；物化本身带指针字节预算。
    materialized: list = []
    limit_items = _EXPR_MAX_RESULT_HARD // _JOIN_POINTER_BYTES
    try:
        for item in value:
            materialized.append(item)
            if len(materialized) > limit_items:
                raise _size_refusal(f"$F materialize({_type_name(value)})",
                                    len(materialized) * _JOIN_POINTER_BYTES, "bytes",
                                    _EXPR_MAX_RESULT_HARD)
    except ValueError:
        raise
    except Exception as exc:                  # noqa: BLE001 - 用户对象异常不得漏出
        _render_unmeasurable(exc)
        raise _size_refusal(f"$F.* unmeasurable iterable ({_type_name(value)})",
                            0, "bytes", 0) from exc
    return materialized


# 这些**算子名**在调用点先物化单次迭代器（按名字而不是函数对象，包装形态
# ——functools.partial / lambda / 绑定方法 / 同名覆盖——同样生效）。
ITERABLE_CONSUMING_NAMES = frozenset({"join", "sort"})


def _size_refusal(what: str, observed: int, unit: str, allowed: int,
                  hint: str = "") -> ValueError:
    """构造与 ``_fn_pow`` 同形的拒绝文案：被拒对象 + 实测规模 + 上限 + 理由。

    规模数字走 :func:`_safe_int_text`：拒绝路径不得因为渲染"已算出的巨大规模"
    而把十进制展开成新的分配（小整数文案不变，钉子测试覆盖）。
    """
    return ValueError(
        f"{what} result too large: {_safe_int_text(observed)} {unit} "
        f"(limit {_safe_int_text(allowed)}). "
        "Refusing to build an unbounded result." + hint
    )


#: AU3（第二十七轮）：`%a` 因 ascii 展开被规模闸拒绝时的**阈值说明 + 逃生口**。
_ASCII_GROWTH_HINT = (
    " %a (ascii()) expands each non-ASCII character to \\xNN/\\uNNNN/\\UNNNNNNNN "
    "(up to 10x); the bound is the exact per-character expansion, and the growth budget "
    "is the value's unescaped content size. Use %r to keep printable non-ASCII unescaped, "
    "or raise the growth factor via the PLAITA_EXPR_GROWTH_FACTOR environment variable "
    "(the hard cap still applies)."
)


def _check_result_size(what: str, projected: int, input_size: int, unit: str,
                       hint: str = "") -> None:
    """**分配之前**的规模预检：越界即抛 ``ValueError``（硬错误，不截断）。

"""
    allowed = _allowed_result_size(input_size)
    if projected > allowed:
        raise _size_refusal(what, projected, unit, allowed, hint)


def _is_repeatable(value: Any) -> bool:
    """``value * int`` 合法且结果规模 = ``len(value) * n``。

    用**类型属性**判定（有 ``__mul__`` 且有 ``__len__``）而不是硬编码白名单：
    这样 ``array.array`` 这类未枚举序列也进预检，而 ``set * int`` 这种原生
    TypeError 不会被说成尺寸错误。
    """
    return hasattr(type(value), "__mul__") and hasattr(type(value), "__len__")


def _pair_total(a: Any, b: Any) -> Optional[int]:
    """两个**同类可拼接**容器的结果规模；其它组合返回 ``None``。

    返回 ``None`` 表示"这次 ``+`` 不是同类序列拼接"——数值加法（不放大）或
    ``str + list``、``set + set`` 这类原生 TypeError，预检都不该插手。
    同类序列的**长度一律走内建祖先 C 槽**（第十二轮 M1）：子类谎报 ``__len__``
    骗不到原生 ``+`` 读的真实存储；没有 C 槽可读 ⇒ 预检不可证 ⇒ 拒绝。
    """
    kind = _seq_kind(a)
    if kind is not None and _kinds_compatible(kind, _seq_kind(b)):
        return _trusted_sum(a, b)
    if type(a) is type(b) and hasattr(type(a), "__add__") and hasattr(type(a), "__len__"):
        return _trusted_sum(a, b)
    return None


def _trusted_sum(a: Any, b: Any) -> int:
    """两个值的内建祖先 C 槽长度之和；任一不可证 ⇒ 拒绝。"""
    return _trusted_operand_len(a) + _trusted_operand_len(b)


def _trusted_operand_len(value: Any) -> int:
    length = _trusted_len(value)
    if length is None:
        _raise_unprovable_length(value)
    return length


def _fn_add(a, b):
    """``a + b``：同类序列拼接按**和**预检；其它组合交给 Python 原生语义。

    ``add`` 本身不可放大（结果 ≈ 两个操作数的规模之和），在比值判定下不会
    触发拒绝——预检保留是为了与同族算子共用同一条判定规则。注意 ``dict``
    在 Python 3 里没有 ``+``（``dict + dict`` 是原生 TypeError，合并不走这里），
    预检不介入。
    """
    total = _pair_total(a, b)
    if total is not None:
        _check_result_size(f"$F.add({_trusted_operand_len(a)} + {_trusted_operand_len(b)})",
                           total, total, _unit_for(a))
    return a + b


def _fn_sub(a, b): return a - b
def _fn_div(a, b): return a / b


def _fn_mul(a, b):
    """乘法（数值 / 序列重复）：重复分支按**结果规模**预检。

    **两个方向都覆盖**——``seq * int`` 与 ``int * seq`` 是同一个结果，
    ``$F.mul(1000000000, 'A')``（``@flow`` 源码写 ``1000000000 * "A"``）与
    ``$F.mul('A', 1000000000)`` 等价，只查一个方向等于没查。``bool`` 是
    ``int``，一并按重复次数处理；负/零倍数结果为空，不放大。

    数值分支不做规模预检：Python 大整数按需分配，乘出 10^9 位整数要的是
    CPU 而不是"小输入换大内存"。
    """
    seq = n = None
    if isinstance(a, int) and _is_repeatable(b):
        seq, n = b, a
    elif _is_repeatable(a) and isinstance(b, int):
        seq, n = a, b
    if seq is not None and int.__gt__(n, 0):
        length = _trusted_operand_len(seq)
        _check_result_size(f"$F.mul({length} * {_safe_int_text(n)})",
                           length * n, length, _unit_for(seq))
    return a * b


def _fn_append(a, b):
    """``a + [b]``：仅当 ``a`` 是 list 时预检。

    ``a`` 不是 list 时（如 ``str + [b]``）本来就抛原生 TypeError，预检不介入，
    免得错误归因随规模变化。``bytearray``/``tuple`` 同理交给 Python 抛。
    """
    if isinstance(a, list):
        length = list.__len__(a)              # C 槽：忽略子类覆写的 __len__
        _check_result_size(f"$F.append({length} + 1)", length + 1, length + 1, "elements")
    return a + [b]


def _fn_extend(a, b):
    """``a + b``（两序列拼接）：同类按**和**预检，混合类型交给原生 TypeError。"""
    total = _pair_total(a, b)
    if total is not None:
        _check_result_size(f"$F.extend({_trusted_operand_len(a)} + {_trusted_operand_len(b)})",
                           total, total, _unit_for(a))
    return a + b


def _fmt_text(fmt: Any) -> str:
    """把 str/bytes 格式串统一成 str（bytes 走 latin-1，仅用于扫描）。

    一律返回**精确** str（走祖先 C 槽取真实存储）：子类覆写的 `__str__`/
    `__len__`/`__getitem__` 不得影响格式解析（第十二轮 M2）。
    """
    if isinstance(fmt, bytes):
        return bytes.__getitem__(fmt, slice(None)).decode("latin-1")
    if isinstance(fmt, bytearray):
        return bytes(fmt).decode("latin-1")
    if isinstance(fmt, str):
        return fmt if type(fmt) is str else str.__getitem__(fmt, slice(None))
    return ""


class _FormatSpec(NamedTuple):
    """一个 ``%`` 转换符的结构（用于**原生错误优先**的结构校验与宽度上界）。"""

    key: Optional[str]
    width: int
    star_width: bool
    precision: int          # -1 = 未指定
    star_precision: bool
    conversion: str


_STR_CONVERSIONS = frozenset("diouxXeEfFgGcrsa")
_BYTES_CONVERSIONS = frozenset("diouxXeEfFgGcrsab")
_FORMAT_FLAGS = "-+#0 "


def _parse_format_specs(fmt: str, bytes_mode: bool) -> Optional[List[_FormatSpec]]:
    """解析 ``%`` 格式串；非法转换符/未闭合/缺转换符一律返回 ``None``。

    返回 ``None`` = "交给原生 ``a % b`` 抛错"（保证 TypeError/KeyError/ValueError
    等**原生错误优先于尺寸错误**）。解析覆盖映射键 ``%(k)``、flags、宽度（数字或
    ``*``）、精度（``.N`` / ``.*``）与转换符——第一版的扫描器不认映射键后的宽度
    （``%(key)100000000s``），于是那条路径完全没有预检。
    """
    valid = _BYTES_CONVERSIONS if bytes_mode else _STR_CONVERSIONS
    specs: List[_FormatSpec] = []
    i, n = 0, len(fmt)
    while i < n:
        if fmt[i] != "%":
            i += 1
            continue
        i += 1
        if i < n and fmt[i] == "%":
            i += 1
            continue
        key = None
        if i < n and fmt[i] == "(":
            close = fmt.find(")", i)
            if close < 0:
                return None
            key = fmt[i + 1:close]
            i = close + 1
        while i < n and fmt[i] in _FORMAT_FLAGS:
            i += 1
        width, star_width = 0, False
        if i < n and fmt[i] == "*":
            star_width = True
            i += 1
        else:
            j = i
            while i < n and fmt[i].isdigit():
                i += 1
            if i > j:
                width = int(fmt[j:i])
        precision, star_precision = -1, False
        if i < n and fmt[i] == ".":
            i += 1
            if i < n and fmt[i] == "*":
                star_precision = True
                i += 1
            else:
                j = i
                while i < n and fmt[i].isdigit():
                    i += 1
                precision = int(fmt[j:i]) if i > j else 0
        if i >= n:
            return None
        conversion = fmt[i]
        if conversion not in valid:
            return None
        i += 1
        specs.append(_FormatSpec(key, width, star_width, precision, star_precision, conversion))
    return specs


def _numeric_kind(value: Any) -> Optional[str]:
    """**可证**的数值语义：``'int'``/``'float'``/``'decimal'``；其余 ``None``。

    第十五轮 Q4.2/Q4.3：只用 ``isinstance``（类型层），**绝不** ``hasattr``——那会触发
    作者 ``__getattr__``（实测 1 次调用）。自定义 ``__index__``/``__int__``/``__float__``
    （numpy 标量等）一律按"没有可读祖先 C 槽"处理 ⇒ 调用方拒绝并点名逃生口
    （``int(x)``/``float(x)``）。
    """
    if isinstance(value, bool):
        return "int"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, _decimal_module.Decimal):
        return "decimal"
    return None


import types as _types_module
import array as _array_module
import collections as _collections
import datetime as _datetime_module
import decimal as _decimal_module
import enum as _enum_module
import itertools as _itertools_module
import pathlib as _pathlib_module
import re as _re_module
import uuid as _uuid_module
import zoneinfo as _zoneinfo_module

class _RenderRule(NamedTuple):
    """`_RENDER_DISPATCH` 的一行（**唯一判据表**的行形状）。

    * ``kind``  —— 原生 `%` 在这组转换符上按什么语义读取真值
      （``"int" | "index" | "float" | "char" | "str" | "repr" | "buffer"``）；
    * ``slots`` —— 原生会派发（因而**覆写即不可证**）的槽名（实测，AB5.3 / ★4）；
    * ``bound`` —— ``(value, as_repr) -> 可证上界 | None``（``char`` 行为 ``None``：
      该 kind 的上界是宽度常量，由 `_spec_bound` 直接算）。**归属判定分散在两处、但都只由
      本表驱动**：数值族的槽归属在 `_conversion_accepts` 里按本行的 ``kind``/``slots``
      调 `_numeric_slots_clean`；文本/buffer 族（``s``/``r``/``a``/``b``）的归属在
      ``bound`` 里面（`_slots_owned_by_base` + 纯 Python 祖先的实例属性闸）。
      ``_conversion_accepts`` / `_spec_bound` / `_spec_input_size` / `_render_size`
      对文本族只复用它，不再另判。

    ★4（实测）：**int 及 int 子类**在 `d/i/u/o/x/X/c` 上原生**不派发任何槽**（C 存储
    直读；覆写 `__int__`/`__index__` 被忽略、0 次作者调用）⇒ 这些 kind 对 int 家族不做
    槽检查；`__int__`/`__float__` 只对 **float/Decimal** 家族生效。`s` 的 `slots` 额外含
    `__repr__`：原生派发 `__str__`，而 `object.__str__` 的返回值就是 `repr(self)`
    （list/自定义对象都走这条）⇒ 上界必须同时要求 `__repr__` 未被覆写（`r`/`a` 两行也
    按这个**并集**收紧，见 `_slots_owned_by_base`）。
    """

    kind: str
    slots: tuple
    bound: Callable


#: 数值"真值可读"允许的祖先类型：只有这些（含其**槽未被覆写**的子类）才可证。
_NUMERIC_ANCESTORS = (int, float, _decimal_module.Decimal)


#: AC1：**不可变**类型层读取——`type.__dict__[...]` 是 C getset，`.__get__(cls, type)`
#: 调用时元类数据描述符（`class Meta(type): __mro__ = property(...)`）无法覆写；
#: `cls.__mro__` / `type.__getattribute__` 都会被骗。
_TYPE_MRO_GETTER = type.__dict__["__mro__"]
_TYPE_DICT_GETTER = type.__dict__["__dict__"]
_TYPE_NAME_GETTER = type.__dict__["__name__"]
_TYPE_MODULE_GETTER = type.__dict__["__module__"]
_TYPE_QUALNAME_GETTER = type.__dict__["__qualname__"]
_FUNCTION_QUALNAME_GETTER = _types_module.FunctionType.__dict__["__qualname__"]


def _type_mro(cls: type) -> tuple:
    return _TYPE_MRO_GETTER.__get__(cls, type)


def _type_namespace(cls: type) -> dict:
    return _TYPE_DICT_GETTER.__get__(cls, type)


def _class_name(cls: Any) -> str:
    try:
        return _TYPE_NAME_GETTER.__get__(cls, type) if isinstance(cls, type) else ""
    except (AttributeError, TypeError):
        return ""


def _class_name_bound(cls: Any) -> Optional[int]:
    """``tp_name``（``__module__`` + ``__qualname__``）的 repr 上界（AM1）。

    第二十四轮：类对象的 repr/str、默认 ``object.__repr__``、``type.__repr__`` 都把
    ``__module__``/``__qualname__`` 嵌进输出，而这两个**建成后仍可写**
    （``class C: pass; C.__qualname__ = "E"*10**7`` ⇒ ``repr(C())`` 20,000,025 字符）。
    所以"常量 256"是推理不是证明。这里用**不可变 getsets** 读真实值，按 repr 转义最坏
    10× 计；读不到/类型不对 ⇒ ``None`` ⇒ 调用方拒绝。
    """
    if not isinstance(cls, type):
        return None
    total = 0
    for getter in (_TYPE_MODULE_GETTER, _TYPE_QUALNAME_GETTER):
        try:
            part = getter.__get__(cls, type)
        except (AttributeError, TypeError):
            return None
        if part is None:
            continue
        if type(part) is not str:
            return None
        total += 10 * str.__len__(part)
    return total


def _function_repr_bound(value: Any) -> Optional[int]:
    """Python 函数的 repr：``<function QUALNAME at 0x…>``（AM1：``__qualname__`` 可写）。"""
    try:
        qualname = _FUNCTION_QUALNAME_GETTER.__get__(value, _types_module.FunctionType)
    except (AttributeError, TypeError):
        return None
    if type(qualname) is not str:
        return None
    return 10 * str.__len__(qualname) + 64


#: AU1（第二十七轮）：**真实逐字符转义宽度**用的 `str` 字符类（C 级 `re.subn` 链式
#: 剥离；`str` 的 `translate` 没有 delete 参数）。AX2（第二十八轮）：`bytes` 侧改用
#: `bytes.translate(None, delete)`——正则 `subn` 会为**每个匹配**物化一个 match 对象
#: （bytes 实测 **88×** 输入内存），translate 只产生一份与输入同大的结果串（实测 1.00×）。
_RE_ASCII_ESCAPES = _re_module.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_RE_LATIN1 = _re_module.compile(r"[\x80-\xff]")
_RE_BMP = _re_module.compile(r"[\u0100-\uffff]")
_RE_ASTRAL = _re_module.compile(r"[\U00010000-\U0010ffff]")
_BYTES_DELETE_NONPRINT = bytes(
    b for b in range(256) if not 0x20 <= b <= 0x7E)

#: `%a`（`ascii()`）下每个字符的真实展开宽度：1 = 可打印 ASCII（`'`/`\` 除外）、
#: 2 = `'`/`\`/`\t`/`\n`/`\r`、4 = 其它控制字符与 0x80–0xff、6 = 0x100–0xffff、
#: 10 = ≥0x10000；两端引号固定 +2。`%r` 用同一个和（对可打印非 ASCII 保守），因此二者
#: **同源**：`"\n"*9999` ⇒ 20,000（不是扁平 10× 的 100,010）。
def _str_escape_width(value: Any) -> int:
    """精确的 ``ascii(value)`` 长度（对 ``%r`` 是保守上界）：C 级计数，不构造展开串。"""
    length = str.__len__(value)
    total = length + 2                                  # 每字符 1 + 两端引号
    total += str.count(value, "\\")
    # repr 的引号选择：串里同时有 `"` 时用 `'` 定界 ⇒ `'` 被转义（+1）；否则用 `"` 定界，
    # `'` 原样保留。这是 `ascii()`/`repr()` 的真实行为，不是最坏情形近似。
    if str.__contains__(value, '"'):
        total += str.count(value, "'")
    total += str.count(value, "\t") + str.count(value, "\n") + str.count(value, "\r")
    # AU1：链式剥离（类互斥）；每步一份与当前串同大的结果，峰值实测 2.1–4.2×（见
    # `TestTwentyEighthRoundBoundComputePeak` 的预算断言与文档）。
    kept, escaped = _RE_ASCII_ESCAPES.subn("", value)
    total += 3 * escaped
    kept, latin = _RE_LATIN1.subn("", kept)
    total += 3 * latin
    kept, bmp = _RE_BMP.subn("", kept)
    total += 5 * bmp
    kept, astral = _RE_ASTRAL.subn("", kept)
    total += 9 * astral
    return total


#: `bytes`/`bytearray` 的 repr 前缀+收尾常量（`b'`+`'` = 3；`bytearray(b'`+`')` = 14）。
_BYTES_REPR_PREFIX = 3
_BYTEARRAY_REPR_PREFIX = 14


def _bytes_repr_width(value: Any, prefix: int, prototype: type,
                      always_escape_apostrophe: bool = False) -> int:
    r"""bytes/bytearray 的 repr 上界：可打印 ASCII 1、其余 `\xNN` 4、`'`/`\` 最坏 2。

    AX1（第二十八轮）：CPython 的 ``bytearray.__repr__`` **无条件**转义 ``'``，而
    ``bytes.__repr__`` 只在串里同时含 ``"`` 时才转义 ``'``（否则换用 ``"`` 定界）。
    两者的引号规则必须分开，否则 bytearray 上界低于实际（且能绕过 1e8 硬上限）。
    AX2：不可打印计数用 ``translate(None, delete)``（1× 临时量），不用 regex `subn`（88×）。
    """
    length = prototype.__len__(value)
    total = length + prefix
    total += prototype.count(value, b"\\")
    if always_escape_apostrophe or prototype.__contains__(value, b'"'):
        total += prototype.count(value, b"'")
    nonprint = length - prototype.__len__(
        value.translate(None, _BYTES_DELETE_NONPRINT))
    short = (prototype.count(value, b"\t") + prototype.count(value, b"\n")
             + prototype.count(value, b"\r"))
    # `\t`/`\n`/`\r` 的真实宽度是 2（只 +1），其余不可打印/非 ASCII 才是 `\xNN`（+3）。
    total += short + 3 * (nonprint - short)
    return total


#: repr **内嵌参数**的精确 C 迭代器（AM1）：`repeat(element)` / `count(start[, step])`。
_ARG_EMBEDDING_ITERATOR_TYPES = frozenset({
    t for t in (getattr(_itertools_module, n, None) for n in ("repeat", "count"))
    if isinstance(t, type)
})


def _arg_embedding_iter_bound(value: Any, prototype: type) -> Optional[int]:
    """`repr(repeat(x))`/`repr(count(a,b))` = ``名(args repr)``：读 C `__reduce__` 的真实参数。"""
    try:
        reduced = prototype.__reduce__(value)          # C 槽，0 作者调用
    except Exception as exc:                  # noqa: BLE001 - 读不到 ⇒ 拒绝
        _render_unmeasurable(exc)
        return None
    if not isinstance(reduced, tuple) or len(reduced) < 2:
        return None
    args = reduced[1]
    if not isinstance(args, tuple):
        return None
    total = 16
    for arg in args:
        part = _render_size(arg, as_repr=True)
        if part is None:
            return None
        total += part + 2
    return total


def _slot_owner(cls: type, name: str) -> Optional[type]:
    """``cls`` 的 MRO 里**第一个**在 ``__dict__`` 定义 ``name`` 的类（不跑作者代码）。"""
    try:
        mro = _type_mro(cls)
    except (AttributeError, TypeError):
        return None
    for base in mro:
        try:
            namespace = _type_namespace(base)
        except (AttributeError, TypeError):
            continue
        if name in namespace:
            return base
    return None


def _numeric_slots_clean(value: Any, slots: Tuple[str, ...]) -> bool:
    """数值子类的相关槽都还是**祖先**的实现 ⇒ 可用 C 槽读真值（否则不可证）。"""
    cls = type(value)
    for base in _NUMERIC_ANCESTORS:
        if isinstance(value, base):
            ancestor = base
            break
    else:
        return False
    if cls is ancestor:
        return True
    for name in slots:
        owner = _slot_owner(cls, name)
        if owner is None:
            continue                           # 祖先本就没有该槽 ⇒ 不是覆写（AC6②）
        if owner is not ancestor:
            return False                       # 覆写 ⇒ fail-closed
    return True


def _is_signaling_nan(value: Any) -> bool:
    """可证的 sNaN（AB4）：只有精确 Decimal 的 ``is_snan`` C 槽。"""
    if type(value) is not _decimal_module.Decimal:
        return False
    try:
        return bool(_decimal_module.Decimal.is_snan(value))
    except (ArithmeticError, TypeError, ValueError):
        return False


def _conversion_accepts(conversion: str, value: Any, bytes_mode: bool) -> bool:
    """转换符 × 值类型的**可证**合法性——由 `_RENDER_DISPATCH` 派生（fail-closed）。

    只放行能用**祖先 C 槽**读出真实值、且原生派发的槽**未被覆写**的形状；
    其余一律 ``False``（调用方据此拒绝，绝不跑作者协议）。

    第二十一轮：文本族（``s``/``r``/``a``）与 bytes 模式 buffer 族（``s``/``b``）**不再
    另设白名单**——合法性就是"表里该行的 ``bound`` 能给出可证上界"（归属判定只此一处）。
    """
    rule = _render_rule(conversion, bytes_mode)
    if rule is None:
        return False                           # 不认识的转换符 / 模式 ⇒ 不可证
    kind = rule.kind
    slots = rule.slots
    if kind in ("str", "repr"):
        # AC2.1：`%s`/`%r`/`%a` 原生会派发 `__str__`/`__repr__`；上界函数自己判槽归属
        # （含 Enum/UUID/pathlib 的专用证明），这里不重复做第二套判断。
        return rule.bound(value, kind == "repr") is not None
    if kind == "buffer":                       # 只有 bytes 模式的 "s"/"b"
        if not bytes_mode:
            return False
        return rule.bound(value, False) is not None
    cls = type(value)
    if kind == "char":
        if isinstance(value, int):
            return True
        if bytes_mode:
            return _is_bytes_like(value) and _trusted_len(value) == 1
        return isinstance(value, str) and _trusted_len(value) == 1
    if kind == "index":
        if isinstance(value, int):
            # ★4（实测）：int 及 int 子类在 o/x/X/c 上原生**不派发任何槽**（C 存储直读，
            # 覆写 __index__/__int__ 无效）⇒ 无槽要求，上界走 `int.__index__` C 槽。
            return True
        return False                           # float/Decimal（即使有 __index__）不可证
    if kind == "int":
        numeric = _numeric_kind(value)
        if numeric == "int":
            # ★4：int 子类原生按 **C 存储**读真值（覆写 __int__ 被忽略，实测 0 次调用）
            # ⇒ 不做槽检查；float/Decimal 才派发 __int__。
            return True
        if numeric in ("float", "decimal"):
            return _numeric_slots_clean(value, slots)
        return False
    if kind == "float":
        numeric = _numeric_kind(value)
        if numeric is None:
            return False
        return _numeric_slots_clean(value, slots)
    return False


# 渲染路径的判据（第八轮验收：方案 A —— **精确类型 + 可证上界**，其余一律拒绝）。
# 第二十一轮把旧的分类集合（`_RENDER_SCALARS`/`_RENDER_CONTAINERS`/`_RENDER_VIEWS`/
# `_RENDER_STDLIB_FIXED`/`_SHORT_REPR_ITERATOR_TYPES`）**合并**成一张类型分类表
# `_PROVEN_RENDER_OWNERS`（`_safe_render_base` 唯一查它）；`_DICT_VIEW_TYPES` /
# `_SHORT_REPR_ITERATOR_TYPES` 是 `_render_type_bound` 里两个内联快路径家族。
_DICT_VIEW_TYPES = frozenset({type({}.keys()), type({}.values()), type({}.items())})
_SHORT_REPR_ITERATOR_TYPES = frozenset({
    type(iter(())), type(iter([])), type(iter("")), type(iter(b"")),
    type(iter(bytearray())), type(iter(set())), type(iter({})),
    type(iter({}.keys())), type(iter({}.values())), type(iter({}.items())),
    type(iter(range(0))), type((x for x in ())), zip, map, filter, enumerate, reversed,
} | {
    # AM1（第二十四轮）：`repeat(x)`/`count(n)` 的 repr **内嵌参数**
    # （`repeat('E'*1e7)` 50 字符起、`count(10**1000)` 1008 字符）⇒ 不能当固定短 repr；
    # 它们从本集合移除后走 fallback：自身定义了 `__repr__` ⇒ 不可证 ⇒ 拒绝。
    obj for name in (
        "cycle", "chain", "islice", "accumulate", "product",
        "permutations", "combinations", "combinations_with_replacement", "zip_longest",
        "starmap", "takewhile", "dropwhile", "filterfalse", "groupby", "tee",
        "compress", "pairwise", "batched",
    ) if isinstance((obj := getattr(_itertools_module, name, None)), type)
})
#: **唯一类型分类表**：我们给出**可证渲染上界**的祖先类型。`object` 在表里
#: （默认 `repr` = `<__module__.qualname object at 0x…>`），由 `_render_type_bound` 以
#: `base is object` 特判：**读真实 `__module__`/`__qualname__`**（AM1，第二十四轮；
#: 二者建成后仍可写，常量 256 是推理不是证明），不再走 `_subclass_render_bound`。
_PROVEN_RENDER_OWNERS = frozenset({
    str, bytes, bytearray, int, float, bool, type(None), object,
    list, tuple, set, frozenset, dict,
}) | _DICT_VIEW_TYPES | _SHORT_REPR_ITERATOR_TYPES | frozenset({
    _datetime_module.datetime, _datetime_module.date, _datetime_module.time,
    _datetime_module.timedelta, _datetime_module.timezone,
    _uuid_module.UUID, _decimal_module.Decimal, _re_module.Pattern,
    _array_module.array, _collections.deque,
    _enum_module.Enum, _pathlib_module.PurePath,
    type(_collections.OrderedDict()), type(_collections.defaultdict()),
})
# 可信任为"可重复迭代"的**精确**内建类型：守卫测量与执行各迭代一次也不会不一致。
_REUSABLE_ITERABLE_TYPES = frozenset({
    list, tuple, str, bytes, bytearray, dict, set, frozenset, range, memoryview,
    type({}.keys()), type({}.values()), type({}.items()),
    _array_module.array, _collections.deque,
})


_RENDER_ESCAPE_HINT = (
    "可用 str(x) 预先转换，或改成只含内建类型（str/int/float/list/tuple/dict…）的容器"
)

# 第十五轮 owner 硬要求 2：拒绝消息必须给**可操作**逃生口（在节点/插件侧显式包装），
# 而不是通用措辞。上游产值的节点/插件负责把值包成精确内建类型。
_ESCAPE_LIST_HINT = (
    "逃生口：在**节点/插件侧**用 list(x) / tuple(x) 之类的**显式**包装"
    "（无内建祖先 C 槽的自定义序列一律拒绝）"
)
_ESCAPE_DICT_HINT = (
    "逃生口：在**节点/插件侧**用 dict(x) **显式**包装"
    "（无内建祖先 C 槽的自定义映射一律拒绝）"
)
_ESCAPE_INT_HINT = (
    "逃生口：在**节点/插件侧**用 int(x) / float(x) 之类的**显式**包装"
    "（自定义 __index__/__int__/__float__ 协议不在判定路径上运行）"
)
_ESCAPE_BYTES_HINT = (
    "逃生口：在**节点/插件侧**用 bytes(x) / bytearray(x) **显式**包装"
)
_ESCAPE_INT_HINT_BY_CONVERSION = {
    "diu": _ESCAPE_INT_HINT, "oxX": _ESCAPE_INT_HINT, "eEfFgG": _ESCAPE_INT_HINT,
    "c": _ESCAPE_INT_HINT,
}


def _numeric_escape_hint(conversion: str, bytes_mode: bool) -> str:
    if conversion == "s" and bytes_mode:
        return _ESCAPE_BYTES_HINT
    return _ESCAPE_INT_HINT_BY_CONVERSION.get(conversion, _ESCAPE_INT_HINT)


def _safe_render_base(cls: type) -> Optional[type]:
    """``cls`` 的 MRO 上第一个"我们给出可证渲染上界"的类型；没有则 ``None``。

    **只看真实类型对象**（精确成员或 MRO 祖先），不做 ``__module__`` 之类字符串判据
    ——字符串可被值自己伪造（第八轮验收 H2 实测 ``__module__ = "itertools"`` 让
    99MB 的 ``__str__`` 通过）。子类只有在表里该转换符的 ``slots`` 与安全祖先
    **是同一个函数对象**时才按祖先语义处理（未覆写 ⇒ 语义不变 ⇒ 可证）。

    第二十一轮：唯一查 **`_PROVEN_RENDER_OWNERS`**（旧的多套分类集合已合并进它）。
    """
    for base in _type_mro(cls):
        if base in _PROVEN_RENDER_OWNERS:
            return base
    return None


def _slots_owned_by_base(cls: type, base: type) -> bool:
    """该值的文本渲染槽是否都由 ``base`` 提供（**唯一文本归属判定**）。

    用不可变的 ``_slot_owner``/``_type_namespace``（AC1）读取真实 MRO：槽属主就是
    ``base`` ⇒ 未覆写；属主是别的类但**原始槽对象与 base 的同一个**（显式别名
    ``__str__ = str.__str__``）⇒ 语义不变；其余 ⇒ 不可证。

    **为什么用 `s` 行的 slots（``__str__`` + ``__repr__``）而不是调用方那一行（AG4 ②）**：
    `s`/`r`/`a` 三个转换符共用同一个类型层上界函数（`_render_type_bound`），而
    `object.__str__` 的返回值就是 `repr(self)`、容器/模板里的元素也走 repr ⇒ 任一被覆写
    都可能换掉真实的渲染语义。取**并集**是刻意 fail-closed 的收紧：它只会比"按实际行
    只查 `__repr__`"更严，不会更松；`r`/`a` 行的 ``slots`` 仍驱动
    `_definitely_rejected`（kind=repr ⇒ 永不判 native）与对抗语料。
    """
    for name in _RENDER_DISPATCH["s"].slots:
        owner = _slot_owner(cls, name)
        if owner is base:
            continue
        if owner is None or _dunder_is_builtin(cls, name, base):
            continue                          # 祖先没有该槽 / 子类只是显式别名
        return False
    return True


def _raw_dunder(cls: type, name: str) -> Any:
    """在 MRO 的 ``__dict__`` 里找 ``name`` 的**原始槽对象**（不触发描述符 ``__get__``）。

    第十轮 F1-b：`cls.__str__` 这种属性查找会调用描述符的 `__get__(None, cls)`，
    敌意描述符可以"类上返回真实现、实例上返回敌意实现"从而骗过同一性比较。
    这里只读原始槽，且要求它必须是**普通函数或 C 槽**（`types.FunctionType` /
    `wrapper_descriptor` / `slot wrapper` 之类），描述符对象一律不认。
    """
    for base in _type_mro(cls):
        raw = _type_namespace(base).get(name)
        if raw is not None:
            return raw
    return None


def _dunder_is_builtin(cls: type, name: str, prototype: type) -> bool:
    """``cls`` 的 ``name`` 原始槽是否与 ``prototype`` 的**是同一个对象**。"""
    left = _raw_dunder(cls, name)
    right = _raw_dunder(prototype, name)
    if left is None or right is None:
        return False
    if not (isinstance(left, _types_module.FunctionType) or type(left) is type(right)):
        return False                          # 描述符对象/自定义可调用 ⇒ 不认
    return left is right


def _iter_values(value: Any):
    """容器元素的**祖先槽**枚举入口（默认按实例协议；子类请用祖先槽）。"""
    return value


def _iter_dict_items(value: Any):
    for key, item in value.items():
        yield key, item


def _render_unmeasurable(exc: Exception) -> None:
    """记录"渲染规模无法界定"的原因（`test_no_silent_swallow` 要求 raise 或 log）。

    第十二轮 M4：**日志不得插值作者数据**——`exc` 的消息里可能内嵌作者对象，
    ``%s`` 会调用它的 ``__str__``（实测 5MB 字符串 / 10.7MB 峰值）。只记**类型名**。
    """
    logger.warning("$F render size unmeasurable (%s); refusing to render",
                   _exception_name(exc))


# 有 C 槽可读真实存储的内建容器/标量祖先。计费与枚举**一律走这些槽**，
# 绝不调用实例覆写的 `__iter__`/`__len__`（第十一轮 H1：覆写 `__iter__` 只能骗
# 到上界、骗不到 `str()`/`repr()` 的 C 槽 ⇒ 曾按 16 字节放行 100,004,000 字符）。
_ANCESTOR_WALK_BASES = (dict, list, tuple, set, frozenset)


def _builtin_ancestor(cls: type) -> Optional[type]:
    for base in _type_mro(cls):
        if base in _ANCESTOR_WALK_BASES:
            return base
    return None


def _ancestor_len(value: Any, base: type) -> int:
    """``base.__len__(value)``：C 槽，忽略实例覆写的 ``__len__``。"""
    return base.__len__(value)


def _ancestor_stream(value: Any, base: type, mapping: bool = False):
    """祖先槽枚举；``dict`` 用 ``dict.items`` / ``dict.__iter__``。"""
    if base is dict:
        return dict.items(value) if mapping else dict.__iter__(value)
    return base.__iter__(value)


# 第十二轮 M1：**凡是为某个值计费/取长度/取内容的路径**（含所有算子的前置检查与
# 预留）都必须走内建祖先的 C 槽。说谎的 `__len__`/`count`/`join` 只能骗到额度，
# 骗不到原生算子读的**真实 C 存储**：`_fn_mul(LyingLenStr("a"*1000), 50000)` 曾按
# `1*50000=50000 ≤ 允许` 放行，而 `str.__mul__` 造出 50,000,000 字符。
_ANCESTOR_LEN_BASES = (
    str, bytes, bytearray, list, tuple, dict, set, frozenset,
    range, memoryview, _array_module.array, _collections.deque,
)


def _trusted_len(value: Any) -> Optional[int]:
    """第一个内建祖先的 C 槽 ``__len__``；无内建祖先 ⇒ ``None``（不可证 ⇒ 拒）。

    第十二轮 O6：精确 ``collections.UserList`` / ``UserString`` 有精确的
    ``.data``（list/str），走其祖先 C 槽读取——stdlib 包装类型不再被误拒；
    ``.data`` 不是精确内建类型、或子类 ⇒ 仍然拒。
    """
    for base in _type_mro(type(value)):
        if base in _ANCESTOR_LEN_BASES:
            return base.__len__(value)
    exact = type(value)
    if exact is _collections.UserList or exact is _collections.UserString:
        try:
            data = object.__getattribute__(value, "data")
        except AttributeError:                # 精确 UserList/UserString 一定有 .data
            return None
        if exact is _collections.UserList and type(data) is list:
            return list.__len__(data)
        if exact is _collections.UserString and type(data) is str:
            return str.__len__(data)
    return None


def _raise_unprovable_length(value: Any) -> None:
    """没有可读真实存储的内建祖先 C 槽 ⇒ 预检不可证，按"拒绝"而不是"按实例协议猜"。"""
    raise ValueError(
        f"$F.* cannot bound the size of {_type_name(value)!r} "
        f"(没有可读祖先 C 槽; 实例 __len__ 不可信). "
        f"Refusing to compute an unbounded result. {_ESCAPE_LIST_HINT}"
    )


def _bytes_like_len(value: Any) -> Optional[int]:
    """bytes-like 的**字节**长度：内建祖先走 C 槽，其余走真实 buffer 协议。"""
    if isinstance(value, bytes):
        return bytes.__len__(value)
    if isinstance(value, bytearray):
        return bytearray.__len__(value)
    if isinstance(value, str):
        return str.__len__(value)
    try:
        return memoryview(value).nbytes
    except TypeError:
        return None


def _buffer_byte_len(value: Any) -> Optional[int]:
    """**真 buffer 协议**的字节长度；``str`` 不算（原生 bytes `%s`/`%b` 会拒 str）。

    `_bytes_like_len` 对 ``str`` 返回字符数（join 路径要用），但 bytes `%` 的
    buffer 语义下 ``str`` 不是 bytes-like（``b"%b" % "x"`` 是 TypeError），
    这里必须区分：``str`` 子类若带作者 ``__bytes__`` 更会走作者代码。
    """
    if isinstance(value, bytes):
        return bytes.__len__(value)
    if isinstance(value, bytearray):
        return bytearray.__len__(value)
    if not _is_bytes_like(value):
        return None
    try:
        return memoryview(value).nbytes
    except TypeError:
        return None


#: **纯 Python 渲染祖先**的子类闸（第二十二轮 AG1/AG2）。
#: `PurePath`/`SimpleNamespace`/`UUID` 的 `__str__`/`__repr__` 是 Python 代码，会读**实例
#: 属性**；作者只要在子类类体里放一个同名 property（数据描述符）就能把祖先的"安全投影"
#: 换掉（实测 `%s` 上界 67、实际 10,000,000）。因此：从 `cls` 往下直到已知 stdlib 祖先之前，
#: **任何一层类命名空间只允许解释器自动生成的名字**；做到可证才给上界，否则 `None` ⇒ 拒绝。
_STDLIB_CLASS_NS_KEYS = frozenset({
    "__module__", "__qualname__", "__doc__", "__slots__",
    # plaita#68 r30：Python 3.13 起解释器给类命名空间新增的两个自动键
    # （定义行号 / 编译器静态属性分析）——与 __module__/__doc__ 同类，
    # 都是解释器自动生成、非作者机器。
    "__firstlineno__", "__static_attributes__",
})
#: 解释器在**没有 `__slots__`** 的子类上自动补的实例 dict/weakref 描述符：只接受
#: `getset_descriptor`（作者 property/函数一律不算）。
_AUTO_NS_DESCRIPTOR_KEYS = frozenset({"__dict__", "__weakref__"})

_PATH_TRUSTED_TYPES = frozenset({
    _pathlib_module.PurePath, _pathlib_module.PurePosixPath,
    _pathlib_module.PureWindowsPath, _pathlib_module.PosixPath,
    _pathlib_module.WindowsPath,
})
_UUID_TRUSTED_TYPES = frozenset({_uuid_module.UUID})
#: `SimpleNamespace.__dict__` 的 member_descriptor：C 级直读真实实例 dict，子类的
#: `__dict__` property 无法遮蔽它（AG1/AG2）。
_NAMESPACE_DICT_DESCRIPTOR = _types_module.SimpleNamespace.__dict__["__dict__"]


def _stdlib_only_below(cls: type, trusted: frozenset) -> bool:
    """``cls`` 到最近的 ``trusted`` 祖先之间，每层类命名空间是否只含解释器自动生成的名字。"""
    for klass in _type_mro(cls):
        if klass in trusted:
            return True
        if klass is object:
            return False
        for key, raw in _type_namespace(klass).items():
            if key in _STDLIB_CLASS_NS_KEYS:
                continue
            if key in _AUTO_NS_DESCRIPTOR_KEYS \
                    and type(raw) is _types_module.GetSetDescriptorType:
                continue
            return False
    return False


def _metaclass_name_unshadowed(cls: type) -> bool:
    """元类不得用数据描述符遮蔽 `type.__name__`（原生 repr/str 会读 ``cls.__name__``）。"""
    owner = _slot_owner(type(cls), "__name__")
    return owner is None or owner is type


def _path_state_chars(value: Any) -> Optional[int]:
    """`PurePath` 家族**真实实例状态**的字符数上界（只读祖先槽，0 次作者调用）。

    **第一代（3.9–3.12）**：`PurePath.__str__` 返回 ``self._str``，缺失时由
    ``_format_parsed_parts(_drv,_root,_parts)`` 现算；`__repr__` 再内嵌
    ``ClassName(as_posix)``。
    **第二代（3.13+，pathlib 重写为 ``__slots__`` 实现，无 ``__dict__``）**：
    渲染相关槽换名为 ``_str``（缓存 str）/ ``_drv`` / ``_root``（str）/
    ``_raw_paths``（list[str]，构造参数）；``_raw_path`` 是只读 property。

    两代的**所有候选状态真实值都要计入**（作者可以在构造后直接写槽得到巨大
    `_str`/`_drv`/`_raw_paths` —— 3.13 上实测三个槽 setattr 均可膨胀渲染），
    全部经 ``object.__getattribute__`` 读 member_descriptor / slot descriptor
    （C，不跑作者描述符）；任一处类型不符 ⇒ ``None`` ⇒ 拒绝。两代槽都存在时
    与原实现同口径**叠加计入**（上界方向安全）。两代槽都缺席 ⇒ ``None`` ⇒ 拒绝
    （未来版本再换名时 fail-closed，与 3.13 之前的行为一致）。
    """
    total = 0
    seen = False
    for name in ("_str", "_drv", "_root", "_pparts", "_cached_cparts"):
        try:
            item = object.__getattribute__(value, name)
        except AttributeError:
            continue
        if type(item) is str:
            total += str.__len__(item)
        elif type(item) in (tuple, list):
            for part in item:
                if type(part) is not str:
                    return None
                total += str.__len__(part)
            total += 1
        else:
            return None
        seen = True
    try:
        parts = object.__getattribute__(value, "_parts")
    except AttributeError:
        parts = None
    if parts is not None:
        if type(parts) is not list:
            return None
        for part in parts:
            if type(part) is not str:
                return None
            total += str.__len__(part)
        total += 1
        seen = True
    if seen:
        return total
    # ── 第二代（3.13+，pathlib 重写为 __slots__ 实现）──
    # 渲染相关槽：_str（str 缓存）/ _drv / _root（str）/ _raw_paths（list[str]）。
    # _raw_path 是只读 property（setattr 即 AttributeError，无篡改面）。
    # 槽未赋值时 object.__getattribute__ 抛 AttributeError ⇒ 跳过；显式 None 亦跳过。
    for name in ("_str", "_drv", "_root"):
        try:
            item = object.__getattribute__(value, name)
        except AttributeError:
            continue
        if item is None:
            continue
        if type(item) is not str:
            return None
        total += str.__len__(item)
        seen = True
    try:
        raw_paths = object.__getattribute__(value, "_raw_paths")
    except AttributeError:
        raw_paths = None
    if raw_paths is not None:
        if type(raw_paths) is not list:
            return None
        for part in raw_paths:
            if type(part) is not str:
                return None
            # join 每段至多补一个分隔符（段内分隔符已在 len 里）——每段 +1 计入
            total += str.__len__(part) + 1
        seen = True
    return total if seen else None


#: `PurePath` 渲染用**普通属性查找**读的协议名（`self._format_parsed_parts` / `self.as_posix`）：
#: 它们不是数据描述符，**实例 dict 可以遮蔽**（实测 `p.as_posix = lambda: "y"*10**7` ⇒
#: `repr(p)` 10,000,009 字符、上界 81）⇒ 实例 dict 里出现这些名字就 fail-closed。
_PATH_INSTANCE_GUARDED_NAMES = ("_format_parsed_parts", "as_posix", "_from_parts", "_flavour")


def _path_instance_state_clean(value: Any) -> bool:
    """路径实例的 ``__dict__`` 里不得有会遮蔽祖先协议方法的名字（0 作者调用读取）。"""
    try:
        namespace = object.__getattribute__(value, "__dict__")
    except AttributeError:
        return True                           # `__slots__` 子类没有实例 dict
    if type(namespace) is not dict:
        return False
    return not any(name in namespace for name in _PATH_INSTANCE_GUARDED_NAMES)


def _path_render_bound(value: Any, as_repr: bool) -> Optional[int]:
    """``PurePath`` 子类的可证渲染上界（第二十二轮 AG1：读实例状态，而非 `parts` 投影）。"""
    cls = type(value)
    if not _stdlib_only_below(cls, _PATH_TRUSTED_TYPES) \
            or not _metaclass_name_unshadowed(cls) \
            or not _path_instance_state_clean(value):
        return None
    state = _path_state_chars(value)
    if state is None:
        return None
    # repr 还要内嵌 `ClassName('…')`：路径字符与**类名**都要按 `%r`/`%a` 的转义最坏
    # 10×计（`ascii()` 会把类名里的非 ASCII 字符展开成 \xNN/\uNNNN/\UNNNNNNNN）。
    return 10 * state + 10 * len(_class_name(cls)) + 16


def _path_bytes_bound(value: Any) -> Optional[int]:
    """bytes 模式下 ``PurePath`` 家族输出的**字节数**上界（fsencode 的 UTF-8 ≤4 字节/字符）。"""
    cls = type(value)
    if not _stdlib_only_below(cls, _PATH_TRUSTED_TYPES) \
            or not _metaclass_name_unshadowed(cls) \
            or not _path_instance_state_clean(value):
        return None
    state = _path_state_chars(value)
    if state is None:
        return None
    return 4 * state + 16


def _uuid_render_bound(value: Any, as_repr: bool) -> Optional[int]:
    """``UUID`` 子类的可证渲染上界（AG1：`UUID.__str__` 读真实 `int` 槽，不是常量 64）。"""
    cls = type(value)
    if not _stdlib_only_below(cls, _UUID_TRUSTED_TYPES) \
            or not _metaclass_name_unshadowed(cls):
        return None
    try:
        raw = object.__getattribute__(value, "int")    # UUID.__slots__ → member_descriptor
    except AttributeError:
        return None
    if type(raw) is not int:
        return None
    hex_len = max(32, (int.bit_length(raw) + 3) // 4)
    total = max(36, hex_len + 4)                       # '%032x' + 4 个 '-'
    if as_repr:
        total += 10 * len(_class_name(cls)) + 4
    return total


#: `array.array` 每元素的**最坏 repr 宽度 + ", " 分隔符**（AP1，第二十五轮，逐 typecode 实测）：
#: `d` = `repr(-sys.float_info.max)` 24 + 2；`f` = 23 + 2；64 位整数
#: `-9223372036854775808` = 20 + 2；`u` 是**单个 quoted string**，按转义 10×/字符另算。
#: 旧实现一律 `32 + len*24` 对 `d` 低估 2/元素（实测 2e6 元素：投影 48,000,042 /
#: 实际 52,000,012 ⇒ 连 1e8 硬上限都被绕过）。
_ARRAY_ITEM_BOUND = {
    "d": 26, "f": 26,
    "b": 6, "B": 6, "h": 8, "H": 8,
    "i": 13, "I": 13, "l": 23, "L": 23, "q": 23, "Q": 23,
}


def _array_render_bound(value: Any, prototype: type) -> Optional[int]:
    """`array.array` 的 repr 上界：按 **typecode** 的最坏每元素宽度（AP1）。

    typecode 走 C getset、长度走 `array.__len__`（0 作者调用）；未知 typecode ⇒ ``None``。
    """
    try:
        typecode = _array_module.array.typecode.__get__(value, prototype)
        count = _array_module.array.__len__(value)
    except (AttributeError, TypeError):
        return None
    if type(typecode) is not str or type(count) is not int:
        return None
    if typecode == "u":                        # repr 是单个 quoted string：转义最坏 10×/字符
        return 32 + 10 * count
    per_item = _ARRAY_ITEM_BOUND.get(typecode)
    if per_item is None:
        return None
    return 32 + count * per_item


def _bytes_mode_bound(value: Any, as_repr: bool = False) -> Optional[int]:
    """表里 ``b`` 行的 ``bound``：bytes 模式 ``%s``/``%b`` 输出的**字节数**上界。

    只放行 C buffer（``_buffer_byte_len``）或 `_path_bytes_bound` 证明的 ``PurePath``；
    作者 ``__bytes__`` 一律 ``None`` ⇒ 调用方拒绝（0 次作者调用）。
    """
    size = _buffer_byte_len(value)
    if size is not None:
        return size
    return _path_bytes_bound(value)


def _item_render_len(item: Any, sep_kind: str) -> int:
    """join 元素的内容长度：str 元素走 ``str.__len__``，bytes-like 走 buffer 字节数。"""
    length = _trusted_len(item) if sep_kind == "str" else _bytes_like_len(item)
    if length is None:
        _raise_unprovable_length(item)
    return length


def _is_plain_namespace(value: Any) -> bool:
    """``SimpleNamespace``（精确类型 or ``__repr__`` 为同一个原始槽）。"""
    return (type(value) is _types_module.SimpleNamespace
            or (isinstance(value, _types_module.SimpleNamespace)
                and _dunder_is_builtin(type(value), "__repr__", _types_module.SimpleNamespace)))


def _render_namespace_bound(value: Any) -> Optional[int]:
    """``namespace(a=1)`` 的渲染上界：逐项按 repr 语义（AG1/AG2 加固）。

    原生 repr 由 C 直接读**真实实例 dict**；`vars()`/`value.__dict__` 会被子类的
    ``__dict__`` property 换掉（实测上界 51、实际 10,000,010，且决策路径 3 次作者调用）。
    这里改读 **`SimpleNamespace.__dict__` 上的 member_descriptor**（C 级，直接取
    ``tp_dictoffset``）：即使子类放了 `__dict__` property 也拿到真实 dict、**0 次作者调用**，
    且与原生 C repr 看到的是同一份数据（实测 native repr 10,000,011 字符 / 0 次调用）。

    key 也必须按 **repr** 语义可证：exact ``str`` 或 ``__repr__`` 归属仍是 ``str`` 的
    子类才按 ``10×len+10`` 计（repr 转义最坏 10×），否则整条 ``None``。
    """
    total = 16 + len(_class_name(type(value))) + 2
    try:
        namespace = _NAMESPACE_DICT_DESCRIPTOR.__get__(value, type(value))
        if type(namespace) is not dict:
            return None
        for key, item in namespace.items():
            if isinstance(key, str):
                if type(key) is not str and _slot_owner(type(key), "__repr__") is not str:
                    return None               # str 子类覆写 repr ⇒ 原生会跑作者代码
                total += 10 * str.__len__(key) + 10
            else:
                part = _render_size(key, as_repr=True)
                if part is None:
                    return None
                total += part
            part = _render_size(item, as_repr=True)
            if part is None:
                return None
            total += part + 4
            if total >= _EXPR_MAX_RESULT_HARD:
                return _EXPR_MAX_RESULT_HARD + 1
    except ValueError:
        raise
    except Exception as exc:                  # noqa: BLE001
        _render_unmeasurable(exc)
        return None
    return total


def _enum_member_name(value: Any) -> str:
    """成员的 ``_name_``：走 ``object.__getattribute__``，绕开覆写的属性访问。"""
    try:
        name = object.__getattribute__(value, "_name_")
    except Exception as exc:                  # noqa: BLE001 - 取不到就按空名计
        _render_unmeasurable(exc)
        return ""
    return name if isinstance(name, str) else ""


def _enum_member_raw_value(value: Any) -> Any:
    """成员的 ``_value_``（原始作者数据，不是 `member.value` 属性）。"""
    return object.__getattribute__(value, "_value_")


def _enum_str_impl(cls: type) -> Optional[str]:
    """enum 成员的 ``str()`` 语义：``enum``/``flag``/``int``/``str``/``None``。

    用 enum 标准原型（``Enum``/``Flag``/``IntEnum``/``IntFlag``/``StrEnum``）的
    **原始槽**做同一性比较；用户覆写 ``__str__`` ⇒ ``None`` ⇒ 不可界定。
    """
    for label, prototype in (("enum", _enum_module.Enum), ("flag", _enum_module.Flag),
                             ("int", _enum_module.IntEnum), ("int_flag", _enum_module.IntFlag),
                             ("str", _enum_module.StrEnum)):
        if _dunder_is_builtin(cls, "__str__", prototype):
            return "int" if label in ("int", "int_flag") else label
    return None


def _enum_member_render_bound(value: Any, as_repr: bool) -> Optional[int]:
    """enum 成员的上界：``str`` 是 ``Class.NAME``/整数值/字符串值；``repr`` 内嵌 value。

    AG1/AG2：类名经不可变 getter 读（`_class_name`），并要求元类没有遮蔽 `type.__name__`
    —— 原生 `Enum.__str__/__repr__` 会读 ``self.__class__.__name__``。
    """
    cls = type(value)
    if not _metaclass_name_unshadowed(cls):
        return None
    # `%r`/`%a` 共用本函数：类名与成员名都按转义最坏 10×计（`ascii()` 展开非 ASCII）。
    name_part = 10 * (len(_class_name(cls)) + len(_enum_member_name(value))) + 8
    if as_repr:
        # 第十二轮 O5：repr 必须由 enum 标准原型提供；只覆写 __repr__ 的子类
        # （上界曾 28 vs 实际 5,000,000）一律拒。
        if not (_dunder_is_builtin(cls, "__repr__", _enum_module.Enum)
                or _dunder_is_builtin(cls, "__repr__", _enum_module.Flag)):
            return None
        # repr = `<Class.NAME: value_repr>`：value 是**作者数据**，必须计入
        value_bound = _render_size(_enum_member_raw_value(value), as_repr=True)
        if value_bound is None:
            return None
        return name_part + value_bound + 8
    impl = _enum_str_impl(cls)
    if impl in ("enum", "flag"):
        return name_part
    if impl == "int":
        return _value_size(int.__index__(value)) + 2
    if impl == "str":
        return _render_size(str.__getitem__(value, slice(None)), as_repr=False) + 2
    return None


def _tzinfo_extra(value: Any, prototype: type) -> Optional[int]:
    """``repr(datetime/time)`` 里 ``tzinfo=<repr>`` 的规模（作者数据，必须计入）。"""
    try:
        tz = prototype.tzinfo.__get__(value, type(value))
    except Exception as exc:                  # noqa: BLE001
        _render_unmeasurable(exc)
        return None
    if tz is None:
        return 0
    if type(tz) is _zoneinfo_module.ZoneInfo:
        # 第十二轮 O6：精确 ZoneInfo 是**合法数据**（一列时间戳的常见形态），
        # 其 repr 由 C 槽给出：`zoneinfo.ZoneInfo(key='<key>')`。
        try:
            key = object.__getattribute__(tz, "key")
        except AttributeError:                # 精确 ZoneInfo 一定有 .key
            return None
        if not isinstance(tz, _zoneinfo_module.ZoneInfo) or not isinstance(key, str):
            return None
        return 32 + 10 * str.__len__(key) + 8      # `%r`/`%a`：key 按转义最坏 10×
    bound = _render_size(tz, as_repr=True)
    return None if bound is None else bound + 8


def _render_type_bound(value: Any, as_repr: bool = False) -> Optional[int]:
    """``s``/``r``/``a`` 三个转换符的**类型层**上界（表里那三行的 ``bound``）。

    ``as_repr=True`` 用于 ``%r``/``%a`` 以及**容器内部元素**（``str(list)`` 用的是
    元素的 ``repr``）：字符串要按转义最坏 4×计（``'\x00'*100`` 实际渲染 404 字符），
    ``bytearray`` 还要加 ``bytearray(b'…')`` 前缀。

    ``None`` = 无法界定 ⇒ 调用方必须在**渲染之前**拒绝。方案 A：精确类型 + 可证
    上界；子类只在表里该转换符的 ``slots`` 与安全祖先**同一**时按祖先语义处理；
    其余（含自定义对象、覆写 repr 的子类）一律 ``None``。
    """
    cls = type(value)
    # --- 精确标量 ---
    if cls is str:
        # repr 最坏情况：每个字符转义成 \U0010ffff（10 字符）+ 两个引号
        if not as_repr:
            return len(value)
        # AU1：真实逐字符转义宽度（不是两分法最坏值）。
        return _str_escape_width(value)
    if cls is bytes:
        return _bytes_repr_width(value, _BYTES_REPR_PREFIX, bytes)
    if cls is bytearray:
        return _bytes_repr_width(value, _BYTEARRAY_REPR_PREFIX, bytearray,
                                 always_escape_apostrophe=True)

    if cls is bool:
        return 8
    if cls is int:
        return _value_size(value) + 2
    if value is None:
        return 4
    if cls is float:
        return 32
    # --- 精确容器/视图：逐项（含 2 字符分隔余量），**不用元素数当渲染上界** ---
    if cls in (list, tuple, set, frozenset):
        return _container_render_bound(len(value), _iter_values(value))
    if cls is dict:
        return _container_render_bound(len(value), _iter_dict_items(value), pairs=True)
    if cls in _DICT_VIEW_TYPES:
        return _container_render_bound(len(value), _iter_values(value),
                                       pairs=cls is type({}.items()))
    if cls is range:
        return 48 + _value_size(value.start) + _value_size(value.stop) + _value_size(value.step)
    if cls is memoryview:
        return 64                            # str(memoryview) 是 "<memory at 0x…>"
    if cls is type or (isinstance(value, type) and _dunder_is_builtin(cls, "__repr__", type)):
        # AM1：`type.__repr__`/`type.__str__` 输出 `<class '__module__.qualname'>`，
        # 两个名字**建成后仍可写** ⇒ 必须读真实值（不可变 getters），不能用常量。
        name = _class_name_bound(value)
        return None if name is None else name + 64
    if (isinstance(value, type) and issubclass(value, _enum_module.Enum)
            and _dunder_is_builtin(cls, "__repr__", _enum_module.EnumMeta)):
        # AM1：`EnumMeta.__repr__` = `<enum 'qualname'>` ⇒ 读真实类名，不能用常量。
        name = _class_name_bound(value)
        return None if name is None else name + 32
    if cls in _ARG_EMBEDDING_ITERATOR_TYPES:
        # AM1：`repeat(x)`/`count(start, step)` 的 repr **内嵌参数**（元素/整数）⇒
        # 读 C `__reduce__` 拿真实参数并递归求界；读不到 ⇒ 拒绝。精确 C 类型，0 作者调用。
        return _arg_embedding_iter_bound(value, cls)
    if cls in _SHORT_REPR_ITERATOR_TYPES:
        return 256                           # 精确 C 迭代器类型（repr 形态固定，已逐类实测）
    # --- 标准库精确类型 ---
    if cls is _decimal_module.Decimal:
        try:
            parts = value.as_tuple()
            # 位数**不能截断**（1000 位 Decimal 的 str 就是 1000 字符）
            return len(parts.digits) + len(str(parts.exponent)) + 32
        except Exception as exc:              # noqa: BLE001 - 用户对象异常不得漏出
            _render_unmeasurable(exc)
            return None
    if cls is _uuid_module.UUID:
        # AJ1（第二十三轮）：**精确类型**的实例槽同样可写（`object.__setattr__(u,"int",10**400000)`
        # ⇒ 常量 64 被击穿）⇒ 精确类型也必须走同一道闸，不能走常量。
        return _uuid_render_bound(value, as_repr)
    if cls is _datetime_module.datetime or cls is _datetime_module.time:
        if not as_repr:
            return 160                        # str() 只有偏移，没有 tzinfo 的 repr
        extra = _tzinfo_extra(value, cls)     # 第十二轮 M6：repr 内嵌 tzinfo 的 repr
        return None if extra is None else 160 + extra
    if cls is _datetime_module.timezone:
        # AM1：`str(timezone)` 就是**作者可写的 name**（`timezone(timedelta(0), "E"*1e7)`
        # ⇒ 10,000,000 字符）；repr 再内嵌 name 的 repr。读 C 槽
        # `timezone.tzname(value, None)`（与 `str()` 同值，0 作者调用）取真实 name，
        # 按 repr 转义 10× 计。
        try:
            name = _datetime_module.timezone.tzname(value, None)
        except Exception as exc:              # noqa: BLE001 - 读不到 ⇒ 拒绝
            _render_unmeasurable(exc)
            return None
        if type(name) is not str:
            return None
        if as_repr:
            return 10 * str.__len__(name) + 64
        return str.__len__(name) + 8
    if cls is _datetime_module.date:
        return 40
    if cls is _datetime_module.timedelta:
        return 96                             # timedelta.max repr = 70
    if cls is _re_module.Pattern:
        try:
            # repr 里带 flags 文字（re.IGNORECASE|MULTILINE|… 实测 66 字符）
            return 10 * len(value.pattern) + 32 + 16 * bin(value.flags).count("1")
        except Exception as exc:              # noqa: BLE001 - 用户对象异常不得漏出
            _render_unmeasurable(exc)
            return None
    if cls in _PATH_EXACT_TYPES:
        # AJ1（第二十三轮）：精确 `PurePath` 的 `_str`/`_drv`/`_root` 槽同样可写
        # （`p._str = "E"*1e7` ⇒ 投影 67 / 实际 10,000,000）⇒ 精确类型也走同一道闸
        # （`_path_state_chars` 读的**就是**真实槽，所以精确类型同样成立）。
        return _path_render_bound(value, as_repr)
    if cls is _array_module.array:
        return _array_render_bound(value, _array_module.array)
    if cls is _collections.deque:
        return _container_render_bound(len(value), _iter_values(value))
    if cls is _types_module.FunctionType:
        # AM1：`<function QUALNAME at 0x…>`，`__qualname__` 可写（实测 10,000,026）。
        return _function_repr_bound(value)
    if cls in (type(len), type(print), type("".join)):
        # 内建函数/内建方法的 repr 只嵌 C 内建名（`len.__qualname__` 只读）⇒ 常量可证；
        # Python 绑定方法（`types.MethodType`）不在表里：它内嵌 self 的 repr ⇒ 走 fallback 拒绝。
        return 256
    if cls is type(_collections.OrderedDict()) or cls is type(_collections.defaultdict()):
        return _container_render_bound(len(value), _iter_dict_items(value))
    if isinstance(value, _enum_module.Enum):
        # 第十二轮 M6/M7：repr 内嵌 value（作者数据）必须计入；IntEnum/StrEnum/Flag
        # 成员的 str() 语义由各自的原始槽决定，不再一律当作"覆写 ⇒ 拒"。
        return _enum_member_render_bound(value, as_repr)
    # --- 子类：按表里该转换符的 `slots` 归属（唯一判据）走安全祖先语义 ---
    base = _safe_render_base(cls)
    if base is not None and _slots_owned_by_base(cls, base):
        if base is object:
            # AM1：默认 `object.__repr__` = `<__module__.qualname object at 0x…>`，
            # 类名可写（实测 20,000,025 字符 / 峰值 25MB）⇒ 读真实类名。
            name = _class_name_bound(cls)
            return None if name is None else name + 64
        return _subclass_render_bound(value, base, as_repr)
    # **namedtuple 快速通道已删除**（第十轮 F1/F3 方案 1）："代码对象相同"不等于
    # "行为相同"——闭包 cell / globals / 描述符 `__get__` 都能在 code 相同的前提下
    # 换成敌意实现（D1-a / D1-b 实测 50,000,004 字符被放行、先渲染再被事后网拒）。
    # namedtuple 因此视为**不可整体渲染**（字段/索引仍可取值下钻），
    # 需要整体渲染请先 `str(x)`（见"已知边界"）。
    if _is_plain_namespace(value):
        return _render_namespace_bound(value)
    return None


def _bound_text(value: Any, as_repr: bool = False) -> Optional[int]:
    """表里 ``s`` 行的 ``bound``：``str(value)`` 的可证上界（强制非 repr 语义）。"""
    return _render_type_bound(value, False)


def _bound_repr(value: Any, as_repr: bool = False) -> Optional[int]:
    """表里 ``r``/``a`` 两行的 ``bound``：``repr(value)`` 的可证上界。"""
    return _render_type_bound(value, True)


def _render_size(value: Any, as_repr: bool = False) -> Optional[int]:
    """``%s``/``%r``/``%a``/模板插值/``concat`` 的**可证**渲染上界。

    **执行点只做一件事**：按语义取表里 ``s``/``r`` 那一行的 ``bound`` 调用
    （``s``→`_bound_text`，``r``/``a``→`_bound_repr`）。归属判定全在 bound 里面。
    """
    rule = _RENDER_DISPATCH["r" if as_repr else "s"]
    return rule.bound(value, as_repr)


_PATH_EXACT_TYPES = frozenset({
    _pathlib_module.PurePosixPath, _pathlib_module.PureWindowsPath,
    _pathlib_module.PosixPath, _pathlib_module.WindowsPath,
})


def _container_render_bound(count: Optional[int], items: Any,
                            extra: int = 2, pairs: bool = False) -> Optional[int]:
    """容器渲染上界 = 16 + Σ(每项渲染 + extra)。

    任何一项无法界定 ⇒ ``None``；**枚举过程抛出的任何用户异常也转成** ``None``
    （第九轮 D4：``__iter__`` 抛异常曾把原始 ``RuntimeError`` 漏给作者）。
    """
    total = 16
    if count is not None and count * 4 + 16 > _EXPR_MAX_RESULT_HARD:
        # 每项至少 "''" + ", " = 4 字符 ⇒ 这个**下界**已超硬上限，必然拒绝；
        # 直接返回（不必枚举 2500 万项），作为 projected 会被硬上限拦下。
        return count * 4 + 16
    try:
        for item in items:
            # 同质常见类型的内联快路径（1–25M 项区间实测 14.55s → 明显下降）
            tipo = type(item)
            if tipo is str:
                # AX3（第二十八轮）：容器元素与标量共用**真实逐字符**宽度（不再是扁平
                # 10×），否则预算内的合法值被拒。
                total += _str_escape_width(item) + extra
                if total >= _EXPR_MAX_RESULT_HARD:
                    return total
                continue
            if tipo is int:
                total += _value_size(item) + 2 + extra
                if total >= _EXPR_MAX_RESULT_HARD:
                    return total
                continue
            if pairs:
                part = _render_size(item[0], as_repr=True)
                if part is None:
                    return None
                total += part + extra
                part = _render_size(item[1], as_repr=True)
                if part is None:
                    return None
                total += part + extra
            else:
                part = _render_size(item, as_repr=True)
                if part is None:
                    return None
                total += part + extra
            if total >= _EXPR_MAX_RESULT_HARD:
                return total
    except Exception as exc:                  # noqa: BLE001 - 用户对象异常不得漏出
        _render_unmeasurable(exc)
        return None
    return total


def _subclass_render_bound(value: Any, base: type,
                           as_repr: bool = False) -> Optional[int]:
    """子类（未覆写 ``__str__``/``__repr__`` 原始槽）按安全祖先语义求上界。

    **计费一律走祖先 C 槽**（第十一轮 H1）：``str.__len__`` / ``list.__iter__`` /
    ``dict.items`` …，绝不调用实例覆写的 ``__iter__``/``__len__``；祖先没有可用槽
    ⇒ ``None``（不可证 ⇒ 渲染前拒绝）。
    """
    try:
        if base is int:
            return _value_size(int.__index__(value)) + 2
        if base is float:
            return 32
        if base is bool:
            return 8
        if base is type(None):
            return 4
        if base is str:
            if not as_repr:
                return str.__len__(value)
            return _str_escape_width(value)
        if base is bytes or base is bytearray:
            is_bytes = base is bytes
            prefix = _BYTES_REPR_PREFIX if is_bytes else _BYTEARRAY_REPR_PREFIX
            return _bytes_repr_width(value, prefix, base,
                                     always_escape_apostrophe=not is_bytes)
        if base is _decimal_module.Decimal:
            parts = _decimal_module.Decimal.as_tuple(value)
            return len(parts.digits) + len(str(parts.exponent)) + 32
        if base is _uuid_module.UUID:
            return _uuid_render_bound(value, as_repr)
        if base in (_datetime_module.datetime, _datetime_module.time):
            if not as_repr:
                return 160
            extra = _tzinfo_extra(value, base)
            return None if extra is None else 160 + extra
        if base is _datetime_module.timezone:
            return 64
        if base is _datetime_module.date:
            return 40
        if base is _datetime_module.timedelta:
            return 96
        if base is _re_module.Pattern:
            return (10 * _re_module.Pattern.pattern.__get__(value, base).__len__()
                    + 32 + 16 * bin(_re_module.Pattern.flags.__get__(value, base)).count("1"))
        if issubclass(base, _pathlib_module.PurePath):
            return _path_render_bound(value, as_repr)
        if base is _enum_module.Enum:
            return _enum_member_render_bound(value, as_repr)
        if base is _array_module.array:
            return _array_render_bound(value, base)
        if base is _collections.deque:
            return _container_render_bound(_collections.deque.__len__(value),
                                           _collections.deque.__iter__(value))
        if base is dict:
            return _container_render_bound(_ancestor_len(value, dict),
                                           _ancestor_stream(value, dict, mapping=True),
                                           pairs=True)
        if base in _ANCESTOR_WALK_BASES:
            return _container_render_bound(_ancestor_len(value, base),
                                           _ancestor_stream(value, base))
        return None                           # 祖先没有可用槽 ⇒ 不可证
    except ValueError:
        raise
    except Exception as exc:                  # noqa: BLE001 - 用户对象异常不得漏出
        _render_unmeasurable(exc)
        return None


#: 有限 float 的整数部分最多 309 位（最大 ~1.8e308）；再加符号位。
_FLOAT_MAGNITUDE_DIGITS = 310


def _numeric_magnitude_bound(value: Any, as_repr: bool = False) -> Optional[int]:
    """数值转换的**量级**下界（T2）：渲染长度至少是数值自身的十进制位数。

    * ``int``：``_value_size``（十进制位数上界）；
    * ``float``：有限值 ≤309 位整数部分（``inf``/``nan`` 是常量 3–4 字符）；
    * ``Decimal``：``Decimal.adjusted()``（C 槽）给出指数 ⇒ 位数 ≈ adjusted+1。

    旧实现只给 ``_value_size``（float/Decimal 只有 8/24）⇒ ``%f % 1e300`` 上界 40、
    实际 308（T2 的 6,500 例低估都来自这里）。``as_repr`` 仅为表里 bound 的统一签名
    存在（数值渲染上界与 repr 无关）。
    """
    kind = _numeric_kind(value)
    if kind == "int":
        if isinstance(value, bool):
            return 1
        if type(value) is not int:
            # 子类：`int.__index__` C 槽读**真实值**（不跑覆写的 __index__/__int__），
            # 与原生 `%d` 读真实存储一致。
            value = int.__index__(value)
        return _value_size(value)
    if kind == "float":
        if type(value) is not float:
            # 子类：`float.__float__` C 槽读真实值（不跑覆写的 __float__/__abs__/__eq__）
            value = float.__float__(value)
        if math.isnan(value) or math.isinf(value):
            return 8
        if value == 0.0:
            return 1
        try:
            exponent = math.floor(math.log10(abs(value)))
        except (ValueError, OverflowError):
            return _FLOAT_MAGNITUDE_DIGITS
        return max(1, int(exponent) + 1) + 1      # +1 吸收 log10 的舍入误差
    if kind == "decimal":
        if type(value) is not _decimal_module.Decimal and not _numeric_slots_clean(
                value, ("adjusted", "is_nan", "is_infinite", "is_zero", "__float__")):
            # AC6：Decimal 子类**槽干净**时可走精确类型的 C 槽读量级；覆写才拒绝。
            return None
        if _decimal_module.Decimal.is_nan(value) or \
                _decimal_module.Decimal.is_infinite(value):
            return 8
        if _decimal_module.Decimal.is_zero(value):
            return 1
        try:
            adjusted = _decimal_module.Decimal.adjusted(value)
        except (ArithmeticError, ValueError, TypeError):
            return 8
        return max(1, int(adjusted) + 1) + 1
    return 8


#: **唯一转换符派发表**（第二十一/二十二轮）：key = 转换符，value = `(kind, slots, bound)`。
#: 它合并了旧的 `_CONVERSION_DISPATCH`（原生派发事实）与旧的渲染分类表
#: （类型半边是 `_PROVEN_RENDER_OWNERS`）。`_conversion_accepts` / `_definitely_rejected`
#: / `_spec_bound` / `_spec_input_size` / `_render_size` 全部只查这一张表：**归属判定
#: 只发生在每行的 `bound` 里**，任何"判必错"的分支都由这里的 `kind`/`slots` 派生。
_RENDER_DISPATCH: Dict[str, _RenderRule] = {
    # 数值：实测★4 —— int 家族在 d/i/u/o/x/X/c 上 C 存储直读、不派发任何槽；
    # float/Decimal 家族才派发 __int__/__float__/__index__（覆写 ⇒ 由 bound 拒绝）。
    "d": _RenderRule("int", ("__int__",), _numeric_magnitude_bound),
    "i": _RenderRule("int", ("__int__",), _numeric_magnitude_bound),
    "u": _RenderRule("int", ("__int__",), _numeric_magnitude_bound),
    "o": _RenderRule("index", ("__index__",), _numeric_magnitude_bound),
    "x": _RenderRule("index", ("__index__",), _numeric_magnitude_bound),
    "X": _RenderRule("index", ("__index__",), _numeric_magnitude_bound),
    "c": _RenderRule("char", ("__index__",), None),   # bound 不参与：宽度常量，见 _spec_bound
    "e": _RenderRule("float", ("__float__",), _numeric_magnitude_bound),
    "E": _RenderRule("float", ("__float__",), _numeric_magnitude_bound),
    "f": _RenderRule("float", ("__float__",), _numeric_magnitude_bound),
    "F": _RenderRule("float", ("__float__",), _numeric_magnitude_bound),
    "g": _RenderRule("float", ("__float__",), _numeric_magnitude_bound),
    "G": _RenderRule("float", ("__float__",), _numeric_magnitude_bound),
    # 文本：`s` 派发 `__str__`（`object.__str__` 就是 `repr`）⇒ slots 含 `__repr__`；
    # bytes 模式的 `s`/`b` 由 `_render_rule` 路由到 `b` 的 buffer 规则。
    "s": _RenderRule("str", ("__str__", "__repr__"), _bound_text),
    "r": _RenderRule("repr", ("__repr__",), _bound_repr),
    "a": _RenderRule("repr", ("__repr__",), _bound_repr),
    "b": _RenderRule("buffer", ("__bytes__",), _bytes_mode_bound),
}

#: 兼容视图（第二十轮测试/语料按 ``conversion -> (kind, slots)`` 读取）：**派生**自
#: `_RENDER_DISPATCH`，不是第二张表；新增/修改转换符只改 `_RENDER_DISPATCH`。
_CONVERSION_DISPATCH = {c: (r.kind, r.slots) for c, r in _RENDER_DISPATCH.items()}


def _render_rule(conversion: str, bytes_mode: bool) -> Optional[_RenderRule]:
    """转换符 → 表行；bytes 模式的 ``%s``/``%b`` 都由 **buffer** 行处理。

    实测：bytes `%s` 对非 buffer 值走 ``__bytes__``、对 buffer 直读（与 ``%b`` 同）；
    str 模式没有 ``%b``（`_parse_format_specs` 在格式串层面就拒它）。
    """
    if bytes_mode:
        if conversion in ("s", "b"):
            return _RENDER_DISPATCH["b"]
    elif conversion == "b":
        return None
    return _RENDER_DISPATCH.get(conversion)


def _spec_bound(spec: _FormatSpec, value: Any, width: int,
                bytes_mode: bool = False) -> Optional[int]:
    """单个转换符的**分配**上界（含**字面精度**造成的分配，第三轮验收 A）。

    实测（CPython 3.11，精度 2e6）：``%.<P>d/i/u/o/x/X`` 分配 ≈P、``f/F`` ≈P+2、
    ``e/E`` ≈P+6，而 ``%.<P>g/G`` **结果只有 1 字符却仍分配 ≈P**（P=9e8 时实测
    1024MB）——所以精度必须进上界，且上界要按"分配量"而不是"结果长度"给。
    ``s``/``r``/``a`` 的精度是**截断**而不是填充（实测 0 额外分配），按
    ``min(内容量, 精度)`` 计。

    第二十一轮：**这里不再做归属判定**——基础量级/渲染上界取自表行的 ``bound``
    （bytes 模式 ``s``/``b`` 由 `_render_rule` 路由到 buffer 行，输出是原始字节）。
    """
    rule = _render_rule(spec.conversion, bytes_mode)
    if rule is None:
        return None
    if rule.kind == "char":
        return max(1, width) + 8
    base = rule.bound(value, spec.conversion in "ra")
    if base is None:
        return None                            # 不可证 ⇒ 调用方具名拒绝（Z1）
    if rule.kind in ("str", "repr", "buffer"):
        # S1：bytes 模式 %s/%b 输出就是**原始字节**（不转义），按 buffer 字节数计；
        # 旧实现落进 float 分支给 32 ⇒ `%b` 配 200 万字节 memoryview 时上界低于实际 2.7 万倍。
        if spec.precision >= 0:
            base = min(base, spec.precision)
        return max(base, width) + 8
    if spec.conversion in "diuoxX":
        pad = spec.precision if spec.precision > 0 else 0
        return max(base, pad, width) + 8
    precision = spec.precision if spec.precision >= 0 else 6
    if spec.conversion in "eE":
        return max(precision + 8, width) + 8
    # f/F/g/G：整数部分按量级计（`%f % 1e300` 实测 308 字符）
    return max(base + precision, width) + 8


_MISSING = object()


def _has_raw_slot(cls: type, name: str) -> bool:
    """``cls`` 的 MRO ``__dict__`` 里是否有 ``name``（不触发描述符/元类作者代码）。"""
    try:
        mro = _type_mro(cls)
    except (AttributeError, TypeError):       # 非普通类对象
        return False
    for base in mro:
        try:
            namespace = _type_namespace(base)
        except (AttributeError, TypeError):
            continue
        if name in namespace:
            return True
    return False


def _star_width(value: Any) -> Optional[int]:
    """``*`` 宽度/精度参数的**C 槽**整数值；非 int（含子类）⇒ ``None``。

    第十五轮 Q1/Q2：只接受 ``int`` 及其子类，用 ``int.__index__`` 读真实值，绝不跑
    覆写的 ``__int__``（实测 `EvilInt` 让上界算成 1、原生却按 90,000,000 分配）。
    """
    if isinstance(value, int):
        return int.__index__(value)
    return None


def _is_non_finite_number(value: Any) -> bool:
    """可证的"非有限数值"（Z2）：float/Decimal 的 inf/nan —— 全走 C 槽，0 作者调用。"""
    kind = _numeric_kind(value)
    if kind == "float":
        real = value if type(value) is float else None
        if real is None:
            return False                       # 子类不可证 ⇒ 交给上层拒绝
        return math.isnan(real) or math.isinf(real)
    if kind == "decimal":
        if type(value) is not _decimal_module.Decimal:
            return False                       # 子类不可证 ⇒ 交给上层拒绝
        return (_decimal_module.Decimal.is_nan(value)
                or _decimal_module.Decimal.is_infinite(value))
    return False


def _definitely_rejected(conversion: str, value: Any, bytes_mode: bool = False) -> bool:
    """native ``%`` 是否**必然**在该转换符上抛错（因此**不构造**该字段）？

    由 `_RENDER_DISPATCH`（经 `_render_rule`）派生且 **fail-closed**：只有"该转换符要
    派发的槽在类型上**根本不存在**"（或值类型与转换符确定不匹配）才返回 True；只要
    类里有那个槽（哪怕会返回伪造值）就返回 False ⇒ 调用方必须拒绝，而不是判 native。
    """
    rule = _render_rule(conversion, bytes_mode)
    if rule is None:
        return False
    kind = rule.kind
    cls = type(value)
    if kind == "buffer":
        # bytes 模式的 `s`/`b`：真 buffer 直读；否则原生派发 `__bytes__`
        # （类里没有 ⇒ 必然 TypeError；有 ⇒ 不可证 ⇒ 交给 `_conversion_accepts` 拒）。
        return not _is_bytes_like(value) and not _has_raw_slot(cls, "__bytes__")
    if kind in ("str", "repr"):
        return False                           # str 模式：任何值都可能 str/repr 成功
    if kind == "char":
        if isinstance(value, int):
            return False
        if bytes_mode:
            if isinstance(value, str):
                return True
            if isinstance(value, (bytes, bytearray)):
                return _trusted_len(value) != 1
            return not _has_raw_slot(cls, "__index__")
        if isinstance(value, (bytes, bytearray, memoryview)):
            return True
        if isinstance(value, str):
            return _trusted_len(value) != 1
        return not _has_raw_slot(cls, "__index__")
    if kind == "index":
        # AB1：原生对**任何**对象都会派发 `__index__` ⇒ 类里有这个槽就绝不能判必错。
        if isinstance(value, int):
            return False
        return not _has_raw_slot(cls, "__index__")
    if kind == "int":
        if _is_non_finite_number(value):
            return True                        # Z2：OverflowError/ValueError
        if _numeric_kind(value) is not None:
            return False
        return not (_has_raw_slot(cls, "__index__") or _has_raw_slot(cls, "__int__"))
    if kind == "float":
        # AB4：Decimal('sNaN') 原生是 TypeError；float 的 inf/nan 原生**成功**。
        if _is_signaling_nan(value):
            return True
        if _numeric_kind(value) == "int":
            if not _numeric_slots_clean(value, rule.slots):
                # 覆写了 __float__/__index__/__int__ 的 int 子类：原生会派发作者协议
                # ⇒ 不可证 ⇒ 返回 False（调用方拒绝），绝不判 native。
                return False
            return int.bit_length(value) > 1024      # 巨大 int ⇒ OverflowError/TypeError
        if _numeric_kind(value) is not None:
            return False
        return not (_has_raw_slot(cls, "__index__") or _has_raw_slot(cls, "__float__"))
    return False


class _ModDecision(NamedTuple):
    """``%`` 判定的结果。

    * ``kind == "render"``：每个字段都有可证上界 ⇒ ``projected`` 是结果上界；
    * ``kind == "native"``：**已证明**原生必然抛错（类型/结构/键），``projected`` 是
      "错误点之前原生会构造的字段"的上界（第十五轮 Q3：doomed-field 放大）；
    * ``kind == "refuse"``：既不能证明原生会抛、又给不出可证上界 ⇒ 必须拒绝
      （第十五轮 Q1.1：解析不出来/`*` 解不出来/未知类型 `%s` 的**默认分支是拒绝**）。
    """

    kind: str
    projected: int
    reserve: int
    values: List[Any]
    widths: List[int]
    reason: str


_NON_MAPPING_BUILTINS = (str, bytes, bytearray, list, tuple, set, frozenset,
                         int, float, bool, complex, range, memoryview, type)


def _mapping_kind(value: Any) -> str:
    """``'mapping'`` | ``'unsupported'`` | ``'nonmapping'``（都不触发作者代码）。

    * ``'mapping'``：精确 ``dict``/``dict`` 子类，或三种已知 stdlib 映射；
    * ``'nonmapping'``：**证明**原生一定 TypeError 的内建非映射（str/list/int/None…），
      交给原生抛（原生错误优先）；
    * ``'unsupported'``：像映射但无我们可读的祖先 C 槽（自定义 ``__getitem__``/
      有状态映射）⇒ 拒绝 + ``dict(x)`` 逃生口（第十五轮 Q2，owner 的 (a) 口径）。
    """
    cls = type(value)
    if isinstance(value, dict):
        return "mapping"
    if cls is _types_module.MappingProxyType:
        # S2：`type(proxy)` 恒为 mappingproxy，被包对象**不可访问** ⇒ 无法证明它是
        # 精确 dict；`proxy.copy()` 实测会走底层 dict 子类的实例 `__getitem__`
        # （1 次作者调用 + 伪造值），不是 C 槽 ⇒ 一律拒绝（owner 的 (a) 口径）。
        return "unsupported"
    if cls in (_collections.UserDict, _collections.ChainMap):
        return "mapping"                       # 只认**精确**类型
    if isinstance(value, (_collections.ChainMap, _collections.UserDict)):
        # X2.1：子类可能覆写 `maps`/`data`（property 或换成别的对象类型）——
        # 读那些属性就是运行作者代码，必须在读之前拒绝。
        return "unsupported"
    if value is None or isinstance(value, _NON_MAPPING_BUILTINS):
        return "nonmapping"
    if _has_raw_slot(cls, "keys") and _has_raw_slot(cls, "items"):
        return "unsupported"
    if _has_raw_slot(cls, "__getitem__"):
        return "unsupported"
    return "nonmapping"


def _spec_input_size(spec: _FormatSpec, value: Any, bytes_mode: bool = False) -> int:
    """字段的**原始输入量**（用于成长额度；上界用于投影，两者分开，Q1/Q3）。

    第二十一轮：模式/转换符 → 表行（`_render_rule`）只在这里用来区分"buffer 字节数"
    与"渲染量"，**不做归属判定**（`_conversion_accepts` 已经用同一行的 bound 判过）。
    与旧口径一致地取渲染量作为输入量，数值退回 ``_value_size``；单位换算（int 的 3
    字符）因此保持既有文案契约。
    """
    rule = _render_rule(spec.conversion, bytes_mode)
    if rule is None:
        return 0
    if rule.kind == "char":
        return 1
    if rule.kind == "buffer":
        size = rule.bound(value, False)
        return max(size if size is not None else 0, 1)
    # AB3：数值字段到这里时**已被判为可证**（不可证的数值子类在前面就被拒），
    # 因此下面的 `_render_size` 不会碰到作者 `__len__`/`__str__`。
    if spec.conversion == "a":
        # AR4：`ascii()` 的转义展开**不吃成长额度**——输入量只按值的权威内容量计
        # （非权威类型按 0 ⇒ 落到 floor），于是 4–10× 的展开会在**分配前**被拒。
        return max(_content_size(value), 1)
    size = _render_size(value, as_repr=spec.conversion == "r")
    if size is None:
        size = _value_size(value)
    return max(size, 0)


def _mapping_key(key: Optional[str], bytes_mode: bool) -> Any:
    """映射键按模式取类型（T1）：bytes 模式编码成 bytes（格式串按 latin-1 解码而来）。"""
    if key is None:
        return None
    if bytes_mode:
        return key.encode("latin-1")
    return key


def _mapping_snapshot(values: Any) -> Any:
    """已知标准库映射 ⇒ 一次性快照（精确 ``dict``）；其余 ⇒ ``None``（拒绝）。

    第十五轮 Q2（与 owner 的 (a) 口径对齐）：

    * 精确 ``dict``/``dict`` 子类：**不物化**，逐键 ``dict.get`` C 槽（不触发
      ``__missing__``，也不跑覆写的 ``__getitem__``/``__contains__``）；
    * 精确 ``types.MappingProxyType``：``dict(values)``（C 迭代底层 dict）；
    * 精确 ``collections.UserDict``：``.data`` 必须是精确 ``dict``；
    * 精确 ``collections.ChainMap``：``.maps`` 必须是精确 ``list`` 且每项是 ``dict``，
      逐项用 ``dict.items`` C 槽合并（先者优先）；
    * 其余（自定义 Mapping / 有状态映射 / 上述类型的子类）⇒ ``None``：
      "无 C 槽自定义类型 ⇒ 拒绝"，逃生口是把值在节点/插件侧 ``dict(x)`` 显式包装。
    """
    cls = type(values)
    if isinstance(values, dict):
        return values
    if cls is _types_module.MappingProxyType:
        return None                            # S2：不走 copy()/dict()，一律拒绝
    if cls is _collections.UserDict:
        try:
            data = object.__getattribute__(values, "data")
        except AttributeError:
            return None
        if type(data) is not dict:
            return None
        return dict(data)
    if cls is _collections.ChainMap:
        try:
            maps = object.__getattribute__(values, "maps")
        except AttributeError:
            return None
        if type(maps) is not list:
            return None
        merged: dict = {}
        for one in reversed(maps):
            if not isinstance(one, dict):
                return None
            for key, item in dict.items(one):   # dict.items C 槽：dict 子类的覆写不跑
                merged[key] = item
        return merged
    return None


def _mod_decide(fmt: str, fmt_len: int, specs: Optional[List[_FormatSpec]],
                values: Any, bytes_mode: bool) -> _ModDecision:
    """``%`` 的**结构性**判定：默认拒绝，只有全字段可证才放行（第十五轮 Q1/Q2/Q3）。

    不含任何"逐形状补丁"：``specs is None``（解析不出来）直接拒；``*`` 宽度/精度必须
    从参数元组按精确 ``int`` 解出（解不出就拒）；``%s``/``%r``/``%a`` 的未知类型参数
    直接拒（不运行作者 ``__str__``/``__repr__``）；错误点之前的字段全部计入上界。
    """
    if specs is None:
        return _ModDecision("refuse", 0, 0, [], [],
                            "格式串无法在不运行作者代码的前提下解析（未闭合/非法转换符）")
    if not specs:
        # Z5.2：没有转换符（如 "%%(a)s"）——原生在 dict 参数下**成功**且结果 ≤ 格式串
        # 长度（无放大）⇒ 按 render 处理，不再错误地声称"原生必错"。
        return _ModDecision("render", fmt_len, fmt_len, [], [], "")
    mapping = any(spec.key is not None for spec in specs)
    snapshot = None
    args = None
    if mapping:
        if any(spec.star_width or spec.star_precision for spec in specs):
            return _ModDecision("native", 0, 0, [], [], "mapping + '*'")
        kind = _mapping_kind(values)
        if kind == "nonmapping":
            # 根本不是映射：原生 `'%(a)s' % 'x'` 是 TypeError（结构错误先于尺寸错误）
            return _ModDecision("native", 0, 0, [], [], "mapping 格式 + 非映射值")
        if kind == "unsupported":
            return _ModDecision(
                "refuse", 0, 0, [], [],
                f"映射类型 {_type_name(values)!r} 没有可读祖先 C 槽; " + _ESCAPE_DICT_HINT)
        snapshot = _mapping_snapshot(values)
        if snapshot is None:
            return _ModDecision("refuse", 0, 0, [], [],
                                f"映射类型 {_type_name(values)!r} 没有可读祖先 C 槽; "
                                + _ESCAPE_DICT_HINT)
    elif isinstance(values, tuple):
        count = tuple.__len__(values)
        args = [tuple.__getitem__(values, index) for index in range(count)]
    else:
        if len(specs) != 1 or specs[0].star_width or specs[0].star_precision:
            return _ModDecision("native", 0, 0, [], [], "单值 + 多转换符/星号宽度")
        args = [values]

    cache: dict = {}
    upper = 0
    input_size = fmt_len
    plan_values: List[Any] = []
    plan_widths: List[int] = []
    idx = 0
    for spec in specs:
        width = spec.width
        precision = spec.precision
        for is_precision in (False, True):     # 原生顺序：先宽度、后精度
            star = spec.star_precision if is_precision else spec.star_width
            if not star:
                continue
            if idx >= len(args):
                return _ModDecision("native", upper, 0, [], [], "参数不足")
            resolved = _star_width(args[idx])
            if resolved is None:
                if _has_raw_slot(type(args[idx]), "__index__") or \
                        _has_raw_slot(type(args[idx]), "__int__"):
                    return _ModDecision(
                        "refuse", 0, 0, [], [],
                        "星号宽度/精度需要运行作者代码才能解出; " + _ESCAPE_INT_HINT)
                return _ModDecision("native", upper, 0, [], [], "原生 '* wants int'")
            if is_precision:
                precision = abs(resolved)
            else:
                width = max(width, abs(resolved))
            idx += 1
        if mapping:
            # T1：键查找类型**由模式决定**——原生 bytes `%` 用 **bytes** 键，
            # str `%` 用 str 键；旧实现一律用 spec.key（str）⇒ `b'%(a)s' % {b'a': …}`
            # 被判"缺键 ⇒ 原生必错"，而原生成功（fail-open 放大）。
            key = _mapping_key(spec.key, bytes_mode)
            if key not in cache:
                cache[key] = dict.get(snapshot, key, _MISSING)   # C 槽 + 只读一次
            value = cache[key]
            if value is _MISSING:
                if _has_raw_slot(type(snapshot), "__missing__"):
                    return _ModDecision(
                        "refuse", 0, 0, [], [],
                        f"映射 {_type_name(snapshot)!r} 的缺键会触发 __missing__; "
                        + _ESCAPE_DICT_HINT)
                return _ModDecision("native", upper, 0, [], [], "映射缺键")
        else:
            if idx >= len(args):
                return _ModDecision("native", upper, 0, [], [], "参数不足")
            value = args[idx]
            idx += 1
        if _definitely_rejected(spec.conversion, value, bytes_mode):
            return _ModDecision("native", upper, 0, [], [], "转换符 × 值类型必然不匹配")
        if not _conversion_accepts(spec.conversion, value, bytes_mode):
            return _ModDecision(
                "refuse", 0, 0, [], [],
                f"字段 {spec.conversion!r} 的值 {_type_name(value)!r} 没有可读祖先 C 槽; "
                + _numeric_escape_hint(spec.conversion, bytes_mode))
        effective = spec._replace(width=width, precision=precision,
                                  star_width=False, star_precision=False)
        bound = _spec_bound(effective, value, width, bytes_mode)
        if bound is None:
            hint = _RENDER_ESCAPE_HINT
            if spec.conversion in "diuoxXeEfFgGc":
                hint = _numeric_escape_hint(spec.conversion, bytes_mode)
            return _ModDecision(
                "refuse", 0, 0, [], [],
                f"字段 {spec.conversion!r} 的值 {_type_name(value)!r} 没有可证渲染上界; "
                + hint)
        upper += bound
        input_size += _spec_input_size(effective, value, bytes_mode)
        plan_values.append(value)
        plan_widths.append(width)
    if not mapping and idx != len(args):
        return _ModDecision("native", fmt_len + upper, fmt_len, [], [],
                            "参数过多（此前字段已被构造）")
    return _ModDecision("render", fmt_len + upper, input_size,
                        plan_values, plan_widths, "")

def _native_mod(a, b):
    """非 str/bytes 的 ``%``（数值取模）：原生语义，不放大，本单不设界。"""
    return a % b


def _format_values_size(values: Any) -> int:
    """``%`` 实参的**内容量**下界（只算 value，映射 key 不进结果）。"""
    if isinstance(values, dict):
        return sum(_content_size(v) for v in values.values())
    if isinstance(values, (tuple, list)):
        return sum(_content_size(v) for v in values)
    return _content_size(values)


def _fn_mod(a, b):
    """取模 / printf 式格式化（第十五轮：判定是**结构性**的，默认拒绝）。

    口径：``specs`` 解析不出来 ⇒ 拒；``*`` 宽度/精度解不出（非精确 int）⇒ 拒；
    ``%s``/``%r``/``%a`` 的未知类型值 ⇒ 拒（绝不运行作者 ``__str__``/``__repr__``）；
    字段全部可证 ⇒ ``len(fmt) + Σ 字段上界`` 预检后渲染。**已证明**原生必然抛错的
    形状（转换符 × 类型不匹配 / 键缺失 / 参数个数不符 / mapping+``*``）才放行给原生，
    此时把"错误点之前会被构造的字段"上界也计入（第十五轮 Q3：doomed-field 放大）。

    数值 ``%`` 保持不动。
    """
    if type(a) not in (str, bytes, bytearray):
        if isinstance(a, (str, bytes, bytearray)):
            # Z4.1：子类可覆写 `__mod__`/`__bytes__` ⇒ 原生会跑作者代码且预检被绕过
            # （实测 `$F.mod(EvilStr(...), …)` 先分配 90.1MB）。一律分配前拒绝。
            raise ValueError(
                f"$F.mod cannot bound the {_type_name(a)} format operand "
                f"(格式串必须是**精确** str/bytes/bytearray; 子类可覆写 __mod__/__bytes__). "
                f"Refusing to build an unbounded result. {_RENDER_ESCAPE_HINT}"
            )
        return _native_mod(a, b)
    fmt = _fmt_text(a)
    fmt_len = _trusted_operand_len(a)
    bytes_mode = not isinstance(a, str)
    specs = _parse_format_specs(fmt, bytes_mode=bytes_mode)
    decision = _mod_decide(fmt, fmt_len, specs, b, bytes_mode)
    what = f"$F.mod(format {fmt_len} {_unit_for(a)})"
    if decision.kind == "refuse":
        raise ValueError(
            f"$F.mod cannot bound the rendered size of the {_unit_for(a)} format "
            f"({decision.reason}). Refusing to build an unbounded result. "
            f"{_RENDER_ESCAPE_HINT}"
        )
    ascii_hint = _ASCII_GROWTH_HINT if any(spec.conversion == "a" for spec in specs) else ""
    if decision.kind == "native":
        _check_result_size(what, decision.projected, decision.reserve, _unit_for(a),
                           ascii_hint)
        return a % b
    _check_result_size(what, decision.projected, decision.reserve, _unit_for(a), ascii_hint)
    if any(spec.key is not None for spec in specs):
        # T1：渲染用的精确 dict 也要用**模式对应的键类型**，否则原生找不到键。
        resolved = {_mapping_key(spec.key, bytes_mode): value
                    for spec, value in zip(specs, decision.values)}
        return a % resolved
    return a % b


def _fn_pow(a, b):
    """幂运算，指数设上限（2026-09 安全评审 P1）。

    ``$F.pow(9, 9999999)`` 这种约 20 字节的表达式曾可烧掉分钟级 CPU 并膨胀
    数十 MB 内存——单流程即可拖死 worker，并行分支还能线性放大。
    """
    if isinstance(b, int):
        exponent = int.__abs__(int.__index__(b))     # C 槽：不跑覆写的 __int__
        if exponent > _POW_MAX_EXPONENT:
            raise ValueError(
                f"$F.pow exponent too large: {_safe_int_text(b)} "
                f"(limit {_POW_MAX_EXPONENT}). "
                "Refusing to burn CPU on an unbounded power computation."
            )
        if isinstance(a, int):
            # 渲染闸的**分配前**预检：int 结果的十进制位数 ≤ |b| * digits(a)
            digits = _value_size(a)
            projected = exponent * max(digits, 1) + 8
            _check_result_size(f"$F.pow({_safe_int_text(b)})", projected,
                               digits + len(str(exponent)), "chars")
    return a ** b

def _fn_lower(a): return a.lower()
def _fn_upper(a): return a.upper()
def _fn_capitalize(a): return a.capitalize()
def _fn_title(a): return a.title()
def _fn_strip(a): return a.strip()
def _fn_lstrip(a): return a.lstrip()
def _fn_rstrip(a): return a.rstrip()
def _is_bytes_like(value: Any) -> bool:
    """任意 buffer 协议对象（bytes/bytearray/memoryview/array/mmap…）。

    第三轮验收 B：只认 ``bytes``/``bytearray`` 会让 ``bytes.replace(memoryview(…))``
    这类**合法**调用跳过预检 ⇒ 先分配后检测。"预检必须是全定义"要求这里用
    buffer 协议判定，而不是枚举具体类型。
    """
    if isinstance(value, (bytes, bytearray)):
        return True
    try:
        memoryview(value)
    except TypeError:
        return False
    return True


def _ancestor_replace(value: Any, old: Any, new: Any) -> Any:
    """``value.replace(old, new)`` 的 C 槽形态：忽略子类覆写的 ``replace``。"""
    if isinstance(value, str):
        return str.replace(value, old, new)
    if isinstance(value, bytes):
        return bytes.replace(value, old, new)
    return bytearray.replace(value, old, new)


def _fn_replace(a, b, c):
    """``a.replace(b, c)``：替换放大必须**在替换之前**预检（#68 返工 + 第十二轮 M1）。

    ``'a'*100000 .replace('a', 'b'*1000)`` 的结果约 10^8 字符（≈100MB），
    而输入只有 101KB——与 ``mul`` 同族的放大路径；空模式更狠（每个字符间插一次）。
    类型判定按 Python 的真实语义：``str`` 要 str/str；``bytes``/``bytearray``
    接受任意 bytes-like 的 pattern/replacement（``bytes.replace(bytearray(...))``
    合法——第一版把它当"类型不对"跳过，于是完全没有预检）。

    **长度与命中数一律走内建祖先 C 槽**（第十二轮 M1）：``str.__len__`` /
    ``str.count`` / ``bytes.count``……实例覆写的 ``__len__``/``count`` 只能骗到
    额度，骗不到原生替换读的真实存储（实测 ``LyingLenStr("a"*20k, __len__→1)``
    按 1 计费、原生造出 20,021,000 字符）。非 str/bytes/bytearray 的自定义
    ``replace`` 不可证 ⇒ 拒绝。
    """
    kind = _seq_kind(a)
    pattern = b
    count_base = None
    if kind == "str":
        well_typed = isinstance(b, str) and isinstance(c, str)
        count_base = str
    elif kind in ("bytes", "bytearray"):
        well_typed = _is_bytes_like(b) and _is_bytes_like(c)
        count_base = bytes if kind == "bytes" else bytearray
        if well_typed and not isinstance(b, (bytes, bytearray)):
            pattern = bytes(memoryview(b))      # buffer 对象：count 需要 bytes-like
    elif hasattr(type(a), "replace"):
        _raise_unprovable_length(a)             # 自定义 replace 不可证 ⇒ 拒
    else:
        well_typed = False
    if kind is not None and not well_typed:
        return _ancestor_replace(a, b, c)       # 类型不符 ⇒ 原生 TypeError
    if well_typed:
        length_a = _trusted_operand_len(a)
        length_b = _bytes_like_len(b)
        length_c = _bytes_like_len(c)
        if length_b is None:
            _raise_unprovable_length(b)
        if length_c is None:
            _raise_unprovable_length(c)
        if length_b == 0:
            hits = length_a
            projected = length_a * (length_c + 1) + length_c
        else:
            hits = count_base.count(a, pattern)  # C 槽：忽略覆写的 count
            projected = (length_a + hits * (length_c - length_b)
                         if length_c > length_b else length_a)
        _check_result_size(f"$F.replace({length_a} {_unit_for(a)}, {hits} hits)",
                           projected, length_a + length_b + length_c, _unit_for(a))
        return _ancestor_replace(a, b, c)
    return a.replace(b, c)


def _fn_split(a, b): return a.split(b)


_JOIN_POINTER_BYTES = 8             # 物化 list 每项一个指针


def _join_input_size(count: int, item_total: int, sep_len: int) -> int:
    """join 的允许额度所依据的输入规模：内容量之和，**以元素数作下界**。

    ``join(['']*200000, 'x')`` 的"内容量"是 1，但输入确实有 20 万个元素
    （结果 199999 字符是非放大的）——只用内容量会误拒（第七轮验收 必修 2）。
    """
    return max(item_total + sep_len, count)


def _join_item_lengths(items: Any, sep: Any) -> Optional[Tuple[int, int, Optional[list]]]:
    """``(元素数, 元素内容总长, 物化后的元素表)``；``None`` = 交给原生抛类型错误。

    **长度信息一律不可信**（第三轮验收 H）：``__len__`` 少报会让"按 len 截断"的实现
    静默丢数据、多报会多分配，所以这里枚举到迭代器**自然耗尽**为止，绝不用
    ``len()`` 截断，也不用它决定取多少项。物化规模由两条**显式**守卫界定，且都
    发生在分配 join 结果之前：

    * **指针字节预算**：物化 list 每项 8 字节，超过 ``_EXPR_MAX_RESULT_HARD``
      字节（默认 1250 万项）即拒绝——这是"能不能把可迭代对象拿在手里"的
      **字节**口径，不是"结果长度"口径；
    * **内容下界**：``已累计内容 + 分隔符 × (已见项数-1)`` 一旦超过
      ``_allowed_result_size(已累计内容 + 分隔符)`` 立即拒绝（内容量口径）。

    类型不符（分隔符与元素不匹配、bytes 迭代出 int 等）返回 ``None``，保证原生
    ``TypeError`` 优先。字符串元素的内容量用长度，因此"20 段 × 10000 字符"这类
    **非放大**的大输入照常放行。
    """
    sep_kind = _seq_kind(sep)
    if sep_kind not in ("str", "bytes", "bytearray"):
        return None
    sep_len = _trusted_len(sep)          # C 槽：忽略子类覆写的 __len__
    if sep_len is None:
        _raise_unprovable_length(sep)
    if isinstance(items, (bytes, bytearray)):
        return None                     # 迭代出 int → 原生 TypeError
    if type(items) is str:
        return (len(items), len(items), None) if sep_kind == "str" else None
    if type(items) in (list, tuple):
        # **精确**内建类型：len() 是 CPython 自己的实现，可信；元素长度仍走 C 槽
        total = 0
        for item in items:
            if not _element_compatible(sep_kind, item):
                return None
            total += _item_render_len(item, sep_kind)
        return len(items), total, None
    # 子类（list/tuple/str 子类）与任意可迭代：**一律枚举计数**——
    # 子类可以谎报 __len__（第八轮验收 H1：谎报 0 让 len(sep)*(n-1) 项消失，
    # 预检通过后原生 join 造出整块结果，实测峰值 206MB~961MB）。
    materialized: list = []
    total = 0
    for item in items:
        if not _element_compatible(sep_kind, item):
            return None
        materialized.append(item)
        total += _item_render_len(item, sep_kind)
        seen = len(materialized)
        pointer_bytes = seen * _JOIN_POINTER_BYTES
        if pointer_bytes > _EXPR_MAX_RESULT_HARD:
            limit_items = _EXPR_MAX_RESULT_HARD // _JOIN_POINTER_BYTES
            raise _size_refusal(f"$F.join(iterable with > {limit_items} items)",
                                pointer_bytes, "bytes", _EXPR_MAX_RESULT_HARD)
        if seen >= 2:
            lower = total + sep_len * (seen - 1)
            allowed = _allowed_result_size(_join_input_size(seen, total, sep_len))
            if lower > allowed:
                raise _size_refusal(f"$F.join({seen} items)", lower,
                                    _unit_for(sep), allowed)
    return len(materialized), total, materialized


def _element_compatible(sep_kind: str, item: Any) -> bool:
    """join 元素是否与分隔符同类；bytes/bytearray 分隔符额外接受任意 buffer 协议对象。"""
    item_kind = _seq_kind(item)
    if _kinds_compatible(sep_kind, item_kind):
        return True
    return sep_kind in ("bytes", "bytearray") and item_kind is None and _is_bytes_like(item)


def _fn_join(a, b):
    """``b.join(a)``：分隔符与各元素之和即结果规模，**分配之前**预检。

    两条容易算错的细节（#68 返工）：

    * 分隔符只在 ``元素数 >= 2`` 时进入结果——``join(['b'], 's'*100001)``
      的结果就是 ``"b"``，不能因为分隔符长而拒绝；
    * 被 join 的对象可以是任意可迭代（``str``/``bytes``/视图/生成器/自定义），
      由 :func:`_join_item_lengths` 保守估算；类型不符时不做预检，交给
      ``join`` 抛原生 TypeError。

    第十二轮 M1：分隔符长度与 ``join`` 本身**一律走内建祖先 C 槽**
    （``str.__len__`` / ``str.join``…）。子类覆写的 ``__len__``/``join`` 只骗到
    额度、骗不到原生读的真实存储。
    """
    projected = _join_item_lengths(a, b)
    if projected is None:
        if _trusted_len(b) is None and hasattr(type(b), "join"):
            _raise_unprovable_length(b)     # 自定义 join 不可证 ⇒ 拒
        if isinstance(b, str):
            return str.join(b, a)           # 类型不符 ⇒ 原生 TypeError
        if isinstance(b, bytes):
            return bytes.join(b, a)
        return bytearray.join(b, a)
    n, item_total, materialized = projected
    sep_len = _trusted_operand_len(b)
    total = item_total + sep_len * (n - 1) if n > 1 else item_total
    _check_result_size(f"$F.join({n} items)", total,
                       _join_input_size(n, item_total, sep_len), _unit_for(b))
    items = materialized if materialized is not None else a
    if isinstance(b, str):
        return str.join(b, items)
    if isinstance(b, bytes):
        return bytes.join(b, items)
    return bytearray.join(b, items)


def _fn_startswith(a, b): return a.startswith(b)
def _fn_endswith(a, b): return a.endswith(b)


def _fn_concat(*args):
    """``"".join(str(arg) for arg in args)``：与 join 同族的拼接路径。

    规模用 ``_render_size``（不真的做 ``str()``）估**渲染**上界：无法界定
    （自定义 ``__str__``/``__repr__``）时在渲染**之前**拒绝。
    """
    bounds = [_render_size(arg) for arg in args]
    if any(bound is None for bound in bounds):
        unbounded = next(_type_name(arg) for arg, bound in zip(args, bounds) if bound is None)
        raise ValueError(
            f"$F.concat cannot bound the rendered size of {unbounded!r} "
            f"(渲染语义不在可证白名单里). Refusing to build an unbounded result. "
            f"{_RENDER_ESCAPE_HINT}"
        )
    total = sum(bounds)
    _check_result_size(f"$F.concat({len(args)} args)", total, total, "chars")
    return "".join([str(arg) for arg in args])


def _fn_isDigit(a): return a.isdigit()

def _fn_and(a, b): return a and b
def _fn_or(*args): return next((arg for arg in args if arg), False)
def _fn_not(a): return not a

# 比较函数：名字与 if 节点条件算子（decide.py condition_matcher）一一对应，
# 供表达式位置（assignment 的 output 等）做谓词/三元。contains 的参数序
# 与条件算子一致：contains(a, b) 表示 b 在 a 中。
def _fn_eq(a, b): return a == b
def _fn_ne(a, b): return a != b
def _fn_gt(a, b): return a > b
def _fn_gte(a, b): return a >= b
def _fn_lt(a, b): return a < b
def _fn_lte(a, b): return a <= b
def _fn_in(a, b): return a in b
def _fn_notIn(a, b): return a not in b
def _fn_contains(a, b): return b in a
def _fn_notContains(a, b): return b not in a

def _fn_ifelse(c, a, b):
    # 三元。注意急切求值：a、b 在调用前都已算出（$F 函数皆纯，无短路风险）。
    return a if c else b

def _fn_index(a, b): return a.index(b)
def _fn_slice(a, b, c): return a[b:c]
def _fn_insert(a, b, c): return a[:b] + [c] + a[b:]
def _fn_pop(a, b): return a.pop(b)
def _fn_remove(a, b): return a[:a.index(b)] + a[a.index(b) + 1:]
def _fn_reverse(a): return a[::-1]
def _fn_sort(a, sort_key=None, desc=False):
    return sorted(a, key=lambda x: x.get(sort_key) if isinstance(x, dict) else x, reverse=desc)
def _fn_getListItem(a, b): return a[b]
def _fn_delListItem(a, b): return a.pop(b)
def _fn_setListItem(a, b, c): return a.__setitem__(b, c)
def _fn_addListItem(a, b): return a + [b]
def _fn_insertListItem(a, b, c): return a[:b] + [c] + a[b:]

def _fn_keys(a): return a.keys()
def _fn_values(a): return a.values()
def _fn_items(a): return a.items()
def _fn_get(a, b, c=None): return a.get(b, c)
def _fn_set(a, b, c): return a.update({b: c})
def _fn_delete(a, b): return a.pop(b)
def _fn_clear(a): return a.clear()
def _fn_getDictValue(a, b, c=None): return a.get(b, c)
def _fn_delDictValue(a, b): return a.pop(b)
def _fn_setDictValue(a, b, c): return a.update({b: c})
def _fn_getDictKeys(a): return a.keys()
def _fn_getDictValues(a): return a.values()
def _fn_clearDict(a): return a.clear()

def _fn_now(fmt="%Y-%m-%d %H:%M:%S"): return datetime.now().strftime(fmt)
def _fn_today(fmt="%Y-%m-%d"): return date.today().strftime(fmt)

def _prepare_join(args: Tuple[Any, ...]) -> Optional[Tuple[Any, ...]]:
    """调用点级准备：**物化一次**并替换实参，投影/输入规模/真正的 join 共用它。

    list/tuple/str 等可重复迭代的入参原样返回 ``None``（不需要替换）。越界在
    **分配之前**抛 ``ValueError``；类型不符返回 ``None`` 交给原生 ``join`` 抛
    ``TypeError``。任何被调用者都无法影响这次判定的额度——它只用本次实参。
    """
    items, sep = args
    projected = _join_item_lengths(items, sep)
    if projected is None:
        return None
    count, item_total, materialized = projected
    sep_len = _trusted_operand_len(sep)         # C 槽：忽略子类覆写的 __len__（M1）
    total = item_total + sep_len * (count - 1) if count > 1 else item_total
    _check_result_size(f"$F.join({count} items)", total,
                       _join_input_size(count, item_total, sep_len), _unit_for(sep))
    if materialized is None:
        return None
    return (materialized, sep)


def _prepare_sort(args: Tuple[Any, ...]) -> Optional[Tuple[Any, ...]]:
    """``sorted()`` 也会**消费**单次迭代器（第四轮验收 必修 4）：先物化再替换实参。

    可重复迭代的对象（list/tuple/视图/str）返回 ``None``；结果元素数按元素口径
    受 hard 约束。
    """
    items = args[0]
    if isinstance(items, (list, tuple, str, bytes, bytearray)):
        return None
    if not _has_raw_slot(type(items), "__iter__"):
        return None
    try:
        if iter(items) is not items:
            return None                     # 视图等可重复迭代，不必替换
    except TypeError:
        return None
    materialized = list(items)
    _check_result_size(f"$F.sort({len(materialized)} items)", len(materialized),
                       len(materialized), "elements")
    return (materialized,) + tuple(args[1:])


# 调用点级准备表：**按函数对象**索引（不是名字）——scoped registry 用同名函数覆盖
# 内建算子时对象不同，自然走不到这里，回到探针按真实参数测量。
PROJECTIONS: Dict[Any, Any] = {
    _fn_join: _prepare_join,
    _fn_sort: _prepare_sort,
}


def _fn_json_loads(a): return json.loads(a)
def _fn_json_dumps(a): return json.dumps(a)


# ---------------------------------------------------------------------------
# Default registry factory
# ---------------------------------------------------------------------------

_default_registry: Optional[ExpressionRegistry] = None


def get_default_expression_registry() -> ExpressionRegistry:
    """Return the module-level default registry with all built-in functions.

    The registry is created lazily on first access and cached.
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = _build_default_registry()
    return _default_registry


def _reset_default_registry() -> None:
    """Reset the cached default registry. Used in tests to force re-initialization."""
    global _default_registry
    _default_registry = None


def _register_math(reg: ExpressionRegistry) -> None:
    """Register math functions — all pure, no side effects."""
    _fns: list[tuple[str, Callable, str]] = [
        ("add", _fn_add, "Add two values"),
        ("sub", _fn_sub, "Subtract b from a"),
        ("mul", _fn_mul, "Multiply two values"),
        ("div", _fn_div, "Divide a by b"),
        ("mod", _fn_mod, "Modulo a by b"),
        ("pow", _fn_pow, "Raise a to the power of b"),
        ("abs", abs, "Absolute value"),
        ("ceil", math.ceil, "Ceiling"),
        ("floor", math.floor, "Floor"),
        ("round", round, "Round to n digits"),
        ("trunc", math.trunc, "Truncate to integer"),
        ("sqrt", math.sqrt, "Square root"),
    ]
    for name, func, desc in _fns:
        reg.register(name, func, FunctionCategory.MATH, description=desc)


def _register_string(reg: ExpressionRegistry) -> None:
    """Register string functions — all pure, no side effects."""
    _fns: list[tuple[str, Callable, str]] = [
        ("lower", _fn_lower, "Lowercase"),
        ("upper", _fn_upper, "Uppercase"),
        ("capitalize", _fn_capitalize, "Capitalize first char"),
        ("title", _fn_title, "Title-case"),
        ("strip", _fn_strip, "Strip whitespace"),
        ("lstrip", _fn_lstrip, "Strip leading whitespace"),
        ("rstrip", _fn_rstrip, "Strip trailing whitespace"),
        ("replace", _fn_replace, "Replace substring"),
        ("split", _fn_split, "Split string"),
        ("join", _fn_join, "Join iterable with separator"),
        ("startswith", _fn_startswith, "Check string prefix"),
        ("endswith", _fn_endswith, "Check string suffix"),
        ("concat", _fn_concat, "Concatenate values as strings"),
        ("isDigit", _fn_isDigit, "Check if string is all digits"),
    ]
    for name, func, desc in _fns:
        reg.register(name, func, FunctionCategory.STRING, description=desc)


def _register_array(reg: ExpressionRegistry) -> None:
    """Register array functions with correct side-effect labels.

    Side-effect functions (``pop``, ``delListItem``, ``setListItem``) mutate
    the input list in-place.  They are **not thread-safe** — using them in
    concurrent execution contexts (e.g. parallel node execution, shared
    context across async tasks) may corrupt data.  Callers should either
    serialize access or copy the list before mutation.
    """
    _pure: list[tuple[str, Callable, str]] = [
        ("len", len, "Length of sequence"),
        ("length", len, "Length of sequence (alias)"),
        ("index", _fn_index, "Find index of element"),
        ("slice", _fn_slice, "Slice list"),
        ("append", _fn_append, "Append element (returns new list)"),
        ("extend", _fn_extend, "Extend list (returns new list)"),
        ("insert", _fn_insert, "Insert element (returns new list)"),
        ("remove", _fn_remove, "Remove first occurrence (returns new list)"),
        ("reverse", _fn_reverse, "Reverse list (returns new list)"),
        ("sort", _fn_sort, "Sort list (returns new list)"),
        ("getListItem", _fn_getListItem, "Get item by index"),
        ("addListItem", _fn_addListItem, "Add item (returns new list)"),
        ("insertListItem", _fn_insertListItem, "Insert item (returns new list)"),
    ]
    for name, func, desc in _pure:
        reg.register(name, func, FunctionCategory.ARRAY, description=desc)

    _side_effect: list[tuple[str, Callable, str]] = [
        ("pop", _fn_pop, "Pop item at index (mutates list)"),
        ("delListItem", _fn_delListItem, "Delete item at index (mutates list)"),
        ("setListItem", _fn_setListItem, "Set item at index (mutates list)"),
    ]
    for name, func, desc in _side_effect:
        reg.register(name, func, FunctionCategory.ARRAY, has_side_effects=True, description=desc)


def _register_dict(reg: ExpressionRegistry) -> None:
    """Register dict functions with correct side-effect labels.

    Side-effect functions (``set``, ``delete``, ``clear``, ``setDictValue``,
    ``delDictValue``, ``clearDict``) mutate the input dict in-place.  They
    are **not thread-safe** — using them in concurrent execution contexts
    (e.g. parallel node execution, shared context across async tasks) may
    corrupt data.  Callers should either serialize access or copy the dict
    before mutation.
    """
    _pure: list[tuple[str, Callable, str]] = [
        ("keys", _fn_keys, "Dict keys"),
        ("values", _fn_values, "Dict values"),
        ("items", _fn_items, "Dict items"),
        ("get", _fn_get, "Get value with default"),
        ("getDictValue", _fn_getDictValue, "Get dict value with default"),
        ("getDictKeys", _fn_getDictKeys, "Get dict keys"),
        ("getDictValues", _fn_getDictValues, "Get dict values"),
    ]
    for name, func, desc in _pure:
        reg.register(name, func, FunctionCategory.DICT, description=desc)

    _side_effect: list[tuple[str, Callable, str]] = [
        ("set", _fn_set, "Set dict value (mutates dict)"),
        ("delete", _fn_delete, "Delete dict key (mutates dict)"),
        ("clear", _fn_clear, "Clear all dict entries (mutates dict)"),
        ("setDictValue", _fn_setDictValue, "Set dict value (mutates dict)"),
        ("delDictValue", _fn_delDictValue, "Delete dict key (mutates dict)"),
        ("clearDict", _fn_clearDict, "Clear all dict entries (mutates dict)"),
    ]
    for name, func, desc in _side_effect:
        reg.register(name, func, FunctionCategory.DICT, has_side_effects=True, description=desc)


def _register_logic(reg: ExpressionRegistry) -> None:
    """Register logic functions — all pure."""
    reg.register("and", _fn_and, FunctionCategory.LOGIC, description="Logical AND")
    reg.register("or", _fn_or, FunctionCategory.LOGIC, description="Logical OR (returns first truthy value)")
    reg.register("not", _fn_not, FunctionCategory.LOGIC, description="Logical NOT")
    comparisons: list[tuple[str, Callable, str]] = [
        ("eq", _fn_eq, "Equality comparison (a == b)"),
        ("ne", _fn_ne, "Inequality comparison (a != b)"),
        ("gt", _fn_gt, "Greater than (a > b)"),
        ("gte", _fn_gte, "Greater than or equal (a >= b)"),
        ("lt", _fn_lt, "Less than (a < b)"),
        ("lte", _fn_lte, "Less than or equal (a <= b)"),
        ("in", _fn_in, "Membership test (a in b)"),
        ("notIn", _fn_notIn, "Negated membership test (a not in b)"),
        ("contains", _fn_contains, "Containment test (b in a)"),
        ("notContains", _fn_notContains, "Negated containment (b not in a)"),
        ("ifelse", _fn_ifelse, "Ternary: returns a if c else b (both branches evaluated eagerly)"),
    ]
    for name, func, desc in comparisons:
        reg.register(name, func, FunctionCategory.LOGIC, description=desc)


def _register_datetime(reg: ExpressionRegistry) -> None:
    """Register datetime functions — pure (snapshot of current time)."""
    reg.register("now", _fn_now, FunctionCategory.DATETIME, description="Current datetime formatted")
    reg.register("today", _fn_today, FunctionCategory.DATETIME, description="Current date formatted")


def _register_json(reg: ExpressionRegistry) -> None:
    """Register JSON functions — pure."""
    reg.register("json_loads", _fn_json_loads, FunctionCategory.JSON, description="Parse JSON string")
    reg.register("json_dumps", _fn_json_dumps, FunctionCategory.JSON, description="Serialize to JSON string")


def _build_default_registry() -> ExpressionRegistry:
    """Build the default registry with all built-in functions."""
    reg = ExpressionRegistry()
    _register_math(reg)
    _register_string(reg)
    _register_array(reg)
    _register_dict(reg)
    _register_logic(reg)
    _register_datetime(reg)
    _register_json(reg)
    return reg


# ---------------------------------------------------------------------------
# ExpressionEvaluator
# ---------------------------------------------------------------------------

class ExpressionEvaluator:
    """Evaluates expressions against an execution context.

    This class wraps the existing ``plaita.io.evaluate`` / ``parse_function``
    logic and exposes the structured ``ExpressionRegistry``.  The actual
    parsing and evaluation is delegated to the battle-tested functions in
    ``plaita.io`` to guarantee identical results.
    """

    def __init__(self, registry: Optional[ExpressionRegistry] = None) -> None:
        self._registry = registry or get_default_expression_registry()

    @property
    def registry(self) -> ExpressionRegistry:
        return self._registry

    def evaluate(
        self,
        value: Any,
        context: dict[str, Any],
        prefix: str = "$",
    ) -> Any:
        """Evaluate a value, resolving any expressions.

        Delegates to ``plaita.io.evaluate`` but passes this evaluator's
        ``ExpressionRegistry`` so that ``$F.func(...)`` calls resolve against
        the scoped registry.  When the registry is the default one this is
        behavior-preserving; when a custom registry is supplied, function
        lookups go through it instead of the module-level global.
        """
        from plaita.io import evaluate as _io_evaluate
        return _io_evaluate(value, context, prefix, registry=self._registry)
