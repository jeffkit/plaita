"""plaita.core.state — Distributed checkpoint schema + typed state model.

Central registry of the magic-string keys ``ExecutionContext._context`` used
for system state, plus ``CheckpointState`` — a Pydantic ``BaseModel`` that IS
the live store (replacing the loose ``Dict[str, Any]``) while exposing a
dict-like view over the **prefixed** storage keys so the expression engine
and node plugins consume it without knowing it is a model.

Why a BaseModel?
----------------
Historically ``ExecutionContext._context`` was a ``Dict[str, Any]`` with keys
like ``f"{pfx}LAST_NODE"`` / ``f"{pfx}BRANCH"`` built inline across
runner.py / executor.py / assignment.py / event/core.py. Renaming any key was
multi-file shotgun surgery, and the schema was implicit. ``CheckpointState``
makes the schema explicit and centralises every magic string here.

Binary-compatibility with existing checkpoints
-----------------------------------------------
Redis/SQL persisted checkpoints are JSON of the old ``dict(self._context)``.
``to_checkpoint_dict()`` / ``from_checkpoint_dict()`` round-trip the **exact**
key set (presence-tracked via ``_present``), so a checkpoint written by the
old dict-based code loads and re-saves losslessly. See
``tests/unit/test_state.py`` for the round-trip property test.

Note: the storage layer already owns a class named ``ExecutionState``
(``plaita.storage.base.ExecutionState`` — the persisted envelope holding
``execution_id`` / ``flow_id`` / ``context`` / ``status`` ...). The core
checkpoint model is therefore named ``CheckpointState`` to avoid the collision.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from .errors import StateSnapshotError


# Sentinel for "field has not been set / key is absent". Distinct from ``None``
# because a checkpoint value may legitimately be ``None`` (e.g. ``$INPUT: null``)
# and we must not treat that as "missing".
_UNSET = object()


# 快照出口的环与深度保护（plaita#107）。
#
# 历史实现是无 memo、无环检测、无深度上限的朴素递归：任何被回指的容器
# （实测触发路径 ``output: "$NODE"`` —— 整个状态表被写回自身）让它沿环递归到
# Python 栈顶，generator 裸 ``RecursionError``、distributed 被
# ``_error_normalization.raise_distributed_error`` 归一化成误导性的
# ``FlowErrorException("maximum recursion depth exceeded")``（节点 id / 路径全不可见）；
# 即使无环，深嵌套值（实测 2500 层）也会在 checkpoint 出口撞同一个坑。
#
# 现在改为**显式栈迭代物化**：数据深度不再消耗 Python 递归帧，2500 层与 50 层同价。
# 两条降级语义都显式可见，绝不静默产出损坏结构：
#   * 环 —— 命中「当前 DFS 路径上已出现的容器」时按 ``<recursive: path>`` 占位符
#     截断。自引用快照无法 JSON 序列化，若原样保留，checkpoint 会在宿主序列化
#     那一跳才崩且现场没有任何路径信息；占位符把损坏点钉在快照出口。
#   * 深度 —— 仍设防御性上限 ``_SNAPSHOT_MAX_DEPTH``，超限抛
#     :class:`~plaita.core.errors.StateSnapshotError`（消息含深度与路径）。上限存在的
#     意义是防「无界深层嵌套」这一类病态结构，不是防 Python 栈——递归限制已与数据
#     深度无关，故取足以覆盖真实 payload 的 1 万层（issue 实测 2500 层的 4x）。
#     **不要**把它当作 CPU/内存边界：本次物化没有 memo，共享 DAG（同一子对象被多处
#     引用）仍按引用次数指数展开（实测 depth=18 约 43 万容器 / 0.43s，再深即耗尽），
#     与该上限无关；此性质在改动前同样存在，本单不承诺 CPU/内存有界。
_SNAPSHOT_MAX_DEPTH = 10_000
# 环占位符：字符串是 JSON 可表达的，落盘/emit 不会二次炸。
_SNAPSHOT_CYCLE_MARK = "<recursive: {path}>"
# 路径渲染的截断长度（超限路径本身可达上万段，整段回显会淹没日志）。
_SNAPSHOT_PATH_HEAD = 4
_SNAPSHOT_PATH_TAIL = 2
_SNAPSHOT_DONE = object()


def _snapshot_path(frames: List[tuple], key: Any) -> str:
    """把迭代栈还原成 ``$['a'][0]['b']`` 形态的容器路径（错误/占位符专用）。

    只在检测到环或超深时调用，热路径零成本；超长路径按头 ``_SNAPSHOT_PATH_HEAD`` /
    尾 ``_SNAPSHOT_PATH_TAIL`` 段截断，中间以 ``'...'`` 标记。
    """
    segments = [frame[3] for frame in frames[1:]]
    segments.append(key)
    if len(segments) > _SNAPSHOT_PATH_HEAD + _SNAPSHOT_PATH_TAIL + 1:
        segments = (
            segments[:_SNAPSHOT_PATH_HEAD] + ["..."] + segments[-_SNAPSHOT_PATH_TAIL:]
        )
    return "$" + "".join(f"[{segment!r}]" for segment in segments)


def _snapshot_items(value: Any):
    """dict → ``iter(items())``；list/tuple → ``enumerate``（键即下标）。"""
    if isinstance(value, dict):
        return iter(value.items())
    return enumerate(value)


def _snapshot_container(root: Any) -> Any:
    """显式栈物化 ``root``（dict/list/tuple），环截断、超深抛 ``StateSnapshotError``。

    栈本身就是当前 DFS 路径：``seen_ids`` 只收在场祖先（出栈即移除），所以
    「同一个子容器被两个兄弟键共享」这类 DAG 不会被误判成环，仍按值各拷一份。
    """
    root_copy = {} if isinstance(root, dict) else [None] * len(root)
    # frame: (src, dst, items_iter, key_in_parent)，root 无父键。栈即当前 DFS 路径，
    # 元组而非 list：每层一个，热路径上这是每容器一次的分配。
    frames: List[tuple] = [(root, root_copy, _snapshot_items(root), None)]
    seen_ids = {id(root)}
    tuple_fixups = []  # (parent_dst, key, list_dst)：tuple 不可变，出栈后回填
    while frames:
        frame = frames[-1]
        item = next(frame[2], _SNAPSHOT_DONE)
        if item is _SNAPSHOT_DONE:
            seen_ids.discard(id(frame[0]))
            frames.pop()
            continue
        key, child = item
        dst = frame[1]
        if isinstance(child, _LazyRootSnapshot):
            dst[key] = child.materialize()
            continue
        if isinstance(child, (dict, list, tuple)):
            depth = len(frames)
            if depth > _SNAPSHOT_MAX_DEPTH:
                raise StateSnapshotError(message=(
                    f"state snapshot depth {depth} exceeds limit {_SNAPSHOT_MAX_DEPTH} "
                    f"at path {_snapshot_path(frames, key)}"
                ))
            if id(child) in seen_ids:
                dst[key] = _SNAPSHOT_CYCLE_MARK.format(path=_snapshot_path(frames, key))
                continue
            child_copy = {} if isinstance(child, dict) else [None] * len(child)
            dst[key] = child_copy
            if isinstance(child, tuple):
                tuple_fixups.append((dst, key, child_copy))
            seen_ids.add(id(child))
            frames.append((child, child_copy, _snapshot_items(child), key))
            continue
        if isinstance(child, (set, frozenset)):
            # 集合元素必可哈希（不可变），浅重建即快照
            dst[key] = child.copy()
            continue
        dst[key] = child
    for parent_dst, key, child_copy in reversed(tuple_fixups):
        parent_dst[key] = tuple(child_copy)
    if isinstance(root, tuple):
        return tuple(root_copy)
    return root_copy


def _snapshot_value(value: Any) -> Any:
    """物化 dict/list/tuple 容器为私有快照（C1-2，2026-10 评审修复包 C1；#107 加固）。

    历史上 checkpoint/context 出口（``to_checkpoint_dict``、``$PARENT`` 快照）
    是浅拷贝——``$NODE`` 等嵌套容器按引用共享，消费方（分布式宿主异步序列化、
    子流程 ``$PARENT``）拿到的是 live 别名：写穿透回运行态、后续执行让已"落盘"
    的数据漂移。快照只在容器边界重建；JSON 不可表达的非容器对象（与历史行为
    一致）仍按引用共享——它们本就无法进入 checkpoint 序列化。

    #107 起改由 :func:`_snapshot_container` 显式栈物化：不再有 Python 递归深度
    限制（深嵌套值不再 ``RecursionError``），容器环按 ``<recursive: path>``
    占位符显式截断，超过 ``_SNAPSHOT_MAX_DEPTH`` 抛 ``StateSnapshotError``。
    """
    if isinstance(value, _LazyRootSnapshot):
        return value.materialize()
    if isinstance(value, (dict, list, tuple)):
        return _snapshot_container(value)
    if isinstance(value, (set, frozenset)):
        # 集合元素必可哈希（不可变），浅重建即快照
        return value.copy()
    return value


class _LazyRootSnapshot(Mapping):
    """按顶层根键**惰性物化**的快照视图（``$PARENT`` 专用，C1-2 折中设计）。

    动机：``$PARENT`` 必须是真快照（C1-2：浅拷贝让子流程与父运行态双向写
    穿），但子流程 setup 发生在热路径上——while/for 每轮一次，全量深快照会
    重新引入 loop.py ML1 修掉的「千次迭代 = 千次全量深拷贝」回归（实证：
    300 轮 × ~1MB 上下文 7.7s > 5.0s 上限）。惰性方案：构造时钉住**顶层键集
    与各根键当时的值对象**（一次 O(根键数) 的浅拷，与上下文总大小无关），
    **每个根键在首次读取时**才做深快照并缓存；从未被读的根键零成本。

    冻结语义（plaita#72 修正）：历史实现把 **live 源**存进来，只在首读时才
    ``self._source[key]``——「首读时物化」只有在父执行于子执行存续期内被阻塞
    时才等价于「构造时快照」。``InlineFlow`` 满足该前提，``Parallel`` 的
    fire-and-forget 后台分支**不满足**：父在分支提交后立刻继续推进并写
    ``$NODE``，后台分支稍后的首读会把父的"未来态"读进"快照"（非确定性）。
    现在构造时就浅拷顶层映射，构造后父对根键的新增/替换（``update_node_result``
    是 copy-on-write，换的是新 dict）对快照不可见；深物化仍按首读惰性。

    跨进程边界（checkpoint 落盘/恢复）走 ``to_checkpoint_dict`` /
    ``from_checkpoint_dict``，经 ``_snapshot_value`` 完全物化为 plain dict，
    惰性对象不出进程。

    兼容面：表达式引擎 ``_get_attr`` 按「有 ``__getitem__`` + ``get``」识别
    dict-like，``Mapping`` 天然满足；``__eq__``/``__repr__``/``__deepcopy__``
    按物化视图实现，测试断言与 deepcopy 均按快照值工作。
    """

    __slots__ = ("_source", "_cache")

    def __init__(self, source: Any) -> None:
        # 顶层浅拷：键集与「构造这一刻的根键值对象」一并钉住（#72）。值本身
        # 不在构造时深物化，首读路径与热路径成本不变。
        self._source = dict(source)
        self._cache: Dict[Any, Any] = {}

    def __getitem__(self, key: Any) -> Any:
        try:
            return self._cache[key]
        except KeyError:
            pass
        value = _snapshot_value(self._source[key])  # 源缺键 → KeyError 原样传播
        self._cache[key] = value
        return value

    def __iter__(self):
        return iter(self._source)

    def __len__(self) -> int:
        return len(self._source)

    def materialize(self) -> Dict[Any, Any]:
        """物化为 plain dict（每个已缓存根键直接复用，其余首次物化）。"""
        return {k: self[k] for k in self}

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _LazyRootSnapshot):
            other = other.materialize()
        elif not isinstance(other, Mapping):
            return NotImplemented
        return len(self) == len(other) and all(
            k in other and self[k] == other[k] for k in self
        )

    __hash__ = None  # 可变缓存，不可哈希

    def __repr__(self) -> str:
        return f"_LazyRootSnapshot({dict(self)!r})"

    def __deepcopy__(self, memo: Dict[int, Any]) -> Dict[Any, Any]:
        return {k: copy.deepcopy(self[k], memo) for k in self}


class CheckpointSchema:
    """Static description of the checkpoint key namespace.

    A checkpoint key is one of:

    - A *system key*: pinned here in ``SYSTEM_KEY_NAMES``. Written and read by
      core/runner code, survives ``to_dict`` / ``from_dict``.
    - An *expression key*: ``$INPUT`` / ``$NODE`` / ``$GLOBAL`` / ``$PARENT``
      / ``$ENV`` — prefix plus one of the configurable expression-namespace
      names. The names themselves live on ``ExecutionContext``
      (``express_input_name`` etc); only the static suffixes are registered here.
    - ``EXPRESS_PREFIX`` — the literal unprefixed key recording the prefix in
      use (so a checkpoint can be restored even if the prefix was customised).
    - *Node-local keys*: anything else. ``set_state("anything", ...)`` is
      allowed but **not part of the contract** — refactor or rename freely.
    """

    # System keys (prefixed by ``express_prefix``, default ``$``).
    # The suffix is fixed (not configurable): these are core-internal state.
    SYSTEM_KEY_NAMES = (
        "LAST_NODE",     # most recently executed node id
        "BRANCH",        # branch taken by the most recent branching node
        "FLOW_ID",       # flow id of the currently executing flow
        "EXECUTION_ID",  # unique id for this execution (regenerated by clean())
    )

    # Unprefixed keys.
    BARE_KEY_NAMES = (
        "EXPRESS_PREFIX",  # records the prefix string so checkpoints can be restored
    )

    # Expression namespace names (the bit between ``$`` and the rest).
    EXPRESSION_NAMES = (
        "INPUT",
        "NODE",
        "GLOBAL",
        "PARENT",
        "ENV",
    )

    @classmethod
    def system_keys(cls, prefix: str = "$") -> List[str]:
        return [_key(prefix, n) for n in cls.SYSTEM_KEY_NAMES]

    @classmethod
    def bare_keys(cls) -> List[str]:
        return list(cls.BARE_KEY_NAMES)

    @classmethod
    def all_known_keys(cls, prefix: str = "$") -> List[str]:
        """Every key core owns. Used by ``validate_checkpoint`` for drift warnings."""
        return (
            cls.system_keys(prefix)
            + cls.bare_keys()
            + [_key(prefix, n) for n in cls.EXPRESSION_NAMES]
        )


def _key(prefix: str, name: str) -> str:
    return f"{prefix}{name}"


def validate_checkpoint(data: Dict[str, Any], prefix: str = "$") -> List[str]:
    """Inspect a checkpoint dict and return a list of drift warnings.

    A *drift warning* is any key that looks like a system/expression key but
    is not in the official schema — usually a typo or a half-finished rename.
    Unknown node-local keys are NOT warnings (they are a supported escape
    hatch).

    Returns an empty list when the checkpoint is clean.
    """
    warnings: List[str] = []
    known = set(CheckpointSchema.all_known_keys(prefix))
    for k in data:
        if k in known or k in CheckpointSchema.bare_keys():
            continue
        # Looks like a system key (UPPER_CASE under prefix) but isn't registered?
        if k.startswith(prefix) and k[len(prefix):].isupper():
            warnings.append(
                f"checkpoint contains {k!r} which looks like a system key but is "
                f"not in CheckpointSchema; if it is meant to be system state, "
                f"register it in CheckpointSchema.SYSTEM_KEY_NAMES."
            )
    return warnings


# ---------------------------------------------------------------------------
# CheckpointState — the live, typed checkpoint model
# ---------------------------------------------------------------------------

# Field name <-> storage key mapping is built per-instance because the prefix
# and the expression-namespace names are configurable. The static suffixes
# come from CheckpointSchema; the instance supplies the prefix + names.
# Order matters for stable ``to_checkpoint_dict`` output (matches the order
# keys were historically written: system keys, then expression keys, then
# EXPRESS_PREFIX). Extras (node-local keys) preserve their own insertion order.


class CheckpointState(BaseModel):
    """Typed model backing ``ExecutionContext``'s distributed-checkpoint state.

    The model holds the typed system/expression fields plus arbitrary
    node-local extras (``extra="allow"``). It exposes a **dict-like interface
    over the prefixed storage keys** (``$LAST_NODE`` etc) so callers that
    historically received ``Dict[str, Any]`` — the expression engine
    (``context[root_key]``), ``Condition.match``, ``deepcopy``, node plugins
    (``execution.context.get(...)``) — work unchanged.

    Presence of each schema key is tracked in ``_present`` so that
    ``to_checkpoint_dict()`` reproduces the exact key set of the source
    checkpoint (a schema field that was never written is not emitted). This
    is what makes round-trip with old dict-based checkpoints lossless.
    """

    model_config = ConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
        # Mutation happens via __setitem__/setattr throughout the run; do not
        # re-validate on every assignment (also lets Any-typed fields hold
        # arbitrary JSON-able values without coercion changing them).
        validate_assignment=False,
    )

    # -- typed data fields (default _UNSET == "key absent") --
    last_node_id: Any = _UNSET
    last_branch: Any = _UNSET
    flow_id: Any = _UNSET
    execution_id: Any = _UNSET
    input_value: Any = _UNSET
    node_results: Any = _UNSET
    global_context: Any = _UNSET
    parent_context: Any = _UNSET
    env: Any = _UNSET

    # -- routing config (not serialised; used to map fields <-> storage keys) --
    prefix: str = Field(default="$", exclude=True)
    input_name: str = Field(default="INPUT", exclude=True)
    parent_name: str = Field(default="PARENT", exclude=True)
    node_name: str = Field(default="NODE", exclude=True)
    global_name: str = Field(default="GLOBAL", exclude=True)
    env_name: str = Field(default="ENV", exclude=True)

    # -- presence tracker: which storage keys are currently "set" --
    # Holds the *storage* key strings (e.g. ``$LAST_NODE``, ``EXPRESS_PREFIX``).
    # Private attr (not a field): not serialised, not validated, mutated freely.
    _present: set = PrivateAttr(default_factory=set)
    # routing 缓存（2026-10 BFF 热路径评审）：__getitem__/__setitem__ 每次访问
    # 重建 key<->field 映射实测 ~13µs/节点。路由元组不变即命中；prefix 经
    # EXPRESS_PREFIX 写入变更时元组失配自动重建。
    _route_cache_field: Optional[Tuple[tuple, Dict[str, str]]] = PrivateAttr(default=None)
    _route_cache_key: Optional[Tuple[tuple, Dict[str, str]]] = PrivateAttr(default=None)

    # ------------------------------------------------------------------
    # key <-> field routing
    # ------------------------------------------------------------------

    def _routing(self) -> tuple:
        return (self.prefix, self.input_name, self.parent_name,
                self.node_name, self.global_name, self.env_name)

    def _field_to_key(self) -> Dict[str, str]:
        """Map field name -> storage key for the current prefix/names."""
        route = self._routing()
        cached = self._route_cache_field
        if cached is not None and cached[0] == route:
            return cached[1]
        p = self.prefix
        mapping = {
            "last_node_id": _key(p, "LAST_NODE"),
            "last_branch": _key(p, "BRANCH"),
            "flow_id": _key(p, "FLOW_ID"),
            "execution_id": _key(p, "EXECUTION_ID"),
            "input_value": _key(p, self.input_name),
            "node_results": _key(p, self.node_name),
            "global_context": _key(p, self.global_name),
            "parent_context": _key(p, self.parent_name),
            "env": _key(p, self.env_name),
        }
        self._route_cache_field = (route, mapping)
        return mapping

    def _key_to_field(self) -> Dict[str, str]:
        route = self._routing()
        cached = self._route_cache_key
        if cached is not None and cached[0] == route:
            return cached[1]
        mapping = {v: k for k, v in self._field_to_key().items()}
        self._route_cache_key = (route, mapping)
        return mapping

    @property
    def _extras(self) -> Dict[str, Any]:
        return self.__pydantic_extra__ or {}

    # ------------------------------------------------------------------
    # dict-like protocol (over prefixed storage keys)
    # ------------------------------------------------------------------

    def __getitem__(self, key: str) -> Any:
        if key == "EXPRESS_PREFIX":
            if "EXPRESS_PREFIX" in self._present:
                return self.prefix
            raise KeyError(key)
        mapping = self._key_to_field()
        if key in mapping:
            if key in self._present:
                return getattr(self, mapping[key])
            raise KeyError(key)
        extras = self._extras
        if key in extras:
            return extras[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        if key == "EXPRESS_PREFIX":
            # EXPRESS_PREFIX records the prefix string; routing prefix follows.
            self.prefix = value
            self._present.add("EXPRESS_PREFIX")
            return
        mapping = self._key_to_field()
        if key in mapping:
            object.__setattr__(self, mapping[key], value)
            self._present.add(key)
            return
        if self.__pydantic_extra__ is None:
            # pydantic lazily allocates the extras dict; force it so mutation
            # sticks without going through model_validate.
            self.__pydantic_extra__ = {}
        self.__pydantic_extra__[key] = value

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key == "EXPRESS_PREFIX":
            return key in self._present
        return key in self._present or key in self._extras

    def __iter__(self):
        # _present already holds storage keys; extras hold their own keys.
        for k in self._present:
            yield k
        for k in self._extras:
            if k not in self._present:
                yield k

    def __len__(self) -> int:
        return len(self._present) + len(self._extras)

    def keys(self):
        return list(self.__iter__())

    def items(self):
        return [(k, self[k]) for k in self]

    def values(self):
        return [self[k] for k in self]

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __eq__(self, other: object) -> bool:
        if isinstance(other, CheckpointState):
            return dict(self) == dict(other)
        if isinstance(other, dict):
            return dict(self) == other
        return NotImplemented

    __hash__ = None  # explicit: CheckpointState is mutable, not hashable.

    # ------------------------------------------------------------------
    # serialisation
    # ------------------------------------------------------------------

    def to_checkpoint_dict(self) -> Dict[str, Any]:
        """Return the plain ``Dict[str, Any]`` storage form.

        Lossless inverse of ``from_checkpoint_dict``: only keys that have been
        set are emitted, preserving the exact key set of the source checkpoint.

        C1-2（2026-10 评审修复包 C1）：值经 ``_snapshot_value`` 物化——返回值与
        live state 不再共享嵌套容器，消费方（checkpoint 落盘、分布式每步 emit
        的 context、子流程 ``$PARENT``）变异/继续执行都不会写穿或漂移。此边界
        每次调用 O(状态总量)；分布式每步一次，与宿主侧序列化同量级
        （见 tests/unit/test_clean_c1_state.py 的计时基线）。

        #107：物化改为显式栈迭代（数据深度与 Python 递归深度解耦，深嵌套值不再
        ``RecursionError``），容器环按 ``<recursive: path>`` 占位符显式截断，
        超 ``_SNAPSHOT_MAX_DEPTH`` 抛 ``StateSnapshotError``（消息含深度与路径）。
        """
        return {k: _snapshot_value(self[k]) for k in self}

    @classmethod
    def from_checkpoint_dict(
        cls,
        data: Dict[str, Any],
        *,
        prefix: str = "$",
        input_name: str = "INPUT",
        parent_name: str = "PARENT",
        node_name: str = "NODE",
        global_name: str = "GLOBAL",
        env_name: str = "ENV",
    ) -> "CheckpointState":
        """Build a ``CheckpointState`` from an old-style prefixed dict.

        ``prefix`` / ``input_name`` / ... are fallbacks used when the dict does
        not carry ``EXPRESS_PREFIX`` (e.g. a context that was never
        ``setup_flow``-ed). When ``EXPRESS_PREFIX`` is present it wins and the
        keys are interpreted under that prefix.
        """
        resolved_prefix = data.get("EXPRESS_PREFIX", prefix) if isinstance(data, dict) else prefix
        obj = cls(
            prefix=resolved_prefix,
            input_name=input_name,
            parent_name=parent_name,
            node_name=node_name,
            global_name=global_name,
            env_name=env_name,
        )
        # C1-2：恢复侧同样走快照——浅恢复会让 live state 与调用方持有的
        # checkpoint dict 共享嵌套容器，update_node_result 原地 mutate 直接
        # 写穿调用方已拿到的"历史"checkpoint（分布式每步 emit→回传链路实证）。
        for key, value in (data or {}).items():
            obj[key] = _snapshot_value(value)
        return obj

    # ------------------------------------------------------------------
    # state-mutation helpers (called by ExecutionContext; keep the flow
    # runtime's state logic on the state model, not on the context facade)
    # ------------------------------------------------------------------

    @classmethod
    def fresh(
        cls,
        *,
        prefix: str = "$",
        input_name: str = "INPUT",
        parent_name: str = "PARENT",
        node_name: str = "NODE",
        global_name: str = "GLOBAL",
        env_name: str = "ENV",
        execution_id: str,
        env: Dict[str, Any],
    ) -> "CheckpointState":
        """A clean state carrying only ``$EXECUTION_ID`` and ``$ENV``.

        Equivalent to what ``ExecutionContext.clean()`` needs before any
        ``setup_flow``: a fresh model with the run-scoped id and the exposed
        environment snapshot. Everything else is absent.
        """
        obj = cls(
            prefix=prefix, input_name=input_name, parent_name=parent_name,
            node_name=node_name, global_name=global_name, env_name=env_name,
        )
        obj[f"{prefix}EXECUTION_ID"] = execution_id
        obj[f"{prefix}{env_name}"] = env
        return obj

    def setup_flow(
        self,
        *,
        input_value: Any,
        parent_context: Dict[str, Any],
        global_context: Dict[str, Any],
        flow_id: str,
        env: Dict[str, Any],
    ) -> None:
        """Populate the flow-level state keys (``$INPUT``/``$PARENT``/...).

        ``parent_context`` is a deep-value snapshot of the parent's context
        (see ``ExecutionContext.setup_flow`` — a live ``CheckpointState`` would
        break expression path walking via ``_get_attr``, and a shallow dict
        aliased ``$PARENT.NODE`` to the parent's live ``$NODE``; C1-2 fix,
        2026-10 评审修复包 C1).
        """
        p = self.prefix
        self[f"{p}{self.input_name}"] = input_value
        self[f"{p}{self.parent_name}"] = parent_context
        self[f"{p}{self.global_name}"] = global_context
        self["EXPRESS_PREFIX"] = p
        self[f"{p}FLOW_ID"] = flow_id
        self[f"{p}{self.env_name}"] = env

    def update_node_result(self, node_id: str, result: Any) -> None:
        """Record a node's result under ``$NODE`` (creating the map if absent).

        #107：**按值入库**。历史实现 ``node_results = self.get(key, {})`` 拿到的是
        ``$NODE`` 的 live dict，再原地写入 ``result``（同样按引用）——两个方向都
        暴露别名：

        * ``output: "$NODE"`` 把整表写回自身 ⇒ 自引用容器，快照出口沿环递归炸栈
          （generator/distributed 每步都要 ``to_checkpoint_dict``）；
        * 更隐蔽的是后续步骤原地改写「已记录」的历史结果（``$NODE['a'] is $NODE``
          为 True，``b`` 步之后 ``a`` 的条目凭空多出 ``b`` 键）。

        现在整表先浅拷一层（打断 live 绑定）、结果经 ``_snapshot_value`` 深物化
        （与 checkpoint 出口同一语义：环截断/超深抛 ``StateSnapshotError``），
        ``$NODE.<node>`` 因此是**私有快照**，不随执行漂移。
        """
        key = f"{self.prefix}{self.node_name}"
        node_results = dict(self.get(key, {}))
        node_results[node_id] = _snapshot_value(result)
        self[key] = node_results
