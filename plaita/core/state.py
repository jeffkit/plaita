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
from .types import PERSISTABLE_SCALAR_TYPES


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

#: r9 Finding C 快速通道：**精确**不可变标量作为 dict 键 / set 成员时无需归一化
#: （它们已经是数据模型值，且不可变 ⇒ 不是活别名通道）。``tuple``/``frozenset``
#: 不在此列：它们内部还可能嵌宿主子类对象，必须递归过一遍 ``_engine_copy``。
_SNAPSHOT_EXACT_KEY_TYPES = frozenset(PERSISTABLE_SCALAR_TYPES)


def _snapshot_key_or_member(value: Any, role: str) -> Any:
    """dict 键 / set 成员：与 :func:`~plaita.core.expression_parser._engine_copy`
    同一判据 —— 归一化成普通数据模型值，否则**具名**拒绝（r9 Finding B/C）。

    r8 只把**值**路径改成了基类协议重建；键与集合成员仍按引用塞进快照
    （``key_is_live=True`` / ``el_is_live=True``，无任何错误）：宿主 ``dict`` 子类做
    **键**、宿主/``__class__`` 伪造对象做**成员**都会以活别名留在快照里。此外
    ``class HashableDict(dict): __hash__ = ...`` 做成员时会被归一化成普通 ``dict``，
    重建集合那一跳抛裸 ``TypeError: unhashable type: 'dict'``（DSL 可达）。

    r9 选择 (a)：键/成员走与 ``_engine_copy`` **完全相同**的归一化入口（宿主类被
    归一化掉、类伪造被具名拒绝），归一化后不可哈希的（例如 ``HashableDict`` →
    ``dict``）同样具名拒绝（``StateSnapshotError``，带真实类型名）。因此快照里
    不再有按引用共享的键/成员。仍按引用共享的只剩**重建面之外的值**——既有宿主对象
    （非容器），也有 ``types.MappingProxyType`` / ``collections.ChainMap`` /
    ``collections.UserDict`` / ``os._Environ`` 这类**容器**（枚举与"为何共享安全"
    见 :func:`_snapshot_value`，plaita#162）。
    """
    if type(value) in _SNAPSHOT_EXACT_KEY_TYPES:
        return value
    # 延迟导入：state.py 是 checkpoint 层的轻量依赖，不为此把 pyparsing 拉进
    # import 图；同时这也是"单一拷贝入口"纪律的复用而非第二条实现。
    from .expression_parser import _EngineCopyRefused, _engine_copy, unhashable_offender

    try:
        copied = _engine_copy(value)
    except _EngineCopyRefused as exc:
        raise StateSnapshotError(message=(
            f"state snapshot cannot represent {type(value).__name__!r} as a {role}: "
            f"{exc}")) from exc
    try:
        hash(copied)
    except TypeError as exc:
        # r10：嵌套 offender（例如 ``(HashableDict({...}),)`` 做键）要点名**真实**
        # 宿主类型，而不是外层的 ``'tuple'``；与 ``_engine_copy`` 的具名拒绝同判据。
        offender_copied, offender_original = unhashable_offender(copied, value)
        raise StateSnapshotError(message=(
            f"state snapshot cannot represent {type(offender_original).__name__!r} as a "
            f"{role}: it normalises to an unhashable {type(offender_copied).__name__} "
            f"({exc})")) from exc
    return copied


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


#: plaita#69 r8 —— 快照路径的**基类协议白名单**。
#:
#: 快照是**引擎自有重建**：只允许通过下面这些 C 基类方法读取宿主容器
#: （``dict.items(sub)`` / ``list.__iter__(sub)`` / ``tuple.__iter__(sub)`` /
#: ``set.__iter__(sub)`` / ``frozenset.__iter__(sub)`` / ``<base>.__len__(sub)``）。
#: 这些描述符直接访问 CPython 的容器存储，**不会**落回子类覆写的 ``items`` /
#: ``__iter__`` / ``__getitem__`` / ``copy``，因此宿主钩子零调用。
#:
#: r7 的实现用 ``value.items()`` / ``enumerate(value)`` / ``value.copy()``：
#: ``set`` 子类把 ``copy()`` 覆写成 ``return self`` 时，快照**就是活宿主对象**
#: （r8 G1 的 ``HookSet`` 反例，``$GLOBAL.s is <flow 定义里的 set>``）。
#: 机械守卫见 ``tests/unit/test_issue69_single_copy_point.py`` 的
#: ``snapshot_host_protocol_calls``（allow-list 即本元组的基类名）。
_SNAPSHOT_BASE_PROTOCOLS = (dict, list, tuple, set, frozenset)


def _snapshot_rebuild_error(value: Any, kind: str, exc: TypeError) -> "StateSnapshotError":
    """把基类协议对「假容器」抛出的裸 ``TypeError`` 变成**具名**错误（r8 G3）。

    ``__class__`` 被伪造成 ``dict``/``list``/… 的宿主对象能骗过 ``isinstance``，
    却过不了 C 描述符的实体检查（``descriptor 'items' for 'dict' objects doesn't
    apply to a 'Liar' object``）。裸 ``TypeError`` 是不可归因的；这里带上真实
    ``type(value).__name__`` 与期望的基类名，仍是 fail-closed（不静默共享活对象）。
    """
    return StateSnapshotError(message=(
        f"state snapshot cannot rebuild {type(value).__name__!r} as {kind}: {exc} "
        f"(host class spoofing __class__? not a real {kind})"))


def _snapshot_mapping_outside_storage_error(value: Any,
                                            path: Optional[str]) -> "StateSnapshotError":
    """r9 Finding D / r10 Finding D1：``dict`` 子类的自述内容可能不在真实存储里。

    基类 ``dict.items`` 读的是**真实 dict 存储**（对 ``class M(MutableMapping, dict)``
    恒空），r7 走 ``Mapping.items`` 却能看到数据 —— r7→r8 之间同一形状的读数从
    ``{'a': 1}`` 变成 ``{}``，即**静默丢数据**。r10 补上镜像形状：``class
    ItemsLiar(dict)``（数据在 ``_d``、用 Python 覆写映射协议自述）与
    ``class DictFirstMapping(dict, Mapping)``（``Mapping`` 排在 ``dict`` 之后）——两者
    连 MRO 判据都过不了，同样静默产出 ``{}``。引擎的两条拷贝路径都把"真实 ``dict``
    存储"当数据模型，因此这里**具名**拒绝（并点名被覆写的方法）而不是产出空容器。
    """
    from .expression_parser import mapping_storage_refusal_detail

    detail = mapping_storage_refusal_detail(value)
    where = "" if path is None else f" at path {path}"
    return StateSnapshotError(message=(
        f"state snapshot cannot represent {type(value).__name__!r}{where}: this dict "
        f"subclass {detail}, so the base-class rebuild would silently produce an empty "
        f"dict (the real dict storage is the data model)"))


def _snapshot_isinstance(value: Any, classes: Any) -> bool:
    """``isinstance`` 的**具名**版本（plaita#69 r11 finding 5，零宿主钩子归因）。

    ``isinstance(v, C)`` 在真实类型不命中时会回落去读 ``v.__class__``（经
    ``tp_getattro`` ⇒ 宿主 ``__getattribute__``）：宿主把它覆写成抛异常时，快照路径会
    漏出**裸** ``RuntimeError``（r6 起就存在，DSL 可达）而不是具名
    :class:`StateSnapshotError`。``type(value)`` 读的是 C 的 ``ob_type`` 槽，不经宿主
    钩子，所以这里仍然安全。（语义与 ``isinstance`` 一致 —— 依旧采信伪造成容器的
    ``__class__``，好让 C 基类描述符把它具名拒绝。）
    """
    try:
        return isinstance(value, classes)
    except Exception as exc:                # noqa: BLE001 - 宿主 __getattribute__ 的任意异常
        raise StateSnapshotError(message=(
            f"state snapshot cannot probe {type(value).__name__!r}: its host class "
            f"raises from __getattribute__ while isinstance() reads __class__ "
            f"({exc})")) from exc


def _snapshot_len(value: Any) -> int:
    """基类 ``__len__``：覆写 ``__len__`` 的容器子类不影响快照的真实长度。"""
    if isinstance(value, dict):
        try:
            return dict.__len__(value)
        except TypeError as exc:
            raise _snapshot_rebuild_error(value, "dict", exc) from exc
    if isinstance(value, tuple):
        try:
            return tuple.__len__(value)
        except TypeError as exc:
            raise _snapshot_rebuild_error(value, "tuple", exc) from exc
    try:
        return list.__len__(value)
    except TypeError as exc:
        raise _snapshot_rebuild_error(value, "list", exc) from exc


def _snapshot_set(value: Any) -> Any:
    """用基类 ``__iter__`` 重建**普通** ``set``/``frozenset``（零宿主钩子）。

    r9 Finding B/C：成员逐个过 :func:`_snapshot_key_or_member`（归一化 or 具名拒绝），
    不再按引用塞进快照。
    """
    if isinstance(value, frozenset):
        try:
            source = frozenset.__iter__(value)
        except TypeError as exc:
            raise _snapshot_rebuild_error(value, "frozenset", exc) from exc
        members = {_snapshot_key_or_member(item, "frozenset member") for item in source}
        return frozenset(members)
    try:
        source = set.__iter__(value)
    except TypeError as exc:
        raise _snapshot_rebuild_error(value, "set", exc) from exc
    return {_snapshot_key_or_member(item, "set member") for item in source}


def _snapshot_items(value: Any):
    """dict → ``iter(dict.items(sub))``；list/tuple → 基类 ``__iter__`` + enumerate。

    r8 G1：全部走基类协议（见 ``_SNAPSHOT_BASE_PROTOCOLS``）——宿主覆写的
    ``items`` / ``__iter__`` 一次都不会被调用；假容器在这里转成具名
    :class:`StateSnapshotError`（r8 G3）。
    """
    if isinstance(value, dict):
        try:
            return iter(dict.items(value))
        except TypeError as exc:
            raise _snapshot_rebuild_error(value, "dict", exc) from exc
    if isinstance(value, tuple):
        try:
            return enumerate(tuple.__iter__(value))
        except TypeError as exc:
            raise _snapshot_rebuild_error(value, "tuple", exc) from exc
    try:
        return enumerate(list.__iter__(value))
    except TypeError as exc:
        raise _snapshot_rebuild_error(value, "list", exc) from exc


def _snapshot_container(root: Any) -> Any:
    """显式栈物化 ``root``（dict/list/tuple），环截断、超深抛 ``StateSnapshotError``。

    栈本身就是当前 DFS 路径：``seen_ids`` 只收在场祖先（出栈即移除），所以
    「同一个子容器被两个兄弟键共享」这类 DAG 不会被误判成环，仍按值各拷一份。

    r8 G1：所有读取都是**基类协议**调用（``dict.items`` / ``list.__iter__`` /
    ``tuple.__iter__`` / ``set.__iter__`` / ``frozenset.__iter__`` / 基类
    ``__len__``），结果一律是现造的 plain ``dict``/``list``/``tuple``/
    ``set``/``frozenset`` —— 宿主钩子零调用，快照绝不可能是活宿主对象。

    r9 Finding C/D：dict 的**键**与集合的**成员**也逐个归一化（
    :func:`_snapshot_key_or_member`），归一化不了/归一化后不可哈希的具名拒绝；
    "又实现 ``Mapping`` 的 ``dict`` 子类"（数据存在 dict 存储之外）具名拒绝，不再
    静默产出空 ``dict``。r10 Finding D1：判据补上**镜像形状**（``Mapping`` 排在
    ``dict`` 之后、或纯 ``dict`` 子类用 Python 覆写映射协议自述）—— 见
    :func:`~plaita.core.expression_parser.reads_mapping_outside_dict_storage`。
    """
    # 延迟导入：state.py 是 checkpoint 层的轻量依赖，不为此把 pyparsing 拉进 import 图。
    from .expression_parser import reads_mapping_outside_dict_storage

    if reads_mapping_outside_dict_storage(root):
        raise _snapshot_mapping_outside_storage_error(root, None)
    if _snapshot_isinstance(root, dict):
        root_copy = {}
    else:
        root_copy = [None] * _snapshot_len(root)
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
        if _snapshot_isinstance(frame[0], dict):
            # r9 Finding C：dict 的键也必须归一化，否则宿主对象按引用留在快照里。
            key = _snapshot_key_or_member(key, "dict key")
        if _snapshot_isinstance(child, _LazyRootSnapshot):
            dst[key] = child.materialize()
            continue
        if _snapshot_isinstance(child, (dict, list, tuple)):
            depth = len(frames)
            if depth > _SNAPSHOT_MAX_DEPTH:
                raise StateSnapshotError(message=(
                    f"state snapshot depth {depth} exceeds limit {_SNAPSHOT_MAX_DEPTH} "
                    f"at path {_snapshot_path(frames, key)}"
                ))
            if id(child) in seen_ids:
                dst[key] = _SNAPSHOT_CYCLE_MARK.format(path=_snapshot_path(frames, key))
                continue
            if reads_mapping_outside_dict_storage(child):
                raise _snapshot_mapping_outside_storage_error(
                    child, _snapshot_path(frames, key))
            if _snapshot_isinstance(child, dict):
                child_copy: Any = {}
            else:
                child_copy = [None] * _snapshot_len(child)
            dst[key] = child_copy
            if _snapshot_isinstance(child, tuple):
                tuple_fixups.append((dst, key, child_copy))
            seen_ids.add(id(child))
            frames.append((child, child_copy, _snapshot_items(child), key))
            continue
        if _snapshot_isinstance(child, (set, frozenset)):
            # 集合元素必可哈希（不可变），基类协议浅重建即快照（r8 G1：不再
            # ``child.copy()`` —— 覆写 ``copy()`` 返回 self 的 set 子类会把活对象
            # 塞进快照）。
            dst[key] = _snapshot_set(child)
            continue
        dst[key] = child
    for parent_dst, key, child_copy in reversed(tuple_fixups):
        parent_dst[key] = tuple(child_copy)
    if _snapshot_isinstance(root, tuple):
        return tuple(root_copy)
    return root_copy


def _snapshot_value(value: Any) -> Any:
    """物化 dict/list/tuple 容器为私有快照（C1-2，2026-10 评审修复包 C1；#107 加固）。

    历史上 checkpoint/context 出口（``to_checkpoint_dict``、``$PARENT`` 快照）
    是浅拷贝——``$NODE`` 等嵌套容器按引用共享，消费方（分布式宿主异步序列化、
    子流程 ``$PARENT``）拿到的是 live 别名：写穿透回运行态、后续执行让已"落盘"
    的数据漂移。快照只在容器边界重建；**重建面之外的一切值**（与历史行为一致）
    仍按引用共享——其中**包含容器**（枚举见下），它们本就无法被 checkpoint 的
    JSON 序列化表达。

    #107 起改由 :func:`_snapshot_container` 显式栈物化：不再有 Python 递归深度
    限制（深嵌套值不再 ``RecursionError``），容器环按 ``<recursive: path>``
    占位符显式截断，超过 ``_SNAPSHOT_MAX_DEPTH`` 抛 ``StateSnapshotError``。

    plaita#69 r8（单拷贝纪律，G1）：本函数与 ``expression_parser._engine_copy``
    是**同一种**引擎自有重建——**只**通过基类协议读宿主容器
    （``dict.items(sub)`` / ``list.__iter__(sub)`` / ``tuple.__iter__(sub)`` /
    ``set.__iter__(sub)`` / ``frozenset.__iter__(sub)`` / 基类 ``__len__``，见
    ``_SNAPSHOT_BASE_PROTOCOLS``），因此：

    * 宿主覆写的 ``copy`` / ``items`` / ``__iter__`` / ``__getitem__`` **零调用**
      （r7 的 ``value.items()`` / ``enumerate(value)`` / ``value.copy()`` 会被
      ``set`` 子类的 ``copy() -> self`` 骗成"快照就是活对象"）；
    * 结果必然是**现造的普通** ``dict``/``list``/``tuple``/``set``/``frozenset``，
      子类被归一化掉，快照与活对象 ``is`` 永不相等（**仅就重建面内的这五种容器
      而言**——重建面之外的值按引用共享，见下一条）；
    * 产出因而满足 freshness 登记面的精确类型判据（``type(x) is …``，见
      ``expression_parser._is_walkable_container``），
      ``loop._condition_context`` / ``concurrent`` 的 ``note_fresh_closure``
      只可能登记到引擎现造的对象；
    * **重建面恰好是** ``dict``/``list``/``tuple``/``set``/``frozenset`` 五种
      （子类一并按基类协议重建）；**其余一律按引用共享**，其中**包含容器**。
      plaita#162 实测更正：r8 的措辞把共享面描述为"非容器的宿主对象"，这是
      **事实错误**——下述四者 ``_snapshot_value(v) is v`` 全为 ``True``，且四者
      都满足 ``isinstance(v, collections.abc.Container)``：

        * ``types.MappingProxyType``
        * ``collections.ChainMap``
        * ``collections.UserDict``
        * ``os._Environ``（``os.environ``）

      同型（同样按引用共享、同样是容器）还有 ``collections.UserList`` /
      ``collections.deque`` / ``array.array`` / ``bytearray`` / ``memoryview`` /
      ``dict`` 的 ``keys``/``values``/``items`` 视图等。共享**不是**重建语义的
      "例外设计"，而是重建面的**补集**：这些类型无法在不替宿主猜构造协议的前提下
      重建。
    * 共享为何安全（不是数据通道）：快照路径对它们只读（引擎不在快照上就地改写
      ——写路径是 DSL 侧的 ``_engine_copy``），而 ``_engine_copy`` 对不在
      ``plaita.core.types`` 可持久化数据模型里的类型（含上述四个映射型）**一律
      具名**拒绝（``_EngineCopyRefused``），所以没有任何"活映射宿主对象被写穿"的
      通道；
    * ``__class__`` 被伪造成容器的"假容器"过不了基类描述符，抛具名
      :class:`StateSnapshotError`（含真实类型名），绝不静默共享（r8 G3）。

    plaita#69 **r9 Finding C 更正**：r8 的这段措辞把例外写成"非容器的宿主对象"，
    但当时 **dict 的键**与 **set/frozenset 的成员**仍按引用进快照
    （``key_is_live=True`` / ``el_is_live=True``）—— 宿主 ``dict`` 子类做键、宿主/
    ``__class__`` 伪造对象做成员时，快照里确实留着活别名。r9 起键/成员也过
    :func:`_snapshot_key_or_member`（与 ``_engine_copy`` 同一归一化入口，归一化不了
    或归一化后不可哈希的**具名**拒绝，见 r9 Finding B），所以例外确实只剩
    **重建面之外的值**——r8 把这一面等同于"非容器"是错的（plaita#162 的四个容器
    反例），正确的口径是"重建面的补集"。另外 r9 Finding D：``class M(MutableMapping,
    dict)``（数据存在 dict 存储之外）具名拒绝，不再静默产出空 ``dict``。
    """
    if _snapshot_isinstance(value, _LazyRootSnapshot):
        return value.materialize()
    if _snapshot_isinstance(value, (dict, list, tuple)):
        return _snapshot_container(value)
    if _snapshot_isinstance(value, (set, frozenset)):
        # 集合元素经 _snapshot_key_or_member 归一化（r9 C）：结果必然是普通
        # set/frozenset，且宿主覆写的 copy()/__iter__ 零调用；假容器 → 具名错误。
        return _snapshot_set(value)
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
        # issue69-single-copy: protocol-hook
        # plaita#69 r7：这是 plaita/ 里**唯一**允许调用 copy 模块的位置，而且它是
        # 一个**协议钩子**（外部对引擎惰性视图调用 ``copy.deepcopy`` 时的兼容实现），
        # 不是 DSL 数据路径的拷贝调用点——数据路径一律走
        # ``expression_parser._engine_copy``（守卫见
        # tests/unit/test_issue69_single_copy_point.py）。DSL 路径不再 deepcopy 任何
        # ``_LazyRootSnapshot``：``loop._condition_context`` 先 ``materialize()`` 再
        # ``_engine_copy``，所以宿主类的 ``__deepcopy__`` 没有入口。
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
        "LAST_NODE",        # most recently executed node id
        "BRANCH",           # branch taken by the most recent branching node
        "FLOW_ID",          # flow id of the currently executing flow
        "EXECUTION_ID",     # unique id for this execution (regenerated by clean())
        "SUSPEND_FRAMES",   # nested suspension frame path (plaita#311 NS-M3):
                            # list of per-frame dicts, innermost first, last = root
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

    plaita#164：除 checkpoint 键外，模型还持有一层**私有 scratch 绑定**
    （``set_scratch``）——对表达式可见（``context["$V"]``）、对落盘不可见
    （不在 ``__iter__`` / ``to_checkpoint_dict()`` 里）。两套键视图的划分见
    ``set_scratch`` / ``__iter__`` 的说明。
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
    # plaita#164：**不进 checkpoint** 的 scratch 绑定（`set_scratch`）与其"待登记"
    # 声明面。二者都 `exclude=True` ⇒ 不进 `model_dump()`、也不在 `__iter__` /
    # `to_checkpoint_dict()` 的 checkpoint 键视图里（scratch 是本次执行的私有对象，
    # 跨进程/落盘恢复没有语义，见 `set_scratch` 的边界说明）。
    #
    # **为什么是字段而不是 `PrivateAttr`（性能，实测）**：变量 thunk 每次读根都要问一次
    # "这个键有没有待登记的 scratch 声明"，即路径表达式的热路径。pydantic v2 读私有属性
    # 实测 **~0.46–0.50 µs/次**（走 `BaseModel.__getattr__` 分发，而 `__pydantic_private__`
    # 本身只值 0.013 µs）；普通字段读 **~0.015 µs/次**。用 `PrivateAttr` 会让
    # `$INPUT.a.b.c` 这类**纯路径**表达式慢 ~15%（3.48 → 4.03 µs/call，本机实测），
    # 而 `scripts/issue69_mutation_matrix/perf_baseline.json` 的常量因子守卫以
    # `$INPUT.a.b.c` **自身**为分母，看不见这种"分母同步变慢"。改用字段后回到基线噪声内。
    scratch_bindings: Any = Field(default_factory=dict, exclude=True, repr=False)
    scratch_declared: Any = Field(default_factory=dict, exclude=True, repr=False)
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
        # plaita#164：scratch 绑定（**不进 checkpoint**）是表达式可见的普通键。
        scratch = self.scratch_bindings
        if key in scratch:
            return scratch[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        if key == "EXPRESS_PREFIX":
            # EXPRESS_PREFIX records the prefix string; routing prefix follows.
            self.prefix = value
            self._present.add("EXPRESS_PREFIX")
            return
        # plaita#164：一次普通（checkpointed）写入**取代**同名的 scratch 绑定，
        # 保证「一个键要么是 scratch、要么是 checkpoint 键」这条不变量恒成立。
        if key in self.scratch_bindings:
            self.scratch_bindings.pop(key, None)
            self.scratch_declared.pop(key, None)
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
        # plaita#164：scratch 键对表达式可见（`$V in context` 为真），但它们
        # **不属于 checkpoint 键集**——见下面的 `__iter__` 口径说明。
        return (key in self._present or key in self._extras
                or key in self.scratch_bindings)

    # ------------------------------------------------------------------
    # plaita#164 — 显式 scratch 捐献（**不进 checkpoint** 的私有绑定）
    # ------------------------------------------------------------------
    # 两套"键视图"是**故意**分开的，两者都要：
    #   * 表达式可见面（``__getitem__`` / ``get`` / ``__contains__``）= checkpoint
    #     键 ∪ scratch 键——``$F.set($V,…)`` 必须能解析出 ``$V``；
    #   * checkpoint 面（``__iter__`` / ``__len__`` / ``keys`` / ``items`` /
    #     ``values`` / ``to_checkpoint_dict`` / ``__eq__``）= **只有** checkpoint
    #     键——scratch 是本次执行的私有对象，落盘没有意义（它的新鲜度声明本来就
    #     活不过一次逻辑求值），恢复侧也造不出它。
    # 因此：``len(state)`` / ``dict(state)`` 恒等于 checkpoint 键集，scratch 既不会
    # 混进落盘数据、也不会改变 ``__eq__`` 的语义。

    def set_scratch(self, key: str, value: Any) -> None:
        """把 *value* 绑定为 *key* 的 **scratch** 值（plaita#164）。

        语义 = "这个对象只归本次执行所有、可以被就地改写"：宿主用本方法**显式声明**
        之后，下一次读取该键的逻辑求值会把这个对象登记进该次求值的 freshness
        作用域，``$F.set/$F.pop/...`` 因而可以通过 ``#69`` 的 default-deny 门。

        边界（都是**刻意**的，不是遗漏）：

        * **只登记被声明的那一个对象**（对象身份），**不登记它的图**。声明里嵌套的
          容器仍不是 fresh ⇒ 想就地改写嵌套对象，请直接为它单独 ``set_scratch``。
          这条边界让 scratch **不可能**变成把活根"洗白"成可写目标的通道：声明一个
          ``{"cfg": <活 $INPUT 子对象>}`` 也只能改外层，内层照旧被拒。
        * **一次求值有效**（作用域语义）：声明在该次逻辑求值**首次读取**这个键时被
          消费；下一次求值再读同一个对象**不再是 fresh**，照旧被拒（要再用就重新
          声明）。``#69`` 的"fresh == 本次求值由引擎新建"口径因此原样保留。
        * **不进 checkpoint**：scratch 键不在 ``to_checkpoint_dict()`` / ``__iter__``
          里；``clean()``/恢复会丢掉它。
        * 不得占用 checkpoint 键：五个持久化根与其它 schema 键、以及任何**已在**
          checkpoint 里的键都被具名拒绝（否则 scratch 会遮蔽一个落盘绑定）。
        """
        if not isinstance(key, str) or not key:
            raise ValueError(
                "set_scratch: key must be a non-empty string "
                f"(got {type(key).__name__}: {key!r})")
        mapping = self._key_to_field()
        if key in mapping or key == "EXPRESS_PREFIX":
            raise ValueError(
                f"set_scratch: {key!r} is a checkpoint schema key "
                f"({sorted(mapping)}); scratch bindings are private expression "
                "keys and must not shadow the schema")
        if key in self._present or key in self._extras:
            raise ValueError(
                f"set_scratch: {key!r} already holds a checkpointed binding; a "
                "scratch key is never also a checkpoint key (write it with "
                "set_state, or clear it first)")
        self.scratch_bindings[key] = value
        # 声明面与存储面分开：消费发生在"某次逻辑求值读到这个键"的那一刻
        # （见 expression_parser._note_scratch_read），所以这里只标记"待登记"。
        self.scratch_declared[key] = value

    def scratch_keys(self) -> List[str]:
        """当前 scratch 绑定的键（**不是** checkpoint 键集的一部分）。"""
        return list(self.scratch_bindings)

    def clear_scratch(self, key: Optional[str] = None) -> None:
        """丢掉全部 scratch 绑定（或只丢 *key*），不触碰 checkpoint 键。"""
        if key is None:
            self.scratch_bindings.clear()
            self.scratch_declared.clear()
            return
        self.scratch_bindings.pop(key, None)
        self.scratch_declared.pop(key, None)

    def consume_scratch_declaration(self, key: Any, value: Any) -> bool:
        """「本次声明的 scratch 正在被读」——是则消费掉声明并返回 ``True``。

        身份判据（``is``）而非相等：键被重新绑定过时，旧声明不再生效。读取方
        （``expression_parser`` 的变量 thunk）据此把对象登记成 fresh。
        """
        if not isinstance(key, str) or not self.scratch_declared:
            return False
        declared = self.scratch_declared.get(key, _UNSET)
        if declared is _UNSET or declared is not value:
            return False
        self.scratch_declared.pop(key, None)
        return True

    def __iter__(self):
        # _present already holds storage keys; extras hold their own keys.
        # plaita#164：scratch 键**有意**不出现在这里（checkpoint 面）。
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

        plaita#164：``set_scratch`` 的私有绑定**有意**不出现在这里——``__iter__``
        就是 checkpoint 键视图，scratch 键不在其中（它们对表达式可见、对落盘不可见）。
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
