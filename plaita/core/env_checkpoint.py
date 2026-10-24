"""``$ENV`` 快照的 checkpoint 出入口治理（plaita#326 wave 2 → #338 嵌套帧面
→ #339 console local 旧环）。

owner 裁决④：checkpoint 不落 ``$ENV`` 的**值与 allowlist 键名**（键名泄露
技术栈，同源脱敏）；owner 裁决①：恢复路径按当前进程 env 重注、值漂移接受。

strip/重注/purge 三支柱共享本模块（#338：遍历面从顶层键扩到嵌套挂起帧）：

- ``strip_env_snapshot``——持久化面：根面 + ``$SUSPEND_FRAMES[*].context``
  （及帧内 ``$PARENT`` 链）全部拔除 ``$ENV``；copy-on-write——未命中的面
  原样共享引用，引擎在飞对象不动（wave 2 ①a「重绑副本」语义的嵌套延伸）。
- ``reinject_env_snapshots``——恢复面：根面 + 每帧按**该帧生效 allowlist**
  （内层 flow 自声明优先，否则沿 child/reference 链继承父级——
  ``setup_flow`` 同判据）重注当前进程 env；生效 allowlist 为空的面不造空
  ``$ENV``（键名不凭空显形）。in-place 变异是恢复契约的一部分：变异结果即
  ``run_distributed(saved_context=...)`` 的输入。
- ``scripts/purge_env_from_checkpoints.py`` 的解析后递归同走 strip。

帧 schema 见 ``plaita.core.strategies._carrier_frame_dicts``（NS-M3）：
``{"node_id", "flow_id", "context"}``，最内帧在前、根帧（末项）
``context=None``。键名用字面量 ``$`` 前缀——与 wave 2 顶层 strip / purge
字节预滤同口径（非缺省 express prefix 的执行不在本治理面，嵌套帧面同）。
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from plaita.core.context import _safe_environment
from plaita.core.url_redaction import _scrub_exception_text

logger = logging.getLogger(__name__)

#: checkpoint 顶层 ``$ENV`` 键（wave 2 同字面量口径）。
ENV_KEY = "$ENV"
#: 帧内父上下文快照键。
PARENT_KEY = "$PARENT"
#: 嵌套挂起帧路径键（NS-M3；checkpoint 键带 ``$`` 前缀）。
SUSPEND_FRAMES_KEY = "$SUSPEND_FRAMES"


def _strip_env_from_face(face: Dict[str, Any]) -> Dict[str, Any]:
    """拔除本面 ``$ENV``；无该键时原样返回（共享引用，零拷贝）。"""
    if ENV_KEY not in face:
        return face
    return {k: v for k, v in face.items() if k != ENV_KEY}


def _strip_env_deep(face: Dict[str, Any]) -> Dict[str, Any]:
    """拔除本面 ``$ENV`` 并沿 ``$PARENT`` 链递归（多级调用链的父快照可再嵌
    ``$PARENT``）；逐层 copy-on-write，未命中的层共享引用。"""
    new_face = _strip_env_from_face(face)
    parent = new_face.get(PARENT_KEY)
    if not isinstance(parent, dict):
        return new_face
    new_parent = _strip_env_deep(parent)
    if new_parent is parent:
        return new_face
    out = dict(new_face)
    out[PARENT_KEY] = new_parent
    return out


def strip_env_snapshot(context: Any) -> Any:
    """拔除根面与嵌套帧面的 ``$ENV``（值与键名，owner 裁决④）。

    返回值规则：无任何面命中 → 原对象原样返回（调用方 rebind 无副作用）；
    有命中 → 新顶层 dict，未命中的内层容器与引擎在飞对象共享引用——
    ``state.context = strip_env_snapshot(state.context)`` 即完整的重绑副本
    写法，在飞求值不受打断。
    """
    if not isinstance(context, dict):
        return context
    new_ctx = _strip_env_from_face(context)
    frames = new_ctx.get(SUSPEND_FRAMES_KEY)
    if not isinstance(frames, list):
        return new_ctx
    new_frames: Optional[List[Any]] = None
    for i, frame in enumerate(frames):
        if not isinstance(frame, dict):
            continue
        fc = frame.get("context")
        if not isinstance(fc, dict):
            continue
        new_fc = _strip_env_deep(fc)
        if new_fc is fc:
            continue
        if new_frames is None:
            new_frames = list(frames)
        new_frames[i] = {**frame, "context": new_fc}
    if new_frames is None:
        return new_ctx
    out = dict(new_ctx)
    out[SUSPEND_FRAMES_KEY] = new_frames
    return out


def reinject_env_snapshots(context: Any, root_flow: Any) -> Dict[str, Any]:
    """按当前进程 env 重注 ``$ENV``：根面 + 嵌套帧面（#338）。

    每帧生效 allowlist 沿根 flow 的 child/reference 节点（``node.child_flow``）
    逐层下行解析：内层 flow 自声明 ``expose_env`` 优先，否则继承父级生效值
    （``setup_flow`` 同判据；引擎嵌套恢复 ``_handle_nested_resume`` 同一条
    下行路径——帧路径漂移时引擎会具名拒，这里只降级重注并 WARNING，不静默）。
    帧内 ``$PARENT`` 链第 j 层快照属倒数第 j 个生效层，按对应 allowlist 重注。

    in-place 变异（恢复契约）；返回 ``{"root": bool, "frames": int,
    "variables": int}`` 供调用方具名告警（``tenant_env.reinject`` 语义）。
    """
    info = {"root": False, "frames": 0, "variables": 0}
    if not isinstance(context, dict):
        return info
    root_allowlist = list(getattr(root_flow, "expose_env", None) or [])
    root_snapshot = _safe_environment(root_allowlist) if root_allowlist else None
    if root_snapshot:
        context[ENV_KEY] = root_snapshot
        info["root"] = True
        info["variables"] = len(root_snapshot)

    frames = context.get(SUSPEND_FRAMES_KEY)
    if not isinstance(frames, list) or not frames:
        return info

    frame_flow = root_flow
    # effective[i] = reversed 帧序第 i 帧所属 flow 的生效 allowlist
    # （第 0 帧=根帧，allowlist=根 flow；帧 i 的 node 用于下探第 i+1 层 flow）。
    effective: List[Optional[List[str]]] = [root_allowlist]
    for i, entry in enumerate(reversed(frames)):
        # 先按本帧生效 allowlist 重注（帧 i 的 context 属第 i 层 flow）。
        if i > 0 and isinstance(entry, dict):
            fc = entry.get("context")
            eff = effective[i]
            if isinstance(fc, dict):
                snapshot = _safe_environment(eff) if eff else None
                if snapshot:
                    fc[ENV_KEY] = snapshot
                    info["frames"] += 1
                    info["variables"] = max(info["variables"], len(snapshot))
                # 帧内 $PARENT 链第 j 层 = 第 i-j 层 flow 的 allowlist
                parent_face = fc.get(PARENT_KEY)
                j = 1
                while isinstance(parent_face, dict):
                    chain_eff = effective[i - j] if 0 <= i - j < len(effective) else None
                    chain_snapshot = _safe_environment(chain_eff) if chain_eff else None
                    if chain_snapshot:
                        parent_face[ENV_KEY] = chain_snapshot
                    parent_face = parent_face.get(PARENT_KEY)
                    j += 1
        # 再下探：本帧 node 在 frame_flow（第 i 层 flow）里找，
        # 经 node.child_flow 下探第 i+1 层（与引擎嵌套恢复同一条下行；
        # 帧路径漂移时恢复路径会具名拒，这里只降级重注并 WARNING，不静默）。
        node_id = entry.get("node_id") if isinstance(entry, dict) else None
        child_flow = None
        if node_id and frame_flow is not None:
            try:
                node = frame_flow.find_node_by_id(node_id)
                child_flow = getattr(node, "child_flow", None)
            except Exception as exc:  # noqa: BLE001 — 漂移面恢复路径会具名拒
                logger.warning(
                    "event=tenant_env.reinject 嵌套帧路径下探失败（node=%r），"
                    "更内层帧 allowlist 按父级生效值降级重注: %s",
                    node_id, _scrub_exception_text(exc),
                )
        own = list(getattr(child_flow, "expose_env", None) or []) if child_flow is not None else []
        effective.append(own or effective[-1])
        frame_flow = child_flow
    return info
