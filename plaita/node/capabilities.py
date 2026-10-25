"""plaita.node.capabilities — 节点执行台阶契约的注册面（#330/#331 → 台阶化 2026-10-25）。

词表 = 三值台阶（``inline`` < ``managed`` < ``managed:queue``，两语义模型，
owner 裁决），本体住在 :mod:`plaita.core.capabilities`（执行入口 / NodeRunner
在 core 消费，core 不反向 import node 层）；本模块承担：

- :func:`normalize_execution_tier` —— 归一化（幂等）：``Node.__init_subclass__``
  类创建期与注册期共用。显式 ``execution_tier`` > 旧 ``execution_requirements``
  折算（迁移兼容，第三方插件不破）；**推导** ``is_suspending``（单一真相——
  #330 时代「两处手写同一事实 + 注册不变量缝补」的双写面退役）。
- :func:`validate_execution_contract` —— 注册期闸（三路注册入口同闸：
  :meth:`NodeRegistry.register` / ``_register_builtins`` / entry_points 插件发现
  〔违规记 ``_load_errors`` 失败现场，#98 形制〕）：台阶值合法 + 「只写
  ``is_suspending`` 不写台阶」按旧口径具名拒 + 旧 token 拼写具名拒（迁移期）。
- :func:`shipped_capability_matrix` —— 治理矩阵生成源（``node_type → 台阶``，
  账本 ``gov.capability_matrix`` 的对账对象）。

回滚旋钮 ``PLAITA_SUSPEND_GUARD=warn``（见 :mod:`plaita.core.errors`，
2026-11-30 到期删除）把注册闸降 WARNING 放行（只降本闸；运行期 NS-M3 闸
不受影响）。与 console ``dry_run_safe``（试跑安全三态，plaita#190）的关系
裁决见 ``docs/governance/execution-capabilities.md``：同一问题的两个投影，
不收编。
"""
from __future__ import annotations

import logging
from typing import Dict, Tuple

from plaita.core.capabilities import (  # noqa: F401  (再导出：公开路径守恒)
    EXECUTION_TIERS,
    LEGACY_CAPABILITY_TOKENS,
    is_valid_tier,
    legacy_tokens_to_tier,
)
from plaita.core.errors import suspend_guard_mode

logger = logging.getLogger("plaita.node.capabilities")

_LEGACY_WARNED: set = set()


class ExecutionContractError(ValueError):
    """注册/定义期执行台阶契约违规（plaita#330 §4 → 台阶化）。

    具名可修：消息点名 node_type、缺什么声明、合法台阶；注册即红意味着
    缺声明的挂起节点**不可用**（宁可启动期红，不要运行期静默跳过/假成功）。
    """


def normalize_execution_tier(node_cls, *, context: str = "define") -> str:
    """把节点类的执行台阶归一化并推导 ``is_suspending``（幂等；类创建/注册共用）。

    优先级：显式 ``execution_tier``（本类直写，或继承自已归一化的父类）>
    旧 ``execution_requirements``（:func:`legacy_tokens_to_tier` 折算 +
    一次性弃用告警）> ``inline``。冲突处理（不静默改语义，交注册闸拒）：

    - 显式台阶非法（拼写错）→ :class:`ExecutionContractError`（新 API fail-fast，
      在类定义期就红）；
    - 本类直写 ``is_suspending=True`` 但台阶解出 ``inline`` → **不推导覆盖**，
      保留 True，注册闸按「只写 is_suspending 不写台阶」具名拒（#330 旧口径，
      防裸声明被静默降级为纯计算节点）；
    - 本类直写 ``is_suspending=False`` 但台阶 ≥ managed → 以台阶为准（WARNING）。
    """
    node_type = getattr(node_cls, "node_type", None) or getattr(node_cls, "__name__", repr(node_cls))
    declared = node_cls.__dict__.get("execution_tier")
    tier = declared if declared is not None else getattr(node_cls, "execution_tier", None)
    if tier is not None and not is_valid_tier(tier):
        raise ExecutionContractError(
            f"node_type {node_type!r} execution_tier {tier!r} 非法（{context}；"
            f"合法台阶: {list(EXECUTION_TIERS)}）"
        )
    used_legacy = False
    if tier is None:
        reqs = tuple(getattr(node_cls, "execution_requirements", ()) or ())
        if reqs:
            tier = legacy_tokens_to_tier(reqs)
            used_legacy = True
        else:
            tier = "inline"

    resolved_suspending = tier != "inline"
    direct_suspending = node_cls.__dict__.get("is_suspending")
    if direct_suspending is True and not resolved_suspending:
        # 裸声明（只写 is_suspending 不写台阶）：不改写，注册闸具名拒。
        pass
    else:
        if direct_suspending is False and resolved_suspending:
            logger.warning(
                "node_type %r 声明 execution_tier=%r 但手写 is_suspending=False：以台阶为准（is_suspending 是派生值，请勿手写）",
                node_type, tier,
            )
        node_cls.is_suspending = resolved_suspending

    # execution_tier 是派生值，**不写回**类——写回会污染 __dict__（之后动态补挂
    # execution_requirements 的场景会被"显式台阶优先"吃掉）。读取统一走
    # declared_tier / core.node_execution_tier（显式 > legacy 折算的优先级在那边）。
    if used_legacy and node_type not in _LEGACY_WARNED:
        _LEGACY_WARNED.add(node_type)
        logger.warning(
            "node_type %r 仍用已退役的 execution_requirements=%s（#330/#331 token 词表）："
            "折算为 execution_tier=%r；请迁移到台阶声明",
            node_type, tuple(getattr(node_cls, "execution_requirements", ()) or ()), tier,
        )
    return tier


def validate_execution_contract(node_cls, *, context: str = "register") -> None:
    """注册期闸：先重归一化（幂等，覆盖类定义后改声明的动态场景），再查三条。

    ① 台阶值合法（防直接改类属性绕过定义期校验）；
    ② 「只写 is_suspending=True 不写台阶」具名拒（专门抓「继承裸 ``Node`` 的
       挂起节点」——HitlAwaitNode 形状，#330 Stage 1 第 4 条旧口径）；
    ③ 旧 ``execution_requirements`` 拼写具名拒（迁移期）。

    ``context`` 仅用于错误消息定位（register / builtin / plugin:<name>）。
    ``PLAITA_SUSPEND_GUARD=warn`` 时降 WARNING 放行（只降本闸）。
    """
    tier = normalize_execution_tier(node_cls, context=context)
    problems = []
    reqs = tuple(getattr(node_cls, "execution_requirements", ()) or ())
    unknown = sorted(t for t in reqs if not (isinstance(t, str) and t in LEGACY_CAPABILITY_TOKENS))
    if unknown:
        problems.append(
            f"execution_requirements 含未知 token {unknown}"
            f"（旧词表已退役，合法值: {sorted(LEGACY_CAPABILITY_TOKENS)}；"
            "新代码请直接写 execution_tier）"
        )
    if getattr(node_cls, "is_suspending", False) is True and tier == "inline":
        problems.append(
            'is_suspending=True 但未声明执行台阶：请写 execution_tier = "managed"'
            "（会挂起、靠事件总线唤醒）或 \"managed:queue\"（挂起后靠队列唤醒服务恢复）"
        )
    if not problems:
        return
    node_type = getattr(node_cls, "node_type", None) or getattr(node_cls, "__name__", repr(node_cls))
    message = (
        f"node_type {node_type!r} 执行台阶契约违规（{context}）: " + "; ".join(problems)
    )
    if suspend_guard_mode() == "warn":
        logger.warning("%s（PLAITA_SUSPEND_GUARD=warn 降级放行）", message)
        return
    raise ExecutionContractError(message)


def declared_tier(node_cls) -> str:
    """节点类的执行台阶（归一化后的单一真相；未注册类也安全——只读不写回）。"""
    tier = getattr(node_cls, "execution_tier", None)
    if is_valid_tier(tier):
        return tier
    reqs = tuple(getattr(node_cls, "execution_requirements", ()) or ())
    if reqs:
        return legacy_tokens_to_tier(reqs)
    return "inline"


def declared_requirements(node_cls) -> Tuple[str, ...]:
    """旧 token 声明读取（迁移兼容；矩阵与闸一律走 :func:`declared_tier`）。"""
    return tuple(getattr(node_cls, "execution_requirements", ()) or ())


def shipped_capability_matrix() -> Dict[str, str]:
    """plaita 自带注册面的能力矩阵（plaita#331 ⑤，治理账本 ``gov.capability_matrix`` 的生成源）。

    注册面 = 内置节点 + **本仓** ``pyproject.toml`` 声明的 ``plaita.nodes`` entry points
    （按 dist 名过滤为 ``plaita``，第三方插件如 plaita-nodes 的 22 节点归其自家账本）+
    opt-in 的 ``code`` 节点。矩阵值 = 节点台阶（``inline``/``managed``/``managed:queue``）。
    """
    from importlib.metadata import entry_points

    from plaita.node import NodeRegistry
    from plaita.node.code import CodeNode

    reg = NodeRegistry()
    for ep in entry_points(group="plaita.nodes"):
        dist = getattr(ep, "dist", None)
        dist_name = (dist.metadata["Name"] if dist is not None else "") or ""
        if dist_name.lower() != "plaita":
            continue
        reg.register(ep.load())
    reg.register(CodeNode)
    return reg.capability_matrix()
