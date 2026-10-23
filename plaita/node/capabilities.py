"""plaita.node.capabilities — 节点执行能力契约（plaita#330 §4 / #331 前半）。

**5 token 正交词表**（#331：token = 引擎完成 park/resume 或节点功能面所必需的
**宿主基础设施**；节点业务集成的外部单一服务可达性 = 运行期事实，不进词表）：

- ``suspend:root`` —— 宿主支持根流程挂起/恢复（分布式运行时）；
- ``suspend:nested`` —— 宿主支持 child/reference 嵌套链内挂起（NS-M3 帧路径，
  ``suspend:root`` 的超集）；
- ``event_bus`` —— 宿主提供事件总线（订阅注册与恢复事件发布；挂起族的结构半边）；
- ``queue`` —— **持久化队列 + 延迟排程基础设施**（delay_service 的
  list→ZSET + 到期消费线程；定义句钉死「不只是任务队列」，防「queue 满足但
  delay 永不唤醒」灰区）；
- ``sandbox`` —— 宿主提供代码沙箱（docker 等隔离后端）；
- ``network`` —— 节点功能面所需的宿主出网能力（http/redis 等以出网为功能的
  节点；审批服务可达性这类运行期事实仍不进词表）。

**注册期不变量**（#330 Stage 1 第 4 条）：``is_suspending=True`` 的节点必须声明
``execution_requirements``（至少一个 ``suspend:*`` token + 所需宿主 token），
否则注册即红——专门抓「继承裸 ``Node`` 的挂起节点」（HitlAwaitNode 形状）。
三路注册入口同闸：:meth:`NodeRegistry.register`、``_register_builtins``、
entry_points 插件发现（违规记 ``_load_errors`` 失败现场，不炸整个发现过程，
#98 形制）。console ``PLAITA_CONSOLE_NODE_MODULES`` 动态节点经模块
``register_all()`` → ``register()`` 自然覆盖。

回滚旋钮：``PLAITA_SUSPEND_GUARD=warn`` 把不变量降为 WARNING 放行（见
:mod:`plaita.core.errors`，2026-11-30 到期删除）。与 console ``dry_run_safe``
（试跑安全三态，plaita#190）的关系裁决见
``docs/governance/execution-capabilities.md``：同一问题（「节点需要什么才能跑」）
的两个投影，本单不收编、后续单推导化。
"""
from __future__ import annotations

import logging
from typing import Tuple

from plaita.core.errors import suspend_guard_mode

logger = logging.getLogger("plaita.node.capabilities")

#: 合法能力 token 全集（词表封闭：新 token = 运行时能力面变更，须改这里+治理文档）。
EXECUTION_CAPABILITY_TOKENS = frozenset(
    {
        "suspend:root",
        "suspend:nested",
        "event_bus",
        "queue",
        "sandbox",
        "network",
    }
)

#: suspend 家族（is_suspending 节点的最低声明要求）。
SUSPEND_CAPABILITY_TOKENS = frozenset({"suspend:root", "suspend:nested"})


class ExecutionContractError(ValueError):
    """注册期执行能力契约违规（plaita#330 §4）。

    具名可修：消息点名 node_type、缺什么声明、合法词表；注册即红意味着
    缺声明的挂起节点**不可用**（宁可启动期红，不要运行期静默跳过/假成功）。
    """


def validate_execution_contract(node_cls, *, context: str = "register") -> None:
    """校验节点类的执行能力声明（词表封闭 + is_suspending ⇒ 含 suspend token）。

    ``context`` 仅用于错误消息定位（register / builtin / plugin:<name>）。
    ``PLAITA_SUSPEND_GUARD=warn`` 时降 WARNING 放行（只降本闸；运行期 NS-M3
    闸不受影响）。
    """
    reqs = tuple(getattr(node_cls, "execution_requirements", ()) or ())
    problems = []
    unknown = sorted(t for t in reqs if not (isinstance(t, str) and t in EXECUTION_CAPABILITY_TOKENS))
    if unknown:
        problems.append(
            f"execution_requirements 含未知 token {unknown}"
            f"（合法词表: {sorted(EXECUTION_CAPABILITY_TOKENS)}）"
        )
    if getattr(node_cls, "is_suspending", False) is True and not (set(reqs) & SUSPEND_CAPABILITY_TOKENS):
        problems.append(
            "is_suspending=True 但未声明 suspend 能力：execution_requirements "
            f"至少含一个 suspend token（{sorted(SUSPEND_CAPABILITY_TOKENS)}）+ 所需宿主 token（如 event_bus/queue）"
        )
    if not problems:
        return
    node_type = getattr(node_cls, "node_type", None) or getattr(node_cls, "__name__", repr(node_cls))
    message = (
        f"node_type {node_type!r} 执行能力契约违规（{context}）: " + "; ".join(problems)
    )
    if suspend_guard_mode() == "warn":
        logger.warning("%s（PLAITA_SUSPEND_GUARD=warn 降级放行）", message)
        return
    raise ExecutionContractError(message)


def declared_requirements(node_cls) -> Tuple[str, ...]:
    """读取节点类的执行能力声明（统一鸭子兜底：未声明返回空元组）。"""
    return tuple(getattr(node_cls, "execution_requirements", ()) or ())
