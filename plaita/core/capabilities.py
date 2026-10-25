"""plaita.core.capabilities — 节点执行台阶契约（两语义模型终版，owner 裁决 2026-10-25）。

执行语义只有两种（ADR-001）：**直跑**（inline，进程内一口气跑完，进程退即流程
亡）与**托管**（managed，能停能续的执行面）。节点侧唯一模式声明 = 执行台阶
（``execution_tier``，两值）：

    ``inline`` < ``managed``

- ``inline``（默认）—— 纯计算/控制流/出网/本机子进程：任何宿主可跑；
- ``managed`` —— 会挂起、靠事件总线唤醒（event / hitl_await / delay /
  approval / 回调与队列族）：宿主必须具备挂起-恢复（托管面）。

**没有子档**（owner 第二轮裁决：managed:queue 废除）——「挂起后靠哪个唤醒
源」是**接线元数据**不是模式：节点 ``wake_service`` 字段（如 delay→"delay"）
+ 受理门点名（:func:`plaita-console capability_gate.queue_service_alive`，
本地档静态无人、集群档查注册表）。判定 = flow 台阶（站点 max）≤ 环境台阶，
一次比较；差了报**缺什么**，不报「你在哪个档」。

**旧 token 词表已退役**（suspend:root/nested、event_bus、queue、sandbox、
network；#330/#331 前半）：network/sandbox 从不区分任何真实宿主（沙箱白名单
另有 fail-closed 机制），suspend 两分把「当初只实现部分挂起」供成了词表——
帧结构限制归运行期作用域闸（:data:`SUSPEND_ALLOWED_FRAME_NODE_TYPES`，单一
事实源不变），不进能力词。旧声明经 :func:`legacy_tokens_to_tier` 映射兼容
（迁移期，第三方插件如 plaita-nodes 不破）；第三方系统可达性 = 运行期事实，
不进台阶（#331 owner 裁决不变）。

**三件事，一个判定函数**（A2 r61 评审 §3：两层勿写两套判定）：

- :func:`capability_closure` / :func:`required_tier` —— 递归闭包：child / loop
  族 / parallel 分支 + reference 引用的 flow（``resolve_reference`` 钩子；未注入
  的 reference 记 ``unresolved``，不当能力缺口）。每个声明站点带帧路径（根→叶）。
- :func:`validate_for_profile` —— **拒绝面**：独立于 ``model_validate``（读面
  不碰），只在执行入口与 console publish/start 调用。flow 台阶 > 环境台阶即在
  **任何节点执行前**具名拒绝（:class:`SuspendNotExecutableError`，``stage="entry"``，
  #317 机器判据 ``suspend_not_executable`` 不变）。
- :func:`assert_node_executable` —— **兜底面**：NodeRunner 在每个 ``managed``
  节点 ``execute`` 前断言宿主契约（宿主身份取执行链的根），覆盖绕过 strategies
  的旁路；正常流不应触达，触达即入口漏了——具名错误 + WARNING，不静默。

环境台阶（``profile`` 形参）= 台阶名（``inline`` / ``managed`` / ``managed:queue``）。
旧 profile 名兼容映射：``normal``/``generator`` → ``inline``、``local`` →
``managed``、``distributed`` → ``managed:queue``；旧 token 集合（iterable）经
legacy 映射折算成台阶。``run_distributed`` 的模式根默认 ``managed:queue``
（集群档假设服务齐配；本地档由 console 闸在 publish/start 前置拒绝，或经
``execution_profile`` 显式降为 ``managed``）。

回滚旋钮 ``PLAITA_SUSPEND_GUARD=warn``（#330 §7，2026-11-30 到期删除）同样降级
本模块的入口拒绝与 runner 兜底（WARNING + 放行，交回运行期闸/呈现）。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Optional, Tuple, Union

from plaita.core.errors import (
    SuspendNotExecutableError,
    suspend_guard_mode,
)

logger = logging.getLogger("plaita.core.capabilities")

#: 执行台阶全集（两语义模型终版，owner 裁决 2026-10-25 第二轮：**只有两级**，
#: 不设子档——"唤醒源是哪个岗"是接线元数据（:data:`WAKE_SERVICE_NODE_TYPES`
#: + 节点 ``wake_service`` 字段）+ 受理门点名（现场盘点），不进模式词汇。
#: 封闭集：新台阶须真实部署形态背书 + ADR）。
EXECUTION_TIERS: Tuple[str, ...] = ("inline", "managed")

_TIER_RANK: Dict[str, int] = {tier: idx for idx, tier in enumerate(EXECUTION_TIERS)}

#: 旧 token 词表（#330/#331 前半，已退役）：仅迁移期兼容读取用。
LEGACY_CAPABILITY_TOKENS: FrozenSet[str] = frozenset(
    {
        "suspend:root",
        "suspend:nested",
        "event_bus",
        "queue",
        "sandbox",
        "network",
    }
)

#: 旧 token → 台阶高度（sandbox/network 从不区分宿主 → inline；
#: suspend:*/event_bus/queue → managed——旧 queue 家族的"要配套服务"半边
#: 由 wake_service 字段 + 点名闸承接，不进台阶）。
_LEGACY_TOKEN_RANK: Dict[str, int] = {
    "suspend:root": 1,
    "suspend:nested": 1,
    "event_bus": 1,
    "queue": 1,
    "sandbox": 0,
    "network": 0,
}

#: 旧 profile 名 → 台阶（console publish/start 与 flow_compile 的存量调用面）。
_LEGACY_PROFILE_TIER: Dict[str, str] = {
    "normal": "inline",
    "generator": "inline",
    "distributed": "managed",
    "local": "managed",
}

#: profile 形参：台阶名、旧 profile 名，或旧 token 集合（迁移兼容）。
ProfileSpec = Union[str, Iterable[str]]

_MODE_TO_PROFILE = {
    None: "inline",
    "normal": "inline",
    "generator": "inline",
    "distributed": "managed",
}

#: 挂起作用域闸放行的帧类型（v1：只有 child/reference 调用链内的挂起可穿透到
#: 分布式恢复边界）。单一事实源——运行期闸（strategies ``_scope_gate_blocker``）
#: 与入口静态闭包的 ``fix=restructure`` 判定共用。**帧结构限制不是能力词**：
#: 全面放开嵌套挂起时只松本闸，词表零变化。
SUSPEND_ALLOWED_FRAME_NODE_TYPES: FrozenSet[str] = frozenset({"child", "reference"})


#: 挂起后靠**队列唤醒服务**（wake 进程 source）恢复的内置节点族——它们的
#: ``wake_service`` 字段值 = service_type 同名（delay→delay、approval→approval…）。
#: 单源用途：console 本地档静态拒（无 Redis ⇒ 唤醒服务必不在）与点名闸的
#: 兜底集。自定义节点要接自定义唤醒源：直接声明 ``wake_service = "<名>"``。
WAKE_SERVICE_NODE_TYPES: FrozenSet[str] = frozenset(
    {"delay", "approval", "http_callback", "redis_queue", "kafka_queue"}
)


def is_valid_tier(tier: Any) -> bool:
    return isinstance(tier, str) and tier in _TIER_RANK


def tier_rank(tier: str) -> int:
    """台阶 → 高度（0/1/2）。非法台阶具名拒绝（本模块只接受已归一化的台阶）。"""
    try:
        return _TIER_RANK[tier]
    except (KeyError, TypeError):
        raise ValueError(
            f"未知执行台阶 {tier!r}（合法台阶: {list(EXECUTION_TIERS)}）"
        ) from None


def legacy_tokens_to_tier(tokens: Iterable[str]) -> str:
    """旧 token 集合 → 台阶（取映射高度的最大值；空集 = inline）。

    未知 token **不在此拒绝**（注册期闸 :func:`plaita.node.capabilities.
    validate_execution_contract` 沿旧口径具名拒），只按已知 token 折算——
    类定义期的归一化不能因为第三方拼错词而炸掉整个 import。
    """
    rank = 0
    for token in tokens or ():
        token_rank = _LEGACY_TOKEN_RANK.get(token)
        if token_rank is not None and token_rank > rank:
            rank = token_rank
    return EXECUTION_TIERS[rank]


def node_execution_tier(node: Any) -> Tuple[str, str]:
    """节点 → (台阶, cause)。鸭子读取，core 不反向 import node 层。

    优先级：显式 ``execution_tier``（node 层 ``__init_subclass__`` 已归一化写回）
    > 旧 ``execution_requirements``（legacy 映射，cause="legacy"）>
    ``is_suspending=True`` 兜底（cause="undeclared"，与旧闭包 implicit
    ``suspend:root`` 同口径——注册闸照旧具名拒，此处 fail-closed 记 managed）。
    读取做鸭子防御（``Mock(spec=Node)`` 等测试替身的属性不是真值/序列时不炸）。
    """
    tier = getattr(node, "execution_tier", None)
    if is_valid_tier(tier):
        return tier, "declared"
    raw_reqs = getattr(node, "execution_requirements", None)
    reqs = tuple(raw_reqs) if isinstance(raw_reqs, (list, tuple, set, frozenset)) else ()
    if reqs:
        return legacy_tokens_to_tier(reqs), "legacy"
    if getattr(node, "is_suspending", False) is True:
        return "managed", "undeclared"
    return "inline", "declared"


def normalize_profile(profile: ProfileSpec) -> str:
    """profile 形参 → 台阶名。台阶名原样；旧 profile 名按映射；旧 token 集合
    经 legacy 折算；其余具名拒绝。"""
    if isinstance(profile, str):
        if is_valid_tier(profile):
            return profile
        legacy = _LEGACY_PROFILE_TIER.get(profile)
        if legacy is not None:
            return legacy
        raise ValueError(
            f"未知执行 profile {profile!r}（台阶: {list(EXECUTION_TIERS)}；"
            f"旧名兼容: {sorted(_LEGACY_PROFILE_TIER)}）"
        )
    tokens = frozenset(profile)
    return legacy_tokens_to_tier(tokens)


def profile_name(profile: ProfileSpec) -> str:
    """profile 的呈现名（错误体 ``profile`` 键）= 归一化台阶名。"""
    return normalize_profile(profile)


def profile_for_mode(mode: Any) -> str:
    """执行模式 → 台阶（``ExecutionMode`` 枚举 / 字符串 / None 均可，鸭子读 ``.value``）。

    ``distributed`` → ``managed``：模式根按托管面判；队列唤醒服务在不在岗
    不由台阶判——受理门对带 ``wake_service`` 的节点现场点名（managed:queue
    子档已于 2026-10-25 第二轮裁决废除）。
    """
    value = getattr(mode, "value", mode)
    try:
        return _MODE_TO_PROFILE[value]
    except (KeyError, TypeError):
        raise ValueError(f"无法为执行模式 {mode!r} 解析执行台阶") from None


# ---------------------------------------------------------------------------
# 递归闭包
# ---------------------------------------------------------------------------


@dataclass
class RequirementSite:
    """flow 闭包里一个带执行要求的节点站点。

    ``frame_path`` 根→叶、含站点自身，元素形如 ``"call(child)"``（与 #330 根出口
    呈现的 ``frame_path`` 同形）；``frames`` 为帧节点对象（根→叶，不含站点），
    供 ``fix`` 判定。``cause``：``declared``（台阶声明）/ ``legacy``（旧 token
    折算）/ ``undeclared``（is_suspending 却无声明——注册闸照旧具名拒，闭包按
    fail-closed 记 ``managed``）。
    """

    node: Any
    node_id: str
    node_type: str
    flow_id: Optional[str]
    frame_path: List[str]
    frames: List[Any]
    tier: str
    cause: str

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "flow_id": self.flow_id,
            "frame_path": list(self.frame_path),
            "tier": self.tier,
            "cause": self.cause,
        }


@dataclass
class CapabilityClosure:
    tier: str
    sites: List[RequirementSite]
    unresolved: List[dict]


def _label(node: Any) -> str:
    return f"{getattr(node, 'id', '?')}({getattr(node, 'node_type', '?')})"


def _walk(
    flow: Any,
    *,
    recursive: bool,
    resolver: Optional[Callable[[Any], Any]],
    frames: Tuple[Any, ...],
    sites: List[RequirementSite],
    unresolved: List[dict],
    seen: set,
) -> None:
    if flow is None or id(flow) in seen:
        return
    seen.add(id(flow))
    flow_id = getattr(flow, "flow_id", None)
    for node in getattr(flow, "nodes", None) or []:
        if isinstance(node, dict):
            # 未解析节点看不见声明——入口在 _ensure_flow_resolved 之后调用，此处只会在
            # 绕过解析的裸 Flow(...) 上出现；覆盖缩水必须可观测。
            logger.warning(
                "capability closure: flow %r node %r is unresolved (dict); its declaration is invisible",
                flow_id, node.get("id") if isinstance(node, dict) else node,
            )
            continue
        tier, cause = node_execution_tier(node)
        if tier != "inline":
            sites.append(
                RequirementSite(
                    node=node,
                    node_id=str(getattr(node, "id", "?")),
                    node_type=str(getattr(node, "node_type", "?")),
                    flow_id=flow_id,
                    frame_path=[_label(f) for f in frames] + [_label(node)],
                    frames=list(frames),
                    tier=tier,
                    cause=cause,
                )
            )
        if not recursive:
            continue
        child = getattr(node, "child_flow", None)
        if child is not None:
            _walk(child, recursive=True, resolver=resolver, frames=frames + (node,),
                  sites=sites, unresolved=unresolved, seen=seen)
        elif getattr(node, "node_type", None) == "reference":
            resolved = resolver(node) if resolver is not None else None
            if resolved is not None:
                _walk(resolved, recursive=True, resolver=resolver, frames=frames + (node,),
                      sites=sites, unresolved=unresolved, seen=seen)
            else:
                unresolved.append({"node_id": str(getattr(node, "id", "?")),
                                   "flow_id": getattr(node, "flow_id", None)})
        for pb in getattr(node, "branches", None) or []:
            bflow = getattr(pb, "flow", None)
            if bflow is not None:
                _walk(bflow, recursive=True, resolver=resolver, frames=frames + (node,),
                      sites=sites, unresolved=unresolved, seen=seen)


def capability_closure(
    flow: Any,
    *,
    recursive: bool = True,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
) -> CapabilityClosure:
    """递归闭包：返回 flow（含 child/loop 族/parallel 分支/已解析 reference）的全部声明站点。

    ``tier`` = 站点台阶的最大值（无要求站点 = ``inline``）。"""
    sites: List[RequirementSite] = []
    unresolved: List[dict] = []
    _walk(flow, recursive=recursive, resolver=resolve_reference, frames=(),
          sites=sites, unresolved=unresolved, seen=set())
    tier = EXECUTION_TIERS[max((tier_rank(s.tier) for s in sites), default=0)]
    return CapabilityClosure(tier=tier, sites=sites, unresolved=unresolved)


def required_tier(
    flow: Any,
    recursive: bool = True,
    *,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
) -> str:
    """flow 的执行台阶（``recursive=False`` 只看根层节点）。"""
    return capability_closure(flow, recursive=recursive, resolve_reference=resolve_reference).tier


# ---------------------------------------------------------------------------
# profile 校验（拒绝面）
# ---------------------------------------------------------------------------


def _evaluate(flow, profile, *, recursive, resolve_reference):
    env_tier = normalize_profile(profile)
    closure = capability_closure(flow, recursive=recursive, resolve_reference=resolve_reference)
    offending = [s for s in closure.sites if tier_rank(s.tier) > tier_rank(env_tier)]
    report = {
        "profile": env_tier,
        "required": closure.tier,
        "missing": sorted({s.tier for s in offending}),
        "sites": [s.to_dict() for s in closure.sites],
        "unresolved": list(closure.unresolved),
    }
    return closure, env_tier, offending, report


def check_execution_requirements(
    flow: Any,
    profile: ProfileSpec,
    *,
    recursive: bool = True,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
) -> dict:
    """非致命报告 ``{profile, required, missing, sites, unresolved}``（flow_compile 作者期信号用）。"""
    return _evaluate(flow, profile, recursive=recursive, resolve_reference=resolve_reference)[3]


_TIER_HELP = {
    "managed": "托管执行面（能停能续：console 发布，或 FlowExecution.run_distributed 逐步驱动）",
}


def _suspend_error(
    site: RequirementSite,
    env_tier: str,
    *,
    stage: str,
) -> SuspendNotExecutableError:
    node = site.node
    blocker = next(
        (f for f in site.frames if getattr(f, "node_type", None) not in SUSPEND_ALLOWED_FRAME_NODE_TYPES),
        None,
    )
    if blocker is not None:
        fix = {
            "action": "restructure",
            "call": None,
            "message": (
                f"挂起节点位于 {getattr(blocker, 'node_type', '?')!r} 帧 {getattr(blocker, 'id', '?')!r} 内："
                "v1 仅支持 child/reference 调用链内挂起，分布式运行时同样具名拒绝；"
                "请把挂起节点移出迭代/并行帧"
            ),
        }
    else:
        fix = {
            "action": "run_distributed",
            "call": None,
            "message": (
                f"节点 {site.node_type!r} 会挂起，需要 {_TIER_HELP[site.tier]}；"
                "本地直跑（flow.run / normal/generator）无法执行。请到 console 将流程"
                "发布到托管面验证，或使用 FlowExecution.run_distributed(...) 逐步驱动"
            ),
        }
    loc = f"，source line {node.source_line}" if getattr(node, "source_line", None) is not None else ""
    message = (
        "这不是源码缺陷，不要删除/替换该节点：\n"
        f"挂起节点 {site.node_id!r} ({site.node_type}{loc}) 在 flow {site.flow_id!r} 需要执行台阶 "
        f"{site.tier!r}，当前环境台阶 {env_tier!r}（差 {tier_rank(site.tier) - tier_rank(env_tier)} 级）；"
        f"流程已在节点执行前具名拒绝（stage={stage}）。"
    )
    return SuspendNotExecutableError(
        message,
        node=node,
        source_line=getattr(node, "source_line", None),
        flow_id=site.flow_id,
        cause="environment" if site.cause in ("declared", "legacy") else "undeclared",
        fix=fix,
        frame_path=list(site.frame_path),
        stage=stage,
        profile=env_tier,
        required=[site.tier],
        missing=[site.tier],
    )


def _build_error(flow, closure: CapabilityClosure, env_tier: str, offending: List[RequirementSite], report: dict, stage: str):
    return _suspend_error(offending[0], env_tier, stage=stage)


def validate_for_profile(
    flow: Any,
    profile: ProfileSpec,
    *,
    recursive: bool = True,
    resolve_reference: Optional[Callable[[Any], Any]] = None,
    stage: str = "entry",
) -> dict:
    """执行入口 / publish / start 的拒绝面：flow 台阶 > 环境台阶即具名拒绝。

    满足时返回与 :func:`check_execution_requirements` 同形的报告。**独立于
    ``model_validate``**：读面（解析/展示/编辑）永不调用本函数。
    """
    closure, env_tier, offending, report = _evaluate(
        flow, profile, recursive=recursive, resolve_reference=resolve_reference
    )
    if not offending:
        return report
    err = _build_error(flow, closure, env_tier, offending, report, stage)
    if suspend_guard_mode() == "warn":
        logger.warning(
            "[suspend-guard=warn] %s validation degraded to WARNING (env tier %r missing %s); "
            "falling back to the runtime suspend gate: %s",
            stage, report["profile"], report["missing"], err.to_dict(),
        )
        return report
    raise err


# ---------------------------------------------------------------------------
# 执行入口 / NodeRunner 接线助手
# ---------------------------------------------------------------------------


def host_profile(execution: Any) -> ProfileSpec:
    """执行链**根**的宿主台阶：显式 ``execution_profile`` 优先，否则由根的 mode 推导。

    子执行继承根的宿主身份（distributed 根下的 child 子执行虽经 run_compatible 驱动，
    宿主仍是分布式池）。
    """
    root = execution
    while getattr(root, "parent", None) is not None:
        root = root.parent
    explicit = getattr(root, "execution_profile", None)
    if explicit is not None:
        return explicit
    return profile_for_mode(getattr(root, "mode", None))


def gate_execution_entry(execution: Any, flow: Any) -> Optional[dict]:
    """执行入口闸：仅模式根（``parent is None``）校验；子执行不闸（递归闭包已在根算过，
    NS-M3 传播面不动）。返回报告或 ``None``（子执行）。"""
    if getattr(execution, "parent", None) is not None:
        return None
    return validate_for_profile(flow, host_profile(execution), stage="entry")


def assert_node_executable(node: Any, execution: Any, *, flow: Any = None) -> None:
    """NodeRunner 兜底：``managed`` 台阶节点 ``execute`` 前断言宿主契约（含子流程，
    宿主取执行链根）。

    正常流不应触达——入口闭包已覆盖；触达即入口漏了（静态闭包看不见的复合节点、
    绕过 strategies 直接驱动 runner 的旁路）：具名错误 + WARNING，不静默。无宿主
    身份的裸 runner（``execution is None``）无法判定，交回运行期 carrier 闸。
    """
    tier, cause = node_execution_tier(node)
    if tier == "inline":
        return
    if execution is None:
        logger.debug("runner guard skipped: bare NodeRunner without host execution (node %r)", getattr(node, "id", "?"))
        return
    env_tier = normalize_profile(host_profile(execution))
    if tier_rank(tier) <= tier_rank(env_tier):
        return
    site = RequirementSite(
        node=node,
        node_id=str(getattr(node, "id", "?")),
        node_type=str(getattr(node, "node_type", "?")),
        flow_id=getattr(flow, "flow_id", None),
        frame_path=[_label(node)],
        frames=[],
        tier=tier,
        cause=cause,
    )
    err = _suspend_error(site, env_tier, stage="runner")
    if suspend_guard_mode() == "warn":
        logger.warning(
            "[suspend-guard=warn] runner guard degraded to WARNING for managed node %r (env tier %r missing %r): %s",
            site.node_id, env_tier, site.tier, err.to_dict(),
        )
        return
    logger.warning(
        "[execution-capabilities] runner guard tripped before execute of managed node %r — "
        "the entry gate missed it (env tier %r missing %r): %s",
        site.node_id, env_tier, site.tier, err.to_dict(),
    )
    raise err


# ---------------------------------------------------------------------------
# 矩阵差分（治理账本断言）
# ---------------------------------------------------------------------------


def diff_capability_matrix(ledger: Dict[str, Any], matrix: Dict[str, Any]) -> dict:
    """账本 vs 注册表矩阵的差分：``{added, removed, changed}`` 只含非空键；空 dict = 一致。

    承重墙（A2 r61 §1.3）：注册表新增 node_type 而账本未变 → ``added`` 非空 → 红。
    矩阵值 = 节点台阶（字符串）。
    """
    added = {k: v for k, v in matrix.items() if k not in ledger}
    removed = {k: v for k, v in ledger.items() if k not in matrix}
    changed = {
        k: {"ledger": ledger[k], "registry": matrix[k]}
        for k in ledger
        if k in matrix and ledger[k] != matrix[k]
    }
    out: dict = {}
    if added:
        out["added"] = added
    if removed:
        out["removed"] = removed
    if changed:
        out["changed"] = changed
    return out
