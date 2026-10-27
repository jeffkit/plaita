"""B1 所有权 v1（plaita#322）：step token / 幂等键 / 进度租约的纯函数面。

**义务声明（A1 r138 评审注记，落地于此）**：#320 执行状态模型的窗①
（ack 后崩溃）/窗②（租约冲突 ack）的重建语义是「重驱节点」——重建的
副作用安全**依赖本单**：只有声明了 step token 的步骤经连接器幂等键获得
「同逻辑步骤重试 = 同键 = 外部去重」的保证。本模块缺席时，#320 的重建
读法（next_step / pending）只回答「该做什么」，不回答「重做是否安全」。

## 所有权模型

- 状态行 ``next_step.step_id`` 是**分配决定**（write-ahead：先声明工作、
  再执行工作），由 worker 在三个铸造点生成（start 首落 / retry_wakeup /
  step_persist）；执行对象在 run_distributed 前经 ``bind_step`` 消费行声明。
- 重投/重试读**同一行** ⇒ 同 token ⇒ 连接器幂等键稳定：外部系统按
  ``Idempotency-Key`` 去重后，「同逻辑步骤重试 N 次 = 至多 1 次副作用」。
- ``attempt`` / 投递计数**不进键**（只做观测）——进键即去重失效。
- 复合容器（loop/map/while/parallel）经 ``get_child_execution(sub_key=…)``
  为子调用派生作用域：``f"{step_id}:{sub_key}"``——循环 10 次迭代产生
  10 个不同键；同迭代重放复现同键。子执行 ``$EXECUTION_ID`` 每次构造
  随机（``core/context.py`` 构造期 uuid），因此键的执行段**恒取根执行 id**
  （``FlowExecution.idempotency_key`` 沿父链回根）。
- 连接器最终外发键 = ``f"{idempotency_key}:{node_id}"``：同一声明内多个
  外呼节点（子流程/分支）不共享键，防止外部把两次**不同**调用误判为重复。

## 进度租约

三信号正交（后两个的判定谓词在本模块）：

- ``last_update_time``——活性（看门狗每 TTL/3 刷，#320 起语义不变）；
- ``progress_at``——前进（仅步提交刷新，#320 §四.1/§四.2 钉死）；
- ``last_report_at``——步内上报（``report_progress`` 单点写，本单新增）。

租约过期 = ``now - max(progress_at, last_report_at) > step_timeout``。
``step_timeout`` 按节点 ``timeout`` 解析，未声明用环境缺省（默认宽，
``PLAITA_STEP_LEASE_TIMEOUT_SECONDS``）。**过期在本单只是可观测信号**
（#321 载体对账器「running 有租约但停滞 → fence+cancel」的判定输入、
#316 gauge 的升级数据源）；看门狗续租行为不变（#27/#31/#55/#32/#62 五单
误杀家族的教训：活性判断与节点存亡解耦）。
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from plaita.core.url_redaction import _scrub_exception_text
from plaita.runtime_config import optional_float_env

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 幂等键
# ---------------------------------------------------------------------------

IDEMPOTENCY_HEADER = "Idempotency-Key"

# RFC 7231 安全方法：语义幂等，不产生需去重的新副作用——副作用收据不给它们。
SAFE_HTTP_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})


def new_step_id() -> str:
    """铸造 step token（每次声明唯一；跨重放稳定性由「读同一行」保证，
    不由 token 形状保证——故用随机而非可推导形状，防伪造对齐）。"""
    return uuid.uuid4().hex


def compose_idempotency_key(execution_id: Optional[str], step_id: Optional[str]) -> Optional[str]:
    """执行段 + 步段的键合成；任一段缺失 = 不主张所有权（返回 None）。"""
    if not execution_id or not step_id:
        return None
    return f"{execution_id}:{step_id}"


def child_step_scope(step_id: Optional[str], sub_key: Any) -> Optional[str]:
    """复合容器为子调用派生的作用域（loop 迭代 / parallel 分支 / while 轮次）。

    父声明缺失 ⇒ 子作用域同样缺失（tokenless 传播，诚实优于编造）。
    """
    if not step_id:
        return None
    return f"{step_id}:{sub_key}"


def bind_child_step(parent: Any, child: Any, sub_key: Any) -> None:
    """复合容器把派生作用域绑到子执行（plaita#322）。

    与 ``child_step_scope`` 配套的两步式：``child = get_child_execution()``
    （签名零变更——第三方/测试替身实现的是历史签名，单方面加 kwarg 会把
    所有权面放大成替身层的 TypeError）+ 本函数绑定。替身子执行缺
    ``bind_step`` 时静默跳过：所有权是观测与收据面，绝不打断节点执行。
    """
    # span 帧限定符（plaita#344）：sub_key = 迭代索引（loop 族）或分支名
    # （parallel）——与 frame_path（get_child_execution 传播）合成每轮唯一
    # 归属。观测面绝不阻断执行：鸭子赋值，替身缺写入能力时静默跳过。
    try:
        child.frame_key = sub_key
    except Exception as exc:  # noqa: BLE001 — 观测面不阻断执行
        logger.debug(
            "子执行 frame 限定符绑定失败（忽略，帧归属退化到 frame_path）: sub_key=%r: %s",
            sub_key, _scrub_exception_text(exc),
        )
    binder = getattr(child, "bind_step", None)
    if not callable(binder):
        return
    try:
        binder(child_step_scope(getattr(parent, "step_id", None), sub_key))
    except Exception as exc:  # noqa: BLE001 — 所有权面不阻断执行
        logger.debug(
            "子执行 step 作用域绑定失败（忽略，子执行 tokenless）: sub_key=%r: %s",
            sub_key, _scrub_exception_text(exc),
        )


def declared_step_id(next_step: Any) -> Optional[str]:
    """从状态行 next_step 读声明的 step token；畸形/缺失一律 None
    （声明字段是 aid 面，读者按缺省处理、不得放大成毒）。"""
    if not isinstance(next_step, dict):
        return None
    token = next_step.get("step_id")
    return token if isinstance(token, str) and token else None


# plaita#336：error 终态把行上声明转存进 state.error 的扩展键（随失败证据
# 落盘）——retry 唤醒复用同 token，「同逻辑步骤重试 = 同键」在 error→retry
# 主路径不破。值形态与 #320 ``next_step`` 字段设计一致。
FAILED_STEP_EVIDENCE_KEY = "failed_step"


def preserve_failed_step(next_step: Any) -> Optional[Dict[str, Any]]:
    """error 终态清声明**前**的转存面（plaita#336，owner 裁决案 b）。

    #320 咽喉清 ``next_step``（终态不得声明未完成的工作）与 #322「重投读
    同一行 ⇒ 同 token」在 error→retry 路径互斥；本函数校验并摘出转存面，
    由咽喉点写进 ``state.error[FAILED_STEP_EVIDENCE_KEY]``。声明缺失/畸形
    返回 None（无可转存，retry 照铸新——不编造）。
    """
    if not isinstance(next_step, dict):
        return None
    token = next_step.get("step_id")
    if not isinstance(token, str) or not token:
        return None
    node = next_step.get("node_id")
    parent = next_step.get("parent_step_id")
    return {
        "node_id": node if isinstance(node, str) and node else None,
        "step_id": token,
        "parent_step_id": parent if isinstance(parent, str) and parent else None,
    }


def failed_step_evidence(error: Any) -> Optional[Dict[str, Any]]:
    """从 ``state.error`` 扩展键读转存声明（plaita#336）；缺失/畸形一律
    None（读取方照铸新 token，与 ``declared_step_id`` 同款缺省纪律）。
    """
    if not isinstance(error, dict):
        return None
    evidence = error.get(FAILED_STEP_EVIDENCE_KEY)
    if not isinstance(evidence, dict):
        return None
    token = evidence.get("step_id")
    if not isinstance(token, str) or not token:
        return None
    return evidence


def connector_idempotency_key(execution: Any, node_id: Any) -> Optional[str]:
    """连接器外发键：``f"{idempotency_key}:{node_id}"``。

    ``execution`` 可能是测试/第三方的任意替身（MagicMock 等）——读失败
    一律退化为 None（不带头），绝不把键面缺陷放大成节点失败。``node_id``
    缺失（无名节点替身）同样退化 None：无法区分调用方就不出收据。
    """
    try:
        base = getattr(execution, "idempotency_key", None)
        nid = getattr(node_id, "id", node_id)
        # 只认字符串：测试/第三方替身（MagicMock）的 getattr 会返回 mock 对象
        # 而非真实值——拿它拼键 = 把替身烙进外发收据，必须退化为不带头。
        if not isinstance(base, str) or not base or not nid:
            return None
        return f"{base}:{nid}"
    except Exception as exc:  # noqa: BLE001 — 键面只读，任何异常退化为不带头
        logger.debug(
            "连接器幂等键读取失败（不带头）: node=%s: %s",
            node_id, _scrub_exception_text(exc),
        )
        return None


# ---------------------------------------------------------------------------
# 进度租约
# ---------------------------------------------------------------------------

STEP_LEASE_TIMEOUT_ENV = "PLAITA_STEP_LEASE_TIMEOUT_SECONDS"
# 默认宽：阈值本单只产出「载体可能丢失」的可观测信号（消费方 #321 的
# fence+cancel 才是动作），误杀健康长步的代价（五单误杀家族）远大于漏报。
# 运营者可调紧；节点 timeout 声明优先于该缺省。
DEFAULT_STEP_LEASE_TIMEOUT_SECONDS = 24 * 3600.0

PROGRESS_REPORT_MIN_INTERVAL_ENV = "PLAITA_PROGRESS_REPORT_MIN_INTERVAL_SECONDS"
DEFAULT_PROGRESS_REPORT_MIN_INTERVAL_SECONDS = 10.0


def default_step_lease_timeout_seconds() -> float:
    return optional_float_env(
        STEP_LEASE_TIMEOUT_ENV,
        DEFAULT_STEP_LEASE_TIMEOUT_SECONDS,
        minimum=1.0,
    )


def progress_report_min_interval_seconds() -> float:
    """report_progress 落行的最小间隔（秒）；0 = 关闭节流。"""
    return optional_float_env(
        PROGRESS_REPORT_MIN_INTERVAL_ENV,
        DEFAULT_PROGRESS_REPORT_MIN_INTERVAL_SECONDS,
        minimum=0.0,
    )


def resolve_step_timeout_seconds(node: Any, default_seconds: Optional[float] = None) -> float:
    """节点级 step_timeout：节点 ``timeout``（毫秒数或 ISO8601 时长）优先，
    未声明/无法解析用缺省。

    节点对象的 timeout 解析复用引擎 ``_parse_timeout``（#138/#139 的严格
    语义，构造期已拒绝非法值）；任何解析异常回落缺省——阈值是判定参数，
    不是节点执行的组成部分，绝不因它打断流程。
    """
    if default_seconds is None:
        default_seconds = default_step_lease_timeout_seconds()
    raw = None
    try:
        raw = getattr(node, "timeout", None)
    except Exception as exc:  # noqa: BLE001 — 替身对象防御
        logger.debug("节点 timeout 读取失败（用缺省）: %s", _scrub_exception_text(exc))
        raw = None
    if raw in (None, ""):
        return float(default_seconds)
    try:
        from plaita.core.runner import _parse_timeout

        parsed_ms = _parse_timeout(raw)
    except Exception as exc:  # noqa: BLE001 — 阈值解析失败回落缺省
        logger.debug(
            "节点 timeout 解析失败（step_timeout 用缺省）: %s", _scrub_exception_text(exc)
        )
        return float(default_seconds)
    if parsed_ms is None:
        return float(default_seconds)
    return max(parsed_ms / 1000.0, 0.001)


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def progress_anchor(
    progress_at: Optional[str], last_report_at: Optional[str]
) -> Optional[datetime]:
    """租约新鲜度锚 = max(progress_at, last_report_at)；两者皆缺 = None
    （老行无基线，读者不得编造）。"""
    a = _parse_iso(progress_at)
    b = _parse_iso(last_report_at)
    if a is None:
        return b
    if b is None:
        return a
    return a if a >= b else b


def step_lease_expired(
    progress_at: Optional[str],
    last_report_at: Optional[str],
    timeout_seconds: float,
    now: Optional[datetime] = None,
) -> bool:
    """进度租约过期判定（纯函数）。无基线恒 False（宁可漏报不误杀）。"""
    anchor = progress_anchor(progress_at, last_report_at)
    if anchor is None:
        return False
    current = now if now is not None else datetime.now()
    try:
        elapsed = (current - anchor).total_seconds()
    except TypeError:  # noqa: BLE001 — aware/naive 混排不判定
        return False
    return elapsed > timeout_seconds
