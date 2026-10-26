"""runtime 能力门（plaita#300）：publish/start 前把 flow 节点集合对照目标
runtime pool 的 worker 能力清单。

plaita#331 追加执行要求闸（:func:`gate_flow_execution_profile`）：同一发布面
的第二个输入——#300 按池判「节点集支持性」（case/池维度），#331 按 profile 判
「执行要求满足性」（flow 维度），两门并列、错误信封并列。

## 门语义

- 目标池内**存在**带 ``capability_manifest`` 的 worker → 严格校验：flow 节点
  类型集合（含 inline 子流程递归）必须落在池内全部清单 ``supported_node_types``
  的**交集**内；不支持的节点集合非空即抛 :class:`UnsupportedNodeTypesError`，
  调用方映射为 422 结构化错误（``error/runtime/unsupported/supported``）。
- 目标池内**没有任何**清单（池内全是旧 build worker，或池尚未部署）→ 无法
  判定即放行（fail-open），但必须打 :data:`POOL_MANIFEST_MISSING_WARNING`
  级 WARNING——「换道/降级必须可观测」（landing-and-acceptance §2）。这是
  旧 build worker 的兜底路径：没有清单时行为与上线前逐字节一致。

## 校验时机（#300 验收①②）

publish 在状态机 draft→published **之前**；start 在幂等认领/配额占用/queued
占位行/入队**之前**。不支持即拒绝，绝不产生「queued 行 + 永不执行」。

本模块不做 HTTP（无 FastAPI 依赖），API 层负责异常→响应映射。

plaita 队列服务现场盘点（2026-10-25「点名」）：:func:`queue_service_alive`
——``managed:queue`` 台阶的环境侧不再"自报档位/假设在岗"，受理/发布时对
集群档**真查**服务注册表（``plaita:registry:{service_type}:*``，TTL 30s
心跳 10s——有未过期键即活着）；本地档无注册表，静态判不可用。失败面
fail-open + WARNING（与 #300 池清单缺失同风格）：查不动时不拦流程，
但「查不动」本身必须可观测——搁浅风险转告警链，不静默。
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional

from plaita.server.capability import (
    ENGINE_TO_RUNTIME,
    manifest_from_metadata,
    runtime_for_engine,
    unsupported_node_types_for,
)

logger = logging.getLogger(__name__)

#: 台阶 → 服务注册表里的 service_type（节点自述的唤醒依赖 ↔ 守护进程身份）。
#: wake 统一守护进程按 source 各自注册（registry 按各自 service_type 写键），
#: 单进程旧形态与 wake 合并形态在注册面上不可区分——点名语义一致。
TIER_SERVICE_TYPES: Dict[str, tuple] = {
    "managed:queue": ("delay", "approval", "http_callback", "redis_queue", "kafka_queue"),
}

#: fail-open 的兜底口径：注册表查询失败（Redis 抖动等）时按放行处理并告警。
WORKER_POOL_PROBE_FAIL_OPEN_WARNING = (
    "worker_pool_probe_degraded：worker 注册表查询异常，池点名按放行处理"
    "（fail-open）——搁浅风险转告警链"
)

QUEUE_SERVICE_PROBE_FAIL_OPEN_WARNING = (
    "queue_service_probe_unavailable：队列服务注册表查询失败，点名按放行处理"
    "（fail-open）——搁浅风险转告警链，请检查 Redis 连接"
)


def queue_service_alive(redis: Any, service_type: str) -> Optional[bool]:
    """点名：``service_type`` 岗上有没有活着的实例（注册表有未过期键）。

    返回 ``True``（有人）/ ``False``（没人）/ ``None``（查不动——fail-open，
    调用方按放行 + 告警处理）。注册键 TTL 30s 心跳 10s：进程死了键最多
    30s 后消失，点名结果至多滞后一个 TTL。
    """
    if redis is None:
        return False  # 本地档：无注册表，岗位必空（静态判）
    try:
        keys = list(
            redis.scan_iter(f"plaita:registry:{service_type}:*", count=100)
        )
        return len(keys) > 0
    except Exception as e:  # noqa: BLE001 — 查不动 fail-open，转告警链
        logger.warning(
            "%s：service_type=%s（%s）",
            QUEUE_SERVICE_PROBE_FAIL_OPEN_WARNING, service_type, type(e).__name__,
        )
        return None

#: 兜底路径的具名 WARNING 片段（测试断言 + 运维检索锚点）。
POOL_MANIFEST_MISSING_WARNING = "runtime pool 无能力清单"

#: registry 扫描的有界预算（console 读面同量级，能力门是热路径上的读）。
_SCAN_COUNT = 1000
_MAX_SCAN_ROUNDS = 200
_MAX_KEYS = 5000

WORKER_REGISTRY_PATTERN = "plaita:registry:flow_worker:*"


class UnsupportedNodeTypesError(Exception):
    """flow 含目标 runtime 不支持的节点。

    ``detail`` 即 #300 规格的结构化错误体：
    ``{"error", "runtime", "unsupported", "supported"}``。
    """

    def __init__(self, detail: Dict[str, Any]):
        super().__init__(
            f"unsupported_node_types: runtime={detail.get('runtime')} "
            f"unsupported={detail.get('unsupported')}"
        )
        self.detail = detail


def flow_node_types(definition: Any) -> List[str]:
    """收集 flow 定义的节点类型集合（含 inline 子流程递归）。

    ``definition`` 接受 dict 或 JSON 串；解析失败按空集合处理（定义合法性
    由保存路径的 ``Flow.model_validate`` 闸负责，能力门不重复报错）。
    ``reference`` 节点引用的**另一条** flow 在它自己的 publish/start 时过门，
    这里不跨定义追。
    """
    if isinstance(definition, (str, bytes)):
        try:
            definition = json.loads(definition)
        except Exception:  # noqa: BLE001 — 非法 JSON 交由定义校验闸报错
            return []
    if not isinstance(definition, dict):
        return []
    types: List[str] = []

    def _walk(nodes: Any) -> None:
        if not isinstance(nodes, list):
            return
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_type = node.get("type")
            if isinstance(node_type, str) and node_type:
                types.append(node_type)
            child = node.get("child_flow") or node.get("childFlow")
            if isinstance(child, (str, bytes)):
                try:
                    child = json.loads(child)
                except Exception:  # noqa: BLE001 — 子流程串按原样放弃
                    continue
            if isinstance(child, dict):
                _walk(child.get("nodes"))

    _walk(definition.get("nodes"))
    return types


def collect_pool_manifests(redis: Any, runtime: str) -> Dict[str, Dict[str, Any]]:
    """读 registry 里目标 runtime 池的全部能力清单（instance_id → manifest）。

    worker 声明的 ``engine`` 经 ``ENGINE_TO_RUNTIME`` 映射到池键
    （``plaita-go``→``go``、``python``→``python``）；无清单/形状坏的条目跳过
    （读面另走 :mod:`plaita.server.capability` 的同一解析器）。
    """
    manifests: Dict[str, Dict[str, Any]] = {}
    if redis is None:
        return manifests
    keys: List[Any] = []
    try:
        for key in redis.scan_iter(
            WORKER_REGISTRY_PATTERN, count=_SCAN_COUNT
        ):
            keys.append(key)
            if len(keys) >= _MAX_KEYS:
                logger.warning(
                    "能力门 registry 扫描触界（%d 键），结果可能不完整", _MAX_KEYS,
                )
                break
    except Exception as exc:  # noqa: BLE001 — 读不到注册表按「无清单」兜底
        logger.warning(
            "%s：registry 扫描失败（%s: %s），按无清单兜底放行",
            POOL_MANIFEST_MISSING_WARNING, type(exc).__name__, exc,
        )
        return manifests
    for key in keys:
        try:
            raw = redis.get(key)
        except Exception:  # noqa: BLE001 — 单键读失败跳过，不放大故障
            continue
        if not raw:
            continue
        try:
            info = json.loads(raw)
        except Exception:  # noqa: BLE001 — 坏条目跳过
            continue
        manifest = manifest_from_metadata(info.get("metadata"))
        if manifest is None:
            continue
        if runtime_for_engine(manifest.get("engine", "")) != runtime:
            continue
        instance_id = str(info.get("instance_id") or key)
        manifests[instance_id] = manifest
    return manifests


def runtime_pool_alive(redis: Any, runtime: str) -> Optional[bool]:
    """worker 池点名（wake 点名 :func:`queue_service_alive` 的姊妹格，2026-10-27
    Go 生产可选 ADR-002 修订配套）：目标 runtime 池有没有活 worker。

    判据（扫 ``WORKER_REGISTRY_PATTERN``，与 :func:`collect_pool_manifests`
    同 pattern 同界）：

    - 任一 worker 的 manifest 分类到本 runtime（``runtime_for_engine``）→
      ``True``（在岗）；
    - 池内**零 worker 键** → ``False``（确定无人消费——入队必搁浅）；
    - 有 worker 键但**无一可分类**到本 runtime：分两种——①有别的 runtime 的
      可分类 worker（如只有 python worker 在跑，go 流程来了）→ ``False``；
      ②存在**不可分类**条目（旧 build 无 manifest/坏值）→ ``True`` +
      WARNING（与 #300 对同一人群的 fail-open 取舍一致：不能证明它不在，
      不拦——旧 build worker 照样消费队列）；

    返回 ``None`` = Redis 读不动（fail-open，调用方按放行 + WARNING）。
    TTL 滞后与 :func:`queue_service_alive` 同口径（注册键过期前结果不翻转）。
    """
    if redis is None:
        return False  # 本地档：无集群 worker 注册面（admission 不经此路径，纯防御）
    keys: List[Any] = []
    try:
        for key in redis.scan_iter(WORKER_REGISTRY_PATTERN, count=_SCAN_COUNT):
            keys.append(key)
            if len(keys) >= _MAX_KEYS:
                logger.warning(
                    "worker 池点名扫描触界（%d 键），结果可能不完整", _MAX_KEYS,
                )
                break
    except Exception as exc:  # noqa: BLE001 — 读不动 fail-open
        logger.warning(
            "%s：registry 扫描失败（%s: %s），点名按放行处理",
            WORKER_POOL_PROBE_FAIL_OPEN_WARNING, type(exc).__name__, exc,
        )
        return None
    if not keys:
        return False
    matched = 0
    unclassifiable = 0
    for key in keys:
        try:
            raw = redis.get(key)
        except Exception:  # noqa: BLE001 — 单键读失败按不可分类计（fail-open）
            unclassifiable += 1
            continue
        if not raw:
            unclassifiable += 1
            continue
        try:
            info = json.loads(raw)
        except Exception:  # noqa: BLE001 — 坏条目按不可分类计
            unclassifiable += 1
            continue
        manifest = manifest_from_metadata(info.get("metadata"))
        if manifest is None:
            unclassifiable += 1
            continue
        if runtime_for_engine(manifest.get("engine", "")) == runtime:
            matched += 1
    if matched > 0:
        return True
    if unclassifiable > 0:
        logger.warning(
            "%s：runtime=%s 池无已分类 worker，但存在 %d 个不可分类条目"
            "（旧 build/坏值）——按 fail-open 放行（点名不能证明其不在岗）",
            WORKER_POOL_PROBE_FAIL_OPEN_WARNING, runtime, unclassifiable,
        )
        return True
    return False


def _pooled_supported(manifests: Dict[str, Dict[str, Any]]) -> set:
    it = iter(manifests.values())
    first = next(it)
    supported = set(first.get("supported_node_types") or ())
    for manifest in it:
        supported &= set(manifest.get("supported_node_types") or ())
    return supported


def check_node_support(
    manifests: Dict[str, Dict[str, Any]],
    node_types: Iterable[str],
    runtime: str,
) -> Optional[Dict[str, Any]]:
    """纯判定：不支持的节点集合非空时返回结构化错误体，否则 ``None``。

    ``manifests`` 为空返回 ``None``（兜底由调用方先判空并记 WARNING）。
    """
    if not manifests:
        return None
    types = list(node_types)
    supported = _pooled_supported(manifests)
    unsupported = sorted({str(t) for t in types} - supported)
    if not unsupported:
        return None
    return {
        "error": "unsupported_node_types",
        "runtime": runtime,
        "unsupported": unsupported,
        "supported": sorted(supported),
    }


def gate_flow_nodes(
    redis: Any,
    runtime: str,
    definition: Any,
    *,
    flow_id: str = "",
    version: str = "",
) -> None:
    """能力门入口：不支持即抛 :class:`UnsupportedNodeTypesError`。

    池内无清单 → WARNING + 放行（兜底路径，降级可观测）。``redis`` 为
    ``None``（本地单机档：无 registry、执行在 console 进程内）直接放行。
    """
    if redis is None:
        return
    manifests = collect_pool_manifests(redis, runtime)
    if not manifests:
        logger.warning(
            "%s：runtime=%s 池内没有任何带 capability_manifest 的 worker，"
            "能力门按兜底放行（flow=%s@%s）——旧 build worker 部署请尽快升级",
            POOL_MANIFEST_MISSING_WARNING, runtime, flow_id or "?", version or "?",
        )
        return
    detail = check_node_support(manifests, flow_node_types(definition), runtime)
    if detail is not None:
        logger.warning(
            "能力门拒绝：flow=%s@%s runtime=%s 不支持节点 %s",
            flow_id or "?", version or "?", runtime, detail["unsupported"],
        )
        raise UnsupportedNodeTypesError(detail)


class ExecutionProfileUnsatisfiedError(Exception):
    """flow 的执行要求闭包不被目标执行 profile 满足（plaita#331 拒绝面）。

    ``detail``：``{"error", "profile", "required", "missing", "flow_id",
    "version"}`` + suspend 族缺失时的 ``node_id/node_type/frame_path``——
    调用方映射 422 结构化错误。与 #300 门是同一发布面的两个输入：
    本门 = 执行要求满足性（flow 维度），#300 = 节点集支持性（池维度）。
    """

    def __init__(self, detail: Dict[str, Any]):
        super().__init__(
            f"execution_requirements_unmet: profile={detail.get('profile')} "
            f"missing={detail.get('missing')}"
        )
        self.detail = detail


def definition_wake_services(definition: Any) -> List[str]:
    """flow 定义里声明的唤醒源集合（wake_service 字段；两级模型的接线面）。

    供本地静态拒（无 Redis ⇒ 唤醒服务必不在）与点名闸共用。定义解析失败
    返回空表（合法性由定义闸具名，此处跳过留痕由调用方负责）。
    """
    try:
        from plaita.core.flow import Flow as _Flow

        data = (
            __import__("json").loads(definition)
            if isinstance(definition, (str, bytes))
            else definition
        )
        flow = _Flow.model_validate(data)
    except Exception:  # noqa: BLE001 — 定义合法性由定义闸具名
        return []
    out: List[str] = []
    for node in flow.nodes:
        svc = getattr(node, "wake_service", None)
        if svc:
            out.append(str(svc))
    return sorted(set(out))


def gate_flow_execution_profile(
    definition: Any,
    *,
    profile: str,
    flow_id: str = "",
    version: str = "",
) -> None:
    """plaita#331 执行要求闸（publish/start 共用）：递归闭包 required ×
    目标 profile token 集，缺失即抛 :class:`ExecutionProfileUnsatisfiedError`。

    与引擎执行入口（``plaita.core.capabilities.validate_for_profile``）共用
    同一判定函数（A2 r61 评审 §3：两层勿写两套判定）；``stage="publish"``
    落进错误体供消费方辨面。``definition`` 解析/建图失败时跳过并打
    WARNING——定义合法性归保存路径的 ``Flow.model_validate`` 闸（与
    :func:`flow_node_types` 同口径），跳过必须可观测（降级留痕红线）。
    """
    from plaita.core.capabilities import validate_for_profile
    from plaita.core.errors import ExecutionProfileError, SuspendNotExecutableError
    from plaita.core.flow import Flow

    try:
        data = (
            json.loads(definition) if isinstance(definition, (str, bytes)) else definition
        )
        flow = Flow.model_validate(data)
    except Exception as exc:  # noqa: BLE001 — 定义合法性由定义闸具名，此处只留痕
        logger.warning(
            "执行要求闸跳过：flow=%s@%s definition 不可解析（%s），"
            "合法性由定义校验闸负责",
            flow_id or "?", version or "?", type(exc).__name__,
        )
        return
    try:
        validate_for_profile(flow, profile, stage="publish")
        return
    except (SuspendNotExecutableError, ExecutionProfileError) as e:
        body = e.to_dict()
        detail: Dict[str, Any] = {
            "error": body["error_type"],
            "profile": body.get("profile"),
            "required": body.get("required") or [],
            "missing": body.get("missing") or [],
            "flow_id": flow_id or None,
            "version": version or None,
        }
        for key in ("node_id", "node_type", "frame_path", "sites"):
            if body.get(key) is not None:
                detail[key] = body[key]
        logger.warning(
            "执行要求闸拒绝：flow=%s@%s profile=%s 缺少 %s",
            flow_id or "?", version or "?", detail["profile"], detail["missing"],
        )
        raise ExecutionProfileUnsatisfiedError(detail) from e


__all__ = [
    "POOL_MANIFEST_MISSING_WARNING",
    "ExecutionProfileUnsatisfiedError",
    "UnsupportedNodeTypesError",
    "check_node_support",
    "collect_pool_manifests",
    "flow_node_types",
    "gate_flow_execution_profile",
    "gate_flow_nodes",
    "ENGINE_TO_RUNTIME",
]
