"""runtime 能力门（plaita#300）：publish/start 前把 flow 节点集合对照目标
runtime pool 的 worker 能力清单。

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


__all__ = [
    "POOL_MANIFEST_MISSING_WARNING",
    "UnsupportedNodeTypesError",
    "check_node_support",
    "collect_pool_manifests",
    "flow_node_types",
    "gate_flow_nodes",
    "ENGINE_TO_RUNTIME",
]
