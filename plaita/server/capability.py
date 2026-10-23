"""worker 能力清单（capability manifest，plaita#300）。

## 问题

console 能编排的节点集合大于某个 runtime（如 plaita-go worker）IR 白名单的
accept-list。同一份 flow 在 console 发布成功，到 Go worker 执行时才在
``ir.Load`` 上失败（``flow not loadable``）——而且是入队之后才失败。

## 清单结构（#300 规格原文）

worker 在 registry（``{ns}:registry:flow_worker:{instance}``）metadata 的
``capability_manifest`` 键声明::

    {
      "engine": "plaita-go",
      "ir_contract_version": "2026-10-22",
      "supported_node_types": ["start", "end", "http", "..."],
      "unsupported_node_types": ["map", "while", "..."],
      "features": ["suspend", "checkpoint", "hitl_await"]
    }

- ``supported_node_types`` **由 IR 白名单自动生成，不手写第二份**：Python 侧
  真源是进程默认 ``NodeRegistry``（内置 + ``plaita.nodes`` 插件 + 启动期显式
  注册的 CodeNode 等）；plaita-go 侧由 ``ir.SupportedNodeTypes()`` 导出
  （见 #300 关联的 Go issue）；plaita-rs 侧由 ``plaita_engine::ir::
  capability_manifest()`` 导出（键名与 Go json tags 逐字对齐）——
  三引擎共用本模块定义的 JSON 结构。
- ``unsupported_node_types`` 语义是「本 worker **明确知道且拒绝**的类型」
  （Go 侧对 Python 家族的反向声明）。Python 引擎对未注册类型的拒绝发生在
  解析期（``unRecognized node type``），没有一份有限的「已知但拒绝」全集
  可言——故 Python worker 声明空列表；「这条 flow 里哪些节点不被支持」由
  console 能力门按 ``unsupported_node_types_for`` 现算，不靠 worker 预告。
- ``ir_contract_version`` 标识 IR 契约代际（结构/语义变化时推进），console
  展示与诊断用，能力门**只**消费 ``supported_node_types``。

## 消费方

- ``plaita/server/flow_worker.py``：worker 启动注册时写入 metadata；
- plaita-console services 读面（透出展示）与 publish/start 能力门
  （``plaita-console/backend/services/capability_gate.py``）按本结构解析。

本模块只依赖 ``plaita.node``（server 层依赖方向合规），无 Redis/网络副作用。
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from ..node import NodeRegistry, get_default_registry

__all__ = [
    "ENGINE_PYTHON",
    "ENGINE_GO",
    "ENGINE_RS",
    "ENGINES",
    "ENGINE_TO_RUNTIME",
    "runtime_for_engine",
    "IR_CONTRACT_VERSION",
    "MANIFEST_KEYS",
    "PYTHON_WORKER_FEATURES",
    "build_capability_manifest",
    "python_worker_manifest",
    "manifest_from_metadata",
    "unsupported_node_types_for",
]

#: 引擎名（registry metadata ``engine`` 字段的合法值）。
ENGINE_PYTHON = "python"
ENGINE_GO = "plaita-go"
ENGINE_RS = "plaita-rs"
ENGINES = (ENGINE_PYTHON, ENGINE_GO, ENGINE_RS)

#: engine → console 侧 runtime pool 键（flow_versions.runtime 的取值域）。
ENGINE_TO_RUNTIME = {ENGINE_PYTHON: "python", ENGINE_GO: "go", ENGINE_RS: "rust"}

#: Python worker 旗舰能力声明（#300 示例集）。挂起恢复（delay/approval/
#: event 挂起）、检查点（步界 checkpoint + 崩溃续跑）、HITL 等待（approval）。
PYTHON_WORKER_FEATURES = ("suspend", "checkpoint", "hitl_await")

#: IR 契约代际。结构/语义变化（节点字段增删、next 语义调整）时推进。
IR_CONTRACT_VERSION = "2026-10-22"

#: 清单必含键（形状校验口径，console 读面与能力门共用）。
MANIFEST_KEYS = (
    "engine",
    "ir_contract_version",
    "supported_node_types",
    "unsupported_node_types",
    "features",
)


def runtime_for_engine(engine: str) -> Optional[str]:
    """engine 名 → runtime pool 键；未知 engine 返回 ``None``。"""
    return ENGINE_TO_RUNTIME.get(str(engine))


def build_capability_manifest(
    supported_node_types: Iterable[str],
    *,
    engine: str,
    ir_contract_version: str = IR_CONTRACT_VERSION,
    unsupported_node_types: Iterable[str] = (),
    features: Iterable[str] = (),
) -> Dict[str, Any]:
    """构造清单 dict（去重 + 排序，保证同集合产出逐字节相同的 JSON）。"""
    if engine not in ENGINES:
        raise ValueError(
            f"build_capability_manifest: unknown engine {engine!r}; "
            f"supported: {list(ENGINES)}"
        )
    return {
        "engine": engine,
        "ir_contract_version": str(ir_contract_version),
        "supported_node_types": sorted(set(supported_node_types)),
        "unsupported_node_types": sorted(set(unsupported_node_types)),
        "features": sorted(set(features)),
    }


def python_worker_manifest(registry: Optional[NodeRegistry] = None) -> Dict[str, Any]:
    """Python worker 清单：``supported`` 即进程 IR 白名单（registry 真值）。

    ``registry`` 缺省取进程默认 registry（首次调用触发插件懒发现）——worker
    在完成 CodeNode / PLAITA_NODE_MODULES 注册**之后**构造，清单如实反映
    本进程可执行的节点集合。
    """
    reg = registry if registry is not None else get_default_registry()
    return build_capability_manifest(
        reg.list_types(),
        engine=ENGINE_PYTHON,
        features=PYTHON_WORKER_FEATURES,
    )


def manifest_from_metadata(metadata: Any) -> Optional[Dict[str, Any]]:
    """从 registry metadata 读清单；缺失/形状不合法一律 ``None``（不抛）。

    形状闸：五个键齐全且三个列表字段是 list——旧 build（无清单）与被写坏的
    条目在消费侧统一按「无清单」处理（能力门走缺清单兜底，读面不展示）。
    """
    if not isinstance(metadata, dict):
        return None
    manifest = metadata.get("capability_manifest")
    if not isinstance(manifest, dict):
        return None
    if any(key not in manifest for key in MANIFEST_KEYS):
        return None
    if manifest.get("engine") not in ENGINES:
        return None
    for key in ("supported_node_types", "unsupported_node_types", "features"):
        if not isinstance(manifest.get(key), list):
            return None
    return manifest


def unsupported_node_types_for(manifest: Dict[str, Any], node_types: Iterable[str]) -> list:
    """flow 节点类型集合中不被清单支持的子集（排序去重；纯函数，能力门核心）。"""
    supported = set(manifest.get("supported_node_types") or ())
    return sorted({str(t) for t in node_types} - supported)
