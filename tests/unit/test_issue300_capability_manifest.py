"""#300 worker 能力清单（capability manifest）。

验收面：

- 清单由 IR 白名单（进程默认 NodeRegistry）**自动生成**——不手写第二份：
  ``supported_node_types`` 必须逐元素等于 ``get_default_registry().list_types()``
  的排序结果；
- Python worker 注册 metadata 里带同结构清单（engine="python"）；
- 清单可从 registry metadata 读回（console 读面/能力门共用同一解析器）；
- ``unsupported_node_types_for`` 是能力门的纯函数核心：flow 节点集合 − 清单
  支持集合。
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from fakeredis import FakeRedis

from plaita.node import get_default_registry, init_default_registry
from plaita.server.capability import (
    ENGINE_GO,
    ENGINE_PYTHON,
    IR_CONTRACT_VERSION,
    MANIFEST_KEYS,
    build_capability_manifest,
    manifest_from_metadata,
    python_worker_manifest,
    unsupported_node_types_for,
)


@pytest.fixture(autouse=True)
def _fresh_registry():
    """每测显式重建默认 registry（进程级单例可变状态，测试间不得串味）。"""
    init_default_registry(auto_discover=False)
    yield
    init_default_registry(auto_discover=False)


class TestBuildCapabilityManifest:
    def test_shape_and_sorted_lists(self):
        manifest = build_capability_manifest(
            ["end", "start", "http", "start"],
            engine=ENGINE_GO,
            unsupported_node_types=["while", "map"],
            features=["checkpoint"],
        )
        assert manifest["engine"] == ENGINE_GO
        assert manifest["ir_contract_version"] == IR_CONTRACT_VERSION
        assert manifest["supported_node_types"] == ["end", "http", "start"]
        assert manifest["unsupported_node_types"] == ["map", "while"]
        assert manifest["features"] == ["checkpoint"]
        assert set(MANIFEST_KEYS) <= set(manifest)

    def test_unknown_engine_rejected(self):
        with pytest.raises(ValueError, match="engine"):
            build_capability_manifest(["start"], engine="perl")

    def test_python_worker_manifest_is_registry_projection(self):
        # 自动生成的硬判据：supported 逐元素等于 registry 真值，不是第二份手写表。
        manifest = python_worker_manifest()
        assert manifest["engine"] == ENGINE_PYTHON
        assert manifest["supported_node_types"] == sorted(
            get_default_registry().list_types()
        )
        # 内置 IR 白名单的可读性抽检：Python 引擎的 loop 家族必须在列。
        for node_type in ("start", "end", "map", "while", "http"):
            assert node_type in manifest["supported_node_types"]

    def test_registered_extra_node_flows_into_manifest(self):
        from plaita.node.basic import Node

        class Probe(Node):
            node_type = "probe_300"

        get_default_registry().register(Probe)
        manifest = python_worker_manifest()
        assert "probe_300" in manifest["supported_node_types"]
        assert manifest["supported_node_types"] == sorted(
            get_default_registry().list_types()
        )


class TestManifestFromMetadata:
    def test_roundtrip(self):
        manifest = python_worker_manifest()
        assert manifest_from_metadata({"capability_manifest": manifest}) == manifest

    def test_missing_returns_none(self):
        assert manifest_from_metadata({}) is None
        assert manifest_from_metadata({"capability_manifest": None}) is None

    def test_malformed_returns_none(self):
        assert manifest_from_metadata({"capability_manifest": "not-a-dict"}) is None
        assert (
            manifest_from_metadata({"capability_manifest": {"engine": "python"}})
            is None
        )
        assert manifest_from_metadata(
            {"capability_manifest": {"engine": "python", "supported_node_types": "x",
                                     "unsupported_node_types": [], "features": [],
                                     "ir_contract_version": "v"}}
        ) is None

    def test_go_manifest_with_unsupported_list(self):
        go = build_capability_manifest(
            ["start", "end", "http"],
            engine=ENGINE_GO,
            unsupported_node_types=["map", "while"],
            features=["suspend", "checkpoint", "hitl_await"],
        )
        assert manifest_from_metadata({"capability_manifest": go}) == go


class TestUnsupportedNodeTypesFor:
    def test_flow_minus_supported(self):
        manifest = build_capability_manifest(["start", "end", "http"], engine=ENGINE_PYTHON)
        assert unsupported_node_types_for(manifest, ["start", "map", "end", "while"]) == [
            "map",
            "while",
        ]

    def test_all_supported_empty(self):
        manifest = build_capability_manifest(["start", "end"], engine=ENGINE_PYTHON)
        assert unsupported_node_types_for(manifest, ["start", "end"]) == []


class TestWorkerRegistryMetadata:
    def _worker(self, redis):
        from plaita.event.memory import InMemoryEventBus
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        return RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name="plaita:flow:queue:test300",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            event_bus=InMemoryEventBus(),
            redis_client=redis,
            enable_registry=True,
            enable_redis_logging=False,
        )

    def test_registration_metadata_carries_manifest(self):
        from plaita.server.registry import ServiceRegistry

        redis = FakeRedis(decode_responses=True)
        worker = self._worker(redis)
        info = worker._service_info
        manifest = info.metadata.get("capability_manifest")
        assert isinstance(manifest, dict)
        assert manifest["engine"] == ENGINE_PYTHON
        assert manifest["supported_node_types"] == sorted(
            get_default_registry().list_types()
        )
        # 注册键写盘路径与既有 metadata 同一份 SETEX：manifest 随键持久化，
        # console 读面按 metadata.capability_manifest 取到。
        registry = ServiceRegistry(redis)
        assert registry.register(info)
        stored = __import__("json").loads(
            redis.get(f"plaita:registry:flow_worker:{info.instance_id}")
        )
        assert stored["metadata"]["capability_manifest"]["engine"] == ENGINE_PYTHON
        worker.unregister_service()
