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
import json
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


class TestRustEngineSlot:
    """plaita-rs 引擎位次（B4 第十二轮全量入位第二步）：能力门可对 rust 池生效。

    喂法承 A1 r129（跨引擎 manifest 实产喂五键闸）：字面量为 plaita-rs
    ``plaita_engine::ir::capability_manifest()`` 实产 JSON（分支
    feat/manifest-slot，ir_contract_version=2026-10-23.2；25 supported /
    29 unsupported / features 空 / node_dependencies 挂起族七键），非手写。
    """

    RUST_MANIFEST = json.loads(
        """
{
 "engine": "plaita-rs",
 "ir_contract_version": "2026-10-23.2",
 "supported_node_types": [
  "approval",
  "assignment",
  "case",
  "child",
  "code",
  "delay",
  "end",
  "event",
  "filter",
  "find",
  "hitl_await",
  "http",
  "http_callback",
  "if",
  "kafka_queue",
  "loop",
  "map",
  "mock",
  "parallel",
  "redis_queue",
  "reduce",
  "reference",
  "start",
  "switch",
  "while"
 ],
 "unsupported_node_types": [
  "agentrun",
  "api_request",
  "calculate",
  "capture",
  "decision",
  "dingtalk_webhook",
  "email_send",
  "feishu_webhook",
  "gate",
  "generic_webhook",
  "git_publish",
  "github_comment",
  "hitl",
  "ignore",
  "llm",
  "mdb",
  "notify",
  "parse_json",
  "rate_limit",
  "readmdb",
  "redis",
  "report",
  "slack_webhook",
  "sleep",
  "sql_query",
  "trpc",
  "wecom_webhook",
  "writefile",
  "writemdb"
 ],
 "features": [],
 "node_dependencies": {
  "event": "external event publisher + subscription store (engine registers via the host SubscriptionRegistry)",
  "hitl_await": "hitl-server (message send) + reply poller publishing hitl_reply events",
  "delay": "delay service consuming {prefix}:delay:queue (Redis ZSET scheduling)",
  "approval": "approval service consuming {prefix}:approval:queue (console interaction surface)",
  "redis_queue": "redis queue consumer service publishing redis_message events",
  "kafka_queue": "kafka consumer service publishing kafka_message events",
  "http_callback": "callback ingress forwarding requests to the handle_callback_request family"
 }
}
"""
    )

    def test_engine_registered_and_runtime_mapping(self):
        from plaita.server.capability import (
            ENGINE_RS,
            ENGINE_TO_RUNTIME,
            ENGINES,
            runtime_for_engine,
        )

        assert ENGINE_RS == "plaita-rs"
        assert ENGINE_RS in ENGINES
        assert ENGINE_TO_RUNTIME[ENGINE_RS] == "rust"
        assert runtime_for_engine("plaita-rs") == "rust"
        assert runtime_for_engine("plaita-rust") is None

    def test_real_manifest_passes_shape_gate(self):
        # ACCEPT 格：实产五键 + node_dependencies 加法键整体过闸（第六键
        # 加法放行与 Go manifest 同口径，r129 已证）。
        manifest = manifest_from_metadata({"capability_manifest": self.RUST_MANIFEST})
        assert manifest == self.RUST_MANIFEST
        assert manifest["engine"] == "plaita-rs"
        assert manifest["ir_contract_version"] == "2026-10-23.2"
        assert manifest["features"] == []
        assert manifest["supported_node_types"] == sorted(manifest["supported_node_types"])
        assert manifest["unsupported_node_types"] == sorted(
            manifest["unsupported_node_types"]
        )

    def test_gate_semantics_on_rust_manifest(self):
        # 能力门核心纯函数对 rust manifest：集合差非空即拒。
        manifest = manifest_from_metadata({"capability_manifest": self.RUST_MANIFEST})
        assert unsupported_node_types_for(manifest, ["start", "map", "end"]) == []
        assert unsupported_node_types_for(manifest, ["start", "sql_query", "llm"]) == [
            "llm",
            "sql_query",
        ]

    def test_legacy_contract_version_key_is_no_manifest(self):
        # 键名闸（本轮负控锚）：旧键 contract_version 未对齐 console
        # MANIFEST_KEYS / Go json tags——按「无清单」走 fail-open 兜底，
        # 与旧 build worker 同待遇；改键名前此形态曾混过人工目检。
        m = dict(self.RUST_MANIFEST)
        del m["ir_contract_version"]
        m["contract_version"] = "2026-10-23.2"
        assert manifest_from_metadata({"capability_manifest": m}) is None

    def test_unknown_engine_still_rejected(self):
        # 守恒：engine 值不在册（注意 plaita-rust ≠ plaita-rs）仍判无清单。
        m = dict(self.RUST_MANIFEST)
        m["engine"] = "plaita-rust"
        assert manifest_from_metadata({"capability_manifest": m}) is None
        with pytest.raises(ValueError):
            build_capability_manifest(["start"], engine="plaita-rust")

    def test_python_go_manifests_conservation(self):
        # 守恒：注册第三引擎不动既有两引擎的判定。
        py = python_worker_manifest()
        assert manifest_from_metadata({"capability_manifest": py}) == py
        go = build_capability_manifest(["start", "end"], engine=ENGINE_GO)
        assert manifest_from_metadata({"capability_manifest": go}) == go
