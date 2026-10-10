"""worker 引擎版本可观测（#36）：注册元数据带 plaita 版本。

``_compute_flow_hash`` 改算原始存储定义后，引擎版本漂移本身不再毁在途 run（见
``test_worker_flow_hash.py``）；本文件钉住**可观测面**：混部舰队（滚动升级窗口 /
console 用 ``PLAITA_PYTHON`` 指向未同步的 venv）里旧 worker 必须能被认出来——
在此之前注册元数据只有 queue/redis/cache 项，新旧 worker 在服务页长得一模一样。
"""
import json
import sys
import types

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

import plaita
from plaita.server import flow_worker as fw
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _worker(fake, **kw) -> RedisFlowWorker:
    kw.setdefault("enable_redis_logging", False)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:worker-version",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        **kw,
    )


class TestEngineVersion:
    def test_engine_version_is_package_version(self):
        assert fw._engine_version() == plaita.__version__

    def test_engine_version_falls_back_to_unknown(self, monkeypatch):
        """版本读不到绝不能影响执行：退化成 'unknown' 而不是抛错。"""
        monkeypatch.setitem(sys.modules, "plaita", types.ModuleType("plaita"))
        assert fw._engine_version() == "unknown"


class TestRegistrationMetadata:
    def test_registration_metadata_carries_plaita_version(self):
        """注册元数据必须带 plaita_version——服务页/最低版本告警的唯一数据源。"""
        worker = _worker(fakeredis.FakeRedis(decode_responses=True), enable_registry=True)
        assert worker._service_info is not None
        assert worker._service_info.metadata["plaita_version"] == plaita.__version__
        # 队列/缓存项保持原样（零回归）
        assert worker._service_info.metadata["queue_name"] == "test:worker-version"

    def test_registered_payload_exposes_version(self):
        """落进注册表的 JSON 里也带着版本（心跳线程复用同一 ServiceInfo 序列化）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake, enable_registry=True)
        assert worker.register_service() is True
        try:
            key = f"plaita:registry:{worker.SERVICE_TYPE}:{worker.instance_id}"
            payload = json.loads(fake.get(key))
            assert payload["metadata"]["plaita_version"] == plaita.__version__
        finally:
            worker.unregister_service()
