"""console 服务页的 worker 引擎版本展示与最低版本告警（#36）。

worker 在注册/心跳 metadata 里上报 ``plaita_version``（``plaita/server/
flow_worker.py`` 的 ``init_registry(metadata=...)``）。console 侧把该键提到
``ServiceInfo`` 顶层（metadata 不会自动提升）并按下限告警——混部舰队
（``PLAITA_PYTHON`` 指向未同步 venv / 滚升收尾）下这是辨识新旧 worker 的
唯一信号，此前注册元数据只有队列/缓存项。
"""
import asyncio
import json
import sys
from pathlib import Path

import fakeredis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import services as services_api  # noqa: E402


def _payload(metadata: dict, service_type: str = "flow_worker") -> str:
    return json.dumps(
        {
            "instance_id": "w1",
            "service_type": service_type,
            "host": "localhost",
            "status": "running",
            "metadata": metadata,
        }
    )


class TestWorkerVersionAnnotation:
    def test_version_is_promoted_and_no_threshold_means_no_alert(self, monkeypatch):
        monkeypatch.delenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", raising=False)
        info = services_api.parse_service_data(_payload({"plaita_version": "0.6.1"}))
        assert info.plaita_version == "0.6.1"
        assert info.version_alert is None

    def test_below_minimum_alerts(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.7.0")
        info = services_api.parse_service_data(_payload({"plaita_version": "0.6.1"}))
        assert info.version_alert == "plaita 0.6.1 低于最低要求 0.7.0"

    def test_equal_and_above_minimum_do_not_alert(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.6.1")
        assert services_api.parse_service_data(
            _payload({"plaita_version": "0.6.1"})
        ).version_alert is None
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.6.0")
        assert services_api.parse_service_data(
            _payload({"plaita_version": "0.6.1"})
        ).version_alert is None

    def test_missing_version_alerts_under_threshold(self, monkeypatch):
        """未上报版本 = 比上报功能更老的构建：配了下限就必须能看见它。"""
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.7.0")
        info = services_api.parse_service_data(_payload({"queue_name": "q"}))
        assert info.plaita_version is None
        assert "未上报引擎版本" in info.version_alert

    def test_short_version_compares_padded(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.6.1")
        assert services_api.parse_service_data(
            _payload({"plaita_version": "0.6"})
        ).version_alert == "plaita 0.6 低于最低要求 0.6.1"

    def test_unparseable_versions_never_alert(self, monkeypatch):
        """版本串来自注册表（半信任）：不可解析只跳过比对，不误报也不抛错。"""
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "latest")
        assert services_api.parse_service_data(
            _payload({"plaita_version": "0.6.1"})
        ).version_alert is None
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.7.0")
        assert services_api.parse_service_data(
            _payload({"plaita_version": "dev"})
        ).version_alert is None

    def test_non_string_version_does_not_break_the_card(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.7.0")
        info = services_api.parse_service_data(_payload({"plaita_version": 61}))
        assert info.plaita_version == "61"
        assert info.version_alert is None  # (61,) > (0, 7, 0)

    def test_non_worker_services_are_not_annotated(self, monkeypatch):
        """其它服务类型的 metadata 没有 plaita_version，不该被误标「未上报」。"""
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.7.0")
        info = services_api.parse_service_data(
            _payload({"queue_name": "q"}, service_type="schedule_service")
        )
        assert info.plaita_version is None
        assert info.version_alert is None


class TestManagedInstanceCards:
    """托管实例卡不参与版本展示/告警——版本告警的覆盖面**只到注册表里的实例**。

    console ``ServiceManager`` 拉起的 worker 自己会注册进 Redis（那张注册卡带
    ``plaita_version``）；托管卡只是控制台对自己那支进程的账，控制台无从得知
    ``PLAITA_PYTHON`` 那个 venv 里装的是哪版 plaita，故不冒充版本、也不发
    「未上报」告警（前端对 ``managed_by=console`` 的卡同样不渲染版本徽章）。
    """

    class _Manager:
        class _Config:
            infrastructure: dict = {}
            eventbus = None
            queue = None
            storage = None

        config = _Config()

        def list_instances(self):
            class _Instance:
                instance_id = "flow_worker-abc12345"
                service_type = "flow_worker"
                status = "running"
                start_time = "2026-10-10T00:00:00"
                pid = 1234

            return [_Instance()]

    def test_managed_flow_worker_card_has_no_version_and_no_alert(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_MIN_WORKER_VERSION", "0.7.0")
        fake = fakeredis.FakeRedis(decode_responses=True)
        monkeypatch.setattr(services_api, "get_redis", lambda request: fake)
        from services import service_manager

        monkeypatch.setattr(service_manager, "get_service_manager", lambda: self._Manager())

        result = asyncio.run(
            services_api.list_services(service_type="flow_worker", request=object())
        )
        cards = [s for s in result.services if s.instance_id == "flow_worker-abc12345"]
        assert len(cards) == 1
        assert cards[0].metadata["managed_by"] == "console"
        assert cards[0].plaita_version is None
        assert cards[0].version_alert is None
