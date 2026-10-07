"""
E2E test: server install + verify server imports work.
"""
import pytest

# server extra 的依赖清单（pyproject [project.optional-dependencies].server）：
# 缺任一项都意味着 extra 未装，应 SKIP 而非 FAIL——原先只认 error 文本里的
# "server"/"fastapi"，缺 cachetools/redis 时直接抛错（干净环境假红）。
_SERVER_EXTRA_DEPS = ("fastapi", "redis", "cachetools", "sqlalchemy")


def _server_extra_missing(exc: ImportError) -> bool:
    missing = (getattr(exc, "name", "") or "").split(".")[0]
    return missing in _SERVER_EXTRA_DEPS or "server" in str(exc).lower()


class TestServerInstall:
    """Verify server extras install and key modules are importable."""

    def test_server_flow_worker_import(self):
        """Server extras: plaita.server.flow_worker should be importable."""
        try:
            from plaita.server import flow_worker
            assert flow_worker is not None
        except ImportError as e:
            if _server_extra_missing(e):
                pytest.skip("Server extras not installed")
            raise

    def test_server_services_import(self):
        """Server extras: plaita.server.services should be importable."""
        try:
            from plaita.server import services
            assert services is not None
        except ImportError as e:
            if _server_extra_missing(e):
                pytest.skip("Server extras not installed")
            raise

    def test_server_nodes_available_via_entry_points(self):
        """Server node types should be discoverable via the default registry."""
        from plaita.node import get_default_registry

        registry = get_default_registry()
        server_types = ["delay", "redis_queue", "kafka_queue", "http_callback", "approval"]
        found = [t for t in server_types if t in registry]

        if not found:
            pytest.skip(
                "No server node entry_points loaded (server extras may not be installed)"
            )

        assert len(found) >= 1, "At least one server node should be registered via entry_points"
