"""全局 pytest 配置：给 integration 目录的用例自动打 integration marker。

历史上 `-m "not integration"` 只能筛掉显式标记的 1 个用例——
tests/integration/ 下绝大多数文件从未被打标，过滤器形同虚设
（2026-09 版本矩阵评审）。
"""
import pytest


def pytest_collection_modifyitems(config, items):
    integration_dir = config.rootpath / "tests" / "integration"
    for item in items:
        if integration_dir in item.path.parents:
            item.add_marker(pytest.mark.integration)
