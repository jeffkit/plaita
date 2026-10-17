"""全局 pytest 配置：给 integration 目录的用例自动打 integration marker。

历史上 `-m "not integration"` 只能筛掉显式标记的 1 个用例——
tests/integration/ 下绝大多数文件从未被打标，过滤器形同虚设
（2026-09 版本矩阵评审）。

另外把**当前解释器**交给 runner 辅助脚本（``plaita#167``，见下）。
"""
import os
import sys

import pytest

# plaita#167：``scripts/mutation-module-tests.sh`` / ``scripts/lib/
# mutation_recheck_common.sh`` 由测试**子进程**驱动（跑 pytest、读
# ``tomllib``）。它们此前裸解析 PATH 上的 ``python3``——macOS 系统 3.9.6 没有
# pytest 也没有 tomllib，于是「健康机器」上 4 条与变异无关的测试稳定假红
# （按「已知环境红灯」人工豁免正是可伪造通道）。
#
# 脚本侧已改成共享的解析顺序（``PYTHON_BIN`` → ``$VIRTUAL_ENV`` → 仓根
# ``.venv`` → PATH 上第一个 ≥3.11）。这里补上**当前解释器**这一条：跑测试的
# 解释器就是辅助脚本该用的解释器（``sys.executable``），经脚本既有的
# ``PYTHON_BIN`` 契约递进去。已显式设置时不覆盖。
os.environ.setdefault("PYTHON_BIN", sys.executable)


def pytest_collection_modifyitems(config, items):
    integration_dir = config.rootpath / "tests" / "integration"
    for item in items:
        if integration_dir in item.path.parents:
            item.add_marker(pytest.mark.integration)
