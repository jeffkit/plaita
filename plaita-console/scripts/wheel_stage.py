"""wheel 打包暂存（#204）：把 ``backend/`` 的运行时代码拷成干净的包目录。

单独成模块（不 import hatchling）是为了能在普通测试环境里直接单测暂存规则；
``scripts/hatch_build.py`` 的构建钩子只负责调用 :func:`stage_package`。

背景：hatchling 的 ``force-include`` 不经过 ``exclude`` 过滤
（``recurse_forced_files`` 直接遍历源目录），直接
``force-include = { "backend" = "plaita_console" }`` 会把源码目录里的一切装进
用户 site-packages——``backend/tests/``、本地 ``backend/build/lib`` 旧树、
运行期 ``*.db``、backend 自己的 ``pyproject.toml``/``requirements.txt``、
``*.egg-info``。这里做**黑名单式**暂存：只丢已知非运行时产物，运行时代码
（含可选的 ``webDist`` 前端产物）原样保留；新增 backend 顶层模块无需改打包配置。
"""
from __future__ import annotations

import shutil
from pathlib import Path

#: 暂存根目录（相对项目根；``build/`` 已被 .gitignore 排除）
STAGE_ROOT = Path("build") / "wheel-package"
#: 暂存出的包目录
STAGE_PACKAGE = STAGE_ROOT / "plaita_console"

#: backend 顶层里属于「构建/开发配置」而非运行时的文件
EXCLUDED_TOP_LEVEL_FILES = frozenset({"pyproject.toml", "requirements.txt"})
#: backend 顶层里属于「测试/本地构建」而非运行时的目录
EXCLUDED_TOP_LEVEL_DIRS = frozenset({"tests", "build"})
#: backend 顶层里的非运行时文件后缀/名字
EXCLUDED_TOP_LEVEL_SUFFIXES = (".db", ".pyc", ".pyo", ".egg-info", ".dist-info")

#: 任意层级的非运行时目录/文件（shutil.ignore_patterns 语义）
NON_RUNTIME = shutil.ignore_patterns(
    "__pycache__",
    "*.pyc",
    "*.pyo",
    "tests",
    "build",
    "*.egg-info",
    "*.db",
)


def _is_runtime_entry(entry: Path) -> bool:
    name = entry.name
    if name in EXCLUDED_TOP_LEVEL_FILES or name in EXCLUDED_TOP_LEVEL_DIRS:
        return False
    return not name.endswith(EXCLUDED_TOP_LEVEL_SUFFIXES)


def stage_package(project_root: Path) -> Path:
    """把 ``<project_root>/backend`` 暂存为 ``build/wheel-package/plaita_console``。

    返回暂存出的包目录；不清理 ``build/`` 下的其他产物（只替换暂存根）。
    """
    project_root = Path(project_root)
    source = project_root / "backend"
    if not source.is_dir():
        raise FileNotFoundError(f"缺少 backend/ 源码目录: {source}")

    stage_root = project_root / STAGE_ROOT
    stage = project_root / STAGE_PACKAGE
    shutil.rmtree(stage_root, ignore_errors=True)
    stage.mkdir(parents=True)

    for entry in sorted(source.iterdir()):
        if not _is_runtime_entry(entry):
            continue
        if entry.is_dir():
            shutil.copytree(entry, stage / entry.name, ignore=NON_RUNTIME)
        else:
            shutil.copy2(entry, stage / entry.name)

    return stage
