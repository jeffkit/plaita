"""hatchling 构建钩子（#204）：把 ``backend/`` 暂存成干净的 ``plaita_console/``。

暂存规则在 :mod:`scripts.wheel_stage`（不依赖 hatchling，可单测）；本文件只做
钩子接线。为什么需要暂存见该模块 docstring。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

_SPEC = importlib.util.spec_from_file_location(
    "_plaita_wheel_stage", Path(__file__).resolve().with_name("wheel_stage.py")
)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover - 文件必然存在
    raise ImportError("找不到 scripts/wheel_stage.py")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

stage_package = _MODULE.stage_package


class WheelStageHook(BuildHookInterface):
    """把 backend/ 暂存为 wheel 包目录。"""

    PLUGIN_NAME = "wheel-stage"

    def initialize(self, version: str, build_data: dict) -> None:  # noqa: ARG002
        stage_package(Path(self.root))
