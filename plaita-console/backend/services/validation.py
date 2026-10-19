"""保存与晋升导入共用的标识 / Flow 定义校验。

A2R25-03：导入晋升包原先只校验 ``kind`` + 指纹，绕过正常保存路径的
``_check_semver`` 与 ``_validate_definition``，可把不可寻址的版本号
（``not-a-version``、``../escape``）和 schema 非法的 Flow 直接落库/发布。
这里把三条判据收敛到单一实现，保存与导入都调用同一份代码，避免两套口径
再次分叉。

异常一律用 ``ValueError``（调用方按 422 语义映射）；本模块不依赖 FastAPI，
服务层（deployments）与 API 层都能直接复用。
"""
from __future__ import annotations

import re
from typing import Any

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[\w.]+)?(?:\+[\w.]+)?$")

# flows.flow_id / flow_versions.flow_id 的列宽（models/flow.py）。
FLOW_ID_MAX_LENGTH = 128


def is_semver(version: str) -> bool:
    """版本号是否可被 console 的 per-version 路由寻址。"""
    return bool(SEMVER_RE.match(version or ""))


def validate_flow_id(flow_id: str, *, max_length: int = FLOW_ID_MAX_LENGTH) -> None:
    """flow_id 会进入 URL 路径段与存储键：拒绝空值、路径分隔符与超长。

    不收紧字符集（历史 flow_id 可能是中文/空格），只堵路径穿越与数据库
    截断两类确定性问题：``/``、``\\``、NUL、``.``/``..``、超过列宽。
    """
    if not isinstance(flow_id, str) or not flow_id.strip():
        raise ValueError("flow_id 不能为空")
    if len(flow_id) > max_length:
        raise ValueError(f"flow_id 过长（{len(flow_id)} > {max_length}）")
    if flow_id in (".", "..") or any(ch in flow_id for ch in ("/", "\\", "\x00")):
        raise ValueError("flow_id 非法：不得包含路径分隔符或空字节")


def validate_definition(definition: Any) -> None:
    """用 ``Flow.model_validate`` 强校验解析后的 definition。"""
    from plaita.core.flow import Flow

    try:
        Flow.model_validate(definition)
    except Exception as e:  # noqa: BLE001 — 统一转成 422 语义（原 _validate_definition 同口径）
        raise ValueError(f"Flow 校验失败: {e}") from e
