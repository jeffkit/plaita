"""writefile 节点的路径 jail 部署注入（plaita#39）。

plaita-nodes 的 ``WriteFileNode`` 以环境变量 ``PLAITA_NODES_WORKSPACE_ROOT``
作为写入根；未设时保持历史行为——任意路径可写（含绝对路径与 ``../`` 穿越）。
机制在节点侧早已就位，但 worker / console 启动代码从不设置它，等于门一直开着：
能写流程 JSON 的人（或被注入的表达式求值结果）可写 ``/etc/cron.d/...``、worker
自身代码或配置——从「写产物」升级为持久化 RCE 原语。

本模块把缺的调用点补上，由各部署入口（worker CLI、console 后端）启动时调用。
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# 与 plaita-nodes/src/plaita_nodes/write_file.py 的读取端同名（仓外契约）
ROOT_ENV = "PLAITA_NODES_WORKSPACE_ROOT"
# 单机信任部署的显式回退开关
OPT_OUT_ENV = "PLAITA_ALLOW_UNRESTRICTED_WRITES"
_PROJECT_ROOT_ENV = "PLAITA_PROJECT_ROOT"


def _default_root() -> Optional[str]:
    """无显式配置时的默认 jail：部署根 → 工作目录 → 家目录。

    ``/`` 不构成边界（等于不设 jail），逐级后退；都不可用则返回 None。
    """
    candidates = [os.environ.get(_PROJECT_ROOT_ENV) or "", os.getcwd()]
    home = os.path.expanduser("~")
    if home and "~" not in home:
        candidates.append(home)
    for raw in candidates:
        raw = raw.strip()
        if not raw:
            continue
        resolved = Path(raw).resolve()
        if resolved == Path(resolved.anchor):
            continue
        return str(resolved)
    return None


def apply_writefile_jail(component: str = "worker") -> Optional[str]:
    """把 writefile 的写入 jail 注入进程环境，返回生效的根（None = 未设）。

    次序：显式 ``PLAITA_NODES_WORKSPACE_ROOT``（尊重运营者配置）→ 显式 opt-out
    ``PLAITA_ALLOW_UNRESTRICTED_WRITES=1``（单机信任模式，回到历史行为）→
    默认根。默认根由部署根/工作目录/家目录逐级推导，至少把写入约束在 worker
    用户自己的可写范围内，而非整个文件系统。
    """
    explicit = (os.environ.get(ROOT_ENV) or "").strip()
    if explicit:
        logger.info("[%s] writefile 写入 jail: %s（显式配置）", component, explicit)
        return explicit

    if os.environ.get(OPT_OUT_ENV, "").strip() == "1":
        logger.warning(
            "[%s] writefile 写入 jail 已显式关闭（%s=1）：流程可写任意路径，"
            "仅限单机信任部署；多租户/不受信流程请去掉该变量并配置 %s",
            component, OPT_OUT_ENV, ROOT_ENV,
        )
        return None

    root = _default_root()
    if root is None:
        logger.warning(
            "[%s] 无法推导 writefile 写入 jail（%s / 工作目录 / 家目录均不可用）："
            "保持历史行为（任意路径可写），请显式配置 %s",
            component, _PROJECT_ROOT_ENV, ROOT_ENV,
        )
        return None

    os.environ[ROOT_ENV] = root
    logger.info(
        "[%s] writefile 写入 jail 默认注入: %s（%s=1 可显式关闭）",
        component, root, OPT_OUT_ENV,
    )
    return root
