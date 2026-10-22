"""Supervisor 面板后端：把 plaita-ai 的自迭代循环暴露给 console 前端。

设计约束：
- **懒加载 plaita-ai**：backend 默认部署不依赖它；未安装时端点返回 503 与
  安装指引（`pip install -e ./plaita-ai`），不影响现有功能。
- **数据集根目录**：`PLAITA_SUPERVISOR_DATASET_DIR`（默认
  `./supervisor-datasets`）。数据集路径必须落在该目录内（防穿越），目录下每个
  子目录/JSON 文件即一个评测集。
- **promote 永远由人**：本 API 只跑迭代并返回 promotion ticket；发布复用
  既有 `POST /api/flows/{id}/publish`（前端 publishFlow），审计走既有链路。
- **多租户边界（plaita#290）**：iterate 入口先按调用方租户解析 flow 归属——
  跨租户 / 不存在一律 404（与 `GET /api/flows/{id}` 同口径，不暴露存在性）；
  回环客户端以平台 key **钉在调用方租户**（`X-Tenant-ID`），基线评测、候选
  草稿、dry-run 全部落在调用方租户，不再「读全量、写 default」。平台身份
  跨租户须显式带 `X-Tenant-ID`；会话型平台管理员全量视角是只读的（403）。
- **回环目标固定（plaita#291）**：回环地址只来自部署配置（`_loopback_base_url`：
  `PLAITA_CONSOLE_LOOPBACK_URL` 或由自身监听 host/port 推导），**绝不取
  `request.base_url`**——那是请求方可控的 `Host` 头，拿它当目标等于把平台
  admin key 送往调用方指定的任意主机。`_local_client` 不接触 request。
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field

try:  # A2R24-01：统一错误 detail 形状 + 异常文本脱敏
    from ..error_contract import error_detail, scrubbed_detail
    from ..request_models import StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from error_contract import error_detail, scrubbed_detail  # type: ignore
    from request_models import StrictRequestModel  # type: ignore

try:  # plaita#290：flow 归属校验 + 回环租户
    from ..auth import tenant_scope
    from ..services import flow_store
except ImportError:
    from auth import tenant_scope  # type: ignore
    from services import flow_store  # type: ignore

logger = logging.getLogger("api.supervisor")

router = APIRouter()


class SupervisorIterateRequest(StrictRequestModel):
    dataset: str = Field(..., description="数据集名（数据集根目录下的子目录或 .json 文件名）")
    max_iterations: int = Field(1, ge=1, le=5, description="本次跑几轮迭代")
    #: flow = @flow 源码提案 + 编译门（编排单轨默认）；prompt = legacy JSON 提案
    proposer: str = Field("flow", description="flow（默认，FlowSourceProposer）| prompt（legacy）| static")


def _dataset_root() -> Path:
    root = Path(os.getenv("PLAITA_SUPERVISOR_DATASET_DIR", "./supervisor-datasets")).resolve()
    return root


def _resolve_dataset_path(name: str) -> Path:
    root = _dataset_root()
    candidate = (root / name).resolve()
    # #209：按**路径归属**判越界，不是字符串前缀。`startswith` 会放行同前缀
    # 兄弟目录（root 与 root-evil/）；is_relative_to 按路径段比较，绝对路径
    # 与 `..` 逃逸解析后同样拒绝。
    if not candidate.is_relative_to(root):
        raise HTTPException(status_code=400, detail="dataset 路径越界")
    if not candidate.exists():
        raise HTTPException(status_code=404, detail=f"数据集不存在: {name}（根目录 {root}）")
    return candidate


def _load_plaita_ai():
    try:
        from plaita_ai.console_client import ConsoleClientError  # noqa: F401
        from plaita_ai.evals import load_dataset
        from plaita_ai.supervisor import (
            FlowSourceProposer,
            PromptProposer,
            StaticProposer,
            Supervisor,
            SupervisorPolicy,
        )
    except ImportError as exc:
        raise HTTPException(
            status_code=503,
            detail=error_detail(
                "supervisor_unavailable",
                "plaita-ai 未安装。在 backend 环境执行: pip install -e ./plaita-ai",
            ),
        ) from exc
    return load_dataset, FlowSourceProposer, PromptProposer, StaticProposer, Supervisor, SupervisorPolicy


def _loopback_base_url() -> str:
    """supervisor 回环目标（plaita#291）：固定内部源，与请求 ``Host`` 解耦。

    只认部署方权威的两个来源：

    - ``PLAITA_CONSOLE_LOOPBACK_URL``：显式覆盖（uvicorn CLI 另指端口、进程内
      TLS 等非常规部署）。非法值 503 具名拒绝、不静默回退推导值（回环配置
      错误必须可见）；指向非本机地址照用（部署方权威）但 WARNING 留痕——
      平台 admin key 将发往该地址，必须可观测。
    - 否则由 console 自身监听配置推导：``PLAITA_CONSOLE_HOST`` 为通配
      （``0.0.0.0`` / ``::``）取对应回环地址，其余取绑定地址本身；端口取
      ``PLAITA_CONSOLE_PORT``。与 ``run.py`` / ``__main__.py`` 的 uvicorn
      参数同源，常规部署零配置。
    """
    try:
        from ..config import LOOPBACK_HOSTS, get_settings
    except ImportError:
        from config import LOOPBACK_HOSTS, get_settings  # type: ignore

    settings = get_settings()
    override = (os.getenv("PLAITA_CONSOLE_LOOPBACK_URL") or "").strip()
    if override:
        parts = urlsplit(override)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            message = (
                "PLAITA_CONSOLE_LOOPBACK_URL 非法（须为 http(s)://host[:port]）："
                f"{override!r}"
            )
            logger.warning("拒绝 supervisor 回环：%s", message)
            raise HTTPException(status_code=503, detail=error_detail(
                "loopback_misconfigured", message))
        if parts.hostname not in LOOPBACK_HOSTS and parts.hostname != settings.host:
            logger.warning(
                "supervisor 回环目标指向非本机地址 %s（PLAITA_CONSOLE_LOOPBACK_URL）"
                "：平台 admin key 将发往该地址，确认它是本 console 的可信内部入口",
                override,
            )
        return override.rstrip("/")

    host = settings.host or "127.0.0.1"
    if host in ("0.0.0.0", "*", ""):
        host = "127.0.0.1"
    elif host == "::":
        host = "::1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{settings.port}"


def _local_client(tenant_id: str):
    """以 backend 自己的身份（admin key）构造 console 客户端——同进程回环。

    plaita#291：回环目标只来自 ``_loopback_base_url()``（部署配置），本函数
    **不持有 request**——``request.base_url`` 由请求方可控的 ``Host`` 头拼成，
    拿它当目标就是把平台 admin key 送往调用方指定的任意主机。
    plaita#290：平台 key 是全量视角凭证，回环必须**钉在调用方租户**
    （``ConsoleConfig.tenant_id`` → 每个请求带 ``X-Tenant-ID``），否则
    Supervisor 的 get_flow / dry-run 读到别租户、save_version 落 default。
    二选一里取「平台 key + 租户头」而非「转发调用方凭据」：对会话与 API-Key
    两类调用方同构，且保留既有「未配 admin key 即 503」契约。
    """
    from plaita_ai.console_client import ConsoleClient, ConsoleConfig

    if not os.getenv("PLAITA_CONSOLE_ADMIN_API_KEY"):
        raise HTTPException(
            status_code=503,
            detail="PLAITA_CONSOLE_ADMIN_API_KEY 未配置：supervisor 回环客户端需要它与本 console 通信",
        )
    base_url = _loopback_base_url()
    logger.debug("supervisor 回环目标 %s（租户 %s）", base_url, tenant_id)
    config = ConsoleConfig(
        base_url=base_url,
        api_prefix="/api",
        admin_api_key=os.getenv("PLAITA_CONSOLE_ADMIN_API_KEY", ""),
        tenant_id=tenant_id,
    )
    return ConsoleClient(config)


def _require_owned_flow(request: Request, flow_id: str) -> str:
    """解析调用方租户并校验 flow 归属（plaita#290）；返回生效租户。

    iterate 会落候选草稿，按**写路径**取租户（``required=True``）：会话型平台
    管理员全量视角 403（只读，A2R17-01）；无状态平台凭证不带 ``X-Tenant-ID``
    沿契约落 default。归属查无即 404，文案与 ``GET /api/flows/{id}`` 一致——
    「不存在」与「他租户存在」对外不可区分（不做存在性 oracle）。
    """
    tenant = tenant_scope(request, required=True)
    if flow_store.get_flow_store().get_flow_record(flow_id, tenant_id=tenant) is None:
        logger.warning(
            "拒绝 supervisor iterate：flow 不在调用方租户（flow=%s 租户=%s）",
            flow_id, tenant,
        )
        raise HTTPException(status_code=404, detail=f"流程不存在: {flow_id}")
    return tenant


@router.get("/flows/{flow_id}/supervisor/datasets")
def list_datasets(flow_id: str):
    """列出数据集根目录下可用的评测集（子目录与 .json 文件）。"""
    root = _dataset_root()
    if not root.exists():
        return {"flow_id": flow_id, "root": str(root), "datasets": []}
    items: List[Dict[str, Any]] = []
    for child in sorted(root.iterdir()):
        if child.name.startswith("_") or child.name.startswith("."):
            continue
        if child.is_dir():
            count = len(list(child.glob("*.json")))
            items.append({"name": child.name, "kind": "dir", "case_files": count})
        elif child.suffix == ".json":
            items.append({"name": child.name, "kind": "file", "case_files": 1})
    return {"flow_id": flow_id, "root": str(root), "datasets": items}


@router.post("/flows/{flow_id}/supervisor/iterate")
def supervisor_iterate(flow_id: str, req: SupervisorIterateRequest, request: Request):
    """对一条 flow 跑自迭代：基线评测 → 提案 → 候选评测 → 对比 → 闸门。

    只返回 promotion ticket；发布是人的动作（POST /flows/{id}/publish）。
    proposer 默认 flow：提案为 @flow 源码、过 compile_flow 编译门后才存版本，
    坏提案不再烧迭代与版本号（编排单轨 ADR-2026-08-27）；prompt 保留 legacy。
    """
    if req.proposer not in ("flow", "prompt", "static"):
        raise HTTPException(
            status_code=400,
            detail=f"未知 proposer: {req.proposer!r}（可选 flow | prompt | static）",
        )
    # plaita#290：归属校验先于一切回环——跨租户 / 不存在的 flow 到此即止，
    # 不加载 plaita-ai、不解析数据集、不构造回环客户端。
    tenant = _require_owned_flow(request, flow_id)
    load_dataset, FlowSourceProposer, PromptProposer, StaticProposer, Supervisor, SupervisorPolicy = (
        _load_plaita_ai()
    )
    dataset_path = _resolve_dataset_path(req.dataset)
    try:
        dataset = load_dataset(str(dataset_path))
    except Exception as exc:  # noqa: BLE001 —— 数据集问题原样带回给面板
        raise HTTPException(
            status_code=400,
            detail=scrubbed_detail(
                logger, "dataset_invalid", "数据集加载失败，请检查数据集名称与格式", exc
            ),
        ) from exc

    try:
        if req.proposer == "static":
            proposer: Any = StaticProposer()
        elif req.proposer == "prompt":
            proposer = PromptProposer()
        else:
            proposer = FlowSourceProposer()
    except Exception as exc:  # noqa: BLE001 —— proposer 未配置（如缺 PLAITA_AI_PROPOSER_* env）
        raise HTTPException(
            status_code=400,
            detail=error_detail("proposer_not_configured", str(exc)),
        ) from exc

    supervisor = Supervisor(
        _local_client(tenant_id=tenant),
        policy=SupervisorPolicy(promote_gate="manual", max_iterations=req.max_iterations),
        proposer=proposer,
    )
    try:
        result = supervisor.run_loop(flow_id, dataset)
    except Exception as exc:  # noqa: BLE001 —— 引擎/网络错误原样带回
        raise HTTPException(
            status_code=502,
            detail=scrubbed_detail(
                logger, "supervisor_iteration_failed", "supervisor 迭代失败", exc,
                exc_info=True,
            ),
        ) from exc
    # A55-S1：自迭代审计（flow_id/dataset；不记数据集内容与提案正文）。
    # 租户记**生效租户**（plaita#290）：与回环写入候选草稿的租户同值。
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action="supervisor.iterate",
        resource="supervisor",
        resource_id=flow_id,
        detail={
            "flow_id": flow_id,
            "dataset": req.dataset,
            "max_iterations": req.max_iterations,
            "proposer": req.proposer,
        },
        tenant_id=tenant,
    )
    return result


@router.get("/supervisor/config")
def supervisor_config():
    """面板自检：plaita-ai 是否可用、proposer 是否配置、数据集根目录。"""
    try:
        _load_plaita_ai()
        plaita_ai = "ok"
    except HTTPException as exc:
        plaita_ai = exc.detail
    proposer = "ok" if (os.getenv("PLAITA_AI_PROPOSER_BASE_URL") and os.getenv("PLAITA_AI_PROPOSER_MODEL")) else \
        "未配置 PLAITA_AI_PROPOSER_BASE_URL / PLAITA_AI_PROPOSER_MODEL"
    return {"plaita_ai": plaita_ai, "proposer": proposer, "dataset_root": str(_dataset_root())}
