"""Supervisor 面板后端：把 plaita-ai 的自迭代循环暴露给 console 前端。

设计约束：
- **懒加载 plaita-ai**：backend 默认部署不依赖它；未安装时端点返回 503 与
  安装指引（`pip install -e ./plaita-ai`），不影响现有功能。
- **数据集根目录**：`PLAITA_SUPERVISOR_DATASET_DIR`（默认
  `./supervisor-datasets`）。数据集路径必须落在该目录内（防穿越），目录下每个
  子目录/JSON 文件即一个评测集。
- **promote 永远由人**：本 API 只跑迭代并返回 promotion ticket；发布复用
  既有 `POST /api/flows/{id}/publish`（前端 publishFlow），审计走既有链路。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger("api.supervisor")

router = APIRouter()


class SupervisorIterateRequest(BaseModel):
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
    if not str(candidate).startswith(str(root)):
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
            detail=f"plaita-ai 未安装（{exc}）。在 backend 环境执行: pip install -e ./plaita-ai",
        ) from exc
    return load_dataset, FlowSourceProposer, PromptProposer, StaticProposer, Supervisor, SupervisorPolicy


def _local_client(request: Request):
    """以 backend 自己的身份（admin key）构造 console 客户端——同进程回环。"""
    from plaita_ai.console_client import ConsoleClient, ConsoleConfig

    if not os.getenv("PLAITA_CONSOLE_ADMIN_API_KEY"):
        raise HTTPException(
            status_code=503,
            detail="PLAITA_CONSOLE_ADMIN_API_KEY 未配置：supervisor 回环客户端需要它与本 console 通信",
        )
    config = ConsoleConfig(
        base_url=str(request.base_url).rstrip("/"),
        api_prefix="/api",
        admin_api_key=os.getenv("PLAITA_CONSOLE_ADMIN_API_KEY", ""),
    )
    return ConsoleClient(config)


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
    load_dataset, FlowSourceProposer, PromptProposer, StaticProposer, Supervisor, SupervisorPolicy = (
        _load_plaita_ai()
    )
    dataset_path = _resolve_dataset_path(req.dataset)
    try:
        dataset = load_dataset(str(dataset_path))
    except Exception as exc:  # noqa: BLE001 —— 数据集问题原样带回给面板
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        if req.proposer == "static":
            proposer: Any = StaticProposer()
        elif req.proposer == "prompt":
            proposer = PromptProposer()
        else:
            proposer = FlowSourceProposer()
    except Exception as exc:  # noqa: BLE001 —— proposer 未配置（如缺 PLAITA_AI_PROPOSER_* env）
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    supervisor = Supervisor(
        _local_client(request),
        policy=SupervisorPolicy(promote_gate="manual", max_iterations=req.max_iterations),
        proposer=proposer,
    )
    try:
        return supervisor.run_loop(flow_id, dataset)
    except Exception as exc:  # noqa: BLE001 —— 引擎/网络错误原样带回
        raise HTTPException(status_code=502, detail=f"supervisor 迭代失败: {exc}") from exc


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
