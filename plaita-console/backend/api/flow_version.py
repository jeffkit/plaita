"""
对外契约接口 — 兼容 PlaitaClient

POST /api/flowVersion/semver/detail
- 请求：application/x-www-form-urlencoded，字段 flowId、version
- 鉴权：Authorization 头，HMAC-SHA256 签名（与 plaita/client.py 对称）
- 响应：{ code, message, data: { flow: "<Flow JSON string>" } }
  - code 为 0 表示成功；非零表示业务错误（flow 不存在/未发布）
  - 鉴权失败返回 HTTP 401
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Request

try:
    from ..config import get_settings
    from ..services import flow_store, signature
except ImportError:
    from config import get_settings  # type: ignore
    from services import flow_store, signature  # type: ignore

router = APIRouter()


def _envelope(code: int, message: str, flow: Optional[str] = None) -> dict:
    data = {"flow": flow} if flow is not None else {}
    return {"code": code, "message": message, "data": data}


@router.post("/flowVersion/semver/detail")
async def flow_version_detail(request: Request):
    settings = get_settings()
    # fail-closed：空密钥不再接受空串签名（与「空则禁用」文档对齐为真正禁用）
    if not settings.secret_id or not settings.secret_key:
        raise HTTPException(
            status_code=503,
            detail="契约接口未配置 PLAITA_CONSOLE_SECRET_ID/SECRET_KEY，已禁用",
        )

    authorization = request.headers.get("authorization", "")

    if not signature.verify_authorization(
        authorization, settings.secret_id, settings.secret_key
    ):
        raise HTTPException(status_code=401, detail="签名校验失败")

    form = await request.form()
    flow_id = form.get("flowId")
    version = form.get("version")

    if not flow_id or not version:
        return _envelope(1, "flowId/version 不能为空")

    store = flow_store.get_flow_store()
    out = store.get_version(flow_id, version)
    if out is None:
        return _envelope(2, f"流程不存在: {flow_id}@{version}")
    if out.status != "published":
        return _envelope(3, f"版本未发布: {flow_id}@{version}")

    return _envelope(0, "success", flow=out.definition)
