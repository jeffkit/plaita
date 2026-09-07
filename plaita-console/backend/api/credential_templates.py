"""
凭据类型模板 API（2026-09 凭据体验重设计 C3，已定案：后端承载）

- GET /api/credential-templates   返回内置模板注册表（只读）

模板定义新建凭据时的类型化字段表单：选中模板 → 按字段渲染表单 → 序列化回
现有 data JSON 载荷——存储格式零变更。「自定义 (JSON)」保留为兜底，前端渲染。
模板当前为后端代码内置（只读）；存储化自定义模板列后续扩展。
"""

from fastapi import APIRouter

router = APIRouter()

# 模板注册表：type 与现有凭据 type 标签对齐（webhook-bearer/database/generic 等
# 已在凭据页注释中出现）。secret=True 的字段前端以密码框渲染。
_TEMPLATES = [
    {
        "type": "bearer",
        "label": "Bearer Token",
        "desc": "Authorization: Bearer <token> 形式的接口鉴权",
        "fields": [
            {"key": "token", "label": "Token", "input_type": "string", "required": True, "secret": True},
        ],
    },
    {
        "type": "basic_auth",
        "label": "Basic Auth",
        "desc": "用户名 + 密码的基础认证",
        "fields": [
            {"key": "username", "label": "用户名", "input_type": "string", "required": True, "secret": False},
            {"key": "password", "label": "密码", "input_type": "string", "required": True, "secret": True},
        ],
    },
    {
        "type": "api_key_header",
        "label": "API Key（请求头）",
        "desc": "自定义请求头携带的 API Key",
        "fields": [
            {"key": "header_name", "label": "请求头名称", "input_type": "string", "required": True, "secret": False},
            {"key": "header_value", "label": "请求头值", "input_type": "string", "required": True, "secret": True},
        ],
    },
    {
        "type": "database",
        "label": "数据库连接",
        "desc": "关系库连接信息（host/port/user/password/database）",
        "fields": [
            {"key": "host", "label": "主机", "input_type": "string", "required": True, "secret": False},
            {"key": "port", "label": "端口", "input_type": "number", "required": False, "secret": False},
            {"key": "user", "label": "用户名", "input_type": "string", "required": True, "secret": False},
            {"key": "password", "label": "密码", "input_type": "string", "required": True, "secret": True},
            {"key": "database", "label": "库名", "input_type": "string", "required": False, "secret": False},
        ],
    },
    {
        "type": "webhook_secret",
        "label": "Webhook 密钥",
        "desc": "回调签名校验用的共享密钥",
        "fields": [
            {"key": "secret", "label": "密钥", "input_type": "string", "required": True, "secret": True},
        ],
    },
]


@router.get("/credential-templates")
def list_credential_templates():
    """凭据类型模板注册表（只读；generic 兜底由前端渲染）。"""
    return {"templates": _TEMPLATES, "total": len(_TEMPLATES)}
