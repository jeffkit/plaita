"""
应用配置
"""
import os
from typing import Optional
from pydantic import BaseModel, Field


class Settings(BaseModel):
    """应用配置"""
    
    # Redis 配置
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis 连接 URL"
    )
    
    # 服务器配置
    host: str = Field(default="0.0.0.0", description="服务器监听地址")
    port: int = Field(default=8080, description="服务器监听端口")
    debug: bool = Field(default=False, description="调试模式")
    
    # CORS 配置
    cors_origins: list = Field(
        default=["http://localhost:5173", "http://localhost:3000"],
        description="允许的跨域来源"
    )
    
    # API 配置
    api_prefix: str = Field(default="/api", description="API 路径前缀")

    # 流程编排持久化（SQLAlchemy）配置
    db_url: str = Field(
        default="sqlite:///./plaita_console.db",
        description="流程定义/节点描述持久化数据库 URL（SQLAlchemy）"
    )

    # 对外契约接口 /api/flowVersion/semver/detail 的 HMAC 鉴权密钥
    # 空密钥时接口返回 503（fail-closed），不再接受空串签名。
    secret_id: str = Field(default="", description="HMAC secret-id（空则契约接口不可用）")
    secret_key: str = Field(default="", description="HMAC secret-key")

    # 管理面 API Key（X-Admin-API-Key / Bearer）。空且未 allow_insecure_admin 时拒绝管理 API。
    admin_api_key: str = Field(default="", description="管理面 API Key")
    allow_insecure_admin: bool = Field(
        default=False,
        description="显式允许无管理密钥启动（仅本地开发）",
    )

    # 部署环境标识（写入部署记录/审计；prod 时删除操作需 admin）
    console_env: str = Field(default="dev", description="环境标识：dev/test/prod")

    # Copilot 大脑：flow_agent（默认，轻量内置 agent）| recursive | claude
    copilot_brain: str = Field(
        default="flow_agent",
        description="Copilot 大脑：flow_agent | recursive | claude",
    )

    # Copilot 大脑：recursive http 服务的 AG-UI 端点（POST /agui）。
    # URL 为空时 /api/copilot 返回 503。
    recursive_agui_url: str = Field(
        default="",
        description="recursive http 服务 /agui 端点 URL（如 http://127.0.0.1:8787/agui）",
    )
    recursive_agui_api_key: str = Field(
        default="",
        description="recursive 服务 X-API-Key（对应其 RECURSIVE_API_KEYS）",
    )

    class Config:
        env_prefix = "PLAITA_CONSOLE_"


def get_settings() -> Settings:
    """获取配置实例"""
    return Settings(
        redis_url=os.getenv("PLAITA_CONSOLE_REDIS_URL", "redis://localhost:6379/0"),
        host=os.getenv("PLAITA_CONSOLE_HOST", "0.0.0.0"),
        port=int(os.getenv("PLAITA_CONSOLE_PORT", "8080")),
        debug=os.getenv("PLAITA_CONSOLE_DEBUG", "false").lower() == "true",
        db_url=os.getenv("PLAITA_CONSOLE_DB_URL", "sqlite:///./plaita_console.db"),
        secret_id=os.getenv("PLAITA_CONSOLE_SECRET_ID", ""),
        secret_key=os.getenv("PLAITA_CONSOLE_SECRET_KEY", ""),
        admin_api_key=os.getenv("PLAITA_CONSOLE_ADMIN_API_KEY", ""),
        allow_insecure_admin=os.getenv(
            "PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", "false"
        ).lower()
        == "true",
        console_env=os.getenv("PLAITA_CONSOLE_ENV", "dev"),
        copilot_brain=os.getenv("PLAITA_CONSOLE_COPILOT_BRAIN", "flow_agent"),
        recursive_agui_url=os.getenv("PLAITA_CONSOLE_RECURSIVE_AGUI_URL", ""),
        recursive_agui_api_key=os.getenv("PLAITA_CONSOLE_RECURSIVE_AGUI_API_KEY", ""),
    )
