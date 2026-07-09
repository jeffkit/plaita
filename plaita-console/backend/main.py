"""
Plaita Console Backend - FastAPI 应用入口
"""
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis

# 支持多种运行方式的导入
try:
    from .config import get_settings
    from .auth import require_admin_auth
    from .api import services, executions, queues, logs, cluster, events
    from .api import nodes, flows, flow_version, dryrun
    from .services import flow_store, signature
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from config import get_settings
    from auth import require_admin_auth
    from api import services, executions, queues, logs, cluster, events
    from api import nodes, flows, flow_version, dryrun
    from services import flow_store, signature

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 全局 Redis 客户端
redis_client: Redis = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global redis_client
    
    settings = get_settings()
    
    # 启动时连接 Redis
    logger.info(f"连接 Redis: {settings.redis_url}")
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    
    # 测试连接
    try:
        redis_client.ping()
        logger.info("Redis 连接成功")
    except Exception as e:
        logger.error(f"Redis 连接失败: {e}")
        raise
    
    # 将 Redis 客户端注入到应用状态
    app.state.redis = redis_client

    # 初始化流程编排持久化（SQLAlchemy）—— 建表，不破坏 Redis 初始化
    try:
        flow_store.init_engine(settings.db_url)
        logger.info(f"FlowStore 已初始化: {settings.db_url}")
    except Exception as e:
        logger.error(f"FlowStore 初始化失败: {e}")
        raise

    # HMAC 重放保护：有 Redis 时启用 nonce store
    try:
        signature.enable_replay_protection(settings.redis_url)
        logger.info("HMAC replay protection 已启用（Redis nonce store）")
    except Exception as e:
        logger.warning("HMAC replay protection 启用失败，回退内存 nonce: %s", e)

    if not settings.admin_api_key and not settings.allow_insecure_admin:
        logger.warning(
            "未配置 PLAITA_CONSOLE_ADMIN_API_KEY：管理 API 将返回 503，"
            "直到配置密钥或显式设置 ALLOW_INSECURE_ADMIN=true"
        )
    if not settings.secret_id or not settings.secret_key:
        logger.warning(
            "未配置 PLAITA_CONSOLE_SECRET_ID/SECRET_KEY：契约接口 /flowVersion 将返回 503"
        )

    yield
    
    # 关闭时断开 Redis 连接
    logger.info("关闭 Redis 连接")
    redis_client.close()


def create_app() -> FastAPI:
    """创建 FastAPI 应用"""
    settings = get_settings()
    
    app = FastAPI(
        title="Plaita Console API",
        description=(
            "Plaita 流程引擎管理控制台 API。\n\n"
            "**管理面 (admin)**：Console 前端 / 运维；鉴权 "
            "`X-Admin-API-Key` 或 `Authorization: Bearer`。\n\n"
            "**契约面 (contract)**：外部 `PlaitaClient`；鉴权 HMAC "
            "（`PLAITA_CONSOLE_SECRET_ID/SECRET_KEY`）；"
            "仅 `POST /api/flowVersion/semver/detail`。"
        ),
        version="1.0.0",
        lifespan=lifespan,
        openapi_tags=[
            {"name": "admin", "description": "管理面：需 Admin API Key"},
            {"name": "contract", "description": "契约面：需 HMAC 签名"},
            {"name": "health", "description": "健康检查（无鉴权）"},
        ],
    )
    
    # 添加 CORS 中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    admin_deps = [Depends(require_admin_auth)]
    prefix = settings.api_prefix

    def _mount_admin(router, resource_tag: str) -> None:
        """挂载管理面路由：Admin API Key + OpenAPI tag=admin/<resource>。"""
        app.include_router(
            router,
            prefix=prefix,
            tags=["admin", resource_tag],
            dependencies=admin_deps,
        )

    def _mount_contract(router, resource_tag: str) -> None:
        """挂载契约面路由：独立 HMAC，不加 admin 依赖。"""
        app.include_router(
            router,
            prefix=prefix,
            tags=["contract", resource_tag],
        )

    # --- 管理面（需 Admin API Key）---
    _mount_admin(services.router, "services")
    _mount_admin(executions.router, "executions")
    _mount_admin(queues.router, "queues")
    _mount_admin(logs.router, "logs")
    _mount_admin(cluster.router, "cluster")
    _mount_admin(events.router, "events")
    _mount_admin(nodes.router, "nodes")
    _mount_admin(flows.router, "flows")
    _mount_admin(dryrun.router, "dryrun")

    # --- 契约面（独立 HMAC，不加 admin 依赖）---
    _mount_contract(flow_version.router, "flow_version")
    
    @app.get("/", tags=["health"])
    async def root():
        """健康检查"""
        return {"status": "ok", "service": "plaita-console"}
    
    @app.get("/health", tags=["health"])
    async def health():
        """健康检查端点"""
        return {"status": "healthy"}
    
    return app


# 创建应用实例
app = create_app()


def run_server():
    """启动服务器（用于脚本入口）"""
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )


if __name__ == "__main__":
    run_server()
