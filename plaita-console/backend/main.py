"""
Plaita Console Backend - FastAPI 应用入口
"""
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis

# 支持多种运行方式的导入
try:
    from .config import get_settings
    from .auth import require_auth
    from .api import services, executions, queues, logs, cluster, events
    from .api import nodes, flows, flow_version, dryrun, copilot, schedules, credentials, audit
    from .services import flow_store, signature, users_svc
    from .api import auth_users
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from config import get_settings
    from auth import require_auth
    from api import services, executions, queues, logs, cluster, events
    from api import nodes, flows, flow_version, dryrun, copilot, schedules, credentials, audit  # type: ignore
    from services import flow_store, signature, users_svc  # type: ignore
    from api import auth_users  # type: ignore

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
    
    # 启动时连接 Redis；不可达则进入本地单机模式（不阻断启动）
    local_mode = False
    try:
        redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
        redis_client.ping()
        logger.info(f"连接 Redis: {settings.redis_url}")
        app.state.redis = redis_client
    except Exception as e:
        redis_client = None
        app.state.redis = None
        local_mode = True
        logger.warning(
            "Redis 不可达（%s）——进入本地单机模式：流程在本进程内执行，"
            "执行历史存 SQLite；集群/事件/队列功能不可用。"
            "启动 Redis 并重启可恢复完整能力。",
            e,
        )
    app.state.local_mode = local_mode

    # 初始化流程编排持久化（SQLAlchemy）—— 建表，不破坏 Redis 初始化
    try:
        flow_store.init_engine(settings.db_url)
        logger.info(f"FlowStore 已初始化: {settings.db_url}")
        if local_mode:
            try:
                from .services import examples as examples_svc
            except ImportError:
                from services import examples as examples_svc  # type: ignore
            examples_svc.seed_example_flows()
        try:
            from .services import users_svc
        except ImportError:
            from services import users_svc  # type: ignore
        app.state.store = flow_store.get_flow_store()
        users_svc.ensure_bootstrap_user(app.state.store)
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


def _load_external_node_modules() -> None:
    """加载业务侧节点模块（PLAITA_CONSOLE_NODE_MODULES=mod.path[:mod2,...]）。

    console 校验/试跑业务 flow 时，其自定义节点须注册进运行时 registry。
    模块需暴露 register_all()（或任意可调用 register）。
    """
    import importlib
    import logging
    import sys

    for extra in [p for p in os.environ.get("PLAITA_CONSOLE_NODE_PATH", "").split(os.pathsep) if p]:
        if extra not in sys.path:
            sys.path.insert(0, extra)
    raw = os.environ.get("PLAITA_CONSOLE_NODE_MODULES", "")
    for mod_path in [m.strip() for m in raw.split(",") if m.strip()]:
        try:
            mod = importlib.import_module(mod_path)
            register = getattr(mod, "register_all") or getattr(mod, "register")
            register()
            # CODE 节点 opt-in（与 plaita-nodes 的 agentrun/capture 等保持一致）
            try:
                from plaita.node import register_code_node
                register_code_node()
            except ImportError:
                pass
            logging.getLogger("backend.main").info("已加载外部节点模块: %s", mod_path)
        except Exception as exc:  # noqa: BLE001
            logging.getLogger("backend.main").warning("外部节点模块加载失败 %s: %s", mod_path, exc)


if os.environ.get("PLAITA_CONSOLE_NODE_MODULES"):
    _load_external_node_modules()


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

    admin_deps = [Depends(require_auth)]
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
    _mount_admin(copilot.router, "copilot")
    _mount_admin(schedules.router, "schedules")
    _mount_admin(credentials.router, "credentials")
    _mount_admin(audit.router, "audit")

    # --- 契约面（独立 HMAC，不加 admin 依赖）---
    _mount_contract(flow_version.router, "flow_version")

    # --- 登录/用户管理（自带角色规则；users 前缀要求 admin）---
    app.include_router(auth_users.router, prefix=prefix)
    
    @app.get("/health", tags=["health"])
    async def health():
        """健康检查端点"""
        return {"status": "healthy"}

    # --- 打包发布模式：后端直接托管前端构建产物（pip 安装后无需 Node 环境）---
    # webDist 由 scripts/build_package.sh 从 frontend/dist 填充；仓库开发模式
    # 下该目录不存在，走 vite dev server，此处静默跳过。挂载在 API 路由之后，
    # /api 与上方健康检查不受影响；"/" 让给前端首页，健康检查走 /health。
    web_dist = Path(__file__).parent / "webDist"
    if web_dist.is_dir():
        from fastapi.staticfiles import StaticFiles
        from starlette.exceptions import HTTPException as StarletteHTTPException

        class SPAStaticFiles(StaticFiles):
            """BrowserRouter 深链接回退：未命中的静态路径一律回 index.html。"""

            async def get_response(self, path: str, scope):
                try:
                    return await super().get_response(path, scope)
                except StarletteHTTPException as exc:
                    if exc.status_code == 404:
                        return await super().get_response("index.html", scope)
                    raise

        app.mount("/", SPAStaticFiles(directory=web_dist, html=True), name="web")
        logger.info("已托管前端静态资源: %s", web_dist)
    else:

        @app.get("/", tags=["health"])
        async def root():
            """健康检查"""
            return {"status": "ok", "service": "plaita-console"}

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
