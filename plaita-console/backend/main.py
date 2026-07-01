"""
Plaita Console Backend - FastAPI 应用入口
"""
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis

# 支持多种运行方式的导入
try:
    from .config import get_settings
    from .api import services, executions, queues, logs, cluster, events
    from .api import nodes, flows, flow_version, dryrun
    from .services import flow_store
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from config import get_settings
    from api import services, executions, queues, logs, cluster, events
    from api import nodes, flows, flow_version, dryrun
    from services import flow_store

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

    yield
    
    # 关闭时断开 Redis 连接
    logger.info("关闭 Redis 连接")
    redis_client.close()


def create_app() -> FastAPI:
    """创建 FastAPI 应用"""
    settings = get_settings()
    
    app = FastAPI(
        title="Plaita Console API",
        description="Plaita 流程引擎管理控制台 API",
        version="1.0.0",
        lifespan=lifespan
    )
    
    # 添加 CORS 中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # 注册路由
    app.include_router(services.router, prefix=settings.api_prefix, tags=["services"])
    app.include_router(executions.router, prefix=settings.api_prefix, tags=["executions"])
    app.include_router(queues.router, prefix=settings.api_prefix, tags=["queues"])
    app.include_router(logs.router, prefix=settings.api_prefix, tags=["logs"])
    app.include_router(cluster.router, prefix=settings.api_prefix, tags=["cluster"])
    app.include_router(events.router, prefix=settings.api_prefix, tags=["events"])
    app.include_router(nodes.router, prefix=settings.api_prefix, tags=["nodes"])
    app.include_router(flows.router, prefix=settings.api_prefix, tags=["flows"])
    app.include_router(dryrun.router, prefix=settings.api_prefix, tags=["dryrun"])
    app.include_router(flow_version.router, prefix=settings.api_prefix, tags=["flow_version"])
    
    @app.get("/")
    async def root():
        """健康检查"""
        return {"status": "ok", "service": "plaita-console"}
    
    @app.get("/health")
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

