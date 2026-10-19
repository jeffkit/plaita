"""SSE 专用 ``redis.asyncio`` 客户端（#259）。

同步 pubsub 的 ``get_message(timeout=1.0)`` 在 async 事件生成器里会阻塞整个
事件循环——单条空闲 SSE 连接即可冻结 console 的其他请求（idle 时 /health
2s 超时）。SSE 消费统一走本模块的 ``redis.asyncio`` 客户端；它与 lifespan 的
同步 ``app.state.redis`` 并存，按 ``app.state.redis_url``（集群切换会更新）
惰性重建，``api/cluster.py`` 切换时负责关闭旧客户端。
"""

# Redis 抖动时 SSE 循环的重试间隔（异常不终结流）。
SSE_REDIS_RETRY_SECONDS = 0.5


def get_sse_async_redis(request):
    """惰性创建/复用 app 级 ``redis.asyncio`` 客户端（SSE pubsub 专用）。

    测试可直接向 ``app.state.execution_sse_aioredis`` 注入 fake 客户端
    （键名沿用 C4-4；execution/logs 两个 SSE 流共用同一连接池）。
    """
    client = getattr(request.app.state, "execution_sse_aioredis", None)
    if client is None:
        try:
            from ..config import get_settings
        except ImportError:  # 平铺布局（cwd=backend）运行时
            from config import get_settings  # type: ignore
        import redis.asyncio as aioredis

        # 活动集群的 Redis 优先于 env（plaita#65）：切换集群会更新
        # app.state.redis_url 并清掉本缓存，直接读 env 会继续连旧集群。
        url = getattr(request.app.state, "redis_url", None) or get_settings().redis_url
        client = aioredis.from_url(url, decode_responses=True)
        request.app.state.execution_sse_aioredis = client
    return client
