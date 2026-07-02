"""
服务启动器
通过命令行参数启动指定的扩展服务

用法:
    python -m plaita.server.services delay_service
    python -m plaita.server.services redis_queue_service
    python -m plaita.server.services approval_service
    python -m plaita.server.services http_callback_service
"""
import argparse
import asyncio
import os
import signal
import sys

from redis import Redis

from ...logger import logger
from ...event.redis import RedisEventBus
from .delay_service import DelayService
from .redis_queue_service import RedisQueueService
from .approval_service import ApprovalService
from .http_callback_service import HttpCallbackService


# 服务类映射
SERVICE_CLASSES = {
    'delay_service': DelayService,
    'redis_queue_service': RedisQueueService,
    'approval_service': ApprovalService,
    'http_callback_service': HttpCallbackService,
}


def main():
    parser = argparse.ArgumentParser(description='Plaita 扩展服务启动器')
    parser.add_argument(
        'service_type',
        choices=list(SERVICE_CLASSES.keys()),
        help='要启动的服务类型'
    )
    parser.add_argument(
        '--redis-url',
        default=os.environ.get('PLAITA_REDIS_URL', os.environ.get('REDIS_URL', 'redis://localhost:6379/0')),
        help='Redis 连接 URL'
    )
    
    args = parser.parse_args()
    
    # 连接 Redis
    redis_url = args.redis_url
    logger.info(f"连接 Redis: {redis_url}")
    
    redis_client = Redis.from_url(redis_url)
    
    try:
        redis_client.ping()
        logger.info("Redis 连接成功")
    except Exception as e:
        logger.error(f"Redis 连接失败: {e}")
        sys.exit(1)
    
    # 创建事件总线
    event_bus = RedisEventBus(redis_url=redis_url)
    
    # 获取实例 ID
    instance_id = os.environ.get('PLAITA_INSTANCE_ID', f'{args.service_type}-{os.getpid()}')
    
    # 创建服务配置
    service_config = {
        'instance_id': instance_id,
    }
    
    # 根据服务类型创建服务实例（不同服务有不同的初始化参数）
    service_class = SERVICE_CLASSES[args.service_type]
    
    if args.service_type == 'delay_service':
        # DelayService 继承自 BaseExtendedService，支持 redis_client
        service = service_class(
            event_bus=event_bus,
            redis_client=redis_client,
            service_config=service_config
        )
    elif args.service_type == 'redis_queue_service':
        # RedisQueueService 只接受 event_bus 和 retry_config
        service = service_class(event_bus=event_bus)
    elif args.service_type in ('http_callback_service', 'approval_service'):
        # 这些服务接受 event_bus 和 service_config
        service = service_class(event_bus=event_bus, service_config=service_config)
    else:
        # 默认尝试通用初始化
        service = service_class(event_bus=event_bus)
    
    # 信号处理
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        logger.info(f"收到信号 {sig}，正在停止服务...")
        running = False
        service.stop_service()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 启动服务
    logger.info(f"启动服务: {args.service_type} (实例ID: {instance_id})")
    
    if not service.start_service():
        logger.error("服务启动失败")
        sys.exit(1)
    
    logger.info(f"服务 {args.service_type} 已启动，按 Ctrl+C 停止")
    
    # 主循环
    import time
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        service.stop_service()
        logger.info("服务已停止")


if __name__ == "__main__":
    main()

