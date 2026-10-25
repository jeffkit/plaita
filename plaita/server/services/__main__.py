"""
服务启动器
通过命令行参数启动指定的扩展服务

用法:
    python -m plaita.server.services delay_service
    python -m plaita.server.services redis_queue_service
    python -m plaita.server.services approval_service
    python -m plaita.server.services http_callback_service
    python -m plaita.server.services wake --sources delay,approval

``wake``（2026-10-25）：唤醒面统一守护进程——一个进程托管多个队列消费
source（默认 delay,approval），各 source 独立消费线程/注册身份/故障域；
旧服务名保留为单 source 等价运行（存量部署零破坏）。
"""
import argparse
import asyncio
import logging
import os
import signal
import sys

from ...logger import logger
from ...event.redis import RedisEventBus
from ...redis_client import make_redis_client
# plaita#158：连接串只允许以掩码形态（口令 ``***``）进日志。复用 #88 的
# ``factory._mask_url``（**不**另起一套掩码——#88 的根因正是同一个 URL 有两套渲染），
# 外面那层 ``_mask_url_for_log`` 保证掩码失败也不炸启动/停机路径。
from ...core.url_redaction import _mask_url_for_log
from .delay_service import DelayService
from .schedule_service import ScheduleService
from .redis_queue_service import RedisQueueService
from .approval_service import ApprovalService
from .http_callback_service import HttpCallbackService
from .wake import wake_source_classes
from ...core.url_redaction import _scrub_url_text


# 服务类映射
SERVICE_CLASSES = {
    'delay_service': DelayService,
    'schedule_service': ScheduleService,
    'redis_queue_service': RedisQueueService,
    'approval_service': ApprovalService,
    'http_callback_service': HttpCallbackService,
}


def main():
    # plaita#46：库内 logger（plaita/logger.py）只挂 NullHandler、输出交由应用
    # 配置——本启动器此前漏配，经它起的 delay/schedule/approval/callback/queue
    # 五个扩展服务全域静默（运维实证：systemd 日志文件 0 字节，2026-10-07）。
    # 与 flow_worker.main() 同款修法（2026-09 分布式评审 P2-6 的漏修面）：
    # level 取 PLAITA_LOG_LEVEL 缺省 INFO，输出仍交 systemd/容器侧收集。
    from plaita.runtime_config import (
        ConfigError,
        load_runtime_config,
        log_config_effective,
        preflight_security_and_limits,
        resolve_instance_id,
        resolve_redis_url,
    )

    preflight_warnings = []
    try:
        preflight_warnings = preflight_security_and_limits()
        runtime_cfg = load_runtime_config("services", strict=True)
    except ConfigError as exc:
        print(f"配置预检失败，服务拒绝启动：{exc}", file=sys.stderr)
        raise SystemExit(2)
    logging.basicConfig(
        level=os.environ.get("PLAITA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log_config_effective(logger, runtime_cfg)
    for warning in preflight_warnings:
        logger.warning("配置预检：%s", warning)
    parser = argparse.ArgumentParser(description='Plaita 扩展服务启动器')
    parser.add_argument(
        'service_type',
        choices=list(SERVICE_CLASSES.keys()) + ['wake'],
        help="要启动的服务类型（wake = 唤醒面统一守护进程）"
    )
    parser.add_argument(
        '--sources',
        default='delay,approval',
        help="wake 模式的唤醒 source 列表（逗号分隔；可选 delay/approval）"
    )
    parser.add_argument(
        '--redis-url',
        default=resolve_redis_url("services"),
        help='Redis 连接 URL（PLAITA_REDIS_URL > REDIS_URL）'
    )

    args = parser.parse_args()
    
    # 连接 Redis
    redis_url = args.redis_url
    # plaita#158：口令不得进 INFO 日志（落盘 + 日志聚合 + docker logs）。
    logger.info("连接 Redis: %s", _mask_url_for_log(redis_url))
    
    redis_client = make_redis_client(redis_url)
    
    try:
        redis_client.ping()
        logger.info("Redis 连接成功")
    except Exception as e:
        logger.error("Redis 连接失败: %s", _scrub_url_text(e))
        sys.exit(1)
    
    # 创建事件总线
    event_bus = RedisEventBus(redis_url=redis_url)
    
    # 获取实例 ID（B2R31-02：注入值与注册键同源；空串由预检拒绝）
    instance_id = resolve_instance_id() or f'{args.service_type}-{os.getpid()}'
    
    # 创建服务配置
    service_config = {
        'instance_id': instance_id,
    }

    # 根据服务类型创建服务实例（不同服务有不同的初始化参数）
    services = []
    if args.service_type == 'wake':
        # 唤醒面统一守护进程：一个进程托管多个队列消费 source——各 source
        # 独立消费线程/注册身份（registry 按各自 service_type 注册）/故障域；
        # 任一 source 构造失败整体退出（fail-fast，不带病托管半个唤醒面）。
        try:
            for cls in wake_source_classes([s.strip() for s in args.sources.split(',') if s.strip()]):
                services.append(cls(
                    event_bus=event_bus,
                    service_config=service_config,
                    redis_client=redis_client,
                ))
        except ValueError as exc:
            logger.error("wake source 解析失败: %s", exc)
            sys.exit(2)
    else:
        service_class = SERVICE_CLASSES[args.service_type]
        if args.service_type in ('delay_service', 'schedule_service'):
            # 两者均继承自 BaseExtendedService，支持 redis_client
            services.append(service_class(
                event_bus=event_bus,
                redis_client=redis_client,
                service_config=service_config
            ))
        elif args.service_type == 'redis_queue_service':
            # RedisQueueService 只接受 event_bus 和 retry_config
            services.append(service_class(event_bus=event_bus))
        elif args.service_type in ('http_callback_service', 'approval_service'):
            # 两者构造器已对齐基类 (event_bus, service_config, redis_client)
            # （Track C 统一过）——必须接 redis_client：审批/回调记录跨实例共享
            # 与 resume 事件直发频道都依赖它，缺了整体回退进程内存（Track P2）。
            services.append(service_class(
                event_bus=event_bus,
                service_config=service_config,
                redis_client=redis_client,
            ))
        else:
            # 默认尝试通用初始化
            services.append(service_class(event_bus=event_bus))

    # 信号处理
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        logger.info("收到信号 %s，正在停止服务...", sig)
        running = False
        for svc in services:
            svc.stop_service()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 启动服务
    logger.info("启动服务: %s (实例ID: %s)", args.service_type, instance_id)

    if not all(svc.start_service() for svc in services):
        logger.error("服务启动失败")
        for svc in services:
            svc.stop_service()
        sys.exit(1)

    logger.info(
        "服务 %s 已启动（sources: %s），按 Ctrl+C 停止",
        args.service_type,
        ",".join(svc.get_service_type() for svc in services),
    )

    # 主循环
    import time
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for svc in services:
            svc.stop_service()
        logger.info("服务已停止")


if __name__ == "__main__":
    main()

