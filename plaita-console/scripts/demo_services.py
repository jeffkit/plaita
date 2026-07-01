#!/usr/bin/env python
"""
演示脚本：注册模拟服务用于测试 Plaita Console

用法:
    python scripts/demo_services.py

这会注册几个模拟服务到 Redis，让你可以在 Plaita Console 中看到服务拓扑和状态。
"""
import sys
import time
import signal
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from redis import Redis
from plaita.server.registry import ServiceRegistry, ServiceInfo


def main():
    print("🚀 Plaita Console 演示服务启动中...\n")
    
    # 连接 Redis
    redis_client = Redis.from_url('redis://localhost:6379/0')
    
    try:
        redis_client.ping()
        print("✅ Redis 连接成功")
    except Exception as e:
        print(f"❌ Redis 连接失败: {e}")
        print("请确保 Redis 已启动，或使用 docker-compose.deps.yml 启动:")
        print("  cd plaita-console/docker && docker-compose -f docker-compose.deps.yml up -d")
        sys.exit(1)
    
    # 创建服务注册中心 (TTL 设置长一点，方便测试)
    registry = ServiceRegistry(redis_client=redis_client, ttl=3600)  # 1小时 TTL
    
    # 模拟服务列表
    services = [
        ServiceInfo(
            instance_id='flow-worker-001',
            service_type='flow_worker',
            host='localhost:8001',
            status='running',
            metadata={
                'queue_name': 'plaita:flow:queue',
                'version': '1.0.0'
            }
        ),
        ServiceInfo(
            instance_id='flow-worker-002',
            service_type='flow_worker',
            host='localhost:8002',
            status='running',
            metadata={
                'queue_name': 'plaita:flow:queue',
                'version': '1.0.0'
            }
        ),
        ServiceInfo(
            instance_id='delay-service-001',
            service_type='delay_service',
            host='localhost:9001',
            status='running',
            metadata={
                'check_interval': 5,
                'version': '1.0.0'
            }
        ),
        ServiceInfo(
            instance_id='redis-queue-001',
            service_type='redis_queue_service',
            host='localhost:9002',
            status='running',
            metadata={
                'queue_prefix': 'plaita:queue:',
                'version': '1.0.0'
            }
        ),
    ]
    
    # 注册所有服务
    print("\n📋 注册服务:")
    for info in services:
        registry.register(info)
        print(f"   ✅ {info.service_type}: {info.instance_id}")
    
    print(f"\n✨ 已注册 {len(services)} 个模拟服务")
    print("\n🌐 现在可以访问 Plaita Console 查看服务状态:")
    print("   - 仪表盘: http://localhost:5173/")
    print("   - 服务拓扑: http://localhost:5173/topology")
    print("\n💡 按 Ctrl+C 停止并注销所有服务\n")
    
    # 心跳循环
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        running = False
        print("\n\n🛑 正在停止...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    heartbeat_count = 0
    while running:
        try:
            for info in services:
                registry.heartbeat(info.service_type, info.instance_id)
            heartbeat_count += 1
            print(f"\r💓 心跳 #{heartbeat_count}", end="", flush=True)
            time.sleep(10)  # 每 10 秒心跳
        except Exception as e:
            print(f"\n❌ 心跳失败: {e}")
            break
    
    # 注销服务
    print("\n📋 注销服务:")
    for info in services:
        registry.unregister(info.service_type, info.instance_id)
        print(f"   ✅ {info.service_type}: {info.instance_id}")
    
    print("\n👋 演示服务已停止")


if __name__ == "__main__":
    main()

