#!/usr/bin/env python3
"""
事件驱动流程自动化测试脚本（调试版本）

该脚本不清理最终数据，以便检查中间状态
"""

import asyncio
import json
import time
import uuid
import subprocess
import signal
import sys
import logging
from pathlib import Path
from typing import Optional, Dict, Any

import redis
from redis.exceptions import ConnectionError as RedisConnectionError

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def prepare_redis_environment(redis_client):
    """准备Redis环境"""
    logger.info("准备Redis环境...")
    
    # 测试Redis连接
    try:
        redis_client.ping()
        logger.info("Redis连接正常")
    except RedisConnectionError:
        logger.error("无法连接到Redis")
        return False
    
    # 清理旧数据（只清理开始时）
    logger.info("清理Redis旧数据...")
    keys_to_delete = []
    keys_to_delete.extend(redis_client.keys("plaita:flow:queue"))
    keys_to_delete.extend(redis_client.keys("plaita:execution:*"))
    keys_to_delete.extend(redis_client.keys("plaita:subscription:*"))
    
    if keys_to_delete:
        redis_client.delete(*keys_to_delete)
    logger.info("Redis数据清理完成")
    
    return True


def setup_flow_definition(redis_client):
    """设置流程定义"""
    flow_definition = {
        "flow_id": "event_flow_demo",
        "name": "事件流程演示",
        "version": "1.0.0",
        "desc": "使用真正的EventNode节点演示事件驱动流程",
        "author": "Plaita开发团队",
        "runtime": "python",
        "timeout": "PT60S",
        "input_type": {
            "type": "object",
            "properties": {
                "event_type": {
                    "type": "string",
                    "description": "事件类型",
                    "default": "demo.user.action"
                },
                "message": {
                    "type": "string",
                    "description": "要传递的消息",
                    "default": "Hello Event World"
                }
            }
        },
        "output_type": {
            "type": "string",
            "description": "返回的消息"
        },
        "nodes": [
            {
                "id": "start",
                "name": "开始",
                "type": "start",
                "next": "wait_for_event"
            },
            {
                "id": "wait_for_event",
                "name": "等待事件",
                "type": "event",
                "event_type": "$INPUT.event_type",
                "event_filter": {"source": "test"},
                "next": "end"
            },
            {
                "id": "end",
                "name": "结束",
                "type": "end",
                "result_type": "success",
                "output": "$INPUT.message"
            }
        ],
        "metadata": {
            "version": "1.0.0",
            "description": "演示延时和事件节点的流程",
            "categories": ["demo", "event", "delay"]
        }
    }
    
    # 保存流程定义
    key = f"plaita:flow:event_flow_demo:1.0.0"
    redis_client.set(key, json.dumps(flow_definition))
    
    # 维护流程ID列表
    redis_client.sadd("plaita:flow_list", "event_flow_demo")
    
    # 维护版本列表
    redis_client.sadd("plaita:flow_versions:event_flow_demo", "1.0.0")
    
    logger.info("流程定义已加载到Redis")
    return True


def main():
    """主函数"""
    logger.info("开始事件驱动流程自动化测试（调试版本）")
    
    # 连接Redis
    redis_client = redis.from_url("redis://localhost:6379/0")
    
    # 准备环境
    if not prepare_redis_environment(redis_client):
        return False
    
    # 设置流程定义
    setup_flow_definition(redis_client)
    
    logger.info("启动Flow Worker...")
    flow_worker_proc = subprocess.Popen([
        'python', '-m', 'plaita.server.flow_worker',
        '--redis-url', 'redis://localhost:6379/0',
        '--queue-name', 'plaita:flow:queue',
        '--execution-storage-type', 'redis',
        '--flow-storage-type', 'redis',
        '--event-bus-type', 'redis',
        '--use-event-bus'
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    time.sleep(3)
    logger.info("Flow Worker启动成功")
    
    logger.info("启动Event Filter...")
    event_filter_proc = subprocess.Popen([
        'python', '-m', 'plaita.server.event_filter',
        '--redis-url', 'redis://localhost:6379/0',
        '--queue-name', 'plaita:flow:queue',
        '--execution-storage-type', 'redis',
        '--subscription-storage-type', 'redis',
        '--event-bus-type', 'redis',
        '--event-types', 'demo.user.action'
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    time.sleep(3)
    logger.info("Event Filter启动成功")
    
    # 发送流程启动消息
    logger.info("发送流程启动消息...")
    start_message = {
        "type": "start",
        "flow_id": "event_flow_demo",
        "params": {
            "event_type": "demo.user.action",
            "message": "Hello Event World"
        },
        "version": "1.0.0"
    }
    
    redis_client.rpush("plaita:flow:queue", json.dumps(start_message))
    logger.info("流程启动消息已发送")
    
    # 等待流程启动
    time.sleep(5)
    
    # 检查执行状态
    execution_keys = redis_client.keys("plaita:execution:*")
    if execution_keys:
        execution_key = execution_keys[0]
        if isinstance(execution_key, bytes):
            execution_key = execution_key.decode('utf-8')
        execution_id = execution_key.split(":")[-1]
        logger.info(f"流程已启动并暂停，执行ID: {execution_id}")
        
        # 发送事件
        logger.info("发送匹配事件...")
        
        # 创建符合Event模型的事件对象
        event_obj = {
            "event_id": str(uuid.uuid4()),
            "event_type": "demo.user.action",
            "data": {
                "message": "Event triggered"
            },
            "timestamp": time.time(),
            "source": "test",
            "correlation_id": execution_id
        }
        
        # 通过Event Filter的事件总线发送事件
        redis_client.publish("plaita:events:demo.user.action", json.dumps(event_obj))
        logger.info("事件已发送到频道: plaita:events:demo.user.action")
        
        # 等待流程完成
        logger.info("等待流程完成...")
        
        timeout = 30
        start_time = time.time()
        completed = False
        
        while time.time() - start_time < timeout:
            execution_data = redis_client.get(f"plaita:execution:{execution_id}")
            if execution_data:
                state = json.loads(execution_data)
                if state.get("status") == "completed":
                    logger.info("流程执行完成！")
                    logger.info(f"最终状态: {state}")
                    completed = True
                    break
            time.sleep(1)
        
        if not completed:
            logger.error("等待流程完成超时")
            logger.info(f"当前执行状态:")
            execution_data = redis_client.get(f"plaita:execution:{execution_id}")
            if execution_data:
                state = json.loads(execution_data)
                logger.info(f"  状态: {state.get('status')}")
                logger.info(f"  上下文: {state.get('context')}")
            
            # 检查订阅情况
            subscription_keys = redis_client.keys("plaita:subscription:*")
            logger.info(f"订阅数量: {len(subscription_keys)}")
            for key in subscription_keys:
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                if key.endswith(':data:') or not key.startswith('plaita:subscription:data:'):
                    continue  # 跳过索引键，只处理数据键
                sub_data = redis_client.get(key)
                if sub_data:
                    logger.info(f"订阅: {json.loads(sub_data)}")
    else:
        logger.error("未找到有效的执行状态")
    
    # 清理进程
    logger.info("清理资源...")
    flow_worker_proc.terminate()
    event_filter_proc.terminate()
    
    try:
        flow_worker_proc.wait(timeout=3)
        logger.info("Flow Worker已停止")
    except subprocess.TimeoutExpired:
        flow_worker_proc.kill()
        logger.info("Flow Worker被强制终止")
    
    try:
        event_filter_proc.wait(timeout=3)
        logger.info("Event Filter已停止")
    except subprocess.TimeoutExpired:
        event_filter_proc.kill()
        logger.info("Event Filter被强制终止")
    
    logger.info("测试完成，数据保留以供检查")
    return True


if __name__ == "__main__":
    main() 