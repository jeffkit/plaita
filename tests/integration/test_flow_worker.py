#!/usr/bin/env python3
"""
Flow Worker测试脚本

用法:
1. 首先启动FlowWorker: python -m plaita.server.flow_worker
2. 然后运行此脚本: python test_flow_worker.py
"""

import json
import uuid
import time
import redis
import argparse
import subprocess
from datetime import datetime

# 默认配置
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_QUEUE_NAME = "plaita:flow:queue"
TEST_FLOW_ID = "test-flow-1"

def create_test_flow():
    """创建测试流程定义"""
    return {
        "flow_id": TEST_FLOW_ID,
        "name": "测试流程",
        "version": "latest",
        "nodes": [
            {
                "id": "start",
                "type": "start",
                "next": "echo1"
            },
            {
                "id": "echo1",
                "type": "echo",
                "name": "回显1",
                "config": {
                    "message": "执行任务1"
                },
                "next": "echo2"
            },
            {
                "id": "echo2",
                "type": "echo",
                "name": "回显2",
                "config": {
                    "message": "执行任务2"
                },
                "next": "end"
            },
            {
                "id": "end",
                "type": "end"
            }
        ]
    }

def save_flow_to_redis(redis_client, flow):
    """保存流程定义到Redis"""
    flow_id = flow['flow_id']
    version = flow.get('version', 'latest')
    
    # 使用与RedisFlowStorage相同的命名空间
    namespace = "plaita"
    
    # 保存流程定义
    key = f"{namespace}:flow:{flow_id}:{version}"
    redis_client.set(key, json.dumps(flow))
    
    # 维护流程ID列表
    flow_list_key = f"{namespace}:flow_list"
    redis_client.sadd(flow_list_key, flow_id)
    
    # 维护每个流程的版本列表
    flow_versions_key = f"{namespace}:flow_versions:{flow_id}"
    redis_client.sadd(flow_versions_key, version)
    
    print(f"流程定义已保存到Redis: {key}")

def send_start_flow_task(redis_client, queue_name, flow_id, params=None):
    """发送启动流程任务到队列"""
    if params is None:
        params = {}
    
    # 创建启动流程任务
    task = {
        "type": "start",
        "flow_id": flow_id,
        "params": params,
        "version": "latest"
    }
    
    # 发送任务到队列
    redis_client.rpush(queue_name, json.dumps(task))
    print(f"已发送启动流程任务: {task}")

def send_resume_flow_task(redis_client, queue_name, flow_id, execution_id, resume_type="continue", data=None):
    """发送恢复流程任务到队列"""
    if data is None:
        data = {}
    
    # 创建恢复流程任务
    task = {
        "type": "resume",
        "flow_id": flow_id,
        "execution_id": execution_id,
        "resume_type": resume_type,
        "data": data
    }
    
    # 发送任务到队列
    redis_client.rpush(queue_name, json.dumps(task))
    print(f"已发送恢复流程任务: {task}")

def list_executions(redis_client):
    """列出所有执行状态"""
    namespace = "plaita"
    pattern = f"{namespace}:execution:*"
    keys = redis_client.keys(pattern)
    
    if not keys:
        print("没有找到执行状态")
        return []
    
    executions = []
    for key in keys:
        try:
            data = redis_client.get(key)
            if data:
                execution = json.loads(data)
                executions.append(execution)
                print(f"执行状态: {key}")
                print(f"  ID: {execution.get('execution_id')}")
                print(f"  流程: {execution.get('flow_id')}")
                print(f"  状态: {execution.get('status')}")
                print(f"  开始时间: {execution.get('start_time')}")
                print(f"  结束时间: {execution.get('end_time')}")
                if execution.get('error'):
                    print(f"  错误: {execution.get('error')}")
                print()
        except Exception as e:
            print(f"获取执行状态出错: {e}")
    
    return executions

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Flow Worker测试脚本")
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL, help="Redis连接URL")
    parser.add_argument("--queue-name", default=DEFAULT_QUEUE_NAME, help="Redis队列名称")
    parser.add_argument("--action", choices=["start", "resume", "list"], default="start", help="要执行的操作")
    parser.add_argument("--execution-id", help="执行ID，用于恢复流程")
    parser.add_argument("--resume-type", choices=["continue", "cancel", "timeout", "event"], default="continue", help="恢复类型")
    return parser.parse_args()

def test_flow_worker():
    # 使用fakeredis模拟Redis
    from fakeredis import FakeRedis
    client = FakeRedis()
    
    # 检查队列长度
    queue_length = client.llen("plaita:flow:queue")
    print(f"初始队列长度: {queue_length}")
    
    # 启动Flow Worker进程
    print("启动Flow Worker...")
    proc = subprocess.Popen([
        'python', '-m', 'plaita.server.flow_worker',
        '--redis-url', 'redis://localhost:6379/0',
        '--queue-name', 'plaita:flow:queue',
        '--execution-storage-type', 'redis',
        '--flow-storage-type', 'redis', 
        '--event-bus-type', 'redis',
        '--use-event-bus'
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # 等待15秒以给更多时间处理
    print("等待15秒...")
    time.sleep(2)
    
    # 检查队列是否被消费
    queue_length_after = client.llen("plaita:flow:queue")
    print(f"15秒后队列长度: {queue_length_after}")
    
    # 检查执行状态
    execution_keys = client.keys("plaita:execution:*")
    print(f"执行状态键数量: {len(execution_keys)}")
    
    if execution_keys:
        for key in execution_keys:
            execution_data = client.get(key)
            if execution_data:
                execution_state = json.loads(execution_data)
                print(f"执行状态: {execution_state.get('status')}")
                print(f"执行ID: {execution_state.get('execution_id')}")
                if execution_state.get('error'):
                    print(f"错误: {execution_state.get('error')}")
    
    # 停止进程
    proc.terminate()
    try:
        stdout, stderr = proc.communicate(timeout=3)
        print("\nFlow Worker 输出:")
        print("STDOUT:", stdout[:1000] if stdout else "无")
        print("STDERR:", stderr[:1500] if stderr else "无")
    except subprocess.TimeoutExpired:
        proc.kill()
        print("Flow Worker进程被强制终止")

def main():
    """主函数"""
    args = parse_args()
    
    # 连接Redis
    redis_client = redis.from_url(args.redis_url)
    print(f"已连接到Redis: {args.redis_url}")
    
    if args.action == "start":
        # 创建并保存流程定义
        flow = create_test_flow()
        save_flow_to_redis(redis_client, flow)
        
        # 发送启动流程任务
        params = {
            "test_param": "test_value",
            "timestamp": datetime.now().isoformat()
        }
        send_start_flow_task(redis_client, args.queue_name, TEST_FLOW_ID, params)
        
        # 等待执行完成
        print("等待3秒钟...")
        time.sleep(2)
        
        # 列出执行状态
        executions = list_executions(redis_client)
    
    elif args.action == "resume":
        if not args.execution_id:
            print("错误: 恢复流程需要提供execution-id参数")
            return
        
        # 发送恢复流程任务
        send_resume_flow_task(
            redis_client, 
            args.queue_name, 
            TEST_FLOW_ID, 
            args.execution_id, 
            args.resume_type
        )
        
        # 等待执行完成
        print("等待3秒钟...")
        time.sleep(2)
        
        # 列出执行状态
        list_executions(redis_client)
    
    elif args.action == "list":
        # 列出执行状态
        list_executions(redis_client)

if __name__ == "__main__":
    test_flow_worker() 