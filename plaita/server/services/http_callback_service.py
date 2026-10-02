"""
HTTP回调服务实现
负责处理HTTP回调注册和监听

Track C 任务2（2026-10 分布式可靠性修复）：回调注册历史上存进程内 dict
（``registered_callbacks``）——服务重启全丢、多实例时回调请求打到另一实例
查无此路径。现迁 Redis：每回调路径一条 JSON 记录键（带 TTL 自清理，与仓内
7 天键惯例一致）。回调处理是**原子认领**（Lua GET+DEL 一体）：多实例并发
到达同一回调路径时恰一实例触发事件，另一实例得到「未注册」——修复前两步
写在多实例下会双触发 resume。无 redis_client（单测/内存模式）回退进程内
dict，get_registered_callbacks 返回形状不变。
"""
import json
import time
from typing import Any, Dict

from .base_service import BaseExtendedService
from ...logger import logger

# 回调注册 TTL：7 天自清理（与仓内事件/订阅键 TTL 惯例一致）。
CALLBACK_RECORD_TTL_SECONDS = 7 * 86400

# 原子认领：GET+DEL 一体。两个实例同时到达时恰一实例拿到注册信息并触发，
# 另一实例得到 nil →「回调路径未注册」。GET 与 DEL 拆成两步（先 GET 处理、
# 处理完 DEL）在多实例下会让两个实例都触发事件 → 双 resume。
_CLAIM_CALLBACK_LUA = """
local v = redis.call('get', KEYS[1])
if not v then return false end
redis.call('del', KEYS[1])
return v
"""


class HttpCallbackService(BaseExtendedService):
    """
    HTTP回调服务
    负责注册HTTP回调路径并等待回调触发
    """

    def __init__(self, event_bus, service_config=None, redis_client=None):
        # 签名对齐基类（同 DelayService）：(event_bus, service_config, redis_client)
        # ——位置传参 HttpCallbackService(bus, {...}) 时第二位是 service_config。
        # redis_client 用于：resume 事件直发 plaita:events:{type} 频道 + 回调
        # 注册跨实例共享（无 redis 时回退进程内存，单测/内存模式不受影响）。
        super().__init__(
            event_bus=event_bus,
            service_config=service_config,
            redis_client=redis_client,
        )
        self.registered_callbacks = {}  # 存储注册的回调信息（无 redis 回退用）
        self._record_prefix = (service_config or {}).get(
            "http_callback_record_prefix", "plaita:http_callback:registered:"
        )

    @property
    def _use_redis(self) -> bool:
        """回调注册是否走 Redis（无 redis 客户端时回退进程内存）。"""
        return self._redis_client is not None and hasattr(self._redis_client, "get")

    def _record_key(self, path: str) -> str:
        return f"{self._record_prefix}{path}"

    def get_service_type(self) -> str:
        """获取服务类型"""
        return "http_callback"

    async def trigger_event(self, event_type: str, event_data: Dict[str, Any]):
        """触发事件：带 correlation_id（=execution_id），EventFilter 才能关联到挂起执行。

        基类 trigger_event 构造的 Event 不带 correlation_id，会被
        EventFilter.handle_event 直接丢弃——回调到达后挂起执行永远等不到
        resume。统一走基类 publish_resume_event（与 DelayService 的
        trigger_event override 同手法：有 redis 直发 plaita:events:{type}
        频道，无 redis 回退 event_bus.publish）。
        """
        await self.publish_resume_event(event_type, event_data)

    def start_service(self) -> bool:
        """启动HTTP回调服务"""
        try:
            self.is_running = True
            logger.info("HTTP回调服务已启动")
            return True
        except Exception as e:  # noqa: BLE001 — 启动失败只告警不扩散
            logger.error("启动HTTP回调服务失败: %s", e, exc_info=True)
            return False

    def stop_service(self) -> bool:
        """停止HTTP回调服务"""
        try:
            self.is_running = False
            # 只清进程内存回退态；Redis 注册是跨实例共享的持久态，停机不清
            # （带 TTL 自清理）。
            self.registered_callbacks.clear()
            logger.info("HTTP回调服务已停止")
            return True
        except Exception as e:  # noqa: BLE001 — 停止失败只告警不扩散
            logger.error("停止HTTP回调服务失败: %s", e, exc_info=True)
            return False

    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """处理HTTP回调注册任务"""
        try:
            callback_config = task_config.get("callback_config", {})
            node_id = task_config.get("node_id")

            # 注册回调路径
            callback_path = callback_config.get("path")
            if callback_path:
                callback_info = {
                    "task_config": task_config,
                    "registered_time": int(time.time() * 1000)
                }
                if self._use_redis:
                    self._redis_client.set(
                        self._record_key(callback_path),
                        json.dumps(callback_info, ensure_ascii=False),
                        ex=CALLBACK_RECORD_TTL_SECONDS,
                    )
                else:
                    self.registered_callbacks[callback_path] = callback_info

                logger.info("HTTP回调路径已注册: %s for node %s", callback_path, node_id)

                # 在实际实现中，这里会启动HTTP服务器或注册路由
                # 目前只是简单存储配置

                return True
            else:
                logger.error("回调路径不能为空")
                return False

        except Exception as e:  # noqa: BLE001 — 任务失败只告警返回 False
            logger.error("处理HTTP回调任务失败: %s", e, exc_info=True)
            return False

    async def handle_callback_request(self, path: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理HTTP回调请求

        Args:
            path: 回调路径
            request_data: 请求数据

        Returns:
            Dict[str, Any]: 响应数据
        """
        try:
            if self._use_redis:
                # 原子认领（Lua GET+DEL）：恰一实例处理该回调，防多实例双触发。
                # 认领先行于触发——trigger_event 自吞异常不会失败重投，无丢失窗。
                raw = self._redis_client.eval(
                    _CLAIM_CALLBACK_LUA, 1, self._record_key(path)
                )
                if not raw:
                    return {"status": "error", "message": "回调路径未注册"}
                callback_info = json.loads(raw)
            else:
                if path not in self.registered_callbacks:
                    return {"status": "error", "message": "回调路径未注册"}
                callback_info = self.registered_callbacks[path]
            task_config = callback_info["task_config"]

            # 构造事件数据
            event_data = {
                "node_id": task_config.get("node_id"),
                "execution_id": task_config.get("execution_id"),
                "flow_id": task_config.get("flow_id"),
                "tenant_id": task_config.get("tenant_id") or "default",
                "trigger_type": "http_callback",
                "callback_path": path,
                "request_data": request_data,
                "timestamp": int(time.time() * 1000),
                "success": True
            }

            # 触发事件
            await self.trigger_event(task_config.get("event_type"), event_data)

            # 移除已处理的回调（Redis 模式在认领时已原子删除）
            if not self._use_redis:
                del self.registered_callbacks[path]

            logger.info("HTTP回调已处理: %s", path)

            # 返回成功响应
            response_config = task_config.get("response_config", {})
            return response_config.get("success_response", {"status": "success"})

        except Exception as e:  # noqa: BLE001 — 失败转错误响应不扩散
            logger.error("处理HTTP回调请求失败: %s", e, exc_info=True)
            return {"status": "error", "message": str(e)}

    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """验证HTTP回调任务配置"""
        if not super().validate_task_config(task_config):
            return False

        callback_config = task_config.get("callback_config")
        if not callback_config:
            logger.error("缺少回调配置")
            return False

        if not callback_config.get("path"):
            logger.error("缺少回调路径")
            return False

        return True

    def get_registered_callbacks(self) -> Dict[str, Any]:
        """获取已注册的回调信息（对外形状与内存版一致：{path: {task_config, ...}}）"""
        if not self._use_redis:
            return self.registered_callbacks.copy()
        try:
            keys = list(self._redis_client.scan_iter(match=f"{self._record_prefix}*"))
        except Exception as e:  # noqa: BLE001 — 巡检失败返回空不扩散
            logger.error("扫描回调注册失败: %s", e, exc_info=True)
            return {}
        if not keys:
            return {}
        try:
            raws = self._redis_client.mget(keys)
        except Exception as e:  # noqa: BLE001 — 同上
            logger.error("批量读取回调注册失败: %s", e, exc_info=True)
            return {}
        callbacks: Dict[str, Any] = {}
        for key, raw in zip(keys, raws):
            if not raw:
                continue
            if isinstance(key, bytes):
                key = key.decode()
            path = key[len(self._record_prefix):]
            try:
                callbacks[path] = json.loads(raw)
            except Exception:  # noqa: BLE001 — 坏记录跳过
                logger.error("回调注册反序列化失败（跳过）: %r", raw[:200])
        return callbacks
