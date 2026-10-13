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

#113（2026-10 安全收口）：``HttpCallbackNode`` 生成的
``service_config["auth_config"]``（token / basic / signature）历史上只落库、
消费侧零读取——认领回调记录的唯一判据是 URL 路径。现由
``handle_callback_request`` 在**原子认领之前**消费该配置，凭据用
``hmac.compare_digest`` 常数时间比较；校验失败不认领（注册记录保持存在，
挂起执行不会因一次未授权请求永久失醒）。``auth_config`` 缺失/为空视为该记录
未声明认证（兼容历史注册记录）；声明了认证但类型未知则拒绝（fail closed）。

Q5（#113 收口，2026-10）：认证已配置时，未通过认证的调用方**不得**区分
「路径存在但凭据不对」与「路径根本不存在」——三处失败返回（未知路径、
凭据校验失败、认领竞态败方）同状态同措辞（``CALLBACK_FAILURE_MESSAGE`` =
「回调路径未注册」），否则认证失败响应本身就是一个 path-existence oracle。
凭据细节只写日志；鉴权通过后才回成功响应。

**边界（无法关闭，且非认证 oracle）**：注册记录未声明 ``auth_config``
（``require_auth=False`` 或历史/手写 ``task_config``）时，认证不生效，任何
调用方均可正常触发——该路径的响应与「未知路径」的 error 不同。这是
**部署事实**（该回调按契约本就不需要认证），不是「凭据是否正确」的推断
通道；本服务不隐藏「哪些路径被注册为免认证」。
"""
import base64
import binascii
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional, Tuple

from .base_service import BaseExtendedService
from ...logger import logger

# 回调注册 TTL：7 天自清理（与仓内事件/订阅键 TTL 惯例一致）。
CALLBACK_RECORD_TTL_SECONDS = 7 * 86400

# signature 认证的默认时间窗（秒）：请求时间戳与本地时钟偏差超过该值即拒绝
# （防重放）。当前生产者只产出 HMAC-SHA256，窗口没有写在 auth_config 里，
# 因此以模块常量给定。
CALLBACK_SIGNATURE_TOLERANCE_SECONDS = 300

# Q5（#113 收口）：认证已配置时，未通过认证的调用方必须得到**同一条**响应，
# 不论「记录不存在」（未知路径 / 认领竞态中的败方）还是「凭据错误」——两条不同
# 的 message 合起来就是一个 path-existence oracle（未认证者可据此枚举已注册
# 回调路径）。措辞沿用既有的「回调路径未注册」：它对未认证视角是唯一可说的
# 事实（该记录此刻不可认领 / 本调用方无权认领）；凭据失败细节只进日志
# （logger.warning），不进响应。三处失败返回共用该常量，结构上无法再分叉。
CALLBACK_FAILURE_MESSAGE = "回调路径未注册"

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

    # ------------------------------------------------------------------
    # 注册记录读取 / 认领
    # ------------------------------------------------------------------
    def _read_callback_record(self, path: str) -> Optional[Dict[str, Any]]:
        """只读取出注册记录（**不删除**）——供认领前的凭据校验使用。"""
        if self._use_redis:
            raw = self._redis_client.get(self._record_key(path))
            return json.loads(raw) if raw else None
        return self.registered_callbacks.get(path)

    def _claim_callback_record(self, path: str) -> Optional[Dict[str, Any]]:
        """原子认领注册记录（Redis: Lua GET+DEL 一体；内存: pop）。

        未注册或已被其他实例抢走时返回 None。认领**必须**发生在凭据校验
        通过之后：GET+DEL 一体意味着认领即删除，先认领会让一次未授权请求
        删掉注册记录，把挂起执行变成永久失醒。
        """
        if self._use_redis:
            raw = self._redis_client.eval(
                _CLAIM_CALLBACK_LUA, 1, self._record_key(path)
            )
            return json.loads(raw) if raw else None
        return self.registered_callbacks.pop(path, None)

    # ------------------------------------------------------------------
    # 认证校验（消费 auth_config —— 历史缺陷 #113：生产后零读取）
    # ------------------------------------------------------------------
    @staticmethod
    def _header(headers: Optional[Dict[str, str]], name: str) -> Optional[str]:
        """大小写不敏感地取一个请求头（HTTP 头名本身大小写不敏感）。"""
        if not headers:
            return None
        wanted = name.lower()
        for key, value in headers.items():
            if isinstance(key, str) and key.lower() == wanted:
                if value is None:
                    return None
                return value if isinstance(value, str) else str(value)
        return None

    @staticmethod
    def _constant_time_eq(provided: Optional[str], expected: Optional[str]) -> bool:
        """常数时间比较（``hmac.compare_digest``，str → utf-8 bytes）。

        任一为 None（凭据缺失 / 配置缺字段）即不匹配。
        """
        if provided is None or expected is None:
            return False
        return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))

    @classmethod
    def _bearer_token(cls, headers: Optional[Dict[str, str]]) -> Optional[str]:
        """从 ``Authorization: Bearer <token>`` 取出 token（scheme 大小写不敏感）。"""
        raw = cls._header(headers, "Authorization")
        if not raw:
            return None
        parts = raw.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        return parts[1].strip()

    @classmethod
    def _basic_credentials(
        cls, headers: Optional[Dict[str, str]]
    ) -> Optional[Tuple[str, str]]:
        """从 ``Authorization: Basic <base64>`` 解出 (username, password)。"""
        raw = cls._header(headers, "Authorization")
        if not raw:
            return None
        parts = raw.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "basic":
            return None
        try:
            decoded = base64.b64decode(parts[1].strip(), validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return None
        if ":" not in decoded:
            return None
        username, _, password = decoded.partition(":")
        return username, password

    @staticmethod
    def _signature_payload(timestamp: int, request_data: Dict[str, Any]) -> bytes:
        """签名原文：``<unix 秒>.<request_data 的规范 JSON>``。

        规范 JSON = ``sort_keys=True`` + 紧凑分隔符（``,`` / ``:``）+
        ``ensure_ascii=False``——HTTP 层把 body 解析成 dict 后仍可复算出同一原文。
        """
        body = json.dumps(
            request_data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return f"{timestamp}.{body}".encode("utf-8")

    def _verify_signature(
        self,
        auth_config: Dict[str, Any],
        request_data: Dict[str, Any],
        headers: Optional[Dict[str, str]],
    ) -> Optional[str]:
        """校验 HMAC-SHA256 签名 + 时间窗；通过返回 None，拒绝返回错误消息。"""
        secret = auth_config.get("secret")
        if not secret:
            return "认证失败：签名认证缺少 secret"
        algorithm = str(auth_config.get("algorithm") or "").upper()
        if algorithm != "HMAC-SHA256":
            return f"认证失败：不支持的签名算法 {auth_config.get('algorithm')!r}"
        timestamp_raw = self._header(headers, "X-Plaita-Timestamp")
        signature = self._header(headers, "X-Plaita-Signature")
        if not timestamp_raw or not signature:
            return "认证失败：缺少 X-Plaita-Timestamp / X-Plaita-Signature"
        try:
            timestamp = int(timestamp_raw)
        except (TypeError, ValueError):
            return "认证失败：时间戳不是整数"
        if abs(int(time.time()) - timestamp) > CALLBACK_SIGNATURE_TOLERANCE_SECONDS:
            return "认证失败：签名时间戳超出允许窗口"
        expected = hmac.new(
            secret.encode("utf-8"),
            self._signature_payload(timestamp, request_data),
            hashlib.sha256,
        ).hexdigest()
        provided = signature.strip()
        if provided.lower().startswith("sha256="):
            provided = provided[len("sha256="):]
        if not self._constant_time_eq(provided.lower(), expected):
            return "认证失败：签名不匹配"
        return None

    def _verify_callback_auth(
        self,
        auth_config: Optional[Dict[str, Any]],
        request_data: Dict[str, Any],
        headers: Optional[Dict[str, str]],
    ) -> Optional[str]:
        """按注册时落库的 ``task_config["auth_config"]`` 校验回调凭据。

        通过返回 None，拒绝返回错误消息（调用方转成 error 响应）。

        语义（fail closed）：
        * ``auth_config`` 缺失/为空，或既无 ``enabled`` 又无 ``type`` → 该记录
          没有声明认证，放行（兼容历史注册记录与手写 task_config）；
        * 其余一律按 ``type`` 校验，未知/缺失 ``type`` 直接拒绝。
        """
        if not isinstance(auth_config, dict) or not auth_config:
            return None
        if not auth_config.get("enabled") and not auth_config.get("type"):
            return None

        auth_type = auth_config.get("type")
        if auth_type == "token":
            if not self._constant_time_eq(
                self._bearer_token(headers), auth_config.get("token")
            ):
                return "认证失败：token 不匹配"
            return None
        if auth_type == "basic":
            provided = self._basic_credentials(headers)
            username, password = provided if provided else (None, None)
            if not (
                self._constant_time_eq(username, auth_config.get("username"))
                and self._constant_time_eq(password, auth_config.get("password"))
            ):
                return "认证失败：basic 凭据不匹配"
            return None
        if auth_type == "signature":
            return self._verify_signature(auth_config, request_data, headers)
        return f"认证失败：不支持的认证类型 {auth_type!r}"

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

    async def handle_callback_request(
        self,
        path: str,
        request_data: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        处理HTTP回调请求

        认证（#113）：注册时落库的 ``task_config["auth_config"]`` 在这里被消费。
        校验**先于**原子认领——认领是 GET+DEL 一体，先认领会用一次未授权请求
        删掉注册记录，把挂起执行变成永久失醒。

        Args:
            path: 回调路径
            request_data: 请求数据
            headers: 请求头（大小写不敏感）。认证开启时按 ``auth_type`` 读取
                ``Authorization: Bearer <token>`` / ``Authorization: Basic <b64>``
                / ``X-Plaita-Timestamp`` + ``X-Plaita-Signature``。

        Returns:
            Dict[str, Any]: 响应数据
        """
        try:
            # 1) 只读取记录（不删）→ 2) 校验凭据 → 3) 原子认领
            registered = self._read_callback_record(path)
            if not isinstance(registered, dict):
                # Q5：未认证视角下「路径不存在」与「凭据错误」必须同响应，否则
                # 这里（以及下面的认证失败分支）合起来就是 path-existence oracle。
                return {"status": "error", "message": CALLBACK_FAILURE_MESSAGE}

            auth_error = self._verify_callback_auth(
                (registered.get("task_config") or {}).get("auth_config"),
                request_data,
                headers,
            )
            if auth_error:
                # 细节只进日志；响应与「未注册」分支共用同一常量（Q5）。
                logger.warning("HTTP回调认证失败: %s (%s)", path, auth_error)
                return {"status": "error", "message": CALLBACK_FAILURE_MESSAGE}

            callback_info = self._claim_callback_record(path)
            if not isinstance(callback_info, dict):
                # 只读校验与原子认领之间被其他实例抢走 → 同一统一失败响应（Q5）
                return {"status": "error", "message": CALLBACK_FAILURE_MESSAGE}
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
