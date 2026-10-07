"""
审批服务实现
负责处理审批流程

Track C 任务2（2026-10 分布式可靠性修复）：审批记录历史上存进程内 dict
（``pending_approvals``）——服务重启全丢、多实例时决策提交到另一实例查无
此审批。现迁 Redis：每审批一条 JSON 记录键（带 TTL 自清理），决策提交经
每审批 SET NX 锁串行化（execution_lease / event_filter 的既有原子原语
风格），同一审批人跨实例重复提交被原子拦住。无 redis_client（单测/内存
模式）回退进程内 dict，对外方法返回形状不变。
"""
import json
import time
import uuid
from typing import Any, Dict, List

from .base_service import BaseExtendedService
from ...logger import logger

# 审批记录 TTL：7 天自清理（与仓内事件/订阅键 TTL 惯例一致）。
APPROVAL_RECORD_TTL_SECONDS = 7 * 86400
# 决策锁 TTL：只需覆盖单次决策提交的临界区；持锁实例崩溃时靠它自动释放。
APPROVAL_LOCK_TTL_SECONDS = 10
# 锁被他人持有时短暂重试的上限，超过则让调用方稍后重试。
APPROVAL_LOCK_WAIT_SECONDS = 2.0
APPROVAL_LOCK_POLL_SECONDS = 0.05

# compare-and-del 释放（execution_lease 同款）：只释放自己持有的锁，
# 避免误删他人锁（临界区超时被他人抢走后，原持有者的 finally 不该拆新锁）。
_RELEASE_IF_OWNED_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""


class ApprovalService(BaseExtendedService):
    """
    审批服务
    负责创建审批任务并处理审批决策
    """

    def __init__(self, event_bus, service_config=None, redis_client=None):
        # 签名对齐基类（同 DelayService）：(event_bus, service_config, redis_client)
        # ——位置传参 ApprovalService(bus, {...}) 时第二位是 service_config。
        # redis_client 用于：resume 事件直发 plaita:events:{type} 频道 + 审批
        # 记录跨实例共享（无 redis 时回退进程内存，单测/内存模式不受影响）。
        super().__init__(
            event_bus=event_bus,
            service_config=service_config,
            redis_client=redis_client,
        )
        self.pending_approvals = {}  # 存储待审批的任务（无 redis 回退用）
        self._record_prefix = (service_config or {}).get(
            "approval_record_prefix", "plaita:approval:pending:"
        )
        self._lock_prefix = (service_config or {}).get(
            "approval_lock_prefix", "plaita:approval:lock:"
        )

    @property
    def _use_redis(self) -> bool:
        """审批记录是否走 Redis（无 redis 客户端时回退进程内存）。"""
        return self._redis_client is not None and hasattr(self._redis_client, "get")

    def _record_key(self, approval_id: str) -> str:
        return f"{self._record_prefix}{approval_id}"

    def _lock_key(self, approval_id: str) -> str:
        return f"{self._lock_prefix}{approval_id}"

    def get_service_type(self) -> str:
        """获取服务类型"""
        return "approval"

    async def trigger_event(self, event_type: str, event_data: Dict[str, Any]):
        """触发事件：带 correlation_id（=execution_id），EventFilter 才能关联到挂起执行。

        基类 trigger_event 构造的 Event 不带 correlation_id，会被
        EventFilter.handle_event 直接丢弃——审批完成后挂起执行永远等不到
        resume。统一走基类 publish_resume_event（与 DelayService 的
        trigger_event override 同手法：有 redis 直发 plaita:events:{type}
        频道，无 redis 回退 event_bus.publish）。
        """
        await self.publish_resume_event(event_type, event_data)

    def start_service(self) -> bool:
        """启动审批服务"""
        try:
            self.is_running = True
            logger.info("审批服务已启动")
            return True
        except Exception as e:  # noqa: BLE001 — 启动失败只告警不扩散
            logger.error("启动审批服务失败: %s", e, exc_info=True)
            return False

    def stop_service(self) -> bool:
        """停止审批服务"""
        try:
            self.is_running = False
            # 只清进程内存回退态；Redis 记录是跨实例共享的持久态，停机不清
            # （带 TTL 自清理），否则重启/停机即全丢、兄弟实例也被拖垮。
            self.pending_approvals.clear()
            logger.info("审批服务已停止")
            return True
        except Exception as e:  # noqa: BLE001 — 停止失败只告警不扩散
            logger.error("停止审批服务失败: %s", e, exc_info=True)
            return False

    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """处理审批任务创建"""
        try:
            approval_id = task_config.get("approval_id")
            approval_config = task_config.get("approval_config", {})
            approver_config = task_config.get("approver_config", {})

            # 创建审批记录
            approval_record = {
                "approval_id": approval_id,
                "task_config": task_config,
                "created_time": int(time.time() * 1000),
                "status": "pending",
                "approvals": [],  # 存储审批记录
                "required_approvers": approver_config.get("approvers", []),
                "strategy": approver_config.get("strategy", "any")
            }

            if self._use_redis:
                self._redis_client.set(
                    self._record_key(approval_id),
                    json.dumps(approval_record, ensure_ascii=False),
                    ex=APPROVAL_RECORD_TTL_SECONDS,
                )
            else:
                self.pending_approvals[approval_id] = approval_record

            logger.info("审批任务已创建: %s, 审批人: %s", approval_id, approval_record['required_approvers'])

            # 在实际实现中，这里会发送通知给审批人
            # 目前只是简单存储审批记录

            return True

        except Exception as e:  # noqa: BLE001 — 任务失败只告警返回 False
            logger.error("处理审批任务失败: %s", e, exc_info=True)
            return False

    async def submit_approval_decision(self, approval_id: str, approver_id: str, decision: str, comments: str = "") -> Dict[str, Any]:
        """
        提交审批决策

        Args:
            approval_id: 审批ID
            approver_id: 审批人ID
            decision: 审批决策 (approve/reject)
            comments: 审批意见

        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            if self._use_redis:
                return await self._submit_decision_redis(
                    approval_id, approver_id, decision, comments
                )
            return await self._submit_decision_memory(
                approval_id, approver_id, decision, comments
            )
        except Exception as e:  # noqa: BLE001 — 失败转错误响应不扩散
            logger.error("提交审批决策失败: %s", e, exc_info=True)
            return {"status": "error", "message": str(e)}

    async def _submit_decision_memory(
        self, approval_id: str, approver_id: str, decision: str, comments: str
    ) -> Dict[str, Any]:
        """无 redis 回退路径：进程内读改写（历史实现原样保留）。"""
        if approval_id not in self.pending_approvals:
            return {"status": "error", "message": "审批任务不存在"}

        approval_record = self.pending_approvals[approval_id]
        task_config = approval_record["task_config"]

        # 检查审批人权限
        if approver_id not in approval_record["required_approvers"]:
            return {"status": "error", "message": "无审批权限"}

        # 检查是否已经审批过
        for existing_approval in approval_record["approvals"]:
            if existing_approval["approver_id"] == approver_id:
                return {"status": "error", "message": "已经审批过"}

        # 记录审批决策
        approval_decision = {
            "approver_id": approver_id,
            "decision": decision,
            "comments": comments,
            "timestamp": int(time.time() * 1000)
        }

        approval_record["approvals"].append(approval_decision)

        # 检查是否满足审批策略
        final_decision = self._check_approval_result(approval_record)

        if final_decision:
            # 审批完成，触发事件
            event_data = self._build_completed_event_data(
                task_config, approval_id, final_decision, approval_record["approvals"]
            )

            # 触发事件
            await self.trigger_event(task_config.get("event_type"), event_data)

            # 更新状态并从待审批列表移除
            approval_record["status"] = final_decision
            approval_record["completed_time"] = int(time.time() * 1000)
            del self.pending_approvals[approval_id]

            logger.info("审批完成: %s, 最终决策: %s", approval_id, final_decision)

            return {"status": "success", "final_decision": final_decision, "message": "审批完成"}
        else:
            logger.info("审批进行中: %s, 当前审批数: %s", approval_id, len(approval_record['approvals']))
            return {"status": "success", "message": "审批已记录，等待其他审批人"}

    async def _submit_decision_redis(
        self, approval_id: str, approver_id: str, decision: str, comments: str
    ) -> Dict[str, Any]:
        """Redis 路径：每审批 SET NX 锁串行化读改写，跨实例原子一致。

        锁的必要性：记录是「读-校验-追加-回写」多步，多实例并发提交同一审批
        会互相覆盖（丢决策/重复定案）。SET NX + compare-and-del 释放与
        execution_lease / event_filter 去重键同款原子原语；锁 TTL 是持锁
        实例崩溃时的自动释放兜底。
        """
        lock_key = self._lock_key(approval_id)
        holder = f"{id(self):x}:{uuid.uuid4().hex}"
        deadline = time.monotonic() + APPROVAL_LOCK_WAIT_SECONDS
        while not self._redis_client.set(
            lock_key, holder, nx=True, ex=APPROVAL_LOCK_TTL_SECONDS
        ):
            if time.monotonic() >= deadline:
                return {"status": "error", "message": "审批处理中，请稍后重试"}
            time.sleep(APPROVAL_LOCK_POLL_SECONDS)

        try:
            raw = self._redis_client.get(self._record_key(approval_id))
            if not raw:
                return {"status": "error", "message": "审批任务不存在"}
            approval_record = json.loads(raw)
            task_config = approval_record["task_config"]

            # 检查审批人权限
            if approver_id not in approval_record["required_approvers"]:
                return {"status": "error", "message": "无审批权限"}

            # 检查是否已经审批过（记录在 Redis 共享，跨实例拦重复）
            for existing_approval in approval_record["approvals"]:
                if existing_approval["approver_id"] == approver_id:
                    return {"status": "error", "message": "已经审批过"}

            # 记录审批决策
            approval_record["approvals"].append(
                {
                    "approver_id": approver_id,
                    "decision": decision,
                    "comments": comments,
                    "timestamp": int(time.time() * 1000),
                }
            )

            # 检查是否满足审批策略
            final_decision = self._check_approval_result(approval_record)

            if final_decision:
                # 审批完成，触发事件；publish_resume_event 失败上抛（resume
                # 最后一跳不吞）——此刻本地记录未回写、Redis 记录未删除，
                # 调用方可重试重放，不会留下半提交态。
                event_data = self._build_completed_event_data(
                    task_config, approval_id, final_decision,
                    approval_record["approvals"],
                )
                await self.trigger_event(task_config.get("event_type"), event_data)

                approval_record["status"] = final_decision
                approval_record["completed_time"] = int(time.time() * 1000)
                self._redis_client.delete(self._record_key(approval_id))

                logger.info("审批完成: %s, 最终决策: %s", approval_id, final_decision)
                return {
                    "status": "success",
                    "final_decision": final_decision,
                    "message": "审批完成",
                }

            self._redis_client.set(
                self._record_key(approval_id),
                json.dumps(approval_record, ensure_ascii=False),
                ex=APPROVAL_RECORD_TTL_SECONDS,
            )
            logger.info(
                "审批进行中: %s, 当前审批数: %s",
                approval_id, len(approval_record["approvals"]),
            )
            return {"status": "success", "message": "审批已记录，等待其他审批人"}
        finally:
            try:
                self._redis_client.eval(_RELEASE_IF_OWNED_LUA, 1, lock_key, holder)
            except Exception:  # noqa: BLE001 — 释放失败靠锁 TTL 兜底过期
                logger.warning("审批锁释放失败（TTL 兜底）: %s", lock_key, exc_info=True)

    @staticmethod
    def _build_completed_event_data(
        task_config: Dict[str, Any],
        approval_id: str,
        final_decision: str,
        approvals: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """构造审批完成事件数据（内存/Redis 两路共用，形状与历史一致）。"""
        return {
            "node_id": task_config.get("node_id"),
            "execution_id": task_config.get("execution_id"),
            "flow_id": task_config.get("flow_id"),
            "tenant_id": task_config.get("tenant_id") or "default",
            "trigger_type": "approval_completed",
            "approval_id": approval_id,
            "final_decision": final_decision,
            "approval_details": approvals,
            "timestamp": int(time.time() * 1000),
            "success": True
        }

    def _check_approval_result(self, approval_record: Dict[str, Any]) -> str:
        """
        检查审批结果

        Args:
            approval_record: 审批记录

        Returns:
            str: 审批结果 (approve/reject/None表示未完成)
        """
        approvals = approval_record["approvals"]
        strategy = approval_record["strategy"]
        required_approvers = approval_record["required_approvers"]

        approve_count = sum(1 for approval in approvals if approval["decision"] == "approve")
        reject_count = sum(1 for approval in approvals if approval["decision"] == "reject")
        total_approvers = len(required_approvers)

        if strategy == "any":
            # 任一审批
            if approve_count > 0:
                return "approve"
            elif reject_count > 0:
                return "reject"
        elif strategy == "all":
            # 全部审批
            if reject_count > 0:
                return "reject"
            elif approve_count == total_approvers:
                return "approve"
        elif strategy == "majority":
            # 多数审批
            if reject_count > total_approvers // 2:
                return "reject"
            elif approve_count > total_approvers // 2:
                return "approve"

        return None  # 未完成

    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """验证审批任务配置"""
        if not super().validate_task_config(task_config):
            return False

        approval_config = task_config.get("approval_config")
        approver_config = task_config.get("approver_config")

        if not approval_config:
            logger.error("缺少审批配置")
            return False

        if not approver_config:
            logger.error("缺少审批人配置")
            return False

        if not approver_config.get("approvers"):
            logger.error("缺少审批人列表")
            return False

        return True

    def _load_all_records(self) -> List[Dict[str, Any]]:
        """扫描并解析全部待审批记录（Redis 路径）。坏记录跳过不中断。"""
        try:
            keys = list(self._redis_client.scan_iter(match=f"{self._record_prefix}*"))
        except Exception as e:  # noqa: BLE001 — 巡检失败返回空不扩散
            logger.error("扫描审批记录失败: %s", e, exc_info=True)
            return []
        if not keys:
            return []
        try:
            raws = self._redis_client.mget(keys)
        except Exception as e:  # noqa: BLE001 — 同上
            logger.error("批量读取审批记录失败: %s", e, exc_info=True)
            return []
        records = []
        for raw in raws:
            if not raw:
                continue
            try:
                records.append(json.loads(raw))
            except Exception:  # noqa: BLE001 — 坏记录跳过
                logger.error("审批记录反序列化失败（跳过）: %r", raw[:200])
        return records

    def get_pending_approvals(self) -> Dict[str, Any]:
        """获取待审批任务（对外形状与内存版一致）"""
        if not self._use_redis:
            return {k: {
                "approval_id": v["approval_id"],
                "status": v["status"],
                "created_time": v["created_time"],
                "required_approvers": v["required_approvers"],
                "current_approvals": len(v["approvals"]),
                "strategy": v["strategy"]
            } for k, v in self.pending_approvals.items()}
        return {r["approval_id"]: {
            "approval_id": r["approval_id"],
            "status": r["status"],
            "created_time": r["created_time"],
            "required_approvers": r["required_approvers"],
            "current_approvals": len(r["approvals"]),
            "strategy": r["strategy"]
        } for r in self._load_all_records()}

    def get_approval_details(self, approval_id: str) -> Dict[str, Any]:
        """获取审批详情"""
        if self._use_redis:
            raw = self._redis_client.get(self._record_key(approval_id))
            if not raw:
                return {"error": "审批任务不存在"}
            return json.loads(raw)
        if approval_id not in self.pending_approvals:
            return {"error": "审批任务不存在"}

        return self.pending_approvals[approval_id].copy()
