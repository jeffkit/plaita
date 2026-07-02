"""
审批服务实现（简化版本）
负责处理审批流程
"""
import time
from typing import Any, Dict, List

from .base_service import BaseExtendedService
from ...logger import logger


class ApprovalService(BaseExtendedService):
    """
    审批服务
    负责创建审批任务并处理审批决策
    """
    
    def __init__(self, event_bus, service_config=None):
        super().__init__(event_bus, service_config)
        self.pending_approvals = {}  # 存储待审批的任务
    
    def get_service_type(self) -> str:
        """获取服务类型"""
        return "approval"
    
    def start_service(self) -> bool:
        """启动审批服务"""
        try:
            self.is_running = True
            logger.info("审批服务已启动")
            return True
        except Exception as e:
            logger.error("启动审批服务失败: %s", e, exc_info=True)
            return False
    
    def stop_service(self) -> bool:
        """停止审批服务"""
        try:
            self.is_running = False
            self.pending_approvals.clear()
            logger.info("审批服务已停止")
            return True
        except Exception as e:
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
            
            self.pending_approvals[approval_id] = approval_record
            
            logger.info(f"审批任务已创建: {approval_id}, 审批人: {approval_record['required_approvers']}")
            
            # 在实际实现中，这里会发送通知给审批人
            # 目前只是简单存储审批记录
            
            return True
            
        except Exception as e:
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
                event_data = {
                    "node_id": task_config.get("node_id"),
                    "execution_id": task_config.get("execution_id"),
                    "flow_id": task_config.get("flow_id"),
                    "trigger_type": "approval_completed",
                    "approval_id": approval_id,
                    "final_decision": final_decision,
                    "approval_details": approval_record["approvals"],
                    "timestamp": int(time.time() * 1000),
                    "success": True
                }
                
                # 触发事件
                await self.trigger_event(task_config.get("event_type"), event_data)
                
                # 更新状态并从待审批列表移除
                approval_record["status"] = final_decision
                approval_record["completed_time"] = int(time.time() * 1000)
                del self.pending_approvals[approval_id]
                
                logger.info(f"审批完成: {approval_id}, 最终决策: {final_decision}")
                
                return {"status": "success", "final_decision": final_decision, "message": "审批完成"}
            else:
                logger.info(f"审批进行中: {approval_id}, 当前审批数: {len(approval_record['approvals'])}")
                return {"status": "success", "message": "审批已记录，等待其他审批人"}
                
        except Exception as e:
            logger.error("提交审批决策失败: %s", e, exc_info=True)
            return {"status": "error", "message": str(e)}
    
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
    
    def get_pending_approvals(self) -> Dict[str, Any]:
        """获取待审批任务"""
        return {k: {
            "approval_id": v["approval_id"],
            "status": v["status"],
            "created_time": v["created_time"],
            "required_approvers": v["required_approvers"],
            "current_approvals": len(v["approvals"]),
            "strategy": v["strategy"]
        } for k, v in self.pending_approvals.items()}
    
    def get_approval_details(self, approval_id: str) -> Dict[str, Any]:
        """获取审批详情"""
        if approval_id not in self.pending_approvals:
            return {"error": "审批任务不存在"}
        
        return self.pending_approvals[approval_id].copy() 