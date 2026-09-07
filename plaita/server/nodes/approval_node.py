"""
审批节点实现
支持人工审批流程，等待审批决策后触发事件继续流程
"""
import time
from typing import Any, ClassVar, Dict, List, Literal, Optional, Union
from pydantic import Field, model_validator

from .base_extended_node import BaseExtendedNode
from ...logger import logger


class ApprovalNode(BaseExtendedNode):
    """
    审批节点
    发起人工审批流程，等待审批决策后触发事件继续流程
    """

    node_type: ClassVar[str] = "approval"
    node_name: ClassVar[str] = "审批节点"

    # 运行时契约：审批节点固定订阅 approval_decision 事件。历史上 event_type
    # 必填而用户值必被 __init__ 覆盖（伪必填地雷，2026-09 表单评审）；现在由
    # _normalize_enums 在校验前注入，schema 不再 required
    event_type: str = Field(default="approval_decision", description="内部事件类型标识，由节点自动设定，请勿修改")

    # 审批配置
    approval_title: str = Field(description="审批标题")
    approval_content: str = Field(description="审批内容")
    # Literal 生成 schema enum（console 表单渲染下拉）；大小写归一见 _normalize_enums
    approval_type: Literal["manual", "auto"] = Field(default="manual", description="审批类型: manual（人工审批）/ auto（自动通过）")

    # 审批人配置
    approvers: List[str] = Field(description="审批人列表（用户ID或邮箱）")
    approval_strategy: Literal["any", "all", "majority"] = Field(
        default="any", description="审批策略: any（任一审批）/ all（全部审批）/ majority（多数审批）"
    )
    auto_escalation: bool = Field(default=False, description="是否自动升级")
    escalation_timeout_hours: int = Field(default=24, description="升级超时时间（小时）")
    escalation_approvers: List[str] = Field(default_factory=list, description="升级审批人列表")

    # 表单配置
    form_fields: List[Dict[str, Any]] = Field(default_factory=list, description="审批表单字段")
    allow_comments: bool = Field(default=True, description="是否允许审批意见")
    require_comments: bool = Field(default=False, description="是否必须填写审批意见")

    # 通知配置
    notification_config: Dict[str, Any] = Field(default_factory=dict, description="通知配置")

    @model_validator(mode="before")
    @classmethod
    def _normalize_enums(cls, values):
        # "ALL"/"Manual" 等历史写法曾无任何校验直接透传，Literal 之前先归一
        if isinstance(values, dict):
            for key in ("approval_type", "approval_strategy"):
                v = values.get(key)
                if isinstance(v, str):
                    values[key] = v.lower()
            # 强制事件订阅契约（原 __init__ 无条件覆盖行为的等价迁移）
            values["event_type"] = "approval_decision"
        return values

    def generate_service_config(self, execution) -> Dict[str, Any]:
        """
        生成审批服务配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 审批服务配置
        """
        # 解析可能的变量引用
        resolved_config = self._resolve_approval_config(execution)
        
        # 生成审批实例ID
        approval_id = f"approval_{self.id}_{int(time.time() * 1000)}"
        
        config = {
            "type": "approval",
            "node_id": self.id,
            "approval_id": approval_id,
            "execution_id": execution.execution_id,
            "flow_id": execution.state.flow_id,
            "event_type": self.event_type,
            "event_filter": self.event_filter,
            "approval_config": resolved_config["approval"],
            "approver_config": resolved_config["approvers"],
            "form_config": resolved_config["form"],
            "notification_config": resolved_config["notification"],
            "retry_config": self.get_default_retry_config()
        }
        
        logger.info("审批节点 [%s] 配置: 审批ID=%s, 审批人=%s", self.id, approval_id, resolved_config['approvers']['approvers'])
        
        return config
    
    def _resolve_approval_config(self, execution) -> Dict[str, Any]:
        """
        解析审批配置，支持变量引用
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 解析后的配置
        """
        approval_config = {
            "title": self._resolve_value(execution, self.approval_title),
            "content": self._resolve_value(execution, self.approval_content),
            "type": self.approval_type,
            "created_time": int(time.time() * 1000)
        }
        
        # 解析审批人列表
        resolved_approvers = []
        for approver in self.approvers:
            resolved_approver = self._resolve_value(execution, approver)
            if isinstance(resolved_approver, list):
                resolved_approvers.extend(resolved_approver)
            else:
                resolved_approvers.append(resolved_approver)
        
        # 解析升级审批人列表
        resolved_escalation_approvers = []
        for approver in self.escalation_approvers:
            resolved_approver = self._resolve_value(execution, approver)
            if isinstance(resolved_approver, list):
                resolved_escalation_approvers.extend(resolved_approver)
            else:
                resolved_escalation_approvers.append(resolved_approver)
        
        approver_config = {
            "approvers": resolved_approvers,
            "strategy": self.approval_strategy,
            "auto_escalation": self.auto_escalation,
            "escalation_timeout_hours": self.escalation_timeout_hours,
            "escalation_approvers": resolved_escalation_approvers
        }
        
        # 解析表单配置
        resolved_form_fields = []
        for field in self.form_fields:
            resolved_field = {}
            for key, value in field.items():
                resolved_field[key] = self._resolve_value(execution, value)
            resolved_form_fields.append(resolved_field)
        
        form_config = {
            "fields": resolved_form_fields,
            "allow_comments": self.allow_comments,
            "require_comments": self.require_comments
        }
        
        # 解析通知配置
        resolved_notification_config = {}
        for key, value in self.notification_config.items():
            resolved_notification_config[key] = self._resolve_value(execution, value)
        
        return {
            "approval": approval_config,
            "approvers": approver_config,
            "form": form_config,
            "notification": resolved_notification_config
        }
    
    def _resolve_value(self, execution, value):
        """
        解析单个值，支持变量引用
        
        Args:
            execution: 执行上下文
            value: 要解析的值
            
        Returns:
            Any: 解析后的值
        """
        if isinstance(value, str) and value.startswith('$'):
            try:
                resolved = execution.evaluate(value)
                return resolved if resolved is not None else value
            except Exception as e:
                logger.warning("解析变量引用失败 %s: %s", value, e)
                return value
        
        return value
    
    def validate_service_config(self, config: Dict[str, Any]) -> bool:
        """
        验证审批服务配置
        
        Args:
            config: 服务配置
            
        Returns:
            bool: 配置是否有效
        """
        required_fields = ["approval_config", "approver_config", "form_config", "node_id", "event_type"]
        for field in required_fields:
            if field not in config:
                logger.error("审批节点配置缺少必要字段: %s", field)
                return False
        
        # 验证审批配置
        approval_config = config["approval_config"]
        if not approval_config.get("title") or not approval_config.get("content"):
            logger.error("审批标题和内容不能为空")
            return False
        
        # 验证审批人配置
        approver_config = config["approver_config"]
        if not approver_config.get("approvers"):
            logger.error("审批人列表不能为空")
            return False
        
        if approver_config.get("strategy") not in ["any", "all", "majority"]:
            logger.error("不支持的审批策略")
            return False
            
        return True
    
    def create_approval_url(self, approval_id: str, approver_id: str) -> str:
        """
        创建审批URL
        
        Args:
            approval_id: 审批ID
            approver_id: 审批人ID
            
        Returns:
            str: 审批URL
        """
        base_url = self.service_config.get("base_url", "http://localhost:8080")
        return f"{base_url.rstrip('/')}/approval/{approval_id}?approver={approver_id}"
    
    def get_approval_statistics(self) -> Dict[str, Any]:
        """
        获取审批统计信息（用于监控和报表）
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        return {
            "approver_count": len(self.approvers),
            "escalation_enabled": self.auto_escalation,
            "escalation_approver_count": len(self.escalation_approvers),
            "form_field_count": len(self.form_fields),
            "strategy": self.approval_strategy
        } 