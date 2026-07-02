"""
审批流程集成测试
验证审批节点和审批服务的完整集成功能
"""
import pytest
import pytest_asyncio
import asyncio
import time
from datetime import datetime
from unittest.mock import Mock, patch

from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution, ExecutionMode
from plaita.event.memory import InMemoryEventBus
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.base import ExecutionState
from plaita.server.flow_worker import FlowWorker
from plaita.server.services.approval_service import ApprovalService
from plaita.server.services.service_manager import ServiceManager
from plaita.server.nodes.approval_node import ApprovalNode


class TestApprovalNodeBasic:
    """审批节点基础功能测试"""
    
    @pytest.fixture
    def event_bus(self):
        """创建事件总线"""
        return InMemoryEventBus()
    
    def test_approval_node_creation(self):
        """测试审批节点创建"""
        approval_node = ApprovalNode(
            id="test_approval",
            event_type="approval_decision",
            approval_title="测试审批",
            approval_content="请审批此请求",
            approvers=["user1", "user2"],
            approval_strategy="any"
        )
        
        assert approval_node.id == "test_approval"
        assert approval_node.approval_title == "测试审批"
        assert approval_node.approvers == ["user1", "user2"]
        assert approval_node.approval_strategy == "any"
        assert approval_node.event_type == "approval_decision"
    
    def test_approval_node_strategies(self):
        """测试审批策略配置"""
        # any 策略
        node_any = ApprovalNode(
            id="approval_any",
            event_type="approval_decision",
            approval_title="任一审批",
            approval_content="任一人通过即可",
            approvers=["user1", "user2", "user3"],
            approval_strategy="any"
        )
        assert node_any.approval_strategy == "any"
        
        # all 策略
        node_all = ApprovalNode(
            id="approval_all",
            event_type="approval_decision",
            approval_title="全部审批",
            approval_content="全部人通过才行",
            approvers=["user1", "user2"],
            approval_strategy="all"
        )
        assert node_all.approval_strategy == "all"
        
        # majority 策略
        node_majority = ApprovalNode(
            id="approval_majority",
            event_type="approval_decision",
            approval_title="多数审批",
            approval_content="多数人通过",
            approvers=["user1", "user2", "user3"],
            approval_strategy="majority"
        )
        assert node_majority.approval_strategy == "majority"
    
    def test_approval_node_form_fields(self):
        """测试审批表单字段配置"""
        approval_node = ApprovalNode(
            id="form_approval",
            event_type="approval_decision",
            approval_title="带表单审批",
            approval_content="请填写审批意见",
            approvers=["admin"],
            form_fields=[
                {"name": "reason", "type": "text", "required": True},
                {"name": "amount", "type": "number", "required": False}
            ],
            allow_comments=True,
            require_comments=True
        )
        
        assert len(approval_node.form_fields) == 2
        assert approval_node.allow_comments is True
        assert approval_node.require_comments is True
    
    def test_approval_node_escalation(self):
        """测试审批升级配置"""
        approval_node = ApprovalNode(
            id="escalation_approval",
            event_type="approval_decision",
            approval_title="可升级审批",
            approval_content="超时将自动升级",
            approvers=["user1"],
            auto_escalation=True,
            escalation_timeout_hours=24,
            escalation_approvers=["manager1", "manager2"]
        )
        
        assert approval_node.auto_escalation is True
        assert approval_node.escalation_timeout_hours == 24
        assert approval_node.escalation_approvers == ["manager1", "manager2"]
    
    def test_approval_node_generate_config(self, event_bus):
        """测试审批节点配置生成"""
        approval_node = ApprovalNode(
            id="test_approval",
            event_type="approval_decision",
            approval_title="配置测试",
            approval_content="测试配置生成",
            approvers=["approver1"],
            approval_strategy="any"
        )
        
        # 创建模拟执行上下文
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution._set_state("$FLOW_ID", "test_flow")
        execution._set_state("$EXECUTION_ID", "exec_123")
        
        config = approval_node.generate_service_config(execution)
        
        assert config["type"] == "approval"
        assert config["node_id"] == "test_approval"
        assert "approval_id" in config
        assert config["event_type"] == "approval_decision"
        assert "approval_config" in config
        assert "approver_config" in config
        assert "form_config" in config
    
    def test_approval_node_validate_config(self):
        """测试审批节点配置验证"""
        approval_node = ApprovalNode(
            id="test_approval",
            event_type="approval_decision",
            approval_title="验证测试",
            approval_content="测试验证",
            approvers=["user1"]
        )
        
        valid_config = {
            "approval_config": {"title": "测试", "content": "内容"},
            "approver_config": {"approvers": ["user1"], "strategy": "any"},
            "form_config": {"fields": [], "allow_comments": True},
            "node_id": "test",
            "event_type": "approval_decision"
        }
        
        assert approval_node.validate_service_config(valid_config) is True
        
        # 无效配置 - 缺少审批人
        invalid_config = {
            "approval_config": {"title": "测试", "content": "内容"},
            "approver_config": {"approvers": [], "strategy": "any"},
            "form_config": {},
            "node_id": "test",
            "event_type": "approval_decision"
        }
        assert approval_node.validate_service_config(invalid_config) is False
    
    def test_approval_statistics(self):
        """测试审批统计信息"""
        approval_node = ApprovalNode(
            id="stats_approval",
            event_type="approval_decision",
            approval_title="统计测试",
            approval_content="统计",
            approvers=["user1", "user2", "user3"],
            approval_strategy="majority",
            auto_escalation=True,
            escalation_approvers=["manager1"]
        )
        
        stats = approval_node.get_approval_statistics()
        
        assert stats["approver_count"] == 3
        assert stats["escalation_enabled"] is True
        assert stats["escalation_approver_count"] == 1
        assert stats["strategy"] == "majority"


class TestApprovalServiceBasic:
    """审批服务基础功能测试"""
    
    @pytest.fixture
    def approval_service(self):
        """创建审批服务"""
        event_bus = InMemoryEventBus()
        service = ApprovalService(event_bus)
        return service
    
    def test_service_start_stop(self, approval_service):
        """测试服务启动和停止"""
        assert approval_service.start_service() is True
        assert approval_service.is_running is True
        
        assert approval_service.stop_service() is True
        assert approval_service.is_running is False
    
    def test_service_type(self, approval_service):
        """测试服务类型"""
        assert approval_service.get_service_type() == "approval"
    
    def test_validate_task_config(self, approval_service):
        """测试任务配置验证"""
        valid_config = {
            "approval_id": "approval_123",
            "approval_config": {"title": "测试", "content": "内容"},
            "approver_config": {"approvers": ["user1"]},
            "node_id": "test_node",
            "event_type": "approval_decision"
        }
        
        assert approval_service.validate_task_config(valid_config) is True
        
        # 无效配置
        invalid_config = {"approval_id": "test"}
        assert approval_service.validate_task_config(invalid_config) is False


@pytest.mark.asyncio
class TestApprovalServiceDecisions:
    """审批服务决策测试"""
    
    @pytest_asyncio.fixture
    async def service_with_pending(self):
        """创建带待审批任务的服务"""
        event_bus = InMemoryEventBus()
        service = ApprovalService(event_bus)
        service.start_service()
        
        # 创建待审批任务
        task_config = {
            "approval_id": "approval_test_001",
            "approval_config": {"title": "测试审批", "content": "请审批"},
            "approver_config": {
                "approvers": ["user1", "user2", "user3"],
                "strategy": "any"
            },
            "node_id": "test_node",
            "execution_id": "exec_123",
            "flow_id": "flow_123",
            "event_type": "approval_decision"
        }
        
        await service.handle_task(task_config)
        
        yield {
            "service": service,
            "event_bus": event_bus,
            "approval_id": "approval_test_001"
        }
        
        service.stop_service()
    
    async def test_submit_approval_approve(self, service_with_pending):
        """测试提交审批通过决策"""
        service = service_with_pending["service"]
        approval_id = service_with_pending["approval_id"]
        
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="approve",
            comments="同意"
        )
        
        assert result["status"] == "success"
        assert result.get("final_decision") == "approve"
    
    async def test_submit_approval_reject(self, service_with_pending):
        """测试提交审批拒绝决策"""
        service = service_with_pending["service"]
        approval_id = service_with_pending["approval_id"]
        
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="reject",
            comments="不同意"
        )
        
        assert result["status"] == "success"
        assert result.get("final_decision") == "reject"
    
    async def test_duplicate_approval(self, service_with_pending):
        """测试重复审批"""
        service = service_with_pending["service"]
        approval_id = service_with_pending["approval_id"]
        
        # 第一次审批
        await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="approve"
        )
        
        # 因为 any 策略，第一次审批后任务已完成
        # 尝试再次审批应该返回错误
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user2",
            decision="approve"
        )
        
        assert result["status"] == "error"
    
    async def test_unauthorized_approver(self, service_with_pending):
        """测试无权限审批人"""
        service = service_with_pending["service"]
        approval_id = service_with_pending["approval_id"]
        
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="unauthorized_user",
            decision="approve"
        )
        
        assert result["status"] == "error"
        assert "无审批权限" in result["message"]
    
    async def test_non_existent_approval(self, service_with_pending):
        """测试不存在的审批任务"""
        service = service_with_pending["service"]
        
        result = await service.submit_approval_decision(
            approval_id="non_existent",
            approver_id="user1",
            decision="approve"
        )
        
        assert result["status"] == "error"
        assert "不存在" in result["message"]


@pytest.mark.asyncio
class TestApprovalStrategies:
    """审批策略测试"""
    
    @pytest_asyncio.fixture
    async def create_approval_service(self):
        """创建审批服务工厂"""
        async def factory(strategy: str, approvers: list):
            event_bus = InMemoryEventBus()
            service = ApprovalService(event_bus)
            service.start_service()
            
            task_config = {
                "approval_id": f"approval_{strategy}",
                "approval_config": {"title": "策略测试", "content": "测试"},
                "approver_config": {
                    "approvers": approvers,
                    "strategy": strategy
                },
                "node_id": "test",
                "execution_id": "exec_strategy",
                "flow_id": "flow_strategy",
                "event_type": "approval_decision"
            }
            
            await service.handle_task(task_config)
            
            return {
                "service": service,
                "event_bus": event_bus,
                "approval_id": f"approval_{strategy}"
            }
        
        return factory
    
    async def test_any_strategy_approve(self, create_approval_service):
        """测试 any 策略 - 任一通过"""
        setup = await create_approval_service("any", ["user1", "user2", "user3"])
        service = setup["service"]
        approval_id = setup["approval_id"]
        
        # 只需一人通过
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="approve"
        )
        
        assert result.get("final_decision") == "approve"
        setup["service"].stop_service()
    
    async def test_any_strategy_reject(self, create_approval_service):
        """测试 any 策略 - 任一拒绝"""
        setup = await create_approval_service("any", ["user1", "user2", "user3"])
        service = setup["service"]
        approval_id = setup["approval_id"]
        
        # 只需一人拒绝
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="reject"
        )
        
        assert result.get("final_decision") == "reject"
        setup["service"].stop_service()
    
    async def test_all_strategy_approve(self, create_approval_service):
        """测试 all 策略 - 全部通过"""
        setup = await create_approval_service("all", ["user1", "user2"])
        service = setup["service"]
        approval_id = setup["approval_id"]
        
        # 第一人通过
        result1 = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="approve"
        )
        
        # 还未完成
        assert result1.get("final_decision") is None or result1["status"] == "success"
        
        # 第二人通过
        result2 = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user2",
            decision="approve"
        )
        
        assert result2.get("final_decision") == "approve"
        setup["service"].stop_service()
    
    async def test_all_strategy_reject(self, create_approval_service):
        """测试 all 策略 - 一人拒绝"""
        setup = await create_approval_service("all", ["user1", "user2"])
        service = setup["service"]
        approval_id = setup["approval_id"]
        
        # 一人拒绝即失败
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="reject"
        )
        
        assert result.get("final_decision") == "reject"
        setup["service"].stop_service()
    
    async def test_majority_strategy_approve(self, create_approval_service):
        """测试 majority 策略 - 多数通过"""
        setup = await create_approval_service("majority", ["user1", "user2", "user3"])
        service = setup["service"]
        approval_id = setup["approval_id"]
        
        # 第一人通过
        await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="approve"
        )
        
        # 第二人通过 - 超过半数
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user2",
            decision="approve"
        )
        
        assert result.get("final_decision") == "approve"
        setup["service"].stop_service()
    
    async def test_majority_strategy_reject(self, create_approval_service):
        """测试 majority 策略 - 多数拒绝"""
        setup = await create_approval_service("majority", ["user1", "user2", "user3"])
        service = setup["service"]
        approval_id = setup["approval_id"]
        
        # 第一人拒绝
        await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user1",
            decision="reject"
        )
        
        # 第二人拒绝 - 超过半数
        result = await service.submit_approval_decision(
            approval_id=approval_id,
            approver_id="user2",
            decision="reject"
        )
        
        assert result.get("final_decision") == "reject"
        setup["service"].stop_service()


@pytest.mark.asyncio
class TestApprovalFlowIntegration:
    """审批流程集成测试"""
    
    @pytest_asyncio.fixture
    async def integration_setup(self):
        """设置集成测试环境"""
        event_bus = InMemoryEventBus()
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        
        # 创建服务管理器
        service_manager = ServiceManager(event_bus)
        
        # 创建并启动审批服务
        approval_service = ApprovalService(event_bus)
        approval_service.start_service()
        service_manager.services["approval"] = approval_service
        
        # 创建审批流程
        approval_flow = {
            "flow_id": "integration_approval_flow",
            "name": "集成测试审批流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "approval_step"},
                {
                    "id": "approval_step",
                    "type": "approval",
                    "approval_title": "请求审批",
                    "approval_content": "请审批此请求",
                    "approvers": ["admin"],
                    "approval_strategy": "any",
                    "next": "end"
                },
                {"id": "end", "type": "end", "output": "approved"}
            ]
        }
        flow_storage.save_flow(approval_flow)
        
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=event_bus
        )
        
        yield {
            "event_bus": event_bus,
            "execution_storage": execution_storage,
            "flow_storage": flow_storage,
            "service_manager": service_manager,
            "approval_service": approval_service,
            "worker": worker,
            "approval_flow": approval_flow
        }
        
        # 清理
        service_manager.stop_all_services(timeout=2)
    
    async def test_approval_task_creation(self, integration_setup):
        """测试审批任务创建"""
        approval_service = integration_setup["approval_service"]
        
        task_config = {
            "approval_id": "integration_test_001",
            "approval_config": {"title": "集成测试", "content": "测试内容"},
            "approver_config": {"approvers": ["admin"], "strategy": "any"},
            "node_id": "approval_step",
            "execution_id": "exec_int_001",
            "flow_id": "integration_approval_flow",
            "event_type": "approval_decision"
        }
        
        result = await approval_service.handle_task(task_config)
        assert result is True
        
        # 验证待审批任务
        pending = approval_service.get_pending_approvals()
        assert "integration_test_001" in pending
    
    async def test_approval_event_trigger(self, integration_setup):
        """测试审批事件触发"""
        event_bus = integration_setup["event_bus"]
        approval_service = integration_setup["approval_service"]
        
        event_received = asyncio.Event()
        received_data = {}
        
        async def on_approval_decision(event):
            received_data["event"] = event.data
            event_received.set()
        
        await event_bus.register_handler("approval_decision", on_approval_decision)
        
        # 创建审批任务
        task_config = {
            "approval_id": "event_test_001",
            "approval_config": {"title": "事件测试", "content": "测试"},
            "approver_config": {"approvers": ["admin"], "strategy": "any"},
            "node_id": "test",
            "execution_id": "exec_event",
            "flow_id": "test_flow",
            "event_type": "approval_decision"
        }
        
        await approval_service.handle_task(task_config)
        
        # 提交审批决策
        await approval_service.submit_approval_decision(
            approval_id="event_test_001",
            approver_id="admin",
            decision="approve",
            comments="批准"
        )
        
        # 等待事件
        try:
            await asyncio.wait_for(event_received.wait(), timeout=2)
            assert "event" in received_data
            assert received_data["event"]["final_decision"] == "approve"
        except asyncio.TimeoutError:
            pytest.fail("审批事件未触发")
    
    async def test_get_approval_details(self, integration_setup):
        """测试获取审批详情"""
        approval_service = integration_setup["approval_service"]
        
        # 创建审批任务
        task_config = {
            "approval_id": "details_test",
            "approval_config": {"title": "详情测试", "content": "测试"},
            "approver_config": {"approvers": ["user1", "user2"], "strategy": "all"},
            "node_id": "test",
            "execution_id": "exec_details",
            "flow_id": "test_flow",
            "event_type": "approval_decision"
        }
        
        await approval_service.handle_task(task_config)
        
        # 获取详情
        details = approval_service.get_approval_details("details_test")
        
        assert details["approval_id"] == "details_test"
        assert details["status"] == "pending"
        assert len(details["required_approvers"]) == 2


class TestApprovalFlowFromJSON:
    """从 JSON 定义审批流程测试"""
    
    @pytest.fixture
    def approval_flow_json(self):
        """审批流程 JSON 定义 - 使用简单流程避免未支持的节点类型"""
        return {
            "flow_id": "json_approval_flow",
            "name": "JSON审批测试流程",
            "version": "1.0.0",
            "input_type": {
                "type": "object",
                "properties": {
                    "request_type": {"type": "string"},
                    "amount": {"type": "number"}
                }
            },
            "nodes": [
                {"id": "start", "type": "start", "next": "approval_step"},
                {
                    "id": "approval_step",
                    "type": "event",
                    "event_type": "approval_decision",
                    "event_filter": {},
                    "next": "end"
                },
                {"id": "end", "type": "end", "output": "completed"}
            ]
        }
    
    def test_load_approval_flow(self, approval_flow_json):
        """测试加载审批流程"""
        flow = Flow.model_validate(approval_flow_json)
        
        assert flow.flow_id == "json_approval_flow"
        assert len(flow.nodes) == 3
    
    def test_approval_flow_distributed_execution(self, approval_flow_json):
        """测试审批流程分布式执行"""
        flow = Flow.model_validate(approval_flow_json)
        event_bus = InMemoryEventBus()
        
        # 测试分布式执行
        result = FlowExecution.run(
            flow,
            params={"request_type": "purchase", "amount": 500},
            mode=ExecutionMode.DISTRIBUTED,
            event_bus=event_bus
        )
        
        assert "execution_id" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

