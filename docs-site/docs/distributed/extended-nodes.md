# 扩展节点

扩展节点（`plaita.server.nodes`）是基于 `EventNode` 的预置节点，覆盖四类常见长时等待场景。它们经 `plaita.nodes` entry_points **随包自动注册**（无需额外安装即可解析；实际执行需相应基础设施，通常配合 `server` extra 的外延服务）。

!!! note "字段名与事件类型"

    扩展节点字段均为**小写下划线**形式（无驼峰归一化）；字段值不支持 `{{ }}` 模板插值，支持 `$` 前缀表达式。除 `delay` 外，其余扩展节点会在构造时把 `event_type` **固定**为约定值——外延服务按该类型发布事件。

## 总览

| type | 类 | 触发事件（固定） | 用途 |
|------|------|------|------|
| `delay` | `DelayNode` | `delay_trigger`（默认，可改） | 延迟指定时间后触发 |
| `redis_queue` | `RedisQueueNode` | `redis_message` | 等待 Redis 队列消息 |
| `kafka_queue` | `KafkaQueueNode` | `kafka_message` | 等待 Kafka 队列消息 |
| `http_callback` | `HttpCallbackNode` | `http_callback` | 等待外部 HTTP 回调 |
| `approval` | `ApprovalNode` | `approval_decision` | 等待人工审批决策 |

## BaseExtendedNode

所有扩展节点继承 `BaseExtendedNode`（继承自 `EventNode`）。它们的 `execute` 在挂起前生成一份 **service_config**，由外延服务消费：

```python
def execute(self, execution):
    service_config = self.generate_service_config(execution)  # 子类实现
    result = super().execute(execution)          # EventNode 挂起逻辑
    result.update({"service_config": service_config, "node_subtype": self.node_type})
    return result
```

`generate_service_config` 是子类必须实现的抽象方法，产出外延服务触发恢复所需的信息（如延迟时长、队列名、审批人等）。默认重试配置：`max_retries=3, retry_delay_ms=1000, exponential_backoff=True`。

## delay

延迟节点，到点后由 `DelayService` 发布 `delay_trigger` 事件恢复流程。

```json
{
    "type": "delay",
    "id": "wait_5m",
    "delay_seconds": 5,
    "delay_unit": "minutes"
}
```

| 字段 | 说明 |
|------|------|
| `delay_seconds` | 延迟数值（必填），支持 `$` 表达式 |
| `delay_unit` | `seconds`（默认）/ `minutes` / `hours` / `days` |
| `event_type` | 触发事件类型，默认 `delay_trigger`（此节点可自定义） |

`generate_service_config` 产出 `{type, delay_ms, trigger_timestamp, node_id, execution_id, flow_id, event_type, event_filter, retry_config}`。

## redis_queue / kafka_queue

等待队列消息到达后恢复。配置队列地址与消费参数，对应 `RedisQueueService` / `KafkaQueueService` 监听队列、把消息包装成事件 `publish`。

```json
{
    "type": "redis_queue",
    "id": "wait_msg",
    "event_type": "redis_message",
    "queue_name": "orders"
}
```

`redis_queue` 必填 `queue_name`（另有 `redis_host` / `redis_port` / `queue_type` 等可选字段）；`event_type` 会被固定为 `redis_message`。`kafka_queue` 必填 `bootstrap_servers` / `topic` / `group_id`，`event_type` 固定为 `kafka_message`。

## http_callback

等待外部系统回调一个 URL，回调到达后恢复。

!!! warning "HTTP 路由尚未接线（#113 事实）"

    `HttpCallbackService` **没有**暴露 HTTP 端点：仓内唯一入口是
    `HttpCallbackService.handle_callback_request(path, request_data, headers)`
    （`plaita/server/services/http_callback_service.py`），调用点只有测试与
    `examples/server_demo`（进程内模拟）。console 集群档虽然把
    `http_callback_service` 登记为可拉起进程（`python -m plaita.server.services
    http_callback_service`），但该进程既不监听端口也不消费队列
    （`plaita/server/services/__main__.py` 启动后空转），其任务队列
    `plaita:http_callback:queue` 全仓无读取方——分布式档下回调记录不会被创建，
    节点会一直挂起。

```json
{
    "type": "http_callback",
    "id": "wait_cb",
    "callback_path": "/callback/order/{request_id}",
    "callback_method": "POST"
}
```

主要字段：`callback_path`（为空自动生成）/ `callback_method`（默认 `POST`）/ `callback_timeout_minutes`（默认 60）及认证相关 `require_auth` / `auth_type` / `auth_token` 等；`event_type` 固定为 `http_callback`。

### 认证：`auth_config` 在认领前被消费（#113）

节点生成的 `auth_config`（`require_auth=True` 时含 token / basic / signature
完整凭据）随 `service_config` 落库到回调注册记录；消费侧在**原子认领之前**
校验它（`_verify_callback_auth`）：

| `auth_type` | 凭据载体 | 校验方式 |
|-------------|----------|----------|
| `token` | `Authorization: Bearer <token>` | `hmac.compare_digest` 常数时间比对 |
| `basic` | `Authorization: Basic base64(username:password)` | 用户名与口令各做常数时间比对 |
| `signature` | `X-Plaita-Timestamp` + `X-Plaita-Signature` | HMAC-SHA256 覆盖 `<unix 秒>.<规范 JSON body>`（`sort_keys=True` + 紧凑分隔符），时间窗 ±300 秒防重放 |

- 校验**必须先于**认领：注册记录的认领是 Redis `GET`+`DEL` 一体（Lua），
  先认领后校验会让一次未授权请求删掉注册记录，把挂起执行变成永久失醒。
  被拒绝的请求不消费记录，注册仍然有效。
- `auth_config` 缺失/为空 → 视为该记录未声明认证（兼容历史注册记录与手写
  `task_config`）；声明了认证但 `type` 未知/缺失 → 拒绝（fail closed）。
- **未认证响应统一（Q5）**：认证已配置时，「凭据缺失 / 错误 / 签名过期」、
  「路径未注册」、以及认领竞态中的败方返回**同一条**响应
  `{"status": "error", "message": "回调路径未注册"}`（`CALLBACK_FAILURE_MESSAGE`）
  —— 未认证的调用方无法据此判断某个回调路径是否存在（两条不同的 message 就是
  path-existence oracle）。凭据失败细节只写服务日志，鉴权通过后才回成功响应。
- **边界（无法关闭，且非认证 oracle）**：注册记录未声明 `auth_config`
  （`require_auth=False` 或历史/手写 `task_config`）时本服务不校验，任何调用方
  都能触发该路径，其响应与「未知路径」的 error 不同。这是**部署事实**（该回调
  按契约本就不需要认证），不是「凭据是否正确」的推断通道；本服务不隐藏
  「哪些路径被注册为免认证」。
- 签名头名与规范原文是本实现在 Python 入口定义的契约；接入 HTTP 层时需按此
  透传，尚无第二方实现。

!!! note "`require_auth` 默认值：待 owner 裁定"

    `require_auth` 目前默认 `True`，且仓内没有把生成凭据交付给回调方的通道
    （回调路径由节点生成、凭据只落在注册记录里）——默认开启时回调事实上不可达
    （fail closed，不会误放行）。保持 `True` + fail closed，还是把公共默认翻成
    `False`，属于契约变更，**待 owner 裁定**。本页描述的是当前实现，不代表默认
    值已定。

`validate_headers` / `validate_params`（`validation_config`）当前**未接线**：
生成并落库，但消费侧不校验（与 #113 同类问题，另行决定接线或移除）。

## approval

人工审批节点，发起审批并等待决策。`ApprovalService` 通知审批人、接收决策、发布 `approval_decision` 事件恢复流程。

```json
{
    "type": "approval",
    "id": "manager_approve",
    "event_type": "approval_decision",
    "approval_title": "请假申请",
    "approval_content": "{% $INPUT.reason %}",
    "approval_type": "manual",
    "approvers": ["alice", "bob"],
    "approval_strategy": "any",
    "auto_escalation": false,
    "form_fields": [],
    "allow_comments": true
}
```

| 字段 | 说明 |
|------|------|
| `approval_title` / `approval_content` | 标题 / 内容（均必填，支持 `$` 表达式） |
| `event_type` | 必填（继承自事件节点）；构造后固定为 `approval_decision` |
| `approval_type` | `manual`（默认）/ `auto` |
| `approvers` | 审批人列表（用户 id 或邮箱，支持表达式） |
| `approval_strategy` | `any`（默认，任一）/ `all`（全部）/ `majority`（多数） |
| `auto_escalation` / `escalation_timeout_hours` / `escalation_approvers` | 自动升级配置 |
| `form_fields` | 审批表单字段 |
| `allow_comments` / `require_comments` | 审批意见开关 |
| `subscription_timeout` | 订阅超时秒数（继承自事件节点）。审批等待方是人，**缺省 24 小时**：无人审批时落 `timeout` 终态，而不是永久 `suspended`；显式传 `null` 退回无限等待 |

`ApprovalNode` 恢复事件固定为 `approval_decision`；`generate_service_config` 产出审批实例 id、审批人、表单、通知配置等供 `ApprovalService` 使用。`ApprovalService` 消费 worker 派发的 `plaita:approval:queue`，为每个审批落一条 `plaita:approval:pending:{id}` 记录（7 天 TTL）——控制面据此列出待办审批，决策经 `submit_approval_decision` 提交。详见 [审批流场景](../scenarios/approval-flow.md)。

## 在流程中使用

扩展节点通过 entry_points 自动注册，`pip install plaita[server]` 后直接在 JSON 里用即可。在 Distributed 模式下执行到这类节点会自动挂起：

```python
from plaita import Flow, FlowExecution
from plaita.event.memory import InMemoryEventBus

flow = Flow.from_string(open("approval_flow.json").read())
execution = FlowExecution(event_bus=InMemoryEventBus())
step = execution.run_distributed(flow, {"applicant": "alice", "reason": "请假"})
# step["is_suspend"] == True
```

## 自定义扩展节点

继承 `BaseExtendedNode` 实现 `generate_service_config`，并配套一个外延服务（见 [外延服务](services.md)）处理触发：

```python
from plaita.server.nodes.base_extended_node import BaseExtendedNode

class MyWaitNode(BaseExtendedNode):
    node_type = "my_wait"
    node_name = "我的等待"

    def generate_service_config(self, execution):
        return {"type": "my_wait", "node_id": self.id, ...}
```

## 下一步

- [外延服务](services.md) —— 节点挂起后谁来触发恢复
- [审批流场景](../scenarios/approval-flow.md)
- [API: plaita.server.flow_worker](../api/server.md)
