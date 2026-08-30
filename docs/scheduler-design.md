# 调度器（schedule_service）设计说明

> 状态：v1 已实现并验证（2026-08-30）
> 范围：引擎 `plaita/server/services/schedule_service.py` + console 后端 `api/schedules.py` + console 前端「触发器」页

## 1. 定位与目标

让流程可以按 cron 周期自动启动。设计原则：**调度器只负责「到点把消息投进队列」，执行完全复用现有 FlowWorker 语义**——调度器本身不加载、不解析、不执行任何流程定义，因此引擎核心零改动。

## 2. 架构

```
Redis plaita:schedules (hash)
        │  每秒扫描
        ▼
schedule_service ──到期的──▶ XADD plaita:flow:queue (Stream, payload=json)
        ▲                              │
        │ heartbeat (RegistryMixin)     ▼ 消费组 plaita-workers
   console 拓扑/服务列表          FlowWorker（执行，回写 plaita:execution:*）

console 后端 /api/schedules ──CRUD──▶ plaita:schedules（与调度服务共享同一视图）
```

### 关键决策

| 决策 | 选择 | 理由 |
|---|---|---|
| 存储位置 | Redis hash `plaita:schedules` | 与执行状态/日志/注册表同域；调度服务与 console 后端无需共享 DB 即可共享视图 |
| 入队方式 | `enqueue_task`（XADD Stream） | FlowWorker 用消费组（XREADGROUP）消费；console 手动「启动流程」历史上误用 rpush（list），同 key 类型冲突导致消息永远到不了 worker，已一并修复（executions.py `_enqueue`） |
| 消息形状 | 与手动启动完全一致 + `trigger` 字段 | `{"type":"start","flow_id","version","params","timestamp","trigger":{"kind":"cron\\|manual","schedule_id","schedule_name"}}`；单一来源 `fire_schedule()`，cron 触发与「立即触发」共用 |
| 防双发 | `SET plaita:schedule:lock:{id} NX PX 30s` | 多实例部署时同一时点只触发一次；cluster_config 中 `max_instances: 1` 是部署意图，锁是纵深防御 |
| 错过补偿 | **不补偿**（skip-forward） | 到期触发一次后 `next_run_at` 直接跳到现在之后的下一个 cron 时点；停机一小时恢复后只补发一次、不连环补跑。v1 刻意从简，`catch_up` 留作后续字段 |
| 时区 | 调度服务所在机器本地时间 | croniter 必须走 `get_next(datetime)` + naive `.timestamp()`；`get_next(float)` 会把 naive 基准当 UTC，触发时间偏移时区（已踩坑并修复） |
| 注册心跳 | `register_service()`（注册+心跳线程） | `__main__` 启动器只调 `start_service`，不调 `start_with_registry`；不补注册调用，30s TTL 后注册过期，拓扑/服务列表看不到调度服务（delay_service 家族存在同样问题，未在本次修复范围） |

## 3. 数据模型

调度定义（`plaita:schedules` hash 字段值，JSON）：

```json
{
  "schedule_id": "sched-20260830014449901",
  "name": "每日内容生产",
  "flow_id": "content-daily",
  "version": "1.0.0",          // 空 = 最新已发布
  "cron": "0 9 * * *",
  "params": {},
  "enabled": true,
  "created_at": "...", "updated_at": "...", "created_by": "",
  "next_run_at": "1788025500000",   // epoch 毫秒（字符串），服务会自愈重算
  "last_fired_at": "...", "last_enqueue_ok": "1"
}
```

触发历史：`plaita:schedule:fires:{id}` list（LPUSH+LTRIM 保留 50 条）：
`{"fired_at","trigger_kind","flow_id","version","enqueue":"ok|failed","msg_id"}`

## 4. 端点（console 后端，Admin API Key）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/schedules` | 列表（附 `status: running/paused` 计算字段） |
| POST | `/api/schedules` | 新建（校验 cron、流程/版本存在性；重算 next_run_at） |
| GET/PUT/DELETE | `/api/schedules/{id}` | 详情 / 更新（cron 或 enabled 变化重算节奏）/ 删除（含历史） |
| POST | `/api/schedules/{id}/enable` `…/disable` | 启用（重算 next_run_at）/ 暂停 |
| POST | `/api/schedules/{id}/trigger` | 立即触发一次（`trigger_kind=manual`，入队失败返回 502） |
| GET | `/api/schedules/{id}/history` | 最近触发记录 |
| GET | `/api/schedules/preview?cron=&count=` | cron 预览未来 N 次触发（前端表单防抖调用） |

## 5. 前端（触发器页 `/schedules`）

- 「编排」分组新导航项；表格列：名称 / 流程@版本（链到编辑器）/ Cron / 状态 / 下次触发 / 上次触发（入队失败标 ✕）/ 操作（立即触发·历史·暂停恢复·编辑·删除）。
- 新建/编辑对话框：流程过滤选择器、版本下拉（默认最新已发布）、cron 输入 + 防抖的未来 5 次触发预览、入参 JSON（即时校验）。编辑时 flow_id 不可改。
- 页顶提示条：`schedule_service` 未注册时显示「调度服务未运行，触发器不会生效」并链到集群管理。

## 6. 已知边界与后续

1. **引擎流程存储与 console 流程库是两个世界**：console 的流程定义存 SQLite（flow_store），FlowWorker 从引擎 Redis flow 存储解析定义。调度触发的消息要能真正执行，目标流程必须存在于 worker 的存储中（种子方式：`plaita:flow:{flow_id}:{version}` + `plaita:flow_list` / `plaita:flow_versions:{flow_id}` 集合）。这是引擎现有的双存储形态，调度器与手动启动受同一约束；「发布时同步推送引擎存储」是后续合理演进点。
2. `content-daily` 现网定义使用了引擎不识别的节点类型（`flowctx` / `first_non_null` / `summarize` / `notify`），任何入队执行都会被 worker 丢弃（`unRecognized node type`）——与调度无关，需引擎侧补齐节点或修订定义。
3. `delay_service` 等同族服务与调度器存在同样的「启动器不调 start_with_registry，注册 30s 后过期」问题，拓扑上表现为服务时有时无；调度器已内置修复，同族是否跟进由引擎侧定。
4. 多调度服务实例的水平扩容（Redis 锁已防双发，但 next_run_at 竞态回写仍以「先到者」为准）——v1 以 `max_instances: 1` 表达单实例意图。
