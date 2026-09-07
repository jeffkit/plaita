# Console UX 重设计提案：节点管理 / 导航信息架构 / 试跑面板

> 状态：**已定案（2026-09-06 评审通过，按实施切分落地）**
> 范围：`plaita-console/frontend` + `plaita-console/backend` 配套改动
> 视觉规范遵循 `plaita-console/DESIGN.md`（token 先行，禁止一次性 hex）
> 证据引用格式 `file:line` 均为 plaita 仓内相对路径

---

## 1. 问题梳理

### 1.1 节点管理与注册

| # | 现状 | 证据 | 根因 |
|---|------|------|------|
| a | 节点清单是行卡片堆叠，不是表格；无搜索/筛选，30+ 内置节点只能肉眼扫 | `frontend/src/pages/Nodes.tsx:100-129` | 页面未消费 `components/ui/Table.tsx` 基元 |
| b | 注册表单常驻页面左栏；注册后不可编辑，只能删除重建 | `Nodes.tsx:49-90`；操作区仅 delete（`Nodes.tsx:114-124`） | 后端 `POST /api/nodes` 本就是 **upsert**（`node_registry_svc.register_custom` → `upsert_node_descriptor`），编辑能力是前端没接，不是后端缺失 |
| c | schema 只给裸 JSON textarea；用户不知道要配哪些字段、各字段类型与默认值 | `Nodes.tsx:72-78` | 流程编辑器的 `SchemaForm`（core/more/advanced 分区、类型化控件、default 占位）已存在但未复用到节点管理 |
| d | 注册只有 node_type/node_name/category/schema_json，没有代码位置 | `backend/api/nodes.py:39-46` | **这不是 UI 遗漏，是数据模型事实**：控制台注册的"自定义节点"只是编排表单元数据，不注册可执行类（`backend/services/node_registry_svc.py:9-12` 明示 dry-run 会报"未注册节点类型"）；而内置节点的 module/class 信息后端也没透出（`_builtin_descriptor` 未读 `cls.__module__`） |

### 1.2 菜单结构与分类

现状分组（`frontend/src/App.tsx:56-90`）：

- 总览：仪表盘
- **编排 · 定义**：流程编排、触发器、节点管理、**凭据、用户、审计**
- 运行 · 观测：执行实例、事件管理、日志查看、任务队列
- 集群 · 运维：服务拓扑、集群管理

逐项评估：

- **凭据**：它是流程定义的引用目标（连接器节点 `credential` 字段按名引用），但生命周期与流程完全独立——由管理员供给、可轮换而不动流程；页面本身 admin-only（`App.tsx:54` `ADMIN_ONLY_PATHS`），编辑者只引用不创建。放「编排·定义」语义错位：**应归平台管理域**。真正的问题在创建体验：`type` 是自由文本标签、`data` 是裸 JSON（`frontend/src/pages/Credentials.tsx:159-172`），应模板化。
- **审计**：内容是管理面敏感操作留痕（时间/操作人/动作/对象/IP 的只读事件流，`frontend/src/pages/Audit.tsx`），消费方式与日志查看同构——**同意迁入「运行·观测」**。admin-only 是权限问题、与域归属正交，`ADMIN_ONLY_PATHS` 已解耦，迁组不改变可见性。若未来审计长出留存策略等配置能力，那部分再归管理域。
- **用户**：账号与角色治理，与"编排定义"零关系，放在此纯属堆积，**迁管理域**。

### 1.3 试跑面板

| # | 现状 | 证据 | 根因 |
|---|------|------|------|
| 1 | 子图节点与主流程节点平铺对齐，无层级 | `DryRunPanel.tsx:160-232` | 后端采集回调只记 `id/type/name/input/output/status/error`，无深度/路径字段（`backend/services/dryrun.py:32-68`） |
| 2 | 面板固定 `w-96`，不可调宽 | `DryRunPanel.tsx:66` | 无宽度状态与拖拽热区 |
| 3 | 输入只能裸 JSON | `DryRunPanel.tsx:72-78` | 流程 `input_type`（Property 结构，`plaita/core/flow.py:33`）已随编辑器状态存在（`FlowEditor.tsx:84` `inputType`），但没有 Property→JSON Schema 的转换与表单渲染接入 |

另有两个**连带事实**，设计必须显式处理：

- `apply_debug_transform` 只变换顶层节点，pin / 仅运行此节点对子图内节点天然失效（`dryrun.py:72-100`）——层级展示上线后用户会点子图节点的 pin，必须给明确反馈而不是静默无效。
- DryRunPanel 大量 `dark-*`/`red-400` 旧类（DESIGN.md 定稿前的遗留），重构时应一并迁移 token。

---

## 2. 设计方案

### 2.1 导航信息架构（先定骨架）

```
总览          仪表盘
编排 · 定义   流程编排 / 触发器 / 节点管理
运行 · 观测   执行实例 / 事件管理 / 日志查看 / 任务队列 / 服务拓扑 / 审计*
平台 · 管理   凭据* / 用户* / 集群管理*        （* = admin-only）
```

- 「集群 · 运维」更名「平台 · 管理」，收编凭据、用户；admin-only 过滤逻辑沿用 `ADMIN_ONLY_PATHS`（把 `/audit` 保持在集合里即可）。**【定案：方案 A】**
- 服务拓扑本质是运行态服务健康观测，归「运行·观测」。
- 配套闭环：NodeConfigDrawer 中 `credential` 类字段做**下拉选择器**（数据源 `GET /api/credentials` 仅取 name，列表接口本就不含机密），替代手输名称。
- **凭据类型模板（已定案：后端承载）**：模板清单不放前端常量——前端不承载业务语义。后端新增模板注册表（服务模块内置：bearer token / basic auth / api key header / database / webhook secret / generic，含字段定义 `[{key, label, input_type, required, secret}]`），经 `GET /api/credential-templates` 下发；新建/编辑凭据时先选模板，按模板渲染类型化字段表单，序列化回现有 `data` JSON 载荷，存储格式零变更；「自定义 (JSON)」保留为兜底。模板仅后端代码内置（只读），存储化自定义模板列后续扩展。

### 2.2 节点管理页（`/nodes`）

#### 2.2.1 列表 → 纯表格

消费 `components/ui/Table.tsx` 基元，sticky 表头 + 容器内滚动（内置节点 30+，无需分页）：

| 列 | 声道/样式 | 说明 |
|---|---|---|
| 节点类型 | mono 主列 | `n.node_type` |
| 展示名 | body | 空值回落 `—` |
| 分类 | caption | |
| 来源 | badge | 内置 = 中性实底；自定义 = 中性描边（绿留给"活着/可操作"，不用于来源标记） |
| 字段数 | mono 右对齐 `tabular-nums` | 前端 parse `schema_json.properties` 计数 |
| 代码位置 | mono `data-sm` | 内置：`plaita.node.nodes.http.HttpNode`；自定义：`console://metadata` |
| 操作 | — | 编辑（全部可点；内置只读查看）/ 删除（仅自定义，走现有 ConfirmDialog） |

工具栏：关键字搜索（type/name 模糊）+ 分类下拉 + 来源下拉 + 右侧「注册节点」主按钮。

#### 2.2.2 注册/编辑 → 右侧抽屉【定案】

**抽屉**（~560px）而非弹窗或子路由：与流程编辑器 `NodeConfigDrawer` 同一交互范式、字段编辑器需要横向空间、保持列表上下文；子路由把轻量 CRUD 拔高成页面，不值。

抽屉分区：

1. **基本信息**：`node_type`（编辑态锁死）、展示名、分类（datalist：已有分类 ∪ 自由输入）。
2. **来源与代码位置**（只读区，回应 1.1-d）：
   - 内置：后端 descriptor 新增 `source_module` / `source_class`（`_builtin_descriptor` 读 `cls.__module__` / `cls.__qualname__`），展示为可复制 mono 文本。
   - 自定义：明示「控制台元数据——仅驱动编排表单；试跑/执行需运行时存在同 node_type 可执行节点，否则报未注册」。把 1.1-d 的真相摆到 UI 上，而不是假装有个代码位置。
3. **参数 schema**：Tabs「字段列表 ⇄ JSON 源码」，双向同步；JSON 非法时禁用字段列表 tab 并标红。

**字段列表**（回应 1.1-c）：

- 每行：`key`（mono）/ `label`(title) / 类型下拉 / 必填 checkbox / 默认值 / 描述 / 删除；底部「添加字段」。
- **公共基础字段自动排除**：`id / name / desc / output / next / timeout / source_line / timeout_handler / error_handler`（Node 基类实例字段，`plaita/node/basic.py:85-95`）。打开抽屉时滤掉只渲染业务字段，保存时原样保留——用户看到的正是"除公共属性外需要配置的东西"。
- 默认值控件按类型渲染：string→text、number/integer→number、boolean→三态 select（未设置/true/false）、enum→选项下拉、object/array→mini JSON。写入 JSON Schema `default`，与流程编辑器 `SchemaForm` 的 default 占位展示天然衔接。
- 类型下拉分组：
  - 基础类型：string / number / integer / boolean / array / object / enum
  - **自定义类型**（见 2.2.3）：用户注册的命名类型
  - array 选中出 item 类型二级选择；object 提示「嵌套结构请切 JSON 视图编辑」（一期不做嵌套表单，JSON 兜底，与 SchemaForm 哲学一致）
- 顺带校正：前端 `SchemaForm` 的 `DATA_TYPES` 现含 `map / datetime`，而运行时 `Property.match` 只认 string/integer/float/bool/number/array/object/any，未知类型一律 False（`plaita/io.py:260-291`）——列表收敛为运行时认可的集合，datetime 以 string+format 提示表达。

#### 2.2.3 自定义类型注册（回应 1.1-c 的"支持注册其他类型"）

- 入口：节点管理页次级 Tab「节点 | 自定义类型」。
- 类型 = **命名别名**：`{ name, base_type(内置), enum 选项?, default?, desc }`。
- 存储与 API：仿 `node_descriptor` 模式（`flow_store.py:300-403`）新增 property-type 表 + CRUD，暴露 `/api/property-types`。
- **关键约束——别名在保存时展开**：运行时 `Property.match` 不认识任何自定义类型名，所以自定义类型只存在于录入侧；生成节点 `schema_json` 时展开为基础类型 + 约束（enum/default 落进 JSON Schema），**运行时永不接触未知类型名**。不做"运行时可插拔类型匹配器"，那会穿透 plaita 内核，超出 console 改造边界。
- 复用点：节点抽屉类型下拉（本期）、流程 inputType 编辑器与凭据模板（后续）。

### 2.3 试跑面板（DryRunPanel 重构）

#### 2.3.1 子图层级缩进

- **后端**：`_CollectingCallback` 维护 flow 栈——`on_flow_start` push、`on_flow_end` pop（子执行继承 handler，`plaita/core/executor.py:274-279` 已保证）；每个节点 entry 增加 `depth: int`、`flow_path: string[]`（自顶向下 flow 名）、`flow_id`。并行分支按事件序进出栈，depth 天然正确，同层多子图靠 `flow_path` 区分。
- **前端**：按 `depth` 缩进（16px/级）+ 左竖参考线；子流程渲染为可折叠分组头（▸ flow 名 + 聚合状态）；节点行样式沿用现有 error/mock 语义并迁移 DESIGN.md token。
- **嵌套节点交互降级**：depth>0 的节点，pin 与「仅运行此节点」禁用 + tooltip「子流程内节点暂不支持固定/单跑（调试变换仅作用于顶层）」。诚实告知优于静默无效；`apply_debug_transform` 递归化列为后续独立改造。

#### 2.3.2 拖拽调宽

- 左缘 6px 拖拽热区（`cursor-col-resize`），pointer events 实现，无外部依赖。
- 约束：min 320px、max 容器 60%；默认仍 384px；持久化 localStorage（`plaita-dryrun-width`）。

#### 2.3.3 表单化输入（JSON Schema Form）

- 新共享组件 `components/flow/schemaForm/SchemaInput.tsx`：`{ schema, value, onChange }`，Tabs「表单 | JSON」，**JSON 为 source of truth**；表单改动即时序列化回 JSON；JSON 非法 → 锁表单 tab + 错误提示。
- 表单态复用 `SchemaForm`：试跑输入是具体值而非表达式，变量组传空（ExpressionInput 退化为普通输入）。
- 新增 `propertyToJsonSchema()` 转换器（落 `schemaForm/schemaUtils.ts`）：`dataType→type`、`children→properties`、`is_required→required`、`default_value→default`、`label/desc→title/description`。
- 接入点：
  - DryRunPanel：`inputType` 已在 FlowEditor 状态里（`FlowEditor.tsx:84`），prop 直传；`inputType` 无 properties → 表单 tab 显示「流程未定义入参 schema」，默认落 JSON tab。
  - StartFlowDialog（裸 JSON 同款问题，`StartFlowDialog.tsx:254-263`）同组件接入。**数据链路已确认存在，无需后端改动**：`GET /flows/{flow_id}/versions/{version}` 返回完整 `definition` 字符串（`api/flows.py:217`），其中含 flowConverter 保存的 `inputType` 顶层键（`flowConverter.ts:95`）；StartFlowDialog 现只调 `getFlow`（版本摘要不含 definition，`api/flows.py:183-198`），选中流程+版本后补一次 `api.getFlowVersion` 拉取解析即可——纯前端改动。
- 面板整体 token 迁移：`dark-*` → `ink-*/surface/line/status-*`，输出 JSON 用 `data-sm` mono。

---

## 3. 后端配套改动清单

| 改动 | 位置 | 说明 |
|---|---|---|
| dry-run 层级字段 | `services/dryrun.py` `_CollectingCallback` | flow 栈 + `depth`/`flow_path`/`flow_id`；补单测（嵌套 child / parallel 分支） |
| descriptor 代码位置 | `services/node_registry_svc.py` `_builtin_descriptor`、`api/nodes.py` view | 增 `source_module` / `source_class`（内置 only） |
| property-types CRUD | `flow_store` 新表 + 新 `api/property_types.py` | 仿 node_descriptor 模式 |
| credential-templates | 新 `api/credential_templates.py`（或并入 `api/credentials.py`） | `GET /api/credential-templates`，后端服务模块内置模板注册表（只读），载荷格式不变 |

## 4. 实施切分（一 surface 一 commit，改前截图留档 `docs/design-shots/`）

1. ✅ backend(dryrun)：层级栈 + 字段 + 单测
2. ✅ backend(nodes)：内置节点 descriptor 透出 `source_module`/`source_class`（自定义节点为空，前端展示 console://metadata）；property-types CRUD（models 新表 + flow_store 方法 + `api/property_types.py`，base_type 收敛到运行时内置集合，含单测）
3. ✅ backend(credentials)：`api/credential_templates.py` 只读接口——后端内置模板注册表（bearer/basic_auth/api_key_header/database/webhook_secret，字段带 secret 标记），凭据存储载荷格式不变
4. ✅ frontend(schemaForm)：`propertyToJsonSchema` + `SchemaInput` 共享组件（表单⇄JSON 双 tab，JSON 为准）
5. ✅ frontend(DryRunPanel)：层级树（分组头可折叠/计数/错误聚合/嵌套节点顶层限定降级）+ 左缘拖宽（320px–60vw，localStorage 持久化）；分组纯函数抽至 `timeline.ts` 并过六场景行为检查
6. ✅ frontend(DryRunPanel)：SchemaInput 表单输入接入 + DESIGN.md token 迁移（dark-* 遗留清理）
7. ✅ frontend(Nodes)：列表纯表格（类型/展示名/分类/来源/字段数/代码位置/操作 + 搜索/分类/来源筛选）；注册/编辑右侧抽屉（基本信息 + 来源与代码位置只读区 + 字段列表⇄JSON 双视图——公共基础字段自动排除、自定义类型经注册表展开、默认值按类型渲染、required 列表重建）；「自定义类型」次级 Tab（CRUD 表格 + 表单）
8. ✅ frontend(App/Credentials)：✅ 菜单重组（方案 A）；✅ 凭据模板表单（类型下拉 → 模板字段渲染（secret 掩码/number 类型/必填校验），模板外历史字段保留，「自定义 JSON」兜底，历史未知类型整体落自定义模式）。credential 下拉**阻塞于引擎侧**：内置节点 schema 尚无 credential 字段（见 §6 对照表引擎改动），字段落地后接入
9. ✅ frontend(StartFlowDialog)：SchemaInput 接入——选中流程+版本后经 `api.getVersion` 拉定义取 inputType（无后端改动），输入参数升级为表单⇄JSON 双态
10. 🔶 验收：起独立后端（全新库走 setup 向导）+ dev server 实测通过——节点表格/注册抽屉/http 查看抽屉（含 method 枚举下拉与代码位置）/自定义类型注册闭环（order_status → 节点抽屉类型下拉出现）/凭据模板表单/试跑面板表单|JSON 双 tab，截图存 `docs/design-shots/*-after.png`；**验收发现并修复真 bug**：buildFormPlan 的 COMMON/CONNECT/INTERNAL 排除清单会吞掉流程入参中与 Node 基类同名的字段（inputType.name 消失），已加 `includeCommonKeys` 模式（SchemaInput 传 true）并回归。遗留：试跑面板内点击「开始试跑」在 IAB 自动化环境点击事件丢失（非产品问题，面板逻辑由 11 个后端单测 + timeline 六断言覆盖），建议人工打开一次确认层级时间线视觉

## 5. 决议与开放问题

**已决议（2026-09-06 评审）：**

1. ~~StartFlowDialog 数据链路是否已含 inputType~~ → **已确认存在**：`GET /flows/{id}/versions/{version}` 的 `definition` 含 `inputType`（`api/flows.py:217`、`flowConverter.ts:95`），补一次版本详情拉取即可，无后端改动。
2. ~~凭据模板清单放前端常量还是后端~~ → **定案：后端承载**。前端不承载业务语义；后端内置模板注册表 + 只读接口下发，存储载荷格式不变。

**仍开放：**

3. ~~pin / only-node 的子图递归化~~ → **本期定案：降级**。嵌套节点 UI 禁用这两个操作并 tooltip 说明；`apply_debug_transform` 递归化后续单独立项（见 §5-3 保留为内核改造待办）。
4. ~~`map/datetime` 类型口径~~ → **已查明（2026-09-06）**：内核存在两条校验路径——`types.valid()` 经 `native_types` 认识 map(→dict)/datetime(→datetime 实例)（`plaita/core/types.py:43-46`），而 `Property.match()` 无此二者分支、未知类型一律 False（`plaita/io.py:260-291`）；node/dsl 里的 "map" 命中均为循环节点类型，与数据类型无关。**Console 侧定案**：节点抽屉类型清单以 `Property.match` 的保守集合为准——map 在 UI 中视作 object 的别名展示，datetime 以 string+format 提示表达，不透传裸类型名。内核两条路径不一致另立 plaita 内核 issue，不阻塞本设计。

---

## 6. 节点配置表单专项（2026-09 四视角评审定案）

四路独立评审（控件层审计 4/10 分 / 六任务认知走查 / n8n·Dify·Langflow·Airflow 竞品基线 / 引擎 schema 元数据量化审计）一致结论：**当前节点配置表单对不懂引擎源码的用户不可用**——六个典型配置任务仅 reference 可独立完成；对照表结论被证实且更严重，新增三个数据正确性缺陷：JsonField 跨节点串写（切换节点不重挂载，旧 headers 可写进新节点）、ConditionGroup 编辑即污染（存量组条件被改出混合对象）、布尔控件对 default=true 字段显示错误且功能上关不掉。关键量化：124 个业务字段 **0 个 enum**（下拉控件是死代码）、38% 无描述、27% 退化进高级 JSON 区；17 个语义枚举字段自由文本，其中 5 处非法值**静默错行为**（delay_unit 拼错按秒算、auth_type 未知值产出无凭证配置等）；4 个节点的 `event_type` 是必填但被引擎静默覆盖的伪用户字段。竞品对照的关键标定：Airflow 用**同一套 JSON Schema 表单架构**，但 schema 声明了 enum 并在提交时校验——差距在元数据与消费，不在架构。

### 三线实施切分（2026-09-07 起按线推进）

**A 线 · 引擎元数据**（前端零改动或小改动即受益）：

| # | 项 | 状态 |
|---|---|---|
| A1 | 17 个语义枚举字段 Literal 化（http.method、end.result_type、parallel.mode、delay_unit、approval_type/strategy、callback_method、auth_type、kafka security_protocol/sasl_mechanism/auto_offset_reset/message_format、redis queue_type/message_format、Condition.operator、ConditionGroup.relation），配套 before-validator 大小写归一保存量 | ✅ 已完成（kafka message_format 收敛为 json/text——avro 服务端从未实现，存量配置由静默错行为改为解析期明确报错） |
| A2 | Node 基类 output/next/timeout/timeout_handler/error_handler 补中文 description（×23 schema 放大） | ✅ 已完成 |
| A3 | `event_type` 伪必填修复：4 个扩展节点（approval/http_callback/kafka_queue/redis_queue）删 `__init__` 覆盖，改 before-validator 注入固定订阅契约 + 子类重声明默认值——schema 不再 required、用户乱填仍被强制归约（订阅/发布两侧契约不破）；`event` 节点 event_type 补「与发布方 type 匹配、支持 $ 表达式」描述 | ✅ 已完成 |
| A4 | HTTP 的 5 个 camelCase alias 键归一 snake_case（LEGACY_KEYS 模式） | 待办 |
| A5 | B 类 Union 具体化（http.headers/query→Dict[str,str]、case.cases 建模、assignment.upstream_output 建模、end.error 建模） | 待办 |
| A6 | `json_schema_extra` 元数据协议：`x-widget / x-hidden / x-ui-order`；前端 buildFormPlan 小改适配后，三张硬编码清单（COMMON/CONNECT/INTERNAL + coreFields）退役 | 待办 |

**B 线 · 表单控件**（前端）：

| # | 项 | 状态 |
|---|---|---|
| B1 | `key={node.id}` 重挂载修 JsonField 跨节点串写 | ✅ 已完成 |
| B2 | anyOf 变体感知：保留全部非 null 分支，含 string 分支的 Union 渲染表达式输入（堵住 number 控件敲字符删必填键）；`anyOf[{},null]` 字段给 `{}` 初值与 title/desc 渲染 | 待办 |
| B2 | anyOf 变体感知 + 高级区元数据 + 布尔修正：含 string 分支的数值 Union 按 string 渲染（堵住 number 控件敲 `$` 删必填键，如 delay_seconds）；高级字段区渲染 schema title/desc（http.headers 等不再只露裸键名）、未设置时初始 `{}`；引擎 default=true 的布尔字段取消勾选显式写 false、文案标注「不勾选时引擎默认开启」 | ✅ 已完成 |
| B3 | ~~前端 `ENUM_HINTS` 兜底表~~ → **取消**：console 与引擎同仓同发，A1 落地后所有语义枚举已由 schema enum 驱动，前端硬编码枚举清单反成第二数据源（正是 coreFields.ts 漂移的教训），不再引入 | ✅ 已定案取消 |
| B4 | 条件三段式构造器（field 表达式 + operator 下拉 + value，and/or 组合），if/switch/case/loop/filter/find/while 共用；boolean default=true 显示「开启（默认）」且取消写 false | ✅ 一期已完成（`ConditionEditor`：if/loop/while/filter/find 五类顶层 condition 接入，支持 AND/OR 组合与单条件互转、整体替换写回顺带修掉组条件编辑污染；operator 清单由 schema enum 派生（A1）+ 兜底表；真/假分支锚点标注与 switch/case 分支内条件构造列二期） |
| B5 | 键值对表格编辑器（headers/query/validate_* 共用）；保存前同一份 JSON Schema 校验并定位字段 | ✅ 已完成（前半：`KVTable`，headers/query 升格 http 首屏、只产出 string 值；后半：`fieldError/validateNodeFields` 手写校验 required/type/enum/kv——抽屉内即时红字定位字段，FlowEditor 保存与发布前做流程级校验、未通过即阻断并列「节点.字段 问题」。**ajv 明确不引入**：23 个 schema 的约束方言仅 required/type/enum，完整 JSON Schema 校验器是零收益体积，方言扩展时再评估） |
| B6 | 引用选择器：reference.flow_id 流程下拉；对象数组表格（upstream_output/cases/form_fields） | ✅ 一期已完成（reference → 已发布流程下拉（含「列表中不存在」保值项）；assignment → 上游依赖行编辑器（画布节点下拉 + 取值表达式输入），白名单漂移键 `assignments` 清理、`delay` 白名单修正为 delay_seconds/delay_unit 成对置顶、calculate 死条目删除。case.cases/form_fields 数组编辑依赖 A5 的 schema 建模，列二期） |

**B 线收口（2026-09-07）**：B1-B6 全部落地（含 B3 定案取消）；遗留二期项——switch/case 分支内条件构造、true/false 锚点标注、case.cases/form_fields 数组编辑（等 A5）、保存校验与后端 422 文案的映射优化。

### 用户实测反馈修复（2026-09-07，hello-echo 试跑实测）

| # | 反馈 | 修复 |
|---|---|---|
| F1 | 试跑执行失败（如 http 依赖缺失 NodeException）时，画布上没有标识哪个节点出错 | DryRunPanel 新增 `onRunStatus` 回调：试跑开始清除上轮标记、结束后把 status=error 的节点回写画布（store 新增 `setRunErrorNodes`，不置 dirty，复用 nodeTypes 已有的 error 红色样式）；执行级错误文案（"执行节点 X 出错了"）解析出节点 id，面板内给「出错节点」定位按钮 |
| F2 | 赋值节点「声明上游依赖」列出了全部节点，应只能选当前节点之前的上游 | 抽出 `upstreamIds`（沿入边反向遍历）memo，与变量目录共用；上游下拉只列真上游，无上游时显示空态提示「先用连线接入前置节点」 |
| F3 | 取值表达式是什么意义、怎么写？ | 按引擎语义（assignment.py：单条=直接求值；多条=按实际执行到的上游匹配，分支汇聚）在编辑器内补示例文案：`$NODE.http1.data` / `$INPUT.name` / 字面量，及下游引用方式 |
| F4 | 输出类型怎么写？dict/array 应能定义结构 | 新增 `OutputTypeEditor`：object → children 字段行（名+标量类型，可增删）、array → 元素类型下拉、标量直接选、未设置=不校验；说明「不匹配时引擎静默返回 None」。更深层嵌套走源码编辑 |

> F1 的 http 报错本身是后端环境缺依赖：`pip install "plaita[http]"` 安装 aiohttp 后 http 节点可执行。

**C 线 · 已定案页面/面板**：即 §4 原 1-10 步（dry-run 层级 ✅ / nodes API 代码位置 / property-types / credential-templates / SchemaInput / 试跑面板 / Nodes 页 / 菜单凭据 / StartFlowDialog / 验收截图）。

> 竞品对照给出的一致门槛判断：A1+A2（已完成）+ B2/B3 + B4/B5 做完，即可跨过「业务用户能独立配对一个带分支的 HTTP 流程」的门槛；credential 下拉与变量选择器升级（B6 之后）决定与 n8n/Dify 同档对话的能力。
