# 可视化编排体验专题调研：子图编排 + 属性编辑表单（B3 r105，零产品代码产出）

- **状态**：调研稿（2026-10-27，修复席 B3 第 105 轮）。全文只做现状写实 / 根因分析 / 建议，
  **不含任何产品代码改动**；所有结论均有代码行级证据，建议分批仅供 owner 采纳拍板，
  采纳前不动工
- **调研范围**：`plaita-console/frontend/src`（编辑器全部核心文件实读）+ 引擎侧 schema
  生成面（`plaita/node/*.py` 抽样）+ 治理文档（execution-capabilities.md 的 NS-M3 语境）。
  基线 commit：`fdc4a78b`（origin/main，2026-10-27）
- **方法**：纯静态实读（不凭推测、不跑浏览器）；业界参照为公开文档与社区资料
  （引用见文末），只作决策依据、不构成工作量承诺
- **关联**：#330/#331（执行台阶契约 / NS-M3 作用域闸）、#300（worker 池点名，本文多处
  复用其面板灰置先例）、2026-10 表单评审批次（CORE_FIELDS 白名单 / C5 历史 / KV 表格
  等，文中标注「已修」处均为该批次成果）

---

## 题一：子图编排体验

### 1.1 现状写实：子图相关机制实际有三套

| 机制 | 入口 | 代码位置 | 交互形态 |
|---|---|---|---|
| ① **换层编辑**（graphStack） | 卡片子流程迷你带点击；抽屉「编辑子流程」按钮 | `flowEditor.ts:448` `enterSubgraph` / `:493` `exitSubgraph`；`FlowEditor.tsx:786` 面包屑 | **整画布替换**为子图内容，面包屑「主图 › map · 处理订单 › …」逐层进出；与父图共用同一套 FlowCanvas / NodePalette / NodeConfigDrawer |
| ② **容器原位展开** | 节点卡片右上 `[⊞]` 按钮 | `flowEditor.ts:337` `toggleSubflowExpanded`；`nodeTypes.tsx:221` 展开态渲染 | 在父图坐标原地把节点长成虚线容器，子节点以 `parentId` + `extent:'parent'` 挂进容器（视图态，不进 IR） |
| ③ **子流程迷你带**（只读） | 无（被动展示） | `nodeTypes.tsx:196` `subflowBandChips` | 循环族卡片第三行显示体内节点 chip 序列，点击整条带 → 走机制 ① |

三套机制的事实关系：

- 机制 ① 已经是「子图一个完整的父图一样的编排界面」的**完整事实基础**——换层后
  NodePalette / 属性抽屉 / Copilot / 自动布局全部可用；`exitSubgraph` 负责
  `flowToJson` 写回 `fields.child_flow` 或 `branches[i].flow`，缺 start/end 有常驻
  面包屑警告（`flowEditor.ts:531-538`）。
- 机制 ② 是本次 owner 抱怨「父子图内容直接交叠、看不清楚又混乱」的直接来源；
  且嵌套场景已被禁（容器子节点不渲染 `[⊞]`，`nodeTypes.tsx:353` MC1 裁决），
  它只剩单层价值，而单层正是交叠最刺眼的场景。
- 三个入口的**显眼度排序与体验质量倒挂**：最显眼的 `[⊞]`（卡片右上角常驻）导向
  机制 ②（体验最差）；带点击与抽屉按钮（较隐蔽）导向机制 ①（体验最好）。作者
  第一直觉点 `[⊞]`，撞上的正是最混乱的路径——这是「体验差」印象的放大器。

### 1.2 「父子图交叠混乱」的技术根因（实读代码 + xyflow 官方文档）

按影响从大到小，每条独立成立、叠加后即 owner 描述的现状：

**R1（结构根因）原位展开不重排，容器原地长高。**
`toggleSubflowExpanded`（`flowEditor.ts:364-413`）只做两件事：把子节点 concat 进
全局节点数组、把容器自身 `style` 改为 `{width:280, height:48+内容估高}`。
**兄弟节点一个都不动**——容器向下长出的部分直接压在下游节点/连线上。展开 =
结构性交叠，这是最主要根因。

**R2（渲染规则根因）xyflow v12 对「连到带 parent 节点的边」一律渲染在节点之上。**
官方 Sub Flows 文档明确：默认边渲染在节点下方，*“However, edges connected to a
node with a parent are rendered above nodes”*。容器一展开，子节点全部带
`parentId`，子图连线全部盖在卡片之上——即使没有 R1，子图内部连线也会横穿节点
卡片。这是库的固定行为，方案 ② 在 v12 下无解，只能绕（降透明度/自定义边渲染层）
而非修。

**R3 容器尺寸静态估算、内容不回算。**
容器高 = `48 + Σ nodeHeightFor(估高) + 24 间距`（`flowEditor.ts:383-392`），
估高是常量（普通 60 / 带迷你带 84，`flowLayout.ts:23-27`），与实际渲染高度
（副标题换行、chip 溢出）不保证一致；子节点可在 `extent:'parent'` 内自由拖动
（未设 `draggable:false`），拖动后容器不随内容变化，拖出底缘即被容器
`overflow-hidden`（`nodeTypes.tsx:225`）裁掉——内容可以「消失」。

**R4 子节点带 Handle 视觉噪音。**
`PlaitaNodeComponent` 无条件渲染上/下/右三个 Handle（`nodeTypes.tsx:347-364`），
子节点 `connectable:false` 只是禁止连线，Handle 圆点照常显示——容器内每个子节点
带着「可连线」的假信号，诱导拖线失败。

**R5 容器自身 Handle 随容器长高，父图连线跨越容器。**
容器的出入 Handle 挂在容器边缘（top/bottom/right），展开后 bottom Handle 下移到
容器底部，父图的出边从容器底拉出、贴着/穿过子图内容区。

**R6 双写回路径的认知负担。**
展开态下编辑子节点要走 `syncContainerChildUp` 沿 owner 链逐层镜像写回
（`flowEditor.ts:187-213`，MC1 引入的兜底）；换层编辑则走 `exitSubgraph` 整层
写回。两条写回路径并存正是历史上「丢层」缺陷的温床（MC1/E1 评审均已处理），但
对维护者是持续的复杂度税。

> 命名勘误：代码注释中「方案 A」既指容器原位展开（`flowEditor.ts:65`、
> `nodeTypes.tsx:129`）又在 MC1 语境被称作「方案 Y」（`nodeTypes.tsx:353`），
> 迷你带又叫「方案 B」。本文统一用「机制 ①②③」指代，避免歧义。

### 1.3 兜底方案：子图独立编排界面（M1，建议先做）

**判定：不需要新建「编排界面」——机制 ① 已经是它。** 缺的是三件事：入口优先级
反转、子图层能力补齐、（可选的）空间形态升级。分三层给方案：

**第一层：入口反转（S，小时级，收益最大）**

- 机制 ② 退役或降级：`[⊞]` 从「原位展开（可编辑）」改为「只读预览浮层」（或直接
  移除，保留迷你带作为静态概览）。只读化消解 R2-R5 的全部编辑态矛盾——
  预览态无写回、无拖动、边盖卡片可接受（或叠一层半透明遮罩）。
- 主入口统一为「进入子图编辑」（机制 ①）：双击容器节点（现无双击 handler，
  `FlowCanvas.tsx:107` 只有单击选中）+ 保留迷你带点击 + 抽屉按钮。三入口一个行为。
- 改动面：`nodeTypes.tsx`（按钮行为）、`flowEditor.ts`（toggle 保留但只读化或删除）、
  `FlowCanvas.tsx`（onNodeDoubleClick）。**不触 IR / 引擎 / 序列化**——
  `enterSubgraph`/`exitSubgraph` 与 child_flow 的对接已闭环（归一别名键 →
  `subflowJsonOf` 读 → 空体播种 start/end（含循环族 item/index 注入契约）→
  `flowToJson` 写回 → start/end 校验警告）。

**第二层：子图层能力补齐（M，天级）**

- **undo/redo 在子图层可用**：现状是子图视图内整体禁用（`FlowEditor.tsx:154-156`
  + store `:244` 守卫），原因是历史快照按「编辑层整图」记录、跨层回退会错位
  （E1 评审已把退出写回的历史语义做对：压父图快照而非子图快照）。两条路线：
  a. 快照带 layer 标签，undo 只在「与当前同层」的快照间回退（推荐，改动集中在
  `pushHist`/`undo`/`redo` 三处）；
  b. 维持禁用但把「此处撤销不可用」显式提示（现状是静默置灰，作者不知道为什么）。
- **进入子图自动 fitView**：换层后视口沿用父图的缩放/原点，小体量子图常缩在
  一角。`enterSubgraph` 后调用 `fitView`（需把 `ReactFlow` 实例能力引入或走
  `useReactFlow` 事件）。
- **面包屑携带嵌套语义**（与 §1.5 G1 同事）：当前层是嵌套帧时显示「挂起型节点
  在此层不可用」提示。

**第三层：空间形态（三选一，owner 拍板）**

| 形态 | 说明 | 改动面 | 备注 |
|---|---|---|---|
| A1 现有换层 | 机制 ① 原样作为兜底体验 | 零（第一二层做完即完成） | 上下文连续，但子图与父图共享视口，深层嵌套时面包屑导航是唯一方位感 |
| A2 路由化子图编辑 | `/flows/:id/edit?sub=<nodeId>`（或全屏 modal），URL 可分享/刷新可恢复 | M：路由参数 ↔ graphStack 同步 + `exitSubgraph` 写回时机（离开子路由时写回） | 最接近「给子图一个完整的编排界面」的字面预期 |
| A3 右侧滑出大面板 | 父图压缩占左，子图占右侧 2/3 | M-L：布局重构，两画布实例并存 | 可同屏对照父子，但两份 ReactFlow 实例的状态同步是新复杂度 |

**推荐：M1 = 第一层 + 第二层 + A1**（不引入新形态，先把唯一路径做顺）；
A2 作为 M1.5 可选项。理由：owner 原话「可以先以上面的来做兜底体验」——「上面」
即独立编排面，机制 ① + 入口反转已满足「兜底」定义，A2/A3 是锦上添花。

**里程碑切分**

- **M1（兜底）**：机制 ② 只读化/退役 + 入口统一（双击进子图）+ fitView +
  undo 分层（路线 a）或显式禁用提示（路线 b）。工作量 S-M，零 IR/引擎改动。
- **M2（同画布改进）**：见 §1.4，独立立项，不阻塞 M1。

### 1.4 同画布改进探索（M2+，决策依据，非承诺）

**业界三参照：**

1. **@xyflow/react 官方 subflow（本项目机制 ② 的底层）**：官方定位是「分组/容器
   渲染」——`parentId` 本质是相对坐标关系而非编辑边界（子节点默认可拖出容器，
   `extent:'parent'` 才锁住）；且 v12 明确「带 parent 的边渲染在节点之上」。
   官方示例（subflow / contextual zoom）全部是**展示态**用法，没有任何「嵌套流程
   编辑态」的官方方案。结论：该机制适合只读预览，不适合编辑——机制 ② 转只读
   正是把库用在它该用的地方。
2. **n8n**：子流程 = 独立 workflow，`Execute Workflow` 节点引用；官方 2025 年的
   方向是「选中节点 → Convert to sub-workflow」+ 子流程执行活动日志；in-canvas
   drill-down 是社区多年未落地的 feature request。即：**业界头部产品在子流程上
   选择的正是「独立编辑面」而非同画布嵌套编辑**。
3. **node-RED**：subflow 是独立定义页（双击进入 subflow 画布编辑，父图上只是
   一个节点实例），同样 drill-down 范式。

结论：**drill-down 是主流范式，「同画布编辑嵌套流程」在主流工具里都不做**。
owner 的直觉（兜底用独立面）与业界收敛一致；同画布改进属于差异化探索，值得做
但应按「实验」定位。

**「如果要做」的技术路线与工作量（给决策依据）：**

- **T1 展开/收起 + 局部重排**（M，约 1-2 周）：`@dagrejs/dagre` 已在依赖中且
  `flowLayout.ts:47` 已开 compound graph（`{ compound: true }`）；`autoLayout` 加
  `g.setParent(child, parent)` + 展开时对受影响子树重排、兄弟让位，即可消解 R1。
  风险：与 `smoothstep` 走线的配合、与 `symmetricLayout` 的双轨维护；layout 持久化
  需扩展——`extractLayout`（`flowConverter.ts:248`）只存顶层坐标、子节点坐标
  round-trip 即丢，展开布局要么每次重算（可接受，纵向栈本来就该重排）要么扩
  layout schema（不推荐）。
- **T2 容器随内容动态尺寸**（S）：ResizeObserver / xyflow 节点尺寸回写，消解 R3。
- **T3 contextual zoom（按缩放层级显隐细节）**（M-L）：缩放低于阈值容器折叠为
  单卡、放大才见内部（xyflow 官方示例有现成模式）。这是「同画布合理显示」的
  终态形态，但依赖 T1 先把展开态做对。

**总评**：M2 三件合计 M-L；建议仅在 M1 落地、作者实际用一阵子之后再评估是否
立项——若 M1 的独立面足够顺，T3 的边际价值可能不抵其维护成本。

### 1.5 #330/#331 关联：NS-M3 fail-closed 与子图编排的交互面

**事实**：`SUSPEND_ALLOWED_FRAME_NODE_TYPES = frozenset({"child", "reference"})`
（`plaita/core/capabilities.py:117`）——挂起型节点（approval/hitl/event 等）只允许
出现在 `child`/`reference` 帧；拖进 loop/map/while 体（inline 帧）= 运行期作用域闸
fail-closed（#311 语义，台阶化不改变任何运行期行为；`PLAITA_SUSPEND_GUARD=warn`
只是静态闸的临时回滚旋钮，2026-11-30 到期删除）。

**编辑器现状**：`collectFlowSchemaErrors`（`FlowEditor.tsx:57-74`）只做字段级
schema 校验；子图编辑、保存、发布全链路**没有任何「挂起节点在嵌套帧」的静态
校验**。作者在循环体里放 approval：画布不拦、保存成功、发布闸/运行期才炸——
报错现场远离编辑现场，与引擎 422 报错同为 B5 后半已修问题的结构版。

**三个交互面缺口（G1-G3，G2/G3 建议挂 M1 搭车）：**

- **G1 子图层缺乏嵌套语义呈现**：`GraphFrame` 只有 `title/nodeId/kind`
  （`flowEditor.ts:9-20`），编辑器不知道（也不显示）当前层是嵌套帧、挂起型在此
  禁用。改法：面包屑按 `kind` + 帧类型渲染语义提示（纯展示，S）。
- **G2 节点面板不按帧灰置**：NodePalette 已有按 runtime 池灰置的成熟先例
  （`NodePalette.tsx:50-63`，`poolSupportedNodeTypes` + `unsupportedNodeTypes`）；
  复用同机制——进入 child_flow 层（且宿主为循环族）时，挂起型类型灰置 +
  「仅 child/reference 帧可挂起」角标（M，机制现成）。
- **G3 保存前结构校验**：`collectFlowSchemaErrors` 增加递归遍历（child_flow /
  branches[i].flow 内含挂起型节点且帧类型非 child/reference → 报
  「节点.字段」定位），把引擎 fail-closed 的现场搬到编辑器。前端只做**镜像提示**，
  权威判定仍归引擎闸（不改变 fail-closed 语义，不引入双真相——校验规则抄
  `SUSPEND_ALLOWED_FRAME_NODE_TYPES` 单一事实源，M）。

---

## 题二：属性编辑表单体验

### 2.1 现状写实

**生成机制：schema 驱动为主 + 类型专用 UI 并存，不是硬编码。**

- 数据源：`GET /api/nodes` 下发节点 schema——内置类型由
  `cls.model_json_schema()` 自动生成（`console/backend/services/node_registry_svc.py`
  `_builtin_descriptor`），console 进程内注册的类型（含 plaita-nodes 业务族）同
  机制；自定义类型走 `POST /api/nodes` 登记整份 schema JSON。
- 表单层：`SchemaForm.buildFormPlan`（`schemaUtils.ts:206`）三分区——
  **core**（schema `required` ∪ `CORE_FIELDS` 白名单，`coreFields.ts` 覆盖 37 个
  类型）/ **more**（其余标量，折叠）/ **advanced**（实例有而 schema 未覆盖的键 +
  schema 判 json 的复杂字段，JSON 编辑兜底）。
- 类型专用 UI（`NodeConfigDrawer.tsx` 硬编码接管，键从 SchemaForm 排除）：
  child_flow 子图编辑入口、reference 流程下拉、assignment 的上游依赖行编辑器 +
  输出类型结构化编辑器、条件三段式构造器（`ConditionEditor`，if/loop/while/filter/
  find 五类）、parallel 分支列表、容错策略 `HandlerEditor`（下拉联动字段显隐）、
  基础/容错 Tab 的固定字段。另有 `KVTable`（headers/query 类）、`PropertyField`
  （data_type 槽）、`ScalarArray`、表达式输入（$ 变量菜单）、CodeMirror 代码编辑。
- 退化路径：无 schema 的类型整段 JSON（`FallbackJson`，引擎簿记键隐藏、合法即
  写回）；表单与 JSON 双向不锁死是既有设计原则（「能力不锁死」）。

**覆盖面：不是主要矛盾。** plaita 核心注册表 27 个类型全部自动有 schema
（`model_json_schema` 无一失败路径，个别失败也落 `'{}'` → 仍有 FallbackJson），
业务族同机制。真正「无表单」的只有坏 schema / 未注册类型，属边缘。
**主要矛盾在 schema 元数据质量与交互模型（见 2.2 P2/P3）。**

**校验现状：** 字段级即时校验 `fieldError`（仅 required / type / enum 三约束，
手写实现——评审已确认方言里也只有这三种）；保存/发布前
`collectFlowSchemaErrors` 全图校验、未过即阻断并给「节点.字段」清单；JSON 编辑
「合法即写回 + 非法即时红字」。结构类校验（start/end、分支连线完整性、NS-M3 帧）
只有子图缺 start/end 一项（1.5 节）。

**默认值现状：** 不写默认值进 fields（避免保存膨胀——正确设计）；以
placeholder 形式提示 `schema.default`（仅 ExpressionInput）；boolean 对引擎
`default=true` 字段有专门文案（「不勾选时引擎默认开启」，B2 修复）。

**字段说明现状（关键短板）：** 说明完全依赖引擎 `Field(description=)` / `title=`。
抽样：`node/http.py` 12 字段全覆盖；但**循环族字段以裸注解声明**
（`loop.py:219` `item_type: Optional[Property] = None`，零元数据）→ 表单 label
回退裸键名 `item_type`、无说明。总量：`node/*.py` 46 处 `Field(` 中 37 处带
description，裸注解字段（无 Field）不在统计内且更差。**说明覆盖质量按节点类
参差极大。**

### 2.2 问题清单（逐条严重度；「已修」项标注评审批次，避免重复立项）

| # | 问题 | 证据 | 严重度 |
|---|---|---|---|
| P1 | **折叠区吞噬核心参数的残余**：CORE_FIELDS 白名单机制已建（2026-10 批次），但白名单外类型（如 `extended` 在注册表却无白名单条目）、以及新增节点忘记登记 → 首屏空、核心参数落折叠区。机制已有，缺登记纪律与兜底 | `coreFields.ts` 37 类型 vs 注册表 27+业务族；`extended` 缺席 | 中 |
| P2 | **label=裸键名 + 无说明（「用不明白」第一根因）**：`title` 缺失时 label 回退 schemaKey（蛇形命名的英文键名直出），`description` 缺失则整行无说明。循环族 `item_type`/`collection` 等高频字段裸奔 | `SchemaForm.tsx:268` label 回退链；`schemaUtils.ts:266-269`；`loop.py:219` | **高** |
| P3 | **表达式编辑零校验零提示**：ExpressionInput 是纯 input + $ 变量菜单——`$INPUT.a` 键名拼错、`{% %}` 不配对、`$F.()` 函数名错误全部静默通过，运行期才 None/报错。变量目录只在「点开菜单」时可见，输入过程无任何匹配提示 | `ExpressionInput.tsx` 全文；引擎侧表达式求值无前端对应 | **高** |
| P4 | **JSON 裸奔区仍多**：advanced 区全部复杂字段、parallel「分支详细配置」、condition 组的未知形态，均为无 schema 提示的 CodeMirror JSON。改对了不知道、改错了不知道为什么 | `SchemaForm.tsx:69-88`；`NodeConfigDrawer.tsx:1031-1046` | 中高 |
| P5 | **结构类校验后置**：NS-M3 帧规则（§1.5 G3）、switch 分支漏连线、子图缺 start/end（仅子图层有）、if 无 else 等全不进表单/保存校验或只有局部覆盖 | `FlowEditor.tsx:57-74`（仅字段级） | **高** |
| P6 | **引擎语义不透明**：「输出类型闸不匹配静默返回 None」这类关键语义只存在于 assignment 一处的 hint 文案；Property 槽（data_type+children）在通用字段里只有 dataType 下拉 + 其余 JSON，没有 OutputTypeEditor 那样的结构化编辑 | `NodeConfigDrawer.tsx:431-433`（唯一自述点）；`SchemaForm.tsx:378-408` | 中 |
| P7 | **错误呈现分散且难定位**：字段级红字在控件下方；保存阻断错误在页面顶栏且截断 3 条；「节点.字段」用节点 id 而非名称，大图上找不到现场；无「点击定位」 | `FlowEditor.tsx:366-371`、`:826-828`；`schemaUtils.ts:325` | 中 |
| P8 | **抽屉信息架构**：「名称/描述/output」藏在「基础」Tab（作者找节点改名要点两下）；1094 行单文件持续生长（专用编辑器 6 个内嵌）；「类型特定字段」标题下混排专用 UI 与 SchemaForm 无视觉分层 | `NodeConfigDrawer.tsx:346-484`、文件总行数 | 低中 |
| P9 | **字段级操作缺失**：无单字段「恢复默认」、无字段改动指示（dirty 粒度是全节点）、600ms 合并窗口的撤销步对长表单编辑仍偏粗 | `flowEditor.ts:316-318` | 低 |
| P10 | **条件算子裸枚举**：operator 下拉直出 `eq/gt/contains` 英文 token，无中文语义、无值类型提示（schema 派生算子已通，兜底表与引擎漂移风险已随 Literal 化降低） | `ConditionEditor.tsx:7-9`、`:164-176` | 低 |

**已修确认**（2026-10 评审批次成果，调研中核实到位，无需重开）：Any 字段落
JSON 兜底（P0 已修，`schemaUtils.ts:154-169`）、KV 表格（B5）、 Union 数值∪字符串
按 string 渲染（B2）、required 星标 + 即时校验 + 保存阻断（B5 后半）、抽屉逐键
写回 600ms 合并、Jso nField 合法即写回、节点切换强制重挂载防串写（P1-7）。

### 2.3 优化建议（分批；每条附改动面）

**M1 低垂果实（S，天级）**

1. **批量补引擎字段元数据**（对应 P2）：对循环族等裸注解字段补 `Field(description=)`
   /`title=`——纯引擎注解改动，一次 PR 扫一轮 `node/*.py`；console 侧零改动即生效
   （表单已全量消费 title/description）。（改动面：引擎 S）
2. **保存错误可点击定位**（P7）：顶栏错误清单逐条可点 → `selectedNodeId` +
   打开抽屉（store 通道现成）。（S）
3. **表达式语法 lint 第一档**（P3）：$ 前缀词匹配变量目录、`{% %}` 配对、未知
   `$F.` 函数名黄字提示——纯前端正则级，不求语义正确、只求明显笔误即显。（S-M）
4. **必填未填的 Tab 级徽标**（P1/P7 配套）：buildFormPlan 已有 required 信息，
   Tab 标题加红点计数。（S）
5. **NS-M3 结构校验进保存链**（P5，与 §1.5 G3 同一工单）。（M）
6. **CORE_FIELDS 补登记**（P1）：`extended` 等缺席类型补白名单；给「新增节点
   类型必须登记 CORE_FIELDS」立仓规或 CI 检查（对齐 console node registry）。（S）

**M2 结构性改进（M-L，周级）**

7. **子图编辑唯一化 + undo 分层**（题一 M1 全包，此处不重复）。
8. **Union/anyOf 字段变体切换控件**（P4/P6 部分）：deref 取首个非 null 分支是现状，
   作者看不到「还有别的形态」；先做只读的「本字段接受类型」说明行，再评估交互
   切换。（M）
9. **Property 槽结构化编辑器推广**（P6）：`OutputTypeEditor`（object children 行
   编辑 / array item_type）提炼复用到 inputType / item_type 等所有 Property 槽。
   （M）
10. **JSON 区 schema-aware 升级**（P4）：CodeMirror 挂 json schema lint/autocomplete
    （社区扩展成熟），advanced 区从裸 JSON 变「有提示的 JSON」。（M）
11. **NodeConfigDrawer 分拆**（P8）：按 Tab 拆三文件 + 专用编辑器归子目录；对齐
    flow_worker.py 分拆评估的同一原则——小步、facade 保导入面、不大爆炸。（S-M，
    纯重构不改行为，可与 7-10 任一搭车）

**M3 范式升级（L，决策项，先拍方向再评估）**

12. **表单元数据覆盖层**：引擎 pydantic schema 保持单一事实源，console 侧建
    「元数据覆盖层」（title/desc/分组/示例值/widget 提示按 node_type+field 键
    覆盖）——`POST /api/nodes` 自定义描述与 `property_types` 已是雏形，缺字段级
    粒度与管理界面。收益：字段说明的维护从「改引擎代码」降为「控制台登记」；
    代价：多一层真相需治理（引擎改名后覆盖层孤儿检测）。参照：n8n 的参数面板
    即「引擎能力 + 展示层元数据」双层。（L）
13. **节点配置主视图化**：抽屉 → 右侧全高面板 / 弹窗（参照 Retool query editor、
    n8n details view），代码/长表达式字段获得全宽编辑；1094 行分拆（11）是前置。
    （M-L）
14. **字段级 AI 助手**：CopilotPanel 管线已有，注入「当前节点 + 当前字段 schema +
    上游输出示例」做「解释此字段 / 生成值」。（M）

---

## 附 A：实读文件清单

| 文件 | 行数 | 结论用途 |
|---|---|---|
| `frontend/src/stores/flowEditor.ts` | 581 | 机制①②全貌、写回链、undo/redo 守卫 |
| `frontend/src/components/flow/FlowCanvas.tsx` | 125 | 画布装配（无双击 handler、拖放入口） |
| `frontend/src/pages/FlowEditor.tsx` | 1005 | 面包屑、collapseToRoot、保存/发布校验链 |
| `frontend/src/components/flow/nodeTypes.tsx` | 372 | `[⊞]`/迷你带/容器渲染/Handle |
| `frontend/src/components/flow/flowConverter.ts` | 255 | child_flow 序列化 round-trip、layout 提取 |
| `frontend/src/components/flow/NodeConfigDrawer.tsx` | 1094 | 表单装配、专用编辑器全量 |
| `frontend/src/components/flow/schemaForm/`（7 文件） | 1360 | SchemaForm/schemaUtils/ExpressionInput/CodeEditor/ConditionEditor/coreFields/SchemaInput |
| `frontend/src/components/flow/NodePalette.tsx` | 291 | 面板灰置先例（G2 复用点） |
| `frontend/src/components/flow/flowLayout.ts` | 85 | 估高常量、dagre compound |
| `frontend/src/components/flow/AiGenerateDialog.tsx` | 160 | AI 生成导入链（覆盖确认已有） |
| `plaita/node/*.py`（抽样：http/loop 等） | — | Field 元数据质量、SUSPEND_ALLOWED_FRAME_NODE_TYPES |
| `console/backend/services/node_registry_svc.py` | — | schema 生成机制、覆盖面判定 |

## 附 B：业界参照来源

- React Flow (xyflow) v12 官方文档 *Sub Flows*
  （reactflow.dev/learn/layouting/sub-flows）：parentId 语义、`extent:'parent'`、
  「带 parent 的边渲染在节点之上」、subflow/contextual zoom 官方示例均为展示态
- n8n：Execute Workflow 子流程模型、官方「Convert to sub-workflow」、社区
  in-canvas drill-down 长期 feature request（community.n8n.io 多帖，2023-2025）
- node-RED：subflow 独立定义页 / drill-down 范式（官方文档）
- Retool：query editor 侧栏配置面板范式（对照 P8/P13）

## 附 C：提请 owner 拍板清单

| # | 决策点 | 推荐 |
|---|---|---|
| D1 | 机制②（容器原位展开）退役还是只读化？ | 只读预览化（保留概览价值，消解编辑态矛盾） |
| D2 | 兜底形态 A1/A2/A3（§1.3 第三层） | A1（M1 现有换层 + 入口反转即达成），A2 观察后再议 |
| D3 | NS-M3 前端镜像校验（G2/G3）是否随 M1 立项 | 是（机制现成，报错现场前置收益大） |
| D4 | 表单元数据覆盖层（M3-12）方向 | 方向认可即可，立项另议（依赖 M1/M2 验收后的精力） |
| D5 | 引擎字段元数据批量补齐（M1-1） | 是（纯注解 PR，全表单受益，性价比最高单点） |
