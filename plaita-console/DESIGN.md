# Plaita Console — Design System

> 本文件是 plaita-console 视觉的唯一权威（single source of truth）。
> 配套 skill：`.zcode/skills/plaita-console-design`。改动流程与验收规则见该 skill。
> 定案方向：**混合式排版 × 精密仪器风（Operate mode）**。2026-08-29 v1.0。

---

## 0. 设计宣言

**「精密仪器」**——plaita-console 是工程师每天盯 eight 小时的操作台，不是营销页。
它的高级感来自：**冷静的分层灰阶、受控的品牌绿、数据用等宽字排印、毫秒级的动效纪律**。
参考气质：Linear 的克制、Vercel 的黑白分明、Grafana 的数据密度——但没有一者的花哨。

三条铁律：

1. **绿是仪器指示灯，不是油漆。** 品牌绿只出现在「活着/可操作」的地方：running 状态、主按钮、激活导航、焦点环。装饰性用绿（渐变字、发光、大面积底色）一律禁止。
2. **排版即层级。** 不靠颜色和粗细堆层级，靠字号阶梯 + 字距 + 无衬线/等宽的双声道切换。
3. **密度优先于气派。** 正文 13px、行高 1.5、间距 4 的倍数。留白是节奏，不是空旷。

---

## 1. 混合式排版（本方案的灵魂）

双声道：**UI 说话用 Inter，数据说话用 JetBrains Mono。**

| 声道 | 字体 | 用于 |
|---|---|---|
| UI声道 | Inter Variable（自托管 @fontsource-variable/inter），中文回落 PingFang SC / Microsoft YaHei | 导航、标题、按钮、表单、正文、状态徽章文字 |
| 数据声道 | JetBrains Mono（自托管 @fontsource/jetbrains-mono） | 执行 ID、Flow ID、日志、时间戳、指标数字、代码、表格 ID 列、画布节点 label 中的技术名 |

数据声道一律加 `tabular-nums`（等宽数字），表格数值列右对齐。

字号阶梯（Tailwind token，`text-*` 直接可用）：

| Token | 值 | 用途 |
|---|---|---|
| `page-title` | 20px/28 semibold tracking-tight | 页标题。**禁止 3xl 以上大标题** |
| `section` | 14px/20 semibold | 卡片标题、分区标题 |
| `body` | 13px/20 | 正文主力字号（工具密度） |
| `caption` | 12px/16 | 辅助说明、表格次要列 |
| `micro` | 11px/14 medium uppercase tracking-wider | eyebrow 标签、表头、分组标签 |
| `data` | 13px/20 JetBrains Mono | 数据声道主力 |
| `data-sm` | 12px/16 JetBrains Mono | 日志、密集表格 |

## 2. 色彩 Token

### 2.1 背景四层（纯色，禁止渐变）

| Token | 值 | 用途 |
|---|---|---|
| `canvas` | `#0B0D12` | 页面底色（body） |
| `surface` | `#10131A` | 卡片、面板、侧边栏 |
| `elevated` | `#151A23` | 浮层：下拉、抽屉、Dialog、Toast |
| `inset` | `#0D1016` | 下沉区：日志窗、代码块、画布底、输入框底 |

层级规则：卡片浮在 canvas 上靠「背景色差 + 1px 白透明描边 + 极浅阴影」，三者缺一即显平，全堆则显脏。默认组合：`bg-surface border-line shadow-card`。

### 2.2 文字四档

| Token | 值 | 用途 |
|---|---|---|
| `primary` | `#E8EBF0` | 标题、关键数值 |
| `secondary` | `#A8B0BF` | 正文、可读辅助 |
| `muted` | `#6B7484` | 说明、占位、禁用 |
| `faint` | `#454D5C` | 装饰性符号、极弱信息 |

### 2.3 描边（白透明，不是灰线）

| Token | 值 |
|---|---|
| `line` (default) | `rgba(255,255,255,0.07)` |
| `line-strong` | `rgba(255,255,255,0.12)`（hover、浮层描边、分隔强调） |

### 2.4 品牌绿（沿用现有 `plaita.*` 色阶，用途收敛）

允许：主按钮底、激活导航文字/图标、running 状态、焦点环（focus-visible ring）、Logo 指示点、链接 hover。
禁止：渐变文字、渐变背景、box-shadow glow 装饰、非交互元素的大面积底色。
交互主色定在 `plaita-400 #4ade80`（文字/图标）与 `plaita-500/15`（激活底，15% 透明）。

### 2.5 语义状态色

| Token | 值 | badge 底 | 语义 |
|---|---|---|---|
| `status-running` | `#4ade80` | `rgba(74,222,128,0.12)` | 执行中（带呼吸点，2s） |
| `status-success` | `#34d399` | `rgba(52,211,153,0.12)` | 成功/完成（静态实心 + ✓） |
| `status-error` | `#f87171` | `rgba(248,113,113,0.12)` | 失败/异常 |
| `status-warning` | `#fbbf24` | `rgba(251,191,36,0.12)` | 降级/暂停/需注意 |
| `status-pending` | `#9aa3b2` | `rgba(154,163,178,0.10)` | 排队/待触发 |
| `status-cancelled` | `#6e7787` | `rgba(110,119,135,0.12)` | 已取消/跳过 |

> running 与 success 同属绿色系是刻意设计：都是「好」的状态，靠动效（呼吸 vs 静态）与图标区分。
> badge 规范：11px micro 大写标签不强制、12px 常规即可；底用上表 `badge 底`、文字用状态色、描边 `line`；圆角 `rounded-md`（6px）。

### 2.6 双主题机制（dark / light）

所有颜色 token 落在 `index.css` 的 CSS 变量上（`R G B` 三元组），由 `tailwind.config.js` 以 `rgb(var(--x) / <alpha-value>)` 引用，随 `html[data-theme="dark" | "light"]` **整体翻转**——存量页面无需改类名即获得双主题。

- **切换**：侧边栏顶部 ThemeToggle；初始值 = localStorage 键 `plaita-theme` > 系统偏好 `prefers-color-scheme` > 暗色。`index.html` 内联脚本在渲染前设置 `data-theme`，无闪烁。
- **亮色 token**：canvas `#F5F6F8` / surface `#FFFFFF` / elevated `#EEF0F4`（兼 hover 底）/ inset `#ECEEF2`；ink 四档 `#171A20 / #444C59 / #7A828F / #A6ADB8`；line 换成黑透明（同 0.07/0.12）；阴影减弱（`card .06` / `pop .14` 黑）。
- **亮色映射规则**：状态色与品牌绿整体**下移到深档**保文字对比度（如 running 亮色取 `#16a34a`、plaita-400 亮色取 `#16a34a`，`bg-plaita-500` 主按钮亮色为 `#15803d`）；存量 `dark.*` 灰阶**语义反转**（dark-100 系变深色文字、dark-800/900 变白/浅灰背景）。
- **红线**：两主题都必须过 §7 对比度验收；禁止为主题写死 hex（一切经变量）；`color-scheme` 跟随主题，原生控件（滚动条/表单）不允许反色穿帮。

## 3. 形状与阴影

**同心圆角体系**（外角 = 内角 + 内边距）：

| 层级 | 圆角 | 用于 |
|---|---|---|
| L1 卡片/面板 | 12px `rounded-xl` | 页面里的容器 |
| L2 控件 | 8px `rounded-lg` | 卡片内的嵌套块、按钮 |
| L3 元素 | 6px `rounded-md` | input、badge、小 chip |
| Pill | full | 状态点、标签 |

嵌套禁止跳级：12 里直接放 6 会显薄，中间隔 ≥8px 内边距时才可跳。

阴影（只负责「浮起」，描边一律由 §2.3 line token 负责，二者不叠加画线）：

```
shadow-card: 0 1px 2px rgba(0,0,0,.4)
shadow-pop:  0 12px 32px rgba(0,0,0,.5)
```

描边与阴影的默认搭配：静态卡片 `border-line + shadow-card`；浮层 `border-line-strong + shadow-pop`。

## 4. 动效纪律

| 场景 | 时长 | 曲线 |
|---|---|---|
| hover / 按压 | 120–160ms | ease-out |
| 浮层进出（下拉/抽屉/Dialog） | 180–220ms | `cubic-bezier(0.16,1,0.3,1)`，出场 120ms ease-in |
| 列表/卡片入场 | 每项 40–60ms stagger，fade + 位移 4–8px | ease-out，**不弹跳** |
| running 呼吸点 | 2s 循环 | opacity 1→0.5 |
| 数值刷新 | 150ms fade 或直接替换（tabular-nums 保证不跳宽） | — |

规则：`prefers-reduced-motion: reduce` 时全部动效（含呼吸点）降为静态；hover 之外禁止常驻动画；任何动效不得阻塞交互。

## 5. 组件基元（`src/components/ui/`）

页面禁止自带样式配方，一律消费以下基元（新建）：

- `Page` / `PageHeader`：页标题 + eyebrow + 动作区；统一 `px-6 py-5`，标题用 `page-title`
- `Card`：`bg-surface border-line rounded-xl shadow-card`，内边距 `p-4`（密）/`p-5`（舒展）
- `StatCard`：数值主导（`data` 声道 24px），图标退到 16px muted，趋势/占比用 caption
- `StatusBadge`：按 §2.5 语义色，入参只收语义名，不收颜色
- `Button`：primary（plaita-500 底/plaita-950 字）/ secondary（elevated 底 + line 描边）/ ghost；高度 28/32 两档；按压 `scale(0.98)`
- `Table`：表头 `micro` 声道 muted，行高 40px，行分隔 `line`，数值列 `data` 声道右对齐，行 hover `bg-elevated/50`
- `EmptyState`：居中图标（muted）+ 一句话 + 主行动；**禁止白板**
- `Drawer` / `Dialog`：`bg-elevated border-line-strong shadow-pop`，180ms 入场

React Flow 画布：节点 = `surface` 底 + `line` 描边 + 8px 圆角 + 节点名用数据声道；连线 `#3a4250`，hover/选中 `plaita-400`；画布底 `inset` + 点阵。

## 6. 反模式禁止清单（审计即对照此单）

1. 渐变背景 / 渐变文字 / 装饰性 glow（`linear-gradient`、`gradient-text`、彩色 box-shadow）
2. 全站统一「半透明 + rounded-xl + 灰描边」三件套（层级必须来自 §2.1/§3 token 组合）
3. 一次性 hex（新颜色必须先入 token）
4. 正文用等宽字体（等宽只属于数据声道）
5. 图标彩色喧宾夺主（图标默认 muted/secondary，仅状态语义用状态色）
6. 空态白板、加载无骨架、错误无指引
7. 3xl 大标题居中式营销排版

## 7. 验收清单（每页过一遍）

- [ ] 对比度：正文 ≥4.5:1、次要 ≥3:1（**WCAG 实测值见 §7.1，不目测**）
- [ ] 所有可交互元素具备 hover / focus-visible / active 三态，焦点环可见
- [ ] 空态 / 加载态 / 错误态三态设计过
- [ ] 1440px 与 2560px 两档不破版
- [ ] 数据声道用对了位置（ID/日志/数字/时间戳全是 mono + tabular-nums）
- [ ] `web-design-guidelines` 审计无 P0/P1
- [ ] 截图留档 `docs/design-shots/<page>-<before|after>.png`

### 7.1 对比度实测表（WCAG 2.x，2026-08-29 P5 实测）

| 色彩对 | 暗色 | 亮色 | 要求 |
|---|---|---|---|
| ink-primary / surface（标题·数值） | 15.55 | 17.43 | 4.5 ✓ |
| ink-secondary / canvas（正文） | 8.91 | 8.01 | 4.5 ✓ |
| ink-secondary / surface（正文） | 8.52 | 8.66 | 4.5 ✓ |
| ink-muted / surface（辅助说明） | 3.94 | 3.88 | 3.0 ✓ |
| badge running / 自身 dim 底 | 8.55 | 5.95 | 4.5 ✓ |
| badge success / 自身 dim 底 | 7.84 | 4.63 | 4.5 ✓ |
| badge error / 自身 dim 底 | 5.76 | 5.29 | 4.5 ✓ |
| badge warning / 自身 dim 底 | 8.82 | 5.88 | 4.5 ✓ |
| badge pending / 自身 dim 底 | 6.10 | 6.34 | 4.5 ✓ |
| on-accent / 主按钮底 | 8.55 | 7.13 | 4.5 ✓ |
| plaita-400 文字 / canvas（激活导航·链接） | 10.66 | 4.64 | 4.5 ✓ |
| ink-faint / surface（装饰性弱信息） | 2.18 | 2.26 | 装饰层，无硬要求 |

> v1.4 修复记录：亮色状态色与品牌绿原值在浅底仅 2.89–4.25，已整体下移（running→green-800、success→emerald-700、error→red-700、warning→amber-800、pending/cancelled→slate-600 系、plaita-400→green-700）。**规则：新增颜色必须先过此表口径的实测，再进 token。**

## 8. 路线图与变更记录

| 阶段 | 范围 | 状态 |
|---|---|---|
| P0 | Token 基础（tailwind.config / index.css / 字体自托管） | ✅ |
| P0 | App 外壳（侧边栏/Logo）+ Dashboard 示范页 | ✅ |
| P0.5 | 双主题机制（token 变量化 + ThemeToggle + 亮色映射） | ✅ |
| P1 | `ui/` 基元层（Button/Card/StatCard/StatusBadge/Table/Page/EmptyState）+ Dashboard 收口 | ✅ |
| P2 | Flows / FlowEditor（工具栏基元化、画布节点/连线/minimap/Controls 主题化、AI 按钮去紫） | ✅ |
| P3 | Executions / ExecutionDetail / Logs / Queues | ✅ |
| P4 | Cluster / Topology / Nodes / Events（语义色批量映射、分段 Tab、画布图例） | ✅ |
| P5 | 动效专项（fade-up/pop/fade + reduce-motion）+ 对比度实测（§7.1）+ Web Interface Guidelines 审计 | ✅ |

**全站美化完成。** 后续改进备选（非视觉债）：Tabs/筛选/分页深链到 URL 参数（Web Interface Guidelines「URL reflects state」项）；长列表（85+ 节点清单）虚拟化。

### 变更记录

- 2026-08-29 v1.4：**P5 落地，动效与验收收官**——①动效：新增 `fade`/`pop` 关键帧，全部 9 个对话框遮罩 fade、面板 pop，Page 容器一次性 fade-up 入场，`prefers-reduced-motion` 全局降级已有；②对比度实测（§7.1）：亮色 6 个状态色与 plaita-400 原不达 AA（最低 2.89），整体下移一档后全部 ≥4.5，暗色本就全过未动；③Web Interface Guidelines 审计修复：`transition-all`→显式属性、加载/搜索文案 `...`→`…`、PageHeader `text-balance`、`theme-color` 双主题 meta、交互元素 `touch-action: manipulation`、关键图标按钮补 `aria-label`。备选改进入 backlog（URL 深链、长列表虚拟化）。
- 2026-08-29 v1.3：**P4 落地，全站美化完成**——Nodes/Events 基元化（Events 分段 Tab 改浮起选中式）；Topology 画布/图例/节点主题化；Cluster 批量语义色映射（36 处硬编码红黄绿→状态 token）+ 头部/分区标题/启动按钮上基元。至此全部 11 个页面 + FlowEditor 消费同一 token 体系，仅剩 P5 动效与验收专项。
- 2026-08-29 v1.2：**P1–P3 落地**——新增 `src/components/ui/` 基元层（Button/Card/StatCard/StatusBadge/Table/Page/PageHeader/EmptyState，页面只准消费基元与 token）；Dashboard/Flows/FlowEditor/Executions/ExecutionDetail/Logs/Queues 全部基元化；FlowEditor 工具栏去紫（AI 生成改 secondary + Sparkles）、画布节点改 surface+mono 数据声道、连线/Controls/MiniMap 全部主题化；新增 `onAccent` token（品牌绿上的对比字色随主题翻转）；修复 NodePalette 族别色条从未生效（var(--family-*) 未定义）。
- 2026-08-29 v1.1：**双主题落地**——token 全部变量化并挂 `data-theme`；新增 ThemeToggle（localStorage + 系统偏好 + 防闪烁）；亮色映射规则见 §2.6；移除 Google Fonts 外链；清掉 6 处 `.input` 硬编码暗色内联样式与 18 处 `hover:text-white`。
- 2026-08-29 v1.0：定案混合式方向；建立 token 体系（背景四层/文字四档/白透明描边/语义状态色）；移除渐变背景与渐变文字；字体切换 Inter + JetBrains Mono 双声道。
