# 编排台（Console）

编排台是 plaita 的可视化控制台：拖拽画布编排流程、schema 驱动的节点配置表单、
Dry-Run 试运行、执行历史与集群管理。后端为 FastAPI，前端随 pip 包分发，**安装即用**。

## 安装与启动

```bash
pip install plaita-console            # 含后端 API 与打包好的前端页面
pip install "plaita-console[nodes]"   # 连同通用业务节点插件一起安装
python -m plaita_console              # 或使用 plaita-console 命令
```

启动后打开 <http://localhost:8080>。前置条件只有一个：**Redis 可达**
（默认 `redis://localhost:6379/0`，经 `PLAITA_CONSOLE_REDIS_URL` 覆盖）。

从源码开发则克隆仓库，后端 `python run.py`、前端 `pnpm dev`，见仓库内
`plaita-console/README.md`。

## 配置

全部经 `PLAITA_CONSOLE_*` 环境变量配置，常用项：

| 变量 | 说明 |
|------|------|
| `PLAITA_CONSOLE_HOST` / `PLAITA_CONSOLE_PORT` | 监听地址，默认 `127.0.0.1:8080`；非回环地址启动打 WARNING。官方容器 CMD 固定 `--host 0.0.0.0`，容器部署用端口映射控制暴露面 |
| `PLAITA_CONSOLE_REDIS_URL` | Redis 连接 |
| `PLAITA_CONSOLE_DB_URL` | 流程定义持久化（默认本地 SQLite） |
| `PLAITA_CONSOLE_DB_CONNECT_TIMEOUT` / `..._POOL_TIMEOUT` | 非 SQLite 库的连接/池超时（默认 5s；黑洞/半开 DB fail-fast） |
| `PLAITA_CONSOLE_ENGINE_SYNC_DEADLINE` | 启动回填墙钟预算（默认 5s；超时仍启动，`/health.engine_sync=degraded`） |
| `PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true` | 本地开发免密；局域网/生产请改设 `PLAITA_CONSOLE_ADMIN_API_KEY` |
| `PLAITA_CONSOLE_SECRET_ID` / `SECRET_KEY` | 对外契约接口的 HMAC 密钥 |

仓库根目录的 [`.env.example`](https://github.com/jeffkit/plaita/blob/main/.env.example)
给出 compose 必填项与常用项样例；变量的完整默认值、读取时机、0/非法值语义见
[分布式 runbook「环境变量参考」](../distributed/ops-runbook.md#环境变量参考)。

## 企业能力：RBAC / 审计 / 环境晋升

**RBAC**：设置了 `PLAITA_CONSOLE_ADMIN_PASSWORD` 且 users 表为空时，首次启动
自动创建 `admin`（平台管理员）；**未设置时不会自动建号，也不会打印随机密码**，
前端会进入「创建管理员」向导。角色三级：

- **admin**：全部权限（用户管理 / 凭据 / 集群 / 审计 / 生产环境删除）
- **editor**：编排与执行（增删改流程、发布、试跑、启动执行）
- **viewer**：只读

多租户部署下，`admin` 是**租户内**角色，不等于平台身份（#260 / #261 / #263）：
`/api/users` 的全局账号变更（创建 / 改角色 / 改密 / 删除）、全局服务停止，
以及**集群管理面的全部端点**只允许 `platform_admin`（或平台级 API-Key）
执行，拒绝写 `*.denied` 审计。集群面包括全局生命周期与基础设施配置
（服务启停 / stop-all / process-docker 模式切换 / 基础设施 CRUD 与容器
启停 / 集群登记、切换、删除与配置保存），**也包括 GET 读面**（实例列表、
基础设施 URL/健康详情、集群列表与完整配置、模板）——这些响应含数据面
连接串与 env 密钥，没有租户内语义（#263）。所有 cluster 响应再经序列化层
脱敏：URL 的密码段与 `PASSWORD`/`SECRET`/`TOKEN`/`KEY` 等 env 值显示为
`***`。`/api/dispatch/ledger` 对非平台身份按本租户过滤（显式传他租户
403 + 审计），平台身份默认全量。租户 admin 只能经
`/api/tenants/{tid}/members/*` 管理**本租户**成员，且不能操作平台管理员
账号。任何已认证角色都可以修改**自己**的密码。租户 admin 的
`GET /api/users` 只返回本租户成员，且每个成员的 `memberships` 只含**本租户**
的成员关系——同时属于多租户的用户不会把他租户的 (租户, 角色) 带进来
（A2R45-03）。

`POST /api/flows/{id}/supervisor/iterate`（#290）是写路径（会落候选草稿）：
入口先按调用方租户校验 flow 归属，跨租户 / 不存在一律 404（与
`GET /api/flows/{id}` 同口径，不可区分）；Supervisor 的回环客户端以平台 key
**钉在调用方租户**（每个回环请求带 `X-Tenant-ID`），基线评测、dry-run 与
候选草稿全部落在调用方租户，不再「读全量、写 default」。平台身份跨租户
迭代须显式带 `X-Tenant-ID`；会话型平台管理员的全量视角是只读的（403）。

回环**目标地址**与请求解耦（#291）：回环客户端的 base URL 只来自部署配置——
默认由 console 自身监听配置推导（`PLAITA_CONSOLE_HOST` 为 `0.0.0.0`/`::` 时取
对应回环地址，否则取绑定地址；端口取 `PLAITA_CONSOLE_PORT`），非常规部署
（uvicorn CLI 另指端口、进程内 TLS 等）可显式设
`PLAITA_CONSOLE_LOOPBACK_URL`。绝不取 `request.base_url`——那由请求方可控的
`Host` 头拼成，拿它当回环目标等于把平台 admin key（`X-Admin-API-Key`）送往
调用方指定的任意主机。覆盖值指向非本机地址会留 WARNING；非法值 503 具名拒绝。

`POST /api/copilot` 的宿主 Agent 大脑是平台能力（#292）：`PLAITA_CONSOLE_COPILOT_BRAIN`
选 `claude`（本机编码 Agent 子进程，继承 console 进程环境与 provider 凭证）或
`recursive`（以平台 API key 反代外部 Agent）时，仅平台管理员 / 平台级 API-Key
可调用，租户 editor/admin 403 具名拒绝、进审计且大脑零启动（与
`/api/flows/ai-generate/stream` 同口径）；默认 `flow_agent`（纯 validate /
dry-run 工具，无宿主 Agent）保持 editor+ 可用。Copilot 会话（thread）归属
创建租户：upsert 按 (租户, threadId) 复合键，跨租户使用他租户 threadId 一律
404、不接管历史；带 threadId 的请求必须携带 `x-flow-id` 头，缺失即 422
具名拒绝（`flow_id_required`）——thread 归属闸 fail-closed，头缺席不再
跳过闸；并发首用撞 threadId 全局唯一约束时按归属回查：同租户采纳胜者行
（与顺序语义等价），他租户 404（A2R47-01）。claude 会话续轮（`--resume`）
按租户段隔离，知道他租户 threadId 也无法续其会话。

**全量视角是只读的，对所有租户作用域写路径生效**（A2R17-01 / A1 r110）：
会话型平台管理员 switch-tenant 到「全量」后，schedules 的改/删/启停/触发、
自定义节点与属性类型删除、凭据删除、执行的取消/删除/恢复、事件订阅删除——
一律 403（`全量视角为只读，写操作请先选择具体租户`），与 flows / credentials
创建等写端点同一闸；跨租户写须显式 `X-Tenant-ID`（或先切换到具体租户），
审计记录生效租户。不带租户头的无状态平台 API-Key 沿写路径契约落 default，
他租户资源按 404 处理（与租户用户视角同口径，不暴露存在性）。全量视角的
**读**面不变。

`POST /api/dispatch/one` 的 `shadow` / `dry_run` / `tenant_id` 是服务端
权威字段（#262）：`shadow` 只取 `PLAITA_CONSOLE_DISPATCH_SHADOW`（请求体
同名字段忽略）；`dry_run=false` 仅平台身份可写，租户 editor 传 false 会被
强制干跑并留下 WARNING；`tenant_id` 取认证上下文，只有平台身份可代传其它
租户，跨租户请求 403 + 审计。派发台账同时记录认证主体 `actor` 与生效租户。

`POST /api/dispatch/one` 与 `/api/dispatch/plan` 的容量路由带**队列维度**
（A64-05 / B75-01）：只有注册 metadata `queue_name` 与本次派发队列一致的
worker 才可能成为目标，跨队列候选以稳定原因 `queue: mismatch` 进入
`rejected`；命中目标后消息投到目标 worker 消费的队列。避免 v1/v2 金丝雀
或滚动升级时目标指向另一队列、消息落到无人消费队列后交接耗尽、
执行被误终态化为 `AffinityHandoverExhausted`。

队列是**三态**（B75-01）：`queue_name` 非空且非纯空白 = 声明（known）；
缺失/空白/非字符串 = unknown，**不再按默认队列假设**。unknown 只拒绝
**定向**、不拒绝**广播**：默认 `PLAITA_DISPATCH_UNKNOWN_QUEUE=skip` 下
unknown 以 `queue: unknown` 进入 `rejected`，消息仍投到调用方队列且不带
`target_instance_id`，legacy default worker 仍能抢到。需要显式兼容旧部署时
设 `PLAITA_DISPATCH_UNKNOWN_QUEUE=assume-default`：仅在请求默认队列
`plaita:flow:queue` 时把 unknown 当默认队列，响应/台账标记
`queue_source=assumed-default` 并留 WARNING（含 instance_id 列表）；其余
情况仍是 unknown。plan / dispatch / 台账 / 指标
`plaita_dispatch_unknown_queue_total{queue}` 共用同一判据。新 worker 注册时
写 `queue_name` + `queue_contract_version=2`。

`GET /api/dispatch/ledger`（A54-02）：平台身份返回全量台账与全局队列对账；
租户身份只返回**当前世代**的本租户条目（台账写入 `tenant_generation`，
同名租户重建后旧世代条目不可见；缺失世代的旧行 fail-closed），且不返回
全局队列计数——`queue_length=null`、`error` 说明对账仅平台身份可用。

`GET /api/services` 及其详情/拓扑读面（#265）：基础设施与内部组件的合成
条目在响应序列化层脱敏——连接串 URL 的 userinfo 口令与凭据键值换成
`***`，保留 scheme/host/port/path/db。租户 viewer 仍可看 worker 实例与
非敏感状态，但读不到口令。

登录后走会话 token（7 天）；`X-Admin-API-Key` 服务账号兼容（等价 admin）。
角色/密码变更即时撤销旧会话。「用户」页管理账号与角色。

**审计**：「审计」页留痕管理面敏感操作（发布、保存、删除、执行、凭据变更、
用户管理），含操作人 / IP / 时间 / 元数据——机密内容永不入审计。

删除租户时 `audit_logs` **不物理删除**（合规证据保留）：租户 admin 的
`GET /api/audit` 只返回当前世代（`tenants.created_at` 之后）的记录，
平台身份可跨世代全量审计。自定义节点/属性类型/Copilot 会话/部署记录随
租户删除事务清理；日志 Stream（`plaita:logs:*`）按条目 `tenant_id` 清理；
任务队列按 console/worker/allowlist/同族版本队列的**键集合**排空。404
补清扫遇到残留 flow 时返回 409 具名拒绝，`create_tenant` 也拒绝任何同名
继承（要求显式清理/归档）。

**环境晋升**：console 实例经 `PLAITA_CONSOLE_ENV` 标识环境（dev/test/prod）：

- 每次发布写部署记录（环境、操作人、定义 SHA-256 指纹）
- 「导出晋升包」→ 目标环境 console「导入」：指纹校验防篡改，导入为草稿
  再发布，形成 dev → prod 的受控晋升
- `PLAITA_CONSOLE_ENV=prod` 时删除操作需要 admin

## 本地单机模式（零依赖上手）

**不装 Redis 也能用**：console 启动时若 Redis 不可达，自动进入本地单机模式——
流程在 console 进程内执行、执行历史与节点级 trace 存 SQLite，首次启动还会
写入 3 个示例流程（快速开始 / 循环与映射 / HTTP 调用）。

本地模式的边界：集群管理、任务队列、事件管理不可用（对应页面明确提示）；
审批/事件的挂起-恢复支持单进程语义（checkpoint 存 SQLite，重启后仍可显式
恢复）。定时触发器也只在本地档运行：以集群档启动后，SQLite 里的本地调度
保留但不执行、也不在集群调度列表显示（切回本地档恢复，或按相同 cron 在
集群档重建）。完整对比见下文与 [部署模式](deployment.md)。

四档部署形态（本地单机 / 单机标准 / 分布式 / SDK 嵌入）的完整对比与升级路径
见 [部署模式](deployment.md)。

## 调试：数据固定与单节点重放

试跑面板支持 n8n 式调试：

- **节点级 IO 检视**：点击时间线节点展开完整输入/输出 JSON（不再截断）
- **数据固定（pin）**：把某次试跑的节点输出固定下来，后续试跑跳过该节点
  真实执行（HTTP 调用等重副作用节点尤其实用），面板以 mock 高亮
- **仅运行此节点**：上游取固定值、下游 mock 化无副作用，只真实执行目标节点

固定值只在试跑内存里，不会写进流程定义。

## 连接器

`pip install plaita-nodes` 自带的连接器节点（凭据按名引用，机密不落流程定义）：

| 节点 | 用途 | 凭据数据 |
|------|------|----------|
| `feishu_webhook` / `wecom_webhook` / `slack_webhook` / `dingtalk_webhook` | IM 群通知 | `{"url": ...}`；钉钉可加 `"secret"` 自动加签 |
| `generic_webhook` | 任意 Webhook POST | `{"url": ...}` |
| `api_request` | 通用 REST（静态 Header 鉴权） | `{"base_url": ..., "headers": {...}}` |
| `sql_query` | SQL 查询/写入（SQLAlchemy） | `{"url": "postgresql://..."}` 或 host/port/user/password/database |
| `email_send` | SMTP 邮件 | `{"host", "port", "username", "password", "use_tls"/"use_ssl"}` |

新增连接器即一个新的节点类 + entry-point 注册，无需改动编排台。

## 凭据

外部服务机密（webhook 地址、数据库密码等）集中在「凭据」页管理：Fernet
加密落库，流程节点按名引用（`credential: "feishu-bot"`），流程定义里不落
明文。保存后自动导出加密文件供引擎节点运行时解密；console 拉起的
flow_worker 自动带上密钥环境。

密钥来源：`PLAITA_CREDENTIALS_KEY` 环境变量，或自动生成于 DB 同目录的
`.plaita-credentials.key`。多机部署请统一注入同一密钥。

**安全边界（如实说明，A2R25-08/09）**：凭据行本身是 Fernet 密文。同一个
DB 里另有三列敏感数据——`local_executions.input_json`（执行 params）、
`context_json`（checkpoint/`$INPUT`）与 `tenants.contract_secret_key`
（对外 HMAC 契约密钥）——自本轮起**落盘即字段级加密**（`enc:v1:` 密文，
AES-GCM；主密钥与凭据加密同源，按用途经 HKDF 派生独立子密钥）。ORM 读取
透明解密，执行页/恢复/契约验签不受影响；裸 SQL、`sqlite3`、`shutil.copy2`
备份只能看到密文。存量明文行在 `init_engine` 启动时回填并 `VACUUM`
（SQLite），回填失败/密钥不可用会打 WARNING 或 ERROR（绝不静默）。

**密钥就是数据的一部分**：丢失/轮换主密钥会让历史执行 checkpoint 与既有
租户契约密钥**不可恢复**（读取 fail-loud，不会退回明文）。备份 DB 时必须
同时保护 `.plaita-credentials.key`（0600），或把 `PLAITA_CREDENTIALS_KEY`
放进密钥管理设施统一注入；不要把两者放进同一个备份包外发。

SQLite 文件仍在 `init_engine` 收紧为 0600（0600 是第二道闸，不替代字段
加密）；`.plaita-credentials*` 与 `plaita_console.db` 已加入 `.gitignore`
（与 `.dockerignore` 同口径）。`output_json/nodes_json` 仍是明文功能副本，
只在读面脱敏（#166/#222）——**DB 文件/备份整体仍按敏感数据处置**。PostgreSQL
没有文件权限面，字段加密是主要 at-rest 控制。另：错/缺
`PLAITA_CREDENTIALS_KEY` 时控制台拒绝保存凭据（409
`credentials_key_mismatch`），启动对账跳过该租户的导出重写（原文件保留），
不会用新 key 覆盖旧密文造成混合密钥。导出文件在有密文时带
`__plaita_meta__` 版本头（`key_id`/格式版本），引擎读侧忽略该保留键。

**导出文件默认与 DB 同目录**：未显式设置 `PLAITA_CREDENTIALS_FILE` 时，
SQLite 部署导出到 DB 同目录的 `.plaita-credentials.json`。以下情形无法推断
共享目录，按历史语义回退**当前工作目录**并打一次性 WARNING（A67-04）：
未设置 `PLAITA_CONSOLE_DB_URL`、URL 非 `sqlite:///`（PG/MySQL 等）、内存
SQLite；相对 SQLite URL（如 `sqlite:///./x.db`）即使两端字符串相同也会按
各自进程 cwd 解析，同样 WARNING。**显式**设置
`PLAITA_CREDENTIALS_FILE` / `PLAITA_CREDENTIALS_KEY_FILE` 时，绝对路径不触发
路径回退告警；若显式值是**相对路径**，仍按各进程 cwd 解析，并打一次性
WARNING——「值相同」不等于「同一个文件」。DB 敏感列的字段级加密密钥
（与凭据共用 `.plaita-credentials.key`）走同一套解析与告警。回退只保证
单进程可用——升级后忘记注入 DB env 的 worker 会继续读旧 cwd 文件，并能
解出已吊销/已轮换的旧值，因此
**不能把该 WARNING 当噪音**。导出文件是 DB 的派生缓存，按 DB 路径落盘让
**同一 DB 的多个 console/worker 副本共享同一份文件**；不再各按 cwd 持有
一份互不收敛的副本（否则一个副本删除/轮换凭据，其他副本的引擎仍能解出
旧值）。引擎侧 `plaita.credentials` 用同一规则解析默认路径与密钥文件。

- **多副本部署**：共享同一 SQLite DB 的副本无需额外配置；独立启动的
  worker 需能读到同一 `PLAITA_CONSOLE_DB_URL` 的**绝对路径**才能解析到
  DB 同目录（console 拉起的 worker 会自动注入绝对路径）。PostgreSQL 等无法
  从 DB URL 推断共享目录的后端，**必须**给所有 console/worker 显式注入同一
  绝对路径的 `PLAITA_CREDENTIALS_FILE` 与 `PLAITA_CREDENTIALS_KEY_FILE`
  （并确保该路径持久化/挂卷）。相对 URL 不满足「同一 URL」：各进程会按
  自己的 cwd 打开不同 DB。
- **升级迁移（旧版本 → 当前版本）**：先看 console/worker 启动日志是否出现
  「凭据…默认回退到当前工作目录」WARNING。若有，把旧 cwd 的
  `.plaita-credentials.json` / `.plaita-credentials.key` 迁移到 DB 同目录
  （或显式绝对路径）后删除旧副本；只改 DB URL 不够，独立 worker 也必须
  拿到同一绝对路径。旧 cwd 文件留在原处不会自动清理，其里已删除/已轮换的
  值会持续可被该 cwd 的进程解出。
- **容器部署**：DB 同目录若在容器内且未挂卷，重启会丢文件；启动对账会从
  DB 重建，但请仍把该目录挂持久卷以避免窗口期。
- **启动对账**：console 启动时以 DB 为权威重建缺失的导出文件、刷新陈旧
  内容（含进程在「导出成功→提交 DB」间被杀留下的条目），并对没有对应租户
  的旁文件记 WARNING。对账写入失败会**拒绝启动**，不让引擎带着缺失/陈旧
  的密钥继续跑。租户删除/同名重建会清掉同 slug 的凭据行与导出文件，新租户
  不会继承前代密钥。边界（当前定案，自动收敛**不做**）：
  - **孤儿侧车只告警**：没有对应租户行的旁文件/凭据行只记 `extra_files` /
    `orphan_tenants` WARNING，不自动删除或隔离——可能是手工放置的共享密钥，
    自动收敛需 owner 裁决（B67-01）。需要清理时用对应的 `DELETE` 租户
    凭据入口或人工移走文件。
  - **对账只在启动时**：没有周期对账；SIGKILL 落在「导出成功→提交 DB」间
    留下的鬼凭据/落后文件，最长到下一次重启、同租户写或显式 `DELETE` 才
    收敛。长期不重启的部署请把「重启 console」写进凭据轮换/泄露处置步骤
    （B67-02）。
  - **慢盘/滚动重启**：对账在 SQLite 写锁内逐租户做文件 I/O；租户数极多或
    DB 目录在 NFS/慢盘时，第二个实例可能撞 5s busy timeout 而拒绝启动。
    fail-loud 是设计，编排层应串行重启或给足重试/超时（B67-03）。

**导出写入是原子的**：保存/删除凭据时先写同目录临时文件（`flush` + `fsync`
+ 0600），再用 `os.replace` 原子替换目标文件。磁盘写满、超配额或进程中断
不会把上一份完好的导出文件截断成半截 JSON——旧文件逐字节保留，引擎继续
按旧内容解析。

**密钥文件权限**：console 首次生成密钥时用 `O_EXCL` 直接以 0600 创建
（不再先 0644 再 chmod，A67-05）；并发首建只有一把 key 胜出，落败进程
等待并复用胜者写入的 key，不重新生成，并在复用前把旧版本可能留下的
0644 胜者文件收紧为 0600。DB 字段加密（`secure_fields`）使用同一密钥
文件，创建/落败复用/收紧口径与凭据侧一致。已存在的 0644 存量 key 会在启动
对账与每次读取时收紧为 0600 并打 WARNING；只读挂载等无法 chmod 的场景会
持续告警（每个路径一次），请修正属主/挂载权限。密钥是解密所有租户凭据的
唯一材料，`.plaita-credentials.key` 的备份/外发按凭据泄露处置。

**引擎读侧 fail-loud**：文件不存在视为「尚未配置凭据」；文件存在但为空、
JSON 损坏或顶层不是对象时，`plaita.credentials` 抛
`CredentialStoreCorruptError`（`CredentialError` 子类），报错指明文件路径
并提示重新保存任意一条凭据以重建导出文件，不再静默降级成「凭据不存在」。

**多租户隔离**：凭据按租户落库与导出——default 租户沿用历史文件
`.plaita-credentials.json`，其余租户各写旁文件
`.plaita-credentials.{tenant_id}.json`。引擎侧 `plaita.credentials`
按租户上下文路由（worker 消费任务时从消息体 `tenant_id` set，本地档由
执行线程 set；该上下文由 `core/runner` 随 context 副本带进节点执行线程，
故节点内解析到的租户与调度线程同源）：非 default 租户的 flow 只能解析到
自己旁文件里的凭据，default 文件对它不可见，凭据缺失的报错也只列当前
租户文件内的名字。

## 主要能力

- **流程编排**：拖拽画布 + 节点面板；节点配置面板由节点 schema 自动生成表单
  （表达式字段带 `$INPUT` / `$NODE` 变量插入），复杂结构回落 JSON 编辑
- **子流程**：map / loop / while / parallel 分支等进入子画布编辑，面包屑返回
- **Dry-Run**：保存前在控制台进程内试运行并查看逐步输出
- **执行与集群**：执行历史、集群页一键拉起 flow_worker / 调度 / 事件服务等实例
  （`cluster_config.yaml` 定义服务模板与实例数）
- **Copilot**：AI 辅助生成流程（`PLAITA_CONSOLE_COPILOT_BRAIN` 可选大脑；`claude`/`recursive` 宿主 Agent 大脑需平台管理员，`flow_agent` 租户可用）

## 节点插件

节点能力三来源：

1. **内置 23 种**（start / if / switch / loop / while / map / http / event / parallel 等）——随引擎自带（含经 entry_points 自动注册的 delay / approval / redis_queue / kafka_queue / http_callback 扩展节点）
2. **插件包** `pip install plaita-nodes`——agentrun / hitl / gate 等，经
   `plaita.nodes` entry-points 自动注册
3. **自定义**——参照[自定义节点](../nodes/custom.md)编写后，以同样机制打包分发

控制台「节点」页实时展示当前已注册的全部类型及其 schema。
