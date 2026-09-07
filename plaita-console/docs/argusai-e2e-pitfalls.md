# argusai E2E 踩坑与经验记录

> 2026-09-06 首次接入 plaita-console 全系统 E2E（12 suite 125 用例 + 混沌回归）过程中
> 实际踩到的坑与学到的东西。给后续维护者：改 suite / 加服务 / CI 排障前先过一遍。
> 三个 argusai 框架毛边已反提上游 [jeffkit/argusai#11]。

## 一、argusai 框架层

| # | 坑 | 事实与对策 |
|---|-----|-----------|
| 1 | **信封恒 success:true** | `argus-build`/`argus-setup` 的 JSON 信封几乎总返回 success，真实失败藏在 `data.services[].status=failed`。脚本必须逐服务打印，否则 CI 上 build 假成功、到健康等待才爆，隔一层难排查（首跑即中招） |
| 2 | **argus-run 自动启动不等 healthcheck** | `state != running` 时 run 会 auto-start，但不等 healthy 就开跑首批用例 → `fetch failed`（16ms 内）。且 setup 有任一服务失败时全不 transition 到 running，下轮 run 触发 orphan 清理把已健康容器整个重建。对策：runner 脚本在 setup 与 run 之间显式 curl 各端口 `/health` |
| 3 | **argus-clean 泄漏空网络** | 反复 run/clean 后残留无容器挂载的 `argusai-*` 网络，最终耗尽 Docker 地址池。对策：gate 跑前扫描 `Containers=0` 的 argusai 网络直接删 |
| 4 | **CLI 冻结，只有 MCP 入口** | `argusai` CLI 冻结在 0.12.3（setup exec 不执行的回归），唯一维护入口是 `argusai-mcp`，人类经 mcp2cli 当 CLI 用。生命周期 init→build→setup→run→clean |
| 5 | **假绿灯** | 0.14.1 按 suite name 归并事件，名字不匹配时全部静默丢弃仍报 passed。0.14.2 改按 id，但**成功判定必须保留 `total>0` guard** 作纵深防御 |
| 6 | **playwright 可选 peer 依赖** | 不随 `npm i -g argusai-mcp` 安装，且装完模块还会再缺浏览器二进制——用户要连踩两个错。CI 需 `npm i -g playwright && npx playwright install --with-deps chromium` |
| 7 | **browser 选择器直喂 `locator()`** | 不能混用 CSS 与 `text=` 引擎（`button[type=submit], text=登录` 直接报 parse 错）；用纯 `text=登录`。中文 placeholder 选择器（`input[placeholder="用户名"]`）没问题 |
| 8 | **save 支持数组索引路径** | `save: {id: "executions.0.execution_id"}` 可用——列表接口按时间倒序取最新条目时很顺手 |
| 9 | **suite 步骤名硬编码计数会烂** | workflow 步骤名写了"(11 suites)"，加一个 suite 就过时——计数交给运行时报告，名字别带数字 |

## 二、UI（Playwright）专项

| # | 坑 | 事实与对策 |
|---|-----|-----------|
| 10 | **渲染竞态：断言跑赢 React** | 全量跑机器负载高时，React 渲染晚于 `domcontentloaded`，goto 后直接 `expect.page.visible` 必闪失败。**元素断言一律 `waitForSelector` 等待式**，从不依赖 goto 返回时机的隐式就绪 |
| 11 | **webDist 被 gitignore：本地过 ≠ CI 过** | 干净 checkout 没有前端构建产物，UI 镜像里 `/` 是裸 JSON。本地绿 CI 红且只有 UI suite 挂时，先怀疑「本地有而 CI checkout 没有」的文件。CI 上用 pnpm 构建前端后再打镜像 |
| 12 | **截图取证是最快排障路径** | argusai 的 `screenshot` 步骤 + `upload-artifact`，一次就把「runner 页面渲染了什么」变成铁证（实锤了 #11 的裸 JSON），比任何猜测都快。UI suite 已常驻两个截图步骤 |

## 三、plaita 引擎 / 部署层（E2E 实测暴露的真 bug）

| # | 坑 | 事实与对策 |
|---|-----|-----------|
| 13 | **pyproject.toml 不随源码 COPY → entry_points 丢失** | setup.py 只是 shim；只 COPY 源码 + `pip install .` 会得到 0.0.0 空壳，`plaita.nodes` entry_points（approval/delay 等）全部隐形——保存审批流 422、worker 运行即报未知节点。**任何容器化 plaita 的 Dockerfile 都要 COPY pyproject.toml 且真正安装**。prod 的两个 Dockerfile 同病，已修 |
| 14 | **queue.read 只容忍 TimeoutError** | redis-py 5+ 空轮询抛 TimeoutError 的修复漏了 ConnectionError 分支：Redis 瞬断（DNS/拒绝连接）直接炸穿 run() 主循环，worker 退出。混沌脚本首跑复现。已修：退避后返回 None 继续轮询 |
| 15 | **event_filter 是独立部署进程** | 「事件→匹配订阅→回投恢复任务」由 `python -m plaita.server.event_filter` 承担，部署文档列为按需扩容项——不跑它，delay/approval 挂起后事件永远无人消费。E2E 已纳入 services 容器 |
| 16 | **approval_service 状态在进程内存** | 审批实例存 `pending_approvals` dict，无外部可观测状态——E2E 断言不了消费侧（要可观测得改引擎代码） |
| 17 | **集群档启动执行的契约** | `POST /api/executions` 不校验 flow 存在（XADD 完事），响应里**没有 execution_id**（worker 侧才生成）——拿执行 ID 要按 flow_id 过滤列表轮询 `executions.0.execution_id` |
| 18 | **resume_type=continue 对挂起节点被内核拒绝** | 防止绕过 pending 事件静默推进（R6 fuzz 修复）。审批/delay 恢复必须 `resume_type: "event"`（console resume 端点）或事件发布（correlation_id=execution_id）——两条恢复通道都要有回归 |
| 19 | **console lifespan 连不上 Redis 会静默降级本地模式** | 不报错、照常 healthy，但集群档断言全翻车（503）。容器 CMD 必须先等 Redis 端口可连再起 uvicorn（argusai 的 healthcheck 是 HTTP 型，表达不了「Redis 就绪」） |
| 20 | **dry-run pinned 的 result 语义** | pinned 命中后 `result` 变成 `{节点id: 输出}` 字典而非端节点输出值。断言按 `result: {end: 42}` 写 |

## 四、CI / 工程层

| # | 坑 | 事实与对策 |
|---|-----|-----------|
| 21 | **runner 上 daemon 内 `docker build` 秒挂** | GH runner 上从 mcp2cli daemon spawn 的 `docker build` 秒失败（docker run 正常、普通步骤里 build 也正常），本地无法复现。对策：workflow 普通步骤预构建全部镜像，gate/chaos `--no-build`。顺带把「信封假成功」逼了出来（#1） |
| 22 | **runner 没有 uv** | `uv tool install mcp2cli` 前要 `astral-sh/setup-uv` |
| 23 | **pnpm 12 默认拒依赖构建脚本** | corepack 拉最新 pnpm 会 `ERR_PNPM_IGNORED_BUILDS`（esbuild postinstall 被拦）直接 install 失败。CI 钉 pnpm@9（lockfile v9 兼容、无审批闸） |
| 24 | **管道吃退出码** | `docker build ... | tail -1 && break` 的 `&&` 判断的是 tail——build 失败照样走成功分支（本次开发中踩了两次）。脚本一律 `set -o pipefail` |
| 25 | **docker network disconnect 丢 alias** | 重连必须 `docker network connect --alias <原名>`，否则按名字的解析永久失效（混沌脚本细节） |
| 26 | **失败取证设施前置** | CI 失败时自动 dump：argusai 容器日志 + UI 截图 artifact。第一次就要建好，出事时省的是整个排查周期 |

## 五、方法论层面的收获

1. **纸面审计 ≠ 实测**：IPv6/localhost 解析导致 fetch failed 的假设很合理，实测一击即溃；而 event_filter 漏部署、webDist 没进 checkout 这种事，查出来全靠「让现场自己说话」（截图、逐服务打印）。
2. **E2E 的价值在跨进程断点**：单测/集成全绿 ≠ 系统能跑。本次 4 个真 bug（#13/#14 + webDist + 竞态）全是单元与集成层看不见的。
3. **确定性设计先于断言**：全链路不碰真 LLM（确定性节点 + dry_run 契约 + stub）、状态轮询用 retry 而非 sleep、失败取证前置——测试的稳定性一半来自被测系统的可控入口，一半来自测试自身的确定性。
4. **假绿灯比红灯贵**：`total>0` guard、逐服务打印、失败用例详情打印，都是在「绿灯但什么都没测到」上交过学费后的防御。
