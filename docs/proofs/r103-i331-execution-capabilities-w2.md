# r103（B3 修复席）· #331 后半——validate_for_profile + 递归闭包 + NodeRunner 断言 + 作者期信号 + 矩阵治理

- 单：plaita#331（P2，depends-on #330 已合 b9072302；#329 系列）；规格源=正文 + 评论区 A2 r61 实施前评审 + owner 裁决（不加第六 token，5 token 封闭词表定稿）
- 分支：`feat/execution-capabilities-w2`（独立 worktree `/private/tmp/plaita-b3-r103`）
- 基线：开工实测 `origin/main = 5637548c`（E 线 wave 2 并入），门禁前后零漂移
- 续做注记：前两轮同任务书会话配额死（零损失），本轮开工 worktree 已存在且含未提交实现（17 文件 +508 行 / 4 新文件，分支锚在 5637548c 无需 rebase）——按 r101 配方①续做不重做：逐文件审读对照 ①-⑤ → 补接线层（executor/runner/console/plaita-ai）→ 治理文档 §7 → 先红后绿 → 四门 → 入库。

## 规格 → 落地

| 任务书 | 落地 |
|---|---|
| ①`validate_for_profile(flow, profile)` 独立于 `model_validate`，只在执行入口（Normal/Generator/Distributed/Go Execute）与 publish/start 调用；profile 判据按 ADR-001 runtime 组合 | `plaita/core/capabilities.py`（词表本体迁 core，`plaita.node.capabilities` 再导出**同一对象**保公开路径）。`EXECUTION_PROFILES`：normal/generator={network,sandbox}（进程内 Python reference）/ distributed=全集 / local=全集−queue（console 进程内后端 ADR-001 §3.4：InMemoryEventBus+SQLite checkpoint，无持久队列/延迟排程）；自定义 profile=token 集合。调用点：`FlowExecution._prepare_strategy`（run_compatible/arun_compatible 的 sync/async/lazy 三态同咽喉）+ `run_distributed`（入口处钉 `mode=DISTRIBUTED`，具名异常经 `_raise_distributed_error` 归一保 `__cause__`）+ `FlowExecution.run` 分派；console `_publish_gates`（publish 与 import-version 共用，A2 §2.1）+ 本地档 start `_gate_local_execution_profile`；console local 后端 `_run_flow` 置 `execution.execution_profile="local"`（进程内旁路同闸）。`model_validate` 零触碰。「Go Execute」=Go 引擎自有入口（frozen），Python 侧以 `--json` 导出为对拍锚 |
| ②递归闭包 `required_capabilities(flow, recursive=True)` | `capability_closure`：child/loop 族（`child_flow`）/ parallel 分支（`branches[].flow`）/ reference（`resolve_reference` 钩子，未注入记 `unresolved` 不当缺口）；站点带 `frame_path` 根→叶（与 #330 根出口呈现同形）；空声明挂起节点 fail-closed 记 implicit `suspend:root`（`cause=undeclared`） |
| ③NodeRunner 每个 suspending 节点 execute 前断言宿主契约（含子流程），覆盖绕过 strategies 的旁路 | `assert_node_executable` 接入 `NodeRunner.run_node` 取消检查之后、超时预算之前；宿主身份=执行链**根**的 profile（`host_profile` 沿 parent 链）；触达=入口漏了→具名错误（`stage="runner"`）+WARNING 不静默；`SuspendNotExecutableError` 在 runner 层与 carrier 同待遇原样上抛（不被重试/errorHandler 吞） |
| ④作者期信号 | `list_node_types` 行内 `requires`（registry 声明面，不重扫源码）+`suspends`；`compile_flow(source, profile=...)` 附非致命 `execution_requirements:{profile,required,missing}`（稀疏键，不传 profile 载荷逐字节不变、`ok` 不变；未知 profile 名=一条编译错误不炸工具体）；MCP `flow_compile`/`flow_list_nodes` 透传 |
| ⑤矩阵与 nightly | `NodeRegistry.capability_matrix()`（空声明节点同样入列）+`shipped_capability_matrix()`（内置+本仓 entry points+opt-in code，按 dist 名过滤）+`diff_capability_matrix`（`{added,removed,changed}`）；账本 `docs/governance/architecture.yaml` `gov.capability_matrix`（24 node type）；`scripts/export_capability_matrix.py --check/--json`；`ci-slow.yml` nightly 独立步 + `ci.yml` paths 登记账本为测试输入；**大仓侧** `docs/DOC_ASSERTIONS.yml` `gov/capability-matrix`（yaml_path_equals，B2 r93 doctor 形制合流，大仓 `5bbed10`）。差分断言「注册表新增 node_type 而矆阵未变 → added 非空 → 红」先红实例=`test_differential_detects_new_node_type_red_instance` |
| 错误体 | `SuspendNotExecutableError` 增稀疏键 `stage/profile/required/missing`（运行期根出口呈现不含，消费方据此辨面）；`fix.call=None`+message 明确告知不可本地执行（与 #330 `_error_normalization` 同形）；新 `ExecutionProfileError`（`execution_profile_unsatisfied`，非挂起族缺口） |
| 旋钮 | `PLAITA_SUSPEND_GUARD=warn` 同样降级入口拒绝与 runner 兜底（WARNING+放行，交回运行期闸/呈现）；`ExecutionProfileError` 不受旋钮影响；治理文档 §4 已扩 ③④ |

### 大仓 DOC_ASSERTIONS.yml 发现（顺带修复）

7a64e89（r101 F 线骨架）在 `assertions:` 列表外以 0 缩进写入 `gov/version-dimensions` 且字段名不合形制（assertion/path_type vs check/file），**整表 YAML 解析失败**（`yaml.safe_load` HEAD 版 ParserError@370 实证）→ `monarbor doctor` 在 HEAD 不可执行。本轮按 f2be494 形制修复（expected 与账本 `[ir, wire_schema]` 逐序一致）+ 新增 `gov/capability-matrix`。doctor 实测 48/52（两轮独立一致）：3 红为在案既有（README/ARCHITECTURE 仓表 22≠mona 23、im-agentproc README——7a64e89 时代整表解析失败不可见，修复入位后现形），`gov/capability-matrix` 第 4 红=读 main clone 账本（尚无 gov.capability_matrix 键），本分支并入 plaita main 后翻绿（工作树账本与断言字面量已验逐值一致）。

## 先红后绿（干净基线树法，r101 配方⑥变体）

红相树=`git archive origin/main` + 最终三测试文件 + **非接线层**（`core/capabilities.py` 新模块、`node/capabilities.py`、`node/__init__.py`、`core/errors.py`、账本、导出脚本），**接线层缺席**（executor/runner/strategies/console capability_gate+api/local_executor/plaita-ai flow_runner+server 为基线版）——使红全部落在接线承重点：

| 面 | 红相 | 红因样本 | 绿相 |
|---|---|---|---|
| unit `test_issue331_execution_profile.py` | **9F/29P** | `test_normal_entry_rejects_before_any_node_runs`: `assert CALLS == []` → `['before']`（无入口闸：挂起点之前节点已执行）；`test_local_profile_…rejects_delay`: run_distributed 对 delay 放行挂起而非具名拒 queue（local 后端「suspended 行永不唤醒」本相）；runner 旁路两格 CALLS 非空；帧表单源格 | 38P |
| plaita-ai `test_issue331_authoring_signal.py` | **5F**（compile_flow 无 execution_requirements 属性 / list_node_types 行无 requires） | 新 API 面整体缺席 | 5P |
| console `test_issue331_execution_profile_gate.py` | **2F/1P**（**行为红非 import 红**：基线 publish 返 200 且版本转 published；start 受理产生执行行） | `assert r.status_code == 422` → `200`，响应体 `"status":"published"` | 3P |

29 绿=纯函数面（profile 表/词表单源/递归闭包/validate_for_profile 直调/矩阵与账本差分/导出脚本），基线+新模块即应绿；正控（distributed 宿主放行、非挂起节点不闸、approval 本地 start 挂起）两世界皆绿=闸不误伤。
数据：`data/r103-red-unit-entry-runner-wiring.txt` / `data/r103-red-ai-authoring-signal.txt` / `data/r103-red-console-publish-start-gate.txt`。

## 负控格清单（46 格）

**unit 38**：profile 表 7（内置子集/ADR 语义/mode 映射/未知 profile/自定义未知 token/词表再导出同一对象/帧表单源）；递归闭包 7（根层/嵌套 child+孙 child token 累积与 recursive=False/帧路径根→叶/parallel 分支/undeclared implicit/unresolved reference/resolver 钩子）；入口闸 13（normal 任何节点前拒+错误体全键/generator/async 孪生/run 分派 normal 拒 distributed 过/distributed 接受并挂起【验收：approval 在 normal 拒、distributed 跑通】/distributed 不预判作用域闸【守恒 frame boundary】/loop 帧 fix=restructure/local 过 approval 拒 delay missing=[queue]/profile 随子执行继承/自定义 profile ExecutionProfileError/内置四 profile 满足副作用族/子执行不闸 carrier 原样上抛【NS-M3 守恒】/check 非抛报告/warn 旋钮降级）；runner 兜底 4（旁路 execute 前拒+WARNING 副作用 0/distributed 宿主放行→carrier/子流程取根宿主/非挂起节点不闸）；矩阵 6（形状排序全集/账本一致主格/新增 node_type 差分红实例/声明变更差分/声明族覆盖/导出脚本 --check 两模式 rc）。
**console 3**：publish 格（local 档 delay 422 版本保 draft）/ start 格（422 且零执行行）/ 正控 approval 本地 start 挂起。
**plaita-ai 5**：compile(profile) 非致命不改 ok+稀疏键 / distributed missing 空 / @childflow 递归累积 / 未知 profile=编译错误 / list rows requires+suspends。
**守恒改种 3**（既有测试适配入口闸先行语义，docstring 钉明）：`test_issue330::test_conversion_preserves_carrier_as_cause` 与 `test_nested_suspend` 两格——静态闭包看得见的挂起节点在入口即拒（`stage="entry"`，`__cause__` 空），运行期根出口呈现降为兜底（静态闭包看不见的复合节点/warn 路径）。#330 家族+NS-M3 合跑 52P（`data/r103-conserve-330-nsm3.txt`）。

## 门禁（无管道直录 rc，`data/`）

| 门 | 结果 | 文件 |
|---|---|---|
| unit | **9141 passed / 13 skipped，rc=0**（=合并后 main 基线 9103 + 新增 38，零改种丢格） | `r103-gate-unit.txt` |
| console | **1615 passed / 14 skipped，rc=0**（收集总数 1629 = 合并前 main 基线 1626 + 新增 3；CONSOLE_BASELINE_NOTE） | `r103-gate-console.txt` |
| integration | 213 passed / 1 skipped，rc=0（与 r102 同数守恒） | `r103-gate-integration.txt` |
| e2e | 74 passed / 3 skipped，rc=0（与 r102 同数守恒） | `r103-gate-e2e.txt` |
| plaita-ai | **301P/38S + 1F 在案基线同红**（`test_mcp_stdio_smoke`：本机 venv 无 `mcp` 包，同进程先跑的 stub 注入翻转 `_REAL_MCP` 守卫、子进程真导入失败→空响应；基线树全量**同红同签名**，单文件跑=skip；CI 的 plaita-ai job 装真 mcp 不受影响） | `r103-gate-plaita-ai.txt` / `-newfile.txt` |
| ruff | CI 硬门 `ruff check plaita tests --select E722` rc=0；BLE001（`--exit-zero` 咨询）改动 12 既有文件基线 **23 = 工作树 23**（零新增），5 新文件 All checks passed | `r103-gate-ruff-hard.txt` / `r103-gate-ruff-ble001-compare.txt` |
| mkdocs | `--clean --strict` rc=0（INFO 锚点提示为基线噪音） | `r103-gate-mkdocs.txt` |
| 导出脚本 | `--check` rc=0「与账本一致（24 node types）」；`--json` 可读 | 行内 |
| 大仓 doctor | 49/52（3 在案既有红；capability-matrix 待并入翻绿） | 大仓 `5bbed10` |

## 边界与遗留（按 #331 边界节）

- 不做发布门/simulate/manifest 平行词表；`model_validate` 默认行为零改动。
- `dry_run_safe` 推导化（A2 §1 存量平行词表）与 dry-run/schedules 调用点（A2 §2.2/2.3 辅）留后续单；治理文档 §5 保持裁决。
- Python↔Go 对拍：`--json` 导出即锚；Go 侧 `pkg/ir/capability.go` 消费属 frozen 维护线（ADR-001），不在本仓动。
- `SUSPEND_GUARD` 旋钮 2026-11-30 到期删除时须同删 ③④ 两处降级分支（`capabilities.py` `validate_for_profile`/`assert_node_executable`）。
