# r99 · plaita#337 — SuspendNotExecutableError「终态不重试」两层接线 + 终态落盘身份（B2 第九十九轮）

分支 `fix/issue337-sne-terminal`（自 origin/main `3271ba89`）。issue 基线 5637548c，
main 已前进（#331 W2 `3b9f6b32` 在 issue 提报后落地）——**开工对账实测修正了 issue 反证的有效集**。

## 缺陷（修后于 main 的实测有效集）

| # | 病灶 | 修前实测 |
|---|------|----------|
| ① | `runner.is_retryable_error` 无 SNE 分支——分类单一来源对「具名终态失败」返回 True | 探针 P1：`is_retryable_error(SNE)==True` |
| ② | worker `_is_retryable_node_failure` 家族元组 `(NodeTimeoutError, FlowTimeoutError, FlowCancelledException)` 不含 SNE；包装链（NodeExecutionError 在前）下 A50-03 分支因 ① 失守 | 探针 W1：包装链判可重投 `True` |
| ③ | 终态落盘 `_error_fields` 身份取「节点级异常→最深引擎异常」：runtime 呈现的 SNE 带 `__cause__`=carrier（`FlowErrorType.FLOW_ERROR`），最深者=carrier ⇒ `error_type="flow_error"`；abort 包装在场时=`"node_error"`——稳定字符串丢失，#312 分流无机判锚 | 探针 E1：`'flow_error'`；E2：`'node_error'` |

### issue 反证过期注记（开工对账）

issue 反证一（retryTimes 烧穿）与反证二的一半在**当前 main 已不成立**：`#331 W2`
（`3b9f6b32`）给 `NodeRunner._execute_with_retry` 加了
`except (FlowSuspendedCarrier, SuspendNotExecutableError): raise` 逃逸分支（通用
except 之前），SNE 不再进重试/errorHandler/abort 包装。探针实证（修前树）：

- P2 节点循环：SNE + retryTimes=3 → 执行 1 次、SNE 身份原样穿出（绿）；
- P5 集成（自定义节点函数内 eager 子流程含 event 节点）：外层节点 1 次、内层挂起
  点后零执行、SNE 具名穿出（绿）；
- W2 直连链（FlowErrorException→SNE→carrier）：worker 判不可重投——靠「链上无
  NodeExecutionError ⇒ node_failure_seen=False」缺省 False **侥幸**成立，非按类型。

⇒ 本轮真正落地的是：①分类判据本身、②worker 按类型显式判终态（与超时家族同形，
不再依赖缺省）、③终态落盘身份。行为面（重试烧穿）实际已由 #331 关闭，本轮补齐
的是契约接线与机器可判身份。

## 修法

1. `plaita/core/runner.py` `is_retryable_error`：尾部补 `isinstance(exc, SuspendNotExecutableError)`
   → `False`（先于 #219 校验豁免语义位之后置尾部；校验钩子不产 SNE，终态身份不随
   调用面漂移）。docstring 补 SNE 条目。不动 `_DETERMINISTIC_ERROR_CLASSES` 四类
   （与 Go `pyDeterministicEnvelopeTypes` 逐字对齐面不掺 SNE）。
2. `plaita/server/flow_worker.py` `_is_retryable_node_failure`：家族元组补
   `SuspendNotExecutableError`（与超时/取消同形，任意链位直接 False，防御病态包装链）。
3. `plaita/server/flow_worker.py` `_error_fields`：身份判定**链上 SNE 最优先**，
   其后维持「节点级异常 → 最深引擎异常」原序。

## 负控先红后绿（tests/unit/test_issue330_suspend_not_executable.py::TestTerminalNoRetryWiring）

数据：`docs/proofs/data/r99-prefix-probe.txt`（探针 7 格，rc=1→修后 rc=0 全 PASS）、
`docs/proofs/data/r99-prefix-pytest.txt`（pytest 修前 6F/3P，rc=1）。

| 格 | 断言 | 修前 | 修后 |
|----|------|------|------|
| R1 | `is_retryable_error(SNE) is False` | **红**（True） | 绿 |
| R1b | SNE 子类同判 | **红** | 绿 |
| R2 | 节点循环 retryTimes=3：执行 1 次 + SNE 身份原样 + 零退避 | 绿（#331 逃逸，守恒钉） | 绿 |
| W1 | 包装链 `_is_retryable_node_failure is False` | **红**（True） | 绿 |
| W2 | 直连链同判 | 绿（缺省侥幸，钉成按类型） | 绿 |
| W3 | `resume_flow` 终态化 `NodeFailureTerminalizedError` + status=error + noderetry 键空 | **红**（重投异常+预算键写） | 绿 |
| E1 | `_error_fields(直连链)["error_type"]=="suspend_not_executable"` | **红**（flow_error） | 绿 |
| E2 | `_error_fields(包装链)` 同判 | **红**（node_error） | 绿 |
| I1 | 集成：嵌套 eager 子流程 + retryTimes=3 → 外层 1 次/内层挂起后零执行/SNE 穿出 | 绿（守恒钉） | 绿 |

W1 的链形（abort 包装）在当前 main 已不可由引擎路径产出（逃逸分支拦截），格钉的是
「分类经包装层必须保真」的 A50-03 契约面 + 病态链防御（节点函数吞 SNE 再抛普通
异常的场景，`__cause__` 深处仍能按类型判终态）。

## 守恒

- 目标套件（issue330 全文件 32 格 + issue331 + nested_suspend(NS-M3) + issue33 +
  r50(A50-03) + node_f2 + r75 + issue297 + issue73 + issue123）：
  **203 passed, 1 skipped**（skip 为存量 requires_mark），rc=0。
- 呈现面文案零改动：errors.py / _error_normalization.py 未触碰。
- `_DETERMINISTIC_ERROR_CLASSES`、`DETERMINISTIC_ERROR_TYPES`、#219 标记面零改动。
- #317 决策分类锚：本轮未触 strategies/闸点/决策面代码（Go 对拍锚无 Python 侧改动）。

## 门禁

- ruff：改动两源文件 BLE001 计数 12（基线 origin/main 同=12，零新增）；测试文件全绿。
- mkdocs：未触 docs-site/（仅本 proofs 文档，不入 nav，同 ADR-001 precedent）。
- unit 全量：见 `data/r99-unit-full.txt`（引擎面触碰 → 全量纪律）。
- landing_gate：见下节。

## 残面注记

- `_error_fields` 的 SNE 优先仅落在 **worker 落盘面**；normal 模式根的异常对象本体
  （`run_compatible` 抛出）不受影响（呈现面 #330 已定）。
- 沙箱信封路径的 `origin_type` 分类未动（SNE 不经沙箱后端，in-process 闸直接抛）。
- worker「确定性失败记账」（nofail 键，#73 遗留层）对 SNE 生效：`_node_failure_retry_decision`
  判不可重投后照常 `_record_deterministic_failure` 计数——观测面可见反复 SNE 的执行
  （W3 修后走此路径）。
