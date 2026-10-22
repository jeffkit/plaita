# r69 · #297 + #298 修复记录（2026-10-22，B1 修复席）

- 基线：`origin/main` `eb08fd0`（#43/r75 确定性错误集、B2 r70 均已并入）。
- 面：① runner 信封只包 `run()` 函数体（模块级确定性错误烧满 retryTimes）；
  ② supervisor 孤儿守卫起点在 `save_version` 响应校验之后（落库成功 + 响应
  异常产生无主孤儿版本）。
- 负控证据：`docs/proofs/data/r69-297-298-negative-control.txt`（探针原文 +
  定向 pytest pre/post）。

## #297：模块级代码纳入 runner 信封

**根因**：subprocess/docker 共用的 `_RUNNER_TEMPLATE` 把用户代码原样嵌在
模块级，`try` 只从 `_result = run(_input)` 开始。顶层 `raise`/表达式
`TypeError`/未定义名发生在 try 之外 → runner 非零退出、stdout 无信封 →
`run_python_subprocess`/`_run_docker_container` 走「无信封非零退出」分支抛
裸 `RuntimeError`（无 `origin_type`）→ `is_retryable_error` 按基础设施缺省
判可重试 → 烧满 `retryTimes`，顶层副作用重复执行。顶层 `print` 后抛异常
更糟：stdout 非空但无信封，诊断退化为 `not valid JSON`，真实 traceback 丢失。

**修法（两处）**：

1. `_RUNNER_TEMPLATE` 安装 `sys.excepthook`：模块级未捕获异常写成与
   `run()` 函数体同一形状的 `{"ok": false, "type": ..., "error": ...}` 信封
   （flush 后 `os._exit(1)`）；用户代码正常走完后立刻还原默认 hook，runner
   自身尾巴（stdin 非 JSON 等）的失败维持基础设施级可重试缺省。用户源码
   不做缩进/字符串包裹（保持模块级语义与多行字符串原样）。
2. `_decode_runner_output` 在整段 `json.loads` 失败时回退到
   `_extract_trailing_envelope`：从 stdout 最后一个 `{"ok"` 候选位置
   `raw_decode`，且要求信封之后只剩空白——只认**末尾**信封，信封前的
   `print` 不再导致「not valid JSON」；没有合法末尾信封的脏输出仍是
   基础设施级失败（无 `origin_type`、可重试），不做文本嗅探。

**两档终态**：subprocess/docker 与 unsafe 对同一顶层 `TypeError` 都得到
`TypeError: module-level` 具名终态，真实执行一次（探针计数 4 → 1）。

**边界（不改）**：`SyntaxError` 保存期 `ast.parse` 即拒绝、运行期不可达；
类型重命名子类（`class RenamedTypeError(TypeError)`）仍按信封类型名字面集
分类（r75 已记录的 name-vs-structure 边界）；`ImportError`/
`ModuleNotFoundError` 不在确定性集、两档仍可重试；顶层 `sys.exit()` 走
CPython 的 `SystemExit` 特殊处理、不经 `sys.excepthook`，`os._exit()` 直接
终止进程——两者都不产生信封（与修前同形，非本单裁决集）。JS runner 模板
未改（本单只报 Python 信封；JS 信封不在确定性集）。

## #298：孤儿守卫起点前移到 save 调用

**根因**：`evaluate_proposal` 的 A2R46-01 守卫 `try` 从 `evaluate()` 才开始；
`save_version` 与响应校验在守卫之外。服务端已落库但响应缺 `version`（或
连接在读取响应时断开）→ `ConsoleClientError` 从守卫外穿出 →
`run_loop` 兜底 outcome 不带 `candidate_version` → 库里留下无主孤儿版本
（实测 `versions_in_lib=["1.0.0","1.0.1"]`、outcome candidate_version=null）。

**修法**：守卫起点前移到 `save_version` 调用本身：

- 正常路径：响应 `version` 立即写入 `result["candidate_version"]`（原语义）；
- 响应异常路径：`_recover_persisted_version` 回查版本库，只在「save 前快照
  中不存在 + `get_version` 的 definition 与本次提交逐字一致」时认领，取最高
  semver；认领成功则 `failed_iteration` + `candidate_version` + WARNING
  `version kept as lineage`；
- 未落库 / 无法无歧义认领（查询失败、只有并发写入者的不同定义版本）：
  原异常照常抛出，不猜号、不虚构谱系。

**残余边界（诚实记录）**：恢复依赖版本列表/单版本 GET 可用；若恢复查询本身
也不可用，则无法得知服务端分配的号，原异常穿出（与修前同形）。这是信息
不可得的硬边界——不按本地算出的号猜测，避免把谱系指到并发写入者的版本。

## 负控（先红后绿）

| 面 | pre | post |
|---|---|---|
| #297 顶层 TypeError/ValueError/NameError（retryTimes=3） | 4 次执行，`RuntimeError: Subprocess exited...` | 1 次，`SandboxEnvelopeError: TypeError: ...` |
| #297 顶层 print 后 raise | 4 次，`not valid JSON: 'hello\n'` | 1 次，`TypeError: after-print` |
| #297 定向测试 | 8 failed / 2 passed / 1 skipped | 10 passed / 1 skipped |
| #298 W5a 响应缺 version | 1.0.1 落库、outcome candidate_version=null | outcome candidate_version=`1.0.1` + WARNING |
| #298 W5b 响应读取断连 | 同上 | 同上 |
| #298 定向测试 | 2 failed（KeyError candidate_version）/ 2 passed | 4 passed |
| 守恒 | — | 未落库不虚构候选号；并发写入者不同定义不认领（pre/post 均绿） |

## 验收

- 全量 unit `-m "not slow"`：**8836 passed / 13 skipped / 14 deselected /
  3501 subtests**，rc=0。
- e2e + integration：**287 passed / 4 skipped**，rc=0。
- console（`plaita-console/backend/tests/console`）：**1215 passed / 13
  skipped**，rc=0。
- plaita-ai（`uv run --isolated --extra all pytest`）：**329 passed / 13
  skipped / 12 subtests**，rc=0。
- 定向：`test_issue297_runner_module_scope.py` + r75 套件 **41 passed / 1
  skipped**；`plaita-ai/tests/test_supervisor.py` **25 passed**。
- `ci-gate.sh layering|wheel|size` 三相位 PASS；`mkdocs build --strict` rc=0；
  ruff 新增代码零 BLE001/E722（9 条 BLE001 均为存量捕获点）。
- `landing_gate.py ci` rc=0（`eb08fd0` main CI + Console E2E 双绿）。
- docker 实弹在本机因无 `python:3.12-slim` 镜像跳过；docker 与 subprocess
  共用同一 runner 模板、同一 `_decode_runner_output(origin_typed=True)`
  路径，模板/解码行为由 subprocess 实弹与单测覆盖。

## 文档同步

- `docs-site/docs/distributed/ops-runbook.md`：确定性错误节明确信封覆盖
  「run() 函数体与模块级顶层代码」、print 污染不影响识别，并记录 #297 前
  的行为（烧 retryTimes / 重复顶层副作用）；基础设施失败措辞改为「无合法
  信封」。
- `docs-site/docs/guide/error-handling.md`：沙箱信封段同步覆盖范围与末尾
  信封识别。
- `MIGRATION.md`：新增「信封覆盖范围（plaita#297）」迁移注记（受影响=
  顶层初始化/副作用依赖重试的存量 flow）。
- `plaita-ai/plaita_ai/supervisor.py`：`evaluate_proposal` docstring 记录
  save 响应丢失窗口与恢复认领口径。
