# #225 证明：`register_code_node(allowed_backends=...)` 解析期硬闸回归

- 单号：plaita#225（P1，security/test）
- 基线：`origin/main` = `9c8820a`
- 边界：**纯测试**——不改 `register_code_node` API、`_BACKENDS` 档位表、执行器、
  沙箱策略；`git diff` 仅新增测试与证明。

## 缺口（按 issue 卡片）

`plaita/node/code.py::CodeNode.validate_code_node` 的
`if allowed is not None and data["sandbox_backend"] not in allowed:` 是运营者
`allowed_backends` 的**唯一**解析期硬闸（执行路径不再复查）。流程作者若能把
逐节点 `sandbox_backend` 覆盖成 `subprocess`（无 FS/网络隔离）或 `unsafe`
（raw exec），即越过运营者安全边界。

## 新增测试

`tests/unit/test_issue225_allowed_backends_regression.py`（5 条）：

1. `register_code_node(default_backend="restricted", allowed_backends=("restricted",))`
   后，`CodeNode(sandbox_backend="subprocess")` → `ValidationError`；
2. 同上，`sandbox_backend="unsafe"` → `ValidationError`；
3. 默认后端在白名单外（`default_backend="subprocess"` + 仅放行 restricted）时，
   未显式声明后端也必须被拒（防止「不写 backend 就走默认」的绕过）；
4–5. 正控：白名单内 `restricted`（显式/默认）可用；未设置 `allowed_backends`
   时历史行为不变（`unsafe` 可构造）。
- 每条断言命中**安全闸文案**（点名被拒后端、运营者白名单、
  `register_code_node(allowed_backends=` 设置入口），不是泛化的 pydantic 错误；
- fixture 快照/恢复 `_ALLOWED_SANDBOX_BACKENDS` / `_DEFAULT_SANDBOX_BACKEND` /
  `_ALLOWED_LANGUAGES`（卡片要求，防跨用例污染）；注册用独立 `NodeRegistry`，
  不污染进程级默认 registry。

## 负控

把 `code.py` 的白名单条件改为 `if False:`：

```
FAILED test_operator_allowlist_rejects_flow_declared_backend[subprocess]
FAILED test_operator_allowlist_rejects_flow_declared_backend[unsafe]
FAILED test_default_backend_outside_allowlist_is_rejected
3 failed, 2 passed in 0.27s
```

精确红文案（全文 `docs/proofs/data/225-negative-control.txt`）：

```
Failed: DID NOT RAISE <class 'pydantic_core._pydantic_core.ValidationError'>
```

还原后 **5 passed**。issue 指定的「其余 code 测试保持绿」：
`test_code.py + test_code_default_backend.py + test_code_extended.py` =
**66 passed / 6 skipped**（变异下）。

## 对 issue 前提的一处诚实修正

issue 正文称 `grep -rn allowed_backends tests/` 零命中 / **零回归防护**。
实测在基线 `9c8820a` 上，既有 `tests/unit/test_issue22_sandbox_allowlist.py`
**已经**从 worker / CLI 入口间接覆盖了 `unsafe`：同一变异下该文件
**2 failed / 4 passed**（`test_worker_entry_rejects_unsafe_at_parse`、
`test_cli_build_really_enforces_backend_allowlist`）。所以「边界完全无防护」
不成立；真正的缺口是：

- `register_code_node(allowed_backends=...)` 的**直接 API 面**无用例；
- `subprocess` 档未覆盖（既有只走 `unsafe`）；
- 「默认后端落在白名单外」的路径未覆盖。

本文件的 5 条用例补的是这三个面；负控同时显示新用例与既有 #22 用例都会红，
#22 的间接覆盖保持原样（未改动）。

## 验收记录

- #225 定向：**5 passed**；负控 **3 failed / 2 passed**；还原后 5 passed；
- `test_code*` 三件套（负控下）：**66 passed / 6 skipped**；
- console 全套：**530 passed / 13 skipped**；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**7908 passed / 4 failed / 11 skipped / 3434 subtests**。4 条失败
  为 clean 基线同款环境面，与本次纯测试 diff 无关：
  `test_sandbox_release_on_terminal.py` ×3（本机安装的 `plaita-nodes` 缺
  `plaita_nodes.sandbox`）、`test_issue159_policy_url_exception_sinks.py` ×1
  （yarl 错误文案差异）；
- landing gate：`9c8820a` conclusion=**success**；
- 产品代码：无改动（`git diff` 仅测试与 `docs/proofs/`）。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #225`。
- 验收面：unit 7908P/4F（4F 为 clean 基线同款环境面）；console 530P/13S；
  e2e+integration 287P/4S。
- landing gate：绿（base `9c8820a` CI success）。
