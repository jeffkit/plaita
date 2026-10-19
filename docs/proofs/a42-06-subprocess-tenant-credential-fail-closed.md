# A42-06 证明：subprocess 沙箱对非 default 租户 fail-closed

- 单号：台账第四十二轮 A42-06（P2，条件 P1；#256 续；无 GitHub issue）
- 基线：`origin/main` = `373395e`（含 r33 #256 首版）
- 边界：只改 code 节点 `subprocess` 后端（Python + JS）的租户准入与 runner
  模板；default 语义、`docker` / `unsafe` / `restricted` 后端均不变。

## 根因

#256 首版为解决「ContextVar 不跨进程」，把执行租户（`PLAITA_TENANT_ID`）与
凭据路由（基础文件 + 密钥路径）显式注入子进程 env，并在子进程入口
`set_current_tenant`。它只挡住了「租户 B 经公开 API 静默读到 default 文件」，
但子进程本身**不是**凭据隔离边界：

- 子进程与 worker 同 OS 用户，文件系统不受限；
- 所有租户的凭据文件共用同一把 Fernet 密钥（单密钥文件/`PLAITA_CREDENTIALS_KEY`）；
- 因此租户 B 的用户代码可以：
  1. 按 env 里的 `PLAITA_CREDENTIALS_FILE` / `PLAITA_CREDENTIALS_KEY_FILE`
     直接读文件并解密（`raw_read`）；
  2. `set_current_tenant("default")` 改写路由后走公开 API（`api_read`）。

r42 探针（`docs/proofs/data/a42-06-probe.py`）修复前两种取法都返回
`DEFAULT-ONLY-SECRET`。单纯「不传 env 路径」不够：default 文件与密钥通常就在
worker cwd，子进程仍可相对/绝对路径读取；「父进程解密后经 stdin/临时文件交给
子进程」也绕不开同一文件系统与共享密钥。任何在**同一 OS 用户 + 同一文件系统**
内执行的方案都无法给出跨租户保证。

## 修法（选项 ③：fail-closed 收窄）

`subprocess` 后端在 **spawn 之前**拒绝非 default 租户：

- `_refuse_non_default_tenant_in_subprocess(language)` 抛
  `TenantContextPropagationError`，消息含
  `PLAITA_SUBPROCESS_TENANT_ISOLATION_UNAVAILABLE` 与 `docker` 迁移指引；
  用户代码零执行机会（`Popen` 不会被调用）。拒绝点先打 WARNING
  （tenant/language/marker，不含任何路径），满足「fail-closed 必须可观测」。
- 同时覆盖 Python 与 JS subprocess 后端（共享同一文件系统边界；JS 用户代码
  同样可 `fs.readFileSync` default 凭据）。
- 删除 #256 的首版传播面：`_TENANT_BOOTSTRAP` / `PLAITA_TENANT_ID` /
  `PLAITA_TENANT_SOURCE` 注入、子进程入口 exit 97 分支、runner 模板里的
  租户引导。
- **default 语义不变**：仍然注入 `subprocess_credentials_env()`
  （基础文件 + 密钥路径），并继续剔除任何残留的租户 env 变量。
- 租户级 code 节点改用 `docker` 后端（容器内不挂载宿主凭据）。

## 负控与验证

负控（仅回退 `plaita/node/code.py`）：探针 `raw_read` / `api_read` 均为
`DEFAULT-ONLY-SECRET`，主回归用例 `DID NOT RAISE` 精确红。完整输出见
`docs/proofs/data/a42-06-negative-control.txt`；恢复修复后探针返回具名
`TenantContextPropagationError`，用例 5 passed。

相关套件（修复后）：`#256` 5 passed；code/JS/subprocess/credentials/tenant
定向 210 passed / 6 skipped；console quick-test 4 passed、凭据相关 10 passed；
`mkdocs --strict` rc=0；全量 `ci-gate.sh` **PASSED**（8501 passed / 19 skipped /
3498 subtests，coverage 93.34%，layering / wheel / SC-003 全绿）。
