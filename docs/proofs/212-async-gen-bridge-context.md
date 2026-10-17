# RED-PROOF — #212 `async_gen_to_sync` worker 复制 contextvars（lazy/sync 桥）

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `05f588f` · 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| lazy worker 桥 | 调用线程 `copy_context()`，整个 `_drive` 在 `ctx.run` 下执行；tenant/fence 在 async generator 内可见 |
| 修复前/后 | tenant `default → tenant-b`、fence `no-fence → fence-b`、凭据文件 `creds.json → creds.tenant-b.json`、shared `default.example → acme.example` |
| 双租户凭据 | 懒桥内 `get_credential("shared")` 读 tenant-b 旁文件；default-only 对 tenant-b 抛 `CredentialError` |
| #70 语义 | stop/queue、提前 close、worker join 逻辑未改 |
| #199 / #58 | eager 桥用例与节点线程用例保持全绿（`async_gen_to_sync` 是另一条 worker 路径） |
| 负控 | 还原裸 `threading.Thread(target=_drive)` ⇒ 新文件 **3 failed / 2 passed**（租户/凭据/default 三条红；copy 隔离、无 loop 两条绿） |
| 验收面 | unit 快车道 **7735 passed / 3429 subtests**；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`ci-gate.sh` **PASSED**（8035 passed / 覆盖率 93.83%）；`landing_gate.py ci` **success** |
| 同族关系 | #199 修 eager `run_async_from_sync`；#212 修 lazy `async_gen_to_sync` worker；#213 console raw submit 按边界另单，不混入 |

## §1 缺陷

`async_gen_to_sync` 在已有 running loop 时把 async generator 交给 worker 线程
`_drive`（`plaita/core/async_utils.py:191-243`）；`threading.Thread(target=_drive)`
不传播 contextvars。lazy/sync 路径（`drive_strategy(lazy=True, sync=True)` →
`_drive_lazy_sync` → `async_gen_to_sync`）在异步请求内跑 sync flow 时：

- `current_tenant()` / fence ContextVar 退回默认值；
- `credentials_file()` 指向 default 凭据文件——整条 lazy flow 读 default 租户
  凭据（跨租户串用）。

#199（`05f588f`）只修了 eager 桥 `run_async_from_sync`；#212 是同一
`async_utils` 桥接面里**另一条** worker 路径。

## §2 修复

`plaita/core/async_utils.py` 的 running-loop 分支：

```python
ctx = contextvars.copy_context()

def _drive():
    ...

worker = threading.Thread(target=ctx.run, args=(_drive,), daemon=True)
```

- `copy_context()` 在**调用线程**执行；`Context` 不能并发进入，worker 是唯一
  进入者。
- 整个 `_drive`（含 `loop.run_until_complete` 创建的 task）在调用方 context
  副本里运行；每个 `__anext__` 的 task 继承租户 / fence ContextVar。
- 无 running loop 分支仍在当前线程 `run_until_complete`，context 本就正确，
  不变。
- #70 的 `_stop` / queue / 提前 close / `worker.join(timeout=5)` 语义未改。
- `copy_context` 是隔离副本：生成器内改 ContextVar 不回写调用方。

## §3 修复前 / 修复后

探针：`docs/proofs/data/212-repro.py`。修复前在干净 worktree `05f588f` 跑
（`PYTHONPATH=/tmp/wt212-before`），修复后在本树跑；两边都用
`plaita.__file__` 核过。机读结果：`docs/proofs/data/212-before-after.json`。

| 懒桥内观测 | 修复前 | 修复后 |
|---|---|---|
| `current_tenant()` | `default` | `tenant-b` |
| fence ContextVar | `no-fence` | `fence-b` |
| `credentials_file().name` | `creds.json` | `creds.tenant-b.json` |
| `get_credential("shared")["url"]` | `https://default.example` | `https://acme.example` |
| `get_credential("default-only")` | `READ_OK` | `CredentialError`（default 文件对 tenant-b 不可见） |

## §4 验收

### §4.1 定向

```
pytest tests/unit/test_issue212_async_gen_context.py -q  ⇒ 5 passed
pytest tests/unit/test_issue212_async_gen_context.py \
       tests/unit/test_issue199_async_bridge_context.py \
       tests/unit/test_async_utils.py \
       tests/unit/test_runner_tenant_context.py -q  ⇒ 44 passed
```

覆盖：tenant/fence 可见、双租户凭据旁文件、default 不变、copy 隔离、无 loop；
`#199` eager 桥与 `#58` 节点线程用例同跑全绿。

### §4.2 unit 快车道

```
pytest tests/unit -m "not slow" -q ⇒ 7735 passed, 11 skipped, 13 deselected,
3429 subtests passed
```

### §4.3 e2e / integration

```
pytest tests/e2e -q          ⇒ 74 passed, 3 skipped
pytest tests/integration -q  ⇒ 213 passed, 1 skipped
```

### §4.4 本地门禁与 landing gate

- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8035 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.83%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `05f588f`
  conclusion=success，放行。

## §5 负控（变异纪律）

在 `467c67e` 的临时 worktree 中还原裸 worker：

```diff
-        ctx = contextvars.copy_context()
 ...
-        worker = threading.Thread(target=ctx.run, args=(_drive,), daemon=True)
+        worker = threading.Thread(target=_drive, daemon=True)
```

结果：

```
tests/unit/test_issue212_async_gen_context.py ⇒ 3 failed, 2 passed
```

- **红**：`test_lazy_bridge_propagates_tenant_and_fence_contextvars`、
  `test_lazy_bridge_credentials_read_tenant_side_file`、
  `test_lazy_bridge_default_tenant_unchanged`（读到 default / fence 丢失 /
  凭据文件退回 creds.json）；
- **绿**（不依赖跨线程复制）：`test_lazy_bridge_context_is_isolated_from_generator_mutations`
  （worker 空上下文里改为 tenant-x，调用方仍 tenant-b）、
  `test_no_loop_lazy_path_keeps_context`。

变异 diff 与红名单：`docs/proofs/data/212-negative-control.diff` /
`212-negative-control.txt`；SHA256 见 `212-before-after.json`。

## §6 与 #199 / #213 的关系

- **#199**：`run_async_from_sync` 的 running-loop 分支（eager）——已修；
  本单是 `async_gen_to_sync` 的 worker 分支（lazy/sync），两条独立路径共用
  `async_utils` 桥接面。
- **#213**：console `cluster.py:1087` 的 raw `executor.submit(asyncio.run,
  publish_event())` 是同族但**不在 async_utils**；按边界另单，不混入本 commit。
- **#58**：节点线程的 `copy_context` 修复不动；`test_runner_tenant_context.py`
  全绿。

## §7 复跑

```bash
# 修复前/后探针
PYTHONPATH=/tmp/wt212-before .venv/bin/python docs/proofs/data/212-repro.py
PYTHONPATH=.                .venv/bin/python docs/proofs/data/212-repro.py

# 定向
.venv/bin/python -m pytest tests/unit/test_issue212_async_gen_context.py -q
.venv/bin/python -m pytest tests/unit/test_issue212_async_gen_context.py \
    tests/unit/test_issue199_async_bridge_context.py \
    tests/unit/test_async_utils.py tests/unit/test_runner_tenant_context.py -q

# 验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita/core/async_utils.py`：`async_gen_to_sync` running-loop 分支
  `copy_context` + `ctx.run(_drive)`；
- `tests/unit/test_issue212_async_gen_context.py`：5 条性质/凭据/隔离/无-loop 测试；
- `docs/proofs/data/212-repro.py` / `212-before-after.json` /
  `212-negative-control.{diff,txt}`：机读旁证。
