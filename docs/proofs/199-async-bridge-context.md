# RED-PROOF — #199 `run_async_from_sync` 跨线程桥复制 contextvars

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `95bd64c`（#199 修复前；`95bd64c` 仅给 perf 测试加 timeout marker，未动异步桥）· 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| running-loop 桥 | 调用线程 `copy_context()` + worker 内 `ctx.run(asyncio.run, coro)`；租户/fence ContextVar 在协程内可见 |
| 修复前/后 | tenant `default → tenant-b`、fence `no-fence → fence-b`、凭据文件 `creds.json → creds.tenant-b.json`、shared `default.example → acme.example` |
| 双租户凭据 | 桥内 `get_credential("shared")` 读到 tenant-b 旁文件；default-only 对 tenant-b 抛 `CredentialError` |
| #58 回归 | `test_runner_tenant_context.py` 全绿（节点线程修复不动） |
| 负控 | 还原裸 `pool.submit(asyncio.run, coro)` ⇒ 新文件 **3 failed / 3 passed**（红的是租户/凭据断言，绿的是隔离/无-loop/异常传播） |
| 验收面 | unit 快车道 **7730 passed / 3429 subtests**；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`ci-gate.sh` **PASSED**（8030 passed / 覆盖率 93.82%）；`landing_gate.py ci` **success** |
| 同族残面 | `async_gen_to_sync` worker 与 console `cluster.py:1087` raw submit 仍丢 context；按边界另开单，不混入本 commit |

## §1 缺陷

`run_async_from_sync` 在已有 running loop 时把协程丢给
`ThreadPoolExecutor.submit(asyncio.run, coro)`（`plaita/core/async_utils.py:46-52`）。
`submit` 不传播 contextvars——新线程只有默认上下文，于是：

- `current_tenant()` 退回 `default`；
- `credentials_file()` 指向历史凭据文件，非 default 租户的 sync flow 读
  default 租户凭据（跨租户串用）；
- 任何租户/fence ContextVar 的消费方在桥后失效。

#58 修的是 `runner._run_sync_node` 的**节点线程**（`copy_context`），没有覆盖
这条**整条 flow** 的 sync→async 桥。

## §2 修复

`plaita/core/async_utils.py` running-loop 分支：

```python
ctx = contextvars.copy_context()
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
    return pool.submit(ctx.run, asyncio.run, coro).result()
```

- `copy_context()` 必须在**调用线程**执行；`Context` 不能进入另一个线程后
  再被并发进入。
- worker 的 `ctx.run(asyncio.run, coro)` 在调用方 context 副本里创建新的
  event loop / task；协程树继承租户与 fence ContextVar。
- `copy_context` 是隔离副本：协程内改 ContextVar 不回写调用方。
- 无 running loop 时仍走 `asyncio.run(coro)`（同一线程，context 本就正确）。
- 异常/超时契约不变：`ctx.run` 的异常经 `.result()` 原样抛回；桥本身不加
  墙钟超时（节点/flow 级超时仍由上层承担）。

## §3 修复前 / 修复后

探针：`docs/proofs/data/199-repro.py`。修复前在干净 worktree `95bd64c` 跑
（`PYTHONPATH=/tmp/wt199-before`），修复后在本树跑；两边都用
`plaita.__file__` 核过。机读结果：`docs/proofs/data/199-before-after.json`。

| 桥内观测 | 修复前 | 修复后 |
|---|---|---|
| `current_tenant()` | `default` | `tenant-b` |
| fence ContextVar | `no-fence` | `fence-b` |
| `credentials_file().name` | `creds.json` | `creds.tenant-b.json` |
| `get_credential("shared")["url"]` | `https://default.example` | `https://acme.example` |
| `get_credential("default-only")` | `READ_OK` | `CredentialError`（default 文件对 tenant-b 不可见） |

## §4 验收

### §4.1 定向

```
pytest tests/unit/test_issue199_async_bridge_context.py -q  ⇒ 6 passed
pytest tests/unit/test_async_utils.py \
       tests/unit/test_runner_tenant_context.py \
       tests/unit/test_issue199_async_bridge_context.py -q  ⇒ 39 passed
```

覆盖：租户/fence 可见、双租户凭据旁文件、default 不变、copy 隔离、无 loop
路径、异常传播。

### §4.2 unit 快车道

```
pytest tests/unit -m "not slow" -q ⇒ 7730 passed, 11 skipped, 13 deselected,
3429 subtests passed
```

### §4.3 e2e / integration

```
pytest tests/e2e -q          ⇒ 74 passed, 3 skipped
pytest tests/integration -q  ⇒ 213 passed, 1 skipped
```

### §4.4 本地门禁与 landing gate

- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8030 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.82%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `95bd64c`
  conclusion=success（`95bd64c` 只给 perf 预算测试加 `timeout(900)` marker，
  不改 #199 桥的判据），放行。

## §5 负控（变异纪律）

在 `2dcced8` 的临时 worktree 中还原裸 submit：

```diff
-        ctx = contextvars.copy_context()
         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
-            return pool.submit(ctx.run, asyncio.run, coro).result()
+            return pool.submit(asyncio.run, coro).result()
```

结果：

```
tests/unit/test_issue199_async_bridge_context.py ⇒ 3 failed, 3 passed
```

- **红**（精确文案是“读到 default 而非 tenant-b / fence 丢失 / 凭据文件退回
  creds.json”）：
  `test_bridge_propagates_tenant_and_fence_contextvars`、
  `test_bridge_credentials_read_tenant_side_file`、
  `test_bridge_default_tenant_unchanged`；
- **绿**（不依赖跨线程复制）：`test_bridge_context_is_isolated_from_coroutine_mutations`
  （协程在空上下文里改为 tenant-x，调用方仍 tenant-b——隔离语义在裸 submit
  下也成立）、`test_no_loop_path_keeps_context`、`test_bridge_still_propagates_exceptions`。

变异 diff 与红名单：`docs/proofs/data/199-negative-control.diff` /
`199-negative-control.txt`；SHA256 见 `199-before-after.json`。

## §6 同族残面（不在本 commit）

- **`async_gen_to_sync` worker**：lazy/sync 路径的 worker 线程同样不复制
  contextvars。实测（`fence` + tenant）：主线程 `('tenant-b','fence-b')`，懒桥
  读到 `('default','no-fence')`。修复方向同本单：调用线程 `copy_context()`，
  `threading.Thread(target=ctx.run, args=(_drive,))`。按边界另开单。
- **console `plaita-console/backend/api/cluster.py:1087`**：raw
  `executor.submit(asyncio.run, publish_event())` 是同类桥，未复制 context。
  按任务边界（console 侧另开单）不在本 commit。

## §7 复跑

```bash
# 修复前/后探针
PYTHONPATH=/tmp/wt199-before .venv/bin/python docs/proofs/data/199-repro.py
PYTHONPATH=.                .venv/bin/python docs/proofs/data/199-repro.py

# 定向
.venv/bin/python -m pytest tests/unit/test_issue199_async_bridge_context.py -q
.venv/bin/python -m pytest tests/unit/test_async_utils.py \
    tests/unit/test_runner_tenant_context.py -q

# 验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita/core/async_utils.py`：running-loop 分支 `copy_context` + `ctx.run`；
- `tests/unit/test_issue199_async_bridge_context.py`：新增 6 条性质/凭据/隔离/无-loop/异常测试；
- `docs/proofs/data/199-repro.py` / `199-before-after.json` /
  `199-negative-control.{diff,txt}`：机读旁证。
