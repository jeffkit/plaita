# #213 证明：quick-test 挂起发布桥复制 ContextVar（#199/#212 同根第三处）

- 单号：plaita#213（P2）
- 基线：`origin/main` = `d2aa259`（issue 基线 `05f588f`；`cluster.py` 自那时起
  该分支无变更）
- 边界：只动 `plaita-console/backend/api/cluster.py` 的 submit 桥；不改
  `plaita/core/async_utils.py`，不改 EventBus 租户语义（#191 范围）。

## 根因（亲读）

`plaita-console/backend/api/cluster.py:1082-1093`（修复前）：

```python
loop = asyncio.get_event_loop()
if loop.is_running():
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(asyncio.run, publish_event())   # ← 不传播 contextvars
        event_id = future.result(timeout=5)
else:
    event_id = asyncio.run(publish_event())
```

`ThreadPoolExecutor.submit` 不复制调用线程的 contextvars。quick-test 的
`event` 分支在 async 请求上下文里执行；发布协程在新线程里读到
`current_tenant() == "default"`、fence 为默认值，EventBus 发布与按租户路由的
存储可能落到 default 命名空间。同族的 #199（`run_async_from_sync`）与
#212（`async_gen_to_sync`）已在 `async_utils` 修复，本处是 console 侧残留。

## 修法

在**调用线程** `contextvars.copy_context()`，worker 内用 `ctx.run` 驱动
`asyncio.run`（Context 不能在另一线程进入）；保留原有 5s `result(timeout=5)`
与无 loop 分支：

```python
import contextvars
ctx = contextvars.copy_context()
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(ctx.run, asyncio.run, publish_event())
    event_id = future.result(timeout=5)
```

与 #199 的 `pool.submit(ctx.run, asyncio.run, coro)` 同口径；未复用
`run_async_from_sync` 是为了保留本处的 5s 超时（该桥无超时参数）。

## 测试

`plaita-console/backend/tests/console/test_issue213_quick_test_context.py`：

- 在 `asyncio.run(main())` 的请求上下文里 `set_current_tenant("tenant-b")` +
  `_FENCE.set("fence-b")`，调用真实的 `run_quick_test(QuickTestRequest(
  test_type="event"))`；用 fake `FlowExecution.run` 让首步挂起、resume 后结束，
  用 `_RecordingBus` 在 `publish` 协程内记录：
  `current_tenant()`、fence、`credentials_file().name`、`get_credential("shared")`；
- 断言 publish 内 tenant-b / fence-b / `creds.tenant-b.json` /
  `https://tenant-b.example`，且调用方 ContextVar 未被改写；
- default 租户正控：tenant/fence/file/default 值均不变。

## 负控

只把桥还原成 `executor.submit(asyncio.run, publish_event())`（保留
`copy_context` 行不动，隔离到 submit 面）：

```
FAILED test_quick_test_publish_sees_caller_tenant_and_fence
FAILED test_quick_test_publish_default_tenant_unchanged
2 failed in 0.64s
```

精确红文案（全文 `docs/proofs/data/213-negative-control.txt`）：

```
AssertionError: quick-test 发布协程丢了租户 ContextVar（#213）；读到 default
说明 submit 未在 copy_context 下运行
AssertionError: assert 'no-fence' == 'fence-default'
```

还原后：**2 passed**。

## 验收记录

- 新增 #213 用例：**2 passed**；
- #199/#212 同族回归：`tests/unit/test_issue199_async_bridge_context.py` +
  `tests/unit/test_issue212_async_gen_context.py` **11 passed**（`async_utils` 未改）；
- console 全套：**518 passed / 13 skipped**（含 #214 的 9 条、#213 的 2 条，
  以及 A 席 #214/#215 补件的 `test_enterprise` / 凭据用例）；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**7846 passed / 4 failed / 11 skipped / 3429 subtests**。4 条失败
  在 clean 基线上逐条相同（本机环境面，与本次 console diff 无关）：
  `test_sandbox_release_on_terminal.py` ×3（本机安装的 `plaita-nodes` 缺
  `plaita_nodes.sandbox`）、`test_issue159_policy_url_exception_sinks.py` ×1
  （yarl 错误文案差异）。本 diff 只改 `cluster.py` 的 console 分支，unit 不导入
  该模块；失败集合与未改前一致；
- landing gate：`d795b3c` conclusion=**success**；
- 证明数据：`docs/proofs/data/213-negative-control.txt`。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #213`。
- 验收面：unit 7846P/4F（4F 为 clean 基线同款环境面）；console 518P/13S；
  e2e+integration 287P/4S。
- landing gate：绿（base `d795b3c` CI success）。

## 残面补充（2026-10-19）

同一桥的**超时语义**残面已单独修复：`with ThreadPoolExecutor` 的
`shutdown(wait=True)` 会吞掉 5s 超时。见
`docs/proofs/213b-quick-test-publish-timeout.md`。
