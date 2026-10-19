# #213 残面证明：quick-test 发布超时后必须快速失败（不再被 shutdown 等待）

- 单号：plaita#213（P2 残面；前一轮修的是 ContextVar 传播，见
  `docs/proofs/213-quick-test-context.md`）
- 基线：`origin/main` = `786de97`
- 边界：只动 `plaita-console/backend/api/cluster.py` 的事件发布线程桥
  （新增 `_QUICK_TEST_PUBLISH_TIMEOUT`、去掉 `with ThreadPoolExecutor`）；
  ContextVar 复制（#213 已修）与 `mark_tenant_derived`（#239）不变。

## 根因（亲读）

修复前：

```python
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(ctx.run, _run_publish_derived)
    event_id = future.result(timeout=5)
```

`with` 的 `__exit__` 等价 `executor.shutdown(wait=True)`。EventBus 后端
（Redis 等）卡住时：

1. `future.result(timeout=5)` 抛 `concurrent.futures.TimeoutError`；
2. 解释器开始退出 `with`，`__exit__` 等 worker 线程跑完；
3. 请求在 worker 返回前不会失败——5s 超时对请求延迟完全失效；
   若后端永不返回，quick-test 请求永久挂起。

## 修法

```python
executor = ThreadPoolExecutor(max_workers=1,
                              thread_name_prefix="quick-test-publish")
try:
    future = executor.submit(ctx.run, _run_publish_derived)
    try:
        event_id = future.result(timeout=_QUICK_TEST_PUBLISH_TIMEOUT)
    except concurrent.futures.TimeoutError:
        future.cancel()
        raise TimeoutError("事件发布超时（>5s）…") from None
finally:
    executor.shutdown(wait=False, cancel_futures=True)
```

- `cancel()` 清掉尚未开始的任务；运行中的线程 Python 无法强杀，但
  `shutdown(wait=False)` 让调用线程立即返回，请求快速失败；
- 超时异常由 `event` 分支既有的 `except Exception` 转成
  `QuickTestResponse(success=False, message="执行事件驱动测试失败: 事件发布超时…")`；
- 超时常量提为 `_QUICK_TEST_PUBLISH_TIMEOUT = 5.0`，测试可注入而不改语义。

## 测试（2 条，`test_issue213_quick_test_publish_timeout.py`）

- 超时：fake EventBus 的 `publish` 阻塞在 `threading.Event.wait(3s)`，超时
  注入 0.2s；断言请求 <1.5s 返回、`success=False`、message/error 含
  「事件发布超时」、返回时发布线程**仍在阻塞**（`finished` 未置位）、且
  publish 只调用一次（超时异常没有误触发同步回退分支）；
- 正常路径：publish 立即返回 ⇒ quick-test 仍 `success=True`。

## 负控

仅回退 `api/cluster.py`（保留新测试）：**1 failed / 1 passed**——旧实现
等发布线程 3s 跑完并返回 `success=True`，延迟与失败状态断言同时红。见
`docs/proofs/data/213b-quick-test-timeout-negative-control.txt`。

## 验收实跑

- 新文件 **2 passed**；
- #213 原 ContextVar 回归 **2 passed**（未受 shutdown 改动影响）；
- 相邻 8 文件 **65 passed**；console 全套 **590 passed / 13 skipped**；
- ruff 仅既有 BLE001（改动行零新增）。
