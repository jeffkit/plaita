# #218 证明：publish_sync 关闭循环前有界排空分发任务

- 单号：plaita#218（P2）
- 基线：`origin/main` = `1a7efa1`（本工作树 `wt-b2`）
- 边界：不改 async `publish` 的 fire-and-forget 语义；不改事件存储/去重/订阅匹配；
  Redis 后端按 listener 实现核对（见文末）。

## 缺陷

`EventBus.publish_sync` 在没有运行中 loop 时走
`run_async_from_sync` → `asyncio.run(self.publish(...))`。
`InMemoryEventBus.publish`（`plaita/event/memory.py`）把
`_dispatch_event` 建成 fire-and-forget task 后立即返回；`asyncio.run`
随即关闭私有循环并取消 pending task ⇒ 慢 async handler **永远不执行**，
且无报错、`_pending_tasks` 已空（事件已入库，订阅方未被唤醒）。

基线复现（`~/.issue-keeper/duty/r7-scripts/r7_diff3.py`，`seen` 为 handler
执行次数）：

```json
{"async": {"seen": 1, "pending": 0},
 "sync":  {"seen": 0, "pending": 0, "error": null}}
```

## 修法

1. `EventBus.publish_sync` 改为驱动 `_publish_and_drain`：先
   `await self.publish(...)`，再在**同一私有循环**里调用后端钩子
   `_drain_publish_sync_tasks()`，然后才让 `asyncio.run` 关闭循环。
   基类钩子默认空操作（base 总线不登记分发任务）。
2. 新增 `EventBus.publish_sync_drain_timeout: float = 5.0`（类属性，可按
   实例/子类覆盖）：排空上限，与 Redis `close()` 的
   `DISPATCH_DRAIN_TIMEOUT_SECONDS` 同口径；超时**不取消**任务，只打
   WARNING，随后由循环自身收尾（不引入 #109 r7 的自取消环）。
3. `InMemoryEventBus._drain_publish_sync_tasks`：轮询式等待
   `_pending_tasks` 中**当前循环**上的在飞任务。按 `task.get_loop()` 过滤，
   避免 `asyncio.wait` 等待别的 loop 的 Task（"attached to a different
   loop"）；每轮 `wait` 后重新取快照，覆盖分发任务再派生子任务的形态
   （retry 分支）；总时长受 `publish_sync_drain_timeout` 约束。
4. `InMemoryEventBus._dispatch_event` 的 retry 分支把
   `asyncio.create_task(_process_with_retry(...))` 也纳入 `_track_task`：
   否则父分发任务先结束、子任务不在快照里，同步收尾仍会静默取消它。

## 测试

新增 `tests/unit/test_issue218_publish_sync_drain.py`（5 例）：

- `test_slow_async_handler_runs_before_publish_sync_returns`：`await
  asyncio.sleep(0.1)` 的 handler，`publish_sync` 返回后 `seen == [event_id]`，
  `_pending_tasks` 为空（issue 验收条件 1/2）；
- `test_retry_policy_handler_runs_before_publish_sync_returns`：带
  `RetryPolicy` 的 handler 同样在返回前执行（父→子任务轮询收敛）；
- `test_never_returning_handler_returns_within_configured_timeout`：handler
  `await asyncio.Event().wait()`，`publish_sync_drain_timeout=0.1`，断言
  handler 已启动、返回耗时 < 3s（绝对预算，只钉不挂死）、登记表清空
  （issue 验收条件 4）；
- `test_slow_async_handler_runs_when_bridge_uses_worker_loop`：
  running-loop 分支（worker 线程 + 新循环）同样排空；
- `test_async_publish_returns_before_slow_handler_finishes`：正控，async
  `publish` 仍在 handler 完成前返回，之后 handler 最终执行
  （fire-and-forget 语义不变）。

新增 `tests/unit/test_issue218_publish_sync_redis_boundary.py`（1 例）：
fakeredis 下注册 handler（listener 在测试循环上），`publish_sync` 原样返回、
不在 publish 循环上建分发任务、不同步运行 listener 侧 handler；随后
listener 最终完成处理。钉住 issue 边界「Redis 侧按其 publish 实现补测」。

## 负控

把 `publish_sync` 回退为直接 `asyncio.run(self.publish(...))`（drain 钩子
与 retry 登记保留），其余不动：

- InMemory 三条核心用例 **FAILED**（`seen == []`）；
- 有界返回用例与 Redis 边界用例仍绿（前者只钉不挂死，后者本就不建任务）；
- 汇总 **3 failed / 3 passed**。

精确红文案（全文 `docs/proofs/data/218-negative-control.txt`）：

```
AssertionError: Lists differ: [] != ['cf8e4989-...']
- []
+ ['cf8e4989-...'] : publish_sync 返回时慢 async handler 未执行（#218 回归）

AssertionError: Lists differ: [] != ['...'] : retry 分支的分发任务未被 publish_sync 排空

AssertionError: Lists differ: [] != ['...'] : running-loop 桥接分支未排空分发任务
```

## 验收（2026-10-18，wt-b2 · HEAD 1a7efa1 + 本单改动）

解释器：`.venv/bin/python`（worktree 自带）。

| 面 | 命令 | 结果 |
|---|---|---|
| unit | `pytest tests/unit -m "not slow" -q` | **7833 passed / 16 skipped / 13 deselected**（0 failed） |
| integration | `pytest tests/integration -q` | **213 passed / 1 skipped** |
| e2e | `pytest tests/e2e -q` | **74 passed / 3 skipped** |
| 本单用例 | `pytest tests/unit/test_issue218_publish_sync_drain.py tests/unit/test_issue218_publish_sync_redis_boundary.py -q` | **6 passed** |
| 负控 | 同上（回退 `publish_sync` 的 drain） | **3 failed / 3 passed**（见上方精确红文案） |

ruff：`plaita/event/memory.py` 有 3 条 BLE001（`except Exception`），在
HEAD `1a7efa1` 原文件上同样存在（行号平移），非本单引入；本单新增代码
（`core.py` 钩子 + `memory.py::_drain_publish_sync_tasks`）无新增 lint 错误。

`docs-site` 的 `mkdocs build --strict` 本机不可跑：两个 venv 均未安装
`docs-site/requirements.txt` 里的 `mkdocs-mermaid2-plugin`（`mermaid2` 导入
失败），与本次文档改动无关；改动仅为 `event-system.md` API 表一行说明。

## 残留观察（不阻塞本单）

`SqlalchemyEventBus.publish`（`plaita/event/sqlalchemy.py`）仍是裸
`asyncio.create_task(self._dispatch_event(...))`，既不登记强引用也没有 drain
钩子；该后端为 experimental 且本单边界只点名 InMemory/Redis，未纳入修复。
若启用该后端，sync 发布仍有同族静默丢失面，建议另开单（登记与
`_drain_publish_sync_tasks` 可照抄 InMemory 实现）。
