# #247 证明：#241 租约内重读必须刷新 flow_version / flow / hash

- 单号：plaita#247（P1，状态机/身份一致性）
- 基线：`origin/main` = `d9f4567`（含 #241/#242/#243）
- 边界：只动 resume 租约内重读后的 `version` / `flow` / `current_flow_hash`
  刷新；#241 的 load→acquire 顺序与复核主体、#243 的补派租约复用、
  #242 DLQ、#33 suspended 短路均不回退。

## 根因

`resume_flow` 在租约**之前**用预读 state 算出：

```python
version = state.flow_version
flow = self.get_flow_definition(flow_id, version)
current_flow_hash = self.get_flow_definition_hash(flow_id, version)
```

#241 的租约内复核只替换了 `state`，没有重算这三个派生值。若 load→acquire
窗口内另一载体把同一 execution 的 `flow_version` 从 1 改到 2（同 flow_id
新版本 / preminted start 回写），复核看到 V2 的 state，却仍用 **V1 的
flow** 执行，并把结果写回 V2 的状态行：节点副作用、checkpoint 与状态身份
不一致。`current_flow_hash` 同样是 V1 的指纹：V2 状态带 V2 hash 时会被
误判 mismatch；带 V1 hash 时又会误放行旧定义。

## 修法

复核返回后、任何状态写/引擎调用之前刷新派生值：

```python
if state.flow_version != version:
    logger.warning("... window flow_version 变更 ...")
    version = state.flow_version
    flow = self.get_flow_definition(flow_id, version)
    current_flow_hash = self.get_flow_definition_hash(flow_id, version)
```

- 版本变了 ⇒ 用新版本的定义与指纹执行；hash 冲突交给下方**既有**
  `classify_flow_hash_change` mismatch 路径：终态化 error + 抛
  `FlowHashMismatchError`，不执行旧定义。
- 版本未变 ⇒ 不进入重算分支，原行为逐字不变。
- 新版本定义缺失 ⇒ 既有 `get_flow_definition` ValueError 路径（可观测
  error），不会静默退回旧定义。

## 测试（4 条，`tests/unit/test_issue247_resume_version_refresh.py`）

按 #240 的三不变量风格：

1. **定义身份**：窗口内 1→2 ⇒ `result == "V2"`（不是 V1）；
2. **状态身份**：落盘 `flow_version == "2"`、`status=completed`、
   `$NODE.end == "V2"`、`$LAST_NODE == "end"`；
3. **指纹身份（放行面）**：V2 状态带 V2 hash ⇒ 按 V2 指纹校验通过并执行
   V2（旧实现会用 V1 hash 误判 mismatch）；
4. **指纹身份（拒绝面）**：V2 状态仍带 V1 hash ⇒ 按 V2 指纹判 mismatch，
   终态 error + `FlowHashMismatchError`，checkpoint 仍是 `start`
   （任何节点执行之前拒绝）；
5. 回归：窗口内版本不变 ⇒ 仍执行 V1，状态不变。

相邻回归实跑：`#241`（completed/suspended 交错）、`#243`（补派复用外层
租约）、`#242`（DLQ suspended）、`#33`（suspended+continue 零租约）、
`#233`（挂起补偿）、`#240`（空 checkpoint retry）、`test_worker_flow_hash.py`
共 **64 passed**。

## 负控

删除复核后的 version/flow/hash 重算：

```
3 failed, 1 passed
E       AssertionError: result V1, expected V2 (or explicit reject)
E       assert 'V1' == 'V2'
...
E       FlowHashMismatchError: flow 定义自启动后已变更 ... (误报，V2 hash 被 V1 指纹校验)
...
E       Failed: DID NOT RAISE FlowHashMismatchError   （旧指纹误放行 V1）
```

精确对应 issue 的三条建议判据；还原修复后 **4 passed**。完整日志
`docs/proofs/data/247-negative-control.txt`。

## 验收

- 新增 4 passed；相邻回归 64 passed；
- 全量 unit（`-m "not slow"`）**8172 passed / 12 skipped / 14 deselected /
  3492 subtests**；e2e+integration **287P/4S**；console **571P/13S**；
  plaita-ai **233P/14S**；layering / wheel / size 三相位全绿；
- 基线 `d9f4567` CI **success**，`landing_gate.py ci` 绿后单 commit 推送
  （`closes #247`）。
