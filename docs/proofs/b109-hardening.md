# B109-01/02 + B2 r67 注记证明：轻量硬化批

- 单号：duo-a 第一百零九轮 B109-01/02（b/P3）+ B2 r67 注记
- 基线：`origin/main` = `a75b4ab`
- 边界：前端外部链接 scheme 白闸、日志深链 query 编码、mutation matrix 恒真
  断言；不改后端/状态机。

## B109-01：ExecutionDetail Langfuse href 未校验 scheme

`ExecutionDetail.tsx` 直接 `href={execution.langfuse_trace_url}`。后端字段
若被污染为 `javascript:`/`data:`，会渲染可点击危险链接。

修法：`services/transport.ts` 新增 `safeExternalUrl`（`new URL` 解析，仅
`http:`/`https:` 放行，其余/相对/畸形返回 null）；ExecutionDetail 只在
非 null 时渲染 `<a>`，否则降级为纯文本 `<span>`（title 说明 scheme 不支持）。

## B109-02：Cluster 日志 href 字符串拼接

`href={`${SUB_PATH}/logs?service_type=${serviceType}&instance_id=${instanceId}`}`
未编码；instanceId 含 `&`/`=`/`#` 会改写查询结构。

修法：`transport.ts` 新增 `buildLogsHref(subPath, serviceType, instanceId)`，
用 `URLSearchParams` 逐参数编码；Cluster 改用它。

## B2 r67 注记：恒真断言

`tests/unit/test_issue69_mutation_matrix.py:1467-1468` 的
`assertTrue(x is False or x)` 恒真。对照实现：Reduce.initial 的 live 子对象
由引擎物化/隔离，`refused=False`（写穿由上方 live_changed 全表断言兜住）。
改为 `assertFalse(...["refused"])`，期望值明确。

## 测试/负控

- `node scripts/check-api-transport.mjs` **33/33**（新增 scheme 白闸行为
  断言、query 编码行为断言、两处接线静态断言）；`tsc --noEmit` 干净。
- `test_issue69_mutation_matrix.py` **40 passed + 1265 subtests**。
- 负控（隔离/临时变异后恢复）：
  1. safeExternalUrl 去 scheme 检查 → B109-01 断言 1F（javascript/data 放行）；
  2. buildLogsHref 退回拼接 → B109-02 断言 1F（特殊字符注入 query）；
  3. 恒真断言改成 assertTrue → mutation matrix 1F。
  完整输出：`docs/proofs/data/r63-hardening-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8795P/12S/14 deselected/3501 subtests**（rc=0）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1121P/13S**（rc=0）
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `eee7b37` CI success；Console E2E `a75b4ab` 绿）
