# #222 证明：执行详情的 `output` / `nodes[*].output` 读面脱敏

- 单号：plaita#222（P1，security）
- 基线：`origin/main` = `f83a268`（本工作树 `wt-b2`）；落地前 rebase 到
  `afe4bba`（#219/#220/#225/#226）并复跑全套
- 边界：只扩 `_redact_execution_view` 的遍历面；不动队列载体、盘上 checkpoint /
  `output_json` / `nodes_json` 的功能值；租户过滤（#192）不在本单范围。

## 缺陷

`#166` 的脱敏落点正确（构造表示时、序列化之前），但遍历面只有
`("context", "input")`。同一个 `ExecutionInfo` 响应里的：

- `output`（流程最终输出，本地档 `local_executions.output_json`）；
- `nodes[*].output`（本地档节点级 trace，`local_executions.nodes_json`）；

原样返回。`assignment(output="$INPUT")` / `end(output="$INPUT")` 会把整份入参
回显到这两处，http 节点响应体也整段进 `nodes[*].output`——本地单机档
`GET /api/executions/{id}` 与 `GET /api/executions` 因此把提交者以为已掩码的
凭据明文回给任意已认证 viewer。

## 修法

`plaita-console/backend/api/executions.py::_redact_execution_view`：

```python
for field in ("context", "input", "output", "nodes"):
    value = out.get(field)
    if value is not None:
        out[field] = redact_credential_values(value)
```

`nodes` 是 list-of-dict，`redact_credential_values` 已支持 list 递归，无新判据。
返回仍是新对象（逐字段、逐 list 项新建），调用方的功能副本不被改写。所有既有
出口（详情 / 列表 / 本地 SSE `initial_state`+`update` / 集群 SSE
`initial_state` / 事件流）共用本函数，扩面即全出口生效，无遗漏出口。

## 测试

新增 `plaita-console/backend/tests/console/test_issue222_output_redaction.py`
（2 例，真实本地执行，非桩）：

- 流程 `start → assignment(output="$INPUT") → end(output="$INPUT")`，入参含
  显式凭据键（`api_key=sk-live-…`）、URL userinfo 口令（clone URL）、
  webhook path token；
- `test_local_detail_output_and_node_output_are_redacted`：
  `GET /executions/{id}` 整份响应体零命中 3 个 needle；`output` 与
  `assign`/`end` 的 `nodes[*].output` 形状保留（`api_key == "***"`、非凭据值
  原样、URL 已换占位）；
- `test_local_list_output_and_node_output_are_redacted`：
  `GET /executions` 中的同一行同判；
- 两例都在请求前后读取 SQLite 的
  `input_json` / `context_json` / `nodes_json` / `output_json` 原文，先断言
  功能副本确实含明文（用例前提），再断言请求后逐字节相等——`#166` 的
  「只改表示、不动功能副本」红线。

## 红 → 绿与负控

修复前（HEAD 两字段遍历面），两条用例 **2 failed**，三个 needle 全部命中：

```
AssertionError: 执行表示里仍留着明文凭据：['sk-live-3f9a2b7c1d4e', 'Passw0rd-9f2a', 'Ks9Qm4Zt']
```

修复后两条 **2 passed**；与 `#166` 既有用例合跑 **9 passed**。

负控：把遍历面改回 `("context", "input")`、其余不动 ⇒ **2 failed / 2**，
两条用例重新变红（原始输出 `docs/proofs/data/222-negative-control.txt`）。
还原后复跑 **2 passed**。

## 验收（`.venv/bin/python`，py3.13）

- console 全套（`cd plaita-console/backend && pytest tests/console -q`）：
  **532 passed / 13 skipped / 0 failed**（62.79s，基线 `f83a268`）；
  rebase 到 `afe4bba` 后同命令 **539 passed / 13 skipped / 0 failed**（62.81s）；
- `tests/unit -m "not slow"`：**7858 passed / 15 skipped / 13 deselected /
  3429 subtests passed / 0 failed**（309.08s，基线 `f83a268`）；rebase 后
  **7880 passed / 15 skipped / 13 deselected / 3434 subtests passed /
  0 failed**（307.64s）；
- ruff：改动测试文件零告警；`executions.py` 的 13 条 `BLE001` 在基线同样
  存在（逐条对比 HEAD 版本，错误集不变，无新增）；
- `python scripts/landing_gate.py ci`：main 最近完成 run `afe4bba`
  conclusion=success，✅ 允许落地。

## 残面/边界

- 集群档 `ExecutionInfo.output` / `nodes` 恒为 `None`，影响面仍是本地单机档；
  扩面不改变集群档行为。
- 未改 `error` 字段（不在本单读面清单）；若后续发现异常载荷回显凭据，
  按同族另单处理。
- 盘上功能副本保持明文是刻意设计（resume / 流程语义要用它）；本单只改表示面。
