# pn#20：`/api/flows/ai-generate/stream` 平台身份闸 + 工作区隔离

## 缺口

- 路由 `ai_generate_stream` 只受管理面统一 `require_auth` 保护：`POST` 的
  `_authorize` 只要求 editor+，租户 editor/admin 都能让 console 后端在**宿主**
  上启动编码 Agent（`--permission-mode auto`），读取宿主凭据与其它租户产物。
- `ai_flow._workspace()` 固定为 `<tmp>/plaita-console-agent-ws`，所有租户/
  并发执行共享同一目录：可跨租户读到过程产物，且同一 workspace 的 run-lock
  互相冲突。

## 修法

1. **平台身份闸**：端点显式 `Depends(require_auth)` + `ensure_platform_admin`
   （对齐 `/api/users` 族，action=`flows.ai_generate.stream`）。租户
   editor/admin 一律 403（`PLATFORM_ADMIN_REQUIRED_DETAIL`）并写
   `flows.ai_generate.stream.denied` 审计；平台管理员 / API-Key 放行。
   拒绝发生在 `StreamingResponse` 创建之前 ⇒ agent 生成器零启动。
2. **工作区隔离**：`_workspace(tenant_id)` 每次调用生成
   `<root>/<tenant>/<run-id>`（每次执行独立 16 位 hex 目录）；租户 id 经
   `_safe_path_component` 清洗为单层目录名，`..` / `/` 无法逃逸根目录。
   仍持久保留，便于排查 agent 过程产物。

## 负控（tenant editor 的 pre-fix Popen 形态）

仅删除端点中的 `ensure_platform_admin(...)` 调用，保留测试：

```text
$ .venv/bin/python -m pytest \
    plaita-console/backend/tests/console/test_issue20_ai_generate_stream_gate.py -q
2 failed, 5 passed
FAILED test_tenant_editor_denied_before_agent_start
FAILED test_tenant_admin_denied_too
# 精确红：assert 200 == 403，响应体是 agent 事件流
# data: {"type": "finished", "ok": true, ...}
```

证据：`docs/proofs/data/20-nc-console-stream-gate.txt`。

## 正控与回归

```text
$ .venv/bin/python -m pytest .../test_issue20_ai_generate_stream_gate.py -q
7 passed
（平台管理员 200、API-Key 200、X-Tenant-ID 命名空间、
  同租户两次执行目录不同、路径穿越清洗）

$ .venv/bin/python -m pytest <flows/auth/platform-gate 相关 7 文件> -q
66 passed

$ .venv/bin/python -m pytest plaita-console/backend/tests/console -q
642 passed, 13 skipped
```

`scripts/landing_gate.py ci`：main `c0bffe0` CI 仍红（B1 测试债在修），按
分支模式推 `origin/wt-b3`，不压 main。

## 跨仓

plaita-nodes 侧 `recursive_stream_turn` 的同构租户闸见
`plaita-nodes/docs/proofs/20-stream-tenant-guard.md`。
