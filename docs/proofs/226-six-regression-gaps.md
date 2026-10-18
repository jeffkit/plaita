# #226 证明：验签 / 凭据隔离 / 自改进放行 6 处回归钉

- 单号：plaita#226（P1，security/test）
- 基线：`origin/main` = `237f1f1`
- 边界：**纯测试**——不改验签算法、租户模型、密钥文件策略、Supervisor 放行
  语义；`git diff` 仅新增 3 个测试文件与 `docs/proofs/`。
- 组织：A1/A2 一个文件（验签）、A3/A4/A5 一个文件（凭据）、A6 一个文件
  （Supervisor）。

## A1 · 签名有效期服务端上限

- 实现：`plaita-console/backend/services/signature.py:262`
  `if sign_expire - fields["sign_time"] > _MAX_SIGNATURE_VALIDITY:`（1h）。
- 新用例：`generate_signature(validity=3601/7200)` 构造合法签名 →
  `verify_authorization(...) is False`；`validity=3600`（上限本身）→ True。
- 负控（条件 → `if False:`）：**2 failed / 2 passed**；
  红文案 `assert True is False`（3601/7200 被接受）；3600 正控与 A2 spy 保持绿。
  证据 `docs/proofs/data/226-a1-signature-validity-neg.txt`。

## A2 · 签名比较必须常量时间

- 实现：`signature.py:278` `if not hmac.compare_digest(fields["signature"],
  expected_sig):`。
- 新用例：monkeypatch `signature.hmac.compare_digest` 为记录 spy，验签成功后断言
  恰好两次调用（secret-id + signature），且第二次参数为
  `(fields["signature"], 重算的 expected_sig)`。
- 负控（`hmac.compare_digest(...)` → `!=`）：**1 failed / 3 passed**；
  红文案 `验签应做 secret-id 与 signature 两次 compare_digest；实得 1 次：
  [('issue226-id', 'issue226-id')]`。证据
  `docs/proofs/data/226-a2-compare-digest-neg.txt`。

## A3/A4 · 凭据租户过滤

- 实现：`plaita-console/backend/services/credentials_svc.py`
  `list_credentials`（:99）与 `get_credential_record`（:118）的
  `if tenant_id is not None:` 过滤。
- 新用例（A/B 两租户，`a`/`b`/同名 `shared` 各一份）：
  - `list_credentials("A") == ["a","shared"]`、`("B") == ["b","shared"]`，
    `list_credentials(None)` 仍是平台全量 `{a,b,shared}`；
  - `get_credential_record("b","A") is None`、`("a","B") is None`；
    同租户/同名各读各的值。
- 负控（两处过滤 → `if False:`）：**2 failed / 1 passed**；
  红文案 `A 视角泄露了其他租户: ['a', 'b', 'shared', 'shared']` 与
  `A 租户读到了 B 的凭据（跨租户明文泄露）`；A5 密钥模式用例保持绿。证据
  `docs/proofs/data/226-a34-credential-isolation-neg.txt`。

## A5 · 密钥文件 0600

- 实现：`credentials_svc.py:71` `os.chmod(key_file, 0o600)`（无 env key 首次生成）。
- 新用例：tmp 路径 + 无 `PLAITA_CREDENTIALS_KEY`，`save_credential(...)` 触发
  首次生成；断言 `stat.S_IMODE(key_file.stat().st_mode) == 0o600`。
- 负控（`0o600` → `0o644`）：**1 failed / 2 passed**；
  红文案 `密钥文件权限必须 0o600（其他用户不得读 Fernet key），实得 0o644`。
  证据 `docs/proofs/data/226-a5-key-mode-neg.txt`。

## A6 · Supervisor 默认 promote_gate=manual

- 实现：`plaita-ai/plaita_ai/supervisor.py:45` `promote_gate: str = "manual"`；
  `:649` 仅 `auto` 时 `client.publish_version(...)`。
- 新用例：
  - `SupervisorPolicy().promote_gate == "manual"`；
  - **默认策略**（不传 promote_gate）下改进候选：`promoted is False`、
    有 `promotion_ticket`、`fake.published["demo"] == "1.0.0"`（未发布）。
- 负控（默认值 → `"auto"`）：**2 failed**；
  红文案 `SupervisorPolicy 默认 promote_gate 必须是 manual（人工放行）...` 与
  `默认策略不得自动发布（绕过人工放行）` / `assert True is False`。
  既有 `test_supervisor.py` 在变异下 **17 passed**——显式 manual/auto 用例
  覆盖不到「默认值」，正是本单补的缺口。证据
  `docs/proofs/data/226-a6-supervisor-gate-neg.txt`。

## 正控（原实现）

- A1/A2 文件：**4 passed**；A3/A4/A5 文件：**3 passed**；A6 文件：
  **2 passed**（共 9 条）。
- 六处实现均未改动；变异只在负控期间临时注入，跑完逐字还原
  （`git diff` 产品代码为空）。

## 验收记录

- 新增用例：**9 passed**（4 验签 + 3 凭据 + 2 Supervisor）；
- 六条负控结果如上（A1 2F/2P、A2 1F/3P、A3/A4 2F/1P、A5 1F/2P、A6 2F）；
- console 全套：**537 passed / 13 skipped**（含本单 7 条 console 用例）；
- plaita-ai 全套：**225 passed / 14 skipped**（含本单 2 条 Supervisor 用例）；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit 与 landing gate：见交付记录。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #226`。
- 全量 unit：**7908 passed / 4 failed / 11 skipped / 3434 subtests**。4 条失败
  为 clean 基线同款环境面（`plaita_nodes.sandbox` 缺失 ×3、yarl 文案 ×1），
  失败集合与未改前逐条相同；本单测试分布在 console / plaita-ai，不在
  `tests/unit` 内。
- 验收面：unit 7908P/4F；console 537P/13S；plaita-ai 225P/14S；
  e2e+integration 287P/4S。
- landing gate：绿（base `237f1f1` CI success / slow lane success）。
- 产品代码：零改动（`git diff` 仅测试与 `docs/proofs/`；六次变异均逐字还原）。
