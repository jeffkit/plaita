# r99 / A2R58-03（P2）：console 进程注册 `_mask_url` + 未注册 WARNING 频控——读面 URL 不再坠 `<unprintable-url>` 丢 host:port

- 基线：`origin/main` 开工时 `2c83e9f`（A2 r58 判定基线 `9072b9a` 的后裔，已核祖先；
  漂移面 docs+queues.py 与本轮零交集）；**推送前 rebase 至 `976713f`**（门禁期间上游
  进 4 commits：r92 flow_store.py + r93 治理账本，与本轮三文件仍零交集，rebase 零冲突，
  落点 `349fd04`）
- 分支：`fix/a2r58-03-mask-registration`（独立 worktree `/private/tmp/plaita-b3-r99`）
- 规格：duo-a2-ledger.md 第五十八轮 §2 A2R58-03（实弹×5：04a/04b/05a/05b/05d）+ 探针
  `~/.issue-keeper/duty/a2-r58-scripts/test_a2r58_fix_surface_regression-r58.py`

## 缺陷（A2 r58 实弹 16 红之 P2）

r89 把 `_scrub_url_text` 引入 console 读面（detail `error.message` / timeline 端点 /
SSE `initial_state` / SSE `update` 四面），但该函数依赖的掩码实现 `_mask_url` 的注册
唯一点在 `plaita/server/factory.py:1466`（模块级 server→core 向下注册），console 进程
全链零 factory 导入 ⇒ `plaita.core.url_redaction._mask_url` 恒 `None`：

- 一切绝对 URL（含**零凭据**的诊断 URL）坠 `<unprintable-url>`，worker 保住的
  `scheme://host[:port]` 在 console 读面被二次 scrub 抹掉（修前实测：
  `HTTPError 502 Server Error for url: <unprintable-url>`）；
- `_mask_url_for_log` 未注册分支每次 scrub 打一条**无频控** WARNING（读面是逐帧逐字
  段高频路径）。

**测试盲**：既有「凭据针不在」断言对「正确掩码（host 保留）」与「过度掩码（unprintable
常量）」两形态同真；且全量 console 会话里多个测试文件 import `flow_worker`/`factory`
把注册态带进宿主进程，与生产进程形态永久分叉。

## 修法（两件 + 测试盲修正）

1. **console 入口注册**（`plaita-console/backend/main.py`）：plaita.* 导入块之后
   `register_mask_url_impl(factory._mask_url)`——与 worker（`flow_worker.py:155` 导入
   factory）对称；三个启动入口（`__main__.py`/`run.py`/uvicorn 字符串目标）全部汇于
   main.py，注册一次全覆盖。实现仍以 factory 为唯一家（不另起第二套掩码文法，#88
   「同一个 URL 两套渲染」根因形态不重现）。
2. **WARNING 频控**（`plaita/core/url_redaction.py`）：`_unregistered_count` 进程级
   计数，首条 WARNING（带计数），之后降 DEBUG——「未注册」是进程级装配事实而非逐次
   事件，响亮一次 + 计数可查即足；同模块 `_cap_trip_count` 的政策/机制分离形制。
   fail-closed 常量契约不变（未注册态仍绝不回原文）。
3. **测试盲修正**（`tests/console/test_a2r58_03_mask_url_registration.py`，9 用例）：
   - 判据形改**三元组**：`host 在场 + <unprintable-url> 不在场 + 凭据针/path 明文不在
     场`——能区分正确掩码与过度掩码；
   - 注册断言走**子进程**（fresh 解释器只加载 console 导入链 + `import main`，A2 探针
     04a 同款），不受宿主进程 ambient 注册态污染；
   - 四读面断言（detail 端点 / timeline 端点 / `_redact_execution_view`〔initial_state
     同构造面〕/ `_scrub_update_payload`）+ worker 存量二次 scrub 幂等格 +
     未注册 fail-closed 守恒与 WARNING 频控格 + 注册态计数不动对照格。

## 负控（先红后绿，单文件隔离会话＝生产进程形态：零 factory ambient）

- stash 两修复文件 → 9/9 全红（注册缺失 / 四面 host 坍缩 / 无频控 / 计数格），
  `red-file-rc=1`：`data/r99-tests-prefix-red.txt`
- pop 恢复 → 9/9 全绿，`green-file-rc=0`（rebase 后抽查含 r92 新测试同绿）
- 修前行为实证（fresh 进程）：console 链 UNREGISTERED + scrub 坠
  `<unprintable-url>`；注册态保 host 抹 path/query/凭据——与 A2 探针 04a/04b/05a 逐象吻合。

## 门禁（rebase 后终树 `349fd04`，rc 直录无管道）

| 门 | 结果 | rc |
|----|------|----|
| console 全量（`pytest tests/console -q`，backend 下） | 1555 passed, 14 skipped | 0 |
| unit 全量（仓根） | 8938 passed, 13 skipped, 3501 subtests passed | 0 |
| integration 全量 | 213 passed, 1 skipped | 0 |
| e2e 全量 | 74 passed, 3 skipped | 0 |
| ruff check（本轮 3 个改动文件） | All checks passed | 0 |
| mkdocs build --strict（r87 venv） | built in 8.06s | 0 |

日志：`data/r99-gate-{console,unit,integration,e2e,ruff,mkdocs}.txt`（含各自 rc 行）。

## 脱敏家族守恒

- 既有凭据针断言全绿随 console/unit 全量（1555+8938 通过，无一脱敏面红）；
- 新测试保留凭据针不在场对照（`LEGACY_WITH_CRED` 三形载体：`SECRETPW`/`sk-live-r58`
  针 + timeline 响应体全文本扫描）——`_assert_correctly_masked` 三元组中「凭据针不在
  场」腿即家族守恒面。
- 机器判读键守恒：detail `error.error_type`/`code` 原样（断言在案）。
