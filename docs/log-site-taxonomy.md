# 日志站点分类判定表（#151 的实施边界）

> plaita#168 交付物：**本文档 + `docs/log-site-taxonomy.json`（机器可读判定表）**。
> 本单只做**定义**，不改任何 `logger` 调用点；不动 #133 的 evaluator 语义，不动 #88 族掩码实现，不实现 #151 本身。
> 判据全部**机械可判定**——规则即代码（`scripts/log_site_taxonomy.py`），门禁为 `scripts/check-log-site-taxonomy.py`。

## 1. 覆盖口径与清点结果

覆盖口径与 #168 验收 grep 逐字一致：接收器字面为 `logger` 的 `debug/info/warning/error` 调用行。

```
grep -rn "logger\.\(info\|warning\|error\|debug\)" plaita/ | wc -l   # = 806（HEAD 87b8ad0）
```

清点（issue 立单时的 588 是当时基线；以 HEAD 重算为准）：

| 计数 | 值 | 说明 |
|---|---|---|
| grep 命中行 | 806 | 验收口径全部站点，判定表逐行覆盖 |
| AST 关联调用 | 790 | 真实 `logger.<m>(…)` 调用 |
| 插值站点 | 641 | 判定表 A/B/C/D 的对象 |
| 常量消息站点 | 149 | 无插值 ⇒ 无数据流出口，显式标 **N/A** |
| grep 误命中 | 16 | 注释/文档串里的 `logger.warning` 字样，非调用点，显式标 **N/A** |

判定分布（基线 origin/main 87b8ad0；本分支 #94 提交只动 MIGRATION/shim 文案、零 logger 调用点，清点不受影响）：**A 244 / B 85 / C 312 / D 0 / N-A 165**。

已知边界（不在覆盖口径内，#151 实施时须自行对齐同一判据）：

- 其他接收器拼写的 logger（如 `plaita/node/loop.py` 的 `_logger`，共 18 处）——判据相同，接收器名不同；
- `logger.log(...)` 1 处、`logger.exception` 0 处；
- `exc_info=True` 不是插值、不进本表，但 traceback 同样可能携带作者数据——#151 实施时按 §2 出口白名单第 4 行处理（异常文本走构造面清洗，`__globals__`/帧投影一律不落日志）。

## 2. 评估器出口白名单（#133 r22 之上的值/消息两面）

T1（dispatch surface）可闭、T2（environment surface）是威胁模型边界（helper 的可变环境视为受信/冻结）。据此：

| 出口 | 允许进日志 | 必须掩 |
|---|---|---|
| 表达式**求值结果**（数据面） | ✅（它是 flow 的功能数据） | 含凭据形状时按 #88 族掩码 |
| 评估器**错误消息** | 仅具名错误类 + 参数**计数**，不回显参数值 | 参数值（可能含作者数据） |
| 评估器**类型/类名** | ✅（`PurePath`、`dict` 等，非作者数据） | — |
| `__globals__`/闭包 cell/lazy wrapper 的**任何投影** | ❌ 永不 | 一律 |

## 3. 站点四类判定表

站点 → 类的判定按**表达式级规则**先判（每条插值表达式独立判定），再按站点级取最严（任一 C ⇒ C；否则含 A ⇒ A；否则 B）。规则全文见 `scripts/log_site_taxonomy.py`（`C_RULES` / `B_RULES` / `A_RULES` + `OVERRIDES`），与下表一一对应：

| 类 | 判据（机械） | 典型表达式形状 |
|---|---|---|
| **A** 数据面允许 | 插值能追到**流程功能数据**：求值结果/消息载荷/流程配置内容，或可跨日志回查的**实体标识**（execution_id、node 名、flow_id、message_id、队列/存储键、dedup_key…） | `execution_id`、`task.message_id`、`self.dlq_key`、`raw[:200]`（队列载荷截断）、`final_decision` |
| **B** 结构面允许 | 插值是**引擎结构事实**：类型名/错误类名、计数、耗时、状态枚举、布尔判定、引擎常量（含 env 变量**名**）、基础设施标量（port/pid/path）、对象句柄 | `type(exc).__name__`、`len(args)`、`attempt+1`、`task.delivery_count`、`env_name`、`OPT_OUT_ENV` |
| **C** 必须掩 | 插值是**异常文本**（str(e) 可夹带作者数据/凭据形状 ⇒ #88 族构造面清洗）、**凭据/URL 形状**（含已掩码形态——必须保持掩码）、**环境变量值**（env 投影：插值变量在所在函数内赋值自 `os.environ`/`os.getenv`）、`__globals__`/帧/闭包投影 | `e`、`exc`、`_exception_message(exc)`、`_mask_webhook_url_for_log(self.url)`、`raw`（← `os.environ.get(...)`） |
| **D** 应删除 | 整条日志**无意义或重复**：日志内容为对象默认 repr（仅内存地址、与相邻日志重复同一事实）、同一事实的相邻重复行 | HEAD 全量清点 **0 站点命中**（见 §5） |
| N/A 非插值 | 常量消息（无数据流出口）；grep 误命中 | `logger.info("worker started")` |

与 #168 立单判据的对应：**"站点处变量的来源能追到 `context`/`node_results` = A；追到 `evaluator` 内部状态 = C"**——A 规则组即"context/node_results 侧"（实体标识与数据内容），C 规则组即"evaluator/环境内部侧"（异常文本、env 投影、内部投影）；两者之间纯结构事实（计数/类型/状态）= B。判定不允许"看情况"：判据未覆盖的新表达式形状会令重算**硬失败**，必须先扩规则表再重算。

**站点级覆盖（OVERRIDES）**：bare 名在具体站点有更精确来源时逐站点显式枚举（附理由），如 `plaita/event/redis.py:2883` 的 `task` 是 asyncio Task 句柄（B，非队列载荷）、`plaita/core/expression_parser.py:2679` 的 `_expression._clip(path)` 是表达式数据路径（A，非文件系统路径）。

## 4. 每类正反例（真实站点坐标）

**A 数据面允许**

- ✅ 正例 `plaita/server/task_queue.py:424`：`task.message_id` / `self.dlq_key` / `_decode(dlq_id)` —— 实体标识与存储键，跨日志可回查，允许原样进日志。
- ❌ 反例 `plaita/server/task_queue.py:48`：`raw` 看似"配置值"（A 的形状），实则 `os.environ.get("PLAITA_DLQ_MAX_LEN")` 的**值**——环境投影 ⇒ **C**。同形不同源，判据看来源不看名义。

**B 结构面允许**

- ✅ 正例 `plaita/core/expression_parser.py:3066`：`_expression._clip(func_name)` + `len(args)` + `[_expression._type_name(arg) …]` —— 函数名/参数计数/类型名清单，纯结构事实。
- ❌ 反例 `plaita/server/services/http_callback_service.py:387`：`auth_error` 出现在认证失败日志里，但它是**固定文案**的具名拒绝原因（`_verify_callback_auth` 只返回常量串，不回显凭据）⇒ B 而非 C——"认证相关"不等于"必须掩"，判据看值的形状与来源。

**C 必须掩**

- ✅ 正例 `plaita/server/flow_worker.py:4159`：`…: %s", mid, e` —— `e` 为异常对象，文本可能夹带 URL/作者数据 ⇒ 必须 #88 族构造面清洗（`_scrub_exception_text`/共享清洗器）后才进日志。
- ✅ 正例（约束已满足的形态）`plaita/server/alerts.py:149`：`_mask_webhook_url_for_log(self.url)` + `_scrub_url_text(exc)` —— 凭据形状值与异常文本均先经掩码/清洗再插值；类别仍是 C（必须保持掩码形态进日志，#151 验收时按此核对）。
- ❌ 反例 `plaita/core/expression_parser.py:730`：`type(exc).__name__` 与 `exc` 同站出现——前者是错误**类名**（B，允许），后者是异常**文本**（C，必须掩）；同站混合时站点取最严 ⇒ C。

**D 应删除**

- 判据：整条日志无意义或重复——对象默认 repr（仅内存地址）、与相邻日志重复同一事实、日志常量与插值完全冗余。
- HEAD 全量清点 **0 站点命中**（含全部 26 组同模板站点的逐一核对：同模板均为不同代码路径/不同键的不同事实，如 `plaita/server/task_queue.py:466` 与 `:470` 是 `stream_key` 与 `dlq_key` 两次独立探测，非重复）。故本类**无真实正例可引**——不伪造站点坐标；#151 实施时若遇满足判据的新站点按 D 处理（判据已在规则表中，命中即红）。
- ❌ 反例（最接近但不判 D）`plaita/server/flow_worker.py:2011`：`"观测回调 bind_execution 失败: %r", handler` —— `handler` 的默认 repr 信息量低，但警告本身（bind 失败）有意义且 `exc_info` 提供根因，删整条日志会丢失败信号 ⇒ 不判 D；#151 若要收敛，属"插值收敛为类名"而非删站点。

## 5. 「未采集」vs「0」的日志口径

与 #37 ① 的 `source: observed|estimated|mixed` 对齐：日志里不得把"没采集到"打成 `0`——

- `usage` 缺失时打 **`usage=absent`**，不是 `usage=0`；
- 计数/指标类插值若来源未观测（探测失败、字段缺失），必须以具名缺席标记（`absent`/`?`/`unknown`）或整行降级，不得以 `0` 顶替；
- 本口径的机械化检查随 #151 落地（判定表 C/B 类站点中指标形表达式 `…=0` 的兜底分支须能区分"真 0"与"未采集"）。

## 6. 维护方式（对账双向）

- **重算**：`python scripts/log_site_taxonomy.py --emit` —— 用规则表对当前代码重算并重写 JSON；
- **校验**：`python scripts/check-log-site-taxonomy.py` —— T1 覆盖（双向：缺行/死行都红）/ T2 词表 / T3 重算逐字节一致；
- 负向对照实测：删任一判定行 ⇒ T1 红；插值站点改标 N/A ⇒ T2 红；改规则不重算 ⇒ T3 红。
- 新增/修改 `logger` 调用点后门禁即红，按 §3 判据扩规则（必要时加 OVERRIDES 并附理由）→ `--emit` → 校验转绿。
