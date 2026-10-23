# r89 证明：A2R49-02 error 读面脱敏第五字段 + A134-04 种子发布第三点门

- 单号：A2 台账第四十九轮 A2R49-02（P3，#166 家族）+ A1 台账第一百三十四轮
  A134-04（P3；任务书作「A134-05」，以台账编号为准——A134-05 是 `where`
  死参数，不在本轮范围）
- 基线：`origin/main` = `14c9691`（B1 r85 台账点）
- 分支：`fix/p3-redaction-gatefn`（worktree `plaita-wt2`）
- 探针：A2 r49 归档 `~/.issue-keeper/duty/a2-r49-scripts/p2_sse_error_faces.py`
  sed 换根复跑（`/tmp/b2r89/`），实弹输出在
  `docs/proofs/data/r89-p3-redaction-gatefn-probe-{pre,post}-fix.txt`

## 单① A2R49-02：error/timeline/SSE 三读面凭据形文本脱敏

### 缺陷（A2 r49 实弹四红格，本轮换根复现）

`_redact_execution_view` 字段集只有 `context/input/output/nodes`——worker 侧
三个写面的凭据形异常文本原样送达同租户 viewer：

1. **detail `error.message`**：`state.error = {"message": str(e), ...}`
   （flow_worker.py 两处错误收口；`_error_fields` 的结构化 `response` 自带
   脱敏、`message` 原样）；
2. **timeline `events[*].error`**：`execution_timeline._error_text` =
   `str(exception)` 无 scrub（同模块 logger 侧 #151 用 `_scrub_exception_text`
   ——判据不对称）；`/timeline` 端点集群/本地两路投影零滤，且**存量旧行**
   只能读面兜；
3. **SSE**：集群档 update 帧 `_execution_event_stream` 对 worker 发布载荷
   零滤透传（#166 注释自认「不在本单口径内」）；行内 `timeline` 字段随
   initial_state 整行 dump（`ExecutionInfo` 无此字段故 detail 不受影响）。

pre 探针（probe-pre-fix.txt）：P2a error.message 原样 / P2b timeline 原样 /
P2c-1 initial_state error 原样 / P2c-2 update 帧 token 2→3，正控 context
脱敏两格绿。

### 修法：构造面 + 读面双层，四处落点

- **`_redact_execution_view` 字段集 + `error`/`timeline`**（executions.py）：
  `redact_credential_values` 的 str→`_scrub_url_text` 递归既判，覆盖
  detail / 列表 / SSE initial_state（含行内 timeline 事件）；
- **timeline 信封 `_scrub_timeline_events`**（executions.py，`_timeline_envelope`
  接线）：`events[*].error/reason` str 值过 `_scrub_url_text`——集群+本地
  两路投影一处收口，**存量旧行**（修复前落盘）由此兜住；
- **worker 源头 `_error_text` 过 `_scrub_url_text`**（execution_timeline.py）：
  落盘与实时发布共用此构造点，SSE update 帧载荷在序列化前即已抹（worker 是
  频道唯一发布者；顺带消除同模块「logger scrub / 读面裸奔」不对称）；
- **update 帧接码侧 `_scrub_update_payload`**（executions.py）：载荷
  JSON 解析→error/reason scrub→**有变化才重序列化**（零变化保持原字节；
  非 JSON 原样透传）——兜滚动部署窗（旧版 worker 仍在发布未 scrub 载荷）
  与非 worker 频道写入方。

不动：盘上 checkpoint / 队列载体 / `$INPUT` 功能值（#157/#122 通道）；
`reason` 现为枚举常量无自由文本面，scrub 为前瞻对称位。

### 验证

- repo 红→绿：unit `test_issue25_execution_timeline.py` 1F（error 文本未
  scrub）→P + 负控 3P；console `test_issue166_input_redaction.py` 3F
  （detail error / SSE initial_state error / 行内 timeline）→P + 负控；
  console `test_issue25_execution_timeline_api.py` 2F（集群/本地 timeline
  信封）→P + 负控（非凭据文本与事件形状原样）。
- 探针 post（probe-post-fix.txt）：四红格全翻（泄漏判定全 False，全流
  token 计数 0），正控绿。
- 家族守恒：脱敏族（#158/#159/#161/#165/#222/#263/#265）927P；
  timeline 族 40P+11P；examples/gate/SSE 族 28P。
- 全量：console **1518P/18S rc=0**；unit **8902 collected rc=0**（后台运行
  退場 rc=0 零失败；日志汇总行丢失，收集数=上轮 8898P+本轮新增 4 格吻合）；
  integration+e2e **287P/4S rc=0**；ruff 改动文件既有 BLE001 15（与基线
  同数，零新增）；mkdocs rc=0。

## 单② A134-04：examples 种子发布补第三点 gate_fn

### 缺陷与裁量（A1 r134 §4 + 本轮实读调用面）

r86 的发布 re-gate 注入 publish/import 两点；`seed_example_flows` 的
`store.publish_version(...)` 无门。实读调用面：生产路径属实——但仅
local_mode（main.py:249 `if local_mode:`），能力门数据源（worker 注册清单）
在本地档不存在，注入 `_gate_runtime_capability(redis=None, ...)` 对能力门
自跳过；**编码闸（卡 A-1）不依赖 redis，注入后照样执法**——故注入非死代码。
A1 的运行面顾虑（门拒伪打种子、异常被 per-example except 吞成 WARNING）由
「单例门拒=该例跳过+WARNING、启动不受影响」承接：三例为静态定义，现时门
必过；未来门收紧时不再静默绕门（worker 不支持的节点集在发布面即拒）。

### 修法

- `services/examples.py`：`seed_example_flows` 增 `gate_fn` 参数，缺省
  `_default_seed_gate`——惰性导入 api 层 `_publish_gates`（防环；
  api.flows 不反向依赖本模块）以 `where="examples.seed"`、`redis=None`
  调用；「为何无真 redis」「未来集群启动应经 gate_fn 传入」钉在 docstring。
- 种子发布调用带 `gate_fn=gate_fn`；门拒形状=该例版本保持 draft
  （`ensure_flow` 先于门，流程行存在但**永不 published**）。

### 验证

- repo 红→绿：`test_a134_gate_seed_examples.py` 3F（结构格=发布调用未带
  gate_fn；行为格两格 TypeError/断言红）→ 4P：种子发布三调用全带门、默认门
  对静态示例放行（created=3 守恒）、全拒=created 0+三版本 draft、单例拒=
  其余两例照常 published。
- `test_local_mode.py`（种子族既有断言）全绿——默认门对现网示例零行为变化。

## 边界与残留

- `where` 死参数（真 A134-05）未动，仍待派；console 侧 `_timeline_envelope`
  scrub 与 `redact_credential_values` 判据并存是「信封具名 scrub + 整行
  递归」双层，非第二套判据（底层同一 `_scrub_url_text`）。
- SSE 集群档 update 帧的 finished 终态事件缺失（A2R49-04）不在本轮。
