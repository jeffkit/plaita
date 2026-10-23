# duo-b4 台账（plaita-rs 专席）

- 席位：B4（jeffkit/plaita-rs，Rust 引擎 + IR）
- worktree：`/Users/kong/projects/infra4agent/plaita-rs-wt`，分支 `wt-b4`（基线 6f47971 = origin/main）
- 门禁：`cargo fmt` / `cargo clippy --all-targets -- -D warnings`（0 告警）/ `cargo test --workspace` 全绿；差分对账 16/16 一致（go-ref b8fb5a6 侧 2026-10-22 本地构建）

## 首轮（2026-10-22，okguitar 审计批：panic/OOM/timeout 家族，四单全修）

| # | 单 | 修法 | 负控（先红→后绿） | commit |
|---|---|---|---|---|
| 1 | fix(ir): 越界 ISO 时长装载期 panic | `duration.rs`：days/周分量累加改 checked 运算（debug 溢出面）；`from_secs_f64`→`try_from_secs_f64`（std 越界 panic 面），失败映射具名 `DurationError`，三处调用点（normalize/model/ir::node_from_value）的包装自然生效 | 负控：`P999999999999999D`/`PT99999999999999999999S`/`P9223372036854775807W`/`…7D` 四条 catch_unwind 下断言 is_err（pre：debug panic at duration.rs:54、release panic at std time.rs，均 exit 101）。post：validate/run、flow 级/节点级、debug/release 六口径全部 EXIT=1 + `timeout "…": duration "…" is not representable`。正控：`P106751991167301D`/`P1D`/`P1W`/`PT2M30S`/`PT1H`/`PT1.5S`/`PT0S`/`PT` 不变 | `810a0a9` |
| 3 | fix(nodes): http 节点未接 deadline | `execute_http` 加 `deadline` 参数（分发处透传，与 code/loop/parallel/child 同款）；预算 = min(node.timeout&#124;30s, deadline 剩余)，零预算闸拒启动，deadline 只收紧不放宽；预算耗尽的传输失败改归 Timeout 类（对齐 plaita-go ctx.Err() 上浮），未到期仍 1002 瞬态 | 负控：测试内 TcpListener 慢服务（accept 后搁置 5s），流程 PT1S、http 无 node.timeout（pre：5.02s 后错误记到 end 节点、http 被记成功 200）。post：1.01s 失败、归因 `node "h" (http)`、Timeout 类非 1002；事件流不再出现「http 成功→end 报 deadline」。正控：快速 200 契约不变、非 2xx 仍非错误、连接拒绝（未到期）仍 1002；`request_budget` 单测覆盖 min/默认/零 | `08ad37a` |
| 4 | fix(expression): $F.mod 的 i64::MIN % -1 panic | 整数分支 `ai % bi` 改 `checked_rem(bi).unwrap_or(0)`：None 仅 MIN %-1 一种（除零已前置拒绝），0 与 CPython `-2**63 % -1 == 0` 及 plaita-go 补码回绕同口径，注释写死依据 | 负控：单测断言字面量与 `$INPUT` 两路径得 0（pre：debug/release 均 panic at functions.rs:231、exit 101 绕过 errorHandler/$ERROR）。post：release eval 双路径 exit 0 输出 0；errorHandler continue 的 flow 抵达 flow_end（pre：exit 101 无 flow_end）。正控：`mod(7,3)=1`/`mod(-7,3)=2`/`mod(7,-3)=-2`/str %-格式化/整浮除零 ZeroDivisionError 全不变 | `adf614a` |
| 2 | fix(nodes): code 超时只杀直接子进程 | 超时分支改 `kill_process_group`：`libc::kill(-pgid, SIGKILL)`（process_group(0) 使 sidecar 自为组长，-pid 即整组），ESRCH 视为已退出，其他失败回落 `child.kill()`；`child.wait()` 收尸；`cfg(not(unix))` 保留单杀；plaita-engine 增 cfg(unix) 依赖 libc 0.2（lock 已有 0.2.191，零新版本）；「杀整棵树」四处文案由声明变事实 | 负控：sidecar Popen 孙进程（argv 内嵌唯一标记；踩坑记录：`sh -c 'sleep 777 # 标记'` 会被 exec 替换丢 argv 造成假绿，改用 `python3 -c`），ps 轮询断言组内无存活（pre：孙进程 PPID=1 存活、PGID=已死 sidecar pid，与 issue 实测同型）。post：CLI 复现 `ps` 无孤儿、进程组测试绿。正控：正常 code 输出不变、`code_node_timeout_policy_is_reachable` 仍绿 | `8f24ca1` |

**一行摘要**：首轮四单（#1/#2/#3/#4）全修，负控均先红后绿，fmt/clippy(-D warnings)/test 全绿、差分 16/16；分支 `wt-b4` 已推（810a0a9→8f24ca1 四 commit）；全程未动 golden/testdata。

## 残余与移交（本轮发现、不在四单范围）

- **`exec.rs:119` 运行期 `Instant + duration` 溢出 panic**：可解析但超 Instant 值域的时长（如 `P106751991167301D` ≈9.2e18s，Duration 可表示）在 `run` 起流时 panic（`overflow when adding duration to instant`，基线 6f47971 同炸，已 stash 实证非本轮回归）。归属「装载期预算 cap」缺口（= 仓单 #8 的 ir scope），与 #1 的「不可表示」是两件事，本轮按 #1 边界不修。
- `plaita-engine` 流程级超时是 `FlowTimeout` 单元、节点归因路径（exec.rs:304-309 的 `context deadline exceeded`）未动——#3 修后 http 自身超时在节点体内归因，该兜底闸仍保留给其余节点类型。
- 台账文件按席位约定不入主 clone git，待主控统一入库。

## 第二轮（2026-10-22，okguitar 审计批：规模/溢出/上限家族，四单全修）

- 基线：`git reset --hard origin/main`（825ad51 = r1 合并后的 main）；分支 `wt-b4` 已推（26bf85d→9c386f8 四 commit）
- 门禁：fmt / clippy(-D warnings) 0 告警 / `cargo test` 全仓 109 passed 0 failed（expression 40 单测+45 golden，engine 15+28+23+1）；差分对账 **16/16** 一致；全程未动 golden/testdata

| # | 单 | 修法 | 负控（先红→后绿） | commit |
|---|---|---|---|---|
| 5 | fix(expression): $F.mul 只卡重复次数不卡结果规模（100 元素×1e8 → 760GB） | `repeat_count`（次数闸 ≤1e8）换成 `repeat_len`：projected = seq_len×n 分配前对 allowed = clamp(seq_len×4, 100k, 1e8) 判定——**Python `_EXPR_*` 全量口径**（jeffkit「对齐 Python/Go 者优」选型，本地 plaita 实跑 16 例核对文案/单位/4× 系数/严格大于），四路调用点（str×int/int×str/list×int/int×list）同闸；文案 `_size_refusal` 同款（seq_len*n + 投影 + limit + chars/elements）；字节口径注释写死（str::len() ≥ Python 字符数，只严不宽）；空序列投影 0 恒放行（Python `''*1e9==''`，旧次数闸误杀点顺手修）；`Vec::with_capacity` 裸乘改 saturating_mul | 六路拒绝断言（str/list×正逆序×字面量/运行期）+ 文案逐字 + 4× 防误杀（50000×3 放行/×5 拒 limit 200000/100 万×2 放行）+ 空序列×2e8 放行（pre：`got Ok` / unwrap panic）。CLI release：2 亿字节产物归零、exit 1、文案与 plaita 逐字同；mul 短串/数组/负零倍/int 溢出/浮点全原样 | `26bf85d` |
| 7 | fix(expression): sub/abs/round 对 i64::MIN 取负未检查 | `$F.sub` 裸 `-bi` 改 **checked_sub**（不用 issue 建议的 checked_neg+checked_add——那会把真值在域内的 `sub(MIN,MIN)=0` 过拒成 OverflowError；Python=0）；`$F.abs` 改 checked_abs → OverflowError（Python 权威 2**63 出 i64，落 AGENTS「>i64 报 OverflowError」残余出口，与 add/mul/pow 同款；plaita-go 同根静默回负，跨仓另提）；`$F.round`：ndigits 保持 i64，`as i32` 截断拆除（4294967297→1 错值钉子恢复 1.2345）；值域闸按 **CPython 实测口径**（issue 建议的 `n>17→恒等` 被 `round(1e-20,18)==0.0` 证伪）：!is_finite 前置、n≥1075 恒等（f64 十进制展开至多 1074 位）、n<-308→±0.0（copysign 保号）、[−308,1074] 真实按位舍入并**拆掉 py_round_digits 的 min(17) 钳位**（把 round(1e-20,20) 错成 0.0，Python=1e-20） | abs/sub 字面量+$INPUT 两路 OverflowError（pre：std num/mod.rs:451 debug panic）、round 六点（pre：断言失败/panic）。CPython 13 例对拍全同；debug/release 双 profile CLI 均具名错误不 panic；sub(MIN,1)/add(MIN,−1)/mul(MIN,−1)/pow(2,64) 仍 OverflowError | `d5d8153` |
| 6 | perf(expression): $F.sort 插入排序+双 clone 键，64k 17.56s | Schwartzian 变换：每元素取键一次（Python sorted(key=…) 形状），`slice::sort_by`（稳定，Timsort/SliceStable 同约）排序阶段零分配零克隆；desc 用 `Ordering::reverse`；**O(n) 相邻可比性预检**先行——compare 只认 numeric/str 两族，跨类元素必有相邻跨类对，TypeError 同源；预检后比较器不可能失败，不用「错误槽+一律 Equal」破坏 sort_by 全序契约（driftsort/ipnsort 会探测非全序并 panic，不能把 panic 面引进 $F 出口） | 性能分母：64k 倒序 5s 预算断言（pre：release 实测 **54.95s** FAILED；post：debug/release 双绿）。守恒：asc/desc/key/稳定性双锚（升序+倒序相等键原序）/字符串序/None 键 TypeError/混排 TypeError 全部 pre-fix 已绿修后仍绿；CLI 实测 32k 0.035s（旧 4.46s）、64k 0.040s（旧 17.56s）、比值 1.15（旧 3.9）、64k 结果恰为 0..64000 | `3235588` |
| 8 | fix(ir): 装载缺预算 cap（retryTimes/节点 timeout 无上限）+ r1 移交的 Instant 值域 panic | 新增 `engine::budget`（对齐 plaita-go pkg/workflow/budget.go：MAX_RETRY_TIMES=10/MAX_NODE_TIMEOUT=24h）；validate 加 `check_budget_caps`：flow 级 retryTimes 在 validate_doc 入口先查、节点级 per-node 首位（同 Go 顺序），文案同款（owner 点名+cap+plaita#267/#35）；节点 timeout 走归一化后 ISO 串判定（>24h 拒，=24h/PT0S 收）；flow 级 timeout 有意不 cap（三家共同决策，注释写死）。**r1 移交一并修**：normalize_timeout 加 `check_instant_range`（`Instant::checked_add`，flow/node/嵌入流共用、ISO+毫秒两拼写）具名拒载；exec.rs:119 裸加改 checked→具名 FlowError；exec.rs 节点 deadline 与 context.rs child 预算合成同步去裸加 | stash 实现跑六路全红：flow/node retryTimes 11/1e9 拒、24h 边界含括、PT90000S 与 90000000ms 同拒、嵌入流递归覆盖、flow 级不 cap、runtime_guards catch_unwind 抓到裸加 panic；CLI：caps.json exit 1 且错误文本与 Go 侧同 fixture 同款（flow 级先报）、巨值 timeout run 从 exit 101 panic 变装载期具名拒载。踩坑两记：①本机 Instant 是 timespec（tv_sec i64 秒），1.8e18s 不溢出——负控值改 9.9e18s（落 (9.22e18, 1.845e19] 共同溢出区间）；②`"errorHandler": null` 本就被装载拒，fixture 别混入 | `9c386f8` |

**一行摘要**：第二轮四单（#5/#7/#6/#8）全修，负控均先红后绿（#8 用 stash 做 pre-fix 红），fmt/clippy(-D warnings)/cargo test 全绿（109 passed）、差分 16/16 守恒；issue 建议代码两处按实测纠正（#7 的 `n>17→恒等`、#8 值域测试值的平台窗口），均已回写注释与台账。

## 第二轮残余与移交

- **plaita-go 跨仓单**：`$F.abs`/`$F.sub` 对 MinInt64 静默回负（`go run` 实测 `abs` 给 -9223372036854775808）——Python 权威给正值，Go 侧无 issue；按「跨仓同根拆开提」应另开单（issue #7 边界注明）。
- `$F.concat`/`append`/`json_dumps`/`replace` 等其它放大出口的结果规模闸（plaita-go #64 的 Rust 版）不在 #5 边界，另开单。
- 跨引擎口径差：本仓 mul 规模闸与 Python 全量口径对齐（软 100k+4×+硬 1e8），plaita-go 只保留 1e8 硬闸（其注释自称 mirror `_size_refusal`）——同输入两 Go/Rust 拒绝文案的 limit 数字可能不同（判定面：Rust 更严）。双端对账用例若日后覆盖 mul 规模拒绝，需先对齐两家阈值口径。
- round 的 min(17) 钳位拆除后，[18,1074] 区间行为从「错成 0.0 面」变为 Python 精确——golden 无 round 用例，不受影响；plaita 侧 golden 若日后补充 round 大 ndigits 用例可直接对上。
- `flow timeout 不设 cap` 维持三家一致；worker ExecutionDeadline 不在本仓范围（AGENTS「不在范围内」）。

## 第三轮（2026-10-22，okguitar 审计批：IR 正确性，两单全修）

- 基线：`git reset --hard origin/main`（f5977b8 = r2 合并后的 main）；分支 `wt-b4` 已推（1576979→8c1bc36 两 commit）
- 门禁：fmt / clippy(-D warnings) 0 告警 / `cargo test` 全仓 114 passed 0 failed（上轮口径 +5 条新测试：ir_validate 28→31、duration 15→17）；差分对账 **16/16** 一致；全程未动 golden/testdata

| # | 单 | 修法 | 负控（先红→后绿） | commit |
|---|---|---|---|---|
| 9 | fix(ir): node_from_value 把类型错误的节点字段静默降级，joinBranches 失效且校验全绿 | 三层闸，判据「present 且类型不符即错、null/缺省合法」（Go unmarshal null→零值同语义）：①node_from_value 加 `want_string_field`/`want_array_field`——next/else_next/joinBranches/branches（switch+parallel）/upstreamOutput 五字段具名拒（点名 node+field+期望+got），joinBranches 元素非字符串同拒（原 filter_map 静默丢元素）；②validate R4 改「存在但非数组/元素非字符串→具名拒」+ 边收集对非字符串 next 同闸（R1/R3 缺边的根）；③translate_if 对非字符串 else_next 给类型文案（原被 get_str 折成 "" 误报「缺假分支目标」）。语义面守恒：空串 next 仍是无后继、output/condition 任意 JSON、timeout 双拼写、branches 元素 camel/snake 折叠全不动 | typed_field_wrong_type：joinBranches:"b1"（issue 原 fixture，pre 装载出 join_branches:[] 的全绿假象）/branches:"b1"（parallel+switch）/upstreamOutput:"x"/next:42（if 形状，else_next 保底让 R1 看不见缺边）/else_next:7/joinBranches[1]:42 六路 pre Ok→post 具名拒；正控 joinBranches:["b1"] 跑出 {"parallel_result":{"b1":{"v":1}}}、缺省全后台 {}、null joinBranches、null/空串 next 全原样。CLI 实证 issue 原 fixture exit 1 点名字段 | `1576979` |
| 13 | fix(ir): ISO 时长解析放过乱序/重复分量，超时预算被静默改写 | parse_iso8601 对齐 Go isoPattern：date 至多 nW→nD、time 至多 nH→nM→nS，rank 单调扫描（重复/乱序 → `could not parse duration string "…": duplicate or out-of-order duration component`）；数字扫描收紧成 `\d+(\.\d+)?`（前导点 PT.5S——Go 正则与 Python isodate 都拒，顺手对齐，注释写明）；合法面守恒：PT/PT0S 显式零预算、PT0.3S、P1D、P1W、P1W1D、PT1H1S（跳分量）、PT2M30S 值断言、年/月仍拒、超大值 not representable 不 panic 全原样 | duration 8 路乱序/重复断言（pre：PT1S1H 实解析 3601s 求和生效）+ 装载层五份 issue 文档（PT1S1H/PT1H1H/P1D1D/PT1M1H/PT1S1S pre 全 {"ok":true} → post exit 1 could not parse）；CLI 实证 PT1S1H exit 1 具名拒，与 go-ref 同拒（Go exit 2 同核心文案） | `8c1bc36` |

**一行摘要**：第三轮两单（#9/#13）全修，负控均先红后绿，fmt/clippy(-D warnings)/cargo test 全绿（114 passed）、差分 16/16 守恒；#13 顺手对齐前导点分数（PT.5S 三家一致拒），已回写注释与台账。

## 第三轮残余与移交

- #9 按单边界只收五个 typed 字段（next/else_next/joinBranches/branches/upstreamOutput）；`isConditional`/`mode`/`collection` 等仍走原缺省面（issue 边界明示不做全字段 strict schema，误伤双拼写与任意 JSON 字段）。
- #13 未做 normalize_timeout 的 round-trip 二道闸（issue 可选项）：解析即具名拒后落库串只会是合法拼写，暂无必要。
- 两单均为 Rust 独有放行（Go/Python 装载期本来就拒），跨引擎对齐方向是 Rust 收紧、无 plaita-go 新单；修后「原本能装载」的非法文档会 fail-closed 拒载，与两家同严格度。

## 第四轮（2026-10-22，okguitar 审计批：工具链与判等正确性，收官三单全修——13 张全清）

- 基线：`git reset --hard origin/main`（8771855 = r3 合并后的 main）；分支 `wt-b4` 已推（8ae06ee→9269386→14e3a67 三 commit）
- 单号勘误：任务文本「②#10 判等 >2^53」实为仓单 **#12**；「③#12 f64 家族」是 #12 的浮点子项（相对容差/NaN/Inf/混合 int-float，同判等原语一并修，selftest 验证后并在 #12 关单注明）；仓单 **#10**（--events flow_id 归属，P1）任务文本漏述但属收官批开单，一并修清。本轮后仓内审计单 13/13 全关。
- 门禁：fmt / clippy(-D warnings) 0 告警 / `cargo test` 全仓 114 passed 0 failed；`make diffcheck` **16/16** 一致；全程未动 golden/testdata

| # | 单 | 修法 | 负控（先红→后绿） | commit |
|---|---|---|---|---|
| 11 | fix(make): diffcheck 须先构建被测 CLI，禁陈旧二进制假绿 | Makefile 加 `build-cli`（cargo build -p plaita-cli），`diffcheck: build-cli go-ref`——被测侧进依赖图，参照每次重建/被测永不重建的不对称闭合；diffcheck.py 加 `ensure_cli_current` 启动闸：CLI 缺失或 mtime 旧于 crates/*/src/**/*.rs 最新者具名报错+构建命令、非零退出、不回落旧二进制（fail-closed）；只看 src/**——tests/benches 变更不触发 bin 重建，计进去闸会永久卡死 | 三路：`make -n diffcheck` 无 cargo 行→首行 `cargo build -p plaita-cli`；`--cli /tmp/does-not-exist` FileNotFoundError 栈→具名拒 exit 1 无栈；**陈旧假绿实证**（touch main.rs 后 src mtime 1792674181 > bin 1792673594，diffcheck 照样 OK 16/16 exit 0）→同场景 refuse exit 1，`make diffcheck` 构建后真跑 16/16 | `8ae06ee` |
| 12 | fix(tools): 判等先转 f64，>2^53 不同整数判相等（假绿）+ f64 家族 | `numbers_equal` 分域：int/int 与混合 int/float 走 Python 原生 ==（精确、无精度损失：6==6.0 True、2^53+1 vs 2^53.0 False）；仅 float/float 进 `isclose(rel_tol=1e-12, abs_tol=0.0)`；NaN==NaN（双端同产即一致）、±inf 只等于自身（旧式 inf-inf=nan 是方向相反的假红）；bool 闸保持最前，dict/list 递归叶值同规则；加 `--selftest` 固化 17 条断言（整数精确/浮点容差边界 1e-15 与 1e-9/NaN/Inf/bool/递归），docstring 判等声明同步改准 | issue 原复现 pre：i64 邻近/2^53+1 vs 2^53/10^19+1 三条 True（假绿）+ inf==inf、nan==nan 两条 False（假红）→ post selftest 17/17 exit 0；主流程差分 16/16 守恒。任务③「f64 家族」同原语覆盖验证（容差/NaN/Inf/混合四子项全在 selftest），并在关单注明 | `9269386` |
| 10 | fix(cli): --events 用事件自带 flow_id 标注，勿把子流程事件记到根 flow | println_event 四变体由 `..` 丢 flow_id 改解构填 "flow"（引擎 emit 点本就正确，丢信息在 CLI 渲染层）；根标签另以 "root" 字段给出，不新引状态、不改引擎 Event/emit 点/错误文本/退出码 | 两路（zsh multios 会把 stdout tee 进管道污染断言，验收改文件承接 stderr）：child_flow.json flow_start 计数 pre {'greet_go': 2}（greet_cf 归属消失）→ post {'greet_go': 1, 'greet_cf': 1} 且 root 恒 greet_go；child_err 场景（errorHandler=continue 吸收子流失败）pre 无任何非根 error 归属（根 flow_end 带 error=假红）→ post kid_flow 失败记到 kid_flow、root_flow flow_end 干净、exit 0 | `14e3a67` |

**一行摘要**：第四轮收官三单（#11/#12/#10）全修，负控均先红后绿，fmt/clippy(-D warnings)/cargo test 全绿（114 passed）、差分 16/16 守恒；okguitar 审计批 plaita-rs 13 张单全部关闭。

## 第四轮残余与移交

- `tools/go-ref-bin` 是「已跟踪但被 `/.gitignore/` 规则覆盖」的历史遗留（5c05c9b 后未再入库），每轮 go build 后工作树恒显示 modified；建议主控 `git rm --cached` 让 ignore 生效（本轮不扩面未动）。
- zsh multios 踩坑：`cmd 2>&1 >/dev/null \| python3` 在 zsh 下 stdout 会同时 tee 进管道（结果行 'hello p' 混入事件流断言报 TypeError）；事件流验收一律文件承接 stderr。
- #12 边界守住：MANIFEST 未纳入大整数用例（独立决定）；判等原语已由 `--selftest` 17 条钉住，后续加用例可先扩 selftest。
- #10 消费者迁移（"flow" 语义变化 + 新 "root" 字段）不在本单；引擎侧 Event 面未动。

## 第五轮（2026-10-22，主动审计：Rust vs Python 执行语义差分 + 调度/状态层 + 安全面，提报 2 张 P1）

- 基线：分支 `audit2-r5` @ origin/main **e13612d**（go-ref-bin 已移出跟踪，本地构建产物已恢复，工作树洁净）
- 方法面变化：权威侧从 plaita-go 换成 **plaita Python（owner 代拍口径「逐行权威」）**——差分不再经 go-ref，Rust vs Python 双引擎同场景对照；Python 侧用 `plaita/.venv/bin/python` 实跑（`/tmp/b4r5/py_probe.py`），负控全在 `/tmp/b4r5`
- 门禁：`cargo test` 全仓 **114 passed 0 failed**（= r3/r4 基线）；`make diffcheck` 未动但产品代码零改动（探针/flow 文档全在 /tmp，无已跟踪变更）

| # | 发现 | 去向 | 证据（先红在手） |
|---|---|---|---|
| #14 | **流级预算到期可被节点 errorHandler/timeoutHandler 吞**（final_err 路径仍走 error_policy 分发；节点自带 timeout>流剩余时归属 ε 误判使 timeoutHandler 被咨询；flow timeoutHandler 还能软着陆 exit 0） | **P1 已提报** jeffkit/plaita-rs#14 | p1/p1b/p1c 三负控：Rust 吞掉后继续 node_start(e)、p1c **exit 0 输出 "soft-landing"**；Python 双场景 `ExecutionDeadlineError` 于节点 c（1.0s）end 永不执行。根因 exec.rs:306 双 `Instant::now()` + exec.rs:401-438 策略链；Python 判据 runner.py:631-634 静态比较、660-679 终态直抛 |
| #15 | **code sidecar stdin 写入在全部预算之外**——输入>64KB 管道容量 + 子进程不读 stdin（模板里用户代码先于 stdin.read() 执行）→ write_all 无限阻塞，节点/流/沙箱三预算全失效；宿主外杀后 sidecar 孤儿 | **P1 已提报** jeffkit/plaita-rs#15 | Rust 挂死 30s+ 被看门狗 kill -9（pgrep 实证孤儿 sidecar）；Python `ExecutionDeadlineError @2.0s`（`communicate(input=…)` 在 deadline 循环内）。code.rs:264-267 vs code.py:726-770 |
| A3 | 无缺省执行预算：无 timeout 自环流永久自旋（Python 引擎层缺省 1800s + `PLAITA_EXECUTION_DEADLINE_SECONDS` 可调，strategies.py:144）；Rust budget.rs 声明「worker 层负责」与 Python 引擎层缺省相悖，CLI 也无预算旗标 | P2 待裁决 | Rust 自环 15s 看门狗杀；Python env=2s → 2.0s ExecutionDeadlineError |
| A4 | 流级 errorHandler.retryTimes 整流重跑 ×(1+n)：Go-ism（Python 无流级重试，flow.py 无该字段）；副作用节点全部重放；child 的 FlowResultError 经 execute_child 包成 transient 烧流级预算 | P2 待裁决 | p4 `--events`：根 flow_start ×3（retryTimes=2） |
| A5 | parallel 分支无线程/并发上限：60 分支 spawn 60 线程=**61 并发 python3 sidecar**（Python 后台池 8+信号量 gate=峰值 9） | P2 | 采样 max 61 vs 9（同一 60×sleep2s flow） |
| A6 | http 响应帽两侧都默认 ~10MiB 但形态不同：Rust 靠 ureq `into_string` 固定 10MB（不可调、错误 1003 "response too big"）；Python 具名 `ResponseTooLargeError` + `PLAITA_HTTP_MAX_RESPONSE_BYTES` 运营可调（http.py:359-385）；Rust `maxRedirects` 落 metadata 静默忽略（ureq 默认 5 恰好同值） | P3 | 50MB 响应：Rust 1003 拒、峰值 RSS 仅 2MB |
| — | 取消面：Python `cancel()` 置位粘滞 cancel_requested（节点入口+每次重试+退避 ≤0.5s 可打断，runner.py:210-278）；Rust 引擎零取消面（`sleep_respecting`/sidecar 等待不可打断） | P3 范围缺口 | 代码面对照 |
| — | Go-ism 清单：`$ERROR` 根、流级 errorHandler/timeoutHandler、流级重试——Python 均不存在；子流预算 min 合成（r1 已拍板的刻意分歧，Python 子执行是新鲜独立预算、父预算在子流内不生效直到下一父节点边界）；并行哨兵 `{__parallel_error__,__branch__}` 双侧同形 ✓；`$BRANCH` 仅成功落 ✓ | P3 备查 | — |

- **②调度/状态层核实**：plaita-rs 刻意无 worker/租约/checkpoint 层（AGENTS「不在范围内」），本方向落到仓内实际对应物——重投语义=流级重试（A4），终态幂等无持久化面可审，崩溃恢复窗口=后台分支 `join_background` drain（r1 已加、文档化）。无新增立单点。
- **③安全面核实**：serde_json 默认 128 层递归限在位（深嵌套 flow/input/响应 JSON 均 fail-closed 拒）✓；装载预算闸（#8）✓；SSRF 开关（allowedHosts/deniedHosts/blockPrivateNetworks）装载期具名拒 ✓；资源上限缺口=A5/A6/A3 三项，其中两项已含在 #15/#14 威胁面描述内。
- 探针踩坑五记（复现者必读）：①IR 节点类型键是 `type` 非 `nodeType`（node_from_value 只认 type）；②switch 条件形状 `{field,operator,value}`——写 `op/left/right` 反序列化失败**静默落 Zero（恒真）**，p3 首版因此假绿；③parallel 节点必须写 `next`，否则 R1 不可达拒载；④code 节点不配 `input` 字段时 sidecar 收到 `null`（4 字节）——复现 #15 必须让大载荷经 `$INPUT.blob` 显式进节点 input；⑤#15 首版探针因 ④ 出假绿，机制探针（裸 pipe 对不读子进程 write 5s 阻塞实证）复核定案。
- Python 权威侧运行法（后续轮复用）：`plaita/.venv/bin/python` + `from plaita.node import register_code_node; register_code_node()`（0.4.0 起 code 节点出默认注册表）；`Flow.model_validate` 的 flow.timeout/节点 timeout 是 **ISO 串**（"PT1S"），非毫秒数。

**一行摘要**：第五轮主动审计（Rust vs Python 双引擎同场景对照，Python 逐行权威）提报 **P1 两张**（#14 流预算到期可被吞、#15 stdin 写入在预算外永久挂死），P2 三项（缺省执行预算/流级整流重投/并行无并发上限）+ P3 三项（响应帽可调性/取消面缺口/Go-ism 清单）入台账待裁决；cargo test 114P 守恒、产品代码零改动。

## 第七轮（2026-10-22，P2 三项 parity 对齐——均 r5 台账所列，规格以 Python 权威侧实读为准）

- 分支 `fix/p2-parity-trio` @ origin/main **1823ad7**（r6 合并点），四 commit：9c24ddc（①A3）/ 9b18b38（②A4）/ c55d8a3（③A5）/ 8ff24ef（AGENTS.md 文档同步）；另 chore(tools) 一笔 go-ref go.mod 跟进兄弟仓 plaita-go 的 sql_query 三驱动依赖（`go mod tidy`，环境漂移非本变更，diffcheck 门恢复可跑）。
- 方法：每项「gh/源码两头实读 Python 权威 → 负控红 → 对齐 → 负控绿」独立 commit；Python 探针 `/tmp/b4r7/py_probe.py`（p1 缺省预算 / p2 流级 errorHandler / p3 parallel 峰值），Rust 负控全部以测试固化在仓（flows.rs + ir_validate.rs + parallel_cap.rs + plaita-cli/tests 两件）。
- 门禁：`cargo test --workspace` **126 passed 0 failed**（=r6 基线 119 + 新增 7）；`make diffcheck` **16/16**；fmt/clippy(-D warnings) 干净（**教训复踩：`| tail` 吃掉 clippy 退出码致一笔带病 commit，amend 修复——判定一律文件承接退出码**）。

| 项 | Python 权威语义（实读引文+实跑） | Rust 差异 | 对齐结果 |
|---|---|---|---|
| ①缺省执行预算 A3 | `core/deadline.py`：`DEFAULT_EXECUTION_DEADLINE_SECONDS=1800.0`、`PLAITA_EXECUTION_DEADLINE_SECONDS` 可调（秒，可小数）、非法值 WARNING+回默认「未静默变成无限额」、显式 timeout（含 0=零预算 #138）优先；`strategies.py:144/185/232` 三策略统一 `execution_deadline_ms(timeout_ms)`。实跑：env=2 → `ExecutionDeadlineError @2.0s`；default_ms=1800000；env=abc → WARNING | `self.timeout==None ⇒ flow_deadline=None=不限时`，自环流永久自旋（负控：5s 看门狗 SIGKILL 仍在转） | `Flow::execute` 入口 `self.timeout.unwrap_or_else(default_execution_deadline)`；env 解析纯函数 `parse_deadline_seconds`（Err=警告文本）+stderr 告警；到期走 r6 FlowTimeout 终态；`execute_on` 不动（子流沿用父预算=Python 父 remaining）。绿：env=2 → ~2s exit 1「flow budget exceeded」 |
| ②流级 retryTimes A4 | **实读更正任务书口径**：Python **无**「流级 retryTimes 整型化/边界」语义——`core/flow.py` Flow 模型无该字段，顶层 errorHandler=未知键（`node/basic.py warn_unknown_keys`：warning「will be IGNORED」+忽略），strategies/runner/executor/client 全无流级重试环，worker 层 `flow_worker.py` 的 retry 是**节点级**断点续跑（`resume_type=retry`）。实跑：spec 带顶层 errorHandler → 警告+单次执行+错误直抛（0.0s） | 整流重跑 ×(1+n)（10ms 固定延迟；r5 实测 retryTimes=2 → 根 flow_start ×3）+child transient 烧流级预算 | 全部撤除：exec.rs execute/execute_on 拆重试环（FLOW_RETRY_DELAY 删；r6 #14 的 FlowTimeout 终态与节点自有超时→流级 timeoutHandler 面原样保留）；model.rs 删 Flow.error_handler；validate.rs 撤流级 cap+新增 `stash_unknown_flow_fields`（stderr 点名+metadata.unknownFields 双通道，嵌入流递归同享）；budget.rs MAX_RETRY_TIMES 收窄节点级。红：新契约测试 pre 失败（metadata 空+重跑存在）→ post 绿（单次） |
| ③parallel 并发上限 A5 | `core/parallel_executor.py:73`：`_DEFAULT_BG_THREAD_WORKERS=int(os.environ.get("PLAITA_BG_THREAD_WORKERS","8"))`，模块级单例池=所有 parallel（join+后台、嵌套）共同预算；池内线程跑 parallel → 嵌套降级串行内联（`concurrent.py:233-237`，防全员互等死锁）。实跑：16 分支峰值 8；env=2 峰值 2 | 每分支一线程无上限（r5：60 分支=61 sidecar） | 进程级许可池 `branch_permits`（OnceLock 一次取值=池单例同形；Mutex+Condvar 排队；RAII 名额防 panic 漏还）+TLS `IN_BRANCH_POOL`（移植 `_pool_tls.in_pool`）→ 嵌套串行内联。红：16 分支峰值 16（0.93s 完跑）→ 绿：峰值≤8；CLI 子进程 env=2 峰值≤2；env=1 嵌套内联不死锁。注：Python coroutine 模式 gather 实为无上限，Rust 三模式统一走线程+同一上限=刻意取向（注释在案）；env 非法值回 8+告警（Python import 期崩，健壮性分歧） |
- diffcheck 说明：②的新面（流级 errorHandler 行为）是**刻意背离 Go** 的面，diffcheck 是等值门不能钉分歧——16 用例无一涉及该面，16/16 守恒即足；分歧本体由 AGENTS.md「刻意分歧 7」+仓内测试钉住。
- 夹具踩坑两记：①自环流装载被 Flow.validate「必须有 end 节点」+R1 可达性拒——自旋负控用 if 真分支自指（`next:自己,else_next:end`）构造；②分支输入须写在 **branch** 上（`branches[].input`）——写在内层 flow 文档上是流级未知键、serde 静默忽略（夹具假红两轮；Python 侧 warn_unknown_keys 会告警，Rust 流级未知键卫生目前只覆盖 errorHandler，**残余口子备查**）。
- A6（http 响应帽形态）/取消面缺口/其余 Go-ism：仍 P3 台账待裁决，本轮未动。

**一行摘要**：第七轮 P2 三连对齐全落地（①缺省预算 1800s+env 可调非法告警 ②流级 errorHandler 撤除=单次执行+warn+metadata 留痕 ③parallel 上限 8+env+嵌套内联守卫），每项先红后绿独立 commit；cargo test 126P（+7）守恒、diffcheck 16/16、fmt/clippy 干净；open 0 变化（三台账项关账）；A6+取消面留 P3。

## 第八轮（2026-10-23，P3 双单落地 + Go-ism 盘点：响应帽可调 + 取消面补齐）

- 分支 `fix/p3-cap-cancel` @ origin/main **9432428**（r7 合并点），两 commit：139c766（单① A6 响应帽）/ c2d24ae（单② 取消面）
- 方法：同 r7「Python 权威实读 → 负控红 → 对齐 → 负控绿」独立 commit；Python 探针 `/tmp/b4r8/http_cap_probe.py`；负控全部固化仓内（plaita-cli/tests/http_response_cap_env.rs + plaita-engine/tests/cancellation.rs）
- 门禁：cargo test --workspace **135 passed 0 failed**（= r7 基线 126 + 新增 9：单① 3 CLI+2 单测、单② 4 集成）；`make diffcheck` **16/16**；fmt/clippy(-D warnings) 干净（判定一律文件承接退出码）

| 项 | Python 权威语义（实读引文+实跑） | Rust 差异 | 对齐结果 |
|---|---|---|---|
| 单① A6 响应帽可调 | `node/http.py:354-387`：`PLAITA_HTTP_MAX_RESPONSE_BYTES` 默认 10MiB（10485760）；`optional_int_env(minimum=1)`——strip 后 `[+-]?\d+` 严格整数（` 2048 `/`+4096` 过；空串/abc/1.5/1e3/0x10/0/-5 非法→WARNING+回默认，绝不静默）；超限=流式 64KiB 分块 `total > max_bytes` 即抛 `ResponseTooLargeError`（消息点名 maxResponseBytes 与 env 提升路径）→ **1003 节点级策略错、可重试**（handle_http_node_err：有响应快照非 send_request_fail）。实跑：env=1024+2KB body→code=1003 具名；默认帽→200 正常过 | ureq `into_string` 固定 10MiB 不可调（模块头明写 caps 未移植，r5 台账 A6） | `parse_max_response_bytes` 纯函数镜像值域（None→默认静默；>u64 存不下 Python 无界 int→警告+回默认=刻意 fail-closed）+ `into_reader().take(cap+1)` 流式帽读；边界=`total > cap`（恰在帽上过/帽+1 拒，Cursor 单测钉住）；IO/非 UTF-8 维持 `read body` 旧方言 1003。红：env=1024 时 2KB 照常 200（三连红，含两轮测试自身竞态修复）；绿：exit 非零+具名消息/非法 env 告警+照常完成 |
| 单② 取消面 | `core/runner.py:210-278,580-720` + `executor.py:297-313` + `context.py:282-292` + `errors.py:221-248`（语义出处=**plaita#267** 验收「cancel 在 ≤1s 内打断重试/退避」，与 r7 A3 同源；任务书所指 #28x 实为 console/worker 面单、与引擎取消面无关，以四文件实读为准）：`FlowExecution.cancel()` 幂等置位一对信号——`cancel_event`=节点作用域（code 沙箱 `_popen_wait_cancellable` 观察即 killpg 整树；节点入口复位防误杀下节点）+ `cancel_requested`=粘滞（节点边界拒绝、先于重试与 errorHandler，`FlowCancelledException` code=-4 取消专属）；退避 0.5s 分片轮询可打断；取消优先于任何节点错误/重试分类；默认语义=步界取消（在途非沙箱节点跑完）；父子共享同一对 Event 实例 | 零取消面（r5 台账；负控 RED=编译期 `no method named cancel`，API 整体缺位） | Host 增一对 `Arc<AtomicBool>`（Clone 共享同一实例=父子共享对应面）+ `cancel()` 幂等 API；run_nodes 节点边界门（before running）+ execute_node 尝试门（while running）+ 错误分类前取消优先臂（continue 策略接不到）+ 节点入口 `clear_cancel_event`；`sleep_backoff_cancellable` 500ms 分片（`_CANCEL_POLL_INTERVAL_SECONDS` 同值）；`FlowError::Cancelled`（Display=code:-4 方言，execute/execute_on 显式终态臂不经任何 handler）；code 沙箱 10ms 等待环观察旗标→killpg（`WaitOutcome::Cancelled` 被取消优先臂改判，永不进策略链）。绿四景：①沙箱 ~300ms 打断+ps 实证进程组收尸 ②慢 http 在途跑完→下一节点入口 before running 具名拒 ③errorHandler continue+retryTimes=3 退避中取消→不洗成成功 ④parallel join 分支共享 Host、取消传播整链 |

**Go-ism 盘点（r5 P3 条目，本轮只盘点不动工，逐条现状/Python 权威/建议）**：

| 项 | Rust 现状 | Python 权威语义（实读） | 建议处置 |
|---|---|---|---|
| `$ERROR` 根 | exec.rs：error_policy continue/continue-with 时 set_node_error 写 `$ERROR`（code/message/info）供下游引用 | **全仓无 `$ERROR` 根**：错误只进 logger.warning + on_node_end 事件载荷 `{code,message}`（runner.py `_node_error_payload`）；continue 的节点结果=None | P3 待裁决：撤=严格 parity（diffcheck golden 含 Go `$ERROR` 用例，撤除需过一遍）；留=入刻意分歧清单 |
| 流级 timeoutHandler | r6 拍板保留（节点自有超时 abort 传播时咨询；Go 移植面，留回退开关） | flow.py Flow 无该字段=未知键 warn+忽略（r7 实读） | r6 拍板在案，维持现状不重开 |
| 子流预算 min 合成 | execute_on 沿 `rc.flow_deadline`=父 remaining（r7 台账注记「=Python 父 remaining」） | **r7 注记有误，本轮实读更正**：子执行=新 `FlowExecution`（executor.py:397-402 `get_child_execution`），预算由**子 flow 自身 timeout** 经 strategy 链新鲜解析（缺省 1800s，strategies/executor_compat 链），父 remaining 不穿透（child.py:77-84）——Python 比 Rust 宽松（父 100s+子无 timeout ⇒ 子预算独立 1800s） | P3 待裁决：对齐=execute_on 走新鲜预算 `min(子 flow timeout, 1800s)`；注意 r1 审计的「父 1s+子 1h 逃逸」恰是 Python 原语义（父预算本就不管子流内部），对齐前先拍板取舍 |
| maxRedirects | 装载 stash 进 node metadata（normalize.rs:642）运行期忽略；ureq 默认跟随 5 | http.py:1014 `max_redirects=5` 构造参数；1166/1280 显式跳数循环；超限具名 "Too many redirects (> N)"（1003 族） | P3 待裁决：读 spec→ureq `redirects(n)`（小改动）或至少装载期告警；仅声明 ≠5 的 flow 受影响 |
| 并行哨兵 `{__parallel_error__,__branch__}` | 双侧同形 ✓（r5 实测） | 同形 | 无需处置 |
| `$BRANCH` 仅成功落 | 双侧同形 ✓ | 同形 | 无需处置 |
| 流级 errorHandler / 流级重试 | r7 已撤除（单次执行契约） | 无该语义 | 已关账 |

- 探针/夹具坑五记（复现者必读）：①测试内一次性 HTTP server 未读全 GET 即关→macOS 对未读收缓冲回 RST 掐断客户端读状态行（抖动假红；循环读到 `\r\n\r\n`+排空修复）；②三测并行共用同一临时 flow 文件互相覆盖 URL→两个 CLI 撞进对方的一次性 server（按测试名分文件修复）；③code sidecar 模板必须有 `def run(input):`（"No run function found" 具名拒）；④「在途节点」步界负控**不能用 code 节点**——它本身是沙箱、取消可打断（①已证），要用慢 http 当非沙箱在途节点；⑤parallel 分支默认 background（fire-and-forget、节点立即 Ok 空 `{}`）——取消传播测试须 `joinBranches:["b1","b2"]` + 分支 `name`，且分支子 flow 文档间节点 id 不冲突（都用 "c" 即可）。

**一行摘要**：第八轮 P3 双单落地（单① `PLAITA_HTTP_MAX_RESPONSE_BYTES` 旋钮对齐 Python 契约=默认 10MiB/严格整数≥1/非法告警回默认/超限具名 1003 可重试；单② 执行级取消面=`cancel()` 幂等置位粘滞意图+节点边界具名拒绝 code:-4+code 沙箱 killpg+退避 0.5s 分片打断+取消优先于任何策略+父子链共享传播）+ Go-ism 盘点入账（含 r7「子流沿父预算=Python」误记更正：Python 子执行是新鲜独立预算）；cargo test 135P（+9）守恒、diffcheck 16/16、fmt/clippy 干净；负控先红后绿（单① 三连红/单② 编译红）。

## 第九轮（2026-10-23，plaita-go#80 节点 parity 的 Rust 侧：8 类转正 + capability 契约面 + 防漂移门）

- 背景：jeffkit 直接令「plaita-go#80 最终对齐了 python 原版节点，rs 版是不是也没对齐？请帮忙对齐」。当时 rs 白名单仅 10 类（start/end/assignment/switch/if/parallel/loop/child/http/code），Python 权威 45 类（18 core + 27 插件，经 plaita-go 的 NodeRegistry 提取器现场确认）。
- 分支 `feat/node-parity`（worktree plaita-rs-wt2，基线 r7=9432428；**rebase 到 r8 合并点后合并 main=08dbb75**；CI 修复两枚 follow-up：f7fb0cc setup-python SHA、aab294e 差分门绝对路径）。
- **范围裁决（对齐≠全量 45 类）**：本轮转正**引擎内可完整落地的 8 类**；挂起族 7（event/hitl_await/delay/approval/redis_queue/kafka_queue/http_callback——等待/恢复全在引擎外，实装前置=先建 suspend/resume 执行架构）与外部宿主族 21（agentrun+plaita-nodes）保持 fail-closed 并逐类写具名 hint；calculate/ignore 保持拒载（plaita 同样不注册，`unRecognized node type:` 子串逐字对拍）。
- 转正 8 类（权威=Python 真跑，golden 67 例逐例对拍先红后绿；golden 文件自 plaita-go testdata 逐字复用）：
  - **集合五类** map/filter/find/reduce/while（loop.py BaseCollectionNode 家族）：字符串集合按 JSON 数组解析/普通串单元素；reduce initial 缺省取首元素、空集合无 initial=确定性 IndexError；while 先判断后执行/condition 缺省单轮/max_iterations 缺省 1000 触顶仅告警；map concurrent（async 历史别名）走进程级许可池（缺省 8=PLAITA_BG_THREAD_WORKERS）+maxConcurrent gate（负数装载期拒）+池内降级串行+错误取最小失败下标。
  - **case** = SwitchLegacy 装载期脱糖 switch（Branch(eq target, value, next=case.id)+恒 isDefault 兜底；default 缺省=字面 "default" 节点 id；target 缺省以 `$NULL` 承载；缺 id/value 装载期拒；metadata.caseDialect 留痕）。
  - **mock** = value 原样透传不求值（pin 语义）、output 忽略。
  - **reference** = `ir::load_with_flow_provider`+`FlowProvider` 装载期注入（inline childFlow 具名拒——Python 运行时把它丢掉执行期才炸；深度 32 环具名拒）；`RuntimeCtx::child_isolated`：GLOBAL/ENV/执行身份继承、`$PARENT` 指向空 ambient 执行（`$PARENT.<x>`→null 隔离实测口径）、input 缺省继承调用方 `$INPUT`。表达式层配套 `git`：`$PARENT.<未知根>`→null（Go r78 plaitaexpr 同款；无父执行仍报错）。
- **capability 契约面**（plaita#300 五键）：`ir::capability_manifest()`+`CONTRACT_VERSION=2026-10-23.1`；features/node_dependencies 恒空。
- **防漂移门**（make node-parity + CI 同名 job）：vendor 提取器从 plaita NodeRegistry 现场取权威清单（entry points 不可见/插件加载失败硬失败 exit 2，绝不输出缩水清单）；`tests/node_type_parity.rs` 双向差集必须恰等于显式例外表（28+1：挂起族 7+外部宿主族 21+Rust-only code），例外条目必须在 KNOWN_UNSUPPORTED 有具名 hint。负控：权威缩水→红实测、未设 env→skip。
- 门禁：golden 67/67（39 collection+28 parity2）；cargo test **16 二进制全绿**（r8 的 135P 守恒+新增 golden 2/差分门 2/隔离与 $PARENT 单测）；clippy -D warnings/fmt 干净；diffcheck **16/16**；rebase r8（exec.rs/nodes/mod.rs 交叠）后全门禁复跑；GitHub CI 双 job 全绿（39836853826）。
- **CI 首跑两踩**（已修）：①setup-python 的 v6 SHA 凭记忆手写、action 不存在秒红——pin SHA 一律抄兄弟仓在用值；②cargo test 的测试进程 cwd=包目录（crates/plaita-engine），差分门的相对路径 node_types.json 解析不到（本地 /tmp 绝对路径侥幸掩盖）——CI 传 `$PWD` 绝对路径。
- AGENTS.md：节点矩阵/刻意分歧第 9 条/本轮记录入档（顺带修复先前轮次遗留的整段重复）。
- **留白（下轮候选）**：挂起族 7 类的实装前置=引擎 suspend/resume 架构（事件订阅存储+恢复事件+恢复后续跑），是独立立项不是节点补齐；plaita-nodes 族与 agentrun 同前（宿主基础设施）；Go-ism 盘点表（r8 所列 `$ERROR` 根/子流预算新鲜解析/maxRedirects）未动。

**一行摘要**：第九轮 plaita-go#80 parity 对齐——集合五类+case/mock/reference 共 8 类转正（golden 67 例逐例对拍 Python 真跑 oracle 先红后绿）、reference 走 FlowProvider 装载期注入+child_isolated 隔离（$PARENT→null）、capability manifest 2026-10-23.1、Python↔Rust 清单差分门（例外表 28+1 全列明+负控双实证）；挂起族 7+宿主族 21 明确留白 fail-closed；cargo 16 二进制全绿/diffcheck 16-16/CI 双 job 绿，main=08dbb75（+aab294e CI 修复）。

## 第十轮（2026-10-23，双仓联动工单卡施工：A1 r132 卡 G-3+G-1×plaita-go + 卡 R-3×plaita-rs——跨语言契约矿脉卡化的落地轮）

- **编号注记**：派工书称「第九轮」——台账第九轮已被 node-parity 线占用（08dbb75+两 CI 修复在本轮开工后才落到 origin/main），本轮按序记**第十轮**；派工基线 899047d 已漂移至 aab294e，R-3 已 rebase（零冲突，parity 动 crates/、本轮只动 tools/）重推。
- 分支与 commit：plaita-go `fix/golden-expand`（基线 2787a95）两 commit **f5074a7**（G-3）/ **cdcd8e4**（G-1）已推 origin；plaita-rs `fix/cards-g3-r3`（基线 aab294e）一 commit **0d55f43**（R-3）已推 origin。
- **G-3（golden 扩例）关键更正=先分层再动手**：branches-str/next-int **不属 hash 层**——`branches:"oops"` 是合法 JSON、FlowDefinitionHash 照算不误（首版误钉 flow_hash golden 的 goerr_ 形态，TestFlowHashGoldenGoRejects 实测红暴露分层错位）；改钉 pkg/ir `TestLoadRejects` 两案（go typed-unmarshal 文案 `cannot unmarshal string into Workflow.nodes.branches of type []*nodes.Branch` / 可达性门 `not reachable from start`，go-ref 实测定 want 子串）。flow_hash golden 实钉三案：goerr_neg_infinity_literal（G-2 收口）/ err_depth_10000（exporter 新 `export_when_py_rejects` 形态：Python json.loads 自身 RecursionError〔~9998 环境敏感界，O3 owner-call〕→无 canonical/hash 可录，只钉 Go 解码层拒）/ struct_depth_100_nested（canonical+hash 逐字节等）。golden 55→58→62 案全程 live Python 重导出、存量零扰动。
- **G-1（混编撕裂修复对 Go 半边）**：`pkg/ir/surrogates.go RejectLoneSurrogateEscapes`——扫 raw 文本字符串内 \uD800-\uDFFF 转义，高代理必须后随成对 \uDC00-DFFF（成对=真实非 BMP 照旧收）、字面 U+FFFD 照旧收、`\`+`ud800` 文本/逃逸引号不误判（假阳守卫单测钉住）；**双执法点同源**：ir.load（引擎装载）+ worker canonicalFlowDefinitionJSON（指纹层）；flowsource 错误分层未动（Redis 路径指纹错=ErrFlowLoad 瞬态→MaxDeliveries→DLQ 收敛、console 路径=确定性负缓存；残余观察项在案）。负控四景先红后绿：golden goerr_lone_surrogate_high/low（exporter 新 `py_hash_rejects` 形态：Python loads 收/hash 链拒，无 canonical-hash 只钉 Go 必拒；红签名=`FlowDefinitionHash accepted …must stay rejected`）+ TestLoadRejects 两案（红=缺 lone surrogate 文案）；正控双绿防过拒：struct_paired_surrogate_escape（成对转义→raw UTF-8 canonical 等）+ u_fffd_escape。
- **R-3（diffcheck 扩例）**：等值门放宽为**失败类**（卡 R-3 明文「只比失败类不比文案」——原实现要求 Go 核心文案是 rs 报错子串，比 docstring 自述更严；branches-str/next-int 两引擎文案全不同全靠此放宽，子串命中仍标 same core message 留观察面）；MANIFEST +5 绿（neg-infinity/depth-10000/depth-100/branches-str/next-int，当日全绿）；**KNOWN_RED 机制**：lone-surrogate-high/low 以 expected-shape（go ok/rs error）入册标待 G-1，形状漂移即红并给归因指引（双侧同拒=升 MANIFEST 翻绿进门；rs 开始收=回归另查）；**翻绿生命周期已实测**：修复态 go-ref（replace 指向 fix/golden-expand 构建）→ 21/21 manifest 保持绿 + 两案形状 error/error → exit 1 带「升入 MANIFEST」指引。深度 150 按卡永不进门（R-4 另卡）。
- 门禁：plaita-go gofmt/vet 干净、`make test` 唯一红=在案出口解耦红（r69 记录）；plaita-rs fmt --check/clippy -D/test rc 全 0，**140 passed 0 failed**（=r8 135P + parity r9 的 +4 +隔离单测 +1；本轮零 Rust 源变更=构造性守恒）、diffcheck **21/21 manifest + 2/2 known-red，exit 0**（退出码全文件承接）。
- 教训两记：①**/tmp 文件名 b4rN 惯例席内复用会被邻轮覆写**——本轮 /tmp/b4r9_cargo_test.txt 与真 r9（parity）撞名，日志考古虚惊一场；门禁判读一律以「已推树哈希+独立文件名复跑」为准。②卡片引文不标层时以实测定层——golden 钉入先问「这案在哪一层拒/收」：hash 层（flow_hash golden）管合法 JSON 的指纹等价、装载层（TestLoadRejects/diffcheck）管拒载一致性，钉错层测试会用红告诉你。
- 待办移交（下轮）：G-1 主控验收合并 plaita-go main 后——①`make go-ref` 重建 ②diffcheck 两 known-red 照指引升 MANIFEST 翻绿（known-red 表清空）③随 fix/cards-g3-r3 合并收口；修复对另半边=A-1（console 发布闸 422，B3 线）。

**一行摘要**：第十轮双仓卡化施工——plaita-go fix/golden-expand 两 commit（G-3：flow_hash golden 3 案+TestLoadRejects 2 案，branches-str/next-int 分层更正改钉装载层；G-1：RejectLoneSurrogateEscapes 双执法点拒孤立代理，负控四景先红后绿+正控双绿防过拒）；plaita-rs fix/cards-g3-r3 一 commit（R-3：diffcheck 等值门放宽失败类+5 绿 manifest+KNOWN_RED 待 G-1 机制，翻绿生命周期修复态 go-ref 实测闭环）；深度 150 不进门；go 门禁唯一红=在案出口红，rs 140P 守恒+diffcheck 21+2 绿；派工「第九轮」与真 r9（parity）撞号，本轮记第十轮+基线漂移已 rebase。

## 第十一轮（2026-10-23，卡 R-3 生命周期闭环：known-red 翻绿转正升 MANIFEST——混编撕裂 Go 半边修口+门收口收官）

- 基线：plaita-rs origin/main `707fea3`（R-3 已合）、plaita-go main `a20da36`（G-1 已合）；分支 `fix/r3-close` 一 commit **`b881ab3`** 已推 origin。
- 闭环程序（r9 预埋翻绿路径全按剧本走，先红证翻转再动表）：`make go-ref` 重建吃新 plaita-go main → diffcheck 先红实证（21/21 manifest 保持绿 + lone-surrogate-high/low 形状 ok/error → **error/error**，exit 1 带升入 MANIFEST 指引）→ 按指引转正：两案升 MANIFEST（带转正注释注明 G-1 已合/形状翻转）、KNOWN_RED 清空**留机制**（注释改写为机制现状；运行时三处「待 G-1」烙死语一并去掉，负控机制保持可复用）、深度 150 按卡仍永不进门 → diffcheck **23/23 全绿 exit 0**（两孤立代理案以双侧同拒进门；同轮 AGENTS.md 差分门读数 16/16→23/23）。
- 门禁：零 Rust 源变更（diffcheck.py + AGENTS.md 两文件），cargo test **140 passed 0 failed 守恒**、clippy -D warnings 净、fmt 零改动。
- 收官注：plaita-rs 差分门至此=**完整 23 案全绿态**——R-3 卡全生命周期（5 绿先行→known-red 负控→G-1 修口→翻绿转正）闭环，混编撕裂修复对 Go 半边+门收口完结；另半边 A-1（console 发布闸吞 hash 失败，plaita 仓）属 B 线后续不在本轮。
- 编号注记：派工书称「第十轮」——台账第十轮已被双仓卡化施工占用，本轮按序记**第十一轮**。

**一行摘要**：第十一轮卡 R-3 收官——go-ref 重建后两 known-red 实测翻 error/error（先红证翻转再动表），lone-surrogate 两案升 MANIFEST 转正、KNOWN_RED 清空留机制、深度 150 仍永不进门；diffcheck 23/23 exit 0、cargo 140P 构造性守恒、clippy/fmt 净；fix/r3-close@`b881ab3` 已推，plaita-rs 差分门=23 案全绿态，R-3 全生命周期闭环（混编撕裂 Go 半边修口+门收口），A-1 属 B 线后续。

## 第十二轮（2026-10-23，引擎 suspend/resume 架构 + 挂起族七类转正：plaita-go#80 phase 3 的最后一档）

- 背景：jeffkit 令「引擎 suspend/resume 架构要实现的，继续立项完成吧」——r9 的留白面（挂起族 7 类的前置=执行架构）直接立项闭环。
- 分支 `feat/suspend-resume` @ origin/main 2003f05（r11 合并点），单 commit 3113a94，合并 main=**237aa75**，CI 双 job 绿（39879912346）。
- 范围闸（Go checkpoint v1.5 同款 fail-closed）：只有根流程可挂起，嵌套 child/loop/parallel 里的挂起点具名拒（恢复位置无法用执行状态表达）。

| 面 | 权威（Python 实读 + Go 移植面） | Rust 落地 |
|---|---|---|
| 挂起边界 | `strategies._execute_current_node`：pending 载荷产出→订阅失败禁止挂起→`is_suspend=True` 输出停步；Go `Workflow.suspend`：pending 校验/订阅注册/subscription_id+tenant 落挂起态/$NODE/SuspendError 载 Checkpoint | `Flow::suspend_flow`：pending 校验→宿主注册表登记（**无注册表=无唤醒源具名拒**，Host::with_subscriptions）→subscription_id/tenant 写挂起态+`$NODE`→`FlowError::Suspended(Box<SuspendedExecution>)`（载 flow+rc+host）；FlowEnd 对挂起按无错误上报；`run_nodes` 抽 `advance_from`（start/resume 共用步进记账，Go 三入口同形） |
| 恢复入口 | `_handle_resume`：挂起型/pending 校验/ResumeType∈{event,timeout,cancel}/**plaita#221 绑定**（`__plaita_subscription_id` 准入后剥离+event_type 一致）/节点多态 resume/挂起态全量保留仅翻 status+event_data/NodeEnd/订阅注销；Go `Workflow.Resume` 恢复+续跑一步到位 | `SuspendedExecution::resume(kind,payload)`：同款准入链→`resume_node_body`（extended 家族回填 event_id/service_config/node_subtype/subscription_*；event 节点恢复后保持 execute 解析的 event_type〔Python dict.update 覆盖序〕；hitl_await 的 event_data 嵌套合并+status/replies/session_id）→NodeEnd/注销→后继 `advance_from`。**恢复会话预算新鲜发放**（flow timeout 或缺省 1800s——24h 审批不再落在过期 deadline 上，Python worker 语义） |
| 节点面 | server/nodes 五类 + event_node + hitl_await（逐字段：event_id=event_<id>_<ms>/delay 60s 回退+单位乘法表/approval 缺省 24h 订阅超时+显式 null=无限/http_callback 自动路径+三型认证+uuid 凭据/kafka bootstrap 逗号切分/hitl dry_run+图片探路降级+/api/send 协议） | `nodes/suspend.rs` 七执行体/恢复体 + hitl 客户端（http feature 门，default 开） |
| ir 装载面 | pydantic 模型层：必填/Literal 大小写归一/缺省填充/固定事件契约（approval_decision 等 4 类，delay 可配 delay_trigger）/approval null→-1 哨兵 | 七 `translate_*` 函数（Go translate_server.go 逐条移植）+ known keys 表；SUPPORTED 18→**25** 类；KNOWN_UNSUPPORTED 撤七条 |
| 宿主面（刻意不在引擎内） | 订阅存储实现/订阅超时到期投递（SubscriptionTimeoutChecker）/五节点 service 层（deployment 设施） | manifest `node_dependencies` 七节点逐个标注；`CONTRACT_VERSION=2026-10-23.2`；差分门例外表 28→21（外部宿主族） |

- 验收：**server_nodes golden 30 例逐例对拍 Python 真跑 oracle 先红后绿**（挂起态对 `$NODE` 对拍+恢复走 golden 预置载荷+订阅绑定注入+剔键〔subscription_id/tenant/created_time/trigger_timestamp 族/timestamp/recorded_at〕+hex32/ms13 打码两侧同款）；引擎面单测 12 例（作用域闸/无注册表拒/绑定准入/状态机 pending→completed/timeout/cancelled/订阅注销/恢复记账/$LAST_NODE 维持前节点/hitl dry_run+真实发送〔本地 HTTP stub 断言 /api/send 协议〕/approval 24h 缺省·null=无限）。
- 门禁：cargo **18 二进制全绿**（140P 基线守恒+golden 1+suspend 12）；clippy -D warnings/fmt 干净；diffcheck **23/23**；node-parity 门绿（活体提取，例外表收窄后）。CI 双 job 绿（39879912346）。
- 踩坑两记：①cargo test 测试进程 cwd=包目录（r9 已踩），本轮 unit test 全用 CARGO_MANIFEST_DIR 相对；②FlowError 载 SuspendedExecution 后 clippy result-large-err——Box 装箱。
- 遗留：checkpoint 的 JSON 回放入口（跨进程恢复）属 worker 层接线，本引擎只做进程内 resume（`checkpoint_json()` 供取材）；订阅超时自动到期投递是宿主职责（引擎提供 `resume(Timeout)`）。

**一行摘要**：第十二轮挂起/恢复架构+挂起族七类转正（engine: 挂起边界+订阅注册表宿主面+FlowError::Suspended 载现场；resume: plaita#221 绑定准入+挂起态全量保留+恢复续跑一步到位+预算新鲜发放；golden 30 例对拍 Python 真跑先红后绿+单测 12；ir 七 translate；CONTRACT_VERSION 2026-10-23.2+node_dependencies；例外表 28→21；cargo 18 二进制全绿/diffcheck 23-23/CI 绿，main=237aa75）——至此 plaita-go#80 的全部节点面（除外部宿主族 21 类与 sql_query）在 plaita-rs 对齐完成。

## 第十三轮（2026-10-23，双程式立项：嵌套挂起三平台 NS-M1（rs 试点）+ rs worker WK-M1；jeffkit 令「嵌套挂起三平台都要+rs worker 立项，立目标带子智能体团队持续推进」）

- 立项面：程式章程 docs/PROGRAM.md（plaita-rs，目标/语义契约/里程碑/验收）；三仓 issue 互链——rs#16（嵌套挂起 v1）/rs#17（worker）/plaita-go#88（NS-M2）/plaita#311（NS-M3，顺带修 fail-open）。
- 子智能体团队：设计验证席 A（嵌套挂起设计对三仓深挖，在途）/ worker 规格席 B（✅ docs/specs/rs-worker-design.md 811 行：键空间 26 项总表/三段 Lua 逐字移植/E1-E4 引擎最小接口/恢复禁止 fork 红线/M1-M3 里程碑各配行为测试）/ worker 实现席 C（WK-M1 骨架，独立 worktree plaita-rs-wt3 分支 feat/worker-m1，在途）。
- NS-M1 已落（feat/nested-suspend@80b13a6+e8b6aae，待 A 报告核对后合并）：FrameKind 作用域闸（Root/ChildCall/ReferenceCall 放行；LoopIteration/ParallelBranch 具名拒=FlowError::SuspendRejected 专属类型——parallel join 哨兵不得吞挂起拒绝）；传播=child/reference 节点体转 NodeError::suspension 载体+execute_node 策略链前转发+每层 advance_from 压帧（压入序=传播序）；resume 续跑循环（决策步→内层完成→返回步回填父帧+outputType 校验+每帧预算新鲜发放→根完成；再挂起=栈拼接重入）；checkpoint_json 载 call_stack 路径。
- 测试：suspend.rs 3 例改造+nested_suspend.rs 4 例（孙代双帧栈/reference 隔离/恢复中再挂起栈合并/深位绑定准入）；19 测试二进制全绿/clippy -D/fmt/diffcheck 23-23/node-parity 绿。
- 踩坑复踩（第三次）：多补丁 python 脚本中途 assert 失败丢整批未写——**补丁脚本一律单补丁单写**。

**一行摘要**：第十三轮双程式开跑——章程+四单立项；worker 规格席交付 811 行设计（Redis 协议与 plaita-go 二进制兼容）；NS-M1 嵌套挂起 rs 试点落地（FrameKind 闸/SuspendRejected 专属拒绝/传播载体/沿栈恢复+栈合并，7 专项测试，全门禁绿，分支 feat/nested-suspend 待验证席报告核对后合并）；worker 实现席在途（feat/worker-m1）。

## 第十三轮·续（2026-10-23，验证席报告核对与 NS-M1 修补合并）

- 验证席 A 交付 docs/specs/nested-suspend-validation.md（174 行/57 证据）：**核心坐实**——reference 的 ambient 父链遮蔽 frame_chain_blocker 链行走（loop→reference→挂起被放行→carrier 被剥成 transient→重试 N 次×N 孤儿订阅×可被 errorHandler 吞）；SuspendRejected 无拦截臂可被 continue 吞；后台分支拒绝只 eprintln；恢复续跑绕过本帧收尾链（Q8.1）；resume 无取消门。7 必须修全落（4cecc64）：**主闸移至传播转换点**（调用方帧 kind 永远真实；拒绝时注销已注册订阅=无孤儿）+ run_collection_child stringify 消灭 + execute_node SuspendRejected 拦截臂 + parallel 派发前静态闸 flow_contains_suspending（后台分支覆盖）+ advance_and_finalize 抽取（恢复续跑逐帧复用本帧收尾链，Q8.1 修正）+ resume 入口取消门。新增 4 旁路测试。NS-M1 合并 main=ce8df7b，CI 绿（39891483537）。
- NS-M2 实现席 D 在途（plaita-go-wt-ns2 分支 fix/nested-suspend；挂帅裁决：SuspendError 携带活帧调用栈——不做 Checkpoint 帧扩展，留 worker 层）。worker 实现席 C 在途（feat/worker-m1）。
- 语义细则钉入 AGENTS.md：双层闸（转换点主闸+advisory 链检+静态分支闸）/恢复续跑复用本帧收尾链/父帧子节点策略链不作用于续跑/子节点 node.timeout 只覆盖首次执行/恢复无 FlowStart。
- 教训（第四次复踩确认）：多补丁脚本断言失败丢整批——本轮已全部改为单补丁单写盘。

**一行摘要**：十三轮续——验证席 174 行报告 7 必须修全落（转换点主闸/reference 遮蔽兜底/拦截臂/静态分支闸/收尾链复用/取消门+4 旁路测试），NS-M1 合并 main ce8df7b CI 绿；NS-M2（go）与 WK-M1（worker）两实现席并行在途；plaita#311 已留协同进展。

## 第十二轮（2026-10-23，全量入位第二步：manifest 位次——plaita-rs feat/manifest-slot `8fbce14` + plaita manifest-console `d78a31a`，均已推 origin 待主控验收合并）

- **基线对账**：origin/main `f0cd802`（r11 hash 门+NS-M1 ce8df7b 已在册，任务书引 f0cd802 为 NS-M1 合并实为 hash 门合并，两线均零交于 manifest 面）；实读 console 权威符号=`plaita/server/capability.py`（ENGINES/ENGINE_TO_RUNTIME/MANIFEST_KEYS/manifest_from_metadata）+ `plaita/server/runtime_queues.py`（VALID_RUNTIMES），gate=`plaita-console/backend/services/capability_gate.py:143`（runtime_for_engine(manifest.engine) 选池）。
- **①引擎侧**：`ir/mod.rs` capability_manifest 键名 `contract_version`→`ir_contract_version`（Go a20da36 json tags 逐字对拍；旧键名会被 console 形状闸判「无清单」fail-open——本轮抓到的真缺口）；新增 `capability_manifest_json_shape_matches_go`（对拍 Go TestCapabilityManifestJSONShape 六键全集/features 空/deps 对象），cargo 176→177P。
- **②位次面**：console `ENGINE_RS="plaita-rs"` 入 ENGINES + `ENGINE_TO_RUNTIME["plaita-rs"]="rust"` + VALID_RUNTIMES+="rust"（队列通用推导 `plaita:flow:queue:rust`，不设专用 env）；flows.py 文案同步。**★ registry 路径存在性核实结论：Rust 池不经 registry**——plaita-rs main 无 worker crate（rs-worker-design G6 registry 心跳，实现在 feat/worker-m1 未合并），本步按任务书收窄为「manifest 产物+console 识别」，registry 写入记 r13 后 worker 落地时对齐 plaita-go #82（13d1aa6：meta.engine+嵌套 capability_manifest，序列化失败整键省略+PLAITA_CAPABILITY_MANIFEST_UNAVAILABLE）。
- **③负控**（先红后绿，双侧咬合实证）：Rust 侧回退键名→2 案当场红；console 侧回退 ENGINES→TestRustPoolGate 1F+3E（422 变 fail-open 200）。Rust 实产 manifest 字面量（25 supported/29 unsupported/features 空/deps 七键，ir_contract_version=2026-10-23.2）喂 manifest_from_metadata ACCEPT（A1 r129 喂法，第六键加法放行同 Go 口径）；旧键判无清单锚定；engine=plaita-rust 拒绝守恒；rust 池三景承 go 池矩阵（交集 ACCEPT 落 rust 队列不串池/sql_query 422 结构化 draft 不动/缺清单 fail-open+具名 WARNING）；调度行 runtime=rust 路由+normalize/resolve 接线。
- **门禁**：cargo test **177P/0F**（20 二进制）/clippy -D 干净/fmt 干净/diffcheck **23/23** exit 0（退出码文件 /tmp/b4r12-*.rc）；console 全量 **1523P/15S+2 基线红**（test_issue_a2r19_partial_reads metrics 域，stash 后 origin/main 同红实测零交集）；issue300 家族 manifest 30P+gate 27P+trigger 22P+schedule 服务族 37P。
- 教训：多类插入测试脚本用锚点类名定位时，插入点必须选类边界（本轮首插把新类插进既有类中间吞并 2 方法——git diff 结构核对抓出回滚重做）；worktree 无 .venv，pytest 走系统 python3+sys.path 注入 worktree 根即绿。

**一行摘要**：第十二轮 manifest 位次双仓落地——rs 侧键名对齐 Go json tags（contract_version→ir_contract_version，旧键名会被 console 形状闸 fail-open 判无清单的真缺口，负控双向咬合）；console 侧 ENGINES 注册 plaita-rs→rust 池（门/路由/文案三处），Rust 实产 manifest 喂五键闸 ACCEPT+rust 池三景守恒；**registry 路径核实=Rust 池不经 registry（worker 未落地），收窄为 manifest 产物+console 识别，写入接线记 r13 后**；cargo 177P/diffcheck 23-23/console 1523P+2 基线红（main 同红）；feat/manifest-slot `8fbce14`+manifest-console `d78a31a` 已推待验收。

## 第十三轮·NS-M2 收官（2026-10-23，plaita-go 嵌套挂起合并 main dea859f，CI 绿）

- 实现席 D 交付 fix/nested-suspend（4 提交，rs#16 十用例对拍镜像测试集 490 行）。**实现中实弹修掉两个关键缺口**：①FrameKind seed 标注被子流程 prepareExecutionRuntimeContext 的新建上下文抹掉（闸失明）→ NewExecutionRuntimeContext 单点从 seed 传播；②Event.Resume 裸 rebuild 丢挂起态（event_id/subscription_id）→ 补全量保留（对齐 rs resume_node_body 契约）。
- 主控验收：gofmt/vet/build 净、嵌套十用例全绿（注册/注销计数断言、栈路径断言与 rs 逐点对齐）；TestWorkflow_Execute 两子测试系存量失败（基线实跑核实，与本分支无关，建议另开单）。
- **共享主仓竞态实录（纪律三防的活教材）**：在共享主仓做合并期间，B3 席 r94 正在同一 checkout 活跃提交推送——我的合并提交 4b3abc3 被其 reset/推进序列孤立（rev-parse 与 git log 相隔数秒给出矛盾视图）。改走干净打法：分支 rebase 到 r94 最新 main（冲突零）→ 临时 detach worktree 合并 → push HEAD:main（dea859f）→ 临时 worktree 即拆。**规约固化：共享主仓永不直接合并；一律「分支 rebase 最新 + 临时 detach worktree 合并推送」。**
- 验收清单十项全勾（child 链/孙代双帧/reference 隔离/loop 直连注册前拒/loop→reference 无孤儿/errorHandler 不吞/parallel join+background 静态拒/再挂起栈合并/深位绑定+timeout 载体/cancel 期恢复——go 无进程内 cancel 面故该例留 worker 接线单）。
- 遗留：①恢复会话执行级缺省预算（withFrameBudget 现以 flow.Timeout 为准，无 timeout 则帧无预算——缺省 1800s 留 worker 层 TODO）；②Workflow.Resume 旧入口无实例重入守卫（ResumeSuspension 已持有）；③事件账目无专项断言测试（NS-M4 对拍集覆盖）；④r94 的 cmd/plaita-worker CI 红系并行批次业务，我合并后的运行已绿（疑似 flaky）。

**一行摘要**：NS-M2 收官——plaita-go 嵌套挂起 v1 合并 main dea859f CI 绿（活帧调用栈/FrameKind seed 传播修标注抹除缺口/转换点主闸+无孤儿/静态分支闸/ResumeSuspension 沿栈恢复/十用例对拍镜像）；共享主仓竞态实录并固化「分支 rebase+临时 detach worktree 合并推送」规约；NS-M4 三引擎对拍集与 NS-M3 plaita 实现为后续轮。

## 第十三轮·读穿透（2026-10-23，全量入位第三步：outbox 读穿透——plaita-rs feat/outbox-readthrough `cca9ea3`[feat]+`f606ac2`[docs] 已推 origin 待主控验收合并）

- **基线对账**：origin/main `327ca3a`（r11 hash 门 f0cd802+r12 manifest 8fbce14+NS-M1 ce8df7b 均已在册；NS 线/他席与读穿透面零交集核实）。Go 参照=`fix/issue299-readthrough` ef13204 **已并 main**（工作树零 diff 直读）：console_flowsource.go+flowsource.go+测试八件。
- **落点收窄（r12 同款核实）**：Rust 池无 worker crate/无消费循环→读穿透落**定义装载入口层**（引擎新模块 `flowsource.rs`，嵌入式宿主经引擎装载定义的场景）；Redis 存储腿（`LocalFlowStore` trait+内置 MemoryFlowStore 预置面）与消费循环集成等 worker crate 落地接线（worker 规格 flowsource.rs 位，与 registry 同批次）。
- **回源链**：本地 miss→配置 PLAITA_CONSOLE_FLOW_SOURCE_URL 才回源（未配置=特性关闭、miss 即确定性 NotFound 与现状逐字节同）→GET /api/flows/{fid}/versions/{ver}（X-Admin-API-Key+X-Tenant-ID，空/缺省租户按具名缺省拼写）→`ir::engine_flow_payload` 注入 flow_id/version（flow_hash.rs 新公开函数：字面量保持扫描器保真注入后按 #60 canonical 渲染，serde f64 重渲染翻指纹故不走 serde）→ir::load——**r11 hash 门同路径天然执法**（mismatch=flow_hash_mismatch 具名拒+负缓存）。
- **错误语义（安全核心）**：404/无 published/未发布→NotFound+负缓存 30s；网络/超时/5xx/其余 4xx→Transient **绝不伪造 404** 且不进负缓存（outbox 投递窗语义，不可达终态化在途执行=反模式）；unparsable/装载拒→NotFound+负缓存（不烧重试预算）；瞬态重试 attempts=2/250ms 固定退避；响应帽 64MB；回写默认关（WRITEBACK=1 显式开，默认保持 #303 清除语义只读）；latest 哨兵 console 侧解析最新 published（published_at 缺则 created_at，version 串决胜，draft 跳过）。
- **负控先红后绿**：红=编译级 E0432（基线无接受面，r8 单②同款取证）+**变异红**（Unavailable 误映射 NotFound 伪造 404，格3 负控当场抓住后还原）；绿=负控 15 件 tests/flowsource_console.rs（任务书五格：miss→回源→hash 门→装载成功〔回写后二次零 console〕/404→具名负缓存〔窗过期回源实证〕/不可达→瞬态不伪造 404/本地命中零 console 调用/未配置=特性关闭 + 另钉九件：瞬态重试后成功/attempts 耗尽仍瞬态/latest 解析/draft 拒载/hash 门 mismatch 执法/unparsable 确定性拒/租户头透传与缺省映射/engine_payload 大整数字面量保真/回写默认关/本地 not-loadable 确定性）。每测试自带 TCP stub（r8 教训①先读全请求再应答）。
- **门禁**：cargo test **197P/0F**（基线 177+20：负控 15+模块单测 5）/diffcheck **23/23** exit 0/fmt/clippy -D 全净（退出码文件 /tmp/b4r13_*.log|*.out）；`--no-default-features` 构建绿（配置 URL 却无 http 特性=具名构造错误，fail-closed 不留静默失效臂）。
- **教训两记**：①Go `strings.TrimPrefix(a,"v")` 是「有则去无则留」——Rust `strip_prefix('v')` 必须 `unwrap_or(a)` 配对，漏配则无 v 前缀的常见版本号（9/10）全落字典序（模块单测当场抓住）；②测试夹具指纹必须按**实际注入身份**计（latest 解析出 2.0 而夹具按 3 计→门具名拒当场打回——反证 hash 门在回源路径执法，修正后留注释钉语义）。

**一行摘要**：第十三轮读穿透收官——引擎侧 flowsource.rs 落定义装载入口层（本地 miss→console 回源→engine_flow_payload 注入→r11 hash 门执法→装载；六 env 键对齐 plaita-go、未配置=特性关闭、不可达绝不伪造 404、负缓存 30s、回写默认关）；负控 15 件五格全落（编译红+变异红双证）、cargo 197P/diffcheck 23-23 全绿；全量入位三步（hash 门/manifest 位次/读穿透）至此收官，registry+消费循环集成等 worker crate（feat/worker-m1 合并后另立项）。

## 第十三轮·WK-M1 收官（2026-10-23，plaita-rs worker M1 合并 main 89527bf，CI 绿）

- 实现席 C 交付 feat/worker-m1（5 提交，基线 r12）：①引擎 E1-E3（RuntimeCtx::from_snapshot_json 九根重建缺键具名拒/SuspendedExecution::from_parts/往返性质测试）；②crate plaita-worker（blocking redis-rs 0.27 无 tokio；Redis 后端 trait+四段 Lua 具名脚本位+TxOp 事务管道；内存 fake 钉协议语义）；③协议存储（queue 信封/reclaim-before-read/Ack+残留清扫/DLQ 全信封字段、lease holder 16-hex、state fenced CAS 双闸 #54、checkpoint §7.2 四元组+context_flavor fail-closed、flowsource canonical JSON 逐字节对齐 Python+指纹 golden 62 例）；④恢复投递（订阅存储/HandleEvent 租户受信顶层/#87 去重失败释放/超时检查器 #50 一次性 due/PubSub 桥/Worker 主循环 start 闸序+resume 处置总表+真唤醒走 from_snapshot_json+from_parts 复用 resume 全链+ResumeGuard-ack #68+§9.4 派发凭据+§9.6 降级具名+drain D1→D2）；⑤M1 行为测试（fake e2e 9 例+真 Redis 4 例 #[ignore]+指纹 golden 3 例）。
- 主控验收：分支门禁 fmt/clippy -D/全仓绿；真 Redis 腿 4/4（本机 6379 实跑 3 连稳定）；bin 实弹冒烟（投递→挂起落库→桥唤醒→completed→checkpoint 清→SIGTERM 干净退出）。**实现席抓到真缺陷**：redis-rs 0.27 PubSub::psubscribe 内部消费确认帧——桥的确认循环会把业务消息当确认吞掉（真 Redis 腿+bin 冒烟暴露，已修+钉死测试）。
- **rebase 到 NS-M1 后的语义调和**（主控亲手）：from_parts 补 call_stack（checkpoint 重建现场无嵌套路径=根流程挂起，嵌套路径 M2 持久化层回填）+ worker 错误归一补 SuspendRejected 专型。合并期并行会话再并 outbox-readthrough（r14，cargo 197P）——merge 落其上（89527bf），合并点全量 28 测试二进制绿，CI 绿（39900941691）。
- 遗留（按规格 M2/M3）：registry 心跳/LookupStatus、retry 预算/退避、watchdog/deadline、quota、年龄闸、亲和、DLQ replay、FlowSource TTL 缓存、桥「订阅确认前丢事件」窗口回扫器（M3 立项）、终态行 context 采集（引擎 E 面扩展）、嵌套挂起的 checkpoint 调用栈序列化（M2，依赖 NS 侧持久化设计）。

**一行摘要**：WK-M1 收官——plaita-worker M1 骨架合并 main 89527bf CI 绿（队列/租约/fenced CAS/checkpoint 边界/订阅存储+超时检查器+PubSub 桥/真唤醒复用 resume 全链；193P+真 Redis 4/4+bin 实弹；redis-rs PubSub 确认帧缺陷抓修）。至此第十三轮三席全收官：NS-M1（rs）+NS-M2（go）+WK-M1（worker）全合并；余 NS-M3（plaita）/#M4 对拍集/WK-M2-M3 后续轮。

## 第十五轮·入位接线收官（2026-10-23，feat/worker-integration 待主控验收合并）

- 范围=两块搁置面接完（r12「registry 接线记 worker 落地后」+ r13「消费循环集成等 worker crate」），完成后 rs 全量入位：worker 消费+registry+读穿透+hash 门+manifest 全链。
- **① registry 写 manifest（对齐 plaita-go #82 f13819a）**：新模块 registry.rs——`{prefix}:registry:flow_worker:{instance_id}` SETEX TTL 30s/心跳 10s（新 config 键 PLAITA_HEARTBEAT_INTERVAL_SECONDS/PLAITA_REGISTRY_TTL_SECONDS），整份重写即续期；ServiceInfo 与 go 同形；metadata 挂 engine="plaita-rs"（取自清单自身同源）+ capability_manifest 整 dict 嵌套（ir::capability_manifest 五键，ir_contract_version=2026-10-23.2）；生命周期 starting→running→draining(3 次短重试立即落盘)→stopped 墓碑 SETEX 5s→DEL；降级=序列化失败省略两键+PLAITA_CAPABILITY_MANIFEST_UNAVAILABLE 具名告警，不写「在但为空」，不拒启（go 同款口径）。
- **② 消费循环接读穿透**：worker RedisFlowStore 实现引擎 LocalFlowStore（引擎存储形状）；Worker 持 FlowSource，handle_start+load_continuation_definition 两处定义门改走 get_flow_definition(&flows,…)：本地 miss→console 回源（六 env 键，未配置=特性关闭逐字节同 WK-M1）→r11 hash 门→ir::load；NotFound→FlowNotFound 终态族/Transient→FlowLoad 留 pending。**引擎 trait 加错误面**（本轮接口变更）：get_raw/list_versions→Result——Redis 腿读失败必须落瞬态，不得当 miss 引去回源/终态化（MemoryFlowStore Ok 包装语义不变）；flowsource_console.rs 两处 get_raw 断言改 matches! 形。配置 URL 无 http 特性→引擎具名构造错误→worker 降级本地语义+PLAITA_FLOW_SOURCE_DISABLED 告警不拒启。Config 加 flow_source 字段=FlowSourceConfig::from_getenv(同闭包)（引擎新增该装配，from_env 委托之；positive_int_env_with 注入化）。
- **负控（先红后绿）**：变异红×2 实证——负缓存写入失效→404 格红/manifest 键省略→registry 五键格红。绿=fake 读穿透 e2e 5 格（miss→回源→执行成功〔latest+detail 恰 2 调用+回写默认关〕/404→具名终态+负缓存零调用/未配置=本地语义〔消息不谎称查过 console〕/瞬态 503→不终态化留 PEL+负缓存未污染〔换 200 重投即成〕/console 同版本换定义→resume flow_hash_mismatch 具名拒+checkpoint 清）+registry 单测 5+**真 Redis 9/9**（旧 4+新 5：回源全链挂起→事件→恢复完成/404 负缓存/未配置/hash 拒+allow 放行吃 CHANGED/registry 全生命周期 run() 真心跳 1s→running→五键→supported 与引擎清单全等→drain→墓碑 DEL）。
- 门禁：cargo test 247P（**main 89527bf 基线实测 237P**——任务书面 197P 系 WK-M1 合并前读数，守恒按真基线对账+新案 10 另计；flowsource resolve_version 测试重指新链路消息形保计数）/diffcheck 23/23/fmt/clippy -D/node-parity 全绿（rc 文件 /tmp/b4r15-gates/）。worker Config 新增 heartbeat_interval_secs/registry_ttl_secs/flow_source；lib.rs 导出 registry 模块+RedisFlowStore。
- **事故一记（自伤自愈）**：变异 A 回滚误用 `git checkout <file>` 把引擎 flowsource.rs 六处未提交修改一起洗掉——发现后单脚本一次重写六处（单补丁单写盘），clippy/test 复绿。**教训：变异回滚禁用 git checkout 带未提交改动的文件，一律精准 Edit 逆向或提前 commit 基线。**
- worker.rs 遗留更新：模块文档「no registry heartbeats (M2/M3)」已撤；余 M2/M3 面（retry 预算/watchdog/quota/亲和/DLQ replay/FlowSource TTL 缓存/桥回扫器/终态 context 采集/嵌套 checkpoint 调用栈）照旧。

**一行摘要**：第十五轮入位接线——registry manifest 五键+消费循环读穿透落 worker 主循环（引擎 LocalFlowStore 加错误面+RedisFlowStore+两定义门换路；真 Redis 9/9+fake 5 格+registry 单测 5；247P=237 守恒+10 新案；diffcheck 23/23；变异红×2），feat/worker-integration 待主控验收合并→rs 全量入位收官。

## 第十三轮·NS-M3 收官（2026-10-23，plaita 嵌套挂起合并 main 9072b9a，三 CI 工作流全绿——三平台闭环）

- 实现席 E 交付 fix/nested-suspend（4 提交，8 文件 +1433/−60）：①缺陷修复半边（_advance_one 对 is_suspending is True 抛 FlowSuspendedCarrier+runner 逃逸分支；normal/generator/distributed 三模式 fail-closed 化——根流程 normal 的 fail-open 同修=修正清单 #7）；②传播半边（分布式步边界子执行 carrier 转合法挂起、闸先于订阅注册【结构上无孤儿面，强于 rs】、checkpoint 逐帧化 $SUSPEND_FRAMES〔含 $PARENT 构造期快照逐帧显式另存=存疑 #14 的解〕、作用域闸+parallel 静态闸、get_or_create_event_bus 递归查链修孙子执行丢 bus 的既有缺陷）；③沿路径恢复（_handle_nested_resume 决策步/帧回退返回步/再挂起栈拼接/每帧新鲜预算/#33/#221 准入共用）。SC-003 硬预算合规（DistributedStrategy 293<400）。
- 主控验收：分支真实改动面核实（三点 diff 恰 8 文件——两点 diff 的 console-e2e/conftest 反向项系基线落后 r91 的显示效应）；rebase r91 后 27 项嵌套测试绿；**全量自有复跑 8999P/0F**（PYTHONPATH 钉 worktree 核实被测代码）；探针行为翻转核实（NORMAL 假完成→具名失败/DISTRIBUTED 真挂起+绑定守卫生效）。合并经临时 detach worktree 避让共享主仓（9072b9a），plaita 三 CI 工作流全绿。
- 存量零改写确认：fail-open 时代零测试覆盖（正是静默缺陷本身）；仅 CheckpointSchema 精确键清单三处随 $SUSPEND_FRAMES 登记扩展。
- **嵌套挂起程式（三平台）就此闭环**：rs#16+go#88+plaita#311 全落地。余：NS-M4 三引擎对拍集；ir_validate 装载期拦截前移/resume generation 代数（章程修订候选）。

**一行摘要**：NS-M3 收官——plaita 嵌套挂起 v1 合并 main 9072b9a 三 CI 绿（缺陷修复+传播+checkpoint 逐帧化+沿路径恢复；闸先于订阅强于 rs；27 语义测试+全量 8999P 主控复跑+探针翻转核实）——**嵌套挂起三平台闭环**（rs ce8df7b/go dea859f/plaita 9072b9a）；余 NS-M4 对拍集。
