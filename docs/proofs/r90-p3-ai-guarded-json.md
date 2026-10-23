# r90 证明：plaita-ai 解析守卫统一 guarded_json_loads（A2 全仓 11 点清单一轮收口）

- 单号：A2 台账 r53「台账建议」全仓 11 点清单（r53 探针清点表为正典）+
  A2R51-02（flow_run MCP 双面）+ A2R53-04（supervisor 族四点）+ A2R52-05
  （sup_propose/react 裸解析）+ A2R52-06（ops_mcp/_guarded/flow_compile）+
  R49-03（evals dry-run str()）+ CLI cmd_run 同病顺带
- 基线：`origin/main` = `658a912`（r89 已合）
- 分支：`fix/p3-ai-guarded-json`（worktree `plaita-wt2`）
- 探针：A2 r51/r52/r53 归档探针（`~/.issue-keeper/duty/a2-r5{1,2,3}-scripts/`）
  修后复跑 + 本轮新增常驻负控 `plaita-ai/tests/test_json_guard.py`（每点
  ~12k 层嵌套 ~24KB，修前基线树 25 红 → 修后全绿）

## 缺陷（A2 r51/r52/r53 三轮实弹与清点）

调用方自控的 JSON 文本 ~12k 层嵌套只需 ~24KB 就让 `json.loads` 抛
`RecursionError`（RuntimeError 族）：`except ValueError` 窄口径拦不住、
裸调用直接穿出工具体（ToolNode 零包裹直穿引擎节点层）或入口。A2 r53 清点
表（11 行）确认全仓解析点守卫覆盖面参差：judges.py 三段 `except Exception`
是唯一达标形（A2R45-02/A2R46-01），supervisor 族/ops/_guarded/flow_run/
react/evals 均为窄口径或裸奔；另有 `result_json` dumps 面（深 result/
循环引用）与 evals dry-run `str(out["error"])` 面同病。

## 修法：单点 helper + 逐点接入（系统面收口，非逐点打补丁）

新增 `plaita_ai/json_guard.py`：

- `guarded_json_loads(text, *, what)` —— `except Exception` 整族捕获
  （JSONDecodeError / RecursionError / TypeError），折成具名
  `GuardedJsonError`（**ValueError 子类**：迁移期既有 `except ValueError`
  调用方照常接住）；错误消息带字段名与原始异常类型名。
- 调用方语义分层：工具体 → case unavailable 式 `ok:false` 载荷/`{"error"}`
  回复；入口 → 具名 422/400 形（CLI=SystemExit 非 0 退出）。唯一不变量：
  异常不得以裸形状穿出。

逐点状态表（清单 11 行 + 任务书增补面，全部落地）：

| # | 落点 | 修后形制 | 状态 |
|---|------|----------|------|
| 1 | judges.py:153/161 回复解析×2 | 已达标参照形（A2R45-02），**未动** | ✓ 达标 |
| 2 | judges.py:113-124 输入序列化 | 已达标参照形（A2R46-01），**未动** | ✓ 达标 |
| 3 | supervisor_flow.py sup_propose | no-proposer 早退**提前**（A2R52-05 次序格）+ guarded → `{"error": 具名}` | ✓ 修 |
| 4 | supervisor_flow.py sup_evaluate | guarded → `{}` → no_proposal 优雅路径 | ✓ 修 |
| 5 | supervisor.py `_parse_legacy_json` | guarded ×2 → None | ✓ 修 |
| 6 | supervisor.py `_emit_current_source` | JSON 腿 guarded + literal_eval 兜底腿 `except Exception`——docstring「任何失败降级空串」兑现 | ✓ 修 |
| 7 | supervisor.py `_extract_rationale` | guarded → "" | ✓ 修 |
| 8 | mcp/server.py flow_from_json definition | guarded → `ok:false` 具名（含 RecursionError 类型名） | ✓ 修 |
| 9 | react/tools.py plaita_run_flow inputs/globals | guarded → `ok:false` 信封（与 MCP 面同形） | ✓ 修 |
| 10 | ops_mcp.py ×6（console_run_start/console_dry_run/eval_compare×2/canary_shadow_once/canary_verdict） | 全部 guarded（字段名入错）；console_dry_run/canary_verdict **解析先于取 client**（坏载荷降级不依赖 console 配置在场） | ✓ 修 |
| 10' | ops_mcp.py `_guarded` 兜底 | `except (ValueError, KeyError)` → `except Exception`（judges 口径；ConsoleClientError 仍带 detail 优先）——解析点之外的意外异常同样具名 ok:false | ✓ 修 |
| 11 | evals.py `Dataset._load_file` | guarded → 具名 ConsoleClientError（清单注「低危可容忍」，本轮一并收口） | ✓ 修 |
| 增 | mcp/server.py flow_run inputs_json/globals_json（A2R51-02 面1） | guarded → `ok:false` + `error_type=GuardedJsonError` | ✓ 修 |
| 增 | flow_runner.py `result_json` dumps 面（A2R51-02 面2） | 深嵌套（RecursionError）/循环引用（ValueError）→ RunResult 同形状具名信封 `{ok:false, error, error_type}`——MCP/CLI/ReAct 三面共用出口 | ✓ 修 |
| 增 | evals.py dry-run `str(out["error"])`（R49-03） | 深嵌套 error 载荷 → 单 case 降级 unavailable 剔出均值（与 execution 面同口径、同注释） | ✓ 修 |
| 增 | cli/main.py `_load_json_arg`（cmd_run --input/--globals 顺带） | guarded → `SystemExit("error: --input is not valid JSON: …")` 具名非 0 退出 | ✓ 修 |
| 增 | supervisor_flow.py `run_supervisor_flow` 出口解析 | `(ValueError, TypeError)` → `except Exception`（深嵌套 verdict 串同族） | ✓ 修 |

不动清单（边界）：`tools/config/loader.py` 两点=启动期运维配置文件（非调
用方自控，fail-fast 合理）；`cli cmd_emit` 的 `json.loads(flow_from_json(raw))`
=自产 JSON（修后恒可解析）；judges.py 三段=达标参照形原样保留。

## 验证

- **红→绿**：`tests/test_json_guard.py`（38 测试）在修前基线树
  （`658a912` 临时 worktree，唯一差异=测试文件）**25 红**（全部缺陷格：
  flow_run 双面/flow_from_json/result_json 深+循环/sup 族 5/ops 7/evals 2/
  cli 3 等）→ 修后 **38P/2S rc=0**（2S=react 面 langchain 未装，家规
  importorskip）。证明：`data/r90-test-json-guard-prefix-red.txt` /
  `data/r90-test-json-guard-postfix-green.txt`。
  注：基线树运行时 venv editable finder 与 sys.path 混装两棵树——缺陷面
  模块经包 `__path__` 全部解析自基线树（红=修前行为），`json_guard` 单元
  格解析自修后树（两树皆绿，非缺陷证据），红绿结论不受影响。
- **A2 探针复跑**：r53 `probe_a2r53_input_trust.py` 换根修后树 → **7P
  rc=0**（R53-03a/b/c/d 四缺陷格断言「不穿出」全过 + 3 对照绿）；
  `data/r90-a2r53-probe-post-fix.txt`。r51/r52 探针为缺陷断言式（断言
  异常穿出），修后树按设计不再成立，对应面已由本轮测试以正确行为断言覆盖。
- **家族守恒**：plaita-ai 全量 **315P/16S/12 subtests rc=0**；integration+
  e2e 尾段 14 skipped（langchain 环境性，与基线同形）。
- **ruff**：改动文件 BLE001 基线 11 → 修后 11（**零新增**；本轮 5 个守卫
  本体盲捕位 `# noqa: BLE001` 带理由，参照形 judges.py 自身携 5 存量）。
- **mkdocs**：`scripts/docs-build-local.sh`（PYTHON=3.10.11，CI 等价）
  **rc=0**。
- **repo unit 门**：`pytest tests/ -m "not integration and not e2e"`
  **8949P rc=0**（无管道直录；本轮改动不触 engine/console 面）。

## 边界与残留

- `_guarded` 兜底放宽后，工具体内**程序性 bug**（TypeError 等）也折成
  ok:false 而非穿出——这是 judges 口径的本义（调用方拿到具名错误比
  FastMCP 500 可用），但异常类型名保留在 error 前缀里可归因。
- `evals.py:78` 数据集文件面按清单注记「低危」一并收口，语义升级点：深
  嵌套数据集文件从 RecursionError 穿出变为具名 ConsoleClientError。
- langchain 缺装环境下 react 面 2 格 skip；ci 装 `[agent]` extra 处即跑。
