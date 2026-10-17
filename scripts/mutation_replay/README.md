# mutation_replay —— 统一变异 store（JSON 为骨 + 兼容 `mutations.py`）

用途：把「每条修复所依赖的不变量」变成**可独立复现的变异**。每条变异保存定义原文
（`old`，从修复后的源码逐字节摘出）与变异文本（`new`，回到缺陷形态），要求对应的回归
测试**必须变红**（变异被杀死）。不是 mutmut 全量扫描，而是定点变异重放：定义入库，
评审可逐条核对，一条命令复现。

本目录是**基础设施补丁**：不携带任何单位的定义，各单位只带自己的定义文件落地
（见 `../../../STORE-CONSOLIDATION.md`）。

### `mutations/parked/`（#170 route C，2026-10-17）

`mutations/*.json` 是**当前 main 可重放**的定义集合；`mutations/parked/` 存放
**目标代码尚不存在**的定义（逐字保留 + `PARKED.json` 清单说明所需未落地修复）。
`replay.py --check-anchors` 与 r12 聚合判决都只 glob `mutations/*.json`（不递归），
所以归档不会让「锚点悬空」把门禁长期染红，也不会伪造不存在的锚点。

判据：只有 `old` 能在当前源码里**诚实**取到（同一语义位点、`new` 的破坏语义不变）
才留在 `mutations/`；取不到就归档，**不允许**为了变绿把 `old` 改指无关代码。
#170 的 61 条里 50 条属于未落地的 `fix/79-85` 加固面（隔离/页契约/自愈/看门狗），
待该修复落地后从 `parked/` 原样移回并按 §6 playbook 重切。

## 1. 两种 store 及其并存规则

### 1.1 JSON 目录 store（推荐；`fix/68` 起的约定）

```
scripts/mutation_replay/mutations/*.json
```
```json
{
  "name": "68-bytes-mode-fail-open",
  "issue": "68",
  "concern": "AC2/AC5",
  "file": "plaita/core/expression.py",
  "old": "<修复后源码里逐字节的片段，必须恰好出现一次>",
  "new": "<变异后的片段>",
  "rationale": "为什么这条能杀死缺陷",
  "expect_fail": ["tests/unit/test_x.py::TestY::test_z"]
}
```
可选字段：`expect_survive: true`（documented-unobservable，允许 0 红）。

### 1.2 Python 模块 store（`c9d5c25`（旧 `fix/55`）/`#133` 的 `mutations.py`）

```python
DEFAULT_TESTS = ("tests/unit/a.py", "tests/unit/b.py")
MUTATIONS = {
    "M16-sibling-revert": {
        "note": "revert the sibling container helpers to raw calls",
        "edits": [("plaita/core/expression.py", "<old>", "<new>")],
        "tests": ("tests/unit/b.py::test_y",),   # 可选：覆盖 DEFAULT_TESTS
        "expect_survive": True,                  # 可选
    },
}
```

### 1.3 并存规则

- `replay.py` 默认同时加载 `<store>/mutations/*.json` **与** `<store>/mutations.py`
  （`--store` 可指向任一单位的 store 目录）。
- Python store 的规格被**规范化**成与 JSON 同形（`edits` + `expect_fail`；
  缺 `tests` 时取 `DEFAULT_TESTS`）——**只做形状规范化，不改任一 store 的定义语义**
  （判死在 `expect_survive × rc∉{0,1}` 交叉点的口径变化见 §1.4）。
- **同名变异跨 store 冲突 ⇒ 显式报错退出**（`同名变异冲突…拒绝静默覆盖`），
  绝不静默取其一。
- **编辑粒度（诚实说明，勿混）**：
  - `mutations.py` 的 `edits` 是 `(文件, old, new)` **三元组列表**，一条定义可含
    **多处编辑**（`#133` 现有 91 条里有 12 条是多编辑）；兼容层原样保留，不做拆分。
  - **JSON store 目前只支持单处编辑**：`_load_json_store` 只读 `file`/`old`/`new`
    三个键，写成 `{"edits": [...]}` 会直接 `KeyError: 'file'`（rc=1）。
    需要多处编辑时，把同一条变异拆成多条同名前缀的 JSON
    （结构扩展方案见 `STORE-CONSOLIDATION.md` §「`#133` 的两种去向」，本补丁不做自动迁移）。

### 1.4 判死口径与 `error` 状态（**判死信道 = rc + pytest 自己的两条机读报告**）

`replay.py` **从不读 stdout 下判决**。每次变异运行都在**同一个私有 0700 临时目录**
（`tempfile.mkdtemp` 建的随机路径，落在仓外、与导出树分离，解析完即删）里生成**两份**
pytest 依据 report 对象写的机读产物：

- `--junit-xml=<tmp>/report.xml`（第一条信道）；
- `--report-log=<tmp>/report.jsonl`（第二条信道，JSON-lines）。本环境 pytest 9.1.1 核心
  **不含** `pytest-reportlog`，且 venv 里没有 pip 无法安装 ⇒ replayer 把一份**等价的最小插件**
  写进同一个私有目录，用 `-p` 注入并注册同名 `--report-log`（若环境已装原生 pytest-reportlog，
  选项名冲突会被捕获、自动让位给原生插件）。**两条信道都由 pytest 的 report 对象派生：
  独立，但不防进程内伪造**（见 §1.5）。

判决只由 **rc + XML 的 `<failure>`/`<error>` 计数 + 两信道一致性 + 红 id 的 XML 佐证** 给出：

| pytest rc | junit XML | 结论 | 记录 |
| --- | --- | --- | --- |
| `0` | 无所谓 | 存活 | `killed: false`，`red: []` |
| `1` | `<failure>` ≥ 1，**且**两信道一致、**stdout 至少一条红 id** 且全部有 XML 佐证 | **杀死** | `killed: true`，`red: [<已佐证 id> …]` |
| `1` | `<failure>` ≥ 1、带 testcase 身份，但 stdout **一条红 id 都没有** | **error**（r6 R2） | `error_kind: "red-unknown"`，`killed: false` |
| `1` | stdout 有红 id，但至少一条**无 XML 佐证**（含 XML 身份的**扩展**形） | **error**（r5 R1 + r6 R1） | `error_kind: "red-not-in-junit"`，`killed: false` |
| `1` | `<failure> == 0` 且 `<error>` ≥ 1（两信道一致） | **error** | `error_kind: "rc1-xml-error-only"` |
| `1` | `<failure> == 0`、`<error> == 0`、无红行 | **error** | `error_kind: "rc1-no-red-line"` |
| `1` | `<failure> == 0`、`<error> == 0`、stdout 却有红行 | **error** | `error_kind: "rc1-xml-no-failure"`（红行不可信） |
| `1` | 缺失 / 不可解析 / 根元素非 junit / **任何异常** | **error** | `error_kind: "junit-xml-missing-or-unparsable"` |
| 无 pytest rc（`old` 命中 ≠ 1 次） | 不适用 | **error**（r6 R9） | `error_kind: "anchors"`，`rc: null`，`killed: false` |
| `124`（本 replayer 的超时哨兵，`--timeout`，默认 900s） | 无所谓 | **error** | `error_kind: "pytest-timeout"`，`error_line` 含阈值秒数 |
| `∉ {0,1}`（收集/导入/内部错误，含 `-11` 段错误） | 无所谓 | **error** | `error_kind: "pytest-rc-not-0-1"` |
| `0` 或 `1`（`expect_survive: true`）且 stdout **无任何红 id** | 无所谓 | documented-unobservable，不判死 | `killed: true`、`red: []`（沿用既有口径） |
| `0` 或 `1`（`expect_survive: true`）但 stdout **有红 id** | 无所谓 | **error**（r7 R3） | `error_kind: "expect-survive-with-red"`，`killed: false`、`red: []` |
| `∉ {0,1}`（`expect_survive: true`） | 无所谓 | **error**（**交叉点语义变化**：r2 记 `killed: true`，r3 起改为 error） | `error_kind: "pytest-rc-not-0-1"`，`killed: false` |

**上表第 2 行（`rc=1` + `<failure> ≥ 1`）在 r5/r6 还要过五道门、r7 另加两条「不许假拒绝」的实现
约束，任何一道不过都是 error、不是杀死**（细节见下）：

- **红 id 非空门（r6 R2）**：XML 有 ≥1 个带身份（`classname`/`name`）的失败 `<testcase>`，stdout
  却**一条红 id 都没有**（真 id 被 `atexit` 的伪造段整段挤掉）⇒ `error_kind: "red-unknown"`，
  **绝不写 `killed: true` + `red: []`**；
- 红 id 佐证门：stdout 的每条红 id 都必须被某个 XML `<testcase>` 佐证（**单向**关系，见下），
  否则 `error_kind: "red-not-in-junit"`；
- 跨信道门：XML 与 report-log 的失败计数与失败 nodeid 必须一致，否则
  `error_kind: "junit-reportlog-mismatch"`（report-log 缺失/不可解析同此）；
- 自洽门：XML `<testsuite>` 自报 `failures`/`errors` 与元素数必须自洽，否则
  `error_kind: "junit-xml-inconsistent"`；
- 窗口门：XML 必须是本次运行窗口内写的（挡复用/陈旧文件），否则
  `error_kind: "junit-xml-outside-run-window"`。

**两条「不许假拒绝」的实现细节（r7 R1/R2；r8 修正其中 `::` 规则与幂等范围，与上面的门配套，
不是额外的门）**：

- **nodeid 可含空格（r7 R1）**：pytest 的红行是 `FAILED {nodeid} - {reprcrash.message}`，
  而 param id 可以含空格（`test_real[a b]`，任何 `parametrize` 带字符串参数都会有），
  `reprcrash.message` 也可以含 `" - "`（`assert 5 - 2 == 4`）。r6 用 `\S+` 抓 nodeid，
  含空格的行**整行匹配失败** ⇒ `red=[]` ⇒ 撞 `red-unknown`、把合法的 KILLED 记成 error。
  现在**不猜**切点：`replay.py` 的 `failed_candidates` 把一行里**每个** `" - "` 切点都作为
  候选（`_dash_cuts`），由 :func:`justify_red` 用 XML 权威身份取**首个被佐证**的候选；
  `FAILED` 分支的 legacy 形状（首个 `::` 之后的部分，§8）与 `SUBFAILED` 的完整 nodeid 都不变，
  故三条既有记录的字节锚不受影响。
- **归一必须幂等，且这个承诺只对 pytest 产出的身份成立（r7 R2；r8 限定范围 + 修正 `::` 规则）**：
  `flat_identity` 只归一 `::` **之前**的 head（`/`/`\`→`.`），**没有顶层 `::` 的输入原样返回**。
  `::` 算分隔符的规则**不是**「方括号外」（r7 这样写是错的）：pytest **不转义** param id 里的
  `]`，`parametrize(..., ['a]::b'])` 产出的 nodeid 是
  `tests/unit/test_vparams.py::test_real[a]::b]`（XML 身份
  `tests.unit.test_vparams.test_real[a]::b]`）——只数括号深度的扫描在第一个 `]` 处把深度归零，
  紧随的 `::` 被误判成顶层分隔符，两条信道**字面相同**的身份被归一成
  `tests.unit.test_vparams.test_real[a].b]` 与 `…test_real[a]::b]` 两个不同串 ⇒ 真被杀的变异记成
  `junit-reportlog-mismatch`（r6/r7 同错，r8 修）。现在的规则是：**末尾 param id 区域整段
  视作一段**，区域之外才用「方括号外」的深度扫描切分（`replay.py` 的 `_param_region_start`
  + `_split_nodeid`）。区域起点两步定：①从串尾**自右向左**配对方括号，取**把深度降到 0 的
  那个 `[`**（修 `[a]::b]` 这类多余 `]`）；②若起点前紧接 `::`（`]::[` 形状，`::` 两侧都是
  param 片段），继续向左吸收到前一个 `[`（修 `[]::[]`；pytest 的类名/用例名不含 `[`，故
  `::` 后紧跟 `[` 只可能是 param id 的延续）。否则 param id 里的
  `/`、`::`、`\` 被二次归一（`justify_red(x, [x]) == False`），合法的失败被记成
  `junit-reportlog-mismatch` / `red-not-in-junit`。
  **r9 更正（起点规则的字面描述曾与代码不符）**：上面①**不是**「取末尾方括号组最左的 `[`」
  ——当**全串** `]` 多于 `[`、右向扫描到串首仍不归零时，`for…else` 兜底取的是**全串最左的
  `[`**。若更靠左还有别的 `[`（如路径里的 `x[1]`），起点会落到它上面：
  `_split_nodeid('tests/unit/x[1]/test_x.py::TestY::test_z[a]::b]')` 返回 **1 段**（整串），
  而「末尾方括号组最左的 `[`」这条字面规则会给出 3 段。这不是**新的**假拒绝（r7 在同一形状上
  切 4 段、同样归一不出 XML 身份），且该形状在 `expect_fail` 里**不可达**——pytest 直接拒绝
  含 `[]` 的路径选择（`ERROR: path cannot contain [] parametrization`、rc=4）。本轮只把文档
  改成与代码一致（夹具 `test_r9_param_region_fallback_takes_the_leftmost_bracket` 钉住真实
  行为），**不动**判死逻辑。
  **幂等范围（r8 诚实化；r9 更正计数口径）**：只在 **pytest 产出的 nodeid/身份**上承诺幂等
  （r9 复算：一次真 pytest 产出 8000 条 nodeid + 8000 条 XML 身份，非幂等 **0/8000**）。
  随机串 fuzz（seed 20261014）非幂等的是 **246/20000 个原始串**、去重后 **229/13634 个去重串**
  ——r8 文档把两个口径混写成「246/13634」（246 是原始串口径、13634 是去重后条数，两者不是
  同一次计数的分子分母）；本轮拆开、两数都由 §5.3 的命令现算。这些例外全是 tail 前导/尾随空格
  与残留 ` \ ` 这类**非 pytest 形状**的二次 `strip`/归一，**不**承诺对任意字符串幂等。r8 修正后
  `justify_red(x, [x]) is True` 对
  `[a b]`/`[a/b]`/`[a::b]`/`[中文]`/`[1.5]`/`[a\b]`/`[]`/嵌套方括号 `[a[b]]`/`[a]b]`/
  `[a]::b]`/`[]::[]` 等真 pytest 形状全部成立（r8 夹具用**真 pytest** 跑 verifier 的 21 条
  对抗 param id 的整套：21/21 红 id 被佐证、两条信道 `agree=True`、`killed=True`），
  佐证关系仍**单向**（伪造的扩展形照样 `red-not-in-junit`）。
- **`expect_survive` 不得带红（r7 R3）**：`expect_survive: true` 的条目仍走 documented-unobservable
  的独立返回（不算杀死），但**记录里 `red` 必须为空**——stdout 出了任何红 id（无论 rc=0 还是
  rc=1）都记 `error_kind: "expect-survive-with-red"`、`killed: false`、`red: []`。r5/r6 的
  提前返回会把伪造的 stdout 红行**原样**写进记录（`killed: true` + `red: [伪造 id]`），而渲染器
  同时说「SURVIVED（documented-unobservable，无红测试）」，自相矛盾。`#133` 的两条既有条目
  `red` 本来就是 `[]`，记录逐字节不变。

#### 红 id 佐证关系（r6 R1，**唯一实现处**：`replay.py` 的 `red_id_matches_identity`）

以 XML `<testcase>` 的扁平身份（`classname.name`）为**权威**，stdout 候选（经 `flat_identity`
归一）只准是它的**等值或点分截断**：

| stdout 候选 | XML 真身份 | 判定 | 场景 |
| --- | --- | --- | --- |
| `test_x.test_real` | `test_x.test_real` | ✅ 等值 | `SUBFAILED(<上下文>) <完整 nodeid>` |
| `test_real` | `test_x.test_real` | ✅ 点分截断 | `FAILED` 分支截断后的短形 |
| `tests.unit.test_x.test_real` | `tests.unit.test_x.test_real` | ✅ 等值 | 完整 nodeid / report-log nodeid |
| `tests.nonexistent.zzz.test_x.test_real` | `test_x.test_real` | ❌ **扩展** | verifier 的 `m-fake` 伪造（更长的路径） |
| `X.test_x.test_real` | `test_x.test_real` | ❌ **扩展** | 前缀段伪造 |
| `::test_real` | `test_x.test_real` | ❌ 空 head（畸形）——**只对 `justify_red` API 与 `SUBFAILED` 分支成立** | 空模块/类段的伪造 |
| `FAILED ::test_real`（候选 `test_real`） | `test_x.test_real` | ✅ 点分截断 | `FAILED` 分支先 `split("::",1)[1]` 再进守卫，见下 |

**⚠️ 分支差异（r9 更正：此前把 `::<name>` 一律写成「rejected outright」是过宽的）**：
空 head 的拒绝只发生在 **`justify_red` API 入口**与 **`SUBFAILED` 分支**（后者保留完整
nodeid，`::test_real` 以 `::` 开头 ⇒ `justify_red` 直接 `False` ⇒ `red-not-in-junit`）。
但 **`FAILED` 分支**在进 `justify_red` 之前先做 `node.split("::", 1)[1]`（`replay.py`
`failed_candidates`）：`FAILED ::test_real` 的候选是 `test_real`——它不是空 head，而是 XML
真身份 `test_x.test_real` 的**点分截断** ⇒ `killed: true`。这是 **r7/r8 完全一致的既有容忍**
（同一个 test 的名字被截短，不是「更长的伪造路径」），**不是伪造向量**：XML 里必须真的有一个
叫 `test_real` 的失败 `<testcase>`（且被 report-log 佐证）才会判死，伪造不出 XML 里不存在的
东西。**r6 不同（r9 实测）**：r6 还没有 `failed_candidates` 的候选消歧，`failed_nodes` 抓
整段，`FAILED ::test_real` 保留 `::test_real` ⇒ `red-not-in-junit`、`killed: false`
（三份 blob `shasum` 可核对：r6 `e6cd33a5…`、r7 `909ed097…`、r8 `3bedef9e…`）——r6→r7 在这里
是**容忍面变大**（少记一次 error），不是伪造面变大。两侧分别由夹具
`test_r9_failed_branch_strips_empty_head_before_guard` 与
`test_r9_subfailed_and_api_reject_empty_head` 钉住。

**方向是关键**：XML 身份是地面真值，stdout 只能「截短」它。r5 用的是**双向**点分后缀匹配
（`flat == candidate or flat.endswith("." + candidate) or candidate.endswith("." + flat)`），
其中「stdout 更长」的那一路 `flat.endswith("." + candidate)` 会把「更长的伪造 stdout id」在
其 XML 真身份恰为它的点分后缀时放行——verifier 在
`ccb3e4d` 上用 `atexit` 在真 summary 之后再打一个表头 + `SUBFAILED(i=1)
tests/nonexistent/zzz/test_x.py::test_real`（真身份 `test_x.test_real`）即被判死，主记录里写进了
一棵**不存在的路径**（证据 `red=1(unjustified=0)`）。r6 删掉扩展方向后同一伪造记
`red-not-in-junit`（证据 `red=1(unjustified=1)`）。`::<name>` 这类空 head 形状也不是 pytest
的合法红行形状（合法形状只有 `路径::Class::test` / `Class::test` / 裸 `test`），`justify_red`
API 与 `SUBFAILED` 分支一并拒绝——**但 `FAILED` 分支会先把 `::` 前缀剥掉再进守卫**（r9 更正，
见上表下方的「分支差异」）。
本关系是命名函数、可单测（`replay.py` 的 `red_id_matches_identity` / `justify_red` /
`flat_identity` 都是模块级函数，可直接 import 断言）。**断言所在的确切位置（r7 R4b 更正，
r8 R4b 补齐交付）**：断言**不随本补丁入库**（补丁 scope 只有本目录 `replay.py`/`README.md`
+ 仓根 `STORE-CONSOLIDATION.md`），而在**与本补丁同一份 gist 里随 store 一起发货的验证夹具**中：

| 夹具（gist 内路径） | 条数 | 内容 |
| --- | --- | --- |
| 夹具 `harness/tests/unit/test_mutation_replay_r6.py` | 15 | R1 单向佐证 / R2 空红两条合法路径的门 |
| 夹具 `harness/tests/unit/test_mutation_replay_r7.py` | 31 | param-id 矩阵（含空格）/ `expect_survive` 门 / 幂等矩阵 |
| 夹具 `harness/tests/unit/test_mutation_replay_r8.py` | 15 | `[a]::b]`/`[]::[]` 末段区修正 + verifier 21 条对抗 param id 真 pytest + 幂等范围诚实化；r9 加 3 条（起点兜底真实行为、`FAILED` 分支剥 `::` 前缀、`SUBFAILED`/API 拒空 head）|
| store `stores/r6mut`（2 条）/ `stores/r7mut`（3 条）/ `stores/r8mut`（1 条） | 6 | 自测变异定义（`expect_fail` 指向上表节点，全部以**正常 `FAILED` 行**判死）|

gist: `<filled at delivery>`   ← 交付步骤填入本补丁的 secret gist URL（同一份 gist 内还有
`DELIVERY-MANIFEST.txt`，逐文件 sha256 可校验）

复现（夹具目录即 gist 解包目录，自带 `run-selftests.sh`）：

```bash
# 一条命令：跑 6 条自测变异（各自 KILLED）+ 三份夹具文件本身（61 passed）
bash <gist 目录>/run-selftests.sh /path/to/plaita <本 revision>
# 等价的手工口径：把 harness/ 下的 *.py 放进导出树的 tests/unit/ 与仓根后
python scripts/mutation_replay/replay.py --all --store <gist 目录>/stores/r6mut --tree <导出树>
python scripts/mutation_replay/replay.py --all --store <gist 目录>/stores/r7mut --tree <导出树>
python scripts/mutation_replay/replay.py --all --store <gist 目录>/stores/r8mut --tree <导出树>
cd <导出树> && python -m pytest tests/unit/test_mutation_replay_r{6,7,8}.py -q
```

r6 的 README 曾写「`tests` 侧断言」但**未把该文件纳入交付**，`/tmp` 夹具随会话消失后
这句话没有可执行的依据；r7 起把夹具放进 gist 并写清「断言在哪、怎么跑」，r8 进一步把
gist 内容做成**自包含**（`run-selftests.sh` + 三份夹具 + 三个 store + 一份
`DELIVERY-MANIFEST.txt`）：r8 复现为 `r6mut` 2/2 KILLED、`r7mut` 3/3 KILLED、
`r8mut` 1/1 KILLED、夹具本身 **61 passed**（r6 15 + r7 31 + r8 12 + r9 3）、exit 0。

- **判死以 XML 的 `<failure>` 计数为准**，但 stdout 红 id 有**一票否决权**：红 id 只是
  证据列，取自 pytest 的 `short test summary info` 段（`FAILED`/`SUBFAILED` 行），
  **每一条都必须能被某个 XML `<testcase classname=… name=…>` 佐证**，否则整条变异记
  `error_kind: "red-not-in-junit"`、**永不判死**。
- **empty-red 的两条合法路径（r6 R2，只此两条）**：① XML **自己不带任何 testcase 身份**
  （`<failure>` 不挂在 `<testcase>` 下，`bad == []`）⇒ 允许 `killed: true` + `red: []`；
  ② `expect_survive: true` 的 documented-unobservable 走**独立的提前返回**（表末三行，
  `#133` 的 `M11-class-facts-no-validation` / `M14-probe-attr-access` 就是这两条，记录逐字节
  为 `killed: true` + `red: []`；r7 R3 起该提前返回**强制 `red: []`**——一旦 stdout 有红 id 就记
  `expect-survive-with-red` error，见上）。**除此之外**，只要 XML 有 ≥1 个带身份的失败 `<testcase>`，
  空 `red` 一律记 `red-unknown`：verifier 的 `m-clear`（`atexit` 只补一个 summary 表头，真 id
  被整段挤掉）在 `ccb3e4d` 上得到 `killed: true, red: []` 且**无 error**，r6 起记
  `red-unknown`、`killed: false`。
- **r4 的两条声明已被证伪并删除**（r5 R1）：r4 曾写「测试/setup 的 `print` 落在
  captured-output 段，位置在真表头**之前**，伪造行**结构上进不了红名单**」「即使连表头
  整行一起伪造……伪造行仍在真表头之前」。**两条都是假的**：`atexit` 钩子在 pytest 打印完
  **真** `short test summary info` 段之后才运行，可以再打一个同样的表头 + 伪造 `FAILED`
  行，于是「最后一次表头之后」反而是伪造段，**真 id 被挤掉**。因此现在的规矩是
  「stdout 候选 + XML 逐条佐证」；伪造能污染**展示**，不能决定**判决**。
- **为什么判死必须离开 stdout**：stdout 是变异代码可写的。r3（`614d747`）的「≥1 条真红行」
  被「`print("FAILED …")` + setup 抛错」的 fixture 伪造成功（pytest 报 `1 error`、`0 failed`，
  记录却是 `killed: true`）；r4 改用机读报告，但 R1 又证明单条 XML 也不够 ⇒ r5 加第二条
  信道交叉确认。**任何 XML 异常一律 error，永不判死。**
- **超时（N3）**：单次 pytest 运行有 `--timeout`（默认 **900s**；理由：三条真实 store 里最重的
  一组实测约 25~30s/次，900s ≈ 30 倍余量；`--timeout 0` 关闭）。超时记 error
  （`error_kind: "pytest-timeout"`，`error_line` 写明阈值），**不判死也不会一直挂着**。
  超时会**整组终止**：子进程用 `start_new_session=True` 起，超时后 `os.killpg(…, SIGKILL)`
  （POSIX，尽力而为；非 POSIX 退回 `proc.kill()`）。**限度（r6 R6，已声明、不解决）**：
  `killpg` 只能杀**同一会话/进程组**的进程；fixture 里 `Popen` 出来的孙进程若自己调
  `os.setsid()` 就另起会话，`killpg` 够不到它，它会以 `ppid=1` 活下来。r4 的
  `subprocess.run(timeout=…)` 只杀直接子进程（同组孙进程也会活成孤儿）是真问题，但「孙进程
  **永不**变孤儿」这个 r5 说法在 setsid 孙进程面前**为假**，本节不再声称。
  非 error 记录**不给超时键**（否则三条记录 sha256 全变）；阈值只进超时 error 的 `error_line`。
- **FIFO/非普通文件不再挂死父进程（r6 R5）**：`--timeout` 只约束 pytest **子进程**，管不到
  父进程自己的读。r5 的 `read_junit` 用 `Path.read_bytes()`：junit 路径被放上 FIFO（无写端）
  时读**永远阻塞**，verifier 的端到端 `--all` 20s 不返回、要 SIGKILL。现在两条信道的读都走
  `read_regular_bytes`：`os.open(..., O_NONBLOCK)` + `fstat` 确认 `stat.S_ISREG`（FIFO/设备/
  目录/指向它们的符号链接一律立即拒绝），读取量还有 64 MiB 封顶。非普通文件 ⇒ `read_junit`
  返回 `None` ⇒ 记 `error_kind: "junit-xml-missing-or-unparsable"`（具名 error，rc=1），
  父进程**秒级返回**（夹具实测 2.3s，之前 25s+ 被 kill）。
- **锚点失效两侧一致（r6 R9）**：`old` 命中 ≠1 次时主记录**也**带 `error: true` /
  `error_kind: "anchors"` / `rc: null` / `error_line`（与旁证的 `verdict=error` 一致），
  `doc_table.py` 渲染成 `ERROR（锚点失效，未判死）`。r5 只写旁证，主记录是 `{"killed": false}`
  无 `error` 键 ⇒ 控制台与 `doc_table` 把它显示成 `SURVIVED`（同一个东西两套说法）。
- **基线跑不起来是 error，不是存活（N5）**：解释器没有 pytest ⇒ `error_kind: "pytest-unavailable"`；
  基线超时 ⇒ `"pytest-timeout"`；基线本来就红 ⇒ `"baseline-not-green"`。
  `--python /nonexistent` ⇒ argparse 用法错 **rc=2**（具名，无裸 traceback）。
- **0 条红行时到底发生什么（r6 R2 起分两种）**：**不写占位红行**。
  ① XML 有 ≥1 个带身份的失败 `<testcase>` ⇒ `error_kind: "red-unknown"`、`killed: false`、
  `red: []`（额外四条键：`error_kind` / `rc` / `error_line` / `error`；`error_line` 是 pytest
  收尾行）；② XML **自己不带 testcase 身份**（`bad == []`，例如 `<failure>` 不挂在
  `<testcase>` 下）⇒ 诚实结论是 `killed: true` + `red: []`（`detail` = `xml-failure xN`）——
  这是**唯一**在 XML 有 `<failure>` 时允许的正常空红路径（`expect_survive` 的
  documented-unobservable 是显式例外，见下）。
  实测形态（pytest 9.1.1，**随版本/位置变，判据只看 rc + 两条机读信道**）：
  - 测试模块本身收集错（import/语法错）⇒ **rc=2**，末行
    `!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!`；
  - `conftest.py` 语法错或命令行用法错 ⇒ **rc=4**，只有 `ERROR <file>` / `unrecognized arguments`，
    没有 `Interrupted:` 行。
  replayer 自身 **rc=1**。旧版（`dcb7136`）会写 `red: ["<rc=2 无 FAILED/SUBFAILED 行>"]`
  且 `killed: true`——那正是被修掉的假阳性。
- **证据旁证（主记录五键冻结）**：非 error 记录的键**逐字节不变**（仍是
  `killed`/`expect_survive`/`red`/`note`/`source`），所以三条既有记录的 sha256 保持可比（§5）。
  判决证据走**独立旁证文件**：给了 `--record` 就默认写 `<record>.evidence.json`（可用
  `--evidence <file>` 指定），每条变异记录 XML 失败/错误计数与坏 testcase、report-log 的
  call/setup/teardown/collect 失败数与 nodeid、红 id 与未佐证 id、XML mtime 是否落在运行窗口、
  `<testsuite>` 自报计数是否自洽。**`red_unjustified` 与判决用的是同一个关系**（r6 R3）：
  都经 `red_id_matches_identity`，另写 `red_justify_rule` 规则串；因此伪造成 XML 身份**扩展**
  的 stdout id 必然出现在 `red_unjustified` 里（`m-fake`：`red_from_stdout=[伪造 id]`、
  `red_unjustified=[伪造 id]`、`junit.bad_testcases=[真身份]`，verdict=error），
  旁证**不会**把伪造认证成 kill。**不塞进主记录是刻意的**：主记录的形状是跨版本 sha256 锚。
- 控制台汇总：`合计: N  杀死: K  存活: S  错误: E  其中 documented-unobservable: D（不判死，不计入杀死）`，
  其中 `K = killed 且非 expect_survive`（`#133` 打印 `杀死: 89`，另 2 条 documented = 91 条总定义）；
  错误多于一条时额外打印 `错误分类: <kind>×n` 与「两信道不一致」条数。

### 1.5 威胁模型（判死信道能保证什么、不能保证什么）

**变异体是我们自己的定义，不是对抗性第三方代码。** 一条变异体在 pytest 进程内可以读到
两份机读产物的路径（它们就在 `sys.argv` 里）并覆写它们。**在进程内同时伪造两条 pytest
产物，无法在进程内阻止**——这是本补丁**明确的威胁模型之外**，不是被「结构性」挡住的。
本补丁保证的是：

1. **不会误判死**：rc ∉ {0,1}、XML 缺失/不可解析/非普通文件、两信道不一致、红 id 无佐证
   （含 XML 身份的**扩展**形与 `SUBFAILED`/API 侧的 `::<name>` 畸形形；`FAILED` 分支会先剥
   `::` 前缀，见 §1.4 的「分支差异」）、XML 有失败身份却**一条红 id 都没有**、
   XML 自相矛盾或不在运行窗口内、锚点失效，一律 error（`killed: false`），**绝不 `killed: true`**。
2. **跨信道不一致 ⇒ error**：只伪造一条信道（XML 或 report-log）必然被另一条抓住。
3. **判决证据可审计**：`--record` 同时写 `<record>.evidence.json`（§1.4 末），人类可以审一条
   kill 的证据，而不必「相信」它。

**演示过的边界（不是假想）**。verifier 的**原样 2-liner**（只伪造 XML + `os._exit(1)`）在 r4
下判死（`killed: true`，`red: []`，`detail: "xml-failure x1"`），在 r5 下因 report-log 的
call-failed 计数为 0 而记 `junit-reportlog-mismatch`——**已堵**：

```python
if any(a.startswith("--junit-xml=") for a in __import__("sys").argv):
    open([a.split("=",1)[1] for a in __import__("sys").argv if a.startswith("--junit-xml=")][0],"w").write(
        "<testsuites><testsuite><testcase><failure/></testcase></testsuite></testsuites>")
    __import__("os")._exit(1)
```

把同一手法扩展到**同时**伪造两份产物（r5 下该变体仍被判死，证据 `cross=agree`）：

```python
_xml = [a.split("=", 1)[1] for a in sys.argv if a.startswith("--junit-xml=")]
_log = [a.split("=", 1)[1] for a in sys.argv if a.startswith("--report-log=")]
if _xml and _log:
    open(_xml[0], "w").write('<testsuites><testsuite failures="1" errors="0" tests="1">'
        '<testcase classname="tests.fake.test_forged" name="test_forged"><failure/></testcase>'
        '</testsuite></testsuites>')
    open(_log[0], "a").write(json.dumps({"$report_type": "TestReport",
        "nodeid": "tests/fake/test_forged.py::test_forged", "when": "call",
        "outcome": "failed"}) + "\n")
    os._exit(1)
```

因此本补丁的准确说法是：**伪造能污染「展示」，不能决定「判决」**。单条信道伪造（只写一份
pytest 产物 / 只在 stdout 打假红行 / 伪造一个更长的红 id）会被另一条信道或 XML 佐证门抓住；
挡不住的只是「蓄意**同时**伪造两条 pytest 产物」——那要读到 `sys.argv` 并分别写出两种格式，
等同主动篡改本机测试基础设施，与「改掉 `replay.py` 本身」同一层级，不在本补丁范围内。

> **r6 更正（r5 的一句话曾经为假）**：r5 的 README 写「本补丁能挡住…伪造 stdout 假象」——
> **该说法在 `ccb3e4d` 上是假的**：`justify_red` 当时是**双向**点分后缀匹配，伪造一个「比 XML
> 真身份更长」的 stdout id 会被 XML 短身份佐证，于是**单靠 stdout** 就能把一条变异判死
> （verifier 的 `m-fake`，见 §1.4）。r6 把佐证关系改成**单向**（`red_id_matches_identity`）
> 之后，「伪造 stdout ⇒ `red-not-in-junit` error、永不判死」才成立。**已删除的 r4 声明**
> （均被 verifier 证伪，以本文替代）：「结构上进不了红名单」「伪造行仍在真表头之前」
> 「变异代码写不到的信道」「与删掉导出树同级」——最后一条尤其低估：两句代码 + `os._exit`
> 即可，不需要删树。

**已加入的最小缓解（不声称能挡伪造）**：① XML 必须是**本次运行窗口内**写的（挡复用旧文件/
陈旧 XML）；② XML 的 `<testsuite>` 自报 `failures`/`errors` 必须与元素数自洽（挡随手写的
畸形 XML）。两项检查的结果都写进证据旁证，供人审。伪造者在同一时间窗口内写出形式自洽的
XML 仍然绕得过这两条——它们降低的是「意外假 kill」，不是「蓄意伪造」。

## 2. 常用命令

```bash
python scripts/mutation_replay/replay.py --list --store <dir>          # 列出定义（含来源）
python scripts/mutation_replay/replay.py --one <name> --store <dir>    # 单条
python scripts/mutation_replay/replay.py --all --store <dir> --tree <export>
python scripts/mutation_replay/replay.py --all --source head          # 只 HEAD（自导出临时树）
python scripts/mutation_replay/replay.py --all --keep                 # 保留临时导出树
python scripts/mutation_replay/replay.py --all --record <record.json> # 机读记录
python scripts/mutation_replay/replay.py --all --record <r> --evidence <e.json>  # 证据旁证（默认 <r>.evidence.json）
python scripts/mutation_replay/replay.py --all --timeout 300           # 单次 pytest 超时（默认 900s；超时整组终止）
python scripts/mutation_replay/replay.py --check-anchors --store <dir> --tree <export>
python scripts/mutation_replay/doc_table.py --record <r> --doc <md> --write   # 渲染文档表
python scripts/mutation_replay/doc_table.py --record <r> --doc <md> --check   # 逐行校验（含结论列、重复行、陈旧行历史豁免只认表行）
python scripts/mutation_replay/check_readme_table.py --store <dir> --readme <README>
```

- **`--store` 指错不静默通过（G4）**：0 条定义时 `--all` / `--check-anchors` / `--list`
  都打印 `错误：store 里没有任何变异定义（0 条）` 到 stderr 并 **rc=1**。
  默认 store（本脚本目录）**就是 0 条**——本补丁不携带定义，所以落地前
  `python scripts/mutation_replay/replay.py --list` 是 rc=1（诚实的「没东西可列」，
  不是崩溃）；落地后该目录有了 `mutations/*.json` / `mutations.py` 即正常。
- **每个变异针对自己的目标树**：`--tree <export>` 让不同单位的定义各自指向其
  修复后的代码状态（各单的 `old` 只在自己的树里恰好命中一次）。见 §7。
- `--record` 记录形如
  `{"<name>": {"killed": true, "expect_survive": false, "red": [...], "note": "...", "source": "json:xx.json"}}`；
  两种 store **同形**，`doc_table.py` 直接消费。error 记录额外带
  `error`/`error_kind`/`rc`/`error_line`（见 §1.4），`doc_table.py` 渲染成
  `ERROR（rc=N，未判死）`（锚点失效记为 `ERROR（锚点失效，未判死）`，`rc: null`），
  **绝不显示为 KILLED/SURVIVED**。
- 退出码：0 = 全部杀死（documented-unobservable 不算存活）；1 = 有存活/error/基线不绿/
  锚点失效/**store 0 条定义**；2 = 用法错（未知 `--one`、`--check-anchors` 缺 `--tree`、
  `--python` 不存在/不可执行、`--timeout` 为负；前两者直接走 argparse，**不再抛裸
  `AssertionError`/`FileNotFoundError`**）。**锚点被改坏 ⇒ rc=1**（见 §5）。

## 3. 锚点纪律（`old` 恰好命中一次）

- 每条 `old` 必须在**目标树**的目标文件里逐字节出现**恰好一次**；`0` 次（实现已演进）
  或 `>1` 次（歧义）都判失败，必须删除该变异并同步删文档红名单。
- `--check-anchors --store <dir> --tree <export>` 一次性列出所有失效/歧义锚点（rc=1）。
- **别在 replay 跑动时读同一棵 `--tree`（r9 运维注意）**：`replay_one` 是把变异**原地写进**
  `--tree` 的目标文件、跑完在 `finally` 里再写回原样（本 revision 的写回在
  `replay.py:1205-1213`），所以与正在跑的
  `--all` 并发地对同一棵树做 `--check-anchors` / 读源码会**读到被改到一半的树**（实测抖动：
  同一棵树的锚点问题数会短暂 `117/116`，稳定值是 `115`）。要做锚点校验或审计，请**先复制一份
  干净的 `--tree`**（或等 replay 结束）再读。
- 定义原文必须从**修复后**源码里摘（不是缺陷版），`new` 才是缺陷形态。

## 4. 各单位定义 ↔ revision / 目标树

<!-- mutation-replay:unified-registry store-version=d95d97497d37dc1d85e8e26a3c906362ee72c83f55db3822f1e366d882e47061 -->
<!-- 本 README 是统一登记册，§4 是按单位/revision 的表，不是「一变异一行」的单位表。
     check_readme_table.py 只在「标记 + store-version == sha256(同目录 replay.py) + 路径同一」
     三条全过时给 N/A（rc=0）；显式 --readme 指向本文件则 FAIL（rc=1）。
     单位表校验请指向单位的 README。详见 §6。 -->

| 单位 / 单号 | revision | store | 条数 | 目标树 |
| --- | --- | --- | --- | --- |
| `#68` | `a2426636` | JSON | 54 | `git archive a2426636` |
| `#79-85` | `5e21e71` | JSON | 61 | `git archive 5e21e71` |
| `#55`/`#133` | `c9d5c25` | `mutations.py` | 91 | `git archive c9d5c25` |
| `#69`/`#87`/`#95-99`/`#109-110`/`#118`/`#137-139` | 各自 fix 分支 | JSON | 各自 | 各自 `git archive` |

落地时**只有 `#68`/`#79-85`/`#133` 曾自带 replayer/README**，它们的补丁在落地时应剥掉
这些文件、只留自己的定义（见 `STORE-CONSOLIDATION.md`）。

> **⚠️ 合并后的 store 是一本「登记册」**：每条定义的 `old` 只在**它自己的** revision
> 的源码里成立。不存在「落地后用**一个** `--tree` 重放全部 206 条」——锚点会大面积失效。
> 复现口径见 §7。

## 5. 复现（本补丁的验证口径）

**第 0 步（接收侧必做）：把各 revision 导出成干净树。** 下面命令里的
`/tmp/store-src/<rev>` 是 `--tree`/`--store` 指向的**导出目录**，不是现成路径；
必须先 `git archive` 解包，否则命令会因目录不存在直接失败。

```bash
REPO=/path/to/plaita              # 本仓 checkout（该仓里存在这些 revision）
for rev in a2426636 5e21e71 c9d5c25; do
  mkdir -p "/tmp/store-src/$rev"
  git -C "$REPO" archive "$rev" | tar -x -C "/tmp/store-src/$rev"
done
```

```bash
# #68：54/54 KILLED；--record 的红用例 id 必须干净（无 subTest 上下文残渣）
python scripts/mutation_replay/replay.py --all \
  --store /tmp/store-src/a2426636/scripts/mutation_replay --tree /tmp/store-src/a2426636 \
  --record /tmp/store-src/record-68.json

# #79-85：61/61 KILLED
python scripts/mutation_replay/replay.py --all \
  --store /tmp/store-src/5e21e71/scripts/mutation_replay --tree /tmp/store-src/5e21e71 \
  --record /tmp/store-src/record-79-85.json

# #133：91/91 as documented（89 KILLED + 2 documented-unobservable）+ 记录/表逐行一致
python scripts/mutation_replay/replay.py --all \
  --store /tmp/store-src/c9d5c25/scripts/mutation_replay --tree /tmp/store-src/c9d5c25 \
  --record /tmp/store-src/record-133.json
python scripts/mutation_replay/doc_table.py --record /tmp/store-src/record-133.json \
  --doc /tmp/store-src/c9d5c25/docs/mutation-testing.md --check
# 期望：OK: 91 行与记录逐行一致（无陈旧行）   rc=0
```

### 5.1 接收侧警告（必读）：**不要整目录 checkout 单位分支**

**不要用 `git checkout <unit-branch> -- scripts/mutation_replay/`（或 cherry-pick 整棵
子树）落地单位补丁。** 单位分支里的 `replay.py` 是**各自旧版**（`a2426636` 201 行、
`5e21e71` 207 行、`c9d5c25` 156 行），与统一版（**1393 行**，带 `--store` / `--record` /
`--evidence` / `--check-anchors` / `--tree` / error 状态 / `--junit-xml` + `--report-log`
两条判死信道 / 红 id 佐证**单向化** + `red-unknown` + FIFO 拒绝 + 进程组超时终止 /
r7 的**空格 param id** 候选消歧 + `flat_identity` 幂等 + `expect-survive-with-red` /
r8 的**末尾 param id 区**（unbalanced `]`）修正 + 幂等范围限定）分别有
**1384 / 1390 / 1461** 行内容不同；同一 diff 含 hunk 头（含 `---` 分隔行）共 **1460 / 1466 / 1511** 行。
复现（在统一版 checkout 里逐字跑）：

```bash
for rev in a2426636 5e21e71 c9d5c25; do
  git show "$rev:scripts/mutation_replay/replay.py" > "/tmp/unit-$rev.py"
  printf '%s: content=%s incl_hunks=%s\n' "$rev" \
    "$(diff scripts/mutation_replay/replay.py /tmp/unit-$rev.py | grep -cE '^[<>]')" \
    "$(diff scripts/mutation_replay/replay.py /tmp/unit-$rev.py | wc -l | tr -d ' ')"
done
```
```
a2426636: content=1384 incl_hunks=1460
5e21e71: content=1390 incl_hunks=1466
c9d5c25: content=1461 incl_hunks=1511
```

（评审记的 `fix/68`「362 lines differ」、`292/298/423`、`362/368/474`、r3 的 `410/488`、
`416/495`、`517/565`、r4 的 `573/655`、`579/661`、`668/720`、r5 的 `1061/1135`、
`1067/1141`、`1134/1185`、r6 的 `1177/1251`、`1183/1257`、`1250/1302`、以及 r7 的
`1304/1378`、`1310/1384`、`1379/1430` 都只对
**各自的更早 revision** 成立，对**本 revision 不成立**，
故本文件已按上面命令重算。另外 `c9d5c25` 的
`replay.py` 与 `fix/55` 分支 tip `3914f67` 是**同一 blob**
（sha256 `b1fab975d72cf384194ca7da2130829cf001dc149f8f2141589214984065c63a`），
所以第三个数字对两个名字都成立。）

整目录 checkout 会**静默覆盖**统一 replayer，并连带覆盖 `README.md`
（`fix/68`/`fix/79-85`）、`doc_table.py` 与 `last_replay.json`（`c9d5c25`/`fix/55`）。
**正确做法**：要求各单位重发**剥离补丁**（只含 `mutations/*.json`；`#133` 只含
`mutations.py`），用 `git apply` 落地（清单见 `STORE-CONSOLIDATION.md` §落地顺序）。
落地后必须验证统一 replayer 原封不动：

```bash
shasum -a 256 scripts/mutation_replay/replay.py
# 期望：d95d97497d37dc1d85e8e26a3c906362ee72c83f55db3822f1e366d882e47061
wc -l < scripts/mutation_replay/replay.py   # 1393
```

### 5.2 三条记录与本 revision 的 sha256（r5–r9 重跑复现）

三个真实 store 的记录**逐字节不变**是本补丁的硬验收（r5 加了第二信道与红 id 佐证、r6 又
把佐证关系收紧为单向并加了 `red-unknown`、r7 修空格 param id 的假拒绝 + `flat_identity` 幂等 +
`expect-survive-with-red`，但非 error 记录仍只写五键，见 §1.4）。用 §5 的三条命令重跑后核对
（r6 在 `e6cd33a5…` 上串行重跑；r7 在 `909ed097…`、r8 在 `3bedef9e…`、r9 在 `d95d9749…` 上
以**本地并行**（三条 `--all` 同时起）重跑，三条逐字节不变）：

```bash
shasum -a 256 /tmp/store-src/record-68.json /tmp/store-src/record-79-85.json /tmp/store-src/record-133.json
# 9113553e9100f45092e92d99f2e213964de912169eaceb0e6e4806e50ebe62d6  record-68.json
# e1d2ad96b7f102c50f832a65e872b086ea49284d640764229c244ba4781cd53d  record-79-85.json
# d4b973897373ddf2d39f33a298b530228c488e5e1184be5eabcbf9be2bf3aa7a  record-133.json
```

（r5 的 `--record` 还会**额外**写出旁证 `record-*.json.evidence.json`（§1.4 末）；它们不参与
上面的 sha256，主记录字节不受影响。`--report-log`/`--junit-xml` 的产物只存在于仓外 0700
临时目录、解析完即删，重放后 `diff -r` 导出树必须**逐字节相同**，见 §5.1 末尾。）

### 5.3 §1.4 幂等数字的 producing command（r9 拆开 246/20000 与 229/13634）

§1.4 的两个随机串数字都要用**同一次生成**算，**别把原始串口径的分子配去重后口径的分母**
（r8 就是在这里混写成「246/13634」）：

```bash
cd <本补丁 checkout>
python - <<'PY'
import importlib.util, random
spec = importlib.util.spec_from_file_location("r", "scripts/mutation_replay/replay.py")
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
random.seed(20261014)
ATOMS = ["::", "/", "\\", "[", "]", " ", "a", "b", "1", ".", "\u4e2d", "-", "- ",
         "::b", "x]", "[y"]
raw  = ["".join(random.choice(ATOMS) for _ in range(random.randint(1, 7)))
        for _ in range(20000)]
uniq = sorted(set(raw))
nonidem = lambda x: (lambda f: r.flat_identity(f) != f)(r.flat_identity(x))
print("raw=%d dedup=%d  raw_nonidem=%d dedup_nonidem=%d"
      % (len(raw), len(uniq), sum(map(nonidem, raw)), sum(map(nonidem, uniq))))
PY
# 期望：raw=20000 dedup=13634  raw_nonidem=246 dedup_nonidem=229
```

`0/8000`（真 pytest 身份）另算：建一棵临时树，用**模块级**变量造 8000 个 param id
（`IDS = [f"{H[i % len(H)]}-{i}" for i in range(8000)]`，`H` 是 §1.4 那批对抗 param id），
`@pytest.mark.parametrize('tag', IDS)` 后真跑 `-q --junit-xml --report-log`，对 8000 条
report-log nodeid 与 8000 条 XML 身份各数一次 `flat_identity` 非幂等 ⇒ **0/8000**。
（**别把 8000 个 id 直接写成 `parametrize` 字面量**：pytest 会在每条失败里回显装饰器源码行，
XML 会平方级膨胀——实测 ~868 MB，超过 `read_regular_bytes` 的 64 MiB 上限后 `read_junit`
返回 `None`。）

## 6. `check_readme_table.py` 的适配

来源 `fix/79-85`，原版把 `ROOT` 固定为脚本所在仓。统一化后新增 `--store` / `--readme`，
因此同一个脚本既能校验本仓默认 store，也能校验任一单位的 store+README。它只对
**JSON store** 成立（README 表按 `| \`name\` |` 行与 `mutations/*.json` 一一对应）；
`mutations.py` store 的表由 `doc_table.py --check` 逐行校验。

**默认位置的行为（G5 + N1 诚实化）**：分三种情况，都不留「落地即可复现的假失败」：

| 默认调用时的状态 | 输出 | rc |
| --- | --- | --- |
| 无 `mutations/*.json`（补丁未落地） | `SKIP: …本目录是统一 replayer，不携带定义，默认跳过` | 0 |
| 有 `mutations/*.json` 且 README 是**统一登记册**（三重校验通过，**落地态**） | `N/A: 默认 README …是统一登记册，§4 是按单位/revision 的表，不是变异表；本检查器对登记册不适用` | 0 |
| **显式** `--store` 却 0 个 JSON | `FAIL: store … 没有 mutations/*.json` | 1 |
| **显式** `--readme` 指向登记册 README | `FAIL: …是统一登记册 README…请传单位的 README` | 1 |

**「登记册」不是可以自报的字符串（r5 N1 补洞）**：r4 只检查 README 里是否出现
`mutation-replay:unified-registry` 这个字符串，于是 verifier 往**任一单位 README** 追加
`<!-- mutation-replay:unified-registry -->` 后，`--store <unit>`（不显式给 `--readme`）就从
`FAIL` 静默变成 `N/A rc=0`。现在必须**同时**满足三条，缺一即按**单位 README 严格校验**
并打印一行 `注意: …未通过登记册身份校验…`：

1. 标记行带 `store-version=<64 位十六进制>`（形式：`<!-- mutation-replay:unified-registry store-version=<hex> -->`）；
2. 该 sha256 **等于检查器现算的**同目录 `replay.py` 的 sha256（值不由 README 自报）；
3. 这份 README **就是检查器旁边那份** `README.md`（路径同一）。

本 README §4 上的登记册标记即按此写成。**维护耦合（须知）**：改了 `replay.py` 就必须
同步改 §4 标记里的 `store-version`（新值 = `shasum -a 256 scripts/mutation_replay/replay.py`），
否则登记册身份失效、落地态默认跑会退回严格校验并报假失败。**残留边界（诚实说明）**：这挡的是「顺手自报」，
不是「蓄意冒充」——能读到本目录 `replay.py` 的人当然能算出哈希贴到别处，但第 3 条（路径
同一）仍然挡住「别处的 README 冒充登记册」；剩下能做的只有直接改检查器，与改 `replay.py`
同级，不在本工具威胁模型内。

没有这层身份校验，落地态（115 个 JSON + 统一 README）默认跑会拿本 README 里 17 行
「``| `x` |`` 形状」的表行去比 115 个定义，输出 `表格行数 17 != 变异文件数 115` 的**假失败**
（N1；r4 文档写 `6`、r5 实测 **9**，r6/r7 均为 **17** = §1.4 判死表 8 行 +
§1.4 佐证关系表 6 行 + §4 的 `#68`/`#79-85` 2 行 + §7 的 `--tree` 1 行；r7 新增的
`expect-survive-with-red` 行首格是 `` `0` 或 `1`（…）``，不是「裸反引号 token + ` |`」形状，
**不进这个计数**）。
`N/A` 只对**通过三重校验的登记册**生效，**单位 store 的检查一点没放松**
（显式 `--store <unit> --readme <unit README>` 照旧）：

```bash
python scripts/mutation_replay/check_readme_table.py \
  --store <unit>/scripts/mutation_replay --readme <unit>/scripts/mutation_replay/README.md
# #79-85：OK: 61 行 == 61 个变异文件（key 唯一、无陈旧行）   rc=0

# 假失败行数复现：本 README 里「| `x` |」形状的表行数（落地态有 115 个 JSON）
python - <<'PY'
import re, pathlib
t = pathlib.Path("scripts/mutation_replay/README.md").read_text(encoding="utf-8")
print(len(re.findall(r"^\| `([^`]+)` \|", t, re.M)))   # 17
PY
```

## 7. 合并后的 store 是**登记册**：不能用一个 `--tree` 整体重放（G3）

各单位都落地后，`scripts/mutation_replay/` 里会同时有 **206 条**定义
（`#68` 54 + `#79-85` 61 条 JSON，`#133` 91 条 `mutations.py`）。但每条定义的 `old`
都是从**它自己那个 revision** 的修复后源码里逐字节摘出来的，所以：

- **本补丁的树不是合并后的树（r9 澄清）**：本补丁交付时 `scripts/mutation_replay/` 里**只有
  四个脚本**（`replay.py`、`README.md`、`check_readme_table.py`、`doc_table.py`），**没有**
  `mutations/` 目录、也没有 `mutations.py`——所以本补丁上 `--list` 是 **0 条 / rc=1**（§2 的
  「store 0 条定义」）。下面表里的 `--list` **206** 与锚点 `205/151/144/115` 全是**各单位全部
  落地之后**（合并 store）的属性，**不是本补丁的属性**，不能拿它们来验收本补丁；
- **`--tree` 是整棵树级别的**，本补丁**不做** per-definition 的 tree pinning；
- 用任何一个单一 `--tree` 去 `--check-anchors` 这本合并登记册，绝大多数锚点必然报
  `old 命中 0 次`——**这是预期**，不是 store 坏了；
- **正确复现口径：按单位分组，各自用自己的 revision 导出树**（§5 的三条命令）。

复现命令（先按 §5「第 0 步」导出三个 revision，再组装落地模拟）：

```bash
REPO=/path/to/plaita
LANDED=/tmp/store-src/landed          # 落地后的统一树 + 全部单位定义
rm -rf "$LANDED"; mkdir -p "$LANDED"
git -C "$REPO" archive <本补丁 revision> | tar -x -C "$LANDED"
mkdir -p "$LANDED/scripts/mutation_replay/mutations"
cp    /tmp/store-src/a2426636/scripts/mutation_replay/mutations/*.json "$LANDED/scripts/mutation_replay/mutations/"
cp    /tmp/store-src/5e21e71/scripts/mutation_replay/mutations/*.json  "$LANDED/scripts/mutation_replay/mutations/"
cp    /tmp/store-src/c9d5c25/scripts/mutation_replay/mutations.py      "$LANDED/scripts/mutation_replay/"

# 定义数（登记册规模）
python "$LANDED/scripts/mutation_replay/replay.py" --list \
  --store "$LANDED/scripts/mutation_replay" | grep -c '^[^ ]'      # 206，rc=0

# 用单一 --tree 重放整本登记册：锚点问题数
for t in "$LANDED" /tmp/store-src/a2426636 /tmp/store-src/5e21e71 /tmp/store-src/c9d5c25; do
  python "$LANDED/scripts/mutation_replay/replay.py" --check-anchors \
    --store "$LANDED/scripts/mutation_replay" --tree "$t" | head -1
done
```

| `--tree` | 定义数 | 锚点问题数 | rc |
| --- | --- | --- | --- |
| `$LANDED`（落地统一树） | 206 | 205 | 1 |
| `/tmp/store-src/a2426636`（`#68`） | 206 | 151 | 1 |
| `/tmp/store-src/5e21e71`（`#79-85`） | 206 | 144 | 1 |
| `/tmp/store-src/c9d5c25`（`#133`） | 206 | 115 | 1 |

> 表里的「问题数」是**定义条数**（一条定义里多个 `old` 失效只算一条）。读法：
> 每个单位自己的树只让自己的定义通过（`#68` 54 → 206−151=55 命中，含 1 条巧合；
> `#79-85` 61 → 206−144=62；`#133` 91 → 206−115=91），**没有任何一个树能让 206 条全过**。
> 落地后如果要把全部定义重放成绿，必须**按单位分批 + 各自的 `--tree`**；把多个单位的
> 修复合并进同一棵树后，锚点是否还成立取决于它们有没有改到同一段源码，本补丁不保证。

## 8. 红用例 id 的两种形状（G8：现状记录，不改既有记录）

**来源限定（r5 更正）**：`red` 从 stdout 的 `short test summary info` 段（**最后一次**表头
之后）提取，这只是**候选**过滤——`atexit` 能在真表头之后再打一个表头（§1.4/§1.5），所以
**每条候选 id 还必须被 junit XML 的 `<testcase>` 佐证**，否则整条变异记
`error_kind: "red-not-in-junit"`。该段内候选 id 有**两种形状**：

- 来自 `FAILED <node>` 行的 id 被截成**首段之后的部分**：`tests/unit/x.py::TestY::test_z`
  → `TestY::test_z`（legacy 形状：取**首个** `::` 之后的部分）。r7 R1 起这一形状**不变**，
  但解析不再用 `\S+` 抓 nodeid：`failed_candidates` 把行内**每个** `" - "` 切点都作为候选
  （param id 与理由里都可能含 `" - "`、param id 还可以含空格），由 XML 身份取首个被佐证的；
  记录里仍是这个 legacy 形状，故三条既有记录的字节不变；
- 来自 `SUBFAILED(<上下文>) <node>` 行的 id 是**完整 nodeid**
  （`tests/unit/x.py::TestY::test_z`），同样按 `" - "` 候选消歧。

在 `#68` 的记录里（54 条定义、73 条红 id）**31 条**是 `Class::test` 形状、**42 条**是
完整 `tests/unit/…::Class::test` 形状（后者全部来自 subTest 的 SUBFAILED 分支）。
复现（用 §5 的 `--record /tmp/store-src/record-68.json`）：

```bash
python - <<'PY'
import json
r = json.load(open("/tmp/store-src/record-68.json"))
ids = [i for v in r.values() for i in v["red"]]
short = [i for i in ids if "::" in i and not i.startswith("tests/")]
print("红 id 总数", len(ids), "Class::test 形状", len(short), "完整 nodeid", len(ids) - len(short))
PY
# 红 id 总数 73 Class::test 形状 31 完整 nodeid 42
```

- **同名 `Class::test` 跨文件塌缩：已复现，作为已知并声明的行为（r6 R8）**。`FAILED` 分支
  丢了文件路径，`failed_nodes` 又做 `sorted(set(...))`，因此**不同文件里的同名 `Class::test`
  会塌缩成一条**。r6 用两个文件（`tests/unit/a.py`、`tests/unit/b.py`，各含 `TestX::test_y`
  且各自失败）端到端复现：stdout 两条 `FAILED` 行在 `failed_nodes` 里塌缩成
  `red == ["TestX::test_y"]`（一条），而证据旁证是 `junit.failures == 2`、
  `junit.bad_testcases == ["tests.unit.a.TestX.test_y", "tests.unit.b.TestX.test_y"]`（两条），
  判决仍是 `KILLED`——**判决不受影响**（XML 的 `<failure>` 计数是权威，短形被两个 XML 身份
  各自佐证），受影响的是**展示层的红数偏小**。r5 把这条写成「未验证的已知隐患」，r6 改为
  **已验证并声明**（上面这组数字就是复现结果）。**不改**：把 `FAILED` 分支改成保留完整
  nodeid（或把 `SUBFAILED` 分支截短）都会**改变现有三条记录的字节**（31 或 42 条 id 变样），
  而三条记录的 sha256 是跨版本比对的锚（§5、`STORE-CONSOLIDATION.md`）。可确认的事实：
  `#68` 记录里 42 条完整 id **全部**来自 `tests/unit/test_expression_size_guard.py` 一个
  文件；31 条短 id 覆盖 19 个类名，跨定义重复（同一用例杀死多条变异）属正常，不代表塌缩。
- **为什么不统一**：把 `FAILED` 分支改成保留完整 nodeid（或把 `SUBFAILED` 分支截短）
  都会**改变现有三条记录的字节**（31 或 42 条 id 变样），而三条记录的 sha256 是
  跨版本比对的锚（§5、`STORE-CONSOLIDATION.md`）。因此**本轮只记录现状，不动记录**：
  统一形状需要维护者拍板 + 一次性重跑重签三条记录。
