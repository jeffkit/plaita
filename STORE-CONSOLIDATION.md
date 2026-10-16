# mutation store 统一 —— 迁移与落地说明

## 背景：机制冲突（不是内容冲突）

`origin/main` 上 `scripts/mutation_replay/` **不存在**（0 文件），各单都在**创建**它，
于是出现两套互不兼容的机制：

| 单位 | store | replayer | 其它 | 条数 |
| --- | --- | --- | --- | --- |
| `c9d5c25`（`#133`，旧分支 `fix/55`） | `mutations.py`（Python 模块，1218 行） | `replay.py`（156 行，`from mutations import DEFAULT_TESTS, MUTATIONS`） | `doc_table.py`、`last_replay.json`、`README.md` | 91 |
| `fix/68`（`a2426636`） | `mutations/*.json` | `replay.py`（201 行，glob JSON） | `README.md` | 54 |
| `fix/79-85`（`5e21e71`） | `mutations/*.json` | `replay.py`（207 行） | `README.md`、`check_readme_table.py` | 61 |
| 其余 8 单（`#69`/`#87`/`#95-99`/`#109-110`/`#118`/`#137-139`） | `mutations/*.json` | 无 | 无 | 各自 |

> **revision 固定（`#133` 的漂移）**：上表 `#133` 一行已从分支名 `fix/55` 改为
> **revision `c9d5c25`**，因为该分支 tip 已漂移——`c9d5c25` 上是 **91 条**定义
> （本节所有数字、记录、`doc_table.py --check` 都取自这里），而 `fix/55` 的当前 tip
> `3914f67` 已有 **104 条**（定义被新增/改写，12 条仍是多编辑）。**引用 `#133` 的
> store 时必须用 `c9d5c25`，不要用分支名。** 同理 `#68` = `a2426636`（54 条）、
> `#79-85` = `5e21e71`（61 条）；完整对照表见 `scripts/mutation_replay/README.md` §4。

按落地顺序 `#133` → `#68` 落，`#68` 的 `replay.py` 会**覆盖** `#133` 的 replayer，
`#133` 的 91 条 `mutations.py` 定义再也无法被任何 replayer 加载。**本补丁消除该冲突**。

## 本补丁交付什么

`scripts/mutation_replay/`（仅此目录 + 本文件）：

- `replay.py`：**JSON 那套为骨**，新增 ① `--store <dir>`（可指向任一单位的 store）
  ② `mutations.py` 兼容加载（`MUTATIONS` + `DEFAULT_TESTS`，规范化成 `edits` + `expect_fail`）
  ③ **同名跨 store 冲突显式报错** ④ `--record <file>` 机读记录 ⑤ `--check-anchors`
  ⑥ `--tree`（每个变异针对**自己**的目标树）⑦ **每次变异运行在同一私有 0700 临时目录里
  加两条机读信道：`--junit-xml` + `--report-log`**（仓外随机路径）⑧ `--evidence <file>`
  审计旁证。算法：干净树基线必须全绿 → `old` 恰好命中一次 → 变异后按 **rc + XML 计数 +
  两信道一致性 + 红 id 的 XML 佐证（`red_id_matches_identity`，**单向**：stdout 候选只准
  等于 XML 身份或作它的**点分截断**，扩展形一律不接受）** 判死：
  - `rc==0` 存活；`rc==1` 且 XML `<failure>` ≥ 1 **且** report-log 计数/nodeid 一致 **且**
    stdout **至少一条**红 id 且每条都有 XML `<testcase>` 佐证 ⇒ **杀死**；
  - 红 id 无佐证（含**扩展**形、`SUBFAILED`/API 侧的 `::<name>` 畸形形——`FAILED` 分支会先
    `split("::",1)[1]` 剥掉 `::` 前缀，故 `FAILED ::test_real` 走的是同 test 的点分截断容忍，
    **r7/r8 一致；r6 不剥（记 `red-not-in-junit`）**、非伪造向量，见 README §1.4）⇒ `error_kind: "red-not-in-junit"`；
    XML 有 ≥1 个带身份的失败 `<testcase>` 而 stdout **一条红 id 都没有** ⇒
    `"red-unknown"`（r6 R2：`killed:true` + `red:[]` 不再可达）；两信道不一致或 report-log
    缺失 ⇒ `"junit-reportlog-mismatch"`；XML 自报 `<testsuite>` 计数与元素数矛盾 ⇒
    `"junit-xml-inconsistent"`；XML 不在本次运行窗口内 ⇒ `"junit-xml-outside-run-window"`；
    锚点失效 ⇒ `"anchors"`（`rc: null`；r6 R9：主记录也带 error，不再被渲染成 SURVIVED）；
  - XML 缺失/不可解析（**任何异常都不许逃出** `read_junit`，`encoding="bogus"` 的
    `LookupError` 也在内）、`rc∉{0,1}`（收集/导入/内部错误）一律 **error**，`killed: false`
    + `error: true`/`error_kind`/`rc`/`error_line`，**绝不写占位红行**；`expect_survive` 的
    documented-unobservable 例外（r7 R3：该条目**不得携带任何红 id**，stdout 有红行 ⇒
    `error_kind: "expect-survive-with-red"`、`killed: false`、`red: []`）。
  **r7 R1/R2（两条「不许假拒绝」）**：红行 nodeid 可含空格（`test_real[a b]`）而
  `reprcrash.message` 也可含 `" - "`（`assert 5 - 2 == 4`）——r6 的 `^FAILED (\S+?)…`
  对含空格的行整行匹配失败 ⇒ 合法 KILLED 被记成 `red-unknown`；现在 `failed_candidates`
  把行内**每个** `" - "` 切点都作为候选、由 XML 权威身份取首个被佐证的，legacy 记录形状
  （`FAILED` 取首个 `::` 之后、`SUBFAILED` 留完整 nodeid）不变。`flat_identity` 对
  **pytest 产出的身份**幂等（只归一 `::` 之前的 head，无顶层 `::` 的输入原样返回；
  **r8 限定**：不承诺对任意字符串幂等），且 `::` 的分隔规则**不是**「方括号外」——
  pytest 不转义 param id 里的 `]`/`[`，`['a]::b']` 的 nodeid `…test_real[a]::b]` 与
  `[']::[']` 的 `…test_real[]::[]` 都会让朴素括号深度扫描误判（r6/r7 同错，r8 修）：
  现在把**末尾 param id 区域**整段视作一段（`_param_region_start`：①取**把括号深度降到 0 的
  那个 `[`**（r9 更正：**不是**「末尾方括号组最左的 `[`」——全串 `]` 多于 `[` 时 `for…else`
  兜底取**全串最左的 `[`**，路径里更靠左的 `[` 会把起点带偏，该形状在 `expect_fail` 里不可达，
  详见 README §1.4 的 r9 更正）；②起点前紧接 `::` 时继续向左吸收，覆盖 `[]::[]`），
  区域之外才用「方括号外」的深度扫描。
  否则 param id 里的 `/`、`::`、`\` 被二次归一，两条信道**字面相同**的身份
  （`justify_red(x,[x])`）被判不等 ⇒ 合法失败记成 `junit-reportlog-mismatch`。
  **stdout 没有判决权，但 stdout 红 id 也不能是无源之水**：r4 曾声称伪造 `FAILED …` 行进不了
  红名单——**被 r5 verifier 证伪**（`atexit` 在真 summary 之后还能再打表头）。现在的口径是
  「stdout 候选 + XML 逐条佐证（单向）」：伪造能污染**展示**，不能决定**判决**。
  **r6 更正**：r5 写的「能挡住…伪造 stdout 假象」在 `ccb3e4d` 上是**假的**——当时的双向点分
  后缀匹配让「比 XML 真身份更长」的伪造 stdout id 被佐证，单靠 stdout 就能判死；r6 单向化
  （`red_id_matches_identity`）之后该说法才成立，同一伪造改记 `red-not-in-junit`。
  **威胁模型**：变异体是我们
  自己的定义；进程内**同时**伪造两条 pytest 产物不可阻止、属明确范围外（详见
  `scripts/mutation_replay/README.md` §1.5），能保证的是不误判死 + 跨信道不一致即 error +
  证据写进旁证。退出码：0 全杀死；1 有存活/error/基线不绿/锚点失效/**store 0 条定义**；
  2 用法错（未知 `--one`、`--check-anchors` 缺 `--tree`、`--python` 不存在/不可执行、
  `--timeout` 为负，走 argparse，不抛裸 `AssertionError`/`FileNotFoundError`）。
  **超时与活性**：`--timeout`（默认 900s）单次 pytest 超时 ⇒ error（`"pytest-timeout"`），
  永不判死、也不会挂死；超时用 `start_new_session=True` + `os.killpg` **整组终止**（POSIX），
  但不解决**自起会话**（`os.setsid()`）的孙进程——那会以 `ppid=1` 活下来（r6 R6，README
  §1.4 已声明此限度）。junit / report-log 两条信道的读先经 `read_regular_bytes` 拒**非普通
  文件**（FIFO/设备/目录）并封顶 64 MiB：r5 的 `Path.read_bytes()` 在 FIFO 上会**永远阻塞**
  父进程（`--timeout` 只管 pytest 子进程），r6 起秒级返回具名 error（R5）。**基线跑不起来**
  （无 pytest / 基线超时 / 基线本来就红）也记 error
  （`pytest-unavailable` / `pytest-timeout` / `baseline-not-green`），不再冒充 SURVIVED。
  详见 `scripts/mutation_replay/README.md` §1.4/§1.5/§2。
  **加载器口径（勿高估）**：JSON store 每条只读 `file`/`old`/`new`——**单处编辑**；
  写成 `{"edits": [...]}` 会 `KeyError: 'file'`（rc=1）。多处编辑目前只存在于
  `mutations.py` 的 `edits` 列表（`#133` 91 条里有 12 条），JSON 侧需拆分或扩展结构（见文末）。
  另外 `failed_nodes` 只在 pytest 的 `short test summary info` 段内解析，且对
  `SUBFAILED(<上下文>) <node>` **整段锚定**后取 node（上下文可含空格/括号），不再按首个空格切；
  r7 R1 起 nodeid 与理由的 `" - "` 切分**不靠猜**：`failed_candidates` 把行内每个切点都作为候选、
  由 XML 权威身份取首个被佐证的（param id 可以含空格甚至 `" - "`，理由里也可以含 `" - "`）；
  红 id 仍有**两种形状**（`FAILED` 分支截成
  `Class::test`、`SUBFAILED` 分支留完整 `tests/unit/…::Class::test`），见
  `scripts/mutation_replay/README.md` §8——同名 `Class::test` 跨文件**塌缩**已由 r6 端到端
  复现（记录红数偏小、判决不受影响），按「已验证并声明」处理，不改三条既有记录。
- `doc_table.py`：由 `--record` 渲染文档表；`--check` 做**逐行**校验
  （变异 id、红数、红用例 id **以及结论列**——`KILLED` / `SURVIVED` /
  `SURVIVED（documented-unobservable…）` / `ERROR（rc=N，未判死）` / `ERROR（锚点失效，未判死）`
  （r6 R9，`rc: null`），结论口径只由一处给出；标了「历史，已删除」的行豁免——**r6 R7 起
  只认能解析的表行**：标记必须紧跟被解析出的名字，r5 的子串匹配（文档任意位置含
  `| **<name>**（历史，已删除）` 即豁免）已被 verifier 用一行解析不出的
  `| **ZZZ**（历史，已删除）| 0 | KILLED |` 打穿）。把 error 记录写成 `KILLED` 会被
  `--check` 挡住（N2）。
  **同名重复行也判 FAIL**（r5 N2 补洞）：`parse_rows` 是 last-wins，靠前的伪造 `KILLED` 行
  会被靠后的正确行静默掩盖（verifier 用改前顺序通过过 `--check`）；现在 `duplicate_names`
  显式报 `文档表重复 key: […]`，与 `check_readme_table.py` 的 `dupes` 同口径。
- `check_readme_table.py`：来源 `fix/79-85`，新增 `--store`/`--readme`。默认位置没有
  `mutations/*.json`（本补丁未落地）时打印 `SKIP: …默认跳过` 并 rc=0；**落地态**（115 个 JSON
  + 统一 README）默认跑打印 `N/A: …统一登记册…本检查器对登记册不适用` 并 rc=0（N1：不再出现
  `表格行数 17 != 变异文件数 115` 假失败，r4 文档写的 `6`、r5 写的 `9` 都是陈旧值；
  r6/r7 实测均为 17 = §1.4 判死表 8 行 + §1.4 佐证关系表 6 行 + §4 的 2 行 + §7 的 1 行；
  r7 新增的 `expect-survive-with-red` 行首格不是「裸反引号 token + ` |`」形状，不进该计数）；**显式**
  `--store` 却为 0 个 JSON、或**显式**
  `--readme` 指向登记册 README ⇒ `FAIL` rc=1。**「登记册」不再是可以自报的字符串**
  （r5 N1 补洞）：r4 只看 README 里有没有 `mutation-replay:unified-registry`，往任一单位
  README 追加一行即可把 `--store <unit>` 变成 `N/A rc=0`；现在必须**同时**满足
  ① 标记带 `store-version=<64 位 hex>`、② 该值 == 检查器现算的同目录 `replay.py` 的 sha256、
  ③ 这份 README 就是检查器旁边那份；缺一即按单位 README 严格校验。单位 store 照旧严格校验
  （`#79-85`：`OK: 61 行 == 61 个变异文件`）。
- 本文件：落地指引。

**不携带任何单位的定义**——各单位落地时只带自己的 `mutations/*.json`（或保留其
`mutations.py`，兼容层直接支持）。

## 落地顺序（建议）

1. **先落本补丁**（`store/unified`）：它只动 `scripts/mutation_replay/`，与任何单位的
   定义零冲突。
2. **再按原顺序落地各单位**，但每单的补丁**剥掉**下面这些文件（它们在统一后由本补丁
   提供，重复落地会互相覆盖）：
   - `fix/68`：删 `scripts/mutation_replay/replay.py`、`README.md`（保留 `mutations/*.json`）；
   - `fix/79-85`：删 `scripts/mutation_replay/replay.py`、`README.md`、
     `check_readme_table.py`（保留 `mutations/*.json`）；
   - `c9d5c25`（`#133`，旧分支 `fix/55`）：删 `scripts/mutation_replay/replay.py`、`doc_table.py`、
     `README.md`、`last_replay.json`（保留 `mutations.py`；如需 JSON 化见下）。
3. 其余只带 `mutations/*.json` 的单（`#69`/`#87`/`#95-99`/`#109-110`/`#118`/`#137-139`）
   原样落地；同名冲突由 `replay.py` 显式报错挡住。

### 接收侧警告（必读）：不要整目录 checkout 单位分支

**不要用 `git checkout <单位分支> -- scripts/mutation_replay/`（或 cherry-pick 整棵子树）
来"剥"文件。** 那等于把整个目录切回该单位的旧版：`replay.py` 会被**静默覆盖**成
201（`a2426636`）/ 207（`5e21e71`）/ 156（`c9d5c25`）行，与统一版（**1393 行**，带
`--store` / `--record` / `--evidence` / `--check-anchors` / `--tree` / error 状态 /
`--junit-xml`+`--report-log` 两条判死信道 / 红 id 佐证单向化 + `red-unknown` + FIFO 拒绝 /
进程组超时终止 / r7 的空格 param id 候选消歧 + `flat_identity` 幂等 + `expect-survive-with-red` /
r8 的末尾 param id 区（unbalanced `]`）修正 + 幂等范围限定）
分别差 **1384 / 1390 / 1461** 行内容（含 hunk 头为 **1460 / 1466 / 1511** 行；逐个复现命令与输出见
`scripts/mutation_replay/README.md` §5.1——更早的 `292/298/423`、`362/368/474`、r3 的
`410/488`、`416/495`、`517/565`、r4 的 `573/655`、`579/661`、`668/720`、r5 的
`1061/1135`、`1067/1141`、`1134/1185`、r6 的 `1177/1251`、`1183/1257`、`1250/1302`
与 r7 的 `1304/1378`、`1310/1384`、`1379/1430`
对**本 revision
不成立**）；`a2426636`/`5e21e71` 连带覆盖
`README.md`，
`c9d5c25` 连带覆盖 `doc_table.py` 与 `last_replay.json`。**必须让各单位重发只含
`mutations/*.json`（`#133` 只含 `mutations.py`）的剥离补丁，用 `git apply` 落地**，
落地后核对：

```bash
shasum -a 256 scripts/mutation_replay/replay.py
# 期望：d95d97497d37dc1d85e8e26a3c906362ee72c83f55db3822f1e366d882e47061
```

### 落地后：合并 store 是**登记册**，不是可整体重放的 store（G3）

全部单位落地后 `scripts/mutation_replay/` 里有 **206 条**定义（54 + 61 条 JSON +
91 条 `mutations.py`）。但每条 `old` 只在**它自己的 revision** 的源码里成立，`--tree`
又是整棵树级别的——所以**不存在**「一个 `--tree` 重放 206 条」。用单一 `--tree` 做
`--check-anchors` 的锚点问题数（完整可复现命令与读法见
`scripts/mutation_replay/README.md` §7）：

> **本补丁交付时不是这个状态（r9 澄清）**：本补丁的 `scripts/mutation_replay/` 里**只有四个
> 脚本**（`replay.py`/`README.md`/`check_readme_table.py`/`doc_table.py`），**没有** `mutations/`、
> 也没有 `mutations.py`——`--list` 是 **0 条 / rc=1**。下面的 **206** 与 `205/151/144/115`
> 都是**各单位全部落地之后**（合并 store）的属性，**不是本补丁的属性**。

| `--tree` | 锚点问题数 / 206 |
| --- | --- |
| 落地统一树 | 205 |
| `a2426636`（`#68`） | 151 |
| `5e21e71`（`#79-85`） | 144 |
| `c9d5c25`（`#133`） | 115 |

**正确做法**：按单位分组、各自用自己的 `--tree` 重放（README §5 的三条命令）。
下面「落地顺序」只管**定义入库**的顺序，不代表入库后能整体重放。

## `#133` 的两种去向

- **保留 `mutations.py`（零迁移，推荐先做）**：统一 replayer 直接加载；`doc_table.py --check`
  对 `docs/mutation-testing.md` 逐行校验（历史行豁免）。
- **可选机械转 JSON**（若维护者要求单一 JSON store）：草案脚本（未随本补丁交付，属迁移工具）：

```python
# scripts/mutation_replay/migrate_py_to_json.py（草案）
import importlib.util, json, pathlib
STORE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("m", STORE / "mutations.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
out = STORE / "mutations"; out.mkdir(exist_ok=True)
for name, entry in m.MUTATIONS.items():
    edits = entry["edits"]
    assert len(edits) == 1, f"{name}: 多处编辑需要拆分为多条 JSON"
    file, old, new = edits[0]
    (out / f"{name.lower()}.json").write_text(json.dumps({
        "name": name, "issue": "133", "concern": "G7",
        "file": file, "old": old, "new": new,
        "rationale": entry.get("note", ""),
        "expect_fail": list(entry.get("tests") or m.DEFAULT_TESTS),
        **({"expect_survive": True} if entry.get("expect_survive") else {}),
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
```
  注意：`#133` 的部分变异含**多处编辑**（`edits` 长度 > 1），迁移时需按「同一变异
  多文件/多片段」扩展 JSON 结构（例如 `"edits": [{"file","old","new"}, ...]`），
  或把它们拆成多条同名前缀的 JSON —— 这一步需要维护者拍板，故本补丁**不做自动迁移**。
