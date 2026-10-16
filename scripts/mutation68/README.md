# plaita#68 规模守卫的变异复核驱动

第三轮验收要求"变异证据可复核"：本目录把当时的驱动脚本与判定口径一起放进交付，
任何人可以在本地按下面的步骤重跑出同样的分类结果（**不需要 gist**）。

| 文件 | 作用 |
|------|------|
| `recheck.py` | **独立进程**重跑 mutmut 的非 killed 变异体（每个变异体一个全新解释器，`-x`） |
| `oracle.py` | **差分 oracle**：用同一份电池判定变异体是否真的等价 |
| `battery.py` | 差分电池：≈450 个调用点 × 2 组环境配置，覆盖 `mul`/`replace`/`mod`/`join`/`concat`/探针与私有 helper |
| `survivors.txt` | 本轮 `mutmut run` 的**全部**非 killed id（`survived` / `timeout` / `not checked`） |

## 复现步骤

> **必须按顺序**：`mutmut results` / `mutmut run <名字>` 依赖 `.mutmut-cache` 里的
> `exit_code_by_key`；没有先跑一次**全量** `mutmut run` 就直接 `mutmut run <名字>`
> 会因为没有基线而直接中止。所以顺序永远是"收窄 → 全量 run → results → 复核"。

```bash
# 0) 前置：只变异本单涉及的两个模块，并只跑相关测试集
#    （临时改 pyproject.toml；跑完必须逐字节还原）
#    [tool.mutmut] only_mutate = ["plaita/core/expression.py", "plaita/core/expression_parser.py"]
#    pytest_add_cli_args_test_selection = <7 个文件，见 docs/mutation-testing.md §2.5>
rm -rf mutants .mutmut-cache && python -m mutmut run

#    收窄只能改 [tool.mutmut]（only_mutate / pytest_add_cli_args_test_selection），
#    跑完**逐字节还原** pyproject.toml（git diff --exit-code pyproject.toml）
#    ⚠ 不要用 `git archive` 导出的目录跑门禁：仓内脚本（test_issue124/125）会因缺文件
#      失败，那是环境伪影，不是回归。

# 1) 非 killed 全量清单（必须先完成上面的全量 run）
python -m mutmut results | grep -E ": *(survived|timeout|not checked)$" \
  | sed 's/:[[:space:]]*[a-z ]*$//;s/^[[:space:]]*//' | sort -u > survivors.txt

# 2) 独立进程复核（rc=0⇒survived；90s 超时/126/127⇒error；其它非零⇒killed）
python scripts/mutation68/recheck.py . 6 < survivors.txt

# 3) 差分 oracle（DIFFERS/PRIV-ONLY/NO-DIFF）
python scripts/mutation68/oracle.py mutants scripts/mutation68/battery.py survivors.txt 6
```

## 口径与注意

* `MUTANT_UNDER_TEST` 必须是**完整** `module.mutant_name`
  （如 `plaita.core.expression.x__fn_mul__mutmut_5`）——mutmut 的 `mutants/` 树按模块
  挂 trampoline，缩写形式会静默跑成"原实现"。
* `rc=0` 只代表"这批测试没杀掉它"；`error`（超时/死循环）按
  `docs/mutation-testing.md` §3 / #124 的约定记 error，**既不算 killed 也不算等价**。
* 电池里所有输入规模都刻意压到"守卫一旦被破坏最多分配几十 MB"——早期版本用过
  `250000 × 100001` 这类组合，破坏守卫的变异体会真的去申请 25GB。
* `NO-DIFF` 是"在这份电池覆盖范围内观察不到行为差异"的证据，**不是**数学证明。
  第六轮验收用**手造变异**证明过：早期的电池对 5 个非等价变异里的 4 个都判
  `NO-DIFF`（缺"巨大返回值/是否被拒/错误类型"的差分、缺说谎对象与包装形态的用例）。
  现已补上判别用例（`eval:mutating-callee`、`priv:content:liar-buffer`、
  `eval:liar-buffer-percent-s`、`eval:template-interpolation`、
  `priv:content:list-{1000001,1000002,1500000}`、`eval:wrapped-join-{partial,lambda}`）。
  **结论口径**：只能说"电池覆盖到的范围内无差异；未覆盖的用手造变异判定"，
  不许写成"0 非等价幸存"。
