# #229 证明：Supervisor 候选放行的 regressions / judge_blind 两道闸

- 单号：plaita#229（P1，security/test）
- 基线：`origin/main` = `d6687a3`（issue 基线 `237f1f1`；提报席载体 base
  `afe4bba1`，树 `c49ba447`，本单只取其中的 #229 hunk）
- 边界：**纯测试**——只改 `plaita-ai/tests/test_supervisor.py` /
  `test_evals_judge.py` 与 `docs/proofs/`；产品代码零改动（变异仅负控期间
  临时注入，跑完逐字还原）。#217 未落地项不混入。

## 缺口

`Supervisor.iterate` 的放行判定（`plaita-ai/plaita_ai/supervisor.py:615-622`）：

```python
improved = (
    not failed_hard
    and not judge_blind
    and base_avg is not None
    and cand_avg is not None
    and cand_avg >= base_avg + policy.min_improvement
    and not verdict.get("regressions")
)
```

当前实现正确，但既有用例走不到这两道闸的**真实风险组合**：

- `test_regression_is_not_promoted` 的候选 **avg 未提升**；
- `test_supervisor_refuses_promotion_when_judge_was_down` 的基线/候选定义相同。

即「候选 avg 更高 + 有 regressions」与「候选 avg 更高 + judge_blind」两种组合
没有用例；去掉任一条件，旧套件仍全绿，auto 档会把回归/盲评候选直接发布。

## 新增测试（4 条，manual/auto 参数化）

1. `test_supervisor.py::test_regressions_block_promotion_even_when_average_improves`
   （manual/auto）：3 case，基线只过 c0，候选过 c1/c2 但打回 c0（avg 1/3 → 2/3，
   `comparison.regressions == ["c0"]`）；断言 `status == "no_improvement"`、
   无 `promotion_ticket`、`publish_version` 调用数 **0**、published 仍 1.0.0、
   候选版本仍 draft。
2. `test_evals_judge.py::test_judge_blind_blocks_promotion_when_candidate_avg_is_higher`
   （manual/auto）：baseline 有 1 个 `judge_unavailable` case（avg 0.5），候选
   avg 1.0、`regressions == []`；断言 `judge_unavailable_cases == {"baseline":1,
   "candidate":0}`、`status == "no_improvement"`、无 ticket、publish 调用数 0，
   reason 保留 `judge unavailable` / `not comparable`。

两条都直接断言「没有调用 `publish_version`」，不是只看 published 指针。

## 负控

### A · 去掉 regressions 闸

`and not verdict.get("regressions")` → `and True`：

```
FAILED test_regressions_block_promotion_even_when_average_improves[manual]
FAILED test_regressions_block_promotion_even_when_average_improves[auto]
2 failed, 17 passed
```

完整 `plaita-ai/tests`：**2 failed / 227 passed / 14 skipped**（仅这两个新用例红）。
红文案 `assert 'improved' == 'no_improvement'`（`- no_improvement + improved`；
auto 档同时会 publish 1.0.1，被 publish 调用数断言拦住）。
证据 `docs/proofs/data/229-a-regressions-neg.txt`。

### B · 去掉 judge_blind 闸

`and not judge_blind` → `and True`：

```
FAILED test_judge_blind_blocks_promotion_when_candidate_avg_is_higher[manual]
FAILED test_judge_blind_blocks_promotion_when_candidate_avg_is_higher[auto]
2 failed, 11 passed
```

完整 `plaita-ai/tests`：**2 failed / 227 passed / 14 skipped**。红文案同为
`assert 'improved' == 'no_improvement'`。
证据 `docs/proofs/data/229-b-judge-blind-neg.txt`。

还原后：两个文件 **32 passed**；完整 `plaita-ai/tests` **229 passed /
14 skipped**。提报席原始负控/探针证据随件在
`docs/proofs/data/229-negative-control.txt` 与 `229-repro.py`。

## 验收记录

- 新增用例：**4 passed**（2 regressions + 2 judge_blind）；
- 负控 A/B：各 **2 failed**，完整 plaita-ai 仅新用例红；还原后 **229P/14S**；
- console 全套：**546 passed / 13 skipped**；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**7955 passed / 1 failed / 12 skipped / 3434 subtests**。唯一失败为
  既知 `test_issue159_policy_url_exception_sinks.py`（yarl 文案环境差异，clean
  基线同样失败）；本单只改 plaita-ai 测试，无新增失败；
- landing gate：base `d6687a3` 的 CI slow lane / Console E2E success，fast CI
  见交付记录；
- 产品代码：零改动（`git diff` 仅测试与 `docs/proofs/`）。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #229`。
- 验收面：plaita-ai 229P/14S；console 546P/13S；e2e+integration 287P/4S；
  unit 7955P/1F（既知 yarl 环境面）。
- landing gate：TODO
