# RED-PROOF — #209 supervisor 数据集路径按归属判越界

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `d1ca49c` · 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| 同前缀兄弟目录 | `../supervisor-datasets-evil/x.json`：修复前 **放行并读取**；修复后 **400** |
| 绝对路径 | `.../other/x.json`：400（前后一致） |
| `..` 逃逸 | `a/../../x.json`：400（前后一致） |
| 正常路径 | `case.json` / `sub/case.json`：可解析（前后一致） |
| 正常缺失 | `missing.json`：404（不是 400） |
| `list_datasets` 过滤 | `["case.json","sub"]`（`_`/`.` 隐藏项被过滤，前后一致） |
| 负控 | 恢复 `startswith` ⇒ 兄弟前缀用例 **1 failed / 5 passed** |
| 验收面 | console 路径测试 **6 passed** + 既有 supervisor **5 passed**；unit 快车道 **7752 passed / 3429 subtests**；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`ci-gate.sh` **PASSED**（8052 passed / 覆盖率 93.83%）；`landing_gate.py ci` **success** |

## §1 缺陷与规格

`_resolve_dataset_path` 用字符串前缀判越界：

```python
root = _dataset_root()
candidate = (root / name).resolve()
if not str(candidate).startswith(str(root)):
    raise HTTPException(400, "dataset 路径越界")
```

`/srv/supervisor-datasets-evil/x.json` 也满足 `startswith('/srv/supervisor-datasets')`
——同前缀兄弟目录被放行，随后 `load_dataset(candidate)` 读取 root 外的用例
（可能是业务数据/评测答案）。

规格（#209 comment）：改为路径归属
`candidate.is_relative_to(root)` / `commonpath`；兄弟前缀、绝对路径、`..`
逃逸均 400；正常子目录/`.json` 保持；`list_datasets` 过滤不变；负控恢复
`startswith` 后必红。

## §2 修复（一行）

`plaita-console/backend/api/supervisor.py`：

```python
if not candidate.is_relative_to(root):
    raise HTTPException(status_code=400, detail="dataset 路径越界")
```

- `root` / `candidate` 均已 `resolve()`（符号链接目标纳入判据）；
- `is_relative_to` 按路径段比较：绝对路径与 `..` 解析后的越界目标都不再
  命中字符串前缀；
- 不改 `load_dataset`、`PLAITA_SUPERVISOR_DATASET_DIR` 配置面、
  `list_datasets` 过滤与数据集格式。

## §3 修复前 / 修复后

探针：`docs/proofs/data/209-repro.py`。修复前在干净 worktree `d1ca49c` 跑
（`PYTHONPATH=/tmp/wt209-before:.../backend`），修复后在本树跑；机读结果
`docs/proofs/data/209-before-after.json`。

| 输入 | 修复前 | 修复后 |
|---|---|---|
| `case.json` | 解析到 root 内 | 解析到 root 内 |
| `sub/case.json` | 解析到 root 内 | 解析到 root 内 |
| `../supervisor-datasets-evil/x.json` | **放行**（root 外兄弟目录） | **400 `dataset 路径越界`** |
| `.../other/x.json`（绝对） | 400 | 400 |
| `a/../../x.json` | 400 | 400 |
| `missing.json` | 404 | 404 |
| `list_datasets` | `["case.json","sub"]` | `["case.json","sub"]` |

## §4 验收

### §4.1 console 测试

```
cd plaita-console/backend
pytest tests/console/test_supervisor_dataset_path.py -q  ⇒ 6 passed
pytest tests/console/test_supervisor_api.py \
       tests/console/test_supervisor_dataset_path.py -q ⇒ 11 passed
```

覆盖：正常文件/子目录、兄弟前缀、绝对路径、`..` 逃逸、正常缺失 404、
`list_datasets` 隐藏项过滤。

### §4.2 主套件 / 门禁

- unit 快车道：**7752 passed / 11 skipped / 3429 subtests**；
- e2e：**74 passed / 3 skipped**；
- integration：**213 passed / 1 skipped**；
- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8052 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.83%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `d1ca49c`
  conclusion=success，放行。

## §5 负控（变异纪律）

在 `e9e9b10` 的临时 worktree 恢复：

```diff
-    if not candidate.is_relative_to(root):
+    if not str(candidate).startswith(str(root)):
```

结果：

```
tests/console/test_supervisor_dataset_path.py ⇒ 1 failed, 5 passed
```

- **红**：`test_sibling_prefix_is_rejected`（兄弟前缀被放行，未返回 400）；
- **绿**：正常/子目录、绝对路径、`..` 逃逸、404、`list_datasets` 过滤
  ——它们不依赖归属判据（绝对/`..` 旧前缀判据也会拒绝）。

变异 diff 与红名单：`docs/proofs/data/209-negative-control.diff` /
`209-negative-control.txt`；SHA256 见 `209-before-after.json`。

## §6 边界

- 只动 supervisor 数据集路径判据；不改 `load_dataset`、数据集格式、
  `PLAITA_SUPERVISOR_DATASET_DIR` 配置面、`list_datasets` 过滤；
- `resolve()` 已把符号链接目标纳入判据；不额外处理链接策略。

## §7 复跑

```bash
# 修复前/后探针（cwd=plaita-console/backend）
PYTHONPATH=/tmp/wt209-before:/tmp/wt209-before/plaita-console/backend \
    <venv>/python docs/proofs/data/209-repro.py
PYTHONPATH=.:../.. <venv>/python ../../docs/proofs/data/209-repro.py

# 定向
cd plaita-console/backend && pytest tests/console/test_supervisor_dataset_path.py -q

# 主验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita-console/backend/api/supervisor.py`：`is_relative_to(root)` 归属判据；
- `plaita-console/backend/tests/console/test_supervisor_dataset_path.py`：6 条路径测试；
- `docs/proofs/data/209-repro.py` / `209-before-after.json` /
  `209-negative-control.{diff,txt}`：机读旁证。
