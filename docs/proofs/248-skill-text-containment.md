# #248 证明：flow_get_skill 的 skill_name 路径穿越收敛

- 单号：plaita#248（P1 security，#54 边界明示的另开单项）
- 基线：`origin/main` = `ec760b2`（含 #54/#241/#242/#243/#247）
- 边界：只动 `get_skill_text` 与 MCP `flow_get_skill` 的校验/包装面；
  不枚举 skill、不改 CLI 参数面、不重提 `get_skill_reference`（#54 已修）、
  不覆盖 `SKILL.md` 之外的文件名。

## 根因

`plaita_ai/flow_runner.py::get_skill_text(skill_name)` 把调用方字符串直拼进
两条路径，读取前不做任何归属校验：

```python
filename = f"{skill_name}/SKILL.md"
resources.files("plaita_ai.skills").joinpath(filename).read_text(...)
# 失败后
here = Path(__file__).resolve().parent / "skills" / skill_name / "SKILL.md"
```

`skill_name="../../.../Users/kong/.agents/skills/agently-mail"` 可逃出包内
`skills/`，读取宿主任意 `SKILL.md`。MCP `flow_get_skill` 原样透传，无包装。

## 修法（双保险）

`_contained_skill_dir(skill_name)`：

1. **白名单**：必须是裸小写字母/数字/连字符目录名
   （`^[a-z0-9-]+$`）——拒绝 `/`、`.`、`..`、绝对路径、大写、下划线、
   空串与非字符串；
2. **归属校验**：`(skills_root / skill_name).resolve()` 必须仍在
   `skills_root.resolve()` 之内（词法 `..` 与符号链接逃逸同时挡住）。

`get_skill_text` 先过校验；importlib.resources 分支使用白名单名，editable
fallback 使用校验后的 `contained / "SKILL.md"`。非法名在触达任何路径前抛
具名 `PathTraversalError`（ValueError 子类）。

MCP `flow_get_skill` 与 `flow_get_skill_reference` 同款包装：捕获
`PathTraversalError`，返回
`{"ok": false, "error_type": "PathTraversalError", "error": ...}` refusal
JSON，不返回任何文件内容。

## 测试（8 tests / 12 subtests，`plaita-ai/tests/test_skill_text_containment.py`）

- 穿越拒绝：`../..`、深 `../../×12`、绝对路径 `/etc`、`flow/coder`、
  `flow.coder`、`Flow-Coder`、`flow_coder`、空串、`.`、`..`、`../flow-coder`、
  `None` → `PathTraversalError`；
- **内容不泄露**：临时目录放 sentinel `SKILL.md`，用
  `os.path.relpath(temp_dir, skills_root)` 构造穿越名；MCP 返回 refusal
  JSON 且输出不含 sentinel，库函数同步拒绝（旧实现会返回 sentinel 全文）；
- 合法面：默认 `flow-coder` 与全部三个 bundled skill
  （flow-coder / plaita-flow-builder / plaita-flow-runner）逐字等于磁盘
  `SKILL.md`；MCP 默认/显式 `flow-coder` 同样逐字一致。

相邻回归：`test_skill_reference_containment.py`（#54）+
`test_mcp_integration.py` + 新文件 **67 passed / 12 subtests**；全量
plaita-ai **241 passed / 14 skipped / 12 subtests**。

## 负控

删除 `get_skill_text` 的 `_contained_skill_dir` 调用并恢复直拼 fallback：

```
14 failed, 3 passed, 3 subtests passed
E       AssertionError: PathTraversalError not raised
...
E       AssertionError: expected refusal payload, got raw output (content leak?):
        'SENTINEL_SKILL_TEXT_248_DO_NOT_LEAK'
```

后者是修复前的真实泄露形态：MCP 直接把宿主 `SKILL.md` 内容返回。还原修复后
**8 passed / 12 subtests**。完整日志
`docs/proofs/data/248-negative-control.txt`。

## 验收

- plaita-ai 全量 **241P/14S/12 subtests**；相邻 67P/12 subtests；
- 全量 unit（`-m "not slow"`）**8172 passed / 12 skipped / 14 deselected /
  3492 subtests**；e2e+integration **287P/4S**；console **571P/13S**；
  layering / wheel / size 三相位全绿；
- 基线 `ec760b2` 的 CI 仍在跑（gate 以最近完成 run `d9f4567` success 放行），
  单 commit 推送后由 `closes #248` 的新 run 覆盖验证。
