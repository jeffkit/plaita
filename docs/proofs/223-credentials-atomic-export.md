# #223 证明：凭据导出原子替换 + 读侧空/残文件 fail-loud

- 单号：plaita#223（P1）
- 基线：`origin/main` = `f83a268`（本工作树 `wt-b3`，落地时 main 已至 `afe4bba`）
- 边界：只动凭据导出/读侧面；不改 DB 模型、加解密格式、租户路由与 API 契约。

## 缺陷

`credentials_svc._export_store` 直接 `path.write_text(...)` 覆盖引擎读取的
凭据文件（先 truncate 再写）。写到一半失败（ENOSPC / EFBIG / SIGKILL）会把
上一份完好文件变成半截 JSON；读侧 `_read_store` 把 `JSONDecodeError` /
`OSError` 一并吞成 `{}`，于是全部凭据表现为「不存在（当前租户可用: （无））」，
而 DB 行仍在——报错文案把排查方向引向「去编排台重新创建凭据」。

复现（issue 正文，OS 级 `RLIMIT_FSIZE` 制造 EFBIG）关键输出：

```
phase1: exported size = 449
after failed export: bytes = 149 | previous good content intact? False
file now INVALID JSON: Unterminated string starting at: line 4 column 11 (char 46)
phase3 (reader): _read_store() = {}
get_credential('feishu') -> 凭据 'feishu' 不存在（当前租户可用: （无））
phase4 (db): DB rows still present: ['db', 'feishu']
```

## 修法

1. **写侧原子替换**：`credentials_svc._atomic_write_text` 用同目录
   `tempfile.mkstemp` 写临时文件 → `flush` + `os.fsync` → `chmod 0600` →
   `os.replace(tmp, path)`。异常路径（含 `BaseException`）关闭 fd、删除临时
   文件后重抛；目标文件从未被打开写，旧内容逐字节不变。
   `mkstemp` 与目标同目录，`os.replace` 保证同一文件系统内原子生效。
2. **读侧具名报错**：新增 `CredentialStoreCorruptError(CredentialError)`。
   `_read_store` 仅对「文件不存在」返回空 store；文件存在但为空/纯空白/
   JSON 截断/顶层非对象/不可读时抛该异常，报错带文件路径并提示重新保存
   任意一条凭据重建导出文件。`credential_type` 复用同一读路径，同样
   fail-loud。

## 负控（旧实现必红）

1. 临时 stash 两处实现后跑新增用例：
   - console：`test_partial_temp_write_keeps_previous_file_intact`、
     `test_fsync_failure_keeps_previous_file_intact`、
     `test_replace_failure_keeps_previous_file_intact` → **3 failed**
     （旧实现不经过临时文件/fsync/replace，注入点不触发，且就地写会截断）。
   - engine：新用例无法导入 `CredentialStoreCorruptError`（collection error）。
2. 行为级探针（保留异常类、仅把 `_read_store` 换回旧实现）：

   ```
   old _read_store on empty file = {}
   old get_credential -> 凭据 'feishu' 不存在（当前租户可用: （无））。请在编排台「凭据」页创建
   ```

   即旧读侧确实把空文件静默成「无凭据」。

## 测试

新增 `plaita-console/backend/tests/console/test_issue223_credentials_atomic_export.py`（5 例）：
- 成功导出内容正确且权限 `0600`；删除最后一条仍导出合法 `{}`；
- 临时文件写到一半抛 `OSError(ENOSPC)`、`fsync` 失败、`os.replace` 失败
  三种注入下：旧文件**逐字节不变**、无 `.tmp` 残留、引擎侧仍解出旧凭据。

新增 `tests/unit/test_issue223_credential_store_corruption.py`（12 例）：
- 缺失文件 / 合法 `{}` → 仍是「不存在」的普通 `CredentialError`（不误报损坏）；
- 空文件 / 纯空白 → `CredentialStoreCorruptError`，报错含「为空」、路径与
  重建指引，且不含「当前租户可用」；
- 截断 JSON → 「不是合法 JSON」；顶层数组/null/字符串/数字 → 「顶层必须是对象」；
- `credential_type` 与 `CredentialError` 子类兼容性；不可读文件（非 root）。

## 验收

```
# 根门禁（pytest + coverage / layering / wheel / size）
bash scripts/ci-gate.sh pytest   → ✓ PASSED（coverage >= 80%）
bash scripts/ci-gate.sh layering → ✓ PASSED
bash scripts/ci-gate.sh wheel    → ✓ PASSED
bash scripts/ci-gate.sh size     → ✓ PASSED

# console 相关面
pytest test_credentials_api.py test_issue223_credentials_atomic_export.py \
       test_enterprise.py test_issue213_quick_test_context.py \
       test_issue217_admin_prefix_matrix.py → 40 passed
pytest test_issue214_viewer_write_matrix.py → 9 passed

# engine 凭据面
pytest test_issue223_credential_store_corruption.py \
       test_credentials_tenant_routing.py → 33 passed

# 文档与落地
mkdocs build -f docs-site/mkdocs.yml --strict → 通过
python scripts/landing_gate.py ci → ✅ main CI 绿，允许落地
```
