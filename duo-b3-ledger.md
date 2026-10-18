# B3 席位交付台账

> 一行摘要（第十五轮）：#223 凭据导出改为同目录临时文件 + fsync + 原子 replace，写失败旧文件逐字节保全；读侧空/残文件抛 `CredentialStoreCorruptError`，不再静默成「无凭据」。

| 轮次 | Issue | 交付内容 | 验收 |
|---|---|---|---|
| 第十五轮 | [#223](https://github.com/jeffkit/plaita/issues/223) | `_export_store` 原子替换（临时文件 + fsync + `os.replace`，异常清理临时文件）；`_read_store` 对空/截断/非对象/不可读文件具名报错，文件缺失仍为空 store；console/engine 负控 17 例 + 文档同步 | `ci-gate.sh` pytest/layering/wheel/size 全绿；console 相关 49 条、engine 凭据 33 条全绿；`mkdocs --strict` 通过；`landing_gate.py ci` 绿 |
