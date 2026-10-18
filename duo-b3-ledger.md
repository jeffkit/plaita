# B3 席位交付台账

> 一行摘要（第十六轮）：#238 把 DSL 往返保真从逐点补丁升级为 registry 驱动 conformance——19 节点类型 × 全字段 × @flow/sexpr 双链 490 个单元逐格断言；本轮补齐 28 处静默丢字段面，所有不可表达项显式登记并反向钉住，registry 新增字段/新节点未登记即红。

| 轮次 | Issue | 交付内容 | 验收 |
|---|---|---|---|
| 第十五轮 | [#223](https://github.com/jeffkit/plaita/issues/223) | `_export_store` 原子替换（临时文件 + fsync + `os.replace`，异常清理临时文件）；`_read_store` 对空/截断/非对象/不可读文件具名报错，文件缺失仍为空 store；console/engine 负控 17 例 + 文档同步 | `ci-gate.sh` pytest/layering/wheel/size 全绿；console 相关 49 条、engine 凭据 33 条全绿；`mkdocs --strict` 通过；`landing_gate.py ci` 绿 |
| 第十六轮 | [#238](https://github.com/jeffkit/plaita/issues/238) | registry 驱动 conformance（`model_fields` 全覆盖；490 单元；双链逐字段+整树深等价；LOST/INERT/DERIVED/TYPE_LOST 白名单 + 反向断言）；修 @flow/sexpr 28 处丢字段面（contains、while 基类、itemType、content_type、end/assignment 基类、on_timeout、sexpr while/数据槽位/Property 等）；main-first 并入落地席 harvest 扫掠 harness（修 package import + 钉值环境自适应 + 基线 441→294 同 commit 收减）；ci-gate `conformance` 相位 + ci.yml 具名步骤 | 两套 conformance 60 条；DSL 相关 4023 passed；全量 CI 口径 8361 passed / 19 skipped / 14 deselected（unit 8074 + integration/e2e 287）；ci-gate conformance/layering/wheel/size 绿；`mkdocs --strict` rc=0；`landing_gate.py ci` 绿 |
