# GraphDTO 与 graph validate API（plaita#305 Phase 1）

宿主应用（墨者 Studio 等）与嵌入编辑器在**保存/发布前**校验图用的契约。
权威实现与输入形状文档：`backend/services/graph_validation.py` 模块 docstring；
前端权威实现：`frontend/src/components/flow/flowConverter.ts`（`flowToJson`）。
两边折叠规则必须同步修改，一致性由
`backend/tests/console/test_issue305_graph_validate.py` 的双向矩阵钉住。

## 接口

```
POST /api/graphs/validate
Authorization: 会话 cookie 或 X-Admin-API-Key（租户作用域照 tenant_scope；
               embed token 授权集成在 #305 Phase 3）

{
  "graph": {                       // 编辑器序列化对象（GraphDTO）
    "nodes": [ { "id": "check", "type": "plaitaNode",
                 "position": {"x":0,"y":0},
                 "data": {"type": "if", "name": "检查",
                          "fields": {"condition": {"field":"INPUT.ok",
                                                   "operator":"eq","value":true}}} } ],
    "edges":   [ { "id":"e1", "source":"check", "target":"end",
                   "sourceHandle":"true" } ],
    "viewport": {"x":0,"y":0,"zoom":1},      // 只收不判
    "meta":    {"flow_id":"...", "version":"1.0.0", "desc":"...", "author":"...",
                "inputType":{...}, "outputType":{...}, "globalContext":{...},
                "metadata":{...}, "timeout":"30s"}
  },
  "runtime": "go"                  // 可选；缺省 python（能力门判定池）
}
```

## 响应

- **合法** → `200 {"valid": true, "errors": [warning...], "definition": {...}}`
  （`definition` 为 GraphDTO 折叠出的 Flow 定义，可直接走 `PUT /api/flows/...` 保存）；
- **非法** → `422 {"code":"invalid_graph", "valid":false, "errors":[...全量...],
  "definition":{...}}`（非首错即停）。

`errors[]` 每项：`{type, code, node_id?, field?, message, severity}`

| type | 含义 | 常见 code |
|------|------|-----------|
| `node` | 节点级结构 | `graph.no_start` / `graph.orphan` / `graph.cycle` / `graph.dead_end_non_end` / `node.duplicate_id` / `node.switch_no_default` / `reference_target_missing` |
| `field` | 字段 schema（与保存/发布同一实现） | `invalid_definition` / `node_schema` / `node_validate` |
| `edge` | 连线语义 | `edge.unknown_target` / `edge.handle_not_a_branch` / `edge.if_branch_missing` / `edge.ambiguous_multiple` |
| `capability` | 能力门（#300 同一实现） | `capability.unsupported_node_type` / `capability.pool_manifest_missing`(warning) |

`severity`：`error` 计入 invalid（422）；`warning` 不阻断（ir 规则告警、
死边、池无清单降级提示）。

## 校验面与 publish 门的共享实现

图内容校验直接调用保存/发布面的两个权威函数，不另写第二份：

- 字段 schema：`services/validation.validate_definition`（= `Flow.model_validate`，
  保存/导入同一函数）；
- 能力门：`services/capability_gate.gate_flow_nodes`（#300 publish/start 同一函数）。

一致性契约（`TestConsistencyMatrix`，双向矩阵）：

- **V⇒P**：validate 通过的 graph，用其 `definition` 保存+发布必成功；
- **P⇒V**：save/publish 拒绝的 graph，validate 必报同因（capability 按 #300
  detail 同键 `runtime/unsupported/supported` 对账；field 按异常文本同因）。

validate 额外做编辑器图层面的结构/连线校验（单入口、可达 end、无环、孤儿、
handle 语义、reference 引用的租户内存在性）——**validate 比 publish 严**是
设计内不对称：发布得了但运行期才炸的图，在保存前被拦下。
