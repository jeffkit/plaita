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
| `node` | 节点级结构 | `graph.no_start` / `graph.orphan` / `graph.cycle` / `graph.dead_end_non_end` / `node.duplicate_id` / `node.switch_no_default` / `graph.malformed_node`（#309：缺形节点具名报错，零报错剔除已废除） / `reference_target_missing` / `reference_target_unspecified`（#309：reference 既无 flow_id 也无 childFlow）/ `reference_unexecutable_on_runtime`(warning，#309：python 池对 reference 零可执行形状，runtime 请指定 go) |
| `field` | 字段 schema（与保存/发布同一实现） | `invalid_definition` / `node_schema` / `node_validate` |
| `edge` | 连线语义 | `edge.unknown_target` / `edge.handle_not_a_branch` / `edge.if_branch_missing` / `edge.ambiguous_multiple`（#309：容器展开态子节点之间的连线不报——已双写进 owner.childFlow，折叠不消费；子节点与其他端点混连仍报） |
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

## graph generate API（#305 Phase 2）

```
POST /api/graphs/generate
Authorization: 会话 cookie 或 X-Admin-API-Key（写方法闸 editor+；
               租户作用域照 tenant_scope(required=True)，与 validate 同口径）

{
  "prompt": "把用户提交的标题批量翻译成英文并汇总",   // 必填，自然语言业务需求
  "runtime": "go",                 // 可选；缺省 python（能力门判定池 +
                                   //   reference 可执行性提示）
  "hints": {                       // 可选
    "preferred_node_types": ["map", "http"],   // 节点类型偏好（注入生成 prompt）
    "reference_flows": [                        // 租户内可引用的子流程清单
      {"flow_id": "lib-1", "desc": "取数"}      //   （reference 只能引用清单内流程）
    ]
  }
}
```

生成闭环（`services/graph_generation.py`）：LLM 单发补全产出 GraphDTO 草稿 →
**必须过 `validate_graph`**（与 validate 端点同一入口）→ 不过则把 errors 原样
回喂模型自纠（有界 3 轮）→ 仍不合法则 errors[] 原样带回调用方。端点纯函数式
**不落库**——保存仍走宿主既有 save。

响应（`200`）：

```json
{
  "valid": true,
  "graph": { "nodes": [...], "edges": [...] },   // GraphDTO 草稿
  "definition": { "nodes": [...] },              // 草稿折叠出的 Flow 定义
  "errors": [],                                  // valid=true 时仅剩 warning
  "generation": {"model": "GLM-5.2", "duration_ms": 1234,
                 "attempts": 1, "usage": {"input_tokens": 0, "output_tokens": 0,
                                          "total_tokens": 0}}
}
```

- `valid=false`（200，非 422）：有界轮数内未产出合法草稿——`errors` 为最后一轮
  validate 的错误**原样**（或生成面具名错误
  `generation.unparseable_output` / `generation.provider_error`），`graph` 为
  最后一次草稿（解析失败时 null）。宿主 AI 可据此改写需求重试；
- `422 empty_prompt`（prompt 空/纯空白）/ `422 invalid_runtime`；
- `503 generation_client_unavailable`：模型凭证/依赖不可用；
- `500 generation.childflow_not_double_written`：**A128-01 内容级自检拦截，
  不出草稿**——generate 是第一个非编辑器 GraphDTO 产源，而折叠豁免子节点
  （`parentId`+`isContainerChild` 双标记）不核内容已双写进宿主 childFlow
  （validate 面对该形态可 `valid=true` 静默丢失，#309 的形状具名覆盖不到
  「标记齐全但内容未写回」）。生成草稿在出产前经
  `graph_validation.childflow_double_write_errors` 抽查：投影宿主非容器类、
  双写副本缺 id（含编辑器 `owner::id` 命名空间）、type/共享字段/branches
  不一致，任一命中即具名 500。宿主是容器类但无 childFlow 载体的形态不在
  自检域（validate 会具名 `node.child_flow_missing`，非静默缝）。

模型接入：pydantic-ai Anthropic 协议端点，凭证走 flowcast providers.json
（与 copilot flow_agent 同源，后端不落密钥）；
`PLAITA_CONSOLE_GRAPH_GEN_PROVIDER` / `PLAITA_CONSOLE_GRAPH_GEN_MODEL` 可覆盖。
测试/宿主经注入式 `complete` 打桩，不真出网。

> Phase 3（embed token + iframe `/embed/graph-editor` + postMessage 协议）
> 待做；embed token 鉴权集成到位前，generate/validate 均走既有会话/API key。
