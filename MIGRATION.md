# 升级指南 (Migration Guide)

本文件列出 plaita 历次版本中**破坏向后兼容**的改动，并给出迁移步骤。如果你升级后遇到行为变化，先来这里查。

本文件只关心**怎么改代码**。

---

## Unreleased

### 手工 event/timeout resume 必须绑定挂起订阅（plaita#221）

**口径（contract）**：`resume_type=event` / `resume_type=timeout` 的 resume
消息必须携带挂起期登记的那条订阅 id，且 event 还要带上原事件的 `event_type`。
内核在 `_handle_resume` 的准入阶段比对：

- 生产消息形状：`data["subscription_id"]`（`event_filter` 代投的 event/timeout
  载荷一直带该字段）；
- 手工 resume：`data["__plaita_subscription_id"]`（控制台/本地档的私有载体键，
  引擎分发前剥除，不会写进 `event_data` 或流程输出）。

两者与挂起 checkpoint 的 `$NODE[$LAST_NODE]["subscription_id"]` 不一致、或
消息完全缺绑定信息而 checkpoint 有订阅 ⇒ 视为**陈旧消息**：记 WARNING 并抛
`ResumeGuardError`。worker 既有豁免把执行**保持原状**（仍 suspended）、ack
消息、不终态化——等待当前订阅的真正决议。

**为什么改**：同一挂起点的 event 与 timeout 用两把不同的去重键
（`event_filter.py` 的 `dedup:<event_id>:<sub>` 与 `timeout:<sub>`），同一订阅可
同时入队两条 resume。先到的一条解决当前节点、流程推进并在下游重新挂起后，后到
的陈旧 resume 历史上会通过准入把**下游**节点标成 completed/timeout 并写入上一
事件的载荷——流程带错数据继续跑且无任何报错。

- **受影响**：任何**手工构造** event/timeout resume 的代码（直接发队列消息、
  自定义审批服务、脚本）。升级后会拿到 `ResumeGuardError`，不再被静默接受。
  迁移：发送前从执行 checkpoint 取当前挂起节点的 `subscription_id` 放入
  `data["subscription_id"]`；event 再带 `event_type`。不知道挂起订阅的消息
  本就不应被当成有效决议。
- **不受影响**：`event_filter` 代投的 event/timeout 消息（生产形状本就带
  `subscription_id`）；控制台 `POST /executions/{id}/resume` 与本地单机档
  （已自动从 checkpoint 附加私有绑定键）；`cancel`（执行级意图，console 消息
  `data=None`，刻意不按订阅绑定）；`continue` / `retry`（不经过本准入）；
  历史 checkpoint 没有 `subscription_id` 的执行（没有可比对的绑定信息，放行）。

### 控制台 resume 请求体新增可选 `params`（plaita#157）

**口径（contract）**：管理面 `POST /api/executions/{execution_id}/resume`
的请求体新增**可选**字段 `params`（object）。它与 `data` 并列，但语义不同：
`data` 是挂起节点的决议数据（event/cancel/timeout 用），`params` 是**调度入参**
——只有 `resume_type=continue` 打在盘上 checkpoint 为空（假值）时才由引擎用于
重新绑定 `$INPUT`（`DistributedStrategy.execute` 的
`setup_flow(flow, (), params or {})` 分支，strategies.py:236-239）。
典型窗口是「人工/值守 `continue` 抢在 pending start 被 worker reclaim 之前」：
此前该请求体**没有** params 字段，调度 params 永远到不了 `$INPUT`，end 节点读
`$INPUT.<字段>` 一律回落流程默认值（只调参数不改代码的止损手段在这条手动路径
上静默失效）；#122 修的是 start→resume 转换路径的同类丢参，本条是**另一条路由**
（不是"丢了"，是"从来传不了"）。

- **受影响（只有 `continue`）**：`resume_type=continue` 打在**盘上 checkpoint
  为空**的执行上、且请求体带了非空 `params` 的调用。此前请求体里的 `params`
  被 pydantic 静默丢弃、`$INPUT` 绑成 `{}`；现在它进入 resume 载体并被引擎绑定
  为 `$INPUT`。**空 checkpoint 下只有 `continue` 会绑定 `$INPUT`**——没有第二种
  resume_type 处在这条路径上（见下一条）。
- **不受影响（空 checkpoint 下的 `retry`/`cancel`/`timeout`/`event`）**：它们
  **不是**"现在会绑定 `$INPUT`"，而是在同一个假值分支**之前**就被策略层守卫拒绝：
  `ResumeGuardError("resume_type='retry' requires a saved_context, but none was
  provided ...")`（strategies.py:248-257）；worker 侧
  `_chain_has_resume_guard_error` 把它归一为 `ResumeProtocolError`——执行
  **保持 running**（不推进、也不终态化），消息被 **ack（+XDEL）吞掉**
  （实测 `XLEN=0`、`XPENDING=0`），`$INPUT` **永不绑定**。**这是预存语义，
  非本单引入**（同一形态在修复前的树上逐字相同）。
- **不受影响（checkpoint 非空）**：引擎走真值分支
  `context.context = saved_context`（strategies.py:219-220）**整体替换** checkpoint，
  `params` 全程不被读取 ⇒ **params 惰性**，绑定值一律来自盘上 checkpoint
  （正常续跑：suspend 后 event/timeout/cancel 决议、error 后 retry 同理；实测
  params 送达 worker 但 `$INPUT`/end 输出仍取自 checkpoint）。
- **不受影响**：不带 `params` / `params={}` / `params=null` 的调用（任意
  resume_type）——resume 载体**不新增** `params` 键，与 #122 的载体形状逐字一致
  （存量消息与既有调用方零变化）。
- **诚实的边界**：控制台 `resume` 路由现在能把 `params` 送进 resume 载体，但
  **只在引擎真的消费它的地方**才有语义——即「空 checkpoint 上的 `continue`」。
  其余组合（其它 resume_type，或 checkpoint 非空）带 `params` 既不报错、也
  不改变行为：**不是** route 级"空 checkpoint ⇒ 绑定 `$INPUT`"规则。
- **不受影响（本地单机档）**：`resume_local_execution` 只接受 `suspended` 行，
  对 running 行的 `continue`/`retry` 直接 **400**（`仅挂起状态可恢复`）——本地档
  本就不存在这条静默路径；`params` 字段在本地分支被忽略（不做本地档透传）。
- **迁移**：无需改代码（纯增量字段）。若你的 keeper/值守脚本此前想用
  「手动 continue + 新参数」止血，现在可以在请求体里带 `params`；
  控制台前端**暂不提供**该输入框（ResumeDialog 故意不提供 `continue` 选项，
  见 `frontend/src/pages/ExecutionDetail.tsx`）——参数化手动 continue 走 API。

### 子执行继承父级 `$ENV` 白名单（plaita#141）

**口径（contract）**：子执行（`child` 内联子逻辑 / `parallel` 分支 /
`map`·`filter`·`find`·`reduce`·`loop` 迭代体）**继承父执行的 `expose_env`
allowlist**；被执行子流程自己声明了非空 `exposeEnv` 时**以声明为准**（替换，
不是合并）。`FlowExecution` 的子执行与 `ExecutionContext.child()` 两条创建路径
自此逐项一致（此前后者复制 7 项、前者只复制 6 项，`expose_env` 分叉）。

- **受影响**：父流程声明了 `expose_env`、子流程自己没有声明的执行——子流程内
  `$ENV`（或自定义名，如 `$VARS`）现在能看到父级白名单里的变量；此前子执行
  `$ENV` 恒为空 dict（读单个变量靠 `$PARENT` 回落兜底才不显形，但整份 `$ENV`
  读出是空的）。
- **不受影响**：父流程从未声明 `expose_env`（allowlist 为空）的执行——子执行
  `$ENV` 仍恒为空，行为逐字不变；根执行的 `$ENV` 语义不变。
- **迁移**：无需改代码。若你依赖"子流程 `$ENV` 恒空"做隔离，请在**子流程**里
  显式声明它自己要用的 `exposeEnv`（声明替换父级白名单），或不要在父流程声明。

**已知边界（不是缺陷；空声明与未声明不可区分）**：

- **`exposeEnv=[]` 不构成"声明"，子流程无法据此退回空 `$ENV`**。`Flow.expose_env`
  的字段定义是 `default_factory=list`（`plaita/core/flow.py`），pydantic 模型不保留
  "用户究竟写没写这个键"这一信息——`exposeEnv=[]` 与完全不写 `exposeEnv` 在模型上
  都是空 list。因此本单的继承口径只能以**非空**为界：子流程内联声明
  `exposeEnv=[]` 仍会被 `setup_flow` 回落到父级 allowlist，**子流程要向父级白名单
  收窄只有两条路**——在子流程里显式列出它自己实际要用的变量（非空即替换，不是
  合并），或**父流程不声明** `expose_env`（父级快照为空 ⇒ 整条继承链为空）。
  本单不为此引入哨兵值 / `Optional[None]` 变体来区分"空 vs 未声明"：那会改变
  `Flow(**ir)` 的既有可序列化形状，超出本单 Boundary。

### Redis URL 解析：畸形形态从「静默回落 localhost」改为具名拒绝（plaita#88）

`plaita/server/factory.py::_parse_redis_url` 此前用手写正则只认
`redis://[:password@]host:port[/db]`，匹配失败就**静默**返回 `localhost:6379/0`：
于是 `rediss://`、`user:pass@`、省略端口、IPv6 字面量等都被改写成 localhost，而同一
进程的队列/事件总线仍按原始 URL 连——存储与队列连到**两个** Redis。现在这些形态
要么完整解析（与队列侧 `Redis.from_url` 同端点），要么在建立连接前抛
`RedisUrlError`（fail-closed，启动期一次性暴露）。

**未配置**（`None` / 空串 / 纯空白）仍然走文档化默认 `redis://localhost:6379/0`，
不是错误——只有「配错了」才拒绝。

#### 旧解析器接受、现在会抛 `RedisUrlError` 的形态

| 形态（密码一律显示为掩码） | 旧行为 | 现在 | 迁移动作 |
|---|---|---|---|
| `redis://` | localhost:6379/0 | 拒绝（无目标） | 补全 host，或直接不传 `redis_url`（走默认） |
| `redis:///3` | localhost:6379/0 | 拒绝（缺主机名） | 写成 `redis://<host>/3` |
| `REDIS://host:6379/0` | localhost | 拒绝（scheme 必须小写） | 改小写 `redis://` |
| `http://host:6379` / `host:6379` / `//host:6379` | localhost | 拒绝（不支持/缺 scheme） | 用 `redis://` / `rediss://` / `unix://` |
| `redis://user:***@host:notaport` | `host` + 默认端口 | 拒绝（端口非整数） | 端口写数字 |
| `redis://host:99999` | `host:99999` | 拒绝（端口越界 0-65535） | 端口改到合法区间 |
| `redis://host:0` | `host:0` | 拒绝（端口 0 不是可连端点） | 省略端口（默认 6379）或写真实端口 |
| `redis://[::1:6379/0` | `host='['` | 拒绝（非法 IPv6 字面量） | 补 `]`：`redis://[::1]:6379/0` |
| `redis://host/foo` | localhost:6379/0 | 拒绝（路径只能是 db 编号） | 写 `/0`…`/15`，或把参数放 query |
| `redis://user:***@host:6379/0?db=x` | `user` + db 0 | 拒绝（db 不是整数） | `?db=<int>` |
| `redis://host:6379/0?db=-1` | localhost | 拒绝（db 不能为负） | 用非负 db |
| `redis://host:6379/0#frag`、`redis://:***@host:6379/0#frag` | `host` + 默认 db | 拒绝（不支持 fragment） | 凭据里的 `#` 写成 `%23`，其余 `#` 删除 |
| `unix://` | localhost | 拒绝（缺 socket 路径） | 写 `unix:///path/to/redis.sock` |

#### query 参数：不再一律拒绝

redis-py 认识的连接参数现在会**转换后透传**给存储客户端，值与队列侧
`Redis.from_url` 一致（同一个 URL 不再有两套连接参数）。透传名单
（`factory._QUERY_PARAMS`）是 **14 个名字**：`socket_timeout`、
`socket_connect_timeout`、`socket_keepalive`、`socket_read_size`、
`health_check_interval`、`protocol`、`legacy_responses`、`retry_on_error`、
`retry_on_timeout`、`max_connections`、`ssl_check_hostname`、`ssl_min_version`、
`ssl_include_verify_flags`、`ssl_exclude_verify_flags`；另有 **`db`**（本模块单独
处理，优先级高于路径）与 **`timeout`**（具名拒绝，见下）——三者合计 16 个，与
`redis.connection.URL_QUERY_ARGUMENT_PARSERS` 逐名对齐（有防漂移测试）。

- `?socket_timeout=5&health_check_interval=30`（生产常见形态）现在**能启动**；
  取值非法（如 `?socket_timeout=abc`）才具名拒绝。
- redis-py **不认识**的名字（如 `?foo=bar`）不再判死：记一条掩码 warning 后忽略。
  它们无法透传（`redis.Redis.__init__` 没有 `**kwargs`）；队列侧也只是把未知名字
  塞进 `connection_kwargs`、第一次连接同样 `TypeError`，所以「忽略」不比队列侧更差，
  但不会再炸启动。
- **仍是与 `parse_url` 的差异**：`?timeout=3` 被具名拒绝。`parse_url` 接受它，但
  `AbstractConnection` 没有 `timeout` 形参——`Redis.from_url("...?timeout=3")` 能构造
  客户端、第一次连接就 `TypeError`。请改用 `?socket_timeout=3`。
- 同样地，以下形态 `parse_url` 也接受、但本解析器仍然拒绝（这是 #88 的修复目标本身，
  不是遗漏）：空目标 `redis://`、缺主机名 `redis:///3`、端口 0、非法 db 路径
  `redis://host/foo`、fragment、`unix://` 缺 socket 路径、`?db=-1`。

#### 错误消息里的 URL 一律掩码

`RedisUrlError` 的消息会被 `flow_worker` / `event_filter` 直接写进生产日志
（后者还带 `exc_info=True`）。所有拒绝分支回显的 URL 现在都经
`factory._mask_url` 渲染：`redis://user:***@host:6379/0`——密码永不出现，
用户名保留；`?socket_timeout=abc` 这类底层 cause 也先洗一遍。

掩码是**结构化**的（镜像 `urlsplit`，不是正则），而且**不依赖输入是否合法**
（r3 只在字面 `://` 的 authority 里找 userinfo，单斜杠与「凭据在 scheme 前」的
形态会因为认不出而**原样回显**——那是 r3 的回归，已经修掉）：

- 锚点是「全串最后一个**凭据形状**的 `@`」：所在段（`/` 分段）里要有 `:`，且该
  `:` 必须在该段首个 `?`/`#` 之前。于是 `redis:/user:pw@host`、`REDIS:/user:pw@host`、
  `user:pw@redis://host`、`redis:///user:pw@host`（空 authority 多写斜杠）、
  `redis://user:p?w@host` 都能掩住；userinfo 再按**第一个** `:` 分用户/口令，
  口令整体替换为 `***`。
- `path`/`query` 里的 `@` 不会被误当 userinfo：`redis://host:6379/0@x`、
  `redis://host:6379/0?ref=a@b` 原样保留（区分方式是「`@` 所在段里没有 `:`」）。
- **兜底重洗**：结构层识别到的口令还会以原文 / percent-encoded / decoded 三种形态，
  在 userinfo 段与 host 之后的 path/query/fragment 里再洗一遍——口令重复写在
  `?socket_timeout=PwX` 或 `/PwX` 里也活不下来。`?db=<口令>`、`/<口令>`、
  `?<口令>=1` 这些**不是 URL 的消息字段**同样先洗后回显。scheme 与 host **不**参与
  替换（它们是结构性诊断信息，也可能恰好是口令的子串——无差别全局替换是假红）。
- **认不出 userinfo 时 fail-closed**：`@` 之前认不出口令的形态，userinfo 整体抹成
  `***`（如 `redis://user@host:6379` → `redis://***@host:6379`），host/port/db 保留
  以便诊断。
- 拒绝分支的 `raise … from exc` 改成 `raise … from <洗过的同类型替身>`：`__cause__`
  是 `traceback` / `logger.error(..., exc_info=True)` **独立渲染的对象**，只把文案洗进
  `RedisUrlError` 的消息并不够。

r5 补的四条（`#88` 独立验证 FAIL 的规则级反例 + `#158` 交回的反例）：

- **全局层（R1）**：上面三个结构档都要求凭据形状的 `@` 落在**同一个 `/` 段**内——
  口令里含 `/` 时（`redis://user:p/w@host`）`/` 先把段截断，三档全部落空，
  `_mask_url` 走早退**原样返回**。现在补一档「**非端口**冒号 + 它之后**最后一个**
  `@`」的全局跨度：`redis://user:p/w@host` → `redis://user:***@host`、
  `redis://user:p/w@host:6379/0` → `redis://user:***@host:6379/0`。
- **兜底层不再依赖 userinfo（R2）**：`_passwords` / `_scrub` 此前依赖
  `_locate_credentials`，没有 userinfo 时整层空转——`redis://host:6379/0?db=Pr0d:x@y`
  在 r2 掩住了、r3/r4 **原样返回**（回归）。现在 query 值 / path 里任何凭据形状跨度
  都洗：`?db=Pr0d:***@y`。
  **更正 r4 的说法**：r4 称「回归集 994 → 0、残留全部是口令自身含 `/`/`?`/`#` 的
  畸形 URL」。独立验证者用另一套生成器给出回归集 **1135 / 残留 37**（全是
  `redis://host:6379/0?u=X:Y@x` 这类 **query 值位置**），另有 437 个 `?u=…` 位置、
  193 个 username 位置的泄露，其口令**不含** `/`/`?`/`#` 任何一个。两个数字的差异
  来自生成器的形态网格与接地方式不同（本单复算用 `parse_url` 接地 + 字面接地两套，
  见 `tests/unit/test_redis_url_parse.py::LITERAL_GROUNDED`），不是「收敛」。
- **没有 `@` 也要洗（C）**：`redis://user:sup3rsecret127.0.0.1:679/0`（`@` 写漏/被抹）
  在 redis-py 里 userinfo 位为空，但 `X:Y` 仍是凭据样文本——现在渲染成
  `redis://user:***:679/0`（host/port/db 保留）。
- **`@` 前没有凭据冒号时 fail-closed（B）**：`:` 被写成 `/`/`?`/`#` 的
  `redis://user/sup3rsecret@host` 不再原样返回（→ `redis://user/***@host`）。
  已验证的 keep 逐字不动：`redis://host:6379/0@x`、`redis://host/0?ref=a@b`、
  `redis://host:6379/0?db=1@2`、`redis://host/0?x=1:2@y`。
- **`_mask_url` 是 total**：任意 `str` 都不抛——`os.environ` 的 surrogateescape 会把
  非 UTF-8 字节解码成**孤立代理字符**，`quote()` 的 utf-8 编码随即抛
  `UnicodeEncodeError`（`#158` 的两个新调用点因此启动期硬崩，而同输入 redis-py 的
  `from_url` 本身是接受的）。现在这类形态按 WTF-8 字节渲染（只多掩不少掩）。
- **`__context__` 也摘干净（R3）**：`raise … from _sanitized_cause(...)` 只置
  `__cause__`；`__context__` 仍是那个 `str()` 带明文口令的**原始**异常，任何走
  `__context__` 的消费者都能读到。解析器现在在异常离开前把 `__context__` 置 `None`
  （`__cause__` 替身链与消息原样保留）。

r6 改的是**结构**，不是再补两个特例（`#88` 独立验证第 3 次同类复发：r3 只锚 authority、
r4 的兜底按 userinfo 有无分档、r5 拿 `rfind("://")` 当锚点且只看第一个候选冒号）：

- **枚举全部跨度（r6）**：`_credential_spans` 一次枚举**每一个**凭据候选跨度——
  冒号候选（**非端口**冒号 + 配对 `@`）、没有 `@` 的凭据 token、`@` 前没有候选冒号的
  fail-closed token、no-scheme 粗档、authority 认不出冒号时的 fail-closed。`_mask_url`
  把它们的口令段**全部**掩掉，不再存在「the credential span」。r5 的两个规则级反例因此
  在结构上没有立足点：

  | 反例 | r5（泄露） | r6 |
  |---|---|---|
  | 口令含 `://`（R1） | `redis://user:pa55word://***@host` | `redis://user:***@host` |
  | 有 userinfo 时的第二条跨度（R2） | 消息字段原样带 `'Acct:Secret1@x'` | `'Acct:***@x'` |

  五条 `@` 跨度/三条非 `@` 跨度的**候选边界**（也是本轮的公开契约）：

  * 候选冒号 = depth 0（不在 IPv6 `[...]` 里）+ 非端口冒号（`_is_port_colon`）+
    不是 scheme 冒号。scheme 冒号取「**第一个** `://` 的冒号」，或没有 `://` 时
    「串首 `scheme:` 且后面紧跟 `/`」的那个——**不再** `rfind("://")`（R1 的根因）；
  * 配对 `@`：authority 里取「首个 `/` 之前最后一个 `@`」（R2 的边界），authority 之外
    取「该冒号之后最后一个 `@`」；段内没有 `@` 或那个 `@` 让口令为空时回退到最后一个
    `@`（口令含 `/` 的畸形态，只多掩不少掩）；
  * **端口判据分区域**：authority 里数字段只在 `/` 或串尾结束才算端口（
    `redis://user:9?0b@host`、`redis://user:9#Z@host` 的 `?`/`#` 是口令字符）；
    authority 之外维持 r4 已验证的 `/?#@`；
  * **边界**（本轮已知的、与 HEAD 同形的盲区，不在候选跨度内，故不被本层覆盖）：
    `?db=<没有冒号的普通值>`、`?<没有冒号的查询名>=1`、`?u=X:911/bzx`（数字打头 + `/`
    与 `host:port/db` 局部同形）、`?db=S3cr3t/1:2` 尾部的 `:digits`。
- **内部一致性不变量（r6，新增）**：`_mask_url` 与 `_passwords` **只**经同一个
  `_credential_spans`（`_redaction_forms` 是它的 needle 版），所以
  **`_passwords(s) != ()` ⟹ `_mask_url(s)` 不含其中任何形态、且 `_mask_url(s) != s`**；
  反之 **`_mask_url(s) != s` ⟹ `_passwords(s) != ()`**（没识别出口令就不许改串）。
  r5 的 R1（`_passwords` 取到 `pa55word` 而 `_mask_url` 掩不住）与 R2
  （`_mask_url` 掩了、`_passwords` 取不到）都是这条不变量的违反。它由
  `tests/unit/test_redis_url_parse.py::TestEnumerateAllSpans::test_consistency_invariant_on_fuzz_corpus`
  钉住。
- **生成式性质测试取代形态表的角色（r6）**：形态表是「枚举我以为的危险形状」，而
  r3/r4/r5 三次漏掉的都是表里**没有**的形状。现在
  `TestMaskerGenerativeProperty::test_generated_secret_in_every_candidate_position_never_leaks`
  把随机口令（含 `@ : / ? # %`、空白、`://`）注入**每一个候选位置**（userinfo /
  empty-user / slash-in-password / query 值 / path / fragment / 无 `@` / 多跨度 /
  用户名==口令 / 单斜杠），对**每个**形状断言口令原文与**所有 ≥3 字符前缀/后缀**都不在
  五个渲染面（`_mask_url` / `str` / `repr` / `traceback` / 真 `logger.error(…, exc_info=True)`）
  上。**3000 个形状，实测 0.75s**（`pytest -k TestMaskerGenerativeProperty`）；
  形态表（`LITERAL_GROUNDED`）保留为回归清单。
- **`_secret_forms` 补三版书写形态（r6）**：`urlsplit` 解析前会剥掉 URL 里的
  `\t`/`\n`/`\r`、`parts.port` 的报错文案是 `%r`（非 ASCII 变转义写法），所以除
  原文/percent-decoded/percent-encoded 之外再补「剥空白」与「`unicode_escape`」两版；
  并按 `: @ ? # & =` 切开取**头截与尾截**（口令本身含分隔符时，消息字段里出现的是
  切开后的那一截）。只多洗不少洗。
- **`_quoted_forms` 的代理字符兜底更正（r6）**：`surrogateescape` 能编回的
  （`U+DC80`-`U+DCFF`）现在按**原始字节**渲染，`_quoted_forms('\udcff')` = `%FF`
  （on-the-wire 形态）；只有编不回的孤立代理（`U+D800` 之类）才退回 `surrogatepass`
  的 WTF-8 字节（`%ED%A0%80`）。r5 的注释把 WTF-8 说成 on-the-wire，是错的（无泄露，
  但注释不真）。
- **`#158` 的既存 sink（本轮**不**修，只登记）**：`plaita/server/services/__main__.py:55`
  的 `logger.info("连接 Redis: %s", redis_url)` 在 INFO 级打印**裸 URL**（含口令）——
  它在正常的、**合法**配置路径上就会被调用，不属于 `_mask_url` 能覆盖的「拒绝文案」面。
  该 sink 归 `#158`，在本 diff 之外；本单只保证 `plaita/server/factory.py` 渲染的文案
  与 `__cause__`/`__context__` 链不泄。

### r7：掩码的验收判据从「自洽」改成「完备」（oracle 接地）

r6 的 `_passwords(s) != () ⟹ _mask_url(s)` 不含那些形态 **与** `_mask_url(s) != s`
两条都成立（`_passwords` 与 `_mask_url` 共用 `_credential_spans` 一个枚举），可它仍然
泄露。独立验证者给出的规则级反例（本单第四次同类复发）：

| 形态 | 三个 oracle 报出的口令 | r5 | r6（泄露） | r7 |
|---|---|---|---|---|
| `redis://zB0@@cB9cB@@Ccc:zB0@@cB9cB@@Ccc@host:6379/0` | `zB0@@cB9cB@@Ccc` | `redis://***:***@host:6379/0` | `redis://***@@***@@Ccc:***@host:6379/0` | `redis://***@host:6379/0` |
| `redis://Q7w@E4r@T5y:Q7w@E4r@T5y@host:6379/0` | `Q7w@E4r@T5y` | `redis://***:***@host:6379/0` | `redis://***@***@T5y:***@host:6379/0` | `redis://***@host:6379/0` |

（三个 oracle = `redis.connection.parse_url`（队列侧真正拿去连的）、
`urllib.parse.urlsplit`、本仓 `_parse_redis_url`，三者逐字一致。）

- **根因（结构）**：`_VALUE_DELIMITERS` 含 `@`，`_at_colon_spans` 的 `start` 从候选
  冒号往左退到用户段**最后一个 `@`** 就停，于是那个 `@` 之后的 token（`Ccc`）既不在
  任何 `[mask_from, stop)` 区间里、也不在 `_secret_forms` 的形态集里（它只按**第一个**
  分隔符切头/尾截）——两头都不覆盖。**r7 的处置不是加特例**：新增
  `_userinfo_split_is_provable`——authority 里候选冒号**之前还有 `@`** ⇒ 用户/口令分界
  **不可证** ⇒ 整段 userinfo fail-closed（`[auth_start, 配对 @)` 全掩）。
  `user==pw`、口令含 `@ : / ? # %` 与空白的**任何**形态都走这一条路。分界**可证**的
  形态逐字不变（`redis://user:p@ssword@redis.internal:0` → `redis://user:***@redis.internal:0`）。
- **验收判据换成完备性（oracle 接地）**：性质测试的 needle **不再来自注入文本**，而是
  来自 oracle 报出的口令——对每个形状断言它的任何 **≥3 前缀/后缀**（主判据）与**所有
  ≥3 子串**（更严）都不出现在五个渲染面（`_mask_url` / `str` / `repr` / `traceback`
  / 真 `logger.error(…, exc_info=True)`）与 `__cause__` 替身上。
  `TestMaskerGenerativeProperty` 的语料 9000 个形状、13 个候选槽、**每个槽同一个全
  字母表**；运行时见 `pytest -k TestMaskerGenerativeProperty`（约 8s，含五次渲染面）。
  **自洽不蕴含完备**——r6 就是反例，所以两条判据都留，完备性是主验收。
- **不变量合格化**：无限定版本是**假**的。验证者的 432 个反例全是「口令形态与**有意
  保留的结构 token** 撞车」，例如 `redis://user:Q7w:9:6379@host:6379/0` 的 `_passwords`
  含 `6379` 而 host 端口 `6379` 本来就该留下。现在的陈述是：
  `_passwords(s) != ()` ⟹ `_mask_url(s)` 不含其中任何形态（**与 scheme / 可证用户名 /
  host / port / db / 非 authority 跨度的用户 token 偶然重合的除外**）、且 `!= s`；
  反之 `_mask_url(s) != s` ⟹ `_passwords(s) != ()`。
- **语料逐槽记账**：r6 的 6000 行语料里 `user==pw` 槽用
  `safe_token(allow_at=False, allow_colon=False)`，545 个形状**没有一个含 `@`**——洞正是
  被这个字母表收窄藏住的。r7 删掉全部 `allow_at` / `allow_colon` / `safe_token` 开关，
  13 个候选槽（userinfo user / password、`user==pw`、query 值 / 名、无 `@` 的 `?u=X:`、
  path、fragment、no-scheme、single-slash、empty-user、multi-span、slash-in-password）
  **每个**都用全字母表；`test_corpus_per_slot_counts_and_alphabet` 逐槽断言样本数与
  `@ : / ? # % ` + `\t` + `\xa0` 的覆盖，任何收窄都会红。
- **三处默认拒绝（同单，不是新工作）**：`?db=<非法值>`、`?<未知名>=1`、fragment /
  非法 query 取值的**回显**原本原样带攻击者可控文本（r5/r6 同形）。现在消息字段、
  回显的 URL 与 `__cause__` 里都抹成 `***`（保留参数名 `db` / `socket_timeout` 与固定
  措辞）。对应 `LITERAL_GROUNDED` 新增两行（`grounding="message-field"`），它们在
  r5/r6 上是红的。
- **已知边界（r7 语料暴露、r5/r6 同形、不在本单的修复范围）**：
  1. `?db=<没有冒号的普通值>`；2. `?<没有冒号的查询名>=1`；3. `?u=X:911/bzx`（数字
  打头 + `/` 与 `host:port/db` 局部同形）；4. `?db=S3cr3t/1:2` 尾部的 `:digits`；
  **5.** r6 列表漏掉的第 5 条——authority 用户段里**最后一个 `@` 之后的 token**
  （`redis://zB0@@…@@Ccc:…` 的 `Ccc`）——**r7 已修**，见上；
  **6.** 注入文本含 `#` 时尾部落在 fragment 里（`redis://user:CcAax??axzA@@x00/x@host`
  的 `x00`、`redis://host/0/X:0190??…@y` 的 `0190??`）：这截不在任何凭据跨度内，
  掩码器按契约保留，**r5 同样泄露**（不是 r7 回归），故 `slash-in-password` /
  `query-value-no-at` / `path` 三个槽在 r7 只做 oracle 断言（逐槽计数可见其
  `oracle_grounded`，`oracle_grounded=0` 的槽即「oracle 说不出话」的槽）。
- **计数更正（r6 报告/提交信息）**：r6 随掩码改动重写的 r5 变异定义是 **7** 条（不是
  6 条）：`88-credentials-fall-back-to-verbatim`、`88-masker-gives-up-without-userinfo-colon`、
  `88-masker-not-global-fail-closed`、`88-masker-not-total-on-surrogates`、
  `88-masker-reverted-to-r2-regex`、`88-masker-skips-residual-redaction`、
  `88-passwords-skip-no-at-token`；随改动变化的变异文件共 **14** 个（7 重写 + 7 新增），
  不是 13。
- **r7 变异**：定义总数 42（31 条由普通 `FAILED` 行杀死、11 条只由 `SUBFAILED(` 行
  杀死，与 r6 的 28 + 11 相比只是分母变大）。新增 3 条单腿：`88-r7-userinfo-split-always-provable`
  （关掉分界可证性 → `Ccc` 重新泄露，2 条 `FAILED`）、`88-r7-db-value-echoed`、
  `88-r7-unknown-name-echoed`（各 1 条 `FAILED`）。r6 三条**夸大**的 `expect_fail`
  已修正——它们的声明杀手现在真的会红：`88-r6-scheme-anchor-is-last`（新断言「scheme
  分隔符 `://` 必须保留」）、`88-r6-masker-only-first-span`（`test_consistency_invariant_on_fuzz_corpus`
  现在同时钉合格化不变量与完备性）、`88-masker-not-global-fail-closed`
  （`GLOBAL_FAIL_CLOSED_URLS` 改成 `(url, secret)` 并补 `redis:/user:911/z@host`）。

**迁移**：若有脚本/告警解析这些消息文本去比对原始 URL，请改比对掩码形态；
仅写用户名不写口令的 URL 也会显示成 `***@host`。

### r8：掩码机制改为**对分隔符闭合** + 非 authority 值位置 default-deny（plaita#88 第五次复发）

r7 的「oracle 接地的完备性」判据本身是对的，但**机制**还开着一个口子：同一缺陷类
第五次复发（r3 只认 authority → r4 单斜杠 → r5 `rfind("://")` 锚点 → r6 漏用户名
token → r7 漏非 authority 尾巴）。规则级反例：

```
redis://user:AA@BB@SuperSecretTail@host:6379/0?u=X:AA@BB@SuperSecretTail
  三个 oracle（redis.connections.parse_url / urlsplit / _parse_redis_url）都报
  AA@BB@SuperSecretTail；r5 == r6 == r7 输出
  redis://user:***@host:6379/0?u=X:***@SuperSecretTail
  ⇒ 105 个 ≥3 子串 × 3 oracle = 315 个非豁免片段存活（最长 16 字符）
```

**根因（结构）**：`_secret_forms` 只按每个分隔符的**第一个**出现位置切头/尾；
`_span_at` 的非 authority 分支停在**最后一个 `@`**。于是口令第二个 `@` 之后的尾巴
既不在任何 `[mask_from, stop)` 区间里、也不在 needle 集里。**r8 不再加模板，改闭合**：

1. **分隔符闭包**（`_secret_forms` / `_delimiter_pieces`）：对分隔符集的**每一个**
   出现位置切头/尾，并把每个片段**重新入队**到不动点；分隔符集补上了此前缺的
   `/`、`%` 与空白（`_SECRET_SPLITTERS`）。闭合性质：对任意返回形态 `f`、任意分隔符
   `sep`、`f` 里 `sep` 的任意出现位置 `k`，**非空**的 `f[:k]` 与 `f[k+1:]` 都仍在
   形态集里（`test_secret_forms_closed_under_resplitting`，逐形态断言）。
   纯分隔符片段（`@@@A` 切出的 `@`/`@@`）仍在形态集里（闭包要求），但**渲染层**
   不拿它们当 needle（`_is_structural_form`），否则结构性 `@` 会被抹成 `******`。
2. **非 authority 值位置 default-deny**（`_span_at`、`_no_at_colon_spans`、
   `_query_pair_spans`、`_path_segment_spans`、fragment 分支）：authority 之外**没有
   host**——query 值 / path 段 / fragment 里 `@` 之后的文本同样是凭据材料。判据：
   配对 `@` 之后的尾巴**长于 1 个字符**就掩到**值 token 末尾**（`&`/`#`/串尾；
   `_value_token_stop`）。单字符尾（`@x`/`@y`/`@b`）是 r2/r4 已验证的 keep，逐字不变；
   未声明 query 名、裸取值、裸 path 段一律按凭据处理（`?socket_timeout=<secret>`、
   `?<secret>=1`、`redis://host/0/<secret>` 在 r7 上 `_mask_url` **原样返回、零覆盖**）。
   query/fragment 段用**紧**判据（尾巴量到段末；`@` 之后即便以 `/`/`:` 开头也算凭据），
   path 段用松判据（r5 B 的 `#Tr0ub4dor@host:6379/0` 形态逐字保留）。
3. **默认拒绝字面量与掩码进同一趟**（`_mask_url(value, *, literals=())` +
   `_literal_ranges`，删除 `_scrub_literal`）：r7 的「先 `str.replace` 再 `_mask_url`」
   会把凭据**切碎**再掩，残余认不出来（`redis://user:xCb0B??AxB#xcC@host…` 的 fragment
   消息留下 `??AxB`；`?u=X:AA@BB@SuperSecretTail` 的未知名 WARNING 留下
   `?0xzzAC?…` 型尾巴）。字面量现在与跨度一样在**原文**上算区间、进同一个 `m` 态，
   没有中间态。`literals` 是**关键字**参数（默认 `()`）：`_mask_url(x)` 单参调用逐字
   不变，`#158`/`#159` 按名字 import 这个函数因此不受影响。

**完备性陈述（升级版）**：完备性 = **对分隔符的闭包**（形态集对 `:`/`@`/`?`/`#`/
`&`/`=`/`/`/`%`/空白闭合）**加上**「每一个可能承载凭据的非 authority 值位置都被
覆盖」（不可证良性即整值/整段掩）。oracle 接地的判据不变：口令的任何 ≥3 前缀/后缀
与**所有** ≥3 子串都不出现在五个渲染面（`_mask_url` / `str` / `repr` /
`traceback` / 真 `logger.error(…, exc_info=True)`）与 `__cause__` 替身上。

**语料与记账（r8 要求 1b/2a/3）**：
- 槽位 13 → **24**（新增 11 个 r8 家族槽：query 值/path 段/fragment 逐字出现、
  `?db=`/`?u=`/`?socket_timeout=` 三种位置、`Acct:`/`X:` 前缀、凭据两次且**不带**
  收尾 `@x`），每个槽仍用**同一个全字母表**（并补上多字节 `ä`/`中` 与
  surrogateescape 字节 `\udcff`）。
- **多种子扫掠**：20 个种子（含验证者用过的 **119** 与 **20261015**）× 480 形状，
  逐槽断言「oracle 接地或字面接地」并有「**未断言形状数 == 0**」断言；
  主语料 9000 形状在种子 88 上跑全子串判据。逐槽断言表（n / oracle / literal /
  needles / 结构性豁免 / 数字豁免 / 泄露 / 未断言）在 `-s` 下打印。
- **r8 家族覆盖**：`_R8_FAMILIES` 的 5 个家族（query-value-verbatim /
  query-value-prefixed / credential-twice-no-closing-at / fragment-verbatim /
  path-verbatim）各自断言「≥1 条 oracle 接地样本」。

**豁免面（要求 3）：从「碰巧在白名单里」改成「位置级可证结构性」**。
`_neutralize` 把**结构 token 的出现**替换成等长占位符，然后才查 needle——
`@Za` / `@@9` 这类「分隔符 + host 前缀」的跨界撞车不再被误判为泄露，
而 `Acc` 落在可证用户名 `Acct:` 里的整条豁免也不再发生（真漏若共享该子串仍会红）。
验证者的 **seed 119** 反例 `redis://user:aCab@@acC010@@acayaaa/x@host` 因此被证明是
**instrument 假红**：三个 oracle 都报 `aCab@@acC010@`，r7 输出
`redis://user:***@acayaaa/***@host` 里唯一「存活」的 needle 是 `@ac`——它是跨度的
`@` + host 前缀 `acayaaa`（`_preserved_literals` 有 `acayaaa`，没有 `@ac`）。
该 shape 已作为**固定回归行**（`test_r8_seed_119_structural_collision_regression`）；
真漏（`@SuperSecretTail`：`SuperSecretTail` 不是任何结构 token）照旧红。
`_INTERPRETER_WORDING` 另收解释器/本项目固定措辞（`base 10:` 含 `10:`、
scheme 列表含 `://`、链式异常样板句），逐条计数。

**已登记的已知边界（r8 起有断言、计数公布、断言不增长）**：
1. `slash-in-password`（数字打头 + `/`：`redis://user:1900/x@host`）与
   `host:port/db` 局部同形，r4 已验证的端口判据按 keep 取——前缀留在消息面；
2. **纯数字 needle**（`?db=Acct:090@x` 原样保留 = r4 的 `?db=1@2` 契约）；
3. 未声明 query **名**里 `:` 之前那截按 r5 C 的 user-token 契约保留
   （`?u=X:Secret1` → `?u=X:***` 逐字已验证）——该槽的字面 needle 因此用固定的
   裸未知名 `Pr0d2024x`；
4. query 值里 `?`/`#`/`/` 造成的**跨字段**溢出（r7 已知边界第 6 条的剩余部分）
   只在 oracle 说不出话、且没有任何凭据跨度可重洗时成立。
这四条都在 `_R8_KNOWN_BOUNDARY_LEAKS` 里按槽登记上限，测试断言**不超过**上限。

**顺带修的 portability 缺陷（同文件）**：`_quoted_forms` 的 docstring 里写了字面
`'\udcff'`——非 raw docstring 会把它解析成**真的孤立代理字符**，CPython **3.13**
在**编译期**就把它编码成 UTF-8，于是 `import plaita.server.factory` 直接
`UnicodeEncodeError: 'utf-8' codec can't encode character '\udcff' …`
（实测 **3.13.15**；3.11/3.12 不报，故 `6a9cdba8` 起潜伏）。r8 把源码里的裸代理
转义改成转义写法，并加了一条**版本无关**回归
（`TestSourcePortability`：源文本无裸代理转义 + 源文件可 `compile`）。

**精度处置**：重洗层不再施加**子 3 字符**的 needle（`?u=…` 的 1 字符名字、
`=` 切出的残片）——它们满足不了完备性判据（≥3），误伤面却很大（实测会把
userinfo 的 `u` 洗成 `redis://***ser:***@…`）。掩码段本身仍覆盖短口令，`_passwords`
仍返回它们（消息字段的 `_scrub` 用得到）。

**精度代价（本轮明确登记，不牺牲安全）**：`redis://@@host` → `redis://******host`、
`redis://@:pw@host` → `redis://******host`（r5/r6 是 `redis://@:***@host`）——
闭包把 `@@`/`@` 当结构形态在**重洗层**跳过，但掩码段仍是整段 `***`。

**导入契约**：`_mask_url` / `_parse_redis_url` / `RedisUrlError` / `RedisTarget`
**不改名**；`_mask_url` 只**新增**关键字参数 `literals=`（`#158`/`#159` 按名字 import
它，单参调用逐字不变）。掩码契约的调用方在别处按名字 import，删除/改名会立刻炸。

**变异**：定义总数 42 → **48**（新增 6 条 r8 单腿：闭包不动点、非 authority 尾巴、
path 裸 token default-deny、query 段 default-deny、fragment default-deny、no-at 值
token 延长；另有 6 条既有定义随 `_scrub_literal` 并入 `_mask_url` 的重写——
`88-cause-object-keeps-raw-exception`、`88-db-query-ignored`、`88-fragment-accepted`、
`88-r7-db-value-echoed`、`88-r7-unknown-name-echoed`、
`88-whole-parser-reverted-to-original-regex`，其中最后一条的 `old` 是整段解析体）。
计数见提交信息与本次报告。

**r8 的相对读数（本单自带生成器，seed 20261015 / 6000 形状 / 5 个模板 A/F/G/H/I，
逐模板 1200；与 committed instrument 同一套豁免规则）**见提交信息与本轮报告；
要点：模板 A（`user==pw`）r7 已 21/31 → r8 **0/0**；F/G/H/I（凭据逐字作为
query 值 / path 段 / fragment / 两次且无收尾 `@x`）r7 的 155/160、41/47、21/25、
154/160 → r8 **0/0**。

**运行时**：`tests/unit/test_redis_url_parse.py` 从 r7 的约 12s 升到约 10 分钟——
24 槽 × 9000 形状的全子串判据 + 20 种子扫掠 + 闭包（形态集中位数 32、长口令可达
数千）都在这里。生产路径不受影响（一条普通 URL 的 `_mask_url` 仍是亚毫秒级）；
`_secret_forms_cached` / `_delimiter_pieces` / `_encoding_forms` / `_quoted_forms`
都带进程内 LRU。

### DSL：`@childflow("<text>")` 的位置实参由 `desc` 变成子流程 `flow_id`（plaita#145）

`childflow()` 此前签名是 `childflow(desc=None)`，位置实参只能是说明文字；现在
与 `@flow("<id>")` 对齐为 `childflow(flow_id=None, *, desc=None)`——**位置实参是
子流程 id，`desc` 改为纯关键字参数**。存量代码若把说明文字写成位置实参，
升级后它会被当成 `flow_id`（不再进 IR 的 `desc`），且**语义会变**：子流程的
`$FLOW_ID` / `$GLOBAL.flow_id` 从"回落到父流程 id"变成该文字。

- **受影响**：`@childflow("说明文字")` 这类**位置实参**写法。实测差异
  （base = 本改动前）：
  - 真实装饰器路径：base 写入 IR `desc="说明文字"`、`flow_id=None`；现在写入
    `flow_id="说明文字"`、无 `desc`。
  - 源码字符串路径（`flow_from_source` / `compile_source`）：base **两者都不写**
    （`_extract_deco_opts` 取到位置实参后丢弃，`desc` 与 `flow_id` 都是 `None`）；
    现在写入 `flow_id="说明文字"`——即从"静默丢弃"变为"生效"，子流程的
    `$FLOW_ID` 因此从父 id 变成该文字。
- **不受影响**：`@childflow()` / `@childflow(desc="说明")` / `@childflow(desc=…)`
  完全不变（行为逐字一致；历史上 `desc` 只有关键字一种写法能可靠工作，本次只是
  把"位置实参"这个空位让给 `flow_id`）。
- **迁移**：把位置实参改成关键字，并（可选）补一个 id：

      # 旧（升级后 "计算折扣" 会变成子流程 id，不再进 desc）
      @childflow("计算折扣")
      def discount(INPUT): ...

      # 新：说明回关键字；需要子流程标识时再给位置 id
      @childflow(desc="计算折扣")
      def discount(INPUT): ...

      # 或者两者都要
      @childflow("discount", desc="计算折扣")
      def discount(INPUT): ...

- **顺带提醒（不是 break，是新能力）**：`flow_id` **不参与节点寻址**（引擎只在
  告警文案里引用它），但**会被写进执行上下文**（`plaita/core/context.py:314` →
  `$GLOBAL.flow_id`；`context.py:331` / `plaita/core/state.py:632` → `$FLOW_ID`，
  别名 `$FLOW`）。因此**显式给 id 会改变子流程内 `$FLOW_ID` 的取值**：不给
  （或旧写法）时它经父上下文回落得到父流程 id，给了就是该 id——按 `$FLOW_ID`
  分支的表达式会走到不同分支。不给 id **不回落函数名**（内联子流程没有可用
  函数名，回落会让同名子流程撞 id）；非 `str` 位置实参（如 `@childflow(123)`）
  编译期抛 `_CodeflowError`。详见
  `plaita-ai/plaita_ai/skills/flow-coder/references/authoring-spec.md` §2.7。

### 表达式：数学/容器算子加「结果规模守卫」，超限硬失败（plaita#68）

表达式引擎对「小输入、巨大输出」的多态放大此前只有 `$F.pow` 有守卫
（`_POW_MAX_EXPONENT = 1_000_000`）。同族的 `$F.mul('A', 1000000000)`、
`$F.mul(1000000000, 'A')`（`@flow` 源码里写 `"A" * 10**9` / `10**9 * "A"` 都会
编译成同一条）、`$F.replace`、字符串 `$F.mod`、`$F.join` 等几十字节表达式能在
worker **自身进程**里一次分配 0.1–1GB；flow 级 timeout、HTTP 响应上限、沙箱
`RLIMIT_AS` 都拦不到（分配发生在求值器内部、早于任何可中断点）。

**判定规则**（识别放大，而不是限制绝对大小）：

```
allowed = min( max(floor, factor * input_size), hard )
```

| 旋钮 | 默认 | 环境变量 |
|------|------|----------|
| floor（小输入时的下限） | `100_000`（字符/字节/元素） | `PLAITA_EXPR_MAX_RESULT_SIZE` |
| factor（允许的放大倍数） | `4` | `PLAITA_EXPR_GROWTH_FACTOR` |
| hard（单次结果硬上限） | `100_000_000` | `PLAITA_EXPR_MAX_RESULT_HARD` |

环境变量在模块导入时读取一次，改后需重启进程；非法/非正值静默回退默认。

**输入规模只信权威协议，且按内容量**：`_content_size` 对 str/bytes/bytearray 取长度、真实缓冲取 `memoryview(x).nbytes`、映射/容器**由我们自己枚举**（不做 `max(前 N 项)×n` 这类近似；自引用只计一次；嵌套深度预算 `_DATA_WALK_MAX_DEPTH = 10^6`（**声明值**；`_content_size` 是递归实现，走查期间把解释器递归上限临时抬到 6000 帧 ⇒ **有效预算 ≈2000 层**，超出的病态嵌套 fail-closed 并给出 `$F.* nested value exceeds the interpreter recursion budget`；超过声明预算时消息为 `$F.* nested value exceeds the data-verification budget: depth <N> (limit 1000000 levels)`）；扫描精确到项，只有累计到 `hard` 才提前返回）；**不信**任何对象自报的 `itemsize`/`__len__`（说谎对象会把额度抬高），其它对象按 0 计。`%s/%r/%a` 的渲染投影另走 `_render_size`，带自定义 `__str__`/`__repr__` 的对象无法界定 ⇒ 预检保守拒绝。

**判定是「调用点级」的，不是「函数级」的**：每个带守卫的算子在调用 Python 算子
之前算出**这次调用**的结果上界——算不出来时（类型不匹配 / 无法枚举）该调用注定
抛**原生** `TypeError`/`KeyError`/`ValueError`，而不是"跳过守卫"。求值侧探针也
**不再按函数白名单跳过**，每次 `$F.*` 调用都按同一条比值规则复核一遍（输入规模用
**内容量**：字符串长度、容器内容之和、int 十进制位数上界——不是元素个数/参数个数）。

**分配之前拦截（prevention）**——这些算子在调用 Python 算子前预检：

| 算子 | 拦截条件 |
|------|----------|
| `mul` | 任意「类型支持 `__mul__` 且有 `__len__`」的对象重复（`seq*int` **与** `int*seq` 双向：str/bytes/bytearray/list/tuple/`array.array`/`deque`）超限；**长度一律走内建祖先 C 槽**（`str.__len__`/`list.__len__`…），实例覆写的 `__len__` 只骗到额度、骗不到 `str.__mul__` 读的真实存储（第十二轮 M1：`LyingLenStr('a'*1000, __len__→1)` × 50000 曾按 1 计费、原生造出 50,000,000 字符）；**没有可读真实存储的内建祖先 ⇒ 直接拒绝**（自定义序列不再按实例 `__len__` 猜） |
| `replace` | `len(s) + 命中数 × (len(new)-len(old))`（空模式按 `len(s)*(len(new)+1)+len(new)`）超限；`bytes`/`bytearray` 的 pattern/replacement 按**任意 buffer 协议对象**判定（`memoryview`/`array`/`mmap`… 与原生语义一致，字节数走真实 buffer 协议）。**长度与命中数走祖先 C 槽**（`str.__len__`/`str.count`…，第十二轮 M1：覆写 `count → 0` 曾让 100,000,000 字符的替换通过）；替换本身走 `str.replace`/`bytes.replace`/`bytearray.replace` 的 C 槽，非 str/bytes 的自定义 `replace` 不可证 ⇒ 拒 |
| `mod` / `concat` / 模板插值 | 渲染类路径（`%s/%r/%a`、`concat`、`{% ... %}`）先过**渲染前闸**（第八轮改为**方案 A：精确类型 + 可证上界**）：只放行**精确内建/标准库类型**（`str`/`bytes`/`bytearray`/`int`/`float`/`bool`/`None`/`list`/`tuple`/`set`/`frozenset`/`dict`/视图/`range`/`memoryview`/内建迭代器/`Decimal`/`datetime`/`date`/`time`/`timedelta`/`UUID`/`PurePath`/`Pattern`/`Enum`/`array`/`deque`/`SimpleNamespace`/默认 repr 的对象；**namedtuple 不在白名单**——它不做整体渲染，见下方「已知边界」），上界按各自规则**可证**（`repr` 转义按 4×、容器含每项 2 字符分隔余量、bytearray 含前缀）；**子类**只有在其 `__str__`/`__repr__` 与安全祖先是**同一原始槽**（未覆写；读 MRO `__dict__`、描述符一律不认）时才按祖先语义处理；**计费与枚举一律走祖先 C 槽**（`str.__len__`/`list.__iter__`/`dict.items`…）——这条对**所有算子**成立（第十一轮 H1 只改了渲染路径；第十二轮 M1 把 `mul`/`replace`/`add`/`extend`/`append`/`join` 的前置检查与预留也改成祖先槽）。实例覆写的 `__iter__`/`__len__`/`count`/`join` 只骗到额度、骗不到原生算子/`str()`/`repr()` 读的真实存储（H1 曾按 16 字节放行 100,004,000 字符）；**没有对应 C 槽可读的值 ⇒ 拒绝**而不是按实例协议猜；可子类化的 stdlib 类型（`PurePath`/`Pattern`/`SimpleNamespace`…）同样只认**精确类型或 dunder 同一性**（D2）。其余**在渲染之前拒绝**，消息带"可用 `str(x)` 放宽"的提示。枚举期间的用户异常一律收口成"不可界定 ⇒ `ValueError`"（D4）。`join`/`sort` 在调用点把不可信任的实参**物化一次**，守卫测量与执行**共用同一次迭代**（D5）。判据只看**真实类型对象**（精确成员/MRO 函数对象比较），**不看** `__module__` 之类可自述的字符串。H1/H2 两个反例（list 子类谎报 `__len__`、`__module__` 伪造）已分别修掉并各有具名测试；第二十一轮起这一族与 `%` 的 `s`/`r`/`a`/bytes 模式 `b` 共用**唯一派发表** `_RENDER_DISPATCH`（见下方「`%` 派发表」），类型层读取改为不可变 `type.__dict__[...]` getsets，元类数据描述符欺骗（`Meta.__mro__`/`Meta.__dict__` 撒谎）不再能骗过归属判定 |
| `mod` | str/bytes 的 `%` **是 printf 格式化**：解析映射键 `%(k)`、flags、宽度（数字或 `*`，含**负宽度取绝对值**）、精度（`.N` / `.*`）与转换符，并按"**分配量**"预检——`'%.90000000f'`、`'%.900000000g'`（结果只有 1 字符却分配 ~1GB）、`'%(k)100000000s'`、`'%100000000s' % {'x': 1}`、`'%*d' % (-1e8, 1)` 都在此拦住。`s`/`r`/`a` 的精度是**截断**（`min(上界, 精度)`，原生 0 额外分配），**不是宽度累加**——
`'%.*s' % (200000, 'x'*500)` 结果只有 500 字符 ⇒ 通过；数值转换的精度才是补零位数
（`max(..., 精度)`）。第十五轮起：`specs` 解析不出来、`*` 宽度/精度解不出（非精确
`int`）、或 `%s/%r/%a` 的值没有可证上界 ⇒ **默认拒绝**（不再有"解析不出来 ⇒ 下界 0 ⇒
放行"的路径）；错误点之前会被原生构造的字段全部计入上界（Q3 doomed-field）。**转换符 × 值类型**不合法（`%d` 配 str/None、`%x` 配 float、`%c` 配长串、bytes 模式 `%s` 配 str…）一律先交给原生抛 `TypeError`。**上限判定不容许空转**（第十三轮 O1）：格式串的宽度/精度本身就是**分配下界**，纯 `str` 操作解析（含未闭合前缀）；`plan` 不可得时若不能证明原生必然抛错，**直接拒绝**而不是放行（旧实现把 `projected` 退化成 0：`'%50000000s%'` 59.7MB、`'%400000000s%'` 476.9MB 之后才 `incomplete format`）。**星号宽度/精度只接受精确 `int`（含子类）并用 `int.__index__` C 槽读**，非 int ⇒ 拒（绝不跑覆写的 `__int__`：实测 `EvilInt` 让上界算成 1、原生按 90,000,000 分配）。**映射取值只读一次**：解析成精确 dict 后**用同一份值**渲染（消除 TOCTOU：有状态映射第一次返回 `'s'`、第二次 150MB 曾绕过 143.1MB），`dict` 及子类走 `dict.get` C 槽（**不触发 `__missing__`**；`defaultdict` 缺键在被拒路径上不再插入默认值）。**映射参数只认可证的映射**（第十二轮 M2 → 第十六轮 S2 收紧）：精确 `ChainMap`/`UserDict` 与 `dict`/`dict` 子类一次性快照后只读一次（`dict.get` C 槽）；`MappingProxyType` 一律拒绝（`copy()` 会跑被包 dict 子类的作者协议，且证明不了被包对象是精确 dict）；**上限判定结构性地无条件执行**（不再「plan 为 None 就整段跳过」），而「原生错误优先」仍是硬契约。第十八/十九/二十一轮补：**唯一判据表** `_RENDER_DISPATCH`（第二十一轮把旧的 `_CONVERSION_DISPATCH` 与渲染分类集合合并成**一张**表：key = 转换符，value = `(kind, slots, bound)`；`_CONVERSION_DISPATCH` 只是它的**派生投影**）驱动 `_conversion_accepts`/`_definitely_rejected`/`_spec_bound`/`_spec_input_size`/`_render_size` 与对抗语料，方向 **fail-closed**（转换符/槽未知 ⇒ 拒绝，绝不判 native）；**归属判定只发生在每行的 `bound` 里**（`_slots_owned_by_base` 用不可变 `_slot_owner`/`_type_namespace` 读真实 MRO），`_STR_SAFE_OWNERS`/`_REPR_SAFE_OWNERS` 已删除。实测事实与测试逐一对照见下方「`%` 派发表」。实测口径：**精确 int/bool/float/Decimal** 与**槽未被覆写的子类**走祖先 C 槽给上界（`int.__index__`/`float.__float__`/`Decimal.adjusted`/`is_nan`/`is_infinite`）；**任一被派发槽被覆写**的 int/float/Decimal 子类（`__int__`/`__index__`/`__float__`/`__trunc__`/`__abs__`/`__eq__`/`__ne__`/`__len__`/`__str__`/`__repr__`/`__bytes__`）⇒ **拒绝 + 具名逃生口**（`int(x)`/`float(x)`/`str(x)`/`bytes(x)`），**0 次作者调用且配额不被抬高**。**★4 实测的例外**：**int 及其子类**在 `d/i/u/o/x/X/c` 上原生按 **C 存储直读**、**不派发任何槽**（覆写 `__int__`/`__index__` 被原生忽略、0 次作者调用）⇒ 这些转换符对 int 家族**不做槽检查**，上界仍走 `int.__index__` C 槽读真实值。**bytes 模式的同类事实**：bytes/bytearray 子类的 `%s`/`%b` 直读 C buffer、**忽略**覆写的 `__bytes__` ⇒ 放行且按 C 字节数计；只有**非 buffer** 带作者 `__bytes__` 的形状（含 `PurePath` 子类覆写 `__bytes__`/`__str__`/`__fspath__`/`_str`）才具名拒绝。`o/x/X` 上 float/Decimal **只有类里根本没有 `__index__`** 才判原生必错（原生对任何对象都会派发 `__index__`）；“干净”的巨大 int 在 `e/f/g` 上判原生 OverflowError/TypeError；`Decimal('sNaN')` 在 `e/f/g` 家族上判原生 TypeError；无转换符（`"%%(a)s"`）按 render。T3 的覆盖是 **`e/f/g` × 巨大 int** 家族（str 模式 OverflowError / bytes 模式 TypeError）；T6/Z3 的覆盖是**推导式 iter（含 ≥2 层嵌套）**。 |
| `join` | 分隔符长度与 `join` 本身**走祖先 C 槽**（`str.__len__`/`str.join`…，第十二轮 M1：覆写 `__len__`/`join` 的分隔符曾让 125,006 字符通过 / 返回 5,000,000 字符）；调用点只把**被消费的那个实参**（元素表）物化一次，分隔符等标量原样传下去。分隔符被 `元素数-1` 次重复后的总长超限；被 join 的对象可以是**任意可迭代**——**调用点**先物化一次再按内容量累计（边枚举边用内容下界提前拒绝），包装/偏应用形态同样生效。物化 list 的指针有独立的**字节**预算（默认 1250 万项 / 100MB 指针）。**`len()` 只对精确内建类型生效**（`type(x) is list/tuple/str`，此时是 CPython 自己的实现）；**子类与其它可迭代一律枚举计数**——list/tuple/str 子类谎报 `__len__` 曾在预检里让 `len(sep)*(n-1)` 项消失、导致整块结果被造出来（第八轮 H1，实测峰值 206–961MB），现在诚实与谎报**行为一致**且峰值 ≈0 |
| `sort` | `sorted()` 同样会**消费**单次迭代器：调用点先物化一次再判定（与 `join` 同一套机制） |
| `add`/`extend`/`append`/`concat` | 只受 hard 约束（结果 ≈ 输入，线性不放大）；未枚举的同类序列（`array.array`/`deque` 等）按元素数一并预检，**元素数走祖先 C 槽**（第十二轮 M1）；没有 C 槽可读的同类型自定义 `__add__` 序列 ⇒ 拒（不按实例 `__len__` 猜） |

#### `%` 派发表：单一口径、实测事实与测试对照（plaita#68 第二十一/二十二轮）

`plaita/core/expression.py` 里 **`_RENDER_DISPATCH` 是唯一判据表**：key = 转换符，
value = `(kind, slots, bound)`。`_conversion_accepts` / `_definitely_rejected` /
`_spec_bound` / `_spec_input_size` / `_render_size` 全部只查它（`_render_size` 只做
"s"/"r" 行的分派）；`_CONVERSION_DISPATCH` 是它的派生投影（`conversion -> (kind, slots)`）。
**归属判定由同一张表分两处驱动**（第二十二轮 AG4 更正措辞）：数值族在
`_conversion_accepts` 里按本行的 `kind`/`slots` 调 `_numeric_slots_clean`；文本/buffer 族
（`s`/`r`/`a`/`b`）在每行的 `bound` 里判定（`_slots_owned_by_base` + 纯 Python 祖先的实例
属性闸，见下）。`r`/`a` 行按 `s` 行的 slots **并集**收紧（`object.__str__` 就是 `repr`，
三行共用一个上界函数）；`char` 行的 `bound` 为 `None`（上界是宽度常量，`_spec_bound`
直接算）。`_STR_SAFE_OWNERS`/`_REPR_SAFE_OWNERS` 已删除。
bytes 模式的 `%s`/`%b` 由 `_render_rule` 路由到同一张表的 buffer 行。

**第二十二轮 AG1/AG2 加固（纯 Python 祖先的实例属性）**：`PurePath`/`UUID`/`SimpleNamespace`
的 `__str__`/`__repr__` 是 Python/C 代码，会读**实例槽**（`_str`/`_drv`/`_root`/`_parts`、
`int`、真实实例 dict）。作者只要在子类类体里放同名 property 就能换掉祖先读到的值
（实测 `class P(PurePosixPath): _str = property(...)` ⇒ r21 `%s` 上界 **67**、实际
**10,000,000**、峰值 **10.00MB**；`SimpleNamespace.__dict__` property ⇒ 上界 51、实际
10,000,010、峰值 22.5MB；`UUID.int` property ⇒ 上界 74、实际 332,197），而且 r21 的
`_mod_decide` 会**调用**这些 property（`_parts` 4 次 / `__dict__` 3 次）。加固后：

* `PurePath`/`UUID` 子类：从 `cls` 到最近的已知 stdlib 祖先之间，**任何一层类命名空间
  不得新增名字**（`_stdlib_only_below`，只放行 `__module__`/`__qualname__`/`__doc__`/
  `__slots__` 与解释器自动补的 `__dict__`/`__weakref__` getset）；上界改读**真实实例槽**
  （`object.__getattribute__` 读 member_descriptor，0 作者调用）——路径 bytes 用
  `4×槽字符数+16`，路径 str/repr 用 `10×槽字符数+真实类名+16`，UUID 按 `int.bit_length`
  算 `'%032x'` 长度；native 会派发作者 property 的形状因此**具名拒绝**。
* `SimpleNamespace`：改读 `SimpleNamespace.__dict__` 上的 member_descriptor（C 直读真实
  实例 dict，property 遮蔽无效），与原生 C repr 看到同一份数据；决策路径 **0 作者调用**，
  上界按真实内容计（key 需 exact `str` 或 `__repr__` 归属仍是 `str`）。
* 元类数据描述符遮蔽 `type.__name__` 的子类一律拒绝（原生 repr/str 会读 `cls.__name__`）；
  `Enum` 的类名也改走不可变 getter。
* **实例 dict** 里出现 `_format_parsed_parts`/`as_posix`/`_from_parts`/`_flavour` 也一律拒绝：
  `PurePath.__str__/__repr__` 用**普通属性查找**读 `self._format_parsed_parts(...)` /
  `self.as_posix()`（二者不是数据描述符，实例属性可以遮蔽；实测 `p.as_posix = lambda:
  "y"*10**7` ⇒ `repr(p)` 10,000,009 字符、r21 上界 81）。检查只读实例 dict，0 作者调用。
* 注意：`_path_bytes_bound` 与 `_render_type_bound` 现在共用同一套实例属性闸——r21 只在
  bytes 模式查了 `_str`，`%r`/`%a` 走 `_render_type_bound` 时没有查，所以同样中招。

**第二十三轮 AJ1（精确类型同样走闸）**：上面这些闸最初只装在**子类**路径上，而
`_render_type_bound` 的**精确类型分支**（`cls in _PATH_EXACT_TYPES` 走 `value.parts`
投影、`cls is UUID` 走常量 64）绕过了它们——精确类型的实例槽同样可写：
`p = PurePosixPath("/x/y"); p._str = "E"*10**7` ⇒ `%s` 投影 **67** / 实际 **10,000,000**，
`%r` 投影 67 / 实际 10,000,017、峰值 **22.50MB**（分配后才拦）；
`object.__setattr__(uuid.UUID(int=1), "int", 10**400000)` ⇒ 投影 **74** / 实际 **332,197**。
修后两个精确分支改调 `_path_render_bound`/`_uuid_render_bound`（与子类共用同一道闸）：
精确 path 巨槽 ⇒ projected 100,000,089 ≥ 实际、`_fn_mod` 分配前 `ValueError`
@0.0018MB；精确 UUID 巨 int ⇒ projected 332,207 ≥ 332,197、**0 作者调用**；
`{% $F.mod('%r',$INPUT.p) %}` 的端到端峰值从 22.501MB（分配后拦）降到 0.002MB。
逐分支清单（含常量/投影标记）见 `docs/mutation-testing.md` 第二十三轮小节。

**第二十四轮 AM1（常量上界必须读真实成分）**：上一轮的"逐分支清单"里，多条**常量上界**
被作者可控字符串证伪——`type.__repr__`/`object.__repr__` 输出 `tp_name`（=
`__module__` + `__qualname__`，**建成后仍可写**）、`EnumMeta.__repr__`、函数 repr（`__qualname__`）、
`timezone.__str__`（`name`）、`repeat`/`count` 的 repr（参数）都会内嵌作者数据：
`class C: pass; C.__qualname__ = "E"*1e7` ⇒ `%r` 投影 **266** / 实际 **20,000,025**、
峰值 25MB；`type("E"*1e7,(),{})` 266/10,000,019；enum 类 266/10,000,009；函数
`__qualname__` 266/10,000,026；`timezone(timedelta(0),"E"*1e7)` `%s` 74/10,000,000；
`itertools.repeat("E"*1e7)` 266/10,000,010。修后这些族一律**读真实成分**（不可变
`type.__dict__["__module__"/"__qualname__"]` getters、`FunctionType.__qualname__` getter、
C 槽 `timezone.tzname(value, None)`、C `__reduce__` 的真实参数递归求界），读不到 ⇒ 拒绝；
`repeat`/`count` 从"固定短 repr"集合移除。实测巨名形状端到端在**分配前**拒绝
（`_fn_mod` `ValueError` @0.0018MB；模板 0.0015–0.087MB）。


**第二十五轮 AP1（array 每元素按 typecode）**：清单里最后的"投影假设" `32 + len×24` 被
实测证伪——`repr(-sys.float_info.max)` 是 24 字符、array repr 每元素再加 `", "` ⇒ **26/元素**
（`array('d',[-max]*2_000_000)`：旧投影 48,000,042 / 实际 52,000,012、`_fn_mod` RET @181MB；
K=4,166,000 旧投影 99,984,032 ≤ 1e8 而实际 108,316,012 ⇒ **绕过 1e8 硬上限**）。现在
`_array_render_bound` 按 **typecode 的实测最坏值**给界（`d`/`f` 26、`b`/`B` 6、`h`/`H` 8、
`i`/`I` 13、`l`/`L`/`q`/`Q` 23、`u` 转义 10×/字符），精确类型与子类共用，未知 typecode 拒绝；
K=4,166,000 现在 projected 108,316,042 > 1e8 ⇒ 分配前 `ValueError`。

**第二十六轮 AR1/AR4（`%a` 的 ascii 展开）**：`_bound_repr` 同时服务 `%r` 与 `%a`，而
`ascii()` 把每个非 ASCII 字符展开成 `\xNN`/`\uNNNN`/`\UNNNNNNNN`（4–10×）。四处作者
可控名字原先按 1× 计：`_enum_member_render_bound`（类名+成员名）、`_path_render_bound`
（类名）、`_uuid_render_bound`（类名）、`_tzinfo_extra`（`ZoneInfo.key`，4×）——`%a` 下
enum 成员 `_name_="é"*1000` ⇒ 投影 1031 / 实际 4008；`"😀"*1e6` ⇒ 投影 1,000,031 /
实际 10,000,008、`_fn_mod` **RET**（峰值 20.00MB）。现在这些名字一律按 **10×** 计
（`%r` 保守、`%a` 正确），并把 `%a` 的成长额度改成按值的**权威内容量**（`_content_size`）
计（ascii 展开不吃额度）⇒ 1M 星平面四形状在**分配前** `ValueError`（峰值 0.0031–0.0936MB；
r25 是分配后 8.0–20.1MB），而 `%r`、非 ASCII 的 `%r`、ASCII 的 `%a` 仍正常渲染。精确
`str` 的 repr 上界改为内容感知（可打印 ASCII ≤2×、控制字符与非 ASCII ≤10×）。

**第二十七轮 AU1/AU2（真实逐字符宽度）**：上一轮的"两分法最坏值"把上界做得比真实更悲观——
`"\n"*9999` 真实 ascii 只有 20,000，却被算成 100,010 > 允许 100,000 而**误拒**（`%r` 能渲染）；
`"é"*9999`、`"\n"*100000`、bytes `"é"*12496` 同样。现在按**真实逐字符宽度**编译上界：
1 = 可打印 ASCII（`'`/`\` 除外）、2 = `'`/`\`/`\t`/`\n`/`\r`、4 = 其它控制字符与
0x80–0xff、6 = 0x100–0xffff、10 = ≥0x10000，两端引号 +2（bytes 前缀 +3 / bytearray +14），
引号按 repr 真实行为；计数全走 C 级（`str.count` + 正则 `subn` 链式剥离），不构造展开串。
`%a` 的成长基准是值的**权威内容量**，拒 ⇔ `fmt_len + spec_bound > max(100_000,
4×(fmt_len+内容))`：`"é"*24_997` 渲染、24,998 起拒；`"😀"*9_998` 渲染、9,999 起拒；
`"😀"*1e6` 仍**渲染前**拒绝（峰值 8.45MB < 实际 10,000,002 字符），`%r` 对同一值正常渲染。
`%a` 的拒绝消息写明 ascii 展开、真实阈值与逃生口（改用 `%r` 或提高
`PLAITA_EXPR_GROWTH_FACTOR`）。bytes 上界同时从 4n+24/32 收紧为真实值（`b"x"*10` 由 64 → 13）。

**第二十八轮 AX1–AX3（bytearray 引号 / 上界计算峰值 / 容器快路）**：
① CPython 的 `bytearray.__repr__` **无条件**转义 `'`，而 `bytes.__repr__` 只在串里同时含
`"` 时才转义——r27 让二者共用 bytes 规则 ⇒ `bytearray(b"'")` 上界 15 < 实际 16、
`bytearray(b"'"*1e6)` 1,000,014 vs 2,000,014（**可绕 1e8 硬上限**）；现在 `bytearray` 传
`always_escape_apostrophe=True`，穷举 256 单字节 + 全部 ≤3 组合 0 低估，`bytearray(b"'"*K)`
上界 = 2K+14 ⇒ K>5e7 越过硬上限。
② bytes 的不可打印计数从 regex `subn`（每匹配一个 match 对象，实测 **88×** 输入内存）
改成 `bytes.translate(None, delete)`（**1.00×**）；`str` 侧保持 `re.subn` 链，峰值预算
实测 2.11×（astral 4MB→8.45MB）/4.22×（BMP 2MB→8.45MB）/2.23×（astral 40MB→89.10MB），
峰值预算**逐路径**写清（H2）：`bytes`/`bytearray` **恰好 1.00×**（0.2/1/2/20MB
各档）⇒ 满足字面 H2；`str` **不满足字面 H2**，采用有界预算——astral 2.11×（4MB）/
2.23×（40MB）、BMP CJK 4.22×（2MB）/4.28×（40MB）storage、纯 ASCII 快路 0×，预算是按
`len(value)`（**字符数**，非 `sys.getsizeof`/storage）的 ≤10×，常数与消息写在
`test_bound_computation_peak_budget` 里；拒绝时机不受影响（1e8 硬上限与形状检查仍在渲染前）。
③ `_container_render_bound` 的 str 快路改用真实逐字符宽度 ⇒ `["\n"*100_000]` 的 `%a`
由“上界 1,000,038 > 允许 400,008 误拒”变为渲染（bound 200,030 ≥ 实际 200,004）。

| 家族 | 实测原生派发 | 放行（可证） | 拒绝（fail-closed，具名 + 逃生口） | 回归测试 |
|------|--------------|--------------|------------------------------------|----------|
| `d/i/u` | int 家族：C 存储直读、**不派发任何槽**（覆写 `__int__`/`__index__` 被忽略、0 次调用）；float/Decimal：`__int__` | 精确 int/bool/float/Decimal 与**槽未被覆写**的子类；量级走 `int.__index__`/`float.__float__`/`Decimal.adjusted` | 覆写 `__int__` 的 float/Decimal 子类；只有 `__index__`/`__int__` 协议可取数的自定义对象 ⇒ `int(x)`/`float(x)` | `TestEighteenthRoundZ1NumericSubclasses`、`TestNineteenthRoundABAdversarialCorpus::test_adversarial_corpus_invariants` |
| `o/x/X` | 同上（int 家族 C 存储；float/Decimal 需 `__index__`，类里根本没有才判原生 TypeError） | 精确 int 与 int 子类 | float/Decimal（即使有 `__index__`）；自定义 `__index__` ⇒ `int(x)` | 同上 |
| `e/E/f/F/g/G` | float/Decimal 派发 `__float__`；巨大 int 原生 OverflowError/TypeError；`Decimal('sNaN')` 原生 TypeError | 精确 int/float/Decimal 与槽干净子类（`inf`/`nan` 原生优先） | 覆写 `__float__`/`__index__` 的子类 ⇒ `float(x)` | `TestEighteenthRoundZ2NonFinite`、`TestSeventeenthRoundT2T3Numeric` |
| `c` | int ⇒ 单码点；str/bytes-like 长度为 1 | 精确 int；长度 1 的 str/bytes-like | 其余 ⇒ 原生 TypeError 或具名拒绝 | `TestFifteenthRoundQ4ValuePredicates` |
| `s`（str 模式） | `__str__`（`object.__str__` 的返回就是 `repr(self)`，所以 slots 同时含 `__repr__`） | 精确 str/bytes/bytearray/int/float/bool/None/容器/视图/迭代器/`Decimal`/`datetime`/`UUID`/`PurePath`/`Pattern`/`Enum`/`array`/`deque`/`SimpleNamespace`/默认 repr 的对象；**干净子类**（`__str__`/`__repr__` 与祖先同一原始槽） | 覆写 `__str__` **或** `__repr__` 的子类，**含元类数据描述符欺骗**（`Meta.__mro__`/`__dict__` 撒谎不影响判定）⇒ `str(x)` | `TestTwentyFirstRoundRenderDispatch::test_legit_types_are_not_over_refused`、`::test_overriding_slot_subclasses_are_refused_by_name`、`TestTwentiethRoundACMetaAndInstrumentation` |
| `r`/`a` | `__repr__` | 同上（Enum 成员按 `enum`/`flag`/`int`/`str` 原始槽分别给上界，value 计入） | 覆写 `__str__`/`__repr__` 的子类 | 同上、`TestTwelfthRoundM6M7EnumAndTimezone` |
| `s`/`b`（bytes 模式） | 真 buffer ⇒ C 直读（**忽略**覆写的 `__bytes__`）；非 buffer ⇒ `__bytes__` | C buffer（bytes/bytearray/memoryview/array…）；`PurePath` 家族：`__bytes__`/`__fspath__`/`__str__`/`_format_parsed_parts` **逐槽归属校验**（不可变 `_slot_owner`）**且**从子类到最近的已知 stdlib 路径类之间任何一层类命名空间不得新增名字（`_stdlib_only_below`——`PurePath.__str__` 是 Python 代码、会读**实例槽** `_str`/`_drv`/`_root`/`_parts`，作者 property 能遮蔽它们），上界取**真实实例槽**字符数 `4×+16` | 作者 `__bytes__`（非 buffer 的任何形状，含 `str`/`PurePath` 子类）；遮蔽 `_str`/`_drv`/`_root`/`_parts` 的路径子类 ⇒ `bytes(x)`/`bytearray(x)` | `TestTwentyFirstRoundRenderDispatch::test_buffer_dispatch_ignores_author_bytes_for_c_slots`、`TestTwentySecondRoundInstanceAttributeShadowing` |

**语料规模（第二十一轮，覆盖 revision 见 `docs/mutation-testing.md`）**：
`test_ac4_render_dispatch_full_table` 生成
`17 转换符 × 2 模式（str 模式跳过 b）× 5 值类型（int/float/Decimal/str/bytes）
× 4 家族（exact / clean / overriding / meta-deceived） = 660 格`，违规判据是两条方向
（原生成功且无作者调用却拒绝；`native` 却原生成功；`render` 上界 < 实际；`render` 但原生
实测会跑作者协议），当前 **660 检查 / 0 违规 / 0 跳过**。
`test_legit_types_are_not_over_refused` 另有 **84 检查 / 0 漏**（str/bytes/bytearray +
干净子类 + Enum/IntEnum + UUID + PurePath（含 Windows）/干净子类 × `s/r/a/b`）。
`test_overriding_slot_subclasses_are_refused_by_name` 覆盖 14 个形状（7 个正常 + 7 个
`_meta_deceived`），每个形状**先用原生正控证明"该槽确实被派发"**，再断言决策为 `refuse`、
0 次作者调用、拒绝消息含类型名、tracemalloc 峰值 < 4MB。

**第二十二轮 AG1/AG2/AG3 语料（同一 revision 见 `docs/mutation-testing.md`）**：
`TestTwentySecondRoundInstanceAttributeShadowing`（7 用例 / 58 subtests）——
① 路径/`UUID`/`SimpleNamespace` 的实例属性**遮蔽 property** 形状（含实例 dict 遮蔽
`as_posix`/`_format_parsed_parts`）：决策 `refuse`、0 次作者调用、native 正控证明该
property/实例属性确实被派发、峰值 < 4MB；
② 清白子类（PurePosixPath/PureWindowsPath/PosixPath/SimpleNamespace/UUID 的 `pass` 子类）
**35 检查 / 0 漏**（≥ 实际、0 调用）；
③ `SimpleNamespace.__dict__` property：按**真实实例 dict** 给上界（`projected ≥ 实际`，
决策路径 0 调用，native C repr 也不调用该 property）；
④ AC4 的"纯 Python 祖先"维度：`5 基类 × 4 家族（exact / clean / shadow-property /
meta-deceived shadow）× 7 格式（str/bytes 两模式）= 140 检查 / 0 违规 / 0 跳过`；
⑤ `test_decision_path_has_zero_author_calls_for_all_shadow_shapes`：真 property 计数器
（不是 dunder）在决策路径上必须为 0。

**量级示例（放行侧）**：`"x" * 10**6`（输入 1 字节）被拒；`join(20 段 × 10000 字符)`
= 200000 字符**通过**（输入内容本身就 20 万）；`json_dumps([{"k":"v"*100}]*2000)`
= 222000 字符**通过**；`mod("%s %s", (100KB, 100KB))` = 200001 字符**通过**；
`join(200 段 × 1KB)`、`join(['b'], $INPUT.long_sep)`（只有一个元素时分隔符根本不进
结果——注意模板文法里没有 `*`，这里的"长分隔符"要来自 context 而不是表达式内运算）、
`mul('x'*200000, 2)`、`mul('x'*100001, 1)` 都通过。
**拒绝侧的例子**：`today('%c' * 100000)` = 2400000 字符（输入 20 万 → 24 倍放大，
超过 factor=4）按设计拒绝——这是**探针（检测）**拦下的（`today` 没有预检），
与"非放大的大输入必须放行"并不矛盾。

**类型错误优先于尺寸错误**：`str + list`、`list * str`、`set * int`、
`join` 非匹配元素、`%` 的参数个数/mapping 键缺失/非法转换符/`b'%s' % 'x'` 等
在任何规模（包括把三只旋钮压到 `1/1/1`）都抛原生 `TypeError`/`KeyError`/
`ValueError`，不会被改写成尺寸错误。

**求值路径上的口径（第六轮修正）**：判定分两层，两层都要说清——

* **预检层（分配前闸）**：上表列出的内建放大算子在调用 Python 算子之前判定；
* **探针层（求值侧网）**：`$F.*` 返回后按同一条比值规则复核。它用的输入规模是
  调用点**在调用之前**算好、并作为**本帧局部量**传进来的快照——既没有线程本地
  之类的共享槽位（第四轮删掉），也**不在调用后回读实参**（第六轮修正：否则被调用者
  只要 `lst.clear(); lst.extend([...巨大...])` 就能把额度抬高，实测旧实现可放行
  99,000,000 字符 / 峰值 123MB）。`{% ... %}` 插值同样过这条守卫。

需要"先物化再判定"的算子（`join`/`sort`，**按算子名**判定，所以
`functools.partial`/lambda/绑定方法/同名覆盖等包装形态同样生效）由**调用点**在调用
前把单次迭代器物化一次并**替换实参**：投影、输入规模与真正的执行共用同一份数据。

**输入/渲染口径**（第六轮修正）：`_content_size` **只信权威协议**——`str`/`bytes`/
`bytearray` 长度、真实缓冲的 `memoryview(x).nbytes`、映射/容器由我们自己枚举；
**不信**任何对象自报的 `itemsize`/`__len__`（说谎缓冲对象实测能把额度抬到 2500 万），
其它对象按 0 计。`%s/%r/%a` 的渲染投影用独立的 `_render_size`：能界定才给上界，
带自定义 `__str__`/`__repr__` 的对象**无法界定 ⇒ 预检保守拒绝**。

**求值侧探针是网，不是闸**：表达式求值时还会对**每一次** `$F.*` 调用做硬上限
判定并套同一条比值规则。两者的差别是准确的：

- 上表列出的内建放大算子：**在分配之前拒绝**（实测 tracemalloc 峰值不随放大
  规模增长，拒绝路径峰值 ≤1MB）；
- 没有预检的路径（自定义注册函数、`$F.slice`、`$F.json_loads`、`$F.today`…）：
  **检测到（值已经被分配出来了），不是阻止**——例如自定义函数
  `$F.boom(1000000)` 会先造出 1MB 字符串再被拒；恶意自定义函数内部先做一次
  合法大操作再返回 99MB 字符串，同样只能**事后**拒绝（峰值就是它自己分配的
  94MB）。把体量大的计算放在自定义函数里不能靠这条守卫省内存。
  **额度只由这次调用在"调用之前"测得的实参快照决定**：判定没有线程本地/跨函数
  通道，也不在调用后回读实参——`func(*args)` 期间跑的任何代码（内部合法调用、嵌套
  `evaluate`、被 monkeypatch 的辅助函数、**改写自己的可变实参**）都**无法**放行
  外层结果。

**迁移**：正常流程无需动作。确有合法的大结果需求时，按量级调大上面三个旋钮
（如 `PLAITA_EXPR_GROWTH_FACTOR=16` 或 `PLAITA_EXPR_MAX_RESULT_SIZE=2000000`），
或把大块拼接/替换移到节点 I/O（CodeNode / 模板节点）而不是表达式里。错误归因
沿用该节点既有的错误处理策略（`error`/`fallback`/默认失败）；超限是确定性失败，
重试不会变好。

**已知边界（plaita#68，第十二轮收敛后的口径）**：

- **渲染类路径一律"先判后渲染"**：`$F.mod` 的**格式操作数必须是精确 `str`/`bytes`/`bytearray`**（子类可覆写 `__mod__`/`__bytes__` ⇒ 预检被绕过，实测先分配 90.1MB；第十八轮 Z4.1 起分配前拒绝 + `str(x)` 逃生口）；`%s/%r/%a`、`$F.concat`、`{% ... %}`
  插值先用 `_render_size` 求**可证上界**，上界不可得或超额度 ⇒ **直接 `ValueError`**
  （不调用 `str()`）。`_size_guard_result` 的事后探针只是**第二道网**，不承担"给分配
  设界"的职责——不能只靠它。**错误消息本身也是一次渲染**：拒绝文案里的 int 走
  `_safe_int_text`（C 槽 + 位数），不会调用敌意 `__str__` 或展开巨大十进制。
- **namedtuple 不做整体渲染**：`__code__` 相同不等于行为相同（闭包 cell /
  `__globals__` / 描述符 `__get__` 都能换成敌意实现），因此删除了 namedtuple 快速
  通道。**索引下钻可用**（`$INPUT.v[1]` 实测返回元素）；**`$INPUT.v.a` 这类字段访问
  在本表达式文法里不可用——它静默返回 `None`**（不是本单引入的行为，实测如此）。
  需要整体渲染时请在**上游**（节点或自定义函数）先 `str(x)` 再传入——表达式文法里
  没有裸函数调用：`$F.concat(str(x))` 这种内层写法直接 `ParseException`；`{% str(x) %}`
  内层不合法 ⇒ 整串**原样返回**并告警；整串 `str(x)` 不含 `{%`、也不以 `$` 开头，
  按纯文本原样返回。
- **子类的 dunder 判据读 MRO `__dict__` 的原始槽**并与安全祖先做同一性比较，描述符
  对象一律不认（"类上返回真实现、实例上返回敌意实现"同样被拒）；无覆写的标量子类
  （`int`/`float`/`str`/`Decimal`/`datetime`…）按祖先**标量**规则给上界，不去迭代。
- **enum / tzinfo 的作者数据计入上界**（第十二轮 M6/M7，第十三轮 O5/O6 收紧/放宽）：
  `repr(Enum 成员)` 内嵌 `value` 的 repr、`repr(datetime/time)` 内嵌 `tzinfo` 的 repr 都跑
  同一套计费；`IntEnum`/`IntFlag`/`StrEnum`/`Flag` 成员按各自 `__str__` 原始槽给上界；
  **只覆写 `__repr__` 的 enum 子类一律拒**（O5：上界曾 28 vs 实际 5,000,000）；
  精确 `zoneinfo.ZoneInfo` 按 `key` 计费（O6，合法时间戳不再误拒），`ZoneInfo` **子类**
  与未知 tzinfo 仍拒。
- **`collections.UserList`/`UserString`**（第十三轮 O6）：精确类型走其精确 `.data`
  （list/str）的 C 槽计长度；`data` 不是精确内建或类型是子类 ⇒ 仍拒。

```
# `$F.mul/add/extend/replace` 不接受无 C 槽自定义序列
- `abc.Sequence` 子类、自定义 `replace` 宿主、自定义非标序列：**被拒**。
- 逃生口：产值的节点/插件**必须**把序列**显式**包成 `list(...)` / `tuple(...)` / `dict(...)` / `set(...)`。
- 已放行：标准库 `list`/`tuple`/`dict`/`set`/`UserList`/`zoneinfo.ZoneInfo`。
```

  同一口径也适用于**数值/映射**（第十五轮 Q2/Q4，第十六轮 S1/S2 统一）：自定义
  `__index__`/`__int__`/`__float__` 的值在 `%d/%x/%c` 上被拒（逃生口
  `int(x)`/`float(x)`）；`fractions.Fraction` 不在可证数值白名单（`%f` 等一律拒，
  逃生口 `float(x)`）；自定义 `__getitem__` 映射（含 `dict` 子类以外的有状态映射）
  被拒（逃生口 `dict(x)`）；**`MappingProxyType` 一律拒绝**（S2：`type(proxy)` 恒为
  `mappingproxy`，被包对象不可访问 ⇒ 证明不了它是精确 `dict`；`proxy.copy()` 实测会跑
  被包 dict 子类的 `__getitem__`，**不是 C 槽**）。放行的是精确
  `ChainMap`/`UserDict` 与 `dict`/`dict` 子类（**只读一次**、计费与渲染共用同一份快照）。
  **bytes 模式与 str 模式共用同一判据**（S1）：`b'%s'`/`b'%b'` 只放行 **buffer 协议**
  （字节数走内存视图 ⇒ 可证上界），自带 `__bytes__`/`__index__`-only 的对象
  **分配前拒绝、0 次作者协议调用**；`b'%a'`/`b'%r'` 走 repr 可证上界；任何
  "解不出来"的形状默认**拒绝**，绝不退回"原生必错 ⇒ projected=0 ⇒ 放行"。
  所有拒绝消息都**点名**对应逃生口（`list(x)`/`tuple(x)`/`dict(x)`/`int(x)`/
  `float(x)`/`bytes(x)`），不是通用措辞。
  **X2.1（映射子类）**：`ChainMap`/`UserDict` 的**子类**在读取任何属性之前就拒绝
  （子类可以覆写 `maps`/`data` 为 property 或换成别的对象类型，读它＝运行作者代码，
  实测 `keys`/`__getitem__` 覆写 0 次调用即拒）；精确 `ChainMap` 的 `.maps` 必须是
  精确 `list`、每项必须是 `dict`，逐项用 `dict.items` C 槽合并；**`MappingProxyType`
  一律拒绝**（第十六轮 S2：`type(proxy)` 恒为 `mappingproxy`、被包对象不可访问，
  `proxy.copy()` 实测会跑被包 dict 子类的实例 `__getitem__`（1 次作者调用 + 伪造值），
  不是 C 槽；逃生口 `dict(x)`）；`UserDict` 的 `.data` 必须是精确 `dict`。
  **走查深度（X2.2 + 第十六轮 T4.2）**：`_DATA_WALK_MAX_DEPTH = 10^6` 是**声明**预算，但
  `_content_size` 是递归实现，实际会被解释器递归上限截断；走查期间把上限临时抬到
  **6000 帧 ⇒ 有效深度 ≈2996 层（±1，随嵌套构造的计数方式而变；测试**动态探测**真实边界，不写死数字）**并在 `finally` 逐字节还原。
  再深的病态嵌套 fail-closed，**实际路径**发出的是带真实深度的准确消息：
  `$F.* nested value exceeds the interpreter recursion budget: depth <N> (declared budget
  1000000 levels; the walk raises the interpreter recursion limit to 6000 frames while it
  runs)` + `list(x)` 逃生口（第十六轮 T4.2 修掉了旧实现的乱码 `result too large: 0 levels`）。
  **精确 `UserString`/`UserList`** 的迭代器产出仍是包装类型（长度 1）⇒ 走它们的精确
  `.data`（C 槽）：`_content_size`/`mul`/`add`/`extend` 可用；`.data` 不是精确内建或
  类型是**子类** ⇒ 迭代前具名拒绝（`replace`/`concat` 仍拒，逃生口 `str(x)`）。
- **`%r`（`as_repr`）与 `__str__`-only 覆写**（第十五轮 Q8②，口径写清）：enum 成员
  只覆写 `__str__` 时，`%r` 仍可用——它走 `Enum.__repr__` 原型槽 + 成员 `value` 的
  repr（各有可证上界，实测上界 22 ≥ 实际 9），而 `%s` 被拒；**`str` 子类**只覆写
  `__str__` 时**两者都拒**（子类判据按 `__str__`/`__repr__` 原始槽整体看，任一覆写即
  视为不可证——保守方向），逃生口是上游 `str(x)`/`repr(x)`。
- **日志/消息里的作者可控字符串一律截断**（第十三轮 O4）：类型名截到 64 字符、
  异常消息只用 `BaseException.args` 的 C 槽取单 str 参数并截到 200 字符（不调用
  覆写的 `__str__`）；`type(x).__name__` 可被作者设成 3MB（实测 WARNING 3,000,051
  字节），截断属本单的渲染量面。
- **渲染点清单守卫（第十二轮 H3 / 第十三轮 O3/O7）**：`expression.py`/`expression_parser.py` 里所有
  `str()`/`repr()`/`format()`/`ascii()`、`%` 格式化、`.format()`/`.join()`/`.replace()`、
  `__str__`/`__repr__`/`__format__`/`__mod__` 属性取用、f-string 形参插值、`logger.*`
  插值调用、以及**可静态识别的间接构造**（模块别名指向渲染原语、`getattr(x, name)(…)`、
  `getattr(str,'__call__')(…)`、`operator.methodcaller`、`json.dumps`、`io.StringIO().write`、
  `print(a)`、`__builtins__['repr'](a)`、`vars(m)['str'](a)`、`__import__('builtins').str(a)`、
  `csv.writer(f).writerow(a)`、`Path(p).write_text(a)`、**未被调用只被传递的** dunder
  （`functools.reduce(str.__add__, xs)`）、以及**根是形参直接别名**的 f-string
  （`x = a; f"{x}"`）），都由 AST 测试**逐点登记**（宁红勿绿）。标为 `guarded` 的点，其
  具名上界调用必须**支配**它：条件链前缀 + 同层更早，且**带 `if` 的推导式、`[:0]` 切片、
  嵌套 `def`/`lambda`、`match` 分支**里的调用一律算不支配（第十三轮 O3 的四种形状都要求红，
  定义原文存在 `TestTwelfthRoundH3RenderSiteInventory._DOMINANCE_MUTATIONS`）。另有每个渲染
  入口的**行为反例**（喂"上界小、实际大"的敌意对象，断言分配前拒绝）。**清单是启发式、
  不是完备证明**。第十五轮 Q7 补齐了 `LOG.warning('%s', a)` / `self.logger.warning(...)`、
  `w = fh.write; w(a)`（绑定方法别名）、`json.dump(a, fh)`、`getattr(fh,'write')(a)`、
  以及**根是形参/函数内局部名的属性/下标链** f-string（`f"{obj.attr}"`、`f"{d['k']}"`）。
  **明确未覆盖（静态扫描器看不到，已逐条点名）**：经 `csv`/`logging` 配置间接拿到的
  格式化器、运行期 `getattr` 拼接出的调用链、模块级常量属性链的 f-string、
  `json.dump` 之外的第三方序列化入口；上界真假由祖先槽反例等行为用例承重。
- **日志不插值作者数据（第十二轮 M4）**：评估路径上的 `logger.*` 只记**类型名/元数/形态**
  （`parse_function` 记 `argc`+`arg_types`；`_render_unmeasurable` 只记异常类型名），
  `%s` 不再渲染作者对象（修复前实测日志 5,000,296 字节、峰值 10.8MB）。仓库级日志卫生
  （全仓 588 站点）是 plaita#151，不在本单。
- **只收口 `Exception`**：守卫把 `Exception` 子类转成"不可界定 ⇒ `ValueError`"，
  `BaseException`/`KeyboardInterrupt`/`SystemExit` 照旧穿透（不会被吞）。
- **enum / tzinfo 的作者数据计入上界（第十二轮 M6/M7）**：`repr(Enum 成员)` 内嵌
  `value` 的 repr、`repr(datetime/time)` 内嵌 `tzinfo` 的 repr，都跑同一套计费；
  `IntEnum`/`IntFlag`/`StrEnum`/`Flag` 成员按各自 `__str__` 原始槽给上界（不再一律
  误拒）；覆写 `__str__` 的 enum 子类仍然拒绝，`tzinfo` 的 repr 不可界定则拒绝渲染。
- **`_render_size(...) is None` 是唯一的生产闸**：第十二轮 M8 删除了实际只被测试引用的
  `_has_bounded_repr` 谓词（它与闸的判据曾在函数/lamda 等类型上不一致），文档不再把
  它称作"渲染前闸"。
- **注册函数（自定义 `$F.xxx`）仍是"检测而非阻止"**：它们在 `$F.*` 返回后由探针
  复核，分配已发生；作者自定义算子若要提前拒绝，请自行调 `_render_size` 或遵循
  文档口径。`$F.json_dumps` 同理（登记在渲染点清单里、无预检）。
- 容器硬上限按**元素数**计量、`join` 物化有独立的**指针字节**预算（显式守卫）。

### 表达式：字段段不再能遍历 Python 内部属性（下划线名硬报错，plaita#55）

`$ROOT.field.field…` 的字段段此前无条件交给 `_get_attr` 的 `getattr` 回退（没有
下划线过滤），而段名字符集允许 `_`：节点 `execute` 返回带 `__dict__` 的对象
（dataclass / pydantic / 普通实例）时，
`$NODE.<id>.__class__.__init__.__globals__.os.environ` 是一条语法合法的表达式，
可读出 worker 进程的全部环境变量——**完全绕开 `$ENV` 的 `exposeEnv` allowlist**。
本版在唯一把字段段变成 Python 属性的解析点（`_get_attr`）收口，两条路径分别处理：

- **属性路径**（对象属性读取）：任何 `_` 开头的字段段一律抛 `ValueError`，消息
  点名被拒段名并说明原因，**不再返回 `None`**——静默 `None` 会把越权尝试伪装成
  "键不存在"，正是本次修复要消除的假象；
- **映射路径**（`dict` / 带 `get` 的映射）：字段段是**键**不是属性，单下划线键
  保持可读（`$NODE._n2` 这类自动生成的节点 id、`{"_foo": …}` 数据键都不受影响）；
  `__` 开头的**路径段**（`$NODE.d.__class__`）同样拒绝——路径形态无法与属性遍历
  尝试区分，静默 `None` 同样会制造"看起来被挡住了"的假象。**该键本身仍是普通
  数据**：显式映射辅助 `$F.get($NODE.d, '__class__')` / `$F.getDictValue` /
  `$F.keys` 照旧可读（见下方「刻意的不对称」）。

- **受影响**：任何用 `$…._x` / `$…. __x__` 读**对象属性**的表达式（此前得到私有
  值或静默 `None`）现在抛错；`$…. __x__` 这种**路径段**写法也抛错。节点插件返回值
  里的 `_` 前缀字段若需被流程引用，请改公开名，或把值放进 dict（dict 的 `_` 前缀键
  不受影响）。
- **不受影响**：公开**数据**属性（模块 / 函数 / 类等代码对象另见下方 #133 条目）、
  `dict` 键（含 `_` 前缀）、`[n]` / `.n` 索引、`{% … %}` 插值、
  `$NODE.raw.…`（`raw` 只关掉字符串二次求值，不关掉本闸门）、
  `$PARENT.$INPUT.name` 这类段名带 `$` 的写法、`$F.*` 注册表，以及
  `exposeEnv` allowlist 本身的语义（未改动）。
- **异常类型**：`ValueError`——表达式层既有类型（`ExpressionRegistry` 同类型），
  **没有新增公共异常类**。在实测的 `end` 节点 `output` 路径上表现为节点错误
  `NodeExecutionError`（`__cause__` 为该 `ValueError`，消息含被拒段名）。
  运行时文案（**逐字**，测试与表达式指南引用同一串）：

      expression field name '__class__' is not allowed in an expression path: names starting with '_' are reserved for Python internals (for example __class__ / __globals__ / __init__), so the attribute form reaches process state and bypasses the $ENV allowlist, which is an opt-in exposure list and not a sandbox. Public attribute traversal is additionally data-only (plaita#133): public attributes that resolve to modules, functions, classes, frames, generators, coroutines or other code objects are refused by name.

- **为何不在编译期拦截**：字段段的容器类型只有运行期才知道（`$NODE._n2` 是 dict
  键、`$NODE.<obj>._x` 才是属性），因此闸门放在运行期唯一收口点，而非 `dot_field`
  文法动作——否则会误伤 dict 键与自动生成的节点 id。
- **刻意的不对称（映射辅助不受限）**：`$NODE.d.__class__` 抛错，但
  `$F.get($NODE.d, '__class__')` / `$F.getDictValue($NODE.d, '__class__')` 仍返回该键
  的数据值，`$F.keys($NODE.d)` 也仍列出它。理由：映射辅助回取的是"该 key 下存的
  **数据值**"——键不是属性、不产生任何反射语义；路径形态的拒绝只针对
  `$NODE.d.<name>` 这种与属性遍历无法区分的写法（它会静默 `None`，制造"看起来
  拦住了"的假象）。本单 Boundary 明确不改 `$F.*` 注册表，该不对称由
  `tests/unit/test_issue55_attr_traversal.py::test_dunder_key_asymmetry_with_mapping_helpers_is_pinned`
  钉为显式行为。

**范围说明（#55 的下界 + #133 的数据面）**：

- **公有属性遍历不是无限制的**：#55 只关闭 `_` 前缀；#133 在同一收口点增加了
  对象种类闸——公有属性解析出 module / function / class / frame / generator /
  coroutine / code 等对象时具名拒绝。公有**数据**属性仍可读；映射 / 索引路径
  保持原语义（见下一条 #133 条目）。
- **映射路径与索引路径不变**：字段段在 dict / 映射上是**键**，不是属性；映射
  值按原语义返回（`$F.*` 也不在 #133 的改动面内）。本单只保证**属性遍历**的
  数据面，不承诺映射容器内部的值形状。
- **`$ENV` 的 `exposeEnv` 是"显式暴露"清单，不是沙箱**。本单修的是绕过它的**反射
  路径**（`__class__` → `__globals__` → 进程环境）；它不承诺"表达式拿不到进程信息"。
- **`plaita.io.get_attr`（Property 匹配用）不在本闸门内**。它对 `dict` **子类**
  实例确实会走 `getattr`（子类实例有 `__dict__`），但返回值只被 `match()` 当
  **布尔**消费、不回流表达式求值，因此不构成逃逸面；详见该函数上方注释。

### 表达式：属性遍历只允许数据——代码对象具名拒绝（plaita#133）

#55 关闭了下划线前缀的反射链，但**公有属性**仍可把模块 / 函数 / 类 / frame 等
挂在节点返回值上：`{"$NODE": {"h": SimpleNamespace(os=os)}}` 下
`$NODE.h.os.environ` 会返回活的 `os.environ`（可继续读任意变量），完全绕开
`$ENV` 的 `exposeEnv` allowlist。本版把属性遍历收紧为**只允许数据**：

- **判据是对象种类，不是属性名**：在唯一收口点 `_get_attr`，先按
  `type(obj).__mro__`（C 层真实类型，不读对象自己的 `__class__`；#163 纪律）
  判断基座对象；属性路径解析出的**值**也做同一判断。命中 module / function /
  builtin function / method / class（`type`）/ frame / traceback / generator /
  coroutine / async generator / code object ⇒ 抛具名 `ValueError`（含路径、
  真实类型名与 `plaita#133`）。
- **早停**：`$NODE.h.os` 本身即抛错，不会先返回模块再等 `.environ`；因此
  `.environ.HOME` 这条链在读取环境变量之前就终止。
- **数据面不变**：dataclass / 普通类 / Pydantic / SimpleNamespace 的公开数据
  字段照旧可读；`dict` 键（含 `_` 前缀）、`[n]` / `.n` 索引、`{% … %}`、
  `$NODE.raw.…`、`$F.*` 注册表与 `$ENV` allowlist 语义都未改动。
- **映射 / 索引路径保持原语义**：字段段在 dict / 映射上是**键**，不是属性；
  映射值按原语义返回。本闸门只针对属性遍历，不把映射容器内部的值形状纳入
  数据面判定。
- **代价**：流程若合法地把一个方法 / 类 / 模块**当值**读取（例如
  `$NODE.obj.some_method`），现在会抛错；请改为返回/读取该字段背后的数据值。
  这是把安全收紧落在攻击面、同时保留普通数据读写的取舍。

运行时文案（**逐字**，测试与表达式指南引用同一串）：

    expression path '$NODE.h.os' traverses a non-data object of type 'module': attribute traversal is data-only (plaita#133), so modules, functions, builtin functions, methods, classes, frames, tracebacks, generators, coroutines, async generators and code objects are refused by name. Read a data field instead; mapping keys and numeric indices are unchanged.

- **异常类型**：`ValueError`（与 #55 同层、无新增公共异常类）；在 `end` 节点
  `output` 路径上表现为 `NodeExecutionError`，`__cause__` 为该 `ValueError`。
- **测试 / 负控**：`tests/unit/test_issue133_data_only_traversal.py`（每种代码
  对象的值位置 + 基座位置；四根对称；数据正控；全路径/类型/不回显断言）；
  负控把 `_get_attr` 的两处闸门关掉后，这些断言必须转红，记录见
  `docs/proofs/133-data-only-traversal.md` 与 `docs/proofs/data/`。

### 表达式：映射容器只允许数据——host mapping-like 具名拒绝（plaita#203）

#133 关闭了**属性**遍历（`$NODE.h.os.environ`），但映射面仍有两条路：容器作值
（`$NODE.h.env` / `$NODE.d.env` 返回 live `os._Environ`，随后的 `.HOME` 走映射
键读取）与 `$F.get` 旁路（`$F.get($NODE.h.env, 'HOME')` 直接调
`_Environ.get`）。本版把**映射容器**也收紧为只允许数据：

- **允许**：真实 `dict`（含子类）与引擎自有状态 mapping（`CheckpointState`
  以及 `$PARENT` 的内部惰性快照 `_LazyRootSnapshot`）。dict 子类一律走
  `dict.get` / `dict.keys` 等 C 槽——子类覆写的 `get` / `__getitem__` /
  `__getattribute__` **不会被调用**（与 #69 r13 的 `$F.keys`/`values`/`items`
  同一纪律）；引擎状态走自己的 `get`。
- **拒绝**：`os._Environ`、`collections.ChainMap`、`collections.UserDict`、
  `types.MappingProxyType`、自定义 `Mapping`、以及 duck-typed
  `__getitem__` + `get` 的 host 对象——在**入口（作基座）/ 结果值 / 索引结果 /
  根值**四个位置都抛具名 `ValueError`（含路径、真实类型与 `plaita#203`），
  在读取任何键/值之前终止。
- **`$F.get` / `$F.getDictValue`**：第一个实参是 host mapping-like 时，在 `#68`
  输入测量和 callee **之前**拒绝；返回值是 host mapping-like 时也拒绝。真实 dict
  走 `dict.get` C 槽，`CheckpointState` 走引擎 `get`；只加 `get` 方法、但没有
  `__getitem__` 的普通对象保持历史 `get` 行为。
- **数据面不变**：`$NODE.d.k`、`$INPUT.x`、`$F.get($NODE.d, 'k')`、dataclass /
  Pydantic / SimpleNamespace、`CheckpointState` 背书的 `$INPUT`、`$ENV` 的
  `exposeEnv` allowlist 语义都未改动。`$F.keys` / `values` / `items` 对非 dict
  容器的拒绝沿用 #69（同一结果，只是文案不同）。

**兼容性裁决 / 迁移**：

- `UserDict` / `ChainMap` / `MappingProxyType` / 自定义 `Mapping` 不再能作为
  表达式映射容器读取；请在交给流程前转成 plain dict（如 `dict(value)`；需要
  深拷贝时用 `$F.json_loads($F.json_dumps(value))`），或改用 `$ENV` 显式暴露。
- 只加方法的 `dict` 子类**保持可用**（读真实存储；覆写的读写方法不执行）。
- `CheckpointState` 只允许**精确类型**；宿主子类覆写 `get` 会被当作 host
  mapping-like 拒绝。
- 需要环境变量时走 `$ENV` + `exposeEnv` 显式白名单，不要依赖 `$NODE.*` 读宿主
  环境。

运行时文案（**逐字**，测试与表达式指南引用同一串）：

    expression path '$NODE.h.env' read a non-data mapping container of type '_Environ': only plain dict (including subclasses, read through dict's C slots) and the engine's CheckpointState may be traversed as a mapping (plaita#203). Host mapping-like objects such as os.environ / ChainMap / UserDict / custom Mapping are refused by name before any key or value is read; convert the data to a plain dict first.

- **异常类型**：`ValueError`（与 #55/#133 同层、无新增公共异常类）；在 `end`
  节点 `output` 路径上表现为 `NodeExecutionError`，`__cause__` 为该
  `ValueError`。
- **测试 / 负控**：`tests/unit/test_issue203_data_mapping_traversal.py`（容器作值/
  基座/索引/根值；`$F.get` 两处；poisoned mapping 零调用；数据正控；
  CheckpointState / dict 子类兼容性；`$ENV`）。负控把映射容器闸与 `$F.get`
  闸关掉后，这些断言必须转红，记录见
  `docs/proofs/203-data-mapping-traversal.md` 与 `docs/proofs/data/`。

### http 节点：`$NODE.<http_id>` 由**裸响应体**变为**结果结构**（plaita#129）

`http` 节点历史上把响应上下文用 `set_state("$NODE.<id>.RESPONSE", …)` 写入，但状态层
是**扁平键**存储，而表达式寻址 `$NODE.<id>.RESPONSE` 走**嵌套路径** ⇒ 文档承诺的字段
引用**永远取不到**（静默 `None`，或根不存在时抛误导性的 `KeyError: '$NODE' not found`）。
实测也证明：不存在「`$NODE.<id>` 是裸响应体 **且** `$NODE.<id>.data` 能解析」的设计——
点路径要求 `$NODE.<id>` 是字典。

- **变更**：`$NODE.<http_id>` 现在是该节点的**结果结构**
  `{data, status, statusText, headers}`（字段名以
  `docs-site/docs/nodes/builtin.md` 的承诺为准），而**不再是裸响应体**。
- **取响应体**：改用 **`$NODE.<http_id>.data`**（文档原本承诺的写法）。
  取状态码用 `.status`，响应头用 `.headers`。
- **只影响直接读整节点的地方**：形如 `End(output="$NODE.fetch")` 的引用，从"得到响应体"
  变为"得到上述结构"。**节点自身的返回值不变**——无 `output` 时仍返回响应体本身，
  设了 `output` 表达式时仍是该表达式求值结果（next 链路与节点级消费不受影响）。
- **`data` 的语义**：恒为该节点的**值**。设了 `output` 表达式时，`data` 是表达式结果，
  原始响应体另存于 **`$NODE.<http_id>.body`**（避免同一个键既指节点值又指原始 body）。
- **涉及面**：console 的 `http-echo` 示例（会 seed 进用户工作区）已同步改为
  `$NODE.fetch.data`。

### 表达式：引用了父容器缺失根键的 `$NODE.<节点>.<字段>` 数据文本不再被二次求值（plaita#104）

求值变量路径的中段字段时，历史上会把字符串属性值无条件当表达式、并以**持有它的
父容器**（不是流程根上下文）为 context 递归求值。字段值若是含 `{% ... %}` 的
**数据文本**（LLM/agent 输出原文、文件内容、prompt 回显），只会得到两种坏结局：
引用了父容器里没有的根键时抛**归因错误**的 `KeyError`（说某个根键不存在，而它在
顶层上下文里明明有）；父容器恰好能"兜住"这个键时**静默改写**文本。

- **已修（方案 A）**：递归前对字段值做一次**根键扫描**——只认 `$INPUT` / `$NODE` /
  `$GLOBAL` / `$PARENT` / `$ENV` / `$FLOW_ID`（含 `$FLOW` 别名）；字符串字面量里的
  `$...`、`$F` 函数命名空间、以及段名位置的 `$`（`$PARENT.$INPUT.name` 的第二段）
  都不算引用。**只有这些根键全部存在于持有该字段的父容器里才递归求值**，否则按
  普通数据原样返回。即：**"引用了父容器不存在的根键的数据文本"从（静默改写 /
  归因错误的 KeyError）改为原文返回**。
- **默认语义（本版不变）**：`$NODE.<节点>.<字段>` 仍会对"看起来像模板"的字符串叶子
  递归求值。**仍然会被求值**的情形只有两类：① 不引用任何上下文根的模板
  （`"{% $F.eq(1,1) %}"` → `"True"`、`"{% $F.now() %}"`、`"{% $F.concat('a', 'b') %}"`）；
  ② 引用的根键**全部**存在于持有它的父容器里的模板。本版**不**引入基于字段名
  （如 `text`）或"有无字面量"的启发式——那只会把"静默改写"换成"静默不求值"，
  并把业务字段名硬编码进核心解析器。要取数据原文请用显式访问器（见下一节）。
- **迁移**：默认无需改动。只有依赖「把 `"$INPUT.x"` / `"{% ... %}"` 存进节点输出、
  再由下游 `$NODE.<id>.<field>` 引用时自动求值」的流程受影响——该字符串现在原样
  返回。这类"嵌套表达式字符串"必须以**父容器自带根键**的形态存储（父对象里有
  `$INPUT` / `$NODE` / `$GLOBAL` …），例如
  `{"$INPUT": {"name": "$NODE.first", "$NODE": {"first": "ada"}}}`；否则请在写入时
  就求值，或改用显式 `$F.*` 调用。
- **其他可见变化**：
  - `defaultdict` 父容器：基线会因 `container["$INPUT"]` 触发 `__missing__` **自动建键**
    并让模板求出 `'None'`（同时污染父容器）；方案 A 只做 `in` 探测，改为原文返回且
    **不再有建键副作用**。依赖这个"自动建键"副作用的流程需显式预置该键。
  - **归因文案**：逃过扫描的嵌套求值失败（例如未知根 `$CUSTOM`）不再报成"顶层上下文
    缺键"式的误导信息，而是指明字段路径（`$NODE.llm.text`）、父容器缺键与"嵌套字符串
    按父容器求值"的语义；上游错误按原文嵌入（不再被 `KeyError.__str__` 多套一层引号），
    且为不重复打印递归 traceback 保留了 `raise … from None`。
  - `_container_has_root` 的"属性兜底"分支（`hasattr(obj, "$INPUT")`）仅对显式
    `setattr` 前缀键名的容器成立，属与 `_get_attr` 对齐的防御分支；真实容器
    （`dict` / `CheckpointState`）走 `__contains__` / `.get`。

### 表达式：`$NODE.raw.<节点>[.<路径>…]` 显式取原文（plaita#130）

上一节的残留项（不引用任何根键的模板仍会被求值）**不改默认语义**，改为新增显式
访问器——"这个字段是数据"由写表达式的人自己声明，解析器不做任何字段名/字面量启发式：

- **`$NODE.raw.<节点>[.<路径>…]`**：整条路径上的字符串**一律不求值**、原样取回
  （#104 的根键扫描与递归都被绕过）；非字符串值（int/float/bool/None/list/dict）
  与 `$NODE.<节点>…` 完全一致。例：
  `{"$NODE": {"llm": {"text": "{% $F.eq(1,1) %}"}}}` 下
  `$NODE.raw.llm.text` == `"{% $F.eq(1,1) %}"`，而 `$NODE.llm.text` 仍是 `"True"`。
- **`$NODE.<节点>…` 语义不变**：上一节列出的两类情形照旧求值。错误面逐字保持，但
  三种情形要分开说：缺节点 / 缺路径 → 两形态都得到 `None`；整个 `$NODE` 根不存在
  → 两形态是同一条 `KeyError` 文案；**逃过扫描的嵌套求值归因 `KeyError` 只发生在
  非 raw 路径**——`raw` 不递归，直接返回原文（这是访问器的本意，不是行为变化）。
  没有新增诊断或异常类型。
- **命名冲突（确定规则）**：`$NODE` 后第一段**仅当** `$NODE` 容器里没有同名 id、
  且其后紧跟一个字段段（节点 id 位）时才被当作 `raw` 修饰符。id 恰为 `raw` 的节点
  产出后，`$NODE.raw.<路径>` 仍指那个节点（存量语义优先，并发出 once-per-string
  的 WARNING 点名冲突与访问器写法）；该类流程用不了这个访问器，需给节点改名。
  `$NODE.raw`（无后续字段段）与 `$NODE.raw.0` / `$NODE.raw[0]`（节点 id 位是索引
  而非字段）都按今天的语义解析。
- **运行期窗口（显式行为，不是暗坑）**：修饰符判定必须看**当次**的 `$NODE` 容器，
  所以"flow 声明了 id 为 `raw` 的节点、但该节点还没产出"时容器里没有该键，
  `$NODE.raw.<其他节点>…` 会按**访问器**语义解析（取原文、无 WARNING）；节点产出后
  同一表达式翻转为节点语义（并多一条冲突 WARNING）。两态在旧版本下都得到 `None`
  （静默无输出），本版把这条窗口固化成文档 + 测试锁定。
- **尚未覆盖（后续工作）**：`$INPUT` / `$GLOBAL` / `$PARENT` / `$ENV` 没有对应的
  `raw` 修饰符；节点级的"输出即数据"声明（issue 里的 option 1）也未在本版实现。
- **迁移**：默认无需改动（`raw` 是新增语法；对存量流程唯一可见的变化是上面那条
  冲突 WARNING）。要把 LLM/文档文本当数据消费时，把 `$NODE.<id>.<field>` 改写成
  `$NODE.raw.<id>.<field>`。

### HTTP 节点 SSRF 加固（plaita#31）

HTTP 节点的 `allowedHosts` / `deniedHosts` / `blockPrivateNetworks` 此前**只有
节点级**开关且 `blockPrivateNetworks` 默认 `False`——多租户部署没有任何运营者级
强制；开了策略也能被 DNS rebinding 绕过（校验解析、连接池再独立解析一次）；响应
体 `response.text` 全量读入内存。本次三处加固：

1. **运营者级开关 `PLAITA_HTTP_BLOCK_PRIVATE=1`**：置位后所有 HTTP 节点强制
   `blockPrivateNetworks`（effective = 节点声明 OR 运营者开关）——节点声明只能
   更严，不能更松。默认不设，保持历史行为；**多租户 / 不受信流程部署请显式置位**。
2. **DNS pinning**：策略激活时「建连解析本身」经同一份策略判定，且只连接校验过的
   IP（aiohttp 自定义 resolver / requests 自定义 connector），校验与连接同源，
   攻击者权威 DNS 无法先答公网过校验、连接时答 `127.0.0.1` / `169.254.169.254`
   穿透。Host 头与 TLS SNI 仍是原域名。
3. **响应体字节上限**：默认 **10MiB**（`PLAITA_HTTP_MAX_RESPONSE_BYTES` 可调），
   超限抛错而非把 GB 级 body 读进内存 / `$NODE.<id>.RESPONSE` 状态 / checkpoint。

- 变更前：默认无限制；`response.text` 全量读；策略易被 rebinding 绕过。
- 迁移：默认仅新增 10MiB 响应上限——确需更大 body 的部署显式调大
  `PLAITA_HTTP_MAX_RESPONSE_BYTES`。多租户部署显式设
  `PLAITA_HTTP_BLOCK_PRIVATE=1`。策略激活的请求不再复用 flow 作用域共享
  `ClientSession`（改用带 pinning resolver 的连接），属安全换性能的取舍。

复审补丁（2026-10 独立评审，同一组加固的收敛）：

- **重定向不再复用客户端自动跟随**：四条路径组合（sync/async × 策略激活/默认）
  统一在节点侧逐跳手动跟随（中间跳 body 直接丢弃不读）——requests 自动跟随会在
  `resolve_redirects` 里无条件读尽每个 3xx 的 body（无上限），敌意 302 可绕过
  10MiB 上限。逐跳规则逐条对齐 requests
  （`rebuild_method` / `should_strip_auth` / `purged_headers`）：

  | 情况 | 语义 |
  |------|------|
  | 302 / 303 | 非 HEAD 转 GET，丢弃 body 与 `Content-Type`/`Transfer-Encoding` |
  | 301 | **仅 POST** 转 GET；PUT/PATCH/DELETE/OPTIONS 保留方法，但同样丢弃 body（requests 行为） |
  | 307 / 308 | 保留**方法与 body**（sync/async 一致） |
  | 无 `Location` 的 3xx | 不算重定向，按普通响应返回（requests `is_redirect` 口径） |
  | 跨源（host 变化，或同 host 的 scheme/port 变化） | 剥离 `Authorization`/`Cookie`/`Proxy-Authorization` 等凭据头 |

  跨源的判定是 requests 的 `should_strip_auth` 口径：同 host 的 `https→http`、
  `:443→:8443`、`非默认端口→默认端口` 都算跨源（唯一例外是 `http:80 → https:443`
  的同 host 升级）；此前只比 hostname，会把凭据明文转发到另一个 origin。

  **与自动跟随不等价的行为变化**（需读写两侧对齐的部署注意）：

  * **重定向上限统一为节点 `maxRedirects`（默认 5）**，sync/async 同口径、同文案
    `Too many redirects (> N)`。此前默认路径用 requests 的 30（同步）/ aiohttp 的
    10 且规则不同（异步对 301/302 的非 POST 方法会保留 body），而 `maxRedirects`
    只在策略激活时生效——同步默认路径被静默收紧到 5 且与异步不一致。需要更长
    链条的流程显式调大 `maxRedirects`。
  * 不再填充 `response.history`。
  * 不再把 session cookie jar 合并进重定向跳（`PLAITA_HTTP_COOKIES=1` 时可见差异）。
  * 中间跳的 3xx body 不再被读取（自动跟随会读尽）。
- **策略激活的同步请求忽略代理环境变量**（`trust_env=False`，与 aiohttp 侧共享
  session 一致）：`HTTP_PROXY`/`HTTPS_PROXY` 不再把 restricted 请求交给代理——
  经代理时目标解析与建连都在代理侧，节点侧建连 pinning 无从生效。egress 依赖代理
  的部署请改为直连，或在代理侧做同等校验。
- **body 读取阶段失败的错误码对齐**：截断 / 连接重置 / 解码失败等传输类失败
  sync/async 同归 **1002**（`DO_REQUEST`，错误帧带请求快照、无响应）；响应体超限
  （`ResponseTooLargeError`）两条路径同归 **1003**（`NODE_EXEC`，错误帧带响应快照）。

### writefile 写入 jail 默认开启（2026-10）

**部署入口注入 `PLAITA_NODES_WORKSPACE_ROOT`**（plaita#39）：`writefile` 节点
（plaita-nodes）的路径 jail 此前**默认关闭**，且 worker / console 启动代码从不
设置该变量——能提交流程 JSON 的人（或被注入的表达式求值结果）可写 worker 进程
可写的任意路径（含绝对路径与 `../` 穿越），是持久化 RCE 原语。worker / console
启动时现在注入该变量，未显式配置则 fail-closed 推导默认根
（`PLAITA_PROJECT_ROOT` → worker 工作目录 → 家目录；`/` 不构成边界）。

- 变更前：不设即任意路径可写；只有节点级 `workspace_root` 或运营者手工设环境
  变量才受约束。
- 迁移：多租户 / 不受信流程部署显式配置 `PLAITA_NODES_WORKSPACE_ROOT`（收敛到
  业务仓或沙箱目录）。单机信任部署若流程需写默认根之外的绝对路径（如自迭代
  run_dir 不在默认根内），设 `PLAITA_ALLOW_UNRESTRICTED_WRITES=1` 回到历史
  行为——该开关只该用于单机信任环境。

### writefile jail 默认根不再取引擎自身 checkout（2026-10，plaita#51）

默认根推导多一步**归一**（`plaita/writefile_jail.py`）：候选目录若落在引擎自身
的 checkout 内（含其子目录），改用该 checkout 的**父目录**——worker / console
常从自己的 plaita clone 启动，把 clone 当写入根会让**跨仓** run 写不出产物（目标
仓是它的兄弟目录）。

- 变更前：Mac worker `cwd=<...>/infra4agent/plaita` → jail 根 = 该 clone；自迭代
  flow 写目标仓 `run_dir` 的 `land-failure.log` 直接 `escapes workspace_root`，
  节点确定性失败、重投耗尽后 failed-preserved，冲突现场丢失（VM worker
  `cwd=/home/ubuntu` 覆盖 `/home/ubuntu/projects/...`，故只在 Mac 侧暴露）。
- 变更后：默认根 = `<...>/infra4agent`（该 clone 的父目录 = 部署根），与 docker
  档 `PLAITA_PROJECT_ROOT=/app` 同一层级语义；同仓 run（run_dir 在 clone 内）本
  就在父目录内，不受影响；`/etc/...` 与 `../` 逃逸仍被拒。
- 迁移：无需动作。要**更窄**的根（如只放业务仓）仍用显式
  `PLAITA_NODES_WORKSPACE_ROOT`——显式配置原样使用，不做归一。

### js 语言接入沙箱档位体系 + 语言白名单默认只放行 python（plaita#29）

`language: "js"` 此前**完全绕开**沙箱档位：`Runners` 命中即走 PyExecJS（subprocess
拉起外部 JS 引擎，CommonJS `require` 可用），无 timeout、无 cancel，而
`sandbox_backend` / `allowed_backends` / `_PYTHON_BACKENDS` 全部只作用于 Python。
运营者配了 `allowed_backends=("docker",)`，流程作者写一行 `language: "js"` 仍能在
宿主上执行任意代码（可读 worker 全量 env），死循环 JS 还会挂死 worker 步骤。

- 变更前：js 忽略 `sandbox_backend`，一律 PyExecJS 裸跑。
- 变更后（两层）：
  1. **语言白名单默认 fail-closed**：`register_code_node(allowed_languages=...)`
     默认 `("python",)`，js 节点在**解析期**被拒
     （`language='js' is not allowed by the operator`）。worker / console 经
     `PLAITA_SANDBOX_ALLOWED_LANGUAGES`（逗号/冒号/空白分隔，未配置 → 只 python）
     配置，放行 js 时启动日志打 WARNING。
  2. **js 按 `sandbox_backend` 走档位表**：`docker`（`node:20-alpine` 容器，
     `--network none` / 只读 FS / 资源与 pids 上限）/ `subprocess`
     （`node -e`，10s 墙钟 + env 白名单 + 超时或取消时整组击杀）/ `unsafe`
     （历史 PyExecJS，无隔离无超时）。`restricted` 没有 js 实现，声明它会在解析期
     报错（不静默降级）。可用 `PLAITA_SANDBOX_DOCKER_NODE_IMAGE` /
     `PLAITA_SANDBOX_NODE_BIN` 指向内网镜像或自定义 node。
- 迁移：含 js 节点的部署显式放行——`PLAITA_SANDBOX_ALLOWED_LANGUAGES=python,js`
  （并确保生效后端有 node：docker 需预拉 `PLAITA_SANDBOX_DOCKER_NODE_IMAGE`，
  subprocess 需 PATH 上有 node）。不需要 js 的部署保持默认即可。库调用方自己接线时用
  `register_code_node(allowed_languages=(...))`，或经
  `plaita.node.resolve_sandbox_allowed_languages()` 取同一口径。同时
  `register_runner()` 不再能替换内置语言 `python` / `js`（会抛 `ValueError`）——
  内置语言必须走档位表；自定义语言不受语言白名单约束。

### code 沙箱后端白名单在生产入口默认开启（plaita#22）

`CodeNode.sandbox_backend` 由**流程 JSON** 逐节点声明，而流程 JSON 来自流程作者：
不设白名单时，作者写一行 `"sandbox_backend": "unsafe"` 就能在 worker / console
进程内 raw `exec` 任意代码（可读 `PLAITA_CREDENTIALS_KEY`、Redis/DB 凭据）。
机制（`register_code_node(allowed_backends=...)` + 解析期硬拦）2026-09 评审就
已落地，但 worker / console 两个部署入口都不传——防线只存在于注释里。

- 变更前：生效白名单为 `None` = 不限制；`unsafe` / `restricted` / `subprocess`
  均可被流程作者逐节点选中。
- 变更后：worker / console 启动时按 `PLAITA_SANDBOX_ALLOWED_BACKENDS`（未配置则
  `docker`）∪ 生效默认后端（worker 的 `PLAITA_CODE_BACKEND`，默认 `subprocess`；
  console 默认 `docker`）施加白名单，白名单外的 `sandbox_backend` 在**解析期**
  被拒（`sandbox_backend=... is not allowed by the operator`）。白名单含 `unsafe`
  时启动日志打 CRITICAL，未显式配置时打 WARNING。
- 迁移：含以 `unsafe` / `restricted` 为生效后端的部署需显式放行——
  `PLAITA_SANDBOX_ALLOWED_BACKENDS=unsafe`（仅单机信任环境）或改用
  `PLAITA_CODE_BACKEND=docker`。多租户部署保持默认（`docker`）即可。
  详见 [运维 Runbook · code 沙箱后端白名单](docs-site/docs/distributed/ops-runbook.md#code-沙箱后端白名单)。

### Storage（experimental sqlalchemy）：DDL 迁移由手工 `ALTER TABLE` 改为版本化脚本

**变更前**：`SqlalchemyExecutionStorage` 建表走 `Base.metadata.create_all`——只建
缺失的表、**不补列**。存量库新增列（如 `execution_states.flow_hash` /
`tenant_id`）靠注释指引手工 `ALTER TABLE`；漏做则 INSERT 路径 `**state_dict`
展开对未知列直接 `TypeError` → save 返回 False。

**变更后**：`plaita/storage/sqlalchemy.py` 落地最小版本化迁移
（`SCHEMA_MIGRATIONS` + `schema_migrations` 表）。存储初始化 `create_all` 后
按版本序应用未执行的迁移，对已有列 no-op（幂等）。**无需人工 DDL**；
新建库与存量库路径统一。此机制仅覆盖该 experimental 后端，不影响 memory/redis。

### Storage：redis 执行列表新增 `{ns}:execution:index` ZSET

**变更前**：`RedisExecutionStorage.list_executions` 用 `scan_iter` 全命名空间扫键，
再逐键 `GET` + 完整 `model_validate`（含整个 context），排序分页在 Python 内存做。

**变更后**：save/delete 同步维护 `{ns}:execution:index` ZSET
（score=`start_time` epoch、member=`execution_id`；fenced 的 CAS Lua 内一并 ZADD）。
列表分页下推 `ZRANGE`/`ZREVRANGE`，只反序列化本页。存量库首次列表时懒回填
（一次性，置 `{ns}:execution:index:ready` 标记）。新增同前缀机制键
（`execution:index` / `execution:index:ready`）——console 列表路径已登记排除。

`list_executions` 签名与返回结构不变，但有两处调用方可见的变化：

- **默认排序确定化**：原先是 `scan_iter` 的任意顺序，现在恒为 `start_time`
  升序（`order_by` 语义不变）。
- **默认 `limit=100` 只覆盖最旧的 100 条**：需要遍历全部执行必须翻页
  （`scripts/reap_zombie_executions.py` 已改为翻页）。

**滚动升级窗口（运维须知）**：`{ns}:execution:index:ready` 由首个列表调用置位且
永不过期，回填只覆盖置位**之前**写入的执行。置位之后由**尚未升级**的 worker
（仍走裸 `SET`、不 ZADD）写入的执行没有索引成员，也不在回填范围内——它们会
一直从 `list_executions`（console 列表）缺席，直到该 worker 升级。状态键本身
完好，`load_execution_state` / resume 不受影响；需要刷新列表的话，升级完成后删掉
`{ns}:execution:index:ready` 触发一次全量回填即可（execution_id 未被索引的键会
被重新扫描收录）。

### 表达式内建 mutate 函数对五个持久化根就地改写 ⇒ 具名硬报错（plaita#69，r3）

内建「就地改写」函数（`$F.pop` / `$F.delListItem` / `$F.setListItem` / `$F.set` /
`$F.delete` / `$F.clear` / `$F.setDictValue` / `$F.delDictValue` / `$F.clearDict`）
收到的参数是 live 执行上下文里的**对象引用**。此前只要它们出现在循环条件之外的任何
求值点（`assignment.output`、`end` / `http` 的 `output`、`code` 的参数、`child.input`、
`concurrent`、`redis`、`event_node` 的参数），就地改写的就是
`$INPUT` / `$NODE` / `$GLOBAL` / `$PARENT` / `$ENV` 本体（**含任意子路径**，五个根全部
经 `CheckpointState._field_to_key` 落 checkpoint）；改写结果被
`to_dict()` / `to_checkpoint_dict()` 序列化，恢复后仍然是脏的，重试不会自愈。

现在：**目标路径的根是这五个持久化根（别名一并覆盖）、且求值上下文是 live
`CheckpointState` 时，在求值前抛 `ValueError` 并点名完整路径**。例：

```
$F.pop($INPUT.items, 0)
→ ValueError: expression '$INPUT.items' cannot be the target of the in-place
  mutator $F.pop: '$INPUT.items' is a live path into the checkpoint-persisted
  execution context (root '$INPUT' is one of $INPUT/$NODE/$GLOBAL/$PARENT/$ENV,
  aliases included) … Refusing before evaluation (plaita#69).
```

#### r2 起的**破坏性**收紧：不只目标位，**任意实参位**都拒绝

把活根当**值**传进 mutate 函数（`$F.set($VAR,'k',$INPUT)`）会把根对象存进非根键，之后
一条普通表达式（`$F.set($VAR.k,'x','y')`）就能经别名就地写穿 `$INPUT` 并落盘。所以从
r2 起，**任意实参位**（arg0 目标 / arg1 key / arg2 value / index）只要静态解析成五个持久
根的路径（含别名），一律在求值前**具名拒绝**：

```
$F.set($VAR,'k',$INPUT)              → ValueError（argument #3 … '$INPUT' …）
$F.set($VAR,'k',$F.get($INPUT,'cfg')) → ValueError（callee 的实参触达根）
$F.pop($VAR.items,$INPUT.items[0])   → ValueError（argument #2 …）
```

**这是破坏性变更**：base 上这些表达式是静默放行且会写穿的，升级后必须显式派生副本。

#### r3：浅 producer 的值参**去别名**（不再拒绝），callee 子句只看实参可达性

r2 把 `slice` / `reverse` / `append` / `sort` / `values` / `items` / `keys` /
`getDictValues` 等当作"fresh producer"直接放行，但它们只是**浅拷贝**：容器是新的、**元素
仍指向根的子对象**。于是下面这条在 r2 上仍能写穿：

```
$F.set($VAR,'k',$F.slice($INPUT.items,0,1))   # r2 允许 → $VAR.k[0] is $INPUT.items[0]
$F.set($VAR.k[0],'x',9)                        # → $INPUT 被就地改写并落盘
```

r3 把这类 callee 标成 **SHALLOW**（新容器、元素别名），并分位置处理：

- **目标位**（arg0）放行：9 个 mutator 只增删改**新容器自己的条目**，碰不到根；
- **值参位**：若该 callee **自身的实参**可静态触达持久根 ⇒ **深拷贝后再交给 mutator**
  （`copy.deepcopy`；dict view 先物化成 list）。因此上面的反例不再共享任何对象：
  `$VAR.k[0] is $INPUT.items[0]` 为 `False`，`$INPUT` 与 checkpoint 均不变；
  浅 producer 值参**不被拒绝**，合法用法（含 `$F.set($VAR,'total',$F.add($INPUT.n,1))`
  这类标量累加，`add`/`mul` 是标量/列表双形态）继续可用。

同时 r3 **收窄 callee 子句**：`get` / `getDictValue` / `getListItem` / `ifelse` / `and` /
`or` 及未声明 producer 只在**它们自己的实参**能触达持久根时才拒绝。base 上合法、r2 误拒的
两条现在恢复放行并按非根就地语义生效：

```
$F.set($VAR,'k',$F.get($VAR,'x'))   # 放行（表达式里没有任何持久根）
$F.set($F.get($VAR,'x'),'k','v')    # 放行
```

**保留不变（就地语义没有被改成 copy-on-write）**：

- 非根上下文键（如 `$VAR`）与循环条目/局部值**仍然就地可改**（收紧只落在攻击面）；
  `$F.slice($VAR.items,…)` 作值参仍与 `$VAR` 共享元素；
- **循环条件路径逐字不变**：条件在 deepcopy 后的根副本上求值（上下文是普通 `dict`，
  不是 live state），`$F.set($GLOBAL, …)` 之类仍然只改副本 ——
  `tests/unit/test_loop.py::TestLoopConditionIsolation` 与
  `tests/unit/test_mfix_l_loop_context.py` 的契约不变；条件引用 `$LOOP-ITEM` 时该键也会被
  深拷贝，不是别名通道；
- **新对象结果可作目标**：DEEP 生产者（`json_loads` / `json_dumps` / `split` 及纯标量函数）
  与 SHALLOW 生产者（`slice` / `reverse` / `append` / `extend` / `insert` / `remove` /
  `sort` / `add` / `mul` / `addListItem` / `insertListItem` / `keys` / `values` / `items` /
  `getDictKeys` / `getDictValues`）以及 `register(..., fresh_producer=True)` 声明的自定义
  producer；`$F.set($F.json_loads($INPUT.raw), 'q', 1)` 与
  `$F.pop($F.slice($INPUT.items, 0, 3), 0)` **仍然可用**；
- **`get` / `getDictValue` / `getListItem` / `ifelse` / `and` / `or` 在实参触达根时
  不得作 mutate 目标**（live 引用会写穿五个根），报错并点名 callee + 路径；
- **未声明 / 未分类的 callee ⇒ fail-closed 拒绝**（`fresh_producer` 缺省 `False`，
  "忘记声明"的失败方向是**挡住**而不是放行）。

#### r4：容器键名不再能决定判决（结构收紧）+ 子流程边界去别名

r3 的嵌套容器判据是**规范根名白名单**（`_CANONICAL_ROOT_NAMES`）。verifier 的 r4 反例证明
它可以被 DSL-only 绕过：`InlineFlow.input = {"$myroot": "$INPUT"}` 里的 `"$INPUT"` 作为
**字典值**求值就是活根对象（不需要任何赋值/别名函数），活根因此被挂在任意键名下进了子流程；
子流程里 `assignment.output = "$INPUT.cmd"` 触发的嵌套串以子 `$INPUT` 为容器求值，
`$F.set($myroot.cfg,'k','v')` 就地写穿父运行态并落盘（b46f78e 与 r3 树上实测
`WRITE-THROUGH: True`）。r1→r3 三代都只看**名字**，所以逐代换轴（目标根名 → 实参位 →
别名/fresh producer → 容器键名）都绕得开。r4 改成**两层结构**，名字不再参与判决：

- **半边 1（边界去别名）**：交给子执行的 `$INPUT` 一律经 `_snapshot_value` 物化 ——
  `InlineFlow` / `ReferenceFlow` 的 `input`、集合节点（`Loop`/`Map`/`Filter`/`Find`/`Reduce`）
  的 `collection`、`Parallel` 分支输入（含未显式 `input` 时继承父 `$INPUT` 的那条路）。
  活根无法以**任何键名**按引用进入子执行，子流程只拿到私有副本（与 `update_node_result`
  同口径）。**可见副作用**：子流程里对 `$myroot.cfg` 的就地改写只改副本，父与 checkpoint
  零变化。
- **半边 2（身份探测）**：普通容器（嵌套表达式串的父容器）按**对象身份**传递探测容器里
  可达的值；只要某个值**就是**某个活 `CheckpointState` 的持久化对象（活根本体，或求值器
  沿根路径交出并登记过身份的子对象）⇒ **具名拒绝**，消息点名容器键路径 + 它触达的根。
  规范根名只作快速路径，且必须由身份坐实。活 CheckpointState 的**非持久化键**同样按
  身份判（宿主级别名，见下 T2-1）。

两半**各自可证伪**：回退半边 1 后，反例由半边 2 具名拒绝（`['$myroot']` + `$INPUT`）；
只留半边 1 时反例被物化隔离（父与 checkpoint 零变化、无写穿）。**行为变化**：数据容器里
挂在规范根名（如 `$GLOBAL`）下的**物化副本**从 r3 的"按名拒绝"变成 r4 的**放行且就地
生效**——名字不再决定；反之，任意键名下的**活**根/活根子对象从 r3 的"按名放行"变成
**拒绝**。逐格分类见 `docs/issue69-mutation-matrix.md` §4b（27 格容器级 + 54 格子流程级）。

**自证封闭（机械矩阵）**：`docs/issue69-mutation-matrix.md` 由
`tests/unit/test_issue69_mutation_matrix.py` 的同一生成器产出 —— 9 写形式 × 7 根（五规范
根 + 两个配置别名）× 6 路径形式 = **378 cell，实测 378/378 拒绝**（零写入、消息含完整
路径）；值参位 **294/294 拒绝**；浅 producer 值参 **112/112 去别名**（零共享可变对象）；
callee 实参触达根的值参 **42/42 拒绝**；非根 callee × 9 写形式 **72/72 不得因门被拒**；
live 引用 + 未声明目标桶 **72/72 拒绝**（r2 自报 54/54 漏了两个 undeclared 目标，已改正）；
producer × position 全表 **511 + 511 格枚举，0 格可制造别名、0 次根写入**；
r4 容器键名轴 **27 格容器级（18 拒 / 9 放行）+ 54 格子流程级**，同一 (值种类, 嵌套)
下三种键名判决完全一致（名字不决定）。
`=`/`+=`/`del`/字符串下标/`setattr`/`vars`/`eval`/`exec` 在表达式文法是**不可表达**的
（机械验证无写入）。**静态可判定而漏的 = 0**。

**T2 / 边界真话（r4 重写）**：
1. 宿主 Python 把某个上下文键设成持久化根的**同一对象**（`ctx.set_state("$V",
   ctx.get_state("$INPUT"))`）后写 `$V.cfg`：r3 记录为"静态不可判定、仍会写穿"；r4 按
   **对象身份**判非持久化键 ⇒ **已关闭**（`refuse`，`$INPUT` 与 checkpoint 零变化）。
2. **DSL 自己就能制造这种别名**（r3 的"表达式文法无法自行制造"声明**不成立**，已更正）：
   节点字段 `InlineFlow.input = {"$myroot": "$INPUT"}` 把活根挂在任意键名下，不需要任何
   赋值/别名函数。r4 的两层结构关闭了该通道（半边 1 隔离 / 半边 2 具名拒绝）。
   表达式**文法**仍然造不出赋值/别名（`=`/`+=`/`del`/`setattr`/`eval` 不可表达，见
   `docs/issue69-mutation-matrix.md` §5），但"文法造不出"不等于"DSL 造不出"。
3. 自定义 registry 把返回 live 引用的函数声明成 `fresh_producer=True` —— 声明是信任边界；
   默认注册表由清单守卫测试钉住（73 个名字必须四桶全分类，新增一个即红）。
4. 自定义 registry 注册**零实参**的未声明 producer —— 没有实参可做可达性推导，fail-closed
   拒绝（不静默放行）。

迁移写法：需要临时结构时显式派生副本 ——
`$F.set($F.json_loads($INPUT.raw), 'q', 1)`、`$F.pop($F.slice($INPUT.items, 0, 3), 0)`、
`$F.set($VAR,'k',$F.json_loads($F.json_dumps($INPUT)))`（需要把根存下来时用 JSON 往返
显式深拷贝）。`has_side_effects=True` 从本单起有**生产路径的运行时消费者**（求值前的门
+ 浅值参深拷贝），不再是装饰性标志。

### r5：就地 mutate 门反转为 **default-deny on provenance**（plaita#69 r5）

> **r6 修正**：本节里"**本次求值**由引擎新建"与"子流程/集合/`Reduce.initial` 的私有副本
> 在子执行里可继续就地改写"两句与 r5 的实现不符（r5 的 registry 是模块级、进程内的），
> 已被 verifier 的 C1/C2 反例证伪。真实的 r5 行为与 r6 的修正见紧接着的
> **r6** 小节。

r1→r4 的判据都是"**证明目标对象是活的**"（根名 → 实参位 → 别名 → 容器身份）。这个集合
**无界**：任意嵌套子对象、任意被当作用域的容器、懒快照、宿主对象——每轮关掉一个形状，
下一个就出现（r5 的 verifier 反例：嵌套表达式串的作用域是 `$INPUT.cfg` 的**活子对象**；
`Reduce.initial` 未物化，活对象以 `$INPUT[0]` 进子执行后写穿）。

r5 把谓词**反过来**：就地 mutate 的目标必须能证明是引擎新建的对象。**r5 的实际实现**
是一个**模块级、进程内**的 id LRU（容量 65536、弱引用优先但 dict/list/set 不可弱引用 ⇒
强引用持有）——于是"fresh"实际等价于"引擎**在这个进程里**造过它"（r6 修正，见下节）。
其余一切——活根、任意深度的活子对象、键名任意的容器、宿主对象、懒快照——一律
**具名拒绝**，消息含完整路径/根并说明 `provenance could not be proven fresh`。

登记点（作用域/物化面清单见 `docs/issue69-mutation-matrix.md` §S；r6 90 / **r7 87** 个
调用点，守卫测试 `tests/unit/test_issue69_scope_inventory.py` 钉住"新增未分类点即红"）：

* `ExecutionContext.evaluate` 对 list/dict 节点字段**重建的容器外壳**（只登记外壳，元素
  可能引用活对象）；
* DEEP producer（`json_loads`/`split` + `fresh_producer=True`）的**整图闭包**、SHALLOW
  producer 的**新容器本体**（元素可能别名，不登记）；
* 显式物化边界：子流程 `$INPUT`（`_materialise_child_input`）、集合
  （`_eval_collection`）、**`Reduce.initial`（r5 新增，修 X2）**、`Parallel` 分支输入、
  循环条件隔离副本（`_condition_context`）——**r7 更正**：前四处（子流程/集合/
  `Reduce.initial`/分支输入）的登记调用点在求值作用域之外、r6 起就是 no-op，r7 已**删除**
  （物化保留）；只有循环条件副本在自己的显式作用域内被登记；
* 浅 producer 值参的 `_detach_alias`（r6 起为引擎自有结构拷贝 `_engine_copy`，r7 起是
  数据路径**唯一**拷贝入口）。

**破坏性变更 1（owner 决策边界）**：非持久化键不再"按名字放行"。宿主直接
`ctx.set_state("$V", {...})` 交给引擎的是**来源不可证**的对象，r5 起写它会被具名拒绝：

```
ctx.set_state("$V", {"a": 1}); parser.evaluate("$F.set($V,'k','v')", ctx.context)
→ ValueError: … provenance could not be proven fresh …
```

r5 认为合法写法是把值交给引擎物化：`ctx.set_state("$V", ctx.evaluate({"a": 1}))`，或用
producer 结果 / `$F.json_loads($F.json_dumps($INPUT.cfg))`。**r6 修正**：`ctx.evaluate` 是
一次独立求值，其作用域返回即丢弃 ⇒ 该写法在 r6 下**不再**放行（这正是 verifier 的 C2）；
合法的替代是**同一次求值**内的 producer 结果。verifier 的 T2-1 变体
（`ctx.set_state("$V", ctx.get_state("$INPUT")["deep"])`，活子对象从未被当路径终点交出过）
在 r5/r6 同样被拒且零写穿。

**破坏性变更 2**：数据容器里挂**宿主自造副本**（`copy.deepcopy(...)`）从 r4 的"放行且就地
生效"变成**具名拒绝**（来源不可证）；引擎物化的副本（`ctx.evaluate` / producer）仍逐格
放行。r4 的 27 格容器轴在**零登记**状态下曾 9 格写穿（依赖测试的前置求值副作用），r5 在
零登记与有登记两套状态下判决一致（18 拒 / 9 放行 / 零写穿）。

**契约（r6/r7 修正后）**：循环条件的就地 mutate 仍是唯一合法 in-place 现场（隔离副本由
`_engine_copy` 现造、在自己的显式作用域内被登记）；**子流程/集合/`Reduce.initial`/
`Parallel` 分支的私有副本在子执行里不再可改写**——副本是父求值里物化的，子执行是另一次
求值 ⇒ 具名拒绝（r5 曾"只改副本"，r6 改为拒绝；r7 删除那四处作用域外的死登记，语义不变；
0 写穿不变）；浅 producer 值参继续去别名（r6 起用引擎自有结构拷贝，r7 起是唯一拷贝入口）；
`has_side_effects` 的运行时消费者不变。

**别名探测预算**：r4 的"容器身份探测预算耗尽 ⇒ 具名拒绝"在 r5 起**不再拒绝**（探测只作
辅助证据；非 fresh 的目标无论如何都会被 default-deny 拒绝）。因此合法大容器
（`$F.pop($VAR, 0)`，`$VAR` 是引擎物化的 >5 万项容器）不再被误拒；探测预算只约束遍历
成本，不改变安全方向。

### r6：freshness 收进**每次求值的作用域** + 引擎自有结构拷贝（plaita#69 r6）

r5 的谓词方向是对的，但**"fresh"当时的实现并不是文档说的那个意思**。verifier 的反例
全部归到这一点：

* **C2（B3 残留，r5 未文档化）**：r5 的 freshness 是**模块级、进程内**的 LRU
  （`_FRESH_IDS`，容量 65536，从不在生产路径清空），于是"fresh"实际等于"引擎在这个
  进程里造过它"。引擎物化的对象一旦被写进持久化状态，**另一次**求值仍可就地改写它：
  `o = ctx.evaluate({"a": 1}); ctx.set_state("$NODE", {"n1": o});
  parser.evaluate("$F.set($X,'k','v')", {"$X": o})` ⇒ r5 实测 `fresh=True` 且 checkpoint
  被改。副作用还有：最多 65536 个容器被强引用终生持有（`weakref` 分支是死代码——dict/
  list/set 本来就不可弱引用）、每次登记/查询都拿进程级锁、跨执行污染。
* **C1**：`_detach_alias` 把 `copy.deepcopy` 的**结果整体**登记
  （`note_fresh_closure(detached)`）。载荷里任一对象实现 `__deepcopy__` 返回 `self` 时，
  "去别名"根本不去别名——**活对象本身**落进 registry：
  `class Evil(dict): def __deepcopy__(self, memo): return self` ⇒ r5 实测
  `$VAR.k[0] is evil`、活 `$INPUT.items[0]` 与 checkpoint 双双被改。

**r6 的两处修改（不新增登记点）：**

1. **fresh ≡ "本次逻辑求值由引擎新建"**。登记面从模块级 LRU 换成
   `ContextVar` 作用域（`_FRESH_SCOPE`，`dict[id(obj) -> obj]` 强引用、查身份）：
   * **开**：`ExpressionParser.evaluate`（parser 公共入口）与 `ExecutionContext.evaluate`
     （节点字段入口；`Executor.evaluate` 委派后者）中最外层那个开；`Loop.execute` /
     `While._should_continue` 把"条件隔离副本物化 + `Condition.match`"作为**一次**逻辑
     求值显式开域。
   * **嵌套**：ContextVar 已有值时共享、**不重置**——嵌套表达式串、`parse_function`、
     容器分支递归、其中的一切 `note_fresh_*` 都落在同一次求值里。
   * **关**：开域者在 `finally` 里 `reset(token)` ⇒ 正常返回**与异常路径**都丢弃作用域
     （强引用随之释放）。
   * **域外**：`note_fresh_*` 是 **no-op**、`_fresh_has` 为 `False`（失败方向选安全侧）。
   * 因此 **A 里造的对象在 B 里必不 fresh** ⇒ C2 由构造拒绝；无进程级锁、无跨执行污染、
     无生产期无界持有（作用域内仍有 65536 上限，逐出只会更严）。
2. **只登记"确实是引擎造的"副本**。DSL 数据路径上 `copy.deepcopy` 换成引擎自有的结构
   拷贝 `_engine_copy`：只接受**精确类型** `dict`/`list`/`tuple`/`set`/`frozenset` 与标量
   （`type(x) is dict`，**不是** `isinstance`），其余抛**具名错误**
   `… which is **not part of the persistable data model** (…)`（点名违规类型）。自定义类、
   `dict`/`list` 子类（覆写 `__deepcopy__`/`__copy__` 或 `__getitem__` 有副作用）一律拒绝
   ⇒ "detached 副本"必然是本函数造出来的容器，登记它不可能登记到活对象（C1 关闭）。
   **r7 更正**：r6 对容器/标量**子类**的"一律拒绝"过宽——`Decimal`/`datetime`/`date`
   （`plaita/core/types.py::native_types` 明确声明可持久化）与 `OrderedDict`/`defaultdict`/
   `namedtuple`/`IntEnum`/`str`/`int` 子类都被误拒；r7 改为**归一化**，只拒绝真·非数据模型
   的可变对象。见下面的 **r7** 小节。

**破坏性变更 3（r6 收紧）**：**跨求值**的引擎对象不再可写。

```
o = ctx.evaluate({"a": 1})
parser.evaluate("$F.set($X,'k','v')", {"$X": o})     # r5: allow（写穿）；r6: refuse
ctx.set_state("$V", ctx.evaluate({"a": 1}))            # 求值 A 物化
parser.evaluate("$F.set($V,'k','v')", ctx.context)     # 求值 B：r5 allow；r6 refuse
```

合法用法改为**同一次求值**内物化并改写：`$F.set($F.json_loads('{}'),…)`、
`$F.set($X,'k',$F.json_loads($F.json_dumps($INPUT.cfg)))`、同一次求值里的循环条件副本。
浅 producer 值参的引擎结构拷贝（r3 F1）**在纯 JSON 载荷上不变**：值被真正去别名后再存，
两步都不写穿。

**子流程/集合/`Reduce.initial`/`Parallel` 分支的边界副本**（r5 说"子执行里可继续就地
改写"）在 r6 下**不是** fresh——副本是**父求值**里物化的，子执行是另一次求值。子执行里对
`$INPUT` 副本的就地改写因此**具名拒绝**（隔离性质不变：父运行态与 checkpoint 零写穿，
由"拒绝"而不是"改在私有副本上"保证）。循环条件仍放行（上面第 1 条的显式开域）。

**作用域/物化面清单**（`scripts/issue69_mutation_matrix/scope_inventory.{py,json}`）修了
verifier 报的两个缺陷 + 本轮新发现的第三个：键**既不含行号也不含列号**（只改缩进不再假红）；
`getattr(state, "evaluate")(v)` 间接分发归一到 `getattr.evaluate` 后分类；同缩进同形状的
调用点用遍历序 `#k` 区分（旧 col 键会静默吞掉后一个——清单因此从 87 涨到 **90**；
**r7 删掉 4 个作用域外的死登记、并把 `$GLOBAL` 快照改走物化边界 ⇒ 87**）。已知局限：属性名动态计算
（`getattr(state, name)(v)`）静态扫不到，由守卫测试的负控点名。

**成本**：目标位不再走 `_live_alias_index`（`$F.set($VAR,…)` 实测 13.72µs → **5.98µs**；
verifier 报的 23.5µs 路径同因）。该数出自 `scripts/issue69_mutation_matrix/bench_expr.py`
（30000 次/轮 × 7 轮取 min，default-deny refuse 路径）。**当前 revision（r11）的权威读数是
5.252µs**（本机负载 ~7.8–8.3，见 §P 右列；同一 revision 早一轮读数 5.647µs @ 负载
~7.3–8.1 —— 跨负载不可逐字比；r11 只改判据 / 具名化，不在被测热路径上）；
r10 的 5.871µs（负载 ~7.8–8.7）、r9 的 5.368µs（~7.0）、r8 的 5.350µs（~6.0）与 r7 的
5.85µs **都是历史值，只作对比、不称权威** ——
**绝对 µs 只做同机同轮对比**，跨机/跨负载不可比（verifier 在本机负载 10.19 实测 5.654µs）。
**不要做"整表按同一比例抬升"的推断**：验收者在自己的机器上实测 r10 列的
`$F.set($VAR,'k','v')` = **5.375µs @ 负载 8.73→8.07**，落在 r8 的 5.350 与 r9 的 5.368
之间 ⇒ 没有可测的 r10 成本；r11 的 §P 列与 r10 列的逐表达式差与负载方向不一致。
所有引用与 `docs/issue69-mutation-matrix.md` §P **同一列/同一口径**。`tests/unit/test_expression_provenance_perf.py`
的守卫 r6 起覆盖 mutator 路径，并用 checked-in 基线
（`scripts/issue69_mutation_matrix/perf_baseline.json`，比值 + tolerance）抓**常量因子**
回归；负控（人为放慢 40µs）必须红。完整数字与"不可再降"的说明（含 r7 更正的归因：
路径开销的大头是**整条 live-walk 记账**，`_register_live_objects` 单独只占 −4~−6%，
而 `_is_persisted_key`→False 省 −29~−32%）见 `docs/issue69-mutation-matrix.md` §P。

### r7：数据路径**单拷贝入口** + **单一类型表** + 边界死登记清除（plaita#69 r7）

r7 是**一致性**轮，不是新架构：把 r6 的三处"不统一"收敛成一条规则。

1. **数据路径只有一个拷贝入口**。`plaita/node/loop.py` 的循环条件隔离副本曾仍用
   `copy.deepcopy`；宿主类覆写 `__deepcopy__`/`__copy__` 返回 `self` 时，**活对象**会被
   放进每次求值的 freshness 作用域，条件的就地 mutate 写穿活 state 与 checkpoint
   （verifier Finding 1 的逐字反例：`Loop(collection=[1], condition=Condition(field=
   "$F.set($INPUT.items[0],'k','v')", operator="truthy"))` + `Evil(dict)`）。现在
   `_condition_context` 与 `_detach_alias` 走 `_engine_copy`（`loop.py` 为 ML1-C 性能钉
   保留模块级别名 `deepcopy = _engine_copy`，名字是兼容、语义不是 `copy.deepcopy`）；
   `ExecutionContext.setup_flow` 的 `$GLOBAL` 每 run 快照改走**物化边界**
   `_snapshot_value`（非容器宿主对象按引用共享——与 checkpoint / `$PARENT` / 子流程输入
   同一口径；此前 `copy.deepcopy` 会调用宿主 `__deepcopy__` 并让 MagicMock 之类的宿主对象
   参与拷贝）。**r8 更正**：这里 r7 当时写的"容器重建为精确类型、copy 钩子零调用"**不成立**
   —— 该实现用宿主方法读容器（`value.items()` / `enumerate(value)` / `value.copy()`），
   `set` 子类把 `copy()` 覆写成 `return self` 会让快照**就是活宿主对象**
   （`$GLOBAL.s is <flow 定义里的 set>`）；r8 改成基类协议重建后才为真，见下面的 **r8** 小节。守卫：
   `tests/unit/test_issue69_single_copy_point.py` 用 AST 扫描
   `plaita/`，除 `state.py::_LazyRootSnapshot.__deepcopy__`（协议钩子，带行内标记
   `# issue69-single-copy: protocol-hook`）外**禁止**任何 copy 模块调用；回归：
   `tests/unit/test_issue69_r7_uniformity.py::TestIssue69R7LoopConditionCopy`。
2. **单一类型表 + 单一判据**。接受/不可变集合从 `plaita/core/types.py::native_types`
   推导（`PERSISTABLE_SCALAR_TYPES`，`io.py` 的 `Property.default_value` 是其子集，由
   `test_issue69_persistable_types.py` 机械钉住）；容器/标量**子类被归一化成普通基类值**
   （`OrderedDict`/`defaultdict`/`Counter` → `dict`，`namedtuple` → `tuple`，`IntEnum` →
   `int`，`str`/`int` 子类按值），拷贝用**基类协议**（`dict.items(sub)`/`list.__iter__(sub)`…）
   因而宿主钩子一次都不调用；真·非数据模型的可变对象（自定义 `MutableMapping`、
   `bytearray`、不透明宿主类、dict view）仍具名拒绝。登记面与拷贝器消费**同一个**精确
   类型元组对象（`_ENGINE_COPY_CONTAINER_TYPES is _FRESH_WALKABLE`，判据 `type(x) in …`，
   绝不 `isinstance`）。**用户可见修复**：
   `$F.set($F.json_loads('{}'),'k',$F.add($INPUT.amount,1))` 在 `$INPUT.amount` 是
   `Decimal` 时 r5 OK / r6 `ValueError` ⇒ **r7 恢复 OK**（`datetime`/`date` 同理）。
3. **四个作用域外的死登记删除**。`child._materialise_child_input` /
   `loop._eval_collection` / `loop.Reduce._eval_initial` / `concurrent._resolve_branch_input`
   四处的 `note_fresh_closure` 在任何求值作用域之外，r6 起就是 no-op。r7 选择**删除**
   （而不是把整个子执行包进一个作用域——那会让"求值 A 造出的对象在求值 B 里仍 fresh"的
   C2 反例重新成立）。物化（隔离）保留；语义不变：子执行对边界副本的就地改写**具名拒绝**、
   父运行态与 checkpoint 零写穿。清单 `fresh-register` 10 → 6、`materialise` 13 → 14（`$GLOBAL` 快照走 `_snapshot_value`）、总数 90 → 87。
   **因果更正**：r6 文档把 `Parallel` 分支的拒绝归因于"`ContextVar` 不跨线程"——错；
   `mode="coroutine"` 在 MainThread 里同样被拒，根因是**调用点不在任何求值作用域内**
   （两种模式实测 `scope_active=False`）。

### r8：快照路径纳入**单拷贝纪律** + 假容器具名拒绝 + 循环条件过拒文档化（plaita#69 r8）

r8 是 r7 独立复核（PASS-WITH-GAPS）的收口：一条**规则级**缺口（G1）与三处小不一致
（G2/G3/G5）。不改架构，只把"引擎自有重建"这条规则**做成对**并用测试钉住。

1. **物化边界 `_snapshot_value` 也是"引擎自有重建"**（G1）。r7 的快照路径用宿主方法读容器，
   `set` 子类 `copy() -> self` 时快照**就是活宿主对象**（实测 `$GLOBAL.s is <flow 定义里的
   set>: True`），`dict`/`list` 子类覆写的 `items`/`__iter__` 也被真实调用。r8 起只走
   **基类协议**（`dict.items(sub)` / `list.__iter__(sub)` / `tuple.__iter__(sub)` /
   `set.__iter__(sub)` / `frozenset.__iter__(sub)` / 基类 `__len__`）：结果必然是现造的
   普通 `dict`/`list`/`tuple`/`set`/`frozenset`，宿主覆写的
   `copy`/`items`/`__iter__`/`__getitem__` **零调用**，快照与活对象 `is` 永不相等，产出
   因此满足 freshness 登记面的精确类型判据（`type(x) is …`）。守卫：
   `tests/unit/test_issue69_single_copy_point.py` 的兄弟守卫
   `snapshot_host_protocol_calls`（登记表 `_SNAPSHOT_PATH_REGISTRY`，allow-list = 接收者是
   C 基类名；负控覆盖 `value.copy()` / `sub.items()` / `enumerate(value)` 三种 r7 形态，
   对 r7 的 `state.py` 实测命中 5 处）；回归：
   `tests/unit/test_issue69_r8_snapshot_discipline.py` 覆盖 `$GLOBAL` / 子 `$INPUT` /
   `$PARENT` / checkpoint 四条路径 + 钩子计数器 0 + `is` 身份 + 真跑一次 `InlineFlow`。
   该文件同时回归既有的 `$GLOBAL` 自定义 `Mapping` 规则：**按引用共享 + 就地写具名拒绝 +
   0 变化**（它不是数据模型容器，重建语义不适用）。
2. **`__class__` 伪造的"假容器"具名拒绝**（G3）。宿主对象把 `__class__` 设为 `dict`/`list`/…
   能骗过 `isinstance`，却过不了 C 基类描述符（裸
   `TypeError: descriptor 'items' for 'dict' objects doesn't apply to a 'Liar' object`）。
   r8 起 `_engine_copy` 抛 `_EngineCopyRefused`（DSL 面翻译成
   `not part of the persistable data model`）、快照路径抛 `StateSnapshotError`，名字都取
   **真实** `type(x).__name__`；`dict`/`list`/`tuple`/`set`/`frozenset` 五种 + 标量归一化
   + 嵌套 + checkpoint 出口全覆盖，用户不再看到无归因的裸 `TypeError`。
3. **循环条件根含非数据模型对象 ⇒ 整根过拒（用户可见，选择 (a) 文档化）**（G2）。规则：
   循环条件的根只要含**任何**非数据模型对象（`bytearray` / 不透明宿主对象 / 自定义
   `Mapping`…），`_condition_context` 的 `_engine_copy` 对**整根**拒绝 ⇒ 该根保留活引用且
   **不登记** fresh ⇒ 该根内**任何**就地写都被 default-deny 具名拒绝，**包括兄弟纯数据
   子树**（实测载荷 `{"items": [{"n": 1}], "blob": bytearray(b"x")}` + 条件
   `$F.set($INPUT.items[0],'k','v')`：冒犯子树与 `$F.set($INPUT.cfg,'k','v')` 全拒、活对象
   0 变化；同一表达式对纯数据载荷仍放行）。要就地改就**先物化要改的部分**：
   `ctx.evaluate` / producer / `$F.json_loads($F.json_dumps(<part>))`。选择 (a) 而非 (b)
   （只拒冒犯子对象）：(b) 需要**部分物化**，即第二条拷贝实现，与第 1 条的单拷贝纪律冲突，
   并把"引擎自有 or 活引用"的二元 provenance 变成第三种状态，gate 的 default-deny 无法在
   不放宽登记判据的前提下表示它。回归：
   `tests/unit/test_issue69_r8_snapshot_discipline.py::TestIssue69R8LoopConditionNonDataModelRoot`。
   **该逃生口有损（r9/E 明说）**：`$F.json_loads($F.json_dumps(<part>))` 是 JSON 往返 ——
   `Decimal` / `datetime` / `date` / `bytes` / `set`（含 `frozenset`）**直接抛
   `TypeError`**（`Object of type Decimal is not JSON serializable`），`tuple` **静默变成
   `list`**。按类型换替代：`tuple`/`frozenset` → `ctx.evaluate` / producer（按值物化、
   不改形状）；`Decimal`/`datetime`/`date` → 先**显式转换**成 `str`/`int`/`float` 再往返，
   或整体改用 `ctx.evaluate` / producer；`bytes`/`set` → `ctx.evaluate` / producer
   （`json_dumps` 无解）。同一条注记见 `docs-site/docs/guide/expressions.md`；回归
   `tests/unit/test_issue69_r9_host_object_loopholes.py::TestIssue69R9EscapeHatchIsLossy`。
4. **性能数字统一**（G5，r11 已把权威更新到当前 revision）：`$F.set($VAR,…)` 的
   **当前 revision（r11）权威读数 = 5.252µs**
   （`scripts/issue69_mutation_matrix/bench_expr.py`，30000 次/轮 × 7 轮取 min，default-deny
   refuse 路径含具名 `ValueError` 构造；本机负载 ~7.8–8.3，三次运行各表达式取 min。
   同一 revision 早一轮读数 **5.647µs @ 负载 ~7.3–8.1** —— 两次相差 7%，与负载同向，
   正是"绝对 µs 不跨负载比"的实证）。
   `MIGRATION`/`§P` 引用同一列。**其余一律是历史值**（不得称权威）：r10 同口径
   **5.871µs**（负载 ~7.8–8.7）、r9 **5.368µs**（~7.0）、r8 **5.350µs**（~6.0）、
   r7 **5.85µs**、r6 **5.98µs**、r5 **13.72µs**（当时是 allow
   路径）、verifier 机 **~23.5µs**。**绝对 µs 只做同机同轮对比**：跨机、跨负载不可比
   （verifier 在本机负载 10.19 实测 5.654µs，与 5.350µs@负载 ~6.0 的差异与负载一致）。
   **撤回一条不可证的归因**：旧版 §P 用"整表所有表达式同向抬升若干个百分点"解释 r8/r9/r10
   的差（该全局比例说法已删除，不再复述原文数值）；验收者实测 r10 列比文档数字**低 3–6%**
   （`$F.set($VAR,'k','v')` = **5.375µs** @ 负载 8.73→8.07，落在 r8 5.350 与 r9 5.368
   之间）⇒ **r10 无可测成本**。

### r9：宿主对象的四处"静默"收口 + 逃生口有损注记（plaita#69 r9）

r9 是 r8 独立复核（PASS-WITH-GAPS）的收口。复核**没有**给出规则级反例（快照路径 0 宿主
钩子、精确基类容器、绝不 `is` 活对象），但报出四个真实缺陷（A/B 规则级，C/D 承诺级）与
两处文档不一致（E/F）。不改架构，全部是"把已声明的规则**做成对**"。

1. **标量归一化只走基类协议（A，规则级，用户可见修复）**。r7/r8 用内建构造器：
   `str(value)` 落回宿主 `__str__`/`__repr__` —— `__str__` 返回
   `'<LiarStr SECRET=TOPSECRET>'` 的 `str` 子类会被**静默**写进数据模型（宿主 repr 里的
   秘密变成数据，且无任何错误：实测 `$F.set($VAR,'k',$F.slice($INPUT.items,0,1))` 把秘密
   写进 `$VAR`）；伪造的 `bool` 触发宿主 `__len__` 抛裸 `RuntimeError`；伪造的 `bytes`
   触发宿主 `__iter__`/`__bytes__` 同理。r9 起换成**基类协议读取器**：`str.__str__` /
   `bytes.__getitem__(v, slice(None))`（r10 定稿；r9 的 `bytes(memoryview(...))` 在 3.12+
   会被宿主 `__buffer__` 劫持，见下面的 **r10** 小节）/ `int.__int__` / `float.__float__` /
   `complex.__getnewargs__` / `Decimal.__str__` / `datetime.__reduce__` + 基类
   `datetime.fold`/`datetime.replace`（r10 补 fold）/ `date.__reduce__`；`bool`/`NoneType` 不可继承、没有基类协议 ⇒ 只接受**精确**类型，
   `__class__` 伪造的一律**具名** `_EngineCopyRefused`（带真实 `type(x).__name__`）。
   回归：`tests/unit/test_issue69_r9_host_object_loopholes.py::TestIssue69R9ScalarNormalisationUsesBaseProtocols`
   （宿主钩子计数器恒 0 + DSL 面秘密不落地 + 十种伪造标量具名拒绝）。
2. **键/成员归一化后不可哈希 ⇒ 具名拒绝（B，规则级）**。
   `class HashableDict(dict): __hash__ = …` 做 `set` 成员会被归一化成普通 `dict`，集合
   重建抛裸 `TypeError: unhashable type: 'dict'`（DSL 可达：循环条件引用含该集合的
   `$INPUT` ⇒ `NodeExecutionError: TypeError: unhashable type: 'dict'`，无归因）。r9 起
   成员/键逐一过 `hash()` 校验，失败即 `_EngineCopyRefused`（真实类型名 + `set member`/
   `frozenset member`/`dict key` 角色）；DSL 面变成具名的
   `cannot be the target of the in-place mutator $F.set`。
3. **快照的键/成员不再按引用共享（C，承诺级，选择 (a)）**。r8 只把**值**路径改成基类协议
   重建；`dict` 的键与 `set`/`frozenset` 的成员仍以活别名进快照（`key_is_live` /
   `el_is_live`，无任何错误）。r9 让键/成员走**同一个** `_engine_copy` 归一化入口（宿主
   子类被归一化掉、类伪造具名拒绝、归一化后不可哈希具名拒绝），于是 `state.py` 里"唯一
   例外是非容器宿主对象"这句措辞**现在为真**（原来对成员/键是假的）。选 (a) 而非 (b)
   （只文档化 + 钉非可写性）：(a) 是把**已存在**的判据接到键/成员上（单拷贝入口的复用，
   `_ENGINE_COPY_CENSUS` 里新增一个可见登记点 `state.py::_snapshot_key_or_member`），
   而 (b) 会把"快照里可以有活宿主对象"这条与第 1 条单拷贝纪律冲突的说法留成书面承诺。
4. **`MutableMapping` + `dict` 病态形状具名拒绝（D，承诺级，选择 (a)）**。
   `class M(MutableMapping, dict)` 把数据存在 `_d`、真实 `dict` 存储恒空：r7 快照走
   `Mapping.items` 得 `{'m': {'a': 1}}`，r8 走基类 `dict.items` 得 `{'m': {}}` —— **静默丢
   数据**（`_engine_copy` 自 r6 起就是同一读数，两条路径"一致"但都不保真）。r9 用
   `plaita/core/expression_parser.py::reads_mapping_outside_dict_storage`（`Mapping` 在 MRO 里排在
   `dict` **之前**）在**两条路径**都具名拒绝，不再静默产出空 `dict`。负控：`OrderedDict`/
   `defaultdict`/`Counter`/普通 `dict` 子类的 MRO 里没有真实 `Mapping` 基类（`dict` 只是
   `Mapping` 的**虚拟**子类，`register` 不进 MRO），实测不受影响。选 (a) 而非"只文档化"：
   这个形状的后果不是保真度下降，而是**写进快照的数据消失** —— 循环条件副本会静默变成
   `{}` 并被当作 fresh 登记（r8 实测该条件下 `$F.set($INPUT.cfg,'k','v')` "成功"）。
5. **逃生口有损注记（E，文档级）**：见上一条 r8/G2 段末与
   `docs-site/docs/guide/expressions.md` 同一处；回归
   `TestIssue69R9EscapeHatchIsLossy` 同时钉住类型行为与两处注记文字。
6. **性能权威读数统一（F）**：权威 = **当前 revision（r11）的 5.252µs**（见上条与
   `docs/issue69-mutation-matrix.md` §P 的 r11 列）；r10 的 5.871µs、r9 的 5.368µs、
   r8 的 5.350µs 及更早数值一律只作
   **历史**并标注 revision，绝对 µs 不跨机/跨负载比较。
7. **验证数字（明说，防抄错）**：r8 的 diff 在 r7 的 **5187** 上新增 **+23**
   （= **5210**；**不是 +19** —— 19 只是新文件里的测试函数数，另有 4 个加在
   `single_copy_point`）。r9 的 diff 在 5210 上新增 **+22**
   （`test_issue69_r9_host_object_loopholes.py` 里全部 22 个），**农场全量
   5232 passed / 0 failed / 12 skipped / 1813 subtests passed**
   （`run_local_revision.py <worktree> b46f78e <HEAD> tests/unit -q`）；本地
   `tests/unit/test_issue69_*.py` + `test_expression_provenance_perf.py` 十文件
   **186 passed / 1551 subtests**；变异 `--all --source head` **34/34 KILLED**。
   绝对数字只对本 revision 成立，改动后必须重跑。

### r10：r9 修复引入的**静默数据损坏**（PEP 495 `fold`）收口 + 逐标量字段审计 + D 的镜像形状（plaita#69 r10）

r10 是 r9 的**回归修复轮**：r9 的独立验收判 **FAIL** —— r9 为 Finding A 换上的
`datetime(*datetime.__reduce__(value)[1])` 丢掉了 PEP 495 的 `fold`，即"修一个 bug 换一个
**静默**数据损坏"，本项目的标准不接受。r10 精修该读取器，并把同类"缺字段/宿主钩子"
面逐标量复查一遍。

1. **`datetime` 读取器逐字段保真（含 `fold`），规则级、DSL 可达**。`datetime.__reduce__`
   是 **protocol 2** 形态：它把 7 个分量 + `tzinfo` 打包成字节，**不含 `fold`**（实测
   3.9.6 / 3.10.21 / 3.11.16 / 3.12.x / 3.13.15 一致；只有 `__reduce_ex__(v, 4)` 把 fold 位
   写进打包字节）。于是 DST 歧义墙钟时间被静默改成 `fold=0`：

   ```
   NY = ZoneInfo("America/New_York")
   SubDT(2020, 11, 1, 1, 30, tzinfo=NY, fold=1)      # 本地时间出现两次的那一格
     r9 读数：  fold=0  timestamp()=1604208600.0  utcoffset()=-4:00(EDT)   # 错 3600s
     r10 读数： fold=1  timestamp()=1604212200.0  utcoffset()=-5:00(EST)
   ```

   它是**静默**的：`type(copy) is datetime.datetime` 仍成立，`copy == value` 仍为 True、
   `hash(copy) == hash(value)` 也相等（CPython 的同 tzinfo 比较与 datetime 的 hash 对
   `fold` 全盲）—— r9 自己的回归用 naive `fold=0` 样本 + `assertEqual(expected, copied)`，
   是 fold 盲的。DSL 可达：`ctx.set_state("$INPUT", {"items": [SubDT(..., fold=1)]})` 后
   `$F.set($VAR,'k',$F.slice($INPUT.items,0,1))` 存进 `$VAR` 的是 `fold=0` 的副本；快照
   路径同理（`set` 成员 / `dict` 键经同一归一化入口）。r10 的读法：仍用基类
   `datetime.__reduce__` 重建 7 分量 + `tzinfo`，再用**基类描述符**
   `datetime.fold.__get__(value)` 取值、**基类** `datetime.replace(built, fold=…)` 写回 ——
   宿主覆写 `fold` property / `replace` / `__reduce__` / `__reduce_ex__`（含 `fold`
   property 返回谎言）一次都进不去。回归：
   `tests/unit/test_issue69_r10_value_preserving_scalars.py::TestIssue69R10FoldIsValuePreserving`
   （DST 两 fold 的 `fold`/`timestamp()`/`utcoffset()`/`tzinfo` 身份、**naive / `timezone` /
   `zoneinfo` 三形态**都带过 fold、非歧义对照证明不是"fold=1 就加一小时"的特判、DSL 面、
   快照面、宿主谎言探针）。
2. **逐标量"全可观察字段"审计 + 判定标准收紧（A 的判决面）**。r9 的
   `type(x) is base` + `==` **不足以**判决 A（正是 fold 漏网的原因）。r10 把判定改成逐字段：
   `datetime` = 7 分量 + `tzinfo` **身份** + `fold` + `timestamp()` + `utcoffset()` +
   `tzname()`；`date` = `year/month/day` + `toordinal()`；`Decimal` = `is_signed` +
   `as_tuple`（符号/系数/**指数**）+ `str()`（`-0`/`1E+3`/`sNaN123`/`Infinity` 全覆盖）；
   `str`/`bytes` = 完整内容；`int`/`float`/`complex` = 完整值（含 ±0 与 NaN）。
   r9 的 `test_every_scalar_subclass_normalises_to_the_exact_base_with_zero_hooks` 已就地
   强化（新增 fold 样本 + 逐字段同判）。审计结论：`str.__str__` / `int.__int__` /
   `float.__float__` / `complex.__getnewargs__` / `Decimal.__str__` / `date.__reduce__`
   （打包字节带 year/month/day 全部字段）**没有**缺字段问题；`datetime` 是唯一一处，
   已修。
3. **`bytes` 读取器不再经过 `memoryview`（PEP 688，3.12+）—— r10 复核发现的第二个同类洞**。
   r9 用 `bytes(memoryview(sub))` 读 buffer 协议；但 3.12 起 `memoryview()` 认 Python 级
   `__buffer__`：实测 3.13.15 上
   `class EvilBytes(bytes): def __buffer__(self, flags): return memoryview(b"SECRET")`
   让读数变成 `b'SECRET'`（3.9–3.11 没有该协议，旧式在这三版"看起来"干净）—— 与 r9 的
   `str` 秘密落地同一类**静默**数据损坏，DSL 可达。r10 改走 C 基类描述符
   `bytes.__getitem__(value, slice(None))`（直接按 bytes 存储整体拷贝，返回精确 `bytes`），
   宿主 `__bytes__`/`__index__`/`__iter__`/`__len__`/`__getitem__`/`__buffer__` 零进入；
   回归里有一条**版本无关的结构性判据**（把 `builtins.memoryview` 换成会炸的替身，读取器
   必须不依赖它），因此该洞在 3.11 的开发机上也能被回归钉住。
4. **D 判据补上"镜像形状"（r9-D1，规则级、DSL 可达）**。r9 的 D 只看"`Mapping` 是否排在
   `dict` **之前**"，于是两个镜像形状两条拷贝路径都**静默**产出 `{}` 且 DSL 报成功：
   `class ItemsLiar(dict)`（数据在 `self._d`，用 Python 覆写 `items/keys/values/__iter__/
   __len__/__getitem__` 自述）与 `class DictFirstMapping(dict, Mapping)`（`Mapping` 排在
   `dict` **之后**）。r10 把 `reads_mapping_outside_dict_storage` 扩成**三条机械可判、
   零宿主钩子**的析取：①`Mapping` 先于 `dict` 的 MRO；②用 Python 级实现覆写了**容器外延
   声明** `__iter__`/`__len__`；③实例带**自己的** `__dict__` 状态（`object.__getattribute__`
   读，`__slots__` 类型为假）。两条拷贝路径与 DSL 一致：**具名拒绝**（消息点名触发的信号），
   不再"报成功却丢数据"。**为什么不取"覆写六个协议名中的任何一个"**（验收建议的最宽口径）：
   那会拒掉 **r7/r8 已验证为好的契约** —— `HookDict`（只覆写 `items()`、数据仍在真实存储）
   在 r8 G1 里必须被**归一化**成精确 `dict`、零钩子、非活对象，`EvilDict` 同理；引擎不采信
   宿主自述，所以"覆写 `items()` 且数据在存储里"与"覆写 `items()` 且数据在别处"光看覆写名
   **不可区分**，只能以**数据位置**（判据③）为据。**过拒代价（明说）**：带任意实例属性
   （缓存/元数据）的 `dict` 子类会被具名拒绝，即使数据确实在真实存储里。
   **r11 更正 —— `dict(...)` 不是逃生口**：`dict(v)` 读的是**真实 dict 存储**
   （即 `real dict storage`），对
   "数据在别处"的形状（`ItemsLiar`/`SlotsItemsLiar`/`HidingDict`）恰好返回 `{}` ——
   **静默给出错误答案**；`dict(v.items())` 则**采信宿主自述**（调用宿主 `items()`）。
   诚实的恢复是从**已知良好**的数据重建，或用 `ctx.evaluate` / producer 按值物化
   （回归 `test_dict_of_a_refused_shape_is_empty` 钉住 `dict(<refused>) == {}`）。
   负控：`OrderedDict`/`defaultdict`/`Counter`/纯 `dict` 子类/精确
   `dict`/引擎自有的 `NodeSpec`/`_IsolatedConditionContext` 全部不受影响。
5. **嵌套 offender 的真实类型名（归因改进）**。`{(HashableDict({'a': 1}),): 1}` 做字典键时，
   r9 的具名拒绝只报外层的 `'tuple'`（失败发生在 `hash(copied)` 那一跳）；r10 沿 `tuple`
   下标下钻到真实 offender（`unhashable_offender`，只读引擎现造的值），两条路径都点名
   `HashableDict`；`frozenset({hd})` 做键的读数与之一致。
6. **性能权威读数统一（F 的继续）**：r10 当时的权威读数（r11 已移到 5.252µs）= **5.871µs**
   （`$F.set($VAR,…)`
   default-deny refuse 路径，`scripts/issue69_mutation_matrix/bench_expr.py` 30000 次/轮 × 7
   轮取 min，本机负载 ~7.8–8.7；**r11 已把权威列移到 5.252µs（负载 ~7.8–8.3）**），r10 的
   5.871µs、r9 的 5.368µs（负载 ~7.0）、r8 的 5.350µs（~6.0）及更早
   一律只作**历史**并标 revision。验收者自己的读数 **5.844µs @ 负载 6.21→7.40** 与两者同
   量级；**绝对 µs 不跨机、不跨负载比较**。r10 的改动只在**标量/子类 dict** 的判据上，不在
   被测热路径（路径表达式 / 目标位门）的分子分母上。
7. **验证数字（明说，防抄错）**：r8 的 diff 在 r7 的 **5187** 上新增 **+23**（= **5210**；
   **不是 +19**）、r9 的 diff 在 5210 上新增 **+22**（= **5232**，农场同读数）。r10 的 diff
   在 5232 上新增 **+28**（`tests/unit/test_issue69_r10_value_preserving_scalars.py` 里全部
   28 个测试函数；r9 那个文件只**强化**判据、不增函数数）⇒ **5260**，**农场全量实测
   5260 passed / 0 failed / 12 skipped / 5 warnings / 1885 subtests passed**
   （`run_local_revision.py <worktree> b46f78e 80705a6cf047 tests/unit -q`，平台
   `state=done`，pytest 158.07s）—— 与 r9 的 5232 恰好差 **+28**，算术闭合。本地同口径面
   （`tests/unit/test_issue69_*.py` 9 个 + `test_expression_provenance_perf.py` +
   `test_mfix_l_loop_context.py` = **11 文件**）实测 **214 passed / 0 failed / 1623 subtests
   passed** —— 与 r9 的十文件 **186 passed / 1551 subtests** 同样恰好差 **+28**（同一批文件、
   只多出本轮的测试文件）。变异 `--all --source head` **39/39 KILLED**（r10 新增 5 条，
   全部死在正常 `FAILED` 行）。绝对数字只对本 revision 成立，改动后必须重跑。

### r11：D 的 clause (iii) 从**代理**改成**性质** + 三处具名化（plaita#69 r11）

r11 是 r10 独立验收判 **FAIL** 的收口。验收给出了一个**规则级**反例：r10 的 clause (iii)
测的是**代理**（"实例有非空 `__dict__`"），不是它宣称的**性质**（"对象可能在真实 `dict`
存储之外持有状态"）。

1. **clause (iii) 换成性质判据（规则级，用户可见修复）**。阻塞反例：

   ```python
   class SlotsItemsLiar(dict):
       __slots__ = ("_d",)
       def __init__(self, d): self._d = dict(d)
       def items(self): return self._d.items()
   ```

   r10 实测：`dict.items(v) == {}`（真实存储是空的）、`v.items() == {'a': 1}`（宿主自述有
   一个键）、`instance_carries_own_state(v) is False`（没有实例 `__dict__`）⇒
   `_engine_copy(v) == {}`、`_snapshot_value({'m': v}) == {'m': {}}`，DSL 循环条件里
   `$F.set($INPUT.cfg,'k','v')` **报成功**（丢数据但无错误）。同族：`SlotsItemsKeysValuesLiar`
   （items/keys/values）、`SlotsGetItemLiar`（只 `__getitem__`）、`HidingDict`
   （`@property def __dict__` 遮蔽真实实例字典）。对照：`SlotsIterLiar`（同样的 `__slots__`，
   另加 `__iter__`）**被拒** —— 两者只差**数据放在哪**，正是判据要机械判定的东西。

   r11 把 clause (iii) 换成 `instance_state_signal(value)`，**四条机械可判、零宿主钩子**的
   析取（只读 `type(value)` 与各层 `vars(klass)`；**不**调用宿主
   `__getattribute__`/`items`/`__iter__`/`__len__`）：

   1. `instance state (__dict__)`：实例的真实 `__dict__` 非空（r10 原判据）；
   2. `instance state (__slots__)`：MRO 里 `dict` **之前**任一层声明**非空** `__slots__`
      —— 字符串简写（`__slots__ = "_d"`）、`tuple`/`list`/`set`/`dict` 形态、**继承/合并**的
      槽、以及任何非 `dict` 基类（mixin）上的槽都算；`__slots__ = ()` 与只含
      `__dict__`/`__weakref__` 占位项的形态算**空**；
   3. `shadowed __dict__`：某层用描述符（`property` / 任意非 `getset_descriptor`）遮蔽了
      `__dict__` —— 此时 `object.__getattribute__(v,'__dict__')` 会**进入宿主 getter**
      （r10 的 `instance_carries_own_state` 正是这样，r11 改成只看类对象）；
   4. `instance state hooks (__getstate__|__setstate__|__reduce_ex__)`：某层用**非 C 槽**
      实现声明了序列化钩子。`__reduce__` **刻意不在表内**：`Counter`（已验证负控）用
      Python 级 `__reduce__` 覆写而数据就在真实存储里 ⇒ 该名字不构成"存储之外"的证据。

   类自身的读取也**不走属性协议**：MRO 与类命名空间分别经 C 描述符
   `type.__dict__["__mro__"]` / `type.__dict__["__dict__"]`（`_real_mro` /
   `_class_namespace`）。宿主**元类**覆写 `__getattribute__` 时，普通属性访问会
   抛异常（判据漏**裸** `RuntimeError`）或**说谎**（实测把 `__mro__` 换成
   `(dict, object)`、`__dict__` 换成 `{}` 就能把声明了 `__slots__` 的基类从判据里
   抹掉、重新静默产出 `{}` —— 同一条阻塞反例换个通道）。这与 `type(value)` 读
   `ob_type` 槽是同一手法。

   两条拷贝路径（`_engine_copy` / `_snapshot_value`）与 DSL 一致**具名拒绝**，消息**点名
   触发的信号**。**过拒代价（明说，r10 面 + r11 扩面）**：带任意实例属性**或任何非空
   `__slots__`** 的 `dict` 子类被拒，即使数据确实在真实存储里。
   **恢复不要用 `dict(...)`**（r11 修正的文档错误）：`dict(v)` 读的是**真实 dict 存储**
   （`real dict storage`），对"数据在别处"的形状恰好返回 `{}`（**静默错误答案**）；
   `dict(v.items())` 则**采信宿主自述**（调用宿主 `items()`）。诚实的恢复是从**已知良好**
   的数据重建，或用 `ctx.evaluate` / producer 按值物化。回归
   `TestIssue69R11DictEscapeHatchIsLossy::test_dict_of_a_refused_shape_is_empty` 钉住
   `dict(<refused shape>) == {}`。
2. **宿主 `__getattribute__` 抛异常不再漏裸 `RuntimeError`（finding 5）**。
   `isinstance(v, base)` 在真实类型不命中时会回落读 `v.__class__`（`tp_getattro` ⇒ 宿主
   `__getattribute__`）；宿主让它抛异常时，`_canonical_persistable_kind`（r6 起）与
   `state._snapshot_value` 的探测会漏出**裸** `RuntimeError`（DSL 可达）。r11 把
   `_canonical_persistable_kind` 的 `isinstance` 循环包成具名 `_EngineCopyRefused`，并给
   快照路径加 `state._snapshot_isinstance`（具名 `StateSnapshotError`）；`type(value)` 读
   C 的 `ob_type` 槽，不经 `tp_getattro`，归因类型名仍然拿得到。
3. **`_detach_alias` 的视图物化收窄（finding 6，pre-existing r3/r8）**。视图不可索引，
   浅 producer 结果为视图时先 `list(value)`；`list` 会调用 `__iter__` 并用 `__len__` 做长度
   提示。对引擎自产的视图（`dict_keys`/`dict_values`/`dict_items` 与 C 派生
   `odict_keys`/`odict_values`/`odict_items`）两者都是 **C 槽描述符** ⇒ 零宿主钩子；但
   `collections.abc` 视图（`ItemsView` & co.）的这两个名字是 **Python** 实现（读
   `self._mapping`），宿主子类更可任意覆写 —— 旧实现实测命中
   `['view.__iter__','view.__len__']` 并把宿主**自述**内容物化成数据（`$VAR.k == ['LIE']`）。
   风险有界（`_engine_copy(<view subclass>)` 本身具名拒绝，且物化结果是全新 plain `list`、
   绝不是活对象），但 r11 把这一跳也钉成零钩子：只有**解析到的 `__iter__` 与 `__len__` 都是
   C 槽描述符**（`_dict_view_is_materialisable`）才 `list(value)`，其余（含直接构造的
   `KeysView`/`ValuesView`/`ItemsView` 实例）**具名拒绝**；视图分类也改成
   `issubclass(type(value), _VIEW_TYPES)`（不再经 `ABCMeta.__instancecheck__` 读
   `__class__`）。**仍然按引用共享的**只有**非容器宿主对象**（它们不是容器，重建语义不
   适用）—— 见 `plaita/core/expression_parser.py` 模块 docstring 的同一段说明。
4. **性能口径修正（finding 7）**：旧版 §P/MIGRATION 有四处仍把**权威**写成 r9 的
   5.368µs，且用"整表所有表达式同向抬升若干个百分点"解释 r8/r9/r10 的差 —— 该全局比例
   说法**不可证且与验收实测相反**（验收在负载 8.73→8.07 实测 r10 的 `$F.set($VAR,'k','v')`
   = **5.375µs**，落在 r8 5.350 与 r9 5.368 **之间** ⇒ 没有可测的 r10 成本）。r11：
   权威 = **当前 revision（r11）的 5.252µs @ 本机负载 ~7.8–8.3**（同一 revision 早一轮
   读数 5.647µs @ ~7.3–8.1，两次差 7% 与负载同向 ⇒ 跨负载不可比），其余全部标 revision 并
   写"历史值"；**绝对 µs 不跨机/跨负载比较**。
5. **验证数字（明说，防抄错）**：r11 的 diff 在 r10 的 **5260** 上新增 **+41**
   （`tests/unit/test_issue69_r11_instance_state_generalisation.py` 里全部 41 个测试函数，
   7 个 `TestCase`）⇒ **5301**，**农场全量实测 5301 passed / 0 failed / 12 skipped /
   5 warnings / 1959 subtests passed**（`run_local_revision.py <worktree> b46f78e
   06df19c51501 tests/unit -q`，平台 `state=done`，pytest 158.07s；**代码/测试与本行所引
   HEAD 相同、只多一条变异重锚与文档的 `def93bceb2d8` 上复跑一致**：5301 passed /
   0 failed / 12 skipped / 5 warnings /
   1959 subtests passed，161.04s）—— 与 r10 的 5260
   恰好差 **+41**，算术闭合。本地同口径面
   （`tests/unit/test_issue69_*.py` 10 个 + `test_expression_provenance_perf.py` +
   `test_mfix_l_loop_context.py` = **12 文件**）实测 **255 passed / 0 failed / 1697 subtests
   passed** —— 与 r10 的 11 文件 **214 passed / 1623 subtests** 恰好差 **+41**（同一批文件、
   只多出本轮的测试文件），测试函数计数算术闭合。变异 `--all --source head` **46/46
   KILLED**（r11 新增 7 条，全部死在正常 `FAILED` 行）。绝对数字只对本 revision 成立，
   改动后必须重跑。

### r12：clause (iii) 的第 5 条（C member/getset 描述符）+ 视图物化按**真实布局** + 变异 JSON 契约（plaita#69 r12）

r12 是 r11 独立验收判 **FAIL** 的收口。反例仍是**规则级**的：clause (iii) 测的是四条
*声明形态*（实例 `__dict__` / `__slots__` / 遮蔽 `__dict__` / 序列化钩子），而不是它宣称的
**性质**（"对象可能在真实 `dict` 存储之外持有状态"）。这次通道是 **C 级成员槽**：

```python
class N_DDLiar(defaultdict):
    def __init__(self, d):
        super().__init__(None)
        self.default_factory = dict(d)      # C member slot (member_descriptor), per-instance
    def items(self): return self.default_factory.items()
```

r11 实测（`56b7678`）：`dict.items(v) == {}`（真实存储是空的）| `v.items() == {'a': 1}`
（宿主自述有内容）| `object.__getattribute__(v,'__dict__') == {}` |
`instance_state_signal(v) is None` | `reads_mapping_outside_dict_storage(v) is False` |
`_engine_copy({'m': v}) == {'m': {}}` | `_snapshot_value({'m': v}) == {'m': {}}` |
DSL `$F.set($INPUT.cfg,'k','v')` → **`raised=None`（报成功）**，`cfg` 未变。同族
`N2_DDGetItem`（只覆写 `__getitem__`，`v['a'] == 1` 而真实存储是 `{}`）。这条通道
**机械可判**：`types.MemberDescriptorType` 本来就在 `_C_SLOT_DESCRIPTOR_TYPES` 里，
判据本来就逐层走 `_class_namespace`；而 r11 自己的引理（"无槽 ⇒ 状态只能在实例
`__dict__` 或真实 `dict` 存储里"）是**假的**。

1. **clause (iii) 增加第 5 条（规则级，用户可见修复）**。
   `declared_c_storage_descriptors(cls)`：逐层扫 MRO 里 `dict` **之前**的**真实**类命名空间，
   收集 **C member/getset 描述符**（`member_descriptor` / `getset_descriptor`），
   排除 `__dict__`/`__weakref__` 两个占位名，并且**跳过声明层就是 `cls` 自己的那种**。
   信号名（逐条点名）：`instance state (C descriptor default_factory on defaultdict)`。
   **r13 更正**：本段原来的理由（"C 描述符只可能由**拥有 C 布局的类型**在自己的 `tp_dict`
   里生成，Python 类体写不出 `member_descriptor`"）**是假的** —— Python 类体的
   `__slots__` 就会在该类**自己的** `tp_dict` 里生成 `member_descriptor`，而"声明层 ≠ `cls`"
   这条分界线因此可以被**隐藏 `__slots__` 声明**绕过（r13 的阻塞反例）。真实的分界线是
   **布局 + 类型种类**：豁免仅限"声明层 `is cls` **且** `cls` 不是 **Python 定义的类**
   （`_is_python_defined_class` = `Py_TPFLAGS_HEAPTYPE` ∧ ¬`Py_TPFLAGS_IMMUTABLETYPE`）"。
   r12 原文列的两条后果仍然成立（精确 `defaultdict`
   归一化；Python 子类里来自前置层的槽被拒），但"Python 类体写不出"必须删掉。详见
   r13 一节。
   * 精确 `defaultdict`（`cls` 就是那个扩展类型）⇒ `default_factory` 成员槽是**它自己的**
     存储口径，本仓已验证的负控契约要求照常**归一化**；精确 `OrderedDict`/`Counter` 更简单：
     它们（**以及它们的子类**）的非 `cls` 层只声明被排除的 `__dict__`/`__weakref__`；
   * Python 子类 ⇒ 来自前置层的任何其它 C 成员/getset 都意味着"继承了别人声明的 on-object
     存储" ⇒ 无法证明它没被用来装数据 ⇒ fail-closed 具名拒绝。

   两条拷贝路径（`_engine_copy` / `_snapshot_value`）与 DSL 一致具名拒绝、零写穿；
   判据零宿主钩子（不调用宿主 `__getattribute__`/`items`/`__iter__`/`__len__`；类自身的读取
   仍走 C 描述符 `type.__dict__["__mro__"]`/`["__dict__"]`，宿主元类覆写同样进不来）。

   **负控（实测不变）**：精确 `defaultdict(int,{'a':1})` / `OrderedDict` / `Counter` /
   纯 `dict` 子类 / 精确 `dict` / `NodeSpec` / `_IsolatedConditionContext` 全部照常归一化；
   `Counter` 的 Python 级 `__reduce__` 仍**刻意**不在 `_DICT_INSTANCE_STATE_HOOKS` 里。

   **过拒面（明说，与 `AttrDict`/`SlotsCacheDict` 同级）**：无数据在存储之外的良性
   `defaultdict` **子类**（`class BenignDD(defaultdict): pass`，数据全在真实存储里）现在
   也被拒 —— `default_factory` 的声明层是 `defaultdict`（≠ `cls`）。**stdlib 的真实例子**
   （r13 记入）：`importlib.metadata._collections.FreezableDefaultDict`（Python 子类，
   数据在真实存储里）同样被拒。**实测细化**：
   `OrderedDict`/`Counter` 的子类**不受**影响，因为它们的非 `cls` 层只声明被排除的
   `__dict__`/`__weakref__`（`declared_c_storage_descriptors(BenignOD) == ()`，有测试钉住）。
   代价与"带实例属性/非空 `__slots__` 的 `dict` 子类被过拒"同类，取 fail-closed：需要它进
   数据模型时从**已知良好**的数据重建或用 `ctx.evaluate`（**不要**用 `dict(v)`，见下）。
2. **被推翻的引理改写**。`expression_parser.declared_instance_slots` 的 docstring 原来写
   "无槽 ⇒ 状态只能在实例 `__dict__` 或真实 `dict` 存储里"——**这是假的**。现在写明真实规则：
   对象在真实存储之外的 **on-object** 状态有**五个**来源（实例 `__dict__`、`__slots__`、
   遮蔽 `__dict__` 的描述符、序列化钩子、**MRO 里 `dict` 之前某一层命名空间里真实存在的
   on-object 描述符**；r13 改成布局判据 —— 豁免仅限"声明层 `is cls` 且 `cls` 不是
   Python 定义的类"（`_is_python_defined_class`）），五条都不命中，引擎才认为真实 `dict` 存储就是全部内容。
3. **视图物化的两条旁路（finding 6 的残留）**。r11 的闸门只检查"解析到的实现是 C 描述符
   **类型**"，没检查"这个描述符**属于该对象的真实布局**"：
   * `class V1(KeysView): __iter__ = dict.__iter__; __len__ = dict.__len__`：`dict.__iter__`
     确实是 C 包装描述符 ⇒ r11 过闸 ⇒ `list(v1)` 抛**裸**
     `TypeError: descriptor '__iter__' requires a 'dict' object but received a 'V1'`
     （DSL 可达、消息里没有 `plaita#69`）；
   * `KeysView.register(LieList)`（`LieList(list)` 不覆写任何名字）：ABC **虚拟注册**让
     `_is_dict_view_type` 为真、而 `list.__iter__`/`list.__len__` 是 C 描述符 ⇒ r11 过闸 ⇒
     宿主登记的非视图类型被当视图物化（实测拿到 `['LIE']`）。更尖锐的是同一通道能**改变值
     的种类**：`KeysView.register(LieBytes)`（`LieBytes(bytes)`）在 r11 上
     `_detach_alias(LieBytes(b'LIE'))` 得到 **`[76, 73, 69]`**，而 `_engine_copy` 的
     数据模型口径是 **`b'LIE'`**（`bytes` 是持久化标量）—— 静默把标量变成 list。

   r12：物化只允许**精确类型**落在 `_ENGINE_VIEW_TYPES`（引擎自产的 `dict_keys`/
   `dict_values`/`dict_items` 与 C 派生 `odict_keys`/`odict_values`/`odict_items`）里的视图；
   `list(value)` 另外包在同一具名 fail-closed 包装里，所以物化那一跳**任何**失败都变成具名
   `plaita#69` 拒绝。两条旁路现在都得到具名 `ValueError`（含 `dict view`/`__iter__`）与
   **零宿主钩子**。附带实测：真视图类型在五个解释器上都**不能做基类**
   （`type 'dict_keys' is not an acceptable base type`）⇒ 精确类型闸门没有"宿主子类"这一档，
   ABC 注册就是唯一旁路。
4. **变异 JSON 契约**。6 个 r11 变异文件缺 `"issue"` 键（只有
   `69-r11-metaclass-attribute-protocol-trusted` 有），`replay.py --list` 会直接
   `KeyError: 'issue'`。r12 补齐这 6 个，并新增回归
   `TestIssue69R12MutationJsonContract`：每个 `mutations/*.json` 必须带重放器读的全部 8 个键
   （`name`/`issue`/`concern`/`file`/`rationale`/`old`/`new`/`expect_fail`），且 `old` 在目标
   文件里**恰好出现 1 次**（锚点陈旧会被重放器判"定义原文 0 次"，这条测试让它在 pytest 里
   就红）。
5. **off-object 残余按设计之外（out of scope）明说**。判据是**零钩子静态判定**：它看得见
   实例上的存储（`__dict__`/`__slots__`/遮蔽描述符/`dict` 之前的 on-object 描述符）与宿主
   **声明**的序列化钩子；看不见**根本不在实例上**的状态 —— 数据放在**类属性**
   （`class attribute`）、**闭包**（`closure`）、**模块级旁表**（`module-level side table`，
   例如按 `id(self)` 索引）、或藏在 **`property`/数据描述符**（`property/data descriptor`）
   后面。**r13 理由更正**：旧措辞把这四族的原因统一写成"要调宿主代码或判定不可决定的可达性"，
   但对**类属性**族**不准确** —— `_class_namespace(klass)` 本来就零钩子地扫过每一层的真实
   `tp_dict`，类属性**看得见**；真正的理由是：一旦属性进了命名空间，"**这个属性是内容，
   还是常量/缓存/类级默认值？**"**无法仅从类对象判定**，而把"任何非 dunder 类属性"都当成
   状态会拒绝语言本身（每个子类都带 `__module__`/`__qualname__`/`__doc__`，每个装饰过的方法、
   每个类常量）。其余三族（闭包 / 模块级旁表 / `property`-或-数据描述符）才确实需要
   "调用宿主代码"（而"你有哪些内容"的宿主钩子正是本纪律要避开的不可信通道）或判定任意
   Python 对象的**可达性/别名**（**不可判定**）。边界仍画在"**on-object** 存储声明 + 声明的
   序列化钩子"，这四族按 **out of scope** 明说（模块 docstring、本节、
   `docs-site/docs/guide/expressions.md` 各一处），不再被静默暗示为已覆盖。
6. **解释器可用性口径更正**。r11 的交付说明称本机只有部分解释器可用 —— **说反了**：
   **五个解释器全部可用**，`3.9.6`（`/usr/bin/python3`）、`3.10.21`
   （`~/.local/bin/python3.10`）、`3.11.16`（`~/.venvs/plaita/bin/python`，本仓测试用 venv；
   另有 `~/.local/bin/python3.11`）、`3.12.14`（`uv` 的
   `cpython-3.12-macos-aarch64-none`）、`3.13.15`（`~/.local/bin/python3.13`）。C 成员描述符面
   在这五版上逐个实测（`member_descriptor` 五版一致；`odict_keys`/`odict_values`/`odict_items`
   五版一致；`dict_keys` 等真视图类型五版都不可子类化）。**重算命令（纯 stdlib，五版直接可跑，
   不需要装依赖）**：

   ```bash
   for p in /usr/bin/python3 ~/.local/bin/python3.10 ~/.local/bin/python3.11 \
            ~/.local/share/uv/python/cpython-3.12-macos-aarch64-none/bin/python3.12 \
            ~/.local/bin/python3.13; do
     $p -c "import sys, types; from collections import defaultdict, OrderedDict; \
            print(sys.version.split()[0], \
                  type(defaultdict.__dict__['default_factory']).__name__, \
                  isinstance(defaultdict.__dict__['default_factory'], types.MemberDescriptorType), \
                  type({}.keys()).__name__, type(OrderedDict().values()).__name__)"
     $p -c "class S(type({}.keys())): pass"   # 期望：TypeError（真视图类型不可子类化）
   done
   ```

   五版读数（本轮实测，逐字）：`3.9.6 / 3.10.21 / 3.11.16 / 3.12.14 / 3.13.15` →
   `default_factory` 一律 `member_descriptor` 且 `isinstance(..., types.MemberDescriptorType)`
   为真；视图类型一律 `dict_keys`/`dict_values`/`dict_items`/`odict_keys`/`odict_values`/
   `odict_items`；`dict_keys`/`dict_values`/`dict_items`/`odict_keys`/`odict_items` 一律
   `type 'dict_keys' is not an acceptable base type`（**不可子类化**）。plaita 判据面在五版上
   也逐个跑过（3.10/3.12/3.13 用 `uv venv` 装了 `pydantic==2.14.1`/`pyparsing==3.3.3`/
   `pytest==9.1.1`/`isodate==0.7.2`）：`instance_state_signal(N_DDLiar)`、`_engine_copy`、
   `_snapshot_value`、DSL 拒绝、视图闸门、精确负控的读数五版**逐字一致**（见
   `tests/unit/test_issue69_r12_c_storage_and_view_layout.py` 与本节 1–3 条）。
7. **性能口径（不产生新权威）**。r12 的改动只在 `dict` 子类的谓词与视图闸门上，**不在** §P
   被测的 `$F.set($VAR,'k','v')` 热路径上；本轮**未重测**、**不产生**新的权威读数。§P 的
   单一权威仍是 r11 的 `5.252µs` @ 本机负载 ~7.8–8.3（绝对 µs 跨机/跨负载不可比，见 §P 的
   `跨机` 注记；验收者自测 r11 读数 **6.405µs** @ 负载 10.19，与负载方向一致）。
8. **验证数字（明说，防抄错）**：r12 的 diff 在 r11 的 **5301** 上新增 **+25**
   （`tests/unit/test_issue69_r12_c_storage_and_view_layout.py` 里全部 25 个测试函数、
   4 个 `TestCase`）⇒ **5326**；本地 13 文件同口径面
   （`tests/unit/test_issue69_*.py` 11 个 + `test_expression_provenance_perf.py` +
   `test_mfix_l_loop_context.py`）实测 **280 passed / 0 failed / 1753 subtests passed** ——
   与 r11 的 12 文件 **255 passed / 1697 subtests** 恰好差 **+25**（subtests +56：两条新
   契约判决改成**聚合 assertEqual**、不再逐个 `subTest`）。变异 `--all --source head`
   **49/49 KILLED**（r12 新增 3 条、**重锚 2 条** r11 变异、补齐 6 个 `issue` 键，全部死在
   正常 `FAILED` 行）。**农场全量实测**（`run_local_revision.py <worktree> b46f78e
   4a6b2d206cb4 tests/unit -q`，平台 `state=done` +174s，rc=0）：**5326 passed / 12 skipped /
   5 warnings / 2015 subtests passed in 158.54s**（r11 农场是 5301 passed / 12 skipped /
   1959 subtests / 158.07s ⇒ 测试 +25、subtests +56，算术闭合）；变异重放
   `replay.py --all --source head` **合计 49 / 杀死 49 / 存活 0**，`old` 锚点 49/49 恰好
   出现 1 次，空 killer（只由 `subTest` 记账杀死）**恰好 2 条、全部是 pre-existing**
   （`69-fresh-producer-default-true`、`69-callee-args-not-recorded`），无 timeout-only。
   绝对数字只对本 revision 成立，改动后必须重跑。

   **复跑确认（文档 HEAD `3ef21e6`，代码/测试/变异与 `4a6b2d2` 逐字相同、只差 `MIGRATION.md`
   措辞）**：变异重放 `--all --source head` 仍 **49/49 KILLED / 0 存活**（空 killer 仍恰好那
   2 条 pre-existing）；农场复跑 **5326 passed / 12 skipped / 5 warnings / 2015 subtests
   passed in 158.79s**（平台 `state=done`、rc=0）。首次复跑曾出现 **1 failed**，是本单**无关**的
   负载敏感时间断言：`tests/unit/test_issue139_distributed_timeout_validation.py::TestMalformedTimeoutIsAFlowError::test_malformed_call_timeout_raises_flow_error_with_value`
   的 `assertLess(elapsed, 0.15)` 实测 **0.370s** —— 该文件在 `56b7678..HEAD` 里**零改动**
   （`git diff --stat 56b7678 HEAD -- tests/unit/test_issue139_distributed_timeout_validation.py`
   为空），本机单独跑 **9 passed**，复跑即绿。

### r13：clause (iii) 第 5 条改成**布局**判据（`Py_TPFLAGS_HEAPTYPE` ∧ ¬`Py_TPFLAGS_IMMUTABLETYPE`）+ `$F.keys/items/values` 读**真实存储** + off-object/快照口径两处更正（plaita#69 r13）

r13 是 r12 独立验收判 **FAIL** 的收口，两个反例都是 r12 **自己的理由**被反证。

1. **阻塞反例：隐藏 `__slots__` 声明、留下槽描述符（规则级）**。r12 的豁免理由是
   "C 描述符只可能由**拥有 C 布局的类型**在自己的 `tp_dict` 里生成，Python 类体写不出
   `member_descriptor`" —— **假的**。Python 类体的 `__slots__` 就在该类**自己的**
   `tp_dict` 里生成 `member_descriptor`：

   ```python
   class HiddenSlotsLiar(dict):
       __slots__ = ("_store",)                      # cls 自己的 tp_dict 里有 member_descriptor
       def __init__(self, d): self._store = dict(d) # 数据全在 C 槽里
       def items(self): return self._store.items()
   del HiddenSlotsLiar.__slots__                    # 抹掉声明名，槽仍在
   ```

   因 clause (iii).2 按**名字** `__slots__` 找槽、而 clause 5 有 `if klass is cls: continue`，
   **隐藏声明、保留槽**就能恢复静默 `{}`/DSL 报成功：pre（`56b7678` = `bddeb72` 同）
   实测 `type(cls.__dict__['_store']).__name__ == 'member_descriptor'`、
   `declared_instance_slots(cls) == ()`、`declared_c_storage_descriptors(cls) == ()`、
   `instance_state_signal(v) is None`、`reads_mapping_outside_dict_storage(v) is False`、
   `_engine_copy({'m': v}) == {'m': {}}`、`_snapshot_value({'m': v}) == {'m': {}}`、
   DSL `$F.set($INPUT.cfg,'k','v')` → **`raised=None`**（零钩子、`cfg` 未变）。
   三个等价形态都复现：`del cls.__slots__` / 事后 `cls.__slots__ = ()` / 元类 `__new__`
   设置后再删。**子类** `class Sub(HiddenSlotsLiar)` 在 r12 下**已被拒**（槽的声明层
   `HiddenSlotsLiar` ≠ `cls`）⇒ 只有**定义层自己**漏。

   **r13 规则（按布局，不按名字）**：逐层枚举 MRO 里 `dict` **之前**（含 `cls` 自己）的
   **真实**命名空间里的 `member_descriptor`/`getset_descriptor`，忽略
   `__dict__`/`__weakref__` 占位名；**豁免只有一条**：声明层 `is cls` **且** `cls` 不是
   **Python 定义的类**（`_is_python_defined_class`）。判据是
   **`Py_TPFLAGS_HEAPTYPE` ∧ ¬`Py_TPFLAGS_IMMUTABLETYPE`**：
   `Py_TPFLAGS_HEAPTYPE` = `1 << 9` = `0x200`（运行期造的类型），
   `Py_TPFLAGS_IMMUTABLETYPE` = `1 << 8` = `0x100`（CPython 3.10 起：类型对象不可改属性）。
   **为什么不是裸 heap 位**（r13 实测踩坑）：CPython **3.12/3.13 的 `collections.defaultdict`
   自己是堆类型**（裸解释器里 `__flags__` 实测 `0x20405740`，heap 位 `0x200` 置位），只看
   heap 位会把精确 `defaultdict` 误拒、推翻负控契约。五解释器实测
   （3.9.6/3.10.21/3.11.16/3.12.14/3.13.15，按**位**读、不按整值）：`defaultdict` 的 heap 位
   在 3.9–3.11 **清**、在 3.12/3.13 **置**；但 `IMMUTABLETYPE` 位（`0x100`）五版都在
   （3.12/3.13 引擎进程里 `defaultdict.__flags__` 观测到 `0x20485740`，比裸解释器多出的
   `0x80000` 是无关的 version-tag 位）⇒ `_is_python_defined_class(defaultdict)` 五版都
   `False` ⇒ 精确 `defaultdict` 照常归一化；Python 类（含 `HiddenSlotsLiar`/`Counter`）
   五版都是 heap 位置 + immutable 位清 ⇒ `True` ⇒ 它自己 `tp_dict` 里的槽
   **无论声明名是否还在**都被看见；子类继承到的扩展类型层槽（声明层 ≠ `cls`）照旧被看见。
   信号：`instance state (C descriptor _store on HiddenSlotsLiar)`。两条拷贝路径与 DSL
   一致具名拒绝、零写穿、零宿主钩子。3.9 上 `IMMUTABLETYPE` 位尚未启用（所有类型都清），
   判据退化成"只看 heap"——而 3.9 上所有扩展类型都是静态类型（heap 位清），结论一致。

2. **`$F.keys`/`$F.values`/`$F.items`（及 `getDictKeys`/`getDictValues` 别名）读真实存储**。
   r12 的交付说明称"假视图返回 `['LIE']` 已不能写成数据、零宿主钩子" —— **被反证**：
   `$F.keys` 从来不是引擎自产视图，它直接问宿主（`_fn_keys(a) = a.keys()`），
   `class LieKeysPlainList(dict): def keys(self): return ["LIE"]` 在 pre 实测
   `$F.set($VAR,'k',$F.keys($INPUT.d))` → `raised=None`、`$VAR == {'k': ['LIE']}`、
   宿主钩子 `keys` **被调用**（`items`/`values` 同形）。r13：参数是 `dict` 时改走
   **基类 C 描述符**（`dict.keys(v)`/`dict.values(v)`/`dict.items(v)`，宿主覆写**零调用**）；
   非 `dict` 的映射无法在不进入宿主代码的前提下读到真实存储 ⇒ **具名拒绝**
   （`ValueError`，消息含 `plaita#69`）。只拦**引擎自带**实现（按 callable 身份），宿主在
   自定义 registry 里注册自己的 `keys` 不在拦截范围内。普通 `dict` 的
   `$F.keys(plain_dict)` 行为**逐字不变**（仍是 `dict_keys` 视图 → 物化成 list）。
   `$VAR == {'k': ['a']}`（真实存储键），假视图**够不到**值参 —— 这条通道因此关闭；
   视图物化的精确类型闸门与 `list(value)` 具名包装**保留**（仍由 `_detach_alias` 直调用例钉住）。

3. **off-object 理由更正（类属性族）**。四族（class attribute / closure / module-level side
   table / property-或-数据描述符）原来统一写成"要调宿主代码或判定不可决定的可达性" ——
   这对**类属性**族**不准确**：`_class_namespace(klass)` 本来就零钩子扫过每层真实
   `tp_dict`，类属性**看得见**。真实理由："这个属性是内容，还是常量/缓存/类级默认值？"
   **无法仅从类对象判定**，而把"任何非 dunder 类属性"都当状态会拒绝语言本身。其余三族
   才确实需要宿主代码或不可判定的可达性判定。模块 docstring、本节、
   `docs-site/docs/guide/expressions.md` 三处同步。

4. **快照按引用共享的口径更正**。r8 起的措辞写"唯一按引用共享的是**非容器**宿主对象" ——
   假的：`_snapshot_value` 只重建 `dict`/`list`/`tuple`/`set`/`frozenset` 五种容器，
   **其它一律按引用返回**，其中包含**是容器**但非这五种的 `types.MappingProxyType`、
   `collections.ChainMap`、`collections.UserDict`、`os.environ`（`os._Environ`）。它们按引用
   共享是因为重建必须猜构造协议；在快照路径上安全（引擎不就地改写快照），且**不是数据
   通道**：DSL 路径上 `_engine_copy` 对这四个**具名拒绝**（它们不在
   `plaita.core.types` 的可持久化数据模型里）。有测试逐个钉住"共享同一对象 + DSL 具名拒绝"。
   **plaita#162 补记**：本节、`docs-site/docs/guide/expressions.md` 与 `expression_parser`
   早已改用枚举口径，但 `plaita/core/state.py` 的**用户可见 docstring** 在 #69 的验收契约里
   被冻结（必须与 base 逐字节相同），当时仍写着那句假话。#162 把
   `_snapshot_value.__doc__`（连同 `_snapshot_key_or_member.__doc__` 与 r9 Finding C 叙述）
   改成同一枚举口径，并加 `tests/unit/test_issue162_snapshot_docstring.py` 做 doc-pin：
   枚举必须留在 docstring、假话原文必须从 `state.py` 整份源码消失，且"四个反例确实按引用
   共享 + 确实是容器 + 确实被 `_engine_copy` 具名拒绝"由同一文件实测钉住。
   `_snapshot_value` 的行为一字未改。

5. **过拒面补充**：除 `BenignDD` 外，记入 stdlib 的真实例子
   `importlib.metadata._collections.FreezableDefaultDict`（Python 子类，数据在真实存储里，
   同样被拒），与 `AttrDict`/`SlotsCacheDict` 同级注记并有 pinning 测试。

6. **变异与验证**：新增 2 条变异（布局规则豁免、`$F.keys` 基类读），重锚 2 条 r12 变异
   （`old` 随函数体改写）与 1 条 r11 视图变异（`$F.keys` 通道关闭后其一条 `expect_fail`
   改写为"通道关闭断言"，从表移除）。绝对数字只对本 revision 成立，改动后必须重跑。

## 0.6.1

0.6.1 为修复版本：`MIGRATION.md` 与 v0.6.0 逐字节相同（`git diff v0.6.0..v0.6.1 -- MIGRATION.md` 为空），无新增破坏性变更。0.6.0 带入的破坏性变更见下节。

## 0.6.0

### 安全与分布式加固（2026-09 R4 定向深潜轮）

安全专项（威胁模型：能写流程 JSON 的人是半信任主体）：

1. **CodeNode 后端白名单**：`register_code_node(allowed_backends=(...))` 设置后，
   流程 JSON 里的 `sandbox_backend` 不在白名单内即**解析期硬失败**。此前流程作者
   可把运营者选定的默认后端逐节点覆盖为 `"unsafe"`（宿主任意代码执行）。
2. **restricted 沙箱移除 `functools`/`operator`**：`operator.attrgetter("__class__")`
   走 C 层属性访问绕过 `_getattr_` 拦截，可完整逃逸（确定性 PoC 已验证）。
   restricted 定位收敛为"仅防误用"，禁止用于半信任作者。
3. **subprocess 沙箱环境变量白名单**：子进程不再继承宿主全量 `os.environ`
   （历史等于把 API key/云凭证交给沙箱内代码）。白名单外用
   `code.SUBPROCESS_ENV_EXTRA` 按需补充。注意 macOS 无 RLIMIT 内存上限。
4. **HTTP 节点 SSRF 防护钩子**（默认行为不变）：新增 `requestTimeout`（默认 30s）、
   `allowedHosts` / `deniedHosts`（精确/`*.suffix`/CIDR）、`blockPrivateNetworks`、
   `maxRedirects`。任一策略激活时重定向改为逐跳校验后手动跟随。
5. **`$F.pow` 指数上限**（默认 **1000000**，owner 的 K1 口径：上限取真实流量不可达量级）
   + **分配前**结果规模预检（`int` 结果的位数 ≤ `|b| × digits(a)`，放大由渲染闸负责）：
   `$F.pow(9, 9999999)` 这类 20 字节表达式仍在分配前拒（指数超上限），
   `$F.pow(2, 20000)`（6KB，纯数据）**通过**，`$F.pow(9, 999999)`（~1MB，放大）按渲染闸拒。
6. **`expose_env` 拒绝 `"*"`/空串/带空白键**：此前静默落空（以为暴露了实际没有）。

分布式专项（Redis 全链路实测）：

7. **RedisEventBus/RedisExecutionStorage 客户端按事件循环重建**：worker 每个分布式步骤
   跑在新建的 asyncio 循环里，缓存的 aioredis 连接跨循环失效——含 event 节点的
   流程约每两次挂起即失败一次并丢单。修复后按循环重建（外部注入的客户端不接管）。
8. **重复投递的 resume 任务幂等短路**：终态执行的 resume 原样返回，不再把
   completed/error 改写。`redis publish` 的 dict/str 形式支持 `correlation_id`
   关键字（EventFilter 的 correlation 匹配此前无法通过该形式设置）。
9. **subprocess 沙箱地址空间上限默认关闭**：`PLAITA_SANDBOX_MEMORY_MB` 默认值
   从 256 改为 **0（不限制）**。RLIMIT_AS 卡的是虚拟地址而非 RSS，Linux 上
   256MB 让 node/pnpm 类命令启动即 V8 OOM（V8 启动期预留远超 256MB 虚拟地址），
   失败只剩截断崩溃栈；macOS 不强制 RLIMIT_AS，故本地部署从未暴露。
   - 变更前：默认 256MB，Linux 上 node/npm/pnpm 系代码节点、setup、gate 全部不可用。
   - 迁移：无需动作。确有需要（纯 Python 小脚本、多租户防护）才显式设
     `PLAITA_SANDBOX_MEMORY_MB=<MB>`；node 系命令请保持 0。

### 编排内核行为收紧（2026-09 评审修复轮）

本轮把一批"静默错误结果"变成显式报错。若升级后流程开始抛错，通常说明流程
本身有配置问题——以下是判断与迁移方法：

1. **switch/if 分支未命中且无 default → 抛 `FlowExecutionException`**
   - 变更前：调度层把整个 `$NODE` 状态表当流程结果返回，流程"成功"结束。
   - 迁移：给分支节点补 default 分支；或在该节点上配
     `"errorHandler": {"strategy": "continue"}` 显式保留旧的跳过行为。
2. **http 节点失败 → raise（errorHandler 真正生效）**
   - 变更前：请求失败时把 `NodeException` 对象当**节点结果**返回，下游拿到
     异常对象当数据用；`errorHandler` 的 continue/continue_with 永不生效。
   - 迁移：依赖旧行为（拿到 NodeException 对象继续跑）的流程，改为配置
     `errorHandler`（`continue` / `continue_with` + `defaultValue`）。
   - 同时修复了 http 节点表达式求值的参数颠倒——0.5.x 里真实 HTTP 请求
     实际上从不成功（集成测试全 mock 未覆盖真实路径）。
3. **挂起中的 EventNode 拒绝 `resume_type="continue"` 绕过**（抛 `ResumeError`）：
   必须先用 `event`/`cancel`/`timeout` 消费事件，之后才能 continue 推进。
4. **分布式 resume 完成后注销事件订阅**：此前死订阅会持续匹配后续同类型
   事件。依赖"死订阅仍命中"的行为（不应该存在）需自行改掉。
5. **`FlowExecution` 实例不可重入**：同一实例并发/重入 run 会抛错。并发场景
   每次新建实例（`Flow.run()` 每次新建，不受影响）。
6. **End 节点 `resultType` 默认 `success`**：不再需要显式写
   `"resultType": "success"` 才能拿到 output；未知的 resultType 值打
   warning 并按 success 处理（此前静默返回 None）。
7. **未知配置键告警**：节点/Flow JSON 里拼错的字段名（如 `conditon`）现在
   打 warning（含合法键清单），不再被 pydantic `extra=ignore` 静默吞掉。
   CI 里若把 warning 当错误需注意存量流程的清理。
8. **`ExecutionMode.from_string` 非法值抛 `ValueError`**（原裸 `KeyError`）；
   `parse()` 对非 python runtime 抛 `ValueError`（原 `RuntimeError`）。
9. **`retryTimes` 负数按 0 处理**：至少执行一次节点；此前负数会静默跳过
   节点执行并吞掉 abort 错误。
10. **PlaitaClient**：新增 `PlaitaClientError`/`PlaitaClientNetworkError`/
    `PlaitaClientResponseError`（均继承 `Exception`，老 `except Exception` 不受影响）；
    `run_flow` 不传 `input_data` 现在等价于 `{}`；`clear_cache(flow_id)` 只清
    该 flow 的版本（此前静默清空全部）。
11. **`timeout` 字段非法格式**（如 `"100ms"`）抛 `ValueError` 并列出合法写法
    （纯数字毫秒或 ISO 8601，如 `"300"` / `"PT0.3S"`）。
12. **表达式默认注册表未知函数 → `NameError`**（带 did-you-mean 与可用清单）：
    `$F.JSON_DUMPS(...)` 这类拼写错误不再返回 `'undefined'` 字符串流入下游。
    scoped 自定义注册表保持 `'undefined'` 契约不变。
13. **可疑字面量告警**：表达式位置出现 `{{name}}`、f-string、`await ...`、
    裸函数调用等"其他语言模板/代码习惯"的字符串，现在打 warning（原样返回
    不变，历史上零告警）。
14. **JSON 路径结构检查告警**：`Flow.from_string` 现在会跑 ir_validate 的
    结构检查（id 重复 / next 悬空 / if 缺分支目标 / switch 缺 default /
    子流程节点缺 childFlow），命中即 warning——历史上这些只在
    Builder/codeflow 路径生效，JSON 路径重复 id 静默 last-wins。
15. **`$FLOW` 别名 / 表达式 KeyError 带可用根清单 / YAML fallback 告警**：
    `$FLOW` 等价 `$FLOW_ID`；未知根的 KeyError 列出全部可用根；JSON 解析
    失败但 YAML fallback 成功时打 warning（YAML 会把裸 `no/on` 篡改成
    `False/True`）。


    （纯数字毫秒或 ISO 8601，如 `"300"` / `"PT0.3S"`）。

### Storage：`db` 执行/流程存储从公开路径下架

**变更前**：`--execution-storage-type db` / `--flow-storage-type db` 以及
`create_storage_component("db", "execution"|"flow")` 会实例化
`SqlalchemyExecutionStorage` / `SqlalchemyFlowStorage`。
**问题**：这些实现的方法是 `async def`，而 `ExecutionStorage`/`FlowStorage` ABC
与 `FlowWorker`/`EventFilter` 均为**同步**调用——状态不会真正落盘（coroutine 被当返回值）。

**变更后**：

- factory 对 `db` + `execution`/`flow` 直接 `ValueError`（说明原因与替代方案）
- FlowWorker / EventFilter CLI：`--execution-storage-type` / `--flow-storage-type`
  仅接受 `memory` | `redis`
- `db` **subscription** / **event-bus** 另需 `PLAITA_ALLOW_EXPERIMENTAL_DB=1`（见下节）
- `plaita.storage.sqlalchemy` 类保留供实验，**不得**再经公开 factory/CLI 用于执行状态

```diff
-python -m plaita.server.flow_worker --execution-storage-type db --database-url ...
+python -m plaita.server.flow_worker --execution-storage-type redis --redis-url ...
```

```diff
-create_storage_component("db", "execution", database_url=...)
+create_storage_component("redis", "execution", redis_url=...)
+# 或 memory（单测 / 本地）
```

### Event：`HAS_SQLALCHEMY` 与 `__all__` 条件修复

**变更前**：SQLAlchemy 符号是否进入 `plaita.event.__all__` 错误地绑定 `HAS_REDIS`。
**变更后**：独立 `HAS_SQLALCHEMY` 标志；`__all__` 按该标志扩展。

### 任务队列：Redis List → Stream（at-least-once）

**变更前**：`EventFilter` / 测试脚本 `RPUSH`，`RedisFlowWorker` `BLPOP` — **at-most-once**。
**变更后**：

- 同一 `--queue-name` 键现为 **Redis Stream**（需 Redis 5+）
- 入队：`XADD`（`plaita.server.task_queue.enqueue_task`）
- 消费：consumer group（默认 `plaita-workers`）+ `XREADGROUP`；成功 `XACK`；超时 `XCLAIM` 回收 pending
- 处理失败**不** ack → 至少一次重投；畸形消息 ack 丢弃
- CLI：`--consumer-group`、`--consumer-name`、`--claim-min-idle-ms`

**迁移**：

```diff
-redis-cli RPUSH plaita:flow:queue '{"type":"start",...}'
+python -c "from plaita.server.task_queue import enqueue_task; import redis; r=redis.from_url('redis://...'); enqueue_task(r,'plaita:flow:queue',{...})"
```

旧 List 键上的积压消息**不会**自动迁移；升级前 drain 或换新 stream 键名。

重复 `start`/`resume` 在崩溃回收后可能再执行一次——节点与副作用应幂等。

### 中间态落盘：每步持久化

**变更前**：`PERSIST_EVERY_N_STEPS = 5`，连续推进最多丢 4 步。
**变更后**：默认 **1**（每步 `save_execution_state`）。挂起/结束/出错仍为立即落盘。

### Resume：execution lease（防并发）

**变更前**：多 worker 可同时 `resume_flow` 同一 `execution_id`。
**变更后**：

- `RedisFlowWorker` 默认注入 `RedisExecutionLease`（键 `plaita:execution:lease:{execution_id}`）
- 取得租约后才 resume；失败抛 `ExecutionLeaseError`，任务**不** XACK
- 推进循环中 `renew`；结束 `release`（仅 holder 可删）
- CLI：`--lease-ttl-seconds`（默认 120）
- 纯 `FlowWorker`（无 Redis）默认 `NullExecutionLease`（测试 / 单进程无约束）

无调用方代码迁移；部署多 worker 时重复 resume 会被租约挡住，直至 holder 释放或 TTL 过期。

### Experimental：`db` / SQLAlchemy 需显式门闩

**变更前**：`create_event_bus("db")` / `create_storage_component("db", "subscription")` 默认可建。
**变更后**：须设置环境变量 `PLAITA_ALLOW_EXPERIMENTAL_DB=1`，否则 `ValueError`。  
FlowWorker / EventFilter CLI 的 `--event-bus-type` / `--subscription-storage-type` **不再提供 `db` 选项**（生产用 redis）。

### 任务队列：DLQ + max deliveries

**变更后**：

- 默认 `--max-deliveries=5`；超过后写入 `<queue-name>:dlq` 并 ack 原消息
- CLI：`--max-deliveries`、`--dlq-key`
- status 响应含 `queue` 统计（pending / dlq_length / dead_lettered 等）
- List→Stream 迁移脚本：`scripts/drain_list_queue_to_stream.py`
- 运维文档：`docs-site/docs/distributed/ops-runbook.md`；幂等：`idempotent-resume.md`

### 文档：Distributed / FlowWorker 可靠性表述收紧（非 API break）

**变更前**：部分文档写「至少一次」「容错长时工作流」「生产级」；订阅 `event_type` 被写成 fnmatch。
**变更后（文档与注释）**：

- FlowWorker = suspend/resume 编排器；任务队列为 Stream **at-least-once**（见上节）
- 中间态落盘间隔公开为 `FlowWorker.PERSIST_EVERY_N_STEPS`（默认 1）
- 订阅匹配为 event_type **全等**；fnmatch 仅 handler 路径
- 用户文档入口：`docs-site/docs/distributed/flow-worker.md`「可靠性边界」

### 升级时存量状态处理（Redis / SQLite 里已有的东西怎么办）

0.4.x → 0.5.x 的执行状态 JSON schema 逐键兼容（已实测：0.4.0 挂起 → 0.5.x
手动 resume 可完成；回滚方向数据层亦可读）。断层在**键的语义与通道**：

1. **升级前 drain 队列**：队列键 `plaita:flow:queue` 从 0.4.x 的 List 换成了
   Stream（同名不同形）。不 drain 则新 worker 启动即 `WRONGTYPE`。用
   `scripts/drain_list_queue_to_stream.py` 迁移积压。
2. **回滚（0.5.x → 0.4.x）前必须排干 Stream**：0.4.x worker 用 BLPOP 消费
   List，对 Stream 同样 WRONGTYPE。手动 `XACK`+`DEL` 或让新 worker 清空后再回滚。
3. **存量挂起执行的事件驱动 resume 会静默失效**：0.4.x 的订阅 type 索引挂的是
   未解析表达式（`plaita:subscription:type:$INPUT.event_type`），0.5.x 的
   EventFilter 按解析后的类型检索查不到。手动 `resume_flow` 兜底可用；0.5.x 起
   EventFilter 对同 flow 孤儿订阅会打 warning 提示。
4. **凭据轮换提醒**：分布式 checkpoint 的 context 含**全量 `$ENV` 快照**
   （两版皆然），Redis 里的旧 state 携带密钥——升级窗口应轮换凭据。


---

### r14：显式 scratch 捐献 `ctx.set_scratch(key, value)`（plaita#164）

r5–r13 把就地改写的判据收紧成 **default-deny on provenance**（"只有**本次求值中由引擎
新建**的对象才允许被就地改写"）。方向是对的，但宿主最自然的写法被收紧了：

```python
ctx.set_state("$V", {"a": 1})                          # 宿主绑定（checkpoint 键）
parser.evaluate("$F.set($V,'k','v')", ctx.context)
# → ValueError: ... provenance could not be proven fresh（零写入）
```

**变更后**：给宿主一个**显式声明**入口。它不新增任何"证明对象是活的"谓词，只是把宿主
亲自声明为 scratch 的对象登记进**读取它的那一次逻辑求值**的 freshness 作用域。

```python
ctx.set_scratch("$V", {"a": 1})                        # 显式捐献（私有绑定）
parser.evaluate("$F.set($V,'k','v')", ctx.context)     # 就地写，允许
ctx.get_state("$V")                                    # {'a': 1, 'k': 'v'}
"$V" in ctx.to_dict()                                  # False —— 不进 checkpoint
```

**语义与边界（每条都有回归测试与变异钉住）**：

- **读取点登记**：`set_scratch` 只**标记**"待登记"。登记发生在某次逻辑求值真正读到这个
  键的那一刻（变量 thunk 从 context 取出根之后），所以不需要在 `set_scratch` 里造作用域、
  也不会在作用域打开时被一次**无关**求值消费掉——只有读到 `$V` 的求值才消费它。
- **只登记被声明的那个对象**（对象身份），**不登记它的图**：`$V.cfg` 这类嵌套目标仍被
  default-deny 拒绝。因此 scratch **不可能**把活根"洗白"成可写目标——把 `$INPUT` 的子对象
  塞进 scratch 的图里，嵌套目标照旧拒、`$INPUT` 零写穿。
- **一次求值有效**（r6 作用域语义原样保留）：声明被"读到它的那次求值"消费；**第二次求值
  里同一个宿主对象仍被拒**。要再用就重新 `set_scratch`。
- **不进 checkpoint**：scratch 是本次执行的私有绑定。它对表达式可见
  （`context["$V"]` / `"$V" in context`），但**不在** `__iter__` / `len()` / `keys()` /
  `__eq__` / `to_checkpoint_dict()` 的键视图里，`clean()` 与恢复会丢掉它。
- **不得占用 checkpoint 键**：五个持久化根与其它 schema 键、以及任何**已在** checkpoint
  里的键都被具名 `ValueError` 拒绝（否则 scratch 会遮蔽一个落盘绑定）。反向也成立：
  对同名键调用 `set_state` 会**取代** scratch 绑定。
- **声明一个"就是活持久化对象"的对象被具名拒绝**（`plaita#164`，含被别名到的根名）：
  宿主把同一个对象同时挂在 checkpoint 上时，登记它等于新开一条写穿通道。

**`set_scratch` 还是 `ctx.evaluate({...})`？选择建议**：

| 你的形状 | 用哪个 | 为什么 |
| --- | --- | --- |
| 宿主手里**已有**一个对象（自己造的 dict/list、来自外部库的载荷），想让本次求值就地改它 | `ctx.set_scratch("$V", obj)` | 保留对象身份与就地语义（像 `#69` 之前一样）；`ctx.evaluate(obj)` 会把它**换成引擎副本**，你手里的那个不会被改 |
| 只想在一个表达式里派生一个临时容器并就地改它 | `$F.json_loads(...)` / `$F.slice(...)` 等 producer | 不需要绑定，producer 结果本来就在本次求值里 fresh |
| 需要在**同一次**求值里让引擎物化一份副本（不关心原对象） | `with freshness_scope(): ctx.set_state("$V", ctx.evaluate({...}))` | `ctx.evaluate` 的 fresh 只活到它自己的作用域结束；跨求值再写它照旧被拒（r6 C2 反例） |
| 想跨多次求值反复改同一个对象 | 每次求值前 `set_scratch` 重新声明 | 不存在"长期活的证明"——那正是 r5 的进程级登记被 verifier 打掉的形状 |

**验证**：`tests/unit/test_issue164_scratch_declaration.py`（25 个测试 / 38 个 subTest）、
2 条变异（`164-scratch-registration-off` 撤销登记、`164-scratch-leaks-into-checkpoint`
让 scratch 漏进 checkpoint 键视图）全部 KILLED，且都由**普通 `FAILED` 行**杀死。
`#69` 的不变量未松动：五根 × 就地形状仍具名拒绝、零写穿；`#69` 的 14 位置探针 / 容器轴 /
X1/X2 由既有测试与 `scripts/issue69_mutation_matrix/probes.py` 复跑钉住。



## 0.5.0

0.5.0 是一次"激进 break"主版本，按架构师/开发者/使用者三视角批评集中整改。下面按"踩坑概率从高到低"排序。

### 0. 分布式：订阅失败不再挂起 + FlowWorker 默认启用 EventBus

**变更前**：`register_subscription` 失败仍返回 `is_suspend=True`；FlowWorker 需 `--use-event-bus` 才注入总线，默认与 EventFilter 订阅存储分裂。
**变更后**：

- 订阅失败抛 `FlowExecutionException`，**拒绝挂起**
- FlowWorker **默认**创建 EventBus；仅 `--no-event-bus` 可关闭（`--use-event-bus` 保留为无操作兼容）
- EventFilter CLI 优先使用 `event_bus.subscription_storage`

```diff
-python -m plaita.server.flow_worker --use-event-bus ...
+python -m plaita.server.flow_worker ...
+# 仅在确认无挂起节点时:
+python -m plaita.server.flow_worker --no-event-bus ...
```

### 0b. DSL：共享 ``validate_flow_ir`` + AI 编译门闭合

**变更前**：拓扑校验在 builder / sexpr 各写一份；`flow_from_source` / `compile_flow` 只做 AST→IR，不查拓扑；`_default_known_node_types` 吞掉 registry 异常返回空集。
**变更后**：

- 唯一入口：`plaita.dsl.ir_validate.validate_flow_ir` / `build_flow`（默认递归 `childFlow` / parallel branch flow）
- `FlowBuilder.build`、`parse_sexpr`、`flow_from_source`、`plaita_ai.compile_flow` 全部走同一门
- registry 不可用时**显式抛错**，不再静默空集

```python
from plaita.dsl import validate_flow_ir, build_flow, FlowIRValidationError
from plaita.dsl.codeflow import flow_from_source

flow = flow_from_source(src)  # 含拓扑校验
```

### 0c. EventBus：handler 成功后再去重；factory db 传 engine；sexpr experimental

**变更前**：三后端在 handler 执行**前** `mark_event_processed`，首次失败即永久丢事件；`create_event_bus("db")` 传 `database_url` 但构造函数要 `engine`；sexpr 与 @flow 并列宣传。
**变更后**：

- 去重改为 `is_event_processed` 跳过已成功项，**成功后再** `mark_event_processed`
- factory db / subscription storage：`database_url → create_async_engine → engine=`
- `plaita.dsl.sexpr` 标为 **experimental**（非一等作者路径）
- 新增 `plaita.core.node_context.NodeExecutionContext` Protocol；`Node.execute` 类型注解切过去

### 0d. codeflow 包拆分 + Console Admin/Contract 分面（非破坏）

**变更前**：`plaita/dsl/codeflow.py` 单文件巨石；Console Swagger/README 未区分管理面与契约面。
**变更后**：

- `plaita.dsl.codeflow` 改为包：`_common` / `_expr` / `_nodes` / `_stmt` / `_source`；公开导入路径不变（`from plaita.dsl.codeflow import flow, ...`）
- Console：`main.py` 用 `_mount_admin` / `_mount_contract` + OpenAPI tag `admin`/`contract`；README 增加「API 分面」说明（URL 不变）

### 1. `plaita.flow` shim 删除（import 路径 break）

**变更前**：`from plaita.flow import Flow, FlowExecution, ...` 可用（带 `DeprecationWarning`，原计划 0.6.0 删；0.6.0/0.6.1 发布时该承诺未执行，现顺延至 0.7.0——`plaita.errors` / `plaita.types` 兼容 shim 的移除承诺同步改为 0.7.0）。
**变更后**：`plaita/flow.py` 模块**已删除**，`from plaita.flow import ...` 直接 `ImportError`。

```diff
-from plaita.flow import Flow, FlowExecution, ExecutionMode
+from plaita import Flow, FlowExecution, ExecutionMode
# 或显式走 core:
+from plaita.core.flow import Flow
+from plaita.core.executor import FlowExecution, ExecutionMode
```

**迁移**：全局搜索 `from plaita.flow import` / `import plaita.flow`，改成 `from plaita import ...`（推荐）或 `from plaita.core.* import ...`。`parse` / `parse_and_run` / `FlowCallback` / `FlowEvent` / `CallbackManager` / `LoggerCallback` / `FlowExecutionException` 同样从 `plaita` 顶层导出。

### 2. CodeNode 默认 `sandbox_backend` 改 `docker`（需装 Docker daemon）

**变更前**：`register_code_node()` 默认 `sandbox_backend="restricted"`（RestrictedPython AST 沙箱，文档自承有 AST 绕过向量）。
**变更后**：默认 `sandbox_backend="docker"`（容器级隔离：`--network none` / `--read-only` / 资源上限）。Docker daemon 不可用时 **`register_code_node()` 拒绝注册** 并报错，列出三种降级路径，**不允许静默降级到 `restricted`**。

```python
from plaita.node import register_code_node

register_code_node()                  # 0.5.0 默认 docker, 需本机 Docker daemon
register_code_node(default_backend="subprocess")  # 半信任: 进程级, 不隔离 FS/网络
register_code_node(default_backend="unsafe")      # 完全信任代码作者
register_code_node(default_backend="restricted")  # 显式声明半信任 (AST 沙箱, 有已知绕过向量)
```

**迁移**：
- 生产多租户：装 Docker daemon，开箱即用。
- 无 Docker 环境 / 半信任：显式 `register_code_node(default_backend="subprocess")`。
- 内部开发完全信任：`register_code_node(default_backend="unsafe")`。
- daemon 不可用且未显式降级时，注册期报错：`CodeNode default sandbox backend is "docker" but Docker daemon is unavailable. Install Docker, or pass default_backend="subprocess"|"unsafe"|"restricted".`

### 3. HMAC nonce 改 Redis 后端（多 worker 真重放保护）

**变更前**：`plaita-console` 的 HMAC nonce 缓存是进程内单例，gunicorn 多 worker 下每个 worker 独立缓存，3 秒窗口内**跨 worker 重放可行**。`replay_protected=True` 给"已保护"的错觉。
**变更后**：抽象 `NonceStore` 接口，新增 `RedisNonceStore`（`SET nonce NX EX ttl` 原子操作，跨 worker 共享）。`verify_authorization` / `PlaitaClient` 启动时按配置注入 store。

```python
from plaita_console.backend.services.signature import (
    configure_nonce_store, enable_replay_protection,
)

# 单进程 / 测试 (默认)
enable_replay_protection()                       # InMemoryNonceStore

# 生产多 worker (推荐)
configure_nonce_store(redis_url="redis://...")   # 自动切 RedisNonceStore
enable_replay_protection()
```

**break**：`replay_protected=True` 在多 worker 部署下若未配 Redis store，启动时 `logger.warning` 明确告知"仅单 worker 保护"。

**迁移**：多 worker 部署（gunicorn/uvicorn workers>1）配 `configure_nonce_store(redis_url=...)`；单进程可继续用默认 `InMemoryNonceStore`。

### 4. `Flow.parse_flow` 解耦全局 registry（解析/执行分离）

**变更前**：`Flow` 在 Pydantic `model_validator(mode="before")` 里调 `get_default_registry()`，Flow 解析隐式耦合模块级单例 registry 状态——import 期注册了坏节点，全进程解析任何 flow 都坏。
**变更后**：`Flow` 只解析结构，`nodes` 字段保留为原始 dict；节点解析延迟到执行期 `Flow.resolve_nodes(registry)`。`Flow.from_string` / `from_file` / `Flow.model_validate` 默认仍调一次 `resolve_nodes(get_default_registry())`，**99% 用户用法不变**。

**迁移**：一般无需改动。仅当**自定义 registry** 或**在 import 期注册节点**时：
- 解析期显式传 registry：`Flow.from_string(content, registry=my_registry)` / `Flow.from_file(path, registry=my_registry)` / `Flow.model_validate(data, registry=my_registry)`，或对已解析对象调 `flow.resolve_nodes(my_registry)`。
- 也可实例化 `FlowExecution(registry=my_registry)`（注意第一位置参数是 parent execution，不是 flow），再调用其 `run_compatible` / `run_distributed`。
- 不再依赖"import 期注册的节点立刻对 `Flow.from_string` 生效"——确保注册发生在 `Flow` 解析之前（`init_default_registry()` 是推荐入口，见下条）。

### 5. `init_default_registry()` 显式初始化入口

**变更前**：默认 `NodeRegistry` 是模块级 `_default_registry = NodeRegistry()`，import 期静默创建，插件发现首次 `get_default_registry()` 时隐式触发——隐式可变单例 + import 期副作用。
**变更后**：新增 `init_default_registry(*extra_nodes, auto_discover=True)` 显式入口，建议在启动脚本调一次。`get_default_registry()` 仍可用，但首次隐式调用会 `logger.debug` 提示。

```python
from plaita.node import init_default_registry, register_code_node

init_default_registry()                       # 显式装载内置 + 插件节点
register_code_node(default_backend="docker")  # 按需 opt-in CodeNode
```

**迁移**：建议在应用启动脚本顶部把"隐式 `get_default_registry()`"换成显式 `init_default_registry()`。不调仍可用（向后兼容），仅多一条 debug 日志。

### 6. `execution.mode` 内部类型 str → `ExecutionMode` enum

**变更前**：`execution.mode` 是裸字符串（`"normal"`/`"generator"`/`"distributed"`），全库散落 `mode == "generator"` 字符串比较——拼写错误静默成 `False`。
**变更后**：内部类型 `Optional[ExecutionMode]`，比较一律走 enum。**公共入口仍接受字符串**：`FlowExecution(mode="generator")` / `execution.mode = "generator"` / `FlowExecution.run(flow, mode="generator")` 在边界处经 `_coerce_mode` 统一一次。注意 `Flow.run()` / `flow.debug()` **没有** `mode` 参数（`flow.run(mode=...)` 会把 `mode` 当作输入 dict 的一个键）——生成器模式请用 `flow.debug()`。

```python
from plaita.core.executor import ExecutionMode

# 这两种写法等价 (公共入口接受字符串):
FlowExecution.run(flow, mode="generator")
FlowExecution.run(flow, mode=ExecutionMode.GENERATOR)

# 同步生成器单步调试的便捷写法 (等价于上面的 generator 模式):
for step in flow.debug():
    ...

# 节点插件内部比较改 enum (若你直接读 execution.mode):
if execution.mode == ExecutionMode.GENERATOR:   # 0.5.0
    ...
# 而非:
# if execution.mode == "generator":             # 0.4.x (0.5.0 起不再推荐)
```

**迁移**：99% 代码无需改动（公共入口兼容字符串）。仅当你**直接读 `execution.mode` 并和字符串比较**（第三方节点插件），建议改成 `ExecutionMode.GENERATOR` / `ExecutionMode.DISTRIBUTED` / `ExecutionMode.NORMAL`。`execution.mode` getter 现在返回 `ExecutionMode` enum（不再是 str），`assertEqual(execution.mode, "normal")` 这类断言需改成 `ExecutionMode.NORMAL`。

### 7. `Parallel` background branches 失败不再静默

**变更前**：`Parallel` 节点 fire-and-forget 后台分支失败时无 future 引用、无错误回调、无超时——失败完全沉默。
**变更后**：持有 future 引用，`add_done_callback` 记录失败分支异常到模块级 `_BG_STATE`（按 `execution_id` 索引，避免 `FlowExecution` 在多进程下不可 pickle）。新增调试接口：

```python
from plaita.node.concurrent import wait_background_branches, get_background_errors

wait_background_branches(execution, timeout=5)   # 可选: 等后台分支收尾
errors = get_background_errors(execution)         # 取后台分支失败列表
```

**迁移**：无需改动（fire-and-forget 语义不变，只是失败不再沉默）。调试时可调 `wait_background_branches` / `get_background_errors`。

### 8. `$ENV` 升级静默失败告警

**变更前**（0.4.0）：`$ENV` 改 allowlist，旧 flow 升级后 `$ENV.HOME` 静默变空字符串，下游继续跑不报错——"沉默地坏掉"。
**变更后**：`Flow.model_validate` / `model_validate_json` 扫描所有节点表达式，若发现 `$ENV.` 引用但 `expose_env` 为空，`logger.warning` 一次列出 key 名 + 修复指引。**不报错**（保持兼容），让沉默变可见。

**迁移**：无需改动。升级后留意日志 warning，按提示把 key 加到 `expose_env`。

### 附：异常治理 / executor 拆分 / mutmut 范围

- **bare `except:` 清零**：`plaita/` 内 5 处 bare `except:` 全部改 `except Exception:` 并加 `logger.warning`/`debug`；CI 对 `E722`（bare except）硬失败，对 `BLE001`（blind-except）仅 warn（`--exit-zero`），存量位点随整改逐步清理。
- **`executor.py` 拆分**：757 行拆成 `plaita/core/strategies.py`（`RunOptions`/`ExecutionMode`/三策略/helper）+ `plaita/core/executor.py`（`FlowExecution` facade）。公共导出不变（`from plaita import FlowExecution, ExecutionMode` 仍可用）。
- **mutmut 覆盖范围诚实声明**：变异测试**仅覆盖纯同步路径**（callback/expression/parallel_executor/calculate/decide）；async/distributed/timeout 不在 mutmut 自动覆盖范围，见 `docs/mutation-testing.md` 顶部声明。引用变异测试结果时请标注"仅同步路径"。

---

## 0.4.0

0.4.0 一次性引入了 5 处破坏性变更，集中在**安全模型**与**接口收紧**。下面按"踩坑概率从高到低"排序。

### 1. `$ENV` 改为 allowlist 模型（默认空）

**变更前**：流程表达式 `$ENV.XXX` 可读取任意环境变量，靠一份"敏感前缀黑名单"过滤。
**变更后**：`$ENV` 默认空；Flow 必须显式声明 `expose_env` 才能读到环境变量。

```python
# 变更前
flow = Flow.from_string('{"flow_id": "demo", ...}')
# 流程里 $ENV.HOME 仍能读到

# 变更后
flow = Flow.from_string(
    '{"flow_id": "demo", "exposeEnv": ["HOME", "API_BASE"], ...}',
)
# 或 Python 构造：
flow = Flow(flow_id="demo", expose_env=["HOME", "API_BASE"], ...)
```

**迁移**：搜索代码库所有 `$ENV.` 引用，把 key 名收集起来，加到对应 Flow 的 `expose_env`（JSON 写 `exposeEnv`，Python 写 `expose_env`）。

**注意（黑名单已移除）**：早期 0.4.0 曾保留 `_SENSITIVE_ENV_PREFIXES` 黑名单作为 allowlist 之上的「第二层防御」，但它的 `startswith` 匹配挡不住 vendor 前缀密钥名（`OPENAI_API_KEY` / `STRIPE_KEY` / `PG_CONN` 等），已移除。现在 allowlist 命中即暴露，仅打 `logger.warning` 做审计——**请自行确认 `expose_env` 里没有不该暴露的密钥**，不要再依赖任何「敏感词拦截」。

### 2. `CodeNode` 移出默认注册表（opt-in）

**变更前**：`{"type": "code", ...}` 节点开箱即用。
**变更后**：CodeNode 不在默认 `_BUILTIN_NODES` 里，必须显式注册。

```python
from plaita.node import register_code_node

register_code_node()  # 默认注册到全局 registry
# 或自定义 registry：
# register_code_node(my_registry)
```

**迁移**：在 worker / server 启动入口（`main()` 或 FastAPI lifespan）调一次 `register_code_node()`。

未注册时解析流程会得到错误：`unRecognized node type: code. CodeNode was moved out of the default registry in 0.4.0; call register_code_node() at startup to opt in.`

### 3. CodeNode 默认沙箱化（RestrictedPython）

**变更前**：Python 后端走 raw `exec()`，可访问任意内置模块。
**变更后**：默认 `sandbox_backend="restricted"`（RestrictedPython AST 沙箱），允许 import 的模块白名单见 `SANDBOX_SAFE_MODULES`（math/json/re/datetime 等纯计算模块）。

可选后端：

| backend | 隔离强度 | 适用场景 |
|---|---|---|
| `restricted`（默认） | AST 级 | 多租户、纯计算 |
| `subprocess` | 进程级 | 需要 math/datetime/json 以外的模块，半信任 |
| `docker` | 容器级（无网络/只读 FS） | 完全不信任代码，生产多租户 |
| `unsafe` | 无 | 完全信任代码作者（内部开发） |

```json
{
  "type": "code",
  "language": "python",
  "sandbox_backend": "docker",
  "code": "def run(input): ..."
}
```

**迁移**：如果你的 CodeNode 用了 `os`/`sys`/`open`/网络/文件，要么改代码、要么显式设 `sandbox_backend="unsafe"`（仅当你完全信任代码作者）。**JS 后端（PyExecJS）仍未沙箱化，且 PyExecJS 自 2020 年起未维护——不要在生产多租户场景使用。**

### 4. `parallel mode="coroutine"` 下线

**变更前**：`mode="coroutine"` 可用，但实际在任何 running event loop（FastAPI/Starlette）中 100% 崩 `RuntimeError`。
**变更后**：直接 `raise ValueError`，错误消息指引改用 `mode="thread"` 或 `mode="process"`。

**迁移**：把 `"mode": "coroutine"` 改成 `"mode": "thread"`。

```diff
 {
   "type": "parallel",
-  "mode": "coroutine",
+  "mode": "thread",
   "branches": [...]
 }
```

### 5. `Flow.start_node` 删入度 0 启发式

**变更前**：Flow 没有显式 `type=start` 节点时，会扫描节点数组找"未被任何 next/branch 引用的节点"当入口——多个孤儿节点时结果依赖数组顺序。
**变更后**：没有显式 Start 节点直接抛 `FlowStartMissingError`。

**迁移**：

1. 手写 JSON/YAML 的：检查所有 flow 定义，确保每个 flow 都有一个 `{"type": "start", ...}` 节点。
2. 用 `@flow` / `FlowBuilder` 的：框架会自动注入，无需改动。
3. 用可视化编排工具导出的：检查导出结果是否包含 Start 节点。

### 6. `execution.context` 类型由 `dict` 改为 `CheckpointState`

**变更前**：`ExecutionContext.context` / `FlowExecution.context` 返回 `Dict[str, Any]`（内部 `_context` dict 的活引用）。
**变更后**：底层存储改为 `plaita.core.state.CheckpointState`（Pydantic `BaseModel`），`context` 返回该 model 实例。它实现完整 dict-like 协议（`__getitem__`/`__setitem__`/`__contains__`/`get`/`keys`/`items`/`__iter__`/`__eq__`），对 `$INPUT`/`$NODE` 等 prefixed key 的访问与旧 dict 完全一致；`__eq__` 与 plain dict 比较也保持兼容（`assertEqual(ctx.context, {...})` 仍通过）。

```python
# 变更前
assert isinstance(execution.context, dict)

# 变更后
from plaita.core.state import CheckpointState
assert isinstance(execution.context, CheckpointState)
# dict-like 行为不变：
execution.context["$INPUT"]
"$LAST_NODE" in execution.context
execution.context.get("$NODE", {})
```

**迁移**：

- 直接 `isinstance(ctx, dict)` 的断言会失败——改用 `isinstance(ctx, CheckpointState)` 或只断言 dict-like 行为（`in` / `[]` / `.get`）。
- `to_dict()` / `from_dict()` 的存储格式**逐键兼容**，Redis/SQL 里旧的 checkpoint 无需迁移即可加载（round-trip property test 钉死）。
- 把 `execution.context` 当 dict 传给第三方库（`json.dumps` / `dict(...)` / `deepcopy`）仍可用（dict-like 协议）；但需要"纯 dict"的场景请显式 `dict(execution.context)` 或 `execution.to_dict()`。

### 7. `execution.flow_id` / `last_node_id` / `last_branch` → `execution.state.xxx`

**变更前**：`FlowExecution` facade 以裸属性透传 typed 系统状态——`execution.flow_id`、`execution.last_node_id`、`execution.last_branch`（13 个 `@property` 透传的一部分）。
**变更后**：裸属性删除，统一走 `execution.state` 视图（`plaita.core.executor._StateView`，`None` 归一化）：

```python
# 变更前
flow_id = execution.flow_id or "unknown"
upstream = execution.last_node_id

# 变更后
flow_id = execution.state.flow_id or "unknown"
upstream = execution.state.last_node_id
branch = execution.state.last_branch
```

**迁移**：

- `plaita/server/nodes/{redis_queue,kafka_queue,http_callback,delay,approval}_node.py` 与 `plaita/node/assignment.py` 已一并改到新 API；第三方节点插件读这几处需同步改。
- `execution.context["$FLOW_ID"]` / `execution.get_state("$FLOW_ID")` 字典式访问**不变**，仍可用。
- `execution.mode` / `execution.timeout` 不变（内部改存 `RunOptions`，facade property 保持兼容）。
- `ExecutionContext`（内部对象）的 `last_node_id` / `last_branch` / `flow_id` typed property 保留——它们是 `execution.state` 视图的底层实现，runner / strategy 仍用。

### 附：`ErrorStrategy` 字段类型 str → enum

**变更前**：`ErrorHandler.strategy` 字段类型 `Optional[str]`，比较时用 `_strategy_eq(value, member)` 容忍字符串/enum 混用。
**变更后**：字段类型 `ErrorStrategy`（enum），validator 接受 enum/str/`continue_with` 下划线别名，比较用 `==`。

**迁移**：99% 的代码无需改动（validator 兼容字符串）。仅当你**手动构造** `ErrorHandler(strategy="continue_with")` 而不是从 JSON 解析时，建议改成 `ErrorHandler(strategy=ErrorStrategy.CONTINUE_WITH)`。`_strategy_eq` 已删除，外部 import 需要清理。

---

## 升级检查清单

升级到 0.5.0 前过一遍：

- [ ] 全局搜索 `from plaita.flow import` —— 改成 `from plaita import ...`
- [ ] 全局搜索 `register_code_node` —— 确认 Docker daemon 可用，或显式 `default_backend="subprocess"|"unsafe"|"restricted"`
- [ ] 多 worker 部署：配 `configure_nonce_store(redis_url=...)` + `enable_replay_protection()`
- [ ] 节点插件直接读 `execution.mode` 的：字符串比较改 `ExecutionMode` enum
- [ ] 启动脚本：用 `init_default_registry()` 显式初始化默认 registry（可选，向后兼容）
- [ ] 跑一遍单元测试套件：`pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"`
- [ ] 跑一遍集成测试套件：`pytest tests/integration/`

升级到 0.4.0 前过一遍：

- [ ] 全局搜索 `$ENV.` —— 列出 key 加到对应 Flow 的 `expose_env`
- [ ] 全局搜索 `type.*code` —— 在启动入口加 `register_code_node()`
- [ ] 全局搜索 `mode.*coroutine` —— 改成 `thread`
- [ ] 全局搜索 flow 定义 —— 确认每个都有 `type=start` 节点
- [ ] 跑一遍单元测试套件：`pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"`
- [ ] 跑一遍集成测试套件：`pytest tests/integration/`

升级遇到本文件没覆盖的行为变化？开 issue 贴复现步骤，会补到这里。
