# plaita#69 就地 mutate 门 —— 机械生成的分类矩阵（r7：单拷贝入口 + 单一类型表）

本文件由仓内生成器 ``scripts/issue69_mutation_matrix/table.py``、
``scripts/issue69_mutation_matrix/probes.py``、
``scripts/issue69_mutation_matrix/scope_inventory.py`` 与
``tests/unit/test_issue69_mutation_matrix.py`` 的**同一批**生成器/断言函数产出：下面的
每个计数与全表都能由 §8 的命令原样重算。

**规则（r6 可判定谓词，与实现 / `MIGRATION.md` / `docs-site/docs/guide/expressions.md`
逐句一致）**：

> 就地 mutator（`has_side_effects=True`）的**目标位**（arg0）在求值后必须被证明是
> **本次逻辑求值**由引擎新建的对象（freshness **作用域**：`ContextVar` 持有
> `id(obj) -> obj`，随最外层求值入口开、`finally` 里关；嵌套求值共享；域外登记是
> no-op）。这是唯一的"允许"来源（default-deny on provenance），不再试图证明"这个对象
> 是活的"（那个集合无界：任意嵌套子对象、任意容器、懒快照、宿主对象），也不再保留
> **进程级**登记（r5 的模块级 LRU 让"引擎在这进程里造过它"就等于 fresh ⇒ verifier 的
> C2 反例：写进 `$NODE`/checkpoint 的引擎对象在**另一次**求值里仍可被就地改写）。
>
> ① 目标路径的根映射到五个持久化字段之一（`input_value`/`node_results`/
> `global_context`/`parent_context`/`env`，含 `express_*_name` 别名）且上下文是 live
> `CheckpointState` ⇒ **具名拒绝**（消息点名实参位 + 完整路径 + 根）；目标位**不再**为
> 活 state 的非持久化键构造 `_live_alias_index`（实测 ~8µs/次）——拒绝依据是
> default-deny 的 freshness，容器值身份探测（`_probe_container_alias`）在目标位保留；
> ② 普通容器（嵌套表达式串的作用域）按**对象身份**探测可达的活对象 ⇒ **具名拒绝**
> （键名不参与判定；r4 保留，但已非 load-bearing —— freshness 独立拒）；
> ③ 调用实参按 callee 的**结果来源**分类（r3 保留）：**DEEP**（新图：`json_loads`/`split`/
> `fresh_producer=True`）/ **SHALLOW**（新容器、元素可能别名：`slice`/`reverse`/…）由
> 分类保证 fresh ⇒ 目标位放行，值参位浅 producer 仍走**引擎自有结构拷贝**去别名（r3 F1）；
> **LIVE**（`get`/`getDictValue`/`getListItem`/`ifelse`/`and`/`or`/mutator）⇒ 目标位要求
> 运行时身份命中 fresh，否则具名拒绝；**UNKNOWN** ⇒ fail-closed 拒绝；
> ④ 引擎新建的容器（`ExecutionContext.evaluate` 重建的字面量外壳 / producer 结果 /
> `_detach_alias` 的**引擎结构拷贝** / 循环条件隔离副本）⇒ **放行**（由构造证明）。
> 数据路径上的结构拷贝只有**一个入口** `_engine_copy`（r7）：接受集合 = `plaita.core.types`
> 的数据模型（`dict`/`list`/`tuple`/`set`/`frozenset` + `native_types` 推导的标量，含
> `Decimal`/`datetime`/`date`，另加不可变额外类型 `bytes`/`complex`）；容器/标量**子类**
> （`OrderedDict`/`defaultdict`/`namedtuple`/`IntEnum`/覆写 `__deepcopy__`/`__copy__`/
> `items`/`__iter__`/`__getitem__` 的宿主类）被**归一化**成普通基类值（基类协议重建，
> 宿主钩子一次都不调用），绝不按引用透传；只有真·非数据模型的**可变**对象（自定义
> `MutableMapping`、`bytearray`、不透明宿主类、dict view）才抛具名错误
> `not part of the persistable data model`（点名类型）。r7 之前 `Decimal`/`datetime`/
> `date` 被这份 ad-hoc 列表误拒（Finding 2），现已由 `native_types` 推导修复。
> `$GLOBAL` 的每 run 快照不属于"拷贝入口"，走**物化边界** `_snapshot_value`
> （非容器宿主对象按引用共享——与 checkpoint / `$PARENT` / 子流程输入同一口径）。
> **r8 更正**：r7 这里写的"容器重建为精确类型、copy 钩子零调用、AST 守卫覆盖"当时**不成立**
> ——快照用宿主方法读容器（`value.items()`/`enumerate(value)`/`value.copy()`），`set` 子类
> `copy()->self` 会让快照就是活对象（`$GLOBAL.s is <flow 定义里的 set>: True`）。r8 改为
> **基类协议重建**（`dict.items(sub)`/`list.__iter__(sub)`/…/基类 `__len__`）后该承诺才为真，
> 并由兄弟守卫 `snapshot_host_protocol_calls` 机械覆盖（登记 `_SNAPSHOT_PATH_REGISTRY`，
> allow-list = 接收者是 C 基类名；对 r7 的 `state.py` 实测命中 5 处）。
>
> **r10 更正（三处，与实现逐句一致）**：① D 的判据从"`Mapping` 是否排在 `dict` 之前"扩成
> **三条机械可判、零宿主钩子**的析取（MRO 顺序 ∨ Python 级覆写 `__iter__`/`__len__` ∨
> 实例自带 `__dict__` 状态）—— r9 的 MRO-only 漏掉 `ItemsLiar(dict)` /
> `DictFirstMapping(dict, Mapping)` / `ItemsOnlyLiar(dict)` 这些**数据在 `self._d`** 的形状：
> 两条拷贝路径都**静默**产出 `{}` 且 DSL 报成功；扩后两条路径与 DSL 一致**具名拒绝**
> （消息点名触发的信号）。
> ② 标量归一化的 `datetime` 读取器补回 PEP 495 `fold`（protocol 2 的 `__reduce__` 不带它
> ⇒ DST 歧义墙钟时间静默偏 1 小时，而 `type(copy) is datetime` 与 `copy == value` 仍成立）。
> ③ `bytes` 读取器从 `bytes(memoryview(v))` 换成基类描述符
> `bytes.__getitem__(v, slice(None))`（PEP 688 起 3.12+ 的宿主 `__buffer__` 能劫持
> `memoryview`）。判定面同时收紧：标量归一化的判决是**全部可观察字段**（`datetime` 含
> `fold`/`tzinfo` 身份/`timestamp()`），不再是 `type(x) is base` + `==`。
>
> **r11 更正（与实现逐句一致）**：④ r10 的 clause (iii) 测的是**代理**（"实例有非空
> `__dict__`"），不是它宣称的**性质**（"对象可能在真实 `dict` 存储之外持有状态"）。
> `class SlotsItemsLiar(dict): __slots__ = ("_d",)` 没有实例 `__dict__` ⇒ r10 判否 ⇒
> 两条拷贝路径静默产出 `{}`、DSL **报成功**（`SlotsItemsKeysValuesLiar` /
> `SlotsGetItemLiar` / `HidingDict`（`@property def __dict__` 遮蔽真实实例字典）同族；
> 只多一个 `__iter__` 的 `SlotsIterLiar` 却被 clause (ii) 拒 —— 两者只差**数据放在哪**）。
> r11 把 clause (iii) 换成**性质判据** `instance_state_signal`：非空 `__dict__` ∨ MRO 里
> `dict` 之前任一层声明**非空** `__slots__`（字符串/元组/列表/集合/字典形态、继承/合并的
> 槽）∨ 遮蔽的 `__dict__` ∨ 非 C 槽实现的 `__getstate__`/`__setstate__`/`__reduce_ex__`
> —— 全部**只看类对象**，不调用宿主 `__getattribute__`/`items`/`__iter__`。
> 拒绝消息**点名触发的信号**（`instance state (__slots__)` / `instance state (__dict__)` /
> `shadowed __dict__` / `instance state hooks (...)`）。
> **过拒代价（明说）**：带任意实例属性**或任何非空 `__slots__`** 的 `dict` 子类会被具名
> 拒绝，即使数据确实在真实存储里 —— 恢复**不要**用 `dict(...)`：它读的是**真实 dict
> 存储**（即 `real dict storage`），对"数据在别处"的形状恰好返回 `{}`（静默错误答案）；`dict(v.items())` 则**采信
> 宿主自述**（调用宿主 `items()`）。诚实的恢复是从**已知良好**的数据重建，或用
> `ctx.evaluate` / producer（`docs-site/docs/guide/expressions.md` 同一处注记）。
> ⑤ 宿主 `__getattribute__` 抛异常时 `isinstance` 回落读 `__class__` ⇒ r6 起漏**裸**
> `RuntimeError`；r11 在 `_canonical_persistable_kind` / `state._snapshot_isinstance` 具名
> `_EngineCopyRefused` / `StateSnapshotError`。
> ⑥ `_detach_alias` 的视图分支以前对宿主导出的 ABC 视图子类调用 `list(value)`
> （实测钩子 `['view.__iter__','view.__len__']`）并把宿主自述内容写成数据；r11 只对
> `__iter__`/`__len__` 都解析到 **C 槽描述符**的视图（引擎自产的 `dict_keys`/
> `dict_values`/`dict_items` 及 C 派生 `odict_*`）物化，其余具名拒绝。
> ⑦ 判据读 MRO 与类命名空间时**不走属性协议**：用 C 描述符
> `type.__dict__["__mro__"]` / `type.__dict__["__dict__"]`（`_real_mro` /
> `_class_namespace`）。宿主**元类**覆写 `__getattribute__` 时，`cls.__mro__` /
> `vars(klass)` 会抛异常（判据漏**裸** `RuntimeError`）或**说谎**（实测把 `__mro__`
> 换成 `(dict, object)`、`__dict__` 换成 `{}` 就能把声明了 `__slots__` 的基类抹掉 ⇒
> 重新静默产出 `{}`，同一条阻塞反例换个通道）。断言见
> `TestIssue69R11InstanceStateIsTheProperty::test_lying_metaclass_cannot_hide_the_slots_base`
> / `test_raising_metaclass_does_not_leak_a_bare_runtimeerror`。
>
> **作用域/物化面清单（r7 结构要求）**：`plaita/` 里"节点字段/状态值变成求值作用域、
> 子流程输入、集合/初值/item"的**全部**调用点被 AST 枚举进
> ``scripts/issue69_mutation_matrix/scope_inventory.json``（§S，**87** 点），
> ``tests/unit/test_issue69_scope_inventory.py`` 守卫"新增未分类点 ⇒ 红"。键**既不含
> 行号也不含列号**（只改缩进/上方插空行不再假红）；`getattr(state, "evaluate")(v)`
> 间接分发被归一为 `getattr.evaluate` 后分类；同缩进同形状的点用遍历序 `#k` 区分。
> r7 删除四个**作用域外死登记**（r6 起就是 no-op 的 `note_fresh_closure`：子流程输入 /
> 集合 / `Reduce.initial` / 并行分支输入），`fresh-register` 10 → **6**、`materialise` 13 → **14**（`$GLOBAL` 快照改走 `_snapshot_value`）、总数 90 → **87**
> ——见 Finding 3 的语义决定。已知局限：属性名动态计算时静态扫不到（负控点名）。
> X1/X2 正因这一面从未被枚举而被漏（嵌套串作用域、`Reduce.initial`）。

## 1. 生成维度与总数（实测）

| 轴 | 取值 | 基数 |
|---|---|---|
| 写形式 | 9 个 `has_side_effects=True` 内建 | 9 |
| 根 | 5 规范根 + 别名 `$IN` `$G` | 7 |
| 路径形式 | 本体 / `.cfg` / `.nested.d` / `.items` / `.lst` / `.items[0]` | 6 |
| **目标位（arg0）** | 9 × 7 × 6 | **378** |
| **值参位（arg1/arg2，7 个有值参位的函数）** | 7 × 7 × 6 | **294** |
| **浅 producer 值参（r3 F1）** | 16 浅 producer × 7 根 | **112** |
| **callee 实参触达根的值参（r3 F2 拒绝半）** | 6 callee × 7 根 | **42** |
| **非根 callee × 9 写形式（r3 F2 允许半）** | 8 callee × 9 写形式 | **72** |
| **producer × position 全表（值参轴）** | 73 函数 × 7 根 | **511** |
| **producer × position 全表（目标位轴）** | 73 函数 × 7 根 | **511** |
| **r6 容器键名轴（容器级 × 零登记/有登记）** | 27 × 2 | **54** |
| **r6 多位置探针（§X）** | 14 位置 | **14** |
| **作用域/物化面清单（§S）** | 87 调用点 | **87** |

实测：目标位 **378/378 拒绝**、值参位 **294/294 拒绝**、浅 producer 值参 **112/112 去别名**
（零共享可变对象、零写穿）、callee 触达根的值参 **42/42 拒绝**、非根 callee **72/72 不被门拒**；
producer × position 全表 511 值参格 **0 格可制造别名**（406 allow / 105 refuse），
511 目标位格 **0 次根写入**（77 allow / 105 refuse / 329 native-error）；
容器轴 **零登记与有登记两套状态各 27 格 = 18 拒绝 / 9 放行（引擎物化副本）/ 0 native-error**，
同一 (值种类, 嵌套) 下三种键名判决完全一致（名字不决定），**零写穿**；
多位置探针 14 格 = **13 具名拒绝 + 1 私有副本隔离 / 0 写穿**（§X；`Reduce.initial`、
`loop-item`、`map-item`、`parallel-branch-input` 四类**边界副本**的就地写在 r5 是"改在私有
副本上"、在 r6/r7 是"具名拒绝"——副本在父求值里物化、子执行是**另一次**逻辑求值，登记
面按"本次求值"判 fresh；r7 进一步删掉了这四处**作用域外**的 `note_fresh_closure` 死登记
（§S），语义不变。拒绝原因**不是**"`ContextVar` 不跨线程"：`mode="coroutine"` 在
MainThread 里同样被拒，根因是调用点不在任何求值作用域内；`parallel-branch-input` 的拒绝
被并行执行器记进 `$NODE.p.<branch>.__parallel_error__` 而不是 flow 级上抛，探针改为读该
通道，见 §7/§X）。

## 2. 允许桶（每条 allow 都由构造证明 fresh）

| 目标种类 | 实测分布（9 写形式） |
|---|---|
| `non-root-var` = `$VAR`（引擎物化） | {"allow": 5, "native-error": 4} |
| `non-root-child` = `$VAR.cfg` | {"allow": 7, "native-error": 2} |
| `producer-json_loads` = `$F.json_loads($INPUT.raw)` | {"allow": 5, "native-error": 4} |
| `producer-slice` = `$F.slice($INPUT.items,0,3)` | {"allow": 5, "native-error": 4} |
| `producer-reverse` = `$F.reverse($INPUT.items)` | {"allow": 5, "native-error": 4} |
| `producer-append` = `$F.append($INPUT.items,4)` | {"allow": 5, "native-error": 4} |
| `producer-insert` = `$F.insert($INPUT.items,0,4)` | {"allow": 5, "native-error": 4} |
| `producer-sort` = `$F.sort($INPUT.items)` | {"allow": 5, "native-error": 4} |
| `producer-concat` = `$F.concat('a','b')` | {"native-error": 9} |
| `producer-json_literal` = `$F.json_loads('{"a": 1}')` | {"allow": 7, "native-error": 2} |

**fresh 来源逐条对应**：`$VAR`/`$VAR.cfg` = `ExecutionContext.evaluate` 重建的字面量外壳
（测试 harness 用 `ctx.evaluate(PAYLOAD())` 绑定；宿主直接 `set_state` 的见 §7 边界）。
**r6 关键**：物化与就地改写必须在**同一次**逻辑求值里——生成器/测试把 `$VAR` 的物化与
该 cell 包在同一个 `freshness_scope()` 内（真实用法：节点字段一次求值里造出容器、随即
就地改写它）。**跨求值**的引擎对象不再是 fresh：`ctx.evaluate(...)` 之后**另一次**
`parser.evaluate("$F.set($VAR,…)")` 具名拒绝（verifier 的 C2/B3 形状，见 §7 与 §M）。
producer 行 = DEEP/SHALLOW 分类在调用点登记；逃生口
`$F.set($VAR,'k',$F.json_loads($F.json_dumps({p})))` 五根全 `allow`（DEEP 闭包）；
`$F.set($VAR,'k',$F.slice({p},0,1))` 五根全 `native-error`（浅结果经**引擎自有结构拷贝**
去别名后，dict 本体上的 `slice` 原生 TypeError —— 门并未拒绝）。

**浅 producer 目标位**（§4c 的 77 allow）：`$F.slice/append/reverse/...` 的结果是**新容器**，
就地改写只改它自己的条目，不触根；其元素仍可能与实参共享，但**后续对元素的就地写**
会因元素不是 fresh 被具名拒绝（r3 F1 的通道因此结构性关闭，引擎结构拷贝仍保留为第二道
防线）。`$F.slice($VAR.items,…)` 与 `$VAR` 仍共享元素（就地语义保留），但
`$F.set($VAR.k[0],…)` 只有在 `$VAR.k` 是**同一次求值**里物化的 fresh 容器时才放行。

## 3. 拒绝桶：live 引用 / 未声明 callee（9 写形式 × 8 目标 = **72/72**）

| 目标 | 实测 |
|---|---|
| `live-get` = `$F.get($INPUT,'cfg')` | refuse |
| `live-getDictValue` = `$F.getDictValue($INPUT,'cfg')` | refuse |
| `live-getListItem` = `$F.getListItem($INPUT.items,0)` | refuse |
| `live-ifelse` = `$F.ifelse(1,$INPUT.cfg,$INPUT.cfg)` | refuse |
| `live-and` = `$F.and($INPUT.cfg,$INPUT.cfg)` | refuse |
| `live-or` = `$F.or($INPUT.cfg,$INPUT.cfg)` | refuse |
| `undeclared-copy` = `$F.copy($INPUT.cfg)` | refuse（fail-closed） |
| `undeclared-deepcopy` = `$F.deepcopy($INPUT.cfg)` | refuse（fail-closed） |

**r3 F2 允许半**：同样 8 个 callee 只在**引擎物化的** `$VAR` 上取值 ⇒ `8 × 9 = 72/72`
**不得因门被拒**；`$F.set($VAR,'k',$F.get($VAR,'x'))` 与
`$F.set($F.get($VAR,'x'),'k','v')` 均放行（后者目标位运行时身份命中 fresh 容器）。
**r3 F2 拒绝半**：callee 自己的实参触达根（6 callee × 7 根 = 42 格）⇒ 值参位 42/42 拒绝、
零写入；消息含 `argument #3` + 拒绝根路径。

## 4. producer × position 值参全表（73 名 × 7 根）

**值参轴（511 格）**：DEEP 42 名 + SHALLOW 16 名 = 406 格 allow（浅名去别名后零共享可变
对象）；LIVE 6 名 + MUTATOR 9 名 = 105 格 refuse；**alias-manufacturing 格 = 0**；
非根值参 73/73 allow（`$VAR` 为引擎物化值）。`add`/`mul` 双形态 ⇒ 归 SHALLOW 并走"值参
深拷贝"，标量累加 `$F.set($VAR,'total',$F.add($INPUT.n,1))` 不被误拒。

| function | class | value arg over root (7 cells) | alias manuf. | value arg over $VAR |
|---|---|---|---|---|
| `abs` | DEEP | allow | no | allow |
| `add` | SHALLOW | allow | no | allow |
| `addListItem` | SHALLOW | allow | no | allow |
| `and` | LIVE | refuse | no | allow |
| `append` | SHALLOW | allow | no | allow |
| `capitalize` | DEEP | allow | no | allow |
| `ceil` | DEEP | allow | no | allow |
| `clear` | MUTATOR | refuse | no | allow |
| `clearDict` | MUTATOR | refuse | no | allow |
| `concat` | DEEP | allow | no | allow |
| `contains` | DEEP | allow | no | allow |
| `delDictValue` | MUTATOR | refuse | no | allow |
| `delListItem` | MUTATOR | refuse | no | allow |
| `delete` | MUTATOR | refuse | no | allow |
| `div` | DEEP | allow | no | allow |
| `endswith` | DEEP | allow | no | allow |
| `eq` | DEEP | allow | no | allow |
| `extend` | SHALLOW | allow | no | allow |
| `floor` | DEEP | allow | no | allow |
| `get` | LIVE | refuse | no | allow |
| `getDictKeys` | SHALLOW | allow | no | allow |
| `getDictValue` | LIVE | refuse | no | allow |
| `getDictValues` | SHALLOW | allow | no | allow |
| `getListItem` | LIVE | refuse | no | allow |
| `gt` | DEEP | allow | no | allow |
| `gte` | DEEP | allow | no | allow |
| `ifelse` | LIVE | refuse | no | allow |
| `in` | DEEP | allow | no | allow |
| `index` | DEEP | allow | no | allow |
| `insert` | SHALLOW | allow | no | allow |
| `insertListItem` | SHALLOW | allow | no | allow |
| `isDigit` | DEEP | allow | no | allow |
| `items` | SHALLOW | allow | no | allow |
| `join` | DEEP | allow | no | allow |
| `json_dumps` | DEEP | allow | no | allow |
| `json_loads` | DEEP | allow | no | allow |
| `keys` | SHALLOW | allow | no | allow |
| `len` | DEEP | allow | no | allow |
| `length` | DEEP | allow | no | allow |
| `lower` | DEEP | allow | no | allow |
| `lstrip` | DEEP | allow | no | allow |
| `lt` | DEEP | allow | no | allow |
| `lte` | DEEP | allow | no | allow |
| `mod` | DEEP | allow | no | allow |
| `mul` | SHALLOW | allow | no | allow |
| `ne` | DEEP | allow | no | allow |
| `not` | DEEP | allow | no | allow |
| `notContains` | DEEP | allow | no | allow |
| `notIn` | DEEP | allow | no | allow |
| `now` | DEEP | allow | no | allow |
| `or` | LIVE | refuse | no | allow |
| `pop` | MUTATOR | refuse | no | allow |
| `pow` | DEEP | allow | no | allow |
| `remove` | SHALLOW | allow | no | allow |
| `replace` | DEEP | allow | no | allow |
| `reverse` | SHALLOW | allow | no | allow |
| `round` | DEEP | allow | no | allow |
| `rstrip` | DEEP | allow | no | allow |
| `set` | MUTATOR | refuse | no | allow |
| `setDictValue` | MUTATOR | refuse | no | allow |
| `setListItem` | MUTATOR | refuse | no | allow |
| `slice` | SHALLOW | allow | no | allow |
| `sort` | SHALLOW | allow | no | allow |
| `split` | DEEP | allow | no | allow |
| `sqrt` | DEEP | allow | no | allow |
| `startswith` | DEEP | allow | no | allow |
| `strip` | DEEP | allow | no | allow |
| `sub` | DEEP | allow | no | allow |
| `title` | DEEP | allow | no | allow |
| `today` | DEEP | allow | no | allow |
| `trunc` | DEEP | allow | no | allow |
| `upper` | DEEP | allow | no | allow |
| `values` | SHALLOW | allow | no | allow |

## 4c. producer × position 目标位全表（73 名 × 7 根）

**目标位轴（511 格）**：结果是一个**新**容器且可就地改写目标的 11 名（10 个 SHALLOW 列表
构造器 + `json_loads`（新 dict）+ `split`（新 list））× 7 = 77 allow；LIVE 6 + MUTATOR 9 =
105 refuse；其余 47 × 7 = 329 是类型原生错误（标量 / dict view 没有 `.clear`）
—— **0 次根写入**。

| function | class | target arg over root (7 cells) |
|---|---|---|
| `abs` | DEEP | native |
| `add` | SHALLOW | native |
| `addListItem` | SHALLOW | allow |
| `and` | LIVE | refuse |
| `append` | SHALLOW | allow |
| `capitalize` | DEEP | native |
| `ceil` | DEEP | native |
| `clear` | MUTATOR | refuse |
| `clearDict` | MUTATOR | refuse |
| `concat` | DEEP | native |
| `contains` | DEEP | native |
| `delDictValue` | MUTATOR | refuse |
| `delListItem` | MUTATOR | refuse |
| `delete` | MUTATOR | refuse |
| `div` | DEEP | native |
| `endswith` | DEEP | native |
| `eq` | DEEP | native |
| `extend` | SHALLOW | allow |
| `floor` | DEEP | native |
| `get` | LIVE | refuse |
| `getDictKeys` | SHALLOW | native |
| `getDictValue` | LIVE | refuse |
| `getDictValues` | SHALLOW | native |
| `getListItem` | LIVE | refuse |
| `gt` | DEEP | native |
| `gte` | DEEP | native |
| `ifelse` | LIVE | refuse |
| `in` | DEEP | native |
| `index` | DEEP | native |
| `insert` | SHALLOW | allow |
| `insertListItem` | SHALLOW | allow |
| `isDigit` | DEEP | native |
| `items` | SHALLOW | native |
| `join` | DEEP | native |
| `json_dumps` | DEEP | native |
| `json_loads` | DEEP | allow |
| `keys` | SHALLOW | native |
| `len` | DEEP | native |
| `length` | DEEP | native |
| `lower` | DEEP | native |
| `lstrip` | DEEP | native |
| `lt` | DEEP | native |
| `lte` | DEEP | native |
| `mod` | DEEP | native |
| `mul` | SHALLOW | native |
| `ne` | DEEP | native |
| `not` | DEEP | native |
| `notContains` | DEEP | native |
| `notIn` | DEEP | native |
| `now` | DEEP | native |
| `or` | LIVE | refuse |
| `pop` | MUTATOR | refuse |
| `pow` | DEEP | native |
| `remove` | SHALLOW | allow |
| `replace` | DEEP | native |
| `reverse` | SHALLOW | allow |
| `round` | DEEP | native |
| `rstrip` | DEEP | native |
| `set` | MUTATOR | refuse |
| `setDictValue` | MUTATOR | refuse |
| `setListItem` | MUTATOR | refuse |
| `slice` | SHALLOW | allow |
| `sort` | SHALLOW | allow |
| `split` | DEEP | allow |
| `sqrt` | DEEP | native |
| `startswith` | DEEP | native |
| `strip` | DEEP | native |
| `sub` | DEEP | native |
| `title` | DEEP | native |
| `today` | DEEP | native |
| `trunc` | DEEP | native |
| `upper` | DEEP | native |
| `values` | SHALLOW | native |

## 4b. r6 容器键名轴（容器级实测，27 格 × 零登记/有登记）

轴：键名桶（canonical = `$INPUT`；non-canonical = `$MYROOT`；arbitrary = `$zzz_q9`）
× 容器里那个键挂的**值**（活根本体 / 活根子对象 `$INPUT.cfg` / **引擎物化副本**）
× 嵌套（平铺 / `{k: {"wrap": v}}` / `{k: [v]}`）。

判据是**对象身份 + freshness**：`live-root` 与 `live-root-child` 逐格拒绝；副本只有经
`ctx.evaluate`/producer **由引擎物化**才逐格放行。**r6 的构造修正**：副本的物化与就地
改写现在被同一个 `freshness_scope()` 包住（"同一次逻辑求值"）——否则它就是**跨求值**
对象，default-deny 会独立拒绝。**verifier 的关键补充**：r4 的 9 个 `live-root-child` 格
在"零登记"（不做任何前置求值）下会全部写穿；r5/r6 在**两套状态下判决完全一致**
（生成器 `_axis_rows(zero_registration=…)` 实测）：

| 状态 | 格数 | 判决 | 写穿 |
|---|---|---|---|
| zero-registration（不做任何前置 `$INPUT.cfg` 求值） | 27 | 18 refuse / 9 allow | 0 |
| pre-registered（复刻 r4 的前置求值） | 27 | 18 refuse / 9 allow | 0 |

| 键名桶 | 值种类 | 嵌套 | 容器级判决 |
|---|---|---|---|
| canonical | live-root | flat / dict / list | refuse |
| canonical | live-root-child | flat / dict / list | refuse |
| canonical | engine-materialised copy | flat / dict / list | allow |
| non-canonical | live-root | flat / dict / list | refuse |
| non-canonical | live-root-child | flat / dict / list | refuse |
| non-canonical | engine-materialised copy | flat / dict / list | allow |
| arbitrary | live-root | flat / dict / list | refuse |
| arbitrary | live-root-child | flat / dict / list | refuse |
| arbitrary | engine-materialised copy | flat / dict / list | allow |

**宿主自造副本**（`copy.deepcopy(...)` 直接塞进数据容器）在 r5 起**具名拒绝**
（`provenance could not be proven fresh`）；r4 曾按"物化副本"放行。合法替代 = 引擎物化
（`ctx.evaluate` / `$F.json_loads($F.json_dumps(...))`）。见 §7 边界。

## 5. 表达式文法里**不可表达**的形式（机械验证：无写入）

| 表达式 | 解析结果 | 是否写入 |
|---|---|---|
| `$INPUT += 1` | ParseException | 否 |
| `del $INPUT` | parsed | 否 |
| `$INPUT['cfg'] = 1` | ParseException | 否 |
| `$INPUT.items += [4]` | ParseException | 否 |
| `$INPUT['cfg']` / `$INPUT["cfg"]` | ParseException | 否 |
| `$F.set($INPUT['cfg'],'k','v')` | ParseException | 否 |
| `$F.setattr($INPUT,'cfg',{})` | ParseException | 否 |
| `$F.__setitem__($INPUT,'k',1)` | ParseException | 否 |
| `$F.vars($INPUT)` / `$F.eval('$INPUT')` / `$F.exec('x=1')` | NameError | 否 |

表达式**文法**造不出赋值/别名，但 DSL **本身能**把活根挂到任意键名下（节点字段
`InlineFlow.input = {"$myroot": "$INPUT"}` 里的 `"$INPUT"` 作为字典值求值就是活根对象）。
r4 的边界物化 + 身份探测 / r5 的 freshness 才关闭该通道（§X 的 X1 逐字探针）。

## 6. 作用域维度（r6：fresh ≡ **本次逻辑求值**由引擎新建）

| 作用域 | 实测 |
|---|---|
| `ExecutionContext.evaluate`（所有节点参数共同入口） | 目标位/值参位全格拒绝（§1） |
| 真实 flow `assignment.output` / `end.output` | 包装异常上抛，消息含 `plaita#69` + 具名路径，零写入 |
| **求值作用域的边界**（`ContextVar`） | 开：`ExpressionParser.evaluate` / `ExecutionContext.evaluate` 最外层；关：`finally`（异常路径也关）；嵌套共享；域外 `note_fresh_*` 是 no-op |
| 循环条件（`_IsolatedConditionContext` 隔离副本） | 放行——`Loop.execute` / `While._should_continue` 把"副本物化 + `Condition.match`"开成**一次**逻辑求值；副本由**唯一**拷贝入口 `_engine_copy` 现造（r7）。**前提**：条件根整棵子树都是数据模型值；根里含**任何**非数据模型对象（`bytearray`/不透明宿主对象/自定义 `Mapping`…）⇒ 整根保留活引用且不登记 fresh ⇒ **该根内任何就地写（含兄弟纯数据子树）都具名拒绝**（r8 文档化，选择 (a)；测试 `TestIssue69R8LoopConditionNonDataModelRoot`；逃生口：先物化要改的部分） |
| 循环条件里的 `$LOOP-ITEM` | `_condition_context` 用 `_engine_copy` 拷贝它 ⇒ 就地 mutate 只改副本（同一作用域内）；不可拷贝的根保留活引用且**不登记** ⇒ default-deny 拒绝 |
| 嵌套表达式串在数据容器上求值 | 容器里可达**活**对象（任意键名/任意深度）⇒ 具名拒绝；只有**本次求值**里引擎新建的值 ⇒ 放行 |
| **线程**（`Parallel(mode="thread")` 的 worker） | 分支边界处**没有**活动作用域：`_resolve_branch_input` 在任何求值作用域之外被调用（`scope_active=False`），所以父求值里物化的分支输入在分支里不是 fresh ⇒ 具名拒绝（fail-closed）。r6 文档把原因写成"`ContextVar` 不跨线程"是**错的**：`mode="coroutine"` 跑在 MainThread 里、`scope_active` 同样为 `False`、同样被拒（`test_parallel_modes_have_no_active_scope_at_boundary` 实测两种模式）|
| 子流程边界（`InlineFlow`/`ReferenceFlow`/集合节点/`Parallel` 分支/`Reduce.initial`） | `_snapshot_value` 物化仍保证活根**按引用**进不去；四处 `note_fresh_closure` 是作用域外的死登记，r7 已删除（§S）⇒ 副本在子执行（另一次求值）里不是 fresh，子执行对副本的就地改写**具名拒绝**（0 写穿不变，见 §7/§X） |

## 7. T2 / 边界真话（r7 重写）

| 项 | r7 状态 | 实测 |
|---|---|---|
| 宿主把某上下文键设成持久化根的**同一对象**后写 `$V.cfg` | 已关闭（r4 起） | 按对象身份 ⇒ `refuse`，零变化 |
| 宿主把**活根子对象**塞进非持久化键（`ctx.set_state("$V", ctx.get_state("$INPUT")["deep"])`，求值器从未把它当路径终点交出过） | **已关闭**（r5 起） | freshness 判据不依赖索引/键名 ⇒ `refuse`、零写穿（§X 探针行 `non-persisted-key (T2-1 sub-object)`） |
| 宿主**直接绑定**普通对象（`ctx.set_state("$V", {...})`）后写它 | **拒绝（owner 决策边界）** | 来源不可证 fresh；合法写法是在**同一次求值**里物化：`$F.set($F.json_loads('{}'),…)` / producer 结果 |
| 数据容器里挂**宿主自造副本** | **拒绝**（r5 起，r4 曾放行） | 引擎物化副本（同一次求值里的 `ctx.evaluate`/producer）仍放行 |
| **C2/B3**：引擎在**求值 A** 里物化的对象被写进 `$NODE`/checkpoint 后，**求值 B** 就地写它 | **已关闭（r6）** | 作用域随求值 A 返回而丢弃 ⇒ `refuse`、checkpoint 零变化（`TestIssue69R6PerEvaluationScope`）；r5 实测 `fresh=True` 且写穿 |
| **C1**：载荷里 `__deepcopy__`/`__copy__` 返回 `self` 的宿主 `dict`/`list` 子类经值参去别名 | **已关闭（r6/r7 by construction）** | `_engine_copy` 用**基类协议**重建（`dict.items(sub)`/`list.__iter__(sub)`…）：宿主类被归一化成 plain 容器、`copy` 钩子一次不调用 ⇒ 活对象与 checkpoint 零变化（`TestMutatorGateEngineOwnedCopier`）。r6 对这类子类**具名拒绝**，r7 改为**归一化**（Finding 2 要求 `OrderedDict`/`defaultdict`/`namedtuple`/`IntEnum` 可接受）；真·非数据模型的可变对象仍具名拒绝 |
| **B1**：宿主把活根挂到任意键名的**数据容器**里 | 已关闭（r4 身份 + r5/r6 freshness 双拒） | `refuse`、0 写穿 |
| **B2**：`_condition_context` 隔离副本后条件里的就地 mutate | **放行**（唯一合法 in-place 现场，r6 显式作用域 + r7 引擎自有拷贝保住） | `$GLOBAL`/`$INPUT` 零污染；副本由 `_engine_copy` 现造（r7 前是 `copy.deepcopy` ⇒ verifier Finding 1 反例） |
| **B3**：跨求值的引擎对象 | **已关闭（r6）** | 见上 C2 行；最小复现 `MIGRATION.md` r6 节 |
| 子流程/集合/`Reduce.initial`/`Parallel` 分支的**边界副本** | **拒绝**（r6 收紧；r7 删除死登记） | 副本在父求值里物化，子执行是另一次求值；父运行态与 checkpoint 0 写穿（由拒绝保证） |
| 自定义 registry 把返回 live 引用的函数声明成 `fresh_producer=True` | 仍是信任边界 | 声明是唯一证明；默认注册表 73 名字四桶全分类（新增即红） |
| 自定义 registry 注册**零实参**未声明 producer | fail-closed 拒绝 | `69-undeclared-producer-allowed` 钉住；`69-callee-args-not-recorded` 由 `SUBFAILED(` 杀死（§M） |
| 深拷贝探针共享面（`set` 元素、自定义类实例、`__deepcopy__`/`__copy__` 覆写） | **已关闭 by construction（r7，r8 补上快照路径）** | 数据路径上只剩**一个**拷贝入口 `_engine_copy`，物化边界 `_snapshot_value` 是同一纪律的**第二处引擎自有重建**：两者都只按基类协议读宿主容器、按值归一化标量，**从不**调用任何宿主钩子（`tests/unit/test_issue69_single_copy_point.py` 用 AST 机械守卫"没有第二个 copy 模块调用点"+ 兄弟守卫 `snapshot_host_protocol_calls` 覆盖 `state.py` 快照路径；`TestMutatorGateEngineOwnedCopier` 与 `TestIssue69R8SnapshotIsAnEngineOwnedRebuild` 用钩子计数器证明 0 调用、用 `is` 证明快照不是活对象）。不再依赖"证不出 fresh 就拒"这层运行时兜底来解释该面 |
| 别名探测预算（50000 节点） | **不再拒绝**（r5 起） | 非 fresh 的目标由 default-deny 独立拒；合法大容器（同一次求值里引擎物化）不再被误拒 |
| 目标位热路径身份索引（`_live_alias_index`） | **目标位不再调用**（r6，r8/r9/r10/r11 复核） | 拒绝依据是 freshness；容器值身份探测保留。`$F.set($VAR,…)` 同机：r5 **13.72µs**（当时是 **allow** 路径）→ r6 **5.98µs**（**refuse** 路径，含异常构造）→ r7 **5.85µs**（**历史值**，同口径）→ r8 **5.350µs**（**历史值**）→ r9 **5.368µs**（**历史值**）→ r10 **5.871µs**（**历史值**，负载 ~7.8–8.7）→ **当前 revision 权威 = r11 的 5.252µs**（本机负载 ~7.8–8.3，见 §P，同口径） |

**静态可判定而漏的 = 0**（§1 目标 378/378 + 值参 294/294 + 浅值参 112/112 +
callee 触达 42/42；§3 72/72；§4/§4c 511×2；§4b 54；§X 14；§S 87；§5/§6 全拒或放行如述）。

## X. r7 多位置探针（活子对象；零登记；X1 逐字）

来源：`build_r5_probe_rows()`（`probes.py` 与 `table.py` 共用）。判定：每个位置要么
**具名拒绝**，要么落在**引擎物化的私有副本**上 —— 两者都必须对父活 `$INPUT` 与 checkpoint
**零写穿**。

| position | verdict | live/checkpoint changed |
|---|---|---|
| container-scope | refuse | False |
| container-value-scope | refuse | False |
| container-dict-nested | refuse | False |
| container-list-nested | refuse | False |
| container-tuple-nested | refuse | False |
| non-persisted-key (T2-1 sub-object) | refuse | False |
| non-persisted-key (fresh shell, live value) | refuse | False |
| host-bound non-persisted key (boundary) | refuse | False |
| engine-bound non-persisted key (fresh by construction) | allow/isolated | False |
| X1 verbatim (nested scope = live sub-object) | refuse | False |
| Reduce.initial (live sub-object) | refuse | False |
| loop-item | refuse | False |
| map-item | refuse | False |
| parallel-branch-input (live sub-object) | refuse | False |

合计 **14 位置：13 具名拒绝 + 1 私有副本隔离 / 0 写穿**。
r5 下 `Reduce.initial` / `loop-item` / `map-item` / `parallel-branch-input` 四行是
"私有副本隔离（不报错）"；r6 四行全部改成**具名拒绝**（副本在父求值里物化，子执行是
另一次求值）；r7 把四处作用域外的 `note_fresh_closure` 死登记删掉（语义不变，见 §S），
并用 `TestIssue69R7BoundaryCopySemantics` 显式钉住四行。`parallel-branch-input` 的拒绝被
并行执行器记进 `$NODE.p.b.__parallel_error__`（thread 与 coroutine 两种模式都是该通道），
探针读该通道判定 —— flow 级 `raised` 可能是 `None`。**因果纠正**：拒绝与线程无关，
`mode="coroutine"` 在 MainThread 里同样被拒（边界处 `scope_active=False`，两种模式实测）。
剩下唯一的 `allow/isolated` 行是 `engine-bound non-persisted key`（同一次求值内物化）。
`set`/`frozenset` 元素位置在数据模型里**不可表达**（dict/list 不可哈希，类型原生
TypeError）——由 `test_set_element_position_is_not_expressible` 机械钉住。
X1 与 X2 各有自己的负控：`test_x1_control_canonical_path_is_still_refused`（规范形式一直
被拒）与 `test_r3_baseline_shape_writes_through`（三层机制全关时写穿，证明断言有区分力）。

## S. 作用域/物化面清单（r7 结构要求）

`plaita/` 的 AST 枚举（`scope_inventory.py`），kind 计数：

| kind | 含义 | 计数 |
|---|---|---|
| `explore-scope` | `X.evaluate(value, context[, registry])`：显式把对象当求值容器 | 15 |
| `state-scope` | `X.evaluate(value)`：以执行体自身的 live state 为作用域 | 27 |
| `child-input` | `run_compatible/arun_compatible`（含 `item`/`first`/`second`/`index` 关键字） | 25 |
| `materialise` | `_snapshot_value` / `_materialise_child_input`：边界去别名（r7：`setup_flow` 的 `$GLOBAL` 快照也走这里） | 14 |
| `fresh-register` | `note_fresh_object` / `note_fresh_closure`：freshness 登记点（**只在活动求值作用域内生效**；r7 删掉 4 个作用域外死点） | 6 |
| **合计** | | **87** |

**r7 的清单变化（Finding 3）**：`child._materialise_child_input` /
`loop._eval_collection` / `loop.Reduce._eval_initial` / `concurrent._resolve_branch_input`
四处的 `note_fresh_closure` 在任何求值作用域之外被调用，r6 起就是 **no-op**（死调用点）。
r7 逐个决定：**删除登记**而不是"把整个子执行包进一个作用域"——后者会让同一次子执行里
"求值 A 造出的对象在求值 B 里仍 fresh"（C2 反例）重新成立。物化（隔离）本身保留；
`fresh-register` 10 → **6**（唯一保留的是 `loop._condition_context`，它在
`Loop.execute`/`While._should_continue` 的显式作用域内），总数 90 → **87**（`materialise` 13 → 14）。
`tests/unit/test_issue69_scope_inventory.py::test_four_dead_boundary_registrations_are_gone`
钉住删除，且断言物化点本身仍在。

**r6 修的两个 verifier 缺陷 + 一个新发现的第三个**（保留）：

* 键里**既不含行号也不含列号**（旧键含 `col_offset` ⇒ 只改缩进就 missing+stale 假红；
  守卫新增 `test_indentation_only_edit_is_green` / `test_forty_blank_lines_above_green`）；
* `getattr(state, "evaluate")(v)` 这类**间接分发**归一到 `getattr.evaluate` 后分类
  （旧扫描完全看不见它）；**已知局限**：属性名动态计算（`getattr(state, name)(v)`）
  静态扫不到，由负控 `test_dynamic_attribute_name_is_a_documented_blind_spot` 点名；
* **同缩进同形状的调用点用遍历序 `#k` 区分**（旧 col 键在同函数内两个同缩进同形状的
  点会**相同**、后一个静默覆盖前一个）：`context.py` 的 list/dict 两个
  `note_fresh_object`、`concurrent.py` 的两个 `_snapshot_value` 与两个
  `execution.evaluate` 此前被吞掉 ⇒ 清单 87 → **90**；r7 删掉 4 个死登记、新增 1 个 `$GLOBAL` 物化点 ⇒ **87**。

X1 的 `expression_parser.py::ExpressionParser._compile_variable.thunk::parser.evaluate`
与 X2 的 `loop.py::Reduce._eval_initial::{execution.evaluate,_snapshot_value}` 都在清单里
被点名（守卫测试 `test_shape_matcher_catches_the_known_x1_x2_sites`）；r7 起循环条件隔离
副本是唯一保留的 fresh-register 点（`loop.py::_condition_context::note_fresh_closure`）。
守卫：`tests/unit/test_issue69_scope_inventory.py`（新点未登记 ⇒ 红；计数钉住 **87**）。

## P. 性能（µs 级；base / r4 / r5 / r6 / r7 / r8 / r9 / r10 / r11）

`scripts/issue69_mutation_matrix/bench_expr.py`，30000 次/轮 × 7 轮取最小值。前三列是
verifier 机上的历史测量（**跨机不可逐字比**）；r6 起的列在**本机**测得（各列在该 revision
的改动后同一条命令跑出；负载随列标注 —— 绝对 µs 只做**同机同轮**对比，跨列/跨负载不可
逐字比）。**只有当前 revision（r11）那一个数可以称"权威读数"**，其余全部是历史值。
**r12 不动被测热路径**（改动只在 `dict` 子类谓词与视图闸门上，不在 `$F.set($VAR,'k','v')`
的 default-deny 路径上），因此**未重测、不产生新权威**：下表不新增 r12 列，§P 的单一权威
仍是 r11 的 `5.252µs` @ 本机负载 ~7.8–8.3（验收者自测同一 revision 为 `6.405µs` @ 负载
10.19，与负载方向一致 —— 绝对 µs 跨机、跨负载都不可比）。

| expression | base b46f78e | r4 088ba9d | r5（verifier 机） | r5（本机） | r6（本机） | r7（本机） | r8（本机负载 ~6.0） | r9（本机负载 ~7.0，历史列） | r10（本机负载 ~7.8–8.7，历史列） | r11（本机负载 ~7.8–8.3，**权威**） |
|---|---|---|---|---|---|---|---|---|---|---|
| `$INPUT.a.b.c` | 2.171 | 3.543 | 3.616 | 3.32 | 3.99 | 4.25 | 3.656 | 3.876 | 4.149 | 3.716 |
| `$F.add($INPUT.a.b.c,1)` | 2.573 | 4.121 | 4.396 | 3.96 | 4.80 | 5.06 | 4.347 | 4.646 | 4.818 | 4.372 |
| `$F.eq($INPUT.a.b.c,1)` | 2.520 | 4.112 | 4.283 | — | 4.69 | 4.86 | 4.398 | 4.702 | 5.033 | 4.355 |
| `$F.get($INPUT.a.b,'b')` | 2.335 | 4.033 | 4.087 | — | 4.44 | 4.73 | 4.120 | 4.335 | 4.644 | 4.138 |
| `$F.json_loads($INPUT.raw)` | 2.659 | 4.315 | 5.552 | — | 5.33 | 5.56 | 5.019 | 5.017 | 5.409 | 4.904 |
| `$F.set($F.json_loads('{"q":0}'),'q',1)`（分类 allow，无 plan） | — | — | 3.392 | 3.39 | 3.56 | 3.53 | 3.349 | 3.507 | 3.716 | 3.313 |
| `$F.set($PROD.v,'k','v')`（plan allow，同一次求值） | — | — | — | — | 7.90 | 8.38 | 7.389 | 7.484 | 8.233 | 7.328 |
| `$F.set($VAR,'k','v')`（plan default-deny） | — | — | ~23.5（verifier 测） | 13.72（当时是 **allow** 路径） | 5.98（**refuse** 路径，含异常构造） | 5.85（同口径，**refuse**，历史值） | 5.350（同口径，**refuse**，历史值） | 5.368（同口径，**refuse**，历史值） | 5.871（同口径，**refuse**，历史值） | **5.252**（同口径，**refuse**，**权威**） |

**`$F.set($VAR,…)` 的读数（每个数都标 revision；只有 r11 那个是权威）**：verifier 机
~23.5µs（r5，跨机不可比）→ **历史**：本机 13.72µs（r5，当时是 **allow** 路径）→ 本机
5.98µs（r6，**refuse** 路径，含具名 `ValueError` 构造）→ 本机 5.85µs（r7）→ 本机 5.350µs
（r8 复跑，负载 ~6.0）→ 本机 5.368µs（r9 复跑，负载 ~7.0）→ 本机 5.871µs（r10 复跑，
负载 ~7.8–8.7）→ **权威：本机 5.252µs（r11 复跑，负载 ~7.8–8.3，1 分钟均值 8.12/8.34/7.84
三次运行取各表达式的 min；同窗口 5 分钟负载 ~8.5–8.8）**。**同一 revision 早一轮读数
5.647µs @ 负载 ~7.3–8.1（1 分钟均值 7.29/7.58/8.12）** —— 两次相差 7%，与负载同向；这就是
"绝对 µs 不跨负载比"的直接证据（同一代码、同一命令、同一评测口径）。r11 那条就是 `bench_expr.py`
30000×7 取 min、默认注册表、`$VAR` 是上一次求值物化的对象 ⇒ default-deny refuse 路径，
**出处**就是 §8 那条命令的输出；`MIGRATION.md` 引用同一个口径/同一列。

**r10 的 5.871µs 与 r11 的 5.252µs 不可当"r10 有成本"**：验收者在自己的机器上实测 r10 列
比 r10 文档里的数字**低 3–6%**（同负载 8.73→8.07 下 `$F.set($VAR,'k','v')` = **5.375µs**，
落在 r8 的 5.350 与 r9 的 5.368 之间 ⇒ **没有可测的 r10 成本**）。r10/r11 的改动都只在
**标量读取器**（`datetime`/`bytes`）、**子类 dict 的判据**与 **`isinstance` 具名化**上，
不在被测热路径（路径表达式 / 目标位门）的分子分母上；比值预算（`perf_baseline.json`）
实测未变（§P 守卫仍全绿）。**绝对 µs 不可跨机/跨负载比较**：跨机比、跨负载比都是错的
（verifier 机 ~23.5µs vs 本机 5.9µs 是同一代码）。不做"整表按同一比例抬升"的推断 ——
r11 本轮实测的列间差（例如 r10 4.149 → r11 3.716 的 `$INPUT.a.b.c`，两次 r11 读数
3.917 / 3.716）与负载方向**不一致**，足以说明这类全局归因不可证。

**权威的定义（防再次自相矛盾）**：本文档与 `MIGRATION.md` 里"权威读数"**只能**指**当前
revision** 那一列（现在是 r11 的 **5.252µs**）；任何更早的数字（含 r10 的 5.871µs、r9 的
5.368µs、r8 的 5.350µs、r7 的 5.85µs）**必须**标 revision 并写成"历史值"，**不得**称权威。

**r6 的改动与成本**：目标位不再走 `_path_alias_state`（它会对活 state 的非持久化键调用
`_live_alias_index` 构造身份索引）。同机 profile（每次调用 µs）：

| 子调用 | r5 | r6 |
|---|---|---|
| `_path_alias_state("$VAR", (), live_state, "$")` | 10.13 | 目标位不再调用 |
| `_is_persisted_key_strict(live_state, "$VAR")` | — | 0.73 |
| `_probe_container_alias(live_state)`（活 state 提前返回） | 0.07 | 0.07 |
| `_fresh_has`（含进程级锁 / 作用域查身份） | 0.11 | 0.04（域外）/ 0.05（域内） |
| `_live_alias_index()`（目标位省掉的那次） | 7.89 | — |

⇒ `$F.set($VAR,…)` 的目标位门成本从 ~10.1µs 降到 ~0.8µs。verifier 报的 23.5µs 路径
（跨求值 `$VAR`：`_live_alias_index` + `_path_alias_state` + plan）同因下降；r6 该式在
本机 5.98µs，其中相当一部分是**具名 ValueError 的构造**（default-deny 的常规结局）。

**不可再降的部分（明说，r7 修正归因）**：路径表达式相对 base 的 +57~84% **没有拿回来**。
r6 曾把原因写成"`_register_live_objects`（r4 半边 2 的身份登记）占路径开销的**大头**"——
**这个归因是错的**。同机实测（`$INPUT.a.b.c`，`_us_per_call` 20000×5 min）：

| 处置 | 相对基线 | verifier 读数 | r7 复测（本机负载 ~7） |
|---|---|---|---|
| `_register_live_objects` 改 no-op | 只省注册本身 | **−6.3%** | −4.3% |
| `_is_persisted_key` 强制返回 `False`（`live_walk=False`：跳过注册**与**增量路径拼装） | 省整条 live-walk 记账 | **−29.4%** | −32.3% |

⇒ 花掉的是 `_compile_variable.thunk` 里**整条 live-walk 记账**（`_is_persisted_key` 判定 +
逐段路径拼装 + 登记），其中登记本身只占一小部分（r6 曾把它写成"大头"，**归因已更正**）。
这套记账**不能删**：**值参位没有 freshness 判定兜底**（freshness 只管目标位求值后的对象），
容器值身份探测（`_probe_container_alias`，靠登记出来的 `_live_alias_index` 识别**活子对象**
——活根本体另可由 state 字段扫描独立找到）是值参位/容器作用域识别并**点名**活子对象的唯一
依据（§3 的 42/42 与 `$V.cfg` 类形状依赖它）。证据：把 `_register_live_objects` 改 no-op 后，
`TestIssue69R4ContainerKeyAxis::test_key_name_cannot_decide_and_copies_stay_allowed` 与
`TestIssue69R4ChildFlowBoundaryAxis::test_half2_alone_classifies_by_identity_not_by_key_name`
立刻变红（拒绝消息不再点名活子对象与根，退化为 freshness 判据）；verifier 在同类形状上实测
宿主登记的值参 mutator 会**写穿**（本机 r7 复跑这两类的全集只观察到"点名退化"，写穿未复现——
按"失败方向选安全侧"保留该机制）。DEEP producer 的闭包登记（`json_loads` 同机 5.56µs）
属于"证明 fresh"的必要成本。

**守卫（r6 扩到 mutator 路径 + 常量因子）**：`tests/unit/test_expression_provenance_perf.py`

* 判定的不是绝对 µs 而是**可判定性质**：单次求值成本不得随上下文规模增长（4000 键 vs
  3 键）；r6 起覆盖 `$F.set` 的三条分支（分类 allow / plan allow / plan default-deny）。
  负控（人为在 `_is_persisted_key_strict` 里加一次全量遍历）必须让性质不成立。
* **常量因子预算**（r6 新增）：把每条 mutator 表达式的 µs 除以同一次运行里
  `$INPUT.a.b.c` 的 µs 得到**比值**，与 checked-in 基线
  `scripts/issue69_mutation_matrix/perf_baseline.json`（`ratios` × `tolerance=2.0`）比；
  比值与机器绝对速度无关。负控（人为把目标位热路径放慢 40µs）实测比值越过 ceiling，
  守卫必红。
* 粗粒度的 `tests/unit/test_performance_benchmark.py`（阈值 `avg_time<0.1s`）抓不到这一量级。

## M. 变异（51/51 杀死）

`scripts/mutation_replay/mutations/69-*.json`（`replay.py` 从 `fix/68` 取，跑完即删）。
r6 改了 freshness 的实现与目标位热路径，**6 条变异受影响**（位点文本/expect_fail 随实现
更新，语义不变；全部实测 KILLED）：

| 变异 | r6 调整 | 杀死它的断言（摘要） |
|---|---|---|
| `69-freshness-default-allow` | 重锚到新 `_fresh_has`（作用域查询） | `TestIssue69R6PerEvaluationScope::test_object_created_in_one_evaluation_is_refused_in_the_next` 等 |
| `69-freshness-registration-off` | 重锚到新 `_fresh_note` | `TestIssue69R5Provenance::test_engine_bound_values_are_allowed_by_construction` |
| `69-freshness-path-plan-off` | 重锚到目标位 `_FreshnessPlan` 那一行 | `TestIssue69R5Provenance::test_x1_verbatim_is_refused_and_writes_nothing` |
| `69-alias-roots-uncovered` | expect_fail 增补值参轴 | `test_generated_value_argument_cells_all_refuse`（294 格；值参位没有 freshness 兜底，`_path_alias_state` 仍是唯一依据） |
| `69-root-path-check-off` | expect_fail 增补值参轴 | 同上（目标位已改走 `_is_persisted_key_strict`，只靠目标位测试不再有区分力） |
| `69-r4-boundary-materialisation-off` | **r7 重锚**：`child.py` 的 `note_fresh_closure` 死登记已删，锚点改到物化本体 `return _snapshot_value(value)` | `TestIssue69R4ChildFlowBoundaryAxis::test_child_input_boundary_never_aliases_the_parent_object`（行为层零写穿已由 freshness 独立保证，必须直接钉去别名） |

**r7 新增两条变异**（Finding 1 的反例面）：

| 变异 | 位点 | 杀死它的断言 |
|---|---|---|
| `69-r7-condition-copy-deepcopy` | `loop._condition_context`：`_engine_copy(value)` → `copy.deepcopy(value)` | `TestIssue69R7LoopConditionCopy::test_evil_dict_in_the_condition_root_detaches_with_zero_write_through`（verifier Finding 1 逐字反例）；`test_evil_list_...`；白盒 `test_condition_isolation_copy_is_a_plain_engine_owned_object` |
| `69-r7-freshness-walkable-isinstance` | `_is_walkable_container`：`type(value) in _FRESH_WALKABLE` → `isinstance(value, _FRESH_WALKABLE)` | `TestMutatorGateEngineOwnedCopier::test_copier_and_registrar_share_one_exact_type_predicate`（含"闭包里挂 dict 子类后仍不能作为就地改写目标"的行为断言） |

`69-callee-args-not-recorded` 的位点（`thunk._plaita_args = tuple(arg_thunks)`）**未随 r6/r7
移动**，保持原锚（要求 6：只在本轮 diff 移动了锚点时才改）。

**两条 `SUBFAILED(`-only 变异（重放器不解析 `SUBFAILED(`，killer 列为空，但确实 KILLED）**：

* `69-fresh-producer-default-true`（`fresh_producer: bool = True`）——由
  `TestMutatorGateUndeclaredProducers::test_custom_registry_producer_requires_declaration`
  的 `subTest` 杀死；
* `69-callee-args-not-recorded`（F2 的实参溯源关掉）——由
  `TestMutatorGateCalleeArgumentReach::test_non_root_callee_arguments_are_allowed` 与
  `TestIssue69GeneratedMatrix::test_generated_non_root_callee_cells_are_allowed` 的
  `subTest` 杀死（verifier 在 r5 的复核里发现这一条此前**未在文档中出现**，r6 补上）。

r8 复跑把这两条再次用 `-p no:subtests` 单独确认：`69-fresh-producer-default-true` rc=1
（5 条 `FAILED`，含 `test_live_reference_functions_are_refused`）、
`69-callee-args-not-recorded` rc=1（3 条 `FAILED`，含
`test_non_root_callee_arguments_are_allowed`）——即二者的死亡都不依赖 subtest 记账。

**r8 新增三条变异**（G1 的第二条拷贝路径 + G3）：

| 变异 | 位点 | 杀死它的断言（正常 `FAILED` 行） |
|---|---|---|
| `69-r8-snapshot-set-copy-method` | `state._snapshot_set`：`{item for item in set.__iter__(value)}` → `value.copy()` | `TestIssue69R8SnapshotIsAnEngineOwnedRebuild::test_snapshot_value_rebuilds_hooked_subclasses_as_plain_containers`（快照类型变成 `HookSet`、`is` 活对象、钩子计数 ≠ 0）；另有 `test_global_context_path` / `test_checkpoint_paths` 与 AST 兄弟守卫 `test_snapshot_path_uses_only_base_class_protocols` |
| `69-r8-snapshot-host-items` | `state._snapshot_items`：`iter(dict.items(value))` → `iter(value.items())` | 同上（宿主 `items()` 钩子被真实调用）；AST 兄弟守卫同时命中 |
| `69-r8-engine-copy-spoofed-bare-typeerror` | `expression_parser._engine_copy_base_read`：`except TypeError` → `except AttributeError` | `TestIssue69R8SpoofedClassIsNamed::test_detach_alias_maps_the_spoofed_class_to_the_named_dsl_error`（DSL 面重新漏裸 `TypeError`）；`test_engine_copy_refuses_every_spoofed_container_kind_by_name` |

**r9 新增六条变异**（Finding A ×2 / B / C ×2 / D，全部死在正常 `FAILED` 行）：

| 变异 | 位点 | 杀死它的断言（正常 `FAILED` 行） |
|---|---|---|
| `69-r9-scalar-str-host-hooks` | `_scalar_base_str`：`str.__str__(value)` → `str(value)` | `TestIssue69R9ScalarNormalisationUsesBaseProtocols::test_host_repr_secret_never_becomes_data`（宿主 `__str__` 的秘密被静默写成数据）；DSL 面 `test_dsl_value_argument_detach_never_writes_the_host_repr` |
| `69-r9-scalar-bool-bare-runtimeerror` | `_SCALAR_NORMALISERS`：`bool: _scalar_base_bool` → `bool: bool` | `...::test_spoofed_scalars_are_named_refused_not_bare_runtimeerror`（伪造 bool 触发宿主 `__len__`，裸 `RuntimeError`）；DSL 面 `test_spoofed_bool_and_bytes_are_named_at_the_dsl_boundary` |
| `69-r9-hashable-member-bare-typeerror` | `_engine_copy_members`：`_check_hashable_key(...)` → `hash(copied)` | `TestIssue69R9HashableMemberIsNamedRefused::test_engine_copy_names_the_set_member_and_the_dict_key`（裸 `TypeError: unhashable type`） |
| `69-r9-snapshot-member-shared` | `_snapshot_set`：成员归一化 → `{item for item in source}` | `TestIssue69R9SnapshotKeyAndMemberAliases::test_spoofed_class_as_member_or_key_is_named_refused`（伪造成员按引用进快照）；`test_host_set_subclass_members_are_not_shared` |
| `69-r9-snapshot-key-shared` | `_snapshot_container`：`key = _snapshot_key_or_member(key, "dict key")` → `key = key` | 同上（键那一半）；`test_host_str_subclass_key_is_normalised_not_shared` |
| `69-r9-mapping-outside-dict-storage` | `_snapshot_container`：`if reads_mapping_outside_dict_storage(root):` → `if False:` | `TestIssue69R9MappingOutsideDictStorage::test_snapshot_refuses_the_shape_at_root_and_nested`（静默 `{}`：r7 的 `{'m': {'a': 1}}` 丢数据） |

**r9 重锚两条 r8 变异**（实现位点文本变了，语义不变；`old` 仍**恰好出现 1 次**）：

| 变异 | 重锚 | 原因 |
|---|---|---|
| `69-r8-snapshot-set-copy-method` | `old` 改为 `_snapshot_set` 里逐成员归一化那一行，`new` 仍是 `value.copy()` | r9 Finding C 改写了该行；语义仍是"set 快照改回宿主 `copy()` ⇒ 快照就是活对象" |
| `69-r8-engine-copy-spoofed-bare-typeerror` | `old` 收窄到 `_engine_copy_base_read` 的 `try: return reader(value) / except TypeError ...` 四行上下文 | r9 Finding B 新增的具名拒绝让裸的 `except TypeError ... raise _EngineCopyRefused(` 在本文件出现**两次**；收窄后仍唯一 |

全部 **49** 条由 `replay.py` 实测 **KILLED**（r12，`--all --source head`：
`合计: 49  杀死: 49  存活: 0`；每条变异的两侧基线都先验证全绿，`old` 锚点文本在目标文件里
**恰好出现 1 次**，否则重放器直接判失败）；r11 的复跑读数是 46/46、r10 是 39/39、r9 是 34/34。
逐条 killer 见上表、下节与 §8 的日志。

**r10 新增五条变异**（回归修复轮：fold 丢失 + 字段丢失类 + PEP 688 buffer 洞 + D1 镜像
形状；全部死在正常 `FAILED` 行）：

| 变异 | 位点 | 杀死它的断言（正常 `FAILED` 行） |
|---|---|---|
| `69-r10-datetime-reduce-drops-fold` | `_scalar_base_datetime`：r10 的两行（`__reduce__` 重建 + 基类 `fold` 写回）→ 退回 r9 的 `datetime(*datetime.__reduce__(value)[1])` | `TestIssue69R10FoldIsValuePreserving::test_dst_ambiguous_wall_time_keeps_fold_timestamp_and_offset`（fold 1→0、`timestamp()` 1604212200.0→1604208600.0）、`test_fold_is_carried_for_naive_and_fixed_offset_forms_too`（naive/`timezone` 形态也丢）、`test_dsl_level_fold_survives_the_detach_copy`、`test_snapshot_path_keeps_fold_for_members_and_keys`；以及**强化的 A 判据** `TestIssue69R9ScalarNormalisationUsesBaseProtocols::test_every_scalar_subclass_normalises_to_the_exact_base_with_zero_hooks`（新增的 fold 样本） |
| `69-r10-datetime-microsecond-dropped` | `_scalar_base_datetime`：`datetime.replace(built, fold=…)` → `datetime.replace(built, microsecond=0, fold=…)` | `TestIssue69R10ScalarFieldPreservation::test_datetime_all_fields_including_fold_and_tzinfo_identity`（**逐字段**判据抓 7 分量之一被清零）与强化后的 A 判据 |
| `69-r10-decimal-exponent-dropped` | `_scalar_base_decimal`：`Decimal(Decimal.__str__(value))` → `Decimal(Decimal.__str__(value).split("E")[0])` | `TestIssue69R10ScalarFieldPreservation::test_decimal_sign_coefficient_exponent_and_str`（`as_tuple` 的 exponent 变了）与强化后的 A 判据（`1.5E+3`） |
| `69-r10-bytes-memoryview-buffer-hook` | `_scalar_base_bytes`：`bytes.__getitem__(value, slice(None))` → `bytes(memoryview(value))` | `TestIssue69R10ScalarFieldPreservation::test_bytes_reader_does_not_depend_on_memoryview`（**版本无关**：把 `builtins.memoryview` 换成会炸的替身）；3.12+ 上另有 `test_bytes_reader_is_immune_to_the_pep688_buffer_hook`（宿主 `__buffer__` 返回 `b'SECRET'`） |
| `69-r10-mapping-storage-mirror-shape` | `reads_mapping_outside_dict_storage`：三条析取的尾部（`dict_storage_protocol_overrides` + `instance_carries_own_state`）→ `return False`（退回 r9 的 MRO-only） | `TestIssue69R10MappingStorageProtocol::test_mirror_shapes_are_named_refused_on_both_copy_paths`（`ItemsLiar`/`DictFirstMapping`/`ItemsOnlyLiar` 静默 `{}`）、`test_dsl_loop_condition_refusal_is_named_and_writes_nothing`（DSL 重新"报成功"）、`test_instance_state_costs_an_over_refusal_and_that_is_declared`、`test_only_the_identity_pair_or_instance_state_triggers_the_extension`；`test_mutable_mapping_before_dict_shape_is_still_refused` 在变异下**仍绿**（证明变异只摘掉扩展、没动 r9 原判据） |

**r11 新增七条变异**（性质判据 + 元类属性协议 + `isinstance` 具名化 + 视图收窄；全部死在正常 `FAILED` 行）：

| 变异 | 位点 | 杀死它的断言（正常 `FAILED` 行） |
|---|---|---|
| `69-r11-instance-dict-proxy-only` | `instance_state_signal`：四条析取 → 退回 r10 的 `__dict__` 代理（只读实例字典） | `TestIssue69R11InstanceStateIsTheProperty::test_slots_items_liar_is_named_refused_on_both_copy_paths`（`SlotsItemsLiar` 静默 `{}`）、`test_dsl_loop_condition_refusal_is_named_and_writes_nothing`（DSL 重新"报成功"）、`test_control_shapes_still_normalise` |
| `69-r11-shadowed-dict-trusted` | `instance_state_signal`：删掉遮蔽判据（`if shadowed: return "shadowed __dict__"`） | `...::test_hiding_dict_is_named_refused_on_both_copy_paths`、`test_every_silent_loss_shape_is_refused_on_both_paths` |
| `69-r11-slots-placeholders-are-state` | `_normalised_slot_names`：`name not in _SLOT_PLACEHOLDERS` 过滤去掉 | `...::test_white_box_signal_is_mechanically_discriminating`（`__slots__ = ("__weakref__",)` 等被误判非空）、`test_control_shapes_still_normalise` |
| `69-r11-getattribute-bare-runtimeerror` | `_canonical_persistable_kind`：`isinstance` 循环的具名 `except` 摘掉 | `TestIssue69R11RaisingGetattributeIsNamed::test_engine_copy_names_the_raising_getattribute`（裸 `RuntimeError`）、`test_canonical_kind_names_the_raising_getattribute`、`test_dsl_loop_condition_refusal_is_named_and_writes_nothing` |
| `69-r11-view-materialisation-trusts-host` | `_dict_view_is_materialisable`：C 槽判定 → `return True` | `TestIssue69R11ViewMaterialisationIsNarrowed::test_host_view_subclass_is_named_refused_with_zero_hooks`（宿主的 `__iter__`/`__len__` 被调用、`['LIE']` 被物化）、`test_abc_views_are_refused_even_without_an_override`、`test_dsl_value_argument_from_a_host_view_is_named_refused` |
| `69-r11-snapshot-probe-bare-runtimeerror` | `state._snapshot_isinstance`：具名 `except` 摘掉 | `TestIssue69R11RaisingGetattributeIsNamed::test_nested_and_snapshot_paths_name_it_too`（快照路径漏裸 `RuntimeError`） |
| `69-r11-metaclass-attribute-protocol-trusted` | `_real_mro` / `_class_namespace`：C 描述符读法 → `cls.__mro__` / `vars(klass)` | `TestIssue69R11InstanceStateIsTheProperty::test_lying_metaclass_cannot_hide_the_slots_base`（说谎元类把 `__slots__` 基类抹掉 ⇒ 静默 `{}`）、`test_raising_metaclass_does_not_leak_a_bare_runtimeerror`（裸 `RuntimeError`） |

**r11 重锚一条 r10 变异**（实现位点文本变了，语义不变；`old` 仍**恰好出现 1 次**）：

| 变异 | 重锚 | 原因 |
|---|---|---|
| `69-r10-mapping-storage-mirror-shape` | `old` 的末行从 `return instance_carries_own_state(value)` 改成 `return instance_state_signal(value) is not None` | r11 把 clause (iii) 换成性质判据改写了该行；语义仍是「退回 r9 的 MRO-only 判据」（`new` 仍是 `return False`）。**若不同步重锚，重放器会以「定义原文出现 0 次」判该变异失败** —— r11 首轮 `--all --source head` 正是这样暴露的（45/46）。 |

**r12 新增三条变异**（clause (iii) 第 5 条的两侧 + 视图物化的真实布局闸门；全部死在正常
`FAILED` 行）：

| 变异 | 位点 | 杀死它的断言（正常 `FAILED` 行） |
|---|---|---|
| `69-r12-c-storage-descriptor-trusted` | `declared_c_storage_descriptors`：整个 C 描述符扫描 → `return ()`（退回 r11 的四条判据） | `TestIssue69R12CStorageDescriptorIsTheProperty::test_c_member_descriptor_liar_is_named_refused_on_both_copy_paths`（`N_DDLiar` 静默 `{}`：`dict.items(v)=={}` 而 `v.items()=={'a':1}`）、`test_c_member_descriptor_getitem_liar_is_named_refused_on_both_copy_paths`、`test_dsl_loop_condition_refusal_is_named_and_writes_nothing`（DSL 重新 `raised=None` 报成功）、`test_benign_defaultdict_subclass_is_the_declared_over_refusal` |
| `69-r12-c-storage-cls-layer-not-exempt` | `declared_c_storage_descriptors`：去掉「声明层是 `cls` 自己就跳过」（`if klass is cls: continue`） | `...::test_the_declaring_layer_being_cls_itself_is_exempt`（**非 subTest** 判决：`declared_c_storage_descriptors(defaultdict) != ()`、精确 `defaultdict(int,{'a':1})` 被误拒）、`...::test_white_box_predicate_is_mechanically_discriminating`、`...::test_negative_controls_still_normalise` —— 证明第 5 条只在**非 `cls` 层**命中，负控契约未被推翻 |
| `69-r12-view-materialisation-abc-registration-trusted` | `_dict_view_is_materialisable`：去掉精确类型闸门（`if cls not in _ENGINE_VIEW_TYPES: return False`）→ 退回 r11 的"解析到 C 描述符类型即可" | `TestIssue69R12ViewMaterialisationNeedsTheRealLayout::test_abc_registered_non_view_type_is_named_refused`（`KeysView.register(LieList)` 被物化成 `['LIE']`）、`test_abc_registered_bytes_subclass_cannot_change_the_value_kind`（`b'LIE'` → `[76, 73, 69]`）、`test_resolved_c_descriptor_from_another_layout_is_named_refused`（`V1` 漏**裸** `TypeError`）、`test_only_exact_engine_view_types_are_materialisable` |

**r12 重锚两条 r11 变异**（实现位点文本变了，语义不变；`old` 仍**恰好出现 1 次**）：

| 变异 | 重锚 | 原因 |
|---|---|---|
| `69-r11-instance-dict-proxy-only` | `old` 扩到 clause (iii) 的**整个函数体**（新增第 5 条的 C 描述符块也含在内），`new` 仍是 r10 的 `__dict__` 代理 | r12 在 `instance_state_signal` 里插入了第 5 条，r11 的 `old` 文本变为 0 次；语义仍是「退回 r10 的代理判据」。**若不同步重锚，重放器会以「定义原文出现 0 次」判该变异失败**。 |
| `69-r11-view-materialisation-trusts-host` | `old` 扩到 `_dict_view_is_materialisable` 的**整个函数体**（精确类型闸门 + 两行 C 槽判定），`new` 仍是 `return True` | r12 把精确类型闸门放进同一函数体：只改那两行判定时，`return True` 在变异后**仍保留闸门** ⇒ 该变异**存活**（r12 首轮 `--all --source head` 正是这样暴露的：48/49）。扩到整个函数体后语义恢复为「任何自称视图的对象都 `list(value)`」。 |

**r12 补齐 6 个 r11 变异文件缺的 `"issue"` 键**（`69-r11-getattribute-bare-runtimeerror` /
`-instance-dict-proxy-only` / `-shadowed-dict-trusted` / `-slots-placeholders-are-state` /
`-snapshot-probe-bare-runtimeerror` / `-view-materialisation-trusts-host`）：此前
`replay.py --list` 读 `s['issue']` 直接 `KeyError`。新增回归
`TestIssue69R12MutationJsonContract`（8 个键 + `old` 恰好 1 次，两条都是**聚合判决** ⇒
缺键/锚点陈旧以普通 `FAILED` 行变红）让它不可能复现。

**r13 新增两条变异**（clause (iii) 第 5 条改成**布局**判据 + `$F.keys/values/items`
读真实存储；全部死在正常 `FAILED` 行）：

| 变异 | 位点 | 杀死它的断言（正常 `FAILED` 行） |
|---|---|---|
| `69-r13-hidden-slots-layout-rule-off` | `declared_c_storage_descriptors`：`if klass is cls and not python_defined:` → `if klass is cls:`（退回 r12 的"声明层是 `cls` 就跳过"） | `TestIssue69R13HiddenSlotsLayoutRule::test_every_hiding_form_is_named_refused_on_both_copy_paths`（`HiddenSlotsLiar` 三个隐藏形态静默 `{}`）、`test_every_hiding_form_refuses_the_dsl_loop_condition_with_zero_hooks`（DSL 重新 `raised=None` 报成功）、`test_the_defining_layer_is_caught_while_subclasses_stay_refused`、`test_white_box_layout_rule_is_mechanically_discriminating`、`test_the_heap_flag_reader_resists_a_lying_metaclass`。r12 的 `test_the_declaring_layer_being_cls_itself_is_exempt` 因精确扩展类型**仍**豁免而**不**变红 ⇒ killer 必须是本轮的布局判决 |
| `69-r13-mapping-view-trusted-host` | `_compile_function_call` 的拦截块 → `result = func(*args)`（退回 r12 的"直接问宿主"） | `TestIssue69R13MappingViewReadsRealStorage::test_lying_keys_items_values_yield_the_real_storage`（`$VAR == {'k': ['LIE']}`、宿主钩子被调用）、`test_the_alias_helpers_read_the_same_real_storage`；外加两条改成「通道关闭」的 DSL 回归（r11 `test_dsl_keys_never_calls_the_host_view_factory`、r12 `test_dsl_keys_never_materialises_a_registered_abc_view`） |

**r13 重锚三条变异**（实现位点文本变了，语义不变；`old` 仍**恰好出现 1 次**）：

| 变异 | 重锚 | 原因 |
|---|---|---|
| `69-r12-c-storage-cls-layer-not-exempt` | `old`/`new` 从"删掉 `if klass is cls: continue`"改成 `if klass is cls and not python_defined:` → `if False:` | r13 把豁免条件改成布局问句（`klass is cls and not _is_python_defined_class(cls)`，heap ∧ ¬immutable），旧 `old` 文本 0 次；语义仍是"去掉整条豁免 ⇒ 精确 `defaultdict`/`OrderedDict` 被误拒"。**若不同步重锚，重放器会以「定义原文出现 0 次」判该变异失败。** |
| `69-r12-c-storage-descriptor-trusted` | `old` 换成新函数体（`python_defined = _is_python_defined_class(cls)` + `if klass is cls and not python_defined:` + `found.append(...)`），`new` 仍是 `return ()` | 同上；语义仍是"on-object 描述符一条都不看 ⇒ 退回 r11 四条判据"。 |
| `69-r11-view-materialisation-trusts-host` | `expect_fail` 移除 `test_dsl_value_argument_from_a_host_view_is_named_refused`（该用例改写成 `test_dsl_keys_never_calls_the_host_view_factory`，变异下**仍通过**） | r13 让 `$F.keys` 只走基类 C 描述符 ⇒ 宿主的 `keys()` 零调用、假视图到不了值参，那条 DSL 断言不再有区分力；其余三条 `_detach_alias` / 精确类型判决照旧杀死本变异。 |

**r13 性能口径**：改动只在 `dict` 子类的谓词（`declared_c_storage_descriptors` 多读一次
`tp_flags`）与映射辅助 `$F.keys/values/items`（参数是普通 `dict` 时多一次基类描述符调用），
**不在** §P 被测的 `$F.set($VAR,'k','v')` 热路径上；本轮**未重测**、**不产生**新权威读数。
§P 的单一权威仍是 r11 的 `5.252µs`（跨机/跨负载不可比）。

## 8. 重算（命令原样可跑，输出即上面的数字）

```bash
cd <checkout> && PATH=<venv>/bin:$PATH PYTHONPATH=. python \
  scripts/issue69_mutation_matrix/table.py          # §1/§1b/§2/§3/§4/§4c/§4b/§X/§S 全部计数与全表
cd <checkout> && PATH=<venv>/bin:$PATH PYTHONPATH=. python \
  scripts/issue69_mutation_matrix/probes.py         # §X 多位置探针 + §4b 零登记/有登记对照
cd <checkout> && PATH=<venv>/bin:$PATH PYTHONPATH=. python \
  scripts/issue69_mutation_matrix/scope_inventory.py  # §S 清单与源码一致性（0 = 一致）
cd <checkout> && PATH=<venv>/bin:$PATH PYTHONPATH=. python \
  scripts/issue69_mutation_matrix/bench_expr.py    # §P 的 µs 表（含 mutator 三行）
cd <checkout> && PATH=<venv>/bin:$PATH PYTHONPATH=. python -m pytest \
  tests/unit/test_issue69_mutation_matrix.py tests/unit/test_issue69_mutation_gate.py \
  tests/unit/test_issue69_scope_inventory.py tests/unit/test_expression_provenance_perf.py \
  tests/unit/test_issue69_single_copy_point.py tests/unit/test_issue69_persistable_types.py \
  tests/unit/test_issue69_r7_uniformity.py -q
```

**常量因子预算**（§P）：基线在
`scripts/issue69_mutation_matrix/perf_baseline.json`（`reference` + `ratios` +
`tolerance`）；守卫测试把实测比值与 `ratios[expr] * tolerance` 比。基线值来自 r6 提交上
`bench_expr.py` 的比值上取整（留 10–30% 余量）；负控（把目标位热路径人为放慢 40µs）
实测越过 ceiling。r7 改动只在拷贝/登记判据（不在被测热路径的分子分母上），实测预算未变、
仍全绿（§P 的 r7 列）。

性能列（§P；每个 revision 的 checkout 内跑同一条）：把 `bench_expr.py` 放到该
checkout 的 `scripts/issue69_mutation_matrix/` 下，`PLAITA_BENCH_ROOT=<checkout>
PYTHONPATH=. python scripts/issue69_mutation_matrix/bench_expr.py`。

预算对照（§7 最后一行，一次性）：活 state 存在 + 纯容器作用域里挂引擎物化的
N 项 list，`$F.pop($VAR,0)`：r4 `N=49991/51001` 都具名 fail-closed 拒绝，r5 同两格都
allow（探测预算不再拒绝；非 fresh 目标仍由 default-deny 拒）。

变异重放（51 条 `69-*.json`）：

```bash
cd <checkout>
git show fix/68:scripts/mutation_replay/replay.py > scripts/mutation_replay/replay.py
python scripts/mutation_replay/replay.py --all --source worktree   # 提交前；--source head 提交后
rm scripts/mutation_replay/replay.py
```

（**必须放在 `<checkout>/scripts/mutation_replay/` 下**：`replay.py` 用
`Path(__file__).parents[2]` 定位仓库根与 `mutations/`；放 `/tmp` 会因 `git -C / archive
HEAD` 失败或静默加载 0 条定义。跑完即删。）

**工具限制（已知，统一 store 补丁修）**：重放器只用退出码判定杀死，`killed-by` 一栏只解析
`FAILED`/`ERROR` 行，**不解析 `SUBFAILED(`**。`69-fresh-producer-default-true` 正是被
`subTest` 断言杀死的，重放器显示 `KILLED red`（无 killer id）。
