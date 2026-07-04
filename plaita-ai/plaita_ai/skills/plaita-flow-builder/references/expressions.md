# 表达式参考

表达式是 plaita 的「数据胶水」：在节点的 `output`、`condition`、`input` 等字段中引用上下文数据、调用内置函数。

> **重要：表达式前缀是 `$`，不是 `${}`。**
> 变量引用写作 `$INPUT.name`，函数调用写作 `$F.upper($INPUT.name)`。
> ❌ 不要写成 `${INPUT.name}`——那是错误语法，会被当普通字符串原样返回。

## 变量引用

用 `$` 前缀引用执行上下文中的命名空间：

| 表达式 | 含义 |
|--------|------|
| `$INPUT` | 整个输入对象 |
| `$INPUT.name` | 输入对象的 `name` 字段 |
| `$NODE.assign` | 节点 `assign` 的输出 |
| `$NODE.assign.field` | 节点 `assign` 输出的 `field` 字段 |
| `$GLOBAL.key` | 全局上下文变量 |
| `$PARENT.x` | 父流程上下文（子流程中可用） |
| `$ENV.PATH` | 环境变量 `PATH` |

命名空间：`INPUT` / `NODE` / `GLOBAL` / `PARENT` / `ENV`。

> `$ENV` 会自动过滤以 `SECRET`/`TOKEN`/`PASSWORD`/`API_KEY`/`CREDENTIAL`/`DATABASE_` 等前缀开头的敏感变量。

### 数组索引

```
$NODE.list.0           # 列表第 0 项
$NODE.list[0]          # 等价写法
$NODE.list[-1]         # 末项
```

## 字符串插值

当表达式只是字符串的**一部分**时，用 `{% ... %}` 包裹表达式做插值：

```json
"output": "你好，{% $INPUT.name %}，今年 {% $INPUT.age %} 岁"
```

- 整个值就是表达式：`"output": "$INPUT.name"` —— 直接写 `$INPUT.name`
- 表达式是字符串的一部分：`"output": "hi {% $INPUT.name %}"` —— 用 `{% %}`

## 函数调用

用 `$F.funcName(args)` 调用内置函数，参数本身也可以是表达式：

```json
"output": "$F.upper($INPUT.name)"
"output": "$F.concat($INPUT.first, '-', $INPUT.last)"
"output": "$F.len($NODE.items)"
```

## 内置函数一览

### 数学 math
`add` `sub` `mul` `div` `mod` `pow` `abs` `ceil` `floor` `round` `trunc` `sqrt`

### 字符串 string
`lower` `upper` `capitalize` `title` `strip` `lstrip` `rstrip` `replace` `split` `join` `startswith` `endswith` `concat` `isDigit`

### 逻辑 logic
`and` `or` `not`

### 数组 array
- 纯函数：`len` `length` `index` `slice` `append` `extend` `insert` `remove` `reverse` `sort` `getListItem` `addListItem` `insertListItem`
- 带副作用（就地修改，非线程安全）：`pop` `delListItem` `setListItem`

### 字典 dict
- 纯函数：`keys` `values` `items` `get` `getDictValue` `getDictKeys` `getDictValues`
- 带副作用：`set` `delete` `clear` `setDictValue` `delDictValue` `clearDict`

### 日期时间 datetime
`now` `today`（接受可选 `fmt` 参数）

### JSON
`json_loads` `json_dumps`

> 副作用函数（`pop`/`set`/`delete`/`clear`）非线程安全，在 `Parallel` 并行或共享上下文异步场景中慎用。
