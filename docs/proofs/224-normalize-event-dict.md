# #224 证明：`normalize_event` 的 dict 分支不再调用不存在的 `Event.from_dict`

- 单号：plaita#224（P2，真实崩溃 bug）
- 基线：`origin/main` = `5d828f0`（本工作树 `wt-b2`）
- 边界：只动 `plaita/event/utils.py::normalize_event` 的实现与回归测试；
  不改事件总线后端、不改 `Event` 模型字段。

## 缺陷

`normalize_event` 的 dict 分支：

```python
return Event.from_dict(event_type_or_obj)
```

`Event` 是 pydantic v2 的 `BaseModel`，全仓没有 `from_dict` 定义，pydantic
也不提供该属性 ⇒ 任何带 `event_type` 的 dict 输入都在 `__getattr__` 处抛
`AttributeError: from_dict`。该函数是 `plaita.event` 的公开再导出，且
`SqlalchemyEventBus.publish` 的入参类型正是 `Union[Event, str, Dict[str, Any]]`
——db 事件总线的 dict 形态发布 100% 失败，事件既不落库也不分发。

同函数另有两处相邻缺陷：

1. str 分支 `event_data.update(kwargs)` 就地改写调用方传入的 `data`；
2. dict 分支（改前直接抛错，改后需保真）会把 `correlation_id` 等 dict 字段
   丢掉——`EventFilter` 对无 `correlation_id` 的事件直接跳过，会让
   「dict 形态 + correlation 匹配」整条链失效。

## 修法

`plaita/event/utils.py::normalize_event`：

```python
elif isinstance(event_type_or_obj, dict):
    if 'event_type' in event_type_or_obj:
        fields = dict(event_type_or_obj)          # 拷一份，不写调用方字典
        event_data = fields.get('data', {})
        if isinstance(event_data, str):
            try:
                event_data = json.loads(event_data)
            except json.JSONDecodeError:
                event_data = {'value': event_data}
        if event_data is None:
            event_data = {}
        fields['data'] = dict(event_data) if isinstance(event_data, dict) else event_data
        for key, value in kwargs.items():          # kwargs 只补缺省字段
            fields.setdefault(key, value)
        return Event.model_validate(fields)
```

- 走 pydantic v2 既有构造路径 `model_validate`，不再依赖不存在的 `from_dict`；
- `correlation_id` / `event_id` / `source` / `tenant_id` 等 dict 字段随
  `model_validate` 保真，`EventFilter` 的 correlation 路由恢复；
- dict 与 str 两个分支都先拷贝：规范化不改写调用方入参。

## 测试

新增 `tests/unit/test_issue224_normalize_event_dict.py`（19 例）：

- 全分支正常态：`Event` 实例原样返回、str 构造、dict 构造；
- dict 细节：`correlation_id` 保真、缺 `data` / `data=None` → `{}`、
  JSON 字符串解码与非法 JSON 落 `{"value": ...}`、kwargs 补
  `correlation_id`、无 `event_type` 的 dict 当 data；
- 不改写入参：dict 分支与 str 分支各一例；
- 畸形输入三态：list / `None` / int → `TypeError`；dict 的 `data` 为 list、
  `event_type` 为 `None` → pydantic `ValidationError`（不再 `AttributeError`、
  也不静默吞掉）；
- 实际触发点：`SqlalchemyEventBus.publish(dict)` → `get_event` 回读
  `event_type` / `data` / `correlation_id` 保真，且发布后调用方字典不变
  （sqlite+aiosqlite 真实存储，非桩）。

## 红 → 绿与负控

修复前（HEAD 的 `Event.from_dict` 实现），新用例 **12 failed / 7 passed**：

```
plaita/event/utils.py:55: in normalize_event
    return Event.from_dict(event_type_or_obj)
...
E       AttributeError: from_dict
```

保持绿的 7 例是修前也不经过 `from_dict` 的分支（Event 透传 / str 构造 /
无 `event_type` 的 dict / list、None、int 的 TypeError），属正确反例。

负控按「只还原实现、测试与 repro 不动」执行（`git checkout HEAD --
plaita/event/utils.py`），原始输出 `docs/proofs/data/224-negative-control.txt`。
还原后：新用例 **19 passed**，repro **ALL OK**。

## 验收（`.venv/bin/python`，py3.13）

- `tests/unit/test_issue224_normalize_event_dict.py`：**19 passed**（1.04s）；
- 定向事件面（#224 + event core/filter/queue correlation/redis parity/
  memory/reviewfix）：**253 passed / 3 subtests passed**（9.52s）；
- `tests/unit -m "not slow"`：**7899 passed / 15 skipped / 13 deselected /
  3434 subtests passed / 0 failed**（333.40s；基线 7880 + 本单 19）；
- `tests/integration`：**213 passed / 1 skipped / 0 failed**（32.21s）；
- `tests/e2e`：**74 passed / 3 skipped / 0 failed**（5.05s）；
- ruff（实现 + 新测试 + repro）：**All checks passed**；
- `python scripts/landing_gate.py ci`：main 最近完成 run `5d828f0`
  conclusion=success，✅ 允许落地。

## 残面/边界

- `Event.model_validate` 对 `data` 非 dict（如 list）会抛 pydantic
  `ValidationError`——这是领域内的类型错误，不再是 AttributeError；调用方
  若需宽松归一化应另开单定义语义。
- 无 `event_type` 的 dict 仍按既有语义当 data 处理（`generic.event` 或
  kwargs 指定），本单未改。
- 事件总线后端（sqlalchemy 的 fire-and-forget 分发等）不在本单范围。
