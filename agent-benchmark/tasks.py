"""agent-benchmark 任务集定义

每个任务描述一个"需求 + 测试用例 + 验收方式"。harness 把需求交给 AI Agent
（claude code CLI + deepseek-v4-flash），让它用 @flow 生成并执行流程，
再对照测试用例自动评分。

任务按难度与考查的 @flow 特性分组，覆盖：
  - 条件分支 (if/elif/else)
  - 赋值 + 表达式函数 (F.*)
  - 字符串拼接 (F.concat，避免 f-string)
  - 集合节点 (MAP / FILTER / FIND / REDUCE)
  - 子流程 (@childflow + CHILD)
  - 并行 (PARALLEL)
  - HTTP + 错误处理
  - 多步骤组合

validator 取值：
  - "exact"   : actual 与 expected 完全相等
  - "contains": expected 是 actual 的子串（用于 LLM/HTTP 等非确定性输出）
  - "keys"    : actual 是 dict，且指定 key 的值与 expected 相等
  - "callable": 用 validator_fn(actual, expected) -> bool 自定义（见 TASKS 注释）
"""

from __future__ import annotations

from typing import Any


def _exact(actual: Any, expected: Any) -> bool:
    return actual == expected


def _contains(actual: Any, expected: Any) -> bool:
    return str(expected) in str(actual)


def _keys(actual: Any, expected: Any) -> bool:
    if not isinstance(actual, dict):
        return False
    return all(actual.get(k) == v for k, v in expected.items())


VALIDATORS = {
    "exact": _exact,
    "contains": _contains,
    "keys": _keys,
}


TASKS: list[dict[str, Any]] = [
    # ------------------------------------------------------------------ easy
    {
        "id": "cond-grade",
        "category": "conditional",
        "difficulty": "easy",
        "requirement": (
            "实现一个评分流程：输入 score（整数），score>=90 返回 'A'，"
            "60<=score<90 返回 'B'，否则返回 'C'。"
        ),
        "test_cases": [
            {"input": {"score": 95}, "expected": "A"},
            {"input": {"score": 72}, "expected": "B"},
            {"input": {"score": 60}, "expected": "B"},
            {"input": {"score": 59}, "expected": "C"},
            {"input": {"score": 0}, "expected": "C"},
        ],
        "validator": "exact",
    },
    {
        "id": "str-greet",
        "category": "string_concat",
        "difficulty": "easy",
        "requirement": (
            "实现打招呼流程：输入 name（字符串），先把它转大写，"
            "再返回 'HELLO, ' + 大写名 + '!' 的拼接结果。"
            "（注意：不要用 f-string 或 + 拼字符串，用 F.concat）"
        ),
        "test_cases": [
            {"input": {"name": "alice"}, "expected": "HELLO, ALICE!"},
            {"input": {"name": "Bob"}, "expected": "HELLO, BOB!"},
        ],
        "validator": "exact",
    },
    {
        "id": "arith-calc",
        "category": "arithmetic",
        "difficulty": "easy",
        "requirement": (
            "实现一个简单计算器：输入 a、b（整数），返回 (a + b) * 2 的结果。"
        ),
        "test_cases": [
            {"input": {"a": 3, "b": 5}, "expected": 16},
            {"input": {"a": 0, "b": 0}, "expected": 0},
            {"input": {"a": -2, "b": 4}, "expected": 4},
        ],
        "validator": "exact",
    },
    # ---------------------------------------------------------------- medium
    {
        "id": "map-double",
        "category": "map",
        "difficulty": "medium",
        "requirement": (
            "实现翻倍列表流程：输入 numbers（整数列表），返回每个元素乘 2 后的新列表。"
            "（用 MAP 集合节点）"
        ),
        "test_cases": [
            {"input": {"numbers": [1, 2, 3, 4]}, "expected": [2, 4, 6, 8]},
            {"input": {"numbers": []}, "expected": []},
            {"input": {"numbers": [10]}, "expected": [20]},
        ],
        "validator": "exact",
    },
    {
        "id": "filter-evens",
        "category": "filter",
        "difficulty": "medium",
        "requirement": (
            "实现过滤偶数流程：输入 nums（整数列表），返回只含偶数的新列表。"
            "（用 FILTER 集合节点，子流程返回 bool）"
        ),
        "test_cases": [
            {"input": {"nums": [1, 2, 3, 4, 5, 6]}, "expected": [2, 4, 6]},
            {"input": {"nums": [1, 3, 5]}, "expected": []},
            {"input": {"nums": [2, 4]}, "expected": [2, 4]},
        ],
        "validator": "exact",
    },
    {
        "id": "find-first-even",
        "category": "find",
        "difficulty": "medium",
        "requirement": (
            "实现找首个偶数流程：输入 nums（整数列表），返回第一个偶数；"
            "若没有偶数返回 None。"
            "（用 FIND 集合节点）"
        ),
        "test_cases": [
            {"input": {"nums": [1, 3, 4, 6]}, "expected": 4},
            {"input": {"nums": [2, 4, 6]}, "expected": 2},
            {"input": {"nums": [1, 3, 5]}, "expected": None},
        ],
        "validator": "exact",
    },
    {
        "id": "reduce-sum",
        "category": "reduce",
        "difficulty": "medium",
        "requirement": (
            "实现求和流程：输入 nums（整数列表），返回所有元素之和。"
            "（用 REDUCE 集合节点，循环变量写成 (first, second) 元组解包，初始值 0）"
        ),
        "test_cases": [
            {"input": {"nums": [1, 2, 3, 4]}, "expected": 10},
            {"input": {"nums": []}, "expected": 0},
            {"input": {"nums": [100]}, "expected": 100},
        ],
        "validator": "exact",
        "known_broken": True,  # plaita 运行时 REDUCE 报 KeyError，非 skill 问题
    },
    {
        "id": "childflow-double",
        "category": "childflow",
        "difficulty": "medium",
        "requirement": (
            "用子流程实现翻倍：定义一个 @childflow 把输入的 item 翻倍，"
            "主流程用 CHILD 调用它，输入 payload（整数），返回翻倍结果。"
        ),
        "test_cases": [
            {"input": {"payload": 21}, "expected": 42},
            {"input": {"payload": 0}, "expected": 0},
            {"input": {"payload": -5}, "expected": -10},
        ],
        "validator": "exact",
    },
    {
        "id": "router-intent",
        "category": "conditional_nested",
        "difficulty": "medium",
        "requirement": (
            "实现意图路由流程：输入 type（字符串）。"
            "type=='sales' 返回 '销售客服'；type=='support' 返回 '技术支持'；"
            "type=='billing' 返回 '计费客服'；其余返回 '通用客服'。"
        ),
        "test_cases": [
            {"input": {"type": "sales"}, "expected": "销售客服"},
            {"input": {"type": "support"}, "expected": "技术支持"},
            {"input": {"type": "billing"}, "expected": "计费客服"},
            {"input": {"type": "other"}, "expected": "通用客服"},
        ],
        "validator": "exact",
    },
    # ------------------------------------------------------------------ hard
    {
        "id": "map-filter-chain",
        "category": "composite",
        "difficulty": "hard",
        "requirement": (
            "实现组合流程：输入 nums（整数列表）。先把每个元素乘 3（MAP），"
            "再过滤出大于 10 的结果（FILTER），返回最终列表。"
            "可以用两个集合节点串起来，或用子流程组合。"
        ),
        "test_cases": [
            {"input": {"nums": [1, 2, 3, 4, 5]}, "expected": [12, 15]},
            {"input": {"nums": [0, 1]}, "expected": []},
            {"input": {"nums": [10]}, "expected": [30]},
        ],
        "validator": "exact",
    },
    {
        "id": "parallel-fanout",
        "category": "parallel",
        "difficulty": "hard",
        "requirement": (
            "用 PARALLEL 实现并行扇出：输入 x（整数）。两个分支："
            "分支 a 返回 x*2，分支 b 返回 x+10。两个分支都要 join，"
            "返回形如 {'a': ..., 'b': ...} 的字典。"
            "（需要定义两个 @childflow 子流程）"
        ),
        "test_cases": [
            {"input": {"x": 5}, "expected": {"a": 10, "b": 15}},
            {"input": {"x": 0}, "expected": {"a": 0, "b": 10}},
        ],
        "validator": "exact",
        "known_broken": True,  # plaita 运行时 parallel 子流程表达式求值 bug
    },
    {
        "id": "discount-price",
        "category": "multi_step",
        "difficulty": "hard",
        "requirement": (
            "实现折扣计算流程：输入 price（原价，数字）和 is_vip（布尔）。"
            "VIP 打 8 折，非 VIP 打 9 折；若折后价低于 50，则取 50（保底价）。"
            "返回最终价格。"
        ),
        "test_cases": [
            {"input": {"price": 100, "is_vip": True}, "expected": 80},
            {"input": {"price": 100, "is_vip": False}, "expected": 90},
            {"input": {"price": 40, "is_vip": True}, "expected": 50},
            {"input": {"price": 50, "is_vip": False}, "expected": 50},
        ],
        "validator": "exact",
    },
    {
        "id": "http-continue-with",
        "category": "http",
        "difficulty": "hard",
        "requirement": (
            "实现一个带错误兜底的 HTTP 流程（用 plaita[http]）："
            "向 'https://httpbin.org/status/500' 发 POST，"
            "on_error 用 ErrorHandler('continue_with', default={'data': 'fallback'})，"
            "返回 resp.data。预期该接口返回 500，因此流程应兜底返回 'fallback'，不抛错。"
            "（注意 HTTP 只能作赋值右侧，不能嵌在 return 里）"
        ),
        "test_cases": [
            {"input": {}, "expected": "fallback"},
        ],
        "validator": "exact",
        "requires_http": True,
        "flaky": True,  # 依赖外部 httpbin，可能偶发失败
    },

    # ---------------------------------------------------------- 新增用例
    {
        "id": "in-op-small",
        "category": "conditional_in",
        "difficulty": "easy",
        "requirement": (
            "实现判定流程：输入 x（整数）。若 x 在 [1, 2, 3] 中返回 'small'，"
            "否则返回 'big'。（用 in 操作符，写在 if 判断位置）"
        ),
        "test_cases": [
            {"input": {"x": 2}, "expected": "small"},
            {"input": {"x": 3}, "expected": "small"},
            {"input": {"x": 9}, "expected": "big"},
            {"input": {"x": 0}, "expected": "big"},
        ],
        "validator": "exact",
    },
    {
        "id": "len-count",
        "category": "builtin",
        "difficulty": "easy",
        "requirement": (
            "实现计数流程：输入 items（列表），返回其长度。（用 F.len）"
        ),
        "test_cases": [
            {"input": {"items": [1, 2, 3, 4, 5]}, "expected": 5},
            {"input": {"items": []}, "expected": 0},
            {"input": {"items": ["a"]}, "expected": 1},
        ],
        "validator": "exact",
    },
    {
        "id": "arith-precedence",
        "category": "arithmetic",
        "difficulty": "easy",
        "requirement": (
            "实现算术流程：输入 a、b、c（整数），返回 (a + b) * c。"
            "（表达式语言没有中缀优先级，先用赋值算 a+b，再乘 c）"
        ),
        "test_cases": [
            {"input": {"a": 3, "b": 5, "c": 2}, "expected": 16},
            {"input": {"a": 1, "b": 2, "c": 10}, "expected": 30},
            {"input": {"a": 0, "b": 0, "c": 99}, "expected": 0},
        ],
        "validator": "exact",
    },
    {
        "id": "string-template",
        "category": "string_concat",
        "difficulty": "medium",
        "requirement": (
            "实现字符串模板流程：输入 name（字符串）和 age（整数），"
            "返回 'name=<name>, age=<age>'。"
            "（不要用 f-string；用 F.concat 把各段拼起来，age 需要拼成字符串）"
        ),
        "test_cases": [
            {"input": {"name": "alice", "age": 20}, "expected": "name=alice, age=20"},
            {"input": {"name": "bob", "age": 5}, "expected": "name=bob, age=5"},
        ],
        "validator": "exact",
    },
    {
        "id": "map-dict-price",
        "category": "map",
        "difficulty": "medium",
        "requirement": (
            "实现价格翻倍流程：输入 items（列表，每个元素是 {'price': 数字}），"
            "用 MAP 对每个元素的 price 乘 2，返回翻倍后的价格列表。"
        ),
        "test_cases": [
            {"input": {"items": [{"price": 10}, {"price": 5}]}, "expected": [20, 10]},
            {"input": {"items": [{"price": 0}]}, "expected": [0]},
            {"input": {"items": []}, "expected": []},
        ],
        "validator": "exact",
    },
    {
        "id": "filter-count",
        "category": "filter",
        "difficulty": "medium",
        "requirement": (
            "实现偶数计数流程：输入 nums（整数列表），用 FILTER 过滤出偶数，"
            "再返回偶数的个数。（用 F.len 统计 FILTER 结果）"
        ),
        "test_cases": [
            {"input": {"nums": [1, 2, 3, 4, 5, 6]}, "expected": 3},
            {"input": {"nums": [1, 3, 5]}, "expected": 0},
            {"input": {"nums": [2, 4]}, "expected": 2},
        ],
        "validator": "exact",
    },
    {
        "id": "nested-childflow",
        "category": "childflow",
        "difficulty": "medium",
        "requirement": (
            "用嵌套子流程实现：定义子流程 add_one（输入 v，返回 v+1），"
            "再定义子流程 double_then_add（输入 v，用 CHILD 调 add_one，把结果乘 2 返回）。"
            "主流程输入 v，用 CHILD 调 double_then_add，返回结果。"
        ),
        "test_cases": [
            {"input": {"v": 10}, "expected": 22},
            {"input": {"v": 0}, "expected": 2},
            {"input": {"v": -1}, "expected": 0},
        ],
        "validator": "exact",
    },
    {
        "id": "guard-validate",
        "category": "conditional_nested",
        "difficulty": "medium",
        "requirement": (
            "实现带校验的流程：输入 age（整数）。若 age < 0 或 age > 150，返回 'invalid'；"
            "否则若 age >= 18 返回 'adult'，否则返回 'minor'。"
        ),
        "test_cases": [
            {"input": {"age": 25}, "expected": "adult"},
            {"input": {"age": 10}, "expected": "minor"},
            {"input": {"age": -1}, "expected": "invalid"},
            {"input": {"age": 200}, "expected": "invalid"},
            {"input": {"age": 18}, "expected": "adult"},
        ],
        "validator": "exact",
    },
    {
        "id": "assignment-chain",
        "category": "multi_step",
        "difficulty": "medium",
        "requirement": (
            "实现多步计算流程：输入 x（整数）。先算 step1 = x + 1，"
            "再算 step2 = step1 * 3，最后返回 step2 - 2。"
            "（用三个赋值节点串联，引用上游节点输出）"
        ),
        "test_cases": [
            {"input": {"x": 5}, "expected": 16},
            {"input": {"x": 0}, "expected": 1},
            {"input": {"x": 1}, "expected": 4},
        ],
        "validator": "exact",
    },
    {
        "id": "map-dict-filter-chain",
        "category": "composite",
        "difficulty": "hard",
        "requirement": (
            "实现组合流程：输入 items（列表，每个元素是 {'price': 数字}）。"
            "先用 MAP 把每个 price 乘 2，再用 FILTER 过滤出大于 20 的结果，返回最终列表。"
            "（MAP 与 FILTER 串联；注意集合节点输出引用）"
        ),
        "test_cases": [
            {"input": {"items": [{"price": 5}, {"price": 12}, {"price": 3}]}, "expected": [24]},
            {"input": {"items": [{"price": 1}, {"price": 2}]}, "expected": []},
            {"input": {"items": [{"price": 100}]}, "expected": [200]},
        ],
        "validator": "exact",
    },
    {
        "id": "tiered-discount",
        "category": "multi_step",
        "difficulty": "hard",
        "requirement": (
            "实现阶梯折扣流程：输入 price（原价，数字）。"
            "price > 100 打 7 折；50 < price <= 100 打 85 折；其余不打折。"
            "返回折后价（数字，不做保底）。"
        ),
        "test_cases": [
            {"input": {"price": 200}, "expected": 140},
            {"input": {"price": 100}, "expected": 85},
            {"input": {"price": 60}, "expected": 51},
            {"input": {"price": 50}, "expected": 50},
            {"input": {"price": 30}, "expected": 30},
        ],
        "validator": "exact",
    },
]


def get_task(task_id: str) -> dict[str, Any]:
    for t in TASKS:
        if t["id"] == task_id:
            return t
    raise KeyError(f"未知任务: {task_id}")


def filter_tasks(
    *,
    ids: list[str] | None = None,
    difficulty: str | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    out = []
    for t in TASKS:
        if ids and t["id"] not in ids:
            continue
        if difficulty and t["difficulty"] != difficulty:
            continue
        if category and t["category"] != category:
            continue
        out.append(t)
    return out
