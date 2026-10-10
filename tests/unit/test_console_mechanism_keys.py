"""机制键排除回归：与执行状态同前缀的「裸值」键必须被 list API 过滤。

背景（同类第三次）：
- 2026-10-02 `:execution:fence:`（裸整数）→ 列表 500；
- 2026-10-06 `:execution:noderetry:`（裸整数）→ 远端 console 列表 API 全崩
  （AttributeError: 'int' object has no attribute 'get'）；
- 2026-10-07 `:execution:index`（ZSET）/ `:execution:index:ready`（裸字符串
  "1"）→ #40 存储面索引，ready 标记同样会被当 summary dict 崩。

本测试把「哪些 key 必须排除」钉死，新增同类键时在此登记。
"""
import re
from pathlib import Path

import pytest


def get_is_mechanism_key():
    """从 console backend 源码提取 _is_mechanism_key（backend 不在包路径，
    直接 exec 其函数体，保证测的是真源码而非副本）。"""
    src = Path(__file__).resolve().parents[2] / "plaita-console" / "backend" / "api" / "executions.py"
    if not src.exists():
        pytest.skip("console backend 不在本检出")
    text = src.read_text(encoding="utf-8")
    m = re.search(r"def _is_mechanism_key\(key: str\) -> bool:(.*?)\n\ndef ", text, re.S)
    assert m, "未找到 _is_mechanism_key"
    ns = {}
    exec("def _is_mechanism_key(key):" + m.group(1), ns)
    return ns["_is_mechanism_key"]


def test_mechanism_keys_are_excluded():
    f = get_is_mechanism_key()
    # 全部机制键（同前缀、值非 summary dict）
    assert f("plaita:execution:lease:abc")
    assert f("plaita:execution:fence:abc")
    assert f("plaita:execution:cancel:abc")
    assert f("plaita:execution:noderetry:abc")   # ← 2026-10-06 补
    assert f("plaita:execution:g1wakeups:abc")   # ← 2026-10-10 补（#73 补丁，值守引入）
    assert f("plaita:execution:index")           # ← 2026-10-07 补（#40）
    assert f("plaita:execution:index:ready")     # ← 同上（裸字符串 "1"）
    assert f("plaita:flow:queue:v2:dlq")


def test_real_execution_keys_pass():
    f = get_is_mechanism_key()
    # 真实执行状态键（value 是 summary dict）不应被排除
    assert not f("plaita:execution:abcdef012345")
    assert not f("plaita:tenant-a:execution:abcdef012345")


def test_no_unregistered_bare_int_keys_from_worker():
    """★ 自动防线：worker 源码里声明的机制键**必须**被 console 排除。

    本类事故已发生五次（fence / noderetry / index / index:ready / g1wakeups），
    每次都是「worker 新增一个同前缀裸值键 → console 列表 API 500」。人工登记
    总会漏，所以这里直接扫 worker 源码里所有 `execution:<name>:` 形态的键构造，
    凡不在排除名单者即失败——**新增机制键会被立刻挡住**，而不是等线上 500。
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    worker = root / "plaita" / "server" / "flow_worker.py"
    if not worker.exists():
        pytest.skip("worker 源码不在本检出")

    text = worker.read_text(encoding="utf-8")
    # 形如 f"{ns}:execution:xxx:{id}" 的键名（取 :execution: 与 :{ 之间的段）。
    # 名字段必须含数字——`g1wakeups` 正是自动防线要挡的那类键，旧正则
    # `[a-z_]+` 匹配不到它（解析结果里没有它，只有人工 assert 覆盖）。
    names = set(re.findall(r":execution:([a-z0-9_]+):\{", text))
    assert names, "未从 worker 源码解析出任何机制键（正则失效？）"
    assert "g1wakeups" in names, "含数字的机制键必须被自动防线解析到（防止正则退化）"

    f = get_is_mechanism_key()
    missing = [n for n in sorted(names)
               if not f(f"plaita:execution:{n}:probe")]
    assert not missing, (
        "worker 声明了 console 未排除的机制键 → 这些键的值若是裸值（int/str），"
        "会让 GET /api/executions 500（AttributeError: 'int' object has no attribute 'get'）。"
        f"\n未登记: {missing}"
        "\n请在 plaita-console/backend/api/executions.py 的 _is_mechanism_key 补登记。"
    )
