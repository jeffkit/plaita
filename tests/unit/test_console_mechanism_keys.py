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
    assert f("plaita:execution:index")           # ← 2026-10-07 补（#40）
    assert f("plaita:execution:index:ready")     # ← 同上（裸字符串 "1"）
    assert f("plaita:flow:queue:v2:dlq")


def test_real_execution_keys_pass():
    f = get_is_mechanism_key()
    # 真实执行状态键（value 是 summary dict）不应被排除
    assert not f("plaita:execution:abcdef012345")
    assert not f("plaita:tenant-a:execution:abcdef012345")
