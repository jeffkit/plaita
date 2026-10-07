"""节点 spawn 子进程时的公共安全层：环境白名单 + 输出截断（stdlib-only）。

2026-09 安全评审 P1：CodeNode 的 subprocess 后端曾把宿主全量 ``os.environ``
交给沙箱内代码——生产环境等于把 API key / 云凭证 / DB、Redis 连接串交出去。
修复只落在 code 节点，同类的 spawn 节点（plaita-nodes 兄弟仓的 gate /
capture / agent 直跑）沿用同一策略：**白名单重建 + 显式 extra**，而不是
``os.environ.copy()``。

上沉为顶层模块的原因同 ``plaita.credentials`` / ``plaita.tenant_context``：
兄弟仓只依赖 plaita 的公开窄接口，不应 import 节点实现的私有符号。

节点用法::

    from plaita.subprocess_env import build_subprocess_env, clip_output

    proc = subprocess.Popen(cmd, env=build_subprocess_env(extra={"FOO": "bar"}))
    return {"stdout": clip_output(stdout, 4000)}

``build_subprocess_env`` 的合并顺序（后者覆盖前者）：
宿主白名单命中项 → 模块级 :data:`SUBPROCESS_ENV_EXTRA` → 调用点 ``extra``。
"""
from __future__ import annotations

import os
from typing import Dict, FrozenSet, Mapping, Optional

__all__ = [
    "SUBPROCESS_ENV_ALLOWLIST",
    "SUBPROCESS_ENV_EXTRA",
    "build_subprocess_env",
    "clip_output",
]

# 子进程环境变量白名单：只这些宿主变量进子进程。需要额外变量时往模块级
# SUBPROCESS_ENV_EXTRA 里加（启动脚本里设置），或调用点传 extra=。
SUBPROCESS_ENV_ALLOWLIST: FrozenSet[str] = frozenset({
    "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE", "PYTHONIOENCODING",
})

SUBPROCESS_ENV_EXTRA: Dict[str, str] = {}


def build_subprocess_env(extra: Optional[Mapping[str, object]] = None) -> Dict[str, str]:
    """按白名单重建子进程环境，返回可直接传给 ``Popen(env=...)`` 的 dict。

    ``extra`` 内的值统一 ``str()`` 化（子进程 env 只接受字符串）。
    """
    child_env: Dict[str, str] = {
        key: os.environ[key]
        for key in sorted(SUBPROCESS_ENV_ALLOWLIST)
        if key in os.environ
    }
    child_env.update(SUBPROCESS_ENV_EXTRA)
    if extra:
        child_env.update({str(k): str(v) for k, v in extra.items()})
    return child_env


def clip_output(text: str, cap: int) -> str:
    """超阈值时头 1/4 + 尾 3/4 保留并标注省略量。

    头尾都留而不纯头部切片：诊断信息（``failures:`` / ``test result:`` /
    traceback）几乎总在尾部，纯头部切片会把唯一有用的信息丢掉。
    """
    if cap <= 0 or len(text) <= cap:
        return text
    head_len = cap // 4
    return f"{text[:head_len]}…[省略 {len(text) - cap} 字符]…{text[-(cap - head_len):]}"
