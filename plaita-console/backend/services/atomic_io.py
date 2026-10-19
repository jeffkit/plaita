"""同目录原子写助手（plaita#253）。

``open(path, "w")`` 在写第一个字节前就把旧文件截断了：之后 ENOSPC / 配额 /
进程被 kill 只会留下半截 YAML，上一份完好配置已不可恢复。这里沿用 #249
registry 的模板：调用方先序列化到字符串，再同目录 ``mkstemp`` →
write + flush + fsync → ``os.replace``；失败路径只清理临时文件，旧文件
逐字节不变。
"""
from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path
from typing import Union


def atomic_write_text(
    path: Union[str, Path], text: str, *, default_mode: int = 0o600
) -> None:
    """原子替换 ``path`` 的文件内容。

    已存在的目标沿用原权限（避免保存一次就悄悄改变可读范围）；新建文件按
    ``default_mode`` 落盘（mkstemp 自身是 0600）。失败时临时文件被清理并
    原样抛出异常，目标文件保持调用前的内容。
    """
    target = Path(path)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        if target.exists():
            os.chmod(tmp, stat.S_IMODE(target.stat().st_mode))
        else:
            os.chmod(tmp, default_mode)
        os.replace(tmp, target)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise
