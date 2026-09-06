"""P0-2 守卫：扫描 ``plaita/`` 源码, 拦截"静默吞咽异常"的 except 块。

一个 except 块如果既不 ``raise``、不记日志 (``logger.*`` / ``logging.*`` /
``self.log*``)、也不把异常对象显式存进返回值/哨兵, 就是在静默吞咽——这是
本项目历史重灾区 (历史上 161 处 ``except Exception`` + 5 处 bare ``except:``)。

本测试把当前已知的"可接受位点"列入白名单, 阻止新增静默吞咽。新代码请:
要么精确捕获并 ``raise``/记日志, 要么显式返回带 ``__error__`` 的哨兵。
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

PLAITA_DIR = Path(__file__).resolve().parents[2] / "plaita"

# 已审计的"可接受静默位点": 每条是 (相对 plaita/ 的文件路径, 行号)。
# 这些是预期的 fallback 分支 (如 JSON 解析失败回退到原始文本、版本号排序
# 失败回退到任意版本), 不记录异常是合理的。
ALLOWED_SILENT = {
    ("node/http.py", 318),   # response.json() 失败回退到 response.text (sync path)
    ("node/http.py", 369),   # json.loads() 失败回退到原始文本 (async aiohttp path)
    ("storage/memory.py", 138),  # 版本号非纯数字排序失败, 回退到任意版本
}


def _is_logging_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
        return False
    func = node.value.func
    # logger.xxx / logging.xxx / self.log.xxx / self.logger.xxx
    if isinstance(func, ast.Attribute):
        name = func.attr
        if name in {"debug", "info", "warning", "warn", "error", "critical",
                    "exception", "log"}:
            return True
    return False


def _body_is_silent(body: list[ast.stmt]) -> bool:
    """真"静默": body 里既无 ``raise``、无任何函数调用。有调用即视为非静默——
    调用可能是记日志、调 helper 重新抛、或把异常对象放进 queue/返回值传播。
    真静默形态: ``pass`` / ``return None`` / ``return {}`` / ``x = None``。"""
    for stmt in body:
        for child in ast.walk(stmt):
            if isinstance(child, ast.Raise):
                return False
            if isinstance(child, ast.Call):
                return False
    return True


def _is_blind_except(handler: ast.ExceptHandler) -> bool:
    """只盯"盲捕": ``except Exception``/``except BaseException``/bare ``except:``。
    窄捕获 (``except ImportError``/``except KeyError`` 等) 是显式选择, 不算静默吞咽文化。"""
    t = handler.type
    if t is None:
        return True  # bare except
    if isinstance(t, ast.Name) and t.id in {"Exception", "BaseException"}:
        return True
    # except (Exception, ...) 形式
    if isinstance(t, ast.Tuple):
        return any(isinstance(e, ast.Name) and e.id in {"Exception", "BaseException"}
                   for e in t.elts)
    return False


def _collect_silent_except(path: Path) -> list[tuple[int, str]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and _is_blind_except(node):
            if not _body_is_silent(node.body):
                continue
            offenders.append((node.lineno, path.relative_to(PLAITA_DIR).as_posix()))
    return offenders


def test_no_bare_except_in_plaita():
    """bare ``except:`` 连 KeyboardInterrupt/SystemExit 都吞, 必须为 0。"""
    bare = []
    for path in PLAITA_DIR.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler) and node.type is None:
                bare.append((path.relative_to(PLAITA_DIR).as_posix(), node.lineno))
    assert not bare, f"bare `except:` forbidden in plaita/: {bare}"


def test_silent_except_blocks_are_whitelisted():
    """所有"无日志无 raise"的 except 块必须在白名单里, 防止新增静默吞咽。"""
    all_silent = []
    for path in PLAITA_DIR.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        for lineno, rel in _collect_silent_except(path):
            all_silent.append((rel, lineno))

    unlisted = [
        (rel, lineno) for (rel, lineno) in all_silent
        if (rel, lineno) not in ALLOWED_SILENT
    ]
    if unlisted:
        pytest.fail(
            "Found silent except blocks (no raise / no logging) not in ALLOWED_SILENT:\n"
            + "\n".join(f"  {rel}:{lineno}" for rel, lineno in unlisted)
            + "\nEither log the exception, re-raise it, or add to ALLOWED_SILENT "
            "with a justification comment."
        )
