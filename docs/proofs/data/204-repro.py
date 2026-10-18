#!/usr/bin/env python3
"""plaita#204 repro / release smoke：真 wheel → 干净 venv → 导入 + 启动。

前置（脚本不负责装依赖，只验证发布物）：

    uv venv /tmp/p204/venv
    uv pip install --python /tmp/p204/venv/bin/python /path/to/plaita-repo
    uv pip install --python /tmp/p204/venv/bin/python /path/to/plaita_console-*.whl
    /tmp/p204/venv/bin/python docs/proofs/data/204-repro.py \
        --wheel /path/to/plaita_console-*.whl --python /tmp/p204/venv/bin/python

断言（修前红 / 修后绿）：

1. wheel 清单不含 ``plaita_console/tests/``、``plaita_console/build/``、``*.db``、
   ``*.egg-info``、``plaita_console/pyproject.toml``、``requirements.txt``；
2. wheel 含运行时代码：main.py / auth.py / config.py / __main__.py / api/…、
   dist-info entry_points 指向 ``plaita_console.__main__:main``；
3. 干净解释器（清空 PYTHONPATH、cwd=临时目录）``import plaita_console.main`` 成功；
4. ``python -m plaita_console`` 能起 uvicorn（无 ImportError），随后被终止。
"""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

FORBIDDEN_PREFIXES = ("plaita_console/tests/", "plaita_console/build/")
FORBIDDEN_SUFFIXES = (".db", ".pyc", ".pyo")
FORBIDDEN_FRAGMENTS = ("egg-info", "__pycache__")
FORBIDDEN_EXACT = {"plaita_console/pyproject.toml", "plaita_console/requirements.txt"}
REQUIRED_PREFIXES = (
    "plaita_console/main.py",
    "plaita_console/auth.py",
    "plaita_console/config.py",
    "plaita_console/__main__.py",
    "plaita_console/api/",
    "plaita_console/services/",
    "plaita_console/models/",
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def check_manifest(wheel: Path) -> list[str]:
    failures = []
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        entry_points = ""
        for name in names:
            if name.endswith(".dist-info/entry_points.txt"):
                entry_points = archive.read(name).decode("utf-8", "replace")

    for name in names:
        if name.startswith(FORBIDDEN_PREFIXES) or name.endswith(FORBIDDEN_SUFFIXES):
            failures.append(f"wheel 含非运行时产物: {name}")
        if any(fragment in name for fragment in FORBIDDEN_FRAGMENTS):
            failures.append(f"wheel 含构建残留: {name}")
        if name in FORBIDDEN_EXACT:
            failures.append(f"wheel 含源码/开发配置文件: {name}")
    for required in REQUIRED_PREFIXES:
        if not any(name.startswith(required) for name in names):
            failures.append(f"wheel 缺运行时文件/目录: {required}")
    if "plaita_console.__main__:main" not in entry_points:
        failures.append(f"entry_points 未指向 plaita_console.__main__:main: {entry_points!r}")
    return failures


def run_import_smoke(python: Path, workdir: Path) -> list[str]:
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    proc = subprocess.run(
        [str(python), "-c", "import plaita_console.main; print('import-ok')"],
        cwd=str(workdir),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0 or "import-ok" not in output:
        tail = "\n".join(output.strip().splitlines()[-12:])
        return [f"干净解释器 import plaita_console.main 失败（rc={proc.returncode}）:\n{tail}"]
    return []


def run_start_smoke(python: Path, workdir: Path) -> list[str]:
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    port = _free_port()
    env.update(
        {
            "PLAITA_CONSOLE_HOST": "127.0.0.1",
            "PLAITA_CONSOLE_PORT": str(port),
            "PLAITA_CONSOLE_DB_URL": f"sqlite:///{workdir}/smoke.db",
        }
    )
    proc = subprocess.Popen(
        [str(python), "-m", "plaita_console"],
        cwd=str(workdir),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines: list[str] = []
    deadline = time.time() + 40
    started = False
    try:
        assert proc.stdout is not None
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    break
                continue
            lines.append(line.rstrip())
            if "Application startup complete" in line or "Uvicorn running" in line:
                started = True
                break
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - 强制收尸
            proc.kill()
            proc.wait(timeout=10)

    output = "\n".join(lines)
    failures = []
    if not started:
        tail = "\n".join(lines[-15:]) or "(无输出)"
        failures.append(f"python -m plaita_console 未进入启动完成（{output.count(chr(10))} 行）:\n{tail}")
    for marker in ("ImportError", "ModuleNotFoundError", "Traceback (most recent call last)"):
        if marker in output:
            failures.append(f"启动输出含 {marker}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path, help="plaita_console-*.whl")
    parser.add_argument("--python", required=True, type=Path, help="安装了该 wheel 的 venv python")
    args = parser.parse_args()

    if not args.wheel.is_file():
        print(f"FAIL: wheel 不存在: {args.wheel}")
        return 1
    if not args.python.is_file():
        print(f"FAIL: 解释器不存在: {args.python}")
        return 1

    failures = []
    with tempfile.TemporaryDirectory(prefix="plaita204-") as tmp:
        workdir = Path(tmp)
        for label, fn in (
            ("wheel 清单", lambda: check_manifest(args.wheel)),
            ("导入冒烟", lambda: run_import_smoke(args.python, workdir)),
            ("启动冒烟", lambda: run_start_smoke(args.python, workdir)),
        ):
            found = fn()
            if found:
                failures.extend(found)
                print(f"RED {label}")
                for item in found:
                    print(f"  - {item}")
            else:
                print(f"GREEN {label}")

    if failures:
        print(f"\nRED: #204 发布冒烟失败（{len(failures)} 项）")
        return 1
    print("\nGREEN: #204 wheel 安装后可用")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
