#!/usr/bin/env python3
"""统一变异重放器（JSON store 为骨 + 兼容 ``mutations.py`` 模块 store）。

两种 store（可并存，同名冲突**显式报错**）：

1. **JSON 目录 store**（``fix/68`` / ``fix/79-85`` 起的约定，推荐）：
   ``mutations/*.json``，每份形如::

       {"name": "...", "issue": "68", "concern": "AC2",
        "file": "plaita/core/expression.py",
        "old": "<修复后源码里逐字节的片段，恰好出现一次>",
        "new": "<变异后的片段>",
        "rationale": "...",
        "expect_fail": ["tests/unit/...::test_..."]}

2. **Python 模块 store**（``fix/55``/``#133`` 的 ``mutations.py``）::

       DEFAULT_TESTS = ("tests/unit/a.py", ...)
       MUTATIONS = {"M16-x": {"note": "...",
                              "edits": [("file", "old", "new"), ...],
                              "tests": ("tests/unit/b.py::test_y",),   # 可选：覆盖 DEFAULT_TESTS
                              "expect_survive": True}}                 # 可选：documented-unobservable

   兼容层把它规范化成与 JSON 同形的 ``edits`` + ``expect_fail``（缺 ``tests`` 时取
   ``DEFAULT_TESTS``）——**不改任一 store 的语义**。

每条变异的流程（两种 store 共用同一算法）：

1. 目标树 = ``--tree``（复用导出树，各单的定义针对**各自**的代码状态）或按
   ``--source head|worktree`` 现导一棵；
2. 断言 ``plaita.__file__`` 落在该树内（隔离纪律，禁止测到别的 checkout）；
3. 干净树上跑 ``expect_fail``：必须**全绿**（基线不绿 ⇒ 失败，不算杀死）；
4. 逐条应用 ``edits``（每处 ``old`` 在目标文件里**恰好出现一次**）；
5. 变异树上跑同一组节点，**按 pytest 退出码 + ``--junit-xml`` 报告判死**（见下）；
6. 还原文件，继续下一条。

判死口径（**判死信道 = rc + pytest 自己的两条机读报告（``--junit-xml`` + ``--report-log``），
绝不读 stdout 下判决**）：

- 每次变异运行都在**同一个私有 0700 临时目录**（``tempfile.mkdtemp`` 建于仓外，随机路径，
  用完即删）里生成两份 pytest 机读产物：``--junit-xml=<tmp>/report.xml`` 与
  ``--report-log=<tmp>/report.jsonl``。判决由 **rc + XML 的 ``<failure>``/``<error>`` 计数 +
  两条信道的一致性**给出：
  - ``rc == 0`` ⇒ 存活（0 红）；
  - ``rc == 1``、XML ``<failure>`` ≥ 1 **且两条信道一致**、且 stdout 红 id **全部**能被 XML
    的 ``<testcase>`` 佐证、且 stdout 至少有一条红 id ⇒ **杀死**；
  - ``rc == 1`` 且 XML ``<failure> == 0`` 且（XML ``<error>`` ≥ 1 或无红 id）⇒ error；
  - ``rc == 1`` 且 stdout 红 id **有任一无法被 XML 佐证** ⇒ error（kind ``red-not-in-junit``），
    **永不判死**（r5 的 R1：``atexit`` 可在真 summary 之后伪造表头，故 stdout 红 id 必须有
    XML 佐证才进记录）。佐证关系**单向**（r6 R1）：以 XML 身份为权威，stdout 候选只准**等值
    或点分截断**（``red_id_matches_identity``）——``X.<真身份>``、``<anything>::test_real``、
    ``::test_real`` 这类**扩展**一律不接受（r5 的双向后缀匹配曾把伪造的更长的 id 当成真 id）；
    红行里 nodeid 与理由都**可以含空格甚至 ``" - "``**（``test_real[a b]`` / ``assert 5 - 2 == 4``），
    故 ``" - "`` 的切分不靠猜：每个切点各给一个候选，取首个被 XML 佐证的（r7 R1）；
    两条信道给出的**字面相同**的身份必须比较相等——``flat_identity`` 对 **pytest 产出的
    nodeid/身份** 幂等（r7 R2；r8 限定：**不**对任意字符串承诺幂等），param id 里的
    ``/``/``\\``/``::`` 不再被二次归一；
  - ``rc == 1``、XML ``<failure> ≥ 1`` 且带 testcase 身份，但 stdout **一条红 id 都没有**
    （真 id 被 ``atexit`` 的伪造段整段挤掉）⇒ error（kind ``red-unknown``），**永不判死**，
    绝不写 ``killed: true`` + ``red: []``（r6 R2）；
  - XML 与 report-log 的失败计数/失败 nodeid **不一致**、或 report-log 缺失/不可解析 ⇒ error
    （kind ``junit-reportlog-mismatch``）；
  - XML 的 ``<testsuite>`` 自报计数与元素数自相矛盾，或 XML 写入时间**不在本次运行窗口内**
    ⇒ error（kind ``junit-xml-inconsistent`` / ``junit-xml-outside-run-window``）；
  - ``rc ∉ {0, 1}`` ⇒ error（收集/导入/内部错误），**永不判死**；
  - XML **缺失或不可解析**（任何异常都不许逃出 :func:`read_junit`）⇒ error
    （kind ``junit-xml-missing-or-unparsable``），**永不判死**。读之前先用
    :func:`read_regular_bytes` 拒掉**非普通文件**（FIFO/设备/目录）并给读取量封顶：r5 的
    ``Path.read_bytes()`` 遇到 junit 路径上的 FIFO 会**永远阻塞**，``--timeout`` 只约束
    pytest 子进程、救不了父进程（r6 R5）。
  - 锚点失效（``old`` 不在目标树里恰好命中一次）⇒ error（kind ``anchors``，``rc: null``，
    ``error_line`` 是问题清单）。**主记录与旁证两侧都记 error**（r6 R9）：r5 只写旁证
    ``verdict=error``，主记录是 ``{"killed": false}`` 无 ``error`` 键，``doc_table`` 会把它
    渲染成 ``SURVIVED``。
- 变异代码能往 stdout 里 ``print("FAILED …")``，**也能伪造上面两份机读产物**（路径就在同一
  进程的 ``sys.argv`` 里）。stdout **没有**判死权：它只提供**红用例 id 展示**，且只从 pytest
  自己写的 ``short test summary info`` 段里取（captured stdout 段里的伪造行结构上进不来，
  见 :func:`failed_nodes`），并且每条 id 必须有 XML 佐证（见 :func:`justify_red`）。
  两条机读信道是**独立但不防伪造**的：进程内同时伪造两者**不可在进程内阻止**，属明确
  **威胁模型之外**；该边界与「记录内嵌可审计证据」的处置见 README「威胁模型」一节。
- 每次运行还把证据写进**记录旁证文件**（``--record`` 时默认 ``<record>.evidence.json``，
  可用 ``--evidence`` 指定）：XML 失败计数、report-log 结果、红 id 佐证情况、运行窗口与
  自洽性检查。主记录的五键形状**逐字节冻结**（§1.4 末），故证据走旁证文件。
- 单次 pytest 运行有超时（``--timeout``，默认 ``900``s，理由见 ``DEFAULT_TIMEOUT``）：超时 ⇒
  error（kind ``pytest-timeout``，``error_line`` 含阈值秒数），**永不判死、也不会一直挂着**；
  超时会**整组**（``start_new_session`` + ``os.killpg``，POSIX）终止；**限度**同
  :func:`kill_process_group`：``os.setsid()`` 自起会话的孙进程 ``killpg`` 够不到，会以
  ``ppid=1`` 活下来（README §1.4 已按此声明，勿再声称「不留孙进程」）。
- 基线跑不起来也是 error，不是存活：解释器没有 pytest ⇒ kind ``pytest-unavailable``；
  基线超时 ⇒ ``pytest-timeout``；基线本来就红 ⇒ ``baseline-not-green``。
- ``expect_survive`` 的 documented-unobservable 允许 0 红（不判死，仍写 ``killed: true``），
  但**不得携带任何红 id**（r7 R3：任何 stdout 红行 ⇒ error ``expect-survive-with-red``）；
  且 **rc ∉ {0,1} 时 expect_survive 记 error**（r2 曾记 ``killed: true``，交叉点语义已变，见 README §1.4）。
- 记录**不再有占位红行**：0 红就写 ``"red": []``；error 额外写
  ``"error": true`` / ``"error_kind"`` / ``"rc"`` / ``"error_line"``。三条真实 store 记录
  **逐字节不变**，故超时值只进 error 记录的 ``error_line``，不给非 error 记录加键。

用法::

    python scripts/mutation_replay/replay.py --list
    python scripts/mutation_replay/replay.py --one m04-cursor-overshoot-window-end
    python scripts/mutation_replay/replay.py --all
    python scripts/mutation_replay/replay.py --all --keep                 # 保留临时树
    python scripts/mutation_replay/replay.py --all --store <dir> --tree <export>
    python scripts/mutation_replay/replay.py --all --record <record.json>  # 机读记录
    python scripts/mutation_replay/replay.py --all --evidence <e.json>     # 证据旁证（默认 <record>.evidence.json）
    python scripts/mutation_replay/replay.py --all --timeout 300           # 单次 pytest 超时
    python scripts/mutation_replay/replay.py --check-anchors --store <dir> --tree <export>

``--store`` 默认 = 本脚本所在目录（即同时加载 ``<store>/mutations/*.json`` 与
``<store>/mutations.py``）。退出码 0 = 全部杀死；1 = 有存活/error/基线不绿/锚点失效/
store 0 条定义；2 = 用法错（未知 ``--one``、``--check-anchors`` 缺 ``--tree``、
``--python`` 不存在/不可执行、``--timeout`` 为负）。
"""
from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

SCRIPT = Path(__file__).resolve()
ROOT = SCRIPT.parents[2]
DEFAULT_STORE = SCRIPT.parent

# N4：本进程 ``exec_module(mutations.py)`` 也不许往 ``--tree``/store 里写 ``__pycache__``
# （子进程另有 ``PYTHONDONTWRITEBYTECODE=1``；探针同样显式带上）。
sys.dont_write_bytecode = True

# 单次 pytest 运行的超时（秒）。理由：三条真实 store 里最重的一组 ``expect_fail`` 在负载下
# 实测约 25~30s/次（含基线共两次），900s ≈ 30 倍余量，既能兜住挂死又不误杀慢机；可用
# ``--timeout`` 覆盖，``--timeout 0`` 关闭。超时走 GNU timeout 约定值 124（pytest 自身
# 只用 0–5 与信号负值），⇒ 记 error（kind ``pytest-timeout``），**永不判死**。
DEFAULT_TIMEOUT = 900.0
TIMEOUT_RC = 124

# r7 R1：``FAILED`` 行**不能**用 ``\S+`` 抓 nodeid——param id 里可以含空格
# （``test_real[a b]``，任何 ``parametrize`` 带字符串参数都会中），``\S+`` 直接匹配失败、
# 红 id 变成 []，再撞上 ``red-unknown`` 就变成**假 error**（r6 引入的假拒绝）。
# 现在只锚定 ``FAILED `` 前缀取整段正文，nodeid 与理由的切分交给 :func:`_dash_cuts`
# （每个 ``" - "`` 切点都是候选，由 junit XML 的权威身份消歧）。
_FAILED = re.compile(r"^FAILED (.*)$")
# ``SUBFAILED(<subTest 上下文>) <node>``：上下文里**可以含空格与括号**
# （``want=1, tag='a b'``），所以必须整段锚定 ``SUBFAILED(...)`` 再取紧跟其后的
# node（``.*?`` 非贪婪：优先取第一个能让整行匹配的 ``)``，从而不会吃进理由里的括号），
# 并像 FAILED 分支一样容许 `` - <理由>`` 尾巴（nodeid 本身也可含空格，理由交给
# :func:`_dash_cuts` 的候选消歧）。
_SUBFAILED = re.compile(r"^SUBFAILED\(.*?\)\s+(.*)$")
# pytest 自己写的红行汇总段表头（整行 ``===… short test summary info ===…``）。
# 只有这一段之后的 ``FAILED``/``SUBFAILED`` 行才算红 id：测试/setup 的 ``print``
# 落在 captured-output 段，**在**表头之前。**但这条不是结构性保证**（r5 的 R1）：
# ``atexit`` 钩子在 pytest 打印完真 summary 之后才跑，可以再打一个同样的表头 + 伪造
# ``FAILED`` 行，从而清掉真 id。故 stdout 红 id **必须**再由 junit XML 的 ``<testcase>``
# 佐证（见 :func:`justify_red`），否则记 error。
_SHORT_SUMMARY = re.compile(r"^=+ short test summary info =+$")

# XML 写入时间的容许偏差（秒）：XML 由 pytest 在 sessionfinish 写、进程退出前落盘，
# 因此 mtime 应落在 ``[t0-slack, t1+slack]``。这只挡「复用/陈旧文件」，**不挡进程内伪造**。
WINDOW_SLACK = 5.0

# 第二信道：pytest 的机读报告 ``--report-log``（JSON-lines）。本环境 pytest 9.1.1 核心
# **不含** pytest-reportlog（venv 无 pip，无法安装），故 replayer 运行时把一个等价的最小
# 插件写进同一个私有临时目录，并用 ``-p <name>`` 注入；它注册同名 ``--report-log`` 选项，
# 若环境已装原生 pytest-reportlog（选项名冲突）则让位给原生插件。两条信道的判读代码共用。
_REPORTLOG_PLUGIN_NAME = "_plaita_replay_reportlog"
_REPORTLOG_PLUGIN_SOURCE = '''\
"""replayer 自带的最小 pytest 报告插件：把 report 对象写成 --report-log 的 JSON-lines。

仅在**没有** pytest-reportlog 的环境生效（``--report-log`` 选项名冲突时放弃注册，让位给
原生插件）。格式与 pytest-reportlog 兼容（``$report_type`` / ``nodeid`` / ``when`` /
``outcome``）。与 junit XML 一样，它由 pytest 的 report 对象派生：**独立，但不防进程内伪造**
（变异代码可从 ``sys.argv`` 读到路径并覆写）。
"""
import json


class _Writer:
    def __init__(self):
        self.fh = None

    def open(self, path):
        self.fh = open(path, "a", buffering=1, encoding="utf-8")

    def emit(self, record):
        if self.fh is not None:
            self.fh.write(json.dumps(record, ensure_ascii=False) + "\\n")
            self.fh.flush()


_WRITER = _Writer()


def pytest_addoption(parser):
    try:
        parser.addoption("--report-log", action="store", dest="replay_report_log", default=None,
                         help="replayer 自带：无 pytest-reportlog 时的等价机读报告")
    except Exception:  # 选项名已被原生 pytest-reportlog 占用 ⇒ 让位
        pass


def pytest_configure(config):
    try:
        path = config.getoption("replay_report_log")
    except Exception:
        path = None
    if path:
        _WRITER.open(path)


def pytest_runtest_logreport(report):
    _WRITER.emit({"$report_type": "TestReport", "nodeid": report.nodeid,
                  "when": report.when, "outcome": report.outcome})


def pytest_collectreport(report):
    _WRITER.emit({"$report_type": "CollectReport", "nodeid": report.nodeid,
                  "outcome": report.outcome})


def pytest_sessionfinish(session, exitstatus):
    _WRITER.emit({"$report_type": "SessionFinish", "exitstatus": exitstatus})
'''


def _load_json_store(store: Path):
    specs = []
    for path in sorted((store / "mutations").glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        specs.append({
            "name": raw["name"],
            "issue": raw.get("issue", ""),
            "concern": raw.get("concern", ""),
            "edits": [(raw["file"], raw["old"], raw["new"])],
            "expect_fail": list(raw.get("expect_fail") or ()),
            "note": raw.get("rationale") or raw.get("note") or "",
            "expect_survive": bool(raw.get("expect_survive")),
            "source": f"json:{path.name}",
        })
    return specs


def _load_python_store(store: Path):
    module_path = store / "mutations.py"
    if not module_path.exists():
        return []
    spec = importlib.util.spec_from_file_location("plaita_mutation_store", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["plaita_mutation_store"] = module
    spec.loader.exec_module(module)
    default_tests = tuple(getattr(module, "DEFAULT_TESTS", ()) or ())
    specs = []
    for name, entry in (getattr(module, "MUTATIONS", {}) or {}).items():
        edits = [tuple(edit) for edit in entry.get("edits", ())]
        tests = tuple(entry.get("tests") or default_tests)
        specs.append({
            "name": name,
            "issue": str(entry.get("issue", "")),
            "concern": str(entry.get("concern", "")),
            "edits": edits,
            "expect_fail": list(tests),
            "note": entry.get("note", ""),
            "expect_survive": bool(entry.get("expect_survive")),
            "source": f"python:{module_path.name}",
        })
    return specs


def load_mutations(store: Path):
    """JSON store ∪ ``mutations.py`` store；同名冲突显式报错（绝不静默取其一）。"""
    json_specs = _load_json_store(store)
    python_specs = _load_python_store(store)
    by_name = {}
    for spec in json_specs + python_specs:
        existing = by_name.get(spec["name"])
        if existing is not None:
            raise SystemExit(
                f"同名变异冲突：{spec['name']!r} 同时来自 {existing['source']} 与 "
                f"{spec['source']}；统一 store 拒绝静默覆盖——请改名或删除其一"
            )
        by_name[spec["name"]] = spec
    return [by_name[name] for name in sorted(by_name)]


def kill_process_group(proc):
    """整组终止（POSIX 尽力而为）：``start_new_session=True`` 后 ``proc.pid`` 即组长。

    ``subprocess.run(timeout=…)`` 只 kill 直接子进程，fixture 里 ``Popen`` 出来的孙进程会
    活成孤儿（``ppid=1``）。故这里对**进程组**发 ``SIGKILL``，失败再退回 ``proc.kill()``。

    **限度（r6 R6，已知、不解决、不许再声称已解决）**：``killpg`` 只能杀**同一会话/进程组**
    里的进程。孙进程若自己调 ``os.setsid()`` 就另起会话，``killpg`` 够不到它，它会以
    ``ppid=1`` 活下来。README §1.4 已按此更正（r5 的「grandchildren never become orphans」
    在 setsid 孙进程面前为假）。
    """
    if os.name == "posix":
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            return True
        except (ProcessLookupError, PermissionError, OSError):
            pass
    try:
        proc.kill()
    except OSError:
        pass
    return False


# 单份机读产物的读取上限（默认 64 MiB）：正常 junit XML / report-log 远小于此，
# 超过即视为不可信（返回 None ⇒ 调用方记 error），避免把内存读爆。
MAX_REPORT_BYTES = 64 * 1024 * 1024


def read_regular_bytes(path, limit=MAX_REPORT_BYTES):
    """读**普通文件**的字节；非普通文件（FIFO/设备/目录/符号链接指向的非普通文件）或读不完
    ⇒ ``None``（任何异常都不许逃出）。

    r6 R5：``Path.read_bytes()`` 打开 FIFO 时若无写端会**永远阻塞**，父进程随即挂死；
    ``--timeout`` 只约束 pytest 子进程，管不到父进程的这次读（verifier 的端到端 ``--all``
    20s 不返回、要 SIGKILL）。故先 ``os.open(..., O_NONBLOCK)``（对 FIFO 立即返回，不等写端），
    再 ``fstat`` 确认 ``stat.S_ISREG``，之后按块读并给总量封顶。
    """
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
    except OSError:
        return None
    try:
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                return None
        except OSError:
            return None
        chunks = []
        total = 0
        while True:
            try:
                chunk = os.read(fd, 65536)
            except (BlockingIOError, InterruptedError):
                return None
            except OSError:
                return None
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                return None
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def run(cmd, cwd, timeout=None, extra_path=None):
    """跑子进程；``timeout`` 秒内没结束 ⇒ **整组**终止并返回哨兵 rc ``TIMEOUT_RC``（124）。

    超时**不是** pytest 的判定：调用方据此记 error（``pytest-timeout``），绝不判死。
    子进程以 ``start_new_session=True`` 起（新进程组/会话），超时时 ``os.killpg`` 整组杀，
    把 fixture 里 ``Popen`` 出来、**未自起会话**的孙进程一并带走；``os.setsid()`` 自起会话的
    孙进程 ``killpg`` 够不到（限度见 :func:`kill_process_group`）。``extra_path`` 追加到
    ``PYTHONPATH``（第二信道插件目录）。
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [piece for piece in (str(extra_path) if extra_path else None, str(cwd)) if piece])
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    kwargs = {}
    if os.name == "posix":
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(cmd, cwd=str(cwd), env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, **kwargs)
    try:
        out, err = proc.communicate(timeout=timeout)
        return subprocess.CompletedProcess(cmd, proc.returncode, out, err)
    except subprocess.TimeoutExpired:
        kill_process_group(proc)
        try:
            out, err = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
        return subprocess.CompletedProcess(
            cmd, TIMEOUT_RC,
            f"<replay 超时：单次 pytest 超过 {timeout:g}s 未结束，已终止整组（非 pytest 判定）>\n",
            "")


def export_tree(dest: Path, source: str = "worktree"):
    """导出干净树：``head`` 用 ``git archive HEAD``；``worktree`` 再叠加工作区改动。"""
    dest.mkdir(parents=True, exist_ok=True)
    tar = subprocess.run(["git", "-C", str(ROOT), "archive", "HEAD"], capture_output=True)
    if tar.returncode != 0:
        raise SystemExit(f"git archive HEAD 失败: {tar.stderr.decode()}")
    extract = subprocess.run(["tar", "-x", "-C", str(dest)], input=tar.stdout, capture_output=True)
    if extract.returncode != 0:
        raise SystemExit(f"解包失败: {extract.stderr.decode()}")
    if source != "worktree":
        return
    status = subprocess.run(
        ["git", "-C", str(ROOT), "status", "--porcelain", "-z"], capture_output=True
    )
    for entry in status.stdout.decode().split("\0"):
        if not entry:
            continue
        code, path = entry[:2], entry[3:]
        src, dst = ROOT / path, dest / path
        if code.strip() == "D":
            if dst.exists():
                dst.unlink()
            continue
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        elif src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def assert_import_is_tree(tree: Path, python: str):
    """硬隔离探针：``plaita.__file__`` 非 None **且** realpath 落在目标树内。

    venv 的 editable install 指向主 checkout；只有 ``cwd=<tree>``（``sys.path[0]=''``）
    才能盖住它。探针把解析到的路径打出来当证据，不通过就**拒绝继续**——绝不在
    错误的树上跑变异（那会把「别的 checkout 的绿」当成自己的基线）。
    """
    tree_real = os.path.realpath(str(tree))
    code = (
        "import os, sys\n"
        "import plaita\n"
        "f = getattr(plaita, '__file__', None)\n"
        f"t = os.path.realpath({tree_real!r})\n"
        "if f is None:\n"
        "    print('<None>')\n"
        "    sys.exit(3)\n"
        "p = os.path.realpath(f)\n"
        "print(p)\n"
        "sys.exit(0 if p.startswith(t + os.sep) else 1)\n"
    )
    res = subprocess.run([python, "-c", code], cwd=str(tree), capture_output=True, text=True,
                         env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    resolved = (res.stdout or "").strip()
    if res.returncode != 0:
        raise SystemExit(
            f"导入隔离失败：plaita.__file__={resolved!r}（rc={res.returncode}）"
            f"未落在 {tree}；venv editable 指向别的 checkout？"
        )
    print(f"隔离探针: plaita.__file__ = {resolved}  ⊂ {tree}")


def pytest_cmd(python: str, nodes, junit=None, reportlog=None):
    """pytest 命令；``junit``/``reportlog`` 给出时追加两条**机读**信道（判死依据）。

    ``reportlog`` 需要把 replayer 自带的插件目录加进 ``PYTHONPATH`` 并用 ``-p`` 注入
    （见 :func:`pytest_report_dir` 与 :func:`run` 的 ``extra_path``）。
    """
    cmd = [python, "-m", "pytest", *nodes, "-q", "-p", "no:cacheprovider"]
    if reportlog is not None:
        cmd += ["-p", _REPORTLOG_PLUGIN_NAME, f"--report-log={reportlog}"]
    if junit is not None:
        cmd.append(f"--junit-xml={junit}")
    return cmd


@contextlib.contextmanager
def pytest_report_dir():
    """per-mutation 的机读报告目录（``tempfile`` 默认落仓外；mode 0700；用完即删）。

    yield ``{"dir", "junit", "reportlog"}``。目录每次新建（随机路径），**绝不复用上一次
    的产物**：缺失即 error。第二信道插件源码也写在这里（不落仓、随目录删除）。
    """
    dirname = Path(tempfile.mkdtemp(prefix="plaita-reports-"))
    (dirname / f"{_REPORTLOG_PLUGIN_NAME}.py").write_text(_REPORTLOG_PLUGIN_SOURCE,
                                                         encoding="utf-8")
    try:
        yield {"dir": dirname, "junit": dirname / "report.xml",
               "reportlog": dirname / "report.jsonl"}
    finally:
        shutil.rmtree(dirname, ignore_errors=True)


def _testcase_identity(testcase) -> str | None:
    """junit ``<testcase classname=… name=…>`` 的扁平身份（``classname.name``）。

    pytest 的 ``classname`` 已是点分 rootdir-relative 路径（``tests.unit.test_x.TestY``），
    与 ``--report-log`` 的 nodeid 用 :func:`flat_identity` 归一到同一形状后可比较。
    """
    classname = (testcase.get("classname") or "").strip()
    name = (testcase.get("name") or "").strip()
    if classname and name:
        return f"{classname}.{name}"
    return classname or name or None


def _param_region_start(text: str) -> int:
    """末尾 param id 区域的起点（区域内最左 ``[`` 的下标），无则 ``-1``。

    pytest 的 nodeid 形状是 ``路径::[类::]用例[param id]``，param id 是**末尾**那段方括号，
    且 pytest **不转义** param id 里的 ``]``/``[``：

    - ``parametrize(..., ['a]::b'])`` → ``tests/unit/test_vparams.py::test_real[a]::b]``
      （XML 身份同形 ``…test_vparams.test_real[a]::b]``）；
    - ``parametrize(..., [']::['])`` → ``…::test_real[]::[]``。

    只数括号深度的扫描在第一个 ``]`` 处就把 depth 归零，紧随其后的 ``::`` 于是被误判成顶层
    分隔符（r6/r7 都有这个缺陷）。这里的规则分两步：

    1. 只有**以 ``]`` 结尾**的串才认为带 param id；从串尾**自右向左**配对方括号（``]`` 加
       深度、``[`` 减深度），取**把深度降到 0 的那个 ``[``** 作起点（组内 ``]`` 比 ``[``
       多时也成立）——这修掉 ``[a]::b]``；
    2. 若起点之前紧接 ``::``（``]::[`` 形状：``::`` 两侧都是 param id 的片段），继续向左
       吸收到前一个 ``[``，直到 head 不再以 ``::`` 结尾——这修掉 ``[]::[]``。
       （head＝路径/类/用例名，pytest 的类名/用例名不可能含 ``[``，故 ``::`` 后紧跟 ``[``
       只可能是 param id 的延续。）

    **已知偏离（r9 更正，让文档与代码一致）**：第 1 步**不是**「取末尾方括号组最左的 ``[``」，
    而是「取把深度降到 0 的 ``[``」。当**全串** ``]`` 多于 ``[``、右向扫描到串首仍不归零时，
    ``for…else`` 兜底取的是**全串最左的 ``[``**——若更靠左还有别的 ``[``（例如路径里的
    ``x[1]``），起点会落到那个 ``[`` 上，``_split_nodeid`` 于是返回**1 段**（整串
    ``['tests/unit/x[1]/test_x.py::TestY::test_z[a]::b]']``），
    而「末尾方括号组最左的 ``[``」这条字面规则会给出 3 段。
    这**不是新的假拒绝**：r7 在同一形状上切成 4 段、同样归一不出 XML 身份（两侧都错），
    而且该形状在 ``expect_fail`` 里**不可达**——pytest 直接拒绝含 ``[]`` 的路径选择
    （``ERROR: path cannot contain [] parametrization``、rc=4）。故 r9 只更正文档、
    **不动**判死逻辑（夹具 ``test_r9_param_region_fallback_takes_the_leftmost_bracket``
    把真实行为钉住）。

    不以 ``]`` 结尾时返回 ``-1``（退回深度扫描）。
    """
    if not text.endswith("]"):
        return -1
    depth, leftmost = 0, -1
    for index in range(len(text) - 1, -1, -1):
        char = text[index]
        if char == "]":
            depth += 1
        elif char == "[":
            leftmost = index
            depth -= 1
            if depth <= 0:
                start = index
                break
    else:
        start = leftmost
    while start >= 2 and text[start - 2:start] == "::":
        previous = text.rfind("[", 0, start - 2)
        if previous < 0:
            break
        start = previous
    return start


def _split_nodeid(text: str):
    """按 ``::`` 切 nodeid，但**不切 param id 内部的** ``::``。

    ``test_real[a::b]`` / ``test_real[a[b]]`` / ``test_real[a]::b]`` / ``test_real[]::[]``
    这类 param id 里的 ``::`` 不是 pytest 的 nodeid 分隔符；r6 的 ``text.split("::")`` 会把
    它们切开，于是两条信道给出的**字面相同**的身份被归一成不同串（r7 R2 / r8 R1 的假
    ``junit-reportlog-mismatch``）。

    **规则（r8 修正）**：末尾 param id 区域（:func:`_param_region_start`，含 ``[a]::b]``
    的「多余 ``]``」与 ``[]::[]`` 的「``::`` 两侧都是 param 片段」两种吸收）整段视作一段；
    其余部分才用「方括号外才算分隔符」的深度扫描切分。只数括号深度**不够**——pytest 不转义
    param id 里的 ``]``/``[``，深度扫描不能单独作为规则（它只是「区域之外」那半段的实现）。
    """
    start = _param_region_start(text)
    head = text if start < 0 else text[:start]
    parts, buf, depth, index = [], [], 0, 0
    while index < len(head):
        char = head[index]
        if char == "[":
            depth += 1
        elif char == "]" and depth:
            depth -= 1
        if not depth and head.startswith("::", index):
            parts.append("".join(buf))
            buf = []
            index += 2
            continue
        buf.append(char)
        index += 1
    parts.append("".join(buf))
    if start >= 0:
        parts[-1] += text[start:]
    return parts


def flat_identity(node: str) -> str:
    """把 nodeid / testcase 身份归一成 ``点分模块[.类…].用例名`` 的扁平串。

    对 **pytest 产出的 nodeid/身份**本归一是**幂等**的（r7 R2，r8 限定范围）：

    - ``tests/unit/test_x.py::TestY::test_z`` → ``tests.unit.test_x.TestY.test_z``
    - ``TestY::test_z``（``FAILED`` 分支截断后的短形）→ ``TestY.test_z``
    - XML ``classname=tests.unit.test_x.TestY`` + ``name=test_z`` → 同一串
    - **没有** ``::`` 的输入视为**已经是扁平身份**，原样返回（不再把 ``/``/``\\`` 归一）：
      否则 ``flat_identity('tests.unit.test_p.test_real[a/b]')`` 会把 param id 里的 ``/``
      再变成 ``.``，``flat_identity(flat_identity(x)) != flat_identity(x)``，于是
      ``justify_red(x, [x]) == False``（r7 R2：两条信道**字面相同**却被判
      ``junit-reportlog-mismatch`` / ``red-not-in-junit``）。带 ``::`` 的输入只归一
      ``::`` **之前**的 head，而 param id 里的 ``::`` 一律不切（见 :func:`_split_nodeid`），
      故第一次调用后 param id 内的 ``::`` 仍在**末尾方括号区**里、第二次调用原样返回。

    **不承诺对任意字符串幂等（r8 诚实化）**：归一只发生在第一个顶层 ``::`` 之前的 head
    （``/``/``\\``→``.``、``.strip(".")``），末尾 tail 只做 ``strip()``（见函数体
    ``node.strip()``）。对非 pytest 形状的串，第二次调用可能再剥一层：``':: \\a'``
    第一次得 ``' \\a'``、第二次得 ``'\\a'``（前导空格被 strip）；``' /:: / '`` 第一次
    得 ``' /'``、第二次得 ``'/'``。r8 的 fuzz（20000 个随机串、seed 20261014）实测
    **246/20000 个原始串、去重后 229/13634 个去重串**不幂等（r8 文档把它混写成
    「246/13634」：246 是原始串口径、13634 是去重后口径；r9 复算两数均不变），全部是这两类
    残差；而 pytest 真实 nodeid 上（r9 复算：一次真 pytest 产出 8000 条 nodeid + 8000 条
    XML 身份）非幂等为 **0/8000**。幂等只被用在「两条信道字面相同的身份必须
    比较相等」（``justify_red(x,[x])``）这一处，输入恒为 pytest 身份，故更窄的承诺足够。
    """
    text = node.strip()
    if not text:
        return ""
    parts = _split_nodeid(text)
    if len(parts) == 1:
        return text
    head = parts[0]
    if head.endswith(".py"):
        head = head[:-3]
    head = head.replace("/", ".").replace("\\", ".").strip(".")
    tail = ".".join(piece for piece in parts[1:] if piece)
    if not tail:
        return head
    return f"{head}.{tail}" if head else tail


def red_id_matches_identity(flat: str, identity: str) -> bool:
    """**r6 R1 契约**：以 XML ``<testcase>`` 身份为**权威**，stdout 候选只准「等值或点分截断」。

    - ``flat == identity`` ⇒ ``True``：``SUBFAILED(<上下文>) <完整 nodeid>`` 与 XML 的
      ``classname.name`` 同形，以及 ``--report-log`` 的 nodeid 交叉比对都走这条；
    - ``identity.endswith("." + flat)`` ⇒ ``True``：``FAILED`` 分支被截断后的短形
      （stdout ``TestY::test_z`` ↔ XML ``tests.unit.test_x.TestY.test_z``）是 XML 身份的
      **点分截断**，合法；
    - **反向（stdout 是 XML 身份的扩展）一律 ``False``**：``X.<真身份>``、
      ``<anything>::test_real``、``::test_real`` 这类更长的伪造 id **不得**因为「真身份恰好
      是它的点分后缀」而被佐证。r5 的双向后缀匹配正是在这里放过了 verifier 的
      ``SUBFAILED(i=1) tests/nonexistent/zzz/test_x.py::test_real``（XML 真身份
      ``test_x.test_real`` 是它的点分后缀，于是伪造 id 被判死并写进记录）。

    空串（任一参数）一律 ``False``。本函数是**单向**的，且是这一关系的**唯一**实现处：
    :func:`justify_red` 与证据旁证的 ``red_unjustified`` 都经它。
    """
    if not flat or not identity:
        return False
    return flat == identity or identity.endswith("." + flat)


def justify_red(red_id: str, xml_identities) -> bool:
    """stdout 红 id 是否被某个 XML ``<testcase>`` 佐证（**等值或点分截断，单向**）。

    容忍面（r5 R1 + r6 R1）：完整 nodeid、``classname::name``、``FAILED`` 分支截断后的短
    ``Class::test``、以及 ``SUBFAILED(<上下文>) <node>`` 的完整 nodeid——都归一成
    :func:`flat_identity` 后由 :func:`red_id_matches_identity` 判定：``test_y`` 被
    ``tests.unit.test_x.test_y`` 佐证（截断），但 ``tests.nonexistent.zzz.test_x.test_real``
    **不**被 ``test_x.test_real`` 佐证（扩展）。任何一条无佐证 ⇒ 调用方记 error，**永不判死**。
    """
    flat = flat_identity(red_id)
    if not flat:
        return False
    if red_id.strip().startswith("::"):
        # r6 R1：``::test_real`` 的模块/类段是空的——pytest 的合法红行形状只有
        # ``路径::Class::test``（完整 nodeid）、``Class::test``（FAILED 短形）与裸 ``test``
        # （模块级用例的 FAILED 短形）；空 head 归一后虽然也是 ``test_real``，但它是畸形 id，
        # 不接受（否则「扩展成任意前缀 + ::真名」的伪造就能落回截断分支）。
        return False
    return any(red_id_matches_identity(flat, candidate)
               for candidate in xml_identities if candidate)


def _totals_consistent(root, failures: int, errors: int):
    """``<testsuite>``/根元素自报的 ``failures``/``errors`` 是否与元素数自洽。

    返回 ``True``/``False``；完全没有自报计数时返回 ``None``（未知，不据此判死）。
    pytest 的 xunit XML 每个 ``<testsuite>`` 都带这两个属性；subTest 会让 ``tests`` 与
    ``<testcase>`` 个数不等（故**只查 failures/errors**，不查 tests）。
    """
    checked = False
    for suite in root.iter("testsuite"):
        for attr, counted in (("failures", sum(1 for e in suite.iter() if e.tag == "failure")),
                              ("errors", sum(1 for e in suite.iter() if e.tag == "error"))):
            declared = suite.get(attr)
            if declared is None:
                continue
            checked = True
            try:
                if int(declared) != counted:
                    return False
            except ValueError:
                return False
    for attr, counted in (("failures", failures), ("errors", errors)):
        declared = root.get(attr)
        if declared is not None:
            checked = True
            try:
                if int(declared) != counted:
                    return False
            except ValueError:
                return False
    return True if checked else None


def read_junit(path):
    """读 junit XML，返回 ``{"failures", "errors", "testcases", "bad", "totals_consistent"}``；
    缺失/不可解析/非 junit/任何异常 ⇒ ``None``。

    ``None`` 的含义是「拿不到权威判死信道」⇒ 调用方必须记 error，**绝不判死**。
    计数用 ``root.iter()`` 递归找 ``<failure>`` / ``<error>``：pytest 把 test 失败写成
    ``<failure>``、setup/teardown/收集错误写成 ``<error>``（pytest 9 的 subTest 失败
    也**每个失败各产一个** ``<failure>``，与 ``SUBFAILED`` 行一一对应，同一 ``<testcase>``
    下可有多个）。

    **任何异常都不许逃出本函数（r5 R2a）**：``<?xml … encoding="bogus"?>`` 会让
    ``ET.fromstring`` 抛 ``LookupError``（不是 ``ET.ParseError``），r4 只捕 ``OSError`` /
    ``ET.ParseError``，于是整个 ``--all`` 直接 traceback 且一条记录都不写。这里显式列出
    ``OSError/ET.ParseError/LookupError/ValueError/UnicodeDecodeError``，再兜底 ``Exception``。

    **非普通文件先拒（r6 R5）**：读走 :func:`read_regular_bytes`——junit 路径被放上 FIFO 时
    ``Path.read_bytes()`` 会永远阻塞（verifier 的端到端 ``--all`` 20s 不返回、要 SIGKILL），
    现在 FIFO/设备/目录一律立即返回 ``None`` ⇒ 调用方记 error，父进程不再挂死。
    """
    raw = read_regular_bytes(path)
    if raw is None:
        return None
    try:
        root = ET.fromstring(raw)
    except (OSError, ET.ParseError, LookupError, ValueError, UnicodeDecodeError):
        return None
    except Exception:  # 兜底：ET 的任何实现细节异常都不许中断 --all
        return None
    if root.tag not in ("testsuites", "testsuite"):
        return None
    try:
        failures = sum(1 for element in root.iter() if element.tag == "failure")
        errors = sum(1 for element in root.iter() if element.tag == "error")
        bad = [identity for identity in
               (_testcase_identity(tc) for tc in root.iter("testcase")
                if any(child.tag in ("failure", "error") for child in tc))
               if identity]
        return {
            "failures": failures,
            "errors": errors,
            "testcases": sum(1 for _ in root.iter("testcase")),
            "bad": bad,
            "totals_consistent": _totals_consistent(root, failures, errors),
        }
    except Exception:
        return None


def read_report_log(path):
    """读 ``--report-log``（JSON-lines）并统计失败；缺失/损坏 ⇒ ``None``。

    失败口径与 junit XML 对齐：``when == "call"`` 的 ``TestReport(failed)`` ↔ XML
    ``<failure>``；``setup``/``teardown`` 的失败与 ``CollectReport(failed)`` ↔ XML ``<error>``。
    给出失败 nodeid 集合（:func:`flat_identity` 归一）用于跨信道 nodeid 比对。

    与 :func:`read_junit` 同样先拒非普通文件/超大文件（r6 R5）：report-log 路径若被放到
    FIFO 上，父进程同样不得阻塞。
    """
    data = read_regular_bytes(path)
    if data is None:
        return None
    text = data.decode("utf-8", errors="replace")
    records = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            return None
        if isinstance(rec, dict):
            records.append(rec)
    call_failed = setup_failed = teardown_failed = collect_failed = other_failed = 0
    nodeids = set()
    for rec in records:
        report_type, outcome = rec.get("$report_type"), rec.get("outcome")
        if outcome != "failed":
            continue
        if report_type == "TestReport":
            when = rec.get("when")
            if when == "call":
                call_failed += 1
            elif when == "setup":
                setup_failed += 1
            elif when == "teardown":
                teardown_failed += 1
            else:
                other_failed += 1
            nodeid = rec.get("nodeid")
            if isinstance(nodeid, str) and nodeid:
                nodeids.add(flat_identity(nodeid))
        elif report_type == "CollectReport":
            collect_failed += 1
    return {
        "call_failed": call_failed,
        "error_failed": setup_failed + teardown_failed + collect_failed + other_failed,
        "setup_failed": setup_failed,
        "teardown_failed": teardown_failed,
        "collect_failed": collect_failed,
        "other_failed": other_failed,
        "nodeids": sorted(nodeids),
        "records": len(records),
    }


def _nodeids_agree(xml_identities, log_nodeids) -> bool:
    """两条信道的失败 nodeid 集合是否互相佐证（点分后缀容忍匹配）。"""
    for identity in xml_identities:
        if not any(justify_red(identity, [nodeid]) for nodeid in log_nodeids):
            return False
    for nodeid in log_nodeids:
        if not justify_red(nodeid, xml_identities):
            return False
    return True


def _dash_cuts(text: str):
    """一行红行正文里 ``" - "`` 的**全部**切点（升序）＋整段长度（保序、含整段）。

    pytest 的红行是 ``FAILED {nodeid} - {reprcrash.message}``
    （``_pytest.terminal._get_line_with_reprcrash_message``：先 ``f"{word} {node}"``，再追加
    ``" - " + msg``）。切点**两侧都可能含** ``" - "``：param id 里可以含
    （``test_real[a - b]``），``reprcrash.message`` 里也可以含（``assert 5 - 2 == 4``）。故
    既不能只按第一个切、也不能只按最后一个切：这里把**每个**切点都作为候选，由调用方用
    junit XML 的权威身份（:func:`justify_red`）挑出真正被佐证的那个。升序保证「node 里没有
    ``" - "``（绝大多数）」时首个候选与 r5/r6 的 legacy 形状**逐字节一致**——三条真实记录
    的 sha256 是本补丁的锚（README §5.2/§8）。
    """
    cuts = []
    index = text.find(" - ")
    while index != -1:
        cuts.append(index)
        index = text.find(" - ", index + 1)
    cuts.append(len(text))
    return cuts


def failed_candidates(output: str):
    """每个红行 → 该行的**候选 nodeid 列表**（保序；首个 = legacy 形状）。

    与 :func:`failed_nodes` 的区别：保留每个 ``" - "`` 切点给出的候选，让
    :func:`judge_mutation` 用 XML 真身份消歧（nodeid 与理由里都可能含 ``" - "``）。
    ``FAILED`` 分支沿 r5/r6 的 legacy 形状（取**首个** ``::`` 之后的片段，记录形状锚见
    README §8），``SUBFAILED`` 分支留完整 nodeid；两者都按 :func:`_dash_cuts` 逐切点展开。
    """
    out = []
    lines = output.splitlines()
    start = 0
    for index, line in enumerate(lines):
        if _SHORT_SUMMARY.match(line.strip()):
            start = index + 1
    for line in lines[start:]:
        stripped = line.strip()
        bucket = []
        match = _FAILED.match(stripped)
        if match:
            body = match.group(1)
            for cut in _dash_cuts(body):
                node = body[:cut].strip()
                candidate = node.split("::", 1)[1] if "::" in node else node
                if candidate and candidate not in bucket:
                    bucket.append(candidate)
        else:
            match = _SUBFAILED.match(stripped)
            if match:
                body = match.group(1)
                for cut in _dash_cuts(body):
                    candidate = body[:cut].strip()
                    if candidate and candidate not in bucket:
                        bucket.append(candidate)
        if bucket:
            out.append(bucket)
    return out


def failed_nodes(output: str):
    """红用例 id 的**首选候选**（每行一条，legacy 形状，供记录/证据展示）。

    只在 ``short test summary info`` 段内找 ``FAILED``/``SUBFAILED`` 行；``unittest``
    subTest 失败只打 ``SUBFAILED(<上下文>) <node>`` 行（pytest 不产出 FAILED 行）——两者
    都要收进记录。

    为什么取**最后一次**表头之后：测试在 captured-output 段里 ``print`` 的
    ``FAILED tests/fake/path::test_pretend`` 行在真表头**之前**，会被排除。

    **但这不是结构性保证（r5 R1 更正）**：``atexit`` 钩子在 pytest 打印完真 summary 之后
    才运行，可以从容再打一个同样的表头 + 伪造 ``FAILED`` 行，于是「最后一次表头」反而是
    伪造段，真 id 被挤掉。r4 的 README 曾声称「伪造行仍在真表头之前」，**该说法为假**。
    现在的处置：stdout 只提供**候选**红 id，每条都必须由 junit XML 的 ``<testcase>``
    佐证（:func:`justify_red`，逐行取**首个被佐证**的候选，见 :func:`failed_candidates`），
    否则 :func:`judge_mutation` 记 ``error_kind="red-not-in-junit"``，**永不判死**。即伪造
    能污染「展示」，但不能污染「判决」。

    ``SUBFAILED`` 的上下文可以含空格（``want=1, tag='a b'``），因此**不能**按第一个空格切；
    必须从行首的 ``SUBFAILED(...)`` 整段锚定后取紧跟其后的 node，理由/param id 里的空格与
    ``" - "`` 由候选消歧处理（r7 R1）。返回 ``sorted(set(…))``，故同名 ``Class::test``
    跨文件会**塌缩**成一条（已声明，见 README §8）。
    """
    return sorted({candidates[0] for candidates in failed_candidates(output) if candidates})


def check_anchors(spec, tree: Path):
    """每条 ``old`` 在目标树里必须恰好命中一次；返回问题清单。"""
    problems = []
    for relative, old, _new in spec["edits"]:
        target = tree / relative
        if not target.exists():
            problems.append(f"{relative}: 文件不存在")
            continue
        count = target.read_text(encoding="utf-8").count(old)
        if count != 1:
            problems.append(f"{relative}: old 命中 {count} 次（应为 1）")
    return problems


def summarize_error(output: str) -> str:
    """给 error 记录摘一条最能说明问题的 pytest 行（收集中断 / 节点 ERROR / 兜底末行）。"""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for marker in ("Interrupted:", "errors during collection", "error during collection"):
        for line in reversed(lines):
            if marker in line:
                return line
    for prefix in ("ERROR ", "E   ", "FAILED "):
        for line in reversed(lines):
            if line.startswith(prefix):
                return line
    return lines[-1] if lines else ""


def pytest_missing(proc) -> bool:
    """解释器里根本没有 pytest（``python -m pytest`` 的 ``No module named pytest``）。

    这是「根本跑不起来」，必须记 error（kind ``pytest-unavailable``），**不是**存活。
    """
    blob = (proc.stdout or "") + "\n" + (proc.stderr or "")
    return "No module named pytest" in blob or "No module named 'pytest'" in blob


def judge_mutation(spec, mutated, junit_path, reportlog_path, window=None, timeout=None):
    """判死只看 **rc + pytest 自己的两条机读信道**（junit XML + report-log），绝不看 stdout。

    威胁模型（**必须与 README「威胁模型」一节同读**）：变异体是**我们自己的定义**，不是
    对抗性第三方代码；一条变异可以在 pytest 进程内读到两份产物的路径并伪造它们——那
    **不可在进程内阻止**，属明确的范围之外。本函数保证的是：① 不会误判死；② 两条信道
    不一致 ⇒ error；③ 判决所需的证据（失败计数、report-log 结果、红 id 佐证）写进旁证
    记录，人类可审计而不必「相信」。

    - ``rc == 0`` ⇒ 存活；
    - ``rc == 1``、XML ``<failure>`` ≥ 1、两条信道一致、且 stdout 红 id **全部**有 XML
      ``<testcase>`` 佐证、且至少有一条红 id ⇒ 杀死（id 取 summary 段，展示用）。nodeid 与
      理由的 ``" - "`` 切分**不靠猜**：每个切点各给一个候选，取首个被 XML 佐证的
      （r7 R1 修掉「param id 含空格 ⇒ 假 ``red-unknown``」）；
    - ``rc == 1`` 但 stdout 有**无佐证**红 id（含 XML 身份的**扩展**形）⇒ error
      （``red-not-in-junit``）；
    - ``rc == 1``、XML ``<failure>`` ≥ 1 且带 testcase 身份，但 stdout **一条红 id 都没有**
      ⇒ error（``red-unknown``）——只有 XML 自己不带 testcase 身份（``bad == []``）时才允许
      ``killed: true`` + ``red: []``（``expect_survive`` 的 documented-unobservable 另算）；
    - ``rc == 1`` 但两信道失败计数/nodeid 不一致或 report-log 缺失 ⇒ error
      （``junit-reportlog-mismatch``）；
    - ``rc == 1`` 且 XML ``<failure> == 0`` 且（XML ``<error>`` ≥ 1 或无红 id）⇒ error；
    - ``rc == 1`` 且 XML 自报计数自相矛盾 / XML 写入时间不在运行窗口内 ⇒ error
      （``junit-xml-inconsistent`` / ``junit-xml-outside-run-window``）；
    - ``rc ∈ {0, 1}`` 且 ``expect_survive: true`` ⇒ documented-unobservable，**不判死**
      （``killed: true``、``note`` = ``documented-unobservable``），但**必须 ``red == []``**
      （r7 R3）：任何 stdout 红 id ⇒ error（``expect-survive-with-red``），
      **永不判死**——「声明观测不到」不等于可以带红；
    - ``rc == 124``（本 replayer 的超时哨兵）⇒ error（``pytest-timeout``）；
    - ``rc ∉ {0, 1}`` ⇒ error（收集/导入/内部错误）；
    - XML 缺失/不可解析/非 junit ⇒ error。**任何异常都不会变成 killed。**

    返回 ``(killed, detail, red, error, evidence)``；``error`` 为 ``None`` 表示正常判定，
    否则是 ``{"kind", "rc", "line"}``。``evidence`` 是给旁证文件用的审计证据（不进主记录，
    以保住三条既有记录的 sha256）。**绝不写占位红行**——0 红就是 0 红。
    """
    rc = mutated.returncode
    # r7 R1：每行保留 ``" - "`` 的全部候选（nodeid 与理由里都可能含 ``" - "``），
    # ``red`` 是 legacy 形状（每行首个候选）——证据列与 error 记录的字节锚（README §8）。
    candidates = failed_candidates(mutated.stdout)
    red = sorted({bucket[0] for bucket in candidates if bucket})
    line = summarize_error(mutated.stdout)
    evidence = {
        "rc": rc,
        "red_from_stdout": red,
        "red_unjustified": None,
        "junit": None,
        "report_log": None,
        "cross_channel": None,
        "xml_mtime_in_run_window": None,
        "xml_totals_consistent": None,
        "run_window": list(window) if window else None,
        "red_justify_rule": ("red_id_matches_identity：等值或 XML 身份的点分截断（单向；"
                             "stdout 是 XML 身份的扩展一律不接受）"),
    }
    if rc == TIMEOUT_RC:
        shown = f"{timeout:g}s" if timeout else "阈值"
        error = {"kind": "pytest-timeout", "rc": rc, "line": line or f"超过 {shown} 未结束"}
        return (False, f"单次 pytest 运行超过 {shown} 未结束，已终止整组（error，非判死）",
                red, error, evidence)
    if rc not in (0, 1):
        error = {"kind": "pytest-rc-not-0-1", "rc": rc, "line": line}
        return False, f"pytest 错误退出 rc={rc}（收集/导入/内部错误，非判死）", red, error, evidence
    if spec["expect_survive"]:
        # documented-unobservable：不判死，沿用既有记录口径（killed=True + note），
        # 但**不得携带任何红 id**（r7 R3）：作者可以声明「这条没法被测试观测」，不能拿
        # stdout 的（可伪造的）红行给记录撑门面。任何红 id ⇒ 具名 error、永不判死，于是
        # r5 的「`expect_survive` + 伪造 stdout 红行 ⇒ `killed: true` + `red: [伪造 id]`」
        # 不再可达（渲染器也不会再说「SURVIVED（无红测试）」却带着红）。
        if red:
            evidence["red_unjustified"] = sorted(set(red))
            error = {"kind": "expect-survive-with-red", "rc": rc, "line": line}
            return (False, "documented-unobservable 记录不得携带红 id"
                           "（stdout 红行不可信/伪造，非判死）", [], error, evidence)
        return True, "documented-unobservable", [], None, evidence
    if rc == 0:
        return False, "变异存活（expect_fail 全绿）", [], None, evidence

    # rc == 1：判死信道 = junit XML（第一条）+ report-log（第二条，交叉确认）
    verdict = read_junit(junit_path)
    if verdict is None:
        error = {"kind": "junit-xml-missing-or-unparsable", "rc": rc, "line": line}
        return False, "pytest rc=1 但 --junit-xml 报告缺失/不可解析（非判死）", red, error, evidence
    failures, errors = verdict["failures"], verdict["errors"]
    evidence["junit"] = {
        "failures": failures,
        "errors": errors,
        "testcases": verdict["testcases"],
        "bad_testcases": verdict["bad"],
    }
    evidence["xml_totals_consistent"] = verdict["totals_consistent"]

    # 最小缓解：XML 必须是**本次运行窗口内**写的（挡复用/陈旧文件；不挡进程内伪造）
    in_window = None
    if window and window[0] is not None:
        try:
            mtime = os.stat(junit_path).st_mtime
            in_window = (window[0] - WINDOW_SLACK) <= mtime <= (window[1] + WINDOW_SLACK)
            evidence["xml_mtime"] = mtime
            evidence["xml_mtime_delta"] = [mtime - window[0], mtime - window[1]]
        except OSError:
            in_window = False
    evidence["xml_mtime_in_run_window"] = in_window
    if in_window is False:
        error = {"kind": "junit-xml-outside-run-window", "rc": rc, "line": line}
        return (False, "pytest rc=1 但 junit XML 的写入时间不在本次运行窗口内"
                       "（陈旧/复用文件，非判死）", red, error, evidence)
    if verdict["totals_consistent"] is False:
        error = {"kind": "junit-xml-inconsistent", "rc": rc, "line": line}
        return (False, "pytest rc=1 但 junit XML 的 <testsuite> 自报计数与元素数自相矛盾"
                       "（非判死）", red, error, evidence)

    log = read_report_log(reportlog_path)
    if log is None:
        evidence["cross_channel"] = {"agree": False, "reason": "report-log 缺失/不可解析"}
        error = {"kind": "junit-reportlog-mismatch", "rc": rc, "line": line}
        return (False, "pytest rc=1 但第二信道 --report-log 缺失/不可解析"
                       "（无法交叉确认，非判死）", red, error, evidence)
    evidence["report_log"] = {key: log[key] for key in
                              ("call_failed", "error_failed", "setup_failed", "teardown_failed",
                               "collect_failed", "other_failed", "records")}
    evidence["report_log"]["failed_nodeids"] = log["nodeids"]
    counts_agree = log["call_failed"] == failures and log["error_failed"] == errors
    nodeids_agree = _nodeids_agree(verdict["bad"], log["nodeids"])
    evidence["cross_channel"] = {
        "agree": bool(counts_agree and nodeids_agree),
        "xml_failures": failures, "xml_errors": errors,
        "log_call_failed": log["call_failed"], "log_error_failed": log["error_failed"],
        "counts_agree": counts_agree, "nodeids_agree": nodeids_agree,
    }
    if not counts_agree:
        error = {"kind": "junit-reportlog-mismatch", "rc": rc, "line": line}
        return (False, f"pytest rc=1 但 XML（{failures} failure/{errors} error）与 report-log"
                       f"（{log['call_failed']} call-failed/{log['error_failed']} error-cell）"
                       f"不一致（非判死）", red, error, evidence)
    if not nodeids_agree:
        error = {"kind": "junit-reportlog-mismatch", "rc": rc, "line": line}
        return (False, "pytest rc=1 但 XML 与 report-log 的失败 nodeid 集合不一致"
                       "（非判死）", red, error, evidence)

    if failures >= 1:
        # r5 R1 + r6 R1 + r7 R1：stdout 红 id 必须被 XML <testcase> 佐证
        # （red_id_matches_identity：等值或点分截断，**单向**）。逐行取**首个被佐证**的
        # 候选——每行的每个 ``" - "`` 切点都是候选，故 param id 含空格/``" - "``、
        # 或理由里含 ``" - "`` 都不会再产生假 error（r6 的 ``\S+`` 与「只按一个切点」都会）。
        justified, unjustified = [], []
        for bucket in candidates:
            hit = next((item for item in bucket if justify_red(item, verdict["bad"])), None)
            if hit is None:
                unjustified.append(bucket[0])
            else:
                justified.append(hit)
        justified = sorted(set(justified))
        unjustified = sorted(set(unjustified))
        evidence["red_unjustified"] = unjustified
        if unjustified:
            error = {"kind": "red-not-in-junit", "rc": rc, "line": line}
            return (False, f"stdout 红 id 无 junit XML <testcase> 佐证：{unjustified}"
                           f"（扩展/伪造行，非判死）", red, error, evidence)
        if not red and verdict["bad"]:
            # r6 R2：XML 有失败的 <testcase> 身份、stdout 却一条红 id 都没有（atexit 只补了
            # 一个 summary 表头，真 id 被整段挤掉）⇒ 具名 error，**绝不** killed:true + red:[]
            error = {"kind": "red-unknown", "rc": rc, "line": line}
            return (False, f"junit XML 有 {failures} 个 <failure>（testcase 身份 "
                           f"{verdict['bad']}）但 stdout 无任何红 id"
                           f"（真 id 被挤掉/伪造，非判死）", [], error, evidence)
        # 走到这里：要么有全部被佐证的红 id，要么 XML 自己不带任何 testcase 身份
        # （<failure> 不挂在 <testcase> 下，bad == []）——后者是唯一允许的 killed + red: []
        # 的正常路径（expect_survive 的 documented-unobservable 是另一条显式例外）。
        return True, (justified[0] if justified else f"xml-failure x{failures}"), justified, None, evidence
    if errors >= 1:
        error = {"kind": "rc1-xml-error-only", "rc": rc, "line": line}
        return False, (f"pytest rc=1 且 XML 只有 {errors} 个 <error>、0 个 <failure>"
                       f"（收集/setup 错误，非判死）"), red, error, evidence
    if not red:
        error = {"kind": "rc1-no-red-line", "rc": rc, "line": line}
        return False, "pytest rc=1 但 XML 无 <failure>/<error> 且无红行（非判死）", [], error, evidence
    error = {"kind": "rc1-xml-no-failure", "rc": rc, "line": line}
    return (False, "pytest rc=1 但 XML 无 <failure>/<error>（stdout 红行不可信，非判死）",
            red, error, evidence)


def record_entry(spec, killed, red, error):
    """机读记录条目（形状只此一处；error 时追加四条键，非 error 记录逐字节不变）。"""
    entry = {
        "killed": killed,
        "expect_survive": spec["expect_survive"],
        "red": red,
        "note": spec["note"],
        "source": spec["source"],
    }
    if error:  # 只在 error 时增键：非 error 记录**逐字节**不变（G1.4）
        entry.update({
            "error": True,
            "error_kind": error["kind"],
            "rc": error["rc"],
            "error_line": error["line"],
        })
    return entry


def replay_one(spec, tree: Path, python: str, verbose: bool, baseline_cache=None, timeout=None):
    """返回 ``(killed, detail, red, error, evidence)``（``error``/``evidence`` 见
    :func:`judge_mutation`）。"""
    evidence = {"name": spec["name"], "source": spec["source"],
                "expect_fail": list(spec["expect_fail"]),
                "expect_survive": spec["expect_survive"]}
    problems = check_anchors(spec, tree)
    if problems:
        # r6 R9：锚点失效也要在主记录里带 error（没有 pytest rc ⇒ rc=None），与旁证的
        # verdict=error/error_kind=anchors 一致；否则主记录只有 {"killed": false}，
        # doc_table 会把整条渲染成 SURVIVED、控制台也把它算进「存活」。
        error = {"kind": "anchors", "rc": None, "line": "; ".join(problems)}
        evidence["anchors"] = problems
        evidence["verdict"] = "error"
        evidence["error_kind"] = "anchors"
        return False, "; ".join(problems), [], error, evidence
    originals = {}
    for relative, _old, _new in spec["edits"]:
        originals.setdefault(relative, (tree / relative).read_text(encoding="utf-8"))

    # 1) 基线：干净树必须全绿（按节点集合缓存：只缓存「绿」基线，不绿的要保留证据行）
    key = tuple(spec["expect_fail"])
    if baseline_cache is not None and key in baseline_cache:
        base_ok, base_red = baseline_cache[key]
        evidence["baseline"] = {"cached": True, "rc": 0, "green": base_ok}
    else:
        base = run(pytest_cmd(python, spec["expect_fail"]), tree, timeout=timeout)
        base_ok, base_red = base.returncode == 0, failed_nodes(base.stdout)
        evidence["baseline"] = {"cached": False, "rc": base.returncode, "green": base_ok}
        if baseline_cache is not None and base_ok:
            baseline_cache[key] = (base_ok, base_red)
    if not base_ok:
        blob = (base.stdout or "") + "\n" + (base.stderr or "")
        kind = ("pytest-unavailable" if pytest_missing(base) else
                "pytest-timeout" if base.returncode == TIMEOUT_RC else "baseline-not-green")
        detail = ("基线跑不起来：该解释器没有 pytest（非判死）" if kind == "pytest-unavailable" else
                  "基线超时未结束（非判死）" if kind == "pytest-timeout" else
                  "基线不绿（测试本来就红，无法用于杀死变异）")
        error = {"kind": kind, "rc": base.returncode, "line": summarize_error(blob)}
        evidence["error_kind"] = kind
        evidence["verdict"] = "error"
        return False, detail, base_red, error, evidence

    # 2) 应用全部 edits + 跑同一组（这次带 --junit-xml + --report-log：两条判死信道）
    with pytest_report_dir() as reports:
        try:
            for relative, old, new in spec["edits"]:
                target = tree / relative
                target.write_text(target.read_text(encoding="utf-8").replace(old, new, 1))
            started = time.time()
            mutated = run(pytest_cmd(python, spec["expect_fail"], reports["junit"],
                                     reports["reportlog"]),
                          tree, timeout=timeout, extra_path=str(reports["dir"]))
            finished = time.time()
        finally:
            for relative, original in originals.items():
                (tree / relative).write_text(original)
        killed, detail, red, error, judged = judge_mutation(
            spec, mutated, reports["junit"], reports["reportlog"],
            window=(started, finished), timeout=timeout)
    evidence.update(judged)
    evidence["verdict"] = ("error" if error else "killed" if killed else "survived")
    if error:
        evidence["error_kind"] = error["kind"]
    if verbose:
        print(f"    {'error' if error else 'killed-by'}: {detail}")
        print(f"    evidence: {evidence_line(evidence)}")
    return killed, detail, red, error, evidence


def evidence_line(evidence: dict) -> str:
    """把一条审计证据压成单行（控制台展示用，非记录内容）。"""
    junit = evidence.get("junit") or {}
    log = evidence.get("report_log") or {}
    cross = evidence.get("cross_channel") or {}
    return (f"rc={evidence.get('rc')} "
            f"xml={junit.get('failures', '?')}f/{junit.get('errors', '?')}e "
            f"log={log.get('call_failed', '?')}call/{log.get('error_failed', '?')}err "
            f"cross={'agree' if cross.get('agree') else cross.get('reason', 'n/a')} "
            f"window={evidence.get('xml_mtime_in_run_window')} "
            f"totals={evidence.get('xml_totals_consistent')} "
            f"red={len(evidence.get('red_from_stdout') or [])}"
            f"(unjustified={len(evidence.get('red_unjustified') or [])})")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--all", action="store_true", help="重放全部变异")
    ap.add_argument("--one", metavar="NAME", help="只重放一条")
    ap.add_argument("--list", action="store_true", help="列出全部变异定义")
    ap.add_argument("--store", metavar="DIR", default=str(DEFAULT_STORE),
                    help="store 目录（默认本脚本目录；同时加载 mutations/*.json 与 mutations.py）")
    ap.add_argument("--tree", metavar="DIR", help="复用已导出的干净树（不导出）")
    ap.add_argument("--keep", action="store_true", help="保留临时导出树")
    ap.add_argument("--source", choices=["worktree", "head"], default="worktree",
                    help="worktree=HEAD+工作区改动（提交前验证，默认）；head=仅 HEAD")
    ap.add_argument("--record", metavar="FILE", help="机读记录（每变异 killed/红用例 id/note）")
    ap.add_argument("--evidence", metavar="FILE",
                    help="审计证据旁证（XML 失败计数/report-log/红 id 佐证/窗口与自洽检查；"
                         "默认 <record>.evidence.json；主记录五键形状不许变，故证据走旁证文件）")
    ap.add_argument("--check-anchors", action="store_true",
                    help="只做锚点唯一性检查（每条 old 在目标树恰好命中一次）")
    ap.add_argument("--python", default=sys.executable, help="python 解释器")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, metavar="SECONDS",
                    help=f"单次 pytest 运行超时秒数（默认 {DEFAULT_TIMEOUT:g}，理由见模块文档；"
                         "0 = 不设超时）。超时记 error（pytest-timeout），永不判死")
    args = ap.parse_args(argv)

    # G6：缺 --tree 是**用法错**（rc=2，同 argparse），不是断言失败
    if args.check_anchors and not args.tree:
        ap.error("--check-anchors 需要 --tree <导出的干净树>")
    if args.timeout < 0:
        ap.error(f"--timeout 不能为负：{args.timeout:g}")
    # N5：解释器不存在 ⇒ 具名用法错（rc=2），绝不留裸 FileNotFoundError traceback
    if shutil.which(args.python) is None and not (
            os.path.isfile(args.python) and os.access(args.python, os.X_OK)):
        ap.error(f"--python 解释器不存在或不可执行：{args.python!r}")
    timeout = args.timeout or None

    store = Path(args.store).resolve()
    specs = load_mutations(store)

    # G4：0 条定义 = 错误（rc≠0），绝不「静默通过」
    if not specs:
        print(f"错误：store 里没有任何变异定义（0 条）：{store}", file=sys.stderr)
        print("      （查找 <store>/mutations/*.json 与 <store>/mutations.py；--store 指错目录？）",
              file=sys.stderr)
        print("      本基础设施补丁自带 0 条定义，见 README §1/§4。", file=sys.stderr)
        return 1

    if args.one:
        specs = [s for s in specs if s["name"] == args.one]
        if not specs:
            print(f"未找到变异 {args.one!r}", file=sys.stderr)
            return 2

    if args.list:
        for spec in specs:
            files = ", ".join(relative for relative, _o, _n in spec["edits"])
            print(f"{spec['name']}  [{spec['source']}]  {files}")
            if spec["note"]:
                print(f"    {spec['note']}")
        return 0

    if args.check_anchors:
        tree = Path(args.tree).resolve()
        offenders = []
        for spec in specs:
            problems = check_anchors(spec, tree)
            if problems:
                offenders.append((spec["name"], problems))
        print(f"锚点检查：{len(specs)} 条定义，问题 {len(offenders)} 条（tree={tree}）")
        for name, problems in offenders:
            print(f"  {name}: {'; '.join(problems)}")
        return 1 if offenders else 0

    if not args.all and not args.one:
        ap.print_help()
        return 0

    if args.tree:
        tree = Path(args.tree).resolve()
    else:
        tmp = Path(tempfile.mkdtemp(prefix="plaita-mutation-replay-"))
        tree = tmp
        export_tree(tree, args.source)
    assert_import_is_tree(tree, args.python)
    print(f"store: {store}")
    print(f"树: {tree}  (source={args.source})  定义: {len(specs)}\n")

    results = []
    record = {}
    evidence_record = {}
    baseline_cache = {}
    for spec in specs:
        print(f"[{spec['name']}] {spec['source']}")
        killed, detail, red, error, evidence = replay_one(
            spec, tree, args.python, verbose=True, baseline_cache=baseline_cache, timeout=timeout)
        results.append({"name": spec["name"], "killed": killed, "detail": detail,
                        "error": error, "expect_survive": spec["expect_survive"]})
        entry = record_entry(spec, killed, red, error)
        record[spec["name"]] = entry
        evidence_record[spec["name"]] = evidence
        status = "ERROR" if error else ("KILLED" if killed else "SURVIVED")
        print(f"    {status}  {detail}\n")

    survived = [r for r in results if not r["killed"] and not r["error"]]
    errored = [r for r in results if r["error"]]
    real_killed = [r for r in results if r["killed"] and not r["expect_survive"]]
    documented = [r for r in results if r["expect_survive"]]
    print("=" * 60)
    print(f"合计: {len(results)}  杀死: {len(real_killed)}  存活: {len(survived)}  错误: {len(errored)}"
          f"  其中 documented-unobservable: {len(documented)}（不判死，不计入杀死）")
    for r in survived:
        print(f"  存活: {r['name']}  ({r['detail']})")
    for r in errored:
        print(f"  错误: {r['name']}  rc={r['error']['rc']}  {r['detail']}"
              f"  | {r['error']['line']}")
    error_kinds = {}
    for r in errored:
        kind = r["error"]["kind"]
        error_kinds[kind] = error_kinds.get(kind, 0) + 1
    if error_kinds:
        print("  错误分类: " + ", ".join(f"{kind}×{count}"
                                        for kind, count in sorted(error_kinds.items())))
    mismatched = [name for name, ev in evidence_record.items()
                  if (ev.get("cross_channel") or {}).get("agree") is False]
    if mismatched:
        print(f"  两信道不一致: {len(mismatched)} 条 {mismatched[:5]}")

    if args.record:
        Path(args.record).write_text(
            json.dumps(record, indent=1, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"记录已写出: {args.record}（{len(record)} 条）")

    # 审计证据旁证（独立文件：主记录五键形状**逐字节冻结**，故证据不塞进主记录）
    evidence_path = args.evidence
    if not evidence_path and args.record:
        evidence_path = str(Path(args.record)) + ".evidence.json"
    if evidence_path:
        Path(evidence_path).write_text(
            json.dumps(evidence_record, indent=1, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"证据已写出: {evidence_path}（{len(evidence_record)} 条）")

    if args.keep and not args.tree:
        print(f"（临时树保留: {tree}）")
    elif not args.tree:
        shutil.rmtree(tree, ignore_errors=True)
    return 1 if (survived or errored) else 0


if __name__ == "__main__":
    sys.exit(main())
