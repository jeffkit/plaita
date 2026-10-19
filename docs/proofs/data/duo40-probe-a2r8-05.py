"""duo40 / A2R8-05 探针：20k 条 ~1 KiB 协议行的驻留内存峰值。

用法：``PN_SRC=<src 路径> python duo40-probe-a2r8-05.py``。
每行都是新字符串（真实 agentproc 回调行为；复用同一对象只测到 20k 引用）。
pre-fix 树（663dbd0）与修复后树的输出对比见 ``duo40-probe-a2r8-05.txt``。
"""
import json
import os
import sys
import tempfile
import tracemalloc

sys.path.insert(0, os.environ["PN_SRC"])

home = tempfile.mkdtemp(prefix="duo40-home-")
os.environ["HOME"] = home
os.environ["AGENTPROC_RUN_LOCK_DIR"] = os.path.join(home, "locks")
flowcast = os.path.join(home, ".flowcast")
os.makedirs(flowcast)
with open(os.path.join(flowcast, "agents.json"), "w") as f:
    json.dump({"agents": {"echo": {"executor": "test-echo"}}}, f)
with open(os.path.join(flowcast, "providers.json"), "w") as f:
    json.dump({"providers": {}}, f)

import agentproc  # noqa: E402
from agentproc import EXECUTORS  # noqa: E402

EXECUTORS["test-echo"] = {
    "cli_name": "echo", "plain": True,
    "make_handlers": lambda: {"build_args": lambda *a, **k: ["true"]},
}

from plaita_nodes.agent_run import AgentRunNode  # noqa: E402


class Result:
    exit_code = 0
    error = None
    reply = '{"result":"ok","is_error":false}'
    usage = None
    session_id = "s"


line = ('{"type":"assistant","message":{"content":[{"type":"text","text":"'
        + "t" * 1000 + '"}]}}')


def fake_run(spec, options):
    for i in range(20_000):
        options.on_protocol_line(line[:-6] + str(i).zfill(5) + '"]}}')
    return Result()


agentproc.runner.run = fake_run
workspace = tempfile.mkdtemp(prefix="duo40-ws-")
node = AgentRunNode(id="t", agent="echo", prompt="x", repo=workspace)
execution = type("E", (), {
    "evaluate": lambda self, v: v,
    "get_global_variable": lambda self, k, d=None: d,
})()

tracemalloc.start()
out = node.execute(execution)
cur, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
print("text_len=%d" % len(out["text"]))
print("protocol_peak=%d bytes = %.2f MiB" % (peak, peak / 1024 / 1024))
