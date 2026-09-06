"""真实 stdio 握手冒烟测试（2026-09 盲区评审 P0-2/P2-5）。

历史上的 MCP 集成测试全部往 sys.modules 塞 stub，真实 mcp 2.x 安装下
`plaita-ai mcp` 已崩而测试全绿。本文件直接以子进程启动 MCP 服务，
走一次 JSON-RPC initialize → tools/list 握手，锁定真实可用性。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)

try:
    import mcp.server.fastmcp  # noqa: F401
    _REAL_MCP = True
except ImportError:
    _REAL_MCP = False


@unittest.skipUnless(_REAL_MCP, "real mcp package required: pip install 'mcp>=1.0,<2'")
class TestMcpStdioHandshake(unittest.TestCase):
    def test_initialize_and_tools_list(self):
        """子进程 stdio 握手：initialize → initialized → tools/list。"""
        env = dict(os.environ)
        env["PYTHONPATH"] = _PLAITA_AI + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.Popen(
            [sys.executable, "-c",
             "from plaita_ai.mcp.server import main; main()"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            text=True,
        )
        try:
            # initialize
            init_req = {
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "plaita-ai-smoke", "version": "0.0.1"},
                },
            }
            proc.stdin.write(json.dumps(init_req) + "\n")
            proc.stdin.flush()
            line = proc.stdout.readline()
            resp = json.loads(line)
            self.assertEqual(resp.get("id"), 1)
            self.assertIn("serverInfo", resp.get("result", {}))

            # initialized 通知 + tools/list
            proc.stdin.write(json.dumps(
                {"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
            proc.stdin.write(json.dumps(
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}) + "\n")
            proc.stdin.flush()
            # 跳过可能的通知行，读带 id=2 的响应
            tools_resp = None
            for _ in range(10):
                line = proc.stdout.readline()
                if not line:
                    break
                msg = json.loads(line)
                if msg.get("id") == 2:
                    tools_resp = msg
                    break
            self.assertIsNotNone(tools_resp, "no tools/list response")
            tool_names = [t["name"] for t in tools_resp["result"]["tools"]]
            self.assertIn("flow_compile", tool_names)
            self.assertIn("flow_run", tool_names)
        finally:
            proc.kill()
            proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
