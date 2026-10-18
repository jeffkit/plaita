#!/usr/bin/env python3
"""plaita#190 repro / before-after probe：dry-run 不得让内置 http 节点真实出网。

被测面：``plaita-console/backend/services/dryrun.py`` 的试跑闸门。

修复前：闸门只有名黑名单 ``{code,python,javascript,js}``——http 不在列，且
内置 HTTP 节点不读 ``dry_run``；editor 试跑一条含 http 节点的 flow，console
backend 进程会**真实出网**并把响应体回显到试跑面板（进程内 SSRF）。

修复后：内置节点必须显式声明 dry-run 安全才可试跑（能力白名单）；http /
redis / event 等副作用内置节点默认拒绝并点名；同时 HTTP 节点自身在
``dry_run`` 下短路（纵深：直接调用也不出网）。

运行（仓库根）::

    PYTHONPATH=. :plaita-console/backend \
        /Users/jeff.jie/.venvs/plaita/bin/python docs/proofs/data/190-repro.py

退出码 0 = 安全（零请求且错误点名 http）；1 = 仍可真实出网（红）。
"""
from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from services import dryrun as dryrun_svc


class _ProbeHandler(BaseHTTPRequestHandler):
    """本地探针服务器：记录每一个到达的请求路径。"""

    hits: list = []

    def do_GET(self):  # noqa: N802 — http.server 接口
        type(self).hits.append(self.path)
        body = json.dumps({"secret": "internal-metadata", "path": self.path}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # 静默探针日志
        return


def _flow_json(url: str) -> str:
    return json.dumps(
        {
            "flow_id": "issue190-http-dryrun",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "fetch"},
                {
                    "type": "http",
                    "id": "fetch",
                    "method": "GET",
                    "url": url,
                    "next": "end",
                },
                {
                    "type": "end",
                    "id": "end",
                    "output": "$NODE.fetch",
                    "resultType": "success",
                },
            ],
        }
    )


def run() -> dict:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ProbeHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{port}/latest/meta-data/iam"
        out = dryrun_svc.dry_run(_flow_json(url), {})
    finally:
        server.shutdown()
        server.server_close()

    hits = list(_ProbeHandler.hits)
    error = out.get("error") or ""
    # 安全判据：服务器零请求，且试跑错误点名 http 节点（要么白名单拒绝，
    # 要么节点自身 dry-run 短路——两者都不产生网络 I/O）。
    safe = not hits and bool(error) and "http" in error.lower()
    return {
        "python": sys.version.split()[0],
        "requests_observed": len(hits),
        "request_paths": hits,
        "dry_run_error": error,
        "dry_run_result": out.get("result"),
        "nodes": [
            {"id": n.get("id"), "type": n.get("type"), "status": n.get("status"),
             "output": n.get("output")}
            for n in out.get("nodes", [])
        ],
        "verdict": "SAFE（零请求且拒绝/短路点名 http）" if safe else "VULNERABLE（http 节点真实出网）",
    }


if __name__ == "__main__":
    report = run()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["verdict"].startswith("SAFE") else 1)
