"""In-memory fake of the plaita-console admin API, driven through
httpx.MockTransport — the shared fixture for client/ops/evals/supervisor
tests. Mirrors the route shapes plaita-ai consumes (not a full console)."""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx


class FakeConsole:
    """flows/versions/executions/dry-run state + an ASGI-style handler."""

    def __init__(self) -> None:
        # flow_id -> {"flow_id", "author", "desc", "versions": {v: VersionView}, "order": [v...]}
        self.flows: Dict[str, Dict[str, Any]] = {}
        self.published: Dict[str, str] = {}
        self.executions: Dict[str, Dict[str, Any]] = {}
        self._exec_seq = 0
        # Optional hooks a test can override:
        #   (flow_json, input) -> {"result": ...} | {"error": "..."}
        self.dry_run_handler: Optional[Callable[[str, Dict[str, Any]], Dict[str, Any]]] = None
        # (flow_id, version, params) -> (status, output)
        self.run_handler: Callable[[str, Optional[str], Dict[str, Any]], Tuple[str, Any]] = (
            lambda flow_id, version, params: ("completed", {"echo": params})
        )
        self.requests: List[Tuple[str, str]] = []

    # -- seeding ---------------------------------------------------------

    def seed_flow(
        self,
        flow_id: str,
        versions: Dict[str, str],
        published: Optional[str] = None,
        desc: str = "",
    ) -> None:
        """versions: {semver: definition_json_string}."""
        flow = self.flows.setdefault(
            flow_id,
            {"flow_id": flow_id, "author": "seed", "desc": desc, "versions": {}, "order": []},
        )
        for version, definition in versions.items():
            self.put_version(flow_id, version, definition)
        if published:
            self.published[flow_id] = published

    def put_version(self, flow_id: str, version: str, definition: str) -> Dict[str, Any]:
        flow = self.flows.setdefault(
            flow_id,
            {"flow_id": flow_id, "author": "seed", "desc": "", "versions": {}, "order": []},
        )
        view = {
            "flow_id": flow_id,
            "version": version,
            "status": "draft",
            "definition": definition,
            "layout": "{}",
            "created_at": "2026-09-26T00:00:00",
            "published_at": None,
            "created_by": "test",
        }
        if version not in flow["versions"]:
            flow["order"].append(version)
        flow["versions"][version] = view
        return view

    def seed_execution(
        self,
        flow_id: str,
        status: str,
        output: Any = None,
        start_time: str = "2026-09-26T10:00:00",
        end_time: Optional[str] = "2026-09-26T10:00:05",
        error: Any = None,
        execution_id: Optional[str] = None,
    ) -> str:
        self._exec_seq += 1
        exec_id = execution_id or f"exec-{self._exec_seq:04d}"
        self.executions[exec_id] = {
            "execution_id": exec_id,
            "flow_id": flow_id,
            "flow_version": self.published.get(flow_id),
            "status": status,
            "start_time": start_time,
            "end_time": end_time if status in ("completed", "failed", "error", "cancelled") else None,
            "error": error,
            "output": output,
            "invoker": "test",
        }
        return exec_id

    # -- request handling --------------------------------------------------

    def handler(self, request: httpx.Request) -> httpx.Response:
        method, path = request.method, request.url.path
        self.requests.append((method, path))
        body = json.loads(request.content.decode("utf-8")) if request.content else {}
        query = dict(request.url.params)

        # strip the /api prefix
        assert path.startswith("/api"), f"unexpected path {path}"
        route = path[len("/api") :]

        # auth
        if route == "/auth/login":
            if body.get("username") == "sup" and body.get("password") == "pw":
                return httpx.Response(200, json={"token": "sess-123", "username": "sup"})
            return httpx.Response(401, json={"detail": "用户名或密码错误"})

        if route == "/health":
            return httpx.Response(200, json={"status": "healthy"})

        key = request.headers.get("X-Admin-API-Key")
        bearer = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
        if key != "test-key" and bearer != "sess-123":
            return httpx.Response(401, json={"detail": "unauthorized"})

        # flows ----------------------------------------------------------
        if route == "/flows" and method == "POST":
            flow_id = body.get("flow_id", "")
            if not flow_id:
                return httpx.Response(422, json={"detail": "flow_id required"})
            self.flows.setdefault(
                flow_id,
                {"flow_id": flow_id, "author": body.get("author", ""), "desc": body.get("desc", ""), "versions": {}, "order": []},
            )
            return httpx.Response(200, json={"flow_id": flow_id})
        if route == "/flows" and method == "GET":
            flows = [
                {
                    "flow_id": f["flow_id"],
                    "author": f["author"],
                    "desc": f["desc"],
                    "updated_at": "2026-09-26T00:00:00",
                }
                for f in self.flows.values()
            ]
            return httpx.Response(200, json={"flows": flows, "total": len(flows)})

        parts = [p for p in route.split("/") if p]
        if parts and parts[0] == "flows":
            flow_id = parts[1] if len(parts) > 1 else ""
            if method == "POST" and len(parts) == 2 and parts[1] == "dry-run":
                if self.dry_run_handler is not None:
                    return httpx.Response(200, json=self.dry_run_handler(body.get("flowJson", ""), body.get("input") or {}))
                return httpx.Response(200, json={"result": {"ok": True}, "nodes": [], "error": None})
            if len(parts) == 2 and method == "GET":
                flow = self.flows.get(flow_id)
                if flow is None:
                    return httpx.Response(404, json={"detail": "no such flow"})
                versions = [
                    {
                        "version": v,
                        "status": ("published" if self.published.get(flow_id) == v else "draft"),
                        "created_at": "2026-09-26T00:00:00",
                        "created_by": "test",
                    }
                    for v in flow["order"]
                ]
                return httpx.Response(
                    200,
                    json={"flow_id": flow_id, "author": flow["author"], "desc": flow["desc"], "versions": versions},
                )
            if len(parts) == 4 and parts[2] == "versions" and method == "GET":
                view = self.flows.get(flow_id, {}).get("versions", {}).get(parts[3])
                return httpx.Response(200, json=view) if view else httpx.Response(404, json={"detail": "no such version"})
            if len(parts) == 4 and parts[2] == "versions" and method == "PUT":
                view = self.put_version(flow_id, parts[3], body.get("definition", ""))
                view["created_by"] = body.get("created_by", "")
                return httpx.Response(200, json=view)
            if len(parts) == 3 and parts[2] == "publish" and method == "POST":
                version = body.get("version", "")
                if version not in self.flows.get(flow_id, {}).get("versions", {}):
                    return httpx.Response(404, json={"detail": "no such version"})
                self.published[flow_id] = version
                view = self.flows[flow_id]["versions"][version]
                view["status"] = "published"
                return httpx.Response(200, json=view)

        # executions -------------------------------------------------------
        if parts and parts[0] == "executions":
            if method == "GET" and len(parts) == 1:
                matched = [
                    e
                    for e in self.executions.values()
                    if (not query.get("flow_id") or e["flow_id"] == query["flow_id"])
                    and (not query.get("status") or e["status"] == query["status"])
                ]
                return httpx.Response(
                    200, json={"executions": matched, "total": len(matched), "page": 1, "size": int(query.get("size", 20))}
                )
            if method == "POST" and len(parts) == 1:
                self._exec_seq += 1
                exec_id = f"exec-{self._exec_seq:04d}"
                status, output = self.run_handler(
                    body.get("flow_id", ""), body.get("version"), body.get("params") or {}
                )
                self.executions[exec_id] = {
                    "execution_id": exec_id,
                    "flow_id": body.get("flow_id", ""),
                    "flow_version": body.get("version") or self.published.get(body.get("flow_id", "")),
                    "status": status,
                    "start_time": "2026-09-26T10:00:00",
                    "end_time": "2026-09-26T10:00:02" if status != "running" else None,
                    "error": {"message": "boom"} if status == "failed" else None,
                    "output": output,
                    "invoker": "test",
                }
                return httpx.Response(200, json=self.executions[exec_id])
            if len(parts) == 2 and method == "GET":
                ex = self.executions.get(parts[1])
                return httpx.Response(200, json=ex) if ex else httpx.Response(404, json={"detail": "no such execution"})
            if len(parts) == 3 and parts[2] == "cancel" and method == "POST":
                ex = self.executions.get(parts[1])
                if ex is None:
                    return httpx.Response(404, json={"detail": "no such execution"})
                ex["status"] = "cancelled"
                return httpx.Response(200, json=ex)

        return httpx.Response(404, json={"detail": f"no route: {method} {route}"})

    def transport(self) -> httpx.BaseTransport:
        return httpx.MockTransport(self.handler)
