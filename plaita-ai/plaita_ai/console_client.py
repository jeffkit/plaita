"""HTTP client for the plaita-console management API.

This is the data plane for plaita-ai's supervisor loop: flows, versions,
executions, and dry-run all live behind the console, and this client is the
only thing plaita-ai needs to know about it. Auth mirrors the console's
``require_auth``: prefer ``PLAITA_CONSOLE_ADMIN_API_KEY`` (machine clients),
fall back to a username/password login session.

All methods return plain JSON-compatible dicts (the console's response
shapes, lightly tolerated) so callers can serialize results into reports.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional

import httpx

#: Execution statuses the console treats as terminal (api/executions.py).
TERMINAL_STATUSES = frozenset({"completed", "failed", "error", "cancelled"})


class ConsoleClientError(RuntimeError):
    """A console API call failed (non-2xx, bad payload, or unconfigured)."""

    def __init__(self, message: str, status: Optional[int] = None, detail: Any = None):
        super().__init__(message)
        self.status = status
        self.detail = detail


@dataclass
class ConsoleConfig:
    base_url: str
    api_prefix: str = "/api"
    admin_api_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    timeout_s: float = 30.0

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> "ConsoleConfig":
        env = dict(os.environ if env is None else env)
        base_url = (env.get("PLAITA_CONSOLE_URL") or "").rstrip("/")
        if not base_url:
            raise ConsoleClientError(
                "PLAITA_CONSOLE_URL is not configured — point it at a running "
                "plaita-console (e.g. http://127.0.0.1:8000)."
            )
        return cls(
            base_url=base_url,
            api_prefix=env.get("PLAITA_CONSOLE_API_PREFIX", "/api"),
            admin_api_key=env.get("PLAITA_CONSOLE_ADMIN_API_KEY") or None,
            username=env.get("PLAITA_CONSOLE_USERNAME") or None,
            password=env.get("PLAITA_CONSOLE_PASSWORD") or None,
            timeout_s=float(env.get("PLAITA_CONSOLE_TIMEOUT_S", "30")),
        )


class ConsoleClient:
    """Small sync client over the console admin API."""

    def __init__(self, config: ConsoleConfig, transport: Optional[httpx.BaseTransport] = None):
        self.config = config
        self._session_token: Optional[str] = None
        self._http = httpx.Client(
            base_url=config.base_url,
            timeout=config.timeout_s,
            transport=transport,
        )

    # -- auth ------------------------------------------------------------

    def _auth_headers(self) -> Dict[str, str]:
        if self.config.admin_api_key:
            return {"X-Admin-API-Key": self.config.admin_api_key}
        if not self._session_token:
            self.login()
        return {"Authorization": f"Bearer {self._session_token}"}

    def login(self) -> Dict[str, Any]:
        """Username/password login; stores the returned session token."""
        if not (self.config.username and self.config.password):
            raise ConsoleClientError(
                "No console credentials: set PLAITA_CONSOLE_ADMIN_API_KEY "
                "(preferred for machines) or PLAITA_CONSOLE_USERNAME/PASSWORD."
            )
        info = self._request(
            "POST",
            "/auth/login",
            json={"username": self.config.username, "password": self.config.password},
            _skip_auth=True,
        )
        token = info.get("token") or info.get("session_token") or info.get("access_token")
        if not token:
            raise ConsoleClientError("login response carried no session token", detail=info)
        self._session_token = str(token)
        return info

    # -- request core ----------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        _skip_auth: bool = False,
    ) -> Any:
        headers = {} if _skip_auth else self._auth_headers()
        url = f"{self.config.api_prefix}{path}"
        try:
            resp = self._http.request(method, url, json=json, params=params, headers=headers)
        except httpx.HTTPError as exc:
            raise ConsoleClientError(f"console request failed: {method} {url}: {exc}") from exc
        if resp.status_code >= 400:
            detail: Any = None
            try:
                detail = resp.json()
            except ValueError:
                detail = resp.text[:500]
            raise ConsoleClientError(
                f"console returned {resp.status_code} for {method} {url}",
                status=resp.status_code,
                detail=detail,
            )
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    # -- flows & versions --------------------------------------------------

    def list_flows(self) -> Dict[str, Any]:
        return self._request("GET", "/flows")

    def get_flow(self, flow_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/flows/{flow_id}")

    def get_version(self, flow_id: str, version: str) -> Dict[str, Any]:
        return self._request("GET", f"/flows/{flow_id}/versions/{version}")

    def save_version(
        self,
        flow_id: str,
        version: str,
        definition: str,
        layout: str = "{}",
        created_by: str = "plaita-ai",
    ) -> Dict[str, Any]:
        return self._request(
            "PUT",
            f"/flows/{flow_id}/versions/{version}",
            json={"definition": definition, "layout": layout, "created_by": created_by},
        )

    def publish_version(self, flow_id: str, version: str) -> Dict[str, Any]:
        """Publish (= promote) a version — the console's human-facing gate."""
        return self._request("POST", f"/flows/{flow_id}/publish", json={"version": version})

    # -- executions --------------------------------------------------------

    def list_executions(
        self,
        flow_id: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        size: int = 20,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"page": page, "size": size}
        if flow_id:
            params["flow_id"] = flow_id
        if status:
            params["status"] = status
        return self._request("GET", "/executions", params=params)

    def get_execution(self, execution_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/executions/{execution_id}")

    def start_execution(
        self, flow_id: str, version: Optional[str] = None, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"flow_id": flow_id, "params": params or {}}
        if version:
            body["version"] = version
        return self._request("POST", "/executions", json=body)

    def cancel_execution(self, execution_id: str) -> Dict[str, Any]:
        return self._request("POST", f"/executions/{execution_id}/cancel")

    def wait_execution(
        self,
        execution_id: str,
        timeout_s: float = 120.0,
        poll_s: float = 1.0,
        terminal: Iterable[str] = TERMINAL_STATUSES,
    ) -> Dict[str, Any]:
        """Poll one execution until it reaches a terminal status or times out."""
        terminal_set = set(terminal)
        deadline = time.monotonic() + timeout_s
        last: Dict[str, Any] = {}
        while True:
            last = self.get_execution(execution_id)
            status = str(last.get("status", ""))
            if status in terminal_set:
                return last
            if time.monotonic() >= deadline:
                raise ConsoleClientError(
                    f"execution {execution_id} still '{status or 'unknown'}' after {timeout_s}s",
                    detail=last,
                )
            time.sleep(poll_s)

    # -- dry-run -----------------------------------------------------------

    def dry_run(
        self,
        flow_json: str,
        input: Optional[Dict[str, Any]] = None,
        pinned: Optional[Dict[str, Any]] = None,
        only_node: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compile + execute a definition in-process, no deployment needed."""
        body: Dict[str, Any] = {"flowJson": flow_json, "input": input or {}}
        if pinned:
            body["pinned"] = pinned
        if only_node:
            body["onlyNode"] = only_node
        return self._request("POST", "/flows/dry-run", json=body)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "ConsoleClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def client_from_env(env: Optional[Dict[str, str]] = None) -> ConsoleClient:
    """Build a client from environment config; raises when unconfigured."""
    return ConsoleClient(ConsoleConfig.from_env(env))
