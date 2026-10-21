"""hermes_tools — gateway runs API family (background dispatch, post-A2A).

Thin client for the hermes gateway api_server (/v1/runs) — the fire-and-poll
rail that replaced the removed A2A hermes-a2a-run CLI. Runs execute as the
DEFAULT (Rook) agent; long work should be started here and polled, never
blocked on inside a tool call.

Auth: Bearer MCP_API_KEY (env) — the SAME key the gateway api_server validates
(it is the shared mcp-auth/gateway key; no new secret needed).
Egress: allow-mcp-egress already permits 192.168.1.2:8642.

Gotcha: a gateway restart destroys in-flight runs (run_id → run_not_found) —
check the repo/artifacts before re-dispatching.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

from tool_registry import ToolError

BASE = os.environ.get("HERMES_RUNS_API", "http://192.168.1.2:8642")


def _key() -> str:
    k = os.environ.get("MCP_API_KEY", "").strip()
    if not k:
        raise ToolError("tool_error", "MCP_API_KEY env missing on the mcp pod "
                        "(secret mcp-auth) — cannot authenticate to the gateway")
    return k


def _req(path: str, method: str = "GET", body: dict | None = None, timeout: int = 30):
    headers = {"Authorization": "Bearer " + _key(),
               "Content-Type": "application/json"}
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return json.loads(raw.decode() or "{}") if raw else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:400]
        code = e.code
        if code == 401:
            raise ToolError("tool_error",
                            "gateway runs API rejected the key (401) — key drift "
                            "between mcp-auth and gateway.api_server?") from e
        raise ToolError("tool_error", f"gateway runs API HTTP {code}: {detail}") from e
    except Exception as e:  # noqa: BLE001
        raise ToolError("tool_error", f"gateway unreachable ({BASE}): {e}") from e


def start_run(input_text: str, label: str = "") -> str:
    if not (input_text or "").strip():
        raise ToolError("bad_request", "input is required (the task/prompt for the agent)")
    payload = {"input": input_text}
    if label:
        payload["label"] = label
    d = _req("/v1/runs", "POST", payload)
    return json.dumps({"run_id": d.get("run_id"), "status": d.get("status", "started"),
                       "next": f"poll hermes_run_status(run_id={d.get('run_id')!r})"})


def run_status(run_id: str) -> str:
    if not (run_id or "").strip():
        raise ToolError("bad_request", "run_id is required")
    d = _req(f"/v1/runs/{run_id.strip()}")
    out = {k: d.get(k) for k in
           ("run_id", "status", "output", "error", "last_event",
            "created_at", "updated_at", "usage") if d.get(k) is not None}
    st = (d.get("status") or "").lower()
    out["done"] = st in ("completed", "failed", "cancelled", "timeout")
    return json.dumps(out)
