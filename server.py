"""mcp — unified agent MCP service (todo.*, docs.*, k3s.* tools).

Single uvicorn process serving:
  - GET  /health                     liveness
  - GET  /                           service info
  - GET  /api/summary, /api/todos…   REST passthrough for the todos store
                                    (kept from the old host service so
                                    existing consumers don't break)
  - GET  /api/tools                  dynamic tool catalog (REST)
  - POST /api/tools/reindex          rebuild the tool embedding index
  - MCP  /mcp                        Streamable HTTP

Dynamic tool surface (the point of this refactor):
  - discover_tool(query, top_k, min_score) — semantic search over the tool
    catalog (embedding DB in tool_vectors.py, fed by the homelab embeddings
    service; keyword fallback if it's down). Returns names, descriptions and
    input schemas for the ranked hits.
  - execute_tool(tool_name, payload)     — validate the payload against the
    discovered schema and dispatch to the registered callable.
  - tool_catalog()                       — cheap enumeration (no schemas).

Dynamic-only is the DEFAULT (STATIC_TOOLS unset = "0"): the client sees just
discover_tool / execute_tool / tool_catalog and discovers everything else on
demand. Set STATIC_TOOLS=1 to re-register the legacy 47-tool surface
(rollback/back-compat) — both paths draw from the same ToolRegistry, so they
can't drift.

Runs in k3s (ns `mcp`), exposed at http://mcp.ffilt.us/mcp. Data lives on a
Longhorn PVC at /data (todos.db, docs mirror + index, tool-index.db). k3s tools
use the in-cluster ServiceAccount (read-only RBAC).
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

import browser_tools
import docs_index
import gitea_mirror
import gitea_tools
import k3s_tools
import storage_tools
import system_checks
from todos_store import DEFAULT_TTL, TodoError, TodosStore, set_mirror_hook
from tool_registry import ToolError, ToolRegistry
from tool_vectors import ToolIndex

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("mcp")

PORT = int(os.environ.get("PORT", "8650"))
DB_PATH = Path(os.environ.get("TODOS_DB", "/data/todos.db"))
store = TodosStore(DB_PATH)

# Gitea issue mirror (write-through, best-effort). Enabled only when
# GITEA_MIRROR_TOKEN is set; SQLite stays authoritative either way. A full
# reconcile runs in the background at startup to backfill/repair the board.
_mirror = gitea_mirror.setup(store)
if _mirror is not None:
    set_mirror_hook(gitea_mirror.hook)
    gitea_mirror.start_background_reconcile(store)

# Dynamic-only default (fail-forward 2026-08-10): the client sees only the
# meta tools below and must discover_tool/execute_tool the rest. STATIC_TOOLS=1
# re-registers the legacy 47-tool static surface for rollback/back-compat.
STATIC_TOOLS = os.environ.get("STATIC_TOOLS", "0") != "0"

# ---------------------------------------------------------------- MCP app
# Accessible on the LAN + via Traefik at mcp.ffilt.us (Host header preserved).
# Allow the ffilt.us wildcard for the ingress host plus LAN IPs for direct hits.
mcp = FastMCP(
    "mcp",
    instructions=(
        "Unified homelab agent tools. Dynamic discovery:\n"
        "  discover_tool(query) — semantic search over the tool catalog;\n"
        "                        returns names, descriptions, input schemas\n"
        "  execute_tool(name, payload) — call any discovered tool by name\n"
        "  tool_catalog() — list every tool (name/family/one-liner)\n"
        "Workflow: discover_tool('manage pods') -> execute_tool('k3s_list_pods', {...}).\n"
        "Tool families: todo_* (task store w/ leases), docs_* (homelab-docs FTS5),\n"
        "k3s_* (read-only cluster queries), gitea_* (read-only Gitea API),\n"
        "storage_* (host storage via system calls), system_* (opaque health checks),\n"
        "browser_* (CDP browser session handoff — list/create/screenshot/interact/close).\n"
        "All tools return JSON strings. Prefer k3s_* over raw kubectl, system_* for health."
    ),
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            # exact (no-port) entries — Traefik forwards Host: mcp.ffilt.us
            "127.0.0.1", "localhost", "[::1]",
            "192.168.1.2", "192.168.1.50", "192.168.1.150",
            "mcp.ffilt.us",
            # port-qualified + wildcard (SDK matches base+":" prefix)
            "127.0.0.1:*", "localhost:*", "[::1]:*",
            "192.168.1.2:*", "192.168.1.50:*", "192.168.1.150:*",
            "mcp.ffilt.us:*", "*.ffilt.us:*",
        ],
        allowed_origins=[
            "http://127.0.0.1", "http://localhost", "http://[::1]",
            "http://192.168.1.2", "http://192.168.1.50", "http://192.168.1.150",
            "http://mcp.ffilt.us",
            "http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*",
            "http://192.168.1.2:*", "http://192.168.1.50:*", "http://192.168.1.150:*",
            "http://mcp.ffilt.us:*", "http://*.ffilt.us:*",
        ],
    ),
)


def _ok(data: dict | list) -> str:
    return json.dumps(data)


# ------------------------------------------------- dynamic tool plumbing
registry = ToolRegistry()
tool_index = ToolIndex(registry)


def agent_tool(family: str, always_static: bool = False):
    """Register a tool in the dynamic registry; expose it in the static MCP
    tool list when STATIC_TOOLS is on (or always for meta tools)."""

    def deco(fn):
        registry.register(fn, family=family)
        if always_static or STATIC_TOOLS:
            mcp.add_tool(fn)
        return fn

    return deco


# ================================================================ meta_* (microAgent)
@agent_tool(family="omni", always_static=True)
def omni(query: str, execute: bool = False) -> str:
    """microAgent — natural language -> tool calls on this MCP server. Runs a
    short agentic loop over the full registered tool catalog (tool-calling
    against the LLM gateway). execute=False (default): read-only docs lookups
    allowed, mutating tools only PROPOSED — returns instructions / proposed
    call chain. execute=True: actually executes tool calls (any registered tool)
    and returns real results. Use for 'how do I...' (explain) and
    'tell/do X...' (execute=True) requests."""
    import omni as omni_mod
    return _ok(omni_mod.run_omni(registry, query, execute=execute))


@agent_tool(family="meta", always_static=True)
def discover_tool(query: str, top_k: int = 5, min_score: float = 0.2) -> str:
    """Semantic search over the tool catalog (embedding DB). Returns ranked
    tools with name, family, description and input_schema for queries like
    'manage pods' or 'gitea pull requests'. Falls back to keyword matching
    when the embeddings service is unreachable."""
    try:
        q = (query or "").strip()
        if not q:
            return _ok({"query": query, "error": "query required"})
        res = tool_index.search(q, top_k=top_k, min_score=min_score)
        hits = []
        for r in res["results"]:
            try:
                schema = json.loads(r["schema"])
            except Exception:  # noqa: BLE001
                schema = {}
            hits.append(
                {
                    "name": r["name"],
                    "family": r["family"],
                    "description": r["description"],
                    "input_schema": schema,
                    "score": r["score"],
                }
            )
        return _ok({"query": q, "mode": res["mode"], "count": len(hits), "results": hits})
    except Exception as exc:  # noqa: BLE001
        return _ok({"query": query, "error": f"{type(exc).__name__}: {exc}"})


@agent_tool(family="meta", always_static=True)
def execute_tool(tool_name: str, payload: dict | None = None) -> str:
    """Call any registered tool by exact name with a JSON payload (validated
    against the tool's input schema). Returns {"ok": true, "tool": ...,
    "result": ...} on success, or {"ok": false, "tool": ..., "error":
    {"type": ..., "message": ...}} on unknown tool / invalid payload / failure.
    Discover names + payload shapes with discover_tool()."""
    try:
        raw = registry.call(tool_name, payload or {})
    except ToolError as exc:
        return _ok({"ok": False, "tool": tool_name, "error": {"type": exc.kind, "message": exc.message}})
    except Exception as exc:  # noqa: BLE001
        return _ok({"ok": False, "tool": tool_name, "error": {"type": "tool_error", "message": f"{type(exc).__name__}: {exc}"}})
    try:
        result = json.loads(raw)
    except Exception:  # noqa: BLE001
        result = raw
    return _ok({"ok": True, "tool": tool_name, "result": result})


@agent_tool(family="meta", always_static=True)
def tool_catalog() -> str:
    """List every registered tool: name, family, one-line description (no input schemas)."""
    return _ok({"count": len(registry.entries()), "tools": registry.catalog()})


# ================================================================ todo_*
@agent_tool(family="todo")
def todo_create(title: str, description: str = "", dependencies: list[int] | None = None,
                priority: str = "normal", ttl_seconds: int | None = None) -> str:
    """Create a todo. Returns the task with its id. dependencies are ids of tasks
    that must be completed before this one can be claimed; cycles are rejected."""
    try:
        return _ok(store.create(title, description, dependencies, priority, ttl_seconds))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_list(status: str | None = None, owner: str | None = None) -> str:
    """List todos. status: open|claimed|completed. owner filters by claim owner.
    Expired leases are auto-returned to open first."""
    try:
        return _ok(store.list(status=status, owner=owner))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_get(todo_id: int) -> str:
    """Get a single todo by id."""
    try:
        return _ok(store.get(todo_id))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_claim(todo_id: int, owner: str, ttl_seconds: int | None = None) -> str:
    """Atomically claim an open todo for owner. Requires all dependencies completed.
    Returns a lease_token that must be used to complete/release/renew. The lease
    expires after ttl_seconds (default task ttl, min 10) — if it expires, the task
    returns to open and anyone else can claim it."""
    try:
        return _ok(store.claim(todo_id, owner, ttl_seconds))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_complete(todo_id: int, lease_token: str) -> str:
    """Mark a claimed todo completed. Requires the lease_token from todo_claim."""
    try:
        return _ok(store.complete(todo_id, lease_token))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_release(todo_id: int, lease_token: str) -> str:
    """Give up a lease early (returns the todo to open). Requires lease_token."""
    try:
        return _ok(store.release(todo_id, lease_token))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_renew(todo_id: int, lease_token: str, ttl_seconds: int | None = None) -> str:
    """Extend a claim's lease. Requires lease_token. Returns the updated task."""
    try:
        return _ok(store.renew(todo_id, lease_token, ttl_seconds=ttl_seconds))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_delete(todo_id: int) -> str:
    """Delete a todo. Refuses if other tasks depend on it."""
    try:
        return _ok(store.delete(todo_id))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@agent_tool(family="todo")
def todo_summary() -> str:
    """Counts by status + total expirations."""
    return _ok(store.summary())


@agent_tool(family="todo")
def todo_events(todo_id: int | None = None, limit: int = 50) -> str:
    """Audit trail of all actions (created/claimed/renewed/completed/released/expired/deleted)."""
    return _ok(store.events(todo_id=todo_id, limit=limit))


@agent_tool(family="todo")
def todo_mirror_status() -> str:
    """Gitea mirror status: enabled, target repo, label readiness, last reconcile."""
    return _ok(gitea_mirror.status())


@agent_tool(family="todo")
def todo_mirror_reconcile() -> str:
    """Force a full reconcile sweep against Gitea: create missing issues, fix
    drift (labels/assignee/state/due date). Returns created/updated counts."""
    if gitea_mirror.mirror is None:
        return _ok({"error": "mirror_disabled", "message": "GITEA_MIRROR_TOKEN not set"})
    try:
        return _ok(gitea_mirror.mirror.reconcile(store.list()))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


# ================================================================ docs_*
@agent_tool(family="docs")
def docs_search(query: str, limit: int = 8, mode: str = "and") -> str:
    """Full-text search over the homelab-docs repo (architecture + ops docs).
    Returns ranked hits with path, title, score, and a snippet around the match."""
    return docs_index.search(query, limit=limit, mode=mode)


@agent_tool(family="docs")
def docs_get(path: str) -> str:
    """Return the full content of a doc by repo-relative path (e.g. ops/rook-soul.md)."""
    return docs_index.get_doc(path)


@agent_tool(family="docs")
def docs_list(pattern: str = "") -> str:
    """List all indexed docs. Optional substring pattern on path/title."""
    return docs_index.list_docs(pattern=pattern)


@agent_tool(family="docs")
def docs_index_status() -> str:
    """Index health: file count, last scans/rebuilds, watcher state, git pull state."""
    return docs_index.index_status()


@agent_tool(family="docs")
def docs_refresh_index() -> str:
    """Force a full reindex of the docs repo (also picks up a freshly cloned repo dir)."""
    return docs_index.refresh_index()


# ================================================================ k3s_*
@agent_tool(family="k3s")
def k3s_nodes() -> str:
    """List cluster nodes: name, status (Ready), roles, k8s version, internal IP, age."""
    return k3s_tools.nodes()


@agent_tool(family="k3s")
def k3s_namespaces() -> str:
    """List namespaces with status and age."""
    return k3s_tools.namespaces()


@agent_tool(family="k3s")
def k3s_list_services(namespace: str | None = None) -> str:
    """List Services (ClusterIP/LoadBalancer etc). namespace=None means all namespaces."""
    return k3s_tools.list_services(namespace=namespace)


@agent_tool(family="k3s")
def k3s_get_service(name: str, namespace: str) -> str:
    """Get one Service by name (namespace required)."""
    return k3s_tools.get_service(name, namespace)


@agent_tool(family="k3s")
def k3s_list_pods(namespace: str | None = None, label_selector: str | None = None) -> str:
    """List Pods. namespace=None means all namespaces. label_selector e.g. 'app=talaria'."""
    return k3s_tools.list_pods(namespace=namespace, label_selector=label_selector)


@agent_tool(family="k3s")
def k3s_get_pod(name: str, namespace: str) -> str:
    """Get one Pod by name (namespace required). Includes container statuses."""
    return k3s_tools.get_pod(name, namespace)


@agent_tool(family="k3s")
def k3s_pod_logs(name: str, namespace: str, tail: int = 100, container: str | None = None) -> str:
    """Tail container logs of a pod. tail = number of lines (default 100)."""
    return k3s_tools.pod_logs(name, namespace, tail=tail, container=container)


@agent_tool(family="k3s")
def k3s_list_deployments(namespace: str | None = None) -> str:
    """List Deployments with desired/ready replicas."""
    return k3s_tools.list_deployments(namespace=namespace)


@agent_tool(family="k3s")
def k3s_get_deployment(name: str, namespace: str) -> str:
    """Get one Deployment by name (namespace required)."""
    return k3s_tools.get_deployment(name, namespace)


@agent_tool(family="k3s")
def k3s_list_statefulsets(namespace: str | None = None) -> str:
    """List StatefulSets with ready replicas."""
    return k3s_tools.list_statefulsets(namespace=namespace)


@agent_tool(family="k3s")
def k3s_list_daemonsets(namespace: str | None = None) -> str:
    """List DaemonSets with ready replicas."""
    return k3s_tools.list_daemonsets(namespace=namespace)


@agent_tool(family="k3s")
def k3s_list_ingressroutes(namespace: str | None = None) -> str:
    """List Traefik IngressRoutes with their Host() match rules (the ffilt.us routing table)."""
    return k3s_tools.list_ingressroutes(namespace=namespace)


@agent_tool(family="k3s")
def k3s_list_pvcs(namespace: str | None = None) -> str:
    """List PersistentVolumeClaims with status, storage class, capacity."""
    return k3s_tools.list_pvcs(namespace=namespace)


@agent_tool(family="k3s")
def k3s_list_storageclasses() -> str:
    """List StorageClasses (longhorn, nfs-media, local-path, ...)."""
    return k3s_tools.list_storageclasses()


@agent_tool(family="k3s")
def k3s_list_events(namespace: str | None = None, limit: int = 50) -> str:
    """Recent Events (warnings usually first). limit max 200."""
    return k3s_tools.list_events(namespace=namespace, limit=limit)


# ================================================================ gitea_*
@agent_tool(family="gitea")
def gitea_list_repos(owner: str = "reeds") -> str:
    """List Gitea repos for an owner (default reeds): name, description, private,
    default branch, updated time, issue/star/fork counts."""
    return gitea_tools.list_repos(owner=owner)


@agent_tool(family="gitea")
def gitea_get_repo(owner: str, name: str) -> str:
    """Get one Gitea repo (owner/name): clone URLs, size, counts, timestamps."""
    return gitea_tools.get_repo(owner, name)


@agent_tool(family="gitea")
def gitea_list_prs(owner: str, repo: str, state: str = "open", limit: int = 20) -> str:
    """List pull requests (state: open|closed|all). Includes mergeable when known."""
    return gitea_tools.list_prs(owner, repo, state=state, limit=limit)


@agent_tool(family="gitea")
def gitea_get_pr(owner: str, repo: str, number: int) -> str:
    """Get one pull request by number: full detail incl. mergeability + files changed."""
    return gitea_tools.get_pr(owner, repo, number)


@agent_tool(family="gitea")
def gitea_list_issues(owner: str, repo: str, state: str = "open", limit: int = 20) -> str:
    """List issues (state: open|closed|all). Pull requests are excluded (type=issues)."""
    return gitea_tools.list_issues(owner, repo, state=state, limit=limit)


@agent_tool(family="gitea")
def gitea_get_issue(owner: str, repo: str, number: int) -> str:
    """Get one issue by number: full detail incl. body + labels + comments count."""
    return gitea_tools.get_issue(owner, repo, number)


@agent_tool(family="gitea")
def gitea_list_action_runs(owner: str, repo: str, limit: int = 10) -> str:
    """List Gitea Actions workflow runs for a repo: id, name, status, conclusion, sha."""
    return gitea_tools.list_action_runs(owner, repo, limit=limit)


@agent_tool(family="gitea")
def gitea_get_action_run(owner: str, repo: str, run_id: int) -> str:
    """Get one Actions run by id, including its jobs (name, status, conclusion)."""
    return gitea_tools.get_action_run(owner, repo, run_id)


@agent_tool(family="gitea")
def gitea_action_run_logs(owner: str, repo: str, run_id: int, job_id: int | None = None) -> str:
    """Fetch Actions run logs (raw text, last ~8k chars per job). If job_id omitted,
    concatenates all jobs of the run."""
    return gitea_tools.action_run_logs(owner, repo, run_id, job_id=job_id)


@agent_tool(family="gitea")
def gitea_list_contents(owner: str, repo: str, path: str = "", ref: str = "") -> str:
    """List a directory in a repo (or describe a file). ref = branch/tag/sha."""
    return gitea_tools.list_contents(owner, repo, path=path, ref=ref)


@agent_tool(family="gitea")
def gitea_get_file(owner: str, repo: str, path: str, ref: str = "") -> str:
    """Get the full content of one file in a repo (decoded). ref = branch/tag/sha."""
    return gitea_tools.get_file(owner, repo, path, ref=ref)


# ================================================================ storage_*
@agent_tool(family="storage")
def storage_disk_usage() -> str:
    """df on the host: real filesystems with size/used/avail/pct (system call)."""
    return storage_tools.disk_usage()


@agent_tool(family="storage")
def storage_raid_status() -> str:
    """mdadm RAID array status from the host (/proc/mdstat + mdadm --detail)."""
    return storage_tools.raid_status()


@agent_tool(family="storage")
def storage_smart_health() -> str:
    """SMART health per physical disk on the host (smartctl -H; slower — ~seconds/disk)."""
    return storage_tools.smart_health()


@agent_tool(family="storage")
def storage_disks() -> str:
    """Physical disks on the host from lsblk (name, size, model) — fast, no smartctl."""
    return storage_tools.disks()


# ================================================================ system_*
@agent_tool(family="system")
def system_list_checks() -> str:
    """List available system health checks (lowercase-hyphen ids) with descriptions.
    Check ids include: router-alive (OPNsense ping), ping-nodes, wan-access
    (Cloudflare edge download), disk-space, cpu-load, memory-pressure,
    node-cpu, node-memory."""
    return system_checks.list_checks()


@agent_tool(family="system")
def system_check_status(checks: list[str] | None = None) -> str:
    """Run opaque system health checks. checks is a list of check ids
    (lowercase-hyphen, see system_list_checks); empty/null runs ALL checks.
    Each result: {id, status: pass|warn|fail|error, detail, took_ms}.
    Examples: ['router-alive', 'wan-access', 'disk-space', 'node-cpu'] —
    wan-access for internet/WAN reachability, router-alive for the OPNsense
    router, disk-space for host storage."""
    return system_checks.check_status(checks)


# ================================================================ browser_*
@agent_tool(family="browser")
def browser_session_list() -> str:
    """List active Rayobrowse browser sessions: session IDs, status, VNC URLs,
    current page URL/title. Use these session IDs with the other browser tools."""
    return browser_tools.list_sessions()


@agent_tool(family="browser")
def browser_session_create(os_type: str = "windows", headless: bool = True,
                            vnc: bool = True, url: str = "") -> str:
    """Create a new Rayobrowse browser session via CDP. Returns session ID,
    CDP WebSocket URL, and VNC URL (for human viewing). os_type: windows|linux.
    Optionally pass a url to navigate to on creation."""
    return browser_tools.create_session(os_type=os_type, headless=headless,
                                        vnc=vnc, url=url)


@agent_tool(family="browser")
def browser_session_screenshot(session_id: str) -> str:
    """Capture a screenshot of the browser session via CDP. Returns base64-encoded
    PNG image data. Use this to see the current state of the browser."""
    return browser_tools.take_screenshot(session_id)


@agent_tool(family="browser")
def browser_session_navigate(session_id: str, url: str) -> str:
    """Navigate the browser session to a URL via CDP. Handles cert bypass for
    self-signed sites (OPNsense, etc). Returns the current URL and title after
    navigation."""
    return browser_tools.navigate(session_id, url)


@agent_tool(family="browser")
def browser_session_interact(session_id: str, action: str,
                              x: int = 0, y: int = 0,
                              text: str = "", key: str = "", code: str = "",
                              delta_x: int = 0, delta_y: int = 0) -> str:
    """Send input events to the browser session via CDP. Actions:
    click (x,y), type (text), key (key,code), scroll (x,y,delta_x,delta_y),
    hover (x,y). Use this for automated browser interaction — form filling,
    button clicking, scrolling."""
    kwargs: dict = {}
    if action == "click":
        kwargs = {"x": x, "y": y}
    elif action == "type":
        kwargs = {"text": text}
    elif action == "key":
        kwargs = {"key": key, "code": code}
    elif action == "scroll":
        kwargs = {"x": x, "y": y, "delta_x": delta_x, "delta_y": delta_y}
    elif action == "hover":
        kwargs = {"x": x, "y": y}
    return browser_tools.interact(session_id, action, **kwargs)


@agent_tool(family="browser")
def browser_session_close(session_id: str) -> str:
    """Close a Rayobrowse browser session. Frees the browser instance and
    releases resources."""
    return browser_tools.close_session(session_id)


# The MCP Starlette app serves its own streamable_http_path (/mcp by default)
# and needs its session-manager task group started via lifespan. Starlette's
# Mount does NOT propagate sub-app lifespans, so we run the manager from
# FastAPI's own lifespan, then mount at root so the /mcp path survives.
mcp_starlette = mcp.streamable_http_app()
mcp_session_manager = mcp._session_manager


@asynccontextmanager
async def lifespan(_app):
    # Bring the docs mirror up (clone if missing), index it, start watchers.
    clone = docs_index.ensure_repo()
    if not clone.get("ok"):
        log.warning("docs repo not ready: %s", clone.get("error"))
    try:
        docs_index.full_rebuild()
    except Exception as exc:  # noqa: BLE001
        log.warning("docs initial rebuild failed: %s", exc)
    docs_index.start_watchers()
    # Build the tool embedding index (rebuilds only if empty/stale — a failed
    # embed just degrades discovery to keyword mode; never blocks startup).
    try:
        log.info("tool index: %s", tool_index.ensure_index())
    except Exception as exc:  # noqa: BLE001
        log.warning("tool index init failed: %s", exc)
    async with mcp_session_manager.run():
        yield


# ---------------------------------------------------------------- REST app
app = FastAPI(title="mcp — unified agent tools", version="2.1.1", lifespan=lifespan)

# ------------------------------------------------------------------ auth
# Reed 2026-09-11: agent runtimes on sutliffserver (the deploy host) call the
# MCP keyless — authorization is determined by source IP. Everything else
# (LAN, tunnel) must present the Bearer key. Behind Traefik the pod sees the
# svclb NAT address (10.42.0.1), so trusted-source identity only holds for
# HOST-DIRECT connections — clients on the box target the pinned ClusterIP.
import ipaddress
from starlette.responses import JSONResponse

MCP_API_KEY = os.environ.get("MCP_API_KEY", "").strip()
_TRUSTED = [
    ipaddress.ip_network(c.strip())
    for c in os.environ.get("MCP_TRUSTED_CIDRS", "127.0.0.1/32,::1/128,192.168.1.2/32").split(",")
    if c.strip()
]


def _client_is_trusted(request) -> bool:
    try:
        addr = ipaddress.ip_address(request.client.host)
    except (ValueError, AttributeError, TypeError):
        return False
    return any(addr in net for net in _TRUSTED)


# NOTE: added BEFORE CORSMiddleware so CORS stays outermost and answers
# preflights without a key.
@app.middleware("http")
async def key_guard(request, call_next):
    if MCP_API_KEY and request.url.path != "/health":
        if not _client_is_trusted(request):
            supplied = request.headers.get("authorization", "")
            if supplied != f"Bearer {MCP_API_KEY}":
                return JSONResponse(
                    {"error": {"message": "unauthorized (trusted-source or Bearer key required)",
                               "type": "invalid_api_key", "code": "invalid_api_key"}},
                    status_code=401)
    response = await call_next(request)
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class CreateTodo(BaseModel):
    title: str = Field(min_length=1)
    description: str = ""
    dependencies: list[int] = []
    priority: str = "normal"
    ttl_seconds: int | None = None


class ClaimTodo(BaseModel):
    owner: str = Field(min_length=1)
    ttl_seconds: int | None = None


class TokenBody(BaseModel):
    lease_token: str = Field(min_length=1)


class RenewBody(BaseModel):
    lease_token: str = Field(min_length=1)
    ttl_seconds: int | None = None


def _http(e: TodoError) -> HTTPException:
    status = {
        "bad_request": 400,
        "not_found": 404,
        "conflict": 409,
        "blocked": 409,
        "cycle": 400,
        "forbidden": 403,
    }.get(e.code, 400)
    return HTTPException(status_code=status, detail=e.message)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "mcp",
        "tools": ["discover_tool", "execute_tool", "tool_catalog"] + (["todo_*", "docs_*", "k3s_*", "gitea_*", "storage_*", "system_*", "browser_*"] if STATIC_TOOLS else []),
        "static_tools": STATIC_TOOLS,
    }


@app.get("/")
def root():
    return {
        "service": "mcp",
        "mcp_endpoint": "/mcp",
        "dynamic": ["discover_tool", "execute_tool", "tool_catalog"],
        "static_tools": STATIC_TOOLS,
        "tool_families": ["todo_*", "docs_*", "k3s_*", "gitea_*", "storage_*", "system_*", "browser_*"],
        "docs": "mcp.ffilt.us — unified agent tools (dynamic discover/execute)",
    }


@app.get("/api/tools")
def api_tools():
    return {"count": len(registry.entries()), "tools": registry.catalog()}


@app.post("/api/tools/reindex")
def api_tools_reindex():
    return tool_index.rebuild()


@app.get("/api/tools/status")
def api_tools_status():
    return tool_index.status()


@app.get("/api/summary")
def api_summary():
    return store.summary()


@app.get("/api/todos")
def api_list(status: str | None = None, owner: str | None = None):
    try:
        return store.list(status=status, owner=owner)
    except TodoError as e:
        raise _http(e)


@app.post("/api/todos", status_code=201)
def api_create(body: CreateTodo):
    try:
        return store.create(
            title=body.title,
            description=body.description,
            dependencies=body.dependencies,
            priority=body.priority,
            ttl_seconds=body.ttl_seconds,
        )
    except TodoError as e:
        raise _http(e)


@app.get("/api/todos/{todo_id}")
def api_get(todo_id: int):
    try:
        return store.get(todo_id)
    except TodoError as e:
        raise _http(e)


@app.get("/api/todos/{todo_id}/events")
def api_events(todo_id: int, limit: int = 50):
    return store.events(todo_id=todo_id, limit=limit)


@app.post("/api/todos/{todo_id}/claim")
def api_claim(todo_id: int, body: ClaimTodo):
    try:
        return store.claim(todo_id, owner=body.owner, ttl_seconds=body.ttl_seconds)
    except TodoError as e:
        raise _http(e)


@app.post("/api/todos/{todo_id}/complete")
def api_complete(todo_id: int, body: TokenBody):
    try:
        return store.complete(todo_id, body.lease_token)
    except TodoError as e:
        raise _http(e)


@app.post("/api/todos/{todo_id}/release")
def api_release(todo_id: int, body: TokenBody):
    try:
        return store.release(todo_id, body.lease_token)
    except TodoError as e:
        raise _http(e)


@app.post("/api/todos/{todo_id}/renew")
def api_renew(todo_id: int, body: RenewBody):
    try:
        return store.renew(todo_id, body.lease_token, ttl_seconds=body.ttl_seconds)
    except TodoError as e:
        raise _http(e)


@app.delete("/api/todos/{todo_id}")
def api_delete(todo_id: int):
    try:
        return store.delete(todo_id)
    except TodoError as e:
        raise _http(e)


# MCP app served at /mcp (its own internal route). Mount at root.
app.mount("/", mcp_starlette)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
