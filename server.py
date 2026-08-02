"""mcp — unified agent MCP service (todo.*, docs.*, k3s.* tools).

Single uvicorn process serving:
  - GET  /health                     liveness
  - GET  /                           service info
  - GET  /api/summary, /api/todos…   REST passthrough for the todos store
                                      (kept from the old host service so
                                      existing consumers don't break)
  - MCP  /mcp                        Streamable HTTP — tools prefixed
                                      todo_*, docs_*, k3s_*

Runs in k3s (ns `mcp`), exposed at http://mcp.ffilt.us/mcp. Data lives on a
Longhorn PVC at /data (todos.db, docs mirror + index). k3s tools use the
in-cluster ServiceAccount (read-only RBAC).
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

import docs_index
import gitea_tools
import k3s_tools
import storage_tools
import system_checks
from todos_store import DEFAULT_TTL, TodoError, TodosStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("mcp")

PORT = int(os.environ.get("PORT", "8650"))
DB_PATH = Path(os.environ.get("TODOS_DB", "/data/todos.db"))
store = TodosStore(DB_PATH)

# ---------------------------------------------------------------- MCP app
# Accessible on the LAN + via Traefik at mcp.ffilt.us (Host header preserved).
# Allow the ffilt.us wildcard for the ingress host plus LAN IPs for direct hits.
mcp = FastMCP(
    "mcp",
    instructions=(
        "Unified homelab agent tools. Prefixes:\n"
        "  todo_*  — shared task store with leases (create/claim/complete/list)\n"
        "  docs_*  — full-text search over the homelab-docs repo (search/get/list)\n"
        "  k3s_*   — read-only Kubernetes cluster queries (services/pods/nodes/...)\n"
        "  gitea_* — read-only Gitea queries (repos/PRs/issues/actions/files)\n"
        "  storage_* — host storage via system calls (df/mdadm/smartctl)\n"
        "  system_*  — opaque health checks (system_check_status / system_list_checks)\n"
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


# ================================================================ todo_*
@mcp.tool()
def todo_create(title: str, description: str = "", dependencies: list[int] | None = None,
                priority: str = "normal", ttl_seconds: int | None = None) -> str:
    """Create a todo. Returns the task with its id. dependencies are ids of tasks
    that must be completed before this one can be claimed; cycles are rejected."""
    try:
        return _ok(store.create(title, description, dependencies, priority, ttl_seconds))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_list(status: str | None = None, owner: str | None = None) -> str:
    """List todos. status: open|claimed|completed. owner filters by claim owner.
    Expired leases are auto-returned to open first."""
    try:
        return _ok(store.list(status=status, owner=owner))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_get(todo_id: int) -> str:
    """Get a single todo by id."""
    try:
        return _ok(store.get(todo_id))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_claim(todo_id: int, owner: str, ttl_seconds: int | None = None) -> str:
    """Atomically claim an open todo for owner. Requires all dependencies completed.
    Returns a lease_token that must be used to complete/release/renew. The lease
    expires after ttl_seconds (default task ttl, min 10) — if it expires, the task
    returns to open and anyone else can claim it."""
    try:
        return _ok(store.claim(todo_id, owner, ttl_seconds))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_complete(todo_id: int, lease_token: str) -> str:
    """Mark a claimed todo completed. Requires the lease_token from todo_claim."""
    try:
        return _ok(store.complete(todo_id, lease_token))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_release(todo_id: int, lease_token: str) -> str:
    """Give up a lease early (returns the todo to open). Requires lease_token."""
    try:
        return _ok(store.release(todo_id, lease_token))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_renew(todo_id: int, lease_token: str, ttl_seconds: int | None = None) -> str:
    """Extend a claim's lease. Requires lease_token. Returns the updated task."""
    try:
        return _ok(store.renew(todo_id, lease_token, ttl_seconds=ttl_seconds))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_delete(todo_id: int) -> str:
    """Delete a todo. Refuses if other tasks depend on it."""
    try:
        return _ok(store.delete(todo_id))
    except TodoError as e:
        return _ok({"error": e.code, "message": e.message})


@mcp.tool()
def todo_summary() -> str:
    """Counts by status + total expirations."""
    return _ok(store.summary())


@mcp.tool()
def todo_events(todo_id: int | None = None, limit: int = 50) -> str:
    """Audit trail of all actions (created/claimed/renewed/completed/released/expired/deleted)."""
    return _ok(store.events(todo_id=todo_id, limit=limit))


# ================================================================ docs_*
@mcp.tool()
def docs_search(query: str, limit: int = 8, mode: str = "and") -> str:
    """Full-text search over the homelab-docs repo (architecture + ops docs).
    Returns ranked hits with path, title, score, and a snippet around the match."""
    return docs_index.search(query, limit=limit, mode=mode)


@mcp.tool()
def docs_get(path: str) -> str:
    """Return the full content of a doc by repo-relative path (e.g. ops/rook-soul.md)."""
    return docs_index.get_doc(path)


@mcp.tool()
def docs_list(pattern: str = "") -> str:
    """List all indexed docs. Optional substring pattern on path/title."""
    return docs_index.list_docs(pattern=pattern)


@mcp.tool()
def docs_index_status() -> str:
    """Index health: file count, last scans/rebuilds, watcher state, git pull state."""
    return docs_index.index_status()


@mcp.tool()
def docs_refresh_index() -> str:
    """Force a full reindex of the docs repo (also picks up a freshly cloned repo dir)."""
    return docs_index.refresh_index()


# ================================================================ k3s_*
@mcp.tool()
def k3s_nodes() -> str:
    """List cluster nodes: name, status (Ready), roles, k8s version, internal IP, age."""
    return k3s_tools.nodes()


@mcp.tool()
def k3s_namespaces() -> str:
    """List namespaces with status and age."""
    return k3s_tools.namespaces()


@mcp.tool()
def k3s_list_services(namespace: str | None = None) -> str:
    """List Services (ClusterIP/LoadBalancer etc). namespace=None means all namespaces."""
    return k3s_tools.list_services(namespace=namespace)


@mcp.tool()
def k3s_get_service(name: str, namespace: str) -> str:
    """Get one Service by name (namespace required)."""
    return k3s_tools.get_service(name, namespace)


@mcp.tool()
def k3s_list_pods(namespace: str | None = None, label_selector: str | None = None) -> str:
    """List Pods. namespace=None means all namespaces. label_selector e.g. 'app=talaria'."""
    return k3s_tools.list_pods(namespace=namespace, label_selector=label_selector)


@mcp.tool()
def k3s_get_pod(name: str, namespace: str) -> str:
    """Get one Pod by name (namespace required). Includes container statuses."""
    return k3s_tools.get_pod(name, namespace)


@mcp.tool()
def k3s_pod_logs(name: str, namespace: str, tail: int = 100, container: str | None = None) -> str:
    """Tail container logs of a pod. tail = number of lines (default 100)."""
    return k3s_tools.pod_logs(name, namespace, tail=tail, container=container)


@mcp.tool()
def k3s_list_deployments(namespace: str | None = None) -> str:
    """List Deployments with desired/ready replicas."""
    return k3s_tools.list_deployments(namespace=namespace)


@mcp.tool()
def k3s_get_deployment(name: str, namespace: str) -> str:
    """Get one Deployment by name (namespace required)."""
    return k3s_tools.get_deployment(name, namespace)


@mcp.tool()
def k3s_list_statefulsets(namespace: str | None = None) -> str:
    """List StatefulSets with ready replicas."""
    return k3s_tools.list_statefulsets(namespace=namespace)


@mcp.tool()
def k3s_list_daemonsets(namespace: str | None = None) -> str:
    """List DaemonSets with ready replicas."""
    return k3s_tools.list_daemonsets(namespace=namespace)


@mcp.tool()
def k3s_list_ingressroutes(namespace: str | None = None) -> str:
    """List Traefik IngressRoutes with their Host() match rules (the ffilt.us routing table)."""
    return k3s_tools.list_ingressroutes(namespace=namespace)


@mcp.tool()
def k3s_list_pvcs(namespace: str | None = None) -> str:
    """List PersistentVolumeClaims with status, storage class, capacity."""
    return k3s_tools.list_pvcs(namespace=namespace)


@mcp.tool()
def k3s_list_storageclasses() -> str:
    """List StorageClasses (longhorn, nfs-media, local-path, ...)."""
    return k3s_tools.list_storageclasses()


@mcp.tool()
def k3s_list_events(namespace: str | None = None, limit: int = 50) -> str:
    """Recent Events (warnings usually first). limit max 200."""
    return k3s_tools.list_events(namespace=namespace, limit=limit)


# ================================================================ gitea_*
@mcp.tool()
def gitea_list_repos(owner: str = "reeds") -> str:
    """List Gitea repos for an owner (default reeds): name, description, private,
    default branch, updated time, issue/star/fork counts."""
    return gitea_tools.list_repos(owner=owner)


@mcp.tool()
def gitea_get_repo(owner: str, name: str) -> str:
    """Get one Gitea repo (owner/name): clone URLs, size, counts, timestamps."""
    return gitea_tools.get_repo(owner, name)


@mcp.tool()
def gitea_list_prs(owner: str, repo: str, state: str = "open", limit: int = 20) -> str:
    """List pull requests (state: open|closed|all). Includes mergeable when known."""
    return gitea_tools.list_prs(owner, repo, state=state, limit=limit)


@mcp.tool()
def gitea_get_pr(owner: str, repo: str, number: int) -> str:
    """Get one pull request by number: full detail incl. mergeability + files changed."""
    return gitea_tools.get_pr(owner, repo, number)


@mcp.tool()
def gitea_list_issues(owner: str, repo: str, state: str = "open", limit: int = 20) -> str:
    """List issues (state: open|closed|all). Pull requests are excluded (type=issues)."""
    return gitea_tools.list_issues(owner, repo, state=state, limit=limit)


@mcp.tool()
def gitea_get_issue(owner: str, repo: str, number: int) -> str:
    """Get one issue by number: full detail incl. body + labels + comments count."""
    return gitea_tools.get_issue(owner, repo, number)


@mcp.tool()
def gitea_list_action_runs(owner: str, repo: str, limit: int = 10) -> str:
    """List Gitea Actions workflow runs for a repo: id, name, status, conclusion, sha."""
    return gitea_tools.list_action_runs(owner, repo, limit=limit)


@mcp.tool()
def gitea_get_action_run(owner: str, repo: str, run_id: int) -> str:
    """Get one Actions run by id, including its jobs (name, status, conclusion)."""
    return gitea_tools.get_action_run(owner, repo, run_id)


@mcp.tool()
def gitea_action_run_logs(owner: str, repo: str, run_id: int, job_id: int | None = None) -> str:
    """Fetch Actions run logs (raw text, last ~8k chars per job). If job_id omitted,
    concatenates all jobs of the run."""
    return gitea_tools.action_run_logs(owner, repo, run_id, job_id=job_id)


@mcp.tool()
def gitea_list_contents(owner: str, repo: str, path: str = "", ref: str = "") -> str:
    """List a directory in a repo (or describe a file). ref = branch/tag/sha."""
    return gitea_tools.list_contents(owner, repo, path=path, ref=ref)


@mcp.tool()
def gitea_get_file(owner: str, repo: str, path: str, ref: str = "") -> str:
    """Get the full content of one file in a repo (decoded). ref = branch/tag/sha."""
    return gitea_tools.get_file(owner, repo, path, ref=ref)


# ================================================================ storage_*
@mcp.tool()
def storage_disk_usage() -> str:
    """df on the host: real filesystems with size/used/avail/pct (system call)."""
    return storage_tools.disk_usage()


@mcp.tool()
def storage_raid_status() -> str:
    """mdadm RAID array status from the host (/proc/mdstat + mdadm --detail)."""
    return storage_tools.raid_status()


@mcp.tool()
def storage_smart_health() -> str:
    """SMART health per physical disk on the host (smartctl -H; slower — ~seconds/disk)."""
    return storage_tools.smart_health()


@mcp.tool()
def storage_disks() -> str:
    """Physical disks on the host from lsblk (name, size, model) — fast, no smartctl."""
    return storage_tools.disks()


# ================================================================ system_*
@mcp.tool()
def system_list_checks() -> str:
    """List available system health checks (lowercase-hyphen ids) with descriptions."""
    return system_checks.list_checks()


@mcp.tool()
def system_check_status(checks: list[str] | None = None) -> str:
    """Run opaque system health checks. checks is a list of check ids
    (lowercase-hyphen, see system_list_checks); empty/null runs ALL checks.
    Each result: {id, status: pass|warn|fail|error, detail, took_ms}."""
    return system_checks.check_status(checks)


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
    async with mcp_session_manager.run():
        yield


# ---------------------------------------------------------------- REST app
app = FastAPI(title="mcp — unified agent tools", version="2.0.0", lifespan=lifespan)

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
    return {"status": "ok", "service": "mcp", "tools": ["todo_*", "docs_*", "k3s_*", "gitea_*", "storage_*", "system_*"]}


@app.get("/")
def root():
    return {
        "service": "mcp",
        "mcp_endpoint": "/mcp",
        "tool_families": ["todo_*", "docs_*", "k3s_*", "gitea_*", "storage_*", "system_*"],
        "docs": "mcp.ffilt.us — unified agent tools (todo/docs/k3s/gitea/storage/system)",
    }


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
