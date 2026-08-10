# mcp — unified agent MCP service

Single MCP StreamableHTTP server for agents, hosted on the k3s cluster at
`http://mcp.ffilt.us/mcp`.

## Dynamic tool surface (default since 1.3.0)

The MCP tool list is discoverable instead of fixed. Three meta tools are
always registered; the other 47 are found via semantic search over a tiny
embedding database (one row per tool: name, description, input schema, and a
384-dim MiniLM vector produced by the homelab `embeddings` service):

| Meta tool | Purpose |
|-----------|---------|
| `discover_tool(query, top_k?, min_score?)` | Semantic search over the catalog → ranked `{name, family, description, input_schema, score}`. Falls back to keyword matching if the embeddings service is down. |
| `execute_tool(tool_name, payload?)` | Validate `payload` against the discovered input schema and dispatch. Returns `{ok, tool, result}` or `{ok:false, tool, error:{type, message}}`. |
| `tool_catalog()` | Cheap enumeration: name / family / one-line description (no schemas). |

Workflow: `discover_tool('manage pods')` → `execute_tool('k3s_list_pods', {"namespace": "mcp"})`.

Tool families (all return JSON strings):

| Prefix  | Tools | Purpose |
|---------|-------|---------|
| `todo_*`  | create, list, get, claim, complete, release, renew, delete, summary, events, mirror_status, mirror_reconcile | Shared task store with leases/TTL (SQLite, Longhorn PVC) + optional best-effort Gitea issue mirror |
| `docs_*`  | search, get, list, index_status, refresh_index | FTS5 full-text search over `reeds/homelab-docs` mirror |
| `k3s_*`   | nodes, namespaces, list/get services, pods, logs, deployments, statefulsets, daemonsets, ingressroutes, pvcs, storageclasses, events | Read-only Kubernetes cluster queries (in-cluster SA RBAC) |
| `gitea_*` | repos, PRs, issues, actions runs/jobs/logs, contents, files | Read-only Gitea API |
| `storage_*` | disk_usage, raid_status, smart_health, disks | Host storage via system calls |
| `system_*` | list_checks, check_status | Opaque health checks |

Dynamic-only is the default (STATIC_TOOLS unset = "0"); set `STATIC_TOOLS=1`
to re-register the legacy static surface for rollback/back-compat. Both
surfaces draw from the same registry (`tool_registry.py`), so the embedding
index and the tool list can never drift.

## Run locally (dev)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
export TODOS_DB=/tmp/mcp-todos.db DOCS_REPO_DIR=/srv/homelab-docs-mirror DOCS_INDEX_DB=/tmp/mcp-docs-index.db
.venv/bin/python server.py        # :8650
```

## Container env

- `TODOS_DB` (default `/data/todos.db`)
- `DOCS_REPO_DIR` (default `/data/docs-mirror`), `DOCS_INDEX_DB` (default `/data/docs-index.db`)
- `DOCS_GIT_URL` — clone URL (creds embedded) used when the mirror is missing
- `DOCS_AUTO_PULL`, `DOCS_PULL_INTERVAL`, `DOCS_WATCH_INTERVAL`
- `TOOLS_INDEX_DB` (default `/data/tool-index.db`) — the tool embedding DB
- `EMBEDDINGS_URL` (default `http://embeddings.embeddings.svc.cluster.local`) — homelab embeddings service
- `EMBED_TIMEOUT` (default `10`) — HTTP timeout for embed calls
- `STATIC_TOOLS` (`1` default) — `0` disables the legacy static tool surface

## Gitea todo mirror (1.4.0)

Each todo is mirrored to exactly one issue in a dedicated Gitea repo
(default `reeds/homelab-todos`). SQLite stays authoritative; the mirror is
best-effort write-through (never blocks a todo op) plus a reconcile sweep
that runs at startup and on demand via `todo_mirror_reconcile`.

Mapping: `status:open|claimed|completed` + `priority:*` labels, assignee =
claim owner (only when the owner is a real Gitea user), due date = lease
expiry (informational; Gitea due dates are date-granular). Deleted todos
close their issue (never delete). Lease tokens are never mirrored.

Env (all optional — no token = mirror disabled):
- `GITEA_MIRROR_URL` (default `http://192.168.1.2:3000`)
- `GITEA_MIRROR_TOKEN` — Secret `mcp-gitea-mirror-token`; scopes
  `read:user,write:issue,write:repository`
- `GITEA_MIRROR_OWNER` / `GITEA_MIRROR_REPO` (defaults `reeds` / `homelab-todos`)

## Deploy

Manifests live in `reeds/sutliff-cluster` → `apps/mcp/` (Flux applies).
Image: `192.168.1.2:3000/reeds/mcp:<tag>`. Secrets (imperative, not in repo):
`mcp-docs-token` (DOCS_GIT_URL). k3s RBAC via in-cluster SA `mcp` +
ClusterRole `mcp-readonly` (see sutliff-cluster).
