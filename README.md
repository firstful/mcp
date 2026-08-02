# mcp — unified agent MCP service

Single MCP StreamableHTTP server for agents, hosted on the k3s cluster at
`http://mcp.ffilt.us/mcp`. Tool families (all return JSON strings):

| Prefix  | Tools | Purpose |
|---------|-------|---------|
| `todo_*`  | create, list, get, claim, complete, release, renew, delete, summary, events | Shared task store with leases/TTL (SQLite, Longhorn PVC) |
| `docs_*`  | search, get, list, index_status, refresh_index | FTS5 full-text search over `reeds/homelab-docs` mirror |
| `k3s_*`   | nodes, namespaces, list/get services, pods, logs, deployments, statefulsets, daemonsets, ingressroutes, pvcs, storageclasses, events | Read-only Kubernetes cluster queries (in-cluster SA RBAC) |

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

## Deploy

Manifests live in `reeds/sutliff-cluster` → `apps/mcp/` (Flux applies).
Image: `192.168.1.2:3000/reeds/mcp:<tag>`. Secrets (imperative, not in repo):
`mcp-docs-token` (DOCS_GIT_URL). k3s RBAC via in-cluster SA `mcp` +
ClusterRole `mcp-readonly` (see sutliff-cluster).
