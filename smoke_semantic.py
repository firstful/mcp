import json
import os

os.environ["TODOS_DB"] = "/tmp/mcp-t.db"
os.environ["DOCS_REPO_DIR"] = "/tmp/mcp-doc"
os.environ["DOCS_INDEX_DB"] = "/tmp/mcp-di.db"
os.environ["TOOLS_INDEX_DB"] = "/tmp/mcp-ti2.db"
os.environ["EMBEDDINGS_URL"] = "http://10.43.164.97"
os.environ["KUBECONFIG"] = "/etc/rancher/k3s/k3s.yaml"

import server  # noqa: E402

print("rebuild:", server.tool_index.rebuild())
print("status:", server.tool_index.status())

queries = [
    "manage pods",
    "gitea pull requests",
    "disk space",
    "is the router alive",
    "todo board",
    "search the docs",
]
for q in queries:
    r = json.loads(server.discover_tool(q, top_k=4))
    print(f"\n### {q!r} [{r['mode']}]")
    for hit in r["results"]:
        print(f"  {hit['score']:>6}  {hit['name']:<28} {hit['family']:<8} {hit['description'][:60]}")
