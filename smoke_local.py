import json
import os

os.environ["TODOS_DB"] = "/tmp/mcp-t.db"
os.environ["DOCS_REPO_DIR"] = "/tmp/mcp-doc"
os.environ["DOCS_INDEX_DB"] = "/tmp/mcp-di.db"
os.environ["TOOLS_INDEX_DB"] = "/tmp/mcp-ti.db"
os.environ["EMBEDDINGS_URL"] = "http://127.0.0.1:9"  # dead -> keyword fallback
os.environ["KUBECONFIG"] = "/etc/rancher/k3s/k3s.yaml"

import server  # noqa: E402

reg = server.registry
entries = reg.entries()
print("registered:", len(entries))
print("families:", sorted({e.family for e in entries}))
meta = [e.name for e in entries if e.family == "meta"]
print("meta:", meta)

# sample schema
entry = reg.get("k3s_list_pods")
print("schema k3s_list_pods:", json.dumps(entry.schema))

# discover via keyword fallback
print("--- discover_tool('manage pods')")
print(server.discover_tool("manage pods", top_k=3))
print("--- discover_tool('gitea pull requests')")
print(server.discover_tool("gitea pull requests", top_k=3))

# execute happy path
print("--- execute_tool todo_summary")
print(server.execute_tool("todo_summary"))
print("--- execute_tool k3s_nodes")
r = json.loads(server.execute_tool("k3s_nodes"))
print("k3s_nodes ok:", r.get("ok"), "| result keys:", list((r.get("result") or {}).keys()) if isinstance(r.get("result"), dict) else type(r.get("result")).__name__)

# execute error paths
print("--- unknown tool")
print(server.execute_tool("no_such_tool", {"a": 1}))
print("--- validation error")
print(server.execute_tool("k3s_list_pods", {"namespace": 5}))
print("--- payload not dict")
print(server.execute_tool("todo_summary", "nope"))
print("--- catalog")
c = json.loads(server.tool_catalog())
print("catalog count:", c["count"], "| first:", c["tools"][0])

# tool index state after fallback rebuild
print("index status:", server.tool_index.status())
