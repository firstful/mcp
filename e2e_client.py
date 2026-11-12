"""MCP client E2E for the dynamic tool surface (same code path Hermes uses)."""
import asyncio
import json
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


def text_of(result):
    for c in result.content:
        if c.type == "text":
            return c.text
    return None


async def main(url: str):
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            names = [t.name for t in tools.tools]
            print("STATIC TOOLS:", len(names), "| first 6:", names[:6])
            print("meta present:", all(n in names for n in ("discover_tool", "execute_tool", "tool_catalog")))

            print("\n-- discover_tool('manage pods') --")
            r = await session.call_tool("discover_tool", {"query": "manage pods", "top_k": 3})
            out = json.loads(text_of(r))
            print(json.dumps(out, indent=1)[:900])

            print("\n-- discover_tool('disk health') --")
            r = await session.call_tool("discover_tool", {"query": "disk health", "top_k": 2})
            out = json.loads(text_of(r))
            print([(h["name"], h["score"]) for h in out["results"]])

            print("\n-- execute_tool(todo_summary) --")
            r = await session.call_tool("execute_tool", {"tool_name": "todo_summary"})
            print(text_of(r)[:200])

            print("\n-- execute_tool(k3s_list_pods, {namespace: mcp}) --")
            r = await session.call_tool("execute_tool", {"tool_name": "k3s_list_pods", "payload": {"namespace": "mcp"}})
            out = json.loads(text_of(r))
            print("ok:", out["ok"], "| result:", json.dumps(out.get("result"))[:200])

            print("\n-- execute_tool(unknown) --")
            r = await session.call_tool("execute_tool", {"tool_name": "wat", "payload": {}})
            print(text_of(r)[:200])

            print("\n-- execute_tool(validation error) --")
            r = await session.call_tool("execute_tool", {"tool_name": "gitea_get_pr", "payload": {"owner": "reeds", "repo": "mcp"}})
            print(text_of(r)[:300])

            print("\n-- tool_catalog --")
            r = await session.call_tool("tool_catalog", {})
            out = json.loads(text_of(r))
            print("count:", out["count"], "| first:", out["tools"][0])
            print("\nE2E OK")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8651/mcp"))
