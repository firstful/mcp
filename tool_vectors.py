"""tool_vectors.py — tiny embedding database for the dynamic tool surface.

One row per registered tool: name, family, description, input schema, and a
384-dim MiniLM embedding of "{name}: {description}". Vectors come from the
homelab embeddings service (reeds/embeddings, OpenAI-compatible
POST /v1/embeddings) over HTTP — no ML deps in this image, and the model
stays pinned to the AVX node where it belongs.

discover_tool() embeds the query and returns cosine-ranked matches. If the
embeddings service is unreachable the index degrades to deterministic keyword
scoring over the registry, so discovery keeps working during an embeddings
outage. Pure-stdlib (array/struct for vectors, math for cosine) — no numpy.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
import urllib.request
from array import array
from pathlib import Path

from tool_registry import ToolRegistry

log = logging.getLogger("mcp-toolvec")

EMBEDDINGS_URL = os.environ.get(
    "EMBEDDINGS_URL", "http://embeddings.embeddings.svc.cluster.local"
).rstrip("/")
EMBED_TIMEOUT = float(os.environ.get("EMBED_TIMEOUT", "10"))
INDEX_DB = Path(os.environ.get("TOOLS_INDEX_DB", "/data/tool-index.db"))

MAX_DESC_CHARS = 400
TOP_K_MAX = 25


def _pack(vec) -> bytes:
    return array("f", vec).tobytes()


def _unpack(b: bytes) -> array:
    a = array("f")
    a.frombytes(b)
    return a


def _cosine(a: array, b: array) -> float:
    dot = na = nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0 or nb == 0:
        return 0.0
    return dot / ((na ** 0.5) * (nb ** 0.5))


def _embed_http(texts: list[str]) -> list[list[float]] | None:
    """Embed via the homelab embeddings service. None = unreachable/error."""
    body = json.dumps({"input": texts}).encode("utf-8")
    req = urllib.request.Request(
        f"{EMBEDDINGS_URL}/v1/embeddings",
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=EMBED_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
    except Exception as exc:  # noqa: BLE001
        log.warning("embeddings service unreachable (%s) — keyword fallback", exc)
        return None
    vecs = [d.get("embedding") for d in (data.get("data") or [])]
    if not vecs or not isinstance(vecs[0], list):
        log.warning("embeddings service returned no vectors")
        return None
    return vecs


_TOK = re.compile(r"[a-z0-9_]+")


def _tokens(text: str) -> set[str]:
    return set(_TOK.findall(text.lower()))


class ToolIndex:
    """SQLite store of tool metadata + embeddings with cosine discovery."""

    def __init__(self, registry: ToolRegistry, db_path: Path | None = None) -> None:
        self.registry = registry
        self.db_path = db_path or INDEX_DB
        self.stats = {
            "embedder": EMBEDDINGS_URL,
            "embeddings_status": "untested",
            "rebuilds": 0,
            "keyword_fallbacks": 0,
        }

    # ------------------------------------------------------------- storage
    def _conn(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS tools (
                name TEXT PRIMARY KEY,
                family TEXT NOT NULL,
                description TEXT NOT NULL,
                schema TEXT NOT NULL,
                vec BLOB,
                updated_at REAL NOT NULL
            )"""
        )
        return conn

    @staticmethod
    def _descriptor(entry) -> str:
        desc = " ".join(entry.description.split())
        return f"{entry.name}: {desc}"[:MAX_DESC_CHARS]

    # -------------------------------------------------------------- build
    def rebuild(self) -> dict:
        """(Re)embed every registered tool descriptor and upsert the index."""
        entries = self.registry.entries()
        conn = self._conn()
        now = time.time()
        vecs = _embed_http([self._descriptor(e) for e in entries]) if entries else None
        try:
            if vecs is None:
                # Index without vectors: discovery falls back to keyword scoring.
                for e in entries:
                    conn.execute(
                        """INSERT INTO tools (name, family, description, schema, vec, updated_at)
                           VALUES (?,?,?,?,NULL,?)
                           ON CONFLICT(name) DO UPDATE SET family=excluded.family,
                             description=excluded.description, schema=excluded.schema,
                             vec=NULL, updated_at=excluded.updated_at""",
                        (e.name, e.family, e.description, json.dumps(e.schema), now),
                    )
                self.stats["embeddings_status"] = "unavailable"
                mode = "keyword-only"
            else:
                for e, v in zip(entries, vecs):
                    conn.execute(
                        """INSERT INTO tools (name, family, description, schema, vec, updated_at)
                           VALUES (?,?,?,?,?,?)
                           ON CONFLICT(name) DO UPDATE SET family=excluded.family,
                             description=excluded.description, schema=excluded.schema,
                             vec=excluded.vec, updated_at=excluded.updated_at""",
                        (e.name, e.family, e.description, json.dumps(e.schema), _pack(v), now),
                    )
                self.stats["embeddings_status"] = "ok"
                mode = "semantic"
            conn.commit()
        finally:
            conn.close()
        self.stats["rebuilds"] += 1
        return {"ok": True, "mode": mode, "indexed": len(entries), "embedder": EMBEDDINGS_URL}

    def ensure_index(self, registry_hash: str | None = None) -> dict:
        """Rebuild if the table is empty, has stale rows, or lost its vectors."""
        conn = self._conn()
        try:
            row = conn.execute("SELECT COUNT(*), SUM(vec IS NULL) FROM tools").fetchone()
            count, null_vecs = row[0], row[1] or 0
        except sqlite3.Error:
            count, null_vecs = -1, -1
        finally:
            conn.close()
        expected = len(self.registry.entries())
        if count != expected or null_vecs:
            return self.rebuild()
        return {"ok": True, "mode": "semantic", "indexed": count, "fresh": True}

    # ----------------------------------------------------------- discovery
    def search(self, query: str, top_k: int = 5, min_score: float = 0.2) -> dict:
        """Rank tools by cosine similarity to the query (semantic), or fall
        back to keyword scoring when the embeddings service is unreachable.
        Meta tools (discover_tool/execute_tool/tool_catalog) are the static
        entry points, so they are never returned as discoverable results.
        Returns {mode, count, results:[{name, family, description, schema, score}]}."""
        top_k = max(1, min(int(top_k), TOP_K_MAX))
        qv = _embed_http([query])
        if qv is not None:
            hits = self._semantic(qv[0], min_score)
            if hits is not None:
                return {"mode": "semantic", "count": len(hits), "results": hits[:top_k]}
        self.stats["keyword_fallbacks"] += 1
        return {"mode": "keyword", "count": len(self.registry.entries()), "results": self._keyword(query, top_k)}

    def _semantic(self, qv, min_score: float) -> list[dict] | None:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT name, family, description, schema, vec FROM tools WHERE family != 'meta'"
            ).fetchall()
        except sqlite3.Error as exc:
            log.warning("tool index read failed: %s", exc)
            return None
        finally:
            conn.close()
        qa = array("f", qv)
        scored = []
        for name, family, desc, schema, vec_blob in rows:
            if not vec_blob:
                continue
            s = _cosine(qa, _unpack(vec_blob))
            if s >= min_score:
                scored.append(
                    {"name": name, "family": family, "description": desc, "schema": schema, "score": round(s, 4)}
                )
        scored.sort(key=lambda r: r["score"], reverse=True)
        return scored

    def _keyword(self, query: str, top_k: int) -> list[dict]:
        q_tokens = _tokens(query)
        q_lower = query.lower()
        scored = []
        for e in self.registry.entries():
            if e.family == "meta":
                continue
            name_tokens = _tokens(e.name)
            desc_tokens = _tokens(e.description)
            overlap_name = len(q_tokens & name_tokens)
            overlap_desc = len(q_tokens & desc_tokens)
            score = 3.0 * overlap_name + 1.0 * overlap_desc
            if q_lower and (q_lower in e.name or q_lower in e.description.lower()):
                score += 0.5
            if score > 0:
                scored.append(
                    {
                        "name": e.name,
                        "family": e.family,
                        "description": e.description,
                        "schema": json.dumps(e.schema),
                        "score": round(score, 2),
                    }
                )
        scored.sort(key=lambda r: r["score"], reverse=True)
        return scored[:top_k]

    # ---------------------------------------------------------------- info
    def status(self) -> dict:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*), SUM(vec IS NOT NULL), MAX(updated_at) FROM tools"
            ).fetchone()
        except sqlite3.Error:
            row = (0, 0, None)
        finally:
            conn.close()
        return {
            "rows": row[0] or 0,
            "with_vectors": row[1] or 0,
            "last_updated": row[2],
            **self.stats,
        }
