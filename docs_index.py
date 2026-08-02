"""docs_index.py — FTS5 full-text index over the homelab-docs repo mirror.

Ported from the standalone gitea-docs-search MCP server (2026-08-02) into a
plain module so the unified `mcp` service can expose docs.* tools alongside
todo.* and k3s.*.

Config (env vars, all optional):
  DOCS_REPO_DIR       repo mirror to index   (default /data/docs-mirror)
  DOCS_INDEX_DB       sqlite db path          (default /data/docs-index.db)
  DOCS_GIT_URL        clone URL (with creds) used when the mirror is missing
  DOCS_WATCH_INTERVAL scan interval seconds   (default 5.0)
  DOCS_AUTO_PULL      "1" enable git pull     (default 1)
  DOCS_PULL_INTERVAL  git pull interval sec   (default 300)
  DOCS_SNIPPET_WORDS  snippet token count     (default 14)
  DOCS_MAX_FILE_BYTES skip files larger than  (default 2_000_000)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import threading
import time
from pathlib import Path

log = logging.getLogger("mcp-docs")

REPO_DIR = Path(os.environ.get("DOCS_REPO_DIR", "/data/docs-mirror"))
INDEX_DB = Path(os.environ.get("DOCS_INDEX_DB", "/data/docs-index.db"))
GIT_URL = os.environ.get("DOCS_GIT_URL", "")
WATCH_INTERVAL = float(os.environ.get("DOCS_WATCH_INTERVAL", "5.0"))
AUTO_PULL = os.environ.get("DOCS_AUTO_PULL", "1") == "1"
PULL_INTERVAL = float(os.environ.get("DOCS_PULL_INTERVAL", "300"))
SNIPPET_WORDS = int(os.environ.get("DOCS_SNIPPET_WORDS", "14"))
MAX_FILE_BYTES = int(os.environ.get("DOCS_MAX_FILE_BYTES", "2_000_000"))

TEXT_EXT = {
    ".md", ".markdown", ".txt", ".rst", ".yaml", ".yml", ".json",
    ".sh", ".py", ".conf", ".cfg", ".ini", ".toml", ".html", ".css",
    ".env", ".example",
}
SKIP_DIRS = {".git", "node_modules", ".venv", "__pycache__", ".hermes"}

_db_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS docs (
    path      TEXT PRIMARY KEY,
    title     TEXT,
    content   TEXT,
    sha       TEXT,
    mtime     REAL
);
CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(
    path UNINDEXED, title, content, tokenize='porter unicode61'
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""

FRONTMATTER_RE = re.compile(r"^---\s*\n.*?\n---\s*\n", re.S | re.M)
H1_RE = re.compile(r"^#\s+(.+?)\s*$", re.M)
FTS_SPECIAL = re.compile(r'["\'*^(){}[\]~:+\-!\\/]')


def db_connect() -> sqlite3.Connection:
    INDEX_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(INDEX_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    with _db_lock:
        conn.executescript(SCHEMA)
    return conn


def meta_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def upsert_doc(conn: sqlite3.Connection, path: str, title: str, content: str, sha: str, mtime: float) -> None:
    conn.execute(
        "INSERT INTO docs(path,title,content,sha,mtime) VALUES(?,?,?,?,?) "
        "ON CONFLICT(path) DO UPDATE SET title=excluded.title, content=excluded.content, "
        "sha=excluded.sha, mtime=excluded.mtime",
        (path, title, content, sha, mtime),
    )
    conn.execute("DELETE FROM docs_fts WHERE path=?", (path,))
    conn.execute("INSERT INTO docs_fts(path,title,content) VALUES(?,?,?)", (path, title, content))


def delete_doc(conn: sqlite3.Connection, path: str) -> None:
    conn.execute("DELETE FROM docs WHERE path=?", (path,))
    conn.execute("DELETE FROM docs_fts WHERE path=?", (path,))


def extract_text(path: Path) -> tuple[str, str]:
    """Return (title, content). Strips YAML frontmatter; title from frontmatter
    title or first H1, else filename."""
    raw = path.read_text(encoding="utf-8", errors="replace")
    content = raw
    title = path.name

    fm = FRONTMATTER_RE.match(raw)
    if fm:
        content = raw[fm.end():]
        fm_title = re.search(r"^title:\s*(.+?)\s*$", fm.group(0), re.M)
        if fm_title:
            title = fm_title.group(1).strip().strip("\"'")

    if not title or title == path.name:
        m = H1_RE.search(content)
        if m:
            title = m.group(1).strip()

    return title, content


def file_sha(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def scan_repo() -> list[tuple[str, str, float]]:
    """Return [(rel_path, sha, mtime)] for indexable files in REPO_DIR."""
    out: list[tuple[str, str, float]] = []
    if not REPO_DIR.is_dir():
        return out
    for root, dirs, files in os.walk(REPO_DIR):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]
        for name in files:
            p = Path(root) / name
            rel = p.relative_to(REPO_DIR).as_posix()
            if p.stat().st_size > MAX_FILE_BYTES:
                continue
            if p.suffix.lower() not in TEXT_EXT and not name.startswith("."):
                continue
            out.append((rel, file_sha(p), p.stat().st_mtime))
    return sorted(out)


def full_rebuild() -> dict:
    """Index every file. Returns stats."""
    t0 = time.time()
    conn = db_connect()
    with _db_lock:
        conn.execute("DELETE FROM docs")
        conn.execute("DELETE FROM docs_fts")
        manifest = scan_repo()
        n = 0
        for rel, sha, mtime in manifest:
            p = REPO_DIR / rel
            try:
                title, content = extract_text(p)
            except Exception as exc:  # noqa: BLE001
                log.warning("skip %s: %s", rel, exc)
                continue
            upsert_doc(conn, rel, title, content, sha, mtime)
            n += 1
        meta_set(conn, "last_full_index", f"{time.time():.0f}")
        meta_set(conn, "last_scan", f"{time.time():.0f}")
        conn.commit()
    elapsed = time.time() - t0
    log.info("full rebuild: %d files indexed in %.2fs", n, elapsed)
    return {"indexed": n, "elapsed_s": round(elapsed, 2)}


def incremental_scan() -> dict:
    """Compare on-disk manifest to DB, upsert/delete changed docs."""
    if not REPO_DIR.is_dir():
        return {"changed": 0, "error": f"repo dir missing: {REPO_DIR}"}
    manifest = dict((rel, (sha, mtime)) for rel, sha, mtime in scan_repo())
    conn = db_connect()
    changed = 0
    with _db_lock:
        rows = conn.execute("SELECT path, sha FROM docs").fetchall()
        db_map = {r["path"]: r["sha"] for r in rows}
        for rel, (sha, mtime) in manifest.items():
            if db_map.get(rel) != sha:
                p = REPO_DIR / rel
                try:
                    title, content = extract_text(p)
                except Exception as exc:  # noqa: BLE001
                    log.warning("skip %s: %s", rel, exc)
                    continue
                upsert_doc(conn, rel, title, content, sha, mtime)
                changed += 1
                log.info("indexed change: %s", rel)
        for rel in db_map:
            if rel not in manifest:
                delete_doc(conn, rel)
                changed += 1
                log.info("removed from index: %s", rel)
        meta_set(conn, "last_scan", f"{time.time():.0f}")
        conn.commit()
    return {"changed": changed}


def git_pull() -> dict:
    """ff-only pull on the mirror; remote pushes then surface via watcher."""
    if not (REPO_DIR / ".git").exists():
        return {"ok": False, "error": "not a git repo"}
    try:
        r = subprocess.run(
            ["git", "-C", str(REPO_DIR), "pull", "--ff-only", "--quiet", "origin", "main"],
            capture_output=True, text=True, timeout=120,
        )
        ok = r.returncode == 0
        if not ok:
            log.warning("git pull failed rc=%d: %s", r.returncode, r.stderr.strip()[:300])
        return {"ok": ok, "out": r.stdout.strip()[:200], "err": r.stderr.strip()[:200]}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


def ensure_repo() -> dict:
    """Clone the docs mirror if missing (uses DOCS_GIT_URL with embedded creds)."""
    if REPO_DIR.is_dir() and (REPO_DIR / ".git").exists():
        return {"ok": True, "cloned": False}
    if not GIT_URL:
        return {"ok": False, "error": "repo missing and DOCS_GIT_URL not set"}
    REPO_DIR.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = subprocess.run(
            ["git", "clone", "--quiet", GIT_URL, str(REPO_DIR)],
            capture_output=True, text=True, timeout=180,
        )
        if r.returncode != 0:
            log.warning("clone failed rc=%d: %s", r.returncode, r.stderr.strip()[:300])
            return {"ok": False, "err": r.stderr.strip()[:300]}
        log.info("cloned docs mirror from %s", GIT_URL.split("@")[-1])
        return {"ok": True, "cloned": True}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------- watchers
_stop = threading.Event()
_pull_state: dict = {}


def watcher_loop() -> None:
    log.info("watcher started (interval=%ss, repo=%s)", WATCH_INTERVAL, REPO_DIR)
    while not _stop.wait(WATCH_INTERVAL):
        try:
            incremental_scan()
        except Exception as exc:  # noqa: BLE001
            log.warning("watcher scan error: %s", exc)


def pull_loop() -> None:
    log.info("git pull loop started (interval=%ss)", PULL_INTERVAL)
    while not _stop.wait(PULL_INTERVAL):
        try:
            _pull_state.update(git_pull())
        except Exception as exc:  # noqa: BLE001
            log.warning("git pull error: %s", exc)


def start_watchers() -> None:
    threading.Thread(target=watcher_loop, daemon=True, name="docs-watcher").start()
    if AUTO_PULL:
        threading.Thread(target=pull_loop, daemon=True, name="docs-pull").start()


# ---------------------------------------------------------------- tool fns
def search(query: str, limit: int = 8, mode: str = "and") -> str:
    """Full-text search over the docs repo. Returns ranked hits with path,
    title, score, and a snippet around the match."""
    terms = [t for t in query.split() if t]
    if not terms:
        return json.dumps({"query": query, "hits": [], "error": "empty query"})
    cleaned = []
    for t in terms:
        t2 = FTS_SPECIAL.sub("", t)
        if t2:
            cleaned.append(f'"{t2}"')
    if not cleaned:
        return json.dumps({"query": query, "hits": [], "error": "empty query"})
    fts = (" AND " if mode == "and" else " OR ").join(cleaned)
    limit = max(1, min(int(limit), 50))
    conn = db_connect()
    with _db_lock:
        rows = conn.execute(
            "SELECT path, title, bm25(docs_fts) AS rank, "
            f"snippet(docs_fts, 2, '[', ']', ' … ', {SNIPPET_WORDS}) AS snip "
            "FROM docs_fts WHERE docs_fts MATCH ? ORDER BY rank LIMIT ?",
            (fts, limit),
        ).fetchall()
    hits = [
        {
            "path": r["path"],
            "title": r["title"],
            "score": round(float(r["rank"]), 3),
            "snippet": r["snip"],
        }
        for r in rows
    ]
    return json.dumps({"query": query, "mode": mode, "count": len(hits), "hits": hits})


def get_doc(path: str) -> str:
    """Return the full content of a doc by repo-relative path (e.g. ops/rook-soul.md)."""
    if not path or ".." in path or path.startswith("/") or path.startswith("\\"):
        return json.dumps({"error": f"invalid path: {path!r}"})
    p = (REPO_DIR / path).resolve()
    if not p.is_relative_to(REPO_DIR.resolve()) or not p.is_file():
        return json.dumps({"error": f"not found: {path}"})
    try:
        title, content = extract_text(p)
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"error": str(exc)})
    return json.dumps({"path": path, "title": title, "content": content})


def list_docs(pattern: str = "") -> str:
    """List all indexed docs. Optional substring pattern on path/title."""
    conn = db_connect()
    with _db_lock:
        if pattern:
            rows = conn.execute(
                "SELECT path, title FROM docs WHERE path LIKE ? OR title LIKE ? ORDER BY path",
                (f"%{pattern}%", f"%{pattern}%"),
            ).fetchall()
        else:
            rows = conn.execute("SELECT path, title FROM docs ORDER BY path").fetchall()
    return json.dumps({"count": len(rows), "docs": [{"path": r["path"], "title": r["title"]} for r in rows]})


def index_status() -> str:
    """Index health: file count, last scans/rebuilds, watcher state, git pull state."""
    conn = db_connect()
    with _db_lock:
        n = conn.execute("SELECT COUNT(*) AS c FROM docs").fetchone()["c"]
        last_full = meta_get(conn, "last_full_index")
        last_scan = meta_get(conn, "last_scan")
    db_size = INDEX_DB.stat().st_size if INDEX_DB.exists() else 0
    return json.dumps({
        "repo_dir": str(REPO_DIR),
        "repo_exists": REPO_DIR.is_dir(),
        "indexed_files": n,
        "db_size_bytes": db_size,
        "last_full_index": last_full,
        "last_scan": last_scan,
        "watcher_running": any(t.name == "docs-watcher" and t.is_alive() for t in threading.enumerate()),
        "pull_enabled": AUTO_PULL,
        "last_pull": _pull_state,
    })


def refresh_index() -> str:
    """Force a full reindex of the repo (also picks up a freshly cloned/new repo dir)."""
    stats = full_rebuild()
    return json.dumps({"ok": True, **stats})
