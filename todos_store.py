"""Todos store — SQLite-backed task store with leases, TTLs, and dependencies.

Semantics:
  - status: open -> claimed -> completed
  - claim() atomically takes an open task IF all dependencies are completed,
    returns a lease_token that must be presented to complete/release/renew.
  - A claimed task whose lease_expires_at passes is automatically returned to
    open (expired_count incremented) on the next sweep (any read/claim).
  - Dependencies: a task is claimable only when every dep is completed.
    Cycles are rejected at create/update time.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path(os.environ.get("TODOS_DB", "/data/todos.db"))

VALID_STATUS = {"open", "claimed", "completed"}
VALID_PRIORITY = {"low", "normal", "high", "urgent"}
DEFAULT_TTL = 3600  # 1h default lease


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_deps(raw: str | None) -> list[int]:
    if not raw:
        return []
    try:
        return [int(x) for x in json.loads(raw)]
    except (ValueError, TypeError):
        return []


class TodoError(Exception):
    """Business-rule error with an HTTP-ish code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class TodosStore:
    def __init__(self, db_path: Path | str = DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------- plumbing
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS todos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open','claimed','completed')),
                    priority TEXT NOT NULL DEFAULT 'normal'
                        CHECK (priority IN ('low','normal','high','urgent')),
                    deps TEXT NOT NULL DEFAULT '[]',
                    owner TEXT,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    ttl_seconds INTEGER NOT NULL DEFAULT 3600,
                    expired_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    todo_id INTEGER NOT NULL,
                    ts TEXT NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT,
                    detail TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_todos_status ON todos(status);
                CREATE INDEX IF NOT EXISTS idx_events_todo ON events(todo_id);
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _row_to_dict(self, row: sqlite3.Row, include_token: bool = False) -> dict:
        deps = _parse_deps(row["deps"])
        # compute ready = all deps completed
        ready = True
        blocking = []
        if deps:
            conn = self._conn()
            try:
                for d in deps:
                    r = conn.execute("SELECT status FROM todos WHERE id=?", (d,)).fetchone()
                    if r is None or r["status"] != "completed":
                        ready = False
                        blocking.append(d)
            finally:
                conn.close()
        d = {
            "id": row["id"],
            "title": row["title"],
            "description": row["description"],
            "status": row["status"],
            "priority": row["priority"],
            "dependencies": deps,
            "owner": row["owner"],
            "lease_expires_at": row["lease_expires_at"],
            "ttl_seconds": row["ttl_seconds"],
            "expired_count": row["expired_count"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "ready": ready,
            "blocked_by": blocking,
            "expired": row["status"] == "open" and row["expired_count"] > 0,
        }
        if include_token and row["lease_token"]:
            d["lease_token"] = row["lease_token"]
        return d

    def _sweep(self, conn: sqlite3.Connection) -> int:
        """Return any overdue claimed tasks to open. Returns count."""
        now = _now()
        rows = conn.execute(
            "SELECT id FROM todos WHERE status='claimed' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
            (now,),
        ).fetchall()
        for r in rows:
            conn.execute(
                "UPDATE todos SET status='open', owner=NULL, lease_token=NULL, "
                "lease_expires_at=NULL, expired_count=expired_count+1, updated_at=? "
                "WHERE id=?",
                (now, r["id"]),
            )
            conn.execute(
                "INSERT INTO events(todo_id,ts,action,actor,detail) VALUES(?,?,?,?,?)",
                (r["id"], now, "expired", None, "lease expired"),
            )
        if rows:
            conn.commit()
        return len(rows)

    def _validate_deps(self, conn: sqlite3.Connection, deps: list[int], todo_id: int | None) -> None:
        seen = set()
        for d in deps:
            if d == todo_id:
                raise TodoError("cycle", "task cannot depend on itself")
            if d in seen:
                continue
            seen.add(d)
            # BFS cycle check: follow dep chain; if we ever reach todo_id -> cycle
            stack = [d]
            while stack:
                cur = stack.pop()
                if cur == todo_id:
                    raise TodoError("cycle", f"dependency cycle involving task {todo_id}")
                r = conn.execute("SELECT deps FROM todos WHERE id=?", (cur,)).fetchone()
                if r is None:
                    continue  # missing dep tolerated; treated as blocking forever
                stack.extend(_parse_deps(r["deps"]))

    # ------------------------------------------------------------- writes
    def create(self, title: str, description: str = "", dependencies: list[int] | None = None,
               priority: str = "normal", ttl_seconds: int = DEFAULT_TTL) -> dict:
        if not title or not title.strip():
            raise TodoError("bad_request", "title is required")
        if priority not in VALID_PRIORITY:
            raise TodoError("bad_request", f"priority must be one of {sorted(VALID_PRIORITY)}")
        ttl = int(ttl_seconds) if ttl_seconds else DEFAULT_TTL
        if ttl < 10:
            raise TodoError("bad_request", "ttl_seconds must be >= 10")
        deps = [int(x) for x in (dependencies or [])]
        now = _now()
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._validate_deps(conn, deps, None)
            cur = conn.execute(
                "INSERT INTO todos(title,description,status,priority,deps,ttl_seconds,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (title.strip(), description or "", "open", priority, json.dumps(deps), ttl, now, now),
            )
            todo_id = cur.lastrowid
            conn.execute(
                "INSERT INTO events(todo_id,ts,action,actor,detail) VALUES(?,?,?,?,?)",
                (todo_id, now, "created", None, title.strip()),
            )
            conn.commit()
            return self.get(todo_id)
        finally:
            conn.close()

    def claim(self, todo_id: int, owner: str, ttl_seconds: int | None = None) -> dict:
        if not owner or not owner.strip():
            raise TodoError("bad_request", "owner is required")
        now = _now()
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._sweep(conn)
            row = conn.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
            if row is None:
                raise TodoError("not_found", f"task {todo_id} not found")
            if row["status"] == "claimed":
                raise TodoError("conflict", f"task {todo_id} is already claimed")
            if row["status"] == "completed":
                raise TodoError("conflict", f"task {todo_id} is already completed")
            deps = _parse_deps(row["deps"])
            for d in deps:
                r = conn.execute("SELECT status FROM todos WHERE id=?", (d,)).fetchone()
                if r is None or r["status"] != "completed":
                    raise TodoError("blocked", f"task {todo_id} blocked by incomplete dependency {d}")
            ttl = int(ttl_seconds) if ttl_seconds else int(row["ttl_seconds"])
            if ttl < 10:
                raise TodoError("bad_request", "ttl_seconds must be >= 10")
            token = secrets.token_urlsafe(24)
            expires = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat(timespec="seconds")
            conn.execute(
                "UPDATE todos SET status='claimed', owner=?, lease_token=?, lease_expires_at=?, "
                "ttl_seconds=?, updated_at=? WHERE id=?",
                (owner.strip(), token, expires, ttl, now, todo_id),
            )
            conn.execute(
                "INSERT INTO events(todo_id,ts,action,actor,detail) VALUES(?,?,?,?,?)",
                (todo_id, now, "claimed", owner.strip(), f"ttl={ttl}s expires={expires}"),
            )
            conn.commit()
            fresh = conn.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
            return self._row_to_dict(fresh, include_token=True) | {"lease_token": token}
        finally:
            conn.close()

    def complete(self, todo_id: int, lease_token: str) -> dict:
        now = _now()
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._sweep(conn)
            row = conn.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
            if row is None:
                raise TodoError("not_found", f"task {todo_id} not found")
            if row["status"] == "completed":
                raise TodoError("conflict", f"task {todo_id} is already completed")
            if row["status"] != "claimed":
                raise TodoError("conflict", f"task {todo_id} is not claimed")
            if not lease_token or row["lease_token"] != lease_token:
                raise TodoError("forbidden", "invalid lease token")
            deps = _parse_deps(row["deps"])
            for d in deps:
                r = conn.execute("SELECT status FROM todos WHERE id=?", (d,)).fetchone()
                if r is None or r["status"] != "completed":
                    raise TodoError("blocked", f"task {todo_id} blocked by incomplete dependency {d}")
            conn.execute(
                "UPDATE todos SET status='completed', lease_token=NULL, lease_expires_at=NULL, "
                "updated_at=? WHERE id=?",
                (now, todo_id),
            )
            conn.execute(
                "INSERT INTO events(todo_id,ts,action,actor,detail) VALUES(?,?,?,?,?)",
                (todo_id, now, "completed", row["owner"], "completed"),
            )
            conn.commit()
            return self.get(todo_id)
        finally:
            conn.close()

    def release(self, todo_id: int, lease_token: str) -> dict:
        now = _now()
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
            if row is None:
                raise TodoError("not_found", f"task {todo_id} not found")
            if row["status"] != "claimed":
                raise TodoError("conflict", f"task {todo_id} is not claimed")
            if not lease_token or row["lease_token"] != lease_token:
                raise TodoError("forbidden", "invalid lease token")
            conn.execute(
                "UPDATE todos SET status='open', owner=NULL, lease_token=NULL, lease_expires_at=NULL, "
                "updated_at=? WHERE id=?",
                (now, todo_id),
            )
            conn.execute(
                "INSERT INTO events(todo_id,ts,action,actor,detail) VALUES(?,?,?,?,?)",
                (todo_id, now, "released", row["owner"], "released early"),
            )
            conn.commit()
            return self.get(todo_id)
        finally:
            conn.close()

    def renew(self, todo_id: int, lease_token: str, ttl_seconds: int | None = None) -> dict:
        now = _now()
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._sweep(conn)
            row = conn.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
            if row is None:
                raise TodoError("not_found", f"task {todo_id} not found")
            if row["status"] != "claimed":
                raise TodoError("conflict", f"task {todo_id} is not claimed")
            if not lease_token or row["lease_token"] != lease_token:
                raise TodoError("forbidden", "invalid lease token")
            ttl = int(ttl_seconds) if ttl_seconds else int(row["ttl_seconds"])
            if ttl < 10:
                raise TodoError("bad_request", "ttl_seconds must be >= 10")
            expires = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat(timespec="seconds")
            conn.execute(
                "UPDATE todos SET lease_expires_at=?, ttl_seconds=?, updated_at=? WHERE id=?",
                (expires, ttl, now, todo_id),
            )
            conn.execute(
                "INSERT INTO events(todo_id,ts,action,actor,detail) VALUES(?,?,?,?,?)",
                (todo_id, now, "renewed", row["owner"], f"ttl={ttl}s expires={expires}"),
            )
            conn.commit()
            return self.get(todo_id)
        finally:
            conn.close()

    def delete(self, todo_id: int) -> dict:
        now = _now()
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT id FROM todos WHERE id=?", (todo_id,)).fetchone()
            if row is None:
                raise TodoError("not_found", f"task {todo_id} not found")
            # refuse if other tasks depend on it (proper JSON-aware check)
            deps_of = conn.execute(
                "SELECT DISTINCT id FROM todos WHERE EXISTS ("
                "  SELECT 1 FROM json_each(todos.deps) WHERE CAST(json_each.value AS INTEGER) = ?"
                ")",
                (todo_id,),
            ).fetchall()
            if deps_of:
                raise TodoError(
                    "conflict",
                    f"task {todo_id} is a dependency of {[r['id'] for r in deps_of]}",
                )
            conn.execute("DELETE FROM todos WHERE id=?", (todo_id,))
            conn.execute("INSERT INTO events(todo_id,ts,action,actor,detail) VALUES(?,?,?,?,?)",
                         (todo_id, now, "deleted", None, "deleted"))
            conn.commit()
            return {"ok": True, "id": todo_id}
        finally:
            conn.close()

    # ------------------------------------------------------------- reads
    def get(self, todo_id: int) -> dict:
        conn = self._conn()
        try:
            self._sweep(conn)
            row = conn.execute("SELECT * FROM todos WHERE id=?", (todo_id,)).fetchone()
            if row is None:
                raise TodoError("not_found", f"task {todo_id} not found")
            return self._row_to_dict(row)
        finally:
            conn.close()

    def list(self, status: str | None = None, owner: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            self._sweep(conn)
            sql = "SELECT * FROM todos"
            where, params = [], []
            if status:
                if status not in VALID_STATUS:
                    raise TodoError("bad_request", f"status must be one of {sorted(VALID_STATUS)}")
                where.append("status=?")
                params.append(status)
            if owner:
                where.append("owner=?")
                params.append(owner)
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY CASE priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 "
            sql += "WHEN 'normal' THEN 2 ELSE 3 END, created_at ASC"
            rows = conn.execute(sql, params).fetchall()
            return [self._row_to_dict(r) for r in rows]
        finally:
            conn.close()

    def summary(self) -> dict:
        conn = self._conn()
        try:
            self._sweep(conn)
            counts = {"open": 0, "claimed": 0, "completed": 0}
            for r in conn.execute("SELECT status, COUNT(*) AS n FROM todos GROUP BY status"):
                counts[r["status"]] = r["n"]
            counts["total"] = sum(counts.values())
            expired = conn.execute(
                "SELECT COALESCE(SUM(expired_count),0) AS n FROM todos"
            ).fetchone()["n"]
            counts["total_expirations"] = expired
            counts["available"] = counts["open"]  # open tasks are claimable
            return counts
        finally:
            conn.close()

    def events(self, todo_id: int | None = None, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            if todo_id is not None:
                rows = conn.execute(
                    "SELECT * FROM events WHERE todo_id=? ORDER BY ts DESC LIMIT ?",
                    (todo_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM events ORDER BY ts DESC LIMIT ?", (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
