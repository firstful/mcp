"""gitea_mirror.py — best-effort 1:1 mirror of the todos store onto Gitea issues.

Each todo maps to exactly one issue in a dedicated repo (default
reeds/homelab-todos). SQLite stays authoritative; every mirror write is
best-effort (never blocks or breaks a todo op) and never carries the lease
token.

Mapping (1 todo -> 1 issue):
  status open      -> issue open, labels [status:open, priority:<p>]
  status claimed   -> issue open, labels [status:claimed, priority:<p>],
                      assignee = owner, due_date = lease_expires_at
  status completed -> issue closed, labels [status:completed, priority:<p>]
  delete           -> close the issue (Gitea keeps the audit), never delete

Drift (hand-edited issues, missed writes) is repaired by reconcile():
  - create missing issues (and persist gitea_issue back into the store)
  - fix label/assignee/state/due-date skew
  - run at startup (background thread) and on demand via todo_mirror_reconcile

Config (env, all optional — no GITEA_MIRROR_TOKEN = mirror disabled):
  GITEA_MIRROR_URL    default http://192.168.1.2:3000
  GITEA_MIRROR_TOKEN  (required to enable)
  GITEA_MIRROR_OWNER  default reeds
  GITEA_MIRROR_REPO   default homelab-todos
"""

from __future__ import annotations

import json
import logging
import os
import threading
import urllib.error
import urllib.request

log = logging.getLogger("mcp-gitea-mirror")

STATUS_LABELS = {
    "open": "status:open",
    "claimed": "status:claimed",
    "completed": "status:completed",
}
PRIORITY_LABELS = {
    "low": "priority:low",
    "normal": "priority:normal",
    "high": "priority:high",
    "urgent": "priority:urgent",
}
LABEL_COLORS = {
    "status:open": "3a7afe",
    "status:claimed": "f0ad4e",
    "status:completed": "28a745",
    "priority:low": "6c757d",
    "priority:normal": "17a2b8",
    "priority:high": "fd7e14",
    "priority:urgent": "dc3545",
}


class MirrorError(Exception):
    """Gitea API call failed (non-2xx or transport)."""


class GiteaMirror:
    def __init__(self, url: str, token: str, owner: str = "reeds", repo: str = "homelab-todos"):
        self.base = f"{url.rstrip('/')}/api/v1"
        self.token = token
        self.owner = owner
        self.repo = repo
        self.store = None  # TodosStore reference, set by setup(); used to
        # persist gitea_issue numbers back after issue creation
        self._labels_ready = False
        self._label_ids: dict[str, int] = {}  # name -> id (create-issue wants IDs)
        self._known_users: dict[str, bool] = {}  # owner -> is a real Gitea user?
        self.last_reconcile = None  # dict summary for todo_mirror_status

    # ------------------------------------------------------------ plumbing
    def _request(self, method: str, path: str, body: dict | None = None,
                 timeout: int = 20) -> dict | list:
        url = f"{self.base}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={
                "Authorization": f"token {self.token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:300]
            except Exception:  # noqa: BLE001
                pass
            raise MirrorError(f"{method} {path} -> HTTP {e.code}: {detail}") from e
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8", "replace"))

    def _repo_path(self) -> str:
        return f"/repos/{self.owner}/{self.repo}"

    # ------------------------------------------------------------- labels
    def ensure_labels(self) -> None:
        """Create the status:/priority: labels if missing (idempotent)."""
        if self._labels_ready:
            return
        try:
            labels = self._request("GET", f"{self._repo_path()}/labels?limit=100")
        except MirrorError:
            log.exception("mirror: cannot list labels")
            return
        existing = {l.get("name"): l.get("id") for l in labels}
        for name, color in LABEL_COLORS.items():
            if name in existing:
                continue
            try:
                created = self._request(
                    "POST", f"{self._repo_path()}/labels",
                    {"name": name, "color": color, "description": f"todo mirror label ({name.split(':')[0]})"},
                )
                existing[name] = created.get("id")
                log.info("mirror: created label %s", name)
            except MirrorError as e:
                # 422 "already exists" from a concurrent ensure is fine
                if "already exists" in str(e).lower() or "422" in str(e):
                    continue
                log.warning("mirror: label %s create failed: %s", name, e)
        self._label_ids = existing
        self._labels_ready = True

    def _user_exists(self, name: str) -> bool:
        """True if `name` is a real Gitea user (assignee must be). Cached."""
        if name in self._known_users:
            return self._known_users[name]
        try:
            self._request("GET", f"/users/{name}")
            self._known_users[name] = True
        except MirrorError as e:
            if "404" in str(e):
                self._known_users[name] = False
            else:
                # transport/scope error: be optimistic (assignee skipped by Gitea)
                log.warning("mirror: cannot resolve user %s: %s", name, e)
                return True
        return self._known_users[name]

    def _assignees_for(self, todo: dict) -> list[str]:
        """Owner -> assignee only when the owner is a real Gitea user. Agent
        owners (rook, hermes-*) are not Gitea users and stay in the body meta."""
        owner = todo.get("owner")
        if not owner:
            return []
        return [owner] if self._user_exists(owner) else []

    # -------------------------------------------------------- todo -> issue
    @staticmethod
    def _labels_for(todo: dict) -> list[str]:
        labels = [STATUS_LABELS.get(todo.get("status"), STATUS_LABELS["open"])]
        prio = PRIORITY_LABELS.get(todo.get("priority"), PRIORITY_LABELS["normal"])
        if prio not in labels:
            labels.append(prio)
        return labels

    @staticmethod
    def _title_for(todo: dict) -> str:
        return f"[todo #{todo['id']}] {todo['title']}"

    @staticmethod
    def _body_for(todo: dict) -> str:
        parts = [todo.get("description") or ""]
        meta = []
        if todo.get("dependencies"):
            meta.append(f"**dependencies:** {todo['dependencies']}")
        if todo.get("owner"):
            meta.append(f"**owner:** {todo['owner']}")
        meta.append(f"**created:** {todo.get('created_at')}")
        meta.append(f"**updated:** {todo.get('updated_at')}")
        if todo.get("lease_expires_at"):
            meta.append(
                f"**lease expires:** {todo['lease_expires_at']} (ttl {todo.get('ttl_seconds')}s)"
            )
        if todo.get("expired_count"):
            meta.append(f"**expired:** {todo['expired_count']}x")
        body = "\n\n".join(p for p in parts if p)
        body += "\n\n---\n" + "\n".join(meta) if meta else ""
        body += f"\n\n<!-- todo-id: {todo['id']} -->"
        return body

    def sync_todo(self, todo: dict) -> dict:
        """Create or update the issue for one todo. Returns
        {"action": created|updated, "issue": <number>}."""
        self.ensure_labels()
        issue_number = todo.get("gitea_issue")
        if not issue_number:
            created = self._request(
                "POST", f"{self._repo_path()}/issues",
                {
                    "title": self._title_for(todo),
                    "body": self._body_for(todo),
                    # CreateIssueOption.labels is []int64 (label IDs)
                    "labels": [
                        self._label_ids[n]
                        for n in self._labels_for(todo)
                        if n in self._label_ids
                    ],
                },
            )
            num = created.get("number")
            if not num:
                raise MirrorError(f"create issue returned no number: {created}")
            # persist the mapping back into the store (mirror owns this write)
            if self.store is not None:
                try:
                    self.store.set_gitea_issue(todo["id"], num)
                except Exception:  # noqa: BLE001
                    log.exception("mirror: cannot persist gitea_issue for todo %s", todo["id"])
            return {"action": "created", "issue": num}

        patch: dict = {
            "title": self._title_for(todo),
            "body": self._body_for(todo),
            "assignees": self._assignees_for(todo),
            "state": "closed" if todo.get("status") == "completed" else "open",
        }
        if todo.get("status") == "claimed" and todo.get("lease_expires_at"):
            patch["due_date"] = todo["lease_expires_at"]
        else:
            patch["unset_due_date"] = True
        self._request("PATCH", f"{self._repo_path()}/issues/{issue_number}", patch)
        # labels are replaced via the dedicated endpoint (not in EditIssueOption)
        self._request(
            "PUT", f"{self._repo_path()}/issues/{issue_number}/labels",
            {"labels": self._labels_for(todo)},
        )
        return {"action": "updated", "issue": issue_number}

    def close_issue(self, todo: dict) -> dict:
        """Archive (close) the issue for a deleted todo. Never deletes."""
        num = todo.get("gitea_issue")
        if not num:
            return {"action": "skipped", "issue": None}
        self._request(
            "PATCH", f"{self._repo_path()}/issues/{num}",
            {"state": "closed", "unset_due_date": True},
        )
        return {"action": "closed", "issue": num}

    # ---------------------------------------------------------- reconcile
    def reconcile(self, todos: list[dict]) -> dict:
        """Full sweep: create missing issues, fix drift. Returns counts."""
        self.ensure_labels()
        counts = {"created": 0, "updated": 0, "skipped": 0, "failed": 0, "total": len(todos)}
        for todo in todos:
            try:
                res = self.sync_todo(todo)
                if res["action"] == "created":
                    counts["created"] += 1
                else:
                    counts["updated"] += 1
            except Exception as exc:  # noqa: BLE001
                counts["failed"] += 1
                log.warning("mirror: reconcile todo %s failed: %s", todo.get("id"), exc)
        counts["skipped"] = counts["total"] - counts["created"] - counts["updated"] - counts["failed"]
        self.last_reconcile = counts
        return counts


# ------------------------------------------------------------- module wiring
mirror: GiteaMirror | None = None


def setup(store) -> GiteaMirror | None:
    """Read env and enable the mirror if GITEA_MIRROR_TOKEN is present."""
    global mirror
    token = os.environ.get("GITEA_MIRROR_TOKEN", "")
    if not token:
        log.info("gitea mirror: disabled (no GITEA_MIRROR_TOKEN)")
        return None
    mirror = GiteaMirror(
        os.environ.get("GITEA_MIRROR_URL", "http://192.168.1.2:3000").rstrip("/"),
        token,
        owner=os.environ.get("GITEA_MIRROR_OWNER", "reeds"),
        repo=os.environ.get("GITEA_MIRROR_REPO", "homelab-todos"),
    )
    mirror.store = store
    log.info(
        "gitea mirror: enabled -> %s/%s (url %s)",
        mirror.owner, mirror.repo,
        os.environ.get("GITEA_MIRROR_URL", "http://192.168.1.2:3000"),
    )
    return mirror


def hook(action: str, todo: dict) -> None:
    """Store hook: best-effort write-through. Never raises."""
    if mirror is None:
        return
    try:
        if action == "deleted":
            mirror.close_issue(todo)
        else:
            mirror.sync_todo(todo)
    except Exception:  # noqa: BLE001
        log.exception("mirror: %s todo %s failed (reconcile will repair)", action, todo.get("id"))


def start_background_reconcile(store) -> None:
    """Run a full reconcile in a daemon thread at startup (backfill + repair)."""
    if mirror is None:
        return
    def run() -> None:
        try:
            counts = mirror.reconcile(store.list())
            log.info("mirror: startup reconcile done: %s", counts)
        except Exception:  # noqa: BLE001
            log.exception("mirror: startup reconcile failed")
    threading.Thread(target=run, daemon=True, name="gitea-mirror-reconcile").start()


def status() -> dict:
    """Status for the todo_mirror_status tool."""
    if mirror is None:
        return {"enabled": False}
    return {
        "enabled": True,
        "owner": mirror.owner,
        "repo": mirror.repo,
        "labels_ready": mirror._labels_ready,
        "last_reconcile": mirror.last_reconcile,
    }
