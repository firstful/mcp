"""gitea_tools.py — read-only Gitea API tools for agents.

Wraps the Gitea REST API (v1) with a scoped read:repository token
(Secret mcp-gitea-token → GITEA_TOKEN). Every function returns a JSON string.

Read-only by design: repos, PRs, issues, actions runs/jobs/logs, file contents.
No mutations — pushes and PR operations stay in git-ops / explicit approval.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger("mcp-gitea")

GITEA_URL = os.environ.get("GITEA_URL", "http://192.168.1.2:3000").rstrip("/")
GITEA_TOKEN = os.environ.get("GITEA_TOKEN", "")

LOG_CHARS = int(os.environ.get("GITEA_LOG_CHARS", "8000"))


def _err(exc: Exception) -> str:
    reason = getattr(exc, "reason", None)
    status = getattr(exc, "status", None)
    body = getattr(exc, "body", None)
    msg = f"{reason or type(exc).__name__}"
    if status:
        msg += f" (HTTP {status})"
    if isinstance(body, bytes):
        body = body.decode("utf-8", "replace")
    if body:
        msg += f": {str(body)[:300]}"
    return json.dumps({"error": msg})


def _api(path: str, params: dict | None = None, raw_text: bool = False):
    """GET a Gitea API path. Returns parsed JSON (or raw text for logs)."""
    url = f"{GITEA_URL}/api/v1{path}"
    if params:
        qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        if qs:
            url += "?" + qs
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"token {GITEA_TOKEN}",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=25) as r:
        data = r.read()
    if raw_text:
        return data.decode("utf-8", "replace")
    return json.loads(data.decode("utf-8", "replace"))


def _simplify_repo(r: dict) -> dict:
    return {
        "name": r.get("name"),
        "owner": (r.get("owner") or {}).get("login"),
        "description": r.get("description"),
        "private": r.get("private"),
        "default_branch": r.get("default_branch"),
        "updated_at": r.get("updated_at"),
        "open_issues": r.get("open_issues_count"),
        "stars": r.get("stars_count"),
        "forks": r.get("forks_count"),
    }


def _simplify_pr(p: dict) -> dict:
    return {
        "number": p.get("number"),
        "title": p.get("title"),
        "state": p.get("state"),
        "author": (p.get("user") or {}).get("login"),
        "head": (p.get("head") or {}).get("ref"),
        "base": (p.get("base") or {}).get("ref"),
        "mergeable": p.get("mergeable"),
        "merged": p.get("merged"),
        "created_at": p.get("created_at"),
        "updated_at": p.get("updated_at"),
    }


def _simplify_issue(i: dict) -> dict:
    return {
        "number": i.get("number"),
        "title": i.get("title"),
        "state": i.get("state"),
        "author": (i.get("user") or {}).get("login"),
        "labels": [l.get("name") for l in (i.get("labels") or [])],
        "comments": i.get("comments"),
        "created_at": i.get("created_at"),
        "updated_at": i.get("updated_at"),
    }


# ---------------------------------------------------------------- tools
def list_repos(owner: str = "reeds") -> str:
    """List Gitea repos for an owner (default reeds): name, description, private,
    default branch, updated time, issue/star/fork counts."""
    try:
        # /user/repos lists private repos of the token owner; /users/{owner}/repos
        # only shows repos visible to the token (403s on private for other owners).
        path = "/user/repos" if owner in ("reeds", "me", "") else f"/users/{owner}/repos"
        repos = _api(path, {"limit": 100, "sort": "updated", "direction": "desc"})
        if isinstance(repos, dict) and repos.get("error"):
            return json.dumps(repos)
        out = [_simplify_repo(r) for r in repos]
        return json.dumps({"count": len(out), "repos": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_repo(owner: str, name: str) -> str:
    """Get one Gitea repo (owner/name): clone URLs, size, counts, timestamps."""
    try:
        r = _api(f"/repos/{owner}/{name}")
        if isinstance(r, dict) and r.get("error"):
            return json.dumps(r)
        out = _simplify_repo(r)
        out.update({
            "clone_url": r.get("clone_url"),
            "ssh_url": r.get("ssh_url"),
            "html_url": r.get("html_url"),
            "size_kb": r.get("size"),
            "watchers": r.get("watchers_count"),
            "created_at": r.get("created_at"),
        })
        return json.dumps(out, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_prs(owner: str, repo: str, state: str = "open", limit: int = 20) -> str:
    """List pull requests (state: open|closed|all). Includes mergeable when known."""
    try:
        prs = _api(f"/repos/{owner}/{repo}/pulls", {
            "state": state, "limit": min(int(limit), 100),
            "sort": "updated", "direction": "desc",
        })
        if isinstance(prs, dict) and prs.get("error"):
            return json.dumps(prs)
        out = [_simplify_pr(p) for p in prs]
        return json.dumps({"count": len(out), "pulls": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_pr(owner: str, repo: str, number: int) -> str:
    """Get one pull request by number: full detail incl. mergeability + files changed."""
    try:
        p = _api(f"/repos/{owner}/{repo}/pulls/{number}")
        if isinstance(p, dict) and p.get("error"):
            return json.dumps(p)
        out = _simplify_pr(p)
        out.update({
            "body": (p.get("body") or "")[:2000],
            "additions": p.get("additions"),
            "deletions": p.get("deletions"),
            "changed_files": p.get("changed_files"),
            "merged_at": p.get("merged_at"),
            "mergeable_state": p.get("mergeable_state"),
        })
        return json.dumps(out, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_issues(owner: str, repo: str, state: str = "open", limit: int = 20) -> str:
    """List issues (state: open|closed|all). Pull requests are excluded (type=issues)."""
    try:
        issues = _api(f"/repos/{owner}/{repo}/issues", {
            "state": state, "type": "issues", "limit": min(int(limit), 100),
            "sort": "updated", "direction": "desc",
        })
        if isinstance(issues, dict) and issues.get("error"):
            return json.dumps(issues)
        out = [_simplify_issue(i) for i in issues]
        return json.dumps({"count": len(out), "issues": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_issue(owner: str, repo: str, number: int) -> str:
    """Get one issue by number: full detail incl. body + labels + comments count."""
    try:
        i = _api(f"/repos/{owner}/{repo}/issues/{number}")
        if isinstance(i, dict) and i.get("error"):
            return json.dumps(i)
        out = _simplify_issue(i)
        out["body"] = (i.get("body") or "")[:3000]
        return json.dumps(out, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_action_runs(owner: str, repo: str, limit: int = 10) -> str:
    """List Gitea Actions workflow runs for a repo: id, name, status, conclusion, sha."""
    try:
        data = _api(f"/repos/{owner}/{repo}/actions/runs", {"limit": min(int(limit), 50)})
        if isinstance(data, dict) and data.get("error"):
            return json.dumps(data)
        runs = data.get("workflow_runs", data) if isinstance(data, dict) else data
        out = [{
            "id": r.get("id"),
            "name": r.get("name"),
            "status": r.get("status"),
            "conclusion": r.get("conclusion"),
            "event": r.get("event"),
            "head_sha": (r.get("head_sha") or "")[:12],
            "run_number": r.get("run_number"),
            "created_at": r.get("created_at"),
        } for r in runs]
        return json.dumps({"count": len(out), "runs": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_action_run(owner: str, repo: str, run_id: int) -> str:
    """Get one Actions run by id, including its jobs (name, status, conclusion)."""
    try:
        r = _api(f"/repos/{owner}/{repo}/actions/runs/{run_id}")
        if isinstance(r, dict) and r.get("error"):
            return json.dumps(r)
        jobs = _api(f"/repos/{owner}/{repo}/actions/runs/{run_id}/jobs")
        job_list = jobs.get("jobs", []) if isinstance(jobs, dict) else []
        out = {
            "id": r.get("id"),
            "name": r.get("name"),
            "status": r.get("status"),
            "conclusion": r.get("conclusion"),
            "event": r.get("event"),
            "head_sha": r.get("head_sha"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            "jobs": [{
                "id": j.get("id"),
                "name": j.get("name"),
                "status": j.get("status"),
                "conclusion": j.get("conclusion"),
                "labels": j.get("labels"),
            } for j in job_list],
        }
        return json.dumps(out, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def action_run_logs(owner: str, repo: str, run_id: int, job_id: int | None = None) -> str:
    """Fetch Actions run logs (raw text, last GITEA_LOG_CHARS chars per job).
    If job_id omitted, concatenates all jobs of the run."""
    try:
        jobs = _api(f"/repos/{owner}/{repo}/actions/runs/{run_id}/jobs")
        job_list = jobs.get("jobs", []) if isinstance(jobs, dict) else []
        if job_id is not None:
            job_list = [j for j in job_list if j.get("id") == job_id]
            if not job_list:
                return json.dumps({"error": f"job {job_id} not found in run {run_id}"})
        chunks = []
        for j in job_list:
            try:
                txt = _api(f"/repos/{owner}/{repo}/actions/jobs/{j['id']}/logs", raw_text=True)
            except urllib.error.HTTPError as e:
                txt = f"(log unavailable HTTP {e.code})"
            chunks.append(f"--- job {j.get('id')} ({j.get('name')}) ---\n{txt[-LOG_CHARS:]}")
        return json.dumps({"run_id": run_id, "jobs": len(job_list), "logs": "\n".join(chunks)})
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_contents(owner: str, repo: str, path: str = "", ref: str = "") -> str:
    """List a directory in a repo (or describe a file). ref = branch/tag/sha."""
    try:
        p = f"/repos/{owner}/{repo}/contents/{path}" if path else f"/repos/{owner}/{repo}/contents"
        data = _api(p, {"ref": ref or None})
        if isinstance(data, dict) and data.get("error"):
            return json.dumps(data)
        if isinstance(data, list):
            out = [{
                "name": e.get("name"),
                "type": e.get("type"),
                "path": e.get("path"),
                "size": e.get("size"),
            } for e in data]
            return json.dumps({"type": "dir", "count": len(out), "entries": out}, indent=1)
        # single file object
        content = ""
        if data.get("encoding") == "base64" and data.get("content"):
            content = base64.b64decode(data["content"]).decode("utf-8", "replace")
        return json.dumps({
            "type": "file",
            "name": data.get("name"),
            "path": data.get("path"),
            "size": data.get("size"),
            "content": content[:10000],
        }, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_file(owner: str, repo: str, path: str, ref: str = "") -> str:
    """Get the full content of one file in a repo (decoded). ref = branch/tag/sha."""
    try:
        data = _api(f"/repos/{owner}/{repo}/contents/{path}", {"ref": ref or None})
        if isinstance(data, dict) and data.get("error"):
            return json.dumps(data)
        if isinstance(data, list):
            return json.dumps({"error": f"{path} is a directory — use gitea_list_contents"})
        content = ""
        if data.get("encoding") == "base64" and data.get("content"):
            content = base64.b64decode(data["content"]).decode("utf-8", "replace")
        return json.dumps({
            "path": data.get("path"),
            "size": data.get("size"),
            "ref": ref or data.get("ref"),
            "content": content,
        }, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)
