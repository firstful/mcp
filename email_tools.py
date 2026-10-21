"""email_tools — email-sync family for the homelab MCP server.

Read-mostly HTTP client for the email-sync service (ns `email`): health,
envelope search, per-message metadata, audited RFC822 body fetch, body-access
audit trail, and an immediate-sync trigger. Metadata endpoints are cheap;
body fetches are recorded in email-sync's `body_access_log` (that audit IS the
receipt the email-sync-ops skill relies on) — always pass a reason.

Direct in-cluster call (EMAIL_SYNC_API); reachability is granted by the
`allow-email-egress` (mcp) + `allow-mcp-ingress` (email) NetworkPolicies.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request

from tool_registry import ToolError

API = os.environ.get("EMAIL_SYNC_API", "http://email-sync.email.svc.cluster.local")
BODY_CAP = 200_000  # bytes returned to the model; raw mail can be up to 4 MiB


def _req(path: str, headers: dict | None = None, timeout: int = 20):
    req = urllib.request.Request(API + path, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:300]
        raise ToolError("tool_error", f"email-sync HTTP {e.code} on {path}: {detail}") from e
    except Exception as e:  # noqa: BLE001
        raise ToolError("tool_error", f"email-sync unreachable ({API}): {e}") from e


def _json(path: str, **params) -> dict:
    if params:
        qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        path = f"{path}?{qs}" if "?" not in path else f"{path}&{qs}"
    status, raw = _req(path)
    try:
        return json.loads(raw.decode(errors="replace") or "{}")
    except Exception as e:  # noqa: BLE001
        raise ToolError("tool_error", f"non-JSON reply from {path}: {e}") from e


def status() -> str:
    """GET /healthz — service liveness plus inbox/unseen counters."""
    return json.dumps(_json("/healthz"))


def search(folder: str = "INBOX", limit: int = 25, unseen: bool | None = None,
           q: str | None = None) -> str:
    """Envelope-only message list (metadata: uid/subject/from/date/size — NO bodies).
    limit <= 500. unseen=true filters to unseen. q matches subject/from/raw."""
    limit = max(1, min(int(limit), 500))
    return json.dumps(_json("/mail", folder=folder, limit=limit,
                            unseen=("1" if unseen else None) if unseen is not None else None,
                            search=q))


def message_meta(uid: str, folder: str = "INBOX") -> str:
    """One message's metadata: headers, subject, date, size, attachment list.
    No body — use email_fetch_body (audited) when content is genuinely needed."""
    return json.dumps(_json(f"/mail/{urllib.parse.quote(str(uid))}", folder=folder))


def fetch_body(uid: str, folder: str = "INBOX", reason: str = "") -> str:
    """Raw RFC822 body of one message. AUDITED: recorded in email-sync's
    body_access_log with X-Audit-Reason — a non-empty `reason` is required
    (e.g. 'otp-code', 'newsletter', 'reed-request'). Truncated at 200 KB."""
    if not (reason or "").strip():
        raise ToolError("bad_request", "reason is required — this fetch is audited; "
                        "say why (otp-code, reed-request, ...)")
    status_, raw = _req(f"/mail/{urllib.parse.quote(str(uid))}/body?folder={urllib.parse.quote(folder)}",
                        headers={"X-Audit-Reason": reason[:120]}, timeout=30)
    truncated = len(raw) > BODY_CAP
    body = raw[:BODY_CAP].decode(errors="replace")
    return json.dumps({"uid": str(uid), "folder": folder, "status": status_,
                       "bytes": len(raw), "truncated": truncated,
                       "reason": reason, "body": body})


def audit(limit: int = 20) -> str:
    """Recent body-access audit trail (ts, ip, ua, reason, outcome, status) —
    the receipt log: check here to see whether a body was ALREADY fetched
    before fetching again."""
    limit = max(1, min(int(limit), 200))
    return json.dumps(_json("/audit/body-access", limit=limit))


def trigger_sync(folder: str = "INBOX") -> str:
    """POST /sync — request an immediate IMAP sync (no-op if one is running).
    Returns the service's response."""
    req = urllib.request.Request(f"{API}/sync", data=b"{}",
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.dumps({"status": r.status,
                               "body": r.read().decode(errors="replace")[:500]})
    except urllib.error.HTTPError as e:
        raise ToolError("tool_error", f"sync POST HTTP {e.code}: {e.read()[:200]!r}") from e
    except Exception as e:  # noqa: BLE001
        raise ToolError("tool_error", f"email-sync unreachable ({API}): {e}") from e
