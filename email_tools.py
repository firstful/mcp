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
<<<<<<< HEAD
=======

# ------------------------------ SMTP send ------------------------------

import smtplib
import ssl
from email.message import EmailMessage
from email.utils import make_msgid, formatdate

SMTP_HOST = os.environ.get("SMTP_HOST", "mail.privateemail.com")
SMTP_USER = os.environ.get("SMTP_USERNAME") or os.environ.get("IMAP_USERNAME") or ""
SMTP_PASS = os.environ.get("SMTP_PASSWORD") or os.environ.get("IMAP_PASSWORD") or ""
SMTP_FROM_DEFAULT = os.environ.get("SMTP_FROM_DEFAULT", "reed@ffilt.us")
_ALLOWED_FROM = {"reed@ffilt.us", "rook@ffilt.us", "alert@ffilt.us"}


def _addr_list(v):
    if v is None:
        return []
    if isinstance(v, str):
        # allow comma/semicolon separated
        parts = re.split(r'[;,]+', v)
        return [p.strip() for p in parts if p and p.strip()]
    if isinstance(v, list):
        out = []
        for x in v:
            if x is None:
                continue
            xs = str(x).strip()
            if xs:
                out.append(xs)
        return out
    xs = str(v).strip()
    return [xs] if xs else []


def send(sender: str | None = None, to=None, recipients=None, subject: str = "",
         body: str = "", html: str | None = None, cc=None, bcc=None) -> str:
    """Send an email via Namecheap Private Email SMTP.

    Auth uses SMTP_USERNAME/SMTP_PASSWORD (falls back to IMAP_USERNAME/IMAP_PASSWORD),
    same credentials as email-sync. Tries 465/SSL then 587/STARTTLS.
    Returns structured JSON; never includes credential material.
    """
    from_addr = (sender or SMTP_FROM_DEFAULT or "reed@ffilt.us").strip().lower()
    if from_addr not in _ALLOWED_FROM:
        return json.dumps({
            "ok": False,
            "error": "invalid_from",
            "message": f"from must be one of {sorted(_ALLOWED_FROM)}",
            "from": sender,
            "to": _addr_list(to if to is not None else recipients),
            "subject": subject,
        })
    to_addrs = _addr_list(to if to is not None else recipients)
    cc_addrs = _addr_list(cc)
    bcc_addrs = _addr_list(bcc)
    if not to_addrs:
        return json.dumps({
            "ok": False, "error": "bad_request", "message": "to/recipients is required",
            "from": from_addr, "to": to_addrs, "subject": subject,
        })
    if not SMTP_USER or not SMTP_PASS:
        return json.dumps({
            "ok": False, "error": "missing_credentials",
            "message": "SMTP auth credentials not configured (SMTP_USERNAME/SMTP_PASSWORD)",
            "from": from_addr, "to": to_addrs, "subject": subject,
        })

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg["Subject"] = subject or ""
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="ffilt.us")
    msg.set_content(body or "")
    if html:
        msg.add_alternative(html, subtype="html")

    all_rcpts = to_addrs + cc_addrs + bcc_addrs
    errors = []
    for port, mode in ((465, "ssl"), (587, "starttls")):
        try:
            if mode == "ssl":
                ctx = ssl.create_default_context()
                with smtplib.SMTP_SSL(SMTP_HOST, port, context=ctx, timeout=25) as s:
                    s.login(SMTP_USER, SMTP_PASS)
                    refused = s.send_message(msg, from_addr=from_addr, to_addrs=all_rcpts)
            else:
                ctx = ssl.create_default_context()
                with smtplib.SMTP(SMTP_HOST, port, timeout=25) as s:
                    s.ehlo()
                    s.starttls(context=ctx)
                    s.ehlo()
                    s.login(SMTP_USER, SMTP_PASS)
                    refused = s.send_message(msg, from_addr=from_addr, to_addrs=all_rcpts)
            return json.dumps({
                "ok": True,
                "from": from_addr,
                "to": to_addrs,
                "cc": cc_addrs,
                "bcc": bcc_addrs,
                "subject": subject or "",
                "transport": "smtp",
                "host": SMTP_HOST,
                "port": port,
                "mode": mode,
                "message_id": msg.get("Message-ID"),
                "refused": refused or {},
            })
        except Exception as e:  # noqa: BLE001
            errors.append({"port": port, "mode": mode, "error": f"{type(e).__name__}: {e}"})
    return json.dumps({
        "ok": False,
        "error": "send_failed",
        "from": from_addr,
        "to": to_addrs,
        "cc": cc_addrs,
        "bcc": bcc_addrs,
        "subject": subject or "",
        "host": SMTP_HOST,
        "attempts": errors,
    })
>>>>>>> mcp 2.3.9: sync deployed source, add email_send SMTP tool
