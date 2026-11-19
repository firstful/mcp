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


def _fetch_json(path: str, **params) -> dict:
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
    return json.dumps(_fetch_json("/healthz"))


def search(folder: str = "INBOX", limit: int = 25, unseen: bool | None = None,
           q: str | None = None) -> str:
    """Envelope-only message list (metadata: uid/subject/from/date/size — NO bodies).
    limit <= 500. unseen=true filters to unseen. q matches subject/from/raw."""
    limit = max(1, min(int(limit), 500))
    return json.dumps(_fetch_json("/mail", folder=folder, limit=limit,
                            unseen=("1" if unseen else None) if unseen is not None else None,
                            search=q))


def message_meta(uid: str, folder: str = "INBOX") -> str:
    """One message's metadata: headers, subject, date, size, attachment list.
    No body — use email_fetch_body (audited) when content is genuinely needed."""
    return json.dumps(_fetch_json(f"/mail/{urllib.parse.quote(str(uid))}", folder=folder))


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
    return json.dumps(_fetch_json("/audit/body-access", limit=limit))


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

# ------------------------------ SMTP send ------------------------------

import json as _json
import re as _re
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import make_msgid, formatdate

SMTP_HOST = os.environ.get("SMTP_HOST", "mail.privateemail.com")
SMTP_USER = os.environ.get("SMTP_USERNAME") or os.environ.get("IMAP_USERNAME") or ""
SMTP_PASS = os.environ.get("SMTP_PASSWORD") or os.environ.get("IMAP_PASSWORD") or ""
SMTP_FROM_DEFAULT = os.environ.get("SMTP_FROM_DEFAULT", "rook@ffilt.us")
# Constraint (Reed, 2026-11-12): only send as rook for now.
_ALLOWED_FROM = {"rook@ffilt.us"}
BLACKLIST_PATH = os.environ.get("EMAIL_SEND_BLACKLIST_PATH", "/data/email_send_blacklist.json")
LLM_API_BASE = os.environ.get("LLM_API_BASE", "http://192.168.1.2:8088/v1").rstrip("/")
LLM_CLASSIFY_MODEL = os.environ.get("LLM_CLASSIFY_MODEL", "auto")

_ABUSIVE_PATTERNS = [
    r"\bfuck\s+(?:you|off)\b",
    r"\bfuck\s*off\b",
    r"\bgo\s+fuck\s+yourself\b",
    r"\beat\s+shit\b",
    r"\bkill\s+yourself\b",
    r"\bdie\s+in\s+a\s+fire\b",
    r"\bpiss\s+off\b",
    r"\bdon'?t\s+(?:ever\s+)?(?:email|contact|message)\s+me\b",
    r"\bstop\s+(?:emailing|contacting|messaging)\s+me\b",
    r"\bleave\s+me\s+alone\b",
]

def _norm_email(addr: str) -> str:
    a = (addr or "").strip().lower()
    m = _re.search(r"<([^>]+)>", a)
    return (m.group(1) if m else a).strip()

def _load_blacklist():
    try:
        with open(BLACKLIST_PATH, "r", encoding="utf-8") as f:
            data = _json.load(f)
        if isinstance(data, dict):
            bl = data.get("blacklist")
            if isinstance(bl, list):
                return {_norm_email(x): {"reason": "blacklisted"} for x in bl if _norm_email(str(x))}
            entries = data.get("entries")
            if isinstance(entries, dict):
                return { _norm_email(k): (v if isinstance(v, dict) else {"reason": str(v)}) for k,v in entries.items() if _norm_email(k) }
        if isinstance(data, list):
            return {_norm_email(x): {"reason": "blacklisted"} for x in data if _norm_email(str(x))}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return {}

def _save_blacklist(bl: dict):
    try:
        d = os.path.dirname(BLACKLIST_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = BLACKLIST_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            _json.dump({"entries": bl}, f, indent=2, sort_keys=True)
        os.replace(tmp, BLACKLIST_PATH)
    except Exception:
        pass

def blacklist_add(addrs, reason: str = "abusive/hostile language"):
    bl = _load_blacklist()
    changed = False
    for a in (addrs or []):
        n = _norm_email(a)
        if not n or n in _ALLOWED_FROM:
            continue
        if n not in bl:
            bl[n] = {"reason": reason}
            changed = True
    if changed:
        _save_blacklist(bl)
    return bl

def _blacklisted_hits(addrs):
    bl = _load_blacklist()
    hits = []
    for a in (addrs or []):
        n = _norm_email(a)
        if n and n in bl:
            hits.append(n)
    # dedupe preserve order
    out = []
    seen = set()
    for h in hits:
        if h not in seen:
            seen.add(h); out.append(h)
    return out

def _detect_abusive(text: str) -> bool:
    t = (text or "").lower()
    return any(_re.search(p, t) for p in _ABUSIVE_PATTERNS)

def _addr_list(v):
    if v is None:
        return []
    if isinstance(v, str):
        # allow comma/semicolon separated
        parts = _re.split(r'[;,]+', v)
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


def _classify_agent_origin(subject: str, body: str, html: str | None = None) -> dict:
    """Call llm.homelab.local-compatible gateway to classify whether the email
    correctly identifies itself as agent-originating (automated assistant/AI/bot)."""
    text = (body or "").strip()
    if html and len(text) < 200:
        # include a small HTML hint if body is sparse
        text = (text + "\n\n[HTML excerpt]\n" + _re.sub(r"<[^>]+>", " ", html)[:1500]).strip()
    content = f"Subject: {subject or ''}\n\n{text[:4000]}"
    prompt = (
        "Classify whether this outgoing email correctly identifies itself as originating "
        "from an automated agent/assistant/bot/AI (not a human personally writing). "
        "Examples of sufficient identification: states it is an automated assistant, AI agent, bot, "
        "or clearly identifies Rook as an automated assistant/system. "
        "Return ONLY strict JSON: {\"agent_originating\": true|false, \"reason\": \"short reason\"}."
    )
    payload = {
        "model": LLM_CLASSIFY_MODEL,
        "messages": [
            {"role": "system", "content": "You are a strict classifier. Output valid JSON only."},
            {"role": "user", "content": prompt + "\n\nEMAIL:\n" + content},
        ],
        "temperature": 0,
    }
    data = _json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{LLM_API_BASE}/chat/completions",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            resp = _json.loads(r.read().decode(errors="replace") or "{}")
        msg = (((resp.get("choices") or [{}])[0]).get("message") or {})
        raw = (msg.get("content") or "").strip()
        # extract JSON object
        m = _re.search(r"\{.*\}", raw, _re.S)
        obj = _json.loads(m.group(0)) if m else {}
        identified = bool(obj.get("agent_originating") is True)
        reason = str(obj.get("reason") or raw or "")[:300]
        return {"ok": True, "agent_originating": identified, "reason": reason, "model": LLM_CLASSIFY_MODEL}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "model": LLM_CLASSIFY_MODEL}



def send(sender: str | None = None, to=None, recipients=None, subject: str = "",
         body: str = "", html: str | None = None, cc=None, bcc=None,
         in_reply_to: str | None = None, references=None,
         reply_to_abusive: bool = False) -> str:
    """Send an email via Namecheap Private Email SMTP.

    Auth uses SMTP_USERNAME/SMTP_PASSWORD (falls back to IMAP_USERNAME/IMAP_PASSWORD),
    same credentials as email-sync. Tries 465/SSL then 587/STARTTLS.
    Enforces From=rook@ffilt.us only. Calls LLM (model auto) to classify that the email
    correctly identifies itself as agent-originating; denies send if not identified.
    Enforces a persistent send blacklist (abusive/hostile contacts). Supports threading
    headers (In-Reply-To/References). Returns structured JSON; never includes credential material.
    """
    from_addr = (sender or SMTP_FROM_DEFAULT or "rook@ffilt.us").strip().lower()
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
    all_targets = to_addrs + cc_addrs + bcc_addrs
    if not to_addrs:
        return json.dumps({
            "ok": False, "error": "bad_request", "message": "to/recipients is required",
            "from": from_addr, "to": to_addrs, "subject": subject,
        })

    # Blacklist enforcement: if replying to abusive/hostile content (or message is abusive),
    # blacklist involved external recipients and deny send.
    if reply_to_abusive or _detect_abusive(subject) or _detect_abusive(body):
        ext = [a for a in all_targets if _norm_email(a) and _norm_email(a) not in _ALLOWED_FROM]
        if reply_to_abusive and ext:
            blacklist_add(ext, reason="abusive/hostile language reported (reply_to_abusive)")
        hits = _blacklisted_hits(all_targets)
        if reply_to_abusive and not hits and ext:
            hits = sorted({_norm_email(a) for a in ext})
        if hits or (reply_to_abusive and ext):
            return json.dumps({
                "ok": False,
                "error": "blacklisted",
                "message": "Send denied: abusive/hostile language reported; recipient(s) blacklisted.",
                "from": from_addr, "to": to_addrs, "cc": cc_addrs, "bcc": bcc_addrs,
                "subject": subject, "blacklisted": hits or sorted({_norm_email(a) for a in ext}),
            })

    hits = _blacklisted_hits(all_targets)
    if hits:
        return json.dumps({
            "ok": False,
            "error": "blacklisted",
            "message": "Send denied: one or more recipients are blacklisted due to abusive/hostile contact.",
            "from": from_addr, "to": to_addrs, "cc": cc_addrs, "bcc": bcc_addrs,
            "subject": subject, "blacklisted": hits,
        })
    if not SMTP_USER or not SMTP_PASS:
        return json.dumps({
            "ok": False, "error": "missing_credentials",
            "message": "SMTP auth credentials not configured (SMTP_USERNAME/SMTP_PASSWORD)",
            "from": from_addr, "to": to_addrs, "subject": subject,
        })

    # Require the email to correctly identify itself as agent-originating (LLM classification).
    classification = _classify_agent_origin(subject or "", body or "", html)
    if not classification.get("ok"):
        return json.dumps({
            "ok": False,
            "error": "agent_identity_classification_failed",
            "message": "Could not classify whether email identifies itself as agent-originating; send denied.",
            "from": from_addr, "to": to_addrs, "cc": cc_addrs, "bcc": bcc_addrs,
            "subject": subject or "", "classification": classification,
        })
    if not classification.get("agent_originating"):
        return json.dumps({
            "ok": False,
            "error": "agent_identity_required",
            "message": "Send denied: email must clearly identify itself as originating from an automated agent/assistant.",
            "from": from_addr, "to": to_addrs, "cc": cc_addrs, "bcc": bcc_addrs,
            "subject": subject or "", "classification": classification,
        })

    plain_body = body or ""
    html_body = html
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg["Subject"] = subject or ""
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="ffilt.us")
    if in_reply_to:
        msg["In-Reply-To"] = str(in_reply_to).strip()
    refs = _addr_list(references) if not isinstance(references, str) else [r.strip() for r in _re.split(r"[\s,]+", references) if r.strip()]
    if not refs and in_reply_to:
        refs = [str(in_reply_to).strip()]
    if refs:
        msg["References"] = " ".join(refs)
    msg.set_content(plain_body)
    if html_body is not None:
        msg.add_alternative(html_body, subtype="html")

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
                "in_reply_to": msg.get("In-Reply-To", ""),
                "references": msg.get("References", ""),
                "refused": refused or {},
                "agent_identity_classification": classification,
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
