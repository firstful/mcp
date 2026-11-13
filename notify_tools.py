"""notify_tools — push notification family for the homelab MCP server.

Sends push notifications to registered devices via a self-hosted ntfy broker
(ns `notify`, in-cluster). Primary use: notify Reed with a deep link to a
Discord conversation when agent input is needed.

Config (env):
  NOTIFY_NTFY_URL           base URL (default: http://ntfy.notify.svc.cluster.local)
  NOTIFY_NTFY_TOKEN         bearer token for ntfy publish (optional but recommended)
  NOTIFY_DEFAULT_TARGET     default logical target id (default: reed)
  NOTIFY_TARGETS            JSON map of target -> {topic, ...} for static routing
                            e.g. {"reed":{"topic":"rook-notify-reed-<secret>"}}
  NOTIFY_TOPIC_PREFIX       prefix for derived topics (default: rook-notify)
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from tool_registry import ToolError

NTFY_URL = os.environ.get("NOTIFY_NTFY_URL", "http://ntfy.notify.svc.cluster.local").rstrip("/")
NTFY_TOKEN = (os.environ.get("NOTIFY_NTFY_TOKEN") or "").strip()
DEFAULT_TARGET = (os.environ.get("NOTIFY_DEFAULT_TARGET") or "reed").strip() or "reed"
TOPIC_PREFIX = (os.environ.get("NOTIFY_TOPIC_PREFIX") or "rook-notify").strip() or "rook-notify"

_ALLOWED_HOSTS = {"discord.com", "www.discord.com", "ptb.discord.com", "canary.discord.com", "discordapp.com"}
_DISCORD_PATH_RE = re.compile(r"^/channels/(\d+)/(\d+)(?:/(\d+))?/?$")


def _load_targets() -> dict[str, dict[str, Any]]:
    raw = (os.environ.get("NOTIFY_TARGETS") or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            out: dict[str, dict[str, Any]] = {}
            for k, v in data.items():
                if isinstance(v, str):
                    out[str(k)] = {"topic": v}
                elif isinstance(v, dict):
                    out[str(k)] = v
            return out
    except Exception:
        return {}
    return {}


def _redact_topic(topic: str) -> str:
    if not topic:
        return ""
    if len(topic) <= 10:
        return topic[:2] + "***"
    return topic[:6] + "***" + topic[-4:]


def _normalize_discord_url(discord_url: str | None, guild_id: str | None,
                           channel_id: str | None, message_id: str | None) -> tuple[str | None, dict[str, str | None]]:
    ids = {"guild_id": None, "channel_id": None, "message_id": None}
    if discord_url:
        u = discord_url.strip()
        # accept bare discord URLs; only https
        try:
            p = urllib.parse.urlparse(u)
        except Exception as e:
            raise ToolError("bad_request", f"invalid discord_url: {e}") from e
        if p.scheme != "https":
            raise ToolError("bad_request", "discord_url must use https")
        host = (p.hostname or "").lower()
        if host not in _ALLOWED_HOSTS:
            raise ToolError("bad_request", f"discord_url host not allowed: {host}")
        m = _DISCORD_PATH_RE.match(p.path or "")
        if not m:
            raise ToolError("bad_request", "discord_url must be /channels/<guild_id>/<channel_id>[/<message_id>]")
        ids = {"guild_id": m.group(1), "channel_id": m.group(2), "message_id": m.group(3)}
        # canonicalize to discord.com
        canon = f"https://discord.com/channels/{ids['guild_id']}/{ids['channel_id']}"
        if ids["message_id"]:
            canon += f"/{ids['message_id']}"
        return canon, ids

    # build from ids
    g = str(guild_id).strip() if guild_id is not None else ""
    c = str(channel_id).strip() if channel_id is not None else ""
    m_id = str(message_id).strip() if message_id is not None else ""
    if not g and not c and not m_id:
        return None, ids
    if not (g.isdigit() and c.isdigit()):
        raise ToolError("bad_request", "guild_id and channel_id must be numeric strings/ids to build a Discord deep link")
    if m_id and not m_id.isdigit():
        raise ToolError("bad_request", "message_id must be numeric when provided")
    ids = {"guild_id": g, "channel_id": c, "message_id": m_id or None}
    canon = f"https://discord.com/channels/{g}/{c}"
    if m_id:
        canon += f"/{m_id}"
    return canon, ids


def _resolve_target(target: str | None) -> tuple[str, dict[str, Any]]:
    t = (target or DEFAULT_TARGET or "reed").strip()
    targets = _load_targets()
    if t in targets:
        info = targets[t] or {}
        topic = str(info.get("topic") or "").strip()
        if not topic:
            raise ToolError("bad_request", f"target '{t}' has no topic configured")
        return t, info
    # fallback: if exactly one target configured, allow implicit default; else derive if safe-ish name
    if not targets and t == "reed":
        # no static map: require explicit topic via env? derive placeholder won't work without secret.
        raise ToolError("bad_request",
                        "no notify targets configured (set NOTIFY_TARGETS JSON) and target 'reed' unresolved")
    if targets and t not in targets:
        raise ToolError("not_found", f"unknown notify target '{t}'. configured: {sorted(targets.keys())}")
    raise ToolError("not_found", f"unknown notify target '{t}'")


def _publish(topic: str, payload: dict[str, Any], priority: str, tags: list[str] | None,
             click_url: str | None, title: str) -> dict[str, Any]:
    # ntfy JSON publish to base URL
    url = NTFY_URL + "/"
    body = {
        "topic": topic,
        "title": title[:250],
        "message": (payload.get("body") or "")[:4000],
        "priority": priority,
        "tags": tags or ["rook", "discord"],
    }
    if click_url:
        body["click"] = click_url
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if NTFY_TOKEN:
        headers["Authorization"] = f"Bearer {NTFY_TOKEN}"
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode(errors="replace")
            status = resp.status
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:300]
        raise ToolError("tool_error", f"ntfy publish HTTP {e.code}: {detail}") from e
    except Exception as e:
        raise ToolError("tool_error", f"ntfy publish failed ({NTFY_URL}): {type(e).__name__}: {e}") from e
    out = {"http_status": status}
    try:
        j = json.loads(raw) if raw else {}
        if isinstance(j, dict):
            out["id"] = j.get("id")
    except Exception:
        pass
    return out


def push(title: str, body: str, target: str | None = None,
         discord_url: str | None = None, guild_id: str | None = None,
         channel_id: str | None = None, message_id: str | None = None,
         priority: str = "high", tags: list[str] | None = None,
         click_url: str | None = None, dedupe_key: str | None = None,
         source: str | None = None) -> str:
    """Send a push notification to a registered device via ntfy."""
    title = (title or "").strip()
    body_text = (body or "").strip()
    if not title:
        raise ToolError("bad_request", "title is required")
    if not body_text:
        raise ToolError("bad_request", "body is required")
    prio = (priority or "high").strip().lower()
    if prio not in {"min", "low", "default", "high", "urgent"}:
        raise ToolError("bad_request", "priority must be one of min|low|default|high|urgent")

    canon_url, ids = _normalize_discord_url(discord_url, guild_id, channel_id, message_id)
    open_url = (click_url or canon_url or "").strip() or None
    if click_url:
        # validate click_url is https discord or at least https
        try:
            cp = urllib.parse.urlparse(click_url)
            if cp.scheme not in ("https", "http"):
                raise ToolError("bad_request", "click_url must be http/https")
        except ToolError:
            raise
        except Exception as e:
            raise ToolError("bad_request", f"invalid click_url: {e}") from e

    target_id, info = _resolve_target(target)
    if not info.get("enabled", True):
        raise ToolError("bad_request", f"target '{target_id}' is disabled")
    topic = str(info.get("topic") or "").strip()
    if not topic:
        raise ToolError("bad_request", f"target '{target_id}' has no topic")

    payload = {
        "body": body_text,
        "discord_url": canon_url,
        "guild_id": ids.get("guild_id"),
        "channel_id": ids.get("channel_id"),
        "message_id": ids.get("message_id"),
        "source": source or "rook",
        "dedupe_key": dedupe_key,
    }
    pub = _publish(topic, payload, prio, tags, open_url, title)
    return json.dumps({
        "ok": True,
        "target": target_id,
        "transport": "ntfy",
        "ntfy_url": NTFY_URL,
        "topic_hint": _redact_topic(topic),
        "discord_url": canon_url,
        "provider": pub,
        "priority": prio,
    }, ensure_ascii=False)


def targets() -> str:
    """List configured notify targets (no secrets)."""
    t = _load_targets()
    out = []
    for k, v in t.items():
        out.append({
            "id": k,
            "enabled": bool(v.get("enabled", True)),
            "label": v.get("label") or k,
            "has_topic": bool(v.get("topic")),
            "topic_hint": _redact_topic(str(v.get("topic") or "")),
        })
    return json.dumps({
        "ok": True,
        "default_target": DEFAULT_TARGET,
        "ntfy_url": NTFY_URL,
        "configured": bool(t),
        "targets": out,
        "notes": [] if t else ["Set NOTIFY_TARGETS JSON env/secret to register device topics (e.g. {\"reed\":{\"topic\":\"...\"}})."]
    }, ensure_ascii=False)
