"""browser_tools.py — CDP-based browser session handoff tools for agents.

Communicates with Rayobrowse sessions via Chrome DevTools Protocol (CDP)
over WebSocket. Rayobrowse runs Chromium with ``--headless=new`` which
bypasses Xvfb, so VNC is black and VNC input won't reach Chromium.
CDP still works fine — screenshots, input dispatch, navigation.

All CDP communication goes through the proxy at ``browse.homelab.internal``
(WebSocket) which creates a Rayobrowse session on-demand.

Environment:
  BROWSE_CDP_PROXY  — CDP proxy WebSocket base URL
                      (default: ws://browse.homelab.internal/cdp)
  RAYOBROWSE_API    — Rayobrowse API endpoint for session list/create/close
                      (default: http://10.42.2.179:9222)
  BROWSER_SESSION_TIMEOUT — seconds to wait for CDP operations (default: 30)
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
import urllib.request
import urllib.parse
from typing import Any

log = logging.getLogger("mcp-browser")

# ---------------------------------------------------------------------------
# Configuration

CDP_PROXY = os.environ.get("BROWSE_CDP_PROXY", "ws://browse.homelab.internal/cdp").rstrip("/")
RAYOBROWSE_API = os.environ.get("RAYOBROWSE_API", "http://10.42.2.179:9222").rstrip("/")
SESSION_TIMEOUT = int(os.environ.get("BROWSER_SESSION_TIMEOUT", "30"))

# ---------------------------------------------------------------------------
# Session tracking (in-memory, best-effort — refreshed on list)

_session_cache: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Synchronous wrappers around async CDP operations
# (MCP tools are synchronous, but CDP is async over WebSocket)


def _run_coro(coro, timeout: float = SESSION_TIMEOUT) -> Any:
    """Run an async coroutine synchronously with a timeout."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # We're inside an existing event loop (e.g. uvicorn) — use a thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=timeout)
    else:
        return asyncio.run(coro)


# ---------------------------------------------------------------------------
# CDP WebSocket client (lightweight — no external deps beyond websockets)


class CDPSession:
    """Minimal CDP WebSocket client."""

    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self.ws = None
        self._msg_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._listener_task = None

    async def connect(self, timeout: float = SESSION_TIMEOUT):
        import websockets
        self.ws = await asyncio.wait_for(
            websockets.connect(self.ws_url, max_size=50 * 1024 * 1024),
            timeout=timeout,
        )
        self._listener_task = asyncio.create_task(self._listen())

    async def disconnect(self):
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None
        if self.ws:
            await self.ws.close()
            self.ws = None

    async def _listen(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                msg_id = msg.get("id")
                if msg_id and msg_id in self._pending:
                    fut = self._pending.pop(msg_id)
                    if not fut.done():
                        fut.set_result(msg)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            # Fail all pending futures
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(exc)
            self._pending.clear()

    async def send(self, method: str, params: dict | None = None, timeout: float = SESSION_TIMEOUT) -> dict:
        self._msg_id += 1
        msg_id = self._msg_id
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        payload = {"id": msg_id, "method": method, "params": params or {}}
        await self.ws.send(json.dumps(payload))
        result = await asyncio.wait_for(fut, timeout=timeout)
        if "error" in result:
            raise RuntimeError(f"CDP error: {result['error']}")
        return result.get("result", {})

    async def send_no_wait(self, method: str, params: dict | None = None):
        self._msg_id += 1
        msg_id = self._msg_id
        payload = {"id": msg_id, "method": method, "params": params or {}}
        await self.ws.send(json.dumps(payload))

    async def wait_for_event(self, event_name: str, timeout: float = SESSION_TIMEOUT) -> dict:
        """Wait for a specific CDP event."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=min(remaining, 5.0))
                msg = json.loads(raw)
                if msg.get("method") == event_name:
                    return msg.get("params", {})
            except asyncio.TimeoutError:
                continue
        raise TimeoutError(f"Timeout waiting for event {event_name}")
        return {}


# ---------------------------------------------------------------------------
# Rayobrowse HTTP API helpers


def _http_get(url: str, timeout: int = 10) -> dict:
    """HTTP GET with timeout, returns parsed JSON."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
        return json.loads(body) if body.strip() else {}


def _http_post(url: str, data: dict | None = None, timeout: int = 10) -> dict:
    """HTTP POST with timeout, returns parsed JSON."""
    body = json.dumps(data).encode() if data else b""
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", "replace")
        return json.loads(raw) if raw.strip() else {}


def _http_delete(url: str, timeout: int = 10) -> dict:
    """HTTP DELETE with timeout."""
    req = urllib.request.Request(url, method="DELETE")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
        return json.loads(body) if body.strip() else {}


# ---------------------------------------------------------------------------
# Browser session operations


def list_sessions() -> str:
    """List active Rayobrowse browser sessions with status and metadata."""
    try:
        data = _http_get(f"{RAYOBROWSE_API}/browsers", timeout=10)
        sessions = []
        for key, info in data.items():
            if not isinstance(info, dict):
                continue
            session_id = info.get("id") or key
            ws_url = info.get("webSocketDebuggerUrl", "")
            vnc_url = ""
            if info.get("vncToken"):
                vnc_url = f"https://vnc.ffilt.us/sid?token={info['vncToken']}"
            sessions.append({
                "session_id": session_id,
                "status": info.get("status", "unknown"),
                "vnc_token": info.get("vncToken", ""),
                "vnc_url": vnc_url,
                "cdp_ws": ws_url,
                "created": info.get("createdAt", info.get("created", "")),
                "current_url": info.get("currentUrl", info.get("url", "")),
                "title": info.get("title", ""),
            })
        return json.dumps({"count": len(sessions), "sessions": sessions})
    except Exception as exc:
        log.exception("list_sessions failed")
        return json.dumps({"error": f"{type(exc).__name__}: {exc}"})


def create_session(os_type: str = "windows", headless: bool = True,
                     vnc: bool = True, url: str = "") -> str:
    """Create a new Rayobrowse browser session via /connect API.

    Returns session ID, CDP WebSocket URL, and VNC URL.
    """
    try:
        params = {
            "os": os_type,
            "headless": str(headless).lower(),
            "vnc": str(vnc).lower(),
            "keepAlive": "true",
        }
        if url:
            params["url"] = url
        qs = urllib.parse.urlencode(params)
        api_url = f"{RAYOBROWSE_API}/connect?{qs}"
        data = _http_get(api_url, timeout=30)

        session_id = data.get("id") or data.get("sessionId") or data.get("token", "")
        cdp_ws = data.get("webSocketDebuggerUrl", data.get("wsUrl", ""))
        vnc_token = data.get("vncToken", data.get("token", ""))
        vnc_url = f"https://vnc.ffilt.us/sid?token={vnc_token}" if vnc_token else ""

        # If the API doesn't return a direct CDP URL, construct one via proxy
        if not cdp_ws and session_id:
            cdp_ws = f"{CDP_PROXY}/{session_id}"

        result = {
            "session_id": session_id,
            "cdp_ws": cdp_ws,
            "vnc_token": vnc_token,
            "vnc_url": vnc_url,
        }
        # Cache for later use
        if session_id:
            _session_cache[session_id] = {
                "cdp_ws": cdp_ws,
                "vnc_url": vnc_url,
                "created": time.time(),
            }
        return json.dumps(result)
    except Exception as exc:
        log.exception("create_session failed")
        return json.dumps({"error": f"{type(exc).__name__}: {exc}"})


def _resolve_session_ws(session_id: str) -> str:
    """Resolve the CDP WebSocket URL for a session."""
    # Check cache first
    if session_id in _session_cache:
        return _session_cache[session_id]["cdp_ws"]

    # Try to look it up from the Rayobrowse API
    try:
        data = _http_get(f"{RAYOBROWSE_API}/browsers", timeout=10)
        for key, info in data.items():
            if not isinstance(info, dict):
                continue
            sid = info.get("id") or key
            if sid == session_id:
                ws_url = info.get("webSocketDebuggerUrl", "")
                if ws_url:
                    _session_cache[session_id] = {"cdp_ws": ws_url, "created": time.time()}
                    return ws_url
    except Exception:
        pass

    # Fall back to the proxy URL pattern
    return f"{CDP_PROXY}/{session_id}"


def close_session(session_id: str) -> str:
    """Close a Rayobrowse browser session."""
    try:
        # Try CDP close first
        ws_url = _resolve_session_ws(session_id)
        result = _run_coro(_cdp_close(ws_url))
        _session_cache.pop(session_id, None)
        return json.dumps({"session_id": session_id, "closed": True, "result": result})
    except Exception as exc:
        log.warning("CDP close failed for %s, trying API: %s", session_id, exc)
        try:
            _http_get(f"{RAYOBROWSE_API}/disconnect/{session_id}", timeout=10)
            _session_cache.pop(session_id, None)
            return json.dumps({"session_id": session_id, "closed": True, "method": "api"})
        except Exception as exc2:
            return json.dumps({"session_id": session_id, "closed": False,
                              "error": f"CDP: {exc}; API: {exc2}"})


async def _cdp_close(ws_url: str) -> dict:
    """Close a CDP session by closing the target."""
    session = CDPSession(ws_url)
    try:
        await session.connect(timeout=10)
        # Get targets to find the page target
        targets = await session.send("Target.getTargets")
        page_targets = [t for t in targets.get("targetInfos", [])
                       if t.get("type") == "page"]
        for t in page_targets:
            try:
                await session.send("Target.closeTarget", {"targetId": t["targetId"]})
            except Exception:
                pass
        return {"closed_targets": len(page_targets)}
    finally:
        await session.disconnect()


# ---------------------------------------------------------------------------
# Screenshot


def take_screenshot(session_id: str) -> str:
    """Capture a screenshot of the browser session via CDP.

    Returns base64-encoded PNG image data.
    """
    try:
        ws_url = _resolve_session_ws(session_id)
        result = _run_coro(_cdp_screenshot(ws_url))
        return json.dumps(result)
    except Exception as exc:
        log.exception("screenshot failed for %s", session_id)
        return json.dumps({"error": f"{type(exc).__name__}: {exc}"})


async def _cdp_screenshot(ws_url: str) -> dict:
    """Take a CDP screenshot."""
    session = CDPSession(ws_url)
    try:
        await session.connect(timeout=15)
        # Enable Page domain
        await session.send("Page.enable")
        # Ignore certificate errors
        await session.send("Security.setIgnoreCertificateErrors", {"ignore": True})
        # Wait a bit for any pending renders
        await asyncio.sleep(0.5)
        # Capture screenshot
        result = await session.send("Page.captureScreenshot", {
            "format": "png",
            "quality": 80,
        }, timeout=20)
        img_data = result.get("data", "")
        return {
            "screenshot": img_data,
            "format": "png",
            "size_bytes": len(base64.b64decode(img_data)) if img_data else 0,
        }
    finally:
        await session.disconnect()


# ---------------------------------------------------------------------------
# Navigation


def navigate(session_id: str, url: str) -> str:
    """Navigate the browser to a URL via CDP."""
    try:
        ws_url = _resolve_session_ws(session_id)
        result = _run_coro(_cdp_navigate(ws_url, url))
        return json.dumps(result)
    except Exception as exc:
        log.exception("navigate failed for %s", session_id)
        return json.dumps({"error": f"{type(exc).__name__}: {exc}"})


async def _cdp_navigate(ws_url: str, url: str) -> dict:
    """Navigate via CDP."""
    session = CDPSession(ws_url)
    try:
        await session.connect(timeout=15)
        # Enable required domains
        await session.send("Page.enable")
        await session.send("Security.setIgnoreCertificateErrors", {"ignore": True})
        # Navigate
        result = await session.send("Page.navigate", {"url": url}, timeout=20)
        # Wait for page to load
        try:
            await session.wait_forEvent("Page.loadEventFired", timeout=15)
        except TimeoutError:
            pass  # Some pages never fire loadEventFired (SPA, redirects)
        # Get current URL and title
        try:
            eval_result = await session.send("Runtime.evaluate", {
                "expression": "JSON.stringify({url: window.location.href, title: document.title})"
            }, timeout=5)
            page_info = json.loads(eval_result.get("result", {}).get("value", "{}"))
        except Exception:
            page_info = {}
        return {
            "navigated_to": url,
            "frame_id": result.get("frameId", ""),
            "error_text": result.get("errorText", ""),
            "current_url": page_info.get("url", ""),
            "title": page_info.get("title", ""),
        }
    finally:
        await session.disconnect()


# ---------------------------------------------------------------------------
# Interaction


def interact(session_id: str, action: str, **kwargs) -> str:
    """Send input events to the browser session via CDP.

    Actions:
      - click: x, y (coordinates)
      - type: text (string to type)
      - key: key (key name), code, modifiers (optional)
      - scroll: x, y, delta_x, delta_y
      - hover: x, y
    """
    try:
        ws_url = _resolve_session_ws(session_id)
        result = _run_coro(_cdp_interact(ws_url, action, **kwargs))
        return json.dumps(result)
    except Exception as exc:
        log.exception("interact failed for %s", session_id)
        return json.dumps({"error": f"{type(exc).__name__}: {exc}"})


async def _cdp_interact(ws_url: str, action: str, **kwargs) -> dict:
    """Dispatch input events via CDP."""
    session = CDPSession(ws_url)
    try:
        await session.connect(timeout=15)
        await session.send("Page.enable")

        if action == "click":
            x = kwargs.get("x", 0)
            y = kwargs.get("y", 0)
            # Mouse press
            await session.send("Input.dispatchMouseEvent", {
                "type": "mousePressed",
                "x": x, "y": y,
                "button": "left",
                "clickCount": 1,
            })
            # Mouse release
            await session.send("Input.dispatchMouseEvent", {
                "type": "mouseReleased",
                "x": x, "y": y,
                "button": "left",
                "clickCount": 1,
            })
            return {"action": "click", "x": x, "y": y}

        elif action == "type":
            text = kwargs.get("text", "")
            for char in text:
                await session.send("Input.dispatchKeyEvent", {
                    "type": "keyDown",
                    "text": char,
                    "key": char,
                })
                await session.send("Input.dispatchKeyEvent", {
                    "type": "keyUp",
                    "key": char,
                })
            return {"action": "type", "text_length": len(text)}

        elif action == "key":
            key = kwargs.get("key", "")
            code = kwargs.get("code", "")
            modifiers = kwargs.get("modifiers", 0)
            await session.send("Input.dispatchKeyEvent", {
                "type": "keyDown",
                "key": key,
                "code": code,
                "modifiers": modifiers,
            })
            await session.send("Input.dispatchKeyEvent", {
                "type": "keyUp",
                "key": key,
                "code": code,
                "modifiers": modifiers,
            })
            return {"action": "key", "key": key}

        elif action == "scroll":
            x = kwargs.get("x", 0)
            y = kwargs.get("y", 0)
            delta_x = kwargs.get("delta_x", 0)
            delta_y = kwargs.get("delta_y", 0)
            await session.send("Input.dispatchMouseEvent", {
                "type": "mouseWheel",
                "x": x, "y": y,
                "deltaX": delta_x,
                "deltaY": delta_y,
            })
            return {"action": "scroll", "x": x, "y": y, "delta_x": delta_x, "delta_y": delta_y}

        elif action == "hover":
            x = kwargs.get("x", 0)
            y = kwargs.get("y", 0)
            await session.send("Input.dispatchMouseEvent", {
                "type": "mouseMoved",
                "x": x, "y": y,
            })
            return {"action": "hover", "x": x, "y": y}

        else:
            return {"error": f"unknown action: {action}"}
    finally:
        await session.disconnect()
