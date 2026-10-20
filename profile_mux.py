"""profile_mux.py — per-profile tool filtering via path-based mux.

A single FastMCP instance serves all profiles. The profile is extracted
from the URL path (/mcp/<profile>) by a Starlette middleware and made
available to the meta tools (discover_tool, execute_tool, tool_catalog,
omni) via a contextvar.

Config at /data/profile-mux.yaml (or $PROFILE_MUX_CONFIG):

  profiles:
    rook:
      families: [todo, docs, k3s, gitea, storage, system]
    archivist:
      families: [docs, gitea]
    scribe:
      families: [docs]

The meta family is always allowed (entry point).
Unknown profiles get all families (fail-open, same as today).
"""

from __future__ import annotations

import contextvars
import logging
import os
from pathlib import Path

import yaml

log = logging.getLogger("mcp-profile-mux")

DEFAULT_CONFIG_PATH = Path(os.environ.get("PROFILE_MUX_CONFIG", "/data/profile-mux.yaml"))

# Families always available to every profile.
ALWAYS_ALLOWED = {"meta"}

# Default families for unknown profiles (fail-open).
DEFAULT_FAMILIES = {"todo", "docs", "k3s", "gitea", "storage", "system", "meta"}

# ContextVar set by the middleware on every request.
profile_var: contextvars.ContextVar[str] = contextvars.ContextVar("mcp_profile", default="default")


def current_profile() -> str:
    return profile_var.get()


def allowed_families(profile: str) -> set[str]:
    cfg = load_config()
    families = set(cfg.get(profile.lower(), [])) | ALWAYS_ALLOWED
    return families


def load_config(path: Path | None = None) -> dict[str, list[str]]:
    p = path or DEFAULT_CONFIG_PATH
    if not p.exists():
        log.info("profile-mux config not found at %s — using defaults", p)
        return {}
    with open(p) as f:
        data = yaml.safe_load(f) or {}
    profiles = data.get("profiles") or {}
    return {
        name.strip().lower(): [f.strip().lower() for f in fams if f.strip()]
        for name, fams in profiles.items()
    }


def get_profile_families(profile: str) -> set[str]:
    cfg = load_config()
    return set(cfg.get(profile.lower(), [])) | ALWAYS_ALLOWED


def is_tool_allowed(tool_name: str, profile: str) -> bool:
    """Check if a tool is allowed for a profile based on its family."""
    from server import registry
    entry = registry.get(tool_name)
    if entry is None:
        return False
    return entry.family in get_profile_families(profile)


class FilteredRegistry:
    """A read-only view of a ToolRegistry filtered to a set of families.
    Used by omni and other meta tools to scope their view of the catalog."""

    def __init__(self, registry, allowed_families: set[str]):
        self._registry = registry
        self._allowed = allowed_families

    def entries(self):
        return [e for e in self._registry.entries() if e.family in self._allowed]

    def catalog(self):
        out = []
        for e in self.entries():
            first = e.description.splitlines()[0] if e.description else ""
            out.append({"name": e.name, "family": e.family, "description": first})
        return out

    def names(self):
        return [e.name for e in self.entries()]

    def get(self, name: str):
        entry = self._registry.get(name)
        if entry and entry.family in self._allowed:
            return entry
        return None

    def has_entry(self, name: str) -> bool:
        return self.get(name) is not None

    def call(self, name: str, payload: dict | None = None) -> str:
        entry = self.get(name)
        if entry is None:
            from tool_registry import ToolError
            raise ToolError(
                "not_found",
                f"unknown tool '{name}' for this profile — run discover_tool() or tool_catalog() to see available tools",
            )
        args = self._registry.validate(name, payload or {})
        try:
            return entry.fn(**args)
        except Exception as exc:
            from tool_registry import ToolError
            raise ToolError("tool_error", f"{type(exc).__name__}: {exc}") from exc
