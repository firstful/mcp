"""tool_registry.py — single source of truth for the dynamic MCP tool surface.

Every agent tool registers here once: name, family, docstring (the
description), a JSON input schema derived from the function signature, and the
callable itself. Both surfaces draw from this registry:

  - static path: the legacy @mcp.tool() surface (STATIC_TOOLS=1, default)
  - dynamic path: discover_tool()/execute_tool() (always on)

The embedding index (tool_vectors.py) is built from this registry, so the
vectors and the MCP tool list can never drift apart.
"""

from __future__ import annotations

import inspect
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, get_type_hints

from pydantic import ValidationError, create_model

log = logging.getLogger("mcp-registry")


class ToolError(Exception):
    """Raised by the registry for unknown tools / bad payloads / tool failures.

    kind is one of: not_found | bad_request | validation | tool_error
    """

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind
        self.message = message


@dataclass
class ToolEntry:
    name: str
    description: str
    family: str
    schema: dict
    args_model: Any
    fn: Callable[..., str]


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolEntry] = {}

    def register(self, fn: Callable[..., str], *, family: str) -> ToolEntry:
        """Register a tool function. Name = fn.__name__, description = docstring."""
        name = fn.__name__
        desc = (inspect.getdoc(fn) or "").strip() or name
        model, schema = self._schema_for(fn)
        entry = ToolEntry(
            name=name,
            description=desc,
            family=family,
            schema=schema,
            args_model=model,
            fn=fn,
        )
        self._tools[name] = entry
        return entry

    @staticmethod
    def _schema_for(fn: Callable[..., Any]):
        """JSON Schema for the tool's input from its annotated signature."""
        hints = get_type_hints(fn)
        sig = inspect.signature(fn)
        fields: dict[str, tuple[Any, Any]] = {}
        for pname, p in sig.parameters.items():
            if pname in ("self", "cls"):
                continue
            ann = hints.get(pname, Any)
            if p.default is inspect.Parameter.empty:
                fields[pname] = (ann, ...)
            else:
                fields[pname] = (ann, p.default)
        model = create_model(f"{fn.__name__}_args", **fields)
        return model, model.model_json_schema()

    # ------------------------------------------------------------- queries
    def get(self, name: str) -> ToolEntry | None:
        return self._tools.get(name)

    def names(self) -> list[str]:
        return list(self._tools)

    def entries(self) -> list[ToolEntry]:
        return list(self._tools.values())

    def catalog(self) -> list[dict]:
        """Cheap enumeration: name, family, one-line description (no schemas)."""
        out = []
        for e in self._tools.values():
            first = e.description.splitlines()[0] if e.description else ""
            out.append({"name": e.name, "family": e.family, "description": first})
        return out

    # ---------------------------------------------------------- validation
    def validate(self, name: str, payload: dict) -> dict:
        """Coerce/validate payload against the tool's input schema.

        Returns the validated kwargs dict, or raises ToolError.
        """
        entry = self._tools.get(name)
        if entry is None:
            raise ToolError(
                "not_found",
                f"unknown tool '{name}' — run discover_tool() or tool_catalog() to see available tools",
            )
        if not isinstance(payload, dict):
            raise ToolError(
                "bad_request",
                f"payload must be a JSON object (got {type(payload).__name__})",
            )
        try:
            return entry.args_model(**payload).model_dump()
        except ValidationError as exc:
            msgs = [
                f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
                for e in exc.errors()
            ]
            raise ToolError("validation", "; ".join(msgs)) from exc

    # -------------------------------------------------------------- call
    def call(self, name: str, payload: dict | None = None) -> str:
        """Validate payload and invoke the tool. Returns its JSON string."""
        entry = self._tools.get(name)
        if entry is None:
            raise ToolError(
                "not_found",
                f"unknown tool '{name}' — run discover_tool() or tool_catalog() to see available tools",
            )
        args = self.validate(name, payload or {})
        try:
            return entry.fn(**args)
        except ToolError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("tool %s failed", name)
            raise ToolError("tool_error", f"{type(exc).__name__}: {exc}") from exc
