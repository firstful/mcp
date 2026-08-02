"""storage_tools.py — host storage diagnostics via system calls.

"Just call system": df, mdadm, smartctl, lsblk. The mcp pod cannot see the
CP host's disks (container filesystem only), so host commands run through a
forced-command SSH diag to the CP host (Secret mcp-host-ssh, two keys: base
and --smart variant). In dev mode (no KUBERNETES_SERVICE_HOST) commands run
locally.

Host diag script: /usr/local/sbin/mcp-host-diag on sutliffserver (JSON out).
Every function returns a JSON string.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess

log = logging.getLogger("mcp-storage")

MCP_HOST = os.environ.get("MCP_HOST", "192.168.1.2")
MCP_HOST_USER = os.environ.get("MCP_HOST_USER", "eizooz")
SSH_DIR = os.environ.get("MCP_HOST_SSH_DIR", "/run/secrets/mcp-host-ssh")
DIAG_TIMEOUT = int(os.environ.get("MCP_HOST_DIAG_TIMEOUT", "60"))


def _in_cluster() -> bool:
    return bool(os.environ.get("KUBERNETES_SERVICE_HOST"))


def host_diag(include_smart: bool = False) -> dict:
    """Fetch host diagnostics JSON from the CP host. Returns dict; on failure
    returns {"error": ...}."""
    extra = ["--smart"] if include_smart else []
    if _in_cluster():
        key = os.path.join(SSH_DIR, "id_ed25519_smart" if include_smart else "id_ed25519")
        if not os.path.exists(key):
            return {"error": f"ssh key missing: {key}"}
        cmd = [
            "ssh", "-i", key,
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=8",
            "-o", "LogLevel=ERROR",
            f"{MCP_HOST_USER}@{MCP_HOST}",
        ] + extra
    else:
        cmd = ["sudo", "-n", "/usr/local/sbin/mcp-host-diag"] + extra
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=DIAG_TIMEOUT)
        if r.returncode != 0:
            return {"error": f"diag rc={r.returncode}: {(r.stderr or r.stdout).strip()[:300]}"}
        return json.loads(r.stdout)
    except json.JSONDecodeError as exc:
        return {"error": f"bad diag JSON: {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def disk_usage() -> str:
    """df on the host: real filesystems with size/used/avail/pct (system call)."""
    d = host_diag()
    if "error" in d:
        return json.dumps(d)
    mounts = []
    for m in d.get("df", []):
        mounts.append({
            "filesystem": m.get("fs"),
            "mount": m.get("mount"),
            "size": m.get("size"),
            "used": m.get("used"),
            "avail": m.get("avail"),
            "used_pct": m.get("pct"),
        })
    return json.dumps({"host": d.get("host"), "count": len(mounts), "mounts": mounts}, indent=1)


def raid_status() -> str:
    """mdadm RAID array status from the host (/proc/mdstat + mdadm --detail)."""
    d = host_diag()
    if "error" in d:
        return json.dumps(d)
    return json.dumps({
        "host": d.get("host"),
        "mdstat": d.get("mdstat", "").strip(),
        "arrays": [
            {"device": a.get("device"), "detail": a.get("detail", "").strip()}
            for a in d.get("mdadm_arrays", [])
        ],
    }, indent=1)


def smart_health() -> str:
    """SMART health per physical disk on the host (smartctl -H; slower — ~seconds/disk)."""
    d = host_diag(include_smart=True)
    if "error" in d:
        return json.dumps(d)
    disks = []
    for disk in d.get("disks", []):
        smart = disk.get("smart")
        disks.append({
            "name": disk.get("name"),
            "size": disk.get("size"),
            "model": disk.get("model"),
            "smart_passed": smart if smart is not None else "n/a",
        })
    return json.dumps({"host": d.get("host"), "count": len(disks), "disks": disks}, indent=1)


def disks() -> str:
    """Physical disks on the host from lsblk (name, size, model) — fast, no smartctl."""
    d = host_diag()
    if "error" in d:
        return json.dumps(d)
    disks = [{
        "name": x.get("name"),
        "size": x.get("size"),
        "model": x.get("model"),
        "serial": x.get("serial"),
    } for x in d.get("disks", [])]
    return json.dumps({"host": d.get("host"), "count": len(disks), "disks": disks}, indent=1)
