"""system_checks.py — opaque homelab health checks for system_check_status.

Each check has a lowercase-hyphen id and returns (status, detail) where status
is one of pass|warn|fail|error. A single call runs many checks (or all when
checks is empty). "Opaque" — each result is a verdict + one-line detail, not a
data dump.

Check ids:
  router-alive    ICMP to the OPNsense router (192.168.1.1)
  ping-nodes      ICMP to the worker nodes (.50, .150)
  wan-access      short download test (1 MB from Cloudflare edge)
  disk-space      host disks via mcp-host-diag (/, /mnt/raid_store, /mnt/hot)
  cpu-load        host load average vs core count
  memory-pressure host RAM used vs total (available-based)
  node-cpu        k8s node CPU usage (metrics-server)
  node-memory     k8s node memory usage (metrics-server)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
import urllib.request

import k3s_tools
import storage_tools

log = logging.getLogger("mcp-system")

ROUTER_IP = os.environ.get("CHECK_ROUTER_IP", "192.168.1.1")
NODE_IPS = os.environ.get("CHECK_NODE_IPS", "192.168.1.50,192.168.1.150").split(",")
WAN_TEST_URL = os.environ.get("CHECK_WAN_URL", "https://speed.cloudflare.com/__down?bytes=1048576")
WAN_TEST_TIMEOUT = float(os.environ.get("CHECK_WAN_TIMEOUT", "15"))

# thresholds
DISK_WARN_PCT = float(os.environ.get("CHECK_DISK_WARN", "80"))
DISK_FAIL_PCT = float(os.environ.get("CHECK_DISK_FAIL", "95"))
CPU_WARN = float(os.environ.get("CHECK_CPU_WARN", "0.7"))
CPU_FAIL = float(os.environ.get("CHECK_CPU_FAIL", "1.5"))
MEM_WARN_PCT = float(os.environ.get("CHECK_MEM_WARN", "80"))
MEM_FAIL_PCT = float(os.environ.get("CHECK_MEM_FAIL", "90"))
NODE_CPU_WARN_PCT = float(os.environ.get("CHECK_NODE_CPU_WARN", "80"))
NODE_CPU_FAIL_PCT = float(os.environ.get("CHECK_NODE_CPU_FAIL", "95"))
NODE_MEM_WARN_PCT = float(os.environ.get("CHECK_NODE_MEM_WARN", "80"))
NODE_MEM_FAIL_PCT = float(os.environ.get("CHECK_NODE_MEM_FAIL", "95"))

CHECKS = {}  # id -> {"description": ..., "group": ..., "fn": ...}


def _check(cid: str, group: str, description: str):
    def deco(fn):
        CHECKS[cid] = {"id": cid, "group": group, "description": description, "fn": fn}
        return fn
    return deco


# ---------------------------------------------------------------- helpers
def _ping(host: str, count: int = 1) -> tuple[bool, str]:
    try:
        r = subprocess.run(
            ["ping", "-c", str(count), "-W", "2", host],
            capture_output=True, text=True, timeout=8,
        )
        if r.returncode == 0:
            m = [l for l in r.stdout.splitlines() if "time=" in l]
            return True, m[-1].split("time=")[-1].split(" ")[0] if m else "ok"
        return False, "no reply"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _host_diag():
    d = storage_tools.host_diag()
    if "error" in d:
        raise RuntimeError(d["error"])
    return d


def _pct_used(used_mb, avail_mb, total_mb):
    if total_mb in (None, 0, "0"):
        return None
    try:
        return round((float(used_mb) - float(avail_mb)) / float(total_mb) * 100, 1)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _parse_cpu(v: str) -> float:
    v = (v or "0").strip()
    if v.endswith("m"):
        return float(v[:-1]) / 1000.0
    if v.endswith("n"):  # nano-cores
        return float(v[:-1]) / 1e9
    return float(v)


def _parse_bytes(v: str) -> float:
    v = (v or "0").strip().upper()
    if v.endswith("N"):  # k8s Quantity nano-scale (usage AND capacity both scaled — ratio stays right)
        try:
            return float(v[:-1]) / 1e9
        except ValueError:
            return 0
    mult = {"K": 1024, "KI": 1024, "M": 1024**2, "MI": 1024**2, "G": 1024**3, "GI": 1024**3, "T": 1024**4, "TI": 1024**4}
    for suf, m in mult.items():
        if v.endswith(suf):
            try:
                return int(float(v[: -len(suf)]) * m)
            except ValueError:
                return 0
    try:
        return int(float(v))
    except ValueError:
        return 0


def _node_metrics() -> list[dict]:
    """[(name, cpu_pct, mem_pct, cpu_usage, mem_usage, cpu_cap, mem_cap)] via metrics API."""
    k3s_tools._load()
    met = k3s_tools._clients["custom"].list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")["items"]
    nodes = {n.metadata.name: n for n in k3s_tools._clients["core"].list_node().items}
    out = []
    for m in met:
        name = m["metadata"]["name"]
        usage = m.get("usage", {})
        cpu_u = _parse_cpu(usage.get("cpu", "0"))
        mem_u = _parse_bytes(usage.get("memory", "0"))
        node = nodes.get(name)
        cap = node.status.capacity if node else {}
        cpu_c = _parse_cpu(cap.get("cpu", "1"))
        mem_c = _parse_bytes(cap.get("memory", "1"))
        out.append({
            "name": name,
            "cpu_pct": round(cpu_u / cpu_c * 100, 1) if cpu_c else None,
            "mem_pct": round(mem_u / mem_c * 100, 1) if mem_c else None,
            "cpu_usage": usage.get("cpu"),
            "mem_usage": usage.get("memory"),
            "cpu_capacity": str(cap.get("cpu", "?")),
            "mem_capacity": str(cap.get("memory", "?")),
        })
    return out


# ---------------------------------------------------------------- checks
@_check("router-alive", "network", "ICMP to the OPNsense router (192.168.1.1)")
def _router_alive() -> tuple[str, str]:
    ok, detail = _ping(ROUTER_IP)
    return ("pass" if ok else "fail"), f"{ROUTER_IP}: {detail}"


@_check("ping-nodes", "network", "ICMP to worker nodes (192.168.1.50, 192.168.1.150)")
def _ping_nodes() -> tuple[str, str]:
    results = []
    ok_count = 0
    for ip in NODE_IPS:
        ip = ip.strip()
        if not ip:
            continue
        ok, detail = _ping(ip)
        ok_count += 1 if ok else 0
        results.append(f"{ip}:{detail}")
    if ok_count == len([i for i in NODE_IPS if i.strip()]):
        return "pass", "; ".join(results)
    if ok_count > 0:
        return "warn", "; ".join(results)
    return "fail", "; ".join(results)


@_check("wan-access", "network", "short download test (1 MB from Cloudflare edge)")
def _wan_access() -> tuple[str, str]:
    try:
        t0 = time.time()
        req = urllib.request.Request(WAN_TEST_URL, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"})
        with urllib.request.urlopen(req, timeout=WAN_TEST_TIMEOUT) as r:
            data = r.read()
        dt = time.time() - t0
        kb_s = len(data) / 1024.0 / max(dt, 0.01)
        if dt < 5:
            return "pass", f"{len(data)//1024}KB in {dt:.1f}s ({kb_s:.0f} KB/s)"
        if dt < WAN_TEST_TIMEOUT:
            return "warn", f"{len(data)//1024}KB in {dt:.1f}s ({kb_s:.0f} KB/s) — slow"
        return "fail", f"{len(data)//1024}KB in {dt:.1f}s — too slow"
    except Exception as exc:  # noqa: BLE001
        return "fail", f"download failed: {str(exc)[:120]}"


@_check("disk-space", "storage", "host disk usage (/, /mnt/raid_store, /mnt/hot) via df")
def _disk_space() -> tuple[str, str]:
    try:
        d = _host_diag()
        mounts = []
        for m in d.get("df", []):
            mount = m.get("mount", "")
            if mount in ("/",) or mount.startswith("/mnt/"):
                try:
                    pct = float(m.get("pct", "100"))
                except ValueError:
                    pct = 100.0
                mounts.append({"mount": mount, "pct": pct, "avail": m.get("avail")})
        if not mounts:
            return "error", "no host mounts found"
        worst = max(mounts, key=lambda x: x["pct"])
        detail = ", ".join(f"{x['mount']} {x['pct']:.0f}%" for x in mounts)
        if worst["pct"] >= DISK_FAIL_PCT:
            return "fail", f"{detail} (worst {worst['mount']} {worst['pct']:.0f}%)"
        if worst["pct"] >= DISK_WARN_PCT:
            return "warn", f"{detail} (worst {worst['mount']} {worst['pct']:.0f}%)"
        return "pass", detail
    except Exception as exc:  # noqa: BLE001
        return "error", str(exc)[:200]


@_check("cpu-load", "host", "host load average vs core count (1-min load)")
def _cpu_load() -> tuple[str, str]:
    try:
        d = _host_diag()
        la = float(d.get("loadavg", ["0"])[0])
        n = float(d.get("nproc", "1") or "1")
        ratio = la / n
        if ratio >= CPU_FAIL:
            return "fail", f"load {la:.2f} / {n:.0f} cores ({ratio:.2f}x)"
        if ratio >= CPU_WARN:
            return "warn", f"load {la:.2f} / {n:.0f} cores ({ratio:.2f}x)"
        return "pass", f"load {la:.2f} / {n:.0f} cores"
    except Exception as exc:  # noqa: BLE001
        return "error", str(exc)[:200]


@_check("memory-pressure", "host", "host RAM used vs total (available-based)")
def _memory_pressure() -> tuple[str, str]:
    try:
        d = _host_diag()
        mem = d.get("mem", {})
        total = float(mem.get("total", 0) or 0)
        avail = float(mem.get("available", 0) or 0)
        if total <= 0:
            return "error", "no mem data"
        pct = round((total - avail) / total * 100, 1)
        if pct >= MEM_FAIL_PCT:
            return "fail", f"{pct:.0f}% used ({avail:.0f}MB avail of {total:.0f}MB)"
        if pct >= MEM_WARN_PCT:
            return "warn", f"{pct:.0f}% used ({avail:.0f}MB avail of {total:.0f}MB)"
        return "pass", f"{pct:.0f}% used ({avail:.0f}MB avail of {total:.0f}MB)"
    except Exception as exc:  # noqa: BLE001
        return "error", str(exc)[:200]


@_check("node-cpu", "cluster", "k8s node CPU usage from metrics-server")
def _node_cpu() -> tuple[str, str]:
    try:
        rows = _node_metrics()
        if not rows:
            return "error", "no node metrics (metrics-server down?)"
        detail = ", ".join(f"{r['name']} {r['cpu_pct']:.0f}%" for r in rows if r["cpu_pct"] is not None)
        worst = max((r["cpu_pct"] for r in rows if r["cpu_pct"] is not None), default=0)
        if worst >= NODE_CPU_FAIL_PCT:
            return "fail", f"{detail} (worst {worst:.0f}%)"
        if worst >= NODE_CPU_WARN_PCT:
            return "warn", f"{detail} (worst {worst:.0f}%)"
        return "pass", detail or "no cpu data"
    except Exception as exc:  # noqa: BLE001
        return "error", str(exc)[:200]


@_check("node-memory", "cluster", "k8s node memory usage from metrics-server")
def _node_memory() -> tuple[str, str]:
    try:
        rows = _node_metrics()
        if not rows:
            return "error", "no node metrics (metrics-server down?)"
        detail = ", ".join(f"{r['name']} {r['mem_pct']:.0f}%" for r in rows if r["mem_pct"] is not None)
        worst = max((r["mem_pct"] for r in rows if r["mem_pct"] is not None), default=0)
        if worst >= NODE_MEM_FAIL_PCT:
            return "fail", f"{detail} (worst {worst:.0f}%)"
        if worst >= NODE_MEM_WARN_PCT:
            return "warn", f"{detail} (worst {worst:.0f}%)"
        return "pass", detail or "no mem data"
    except Exception as exc:  # noqa: BLE001
        return "error", str(exc)[:200]


# ---------------------------------------------------------------- MCP fns
def list_checks() -> str:
    """List available system checks (lowercase-hyphen ids) with descriptions."""
    out = [{"id": c["id"], "group": c["group"], "description": c["description"]}
           for c in CHECKS.values()]
    out.sort(key=lambda x: x["id"])
    return json.dumps({"count": len(out), "checks": out}, indent=1)


def check_status(checks: list[str] | None = None) -> str:
    """Run system checks. checks is a list of check ids (lowercase-hyphen);
    empty/null runs ALL checks. Returns one verdict per check:
    {id, status: pass|warn|fail|error, detail, took_ms}."""
    wanted = [c.strip() for c in (checks or []) if c and c.strip()]
    if not wanted:
        wanted = sorted(CHECKS.keys())
    results = []
    for cid in wanted:
        if cid not in CHECKS:
            results.append({"id": cid, "status": "error", "detail": "unknown check id (see system_list_checks)", "took_ms": 0})
            continue
        t0 = time.time()
        try:
            status, detail = CHECKS[cid]["fn"]()
        except Exception as exc:  # noqa: BLE001
            status, detail = "error", str(exc)[:200]
        results.append({"id": cid, "status": status, "detail": detail, "took_ms": round((time.time() - t0) * 1000)})
    return json.dumps({"count": len(results), "results": results}, indent=1)
