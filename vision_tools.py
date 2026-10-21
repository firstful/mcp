"""vision_tools — Florence-2 vision family over vision-api (ns `vision`).

API contract (reeds/vision, SPEC-VISION-API v2): POST /v1/{describe,query,
detect,kind} with {"model","image"(data: URI), "question"|"object"} →
{"text"} / {"answer"} / {"objects":[{x1,y1,x2,y2}]} / {"kind"}.

Wake-on-call: both Deployments (vision-api + vision-detect-worker) are parked
at 0 by default and carry NO `replicas:` in git (Flux does not own them), so
the tool may scale them freely. vision_read wakes → waits for BOTH endpoints
(the api silently degrades to "" if its worker is down — never acceptable for
a read) → calls → leaves them up; park explicitly with vision_scale(0).

RBAC: Role mcp-vision-wake in ns vision (deployments get, deployments/scale
get/patch) bound to sa/mcp. Netpols: allow-vision-egress (mcp) +
allow-mcp-ingress (vision) on pod port 8080.
"""
from __future__ import annotations

import base64
import json
import os
import ssl
import time
import urllib.error
import urllib.request

from tool_registry import ToolError

API = os.environ.get("VISION_API", "http://vision-api.vision.svc.cluster.local")
WORKER_SVC = os.environ.get("VISION_WORKER_SVC", "vision-detect-worker")
NS = "vision"
DEPLOYMENTS = ["vision-api", "vision-detect-worker"]
WAKE_TIMEOUT = int(os.environ.get("VISION_WAKE_TIMEOUT", "120"))
CALL_TIMEOUT = int(os.environ.get("VISION_CALL_TIMEOUT", "90"))
CAPS = ("describe", "query", "detect", "kind")
SA_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
SA_CA = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
K8S = "https://kubernetes.default.svc"

_ctx = ssl.create_default_context(cafile=SA_CA)


def _token() -> str:
    with open(SA_TOKEN) as fh:
        return fh.read().strip()


def _k8s(path: str, method: str = "GET", body: dict | None = None):
    headers = {"Authorization": "Bearer " + _token()}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/merge-patch+json"
        data = json.dumps(body).encode()
    req = urllib.request.Request(K8S + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, context=_ctx, timeout=10) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}


def _replicas(dep: str):
    try:
        return (_k8s(f"/apis/apps/v1/namespaces/{NS}/deployments/{dep}/scale")
                .get("spec", {}).get("replicas"))
    except Exception:
        return None


def _ready_addresses(svc: str) -> int:
    try:
        d = _k8s(f"/api/v1/namespaces/{NS}/endpoints/{svc}")
    except urllib.error.HTTPError as e:
        return 0 if e.code == 404 else 0
    return sum(len(s.get("addresses") or []) for s in (d.get("subsets") or []))


def _set_replicas(dep: str, n: int) -> None:
    _k8s(f"/apis/apps/v1/namespaces/{NS}/deployments/{dep}/scale",
         "PATCH", {"spec": {"replicas": n}})


def _avx_nodes() -> int:
    """Nodes labeled rook.io/avx=true — the worker nodeSelector needs one."""
    try:
        d = _k8s("/api/v1/nodes?labelSelector=rook.io%2Favx%3Dtrue")
        return len(d.get("items") or [])
    except Exception:
        return -1


def status() -> str:
    out = {"namespace": NS, "deployments": {}, "api_ready": _ready_addresses("vision-api"),
           "worker_ready": _ready_addresses(WORKER_SVC), "avx_nodes": _avx_nodes()}
    for dep in DEPLOYMENTS:
        out["deployments"][dep] = {"replicas": _replicas(dep)}
    out["awake"] = out["api_ready"] > 0 and out["worker_ready"] > 0
    return json.dumps(out)


def scale(replicas: int) -> str:
    replicas = int(replicas)
    if replicas not in (0, 1):
        raise ToolError("bad_request", "replicas must be 0 (park) or 1 (up)")
    applied = {}
    for dep in DEPLOYMENTS:
        _set_replicas(dep, replicas)
        applied[dep] = replicas
    return json.dumps({"scaled": applied,
                       "note": None if replicas else "parked; wake happens automatically on next vision_read"})


def _ensure_awake() -> None:
    if _ready_addresses("vision-api") > 0 and _ready_addresses(WORKER_SVC) > 0:
        return
    woken = []
    for dep in DEPLOYMENTS:
        if (_replicas(dep) or 0) < 1:
            _set_replicas(dep, 1)
            woken.append(dep)
    deadline = time.time() + WAKE_TIMEOUT
    while time.time() < deadline:
        if _ready_addresses("vision-api") > 0 and _ready_addresses(WORKER_SVC) > 0:
            return
        time.sleep(3)
    avx = _avx_nodes()
    hint = ""
    if avx <= 0:
        hint = (f" No node carries rook.io/avx=true — label one "
                f"(kubectl label node <node> rook.io/avx=true) or drop the "
                f"vision-detect-worker nodeSelector (vision-service-ops skill).")
    raise ToolError("tool_error",
                    f"vision wake timed out after {WAKE_TIMEOUT}s (woken={woken}, "
                    f"api_ready={_ready_addresses('vision-api')}, "
                    f"worker_ready={_ready_addresses(WORKER_SVC)}, avx_nodes={avx}).{hint}")


def _data_uri(image: str) -> str:
    s = (image or "").strip()
    if not s:
        raise ToolError("bad_request", "image is required (data: URI or raw base64)")
    if s.startswith("data:"):
        if ";base64," not in s:
            raise ToolError("bad_request", "only base64 data URIs are supported")
        return s
    # raw base64: sniff the media type from the first bytes
    try:
        head = base64.b64decode(s[:32] + "=" * (-len(s[:32]) % 4), validate=False)
    except Exception as e:  # noqa: BLE001
        raise ToolError("bad_request", f"image is not valid base64: {e}") from e
    if head.startswith(b"\x89PNG"):
        mime = "image/png"
    elif head.startswith(b"\xff\xd8\xff"):
        mime = "image/jpeg"
    elif head.startswith((b"GIF87", b"GIF89")):
        mime = "image/gif"
    elif head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        mime = "image/webp"
    else:
        mime = "image/jpeg"  # forwarder downscales; unknown types are best-effort
    return f"data:{mime};base64,{s}"


def read(capability: str, image: str, question: str | None = None,
         object_name: str | None = None, model: str = "florence-2",
         wake: bool = True) -> str:
    capability = (capability or "").strip().lower()
    if capability not in CAPS:
        raise ToolError("bad_request", f"capability must be one of {CAPS}")
    if capability == "query" and not (question or "").strip():
        raise ToolError("bad_request", "query requires a `question` (VQA prompt)")
    payload = {"model": model, "image": _data_uri(image)}
    if question:
        payload["question"] = question
    if object_name:
        payload["object"] = object_name
    if wake:
        _ensure_awake()
    req = urllib.request.Request(
        f"{API}/v1/{capability}", data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=CALL_TIMEOUT) as r:
            body = json.loads(r.read().decode(errors="replace") or "{}")
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")[:400]
        raise ToolError("tool_error", f"vision-api HTTP {e.code}: {detail}") from e
    except Exception as e:  # noqa: BLE001
        raise ToolError("tool_error", f"vision-api unreachable ({API}): {e}") from e
    # degrade signals empty results per spec — surface them, never pass silent "" home
    if capability in ("describe", "query") and not body.get("text") and not body.get("answer"):
        raise ToolError("tool_error",
                        "vision-api returned an EMPTY result (degraded mode — its "
                        "worker was unreachable mid-call). Check vision_status and retry.")
    return json.dumps({"capability": capability, "model": body.get("model", model),
                       "result": body})
