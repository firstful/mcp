"""k3s_tools.py — read-only Kubernetes cluster query tools for agents.

Uses the official kubernetes python client. In-cluster (ServiceAccount) when
running as a pod; falls back to kubeconfig (~/.kube/config or KUBECONFIG) for
local dev. Every function returns a JSON string (MCP convention).

RBAC needed (see apps/mcp/rbac.yaml in sutliff-cluster): get/list/watch on
core (nodes, namespaces, services, endpoints, pods, pods/log, pvc, pv,
events), apps (deployments, statefulsets, daemonsets, replicasets),
traefik.io (ingressroutes), storage.k8s.io (storageclasses). Read-only.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("mcp-k3s")

_loaded = False
_clients: dict = {}


def _load() -> None:
    global _loaded
    if _loaded:
        return
    from kubernetes import client, config  # lazy import keeps startup fast

    try:
        if os.environ.get("KUBERNETES_SERVICE_HOST"):
            config.load_incluster_config()
            log.info("k8s config: in-cluster ServiceAccount")
        else:
            config.load_kube_config()
            log.info("k8s config: kubeconfig (%s)", os.environ.get("KUBECONFIG", "~/.kube/config"))
    except Exception as exc:  # noqa: BLE001
        log.warning("k8s config load failed: %s", exc)
        raise
    _clients["core"] = client.CoreV1Api()
    _clients["apps"] = client.AppsV1Api()
    _clients["custom"] = client.CustomObjectsApi()
    _clients["storage"] = client.StorageV1Api()
    _loaded = True


def _err(exc: Exception) -> str:
    reason = getattr(exc, "reason", None)
    status = getattr(exc, "status", None)
    body = getattr(exc, "body", None)
    msg = f"{reason or type(exc).__name__}"
    if status:
        msg += f" (HTTP {status})"
    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            pass
    if body:
        msg += f": {str(body)[:300]}"
    return json.dumps({"error": msg})


def _age(ts: datetime | None) -> str:
    if not ts:
        return "?"
    try:
        age = datetime.now(timezone.utc) - ts
        s = int(age.total_seconds())
    except Exception:  # noqa: BLE001
        return "?"
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m"
    if s < 129600:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def _ready(pod) -> str:
    try:
        ready, total = 0, len(pod.status.container_statuses or [])
        for cs in pod.status.container_statuses or []:
            if cs.ready:
                ready += 1
        return f"{ready}/{total}"
    except Exception:  # noqa: BLE001
        return "?"


def _node_status(node) -> str:
    for cond in node.status.conditions or []:
        if cond.type == "Ready":
            return cond.status if cond.status in ("True", "False") else cond.status
    return "Unknown"


def _node_roles(node) -> str:
    roles = [k.replace("node-role.kubernetes.io/", "") for k in (node.metadata.labels or {}) if k.startswith("node-role.kubernetes.io/")]
    return ",".join(sorted(roles)) or "worker"


def _svc_ports(svc) -> list:
    out = []
    for p in svc.spec.ports or []:
        out.append({
            "name": p.name,
            "port": p.port,
            "targetPort": getattr(p.target_port, "value", p.target_port) if hasattr(p.target_port, "value") else p.target_port,
            "nodePort": p.node_port,
            "protocol": p.protocol,
        })
    return out


def _pod_restarts(pod) -> int:
    return sum(cs.restart_count for cs in (pod.status.container_statuses or []))


# ---------------------------------------------------------------- tools
def nodes() -> str:
    """List cluster nodes: name, status (Ready), roles, k8s version, internal IP, age."""
    try:
        _load()
        items = _clients["core"].list_node().items
        out = [{
            "name": n.metadata.name,
            "status": _node_status(n),
            "roles": _node_roles(n),
            "version": n.status.node_info.kubelet_version if n.status.node_info else "?",
            "internal_ip": next((a.address for a in (n.status.addresses or []) if a.type == "InternalIP"), "?"),
            "age": _age(n.metadata.creation_timestamp),
        } for n in items]
        return json.dumps({"count": len(out), "nodes": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def namespaces() -> str:
    """List namespaces with status and age."""
    try:
        _load()
        items = _clients["core"].list_namespace().items
        out = [{
            "name": n.metadata.name,
            "status": n.status.phase,
            "age": _age(n.metadata.creation_timestamp),
        } for n in items]
        return json.dumps({"count": len(out), "namespaces": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_services(namespace: str | None = None) -> str:
    """List Services (ClusterIP/LoadBalancer etc). namespace=None means all namespaces."""
    try:
        _load()
        if namespace:
            items = _clients["core"].list_namespaced_service(namespace).items
        else:
            items = _clients["core"].list_service_for_all_namespaces().items
        out = [{
            "namespace": s.metadata.namespace,
            "name": s.metadata.name,
            "type": s.spec.type,
            "cluster_ip": s.spec.cluster_ip,
            "external_ip": ",".join(
                (i.ip or i.hostname or "") for i in (s.status.load_balancer.ingress or [])
            ),
            "ports": _svc_ports(s),
            "selector": s.spec.selector or {},
            "age": _age(s.metadata.creation_timestamp),
        } for s in items]
        return json.dumps({"count": len(out), "services": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_service(name: str, namespace: str) -> str:
    """Get one Service by name (namespace required)."""
    try:
        _load()
        s = _clients["core"].read_namespaced_service(name, namespace)
        out = {
            "namespace": s.metadata.namespace,
            "name": s.metadata.name,
            "type": s.spec.type,
            "cluster_ip": s.spec.cluster_ip,
            "external_ips": s.spec.external_i_ps or s.status.load_balancer.ingress,
            "ports": _svc_ports(s),
            "selector": s.spec.selector or {},
            "age": _age(s.metadata.creation_timestamp),
        }
        return json.dumps(out, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_pods(namespace: str | None = None, label_selector: str | None = None) -> str:
    """List Pods. namespace=None means all namespaces. label_selector e.g. 'app=talaria'."""
    try:
        _load()
        kw = {}
        if label_selector:
            kw["label_selector"] = label_selector
        if namespace:
            items = _clients["core"].list_namespaced_pod(namespace, **kw).items
        else:
            items = _clients["core"].list_pod_for_all_namespaces(**kw).items
        out = [{
            "namespace": p.metadata.namespace,
            "name": p.metadata.name,
            "node": p.spec.node_name,
            "status": p.status.phase,
            "ready": _ready(p),
            "restarts": _pod_restarts(p),
            "pod_ip": p.status.pod_ip,
            "age": _age(p.metadata.creation_timestamp),
        } for p in items]
        return json.dumps({"count": len(out), "pods": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_pod(name: str, namespace: str) -> str:
    """Get one Pod by name (namespace required). Includes container statuses."""
    try:
        _load()
        p = _clients["core"].read_namespaced_pod(name, namespace)
        out = {
            "namespace": p.metadata.namespace,
            "name": p.metadata.name,
            "node": p.spec.node_name,
            "phase": p.status.phase,
            "pod_ip": p.status.pod_ip,
            "host_ip": p.status.host_ip,
            "restarts": _pod_restarts(p),
            "containers": [
                {
                    "name": c.name,
                    "ready": c.ready,
                    "restarts": c.restart_count,
                    "state": (c.state.to_dict() if c.state else {}),
                    "image": c.image,
                }
                for c in (p.status.container_statuses or [])
            ],
            "age": _age(p.metadata.creation_timestamp),
        }
        return json.dumps(out, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def pod_logs(name: str, namespace: str, tail: int = 100, container: str | None = None) -> str:
    """Tail container logs of a pod. tail = number of lines (default 100)."""
    try:
        _load()
        kw: dict = {"tail_lines": max(1, min(int(tail), 5000))}
        if container:
            kw["container"] = container
        logs = _clients["core"].read_namespaced_pod_log(name, namespace, **kw)
        return json.dumps({"pod": f"{namespace}/{name}", "tail": kw["tail_lines"], "logs": logs})
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_deployments(namespace: str | None = None) -> str:
    """List Deployments with desired/ready replicas."""
    try:
        _load()
        if namespace:
            items = _clients["apps"].list_namespaced_deployment(namespace).items
        else:
            items = _clients["apps"].list_deployment_for_all_namespaces().items
        out = [{
            "namespace": d.metadata.namespace,
            "name": d.metadata.name,
            "replicas": f"{d.status.ready_replicas or 0}/{d.spec.replicas or 0}",
            "image": (d.spec.template.spec.containers[0].image if d.spec.template.spec.containers else ""),
            "age": _age(d.metadata.creation_timestamp),
        } for d in items]
        return json.dumps({"count": len(out), "deployments": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def get_deployment(name: str, namespace: str) -> str:
    """Get one Deployment by name (namespace required)."""
    try:
        _load()
        d = _clients["apps"].read_namespaced_deployment(name, namespace)
        out = {
            "namespace": d.metadata.namespace,
            "name": d.metadata.name,
            "replicas": {
                "desired": d.spec.replicas,
                "ready": d.status.ready_replicas,
                "available": d.status.available_replicas,
            },
            "selector": d.spec.selector.match_labels or {},
            "containers": [c.name for c in d.spec.template.spec.containers],
            "images": [c.image for c in d.spec.template.spec.containers],
            "node_selector": d.spec.template.spec.node_selector or {},
            "age": _age(d.metadata.creation_timestamp),
        }
        return json.dumps(out, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_statefulsets(namespace: str | None = None) -> str:
    """List StatefulSets with ready replicas."""
    try:
        _load()
        if namespace:
            items = _clients["apps"].list_namespaced_stateful_set(namespace).items
        else:
            items = _clients["apps"].list_stateful_set_for_all_namespaces().items
        out = [{
            "namespace": s.metadata.namespace,
            "name": s.metadata.name,
            "replicas": f"{s.status.ready_replicas or 0}/{s.spec.replicas or 0}",
            "age": _age(s.metadata.creation_timestamp),
        } for s in items]
        return json.dumps({"count": len(out), "statefulsets": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_daemonsets(namespace: str | None = None) -> str:
    """List DaemonSets with ready replicas."""
    try:
        _load()
        if namespace:
            items = _clients["apps"].list_namespaced_daemon_set(namespace).items
        else:
            items = _clients["apps"].list_daemon_set_for_all_namespaces().items
        out = [{
            "namespace": d.metadata.namespace,
            "name": d.metadata.name,
            "ready": f"{d.status.number_ready}/{d.status.desired_number_scheduled}",
            "age": _age(d.metadata.creation_timestamp),
        } for d in items]
        return json.dumps({"count": len(out), "daemonsets": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_ingressroutes(namespace: str | None = None) -> str:
    """List Traefik IngressRoutes with their Host() match rules (the ffilt.us routing table)."""
    try:
        _load()
        if namespace:
            items = _clients["custom"].list_namespaced_custom_object(
                "traefik.io", "v1alpha1", namespace, "ingressroutes"
            )["items"]
        else:
            items = _clients["custom"].list_cluster_custom_object(
                "traefik.io", "v1alpha1", "ingressroutes"
            )["items"]
        out = [{
            "namespace": r["metadata"]["namespace"],
            "name": r["metadata"]["name"],
            "match": [route.get("match") for route in r["spec"].get("routes", [])],
            "age": _age(datetime.fromisoformat(r["metadata"]["creationTimestamp"].replace("Z", "+00:00")))
            if "creationTimestamp" in r["metadata"] else "?",
        } for r in items]
        return json.dumps({"count": len(out), "ingressroutes": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_pvcs(namespace: str | None = None) -> str:
    """List PersistentVolumeClaims with status, storage class, capacity."""
    try:
        _load()
        if namespace:
            items = _clients["core"].list_namespaced_persistent_volume_claim(namespace).items
        else:
            items = _clients["core"].list_persistent_volume_claim_for_all_namespaces().items
        out = [{
            "namespace": p.metadata.namespace,
            "name": p.metadata.name,
            "status": p.status.phase,
            "storage_class": p.spec.storage_class_name,
            "capacity": p.status.capacity.get("storage") if p.status.capacity else None,
            "requested": p.spec.resources.requests.get("storage") if p.spec.resources.requests else None,
            "access_modes": p.spec.access_modes,
            "age": _age(p.metadata.creation_timestamp),
        } for p in items]
        return json.dumps({"count": len(out), "pvcs": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_storageclasses() -> str:
    """List StorageClasses (longhorn, nfs-media, local-path, ...)."""
    try:
        _load()
        items = _clients["storage"].list_storage_class().items
        out = [{
            "name": s.metadata.name,
            "provisioner": s.provisioner,
            "reclaim_policy": s.reclaim_policy,
            "default": s.metadata.annotations.get("storageclass.kubernetes.io/is-default-class") == "true",
        } for s in items]
        return json.dumps({"count": len(out), "storageclasses": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)


def list_events(namespace: str | None = None, limit: int = 50) -> str:
    """Recent Events (warnings usually first). limit max 200."""
    try:
        _load()
        limit = max(1, min(int(limit), 200))
        if namespace:
            items = _clients["core"].list_namespaced_event(namespace).items
        else:
            items = _clients["core"].list_event_for_all_namespaces().items
        items.sort(key=lambda e: e.last_timestamp or e.event_time or e.metadata.creation_timestamp or datetime.now(timezone.utc), reverse=True)
        out = [{
            "namespace": e.metadata.namespace,
            "type": e.type,
            "reason": e.reason,
            "involved": f"{e.involved_object.kind}/{e.involved_object.name}" if e.involved_object else "",
            "message": (e.message or "")[:200],
            "count": e.count,
            "last": (e.last_timestamp or e.event_time or e.metadata.creation_timestamp).isoformat() if (e.last_timestamp or e.event_time or e.metadata.creation_timestamp) else "",
        } for e in items[:limit]]
        return json.dumps({"count": len(out), "events": out}, indent=1)
    except Exception as exc:  # noqa: BLE001
        return _err(exc)
