# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Minimal in-cluster I/O for one controller-owned PTG wave Job.

This module deliberately separates mutating calls from observations.  The
durable controller must claim and commit an operation ticket before calling
``post_wave_job`` or ``delete_wave_job``.  After an ambiguous result it may
only use the GET/list helpers below.
"""

from __future__ import annotations

import urllib.parse
from typing import Any, Mapping

from api.control_workers import (
    _KubernetesApiError as KubernetesApiError,
    _kubernetes_namespace,
    _kubernetes_request,
)
from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    _job_name,
    _require_text,
    _wave_labels,
    validate_ptg_wave_job_manifest,
)


def post_wave_job(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """POST one already-validated desired Job exactly once."""

    validate_ptg_wave_job_manifest(manifest)
    response = _kubernetes_request(
        "POST",
        f"/apis/batch/v1/namespaces/{_encoded_namespace()}/jobs",
        dict(manifest),
    )
    return _object_response(response, "Kubernetes Job POST")


def get_wave_job(wave_digest: str) -> dict[str, Any] | None:
    """GET the canonical Job name, translating only HTTP 404 to absence."""

    job_name = _job_name(wave_digest)
    try:
        response = _kubernetes_request(
            "GET",
            _job_path(job_name),
        )
    except KubernetesApiError as exc:
        if exc.status == 404:
            return None
        raise
    return _object_response(response, "Kubernetes Job GET")


def list_wave_pods(wave_digest: str) -> list[dict[str, Any]]:
    """List only Pods carrying the complete controller-owned wave labels."""

    labels = _wave_labels(wave_digest)
    selector = ",".join(
        f"{key}={label_value}" for key, label_value in sorted(labels.items())
    )
    path = (
        f"/api/v1/namespaces/{_encoded_namespace()}/pods?"
        + urllib.parse.urlencode({"labelSelector": selector})
    )
    response = _object_response(
        _kubernetes_request("GET", path),
        "Kubernetes Pod list",
    )
    if response.get("apiVersion") != "v1" or response.get("kind") != "PodList":
        raise PTGWaveContractError("Kubernetes Pod list must be a v1 PodList")
    pod_items = response.get("items")
    if not isinstance(pod_items, list) or not all(
        isinstance(pod_item, dict) for pod_item in pod_items
    ):
        raise PTGWaveContractError(
            "Kubernetes Pod list must contain an object items array"
        )
    pods = []
    for pod_item in pod_items:
        if (
            pod_item.get("apiVersion", "v1") != "v1"
            or pod_item.get("kind", "Pod") != "Pod"
        ):
            raise PTGWaveContractError("Kubernetes Pod list must contain v1 Pod items")
        pod_map = dict(pod_item)
        pod_map.setdefault("apiVersion", "v1")
        pod_map.setdefault("kind", "Pod")
        pods.append(pod_map)
    return pods


def list_generic_ptg_jobs() -> list[dict[str, Any]]:
    """List only launcher-managed PTG-family Jobs; FHIR is excluded."""

    selector = ",".join((
        "app.kubernetes.io/managed-by=healthporta-worker-launcher",
        "healthporta.com/engine=mrf",
    ))
    path = (
        f"/apis/batch/v1/namespaces/{_encoded_namespace()}/jobs?"
        + urllib.parse.urlencode({"labelSelector": selector})
    )
    response = _object_response(
        _kubernetes_request("GET", path),
        "generic Kubernetes Job list",
    )
    job_objects = response.get("items")
    if not isinstance(job_objects, list) or not all(
        isinstance(job_object, dict) for job_object in job_objects
    ):
        raise PTGWaveContractError(
            "generic Kubernetes Job list must contain an object items array"
        )
    ptg_jobs: list[dict[str, Any]] = []
    for job_object in job_objects:
        metadata = job_object.get("metadata")
        annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
        worker_class = (
            str(annotations.get("healthporta.com/worker-class") or "").strip()
            if isinstance(annotations, dict)
            else ""
        )
        if worker_class.startswith("process.PTG"):
            ptg_jobs.append(dict(job_object))
    return ptg_jobs


def delete_wave_job(wave_digest: str, job_uid: str) -> dict[str, Any]:
    """Issue one UID-preconditioned foreground DELETE for the exact Job."""

    uid = _require_text("Kubernetes Job UID", job_uid)
    response = _kubernetes_request(
        "DELETE",
        _job_path(_job_name(wave_digest)),
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": {"uid": uid},
            "propagationPolicy": "Foreground",
        },
    )
    return _object_response(response, "Kubernetes Job DELETE")


def wave_absence_observation(wave_digest: str) -> dict[str, Any]:
    """Return GET-only Job and Pod absence evidence after a DELETE ticket."""

    job = get_wave_job(wave_digest)
    pods = list_wave_pods(wave_digest)
    return {
        "job_absent": job is None,
        "pod_count": len(pods),
        "pods_absent": not pods,
    }


def _encoded_namespace() -> str:
    namespace = _require_text("Kubernetes namespace", _kubernetes_namespace())
    return urllib.parse.quote(namespace, safe="")


def _job_path(job_name: str) -> str:
    encoded_name = urllib.parse.quote(
        _require_text("Kubernetes Job name", job_name),
        safe="",
    )
    return (
        f"/apis/batch/v1/namespaces/{_encoded_namespace()}/jobs/"
        f"{encoded_name}"
    )


def _object_response(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PTGWaveContractError(f"{location} response must be an object")
    return dict(value)


__all__ = [
    "KubernetesApiError",
    "delete_wave_job",
    "get_wave_job",
    "list_generic_ptg_jobs",
    "list_wave_pods",
    "post_wave_job",
    "wave_absence_observation",
]
