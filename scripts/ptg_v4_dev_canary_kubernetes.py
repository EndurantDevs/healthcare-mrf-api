"""In-cluster build attestation for the frozen PTG V3 oracle."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import httpx

from scripts.ptg_v4_dev_canary_io import CanaryInfrastructureError


FROZEN_V3_CANDIDATE_DEPLOYMENT = "healthcare-mrf-api-v3-candidate"
FROZEN_V3_CANDIDATE_SERVICE = "healthcare-mrf-api-v3-candidate"
FROZEN_V3_CANDIDATE_NAMESPACE = "healthporta-dev"
FROZEN_V3_CANDIDATE_IMAGE = (
    "ghcr.io/endurantdevs/healthcare-mrf-api-dev:"
    "dev-main-ca478d32-20260723160432"
)
_SERVICE_ACCOUNT_ROOT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
_SHA256_IMAGE_ID_PATTERN = re.compile(
    r"(?:^|@)sha256:([0-9a-f]{64})$"
)
_ALLOWED_SERVICE_HOSTS = frozenset(
    {
        FROZEN_V3_CANDIDATE_SERVICE,
        f"{FROZEN_V3_CANDIDATE_SERVICE}.{FROZEN_V3_CANDIDATE_NAMESPACE}",
        (
            f"{FROZEN_V3_CANDIDATE_SERVICE}."
            f"{FROZEN_V3_CANDIDATE_NAMESPACE}.svc"
        ),
        (
            f"{FROZEN_V3_CANDIDATE_SERVICE}."
            f"{FROZEN_V3_CANDIDATE_NAMESPACE}.svc.cluster.local"
        ),
    }
)


@dataclass(frozen=True)
class _DeploymentIdentity:
    """Validated frozen deployment identity."""

    uid: str
    generation: int


@dataclass(frozen=True)
class _PodIdentity:
    """Validated sole ready pod identity."""

    uid: str
    image_digest: str


def require_frozen_v3_service_host(hostname: str | None) -> str:
    """Reject an oracle URL that does not target the frozen V3 Service."""

    normalized = str(hostname or "").strip().lower()
    if normalized not in _ALLOWED_SERVICE_HOSTS:
        raise CanaryInfrastructureError(
            "V3 reference URL does not target the frozen candidate Service"
        )
    return normalized


async def collect_frozen_v3_candidate_evidence() -> dict[str, Any]:
    """Read the live Deployment and sole ready Pod from the in-cluster API."""

    api_url = _kubernetes_api_url()
    token = _read_service_account_file("token")
    ca_path = _SERVICE_ACCOUNT_ROOT / "ca.crt"
    verify: str | bool = str(ca_path) if ca_path.is_file() else True
    headers_by_name = {"Authorization": f"Bearer {token}"}
    timeout = httpx.Timeout(10)
    async with httpx.AsyncClient(
        base_url=api_url,
        headers=headers_by_name,
        timeout=timeout,
        verify=verify,
    ) as client:
        deployment = await _kubernetes_json(
            client,
            (
                f"/apis/apps/v1/namespaces/{FROZEN_V3_CANDIDATE_NAMESPACE}"
                f"/deployments/{FROZEN_V3_CANDIDATE_DEPLOYMENT}"
            ),
        )
        pod_list = await _kubernetes_json(
            client,
            f"/api/v1/namespaces/{FROZEN_V3_CANDIDATE_NAMESPACE}/pods",
            parameters={
                "labelSelector": (
                    "app.kubernetes.io/name="
                    f"{FROZEN_V3_CANDIDATE_DEPLOYMENT}"
                )
            },
        )
    return build_frozen_v3_runtime_evidence(deployment, pod_list)


def build_frozen_v3_runtime_evidence(
    deployment_by_field: Mapping[str, Any],
    pod_list_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate Kubernetes objects and return their non-secret build identity."""

    deployment_identity = _frozen_deployment_identity(
        deployment_by_field
    )
    pod_identity = _frozen_pod_identity(pod_list_by_field)
    return {
        "contract": "ptg_v3_frozen_candidate_runtime_v1",
        "namespace": FROZEN_V3_CANDIDATE_NAMESPACE,
        "deployment": FROZEN_V3_CANDIDATE_DEPLOYMENT,
        "service": FROZEN_V3_CANDIDATE_SERVICE,
        "deployment_uid": deployment_identity.uid,
        "deployment_generation": deployment_identity.generation,
        "pod_uid": pod_identity.uid,
        "image": FROZEN_V3_CANDIDATE_IMAGE,
        "image_digest": pod_identity.image_digest,
    }


def _frozen_deployment_identity(
    deployment_by_field: Mapping[str, Any],
) -> _DeploymentIdentity:
    """Validate the pinned candidate Deployment and its exact config sources."""

    metadata_by_field = _mapping(deployment_by_field.get("metadata"))
    specification_by_field = _mapping(deployment_by_field.get("spec"))
    status_by_field = _mapping(deployment_by_field.get("status"))
    pod_template_by_field = _mapping(
        specification_by_field.get("template")
    )
    pod_specification_by_field = _mapping(
        pod_template_by_field.get("spec")
    )
    api_container_rows = [
        container_by_field
        for container_by_field in _mapping_rows(
            pod_specification_by_field.get("containers")
        )
        if container_by_field.get("name") == "healthcare-mrf-api"
    ]
    config_map_names = _candidate_config_map_names(api_container_rows)
    generation = _positive_int(metadata_by_field.get("generation"))
    status_counts_are_ready = all(
        _positive_int(status_by_field.get(field_name)) == 1
        for field_name in (
            "replicas",
            "readyReplicas",
            "updatedReplicas",
            "availableReplicas",
        )
    )
    is_valid = bool(
        metadata_by_field.get("name") == FROZEN_V3_CANDIDATE_DEPLOYMENT
        and metadata_by_field.get("namespace")
        == FROZEN_V3_CANDIDATE_NAMESPACE
        and str(metadata_by_field.get("uid") or "").strip()
        and generation is not None
        and _positive_int(specification_by_field.get("replicas")) == 1
        and _positive_int(status_by_field.get("observedGeneration"))
        == generation
        and status_counts_are_ready
        and len(api_container_rows) == 1
        and api_container_rows[0].get("image")
        == FROZEN_V3_CANDIDATE_IMAGE
        and "mrf-api-config" in config_map_names
        and "mrf-api-ptg-v3-candidate-config" in config_map_names
        and "mrf-api-ptg-v4-candidate-config" not in config_map_names
    )
    if not is_valid:
        raise CanaryInfrastructureError(
            "frozen V3 candidate Deployment is not singular and ready"
        )
    return _DeploymentIdentity(
        uid=str(metadata_by_field["uid"]),
        generation=int(generation),
    )


def _candidate_config_map_names(
    api_container_rows: list[dict[str, Any]],
) -> set[str]:
    if len(api_container_rows) != 1:
        return set()
    return {
        str(_mapping(source_by_field.get("configMapRef")).get("name") or "")
        for source_by_field in _mapping_rows(
            api_container_rows[0].get("envFrom")
        )
    }


def _frozen_pod_identity(
    pod_list_by_field: Mapping[str, Any],
) -> _PodIdentity:
    """Validate the sole ready pod and its immutable image digest."""

    pod_rows = _mapping_rows(pod_list_by_field.get("items"))
    if len(pod_rows) != 1:
        raise CanaryInfrastructureError(
            "frozen V3 candidate Deployment is not singular and ready"
        )
    pod_by_field = pod_rows[0]
    metadata_by_field = _mapping(pod_by_field.get("metadata"))
    status_by_field = _mapping(pod_by_field.get("status"))
    api_status_rows = [
        status_row_by_field
        for status_row_by_field in _mapping_rows(
            status_by_field.get("containerStatuses")
        )
        if status_row_by_field.get("name") == "healthcare-mrf-api"
    ]
    has_ready_condition = any(
        condition_by_field.get("type") == "Ready"
        and condition_by_field.get("status") == "True"
        for condition_by_field in _mapping_rows(
            status_by_field.get("conditions")
        )
    )
    image_identity = (
        str(api_status_rows[0].get("imageID") or "")
        if len(api_status_rows) == 1
        else ""
    )
    image_digest_match = _SHA256_IMAGE_ID_PATTERN.search(image_identity)
    is_valid = bool(
        metadata_by_field.get("namespace")
        == FROZEN_V3_CANDIDATE_NAMESPACE
        and metadata_by_field.get("deletionTimestamp") is None
        and str(metadata_by_field.get("uid") or "").strip()
        and status_by_field.get("phase") == "Running"
        and has_ready_condition
        and len(api_status_rows) == 1
        and api_status_rows[0].get("ready") is True
        and api_status_rows[0].get("image")
        == FROZEN_V3_CANDIDATE_IMAGE
        and image_digest_match is not None
    )
    if not is_valid or image_digest_match is None:
        raise CanaryInfrastructureError(
            "frozen V3 candidate Pod is not uniquely ready on the pinned image"
        )
    return _PodIdentity(
        uid=str(metadata_by_field["uid"]),
        image_digest=str(image_digest_match.group(1)),
    )


def validate_frozen_v3_runtime_evidence(
    evidence_by_field: Mapping[str, Any],
) -> list[str]:
    """Validate persisted Kubernetes attestation fields fail-closed."""

    failures: list[str] = []
    if (
        evidence_by_field.get("contract")
        != "ptg_v3_frozen_candidate_runtime_v1"
        or evidence_by_field.get("namespace")
        != FROZEN_V3_CANDIDATE_NAMESPACE
        or evidence_by_field.get("deployment")
        != FROZEN_V3_CANDIDATE_DEPLOYMENT
        or evidence_by_field.get("service") != FROZEN_V3_CANDIDATE_SERVICE
        or evidence_by_field.get("image") != FROZEN_V3_CANDIDATE_IMAGE
        or not re.fullmatch(
            r"[0-9a-f]{64}",
            str(evidence_by_field.get("image_digest") or ""),
        )
        or not str(evidence_by_field.get("deployment_uid") or "").strip()
        or not str(evidence_by_field.get("pod_uid") or "").strip()
        or _positive_int(evidence_by_field.get("deployment_generation")) is None
    ):
        failures.append("frozen V3 runtime build attestation is invalid")
    return failures


async def _kubernetes_json(
    client: httpx.AsyncClient,
    path: str,
    *,
    parameters: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    try:
        response = await client.get(path, params=dict(parameters or {}))
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise CanaryInfrastructureError(
            "Kubernetes runtime attestation failed"
        ) from exc
    if not isinstance(payload, dict):
        raise CanaryInfrastructureError(
            "Kubernetes runtime attestation returned invalid data"
        )
    return payload


def _kubernetes_api_url() -> str:
    host = str(os.getenv("KUBERNETES_SERVICE_HOST") or "").strip()
    port = str(os.getenv("KUBERNETES_SERVICE_PORT_HTTPS") or "443").strip()
    if not host or not port.isdigit():
        raise CanaryInfrastructureError(
            "in-cluster Kubernetes API coordinates are unavailable"
        )
    return f"https://{host}:{port}"


def _read_service_account_file(filename: str) -> str:
    try:
        value = (_SERVICE_ACCOUNT_ROOT / filename).read_text(
            encoding="utf-8"
        ).strip()
    except OSError as exc:
        raise CanaryInfrastructureError(
            "Kubernetes service-account identity is unavailable"
        ) from exc
    if not value:
        raise CanaryInfrastructureError(
            "Kubernetes service-account identity is empty"
        )
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in value
        if isinstance(row, Mapping)
    ] if isinstance(value, list) else []


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
