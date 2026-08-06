# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure live-object attestation for an exact PTG Indexed Job."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTGWaveJobContract,
    PTG_WAVE_SLOT_COUNT,
    _job_name,
    _mapping,
    _require_text,
    _wave_annotations,
    _wave_labels,
    _worker_config_identity_from_template,
    validate_ptg_wave_job_manifest,
)


@dataclass(frozen=True)
class PTGWaveKubernetesAttestation:
    wave_digest: str
    queue: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    config_identity: str
    manifest_identity: str
    job_name: str
    job_uid: str
    runtime_image_identity: str
    pod_uid_by_slot: dict[int, str]


@dataclass(frozen=True)
class PTGWaveExistingJobAttestation:
    wave_digest: str
    queue: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    config_identity: str
    manifest_identity: str
    image_identity: str
    runtime_image_identity: str
    job_name: str
    job_uid: str


def attest_existing_ptg_wave_job(
    manifest: Mapping[str, Any],
    actual_job: Mapping[str, Any],
) -> PTGWaveExistingJobAttestation:
    """Attest an existing server-side Job before any wave Pod is Ready."""

    contract = validate_ptg_wave_job_manifest(manifest)
    job_name, job_uid = _attest_actual_job(contract, manifest, actual_job)
    return PTGWaveExistingJobAttestation(
        wave_digest=contract.wave_digest,
        queue=contract.queue,
        manifest_digest=contract.manifest_digest,
        jobs_digest=contract.jobs_digest,
        job_count=contract.job_count,
        config_identity=contract.config_identity,
        manifest_identity=contract.manifest_identity,
        image_identity=contract.image,
        runtime_image_identity=contract.runtime_image_identity,
        job_name=job_name,
        job_uid=job_uid,
    )


def attest_ptg_wave_kubernetes_objects(
    manifest: Mapping[str, Any],
    actual_job: Mapping[str, Any],
    actual_pods: Sequence[Mapping[str, Any]],
) -> PTGWaveKubernetesAttestation:
    """Attest the exact live Job and twelve Ready Pods from Kubernetes API data."""

    contract = validate_ptg_wave_job_manifest(manifest)
    job_name, job_uid = _attest_actual_job(contract, manifest, actual_job)
    if len(actual_pods) != PTG_WAVE_SLOT_COUNT:
        raise PTGWaveContractError("Kubernetes must report exactly twelve wave pods")
    pod_uid_by_slot: dict[int, str] = {}
    actual_runtime_image_ids: set[str] = set()
    for actual_pod in actual_pods:
        slot, pod_uid, actual_runtime_image_id = _attest_actual_pod(
            contract,
            actual_pod,
            job_name=job_name,
            job_uid=job_uid,
        )
        if slot in pod_uid_by_slot:
            raise PTGWaveContractError("Kubernetes pods repeat a completion index")
        if pod_uid in pod_uid_by_slot.values():
            raise PTGWaveContractError("each wave slot requires one unique pod UID")
        pod_uid_by_slot[slot] = pod_uid
        actual_runtime_image_ids.add(actual_runtime_image_id)
    if set(pod_uid_by_slot) != set(range(PTG_WAVE_SLOT_COUNT)):
        raise PTGWaveContractError("Kubernetes pods must cover indexes zero through eleven")
    if actual_runtime_image_ids != {contract.runtime_image_identity}:
        raise PTGWaveContractError(
            "actual pod imageIDs do not equal the runtime image identity"
        )
    return PTGWaveKubernetesAttestation(
        wave_digest=contract.wave_digest,
        queue=contract.queue,
        manifest_digest=contract.manifest_digest,
        jobs_digest=contract.jobs_digest,
        job_count=contract.job_count,
        config_identity=contract.config_identity,
        manifest_identity=contract.manifest_identity,
        job_name=job_name,
        job_uid=job_uid,
        runtime_image_identity=contract.runtime_image_identity,
        pod_uid_by_slot=pod_uid_by_slot,
    )


def _attest_actual_job(
    contract: PTGWaveJobContract,
    desired_manifest: Mapping[str, Any],
    actual_job: Mapping[str, Any],
) -> tuple[str, str]:
    if actual_job.get("apiVersion") != "batch/v1" or actual_job.get("kind") != "Job":
        raise PTGWaveContractError("actual workload must be a batch/v1 Job")
    desired_metadata = _mapping(desired_manifest.get("metadata"), "desired metadata")
    actual_metadata = _mapping(actual_job.get("metadata"), "actual Job metadata")
    job_name = _require_text("actual Job name", actual_metadata.get("name"))
    job_uid = _require_text("actual Job UID", actual_metadata.get("uid"))
    if job_name != desired_metadata.get("name") or job_name != _job_name(contract.wave_digest):
        raise PTGWaveContractError("actual Job name does not bind the canonical wave")
    _require_metadata_subset(actual_metadata, desired_metadata, "actual Job")
    desired_spec = _mapping(desired_manifest.get("spec"), "desired Job spec")
    actual_spec = _mapping(actual_job.get("spec"), "actual Job spec")
    for name in ("completionMode", "completions", "parallelism", "backoffLimit"):
        if actual_spec.get(name) != desired_spec.get(name):
            raise PTGWaveContractError(f"actual Job {name} differs from the desired wave")
    _require_safe_job_options(actual_spec)
    _require_actual_template(contract, actual_spec, desired_spec)
    return job_name, job_uid


def _require_safe_job_options(actual_spec: Mapping[str, Any]) -> None:
    ttl_seconds = actual_spec.get("ttlSecondsAfterFinished")
    if ttl_seconds is not None and (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, int)
        or ttl_seconds < 86400
    ):
        raise PTGWaveContractError(
            "actual Job TTL must be omitted or at least 86400 seconds"
        )
    forbidden_options = {
        "activeDeadlineSeconds",
        "backoffLimitPerIndex",
        "maxFailedIndexes",
        "podFailurePolicy",
        "successPolicy",
    }
    if forbidden_options & set(actual_spec):
        raise PTGWaveContractError("actual Job has unsupported execution options")
    suspend = actual_spec.get("suspend")
    if suspend is not None and suspend is not False:
        raise PTGWaveContractError("actual Job must not be suspended")


def _require_actual_template(
    contract: PTGWaveJobContract,
    actual_spec: Mapping[str, Any],
    desired_spec: Mapping[str, Any],
) -> None:
    template = _mapping(actual_spec.get("template"), "actual Job template")
    desired_template = _mapping(desired_spec.get("template"), "desired Job template")
    template_metadata = _mapping(template.get("metadata"), "actual template metadata")
    _require_metadata_contract(template_metadata, contract, "actual Job template")
    if _worker_config_identity_from_template(template) != contract.config_identity:
        raise PTGWaveContractError("actual Job worker config differs from its identity")
    pod_spec = _mapping(template.get("spec"), "actual Job pod spec")
    desired_pod_spec = _mapping(desired_template.get("spec"), "desired Job pod spec")
    for name in ("restartPolicy", "automountServiceAccountToken"):
        if pod_spec.get(name) != desired_pod_spec.get(name):
            raise PTGWaveContractError(
                f"actual Job pod {name} differs from the desired wave"
            )
    container = _one_worker_container(pod_spec, "actual Job template")
    desired_container = _one_worker_container(
        desired_pod_spec,
        "desired Job template",
    )
    for name in ("image", "imagePullPolicy", "workingDir", "command", "env"):
        if container.get(name) != desired_container.get(name):
            raise PTGWaveContractError(
                f"actual Job container {name} differs from the desired wave"
            )


def _attest_actual_pod(
    contract: PTGWaveJobContract,
    actual_pod: Mapping[str, Any],
    *,
    job_name: str,
    job_uid: str,
) -> tuple[int, str, str]:
    slot, pod_uid, worker_status = _attest_pod_identity(
        contract,
        actual_pod,
        job_name=job_name,
        job_uid=job_uid,
    )
    if worker_status.get("ready") is not True:
        raise PTGWaveContractError("actual PTG wave worker container is not Ready")
    return slot, pod_uid, _normalize_actual_image_id(worker_status.get("imageID"))


def _attest_pod_identity(
    contract: PTGWaveJobContract,
    actual_pod: Mapping[str, Any],
    *,
    job_name: str,
    job_uid: str,
) -> tuple[int, str, Mapping[str, Any]]:
    if actual_pod.get("apiVersion") != "v1" or actual_pod.get("kind") != "Pod":
        raise PTGWaveContractError("actual wave member must be a v1 Pod")
    metadata = _mapping(actual_pod.get("metadata"), "actual Pod metadata")
    pod_uid = _require_text("actual Pod UID", metadata.get("uid"))
    _require_metadata_contract(metadata, contract, "actual Pod")
    slot = _pod_completion_index(metadata)
    _require_exact_job_owner(metadata, job_name=job_name, job_uid=job_uid)
    pod_spec = _mapping(actual_pod.get("spec"), "actual Pod spec")
    container = _one_worker_container(pod_spec, "actual Pod")
    if container.get("image") != contract.image:
        raise PTGWaveContractError("actual Pod desired image differs from the wave digest")
    return slot, pod_uid, _worker_status(actual_pod)


def _worker_status(actual_pod: Mapping[str, Any]) -> Mapping[str, Any]:
    status = _mapping(actual_pod.get("status"), "actual Pod status")
    container_statuses = status.get("containerStatuses")
    if not isinstance(container_statuses, list) or len(container_statuses) != 1:
        raise PTGWaveContractError("actual Pod must report one worker container status")
    worker_status = _mapping(container_statuses[0], "actual worker container status")
    if worker_status.get("name") != "ptg-wave-worker":
        raise PTGWaveContractError("actual Pod has an unexpected container status")
    return worker_status


def _one_worker_container(pod_spec: Mapping[str, Any], location: str) -> Mapping[str, Any]:
    containers = pod_spec.get("containers")
    if not isinstance(containers, list) or len(containers) != 1:
        raise PTGWaveContractError(f"{location} must contain one worker container")
    container = _mapping(containers[0], f"{location} worker container")
    if container.get("name") != "ptg-wave-worker":
        raise PTGWaveContractError(f"{location} has an unexpected worker container")
    return container


def _require_metadata_subset(
    actual_metadata: Mapping[str, Any],
    desired_metadata: Mapping[str, Any],
    location: str,
) -> None:
    for name in ("labels", "annotations"):
        desired_values = _mapping(desired_metadata.get(name), f"desired {name}")
        actual_values = _mapping(actual_metadata.get(name), f"{location} {name}")
        if any(actual_values.get(key) != value for key, value in desired_values.items()):
            raise PTGWaveContractError(f"{location} {name} do not bind the desired wave")


def _require_metadata_contract(
    metadata: Mapping[str, Any],
    contract: PTGWaveJobContract,
    location: str,
) -> None:
    expected_metadata_by_kind = {
        "labels": _wave_labels(contract.wave_digest),
        "annotations": _wave_annotations(
            wave_digest=contract.wave_digest,
            queue=contract.queue,
            manifest_digest=contract.manifest_digest,
            jobs_digest=contract.jobs_digest,
            job_count=contract.job_count,
            runtime_image_identity=contract.runtime_image_identity,
            config_identity=contract.config_identity,
            manifest_identity=contract.manifest_identity,
        ),
    }
    _require_metadata_subset(metadata, expected_metadata_by_kind, location)


def _pod_completion_index(metadata: Mapping[str, Any]) -> int:
    annotations = _mapping(metadata.get("annotations"), "actual Pod annotations")
    raw_index = _require_text(
        "batch.kubernetes.io/job-completion-index",
        annotations.get("batch.kubernetes.io/job-completion-index"),
    )
    if not raw_index.isdecimal() or raw_index != str(int(raw_index)):
        raise PTGWaveContractError("actual Pod completion index is not canonical")
    slot = int(raw_index)
    if slot not in range(PTG_WAVE_SLOT_COUNT):
        raise PTGWaveContractError("actual Pod completion index is outside zero through eleven")
    return slot


def _require_exact_job_owner(
    metadata: Mapping[str, Any],
    *,
    job_name: str,
    job_uid: str,
) -> None:
    expected_owner_by_field = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "name": job_name,
        "uid": job_uid,
        "controller": True,
        "blockOwnerDeletion": True,
    }
    if metadata.get("ownerReferences") != [expected_owner_by_field]:
        raise PTGWaveContractError("actual Pod does not have the exact wave Job owner")


def _normalize_actual_image_id(value: Any) -> str:
    image_id = _require_text("actual containerStatus.imageID", value)
    normalized_reference = image_id.split("://", 1)[-1]
    match = re.search(r"(?:^|@)(sha256:[0-9a-f]{64})$", normalized_reference)
    if match is None:
        raise PTGWaveContractError("actual containerStatus.imageID is not a sha256 image")
    return match.group(1)
