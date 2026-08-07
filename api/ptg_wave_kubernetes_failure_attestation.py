# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure fail-closed Kubernetes evidence for an unclaimed PTG wave failure.

This verifier is intentionally narrower than normal terminal evidence.  It
only accepts the exceptional case in which the originally attested twelve
Indexed-Job Pods all reached a failed terminal state without any successful
worker.  A durable caller must separately prove that no worker claim exists;
this module supplies the immutable Kubernetes half of that proof and performs
no Kubernetes I/O.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTGWaveJobContract,
    PTG_WAVE_SLOT_COUNT,
    _mapping,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import (
    PTGWaveKubernetesAttestation,
    _attest_actual_job,
    _attest_pod_identity,
    _normalize_actual_image_id,
)
from api.ptg_wave_kubernetes_terminal_attestation import (
    _require_initial_attestation,
)


_SCHEMA_VERSION = "healthporta.ptg-wave.kubernetes-preclaim-failure.v1"


@dataclass(frozen=True)
class PTGWavePreclaimFailureAttestation:
    """Canonical, exact Kubernetes evidence for an all-unclaimed failure."""

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
    job_active: int
    job_failed: int
    job_succeeded: int
    failed_pod_uid_by_slot: dict[int, str]
    attestation_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return the unsigned canonical evidence that the digest covers."""

        return {
            "schema_version": _SCHEMA_VERSION,
            "wave_digest": self.wave_digest,
            "queue": self.queue,
            "manifest_digest": self.manifest_digest,
            "jobs_digest": self.jobs_digest,
            "job_count": self.job_count,
            "config_identity": self.config_identity,
            "manifest_identity": self.manifest_identity,
            "image_identity": self.image_identity,
            "runtime_image_identity": self.runtime_image_identity,
            "job_name": self.job_name,
            "job_uid": self.job_uid,
            "backoff_limit": 0,
            "job_active": self.job_active,
            "job_failed": self.job_failed,
            "job_succeeded": self.job_succeeded,
            "job_failure_condition": {"type": "Failed", "status": "True"},
            "failed_slots": [
                {
                    "slot": slot,
                    "pod_uid": self.failed_pod_uid_by_slot[slot],
                    "phase": "Failed",
                    "runtime_image_identity": self.runtime_image_identity,
                }
                for slot in sorted(self.failed_pod_uid_by_slot)
            ],
        }

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical evidence together with its SHA-256 digest."""

        return {**self.evidence_mapping(), "attestation_digest": self.attestation_digest}


def attest_preclaim_failure_kubernetes(
    manifest: Mapping[str, Any],
    initial_attestation: PTGWaveKubernetesAttestation,
    actual_job: Mapping[str, Any],
    actual_pods: Sequence[Mapping[str, Any]],
) -> PTGWavePreclaimFailureAttestation:
    """Attest an exact twelve-Pod, no-success, pre-claim Job failure.

    Any incomplete, replaced, retried, active, mixed-success, or malformed
    observation is rejected.  In particular, this cannot be used to turn a
    partly successful wave into an all-unclaimed failure record.
    """

    contract = validate_ptg_wave_job_manifest(manifest)
    _require_no_retry_manifest(manifest)
    _require_initial_attestation(contract, initial_attestation)
    job_name, job_uid = _attest_actual_job(contract, manifest, actual_job)
    if (
        job_name != initial_attestation.job_name
        or job_uid != initial_attestation.job_uid
    ):
        raise PTGWaveContractError(
            "failure Job identity differs from the initial wave"
        )
    job_active, job_failed, job_succeeded = _attest_failed_job_status(actual_job)
    failed_pod_uid_by_slot, _actual_runtime_image_ids = _attest_failed_pods(
        contract,
        actual_pods,
        job_name=job_name,
        job_uid=job_uid,
    )
    if failed_pod_uid_by_slot != initial_attestation.pod_uid_by_slot:
        raise PTGWaveContractError(
            "failure slot-to-Pod UID membership differs from the initial wave"
        )
    # The initial attestation was bound to the manifest runtime above, and
    # `_attest_failed_pods` already required the same runtime for every Pod.
    # A second comparison to the initial runtime was therefore unreachable.
    # Both independently observable bindings must succeed before returning.
    return _failure_attestation(
        contract,
        job_name=job_name,
        job_uid=job_uid,
        job_active=job_active,
        job_failed=job_failed,
        job_succeeded=job_succeeded,
        failed_pod_uid_by_slot=failed_pod_uid_by_slot,
    )


def _failure_attestation(
    contract: PTGWaveJobContract,
    *,
    job_name: str,
    job_uid: str,
    job_active: int,
    job_failed: int,
    job_succeeded: int,
    failed_pod_uid_by_slot: dict[int, str],
) -> PTGWavePreclaimFailureAttestation:
    attestation_field_map = {
        "wave_digest": contract.wave_digest,
        "queue": contract.queue,
        "manifest_digest": contract.manifest_digest,
        "jobs_digest": contract.jobs_digest,
        "job_count": contract.job_count,
        "config_identity": contract.config_identity,
        "manifest_identity": contract.manifest_identity,
        "image_identity": contract.image,
        "runtime_image_identity": contract.runtime_image_identity,
        "job_name": job_name,
        "job_uid": job_uid,
        "job_active": job_active,
        "job_failed": job_failed,
        "job_succeeded": job_succeeded,
        "failed_pod_uid_by_slot": failed_pod_uid_by_slot,
    }
    unsigned_evidence_map = PTGWavePreclaimFailureAttestation(
        **attestation_field_map,
        attestation_digest="",
    ).evidence_mapping()
    return PTGWavePreclaimFailureAttestation(
        **attestation_field_map,
        attestation_digest=_canonical_digest(unsigned_evidence_map),
    )


attest_preclaim_failure_ptg_wave_kubernetes_objects = (
    attest_preclaim_failure_kubernetes
)


def _require_no_retry_manifest(manifest: Mapping[str, Any]) -> None:
    spec = _mapping(manifest.get("spec"), "wave Job manifest spec")
    if spec.get("backoffLimit") != 0:
        raise PTGWaveContractError(
            "pre-claim failure attestation requires backoffLimit zero"
        )


def _attest_failed_job_status(
    actual_job: Mapping[str, Any],
) -> tuple[int, int, int]:
    status = _mapping(actual_job.get("status"), "failed Job status")
    active = _job_status_count(status, "active", default=0)
    failed = _job_status_count(status, "failed")
    succeeded = _job_status_count(status, "succeeded", default=0)
    if active != 0:
        raise PTGWaveContractError("failed Job active must equal zero")
    if failed != PTG_WAVE_SLOT_COUNT:
        raise PTGWaveContractError("failed Job must report exactly twelve failed slots")
    if succeeded != 0:
        raise PTGWaveContractError("failed Job must not report success")
    if status.get("completedIndexes") is not None:
        raise PTGWaveContractError(
            "failed Job must not report completed indexes"
        )
    _require_failed_condition(status)
    return active, failed, succeeded


def _job_status_count(
    status: Mapping[str, Any],
    name: str,
    *,
    default: int | None = None,
) -> int:
    if name not in status:
        if default is None:
            raise PTGWaveContractError(f"failed Job {name} count is missing")
        return default
    value = status[name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PTGWaveContractError(f"failed Job {name} count is invalid")
    return value


def _require_failed_condition(status: Mapping[str, Any]) -> None:
    conditions = status.get("conditions")
    if not isinstance(conditions, list) or not conditions:
        raise PTGWaveContractError("failed Job conditions are missing")
    condition_status_by_type: dict[str, str] = {}
    for item in conditions:
        condition = _mapping(item, "failed Job condition")
        condition_type = condition.get("type")
        condition_status = condition.get("status")
        if not isinstance(condition_type, str) or not condition_type:
            raise PTGWaveContractError("failed Job condition type is invalid")
        if condition_status not in {"True", "False", "Unknown"}:
            raise PTGWaveContractError("failed Job condition status is invalid")
        if condition_type in condition_status_by_type:
            raise PTGWaveContractError("failed Job conditions repeat a type")
        condition_status_by_type[condition_type] = condition_status
    if condition_status_by_type.get("Failed") != "True":
        raise PTGWaveContractError("failed Job must have a true Failed condition")
    if condition_status_by_type.get("Complete") == "True":
        raise PTGWaveContractError("failed Job must not have a true Complete condition")


def _attest_failed_pods(
    contract: PTGWaveJobContract,
    actual_pods: Sequence[Mapping[str, Any]],
    *,
    job_name: str,
    job_uid: str,
) -> tuple[dict[int, str], set[str]]:
    if len(actual_pods) != PTG_WAVE_SLOT_COUNT:
        raise PTGWaveContractError(
            "Kubernetes must report exactly twelve failed wave Pods"
        )
    failed_pod_uid_by_slot: dict[int, str] = {}
    actual_runtime_image_ids: set[str] = set()
    for actual_pod in actual_pods:
        status = _mapping(actual_pod.get("status"), "failed Pod status")
        if status.get("phase") != "Failed":
            raise PTGWaveContractError("every pre-claim failure Pod must be Failed")
        slot, pod_uid, worker_status = _attest_pod_identity(
            contract,
            actual_pod,
            job_name=job_name,
            job_uid=job_uid,
        )
        _require_failed_worker_status(worker_status)
        if slot in failed_pod_uid_by_slot or pod_uid in failed_pod_uid_by_slot.values():
            raise PTGWaveContractError(
                "failed wave Pods must have unique indexes and UIDs"
            )
        failed_pod_uid_by_slot[slot] = pod_uid
        actual_runtime_image_ids.add(
            _normalize_actual_image_id(worker_status.get("imageID"))
        )
    if set(failed_pod_uid_by_slot) != set(range(PTG_WAVE_SLOT_COUNT)):
        raise PTGWaveContractError(
            "failed wave Pods must cover indexes zero through eleven"
        )
    if actual_runtime_image_ids != {contract.runtime_image_identity}:
        raise PTGWaveContractError(
            "failed Pod imageIDs do not equal the runtime image identity"
        )
    return failed_pod_uid_by_slot, actual_runtime_image_ids


def _require_failed_worker_status(worker_status: Mapping[str, Any]) -> None:
    if worker_status.get("ready") is not False:
        raise PTGWaveContractError("failed worker container must not be Ready")
    restart_count = worker_status.get("restartCount")
    if isinstance(restart_count, bool) or restart_count != 0:
        raise PTGWaveContractError("failed worker container restartCount must equal zero")
    state = _mapping(worker_status.get("state"), "failed worker container state")
    if set(state) != {"terminated"}:
        raise PTGWaveContractError("failed worker container must be terminated")
    terminated = _mapping(state["terminated"], "failed worker termination")
    exit_code = terminated.get("exitCode")
    if (
        isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or exit_code == 0
    ):
        raise PTGWaveContractError(
            "failed worker termination exitCode must be a non-zero integer"
        )


def _canonical_digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


__all__ = [
    "PTGWavePreclaimFailureAttestation",
    "attest_preclaim_failure_ptg_wave_kubernetes_objects",
]
