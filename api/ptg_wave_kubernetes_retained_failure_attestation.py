"""Pure evidence for a terminal failed Job after Kubernetes has GC'd Pods."""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTGWaveJobContract,
    PTG_WAVE_SLOT_COUNT,
    _mapping,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import (
    _attest_actual_job,
    _attest_pod_identity,
    _normalize_actual_image_id,
)
from api.ptg_wave_kubernetes_failure_attestation import (
    _attest_failed_job_status,
)
from process.ptg_wave_state import canonical_json, sha256_digest


RETAINED_FAILURE_SCHEMA = (
    "healthporta.ptg-wave.kubernetes-retained-preclaim-failure.v1"
)
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")
_CONDITION_FIELDS = frozenset(
    {
        "type",
        "status",
        "reason",
        "message",
        "lastProbeTime",
        "lastTransitionTime",
    }
)
_WORKER_STATUS_FIELDS = frozenset(
    {
        "allocatedResources",
        "containerID",
        "image",
        "imageID",
        "lastState",
        "name",
        "ready",
        "resources",
        "restartCount",
        "started",
        "state",
        "user",
        "volumeMounts",
    }
)
_TERMINATION_FIELDS = frozenset(
    {"containerID", "exitCode", "finishedAt", "reason", "startedAt"}
)


@dataclass(frozen=True)
class PTGWaveRetainedFailureAttestation:
    """A raw terminal Job state and its nonempty retained failed-Pod subset."""

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
    job_active: None
    job_failed: int
    job_succeeded: None
    job_ready: int
    job_terminating: int
    completed_indexes: None
    failed_indexes: None
    completion_time: None
    start_time: str
    job_conditions: tuple[dict[str, str], ...]
    retained_failures_by_slot: dict[int, dict[str, Any]]
    attestation_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return the exact unsigned evidence covered by the digest."""

        return {
            "schema_version": RETAINED_FAILURE_SCHEMA,
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
            "job_ready": self.job_ready,
            "job_terminating": self.job_terminating,
            "completed_indexes": self.completed_indexes,
            "failed_indexes": self.failed_indexes,
            "completion_time": self.completion_time,
            "start_time": self.start_time,
            "uncounted_terminated_pods": {},
            "job_conditions": list(self.job_conditions),
            "retained_failed_slots": [
                {
                    "slot": slot,
                    "pod_uid": failure["pod_uid"],
                    "phase": "Failed",
                    "runtime_image_identity": self.runtime_image_identity,
                    "termination": failure["termination"],
                }
                for slot, failure in sorted(self.retained_failures_by_slot.items())
            ],
        }

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical evidence and its separately derived digest."""

        return {**self.evidence_mapping(), "attestation_digest": self.attestation_digest}


def attest_retained_preclaim_failure_kubernetes_objects(
    manifest: Mapping[str, Any],
    actual_job: Mapping[str, Any],
    actual_pods: Sequence[Mapping[str, Any]],
) -> PTGWaveRetainedFailureAttestation:
    """Attest the current raw terminal Job and every retained failed Pod."""

    contract = validate_ptg_wave_job_manifest(manifest)
    job_name, job_uid = _attest_actual_job(contract, manifest, actual_job)
    # Reuse the existing pure failure validator for the all-twelve Job fact.
    _attest_failed_job_status(actual_job)
    status = _strict_terminal_status(actual_job)
    retained_failures_by_slot = _attest_retained_failed_pods(
        contract,
        actual_pods,
        job_name=job_name,
        job_uid=job_uid,
    )
    attestation_fields_by_name = {
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
        "job_active": None,
        "job_failed": status["failed"],
        "job_succeeded": None,
        "job_ready": status["ready"],
        "job_terminating": status["terminating"],
        "completed_indexes": None,
        "failed_indexes": None,
        "completion_time": None,
        "start_time": status["start_time"],
        "job_conditions": status["conditions"],
        "retained_failures_by_slot": retained_failures_by_slot,
    }
    unsigned = PTGWaveRetainedFailureAttestation(
        **attestation_fields_by_name,
        attestation_digest="",
    ).evidence_mapping()
    return PTGWaveRetainedFailureAttestation(
        **attestation_fields_by_name,
        attestation_digest=sha256_digest(canonical_json(unsigned)),
    )


def _strict_terminal_status(actual_job: Mapping[str, Any]) -> dict[str, Any]:
    status = _mapping(actual_job.get("status"), "failed Job status")
    expected_fields = {
        "conditions",
        "failed",
        "ready",
        "startTime",
        "terminating",
        "uncountedTerminatedPods",
    }
    if set(status) != expected_fields:
        raise PTGWaveContractError("failed Job status fields are not the retained V13 shape")
    if (
        type(status["failed"]) is not int
        or status["failed"] != PTG_WAVE_SLOT_COUNT
        or type(status["ready"]) is not int
        or status["ready"] != 0
        or type(status["terminating"]) is not int
        or status["terminating"] != 0
        or status["uncountedTerminatedPods"] != {}
    ):
        raise PTGWaveContractError("failed Job status values are not the retained V13 shape")
    return {
        "failed": status["failed"],
        "ready": status["ready"],
        "terminating": status["terminating"],
        "start_time": _time(status["startTime"], "failed Job startTime"),
        "conditions": _conditions(status["conditions"]),
    }


def _conditions(job_conditions: object) -> tuple[dict[str, str], ...]:
    if not isinstance(job_conditions, list) or len(job_conditions) != 2:
        raise PTGWaveContractError("failed Job conditions are not the retained V13 shape")
    expected_condition_types = {"FailureTarget", "Failed"}
    condition_records: list[dict[str, str]] = []
    for condition in job_conditions:
        if not isinstance(condition, Mapping) or set(condition) != _CONDITION_FIELDS:
            raise PTGWaveContractError("failed Job condition fields are not exact")
        condition_type = condition.get("type")
        if condition_type not in expected_condition_types:
            raise PTGWaveContractError("failed Job condition type is invalid")
        condition_by_field = {
            "type": condition_type,
            "status": condition.get("status"),
            "reason": condition.get("reason"),
            "message": condition.get("message"),
            "last_probe_time": _time(
                condition.get("lastProbeTime"),
                "failed Job condition lastProbeTime",
            ),
            "last_transition_time": _time(
                condition.get("lastTransitionTime"),
                "failed Job condition lastTransitionTime",
            ),
        }
        if (
            condition_by_field["status"] != "True"
            or condition_by_field["reason"] != "BackoffLimitExceeded"
            or condition_by_field["message"]
            != "Job has reached the specified backoff limit"
        ):
            raise PTGWaveContractError("failed Job condition values are not exact")
        condition_records.append(condition_by_field)
    if {condition["type"] for condition in condition_records} != expected_condition_types:
        raise PTGWaveContractError("failed Job conditions are incomplete or repeated")
    return tuple(sorted(condition_records, key=lambda condition: condition["type"]))


def _attest_retained_failed_pods(
    contract: PTGWaveJobContract,
    actual_pods: Sequence[Mapping[str, Any]],
    *,
    job_name: str,
    job_uid: str,
) -> dict[int, dict[str, Any]]:
    if (
        not isinstance(actual_pods, Sequence)
        or isinstance(actual_pods, (str, bytes, bytearray))
        or not actual_pods
    ):
        raise PTGWaveContractError("Kubernetes must retain at least one failed wave Pod")
    retained_failure_by_slot: dict[int, dict[str, Any]] = {}
    seen_pod_uids: set[str] = set()
    for actual_pod in actual_pods:
        if not isinstance(actual_pod, Mapping):
            raise PTGWaveContractError("retained wave Pod is not an object")
        status = _mapping(actual_pod.get("status"), "failed Pod status")
        if status.get("phase") != "Failed":
            raise PTGWaveContractError("every retained wave Pod must be Failed")
        slot, pod_uid, worker_status = _attest_pod_identity(
            contract,
            actual_pod,
            job_name=job_name,
            job_uid=job_uid,
        )
        termination = _termination(worker_status, contract)
        if slot in retained_failure_by_slot or pod_uid in seen_pod_uids:
            raise PTGWaveContractError("retained wave Pods must have unique indexes and UIDs")
        seen_pod_uids.add(pod_uid)
        retained_failure_by_slot[slot] = {
            "pod_uid": pod_uid,
            "termination": termination,
        }
    return retained_failure_by_slot


def _termination(
    worker_status: Mapping[str, Any],
    contract: PTGWaveJobContract,
) -> dict[str, Any]:
    if set(worker_status) != _WORKER_STATUS_FIELDS:
        raise PTGWaveContractError("retained worker status fields are not exact")
    if (
        worker_status.get("name") != "ptg-wave-worker"
        or worker_status.get("ready") is not False
        or type(worker_status.get("restartCount")) is not int
        or worker_status.get("restartCount") != 0
        or worker_status.get("started") is not False
        or worker_status.get("lastState") != {}
        or worker_status.get("image") != contract.runtime_image_identity
        or _normalize_actual_image_id(worker_status.get("imageID"))
        != contract.image.rsplit("@", 1)[1]
    ):
        raise PTGWaveContractError("retained worker status values are not exact")
    state = _mapping(worker_status.get("state"), "retained worker state")
    if set(state) != {"terminated"}:
        raise PTGWaveContractError("retained worker must have only terminated state")
    terminated = _mapping(state["terminated"], "retained worker termination")
    if set(terminated) != _TERMINATION_FIELDS:
        raise PTGWaveContractError("retained worker termination fields are not exact")
    container_id = worker_status.get("containerID")
    if (
        not isinstance(container_id, str)
        or not container_id
        or terminated.get("containerID") != container_id
        or terminated.get("reason") != "Error"
        or type(terminated.get("exitCode")) is not int
        or terminated.get("exitCode") != 1
    ):
        raise PTGWaveContractError("retained worker termination values are not exact")
    return {
        "container_id": container_id,
        "reason": "Error",
        "exit_code": 1,
        "started_at": _time(terminated.get("startedAt"), "retained worker startedAt"),
        "finished_at": _time(terminated.get("finishedAt"), "retained worker finishedAt"),
    }


def _time(value: object, name: str) -> str:
    if not isinstance(value, str) or _TIME.fullmatch(value) is None:
        raise PTGWaveContractError(f"{name} is not a canonical UTC timestamp")
    try:
        dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise PTGWaveContractError(
            f"{name} is not a canonical UTC timestamp"
        ) from exc
    return value


__all__ = [
    "RETAINED_FAILURE_SCHEMA",
    "PTGWaveRetainedFailureAttestation",
    "attest_retained_preclaim_failure_kubernetes_objects",
]
