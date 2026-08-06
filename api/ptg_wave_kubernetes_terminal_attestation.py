# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure terminal membership attestation for one persisted PTG wave."""

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
    _require_text,
    validate_ptg_wave_job_manifest,
)
from api.ptg_wave_kubernetes_attestation import (
    PTGWaveKubernetesAttestation,
    _attest_actual_job,
    _attest_pod_identity,
    _normalize_actual_image_id,
)


@dataclass(frozen=True)
class PTGWaveTerminalAttestation:
    wave_digest: str
    queue: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    config_identity: str
    manifest_identity: str
    image_identity: str
    job_name: str
    job_uid: str
    runtime_image_identity: str
    completed_slots: tuple[int, ...]
    pod_uid_by_slot: dict[int, str]
    attestation_digest: str

    def evidence_mapping(self) -> dict[str, Any]:
        """Return the unsigned canonical terminal evidence mapping."""

        return {
            "schema_version": 1,
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
            "completed_slots": list(self.completed_slots),
            "slots": [
                {"slot": slot, "pod_uid": self.pod_uid_by_slot[slot], "phase": "Succeeded"}
                for slot in sorted(self.pod_uid_by_slot)
            ],
        }

    def as_mapping(self) -> dict[str, Any]:
        """Return canonical terminal evidence together with its digest."""

        return {**self.evidence_mapping(), "attestation_digest": self.attestation_digest}


def attest_terminal_ptg_wave_kubernetes_objects(
    manifest: Mapping[str, Any],
    initial_attestation: PTGWaveKubernetesAttestation,
    actual_job: Mapping[str, Any],
    actual_pods: Sequence[Mapping[str, Any]],
) -> PTGWaveTerminalAttestation:
    """Attest exact terminal membership against the persisted initial wave."""

    contract = validate_ptg_wave_job_manifest(manifest)
    _require_initial_attestation(contract, initial_attestation)
    job_name, job_uid = _attest_actual_job(contract, manifest, actual_job)
    if (
        job_name != initial_attestation.job_name
        or job_uid != initial_attestation.job_uid
    ):
        raise PTGWaveContractError(
            "terminal Job identity differs from the initial wave"
        )
    completed_slots = _attest_terminal_job_status(actual_job)
    pod_uid_by_slot, actual_runtime_image_ids = _attest_terminal_pods(
        contract,
        actual_pods,
        job_name=job_name,
        job_uid=job_uid,
    )
    if pod_uid_by_slot != initial_attestation.pod_uid_by_slot:
        raise PTGWaveContractError(
            "terminal slot-to-pod UID membership differs from the initial wave"
        )
    if actual_runtime_image_ids != {initial_attestation.runtime_image_identity}:
        raise PTGWaveContractError(
            "terminal Pod image identity differs from the initial wave"
        )
    return _terminal_attestation(
        contract,
        initial_attestation,
        completed_slots,
        pod_uid_by_slot,
    )


def _require_initial_attestation(
    contract: PTGWaveJobContract,
    initial_attestation: PTGWaveKubernetesAttestation,
) -> None:
    expected_values_by_name = {
        "wave_digest": contract.wave_digest,
        "queue": contract.queue,
        "manifest_digest": contract.manifest_digest,
        "jobs_digest": contract.jobs_digest,
        "job_count": contract.job_count,
        "config_identity": contract.config_identity,
        "manifest_identity": contract.manifest_identity,
        "runtime_image_identity": contract.runtime_image_identity,
    }
    for name, expected_value in expected_values_by_name.items():
        if getattr(initial_attestation, name, None) != expected_value:
            raise PTGWaveContractError(
                f"initial attestation {name} differs from the manifest"
            )
    _require_text("initial Job name", initial_attestation.job_name)
    _require_text("initial Job UID", initial_attestation.job_uid)
    pod_uid_by_slot = initial_attestation.pod_uid_by_slot
    if not isinstance(pod_uid_by_slot, Mapping) or set(pod_uid_by_slot) != set(
        range(PTG_WAVE_SLOT_COUNT)
    ):
        raise PTGWaveContractError(
            "initial attestation must contain exactly twelve indexed Pods"
        )
    pod_uids = [
        _require_text("initial Pod UID", pod_uid)
        for pod_uid in pod_uid_by_slot.values()
    ]
    if len(set(pod_uids)) != PTG_WAVE_SLOT_COUNT:
        raise PTGWaveContractError("initial attestation Pod UIDs must be unique")


def _attest_terminal_job_status(
    actual_job: Mapping[str, Any],
) -> tuple[int, ...]:
    status_by_field = _mapping(actual_job.get("status"), "terminal Job status")
    expected_counts_by_name = {
        "active": 0,
        "failed": 0,
        "succeeded": PTG_WAVE_SLOT_COUNT,
    }
    for name, expected_count in expected_counts_by_name.items():
        if _job_status_count(status_by_field, name) != expected_count:
            raise PTGWaveContractError(
                f"terminal Job {name} does not equal {expected_count}"
            )
    completed_slots = _completed_slots(status_by_field.get("completedIndexes"))
    if completed_slots != tuple(range(PTG_WAVE_SLOT_COUNT)):
        raise PTGWaveContractError(
            "terminal Job completed indexes must cover zero through eleven"
        )
    return completed_slots


def _job_status_count(status_by_field: Mapping[str, Any], name: str) -> int:
    count = status_by_field.get(name, 0)
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise PTGWaveContractError(f"terminal Job {name} count is invalid")
    return count


def _completed_slots(completed_indexes: Any) -> tuple[int, ...]:
    index_text = _require_text("terminal Job completedIndexes", completed_indexes)
    completed_slots: list[int] = []
    for component in index_text.split(","):
        bounds = component.split("-")
        if len(bounds) == 1:
            completed_slots.append(_canonical_slot(bounds[0]))
        elif len(bounds) == 2:
            first_slot = _canonical_slot(bounds[0])
            last_slot = _canonical_slot(bounds[1])
            if first_slot >= last_slot:
                raise PTGWaveContractError(
                    "terminal Job completedIndexes range is invalid"
                )
            completed_slots.extend(range(first_slot, last_slot + 1))
        else:
            raise PTGWaveContractError(
                "terminal Job completedIndexes shape is invalid"
            )
    if completed_slots != sorted(set(completed_slots)):
        raise PTGWaveContractError(
            "terminal Job completedIndexes must be ordered and unique"
        )
    return tuple(completed_slots)


def _canonical_slot(slot_text: str) -> int:
    if not slot_text.isdecimal() or slot_text != str(int(slot_text)):
        raise PTGWaveContractError(
            "terminal Job completedIndexes contains a non-canonical index"
        )
    slot = int(slot_text)
    if slot not in range(PTG_WAVE_SLOT_COUNT):
        raise PTGWaveContractError(
            "terminal Job completedIndexes contains an out-of-range index"
        )
    return slot


def _attest_terminal_pods(
    contract: PTGWaveJobContract,
    actual_pods: Sequence[Mapping[str, Any]],
    *,
    job_name: str,
    job_uid: str,
) -> tuple[dict[int, str], set[str]]:
    if len(actual_pods) != PTG_WAVE_SLOT_COUNT:
        raise PTGWaveContractError(
            "Kubernetes must report exactly twelve terminal wave Pods"
        )
    pod_uid_by_slot: dict[int, str] = {}
    actual_runtime_image_ids: set[str] = set()
    for actual_pod in actual_pods:
        pod_status = _mapping(actual_pod.get("status"), "terminal Pod status")
        if pod_status.get("phase") != "Succeeded":
            raise PTGWaveContractError("every terminal wave Pod must be Succeeded")
        slot, pod_uid, worker_status = _attest_pod_identity(
            contract,
            actual_pod,
            job_name=job_name,
            job_uid=job_uid,
        )
        if slot in pod_uid_by_slot or pod_uid in pod_uid_by_slot.values():
            raise PTGWaveContractError(
                "terminal wave Pods must have unique indexes and UIDs"
            )
        pod_uid_by_slot[slot] = pod_uid
        actual_runtime_image_ids.add(
            _normalize_actual_image_id(worker_status.get("imageID"))
        )
    if set(pod_uid_by_slot) != set(range(PTG_WAVE_SLOT_COUNT)):
        raise PTGWaveContractError(
            "terminal wave Pods must cover indexes zero through eleven"
        )
    if actual_runtime_image_ids != {contract.runtime_image_identity}:
        raise PTGWaveContractError(
            "terminal Pod imageIDs do not equal the runtime image identity"
        )
    return pod_uid_by_slot, actual_runtime_image_ids


def _terminal_attestation(
    contract: PTGWaveJobContract,
    initial_attestation: PTGWaveKubernetesAttestation,
    completed_slots: tuple[int, ...],
    pod_uid_by_slot: dict[int, str],
) -> PTGWaveTerminalAttestation:
    attestation_field_map = {
        "wave_digest": contract.wave_digest,
        "queue": contract.queue,
        "manifest_digest": contract.manifest_digest,
        "jobs_digest": contract.jobs_digest,
        "job_count": contract.job_count,
        "config_identity": contract.config_identity,
        "manifest_identity": contract.manifest_identity,
        "image_identity": contract.image,
        "job_name": initial_attestation.job_name,
        "job_uid": initial_attestation.job_uid,
        "runtime_image_identity": initial_attestation.runtime_image_identity,
        "completed_slots": completed_slots,
        "pod_uid_by_slot": pod_uid_by_slot,
    }
    unsigned_attestation_map = {
        "schema_version": 1,
        "wave_digest": attestation_field_map["wave_digest"],
        "queue": attestation_field_map["queue"],
        "manifest_digest": attestation_field_map["manifest_digest"],
        "jobs_digest": attestation_field_map["jobs_digest"],
        "job_count": attestation_field_map["job_count"],
        "config_identity": attestation_field_map["config_identity"],
        "manifest_identity": attestation_field_map["manifest_identity"],
        "image_identity": attestation_field_map["image_identity"],
        "runtime_image_identity": attestation_field_map["runtime_image_identity"],
        "job_name": attestation_field_map["job_name"],
        "job_uid": attestation_field_map["job_uid"],
        "completed_slots": list(attestation_field_map["completed_slots"]),
        "slots": [
            {
                "slot": slot,
                "pod_uid": attestation_field_map["pod_uid_by_slot"][slot],
                "phase": "Succeeded",
            }
            for slot in sorted(attestation_field_map["pod_uid_by_slot"])
        ],
    }
    digest = hashlib.sha256(json.dumps(
        unsigned_attestation_map,
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")).hexdigest()
    return PTGWaveTerminalAttestation(
        **attestation_field_map,
        attestation_digest=digest,
    )
