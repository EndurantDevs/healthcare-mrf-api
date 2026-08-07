# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure callback-receipt attestation for an exact PTG wave."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from api.ptg_wave_kubernetes import (
    PTGWaveContractError,
    PTGWaveJobContract,
    PTG_WAVE_SLOT_COUNT,
    PTG_WAVE_WORKER_CLASS,
    _require_text,
    validate_ptg_wave_job_manifest,
)


@dataclass(frozen=True)
class PTGWaveSlotAttestation:
    wave_digest: str
    queue: str
    manifest_digest: str
    jobs_digest: str
    job_count: int
    config_identity: str
    manifest_identity: str
    pod_uid_by_slot: dict[int, str]


def attest_ptg_wave_slot_receipts(
    manifest: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
) -> PTGWaveSlotAttestation:
    """Attest callback identities only; Kubernetes proves actual images."""

    contract = validate_ptg_wave_job_manifest(manifest)
    if len(receipts) != PTG_WAVE_SLOT_COUNT:
        raise PTGWaveContractError(
            "expected exactly twelve PTG wave slot receipts"
        )
    pod_uid_by_slot: dict[int, str] = {}
    for receipt in receipts:
        slot = receipt.get("slot_index")
        if (
            isinstance(slot, bool)
            or not isinstance(slot, int)
            or slot not in range(PTG_WAVE_SLOT_COUNT)
        ):
            raise PTGWaveContractError(
                "receipt has an invalid Indexed Job slot"
            )
        pod_uid = _require_text("receipt pod_uid", receipt.get("pod_uid"))
        if slot in pod_uid_by_slot:
            raise PTGWaveContractError(
                "callback receipts repeat an Indexed Job slot"
            )
        if pod_uid in pod_uid_by_slot.values():
            raise PTGWaveContractError(
                "each wave slot requires one unique pod UID"
            )
        _require_receipt_contract(receipt, contract)
        pod_uid_by_slot[slot] = pod_uid
    if set(pod_uid_by_slot) != set(range(PTG_WAVE_SLOT_COUNT)):
        raise PTGWaveContractError(
            "receipts must cover slots zero through eleven exactly once"
        )
    return PTGWaveSlotAttestation(
        wave_digest=contract.wave_digest,
        queue=contract.queue,
        manifest_digest=contract.manifest_digest,
        jobs_digest=contract.jobs_digest,
        job_count=contract.job_count,
        config_identity=contract.config_identity,
        manifest_identity=contract.manifest_identity,
        pod_uid_by_slot=pod_uid_by_slot,
    )


def _require_receipt_contract(
    receipt: Mapping[str, Any],
    contract: PTGWaveJobContract,
) -> None:
    expected_values_by_name = {
        "wave_digest": contract.wave_digest,
        "queue": contract.queue,
        "worker_class": PTG_WAVE_WORKER_CLASS,
        "manifest_digest": contract.manifest_digest,
        "jobs_digest": contract.jobs_digest,
        "job_count": contract.job_count,
        "config_identity": contract.config_identity,
        "manifest_identity": contract.manifest_identity,
        "image_identity": contract.image,
        "runtime_image_identity": contract.runtime_image_identity,
    }
    for name, expected_value in expected_values_by_name.items():
        if receipt.get(name) != expected_value:
            raise PTGWaveContractError(
                f"receipt {name} does not match the wave contract"
            )
