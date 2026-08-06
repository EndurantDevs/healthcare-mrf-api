"""Pure claimed-wave terminal eligibility validation."""

from __future__ import annotations

from typing import Any, Iterable

from process.ptg_wave_outcome_contract import (
    PTGWaveOutcomeConflict,
    _collection_digest,
    _digest,
    _mapping_records,
    _outcome_record,
    _rejected_ordinals_summary,
    _rows_by_ordinal,
    _validate_claim_outcomes,
    _validate_linkage_ack,
)
from process.ptg_wave_state import canonical_json, sha256_digest


def _ready_slots_by_number(wave: Any) -> tuple[list[dict[str, Any]], dict[int, Any]]:
    ready_slots = (wave.kubernetes_ready_attestation or {}).get("slots")
    if not isinstance(ready_slots, list) or len(ready_slots) != 12:
        raise PTGWaveOutcomeConflict(
            "terminal proof lacks the initial exact 12-slot receipt"
        )
    ready_by_slot = {
        entry.get("slot"): entry
        for entry in ready_slots
        if isinstance(entry, dict)
    }
    if set(ready_by_slot) != set(range(12)):
        raise PTGWaveOutcomeConflict(
            "terminal proof lacks the initial exact 12-slot receipt"
        )
    return ready_slots, ready_by_slot


def _is_claim_bound_to_execution(
    wave: Any,
    intent: Any,
    claim: Any,
    ready_by_slot: dict[int, Any],
) -> bool:
    return (
        claim.wave_id == wave.wave_id
        and claim.ordinal == intent.ordinal
        and claim.run_id == intent.run_id
        and claim.job_id == intent.job_id
        and claim.kubernetes_job_uid == wave.kubernetes_job_uid
        and claim.manifest_identity == wave.kubernetes_manifest_identity
        and claim.pinned_image_reference == wave.pinned_image_reference
        and claim.pinned_image_digest == wave.pinned_image_digest
        and claim.runtime_image_identity == wave.runtime_image_identity
        and claim.config_identity == wave.kubernetes_config_identity
        and claim.slot in ready_by_slot
        and ready_by_slot[claim.slot].get("pod_uid") == claim.pod_uid
        and ready_by_slot[claim.slot].get("runtime_image_identity")
        == claim.runtime_image_identity
    )


def _validate_rows(
    wave: Any,
    intents: list[Any],
    claims: list[Any],
    outcomes: list[Any],
    ready_by_slot: dict[int, Any],
) -> list[int]:
    if not all(len(rows) == wave.intent_count for rows in (intents, claims, outcomes)):
        raise PTGWaveOutcomeConflict(
            "terminal proof must cover every admitted intent exactly once"
        )
    for intent, claim, outcome in zip(intents, claims, outcomes):
        if not _is_claim_bound_to_execution(
            wave, intent, claim, ready_by_slot
        ):
            raise PTGWaveOutcomeConflict(
                "worker claim does not bind the persisted exact execution"
            )
        if _mapping_records([outcome])[0] != _outcome_record(intent, outcome):
            raise PTGWaveOutcomeConflict(
                "stable terminal outcome differs from its admitted intent"
            )
    return _validate_claim_outcomes(claims, outcomes)


def _validate_outcome_and_linkage_digests(
    wave: Any,
    outcomes: list[Any],
    *,
    key: str | bytes | None,
) -> None:
    records = [_mapping_records([outcome])[0] for outcome in outcomes]
    digest = _collection_digest("healthporta.ptg-wave.outcomes.v1", records)
    if digest != wave.outcomes_digest:
        raise PTGWaveOutcomeConflict(
            "persisted exact outcomes digest is corrupt"
        )
    if wave.linkage_ack is None or wave.linkage_ack_digest is None:
        raise PTGWaveOutcomeConflict(
            "terminal proof requires the persisted linkage acknowledgement"
        )
    _, ack_digest = _validate_linkage_ack(
        wave, outcomes, wave.linkage_ack, key
    )
    if ack_digest != wave.linkage_ack_digest:
        raise PTGWaveOutcomeConflict(
            "persisted linkage acknowledgement digest is corrupt"
        )


def _expected_kubernetes_receipt(
    wave: Any, ready_slots: list[dict[str, Any]]
) -> dict[str, Any]:
    manifest_metadata = (
        wave.kubernetes_manifest.get("metadata")
        if isinstance(wave.kubernetes_manifest, dict)
        else None
    )
    return {
        "schema_version": 1,
        "wave_digest": wave.wave_digest,
        "queue": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "config_identity": wave.kubernetes_config_identity,
        "image_identity": wave.pinned_image_reference,
        "runtime_image_identity": wave.runtime_image_identity,
        "job_name": (
            manifest_metadata.get("name")
            if isinstance(manifest_metadata, dict)
            else None
        ),
        "completed_slots": list(range(12)),
        "slots": [
            {
                "slot": entry["slot"],
                "pod_uid": entry["pod_uid"],
                "phase": "Succeeded",
            }
            for entry in ready_slots
        ],
    }


def _validate_attestation_digest(receipt: dict[str, Any], message: str) -> None:
    unsigned_attestation_map = {
        name: field_value
        for name, field_value in receipt.items()
        if name != "attestation_digest"
    }
    if receipt["attestation_digest"] != sha256_digest(
        canonical_json(unsigned_attestation_map)
    ):
        raise PTGWaveOutcomeConflict(message)


def _validate_kubernetes_receipt(
    wave: Any,
    receipt: object,
    ready_slots: list[dict[str, Any]],
) -> dict[str, Any]:
    fields = {
        "schema_version", "wave_digest", "queue", "manifest_digest",
        "jobs_digest", "job_count", "config_identity", "manifest_identity",
        "image_identity", "runtime_image_identity", "job_name", "job_uid",
        "completed_slots", "slots", "attestation_digest",
    }
    if not isinstance(receipt, dict) or set(receipt) != fields:
        raise PTGWaveOutcomeConflict(
            "Kubernetes terminal receipt fields are not exact"
        )
    expected = _expected_kubernetes_receipt(wave, ready_slots)
    if any(receipt[name] != value for name, value in expected.items()):
        raise PTGWaveOutcomeConflict(
            "Kubernetes terminal receipt differs from initial execution identity"
        )
    _validate_attestation_digest(
        receipt, "Kubernetes terminal attestation digest is invalid"
    )
    return receipt


def _is_redis_receipt_idle(wave: Any, receipt: dict[str, Any]) -> bool:
    release = wave.redis_release_attestation or {}
    zero_fields = (
        "queue_entry_count", "job_payload_count", "retry_count",
        "in_progress_count",
    )
    return (
        receipt["schema_version"] == 1
        and receipt["wave_id"] == wave.wave_digest
        and receipt["queue_name"] == wave.release_queue
        and receipt["manifest_digest"] == wave.manifest_digest
        and receipt["jobs_digest"] == wave.jobs_digest
        and receipt["job_count"] == wave.intent_count
        and receipt["image_identity"] == wave.pinned_image_reference
        and receipt["release_digest"] == release.get("release_digest")
        and receipt["target_key_count"] == 4 + (4 * wave.intent_count)
        and all(receipt[field] == 0 for field in zero_fields)
        and isinstance(receipt["result_count"], int)
        and not isinstance(receipt["result_count"], bool)
        and 0 <= receipt["result_count"] <= wave.intent_count
        and receipt["health_check_count"] in {0, 1}
    )


def _validate_redis_receipt(wave: Any, receipt: object) -> dict[str, Any]:
    fields = {
        "schema_version", "wave_id", "queue_name", "manifest_digest",
        "jobs_digest", "job_count", "image_identity", "release_digest",
        "target_key_count", "queue_entry_count", "job_payload_count",
        "result_count", "retry_count", "in_progress_count",
        "health_check_count", "result_presence_digest", "attestation_digest",
    }
    if not isinstance(receipt, dict) or set(receipt) != fields:
        raise PTGWaveOutcomeConflict("Redis terminal receipt fields are not exact")
    if not _is_redis_receipt_idle(wave, receipt):
        raise PTGWaveOutcomeConflict(
            "Redis terminal receipt does not prove exact-wave idleness"
        )
    _digest(receipt["result_presence_digest"], "Redis result-presence digest")
    _validate_attestation_digest(
        receipt, "Redis terminal attestation digest is invalid"
    )
    return receipt


def verify_terminal_eligibility(
    wave: Any,
    intents: Iterable[Any],
    claims: Iterable[Any],
    outcomes: Iterable[Any],
    terminal_receipt: object,
    *,
    key: str | bytes | None = None,
) -> dict[str, Any]:
    """Reduce one locked all-N snapshot to claimed terminal evidence."""

    ordered_intents = _rows_by_ordinal(intents)
    ordered_claims = _rows_by_ordinal(claims)
    ordered_outcomes = _rows_by_ordinal(outcomes)
    ready_slots, ready_by_slot = _ready_slots_by_number(wave)
    rejected = _validate_rows(
        wave, ordered_intents, ordered_claims, ordered_outcomes, ready_by_slot
    )
    _validate_outcome_and_linkage_digests(wave, ordered_outcomes, key=key)
    if not isinstance(terminal_receipt, dict) or set(terminal_receipt) != {
        "kubernetes", "redis"
    }:
        raise PTGWaveOutcomeConflict("terminal receipt fields are not exact")
    kubernetes = _validate_kubernetes_receipt(
        wave, terminal_receipt["kubernetes"], ready_slots
    )
    redis = _validate_redis_receipt(wave, terminal_receipt["redis"])
    rejected, rejected_digest = _rejected_ordinals_summary(wave, rejected)
    return {
        "schema_version": "healthporta.ptg-wave.terminal.v1",
        "mode": "claimed",
        "wave_digest": wave.wave_digest,
        "outcomes_digest": wave.outcomes_digest,
        "linkage_ack_digest": wave.linkage_ack_digest,
        "rejected_ordinals": rejected,
        "rejected_ordinals_digest": rejected_digest,
        "kubernetes": kubernetes,
        "redis_pre_cleanup": redis,
    }
