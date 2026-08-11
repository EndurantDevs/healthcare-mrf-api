"""Exact payloads bound into asymmetric PTG wave receipts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

from api.control_import_wave_attestation import (
    RECEIPT_ATTESTATION_VERSION,
)
from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
    require_receipt_key_id,
    require_receipt_public_material,
)
from process.ptg_wave_quarantine_basis import (
    V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
)


ABANDONMENT_REQUEST_SCHEMA = (
    "healthporta.ptg-wave.v12-pristine-materialized-abandonment-request.v1"
)
ABANDONMENT_PROOF_SCHEMA = (
    "healthporta.ptg-wave.v12-pristine-materialized-abandonment-proof.v1"
)
V12_QUARANTINE_REASON = V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS

_CUTOVER_PREFIX = b"ptg-ordinary-cutover-id-v1:"
_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")

LINKAGE_PAYLOAD_FIELDS = frozenset(
    {
        "operation_id",
        "cutover_id",
        "wave_id",
        "wave_digest",
        "request_digest",
        "cohort_attestation_digest",
        "cohort_signature_digest",
        "receipt_public_modulus_hex",
        "receipt_public_exponent",
        "authorization_digest",
        "snapshot_digest",
        "membership_digest",
        "inventory_digest",
        "subscription_coverage_digest",
        "entitlement_coverage_digest",
        "entitlement_coverage_count",
        "catalog_generation",
        "physical_coordinate_digest",
        "imported_coordinate_digest",
        "reused_coordinate_digest",
        "partition_digest",
        "physical_coordinate_count",
        "imported_coordinate_count",
        "reused_coordinate_count",
        "intent_count",
        "jobs_digest",
        "manifest_digest",
        "outcomes_digest",
        "mapping_digest",
        "linkage_ack_digest",
    }
)

ADMISSION_FIELDS = frozenset(
    {
        "attestation_schema",
        "receipt_key_id",
        "receipt_public_modulus_hex",
        "receipt_public_exponent",
        "wave_id",
        "wave_digest",
        "request_digest",
        "cohort_attestation_digest",
        "cohort_signature_digest",
        "authorization_digest",
        "snapshot_digest",
        "membership_digest",
        "inventory_digest",
        "subscription_coverage_digest",
        "entitlement_coverage_digest",
        "entitlement_coverage_count",
        "catalog_generation",
        "physical_coordinate_digest",
        "imported_coordinate_digest",
        "reused_coordinate_digest",
        "partition_digest",
        "physical_coordinate_count",
        "imported_coordinate_count",
        "reused_coordinate_count",
        "intent_count",
        "jobs_digest",
        "manifest_digest",
    }
)


class PTGWaveReceiptContractError(ValueError):
    """Stored or requested receipt material is not one exact V12 wave."""


def ordinary_cutover_id(operation_id: object) -> str:
    """Derive the cutover identity independently of the orchestrator."""

    operation = _digest(operation_id, "operation ID")
    cutover_id = hashlib.sha256(
        _CUTOVER_PREFIX + operation.encode("ascii")
    ).hexdigest()
    if cutover_id == operation:
        raise PTGWaveReceiptContractError("cutover ID is invalid")
    return cutover_id


def admission_receipt_mapping(
    wave: Any,
    intents: Sequence[Any],
) -> dict[str, Any]:
    """Rebuild one admission from its once-verified immutable envelope."""

    from process.ptg_wave_receipt_admission import (
        rebuild_admission_receipt_mapping,
    )

    return rebuild_admission_receipt_mapping(wave, intents)


def linkage_receipt_payload(
    admission: Mapping[str, Any],
    *,
    cutover_id: object,
    outcomes_digest: object,
    mapping_digest: object,
    linkage_ack_digest: object,
) -> dict[str, Any]:
    """Build the frozen flat linkage receipt payload."""

    admission_map = _exact_admission(admission)
    operation_id = admission_map["wave_id"]
    expected_cutover = ordinary_cutover_id(operation_id)
    if cutover_id != expected_cutover:
        raise PTGWaveReceiptContractError(
            "linkage receipt cutover identity is invalid"
        )
    receipt_payload_by_field = {
        "operation_id": operation_id,
        "cutover_id": expected_cutover,
        **{
            field_name: field_value
            for field_name, field_value in admission_map.items()
            if field_name not in {"attestation_schema", "receipt_key_id"}
        },
        "outcomes_digest": _digest(outcomes_digest, "outcomes digest"),
        "mapping_digest": _digest(mapping_digest, "mapping digest"),
        "linkage_ack_digest": _digest(
            linkage_ack_digest,
            "linkage acknowledgement digest",
        ),
    }
    if set(receipt_payload_by_field) != LINKAGE_PAYLOAD_FIELDS:
        raise AssertionError("linkage receipt payload field set changed")
    return receipt_payload_by_field


def validate_abandonment_request(
    request: object,
    *,
    wave: Any,
    admission: Mapping[str, Any],
) -> dict[str, Any]:
    """Require caller material to equal the frozen stored v6 admission."""

    if not isinstance(request, Mapping) or set(request) != {
        "schema",
        "key_id",
        "operation_id",
        "cutover_id",
        "admission",
    }:
        raise PTGWaveReceiptContractError(
            "V12 abandonment request fields are invalid"
        )
    admission_map = _exact_admission(admission)
    if request.get("schema") != ABANDONMENT_REQUEST_SCHEMA:
        raise PTGWaveReceiptContractError(
            "V12 abandonment request schema is unsupported"
        )
    operation_id = _digest(request.get("operation_id"), "operation ID")
    cutover_id = _digest(request.get("cutover_id"), "cutover ID")
    if (
        operation_id != getattr(wave, "wave_id", None)
        or operation_id != admission_map["wave_id"]
        or cutover_id != ordinary_cutover_id(operation_id)
        or request.get("key_id") != admission_map["receipt_key_id"]
        or request.get("admission") != admission_map
    ):
        raise PTGWaveReceiptContractError(
            "V12 abandonment request conflicts with stored admission"
        )
    return dict(request)


def validate_receipt_admission(value: object) -> dict[str, Any]:
    """Validate the exact admission projection embedded in V12 receipts."""

    if not isinstance(value, Mapping):
        raise PTGWaveReceiptContractError(
            "V12 receipt admission must be an object"
        )
    return _exact_admission(value)


def _require_exact_persisted_intents(
    wave: Any,
    intents: Sequence[Any],
    attestation: Mapping[str, Any],
    request_digest: str,
) -> None:
    """Re-derive every persisted job byte and manifest digest."""

    from api.control_import_waves import (
        _prepare_wave_intents,
        _validate_signed_intents,
    )

    signed_intents = _validate_signed_intents(
        attestation.get("intents"),
        wave_id=wave.wave_id,
    )
    preparation_request_by_field = {
        "wave_id": wave.wave_id,
        "request_digest": request_digest,
        "wave_digest": wave.wave_digest,
        "release_queue": wave.release_queue,
        "intents": signed_intents,
    }
    prepared, jobs_digest, manifest_digest = _prepare_wave_intents(
        preparation_request_by_field,
        now=wave.created_at,
        enqueue_time_ms=wave.enqueue_time_ms,
    )
    ordered = sorted(intents, key=lambda intent: int(intent.ordinal))
    if (
        len(ordered) != wave.intent_count
        or [intent.ordinal for intent in ordered] != list(range(wave.intent_count))
        or jobs_digest != wave.jobs_digest
        or manifest_digest != wave.manifest_digest
    ):
        raise PTGWaveReceiptContractError(
            "stored receipt admission intents are incomplete"
        )
    for stored_intent, expected in zip(ordered, prepared):
        expected_by_field = {
            "wave_id": wave.wave_id,
            "ordinal": expected["ordinal"],
            "run_id": expected["run_id"],
            "source_file_import_id": expected["source_id"],
            "content_version": expected["content_version"],
            "run_idempotency_key": expected["run_key"],
            "job_id": expected["job_id"],
            "params": expected["persisted_params"],
            "job_payload": expected["job_payload"],
            "serialized_job": expected["serialized_job"],
            "serialized_job_digest": expected["serialized_job_digest"],
        }
        if any(
            getattr(stored_intent, field_name, None) != expected_value
            for field_name, expected_value in expected_by_field.items()
        ):
            raise PTGWaveReceiptContractError(
                "stored receipt admission intent changed"
            )


def _exact_admission(admission_value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate every field of one immutable admission projection."""
    if not isinstance(admission_value, Mapping) or set(admission_value) != ADMISSION_FIELDS:
        raise PTGWaveReceiptContractError(
            "V12 receipt admission fields are invalid"
        )
    admission_by_field = dict(admission_value)
    if admission_by_field.get("attestation_schema") != RECEIPT_ATTESTATION_VERSION:
        raise PTGWaveReceiptContractError(
            "V12 receipt admission schema is unsupported"
        )
    try:
        receipt_key_id = require_receipt_key_id(admission_by_field.get("receipt_key_id"))
        receipt_public_material = require_receipt_public_material(
            admission_by_field.get("receipt_public_modulus_hex"),
            admission_by_field.get("receipt_public_exponent"),
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise PTGWaveReceiptContractError(str(exc)) from exc
    if receipt_key_id != admission_by_field["receipt_key_id"]:
        raise PTGWaveReceiptContractError(
            "V12 receipt admission key is invalid"
        )
    if receipt_public_material != (
        admission_by_field["receipt_public_modulus_hex"],
        admission_by_field["receipt_public_exponent"],
    ):
        raise PTGWaveReceiptContractError(
            "V12 receipt admission public key is invalid"
        )
    _validate_admission_counts(admission_by_field)
    return admission_by_field


def _validate_admission_counts(admission_by_field: Mapping[str, Any]) -> None:
    """Validate admission digests and exact partition count arithmetic."""

    for field_name in ADMISSION_FIELDS - {
        "attestation_schema",
        "receipt_key_id",
        "receipt_public_modulus_hex",
        "receipt_public_exponent",
        "entitlement_coverage_count",
        "physical_coordinate_count",
        "imported_coordinate_count",
        "reused_coordinate_count",
        "intent_count",
    }:
        _digest(admission_by_field.get(field_name), field_name)
    for field_name in {
        "entitlement_coverage_count",
        "reused_coordinate_count",
    }:
        if type(admission_by_field.get(field_name)) is not int or admission_by_field[field_name] < 0:
            raise PTGWaveReceiptContractError(f"{field_name} is invalid")
    for field_name in {
        "physical_coordinate_count",
        "imported_coordinate_count",
        "intent_count",
    }:
        if type(admission_by_field.get(field_name)) is not int or admission_by_field[field_name] < 1:
            raise PTGWaveReceiptContractError(f"{field_name} is invalid")
    if (
        admission_by_field["physical_coordinate_count"]
        != admission_by_field["imported_coordinate_count"]
        + admission_by_field["reused_coordinate_count"]
        or admission_by_field["imported_coordinate_count"] != admission_by_field["intent_count"]
    ):
        raise PTGWaveReceiptContractError(
            "V12 receipt admission partition counts are invalid"
        )


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _HEX_64.fullmatch(value) is None:
        raise PTGWaveReceiptContractError(f"{name} is invalid")
    return value


__all__ = [
    "ABANDONMENT_PROOF_SCHEMA",
    "ABANDONMENT_REQUEST_SCHEMA",
    "ADMISSION_FIELDS",
    "LINKAGE_PAYLOAD_FIELDS",
    "PTGWaveReceiptContractError",
    "V12_QUARANTINE_REASON",
    "admission_receipt_mapping",
    "linkage_receipt_payload",
    "ordinary_cutover_id",
    "validate_abandonment_request",
    "validate_receipt_admission",
]
