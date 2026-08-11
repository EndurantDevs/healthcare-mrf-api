"""Stored admission reconstruction behind the receipt-contract facade."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from api.control_import_wave_attestation import (
    RECEIPT_ATTESTATION_VERSION,
    _canonical,
    _sha256,
    _validate_attestation_envelope,
    _validate_partition,
    _validate_snapshot,
)
from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
    require_receipt_key_id,
    require_receipt_public_material,
)


_PROTOCOL_IDENTITY = "healthporta.ptg-small.exact-wave.v1"


def rebuild_admission_receipt_mapping(
    wave: Any,
    intents: Sequence[Any],
) -> dict[str, Any]:
    """Rebuild one admission while preserving validation order."""

    from process.ptg_wave_receipt_contract import (
        _require_exact_persisted_intents,
    )

    attestation, key_id, modulus, exponent = (
        _validated_stored_receipt_envelope(wave)
    )
    snapshot, request_digest = _validated_stored_admission_snapshot(
        wave,
        attestation,
    )
    _require_exact_persisted_intents(
        wave,
        intents,
        attestation,
        request_digest,
    )
    return _admission_receipt_fields(
        wave,
        snapshot,
        key_id=key_id,
        modulus=modulus,
        exponent=exponent,
    )


def _validated_stored_receipt_envelope(
    wave: Any,
) -> tuple[dict[str, Any], str, str, int]:
    from process.ptg_wave_receipt_contract import PTGWaveReceiptContractError

    try:
        attestation = _validate_attestation_envelope(
            getattr(wave, "cohort_attestation", None)
        )
    except (ValueError, TypeError) as exc:
        raise PTGWaveReceiptContractError(
            "stored receipt admission envelope is invalid"
        ) from exc
    if attestation.get("schema_version") != RECEIPT_ATTESTATION_VERSION:
        raise PTGWaveReceiptContractError(
            "stored wave is not a fresh receipt-authorized admission"
        )
    try:
        key_id = require_receipt_key_id(
            attestation.get("receipt_key_id"),
            "stored admission receipt key ID",
        )
        modulus, exponent = require_receipt_public_material(
            attestation.get("receipt_public_modulus_hex"),
            attestation.get("receipt_public_exponent"),
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise PTGWaveReceiptContractError(str(exc)) from exc
    if (
        getattr(wave, "receipt_key_id", None) != key_id
        or getattr(wave, "receipt_public_modulus_hex", None) != modulus
        or getattr(wave, "receipt_public_exponent", None) != exponent
    ):
        raise PTGWaveReceiptContractError(
            "stored admission receipt key binding is invalid"
        )
    return attestation, key_id, modulus, exponent


def _validated_stored_admission_snapshot(
    wave: Any,
    attestation: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    from process.ptg_wave_receipt_contract import PTGWaveReceiptContractError

    snapshot = _validate_snapshot(
        attestation.get("snapshot"),
        schema_version=RECEIPT_ATTESTATION_VERSION,
    )
    partition = _validate_partition(attestation.get("partition"))
    unsigned_attestation_by_field = {
        field_name: field_value
        for field_name, field_value in attestation.items()
        if field_name != "signature"
    }
    request_digest = _sha256(_canonical(unsigned_attestation_by_field))
    expected_scalar_by_field = _expected_stored_wave_fields(
        attestation,
        partition,
        request_digest,
    )
    if (
        expected_scalar_by_field["wave_id"]
        != expected_scalar_by_field["idempotency_key"]
        or any(
            getattr(wave, field_name, None) != expected_value
            for field_name, expected_value in expected_scalar_by_field.items()
        )
    ):
        raise PTGWaveReceiptContractError(
            "stored receipt admission identity is invalid"
        )
    return snapshot, request_digest


def _expected_stored_wave_fields(
    attestation: Mapping[str, Any],
    partition: Mapping[str, Any],
    request_digest: str,
) -> dict[str, Any]:
    wave_digest = _sha256(
        (_PROTOCOL_IDENTITY + "\0" + request_digest).encode("utf-8")
    )
    return {
        "wave_id": attestation.get("wave_id"),
        "idempotency_key": attestation.get("idempotency_key"),
        "request_digest": request_digest,
        "cohort_attestation_digest": _sha256(_canonical(attestation)),
        "cohort_signature_digest": _sha256(
            str(attestation.get("signature") or "").encode("utf-8")
        ),
        "wave_digest": wave_digest,
        "physical_coordinate_count": partition["physical_coordinate_count"],
        "physical_coordinate_digest": partition["physical_coordinate_digest"],
        "imported_coordinate_count": partition["imported_coordinate_count"],
        "imported_coordinate_digest": partition["imported_coordinate_digest"],
        "reused_coordinate_count": partition["reused_coordinate_count"],
        "reused_coordinate_digest": partition["reused_coordinate_digest"],
        "partition_digest": partition["partition_digest"],
    }


def _admission_receipt_fields(
    wave: Any,
    snapshot: Mapping[str, Any],
    *,
    key_id: str,
    modulus: str,
    exponent: int,
) -> dict[str, Any]:
    from process.ptg_wave_receipt_contract import ADMISSION_FIELDS

    admission_by_field = {
        "attestation_schema": RECEIPT_ATTESTATION_VERSION,
        "receipt_key_id": key_id,
        "receipt_public_modulus_hex": modulus,
        "receipt_public_exponent": exponent,
        "wave_id": wave.wave_id,
        "wave_digest": wave.wave_digest,
        "request_digest": wave.request_digest,
        "cohort_attestation_digest": wave.cohort_attestation_digest,
        "cohort_signature_digest": wave.cohort_signature_digest,
        "authorization_digest": snapshot["authorization_digest"],
        "snapshot_digest": snapshot["snapshot_digest"],
        "membership_digest": snapshot["membership_digest"],
        "inventory_digest": snapshot["inventory_digest"],
        "subscription_coverage_digest": snapshot["subscription_coverage_digest"],
        "entitlement_coverage_digest": snapshot["entitlement_coverage_digest"],
        "entitlement_coverage_count": snapshot["entitlement_coverage_count"],
        "catalog_generation": snapshot["catalog_generation"],
        "physical_coordinate_digest": wave.physical_coordinate_digest,
        "imported_coordinate_digest": wave.imported_coordinate_digest,
        "reused_coordinate_digest": wave.reused_coordinate_digest,
        "partition_digest": wave.partition_digest,
        "physical_coordinate_count": wave.physical_coordinate_count,
        "imported_coordinate_count": wave.imported_coordinate_count,
        "reused_coordinate_count": wave.reused_coordinate_count,
        "intent_count": wave.intent_count,
        "jobs_digest": wave.jobs_digest,
        "manifest_digest": wave.manifest_digest,
    }
    if set(admission_by_field) != ADMISSION_FIELDS:
        raise AssertionError("V12 admission receipt field set changed")
    return admission_by_field
