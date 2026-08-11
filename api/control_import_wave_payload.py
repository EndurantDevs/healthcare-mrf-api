"""Pure validation and job projection for exact PTGSmall wave admission."""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from typing import Any

from api import control_import_wave_direct as direct_wave
from api.control_import_wave_attestation import (
    RECEIPT_ATTESTATION_VERSION,
    _canonical,
    _identifier,
    _sha256,
    _validate_attestation_envelope,
    _validate_partition,
    _validate_snapshot,
    _verify_attestation,
)
from api.control_import_wave_constants import (
    MAX_INTENTS,
    PROTOCOL_IDENTITY,
    QUEUE,
)
from api.control_import_wave_recovery import project_admission_recovery_proofs
from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
    require_receipt_key_id,
    require_receipt_public_material,
)


def _job_id(
    wave_id: str,
    request_digest: str,
    ordinal: int,
    run_id: str,
) -> str:
    identity = _sha256(
        f"{wave_id}\0{request_digest}\0{ordinal}\0{run_id}".encode()
    )
    return f"ptg_start_{identity}"


def _run_key(wave_id: str, request_digest: str, ordinal: int) -> str:
    return "ptg-wave:" + _sha256(
        f"{wave_id}\0{request_digest}\0{ordinal}".encode()
    )


def _canonical_job_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        if not all(isinstance(key, str) for key in payload):
            raise ValueError("ARQ job payload keys must be strings")
        return {
            key: _canonical_job_payload(payload[key])
            for key in sorted(payload)
        }
    if isinstance(payload, list):
        return [_canonical_job_payload(item) for item in payload]
    if isinstance(payload, (str, int, bool)) or payload is None:
        return payload
    if isinstance(payload, float):
        if not payload == payload or payload in (float("inf"), float("-inf")):
            raise ValueError("ARQ job payload contains a non-finite float")
        return payload
    raise ValueError("ARQ job payload contains an unsupported value")


def _validate_signed_intents(
    raw_intents: object,
    *,
    wave_id: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(raw_intents, list) or not 1 <= len(raw_intents) <= MAX_INTENTS:
        raise ValueError(
            "cohort_attestation intents must contain between 1 and "
            f"{MAX_INTENTS} items"
        )
    intents: list[dict[str, Any]] = []
    run_ids: set[str] = set()
    source_ids: set[str] = set()
    for ordinal, raw in enumerate(raw_intents):
        direct_wave.require_bounded_direct_intent(raw)
        expected_intent_fields = {
            "ordinal", "run_id", "source_file_import_id", "content_version", "params",
        }
        if not isinstance(raw, dict) or set(raw) != expected_intent_fields:
            raise ValueError("each signed intent fields are not exact")
        if raw["ordinal"] != ordinal:
            raise ValueError("signed intent ordinals must be contiguous from zero")
        run_id = _identifier(raw["run_id"], "run_id", 64)
        source_id = _identifier(
            raw["source_file_import_id"], "source_file_import_id", 64
        )
        if run_id in run_ids or source_id in source_ids:
            raise ValueError(
                "signed intent run_id and source_file_import_id values must be unique"
            )
        run_ids.add(run_id)
        source_ids.add(source_id)
        if not isinstance(raw["params"], dict):
            raise ValueError("signed intent params must be an object")
        content_version = _identifier(
            raw["content_version"], "content_version", 128
        )
        normalized_params = direct_wave.normalized_wave_params(raw["params"])
        direct_wave.require_matching_direct_coordinate(
            normalized_params,
            content_version,
            source_file_import_id=source_id,
            wave_id=wave_id or "",
        )
        intents.append({
            "run_id": run_id, "source_file_import_id": source_id,
            "content_version": content_version, "params": normalized_params,
        })
    return intents


def _project_import_wave_payload(
    request_body: object,
    *,
    attestation_key: str | bytes | None = None,
    authenticate: bool,
) -> dict[str, Any]:
    """Validate the closed attestation and derive all persisted identities."""

    if not isinstance(request_body, dict) or set(request_body) != {
        "cohort_attestation"
    }:
        raise ValueError("import wave payload must contain only cohort_attestation")
    direct_wave.require_bounded_wave_request(request_body)
    if authenticate:
        attestation = _verify_attestation(
            request_body["cohort_attestation"],
            attestation_key=attestation_key,
        )
    else:
        attestation = _validate_attestation_envelope(
            request_body["cohort_attestation"]
        )
    receipt_key_id, receipt_public_modulus_hex, receipt_public_exponent = (
        _project_receipt_key_material(attestation)
    )
    wave_id = _identifier(attestation["wave_id"], "wave_id", 64)
    idempotency_key = _identifier(
        attestation["idempotency_key"], "idempotency_key", 160
    )
    _validate_receipt_wave_identity(attestation, wave_id, idempotency_key)
    snapshot, partition, intents, recovery_proofs = _validated_wave_partition(
        attestation, wave_id=wave_id
    )
    unsigned_attestation_map = {
        key: intent_field_value
        for key, intent_field_value in attestation.items()
        if key != "signature"
    }
    request_digest = _sha256(_canonical(unsigned_attestation_map))
    wave_digest = _sha256((PROTOCOL_IDENTITY + "\0" + request_digest).encode())
    validated_request_map = {
        "wave_id": wave_id, "idempotency_key": idempotency_key,
        "attestation": attestation, "snapshot": snapshot, "partition": partition,
        "intents": intents, "request_digest": request_digest,
        "attestation_digest": _sha256(_canonical(attestation)),
        "signature_digest": _sha256(attestation["signature"].encode()),
        "receipt_key_id": receipt_key_id,
        "receipt_public_modulus_hex": receipt_public_modulus_hex,
        "receipt_public_exponent": receipt_public_exponent,
        "wave_digest": wave_digest, "release_queue": f"{QUEUE}:wave:{wave_digest}",
    }
    validated_request_map.update(recovery_proofs)
    return validated_request_map


def _project_receipt_key_material(
    attestation: Mapping[str, Any],
) -> tuple[str | None, str | None, int | None]:
    """Validate and project the optional V6 receipt trust root."""

    if attestation["schema_version"] != RECEIPT_ATTESTATION_VERSION:
        return None, None, None
    try:
        key_id = require_receipt_key_id(attestation["receipt_key_id"])
        modulus, exponent = require_receipt_public_material(
            attestation["receipt_public_modulus_hex"],
            attestation["receipt_public_exponent"],
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise ValueError(str(exc)) from exc
    return key_id, modulus, exponent


def _validate_receipt_wave_identity(
    attestation: Mapping[str, Any],
    wave_id: str,
    idempotency_key: str,
) -> None:
    """Require a V6 wave identity to be its lowercase operation digest."""

    if attestation["schema_version"] == RECEIPT_ATTESTATION_VERSION and (
        len(wave_id) != 64
        or any(character not in "0123456789abcdef" for character in wave_id)
        or idempotency_key != wave_id
    ):
        raise ValueError(
            "v6 receipt wave_id must be the exact lowercase operation digest"
        )


def _validated_wave_partition(
    attestation: Mapping[str, Any],
    *,
    wave_id: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Validate the signed snapshot, partition, members, and recovery proof."""

    snapshot = _validate_snapshot(
        attestation["snapshot"],
        schema_version=attestation["schema_version"],
    )
    partition = _validate_partition(attestation["partition"])
    intents = _validate_signed_intents(attestation["intents"], wave_id=wave_id)
    recovery_proofs = project_admission_recovery_proofs(
        attestation, wave_id=wave_id
    )
    if partition["imported_coordinate_count"] != len(intents):
        raise ValueError(
            "partition imported_coordinate_count must equal signed intent count"
        )
    imported_coordinate_digest = _sha256(
        "\0".join(
            f"{intent['source_file_import_id']}\0{intent['content_version']}"
            for intent in intents
        ).encode("utf-8")
    )
    if not hmac.compare_digest(
        partition["imported_coordinate_digest"], imported_coordinate_digest
    ):
        raise ValueError(
            "partition imported_coordinate_digest does not match signed intents"
        )
    return snapshot, partition, intents, recovery_proofs


def validate_import_wave_payload(
    request_body: object,
    *,
    attestation_key: str | bytes | None = None,
) -> dict[str, Any]:
    """Authenticate one new admission and derive all persisted identities."""

    return _project_import_wave_payload(
        request_body,
        attestation_key=attestation_key,
        authenticate=True,
    )
