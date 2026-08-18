"""V13-specific persistence and replay for ordinary PTG abandonment."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

from sqlalchemy import select

from db.models import PTGImportWaveQuarantine, db
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    canonical_receipt_timestamp,
    require_receipt_key_id,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_REQUEST_SCHEMA,
    PTGWaveReceiptContractError,
    ordinary_cutover_id,
    validate_receipt_admission,
)
from process.ptg_wave_receipt_process_authority import (
    require_process_receipt_keyring,
)
from process.ptg_wave_state import canonical_json
from process.ptg_wave_v13_post_ready_abandonment import (
    V13_ABANDONMENT_REQUEST_SCHEMA,
    V13_QUARANTINE_REASON,
    abandonment_receipt_payload,
    validate_v13_abandonment_proof,
)
from process.ptg_wave_v13_post_ready_abandonment_runtime import (
    attest_locked_v13_abandonment,
)


async def get_v13_post_ready_abandonment(
    wave_id: object,
    *,
    receipt_keyring: PTGWaveReceiptKeyring | None,
) -> dict[str, Any] | None:
    """Return one verified persisted V13 receipt without mutation."""

    normalized_wave_id = _identity(wave_id, "wave ID")
    quarantine = (
        await db.execute(
            select(PTGImportWaveQuarantine).where(
                PTGImportWaveQuarantine.predecessor_wave_id
                == normalized_wave_id
            )
        )
    ).scalar_one_or_none()
    if quarantine is None:
        return None
    if getattr(quarantine, "reason", None) != V13_QUARANTINE_REASON:
        raise PTGWaveMaterializedPreclaimConflict(
            "wave is already quarantined by another recovery"
        )
    cutover_id = _identity(getattr(quarantine, "cutover_id", None), "cutover ID")
    proof = validate_v13_abandonment_proof(
        getattr(quarantine, "recovery_evidence", None),
        operation_id=normalized_wave_id,
        cutover_id=cutover_id,
    )
    receipt_request_by_field = {
        "schema": V13_ABANDONMENT_REQUEST_SCHEMA,
        "key_id": getattr(quarantine, "receipt_key_id", None),
        "operation_id": normalized_wave_id,
        "cutover_id": cutover_id,
        "admission": proof["admission"],
    }
    return existing_v13_response(
        quarantine,
        request=receipt_request_by_field,
        receipt_keyring=receipt_keyring,
    )


def normalize_abandonment_request(
    wave_id: object,
    cutover_or_request: object,
) -> tuple[str, dict[str, Any] | None, str]:
    """Normalize legacy and closed-family fresh abandonment coordinates."""

    normalized_wave_id = _identity(wave_id, "wave ID")
    if isinstance(cutover_or_request, Mapping):
        if cutover_or_request.get("schema") == ABANDONMENT_REQUEST_SCHEMA:
            request_by_field = _v12_request_identity(
                cutover_or_request,
                wave_id=normalized_wave_id,
            )
        elif cutover_or_request.get("schema") == V13_ABANDONMENT_REQUEST_SCHEMA:
            request_by_field = _v13_request_identity(
                cutover_or_request,
                wave_id=normalized_wave_id,
            )
        else:
            raise PTGWaveReceiptContractError(
                "abandonment request schema is unsupported"
            )
        return normalized_wave_id, request_by_field, request_by_field["cutover_id"]
    normalized_cutover_id = _identity(cutover_or_request, "cutover ID")
    if normalized_cutover_id == normalized_wave_id:
        raise ValueError("cutover ID must differ from the wave ID")
    return normalized_wave_id, None, normalized_cutover_id


async def persist_v13_abandonment(
    session: Any,
    wave_id: str,
    request: dict[str, Any],
    *,
    redis: Any,
    receipt_keyring: PTGWaveReceiptKeyring | None,
    receipt_issued_at: dt.datetime | str | None,
) -> tuple[dict[str, Any], bool]:
    """Persist one V13 post-ready failure proof and its existing v2 receipt."""

    proof = await attest_locked_v13_abandonment(
        session,
        wave_id,
        request,
        redis=redis,
    )
    receipt_payload_by_field = abandonment_receipt_payload(proof)
    keyring = require_process_receipt_keyring(receipt_keyring)
    receipt = keyring.sign_receipt(
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id=request["key_id"],
        issued_at=receipt_issued_at or dt.datetime.now(dt.UTC),
        receipt_payload=receipt_payload_by_field,
    )
    unsigned_proof_by_field = {
        field_name: field_value
        for field_name, field_value in proof.items()
        if field_name != "proof_digest"
    }
    issued_at = _receipt_datetime(receipt["issued_at"])
    session.add(
        PTGImportWaveQuarantine(
            predecessor_wave_id=wave_id,
            reason=V13_QUARANTINE_REASON,
            cutover_id=request["cutover_id"],
            recovery_basis=V13_QUARANTINE_REASON,
            recovery_evidence=proof,
            recovery_evidence_canonical=canonical_json(unsigned_proof_by_field),
            recovery_evidence_sha256=proof["proof_digest"],
            receipt_key_id=request["key_id"],
            abandonment_receipt=receipt,
            abandonment_receipt_payload_digest=receipt["payload_digest"],
            abandonment_receipt_issued_at=issued_at,
            created_at=issued_at,
        )
    )
    await session.flush()
    return receipt, True


def existing_v13_response(
    quarantine: Any,
    *,
    request: dict[str, Any],
    receipt_keyring: PTGWaveReceiptKeyring | None,
) -> dict[str, Any]:
    """Return only the persisted, verified V13 v2 receipt envelope."""

    if (
        getattr(quarantine, "reason", None) != V13_QUARANTINE_REASON
        or getattr(quarantine, "recovery_basis", None) != V13_QUARANTINE_REASON
        or getattr(quarantine, "cutover_id", None) != request["cutover_id"]
        or getattr(quarantine, "receipt_key_id", None) != request["key_id"]
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "wave is already quarantined by another recovery"
        )
    proof = validate_v13_abandonment_proof(
        getattr(quarantine, "recovery_evidence", None),
        operation_id=request["operation_id"],
        cutover_id=request["cutover_id"],
        admission=request["admission"],
    )
    unsigned_proof_by_field = {
        field_name: field_value
        for field_name, field_value in proof.items()
        if field_name != "proof_digest"
    }
    if (
        getattr(quarantine, "recovery_evidence_sha256", None)
        != proof["proof_digest"]
        or getattr(quarantine, "recovery_evidence_canonical", None)
        != canonical_json(unsigned_proof_by_field)
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "stored fresh V13 abandonment evidence is invalid"
        )
    receipt_payload_by_field = abandonment_receipt_payload(proof)
    keyring = require_process_receipt_keyring(receipt_keyring)
    receipt = keyring.validate_stored_receipt(
        getattr(quarantine, "abandonment_receipt", None),
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id=request["key_id"],
        expected_payload=receipt_payload_by_field,
    )
    issued_at = getattr(quarantine, "abandonment_receipt_issued_at", None)
    if (
        getattr(quarantine, "abandonment_receipt_payload_digest", None)
        != receipt["payload_digest"]
        or issued_at is None
        or canonical_receipt_timestamp(issued_at) != receipt["issued_at"]
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "stored fresh V13 abandonment receipt metadata is invalid"
        )
    return receipt


def _v12_request_identity(
    request_by_field: Mapping[str, Any],
    *,
    wave_id: str,
) -> dict[str, Any]:
    expected_fields = {
        "schema",
        "key_id",
        "operation_id",
        "cutover_id",
        "admission",
    }
    if set(request_by_field) != expected_fields:
        raise PTGWaveReceiptContractError(
            "V12 abandonment request fields are invalid"
        )
    admission = validate_receipt_admission(request_by_field.get("admission"))
    try:
        key_id = require_receipt_key_id(
            request_by_field.get("key_id"),
            "V12 abandonment request key ID",
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise PTGWaveReceiptContractError(str(exc)) from exc
    if (
        request_by_field.get("schema") != ABANDONMENT_REQUEST_SCHEMA
        or request_by_field.get("operation_id") != wave_id
        or admission["wave_id"] != wave_id
        or request_by_field.get("cutover_id") != ordinary_cutover_id(wave_id)
        or key_id != admission["receipt_key_id"]
    ):
        raise PTGWaveReceiptContractError(
            "V12 abandonment request identity is invalid"
        )
    return {
        "schema": ABANDONMENT_REQUEST_SCHEMA,
        "key_id": key_id,
        "operation_id": wave_id,
        "cutover_id": request_by_field["cutover_id"],
        "admission": admission,
    }


def _v13_request_identity(
    request_by_field: Mapping[str, Any],
    *,
    wave_id: str,
) -> dict[str, Any]:
    """Validate the request coordinate before the locked V13 re-observation."""

    expected_fields = {
        "schema",
        "key_id",
        "operation_id",
        "cutover_id",
        "admission",
    }
    if set(request_by_field) != expected_fields:
        raise PTGWaveReceiptContractError(
            "V13 abandonment request fields are invalid"
        )
    admission = validate_receipt_admission(request_by_field.get("admission"))
    try:
        key_id = require_receipt_key_id(
            request_by_field.get("key_id"),
            "V13 abandonment request key ID",
        )
    except PTGWaveReceiptAuthorityError as exc:
        raise PTGWaveReceiptContractError(str(exc)) from exc
    if (
        request_by_field.get("schema") != V13_ABANDONMENT_REQUEST_SCHEMA
        or request_by_field.get("operation_id") != wave_id
        or admission["wave_id"] != wave_id
        or request_by_field.get("cutover_id") != ordinary_cutover_id(wave_id)
        or key_id != admission["receipt_key_id"]
    ):
        raise PTGWaveReceiptContractError(
            "V13 abandonment request identity is invalid"
        )
    return {
        "schema": V13_ABANDONMENT_REQUEST_SCHEMA,
        "key_id": key_id,
        "operation_id": wave_id,
        "cutover_id": request_by_field["cutover_id"],
        "admission": admission,
    }


def _receipt_datetime(value: str) -> dt.datetime:
    return dt.datetime.strptime(
        canonical_receipt_timestamp(value),
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ).replace(tzinfo=dt.UTC)


def _identity(value: object, name: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > 64
    ):
        raise ValueError(f"{name} is invalid")
    return value
