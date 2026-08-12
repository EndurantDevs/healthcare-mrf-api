"""Audited cutover from one pristine exact wave to ordinary PTG admission."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

from sqlalchemy import select

from api.control_import_wave_supersession import _as_aware_utc
from db.models import PTGImportWaveQuarantine, db
from process.ptg_parts.ptg_wave_admission_fence import (
    acquire_ptg_admission_lock,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
    validate_materialized_preclaim_supersession_proof,
)
from process.ptg_wave_materialized_preclaim_supersession_runtime import (
    attest_locked_materialized_preclaim_abandonment,
    attest_locked_v12_abandonment,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    canonical_receipt_timestamp,
    require_receipt_key_id,
)
from process.ptg_wave_receipt_process_authority import (
    require_process_receipt_keyring,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_REQUEST_SCHEMA,
    PTGWaveReceiptContractError,
    V12_QUARANTINE_REASON,
    ordinary_cutover_id,
    validate_receipt_admission,
)
from process.ptg_wave_state import canonical_json
from process.ptg_wave_v12_pristine_abandonment import (
    abandonment_receipt_payload,
    validate_v12_pristine_abandonment_proof,
)


QUARANTINE_REASON = "materialized_preclaim_failure"


async def abandon_materialized_preclaim_wave(
    wave_id: object,
    cutover_or_request: object,
    *,
    redis: Any,
    receipt_keyring: PTGWaveReceiptKeyring | None = None,
    receipt_issued_at: dt.datetime | str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Atomically quarantine one all-unclaimed wave without a successor.

    Legacy callers pass the cutover ID string and retain the original proof
    and response. Fresh V12 callers pass the exact signed-admission request
    and receive the persisted asymmetric receipt envelope directly.
    """

    normalized_wave_id, v12_request, normalized_cutover_id = (
        _normalized_abandonment_request(wave_id, cutover_or_request)
    )

    async with db.transaction() as session:
        await acquire_ptg_admission_lock(session)
        existing = await _locked_quarantine(session, normalized_wave_id)
        if existing is not None:
            if v12_request is not None:
                return _existing_v12_response(
                    existing,
                    request=v12_request,
                    receipt_keyring=receipt_keyring,
                ), False
            return _existing_legacy_response(
                existing,
                wave_id=normalized_wave_id,
                cutover_id=normalized_cutover_id,
            ), False
        cutover_owner = await _locked_cutover_owner(
            session,
            normalized_cutover_id,
        )
        if cutover_owner is not None:
            raise PTGWaveMaterializedPreclaimConflict(
                "cutover ID is already bound to another wave"
            )
        if v12_request is not None:
            return await _persist_v12_abandonment(
                session,
                normalized_wave_id,
                v12_request,
                redis=redis,
                receipt_keyring=receipt_keyring,
                receipt_issued_at=receipt_issued_at,
            )
        return await _persist_legacy_abandonment(
            session,
            normalized_wave_id,
            normalized_cutover_id,
            redis=redis,
        )


async def get_materialized_preclaim_abandonment(
    wave_id: object,
) -> dict[str, Any] | None:
    """Return one validated persisted legacy abandonment without mutation."""

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
    if (
        getattr(quarantine, "reason", None) != QUARANTINE_REASON
        or getattr(quarantine, "recovery_basis", None) != QUARANTINE_REASON
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "wave is already quarantined by another recovery"
        )
    cutover_id = _identity(getattr(quarantine, "cutover_id", None), "cutover ID")
    proof = _validated_existing_legacy_proof(
        quarantine,
        wave_id=normalized_wave_id,
        cutover_id=cutover_id,
    )
    return {
        "wave_id": normalized_wave_id,
        "cutover_id": cutover_id,
        "recovery_evidence": proof,
    }


def _normalized_abandonment_request(
    wave_id: object,
    cutover_or_request: object,
) -> tuple[str, dict[str, Any] | None, str]:
    """Normalize legacy and V12 abandonment coordinates."""

    normalized_wave_id = _identity(wave_id, "wave ID")
    if isinstance(cutover_or_request, Mapping):
        request_by_field = _v12_request_identity(
            cutover_or_request,
            wave_id=normalized_wave_id,
        )
        return normalized_wave_id, request_by_field, request_by_field["cutover_id"]
    normalized_cutover_id = _identity(cutover_or_request, "cutover ID")
    if normalized_cutover_id == normalized_wave_id:
        raise ValueError("cutover ID must differ from the wave ID")
    return normalized_wave_id, None, normalized_cutover_id


async def _locked_quarantine(session: Any, wave_id: str) -> Any | None:
    """Lock and return an existing quarantine for one wave identity."""

    return (
        await session.execute(
            select(PTGImportWaveQuarantine)
            .where(PTGImportWaveQuarantine.predecessor_wave_id == wave_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _locked_cutover_owner(session: Any, cutover_id: str) -> Any | None:
    """Lock and return the wave already bound to one cutover identity."""

    return (
        await session.execute(
            select(PTGImportWaveQuarantine)
            .where(PTGImportWaveQuarantine.cutover_id == cutover_id)
            .with_for_update()
        )
    ).scalar_one_or_none()


async def _persist_legacy_abandonment(
    session: Any,
    wave_id: str,
    cutover_id: str,
    *,
    redis: Any,
) -> tuple[dict[str, Any], bool]:
    """Persist the unchanged legacy proof and response."""

    witness = await attest_locked_materialized_preclaim_abandonment(
        session,
        wave_id,
        cutover_id,
        redis=redis,
    )
    canonical = canonical_json(
        {
            field_name: field_value
            for field_name, field_value in witness.items()
            if field_name != "proof_digest"
        }
    )
    session.add(
        PTGImportWaveQuarantine(
            predecessor_wave_id=wave_id,
            reason=QUARANTINE_REASON,
            cutover_id=cutover_id,
            recovery_basis=QUARANTINE_REASON,
            recovery_evidence=witness,
            recovery_evidence_canonical=canonical,
            recovery_evidence_sha256=witness["proof_digest"],
            created_at=_as_aware_utc(dt.datetime.now(dt.UTC)),
        )
    )
    await session.flush()
    return _legacy_response(
        wave_id=wave_id,
        cutover_id=cutover_id,
        proof=witness,
        proof_digest=witness["proof_digest"],
        created=True,
    ), True


async def _persist_v12_abandonment(
    session: Any,
    wave_id: str,
    request: dict[str, Any],
    *,
    redis: Any,
    receipt_keyring: PTGWaveReceiptKeyring | None,
    receipt_issued_at: dt.datetime | str | None,
) -> tuple[dict[str, Any], bool]:
    """Persist one distinct fresh-V12 proof and its RSA envelope."""

    proof = await attest_locked_v12_abandonment(
        session,
        wave_id,
        request,
        redis=redis,
    )
    abandonment_payload = abandonment_receipt_payload(proof)
    keyring = require_process_receipt_keyring(receipt_keyring)
    receipt = keyring.sign_receipt(
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id=request["key_id"],
        issued_at=receipt_issued_at or dt.datetime.now(dt.UTC),
        receipt_payload=abandonment_payload,
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
            reason=V12_QUARANTINE_REASON,
            cutover_id=request["cutover_id"],
            recovery_basis=V12_QUARANTINE_REASON,
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


def _existing_legacy_response(
    quarantine: Any,
    *,
    wave_id: str,
    cutover_id: str,
) -> dict[str, Any]:
    proof = _validated_existing_legacy_proof(
        quarantine,
        wave_id=wave_id,
        cutover_id=cutover_id,
    )
    return _legacy_response(
        wave_id=wave_id,
        cutover_id=cutover_id,
        proof=proof,
        proof_digest=proof["proof_digest"],
        created=False,
    )


def _validated_existing_legacy_proof(
    quarantine: Any,
    *,
    wave_id: str,
    cutover_id: str,
) -> dict[str, Any]:
    """Validate the exact stored proof shared by GET and POST replay."""

    if (
        getattr(quarantine, "reason", None) != QUARANTINE_REASON
        or getattr(quarantine, "recovery_basis", None) != QUARANTINE_REASON
        or getattr(quarantine, "cutover_id", None) != cutover_id
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "wave is already quarantined by another recovery"
        )
    proof = validate_materialized_preclaim_supersession_proof(
        getattr(quarantine, "recovery_evidence", None),
        predecessor_wave_id=wave_id,
        successor_wave_id=cutover_id,
    )
    proof_digest = getattr(quarantine, "recovery_evidence_sha256", None)
    canonical = canonical_json(
        {
            field_name: field_value
            for field_name, field_value in proof.items()
            if field_name != "proof_digest"
        }
    )
    if (
        proof_digest != proof["proof_digest"]
        or getattr(quarantine, "recovery_evidence_canonical", None)
        != canonical
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "stored abandonment proof metadata is invalid"
        )
    return proof


def _existing_v12_response(
    quarantine: Any,
    *,
    request: dict[str, Any],
    receipt_keyring: PTGWaveReceiptKeyring | None,
) -> dict[str, Any]:
    """Return only a byte-equivalent verified persisted V12 envelope."""

    if (
        getattr(quarantine, "reason", None) != V12_QUARANTINE_REASON
        or getattr(quarantine, "recovery_basis", None)
        != V12_QUARANTINE_REASON
        or getattr(quarantine, "cutover_id", None) != request["cutover_id"]
        or getattr(quarantine, "receipt_key_id", None) != request["key_id"]
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "wave is already quarantined by another recovery"
        )
    proof = validate_v12_pristine_abandonment_proof(
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
            "stored fresh V12 abandonment evidence is invalid"
        )
    abandonment_payload = abandonment_receipt_payload(proof)
    keyring = require_process_receipt_keyring(receipt_keyring)
    receipt = keyring.validate_stored_receipt(
        getattr(quarantine, "abandonment_receipt", None),
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id=request["key_id"],
        expected_payload=abandonment_payload,
    )
    issued_at = getattr(quarantine, "abandonment_receipt_issued_at", None)
    if (
        getattr(quarantine, "abandonment_receipt_payload_digest", None)
        != receipt["payload_digest"]
        or issued_at is None
        or canonical_receipt_timestamp(issued_at) != receipt["issued_at"]
    ):
        raise PTGWaveMaterializedPreclaimConflict(
            "stored fresh V12 abandonment receipt metadata is invalid"
        )
    return receipt


def _legacy_response(
    *,
    wave_id: str,
    cutover_id: str,
    proof: dict[str, Any],
    proof_digest: str,
    created: bool,
) -> dict[str, Any]:
    database = proof["database"]
    redis = proof["redis"]
    run_count = database["pristine_run_count"]
    return {
        "wave_id": wave_id,
        "cutover_id": cutover_id,
        "state": "abandoned",
        "quarantine_reason": QUARANTINE_REASON,
        "quarantined_run_count": run_count,
        "unclaimed_run_count": run_count,
        "queued_run_count": run_count,
        "claim_count": database["claim_count"],
        "outcome_count": database["outcome_count"],
        "worker_start_event_count": database["worker_start_event_count"],
        "redis_release_present": redis["release_present"],
        "proof_digest": proof_digest,
        "created": created,
    }


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


__all__ = [
    "abandon_materialized_preclaim_wave",
    "get_materialized_preclaim_abandonment",
]
