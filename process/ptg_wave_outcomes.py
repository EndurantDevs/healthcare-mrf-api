"""Stable exact-wave outcomes, linkage receipts, and terminal proof checks.

This module never contacts an external service.  It snapshots terminal
``ImportRun`` records into immutable per-intent rows, so pagination supplied to
the upstream reconciler cannot change beneath a cursor.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select, update

from db.models import (
    ImportRun,
    PTGImportWave,
    PTGImportWaveClaim,
    PTGImportWaveIntent,
    PTGImportWaveOutcome,
    db,
)
from process.ptg_wave_outcome_contract import (
    PTGWaveOutcomeConflict,
    _collection_digest,
    _record_digest,
    _rows_by_ordinal,
    _outcome_record,
    _validate_claim_outcomes,
    _validate_linkage_ack,
    _validate_persisted_linkage_ack_replay,
    _validate_v6_linkage_ack_binding,
    linkage_mapping_digest,
    sign_linkage_ack,
)
from process.ptg_wave_outcome_terminal_validation import (
    verify_terminal_eligibility,
)
from process.ptg_wave_receipt_authority import (
    LINKAGE_RECEIPT_SCHEMA,
    PTGWaveReceiptKeyring,
)
from process.ptg_wave_receipt_process_authority import (
    require_process_receipt_keyring,
)
from process.ptg_wave_receipt_contract import (
    PTGWaveReceiptContractError,
    admission_receipt_mapping,
    linkage_receipt_payload,
    ordinary_cutover_id,
)


@dataclass(frozen=True)
class _V6LinkageReceiptRequest:
    cutover_id: object
    receipt_key_id: object
    receipt_keyring: PTGWaveReceiptKeyring | None
    receipt_issued_at: dt.datetime | str | None


async def snapshot_terminal_outcomes(wave_id: str) -> str:
    """Atomically snapshot all N terminal run outcomes and enter linkage wait."""

    async with db.transaction() as session:
        wave_query_result = await session.execute(
            select(PTGImportWave).where(PTGImportWave.wave_id == wave_id).with_for_update()
        )
        wave = wave_query_result.scalar_one_or_none()
        if wave is None:
            raise PTGWaveOutcomeConflict("exact wave is not admitted")
        if wave.state == "awaiting_linkage":
            if wave.outcomes_digest is None:
                raise PTGWaveOutcomeConflict("linkage wait lacks its stable outcomes digest")
            return wave.outcomes_digest
        if wave.state != "executing":
            raise PTGWaveOutcomeConflict("terminal outcomes are not expected for this wave")
        terminal_outcome_records, digest = await _terminal_snapshot_records(
            session, wave, wave_id
        )
        await _persist_terminal_snapshot(
            session, wave, wave_id, terminal_outcome_records, digest
        )
        return digest


async def _terminal_snapshot_records(
    session: Any, wave: Any, wave_id: str
) -> tuple[list[dict[str, Any]], str]:
    intent_run_rows = (
        await session.execute(
            select(PTGImportWaveIntent, ImportRun)
            .join(ImportRun, ImportRun.run_id == PTGImportWaveIntent.run_id)
            .where(PTGImportWaveIntent.wave_id == wave_id)
            .order_by(PTGImportWaveIntent.ordinal)
            .with_for_update()
        )
    ).all()
    if len(intent_run_rows) != wave.intent_count:
        raise PTGWaveOutcomeConflict("every admitted intent must have one ImportRun")
    terminal_outcome_records = [
        _outcome_record(intent, run) for intent, run in intent_run_rows
    ]
    if [
        terminal_outcome_record["ordinal"]
        for terminal_outcome_record in terminal_outcome_records
    ] != list(range(wave.intent_count)):
        raise PTGWaveOutcomeConflict(
            "terminal outcomes are missing an admitted ordinal"
        )
    claim_rows = (
        await session.execute(
            select(PTGImportWaveClaim)
            .where(PTGImportWaveClaim.wave_id == wave_id)
            .with_for_update()
        )
    ).scalars().all()
    if len(claim_rows) != wave.intent_count:
        raise PTGWaveOutcomeConflict(
            "all terminal intents require an exact worker-start claim"
        )
    if [claim.ordinal for claim in _rows_by_ordinal(claim_rows)] != list(
        range(wave.intent_count)
    ):
        raise PTGWaveOutcomeConflict(
            "worker-start claims do not cover every admitted ordinal"
        )
    _validate_claim_outcomes(claim_rows, terminal_outcome_records)
    return terminal_outcome_records, _collection_digest(
        "healthporta.ptg-wave.outcomes.v1", terminal_outcome_records
    )


async def _persist_terminal_snapshot(
    session: Any,
    wave: Any,
    wave_id: str,
    terminal_outcome_records: list[dict[str, Any]],
    digest: str,
) -> None:
    existing = (
        await session.execute(
            select(PTGImportWaveOutcome)
            .where(PTGImportWaveOutcome.wave_id == wave_id)
            .with_for_update()
        )
    ).scalars().all()
    if existing:
        raise PTGWaveOutcomeConflict(
            "immutable terminal outcomes already exist before linkage wait"
        )
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    for terminal_outcome_record in terminal_outcome_records:
        session.add(
            PTGImportWaveOutcome(
                **terminal_outcome_record,
                wave_id=wave_id,
                outcome_digest=_record_digest(terminal_outcome_record),
                recorded_at=now,
            )
        )
    transition_result = await session.execute(
        update(PTGImportWave)
        .where(
            PTGImportWave.wave_id == wave_id,
            PTGImportWave.state == "executing",
            PTGImportWave.state_version == wave.state_version,
        )
        .values(
            state="awaiting_linkage",
            state_version=wave.state_version + 1,
            outcomes_digest=digest,
        )
    )
    if transition_result.rowcount != 1:
        raise PTGWaveOutcomeConflict(
            "wave state changed while terminal outcomes were recorded"
        )


async def get_wave_outcomes_page(
    wave_id: str, *, after_ordinal: int | None = None, limit: int = 200,
) -> dict[str, Any]:
    """Return an immutable ordinal page after terminal outcome snapshotting."""

    if after_ordinal is not None and (
        not isinstance(after_ordinal, int) or isinstance(after_ordinal, bool) or after_ordinal < -1
    ):
        raise PTGWaveOutcomeConflict("outcomes cursor ordinal is invalid")
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
        raise PTGWaveOutcomeConflict("outcomes page limit must be from 1 through 500")
    page_query_result = await db.execute(
        select(PTGImportWave).where(PTGImportWave.wave_id == wave_id)
    )
    wave = page_query_result.scalar_one_or_none()
    if wave is None or wave.outcomes_digest is None:
        raise PTGWaveOutcomeConflict("stable terminal outcomes are not available")
    query = select(PTGImportWaveOutcome).where(PTGImportWaveOutcome.wave_id == wave_id)
    if after_ordinal is not None:
        query = query.where(PTGImportWaveOutcome.ordinal > after_ordinal)
    outcome_rows = (
        await db.execute(
            query.order_by(PTGImportWaveOutcome.ordinal).limit(limit + 1)
        )
    ).scalars().all()
    outcome_records = [{
        "ordinal": outcome_row.ordinal,
        "run_id": outcome_row.run_id,
        "job_id": outcome_row.job_id,
        "source_file_import_id": outcome_row.source_file_import_id,
        "content_version": outcome_row.content_version,
        "status": outcome_row.status,
        "snapshot_id": outcome_row.snapshot_id,
        "import_id": outcome_row.import_id,
        "outcome_digest": outcome_row.outcome_digest,
    } for outcome_row in outcome_rows[:limit]]
    next_ordinal = (
        outcome_rows[limit - 1].ordinal
        if len(outcome_rows) > limit
        else None
    )
    return {
        "wave_id": wave.wave_id, "wave_digest": wave.wave_digest,
        "outcomes_digest": wave.outcomes_digest, "intent_count": wave.intent_count,
        "items": outcome_records, "next_ordinal": next_ordinal,
    }


async def record_linkage_ack(
    wave_id: str,
    ack: object,
    *,
    key: str | bytes | None = None,
    cutover_id: object = None,
    receipt_key_id: object = None,
    receipt_keyring: PTGWaveReceiptKeyring | None = None,
    receipt_issued_at: dt.datetime | str | None = None,
) -> str | dict[str, Any]:
    """Verify and persist the signed all-N source-linkage acknowledgement."""

    async with db.transaction() as session:
        wave, outcomes, is_v6_wave, is_v6_replay = await _locked_linkage_state(
            session, wave_id
        )
        if is_v6_replay:
            ack, digest = _validate_persisted_linkage_ack_replay(
                wave,
                outcomes,
                ack,
            )
        elif is_v6_wave:
            ack, digest = _validate_v6_linkage_ack_binding(
                wave,
                outcomes,
                ack,
            )
        else:
            ack, digest = _validate_linkage_ack(wave, outcomes, ack, key)
        if getattr(wave, "receipt_key_id", None) is not None:
            return await _record_v6_linkage_receipt(
                session,
                wave,
                outcomes,
                ack,
                digest,
                request=_V6LinkageReceiptRequest(
                    cutover_id=cutover_id,
                    receipt_key_id=receipt_key_id,
                    receipt_keyring=receipt_keyring,
                    receipt_issued_at=receipt_issued_at,
                ),
            )
        if cutover_id is not None or receipt_key_id is not None:
            raise PTGWaveOutcomeConflict(
                "legacy linkage cannot request an asymmetric receipt"
            )
        if wave.linkage_ack_digest is not None:
            if wave.linkage_ack_digest != digest:
                raise PTGWaveOutcomeConflict("linkage acknowledgement conflicts with the first receipt")
            return digest
        wave.linkage_ack = ack
        wave.linkage_ack_digest = digest
        await session.flush()
        return digest


async def _locked_linkage_state(
    session: Any,
    wave_id: str,
) -> tuple[Any, list[Any], bool, bool]:
    """Lock one wave then its complete immutable outcome graph."""

    wave_query_result = await session.execute(
        select(PTGImportWave)
        .where(PTGImportWave.wave_id == wave_id)
        .with_for_update()
    )
    wave = wave_query_result.scalar_one_or_none()
    is_v6_wave = bool(
        wave is not None and getattr(wave, "receipt_key_id", None) is not None
    )
    is_v6_replay = bool(
        is_v6_wave and getattr(wave, "linkage_receipt", None) is not None
    )
    if (
        wave is None
        or (wave.state != "awaiting_linkage" and not is_v6_replay)
        or wave.outcomes_digest is None
    ):
        raise PTGWaveOutcomeConflict(
            "linkage acknowledgement is not expected for this wave"
        )
    outcomes = (
        await session.execute(
            select(PTGImportWaveOutcome)
            .where(PTGImportWaveOutcome.wave_id == wave_id)
            .order_by(PTGImportWaveOutcome.ordinal)
            .with_for_update()
        )
    ).scalars().all()
    if len(outcomes) != wave.intent_count:
        raise PTGWaveOutcomeConflict(
            "linkage acknowledgement requires every stable terminal outcome"
        )
    return wave, outcomes, is_v6_wave, is_v6_replay


async def _record_v6_linkage_receipt(
    session: Any,
    wave: Any,
    outcomes: list[Any],
    ack: dict[str, Any],
    linkage_ack_digest: str,
    *,
    request: _V6LinkageReceiptRequest,
) -> dict[str, Any]:
    """Persist or replay the RSA receipt for one v6 linkage graph."""

    if (
        not isinstance(request.receipt_key_id, str)
        or request.receipt_key_id != wave.receipt_key_id
        or not isinstance(request.cutover_id, str)
        or request.cutover_id != ordinary_cutover_id(wave.wave_id)
    ):
        raise PTGWaveOutcomeConflict(
            "V12 linkage receipt request does not bind the stored key and cutover"
        )
    _validate_v6_outcome_snapshot(wave, outcomes)
    intents = (
        await session.execute(
            select(PTGImportWaveIntent)
            .where(PTGImportWaveIntent.wave_id == wave.wave_id)
            .order_by(PTGImportWaveIntent.ordinal)
            .with_for_update()
        )
    ).scalars().all()
    receipt, is_replay = _resolve_v6_linkage_receipt(
        wave,
        intents,
        ack,
        linkage_ack_digest,
        request,
    )
    if is_replay:
        return receipt
    wave.linkage_ack = ack
    wave.linkage_ack_digest = linkage_ack_digest
    wave.linkage_receipt = receipt
    wave.linkage_receipt_payload_digest = receipt["payload_digest"]
    parsed_issued_at = dt.datetime.strptime(
        receipt["issued_at"],
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ).replace(tzinfo=dt.UTC)
    wave.linkage_receipt_issued_at = parsed_issued_at
    await session.flush()
    return receipt


def _resolve_v6_linkage_receipt(
    wave: Any,
    intents: list[Any],
    ack: dict[str, Any],
    linkage_ack_digest: str,
    request: _V6LinkageReceiptRequest,
) -> tuple[dict[str, Any], bool]:
    """Build a first receipt or verify the exact persisted replay."""

    try:
        admission = admission_receipt_mapping(
            wave,
            intents,
        )
        receipt_payload = linkage_receipt_payload(
            admission,
            cutover_id=request.cutover_id,
            outcomes_digest=wave.outcomes_digest,
            mapping_digest=ack["mapping_digest"],
            linkage_ack_digest=linkage_ack_digest,
        )
        keyring = require_process_receipt_keyring(request.receipt_keyring)
        if wave.linkage_ack_digest is not None:
            if wave.linkage_ack_digest != linkage_ack_digest:
                raise PTGWaveOutcomeConflict(
                    "linkage acknowledgement conflicts with the first receipt"
                )
            if wave.linkage_receipt is None:
                raise PTGWaveOutcomeConflict(
                    "V12 linkage acknowledgement lacks its asymmetric receipt"
                )
            return (
                keyring.validate_stored_receipt(
                    wave.linkage_receipt,
                    schema=LINKAGE_RECEIPT_SCHEMA,
                    key_id=wave.receipt_key_id,
                    expected_payload=receipt_payload,
                ),
                True,
            )
        issued_at = request.receipt_issued_at or dt.datetime.now(dt.UTC)
        receipt = keyring.sign_receipt(
            schema=LINKAGE_RECEIPT_SCHEMA,
            key_id=wave.receipt_key_id,
            issued_at=issued_at,
            receipt_payload=receipt_payload,
        )
    except PTGWaveReceiptContractError as exc:
        raise PTGWaveOutcomeConflict(str(exc)) from exc
    return receipt, False


def _validate_v6_outcome_snapshot(wave: Any, outcomes: list[Any]) -> None:
    """Re-derive the durable all-N graph before granting RSA authority."""

    records = [
        {
            "ordinal": row.ordinal,
            "run_id": row.run_id,
            "job_id": row.job_id,
            "source_file_import_id": row.source_file_import_id,
            "content_version": row.content_version,
            "status": row.status,
            "snapshot_id": row.snapshot_id,
            "import_id": row.import_id,
        }
        for row in outcomes
    ]
    if any(
        getattr(row, "outcome_digest", None) != _record_digest(record)
        for row, record in zip(outcomes, records)
    ) or wave.outcomes_digest != _collection_digest(
        "healthporta.ptg-wave.outcomes.v1",
        records,
    ):
        raise PTGWaveOutcomeConflict(
            "V12 linkage receipt requires the exact persisted outcome graph"
        )


__all__ = [
    "PTGWaveOutcomeConflict", "get_wave_outcomes_page", "linkage_mapping_digest",
    "record_linkage_ack", "sign_linkage_ack", "snapshot_terminal_outcomes",
    "verify_terminal_eligibility",
]
