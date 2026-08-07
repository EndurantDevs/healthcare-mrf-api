"""Stable exact-wave outcomes, linkage receipts, and terminal proof checks.

This module never contacts an external service.  It snapshots terminal
``ImportRun`` records into immutable per-intent rows, so pagination supplied to
the upstream reconciler cannot change beneath a cursor.
"""

from __future__ import annotations

import datetime as dt
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
    linkage_mapping_digest,
    sign_linkage_ack,
)
from process.ptg_wave_outcome_terminal_validation import (
    verify_terminal_eligibility,
)


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
    wave_id: str, ack: object, *, key: str | bytes | None = None,
) -> str:
    """Verify and persist the signed all-N source-linkage acknowledgement."""

    async with db.transaction() as session:
        result = await session.execute(
            select(PTGImportWave).where(PTGImportWave.wave_id == wave_id).with_for_update()
        )
        wave = result.scalar_one_or_none()
        if wave is None or wave.state != "awaiting_linkage" or wave.outcomes_digest is None:
            raise PTGWaveOutcomeConflict("linkage acknowledgement is not expected for this wave")
        outcomes = (await session.execute(
            select(PTGImportWaveOutcome).where(PTGImportWaveOutcome.wave_id == wave_id)
            .order_by(PTGImportWaveOutcome.ordinal).with_for_update()
        )).scalars().all()
        if len(outcomes) != wave.intent_count:
            raise PTGWaveOutcomeConflict("linkage acknowledgement requires every stable terminal outcome")
        ack, digest = _validate_linkage_ack(wave, outcomes, ack, key)
        if wave.linkage_ack_digest is not None:
            if wave.linkage_ack_digest != digest:
                raise PTGWaveOutcomeConflict("linkage acknowledgement conflicts with the first receipt")
            return digest
        wave.linkage_ack = ack
        wave.linkage_ack_digest = digest
        await session.flush()
        return digest


__all__ = [
    "PTGWaveOutcomeConflict", "get_wave_outcomes_page", "linkage_mapping_digest",
    "record_linkage_ack", "sign_linkage_ack", "snapshot_terminal_outcomes",
    "verify_terminal_eligibility",
]
