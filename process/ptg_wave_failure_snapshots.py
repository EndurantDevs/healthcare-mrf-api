"""Transactional all-N dead-letter reducers for exact-wave failures."""

from __future__ import annotations

from types import ModuleType
from typing import Any, Iterable

from sqlalchemy import bindparam, select, text

from db.migration_ptg2_legacy_v3_metadata_reconcile import EVENT_TABLE
from db.models import (
    ImportRun,
    PTGImportWaveClaim,
    PTGImportWaveIntent,
    PTGImportWaveOutcome,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_wave_failure_receipts import (
    _claimed_prestart_failure_receipt,
    _require_claimed_prestart_failure_receipt,
    _require_unclaimed_failure_receipt,
)
from process.ptg_wave_failure_persistence import (
    DeadLetterSnapshot,
    persist_dead_letter_snapshot,
)
from process.ptg_wave_failure_types import (
    PTGWaveFailureConflict,
    _rows_by_ordinal,
)
from process.ptg_wave_state import canonical_json, sha256_digest


async def snapshot_unclaimed_dead_letter_outcomes(
    facade: ModuleType,
    wave_id: str,
    *,
    failure_receipt: object,
) -> str:
    """Atomically dead-letter all N unclaimed runs and enter linkage wait."""

    async with facade.db.transaction() as session:
        wave = await facade._locked_wave(session, wave_id)
        receipt_digest = sha256_digest(canonical_json(failure_receipt))
        if wave.state == "awaiting_linkage":
            if (
                wave.failure_receipt_digest != receipt_digest
                or wave.outcomes_digest is None
            ):
                raise PTGWaveFailureConflict(
                    "failure linkage wait conflicts with its first receipt"
                )
            return wave.outcomes_digest
        if wave.state not in {
            "slots_waiting", "redis_releasing", "released", "executing"
        }:
            raise PTGWaveFailureConflict(
                "all-unclaimed failure outcomes are not expected for this wave"
            )
        receipt = _require_unclaimed_failure_receipt(
            wave, failure_receipt, require_origin_state=True
        )
        intents, runs = await _unclaimed_snapshot_rows(
            session, wave, wave_id
        )
        return await persist_dead_letter_snapshot(
            facade,
            DeadLetterSnapshot(
                session=session,
                wave=wave,
                wave_id=wave_id,
                intents=intents,
                runs=runs,
                receipt=receipt,
                receipt_digest=receipt_digest,
                is_claimed_prestart=False,
            ),
        )


async def _unclaimed_snapshot_rows(
    session: Any, wave: Any, wave_id: str
) -> tuple[list[Any], list[Any]]:
    claims = (
        await session.execute(
            select(PTGImportWaveClaim.ordinal)
            .where(PTGImportWaveClaim.wave_id == wave_id)
            .with_for_update()
        )
    ).scalars().all()
    if claims:
        raise PTGWaveFailureConflict(
            "a claimed wave cannot use the all-unclaimed failure path"
        )
    intents = _rows_by_ordinal(
        (
            await session.execute(
                select(PTGImportWaveIntent)
                .where(PTGImportWaveIntent.wave_id == wave_id)
                .order_by(PTGImportWaveIntent.ordinal)
                .with_for_update()
            )
        ).scalars().all()
    )
    if len(intents) != wave.intent_count:
        raise PTGWaveFailureConflict(
            "all-unclaimed failure requires every admitted intent"
        )
    runs = await _locked_wave_runs(session, wave_id)
    if len(runs) != wave.intent_count:
        raise PTGWaveFailureConflict(
            "all-unclaimed failure requires every admitted ImportRun"
        )
    if any(str(run.status or "") == "succeeded" for run in runs):
        raise PTGWaveFailureConflict(
            "a successful wave run cannot be converted to dead letter"
        )
    await _require_no_existing_outcomes(session, wave_id)
    return intents, runs


async def _locked_wave_runs(session: Any, wave_id: str) -> list[Any]:
    return (
        await session.execute(
            select(ImportRun)
            .join(
                PTGImportWaveIntent,
                ImportRun.run_id == PTGImportWaveIntent.run_id,
            )
            .where(PTGImportWaveIntent.wave_id == wave_id)
            .order_by(PTGImportWaveIntent.ordinal)
            .with_for_update()
        )
    ).scalars().all()


async def _require_no_existing_outcomes(session: Any, wave_id: str) -> None:
    existing_ordinals = (
        await session.execute(
            select(PTGImportWaveOutcome.ordinal)
            .where(PTGImportWaveOutcome.wave_id == wave_id)
            .with_for_update()
        )
    ).scalars().all()
    if existing_ordinals:
        raise PTGWaveFailureConflict(
            "immutable terminal outcomes already exist"
        )


def _started_claim_ordinals(
    wave: Any,
    intents: Iterable[Any],
    claims: Iterable[Any],
) -> list[int]:
    """Validate a zero-or-more subset of immutable exact started claims."""

    ordered_intents = _rows_by_ordinal(intents)
    intent_by_ordinal = {
        int(intent.ordinal): intent for intent in ordered_intents
    }
    ordered_claims = sorted(claims, key=lambda claim: int(claim.ordinal))
    claimed_ordinals = [claim.ordinal for claim in ordered_claims]
    if (
        len(ordered_intents) != wave.intent_count
        or any(
            not isinstance(ordinal, int) or isinstance(ordinal, bool)
            for ordinal in claimed_ordinals
        )
        or claimed_ordinals != sorted(set(claimed_ordinals))
        or any(
            ordinal not in intent_by_ordinal for ordinal in claimed_ordinals
        )
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart claims are not a canonical admitted subset"
        )
    ready_by_slot = _ready_slots_by_number(wave)
    for claim in ordered_claims:
        _validate_started_claim(
            wave,
            intent_by_ordinal[claim.ordinal],
            claim,
            ready_by_slot,
        )
    return claimed_ordinals


def _ready_slots_by_number(wave: Any) -> dict[int, dict[str, Any]]:
    ready_slots = (wave.kubernetes_ready_attestation or {}).get("slots")
    if not isinstance(ready_slots, list) or len(ready_slots) != 12:
        raise PTGWaveFailureConflict(
            "claimed-prestart claims lack the original 12-slot receipt"
        )
    ready_by_slot = {
        entry.get("slot"): entry
        for entry in ready_slots
        if isinstance(entry, dict) and isinstance(entry.get("slot"), int)
    }
    if set(ready_by_slot) != set(range(12)):
        raise PTGWaveFailureConflict(
            "claimed-prestart claims lack the original 12-slot receipt"
        )
    return ready_by_slot


def _validate_started_claim(
    wave: Any,
    intent: Any,
    claim: Any,
    ready_by_slot: dict[int, dict[str, Any]],
) -> None:
    slot = getattr(claim, "slot", None)
    ready_slot = ready_by_slot.get(slot)
    claim_token = getattr(claim, "claim_attempt_token", None)
    if (
        claim.wave_id != wave.wave_id
        or claim.run_id != intent.run_id
        or claim.job_id != intent.job_id
        or claim.claim_status != "started"
        or claim.failure_code is not None
        or claim.kubernetes_job_uid != wave.kubernetes_job_uid
        or claim.manifest_identity != wave.kubernetes_manifest_identity
        or claim.pinned_image_reference != wave.pinned_image_reference
        or claim.pinned_image_digest != wave.pinned_image_digest
        or claim.runtime_image_identity != wave.runtime_image_identity
        or claim.config_identity != wave.kubernetes_config_identity
        or ready_slot is None
        or ready_slot.get("pod_uid") != claim.pod_uid
        or ready_slot.get("runtime_image_identity")
        != claim.runtime_image_identity
        or not isinstance(claim_token, str)
        or len(claim_token) != 32
        or any(
            character not in "0123456789abcdef"
            for character in claim_token
        )
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart claim differs from its admitted execution identity"
        )


def _is_prestart_run_pristine(wave: Any, intent: Any, run: Any) -> bool:
    """Require the admission projection and no execution-attempt marker."""

    expected_progress_map = {
        "unit": "run",
        "total": 1,
        "done": 0,
        "pct": 0,
        "message": "wave admitted; controller materialization pending",
    }
    expected_metrics_map = {
        "wave_id": wave.wave_id,
        "queue": wave.release_queue,
        "base_queue": wave.queue,
        "worker_class": wave.worker_class,
        "resource_class": wave.resource_class,
        "worker_limit": wave.worker_limit,
        "job_id": intent.job_id,
        "ordinal": intent.ordinal,
        "wave_digest": wave.wave_digest,
    }
    return (
        run.run_id == intent.run_id
        and run.importer == "ptg"
        and run.source_file_import_id == intent.source_file_import_id
        and run.import_id == intent.source_file_import_id
        and str(run.status or "") == "queued"
        and run.phase_detail
        == "wave admitted; controller materialization pending"
        and run.started_at is None
        and run.finished_at is None
        and run.snapshot_id is None
        and run.error is None
        and getattr(run, "progress", None) == expected_progress_map
        and getattr(run, "metrics", None) == expected_metrics_map
    )


async def _worker_start_event_ordinals(
    session: Any,
    intents: Iterable[Any],
) -> list[int]:
    """Read execution markers only after corresponding runs are locked."""

    intent_rows = list(intents)
    ordinal_by_run_id = {
        intent.run_id: int(intent.ordinal) for intent in intent_rows
    }
    event_table = (
        f'{_quote_ident(resolve_ptg2_schema())}.{_quote_ident(EVENT_TABLE)}'
    )
    statement = text(
        f"""
        SELECT outer_run_id
          FROM {event_table}
         WHERE event_kind = 'worker_start_admitted'
           AND outer_run_id IN :run_ids
         ORDER BY outer_run_id
        """
    ).bindparams(bindparam("run_ids", expanding=True))
    event_rows = (
        await session.execute(
            statement, {"run_ids": list(ordinal_by_run_id)}
        )
    ).all()
    try:
        event_ordinals = [
            ordinal_by_run_id[
                event_row._mapping["outer_run_id"]
                if hasattr(event_row, "_mapping")
                else event_row[0]
            ]
            for event_row in event_rows
        ]
    except (KeyError, TypeError) as exc:
        raise PTGWaveFailureConflict(
            "claimed-prestart execution marker observation is invalid"
        ) from exc
    return sorted(set(event_ordinals))


async def snapshot_claimed_prestart_dead_letter_outcomes(
    facade: ModuleType,
    wave_id: str,
    *,
    kubernetes_evidence: object,
    redis_evidence: object,
) -> str:
    """Atomically close the claim-commit/import-start crash boundary."""

    async with facade.db.transaction() as session:
        wave = await facade._locked_wave(session, wave_id)
        if wave.state == "awaiting_linkage":
            return _existing_claimed_outcomes_digest(
                wave, kubernetes_evidence, redis_evidence
            )
        if wave.state not in {"released", "executing"}:
            raise PTGWaveFailureConflict(
                "claimed-prestart outcomes are not expected for this wave"
            )
        intents, runs, claimed_ordinals = await _claimed_snapshot_rows(
            session, wave, wave_id
        )
        receipt = _claimed_prestart_failure_receipt(
            wave,
            claimed_ordinals=claimed_ordinals,
            kubernetes_evidence=kubernetes_evidence,
            redis_evidence=redis_evidence,
        )
        receipt_digest = sha256_digest(canonical_json(receipt))
        return await persist_dead_letter_snapshot(
            facade,
            DeadLetterSnapshot(
                session=session,
                wave=wave,
                wave_id=wave_id,
                intents=intents,
                runs=runs,
                receipt=receipt,
                receipt_digest=receipt_digest,
                is_claimed_prestart=True,
            ),
        )


def _existing_claimed_outcomes_digest(
    wave: Any, kubernetes_evidence: object, redis_evidence: object
) -> str:
    receipt = _require_claimed_prestart_failure_receipt(
        wave, wave.failure_receipt, require_origin_state=False
    )
    if (
        receipt["kubernetes_evidence"] != kubernetes_evidence
        or receipt["redis_evidence"] != redis_evidence
        or wave.outcomes_digest is None
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart linkage wait conflicts with its first receipt"
        )
    return wave.outcomes_digest


async def _claimed_snapshot_rows(
    session: Any, wave: Any, wave_id: str
) -> tuple[list[Any], list[Any], list[int]]:
    intents = _rows_by_ordinal(
        (
            await session.execute(
                select(PTGImportWaveIntent)
                .where(PTGImportWaveIntent.wave_id == wave_id)
                .order_by(PTGImportWaveIntent.ordinal)
                .with_for_update()
            )
        ).scalars().all()
    )
    if len(intents) != wave.intent_count:
        raise PTGWaveFailureConflict(
            "claimed-prestart failure requires every admitted intent"
        )
    claims = (
        await session.execute(
            select(PTGImportWaveClaim)
            .where(PTGImportWaveClaim.wave_id == wave_id)
            .order_by(PTGImportWaveClaim.ordinal)
            .with_for_update()
        )
    ).scalars().all()
    claimed_ordinals = _started_claim_ordinals(wave, intents, claims)
    if (claimed_ordinals and wave.state != "executing") or (
        not claimed_ordinals and wave.state != "released"
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart claims conflict with durable wave execution state"
        )
    runs = await _locked_wave_runs(session, wave_id)
    if len(runs) != wave.intent_count or any(
        not _is_prestart_run_pristine(wave, intent, run)
        for intent, run in zip(intents, runs)
    ):
        raise PTGWaveFailureConflict(
            "claimed-prestart failure found a started, progressed, or terminal ImportRun"
        )
    if await _worker_start_event_ordinals(session, intents):
        raise PTGWaveFailureConflict(
            "claimed-prestart failure found a worker execution marker"
        )
    await _require_no_existing_outcomes(session, wave_id)
    return intents, runs, claimed_ordinals
