"""Exact database evidence for pristine V12 wave abandonment."""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import Any

from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_state import canonical_json, sha256_digest


MEMBER_ROWS_DIGEST_DOMAIN = (
    "healthporta.ptg-wave.v12-pristine-member-rows.v1"
)
INTENT_ROWS_DIGEST_DOMAIN = (
    "healthporta.ptg-wave.v12-pristine-intent-rows.v1"
)
RUN_ROWS_DIGEST_DOMAIN = "healthporta.ptg-wave.v12-pristine-run-rows.v1"

_RUN_EVIDENCE_FIELDS = (
    "run_id",
    "engine",
    "node_id",
    "importer",
    "family",
    "status",
    "phase_detail",
    "params",
    "idempotency_key",
    "triggered_by",
    "schedule_id",
    "subscription_id",
    "source_file_import_id",
    "created_at",
    "started_at",
    "finished_at",
    "heartbeat_at",
    "progress",
    "metrics",
    "error",
    "snapshot_id",
    "import_id",
    "retry_of_run_id",
)


def exact_pristine_database_proof(
    wave: Any,
    intents: Sequence[Any],
    runs: Sequence[Any],
    claims: Sequence[Any],
    outcomes: Sequence[Any],
    worker_start_events: Sequence[Any],
) -> dict[str, Any]:
    """Rebuild the exact pristine database-state evidence."""

    ordered_intents, ordered_runs = _validated_pristine_run_rows(
        wave,
        intents,
        runs,
    )
    member_records, intent_records, run_records = (
        _pristine_database_evidence_rows(ordered_intents, ordered_runs)
    )
    intent_count = len(ordered_intents)
    return {
        "state": "slots_waiting",
        "intent_count": intent_count,
        "run_count": len(ordered_runs),
        "pristine_run_count": len(ordered_runs),
        "unassigned_run_count": sum(run.node_id is None for run in ordered_runs),
        "claim_count": len(claims),
        "outcome_count": len(outcomes),
        "worker_start_event_count": len(worker_start_events),
        "member_rows_digest": _collection_digest(
            MEMBER_ROWS_DIGEST_DOMAIN,
            member_records,
        ),
        "intent_rows_digest": _collection_digest(
            INTENT_ROWS_DIGEST_DOMAIN,
            intent_records,
        ),
        "run_rows_digest": _collection_digest(
            RUN_ROWS_DIGEST_DOMAIN,
            run_records,
        ),
    }


def _validated_pristine_run_rows(
    wave: Any,
    intents: Sequence[Any],
    runs: Sequence[Any],
) -> tuple[list[Any], list[Any]]:
    ordered_intents = sorted(intents, key=lambda intent: int(intent.ordinal))
    runs_by_id = {run.run_id: run for run in runs}
    expected_runs = expected_pristine_run_values(wave)
    if set(runs_by_id) != set(expected_runs):
        raise PTGWaveMaterializedPreclaimConflict(
            "fresh V12 ImportRun membership is invalid"
        )
    ordered_runs: list[Any] = []
    for intent in ordered_intents:
        run = runs_by_id[intent.run_id]
        expected = expected_runs[intent.run_id]
        if any(
            _comparable(getattr(run, field_name, None))
            != _comparable(expected_value)
            for field_name, expected_value in expected.items()
        ):
            raise PTGWaveMaterializedPreclaimConflict(
                "fresh V12 ImportRun is not an exact pristine admission"
            )
        ordered_runs.append(run)
    return ordered_intents, ordered_runs


def _pristine_database_evidence_rows(
    ordered_intents: Sequence[Any],
    ordered_runs: Sequence[Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    member_records = [
        {
            "ordinal": intent.ordinal,
            "source_file_import_id": intent.source_file_import_id,
            "content_version": intent.content_version,
        }
        for intent in ordered_intents
    ]
    intent_records = [
        {
            "wave_id": intent.wave_id,
            "ordinal": intent.ordinal,
            "run_id": intent.run_id,
            "source_file_import_id": intent.source_file_import_id,
            "content_version": intent.content_version,
            "run_idempotency_key": intent.run_idempotency_key,
            "job_id": intent.job_id,
            "params_digest": sha256_digest(canonical_json(intent.params)),
            "job_payload_digest": sha256_digest(canonical_json(intent.job_payload)),
            "serialized_job_digest": intent.serialized_job_digest,
        }
        for intent in ordered_intents
    ]
    run_records = [
        {
            field_name: _comparable(getattr(run, field_name, None))
            for field_name in _RUN_EVIDENCE_FIELDS
        }
        for run in ordered_runs
    ]
    return member_records, intent_records, run_records


def expected_pristine_run_values(wave: Any) -> dict[str, dict[str, Any]]:
    """Rebuild the exact pristine ImportRun values from stored admission."""

    from api.control_import_waves import (
        _prepare_wave_intents,
        _validate_signed_intents,
    )

    signed = _validate_signed_intents(
        wave.cohort_attestation["intents"],
        wave_id=wave.wave_id,
    )
    prepared, _jobs_digest, _manifest_digest = _prepare_wave_intents(
        {
            "wave_id": wave.wave_id,
            "request_digest": wave.request_digest,
            "wave_digest": wave.wave_digest,
            "release_queue": wave.release_queue,
            "intents": signed,
        },
        now=wave.created_at,
        enqueue_time_ms=wave.enqueue_time_ms,
    )
    values_by_run_id: dict[str, dict[str, Any]] = {}
    for prepared_item in prepared:
        run_values_by_field = dict(prepared_item["run_values"])
        run_values_by_field.update({"started_at": None, "finished_at": None})
        values_by_run_id[prepared_item["run_id"]] = run_values_by_field
    return values_by_run_id


def _collection_digest(domain: str, records: list[dict[str, Any]]) -> str:
    return sha256_digest(
        domain.encode("ascii") + b"\0" + canonical_json(records)
    )


def _comparable(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return value.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return value
