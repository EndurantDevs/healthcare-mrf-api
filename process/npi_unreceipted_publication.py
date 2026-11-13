# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish an NPI import that carries no public-evidence receipt."""

import json

from process.npi_canonical_publication import publication_error
from process.npi_canonical_publication_store import (
    NpiCanonicalPublicationCommit,
    _database_timestamp,
    _qualified,
)
from process.npi_result_generation import publish_unreceipted_npi_result_generation


async def commit_unreceipted_npi_publication(
    connection: object,
    *,
    schema: str,
    attempt: tuple[str, str, str],
    progress_by_name: dict[str, object],
    metrics_by_name: dict[str, object],
) -> NpiCanonicalPublicationCommit:
    """Bind the rotated tables and mark the exact control attempt terminal without a receipt."""

    run_id, attempt_id, attempt_started_at = attempt
    await publish_unreceipted_npi_result_generation(connection, schema_name=schema)
    committed_progress_by_name = {
        **progress_by_name,
        "attempt_id": attempt_id,
        "attempt_started_at": attempt_started_at,
    }
    try:
        # The receipted path's terminal update, with no snapshot reference.
        updated_run = await connection.fetchrow(
            f"UPDATE {_qualified(schema, 'import_run')} "
            "SET status='succeeded', phase_detail='npi published', "
            "heartbeat_at=transaction_timestamp() AT TIME ZONE 'UTC', "
            "finished_at=transaction_timestamp() AT TIME ZONE 'UTC', "
            "progress=$4::json, metrics=$5::json, error=NULL, snapshot_id=NULL "
            "WHERE run_id=$1 AND importer='npi' AND status='running' "
            "AND progress->>'attempt_id'=$2 "
            "AND progress->>'attempt_started_at'=$3 "
            "RETURNING heartbeat_at, finished_at",
            run_id,
            attempt_id,
            attempt_started_at,
            json.dumps(committed_progress_by_name, ensure_ascii=True, separators=(",", ":")),
            json.dumps(metrics_by_name, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        )
    except Exception:
        raise publication_error() from None
    if updated_run is None:
        raise publication_error()
    try:
        heartbeat_at = _database_timestamp(updated_run["heartbeat_at"])
        finished_at = _database_timestamp(updated_run["finished_at"])
    except KeyError, TypeError:
        raise publication_error() from None
    return NpiCanonicalPublicationCommit(None, heartbeat_at, finished_at)


async def load_unreceipted_npi_commit(
    connection: object,
    *,
    schema: str,
    attempt: tuple[str, str, str],
    progress_by_name: dict[str, object],
    metrics_by_name: dict[str, object],
) -> NpiCanonicalPublicationCommit | None:
    """Load the exact attempt's terminal row, written with the table swap, after an uncertain exit."""

    run_id, attempt_id, attempt_started_at = attempt
    expected_progress_by_name = {
        **progress_by_name,
        "attempt_id": attempt_id,
        "attempt_started_at": attempt_started_at,
    }
    try:
        # A later publication may already own the serving generation; the terminal row alone proves this one.
        committed_run = await connection.fetchrow(
            f"SELECT run.heartbeat_at, run.finished_at FROM {_qualified(schema, 'import_run')} AS run "
            "WHERE run.run_id=$1 AND run.importer='npi' AND run.status='succeeded' "
            "AND run.phase_detail='npi published' AND run.snapshot_id IS NULL "
            "AND run.progress::jsonb=$2::jsonb AND run.metrics::jsonb=$3::jsonb",
            run_id,
            json.dumps(expected_progress_by_name, ensure_ascii=True, separators=(",", ":")),
            json.dumps(metrics_by_name, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        )
    except Exception:
        raise publication_error() from None
    if committed_run is None:
        return None
    try:
        heartbeat_at = _database_timestamp(committed_run["heartbeat_at"])
        finished_at = _database_timestamp(committed_run["finished_at"])
    except KeyError, TypeError:
        raise publication_error() from None
    return NpiCanonicalPublicationCommit(None, heartbeat_at, finished_at)
