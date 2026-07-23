"""Frozen V3 rollback and source-equivalence database evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import asyncpg

from process.ptg_parts.ptg2_shared_source_set import (
    shared_source_set_metadata,
)


@dataclass(frozen=True)
class ReferenceEquivalenceScope:
    """Snapshot and rollback identities bound by the V4 canary."""

    v4_snapshot_id: str
    reference_snapshot_id: str
    rollback_owner_id: str


async def collect_reference_equivalence(
    connection: asyncpg.Connection,
    schema_name: str,
    scope: ReferenceEquivalenceScope,
) -> dict[str, Any]:
    """Prove the frozen V3 oracle is pinned to the exact V4 raw sources."""

    if (
        not scope.reference_snapshot_id.strip()
        or not scope.rollback_owner_id.strip()
    ):
        raise RuntimeError(
            "V3 reference snapshot and rollback owner are required"
        )
    schema = _quote_identifier(schema_name)
    reference_record = await _reference_snapshot_record(
        connection,
        schema_name,
        schema,
        scope.reference_snapshot_id,
    )
    pin_record = await _rollback_pin_record(
        connection,
        schema,
        scope,
    )
    source_records = await _source_identity_records(
        connection,
        schema,
        scope,
    )
    raw_sources_by_snapshot, trace_sets_by_snapshot = (
        _source_identities_by_snapshot(source_records, scope)
    )
    return _reference_report(
        scope,
        reference_record,
        pin_record,
        raw_sources_by_snapshot,
        trace_sets_by_snapshot,
    )


async def _reference_snapshot_record(
    connection: asyncpg.Connection,
    schema_name: str,
    schema: str,
    reference_snapshot_id: str,
) -> Mapping[str, Any]:
    snapshot_record = await connection.fetchrow(
        f"""
        SELECT snapshot.snapshot_id,
               snapshot.status AS snapshot_status,
               binding.snapshot_key,
               layout.state AS layout_state,
               layout.generation AS layout_generation,
               (
                   SELECT COUNT(*)::bigint
                     FROM {schema}.ptg2_v3_snapshot_block AS locator
                    WHERE locator.snapshot_key = binding.snapshot_key
               ) AS snapshot_block_count,
               EXISTS (
                   SELECT 1
                     FROM pg_constraint AS constraint_row
                    WHERE constraint_row.conrelid =
                          '{schema_name}.ptg2_v3_snapshot_block'::regclass
                      AND constraint_row.confrelid =
                          '{schema_name}.ptg2_v3_block'::regclass
                      AND constraint_row.contype = 'f'
                      AND constraint_row.convalidated
               ) AS snapshot_block_foreign_key_validated
          FROM {schema}.ptg2_snapshot AS snapshot
          JOIN {schema}.ptg2_v3_snapshot_binding AS binding
            ON binding.snapshot_id = snapshot.snapshot_id
          JOIN {schema}.ptg2_v3_snapshot_layout AS layout
            ON layout.snapshot_key = binding.snapshot_key
         WHERE snapshot.snapshot_id = $1
        """,
        reference_snapshot_id,
    )
    if snapshot_record is None:
        raise RuntimeError("frozen V3 reference snapshot was not found")
    return snapshot_record


async def _rollback_pin_record(
    connection: asyncpg.Connection,
    schema: str,
    scope: ReferenceEquivalenceScope,
) -> Mapping[str, Any] | None:
    return await connection.fetchrow(
        f"""
        SELECT owner_type, owner_id, snapshot_id, reason, created_at
          FROM {schema}.ptg2_snapshot_pin
         WHERE owner_type = 'ptg_v4_rollback'
           AND owner_id = $1
           AND snapshot_id = $2
        """,
        scope.rollback_owner_id,
        scope.reference_snapshot_id,
    )


async def _source_identity_records(
    connection: asyncpg.Connection,
    schema: str,
    scope: ReferenceEquivalenceScope,
) -> Sequence[Mapping[str, Any]]:
    return await connection.fetch(
        f"""
        SELECT snapshot_id, raw_container_sha256, source_trace_set_hash
          FROM {schema}.ptg2_v3_snapshot_source
         WHERE snapshot_id = ANY($1::varchar[])
         ORDER BY snapshot_id, raw_container_sha256, source_trace_set_hash
        """,
        [scope.v4_snapshot_id, scope.reference_snapshot_id],
    )


def _source_identities_by_snapshot(
    source_records: Sequence[Mapping[str, Any]],
    scope: ReferenceEquivalenceScope,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    raw_sources_by_snapshot = {
        scope.v4_snapshot_id: [],
        scope.reference_snapshot_id: [],
    }
    trace_sets_by_snapshot = {
        scope.v4_snapshot_id: [],
        scope.reference_snapshot_id: [],
    }
    for source_record in source_records:
        source_fields_by_name = dict(source_record)
        row_snapshot_id = str(source_fields_by_name["snapshot_id"])
        if row_snapshot_id not in raw_sources_by_snapshot:
            continue
        raw_sources_by_snapshot[row_snapshot_id].append(
            str(source_fields_by_name["raw_container_sha256"])
        )
        trace_sets_by_snapshot[row_snapshot_id].append(
            str(source_fields_by_name["source_trace_set_hash"])
        )
    if not all(raw_sources_by_snapshot.values()):
        raise RuntimeError("V3 or V4 source identity rows are missing")
    return raw_sources_by_snapshot, trace_sets_by_snapshot


def _reference_report(
    scope: ReferenceEquivalenceScope,
    reference_record: Mapping[str, Any],
    pin_record: Mapping[str, Any] | None,
    raw_sources_by_snapshot: Mapping[str, list[str]],
    trace_sets_by_snapshot: Mapping[str, list[str]],
) -> dict[str, Any]:
    v4_sources = raw_sources_by_snapshot[scope.v4_snapshot_id]
    reference_sources = raw_sources_by_snapshot[scope.reference_snapshot_id]
    return _json_safe(
        {
            "v4_snapshot_id": scope.v4_snapshot_id,
            "reference_snapshot_id": scope.reference_snapshot_id,
            "rollback_owner_id": scope.rollback_owner_id,
            "same_raw_sources": v4_sources == reference_sources,
            "same_source_trace_sets": (
                trace_sets_by_snapshot[scope.v4_snapshot_id]
                == trace_sets_by_snapshot[scope.reference_snapshot_id]
            ),
            "v4_source_set": shared_source_set_metadata(v4_sources),
            "reference_source_set": shared_source_set_metadata(
                reference_sources
            ),
            "reference_snapshot": dict(reference_record),
            "rollback_pin": (
                dict(pin_record) if pin_record is not None else None
            ),
        }
    )


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace(
            "+00:00",
            "Z",
        )
    if isinstance(value, dict):
        return {
            field_name: _json_safe(field_value)
            for field_name, field_value in value.items()
        }
    return value


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
