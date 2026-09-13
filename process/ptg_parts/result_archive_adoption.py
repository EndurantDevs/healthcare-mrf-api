# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare one restored, isolated PTG V4 layout for a local candidate.

The staging schema is a local, short-lived schema populated by the archive
restore worker.  It is not a peer connection: this module only reads it through
the caller's existing destination transaction.  It deliberately does not copy
logical snapshots, source pins, current pointers, frozen-source bindings, or
candidate attestations.  The caller creates the destination candidate from its
own evidence, then obtains a fresh local attestation before activation.

``staging_schema_name`` is a syntactic identifier boundary, not an ownership
claim.  The caller must establish that the schema was created for this restore
and that its archive receipt names the selected closure before calling this
function.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    _initialize_v4_snapshot_map_root,
    bind_snapshot_to_v4_layout,
    reserve_v4_shared_layout,
    seal_v4_shared_layout,
    summarize_persisted_v4_snapshot_maps,
)

RESULT_ARCHIVE_ADOPTION_CONTRACT = "ptg_result_archive_adoption_v1"
_MAX_STAGED_BLOCK_ROWS = 500_000
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

# These are precisely the snapshot-key-scoped physical relations selected by
# result_archive_closure.  Layout identity, logical snapshot rows, source
# evidence, pins, and attestations are intentionally absent.
_REKEYED_TABLES = (
    "ptg2_v3_code",
    "ptg2_v3_provider_set",
    "ptg2_v3_snapshot_block",
    "ptg2_v4_snapshot_map_pack",
    "ptg2_v4_finalizer_map_root",
    "ptg2_v4_finalizer_map_pack",
    "ptg2_v4_finalizer_map_target",
    "ptg2_v4_npi_scope",
    "ptg2_v3_provider_group",
    "ptg2_v4_provider_component",
    "ptg2_v4_pattern",
    "ptg2_v4_relation_manifest",
    "ptg2_v4_heavy_owner",
    "ptg2_v4_provider_set_npi_prefix",
    "ptg2_v4_provider_graph_diagnostic",
    "ptg2_v4_inferred_taxonomy_candidate",
    "ptg2_v3_source_audit_witness",
    "ptg2_v3_source_audit_witness_part",
    "ptg2_provider_tax_identity_manifest",
    "ptg2_provider_tax_identity",
    "ptg2_provider_group_tax_identity",
    "ptg2_provider_tax_identity_source_manifest",
    "ptg2_provider_tax_identity_source_binding",
    "ptg2_provider_group_tax_identity_source",
)
_FINALIZER_MAP_TABLES = (
    "ptg2_v4_finalizer_map_root",
    "ptg2_v4_finalizer_map_pack",
    "ptg2_v4_finalizer_map_target",
)


class ResultArchiveAdoptionError(RuntimeError):
    """The restored staging closure cannot become a destination layout."""


@dataclass(frozen=True)
class PreparedResultArchiveLayout:
    """A locally sealed layout bound to a locally owned logical candidate."""

    contract: str
    destination_snapshot_id: str
    destination_snapshot_key: int
    source_snapshot_key: int
    mapping_digest: bytes
    requires_fresh_destination_attestation: bool = True


@dataclass(frozen=True)
class _NewLayoutPreparation:
    schema_name: str
    staging_schema_name: str
    source_snapshot_key: int
    destination_snapshot_key: int
    build_token: str
    support_digest: bytes
    layout_manifest: Mapping[str, Any]
    max_staged_block_rows: int


def _safe_identifier(value: str, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a simple PostgreSQL identifier")
    return normalized


def _required_snapshot_id(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 160:
        raise ValueError("destination_snapshot_id is required")
    return normalized


def _mapping(row: Any) -> dict[str, Any]:
    return dict(getattr(row, "_mapping", row) or {})


async def _one(session: Any, statement: str, parameters: Mapping[str, Any], label: str) -> dict[str, Any]:
    result = await session.execute(text(statement), dict(parameters))
    rows = [_mapping(row) for row in result]
    if len(rows) != 1:
        raise ResultArchiveAdoptionError(f"archive adoption {label} is missing or ambiguous")
    return rows[0]


async def _one_or_none(
    session: Any,
    statement: str,
    parameters: Mapping[str, Any],
    label: str,
) -> dict[str, Any] | None:
    """Load one optional guarded row while rejecting a malformed duplicate."""

    result = await session.execute(text(statement), dict(parameters))
    rows = [_mapping(row) for row in result]
    if len(rows) > 1:
        raise ResultArchiveAdoptionError(f"archive adoption {label} is ambiguous")
    return rows[0] if rows else None


async def _staged_layout(session: Any, *, staging_schema: str, source_snapshot_key: int) -> dict[str, Any]:
    return await _one(
        session,
        f"""
        SELECT layout.snapshot_key, layout.generation, layout.state,
               layout.support_digest, layout.layout_manifest,
               fingerprint.semantic_fingerprint,
               map_root.state AS map_root_state
          FROM {_quote_ident(staging_schema)}.ptg2_v3_snapshot_layout AS layout
          JOIN {_quote_ident(staging_schema)}.ptg2_v3_layout_fingerprint AS fingerprint
            ON fingerprint.snapshot_key = layout.snapshot_key
          JOIN {_quote_ident(staging_schema)}.ptg2_v4_snapshot_map_root AS map_root
            ON map_root.snapshot_key = layout.snapshot_key
         WHERE layout.snapshot_key = :source_snapshot_key
         FOR KEY SHARE OF layout, fingerprint, map_root
        """,
        {"source_snapshot_key": int(source_snapshot_key)},
        "sealed staging layout",
    )


def _validated_staged_layout(layout: Mapping[str, Any]) -> tuple[bytes, bytes, Mapping[str, Any]]:
    if layout.get("generation") != PTG2_V4_SHARED_GENERATION:
        raise ResultArchiveAdoptionError("archive adoption only supports V4 staging layouts")
    if layout.get("state") != "sealed" or layout.get("map_root_state") != "complete":
        raise ResultArchiveAdoptionError("archive adoption requires a sealed complete staging layout")
    fingerprint = bytes(layout.get("semantic_fingerprint") or b"")
    support_digest = bytes(layout.get("support_digest") or b"")
    manifest = layout.get("layout_manifest")
    if len(fingerprint) != 32 or len(support_digest) != 32 or not isinstance(manifest, Mapping):
        raise ResultArchiveAdoptionError("archive adoption staging layout metadata is invalid")
    return fingerprint, support_digest, manifest


async def _assert_destination_snapshot(session: Any, *, schema: str, snapshot_id: str) -> None:
    await _one(
        session,
        f"""
        SELECT snapshot_id
          FROM {schema}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
         FOR UPDATE
        """,
        {"snapshot_id": snapshot_id},
        "destination logical snapshot",
    )


async def _assert_remapped_logical_identity(
    session: Any, *, staging_schema: str, source_snapshot_key: int, destination_snapshot_id: str
) -> None:
    staged_binding = await _one(
        session,
        f"""
        SELECT snapshot_id
          FROM {_quote_ident(staging_schema)}.ptg2_v3_snapshot_binding
         WHERE snapshot_key = :source_snapshot_key
        """,
        {"source_snapshot_key": int(source_snapshot_key)},
        "staging logical binding",
    )
    if str(staged_binding["snapshot_id"]) == destination_snapshot_id:
        raise ResultArchiveAdoptionError("archive adoption requires a remapped destination snapshot ID")


async def _table_columns(session: Any, *, schema_name: str, table_name: str) -> tuple[str, ...]:
    result = await session.execute(
        text(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = :schema_name AND table_name = :table_name
             ORDER BY ordinal_position
            """
        ),
        {"schema_name": schema_name, "table_name": table_name},
    )
    columns = tuple(str(row[0]) for row in result)
    if not columns or "snapshot_key" not in columns or len(set(columns)) != len(columns):
        raise ResultArchiveAdoptionError(f"archive adoption {table_name} has an unsupported key shape")
    return columns


async def _copy_rekeyed_table(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    table_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    destination_columns = await _table_columns(session, schema_name=schema_name, table_name=table_name)
    staging_columns = await _table_columns(session, schema_name=staging_schema_name, table_name=table_name)
    if destination_columns != staging_columns:
        raise ResultArchiveAdoptionError(f"archive adoption {table_name} column contract differs from staging")
    schema = _quote_ident(schema_name)
    staging = _quote_ident(staging_schema_name)
    table = _quote_ident(table_name)
    quoted_columns = ", ".join(_quote_ident(column) for column in destination_columns)
    non_key_columns = tuple(column for column in destination_columns if column != "snapshot_key")
    copied_expression = ", ".join(
        ":destination_snapshot_key" if column == "snapshot_key" else _quote_ident(column)
        for column in destination_columns
    )
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.{table} ({quoted_columns})
            SELECT {copied_expression}
              FROM {staging}.{table}
             WHERE snapshot_key = :source_snapshot_key
            ON CONFLICT DO NOTHING
            """
        ),
        {"source_snapshot_key": int(source_snapshot_key), "destination_snapshot_key": int(destination_snapshot_key)},
    )
    await _assert_rekeyed_table_matches(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        table_name=table_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
        non_key_columns=non_key_columns,
    )


async def _assert_rekeyed_table_matches(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    table_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
    non_key_columns: tuple[str, ...] | None = None,
) -> None:
    """Reject a rekeyed relation unless its exact source and local rows agree."""

    destination_columns = await _table_columns(session, schema_name=schema_name, table_name=table_name)
    staging_columns = await _table_columns(session, schema_name=staging_schema_name, table_name=table_name)
    if destination_columns != staging_columns:
        raise ResultArchiveAdoptionError(f"archive adoption {table_name} column contract differs from staging")
    compared_columns = non_key_columns or tuple(column for column in destination_columns if column != "snapshot_key")
    schema = _quote_ident(schema_name)
    staging = _quote_ident(staging_schema_name)
    table = _quote_ident(table_name)
    expected_columns = ", ".join(_quote_ident(column) for column in compared_columns) or "1"
    difference_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                (SELECT {expected_columns} FROM {staging}.{table}
                  WHERE snapshot_key = :source_snapshot_key)
                EXCEPT ALL
                (SELECT {expected_columns} FROM {schema}.{table}
                  WHERE snapshot_key = :destination_snapshot_key)
            ) OR EXISTS (
                (SELECT {expected_columns} FROM {schema}.{table}
                  WHERE snapshot_key = :destination_snapshot_key)
                EXCEPT ALL
                (SELECT {expected_columns} FROM {staging}.{table}
                  WHERE snapshot_key = :source_snapshot_key)
            ) AS differs
            """
        ),
        {"source_snapshot_key": int(source_snapshot_key), "destination_snapshot_key": int(destination_snapshot_key)},
    )
    if bool(difference_result.scalar()):
        raise ResultArchiveAdoptionError(f"archive adoption {table_name} conflicts with destination rows")


async def _validate_staged_blocks(
    session: Any, *, schema_name: str, staging_schema_name: str, max_staged_block_rows: int
) -> None:
    staging = _quote_ident(staging_schema_name)
    count_result = await session.execute(text(f"SELECT COUNT(*) FROM {staging}.ptg2_v3_block"))
    if int(count_result.scalar() or 0) > max_staged_block_rows:
        raise ResultArchiveAdoptionError("archive adoption staging block closure exceeds its bound")
    invalid_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                SELECT 1 FROM {staging}.ptg2_v3_block
                 WHERE octet_length(block_hash) <> 32
                    OR raw_byte_count < 0 OR stored_byte_count < 0
                    OR stored_byte_count <> octet_length(payload)
            )
            """
        )
    )
    if bool(invalid_result.scalar()):
        raise ResultArchiveAdoptionError("archive adoption staging CAS metadata is invalid")


async def _assert_staged_blocks_match_local(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
) -> None:
    """Require every staged CAS object to be already identical locally."""

    schema = _quote_ident(schema_name)
    staging = _quote_ident(staging_schema_name)
    conflict_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {staging}.ptg2_v3_block AS staged
                  LEFT JOIN {schema}.ptg2_v3_block AS local USING (block_hash)
                 WHERE local.block_hash IS NULL
                    OR ROW(local.format_version, local.object_kind, local.codec,
                           local.entry_count, local.raw_byte_count,
                           local.stored_byte_count, local.payload)
                       IS DISTINCT FROM
                       ROW(staged.format_version, staged.object_kind, staged.codec,
                           staged.entry_count, staged.raw_byte_count,
                           staged.stored_byte_count, staged.payload)
            )
            """
        )
    )
    if bool(conflict_result.scalar()):
        raise ResultArchiveAdoptionError("archive adoption CAS hash collides with different destination content")


async def _copy_staged_blocks(
    session: Any, *, schema_name: str, staging_schema_name: str, max_staged_block_rows: int
) -> None:
    await _validate_staged_blocks(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        max_staged_block_rows=max_staged_block_rows,
    )
    schema = _quote_ident(schema_name)
    staging = _quote_ident(staging_schema_name)
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_block
                (block_hash, format_version, object_kind, codec, entry_count,
                 raw_byte_count, stored_byte_count, payload, created_at)
            SELECT block_hash, format_version, object_kind, codec, entry_count,
                   raw_byte_count, stored_byte_count, payload, created_at
              FROM {staging}.ptg2_v3_block
            ON CONFLICT (block_hash) DO NOTHING
            """
        )
    )
    await _assert_staged_blocks_match_local(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
    )


async def _prepare_staged_map_root(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    representation_row = await _one(
        session,
        f"SELECT representation FROM {_quote_ident(staging_schema_name)}.ptg2_v4_snapshot_map_root WHERE snapshot_key = :source_snapshot_key",
        {"source_snapshot_key": source_snapshot_key},
        "staging map root",
    )
    await _initialize_v4_snapshot_map_root(
        session,
        schema_name=schema_name,
        snapshot_key=destination_snapshot_key,
        representation=str(representation_row.get("representation") or ""),
    )


async def _copy_staged_layout_rows(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    if not all(table_name in _REKEYED_TABLES for table_name in _FINALIZER_MAP_TABLES):
        for table_name in _REKEYED_TABLES:
            await _copy_rekeyed_table(
                session,
                schema_name=schema_name,
                staging_schema_name=staging_schema_name,
                table_name=table_name,
                source_snapshot_key=source_snapshot_key,
                destination_snapshot_key=destination_snapshot_key,
            )
        return

    first_finalizer_table = _REKEYED_TABLES.index(_FINALIZER_MAP_TABLES[0])
    last_finalizer_table = _REKEYED_TABLES.index(_FINALIZER_MAP_TABLES[-1])
    for table_name in _REKEYED_TABLES[:first_finalizer_table]:
        await _copy_rekeyed_table(
            session,
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            table_name=table_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_key=destination_snapshot_key,
        )
    await _copy_finalizer_map_rows(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    for table_name in _REKEYED_TABLES[last_finalizer_table + 1 :]:
        await _copy_rekeyed_table(
            session,
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            table_name=table_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_key=destination_snapshot_key,
        )


async def _copy_finalizer_map_rows(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Rekey a packed finalizer root without bypassing its building-state guards."""

    source_root = await _staged_finalizer_root(
        session,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
    )
    if source_root is None:
        await _copy_empty_finalizer_map_rows(
            session,
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_key=destination_snapshot_key,
        )
        return
    if source_root.get("state") != "complete":
        raise ResultArchiveAdoptionError("archive adoption finalizer root is not complete")
    await _copy_completed_finalizer_map_rows(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )


async def _staged_finalizer_root(
    session: Any,
    *,
    staging_schema_name: str,
    source_snapshot_key: int,
) -> dict[str, Any] | None:
    """Lock and return the optional finalizer root from the restored closure."""

    return await _one_or_none(
        session,
        f"""
        SELECT state, contract, map_format
          FROM {_quote_ident(staging_schema_name)}.ptg2_v4_finalizer_map_root
         WHERE snapshot_key = :source_snapshot_key
         FOR KEY SHARE
        """,
        {"source_snapshot_key": source_snapshot_key},
        "staging finalizer root",
    )


async def _copy_empty_finalizer_map_rows(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Copy the legacy empty finalizer family through ordinary rekeying."""

    for table_name in _FINALIZER_MAP_TABLES:
        await _copy_rekeyed_table(
            session,
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            table_name=table_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_key=destination_snapshot_key,
        )


async def _copy_completed_finalizer_map_rows(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Replay an authenticated finalizer family in its required state order."""

    await _insert_building_finalizer_root(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    for table_name in _FINALIZER_MAP_TABLES[1:]:
        await _copy_rekeyed_table(
            session,
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            table_name=table_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_key=destination_snapshot_key,
        )
    await _complete_finalizer_root_from_staging(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    await _assert_rekeyed_table_matches(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        table_name=_FINALIZER_MAP_TABLES[0],
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )


async def _insert_building_finalizer_root(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Create the guarded destination root using only source root identity fields."""

    insert_result = await session.execute(
        text(
            f"""
            INSERT INTO {_quote_ident(schema_name)}.ptg2_v4_finalizer_map_root
                (snapshot_key, state, contract, map_format, created_at)
            SELECT :destination_snapshot_key, 'building', contract, map_format, created_at
              FROM {_quote_ident(staging_schema_name)}.ptg2_v4_finalizer_map_root
             WHERE snapshot_key = :source_snapshot_key
               AND state = 'complete'
            RETURNING snapshot_key
            """
        ),
        {
            "source_snapshot_key": source_snapshot_key,
            "destination_snapshot_key": destination_snapshot_key,
        },
    )
    if insert_result.scalar() != destination_snapshot_key:
        raise ResultArchiveAdoptionError("archive adoption finalizer root could not enter building state")


async def _complete_finalizer_root_from_staging(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Apply the immutable source completion receipt after its children exist."""

    completion_result = await session.execute(
        text(
            f"""
            UPDATE {_quote_ident(schema_name)}.ptg2_v4_finalizer_map_root AS destination
               SET state = source.state,
                   map_digest = source.map_digest,
                   canonical_mapping_digest = source.canonical_mapping_digest,
                   canonical_byte_count = source.canonical_byte_count,
                   target_identity_digest = source.target_identity_digest,
                   object_kind_count = source.object_kind_count,
                   map_pack_count = source.map_pack_count,
                   coordinate_count = source.coordinate_count,
                   entry_count = source.entry_count,
                   logical_byte_count = source.logical_byte_count,
                   stored_map_byte_count = source.stored_map_byte_count,
                   target_block_count = source.target_block_count,
                   completed_at = source.completed_at
              FROM {_quote_ident(staging_schema_name)}.ptg2_v4_finalizer_map_root AS source
             WHERE destination.snapshot_key = :destination_snapshot_key
               AND destination.state = 'building'
               AND source.snapshot_key = :source_snapshot_key
               AND source.state = 'complete'
            RETURNING destination.snapshot_key
            """
        ),
        {
            "source_snapshot_key": source_snapshot_key,
            "destination_snapshot_key": destination_snapshot_key,
        },
    )
    if completion_result.scalar() != destination_snapshot_key:
        raise ResultArchiveAdoptionError("archive adoption finalizer root could not complete")


async def _assert_staged_layout_rows_match_local(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Compare the rekeyed staged closure before reusing a sealed layout."""

    for table_name in _REKEYED_TABLES:
        await _assert_rekeyed_table_matches(
            session,
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            table_name=table_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_key=destination_snapshot_key,
        )


async def _reused_mapping_digest(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
    support_digest: bytes,
    layout_manifest: Mapping[str, Any],
    max_staged_block_rows: int,
) -> bytes:
    """Authenticate staged physical content before binding a reused local key."""

    staged_summary = await summarize_persisted_v4_snapshot_maps(
        session,
        schema_name=staging_schema_name,
        snapshot_key=source_snapshot_key,
    )
    local_summary = await summarize_persisted_v4_snapshot_maps(
        session,
        schema_name=schema_name,
        snapshot_key=destination_snapshot_key,
    )
    if staged_summary != local_summary:
        raise ResultArchiveAdoptionError("archive adoption reused map summary differs from staging")
    mapping_digest = await _assert_reused_layout_metadata(
        session,
        schema_name=schema_name,
        destination_snapshot_key=destination_snapshot_key,
        support_digest=support_digest,
        layout_manifest=layout_manifest,
        staged_mapping_digest=staged_summary.map_digest,
    )
    await _validate_staged_blocks(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        max_staged_block_rows=max_staged_block_rows,
    )
    await _assert_staged_blocks_match_local(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
    )
    await _assert_staged_layout_rows_match_local(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    return mapping_digest


async def _assert_reused_layout_metadata(
    session: Any,
    *,
    schema_name: str,
    destination_snapshot_key: int,
    support_digest: bytes,
    layout_manifest: Mapping[str, Any],
    staged_mapping_digest: bytes,
) -> bytes:
    """Require a sealed local root to retain the staging layout identity."""

    local_layout = await _one(
        session,
        f"""
        SELECT layout.generation, layout.state, layout.support_digest,
               layout.mapping_digest, layout.layout_manifest,
               map_root.state AS map_root_state, map_root.map_digest AS map_root_digest
          FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_layout AS layout
          JOIN {_quote_ident(schema_name)}.ptg2_v4_snapshot_map_root AS map_root
            ON map_root.snapshot_key = layout.snapshot_key
         WHERE layout.snapshot_key = :destination_snapshot_key
         FOR KEY SHARE OF layout, map_root
        """,
        {"destination_snapshot_key": destination_snapshot_key},
        "reused destination layout",
    )
    mapping_digest = bytes(local_layout.get("mapping_digest") or b"")
    if (
        local_layout.get("generation") != PTG2_V4_SHARED_GENERATION
        or local_layout.get("state") != "sealed"
        or local_layout.get("map_root_state") != "complete"
        or bytes(local_layout.get("support_digest") or b"") != support_digest
        or mapping_digest != staged_mapping_digest
        or bytes(local_layout.get("map_root_digest") or b"") != mapping_digest
        or local_layout.get("layout_manifest") != layout_manifest
    ):
        raise ResultArchiveAdoptionError("archive adoption reused layout metadata differs from staging")
    return mapping_digest


async def _seal_destination_layout(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    support_digest: bytes,
    layout_manifest: Mapping[str, Any],
) -> tuple[int, bytes]:
    expected_summary = await summarize_persisted_v4_snapshot_maps(
        session, schema_name=schema_name, snapshot_key=snapshot_key
    )
    sealed = await seal_v4_shared_layout(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
        expected_summary=expected_summary,
        support_digest=support_digest,
        layout_manifest=layout_manifest,
    )
    return sealed.snapshot_key, sealed.mapping_digest


def _validated_adoption_request(
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    build_token: str,
    max_staged_block_rows: int,
) -> tuple[str, str, int, str, str]:
    destination_schema_name = _safe_identifier(schema_name, label="schema_name")
    staging_schema = _safe_identifier(staging_schema_name, label="staging_schema_name")
    if destination_schema_name == staging_schema:
        raise ValueError("archive adoption staging schema must differ from destination schema")
    if isinstance(source_snapshot_key, bool) or int(source_snapshot_key) < 0:
        raise ValueError("source_snapshot_key must be non-negative")
    if isinstance(max_staged_block_rows, bool) or not 0 < int(max_staged_block_rows) <= _MAX_STAGED_BLOCK_ROWS:
        raise ValueError("max_staged_block_rows is outside the supported bound")
    destination_snapshot = _required_snapshot_id(destination_snapshot_id)
    token = str(build_token or "").strip()
    if not token or len(token) > 96:
        raise ValueError("build_token is required")
    return destination_schema_name, staging_schema, int(source_snapshot_key), destination_snapshot, token


async def _reserve_destination_layout(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    build_token: str,
) -> tuple[Any, bytes, bytes, Mapping[str, Any]]:
    await _assert_destination_snapshot(session, schema=_quote_ident(schema_name), snapshot_id=destination_snapshot_id)
    await _assert_remapped_logical_identity(
        session,
        staging_schema=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id=destination_snapshot_id,
    )
    staging_layout = await _staged_layout(
        session, staging_schema=staging_schema_name, source_snapshot_key=source_snapshot_key
    )
    semantic_fingerprint, support_digest, layout_manifest = _validated_staged_layout(staging_layout)
    reservation = await reserve_v4_shared_layout(
        session,
        schema_name=schema_name,
        semantic_fingerprint=semantic_fingerprint,
        build_token=build_token,
    )
    return reservation, semantic_fingerprint, support_digest, layout_manifest


async def _prepare_new_destination_layout(
    session: Any,
    *,
    preparation: _NewLayoutPreparation,
) -> tuple[int, bytes]:
    await _copy_staged_blocks(
        session,
        schema_name=preparation.schema_name,
        staging_schema_name=preparation.staging_schema_name,
        max_staged_block_rows=preparation.max_staged_block_rows,
    )
    await _prepare_staged_map_root(
        session,
        schema_name=preparation.schema_name,
        staging_schema_name=preparation.staging_schema_name,
        source_snapshot_key=preparation.source_snapshot_key,
        destination_snapshot_key=preparation.destination_snapshot_key,
    )
    await _copy_staged_layout_rows(
        session,
        schema_name=preparation.schema_name,
        staging_schema_name=preparation.staging_schema_name,
        source_snapshot_key=preparation.source_snapshot_key,
        destination_snapshot_key=preparation.destination_snapshot_key,
    )
    return await _seal_destination_layout(
        session,
        schema_name=preparation.schema_name,
        snapshot_key=preparation.destination_snapshot_key,
        build_token=preparation.build_token,
        support_digest=preparation.support_digest,
        layout_manifest=preparation.layout_manifest,
    )


async def _bind_prepared_snapshot(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    snapshot_key: int,
    source_snapshot_key: int,
    mapping_digest: bytes,
) -> PreparedResultArchiveLayout:
    await bind_snapshot_to_v4_layout(
        session, schema_name=schema_name, snapshot_id=snapshot_id, snapshot_key=snapshot_key
    )
    return PreparedResultArchiveLayout(
        RESULT_ARCHIVE_ADOPTION_CONTRACT,
        snapshot_id,
        snapshot_key,
        source_snapshot_key,
        mapping_digest,
        True,
    )


async def _prepare_reused_destination_layout(
    session: Any,
    *,
    preparation: _NewLayoutPreparation,
    destination_snapshot_id: str,
) -> PreparedResultArchiveLayout:
    """Authenticate and bind an existing canonical layout to a local candidate."""

    mapping_digest = await _reused_mapping_digest(
        session,
        schema_name=preparation.schema_name,
        staging_schema_name=preparation.staging_schema_name,
        source_snapshot_key=preparation.source_snapshot_key,
        destination_snapshot_key=preparation.destination_snapshot_key,
        support_digest=preparation.support_digest,
        layout_manifest=preparation.layout_manifest,
        max_staged_block_rows=preparation.max_staged_block_rows,
    )
    return await _bind_prepared_snapshot(
        session,
        schema_name=preparation.schema_name,
        snapshot_id=destination_snapshot_id,
        snapshot_key=preparation.destination_snapshot_key,
        source_snapshot_key=preparation.source_snapshot_key,
        mapping_digest=mapping_digest,
    )


async def prepare_result_archive_layout(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    build_token: str,
    max_staged_block_rows: int = _MAX_STAGED_BLOCK_ROWS,
) -> PreparedResultArchiveLayout:
    """Prepare a local V4 layout; local candidate, frozen binding, and fresh attestation precede activation."""

    destination_schema, staging_schema, source_key, destination_snapshot, token = _validated_adoption_request(
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id=destination_snapshot_id,
        build_token=build_token,
        max_staged_block_rows=max_staged_block_rows,
    )
    reservation, _fingerprint, support_digest, layout_manifest = await _reserve_destination_layout(
        session,
        schema_name=destination_schema,
        staging_schema_name=staging_schema,
        source_snapshot_key=source_key,
        destination_snapshot_id=destination_snapshot,
        build_token=token,
    )
    preparation = _NewLayoutPreparation(
        destination_schema,
        staging_schema,
        source_key,
        reservation.snapshot_key,
        token,
        support_digest,
        layout_manifest,
        int(max_staged_block_rows),
    )
    if reservation.reused:
        return await _prepare_reused_destination_layout(
            session,
            preparation=preparation,
            destination_snapshot_id=destination_snapshot,
        )

    sealed_key, mapping_digest = await _prepare_new_destination_layout(
        session,
        preparation=preparation,
    )
    return await _bind_prepared_snapshot(
        session,
        schema_name=destination_schema,
        snapshot_id=destination_snapshot,
        snapshot_key=sealed_key,
        source_snapshot_key=source_key,
        mapping_digest=mapping_digest,
    )


__all__ = (
    "PreparedResultArchiveLayout",
    "RESULT_ARCHIVE_ADOPTION_CONTRACT",
    "ResultArchiveAdoptionError",
    "prepare_result_archive_layout",
)
