# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only PostgreSQL state inspection for failed PTG V4 recovery."""

from __future__ import annotations

import json
from typing import Any, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_DENSE_LAYOUT_TABLES


def row_mapping(database_record: Any) -> dict[str, Any]:
    """Return one database record as a mutable field mapping."""

    if database_record is None:
        return {}
    if isinstance(database_record, Mapping):
        return dict(database_record)
    return dict(getattr(database_record, "_mapping", database_record))


def json_mapping(raw_json: Any) -> dict[str, Any]:
    """Return a JSON object or an empty mapping for invalid input."""

    if isinstance(raw_json, Mapping):
        return dict(raw_json)
    if isinstance(raw_json, str):
        try:
            parsed_json = json.loads(raw_json)
        except json.JSONDecodeError:
            return {}
        return dict(parsed_json) if isinstance(parsed_json, Mapping) else {}
    return {}


async def _load_snapshot_record(
    executor: Any,
    *,
    schema: str,
    snapshot_id: str,
    lock_row: bool = False,
) -> dict[str, Any]:
    lock_clause = " FOR UPDATE" if lock_row else ""
    return row_mapping(
        await executor.first(
            f"""
            SELECT snapshot_id, import_run_id, status, published_at
              FROM {schema}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
             {lock_clause}
            """,
            snapshot_id=snapshot_id,
        )
    )


async def _load_run_record(
    executor: Any,
    *,
    schema: str,
    import_run_id: str,
    lock_row: bool = False,
) -> dict[str, Any]:
    lock_clause = " FOR UPDATE" if lock_row else ""
    return row_mapping(
        await executor.first(
            f"""
            SELECT import_run_id, status, report
              FROM {schema}.ptg2_import_run
             WHERE import_run_id = :import_run_id
             {lock_clause}
            """,
            import_run_id=import_run_id,
        )
    )


async def _load_layout_record(
    executor: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[str, Any]:
    return row_mapping(
        await executor.first(
            f"""
            SELECT layout.snapshot_key, layout.generation, layout.state,
                   layout.build_token, layout.created_at, layout.heartbeat_at,
                   layout.lease_until, layout.published_at,
                   fingerprint.semantic_fingerprint,
                   (
                       SELECT root.state
                         FROM {schema}.ptg2_v4_snapshot_map_root AS root
                        WHERE root.snapshot_key = layout.snapshot_key
                   ) AS root_state,
                   (
                       SELECT root.representation
                         FROM {schema}.ptg2_v4_snapshot_map_root AS root
                        WHERE root.snapshot_key = layout.snapshot_key
                   ) AS representation
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
              LEFT JOIN {schema}.ptg2_v3_layout_fingerprint AS fingerprint
                ON fingerprint.snapshot_key = layout.snapshot_key
             WHERE layout.snapshot_key = :snapshot_key
             LIMIT 2
            """,
            snapshot_key=snapshot_key,
        )
    )


async def load_recovery_records(
    executor: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    snapshot_key: int,
    lock_logical_owner: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Load the logical failure owner and its physical V4 layout."""

    schema = _quote_ident(schema_name)
    snapshot_by_field = await _load_snapshot_record(
        executor,
        schema=schema,
        snapshot_id=snapshot_id,
        lock_row=lock_logical_owner,
    )
    run_by_field = await _load_run_record(
        executor,
        schema=schema,
        import_run_id=import_run_id,
        lock_row=lock_logical_owner,
    )
    layout_by_field = await _load_layout_record(
        executor,
        schema=schema,
        snapshot_key=snapshot_key,
    )
    return snapshot_by_field, run_by_field, layout_by_field


async def load_reference_counts(
    executor: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    snapshot_key: int,
) -> dict[str, int]:
    """Count every logical or physical reference that fences recovery."""

    schema = _quote_ident(schema_name)
    count_record = await executor.first(
        f"""
        SELECT
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_binding
              WHERE snapshot_key = :snapshot_key
                 OR snapshot_id = :snapshot_id) AS bindings,
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_layout_fingerprint
              WHERE snapshot_key = :snapshot_key) AS fingerprints,
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_candidate_audit_attestation
              WHERE snapshot_key = :snapshot_key OR snapshot_id = :snapshot_id)
                AS attestations,
            (SELECT COUNT(*) FROM {schema}.ptg2_current_snapshot
              WHERE snapshot_id = :snapshot_id
                 OR previous_snapshot_id = :snapshot_id) AS global_pointers,
            (SELECT COUNT(*) FROM {schema}.ptg2_current_source_snapshot
              WHERE snapshot_id = :snapshot_id
                 OR previous_snapshot_id = :snapshot_id) AS source_pointers,
            (SELECT COUNT(*) FROM {schema}.ptg2_current_plan_source
              WHERE snapshot_id = :snapshot_id
                 OR previous_snapshot_id = :snapshot_id) AS plan_pointers,
            (SELECT COUNT(*) FROM {schema}.ptg2_snapshot_pin
              WHERE snapshot_id = :snapshot_id) AS pins,
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_scope
              WHERE snapshot_id = :snapshot_id) AS scopes,
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_source
              WHERE snapshot_id = :snapshot_id) AS sources,
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block
              WHERE snapshot_key = :snapshot_key) AS mapping_rows,
            (SELECT COUNT(*) FROM {schema}.ptg2_v4_snapshot_map_pack
              WHERE snapshot_key = :snapshot_key) AS map_packs,
            (SELECT COUNT(*) FROM {schema}.ptg2_v4_relation_manifest
              WHERE snapshot_key = :snapshot_key) AS relation_manifests
        """,
        snapshot_id=snapshot_id,
        snapshot_key=snapshot_key,
    )
    return {
        count_name: int(raw_count or 0)
        for count_name, raw_count in row_mapping(count_record).items()
    }


async def load_block_stats(
    executor: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    v4_hashes: set[bytes],
) -> dict[str, int]:
    """Resolve all V3 and packed V4 references against durable CAS."""

    schema = _quote_ident(schema_name)
    stats_record = await executor.first(
        f"""
        WITH candidate_hashes AS MATERIALIZED (
            SELECT DISTINCT block_hash
              FROM {schema}.ptg2_v3_snapshot_block
             WHERE snapshot_key = :snapshot_key
            UNION
            SELECT DISTINCT block_hash
              FROM unnest(CAST(:v4_hashes AS bytea[])) AS hash(block_hash)
        )
        SELECT COUNT(*) AS candidate_hashes,
               COUNT(block.block_hash) AS resolved_hashes,
               COALESCE(SUM(block.stored_byte_count), 0) AS stored_bytes
          FROM candidate_hashes AS candidate
          LEFT JOIN {schema}.ptg2_v3_block AS block
            ON block.block_hash = candidate.block_hash
        """,
        snapshot_key=snapshot_key,
        v4_hashes=sorted(v4_hashes),
    )
    return {
        stat_name: int(raw_stat or 0)
        for stat_name, raw_stat in row_mapping(stats_record).items()
    }


async def load_recovery_postconditions(
    executor: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> dict[str, int]:
    """Count all physical ownership rows that recovery must remove."""

    schema = _quote_ident(schema_name)
    dense_rows_sql = " + ".join(
        (
            f"(SELECT COUNT(*) FROM {schema}.{_quote_ident(table_name)} "
            "WHERE snapshot_key = :snapshot_key)"
        )
        for table_name in PTG2_V3_DENSE_LAYOUT_TABLES
    )
    count_record = await executor.first(
        f"""
        SELECT
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_layout
              WHERE snapshot_key = :snapshot_key) AS layouts,
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_layout_fingerprint
              WHERE snapshot_key = :snapshot_key) AS fingerprints,
            (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block
              WHERE snapshot_key = :snapshot_key) AS mappings,
            (SELECT COUNT(*) FROM {schema}.ptg2_v4_snapshot_map_root
              WHERE snapshot_key = :snapshot_key) AS map_roots,
            (SELECT COUNT(*) FROM {schema}.ptg2_v4_snapshot_map_pack
              WHERE snapshot_key = :snapshot_key) AS map_packs,
            (SELECT COUNT(*) FROM {schema}.ptg2_v4_relation_manifest
              WHERE snapshot_key = :snapshot_key) AS relation_manifests,
            ({dense_rows_sql}) AS dense_rows
        """,
        snapshot_key=snapshot_key,
    )
    return {
        count_name: int(raw_count or 0)
        for count_name, raw_count in row_mapping(count_record).items()
    }


__all__ = [
    "json_mapping",
    "load_block_stats",
    "load_recovery_postconditions",
    "load_recovery_records",
    "load_reference_counts",
]
