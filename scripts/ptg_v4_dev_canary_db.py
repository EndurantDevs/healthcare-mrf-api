"""Read-only PostgreSQL evidence collection for the PTG V4 dev canary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import asyncpg

from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
    PTG2_V4_GRAPH_RESOURCE_FIELDS,
)
from scripts.ptg_v4_dev_canary_db_reference import (
    ReferenceEquivalenceScope,
    _json_safe,
    _quote_identifier,
    _utc_now_text,
    collect_reference_equivalence,
)
from scripts.ptg_v4_dev_canary_db_shape import shape_snapshot_and_root
from scripts.ptg_v4_dev_canary_publication import (
    WHOLE_SNAPSHOT_PHYSICAL_RELATIONS,
)
from scripts.ptg_v4_dev_canary_storage import (
    STORAGE_BASELINE_CONTRACT,
    collect_physical_storage,
    relation_size_rows,
)


@dataclass(frozen=True)
class _DatabaseEvidenceScope:
    """Immutable snapshot, timing, storage, and rollback evidence inputs."""

    schema_name: str
    snapshot_id: str
    storage_baseline: Mapping[str, Any]
    import_started_at: datetime
    import_finished_at: datetime
    reference_snapshot_id: str
    rollback_owner_id: str


async def capture_storage_baseline(
    database_url: str,
    *,
    schema_name: str,
) -> dict[str, Any]:
    """Capture global relation sizes before dispatch; never mutate PostgreSQL."""

    connection = await asyncpg.connect(database_url, statement_cache_size=0)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            relation_rows = await relation_size_rows(
                connection,
                schema_name,
                sorted(WHOLE_SNAPSHOT_PHYSICAL_RELATIONS),
            )
    finally:
        await connection.close()
    return {
        "contract": STORAGE_BASELINE_CONTRACT,
        "captured_at": _utc_now_text(),
        "schema": schema_name,
        "relations": relation_rows,
    }


async def collect_v4_database_evidence(
    database_url: str,
    *,
    schema_name: str,
    snapshot_id: str,
    storage_baseline: Mapping[str, Any],
    import_started_at: datetime,
    import_finished_at: datetime,
    reference_snapshot_id: str,
    rollback_owner_id: str,
) -> dict[str, Any]:
    """Collect one repeatable-read, read-only publication evidence snapshot."""

    scope = _DatabaseEvidenceScope(
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        storage_baseline=storage_baseline,
        import_started_at=import_started_at,
        import_finished_at=import_finished_at,
        reference_snapshot_id=reference_snapshot_id,
        rollback_owner_id=rollback_owner_id,
    )
    connection = await asyncpg.connect(database_url, statement_cache_size=0)
    try:
        async with connection.transaction(
            isolation="repeatable_read",
            readonly=True,
        ):
            return await _collect_v4_database_rows(connection, scope)
    finally:
        await connection.close()


async def _collect_v4_database_rows(
    connection: asyncpg.Connection,
    scope: _DatabaseEvidenceScope,
) -> dict[str, Any]:
    """Collect every V4 acceptance section in one read-only transaction."""

    snapshot_by_field, root_by_field = await _snapshot_and_root(
        connection,
        scope.schema_name,
        scope.snapshot_id,
    )
    snapshot_key = int(snapshot_by_field["snapshot_key"])
    relation_rows = await _relation_manifests(
        connection,
        scope.schema_name,
        snapshot_key,
    )
    return {
        "snapshot": snapshot_by_field,
        "root": root_by_field,
        "exact_counts": await _exact_counts(
            connection,
            scope.schema_name,
            snapshot_key,
        ),
        "relations": relation_rows,
        "heavy_owners": await _heavy_owner_diagnostics(
            connection,
            scope.schema_name,
            snapshot_key,
        ),
        "provider_graph_diagnostic": await _provider_graph_diagnostic(
            connection,
            scope.schema_name,
            snapshot_key,
        ),
        "reference_equivalence": await collect_reference_equivalence(
            connection,
            scope.schema_name,
            ReferenceEquivalenceScope(
                v4_snapshot_id=scope.snapshot_id,
                reference_snapshot_id=scope.reference_snapshot_id,
                rollback_owner_id=scope.rollback_owner_id,
            ),
        ),
        "physical_storage": await collect_physical_storage(
            connection,
            schema_name=scope.schema_name,
            snapshot_id=scope.snapshot_id,
            snapshot_key=snapshot_key,
            relation_manifests=relation_rows,
            baseline=scope.storage_baseline,
            import_started_at=scope.import_started_at,
            import_finished_at=scope.import_finished_at,
        ),
    }


async def _snapshot_and_root(
    connection: asyncpg.Connection,
    schema_name: str,
    snapshot_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load and shape one published snapshot with its completed V4 root."""

    schema = _quote_identifier(schema_name)
    database_record = await connection.fetchrow(
        f"""
        SELECT snapshot.snapshot_id,
               snapshot.import_run_id,
               snapshot.status AS snapshot_status,
               snapshot.published_at,
               binding.snapshot_key,
               layout.state AS layout_state,
               layout.generation AS layout_generation,
               layout.logical_byte_count AS layout_logical_byte_count,
               layout.layout_manifest::text AS layout_manifest_text,
               root.state AS root_state,
               root.format_version,
               root.map_format,
               root.representation,
               root.projection_id_scope,
               encode(root.map_digest, 'hex') AS map_digest,
               root.object_kind_count,
               root.map_pack_count,
               root.coordinate_count,
               root.entry_count,
               root.logical_byte_count,
               root.stored_map_byte_count,
               root.npi_count,
               root.component_count,
               root.pattern_count,
               root.relation_count,
               root.heavy_owner_count,
               root.created_at AS root_created_at,
               root.completed_at AS root_completed_at
          FROM {schema}.ptg2_snapshot AS snapshot
          JOIN {schema}.ptg2_v3_snapshot_binding AS binding
            ON binding.snapshot_id = snapshot.snapshot_id
          JOIN {schema}.ptg2_v3_snapshot_layout AS layout
            ON layout.snapshot_key = binding.snapshot_key
          JOIN {schema}.ptg2_v4_snapshot_map_root AS root
            ON root.snapshot_key = binding.snapshot_key
         WHERE snapshot.snapshot_id = $1
        """,
        snapshot_id,
    )
    if database_record is None:
        raise RuntimeError("published V4 snapshot binding was not found")
    return shape_snapshot_and_root(dict(database_record))


async def _exact_counts(
    connection: asyncpg.Connection,
    schema_name: str,
    snapshot_key: int,
) -> dict[str, int]:
    schema = _quote_identifier(schema_name)
    count_record = await connection.fetchrow(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_snapshot_map_pack
            WHERE snapshot_key = $1)::bigint AS map_pack_count,
          (SELECT COALESCE(SUM(coordinate_count), 0)
             FROM {schema}.ptg2_v4_snapshot_map_pack
            WHERE snapshot_key = $1)::bigint AS map_coordinate_count,
          (SELECT COALESCE(SUM(entry_count), 0)
             FROM {schema}.ptg2_v4_snapshot_map_pack
            WHERE snapshot_key = $1)::bigint AS map_entry_count,
          (SELECT COALESCE(SUM(logical_byte_count), 0)
             FROM {schema}.ptg2_v4_snapshot_map_pack
            WHERE snapshot_key = $1)::bigint AS map_logical_byte_count,
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_npi_scope
            WHERE snapshot_key = $1)::bigint AS npi_count,
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_provider_component
            WHERE snapshot_key = $1)::bigint AS component_count,
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_pattern
            WHERE snapshot_key = $1)::bigint AS pattern_count,
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_relation_manifest
            WHERE snapshot_key = $1)::bigint AS relation_count,
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_heavy_owner
            WHERE snapshot_key = $1)::bigint AS heavy_owner_count,
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_provider_set_npi_prefix
            WHERE snapshot_key = $1)::bigint AS prefix_owner_count,
          (SELECT COALESCE(SUM(member_count), 0)
             FROM {schema}.ptg2_v4_provider_set_npi_prefix
            WHERE snapshot_key = $1)::bigint AS prefix_member_count,
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_provider_graph_diagnostic
            WHERE snapshot_key = $1)::bigint AS diagnostic_count
        """,
        snapshot_key,
    )
    return {
        field_name: int(field_count or 0)
        for field_name, field_count in dict(count_record).items()
    }


async def _relation_manifests(
    connection: asyncpg.Connection,
    schema_name: str,
    snapshot_key: int,
) -> list[dict[str, Any]]:
    schema = _quote_identifier(schema_name)
    relation_records = await connection.fetch(
        f"""
        SELECT relation, member_object_kind, locator_object_kind,
               owner_base, owner_count, logical_member_count,
               vector_member_count, member_width, member_page_bytes,
               locator_page_bytes, locator_owner_span
          FROM {schema}.ptg2_v4_relation_manifest
         WHERE snapshot_key = $1
         ORDER BY relation
        """,
        snapshot_key,
    )
    return [
        _json_safe(dict(relation_record))
        for relation_record in relation_records
    ]


async def _heavy_owner_diagnostics(
    connection: asyncpg.Connection,
    schema_name: str,
    snapshot_key: int,
) -> list[dict[str, Any]]:
    schema = _quote_identifier(schema_name)
    owner_records = await connection.fetch(
        f"""
        SELECT relation,
               COUNT(*)::bigint AS owner_count,
               MAX(member_count)::bigint AS maximum_member_count,
               MAX(member_span)::bigint AS maximum_member_span,
               SUM(fragment_count)::bigint AS fragment_count
          FROM {schema}.ptg2_v4_heavy_owner
         WHERE snapshot_key = $1
         GROUP BY relation
         ORDER BY relation
        """,
        snapshot_key,
    )
    return [
        _json_safe(dict(owner_record))
        for owner_record in owner_records
    ]


async def _provider_graph_diagnostic(
    connection: asyncpg.Connection,
    schema_name: str,
    snapshot_key: int,
) -> dict[str, Any]:
    """Load one compiler-authenticated graph diagnostic and its prefix rows."""

    schema = _quote_identifier(schema_name)
    all_fields_by_name = await _provider_graph_fields(
        connection,
        schema,
        snapshot_key,
    )
    resources_by_field, diagnostic_by_field = _split_provider_graph_fields(
        all_fields_by_name
    )
    prefix_aggregate = await _prefix_aggregate(
        connection,
        schema,
        snapshot_key,
        int(diagnostic_by_field["npi_prefix_target"]),
    )
    selected_owners = await _selected_prefix_owners(
        connection,
        schema,
        snapshot_key,
        diagnostic_by_field,
    )
    return {
        "row_count": 1,
        "fields": _json_safe(diagnostic_by_field),
        "resources": resources_by_field,
        "prefix": {
            **_json_safe(prefix_aggregate),
            "selected_owners": selected_owners,
        },
    }


async def _provider_graph_fields(
    connection: asyncpg.Connection,
    schema: str,
    snapshot_key: int,
) -> dict[str, Any]:
    """Select the singleton resource and hot-prefix diagnostic row."""

    selected_columns = ", ".join(
        (*PTG2_V4_GRAPH_RESOURCE_FIELDS, *PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS)
    )
    diagnostic_records = await connection.fetch(
        f"""
        SELECT {selected_columns}
          FROM {schema}.ptg2_v4_provider_graph_diagnostic
         WHERE snapshot_key = $1
        """,
        snapshot_key,
    )
    if len(diagnostic_records) != 1:
        raise RuntimeError("provider-graph diagnostic is missing or duplicated")
    return dict(diagnostic_records[0])


def _split_provider_graph_fields(
    all_fields_by_name: Mapping[str, Any],
) -> tuple[dict[str, int], dict[str, Any]]:
    """Separate sealed work resources from hot-prefix diagnostics."""

    resources_by_field = {
        field_name: int(all_fields_by_name[field_name])
        for field_name in PTG2_V4_GRAPH_RESOURCE_FIELDS
    }
    diagnostic_by_field = {
        field_name: all_fields_by_name[field_name]
        for field_name in PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS
    }
    for digest_field in ("worst_member_digest", "worst_online_member_digest"):
        digest = diagnostic_by_field.get(digest_field)
        diagnostic_by_field[digest_field] = (
            bytes(digest).hex() if digest is not None else None
        )
    return resources_by_field, diagnostic_by_field


async def _prefix_aggregate(
    connection: asyncpg.Connection,
    schema: str,
    snapshot_key: int,
    prefix_target: int,
) -> dict[str, Any]:
    """Aggregate exact prefix owner/member totals and digest validity."""

    prefix_aggregate = await connection.fetchrow(
        f"""
        SELECT COUNT(*)::bigint AS owner_count,
               COALESCE(SUM(member_count), 0)::bigint AS member_count,
               COALESCE(BOOL_AND(
                   member_count BETWEEN 0 AND $2
                   AND octet_length(member_digest) = 32
               ), TRUE) AS all_rows_valid
          FROM {schema}.ptg2_v4_provider_set_npi_prefix
         WHERE snapshot_key = $1
        """,
        snapshot_key,
        prefix_target,
    )
    return dict(prefix_aggregate)


async def _selected_prefix_owners(
    connection: asyncpg.Connection,
    schema: str,
    snapshot_key: int,
    diagnostic_by_field: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Return persisted prefix metadata only for the two deterministic owners."""

    owner_keys = sorted(
        {
            int(owner_key)
            for owner_key in (
                diagnostic_by_field.get("worst_provider_set_key"),
                diagnostic_by_field.get("worst_online_provider_set_key"),
            )
            if owner_key is not None
        }
    )
    if not owner_keys:
        return []
    prefix_records = await connection.fetch(
        f"""
        SELECT provider_set_key, member_count,
               encode(member_digest, 'hex') AS member_digest
          FROM {schema}.ptg2_v4_provider_set_npi_prefix
         WHERE snapshot_key = $1
           AND provider_set_key = ANY($2::integer[])
         ORDER BY provider_set_key
        """,
        snapshot_key,
        owner_keys,
    )
    return [
        _json_safe(dict(prefix_record))
        for prefix_record in prefix_records
    ]
