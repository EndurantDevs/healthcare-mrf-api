# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build and verify precomputed network statistics inside an open candidate schema."""

from __future__ import annotations

import json
from dataclasses import asdict
from uuid import UUID

from process.network_compact_layout import CompactNetworkLayout, compact_identifier
from process.network_geo_search import TABLE as GEO_SEARCH_TABLE
from process.network_membership_copy import MembershipCopyTarget
from process.network_provider_only_projection import TABLE_NAME as PROVIDER_ONLY_TABLE
from process.network_stats_contract import (
    CONTRACT,
    DIM_TABLE,
    PRIMARY_CARE_CODES,
    ROLLUP_SPECS,
    STATS_TABLE_ORDER,
    fetch_network_stats_index_records,
    require_network_stats_index_records,
    require_stats_condition,
    stats_digest,
)
from process.network_stats_sql import (
    BATCH_NETWORKS_STAGE,
    BATCH_OFFICES_STAGE,
    BATCH_STATES_STAGE,
    NPI_STAGE,
    OFFICE_STAGE,
    PO_NPI_STAGE,
    STAGE_TABLES,
    TAX_MAP_STAGE,
    batch_specialty_inserts_sql,
    batch_state_insert_sql,
    batch_summary_insert_sql,
    create_stats_indexes_sql,
    create_stats_tables_sql,
    fallback_dim_and_map_sql,
    invariant_anti_join_sql,
    nucc_dim_and_map_sql,
    office_stage_sql,
)

_BATCH_EDGE_BUDGET = 500_000
_SCOPE_FIELDS = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")


def _qualified(schema_name: str, table_name: str) -> str:
    return compact_identifier(schema_name) + "." + compact_identifier(table_name)


def _network_batches(network_edge_pairs: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Partition ordered network IDs into inclusive ranges bounded by _BATCH_EDGE_BUDGET."""
    if not network_edge_pairs:
        return []
    ranges: list[tuple[int, int]] = []
    range_min = network_edge_pairs[0][0]
    range_max = range_min
    accumulated_edges = 0
    for network_id, edge_count in network_edge_pairs:
        if accumulated_edges > 0 and accumulated_edges + edge_count > _BATCH_EDGE_BUDGET:
            ranges.append((range_min, range_max))
            range_min = network_id
            accumulated_edges = 0
        range_max = network_id
        accumulated_edges += edge_count
    ranges.append((range_min, range_max))
    return ranges


async def _resolve_reference_relation(connection, layout: CompactNetworkLayout, table_name: str) -> str | None:
    """Locate an optional NPPES/NUCC vocabulary table without crossing synthetic test boundaries."""
    candidate_schemas = [layout.base.schema_name]
    if layout.base.schema_name.startswith("network_composition_"):
        candidate_schemas.append("mrf")
    for schema_name in candidate_schemas:
        qualified = _qualified(schema_name, table_name)
        is_readable = await connection.fetchval(
            "SELECT to_regclass($1::text) IS NOT NULL AND has_table_privilege(current_user, to_regclass($1::text), 'SELECT')",
            qualified,
        )
        if is_readable is True:
            return qualified
    return None


async def _has_column(connection, qualified_table: str, column_name: str) -> bool:
    return bool(
        await connection.fetchval(
            """SELECT EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = to_regclass($1::text)
                  AND attnum > 0 AND NOT attisdropped AND attname = $2::text
            )""",
            qualified_table,
            column_name,
        )
    )


async def _populate_specialty_dimension(connection, schema_name: str, layout: CompactNetworkLayout) -> None:
    """Populate network_stats_specialty_dim and the unlogged taxonomy mapping stage."""
    tax_map = _qualified(schema_name, TAX_MAP_STAGE)
    geo_table = _qualified(schema_name, GEO_SEARCH_TABLE)
    await connection.execute(
        f"""CREATE UNLOGGED TABLE {tax_map} (
            int_code integer,
            code_key text COLLATE "C" NOT NULL,
            specialty_ids integer[] NOT NULL
        )"""
    )
    await connection.copy_records_to_table(
        DIM_TABLE,
        schema_name=schema_name,
        columns=("specialty_id", "specialty_kind", "specialty_key", "display_name", "taxonomy_int_code"),
        records=[(spec_id, "rollup", spec_key, label, None) for spec_id, spec_key, label in ROLLUP_SPECS],
    )
    nucc_relation = await _resolve_reference_relation(connection, layout, "nucc_taxonomy")
    if nucc_relation is not None:
        insert_tax, insert_cls, populate_map = nucc_dim_and_map_sql(schema_name, nucc_relation)
        await connection.execute(insert_tax)
        await connection.execute(insert_cls)
        await connection.execute(populate_map, list(PRIMARY_CARE_CODES))
    insert_fallback, insert_fallback_map = fallback_dim_and_map_sql(schema_name, geo_table)
    await connection.execute(insert_fallback)
    await connection.execute(insert_fallback_map)
    await connection.execute(f"CREATE INDEX ON {tax_map} (int_code) WHERE int_code IS NOT NULL")
    await connection.execute(f"CREATE UNIQUE INDEX ON {tax_map} (code_key)")
    await connection.execute(f"ANALYZE {tax_map}")


async def _has_prepared_provider_only_stage(connection, schema_name: str, layout: CompactNetworkLayout) -> bool:
    """Stage deduplicated NPI attributes and office locations; report provider-only presence."""
    po_source = _qualified(schema_name, PROVIDER_ONLY_TABLE)
    has_provider_only = bool(await connection.fetchval("SELECT to_regclass($1::text) IS NOT NULL", po_source))
    po_stage = _qualified(schema_name, PO_NPI_STAGE)
    if has_provider_only:
        await connection.execute(
            f"""CREATE UNLOGGED TABLE {po_stage} AS
                SELECT network_id, (provider_id::bigint) AS npi
                FROM ONLY {po_source}
                WHERE provider_system = 'npi' AND provider_id ~ '^[0-9]{{1,18}}$'
                GROUP BY network_id, provider_id"""
        )
        await connection.execute(f"CREATE INDEX ON {po_stage} (network_id)")
        await connection.execute(f"ANALYZE {po_stage}")
    await _build_npi_stage(connection, schema_name, layout, has_provider_only=has_provider_only)
    await _build_office_stage(connection, schema_name, layout)
    return has_provider_only


async def _build_npi_stage(
    connection, schema_name: str, layout: CompactNetworkLayout, *, has_provider_only: bool
) -> None:
    """Stage entity types and primary/any specialty ID arrays per distinct NPI."""
    geo_table = _qualified(schema_name, GEO_SEARCH_TABLE)
    po_stage = _qualified(schema_name, PO_NPI_STAGE)
    npi_stage = _qualified(schema_name, NPI_STAGE)
    tax_map = _qualified(schema_name, TAX_MAP_STAGE)
    npi_relation = await _resolve_reference_relation(connection, layout, "npi")
    npi_tax_relation = await _resolve_reference_relation(connection, layout, "npi_taxonomy")
    po_union = f"UNION SELECT npi FROM {po_stage}" if has_provider_only else ""
    entity_join = f"LEFT JOIN ONLY {npi_relation} nr ON nr.npi = d.npi" if npi_relation is not None else ""
    entity_expr = (
        "CASE WHEN nr.entity_type_code IN (1, 2) THEN nr.entity_type_code::smallint ELSE 0::smallint END"
        if npi_relation is not None
        else "0::smallint"
    )
    tax_join = (
        f"""LEFT JOIN LATERAL (
            SELECT
                COALESCE(array_agg(DISTINCT sid ORDER BY sid), '{{}}'::integer[]) AS npi_specialty_ids,
                COALESCE(array_agg(DISTINCT sid ORDER BY sid) FILTER (WHERE is_primary), '{{}}'::integer[])
                    AS primary_specialty_ids
            FROM (
                SELECT s.sid,
                       upper(btrim(COALESCE(nt.healthcare_provider_primary_taxonomy_switch, ''))) = 'Y' AS is_primary
                FROM ONLY {npi_tax_relation} nt
                JOIN {tax_map} tm ON tm.code_key = btrim(nt.healthcare_provider_taxonomy_code)
                CROSS JOIN LATERAL unnest(tm.specialty_ids) AS s(sid)
                WHERE nt.npi = d.npi
            ) mapped_nt
        ) nt_specs ON TRUE"""
        if npi_tax_relation is not None
        else ""
    )
    npi_specs_expr = "COALESCE(nt_specs.npi_specialty_ids, '{}'::integer[])" if npi_tax_relation else "'{}'::integer[]"
    prim_specs_expr = (
        "COALESCE(nt_specs.primary_specialty_ids, '{}'::integer[])" if npi_tax_relation else "'{}'::integer[]"
    )
    await connection.execute(
        f"""CREATE UNLOGGED TABLE {npi_stage} AS
            WITH distinct_npis AS (
                SELECT DISTINCT COALESCE(npi, inferred_npi) AS npi
                FROM ONLY {geo_table} WHERE COALESCE(npi, inferred_npi) IS NOT NULL
                {po_union}
            )
            SELECT d.npi, {entity_expr} AS entity_type,
                   {npi_specs_expr} AS npi_specialty_ids, {prim_specs_expr} AS primary_specialty_ids
            FROM distinct_npis d {entity_join} {tax_join}"""
    )
    await connection.execute(f"ALTER TABLE {npi_stage} ADD PRIMARY KEY (npi)")
    await connection.execute(f"ANALYZE {npi_stage}")


async def _build_office_stage(connection, schema_name: str, layout: CompactNetworkLayout) -> None:
    """Stage practice office coordinates, county FIPS and deduplicated specialty IDs."""
    geo_table = _qualified(schema_name, GEO_SEARCH_TABLE)
    base_table = _qualified(layout.base.schema_name, layout.base.table_name)
    has_base_county = await _has_column(connection, base_table, "county_fips")
    if has_base_county and layout.custom is not None:
        custom_table = _qualified(layout.custom.schema_name, layout.custom.table_name)
        county_expr = """CASE
            WHEN COALESCE(NULLIF(btrim(cu.county_fips), ''), NULLIF(btrim(ba.county_fips), '')) ~ '^[0-9]{5}$'
            THEN COALESCE(NULLIF(btrim(cu.county_fips), ''), NULLIF(btrim(ba.county_fips), '')) COLLATE "C"
        END"""
        county_join = (
            f"LEFT JOIN ONLY {custom_table} cu ON cu.location_key = g.location_key "
            f"LEFT JOIN ONLY {base_table} ba ON ba.location_key = g.location_key AND cu.location_key IS NULL"
        )
    elif has_base_county:
        county_expr = """CASE
            WHEN NULLIF(btrim(ba.county_fips), '') ~ '^[0-9]{5}$' THEN btrim(ba.county_fips) COLLATE "C"
        END"""
        county_join = f"LEFT JOIN ONLY {base_table} ba ON ba.location_key = g.location_key"
    else:
        county_expr = 'NULL::text COLLATE "C"'
        county_join = ""
    await connection.execute(office_stage_sql(schema_name, geo_table, county_expr, county_join))
    office_stage = _qualified(schema_name, OFFICE_STAGE)
    await connection.execute(f"ALTER TABLE {office_stage} ADD PRIMARY KEY (location_key)")
    await connection.execute(f"ANALYZE {office_stage}")


async def _create_batch_scratch_tables(connection, schema_name: str) -> None:
    """Create reusable unlogged scratch tables truncated across network batches."""
    batch_nets = _qualified(schema_name, BATCH_NETWORKS_STAGE)
    batch_offices = _qualified(schema_name, BATCH_OFFICES_STAGE)
    batch_states = _qualified(schema_name, BATCH_STATES_STAGE)
    await connection.execute(f"CREATE UNLOGGED TABLE {batch_nets} (network_id integer PRIMARY KEY)")
    await connection.execute(
        f"""CREATE UNLOGGED TABLE {batch_offices} (
            network_id integer NOT NULL,
            location_key text COLLATE "C" NOT NULL,
            effective_npi bigint,
            address_key uuid,
            state_code text COLLATE "C",
            zip5 text COLLATE "C",
            county_fips text COLLATE "C",
            entity_type smallint NOT NULL,
            any_specialty_ids integer[] NOT NULL,
            primary_specialty_ids integer[] NOT NULL
        )"""
    )
    await connection.execute(
        f"""CREATE UNLOGGED TABLE {batch_states} (
            network_id integer NOT NULL,
            state_code text COLLATE "C" NOT NULL,
            is_core_state boolean NOT NULL,
            office_npi_count integer NOT NULL,
            individual_npi_count integer NOT NULL,
            organization_npi_count integer NOT NULL,
            unique_address_count integer NOT NULL,
            unique_location_count integer NOT NULL,
            county_count integer NOT NULL,
            zip5_count integer NOT NULL
        )"""
    )


async def _load_network_edge_counts(connection, schema_name: str, *, has_provider_only: bool) -> list[tuple[int, int]]:
    """Count office edges per active network ID across slices and provider-only evidence."""
    ref_table = _qualified(schema_name, "network_slice_ref")
    po_stage = _qualified(schema_name, PO_NPI_STAGE)
    po_arm = (
        f"UNION ALL SELECT network_id, count(*)::bigint AS edge_rows FROM {po_stage} GROUP BY network_id"
        if has_provider_only
        else ""
    )
    entries = await connection.fetch(
        f"""SELECT network_id, sum(edge_rows)::bigint AS edge_rows
            FROM (
                SELECT network_id, sum(row_count)::bigint AS edge_rows FROM ONLY {ref_table} GROUP BY network_id
                {po_arm}
            ) net_edges
            GROUP BY network_id ORDER BY network_id"""
    )
    return [(int(entry["network_id"]), int(entry["edge_rows"])) for entry in entries]


async def _populate_network_batch(
    connection,
    schema_name: str,
    sql_by_step: dict[str, str],
    artifact_uuids: tuple[UUID, ...],
    min_net_id: int,
    max_net_id: int,
    *,
    has_provider_only: bool,
) -> None:
    """Populate state, summary and specialty statistics for one bounded network range."""
    batch_nets = _qualified(schema_name, BATCH_NETWORKS_STAGE)
    batch_offices = _qualified(schema_name, BATCH_OFFICES_STAGE)
    batch_states = _qualified(schema_name, BATCH_STATES_STAGE)
    await connection.execute(f"TRUNCATE {batch_nets}, {batch_offices}, {batch_states}")
    await connection.execute(sql_by_step["insert_nets"], min_net_id, max_net_id)
    if sql_by_step["insert_offices"] is not None:
        await connection.execute(sql_by_step["insert_offices"], min_net_id, max_net_id, *artifact_uuids)
    await connection.execute(f"ANALYZE {batch_nets}, {batch_offices}")
    await connection.execute(sql_by_step["compute_states"])
    await connection.execute(sql_by_step["flush_states"])
    await connection.execute(f"ANALYZE {batch_states}")
    if has_provider_only:
        await connection.execute(sql_by_step["summary"], min_net_id, max_net_id)
    else:
        await connection.execute(sql_by_step["summary"])
    await connection.execute(sql_by_step["state_spec"])
    if has_provider_only:
        await connection.execute(sql_by_step["net_spec"], min_net_id, max_net_id)
    else:
        await connection.execute(sql_by_step["net_spec"])


async def _aggregate_all_network_batches(
    connection, schema_name: str, layout: CompactNetworkLayout, *, has_provider_only: bool
) -> None:
    """Aggregate all candidate networks in bounded edge-count batches."""
    network_edge_pairs = await _load_network_edge_counts(connection, schema_name, has_provider_only=has_provider_only)
    batches = _network_batches(network_edge_pairs)
    if not batches:
        return
    await _create_batch_scratch_tables(connection, schema_name)
    ref_table = _qualified(schema_name, "network_slice_ref")
    po_stage = _qualified(schema_name, PO_NPI_STAGE)
    office_stage = _qualified(schema_name, OFFICE_STAGE)
    batch_nets = _qualified(schema_name, BATCH_NETWORKS_STAGE)
    batch_offices = _qualified(schema_name, BATCH_OFFICES_STAGE)
    edge_arms = " UNION ALL ".join(
        f"""SELECT r.network_id, e.location_key FROM ONLY {ref_table} r
            JOIN ONLY {_qualified(artifact.schema_name, "network_office_edge")} e ON e.slice_id = r.slice_id
            WHERE r.artifact_candidate_id = ${idx}::uuid AND r.network_id BETWEEN $1::integer AND $2::integer"""
        for idx, artifact in enumerate(layout.artifacts, 3)
    )
    po_net_union = (
        f"UNION SELECT DISTINCT network_id FROM {po_stage} WHERE network_id BETWEEN $1::integer AND $2::integer"
        if has_provider_only
        else ""
    )
    compute_states_sql, flush_states_sql = batch_state_insert_sql(schema_name)
    state_spec_sql, net_spec_sql = batch_specialty_inserts_sql(schema_name, has_provider_only=has_provider_only)
    insert_offices_sql = (
        f"""INSERT INTO {batch_offices}
            SELECT edges.network_id, os.location_key, os.effective_npi, os.address_key,
                   os.state_code, os.zip5, os.county_fips, os.entity_type,
                   os.any_specialty_ids, os.primary_specialty_ids
            FROM ({edge_arms}) edges JOIN {office_stage} os ON os.location_key = edges.location_key"""
        if layout.artifacts
        else None
    )
    sql_by_step = {
        "insert_nets": f"""INSERT INTO {batch_nets} (network_id)
            SELECT DISTINCT network_id FROM ONLY {ref_table}
            WHERE network_id BETWEEN $1::integer AND $2::integer {po_net_union}""",
        "insert_offices": insert_offices_sql,
        "compute_states": compute_states_sql,
        "flush_states": flush_states_sql,
        "summary": batch_summary_insert_sql(schema_name, has_provider_only=has_provider_only),
        "state_spec": state_spec_sql,
        "net_spec": net_spec_sql,
    }
    artifact_uuids = tuple(UUID(artifact.candidate_id) for artifact in layout.artifacts)
    for min_net_id, max_net_id in batches:
        await _populate_network_batch(
            connection,
            schema_name,
            sql_by_step,
            artifact_uuids,
            min_net_id,
            max_net_id,
            has_provider_only=has_provider_only,
        )


async def _drop_stage_tables(connection, schema_name: str) -> None:
    relations = ", ".join(_qualified(schema_name, stage_name) for stage_name in STAGE_TABLES)
    await connection.execute(f"DROP TABLE IF EXISTS {relations}")


async def _index_and_verify(connection, schema_name: str) -> dict[str, int]:
    """Create serving indexes and verify all set-based anti-joins before sealing."""
    for index_sql in create_stats_indexes_sql(schema_name):
        await connection.execute(index_sql)
    for table_name in STATS_TABLE_ORDER:
        await connection.execute(f"ANALYZE {_qualified(schema_name, table_name)}")
    verification_json = await connection.fetchval(invariant_anti_join_sql(schema_name), schema_name)
    checks_by_name = json.loads(verification_json)
    boolean_flags = (
        "orphan_specialty_rows",
        "orphan_state_specialty_rows",
        "orphan_state_networks",
        "orphan_specialty_networks",
        "orphan_state_specialty_states",
        "state_count_mismatch",
        "leftover_stage_tables",
    )
    require_stats_condition(all(checks_by_name[flag] is False for flag in boolean_flags))
    return {
        key: int(checks_by_name[key])
        for key in (
            "specialty_dim_rows",
            "summary_rows",
            "state_rows",
            "specialty_rows",
            "state_specialty_rows",
        )
    }


async def prepare_network_stats(connection, copy_target: MembershipCopyTarget, layout: CompactNetworkLayout) -> dict:
    """Build precomputed per-network statistics inside the caller's open candidate transaction."""
    from process.registry_network_import_progress import network_publication_step

    require_stats_condition(
        type(copy_target) is MembershipCopyTarget
        and type(layout) is CompactNetworkLayout
        and connection.is_in_transaction()
        and layout.ref_map.schema_name == copy_target.schema_name
    )
    schema_name = copy_target.schema_name
    has_geo = bool(
        await connection.fetchval("SELECT to_regclass($1::text) IS NOT NULL", _qualified(schema_name, GEO_SEARCH_TABLE))
    )
    require_stats_condition(has_geo, "network_geo_search is required before preparing network_stats")
    await network_publication_step("Building network statistics")
    original_work_mem = await connection.fetchval("SHOW work_mem")
    original_workers = await connection.fetchval("SHOW max_parallel_maintenance_workers")
    async with connection.transaction():
        await connection.execute("SET LOCAL work_mem = '256MB'")
        await connection.execute("SET LOCAL max_parallel_maintenance_workers = 4")
        for table_sql in create_stats_tables_sql(schema_name):
            await connection.execute(table_sql)
        await _populate_specialty_dimension(connection, schema_name, layout)
        has_provider_only = await _has_prepared_provider_only_stage(connection, schema_name, layout)
        await _aggregate_all_network_batches(connection, schema_name, layout, has_provider_only=has_provider_only)
        await _drop_stage_tables(connection, schema_name)
        counts_by_name = await _index_and_verify(connection, schema_name)
        await connection.execute("SELECT set_config('work_mem', $1, true)", original_work_mem)
        await connection.execute("SELECT set_config('max_parallel_maintenance_workers', $1, true)", original_workers)
        index_entries = await fetch_network_stats_index_records(connection, schema_name)
        descriptor_by_field = {
            "contract": CONTRACT,
            "scope": {field: getattr(copy_target, field) for field in _SCOPE_FIELDS},
            "tables": list(STATS_TABLE_ORDER),
            "counts": counts_by_name,
            "input_layout": json.loads(json.dumps(asdict(layout))),
            "indexes": [
                {field: entry[field] for field in ("table_name", "name", "definition")} for entry in index_entries
            ],
        }
        descriptor_by_field["sha256"] = stats_digest(descriptor_by_field)
        require_network_stats_index_records(index_entries, descriptor_by_field, require_state_gin=True)
        return descriptor_by_field
