# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL benchmark for EntityAddressUnified alias integrity."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import os
from pathlib import Path
import time
import uuid

import asyncpg
from db.connection import Database


ROOT = Path(__file__).resolve().parents[1]
entity_address_unified = None

_PUBLIC_EVIDENCE_NPI_SQL = """
    CREATE FUNCTION "{schema}".public_evidence_npi_valid(candidate_npi text)
    RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    SET search_path = pg_catalog AS $function$
        SELECT CASE WHEN candidate_npi ~ '^[0-9]{{10}}$' THEN
            CASE WHEN candidate_npi::bigint BETWEEN 1000000000 AND 2999999999
            THEN mod(24 + (
                SELECT sum(CASE
                    WHEN ordinal < 10 AND mod(ordinal, 2) = 1
                    THEN digit * 2 - CASE WHEN digit >= 5 THEN 9 ELSE 0 END
                    ELSE digit END)
                FROM unnest(string_to_array(candidate_npi, NULL))
                    WITH ORDINALITY AS item(value, ordinal)
                CROSS JOIN LATERAL (SELECT value::integer AS digit) AS parsed
            ), 10) = 0 ELSE false END
        ELSE false END;
    $function$;
"""


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

_DIGEST = "0" * 64
_RUN_IDS = (
    "00000000-0000-0000-0000-000000000001",
    "00000000-0000-0000-0000-000000000002",
)
_VIOLATION_KINDS = (
    "source_identity_mismatch",
    "missing_or_merged_target",
    "target_identity_mismatch",
    "multi_hop_alias",
)

def _migration_modules():
    return (
        _load_module(
            ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py",
            "address_alias_foundation_probe",
        ),
        _load_module(
            ROOT / "alembic/versions/20260811100000_address_numeric_grid_alias.py",
            "address_alias_migration_probe",
        ),
        _load_module(
            ROOT / "alembic/versions/20260816020000_address_evidence_alias.py",
            "address_evidence_alias_migration_probe",
        ),
        _load_module(
            ROOT / "alembic/versions/20260815010000_address_formatted_display_v2.py",
            "address_formatted_display_migration_probe",
        ),
    )


async def _connect_migration_probe():
    return await asyncpg.connect(
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.getenv("HLTHPRT_DB_DATABASE"),
    )


async def _upgrade_migration_probe(
    connection,
    probe_schema: str,
    foundation,
    migration,
    evidence_migration,
    formatted_display_migration,
) -> None:
    statements = [
        f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;',
        f'CREATE SCHEMA "{probe_schema}";',
        "CREATE EXTENSION IF NOT EXISTS btree_gin;",
        "CREATE EXTENSION IF NOT EXISTS intarray;",
        foundation._create_functions_sql(probe_schema),
        foundation._create_archive_sql(probe_schema),
        f'CREATE TABLE "{probe_schema}".partd_pharmacy_activity_stage_v2 '
        "(id bigint PRIMARY KEY);",
        migration._numeric_grid_function_sql(probe_schema),
        *migration._split_sql_statements(migration._alias_schema_sql(probe_schema)),
        _PUBLIC_EVIDENCE_NPI_SQL.format(schema=probe_schema),
        *evidence_migration._upgrade_statements(probe_schema),
        formatted_display_migration._humanize_component_function_sql(probe_schema),
        formatted_display_migration._formatted_address_function_sql(probe_schema),
    ]
    for statement in statements:
        await connection.execute(statement)


def _source_select(
    first_line: str,
    *,
    row_count: int = 1,
) -> str:
    return f"""
        SELECT
            'npi'::varchar AS entity_type, ('synthetic:' || series.row_number::text)::varchar AS entity_id, (1000000000 + series.row_number)::bigint AS npi,
            NULL::bigint AS inferred_npi, NULL::float8 AS inference_confidence, NULL::varchar AS inference_method,
            'Synthetic Provider'::varchar AS entity_name, NULL::varchar AS entity_subtype, 'primary'::varchar AS type,
            ARRAY[0]::int[] AS taxonomy_array, ARRAY[0]::int[] AS plans_network_array, ARRAY[0]::int[] AS procedures_array, ARRAY[0]::int[] AS medications_array,
            ARRAY[]::varchar[] AS aca_plan_array, ARRAY[]::varchar[] AS aca_network_array, ARRAY[]::varchar[] AS ptg_plan_array, ARRAY[]::varchar[] AS ptg_source_array, ARRAY[]::varchar[] AS group_plan_array,
            'address_archive_v2:v2+fmt-v2'::varchar AS base_address_version,
            CASE WHEN series.row_number <= {max(1, int(row_count) * 3 // 4)} THEN '{first_line}' ELSE series.row_number::text || ' Synthetic Benchmark Road'
            END::varchar AS first_line, NULL::varchar AS second_line,
            'Example City'::varchar AS city_name, 'TX'::varchar AS state_name, '75001'::varchar AS postal_code, 'US'::varchar AS country_code,
            NULL::varchar AS telephone_number, NULL::varchar AS fax_number, NULL::varchar AS formatted_address,
            NULL::numeric AS lat, NULL::numeric AS long, NULL::date AS date_added, NULL::varchar AS place_id,
            NULL::uuid AS address_key, TIMESTAMP '2026-01-01 00:00:00' AS updated_at, 'synthetic'::varchar AS address_source, ('synthetic:' || series.row_number::text)::varchar AS source_record_id
        FROM generate_series(1, {int(row_count)}) AS series(row_number)
    """


async def _address_identity(
    database: Database, schema: str, first_line: str
) -> tuple[str, str]:
    identity_row = await database.first(
        f"""
        SELECT
            {schema}.addr_key_v1(:first_line, NULL, 'Example City', 'TX', '75001', 'US')::text AS address_key,
            {schema}.addr_identity_key_v1(:first_line, NULL, 'Example City', 'TX', '75001', 'US') AS identity_key;
        """,
        first_line=first_line,
    )
    assert identity_row is not None
    return str(identity_row.address_key), str(identity_row.identity_key)


async def _archive_address(
    database: Database, schema: str, first_line: str
) -> tuple[str, str]:
    archive_row = await database.first(
        f"""
        INSERT INTO {schema}.address_archive_v2 (
            address_key, identity_key, identity_version, precision, premise_key,
            line1_norm, unit_norm, city_norm, state_code, zip5, country_code,
            first_line, city_name, state_name, postal_code, source_bits, strict_source_bits
        )
        SELECT
            {schema}.addr_key_v1(:first_line, NULL, 'Example City', 'TX', '75001', 'US'),
            {schema}.addr_identity_key_v1(:first_line, NULL, 'Example City', 'TX', '75001', 'US'),
            2, 'street',
            {schema}.addr_premise_key_v1(:first_line, NULL, 'Example City', 'TX', '75001', 'US'),
            {schema}.addr_street_norm_v1(:first_line, NULL),
            {schema}.addr_unit_norm_v1(:first_line, NULL),
            {schema}.addr_city_norm_v1('Example City'),
            {schema}.addr_state_code_v1('TX'),
            '75001', 'US', :first_line, 'Example City', 'TX', '75001', 3, 3
        RETURNING address_key::text, identity_key;
        """,
        first_line=first_line,
    )
    assert archive_row is not None
    return str(archive_row.address_key), str(archive_row.identity_key)


async def _seed_runs(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.address_alias_run_v1 (
            run_id, alias_kind, ruleset_version, mode, status, candidate_digest,
            reviewed_shadow_run_id, reviewed_candidate_digest, reviewed_by, reviewed_at
        ) VALUES
            (CAST(:shadow_run_id AS uuid), 'numeric_grid_direction_v1', 1, 'shadow',
             'sealed', :digest, NULL, NULL, NULL, NULL),
            (CAST(:apply_run_id AS uuid), 'numeric_grid_direction_v1', 1, 'apply',
             'applied', :digest, CAST(:shadow_run_id AS uuid), :digest,
             'synthetic-reviewer', now());
        """,
        shadow_run_id=_RUN_IDS[0],
        apply_run_id=_RUN_IDS[1],
        digest=_DIGEST,
    )


async def _insert_alias(
    database: Database,
    schema: str,
    source_key: str,
    source_identity: str,
    target_key: str,
    target_identity: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.address_alias_v1 (
            source_address_key, source_identity_key,
            target_address_key, target_identity_key,
            alias_kind, ruleset_version, target_strict_source_bits,
            target_strict_source_count, candidate_count,
            shadow_run_id, apply_run_id, reviewed_candidate_digest
        ) VALUES (
            CAST(:source_key AS uuid), :source_identity, CAST(:target_key AS uuid),
            :target_identity, 'numeric_grid_direction_v1', 1, 3, 2, 1,
            CAST(:shadow_run_id AS uuid), CAST(:apply_run_id AS uuid), :digest
        );
        """,
        source_key=source_key,
        source_identity=source_identity,
        target_key=target_key,
        target_identity=target_identity,
        shadow_run_id=_RUN_IDS[0],
        apply_run_id=_RUN_IDS[1],
        digest=_DIGEST,
    )


async def _seed_alias_cases(database: Database, schema: str) -> dict[str, str]:
    await _seed_runs(database, schema)
    first_line_by_case = {
        "clean": "1000 Alpha Road",
        "source_identity_mismatch": "2000 Bravo Road",
        "missing_or_merged_target": "3000 Charlie Road",
        "target_identity_mismatch": "4000 Delta Road",
        "multi_hop_alias": "5000 Echo Road",
    }
    identity_by_case = {
        name: await _address_identity(database, schema, first_line)
        for name, first_line in first_line_by_case.items()
    }
    clean_target = await _archive_address(database, schema, "1000 Alpha Road South")
    source_target = await _archive_address(database, schema, "2000 Bravo Road South")
    merged_target = await _archive_address(database, schema, "3000 Charlie Road South")
    merge_destination = await _archive_address(
        database, schema, "3000 Charlie Road Final"
    )
    mismatch_target = await _archive_address(database, schema, "4000 Delta Road South")
    hop_target = await _archive_address(database, schema, "5000 Echo Road South")
    hop_final = await _archive_address(database, schema, "5000 Echo Road Final")

    await _insert_alias(database, schema, *identity_by_case["clean"], *clean_target)
    await _insert_alias(
        database,
        schema,
        identity_by_case["source_identity_mismatch"][0],
        "stale-source-identity",
        *source_target,
    )
    await _insert_alias(
        database,
        schema,
        *identity_by_case["missing_or_merged_target"],
        *merged_target,
    )
    await database.status(
        f"UPDATE {schema}.address_archive_v2 SET merged_into = CAST(:destination AS uuid) "
        "WHERE address_key = CAST(:target AS uuid);",
        destination=merge_destination[0],
        target=merged_target[0],
    )
    await _insert_alias(
        database,
        schema,
        *identity_by_case["target_identity_mismatch"],
        mismatch_target[0],
        "stale-target-identity",
    )
    await _insert_alias(
        database, schema, *identity_by_case["multi_hop_alias"], *hop_target
    )
    await _insert_alias(database, schema, *hop_target, *hop_final)
    return first_line_by_case


async def _prepare_raw_pipeline(
    database: Database,
    schema: str,
    table_name: str,
    source_sql: str,
) -> None:
    await database.status(
        entity_address_unified._prepare_raw_stage_sql(schema, table_name)
    )
    await database.status(
        entity_address_unified._insert_raw_from_source_sql(
            schema, table_name, source_sql
        )
    )
    await database.status(
        f"CREATE INDEX {table_name}_idx_checksum ON {schema}.{table_name} (checksum);"
    )
    await database.status(f"ANALYZE {schema}.{table_name};")
    await database.status(
        entity_address_unified._enrich_raw_stage_sql(
            schema,
            table_name,
            archive_available=True,
        )
    )

async def _measure_alias_integrity(
    schema: str, table_name: str, expected_violation: str | None
) -> tuple[float, str | None]:
    integrity_started = time.perf_counter()
    validator = entity_address_unified._validate_raw_alias_integrity
    enrich_shards = entity_address_unified._env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS",
        entity_address_unified.DEFAULT_ENRICH_SHARDS,
        1,
    )
    validator_parameters = inspect.signature(validator).parameters
    validator_options_by_name = {"is_address_canon_available": True}
    if "checksum_ranges" in validator_parameters:
        validator_options_by_name["checksum_ranges"] = (
            entity_address_unified._integer_ranges(-(2**31), 2**31 - 1, enrich_shards)
            if enrich_shards > 1
            else None
        )
    if "concurrency" in validator_parameters:
        validator_options_by_name["concurrency"] = min(
            entity_address_unified._env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY",
                entity_address_unified.DEFAULT_ENRICH_CONCURRENCY,
                1,
            ),
            enrich_shards,
        )
    try:
        await validator(schema, table_name, **validator_options_by_name)
    except RuntimeError as error:
        if expected_violation is None or f"kind={expected_violation}" not in str(error):
            raise
        return time.perf_counter() - integrity_started, expected_violation
    if expected_violation is not None:
        raise AssertionError(f"expected alias violation {expected_violation}")
    return time.perf_counter() - integrity_started, None


async def _run_pipeline(
    database: Database,
    schema: str,
    table_name: str,
    source_sql: str,
    *,
    expected_violation: str | None = None,
    force_failure: bool = False,
) -> tuple[float, float, str | None, int]:
    started = time.perf_counter()
    try:
        await _prepare_raw_pipeline(database, schema, table_name, source_sql)
        if force_failure:
            raise RuntimeError("synthetic forced failure")
        integrity_seconds, observed_violation = await _measure_alias_integrity(
            schema,
            table_name,
            expected_violation,
        )
        pipeline_seconds = time.perf_counter() - started
        observed_rows = int(
            await database.scalar(f"SELECT COUNT(*) FROM {schema}.{table_name};") or 0
        )
        if observed_violation is not None:
            return (
                pipeline_seconds,
                integrity_seconds,
                observed_violation,
                observed_rows,
            )
        enriched_digest = await database.scalar(
            f"""
            SELECT md5(string_agg(
                concat_ws('|', source_record_id, address_key::text, premise_key::text,
                    base_address_version, location_key, zip5, state_code, city_norm),
                E'\\n' ORDER BY source_record_id
            ))
            FROM {schema}.{table_name};
            """
        )
        return (
            pipeline_seconds,
            integrity_seconds,
            str(enriched_digest),
            observed_rows,
        )
    finally:
        await database.status(f"DROP TABLE IF EXISTS {schema}.{table_name};")


async def _exercise_violation_cases(
    database: Database,
    schema: str,
    first_line_by_case: dict[str, str],
) -> tuple[str | None, list[dict[str, str | None]]]:
    violation_tuples: list[dict[str, str | None]] = []
    for index, violation_kind in enumerate(_VIOLATION_KINDS):
        source_sql = _source_select(first_line_by_case[violation_kind])
        await _run_pipeline(
            database,
            schema,
            f"violation_{index}_raw",
            f"{source_sql} UNION ALL {source_sql}",
            expected_violation=violation_kind,
        )
        violation_tuples.append(
            await _violation_tuple(
                database,
                schema,
                first_line_by_case[violation_kind],
            )
        )
    combined_source_sql = " UNION ALL ".join(
        _source_select(first_line_by_case[violation_kind])
        for violation_kind in _VIOLATION_KINDS
        for _ in range(2)
    )
    _, _, deterministic_kind, _ = await _run_pipeline(
        database,
        schema,
        "combined_violation_raw",
        combined_source_sql,
        expected_violation="missing_or_merged_target",
    )
    assert deterministic_kind == "missing_or_merged_target"
    return next(
        item for item in violation_tuples if item["kind"] == deterministic_kind
    ), violation_tuples


async def _violation_tuple(
    database: Database,
    schema: str,
    first_line: str,
) -> dict[str, str | None]:
    row = await database.first(
        f"""
        WITH source AS (
            SELECT
                {schema}.addr_key_v1(
                    :first_line, NULL, 'Example City', 'TX', '75001', 'US'
                ) AS source_address_key,
                {schema}.addr_identity_key_v1(
                    :first_line, NULL, 'Example City', 'TX', '75001', 'US'
                ) AS current_source_identity_key
        )
        SELECT
            alias.source_address_key::text AS source_address_key,
            alias.target_address_key::text AS target_address_key,
            alias.source_identity_key,
            alias.target_identity_key,
            source.current_source_identity_key,
            target.identity_key AS current_target_identity_key
          FROM source
          JOIN {schema}.address_alias_v1 AS alias
            ON alias.source_address_key = source.source_address_key
           AND alias.revoked_at IS NULL
          LEFT JOIN {schema}.address_archive_v2 AS target
            ON target.address_key = alias.target_address_key
           AND target.merged_into IS NULL
         ORDER BY alias.target_address_key
         LIMIT 1;
        """,
        first_line=first_line,
    )
    assert row is not None
    return {
        "kind": next(
            kind
            for kind, line in {
                "source_identity_mismatch": "2000 Bravo Road",
                "missing_or_merged_target": "3000 Charlie Road",
                "target_identity_mismatch": "4000 Delta Road",
                "multi_hop_alias": "5000 Echo Road",
            }.items()
            if line == first_line
        ),
        "source_address_key": str(row.source_address_key),
        "target_address_key": str(row.target_address_key),
        "source_identity_key": str(row.source_identity_key),
        "target_identity_key": str(row.target_identity_key),
        "current_source_identity_key": str(row.current_source_identity_key),
        "current_target_identity_key": (
            str(row.current_target_identity_key)
            if row.current_target_identity_key is not None
            else None
        ),
    }


async def _has_forced_failure_cleanup(
    database: Database,
    schema: str,
    clean_first_line: str,
) -> bool:
    forced_table = "forced_failure_raw"
    try:
        await _run_pipeline(
            database,
            schema,
            forced_table,
            _source_select(clean_first_line),
            force_failure=True,
        )
    except RuntimeError as error:
        if str(error) != "synthetic forced failure":
            raise
    else:
        raise AssertionError("forced benchmark failure did not run")
    return not bool(
        await database.scalar(
            "SELECT to_regclass(:relation_name) IS NOT NULL;",
            relation_name=f"{schema}.{forced_table}",
        )
    )


def _load_runtime_modules(schema: str):
    """Import production EAU models after fixing the disposable schema."""
    os.environ["DB_SCHEMA"] = schema
    os.environ["HLTHPRT_DB_SCHEMA"] = schema
    runtime_module = importlib.import_module("process.entity_address_unified")
    from db.models import MRFAddress

    return runtime_module, MRFAddress


async def _seed_production_sources(
    database: Database,
    schema: str,
    row_count: int,
    mrf_address_model,
) -> None:
    await database.create_table(mrf_address_model.__table__, checkfirst=True)
    source_key, source_identity = await _address_identity(
        database, schema, "1000 Alias Source Road"
    )
    target_key, target_identity = await _archive_address(
        database, schema, "1000 Alias Target Road"
    )
    await _insert_alias(
        database,
        schema,
        source_key,
        source_identity,
        target_key,
        target_identity,
    )
    if row_count > 1:
        await database.status(
            f"""
            INSERT INTO {schema}.address_archive_v2 (
                address_key, identity_key, identity_version, precision,
                premise_key, line1_norm, unit_norm, city_norm, state_code,
                zip5, country_code, first_line, city_name, state_name,
                postal_code, source_bits, strict_source_bits
            )
            SELECT
                {schema}.addr_key_v1(line, NULL, 'Example City', 'TX', '75001', 'US'),
                {schema}.addr_identity_key_v1(line, NULL, 'Example City', 'TX', '75001', 'US'),
                2, 'street',
                {schema}.addr_premise_key_v1(line, NULL, 'Example City', 'TX', '75001', 'US'),
                {schema}.addr_street_norm_v1(line, NULL),
                {schema}.addr_unit_norm_v1(line, NULL),
                {schema}.addr_city_norm_v1('Example City'),
                {schema}.addr_state_code_v1('TX'),
                '75001', 'US', line, 'Example City', 'TX', '75001', 3, 3
              FROM (
                    SELECT format('%s Synthetic Benchmark Road', 1000 + value) AS line
                      FROM generate_series(2, {row_count}) AS values(value)
              ) AS generated
            ON CONFLICT (address_key) DO NOTHING;
            """
        )
    await database.status(
        f"""
        INSERT INTO {schema}.mrf_address (
            checksum, npi, type, first_line, second_line, city_name,
            state_name, postal_code, country_code, address_key
        )
        SELECT
            value,
            1000000000 + value,
            'practice',
            CASE
                WHEN value = 1 THEN '1000 Alias Source Road'
                ELSE format('%s Synthetic Benchmark Road', 1000 + value)
            END,
            NULL,
            'Example City',
            'TX',
            '75001',
            'US',
            CASE
                WHEN value = 1 THEN CAST(:source_key AS uuid)
                ELSE {schema}.addr_key_v1(
                    format('%s Synthetic Benchmark Road', 1000 + value),
                    NULL, 'Example City', 'TX', '75001', 'US'
                )
            END
          FROM generate_series(1, {row_count}) AS values(value)
        ON CONFLICT DO NOTHING;
        """,
        source_key=source_key,
    )


def _published_table_names(runtime_module) -> list[str]:
    return [
        runtime_module.EntityAddressUnified.__main_table__,
        *(model.__main_table__ for model in runtime_module.SUPPORT_TABLE_MODELS),
    ]


def _stage_table_names(runtime_module, import_date: str) -> list[str]:
    stage_classes = [
        runtime_module.make_class(runtime_module.EntityAddressUnified, import_date),
        *(
            runtime_module.make_class(model, import_date)
            for model in runtime_module.SUPPORT_TABLE_MODELS
        ),
    ]
    return [stage.__tablename__ for stage in stage_classes]


async def _relation_identity(database: Database, schema: str, table_name: str):
    row = await database.first(
        """
        SELECT c.oid::bigint AS relation_oid, c.relpersistence::text AS persistence
          FROM pg_class AS c
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = :schema
           AND c.relname = :table_name
           AND c.relkind IN ('r', 'p');
        """,
        schema=schema,
        table_name=table_name,
    )
    if row is None:
        return None
    return {
        "oid": int(row.relation_oid),
        "persistence": str(row.persistence),
    }


async def _table_digest(database: Database, schema: str, table_name: str) -> str:
    digest = await database.scalar(
        f"""
        SELECT md5(COALESCE(
            string_agg(
                (to_jsonb(record) - ARRAY[
                    'updated_at', 'last_seen_at', 'observed_at',
                    'first_seen_at', 'geocoded_at', 'created_at',
                    'published_at', 'retired_at'
                ])::text,
                E'\\n' ORDER BY (
                    to_jsonb(record) - ARRAY[
                        'updated_at', 'last_seen_at', 'observed_at',
                        'first_seen_at', 'geocoded_at', 'created_at',
                        'published_at', 'retired_at'
                    ]
                )::text
            ),
            ''
        ))
        FROM {schema}.{table_name} AS record;
        """
    )
    assert digest is not None
    return str(digest)


async def _published_snapshot(
    database: Database,
    schema: str,
    table_names: list[str],
) -> dict[str, object]:
    rows: dict[str, int] = {}
    digests: dict[str, str] = {}
    relations: dict[str, dict[str, object]] = {}
    for table_name in table_names:
        relation = await _relation_identity(database, schema, table_name)
        assert relation is not None
        rows[table_name] = int(
            await database.scalar(f"SELECT COUNT(*) FROM {schema}.{table_name};") or 0
        )
        digests[table_name] = await _table_digest(database, schema, table_name)
        relations[table_name] = relation
    return {"rows": rows, "digests": digests, "relations": relations}


async def _drop_relations(
    database: Database,
    schema: str,
    table_names: list[str],
) -> None:
    for table_name in table_names:
        await database.status(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")


def _phase_seconds(context: dict, *fragments: str) -> float:
    timings = context.get("phase_timings") or (context.get("context") or {}).get(
        "phase_timings"
    ) or {}
    return round(
        sum(
            float(entry.get("seconds") or 0.0)
            for phase, entry in timings.items()
            if any(fragment in phase for fragment in fragments)
        ),
        6,
    )


async def _private_artifact_names(runtime_module, import_date: str) -> list[str]:
    stage_names = _stage_table_names(runtime_module, import_date)
    stage_main = stage_names[0]
    return [
        *stage_names,
        runtime_module._raw_stage_table_name(stage_main),
        runtime_module._evidence_stage_table_name(stage_main),
        *(f"{table}_old" for table in _published_table_names(runtime_module)),
    ]


async def _residue_count(
    database: Database,
    schema: str,
    table_names: list[str],
) -> int:
    residue = 0
    for table_name in table_names:
        if await _relation_identity(database, schema, table_name) is not None:
            residue += 1
    return residue


async def _seed_live_markers(
    database: Database,
    schema: str,
    table_names: list[str],
) -> dict[str, dict[str, object]]:
    markers: dict[str, dict[str, object]] = {}
    for table_name in table_names:
        await database.status(
            f"CREATE TABLE {schema}.{table_name} (marker text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {schema}.{table_name} (marker) VALUES ('old');"
        )
        relation = await _relation_identity(database, schema, table_name)
        assert relation is not None
        markers[table_name] = relation
    return markers


async def _run_production_lifecycle(
    runtime_module,
    row_count: int,
) -> tuple[dict, float, float, float]:
    ctx: dict = {}
    alias_seconds = 0.0
    original_validator = runtime_module._validate_raw_alias_integrity

    async def _timed_validator(*args, **kwargs):
        nonlocal alias_seconds
        started = time.perf_counter()
        try:
            return await original_validator(*args, **kwargs)
        finally:
            alias_seconds += time.perf_counter() - started

    runtime_module._validate_raw_alias_integrity = _timed_validator
    started = time.perf_counter()
    try:
        await runtime_module.startup(ctx)
        await runtime_module.process_data(
            ctx,
            {
                "test_mode": True,
                "publish": True,
                "refresh_mode": "full",
                "serving_only_refresh": False,
                "limit_per_source": row_count,
            },
        )
        shutdown_started = time.perf_counter()
        await runtime_module.shutdown(ctx)
        shutdown_seconds = time.perf_counter() - shutdown_started
    finally:
        runtime_module._validate_raw_alias_integrity = original_validator
    return ctx, time.perf_counter() - started, shutdown_seconds, alias_seconds


async def _artifact_residue(
    database: Database,
    schema: str,
    runtime_module,
    import_date: str,
) -> int:
    return await _residue_count(
        database,
        schema,
        [
            *_stage_table_names(runtime_module, import_date),
            runtime_module._raw_stage_table_name(
                _stage_table_names(runtime_module, import_date)[0]
            ),
            runtime_module._evidence_stage_table_name(
                _stage_table_names(runtime_module, import_date)[0]
            ),
        ],
    )


async def _run_failure_containment(
    database: Database,
    schema: str,
    runtime_module,
    row_count: int,
    connection,
) -> dict[str, object]:
    live_table = runtime_module.EntityAddressUnified.__main_table__
    before = await _relation_identity(database, schema, live_table)
    assert before is not None
    original_validator = runtime_module._validate_raw_alias_integrity

    async def _fail_after_validator(*args, **kwargs):
        await original_validator(*args, **kwargs)
        raise RuntimeError("synthetic alias fence failure")

    runtime_module._validate_raw_alias_integrity = _fail_after_validator
    failure_ctx: dict[str, object] = {}
    try:
        await runtime_module.startup(failure_ctx)
        try:
            await runtime_module.process_data(
                failure_ctx,
                {
                    "test_mode": True,
                    "publish": True,
                    "refresh_mode": "full",
                    "serving_only_refresh": False,
                    "limit_per_source": row_count,
                },
            )
        except RuntimeError as error:
            if str(error) != "synthetic alias fence failure":
                raise
        else:
            raise AssertionError("synthetic production validator failure did not run")
    finally:
        try:
            import_date = str(failure_ctx["import_date"])
            stage_names = _stage_table_names(runtime_module, import_date)
            await runtime_module._drop_stage_artifacts(
                schema,
                runtime_module.make_class(runtime_module.EntityAddressUnified, import_date),
                {
                    model: runtime_module.make_class(model, import_date)
                    for model in runtime_module.SUPPORT_TABLE_MODELS
                },
            )
            await database.status(
                f"DROP TABLE IF EXISTS {schema}.{runtime_module._raw_stage_table_name(stage_names[0])};"
            )
            await database.status(
                f"DROP TABLE IF EXISTS {schema}.{runtime_module._evidence_stage_table_name(stage_names[0])};"
            )
        finally:
            runtime_module._validate_raw_alias_integrity = original_validator

    after = await _relation_identity(database, schema, live_table)
    private_names = await _private_artifact_names(runtime_module, str(failure_ctx["import_date"]))
    residue = await _residue_count(database, schema, private_names)
    active_sessions = await connection.fetchval(
        """
        SELECT count(*)
          FROM pg_stat_activity
         WHERE datname = current_database()
           AND pid <> pg_backend_pid()
           AND state <> 'idle'
           AND query ILIKE $1;
        """,
        f"%{schema}%",
    )
    return {
        "live_unchanged": after is not None and after["oid"] == before["oid"],
        "old_swap_absent": await _relation_identity(database, schema, f"{live_table}_old") is None,
        "private_artifacts_cleaned": residue == 0,
        "drained_sessions": int(active_sessions or 0) == 0,
        "residue_count": residue,
    }


def _benchmark_inputs() -> tuple[str, int]:
    event_path = os.getenv("ENDURANT_BENCHMARK_EVENT_PATH")
    if not event_path:
        raise RuntimeError("ENDURANT_BENCHMARK_EVENT_PATH is required")
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.lower():
        raise RuntimeError(
            "HLTHPRT_DB_DATABASE must identify a disposable test database"
        )
    row_count = int(os.getenv("HLTHPRT_ENTITY_ADDRESS_ALIAS_BENCHMARK_ROWS", "20000"))
    if row_count < 1:
        raise ValueError("HLTHPRT_ENTITY_ADDRESS_ALIAS_BENCHMARK_ROWS must be positive")
    return event_path, row_count


def _production_env(schema: str) -> dict[str, str]:
    return {
        "DB_SCHEMA": schema,
        "HLTHPRT_DB_SCHEMA": schema,
        "HLTHPRT_IMPORT_ID_OVERRIDE": "20260101010101",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS": "1",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_TABLE_SHARDS": "1",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SHARDS": "32",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY": "8",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS": "64",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY": "24",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_SHARDS": "4",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_CONCURRENCY": "2",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_INLINE_SOURCE_EVIDENCE": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE": "true",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_RAW_STAGE": "true",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_EVIDENCE_STAGE": "true",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEEP_RAW_STAGE": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE": "all",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE": "none",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_NETWORK_BRIDGE": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_CODE_BRIDGES": "true",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_ENABLE_INFERENCE": "false",
    }


def _restore_environment(previous: dict[str, str | None]) -> None:
    for name, value in previous.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


async def _benchmark() -> dict[str, object]:
    event_path, row_count = _benchmark_inputs()
    schema = f"eau_alias_bench_{uuid.uuid4().hex}"
    connection = await _connect_migration_probe()
    database = Database()
    environment = _production_env(schema)
    previous_environment = {name: os.environ.get(name) for name in environment}
    runtime_module = None
    old_database = None
    utils_module = None
    old_utils_database = None
    try:
        os.environ.update(environment)
        await database.connect()
        await _upgrade_migration_probe(connection, schema, *_migration_modules())
        runtime_module, mrf_address_model = _load_runtime_modules(schema)
        old_database = runtime_module.db
        utils_module = importlib.import_module("process.ext.utils")
        old_utils_database = utils_module.db
        runtime_module.db = database
        utils_module.db = database
        globals()["entity_address_unified"] = runtime_module
        first_line_by_case = await _seed_alias_cases(database, schema)
        await _seed_production_sources(database, schema, row_count, mrf_address_model)
        live_table_names = _published_table_names(runtime_module)
        old_relations = await _seed_live_markers(database, schema, live_table_names)
        ctx, pipeline_seconds, shutdown_seconds, integrity_seconds = (
            await _run_production_lifecycle(runtime_module, row_count)
        )
        published = await _published_snapshot(database, schema, live_table_names)
        output_rows = published["rows"]
        output_digests = published["digests"]
        output_relations = published["relations"]
        assert isinstance(output_rows, dict)
        assert isinstance(output_digests, dict)
        assert isinstance(output_relations, dict)
        assert output_rows[live_table_names[0]] == row_count
        assert output_rows["entity_address_evidence"] > 0
        assert all(
            relation["persistence"] == "p" for relation in output_relations.values()
        )
        old_after_swap = {
            table_name: await _relation_identity(
                database, schema, f"{table_name}_old"
            )
            for table_name in live_table_names
        }
        assert all(old_after_swap.values())
        atomic_swap_oids_valid = all(
            old_after_swap[table_name]["oid"] == old_relations[table_name]["oid"]
            and old_after_swap[table_name]["oid"] != output_relations[table_name]["oid"]
            for table_name in live_table_names
        )
        assert atomic_swap_oids_valid
        import_date = str(ctx["import_date"])
        stage_residue = await _artifact_residue(
            database, schema, runtime_module, import_date
        )
        assert stage_residue == 0

        deterministic_first, violation_tuples = await _exercise_violation_cases(
            database,
            schema,
            first_line_by_case,
        )
        await _drop_relations(
            database,
            schema,
            [f"{table_name}_old" for table_name in live_table_names],
        )
        failure = await _run_failure_containment(
            database, schema, runtime_module, row_count, connection
        )
        assert all(
            failure[key]
            for key in (
                "live_unchanged",
                "old_swap_absent",
                "private_artifacts_cleaned",
                "drained_sessions",
            )
        )

        event_by_field = {
            "schema_version": 1,
            "correctness": {
                "clean_rows": output_rows[live_table_names[0]],
                "main_output_digest": output_digests[live_table_names[0]],
                "main_output_rows": output_rows[live_table_names[0]],
                "support_output_digests": {
                    table_name: output_digests[table_name]
                    for table_name in live_table_names[1:]
                },
                "support_output_rows": {
                    table_name: output_rows[table_name]
                    for table_name in live_table_names[1:]
                },
                "output_persistence": {
                    table_name: output_relations[table_name]["persistence"]
                    for table_name in live_table_names
                },
                "atomic_swap_oids_valid": atomic_swap_oids_valid,
                "stage_artifacts_cleaned": stage_residue == 0,
                "deterministic_first_violation": deterministic_first,
                "violation_tuples": violation_tuples,
                "failure_live_unchanged": failure["live_unchanged"],
                "failure_no_old_swap": failure["old_swap_absent"],
                "failure_private_artifacts_cleaned": failure[
                    "private_artifacts_cleaned"
                ],
                "failure_drained_sessions": failure["drained_sessions"],
            },
            "metrics": {
                "alias_integrity_seconds": round(integrity_seconds, 6),
                "aggregate_seconds": _phase_seconds(ctx, "aggregating"),
                "evidence_seconds": _phase_seconds(ctx, "source evidence"),
                "support_seconds": _phase_seconds(ctx, "building support"),
                "index_seconds": _phase_seconds(ctx, "indexing"),
                "shutdown_seconds": round(shutdown_seconds, 6),
                "pipeline_seconds": round(pipeline_seconds, 6),
            },
        }
        Path(event_path).write_text(
            json.dumps(event_by_field, sort_keys=True) + "\n", encoding="utf-8"
        )
        return event_by_field
    finally:
        if runtime_module is not None and old_database is not None:
            runtime_module.db = old_database
        if utils_module is not None and old_utils_database is not None:
            utils_module.db = old_utils_database
        _restore_environment(previous_environment)
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()
        await database.disconnect()


if __name__ == "__main__":
    asyncio.run(_benchmark())
