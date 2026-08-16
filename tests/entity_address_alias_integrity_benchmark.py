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

from db.connection import Database
from tests.test_address_numeric_grid_alias_db import _PUBLIC_EVIDENCE_NPI_SQL
from tests.test_address_numeric_grid_alias_db import ROOT, _load_module, asyncpg


entity_address_unified = importlib.import_module("process.entity_address_unified")

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
) -> None:
    statements = [
        f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;',
        f'CREATE SCHEMA "{probe_schema}";',
        foundation._create_functions_sql(probe_schema),
        foundation._create_archive_sql(probe_schema),
        f'CREATE TABLE "{probe_schema}".partd_pharmacy_activity_stage_v2 '
        "(id bigint PRIMARY KEY);",
        migration._numeric_grid_function_sql(probe_schema),
        *migration._split_sql_statements(migration._alias_schema_sql(probe_schema)),
        _PUBLIC_EVIDENCE_NPI_SQL.format(schema=probe_schema),
        *evidence_migration._upgrade_statements(probe_schema),
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
    schema: str,
    table_name: str,
    expected_violation: str | None,
) -> tuple[float, str | None]:
    integrity_started = time.perf_counter()
    validator_options_by_name = {}
    if "checksum_ranges" in inspect.signature(
        entity_address_unified._validate_raw_alias_integrity
    ).parameters:
        validator_options_by_name = {
            "checksum_ranges": entity_address_unified._integer_ranges(
                -(2**31), 2**31 - 1, 64
            ),
            "concurrency": 24,
        }
    try:
        await entity_address_unified._validate_raw_alias_integrity(
            schema,
            table_name,
            is_address_canon_available=True,
            **validator_options_by_name,
        )
    except RuntimeError as error:
        integrity_seconds = time.perf_counter() - integrity_started
        if expected_violation is None or f"kind={expected_violation}" not in str(
            error
        ):
            raise
        return integrity_seconds, expected_violation
    integrity_seconds = time.perf_counter() - integrity_started
    if expected_violation is not None:
        raise AssertionError(f"expected alias violation {expected_violation}")
    return integrity_seconds, None


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
) -> str | None:
    for index, violation_kind in enumerate(_VIOLATION_KINDS):
        source_sql = _source_select(first_line_by_case[violation_kind])
        await _run_pipeline(
            database,
            schema,
            f"violation_{index}_raw",
            f"{source_sql} UNION ALL {source_sql}",
            expected_violation=violation_kind,
        )
    combined_source_sql = " UNION ALL ".join(
        _source_select(first_line_by_case[violation_kind])
        for violation_kind in _VIOLATION_KINDS
        for _ in range(2)
    )
    _, _, deterministic_first, _ = await _run_pipeline(
        database,
        schema,
        "combined_violation_raw",
        combined_source_sql,
        expected_violation="missing_or_merged_target",
    )
    return deterministic_first


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


async def _benchmark() -> dict[str, object]:
    event_path, row_count = _benchmark_inputs()
    schema = f"eau_alias_bench_{uuid.uuid4().hex}"
    connection = await _connect_migration_probe()
    database = Database()
    old_database = entity_address_unified.db
    try:
        await database.connect()
        await _upgrade_migration_probe(connection, schema, *_migration_modules())
        entity_address_unified.db = database
        first_line_by_case = await _seed_alias_cases(database, schema)

        pipeline_seconds, integrity_seconds, enriched_digest, clean_rows = await _run_pipeline(
            database,
            schema,
            "clean_raw",
            _source_select(first_line_by_case["clean"], row_count=row_count),
        )
        assert enriched_digest is not None

        deterministic_first = await _exercise_violation_cases(
            database,
            schema,
            first_line_by_case,
        )
        has_forced_failure_cleanup = await _has_forced_failure_cleanup(
            database,
            schema,
            first_line_by_case["clean"],
        )
        assert has_forced_failure_cleanup

        event_by_field = {
            "schema_version": 1,
            "correctness": {
                "clean_rows": clean_rows,
                "deterministic_first_violation": deterministic_first,
                "enriched_digest": enriched_digest,
                "forced_failure_cleanup": has_forced_failure_cleanup,
            },
            "metrics": {
                "alias_integrity_seconds": integrity_seconds,
                "pipeline_seconds": pipeline_seconds,
            },
        }
        Path(event_path).write_text(
            json.dumps(event_by_field, sort_keys=True) + "\n", encoding="utf-8"
        )
        return event_by_field
    finally:
        entity_address_unified.db = old_database
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()
        await database.disconnect()


if __name__ == "__main__":
    asyncio.run(_benchmark())
