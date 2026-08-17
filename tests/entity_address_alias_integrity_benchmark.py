# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL benchmark for EntityAddressUnified alias integrity."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
from pathlib import Path
import uuid

import asyncpg
from db.connection import Database

from tests.entity_address_alias_integrity_benchmark_lifecycle import (
    _artifact_residue,
    _drop_relations,
    _load_runtime_modules,
    _phase_seconds,
    _published_snapshot,
    _published_table_names,
    _relation_identity,
    _run_failure_containment,
    _run_production_lifecycle,
    _seed_live_markers,
)
from tests.entity_address_alias_integrity_benchmark_setup import (
    _exercise_violation_cases,
    _seed_alias_cases,
    _seed_production_sources,
)


ROOT = Path(__file__).resolve().parents[1]

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


def _benchmark_inputs() -> tuple[str, int]:
    event_path = os.getenv("ENDURANT_BENCHMARK_EVENT_PATH")
    if not event_path:
        raise RuntimeError("ENDURANT_BENCHMARK_EVENT_PATH is required")
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.lower():
        raise RuntimeError("HLTHPRT_DB_DATABASE must identify a disposable test database")
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
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE": "all",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_NETWORK_BRIDGE": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_CODE_BRIDGES": "true",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE": "false",
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_ENABLE_INFERENCE": "false",
    }


def _restore_environment(environment_before_by_name: dict[str, str | None]) -> None:
    for name, value in environment_before_by_name.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


def _bind_runtime_databases(runtime_module, database: Database):
    utils_module = importlib.import_module("process.ext.utils")
    previous_runtime_database = runtime_module.db
    previous_utils_database = utils_module.db
    runtime_module.db = database
    utils_module.db = database
    return utils_module, previous_runtime_database, previous_utils_database


def _restore_runtime_databases(
    runtime_module,
    utils_module,
    previous_runtime_database,
    previous_utils_database,
) -> None:
    runtime_module.db = previous_runtime_database
    utils_module.db = previous_utils_database


def _has_valid_published_outputs(
    published_snapshot_by_field: dict[str, object],
    live_table_names: list[str],
    row_count: int,
    marker_relation_by_table: dict[str, dict[str, object]],
    old_relation_by_table: dict[str, dict[str, object]],
) -> bool:
    row_count_by_table = published_snapshot_by_field["rows"]
    relation_by_table = published_snapshot_by_field["relations"]
    index_state_by_table = published_snapshot_by_field["indexes"]
    assert isinstance(row_count_by_table, dict)
    assert isinstance(relation_by_table, dict)
    assert isinstance(index_state_by_table, dict)
    assert row_count_by_table[live_table_names[0]] == row_count
    assert row_count_by_table["entity_address_evidence"] > 0
    assert all(relation["persistence"] == "p" for relation in relation_by_table.values())
    assert all(
        state["count"] > 0 and state["valid_ready_live"]
        for state in index_state_by_table.values()
    )
    return all(
        old_relation_by_table[table_name]["oid"]
        == marker_relation_by_table[table_name]["oid"]
        and old_relation_by_table[table_name]["oid"]
        != relation_by_table[table_name]["oid"]
        for table_name in live_table_names
    )


async def _run_success_case(
    database: Database, schema: str, runtime_module, mrf_address_model, row_count: int
) -> dict[str, object]:
    first_line_by_case = await _seed_alias_cases(database, schema)
    await _seed_production_sources(database, schema, row_count, mrf_address_model)
    live_table_names = _published_table_names(runtime_module)
    marker_relation_by_table = await _seed_live_markers(
        database, schema, live_table_names
    )
    run_context_map, pipeline_seconds, shutdown_seconds, integrity_seconds = (
        await _run_production_lifecycle(runtime_module, row_count)
    )
    published_snapshot_by_field = await _published_snapshot(
        database, schema, live_table_names
    )
    old_relation_by_table = {
        table_name: await _relation_identity(database, schema, f"{table_name}_old")
        for table_name in live_table_names
    }
    assert all(old_relation_by_table.values())
    has_valid_atomic_swap = _has_valid_published_outputs(
        published_snapshot_by_field,
        live_table_names,
        row_count,
        marker_relation_by_table,
        old_relation_by_table,
    )
    assert has_valid_atomic_swap
    stage_residue = await _artifact_residue(
        database,
        schema,
        runtime_module,
        str(run_context_map["import_date"]),
    )
    assert stage_residue == 0
    return {
        "first_line_by_case": first_line_by_case,
        "live_table_names": live_table_names,
        "published_snapshot_by_field": published_snapshot_by_field,
        "run_context_map": run_context_map,
        "pipeline_seconds": pipeline_seconds,
        "shutdown_seconds": shutdown_seconds,
        "integrity_seconds": integrity_seconds,
        "atomic_swap_oids_valid": has_valid_atomic_swap,
        "stage_artifacts_cleaned": stage_residue == 0,
    }


async def _run_failure_case(
    database: Database,
    schema: str,
    runtime_module,
    row_count: int,
    connection,
    success_case_by_name: dict[str, object],
) -> dict[str, object]:
    first_line_by_case = success_case_by_name["first_line_by_case"]
    live_table_names = success_case_by_name["live_table_names"]
    assert isinstance(first_line_by_case, dict)
    assert isinstance(live_table_names, list)
    deterministic_first, violation_tuples = await _exercise_violation_cases(
        database, runtime_module, schema, first_line_by_case
    )
    await _drop_relations(
        database,
        schema,
        [f"{table_name}_old" for table_name in live_table_names],
    )
    failure_result_by_name = await _run_failure_containment(
        database, schema, runtime_module, row_count, connection
    )
    assert all(
        failure_result_by_name[key]
        for key in (
            "live_outputs_unchanged",
            "old_swaps_absent",
            "benchmark_teardown_zero",
            "drained_sessions",
        )
    )
    assert failure_result_by_name["private_artifacts_pre_teardown_count"] > 0
    return {
        "deterministic_first": deterministic_first,
        "violation_tuples": violation_tuples,
        "failure_result_by_name": failure_result_by_name,
    }


def _correctness_event(
    success_case_by_name: dict[str, object], failure_case_by_name: dict[str, object]
) -> dict[str, object]:
    published_snapshot_by_field = success_case_by_name["published_snapshot_by_field"]
    live_table_names = success_case_by_name["live_table_names"]
    failure_result_by_name = failure_case_by_name["failure_result_by_name"]
    assert isinstance(published_snapshot_by_field, dict)
    assert isinstance(live_table_names, list)
    assert isinstance(failure_result_by_name, dict)
    row_count_by_table = published_snapshot_by_field["rows"]
    digest_by_table = published_snapshot_by_field["digests"]
    relation_by_table = published_snapshot_by_field["relations"]
    assert isinstance(row_count_by_table, dict)
    assert isinstance(digest_by_table, dict)
    assert isinstance(relation_by_table, dict)
    return {
        "clean_rows": row_count_by_table[live_table_names[0]],
        "main_output_digest": digest_by_table[live_table_names[0]],
        "main_output_rows": row_count_by_table[live_table_names[0]],
        "support_output_digests": {
            table_name: digest_by_table[table_name] for table_name in live_table_names[1:]
        },
        "support_output_rows": {
            table_name: row_count_by_table[table_name] for table_name in live_table_names[1:]
        },
        "output_persistence": {
            table_name: relation_by_table[table_name]["persistence"]
            for table_name in live_table_names
        },
        "published_index_state": published_snapshot_by_field["indexes"],
        "atomic_swap_oids_valid": success_case_by_name["atomic_swap_oids_valid"],
        "stage_artifacts_cleaned": success_case_by_name["stage_artifacts_cleaned"],
        "deterministic_first_violation": failure_case_by_name["deterministic_first"],
        "violation_tuples": failure_case_by_name["violation_tuples"],
        "failure_live_outputs_unchanged": failure_result_by_name[
            "live_outputs_unchanged"
        ],
        "failure_old_swaps_absent": failure_result_by_name["old_swaps_absent"],
        "failure_private_artifacts_pre_teardown_count": failure_result_by_name[
            "private_artifacts_pre_teardown_count"
        ],
        "failure_benchmark_teardown_zero": failure_result_by_name[
            "benchmark_teardown_zero"
        ],
        "failure_drained_sessions": failure_result_by_name["drained_sessions"],
    }


def _metrics_event(success_case_by_name: dict[str, object]) -> dict[str, float]:
    run_context_map = success_case_by_name["run_context_map"]
    assert isinstance(run_context_map, dict)
    return {
        "alias_integrity_seconds": round(success_case_by_name["integrity_seconds"], 6),
        "aggregate_seconds": _phase_seconds(run_context_map, "aggregating"),
        "evidence_seconds": _phase_seconds(run_context_map, "source evidence"),
        "support_seconds": _phase_seconds(run_context_map, "building support"),
        "index_seconds": _phase_seconds(run_context_map, "indexing"),
        "shutdown_seconds": round(success_case_by_name["shutdown_seconds"], 6),
        "pipeline_seconds": round(success_case_by_name["pipeline_seconds"], 6),
    }


def _benchmark_event(
    success_case_by_name: dict[str, object], failure_case_by_name: dict[str, object]
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "correctness": _correctness_event(success_case_by_name, failure_case_by_name),
        "metrics": _metrics_event(success_case_by_name),
    }


async def _benchmark() -> dict[str, object]:
    event_path, row_count = _benchmark_inputs()
    schema = f"eau_alias_bench_{uuid.uuid4().hex}"
    connection = await _connect_migration_probe()
    database = Database()
    environment_by_name = _production_env(schema)
    environment_before_by_name = {
        name: os.environ.get(name) for name in environment_by_name
    }
    runtime_module = utils_module = previous_runtime_database = previous_utils_database = None
    try:
        os.environ.update(environment_by_name)
        await database.connect()
        await _upgrade_migration_probe(connection, schema, *_migration_modules())
        runtime_module, mrf_address_model = _load_runtime_modules(schema)
        (
            utils_module,
            previous_runtime_database,
            previous_utils_database,
        ) = _bind_runtime_databases(runtime_module, database)
        success_case_by_name = await _run_success_case(
            database, schema, runtime_module, mrf_address_model, row_count
        )
        failure_case_by_name = await _run_failure_case(
            database,
            schema,
            runtime_module,
            row_count,
            connection,
            success_case_by_name,
        )
        event_by_field = _benchmark_event(success_case_by_name, failure_case_by_name)
        Path(event_path).write_text(
            json.dumps(event_by_field, sort_keys=True) + "\n", encoding="utf-8"
        )
        return event_by_field
    finally:
        if runtime_module is not None and utils_module is not None:
            _restore_runtime_databases(
                runtime_module,
                utils_module,
                previous_runtime_database,
                previous_utils_database,
            )
        _restore_environment(environment_before_by_name)
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()
        await database.disconnect()


if __name__ == "__main__":
    asyncio.run(_benchmark())
