# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Production lifecycle and publication evidence for the EAU benchmark."""

from __future__ import annotations

import importlib
import os
import time

from db.connection import Database


def _load_runtime_modules(schema: str):
    """Import production EAU models after fixing the disposable schema."""
    os.environ["DB_SCHEMA"] = schema
    os.environ["HLTHPRT_DB_SCHEMA"] = schema
    runtime_module = importlib.import_module("process.entity_address_unified")
    from db.models import MRFAddress

    return runtime_module, MRFAddress


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
    return [stage_class.__tablename__ for stage_class in stage_classes]


async def _relation_identity(database: Database, schema: str, table_name: str):
    relation_row = await database.first(
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
    if relation_row is None:
        return None
    return {
        "oid": int(relation_row.relation_oid),
        "persistence": str(relation_row.persistence),
    }


async def _table_digest(database: Database, schema: str, table_name: str) -> str:
    digest_value = await database.scalar(
        f"""
        SELECT md5(COALESCE(
            string_agg(
                (to_jsonb(record) - ARRAY[
                    'updated_at', 'last_seen_at', 'observed_at',
                    'first_seen_at', 'geocoded_at', 'created_at',
                    'published_at', 'retired_at', 'evidence_id'
                ])::text,
                E'\\n' ORDER BY (
                    to_jsonb(record) - ARRAY[
                        'updated_at', 'last_seen_at', 'observed_at',
                        'first_seen_at', 'geocoded_at', 'created_at',
                        'published_at', 'retired_at', 'evidence_id'
                    ]
                )::text
            ),
            ''
        ))
        FROM {schema}.{table_name} AS record;
        """
    )
    assert digest_value is not None
    return str(digest_value)


async def _index_state(database: Database, schema: str, table_name: str) -> dict[str, object]:
    index_row = await database.first(
        """
        SELECT count(*)::bigint AS index_count,
               COALESCE(
                   bool_and(ix.indisvalid AND ix.indisready AND ix.indislive),
                   FALSE
               ) AS valid_ready_live
          FROM pg_catalog.pg_index AS ix
          JOIN pg_catalog.pg_class AS tbl ON tbl.oid = ix.indrelid
          JOIN pg_catalog.pg_namespace AS ns ON ns.oid = tbl.relnamespace
         WHERE ns.nspname = :schema
           AND tbl.relname = :table_name;
        """,
        schema=schema,
        table_name=table_name,
    )
    assert index_row is not None
    return {
        "count": int(index_row.index_count),
        "valid_ready_live": bool(index_row.valid_ready_live),
    }


async def _published_snapshot(
    database: Database, schema: str, table_names: list[str]
) -> dict[str, object]:
    row_count_by_table: dict[str, int] = {}
    digest_by_table: dict[str, str] = {}
    relation_by_table: dict[str, dict[str, object]] = {}
    index_state_by_table: dict[str, dict[str, object]] = {}
    for table_name in table_names:
        relation = await _relation_identity(database, schema, table_name)
        assert relation is not None
        row_count_by_table[table_name] = int(
            await database.scalar(f"SELECT COUNT(*) FROM {schema}.{table_name};") or 0
        )
        digest_by_table[table_name] = await _table_digest(database, schema, table_name)
        relation_by_table[table_name] = relation
        index_state_by_table[table_name] = await _index_state(
            database, schema, table_name
        )
    return {
        "rows": row_count_by_table,
        "digests": digest_by_table,
        "relations": relation_by_table,
        "indexes": index_state_by_table,
    }


async def _drop_relations(
    database: Database, schema: str, table_names: list[str]
) -> None:
    for table_name in table_names:
        await database.status(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")


def _phase_seconds(run_context_map: dict, *fragments: str) -> float:
    phase_timings_by_name = run_context_map.get("phase_timings") or (
        run_context_map.get("context") or {}
    ).get("phase_timings") or {}
    return round(
        sum(
            float(entry.get("seconds") or 0.0)
            for phase, entry in phase_timings_by_name.items()
            if any(fragment in phase for fragment in fragments)
        ),
        6,
    )


async def _residue_count(
    database: Database, schema: str, table_names: list[str]
) -> int:
    residue_count = 0
    for table_name in table_names:
        if await _relation_identity(database, schema, table_name) is not None:
            residue_count += 1
    return residue_count


async def _seed_live_markers(
    database: Database, schema: str, table_names: list[str]
) -> dict[str, dict[str, object]]:
    marker_relation_by_table: dict[str, dict[str, object]] = {}
    for table_name in table_names:
        await database.status(
            f"CREATE TABLE {schema}.{table_name} (marker text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {schema}.{table_name} (marker) VALUES ('old');"
        )
        relation = await _relation_identity(database, schema, table_name)
        assert relation is not None
        marker_relation_by_table[table_name] = relation
    return marker_relation_by_table


async def _run_production_lifecycle(
    runtime_module, row_count: int
) -> tuple[dict, float, float, float]:
    run_context_map: dict = {}
    alias_timings: list[float] = []
    original_validator = runtime_module._validate_raw_alias_integrity

    async def _timed_validator(*args, **kwargs):
        started = time.perf_counter()
        try:
            return await original_validator(*args, **kwargs)
        finally:
            alias_timings.append(time.perf_counter() - started)

    runtime_module._validate_raw_alias_integrity = _timed_validator
    started = time.perf_counter()
    try:
        await runtime_module.startup(run_context_map)
        await runtime_module.process_data(
            run_context_map,
            {
                "publish": True,
                "refresh_mode": "full",
                "serving_only_refresh": False,
                "limit_per_source": row_count,
            },
        )
        shutdown_started = time.perf_counter()
        await runtime_module.shutdown(run_context_map)
        shutdown_seconds = time.perf_counter() - shutdown_started
    finally:
        runtime_module._validate_raw_alias_integrity = original_validator
    return run_context_map, time.perf_counter() - started, shutdown_seconds, sum(alias_timings)


def _artifact_table_names(runtime_module, import_date: str) -> list[str]:
    stage_table_names = _stage_table_names(runtime_module, import_date)
    return [
        *stage_table_names,
        runtime_module._raw_stage_table_name(stage_table_names[0]),
        runtime_module._evidence_stage_table_name(stage_table_names[0]),
    ]


async def _artifact_residue(
    database: Database, schema: str, runtime_module, import_date: str
) -> int:
    return await _residue_count(
        database, schema, _artifact_table_names(runtime_module, import_date)
    )


async def _introduce_bad_alias_input(database: Database, schema: str) -> None:
    await database.status(
        f"""
        UPDATE {schema}.mrf_address
           SET first_line = '2000 Bravo Road',
               address_key = {schema}.addr_key_v1(
                   '2000 Bravo Road', NULL, 'Example City', 'TX', '75001', 'US'
               )
         WHERE checksum = 1;
        """
    )


async def _expect_alias_validation_failure(
    runtime_module, failure_context_by_name: dict, row_count: int
) -> None:
    try:
        await runtime_module.process_data(
            failure_context_by_name,
            {
                "publish": True,
                "refresh_mode": "full",
                "serving_only_refresh": False,
                "limit_per_source": row_count,
            },
        )
    except RuntimeError as error:
        if "kind=source_identity_mismatch" not in str(error):
            raise
    else:
        raise AssertionError("bad alias did not fail production validation")


async def _has_no_old_swaps(
    database: Database, schema: str, table_names: list[str]
) -> bool:
    for table_name in table_names:
        if await _relation_identity(database, schema, f"{table_name}_old") is not None:
            return False
    return True


async def _benchmark_teardown(
    database: Database,
    schema: str,
    runtime_module,
    import_date: str,
) -> int:
    stage_table_names = _stage_table_names(runtime_module, import_date)
    stage_class_by_model = {
        model: runtime_module.make_class(model, import_date)
        for model in runtime_module.SUPPORT_TABLE_MODELS
    }
    await runtime_module._drop_stage_artifacts(
        schema,
        runtime_module.make_class(runtime_module.EntityAddressUnified, import_date),
        stage_class_by_model,
    )
    await database.status(
        f"DROP TABLE IF EXISTS {schema}.{runtime_module._raw_stage_table_name(stage_table_names[0])};"
    )
    await database.status(
        f"DROP TABLE IF EXISTS {schema}.{runtime_module._evidence_stage_table_name(stage_table_names[0])};"
    )
    return await _artifact_residue(database, schema, runtime_module, import_date)


async def _has_drained_sessions(connection, schema: str) -> bool:
    active_session_count = await connection.fetchval(
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
    return int(active_session_count or 0) == 0


async def _run_failure_containment(
    database: Database,
    schema: str,
    runtime_module,
    row_count: int,
    connection,
) -> dict[str, object]:
    live_table_names = _published_table_names(runtime_module)
    snapshot_before_by_field = await _published_snapshot(
        database, schema, live_table_names
    )
    failure_context_by_name: dict[str, object] = {}
    private_artifacts_pre_teardown_count = -1
    benchmark_teardown_residue = -1
    snapshot_after_by_field = None
    has_no_old_swaps = False
    try:
        await _introduce_bad_alias_input(database, schema)
        await runtime_module.startup(failure_context_by_name)
        await _expect_alias_validation_failure(
            runtime_module, failure_context_by_name, row_count
        )
        import_date = str(failure_context_by_name["import_date"])
        private_artifacts_pre_teardown_count = await _artifact_residue(
            database, schema, runtime_module, import_date
        )
        snapshot_after_by_field = await _published_snapshot(
            database, schema, live_table_names
        )
        has_no_old_swaps = await _has_no_old_swaps(
            database, schema, live_table_names
        )
    finally:
        if failure_context_by_name.get("import_date"):
            benchmark_teardown_residue = await _benchmark_teardown(
                database,
                schema,
                runtime_module,
                str(failure_context_by_name["import_date"]),
            )
    assert snapshot_after_by_field is not None
    return {
        "live_outputs_unchanged": all(
            snapshot_before_by_field[field] == snapshot_after_by_field[field]
            for field in ("rows", "digests", "relations")
        ),
        "old_swaps_absent": has_no_old_swaps,
        "private_artifacts_pre_teardown_count": private_artifacts_pre_teardown_count,
        "benchmark_teardown_zero": benchmark_teardown_residue == 0,
        "drained_sessions": await _has_drained_sessions(connection, schema),
    }
