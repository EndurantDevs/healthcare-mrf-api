# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic inputs and alias checks for the EAU benchmark."""

from __future__ import annotations

import inspect
import time

from db.connection import Database


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


def _source_select(first_line: str, *, row_count: int = 1) -> str:
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
    await _insert_alias(database, schema, *identity_by_case["multi_hop_alias"], *hop_target)
    await _insert_alias(database, schema, *hop_target, *hop_final)
    return first_line_by_case


async def _seed_production_alias(database: Database, schema: str) -> str:
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
    return source_key


async def _seed_remaining_archive_addresses(
    database: Database, schema: str, row_count: int
) -> None:
    if row_count <= 1:
        return
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


async def _seed_mrf_source_rows(
    database: Database, schema: str, row_count: int, source_key: str
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.mrf_address (
            checksum, npi, type, first_line, second_line, city_name,
            state_name, postal_code, country_code, address_key
        )
        SELECT
            value, 1000000000 + value, 'practice',
            CASE WHEN value = 1 THEN '1000 Alias Source Road'
                 ELSE format('%s Synthetic Benchmark Road', 1000 + value) END,
            NULL, 'Example City', 'TX', '75001', 'US',
            CASE WHEN value = 1 THEN CAST(:source_key AS uuid)
                 ELSE {schema}.addr_key_v1(
                     format('%s Synthetic Benchmark Road', 1000 + value),
                     NULL, 'Example City', 'TX', '75001', 'US'
                 ) END
          FROM generate_series(1, {row_count}) AS values(value)
        ON CONFLICT DO NOTHING;
        """,
        source_key=source_key,
    )


async def _seed_production_sources(
    database: Database, schema: str, row_count: int, mrf_address_model
) -> None:
    await database.create_table(mrf_address_model.__table__, checkfirst=True)
    source_key = await _seed_production_alias(database, schema)
    await _seed_remaining_archive_addresses(database, schema, row_count)
    await _seed_mrf_source_rows(database, schema, row_count, source_key)


async def _prepare_raw_pipeline(
    database: Database,
    runtime_module,
    schema: str,
    table_name: str,
    source_sql: str,
) -> None:
    await database.status(runtime_module._prepare_raw_stage_sql(schema, table_name))
    await database.status(
        runtime_module._insert_raw_from_source_sql(schema, table_name, source_sql)
    )
    await database.status(
        f"CREATE INDEX {table_name}_idx_checksum ON {schema}.{table_name} (checksum);"
    )
    await database.status(f"ANALYZE {schema}.{table_name};")
    await database.status(
        runtime_module._enrich_raw_stage_sql(schema, table_name, archive_available=True)
    )


async def _measure_alias_integrity(
    runtime_module, schema: str, table_name: str, expected_violation: str | None
) -> tuple[float, str | None]:
    integrity_started = time.perf_counter()
    validator = runtime_module._validate_raw_alias_integrity
    enrich_shards = runtime_module._env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS",
        runtime_module.DEFAULT_ENRICH_SHARDS,
        1,
    )
    validator_parameters = inspect.signature(validator).parameters
    validator_options_by_name = {"is_address_canon_available": True}
    if "checksum_ranges" in validator_parameters:
        validator_options_by_name["checksum_ranges"] = (
            runtime_module._integer_ranges(-(2**31), 2**31 - 1, enrich_shards)
            if enrich_shards > 1
            else None
        )
    if "concurrency" in validator_parameters:
        validator_options_by_name["concurrency"] = min(
            runtime_module._env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY",
                runtime_module.DEFAULT_ENRICH_CONCURRENCY,
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
    runtime_module,
    schema: str,
    table_name: str,
    source_sql: str,
    *,
    expected_violation: str | None = None,
) -> tuple[float, float, str | None, int]:
    started = time.perf_counter()
    try:
        await _prepare_raw_pipeline(
            database, runtime_module, schema, table_name, source_sql
        )
        integrity_seconds, observed_violation = await _measure_alias_integrity(
            runtime_module, schema, table_name, expected_violation
        )
        pipeline_seconds = time.perf_counter() - started
        observed_rows = int(
            await database.scalar(f"SELECT COUNT(*) FROM {schema}.{table_name};") or 0
        )
        if observed_violation is not None:
            return pipeline_seconds, integrity_seconds, observed_violation, observed_rows
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
        return pipeline_seconds, integrity_seconds, str(enriched_digest), observed_rows
    finally:
        await database.status(f"DROP TABLE IF EXISTS {schema}.{table_name};")


async def _exercise_violation_cases(
    database: Database,
    runtime_module,
    schema: str,
    first_line_by_case: dict[str, str],
) -> tuple[dict[str, str | None], list[dict[str, str | None]]]:
    violation_tuples: list[dict[str, str | None]] = []
    for index, violation_kind in enumerate(_VIOLATION_KINDS):
        source_sql = _source_select(first_line_by_case[violation_kind])
        await _run_pipeline(
            database,
            runtime_module,
            schema,
            f"violation_{index}_raw",
            f"{source_sql} UNION ALL {source_sql}",
            expected_violation=violation_kind,
        )
        violation_tuples.append(
            await _violation_tuple(
                database, schema, first_line_by_case[violation_kind]
            )
        )
    combined_source_sql = " UNION ALL ".join(
        _source_select(first_line_by_case[violation_kind])
        for violation_kind in _VIOLATION_KINDS
        for _ in range(2)
    )
    _, _, deterministic_kind, _ = await _run_pipeline(
        database,
        runtime_module,
        schema,
        "combined_violation_raw",
        combined_source_sql,
        expected_violation="missing_or_merged_target",
    )
    assert deterministic_kind == "missing_or_merged_target"
    return next(
        violation_record
        for violation_record in violation_tuples
        if violation_record["kind"] == deterministic_kind
    ), violation_tuples


async def _violation_tuple(
    database: Database, schema: str, first_line: str
) -> dict[str, str | None]:
    violation_row = await database.first(
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
    assert violation_row is not None
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
        "source_address_key": str(violation_row.source_address_key),
        "target_address_key": str(violation_row.target_address_key),
        "source_identity_key": str(violation_row.source_identity_key),
        "target_identity_key": str(violation_row.target_identity_key),
        "current_source_identity_key": str(violation_row.current_source_identity_key),
        "current_target_identity_key": (
            str(violation_row.current_target_identity_key)
            if violation_row.current_target_identity_key is not None
            else None
        ),
    }
