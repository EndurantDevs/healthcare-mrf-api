# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL cases for reviewed numeric-grid address aliases."""

from tests.test_address_numeric_grid_alias_db import (
    ROOT,
    _insert_archive_address,
    _load_module,
    _mark_failed,
    _prepare_serving_fence_schema,
    _replace_overlay_relation,
    _requires_test_database,
    _reset_alias_data,
    address_alias_sql,
    address_canon,
    address_strict_source_backfill_sql,
    asyncio,
    asyncpg,
    db,
    entity_address_unified,
    json,
    os,
    provider_directory,
    pytest,
    resolve_into_archive,
    revoke_numeric_grid_alias,
    run_numeric_grid_alias,
    run_strict_source_backfill,
    stamp_address_keys,
    suppress,
)

async def _prepare_materializer_alias() -> tuple[str, str, str]:
    """Apply one reviewed alias and return its schema and endpoint keys."""
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    source_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500",
        second_line="Suite 202",
        strict_source_bits=1,
    )
    target_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500 S",
        second_line="Suite 202",
        strict_source_bits=6,
    )
    shadow = await run_numeric_grid_alias(mode="shadow", schema=schema)
    await run_numeric_grid_alias(
        mode="apply",
        schema=schema,
        alias_run_id=shadow.run_id,
        expected_candidate_sha256=shadow.candidate_digest,
        reviewed_by="ci-reviewer",
    )
    return schema, source_key, target_key


async def _assert_overlay_alias_materialization(
    schema: str,
    source_key: str,
    target_key: str,
) -> None:
    """Build a compact overlay stage and prove its source key is rewritten."""
    overlay_stage = "numeric_grid_alias_overlay_stage"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{overlay_stage};")
    await db.status(
        provider_directory.provider_directory_address_overlay_table_sql(
            schema,
            overlay_stage,
        )
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{overlay_stage} (
            source_record_id,
            source_id,
            resource_type,
            resource_id,
            npi,
            address_key,
            first_line,
            second_line,
            city_name,
            state_name,
            state_code,
            postal_code,
            country_code
        ) VALUES (
            'synthetic:overlay:1',
            'synthetic-source',
            'Location',
            'synthetic-location',
            1000000001,
            CAST(:source_key AS uuid),
            '1548 E 4500',
            'Suite 202',
            'Example City',
            'TX',
            'TX',
            '75001',
            'US'
        );
        """,
        source_key=source_key,
    )
    overlay_metrics = await provider_directory._materialize_address_overlay_aliases(
        schema,
        f'"{schema}"."{overlay_stage}"',
    )
    assert overlay_metrics["alias_candidates"] == 1
    assert overlay_metrics["aliases_materialized"] == 1
    assert overlay_metrics["alias_residual_source_keys"] == 0
    assert await db.scalar(
        f"SELECT address_key::text FROM {schema}.{overlay_stage};"
    ) == target_key


_UNIFIED_ALIAS_SOURCE_SQL = """
    SELECT
        'npi'::varchar AS entity_type,
        '1000000001'::varchar AS entity_id,
        1000000001::bigint AS npi,
        NULL::bigint AS inferred_npi,
        NULL::float8 AS inference_confidence,
        NULL::varchar AS inference_method,
        'Synthetic Provider'::varchar AS entity_name,
        NULL::varchar AS entity_subtype,
        'primary'::varchar AS type,
        ARRAY[0]::int[] AS taxonomy_array,
        ARRAY[0]::int[] AS plans_network_array,
        ARRAY[0]::int[] AS procedures_array,
        ARRAY[0]::int[] AS medications_array,
        ARRAY[]::varchar[] AS aca_plan_array,
        ARRAY[]::varchar[] AS aca_network_array,
        ARRAY[]::varchar[] AS ptg_plan_array,
        ARRAY[]::varchar[] AS ptg_source_array,
        ARRAY[]::varchar[] AS group_plan_array,
        'address_archive_v2:v2'::varchar AS base_address_version,
        '1548 E 4500'::varchar AS first_line,
        'Suite 202'::varchar AS second_line,
        'Example City'::varchar AS city_name,
        'TX'::varchar AS state_name,
        '75001'::varchar AS postal_code,
        'US'::varchar AS country_code,
        NULL::varchar AS telephone_number,
        NULL::varchar AS fax_number,
        NULL::varchar AS formatted_address,
        NULL::numeric AS lat,
        NULL::numeric AS long,
        NULL::date AS date_added,
        NULL::varchar AS place_id,
        CAST('{source_key}' AS uuid) AS address_key,
        NOW()::timestamp AS updated_at,
        'synthetic'::varchar AS address_source,
        'synthetic:1'::varchar AS source_record_id
"""


async def _assert_unified_alias_materialization(
    schema: str,
    source_key: str,
    target_key: str,
) -> None:
    """Enrich a unified raw stage and prove its key and receipt are rewritten."""
    raw_table = "numeric_grid_alias_unified_raw"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{raw_table};")
    await db.status(
        entity_address_unified._prepare_raw_stage_sql(schema, raw_table)
    )
    await db.status(
        entity_address_unified._insert_raw_from_source_sql(
            schema,
            raw_table,
            _UNIFIED_ALIAS_SOURCE_SQL.format(source_key=source_key),
        )
    )
    await entity_address_unified._validate_raw_alias_integrity(
        schema,
        raw_table,
        is_address_canon_available=True,
    )
    await db.status(
        entity_address_unified._enrich_raw_stage_sql(
            schema,
            raw_table,
            archive_available=True,
            is_address_canon_available=True,
        )
    )
    unified_row = await db.first(
        f"""
        SELECT address_key::text, base_address_version
        FROM {schema}.{raw_table};
        """
    )
    assert unified_row is not None
    assert unified_row.address_key == target_key
    assert unified_row.base_address_version.endswith("+alias-v1:g1")


@pytest.mark.asyncio(loop_scope="session")
async def test_offline_serving_materializers_rewrite_aliases():
    """Both offline serving artifacts consume the same reviewed alias set."""
    _requires_test_database()
    schema, source_key, target_key = await _prepare_materializer_alias()
    await _assert_overlay_alias_materialization(schema, source_key, target_key)
    await _assert_unified_alias_materialization(schema, source_key, target_key)



def _migration_modules():
    """Load foundation and numeric-grid migrations without Alembic globals."""
    foundation = _load_module(
        ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py",
        "address_alias_foundation_probe",
    )
    migration = _load_module(
        ROOT / "alembic/versions/20260811100000_address_numeric_grid_alias.py",
        "address_alias_migration_probe",
    )
    return foundation, migration


async def _connect_migration_probe():
    """Open the disposable PostgreSQL connection used by migration proof."""
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
) -> None:
    """Create the prior schema and execute the migration SQL in order."""
    await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
    await connection.execute(f'CREATE SCHEMA "{probe_schema}";')
    await connection.execute(foundation._create_functions_sql(probe_schema))
    await connection.execute(foundation._create_archive_sql(probe_schema))
    await connection.execute(
        f'CREATE TABLE "{probe_schema}".partd_pharmacy_activity_stage_v2 '
        "(id bigint PRIMARY KEY);"
    )
    await connection.execute(migration._numeric_grid_function_sql(probe_schema))
    for statement in migration._split_sql_statements(
        migration._alias_schema_sql(probe_schema)
    ):
        await connection.execute(statement)


async def _assert_migration_upgrade(connection, probe_schema: str) -> None:
    """Verify alias tables, receipts, and Part D lineage column after upgrade."""
    assert await connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL;",
        f"{probe_schema}.address_alias_v1",
    )
    artifact_rows = await connection.fetch(
        f"""
        SELECT artifact_name, generation
        FROM "{probe_schema}".address_alias_artifact_state_v1
        ORDER BY artifact_name;
        """
    )
    assert [
        (artifact_row["artifact_name"], artifact_row["generation"])
        for artifact_row in artifact_rows
    ] == [
        ("provider_directory_address_corroboration", 0),
        ("provider_directory_address_overlay", 0),
    ]
    assert await connection.fetchval(
        """
        SELECT is_nullable = 'NO'
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'partd_pharmacy_activity_stage_v2'
          AND column_name = 'address_observed_in_source';
        """,
        probe_schema,
    )


async def _assert_migration_downgrade(connection, probe_schema: str) -> None:
    """Verify downgrade removes aliases and both strict-lineage columns."""
    assert not await connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL;",
        f"{probe_schema}.address_alias_v1",
    )
    for table_name, column_name in (
        ("address_archive_v2", "strict_source_bits"),
        ("partd_pharmacy_activity_stage_v2", "address_observed_in_source"),
    ):
        assert not await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = $2
                  AND column_name = $3
            );
            """,
            probe_schema,
            table_name,
            column_name,
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_migration_upgrade_and_downgrade_execute_on_postgresql():
    """The exact PostgreSQL upgrade and downgrade remain executable."""
    _requires_test_database()
    probe_schema = "address_alias_migration_probe"
    foundation, migration = _migration_modules()
    connection = await _connect_migration_probe()
    try:
        await _upgrade_migration_probe(connection, probe_schema, foundation, migration)
        await _assert_migration_upgrade(connection, probe_schema)
        for statement in migration._downgrade_statements(probe_schema):
            await connection.execute(statement)
        await _assert_migration_downgrade(connection, probe_schema)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.close()
