# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL planner, concurrency, and lineage tests for address aliases."""

from tests.test_address_numeric_grid_alias_db import (
    ROOT,
    _insert_archive_address,
    _load_module,
    _requires_test_database,
    _reset_alias_data,
    address_canon,
    address_strict_source_backfill_sql,
    asyncio,
    asyncpg,
    db,
    json,
    os,
    pytest,
    resolve_into_archive,
    run_numeric_grid_alias,
    stamp_address_keys,
    suppress,
)
async def _connect_runtime_database():
    """Open a disposable PostgreSQL connection for runtime probes."""
    return await asyncpg.connect(
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.getenv("HLTHPRT_DB_DATABASE"),
    )


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


async def _create_alias_probe_schema(connection, schema: str, module_suffix: str) -> None:
    """Create canonical functions, archive, and alias schema for a probe."""
    foundation = _load_module(
        ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py",
        f"address_alias_{module_suffix}_foundation",
    )
    migration = _load_module(
        ROOT / "alembic/versions/20260811100000_address_numeric_grid_alias.py",
        f"address_alias_{module_suffix}_migration",
    )
    evidence_migration = _load_module(
        ROOT / "alembic/versions/20260816020000_address_evidence_alias.py",
        f"address_alias_{module_suffix}_evidence_migration",
    )
    formatted_migration = _load_module(
        ROOT / "alembic/versions/20260815010000_address_formatted_display_v2.py",
        f"address_alias_{module_suffix}_formatted_migration",
    )
    await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
    await connection.execute(f'CREATE SCHEMA "{schema}";')
    await connection.execute(foundation._create_functions_sql(schema))
    await connection.execute(foundation._create_archive_sql(schema))
    await connection.execute(
        formatted_migration._humanize_component_function_sql(schema)
    )
    await connection.execute(
        formatted_migration._formatted_address_function_sql(schema)
    )
    await connection.execute(
        f"""
        ALTER TABLE "{schema}".address_archive_v2
            ADD COLUMN formatted_address_version smallint,
            ADD COLUMN formatted_address_source varchar(64);
        """
    )
    await connection.execute(migration._numeric_grid_function_sql(schema))
    for statement in migration._split_sql_statements(
        migration._alias_schema_sql(schema)
    ):
        await connection.execute(statement)
    await connection.execute(_PUBLIC_EVIDENCE_NPI_SQL.format(schema=schema))
    for statement in evidence_migration._upgrade_statements(schema):
        await connection.execute(statement)
    await connection.execute(
        f"""
        CREATE TABLE "{schema}".entity_address_unified (
            location_key text PRIMARY KEY,
            npi bigint,
            inferred_npi bigint,
            type varchar(32) NOT NULL,
            address_key uuid,
            address_sources varchar[] NOT NULL DEFAULT '{{}}',
            formatted_address text,
            base_address_version varchar(64) NOT NULL DEFAULT
                'address_archive_v2:v2+fmt-v2+alias-v1:g1'
        );
        """
    )


def _plan_nodes(value):
    """Yield every mapping node from an EXPLAIN JSON tree."""
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _plan_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _plan_nodes(child)


_PLAN_RELATIONS_SQL = """
    CREATE TEMP TABLE address_strict_backfill_targets (
        address_key uuid PRIMARY KEY, identity_key text NOT NULL
    ) ON COMMIT PRESERVE ROWS;
    CREATE TEMP TABLE address_strict_backfill_evidence (
        target_address_key uuid NOT NULL, source_bit integer NOT NULL,
        source_name text NOT NULL,
        PRIMARY KEY (target_address_key, source_bit, source_name)
    ) ON COMMIT PRESERVE ROWS;
    CREATE TABLE "{schema}".npi_address (
        address_key uuid, first_line text, second_line text, city_name text,
        state_name text, postal_code text, country_code text
    );
    CREATE INDEX ON "{schema}".npi_address (address_key);
    CREATE TABLE "{schema}".provider_directory_location (
        address_key text, first_line text, second_line text, city_name text,
        state_name text, state_code text, postal_code text, country_code text
    );
    CREATE INDEX ON "{schema}".provider_directory_location (address_key);
"""


_PLAN_ROWS_SQL = """
    INSERT INTO address_strict_backfill_targets (address_key, identity_key)
    SELECT
        "{schema}".addr_key_v1(
            '1548 E 4500 S', 'Suite 202', 'Example City', 'TX', '75001', 'US'
        ),
        "{schema}".addr_identity_key_v1(
            '1548 E 4500 S', 'Suite 202', 'Example City', 'TX', '75001', 'US'
        );
    INSERT INTO "{schema}".npi_address
    SELECT md5('npi-decoy-' || value::text)::uuid,
           'Decoy ' || value::text, NULL, 'Example City', 'TX', '75001', 'US'
    FROM generate_series(1, 20000) AS value;
    INSERT INTO "{schema}".provider_directory_location
    SELECT md5('location-decoy-' || value::text)::uuid::text,
           'Decoy ' || value::text, NULL, 'Example City',
           'TX', 'TX', '75001', 'US'
    FROM generate_series(1, 20000) AS value;
    INSERT INTO "{schema}".npi_address
    SELECT target.address_key, '1548 E 4500 S', 'Suite 202',
           'Example City', 'TX', '75001', 'US'
    FROM address_strict_backfill_targets AS target;
    INSERT INTO "{schema}".provider_directory_location
    SELECT target.address_key::text, '1548 E 4500 S', 'Suite 202',
           'Example City', 'TX', 'TX', '75001', 'US'
    FROM address_strict_backfill_targets AS target;
    ANALYZE address_strict_backfill_targets;
    ANALYZE "{schema}".npi_address;
    ANALYZE "{schema}".provider_directory_location;
"""


async def _prepare_index_probe(connection, schema: str) -> None:
    """Seed indexed sources with enough decoys to expose sequential scans."""
    await connection.execute(_PLAN_RELATIONS_SQL.format(schema=schema))
    await connection.execute(_PLAN_ROWS_SQL.format(schema=schema))


async def _assert_projection_uses_index(
    connection,
    schema: str,
    projection_name: str,
) -> None:
    """Verify one strict-source projection uses its leading address-key index."""
    projection_by_name = {
        projection.name: projection
        for projection in address_strict_source_backfill_sql.SOURCE_PROJECTIONS
    }
    projection = projection_by_name[projection_name]
    evidence_sql = address_strict_source_backfill_sql.evidence_insert_sql(
        schema=schema,
        projection=projection,
    ).strip().rstrip(";")
    encoded_plan = await connection.fetchval(f"EXPLAIN (FORMAT JSON) {evidence_sql}")
    plan = json.loads(encoded_plan) if isinstance(encoded_plan, str) else encoded_plan
    source_nodes = [
        node
        for node in _plan_nodes(plan)
        if node.get("Relation Name") == projection.table
    ]
    assert source_nodes, projection_name
    assert all(node.get("Node Type") != "Seq Scan" for node in source_nodes)
    assert all(
        node.get("Node Type")
        in {"Index Scan", "Index Only Scan", "Bitmap Heap Scan"}
        for node in source_nodes
    ), (projection_name, source_nodes)


@pytest.mark.asyncio(loop_scope="session")
async def test_strict_backfill_uses_index_probes_for_uuid_and_text_keys():
    """Target-scoped UUID and text projections must avoid full source scans."""
    _requires_test_database()
    probe_schema = "address_alias_backfill_plan_probe"
    connection = await _connect_runtime_database()
    try:
        await _create_alias_probe_schema(connection, probe_schema, "backfill_plan")
        await _prepare_index_probe(connection, probe_schema)
        await _assert_projection_uses_index(connection, probe_schema, "nppes")
        await _assert_projection_uses_index(
            connection,
            probe_schema,
            "provider_directory_location",
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.close()


async def _seed_apply_race(schema: str):
    """Create and seal one initially unique numeric-grid candidate."""
    await _insert_archive_address(
        schema,
        first_line="1548 E 4500",
        second_line="Suite 202",
        strict_source_bits=1,
    )
    await _insert_archive_address(
        schema,
        first_line="1548 E 4500 S",
        second_line="Suite 202",
        strict_source_bits=6,
    )
    shadow = await run_numeric_grid_alias(mode="shadow", schema=schema)
    assert shadow.eligible == 1
    assert shadow.candidate_digest
    return shadow


async def _wait_for_advisory_waiter(connection, apply_task) -> None:
    """Wait until apply is blocked on the archive resolver advisory lock."""
    for _ in range(200):
        is_waiting = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_locks
                WHERE pid <> pg_backend_pid()
                  AND locktype = 'advisory'
                  AND NOT granted
            );
            """
        )
        if is_waiting:
            return
        if apply_task.done():
            await apply_task
        await asyncio.sleep(0.01)
    raise AssertionError("apply did not wait on the archive resolver lock")


async def _start_blocked_apply(connection, schema: str, shadow):
    """Hold the resolver lock and start an apply that must wait behind it."""
    transaction = connection.transaction()
    await transaction.start()
    await connection.fetchval(
        "SELECT pg_advisory_xact_lock(hashtext($1));",
        address_canon._archive_lock_key(schema, "address_archive_v2", "resolve"),
    )
    apply_task = asyncio.create_task(
        run_numeric_grid_alias(
            mode="apply",
            schema=schema,
            alias_run_id=shadow.run_id,
            expected_candidate_sha256=shadow.candidate_digest,
            reviewed_by="ci-reviewer",
            timeout="10s",
        )
    )
    await _wait_for_advisory_waiter(connection, apply_task)
    return transaction, apply_task


_COMPETING_TARGET_SQL = """
    INSERT INTO "{schema}".address_archive_v2 (
        address_key, identity_key, identity_version, precision,
        premise_key, line1_norm, unit_norm, city_norm, state_code,
        zip5, country_code, first_line, second_line, city_name,
        state_name, postal_code, source_bits, strict_source_bits
    )
    SELECT
        "{schema}".addr_key_v1($1, $2, $3, $4, $5, 'US'),
        "{schema}".addr_identity_key_v1($1, $2, $3, $4, $5, 'US'),
        2, 'street',
        "{schema}".addr_premise_key_v1($1, $2, $3, $4, $5, 'US'),
        "{schema}".addr_street_norm_v1($1, $2),
        "{schema}".addr_unit_norm_v1($1, $2),
        "{schema}".addr_city_norm_v1($3),
        "{schema}".addr_state_code_v1($4),
        left($5, 5), 'US', $1, $2, $3, $4, $5, 6, 6
    RETURNING address_key;
"""


async def _insert_competing_target(connection, schema: str) -> None:
    """Commit a second target while reviewed apply waits behind the writer."""
    await connection.fetchval(
        _COMPETING_TARGET_SQL.format(schema=schema),
        "1548 E 4500 N",
        "Suite 202",
        "Example City",
        "TX",
        "75001",
    )


async def _cleanup_apply_race(connection, schema, transaction, apply_task) -> None:
    """Rollback or cancel unfinished race participants and remove the schema."""
    if transaction is not None:
        await transaction.rollback()
    if apply_task is not None and not apply_task.done():
        apply_task.cancel()
        with suppress(asyncio.CancelledError):
            await apply_task
    await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
    await connection.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_apply_rereads_archive_after_waiting_for_resolver_lock():
    """Apply must re-read candidates after waiting for a concurrent resolver."""
    _requires_test_database()
    probe_schema = "address_alias_apply_race_probe"
    connection = await _connect_runtime_database()
    transaction = None
    apply_task = None
    try:
        await _create_alias_probe_schema(connection, probe_schema, "apply_race")
        shadow = await _seed_apply_race(probe_schema)
        transaction, apply_task = await _start_blocked_apply(
            connection,
            probe_schema,
            shadow,
        )
        await _insert_competing_target(connection, probe_schema)
        await transaction.commit()
        transaction = None
        with pytest.raises(RuntimeError, match="candidate set changed after review"):
            await apply_task
        apply_task = None
        assert await db.scalar(
            f"""
            SELECT count(*) FROM {probe_schema}.address_alias_v1
            WHERE revoked_at IS NULL;
            """
        ) == 0
    finally:
        await _cleanup_apply_race(
            connection,
            probe_schema,
            transaction,
            apply_task,
        )






_PARTD_FIELD_MAP = {
    "first_line": "first_line",
    "second_line": "second_line",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "country": "'US'",
}


async def _create_partd_lineage_stage(schema: str, stage: str) -> None:
    """Create a stage carrying explicit raw-source address lineage."""
    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage} (
            address_key uuid, first_line text, second_line text, city text,
            state text, zip_code text,
            address_observed_in_source boolean NOT NULL
        );
        """
    )


async def _resolve_partd_observation(
    schema: str,
    stage: str,
    is_observed: bool,
):
    """Resolve one Part D row with the requested direct-source lineage."""
    await db.status(f"TRUNCATE TABLE {schema}.{stage};")
    await db.status(
        f"""
        INSERT INTO {schema}.{stage} VALUES (
            NULL, '10 E 20 N', 'Suite 5', 'Example City', 'TX', '75001',
            :is_observed
        );
        """,
        is_observed=is_observed,
    )
    await stamp_address_keys(stage, _PARTD_FIELD_MAP, schema=schema)
    await resolve_into_archive(
        stage,
        _PARTD_FIELD_MAP,
        source_bit=64,
        priority=7,
        schema=schema,
        strict_source_predicate="strict_source.address_observed_in_source IS TRUE",
    )
    return await db.first(
        f"""
        SELECT source_bits, strict_source_bits
        FROM {schema}.address_archive_v2
        WHERE address_key = {schema}.addr_key_v1(
            '10 E 20 N', 'Suite 5', 'Example City', 'TX', '75001', 'US'
        );
        """
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_strict_source_predicate_excludes_npi_filled_partd_address(monkeypatch):
    """Only a Part D address observed in its own source may set strict bit 64."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    stage = "numeric_grid_partd_lineage_stage"
    await _create_partd_lineage_stage(schema, stage)
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE", "false")
    try:
        derived_row = await _resolve_partd_observation(schema, stage, False)
        assert derived_row is not None
        assert derived_row.source_bits == 64
        assert derived_row.strict_source_bits == 0

        direct_row = await _resolve_partd_observation(schema, stage, True)
        assert direct_row is not None
        assert direct_row.strict_source_bits == 64
    finally:
        await db.status(f"DROP TABLE IF EXISTS {schema}.{stage};")
