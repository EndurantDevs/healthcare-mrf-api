# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import json
import os
import uuid
from pathlib import Path

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.ext.asyncio import create_async_engine

from db import procedure_taxonomy_signal_sql as taxonomy_signal_sql
from db.plan_release_storage import storage_identifier, storage_relation

POSTGRES_DSN_ENV = "HLTHPRT_SITE_INTELLIGENCE_POSTGRES_DSN"
MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260820130000_site_intelligence_fast_paths.py"
)


def _load_migration():
    specification = importlib.util.spec_from_file_location(
        "site_intelligence_fast_paths_postgres_migration",
        MIGRATION_PATH,
    )
    assert specification and specification.loader
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def _async_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    if dsn.startswith("postgres://"):
        return dsn.replace("postgres://", "postgresql+asyncpg://", 1)
    return dsn


async def _run_upgrade(engine, migration, monkeypatch, schema: str) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    async with engine.connect() as connection:

        def upgrade(sync_connection):
            context = MigrationContext.configure(sync_connection)
            monkeypatch.setattr(migration, "op", Operations(context))
            with context.begin_transaction():
                migration.upgrade()

        await connection.run_sync(upgrade)


async def _create_current_tables(connection, schema: str) -> None:
    relation = f'"{schema}"'
    await connection.execute(f"CREATE SCHEMA {relation}")
    await connection.execute(
        f"""
        CREATE TABLE {relation}.pricing_procedure (
            procedure_code bigint PRIMARY KEY,
            source_year integer
        );
        CREATE TABLE {relation}.pricing_provider (
            npi bigint NOT NULL,
            year integer NOT NULL,
            provider_name varchar,
            provider_type varchar,
            city varchar,
            state varchar(2),
            zip5 varchar(5)
        );
        CREATE INDEX pricing_provider_year_npi_idx
            ON {relation}.pricing_provider (year, npi);
        CREATE TABLE {relation}.pricing_provider_procedure (
            npi bigint NOT NULL,
            year integer NOT NULL,
            procedure_code bigint NOT NULL,
            total_services double precision,
            total_submitted_charges double precision,
            total_allowed_amount numeric(16, 2),
            total_beneficiaries double precision
        );
        CREATE TABLE {relation}.pricing_provider_quality_feature (
            npi bigint NOT NULL,
            year integer NOT NULL,
            taxonomy_code varchar,
            taxonomy_classification varchar
        );
        CREATE TABLE {relation}.npi_taxonomy (
            npi bigint NOT NULL,
            healthcare_provider_taxonomy_code varchar,
            healthcare_provider_primary_taxonomy_switch varchar,
            checksum bigint
        );
        CREATE TABLE {relation}.nucc_taxonomy (
            code varchar,
            classification varchar,
            specialization varchar,
            display_name varchar
        )
        """
    )


async def _seed_current_tables(connection, schema: str) -> None:
    relation = f'"{schema}"'
    await connection.execute(
        f"""
        INSERT INTO {relation}.pricing_procedure
            (procedure_code, source_year)
        VALUES (1099885900, 2023), (1099885999, 2023);
        INSERT INTO {relation}.pricing_provider
        SELECT
            1000000000 + ordinal,
            2023,
            'Synthetic Provider ' || ordinal,
            'Synthetic Specialty',
            'Example City',
            'EX',
            '00000'
        FROM generate_series(1, 20000) ordinal;
        INSERT INTO {relation}.pricing_provider_procedure
        SELECT
            1000000000 + ordinal,
            2023,
            1099885900,
            ordinal::double precision,
            (ordinal * 20)::double precision,
            (20001 - ordinal)::numeric(16, 2),
            1::double precision
        FROM generate_series(1, 20000) ordinal;
        INSERT INTO {relation}.pricing_provider_quality_feature
        SELECT
            1000000000 + ordinal,
            2023,
            CASE WHEN ordinal <= 15000 THEN '207Q00000X' ELSE 'UNKNOWN' END,
            CASE
                WHEN ordinal = 1 THEN 'Primary Care'
                WHEN ordinal <= 15000 THEN 'Family Medicine'
                ELSE 'Unknown'
            END
        FROM generate_series(1, 20000) ordinal;
        INSERT INTO {relation}.npi_taxonomy
        SELECT
            1000000000 + ordinal,
            CASE WHEN ordinal <= 15000 THEN '207Q00000X' ELSE '207R00000X' END,
            'Y',
            ordinal
        FROM generate_series(1, 20000) ordinal;
        INSERT INTO {relation}.nucc_taxonomy VALUES
            ('207Q00000X', 'Family Medicine', NULL, 'Family Medicine'),
            (' 207q00000x ', 'Primary Care', NULL, 'Primary Care'),
            ('207R00000X', 'Internal Medicine', NULL, 'Internal Medicine')
        """
    )


async def _create_current_fixture(connection, schema: str) -> None:
    """Create the current claims shape and deterministic parity rows."""

    await _create_current_tables(connection, schema)
    await _seed_current_tables(connection, schema)


async def _provider_pages(connection, schema: str, *, offset: int = 0):
    """Return the same page from the legacy aggregate and indexed claims queries."""
    relation = f'"{schema}"'
    old_rows = await connection.fetch(
        f"""
        SELECT
            pp.npi,
            provider.provider_name,
            provider.provider_type,
            provider.city,
            provider.state,
            provider.zip5,
            SUM(pp.total_services) AS total_services,
            SUM(pp.total_submitted_charges) AS total_submitted_charges,
            SUM(pp.total_allowed_amount) AS total_allowed_amount,
            SUM(pp.total_beneficiaries) AS total_beneficiaries,
            COUNT(DISTINCT pp.procedure_code) AS matched_service_codes
        FROM {relation}.pricing_provider_procedure pp
        JOIN {relation}.pricing_provider provider ON provider.npi = pp.npi
         AND provider.year = pp.year
        WHERE pp.year = 2023
          AND pp.procedure_code = 1099885900
        GROUP BY
            pp.npi,
            provider.provider_name,
            provider.provider_type,
            provider.city,
            provider.state,
            provider.zip5
        ORDER BY total_allowed_amount DESC
        LIMIT 25 OFFSET {offset}
        """
    )
    new_rows = await connection.fetch(
        f"""
        SELECT
            pp.npi,
            provider.provider_name,
            provider.provider_type,
            provider.city,
            provider.state,
            provider.zip5,
            pp.total_services,
            pp.total_submitted_charges,
            pp.total_allowed_amount,
            pp.total_beneficiaries,
            1::bigint AS matched_service_codes
        FROM {relation}.pricing_provider_procedure pp
        JOIN {relation}.pricing_provider provider
          ON provider.npi = pp.npi
         AND provider.year = pp.year
        WHERE pp.year = 2023
          AND pp.procedure_code = 1099885900
        ORDER BY pp.total_allowed_amount DESC
        LIMIT 25 OFFSET {offset}
        """
    )
    legacy_page_rows = [dict(provider_record) for provider_record in old_rows]
    indexed_page_rows = [dict(provider_record) for provider_record in new_rows]
    return legacy_page_rows, indexed_page_rows


def _plan_nodes(node):
    yield node
    for child in node.get("Plans", ()):
        yield from _plan_nodes(child)


def _signal_fingerprint_sql(schema: str) -> str:
    return taxonomy_signal_sql.procedure_taxonomy_signal_fingerprint_sql(
        schema=schema,
        provider_table="pricing_provider",
        provider_procedure_table="pricing_provider_procedure",
        quality_feature_table="pricing_provider_quality_feature",
        npi_taxonomy_table="npi_taxonomy",
        nucc_taxonomy_table="nucc_taxonomy",
    )


async def _assert_claims_swap_rejects_stale_signals(connection, schema: str) -> None:
    relation = f'"{schema}"'
    guarded_count_sql = (
        f"SELECT COUNT(*) FROM {relation}.procedure_taxonomy_signal "
        f"WHERE source_relation_fingerprint = {_signal_fingerprint_sql(schema)}"
    )
    assert await connection.fetchval(guarded_count_sql) == 3

    transaction = connection.transaction()
    await transaction.start()
    try:
        await connection.execute(
            f"CREATE TABLE {relation}.pricing_provider_procedure_replacement "
            f"(LIKE {relation}.pricing_provider_procedure INCLUDING DEFAULTS)"
        )
        await connection.execute(
            f"ALTER TABLE {relation}.pricing_provider_procedure RENAME TO pricing_provider_procedure_previous"
        )
        await connection.execute(
            f"ALTER TABLE {relation}.pricing_provider_procedure_replacement RENAME TO pricing_provider_procedure"
        )
        assert await connection.fetchval(f"SELECT COUNT(*) FROM {relation}.procedure_taxonomy_signal") == 3
        assert await connection.fetchval(guarded_count_sql) == 0
    finally:
        await transaction.rollback()


async def _assert_index_plan(connection, schema: str, index_name: str) -> None:
    encoded_plan = await connection.fetchval(
        f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT
            pp.npi,
            provider.provider_name,
            provider.provider_type,
            provider.city,
            provider.state,
            provider.zip5,
            pp.total_services,
            pp.total_submitted_charges,
            pp.total_allowed_amount,
            pp.total_beneficiaries,
            1 AS matched_service_codes
        FROM "{schema}".pricing_provider_procedure pp
        JOIN "{schema}".pricing_provider provider
          ON provider.npi = pp.npi
         AND provider.year = pp.year
        WHERE pp.year = 2023
          AND pp.procedure_code = 1099885900
        ORDER BY pp.total_allowed_amount DESC
        LIMIT 25
        """
    )
    plan = json.loads(encoded_plan) if isinstance(encoded_plan, str) else encoded_plan
    assert any(node.get("Index Name") == index_name for node in _plan_nodes(plan[0]["Plan"]))


async def _create_staged_signal(
    connection,
    migration,
    schema: str,
    signal_stage: str,
    provider_stage: str,
    page_stage: str,
) -> None:
    """Materialize the signal with its production keys and lookup index."""

    await connection.execute(
        f'CREATE TABLE "{schema}"."{signal_stage}" AS TABLE "{schema}".procedure_taxonomy_signal WITH NO DATA'
    )
    await connection.execute(
        f'CREATE UNIQUE INDEX "{signal_stage}_idx_primary" '
        f'ON "{schema}"."{signal_stage}" '
        "(procedure_code, year, setting_key, evidence_source, taxonomy_code)"
    )
    await connection.execute(
        taxonomy_signal_sql.procedure_taxonomy_signal_insert_sql(
            schema=schema,
            signal_table=signal_stage,
            provider_table=provider_stage,
            provider_procedure_table=page_stage,
            quality_feature_table="pricing_provider_quality_feature",
            npi_taxonomy_table="npi_taxonomy",
            nucc_taxonomy_table="nucc_taxonomy",
        )
    )
    await connection.execute(
        migration._create_signal_index_sql(schema)
        .replace('"procedure_taxonomy_signal_lookup_idx"', f'"{signal_stage}_taxonomy_lookup"')
        .replace('"procedure_taxonomy_signal"', f'"{signal_stage}"')
    )


async def _publish_staged_indexes(connection, migration, schema: str) -> None:
    """Publish staged claims and signal indexes through production renames."""

    suffix = "abcdefghijkl_12345678"
    provider_stage = f"pricing_provider_{suffix}"
    page_stage = f"pricing_provider_procedure_{suffix}"
    signal_stage = f"procedure_taxonomy_signal_{suffix}"
    await connection.execute(f'CREATE TABLE "{schema}"."{provider_stage}" AS TABLE "{schema}".pricing_provider')
    await connection.execute(f'CREATE TABLE "{schema}"."{page_stage}" AS TABLE "{schema}".pricing_provider_procedure')
    await connection.execute(
        migration._create_page_index_sql(schema, concurrently=False)
        .replace('"pricing_provider_proc_amount_page_idx"', f'"{page_stage}_amt_page"')
        .replace('"pricing_provider_procedure"', f'"{page_stage}"')
    )
    await _create_staged_signal(
        connection,
        migration,
        schema,
        signal_stage,
        provider_stage,
        page_stage,
    )
    async with connection.transaction():
        await connection.execute(
            f'DROP TABLE "{schema}".pricing_provider; '
            f'ALTER TABLE "{schema}"."{provider_stage}" '
            "RENAME TO pricing_provider"
        )
        await connection.execute(
            f'DROP TABLE "{schema}".pricing_provider_procedure; '
            f'ALTER TABLE "{schema}"."{page_stage}" '
            "RENAME TO pricing_provider_procedure; "
            f'ALTER INDEX "{schema}"."{page_stage}_amt_page" '
            f"RENAME TO {migration.PAGE_INDEX_NAME}"
        )
        await connection.execute(
            f'DROP TABLE "{schema}".procedure_taxonomy_signal; '
            f'ALTER TABLE "{schema}"."{signal_stage}" '
            "RENAME TO procedure_taxonomy_signal; "
            f'ALTER INDEX "{schema}"."{signal_stage}_idx_primary" '
            "RENAME TO procedure_taxonomy_signal_idx_primary; "
            f'ALTER INDEX "{schema}"."{signal_stage}_taxonomy_lookup" '
            f"RENAME TO {migration.SIGNAL_INDEX_NAME}"
        )
    assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.procedure_taxonomy_signal_idx_primary")


@pytest.mark.asyncio
async def test_current_schema_backfills_exact_fast_paths(monkeypatch):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"{POSTGRES_DSN_ENV} is required")
    migration = _load_migration()
    schema = f"site_intelligence_{uuid.uuid4().hex[:16]}"
    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(_async_dsn(dsn), pool_size=1, max_overflow=0)
    try:
        await _create_current_fixture(connection, schema)
        await _run_upgrade(engine, migration, monkeypatch, schema)
        await connection.execute(f'ANALYZE "{schema}".pricing_provider_procedure')
        await connection.execute(f'ANALYZE "{schema}".pricing_provider')

        counts = await connection.fetch(
            f'SELECT procedure_code, provider_count FROM "{schema}".pricing_procedure ORDER BY procedure_code'
        )
        assert [tuple(count_record.values()) for count_record in counts] == [
            (1099885900, 20000),
            (1099885999, 0),
        ]
        signal_counts = await connection.fetch(
            f"""
            SELECT evidence_source, taxonomy_code, distinct_npis, total_services
            FROM "{schema}".procedure_taxonomy_signal
            ORDER BY evidence_source, taxonomy_code
            """
        )
        assert [tuple(signal_record.values()) for signal_record in signal_counts] == [
            ("quality_feature", "207Q00000X", 15000, 112507500.0),
            ("quality_or_nppes", "207Q00000X", 15000, 112507500.0),
            ("quality_or_nppes", "207R00000X", 5000, 87502500.0),
        ]
        await _assert_claims_swap_rejects_stale_signals(connection, schema)
        old_page, new_page = await _provider_pages(connection, schema)
        assert old_page == new_page
        assert await _provider_pages(connection, schema, offset=25000) == ([], [])
        await _assert_index_plan(connection, schema, migration.PAGE_INDEX_NAME)
        await _publish_staged_indexes(connection, migration, schema)
        assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{migration.PAGE_INDEX_NAME}")
        assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{migration.SIGNAL_INDEX_NAME}")
        assert (
            await connection.fetchval(
                f'SELECT COUNT(*) FROM "{schema}".procedure_taxonomy_signal '
                f"WHERE source_relation_fingerprint = {_signal_fingerprint_sql(schema)}"
            )
            == 3
        )
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_legacy_claims_shape_is_left_for_import_swap(monkeypatch):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"{POSTGRES_DSN_ENV} is required")
    migration = _load_migration()
    schema = f"site_intelligence_legacy_{uuid.uuid4().hex[:12]}"
    connection = await asyncpg.connect(dsn)
    engine = create_async_engine(_async_dsn(dsn), pool_size=1, max_overflow=0)
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(
            f"""
            CREATE TABLE "{schema}".pricing_provider_procedure (
                npi bigint,
                year integer,
                procedure_code bigint,
                total_claims double precision,
                total_drug_cost numeric,
                total_benes double precision
            )
            """
        )
        await _run_upgrade(engine, migration, monkeypatch, schema)

        assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{migration.SIGNAL_TABLE}")
        assert not await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{migration.PAGE_INDEX_NAME}")
    finally:
        await engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


async def _seed_taxonomy_publication(connection, source_schema, publication_schema):
    """Publish one known code as the incumbent for all subsequent assertions."""
    from process import procedure_taxonomy_signals as publication

    await _create_current_tables(connection, source_schema)
    await connection.execute(f'CREATE SCHEMA "{publication_schema}"')
    control_schema = taxonomy_signal_sql.procedure_taxonomy_control_schema(publication_schema)
    await connection.execute(f'CREATE SCHEMA "{control_schema}"')
    source_taxonomy = storage_relation(source_schema, "npi_taxonomy")
    await connection.execute(
        f"""INSERT INTO "{source_schema}".pricing_provider (npi,year,provider_type)
            VALUES (1000000001,2024,'Example');
            INSERT INTO "{source_schema}".pricing_provider_procedure
                (npi,year,procedure_code,total_services,total_beneficiaries)
            VALUES (1000000001,2024,17,10,5);
            INSERT INTO {source_taxonomy} VALUES (1000000001,'FIRSTCODE','Y',1)"""
    )
    await publication.initialize_procedure_taxonomy_publication(connection, publication_schema)
    publication_outcome = await publication.rebuild_procedure_taxonomy_signals(
        connection, source_schema, publication_schema
    )
    assert (publication_outcome["status"], publication_outcome["rows"]) == ("published", 1)
    return publication_outcome["table"]


async def _served_taxonomy_code(reader_connection, publication_schema):
    serving_view = storage_relation(publication_schema, taxonomy_signal_sql.TAXONOMY_SERVING_VIEW)
    return await reader_connection.fetchval(f"SELECT taxonomy_code FROM {serving_view}")


async def _assert_taxonomy_privileges(
    connection,
    reader_connection,
    source_schema,
    publication_schema,
    writer_role,
    api_reader_role,
    publisher_role,
    incumbent_table,
):
    """Exercise view-only reads and restrict the source writer to its revision column."""
    from process import procedure_taxonomy_signals as publication

    control_schema = taxonomy_signal_sql.procedure_taxonomy_control_schema(publication_schema)
    publication_state = storage_relation(control_schema, taxonomy_signal_sql.TAXONOMY_PUBLICATION_STATE)
    serving_view = storage_relation(publication_schema, taxonomy_signal_sql.TAXONOMY_SERVING_VIEW)
    source_taxonomy = storage_relation(source_schema, "npi_taxonomy")
    await connection.execute(f"""
        CREATE ROLE {writer_role}; CREATE ROLE {api_reader_role}; CREATE ROLE {publisher_role};
        GRANT USAGE ON SCHEMA "{source_schema}" TO {writer_role}, {publisher_role};
        GRANT USAGE ON SCHEMA "{publication_schema}" TO {api_reader_role};
        GRANT USAGE ON SCHEMA "{control_schema}" TO {writer_role};
        GRANT SELECT ON {serving_view} TO {api_reader_role};
        GRANT SELECT (singleton, input_revision), UPDATE (input_revision) ON {publication_state} TO {writer_role};
        ALTER TABLE {source_taxonomy} OWNER TO {writer_role};
    """)
    await reader_connection.execute(f"SET ROLE {api_reader_role}")
    assert await _served_taxonomy_code(reader_connection, publication_schema) == "FIRSTCODE"
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await reader_connection.fetchval(f"SELECT input_revision FROM {publication_state}")
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        incumbent_relation = storage_relation(publication_schema, incumbent_table)
        await reader_connection.fetchval(f"SELECT taxonomy_code FROM {incumbent_relation}")
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await reader_connection.execute(f"UPDATE {serving_view} SET taxonomy_code = 'DENIED'")
    await publication.initialize_procedure_taxonomy_publication(connection, publication_schema)
    assert await _served_taxonomy_code(reader_connection, publication_schema) == "FIRSTCODE"

    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(f"SET LOCAL ROLE {writer_role}")
            await connection.execute(f"UPDATE {publication_state} SET current_table = NULL WHERE singleton")
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(f"SET LOCAL ROLE {writer_role}")
            await _served_taxonomy_code(connection, publication_schema)


async def _replace_taxonomy_source(connection, source_schema, writer_role, taxonomy_code):
    """Update the source and its revision inside the caller's transaction."""
    from process import procedure_taxonomy_signals as publication

    source_taxonomy = storage_relation(source_schema, "npi_taxonomy")
    await connection.execute(f"SET LOCAL ROLE {writer_role}")
    await connection.execute(f"UPDATE {source_taxonomy} SET healthcare_provider_taxonomy_code = $1", taxonomy_code)
    await publication.mark_procedure_taxonomy_inputs_changed(source_schema, connection.execute, "npi_taxonomy")


async def _assert_stale_taxonomy_rejected(
    connection, reader_connection, source_schema, publication_schema, writer_role, publisher_role
):
    """Keep the incumbent through a source rollback and a stale candidate, then publish fresh evidence."""
    from process import procedure_taxonomy_signals as publication

    control_schema = taxonomy_signal_sql.procedure_taxonomy_control_schema(publication_schema)
    publication_state = storage_relation(control_schema, taxonomy_signal_sql.TAXONOMY_PUBLICATION_STATE)
    source_taxonomy = storage_relation(source_schema, "npi_taxonomy")
    with pytest.raises(ValueError, match="source aborted"):
        async with connection.transaction():
            await _replace_taxonomy_source(connection, source_schema, writer_role, "ABORTED")
            raise ValueError("source aborted")
    assert await connection.fetchval(f"SELECT input_revision FROM {publication_state}") == 0

    async with connection.transaction():
        await _replace_taxonomy_source(connection, source_schema, writer_role, "STALECODE")
    async with connection.transaction():
        await connection.execute(f"SET LOCAL ROLE {publisher_role}")
        assert (
            await connection.fetchval(f"SELECT healthcare_provider_taxonomy_code FROM {source_taxonomy}") == "STALECODE"
        )
    stale_table = "procedure_taxonomy_g_" + uuid.uuid4().hex
    stale_revision, _ = await publication._build_generation(connection, source_schema, publication_schema, stale_table)
    async with connection.transaction():
        await _replace_taxonomy_source(connection, source_schema, writer_role, "SECONDCODE")
    assert (
        await publication._publish_generation(connection, publication_schema, stale_table, stale_revision)
        == "inputs_changed"
    )
    assert await _served_taxonomy_code(reader_connection, publication_schema) == "FIRSTCODE"
    publication_outcome = await publication.rebuild_procedure_taxonomy_signals(
        connection, source_schema, publication_schema
    )
    assert publication_outcome["status"] == "published"
    assert await connection.fetchval("SELECT to_regclass($1) IS NULL", f"{publication_schema}.{stale_table}")
    assert await _served_taxonomy_code(reader_connection, publication_schema) == "SECONDCODE"


async def _assert_taxonomy_rollback(connection, reader_connection, source_schema, publication_schema, incumbent_table):
    """Keep an in-flight reader's evidence stable and restore the exact prior published table."""
    from process import procedure_taxonomy_signals as publication

    async with reader_connection.transaction():
        assert await _served_taxonomy_code(reader_connection, publication_schema) == "SECONDCODE"
        with pytest.raises(asyncpg.LockNotAvailableError):
            await publication.rollback_procedure_taxonomy_signals(connection, publication_schema)
        assert await _served_taxonomy_code(reader_connection, publication_schema) == "SECONDCODE"
    rollback_outcome = await publication.rollback_procedure_taxonomy_signals(connection, publication_schema)
    assert rollback_outcome["table"] == incumbent_table
    assert await _served_taxonomy_code(reader_connection, publication_schema) == "FIRSTCODE"
    assert (await publication.rebuild_procedure_taxonomy_signals(connection, source_schema, publication_schema)) == {
        "status": "current"
    }


async def _assert_taxonomy_publication_with_missing_inputs(
    connection, reader_connection, source_schema, publication_schema
):
    """Publish with optional inputs absent and retain that generation if all evidence disappears."""
    from process import procedure_taxonomy_signals as publication

    async with connection.transaction():
        await connection.execute(f'DROP TABLE "{source_schema}".pricing_provider_quality_feature')
        await connection.execute(f'DROP TABLE "{source_schema}".nucc_taxonomy')
        await publication.mark_procedure_taxonomy_inputs_changed(source_schema, connection.execute)

    outcome = await publication.rebuild_procedure_taxonomy_signals(connection, source_schema, publication_schema)
    assert (outcome["status"], outcome["rows"]) == ("published", 1)
    assert await _served_taxonomy_code(reader_connection, publication_schema) == "SECONDCODE"

    async with connection.transaction():
        await connection.execute(f'DROP TABLE "{source_schema}".npi_taxonomy')
        await publication.mark_procedure_taxonomy_inputs_changed(source_schema, connection.execute)

    with pytest.raises(RuntimeError, match="generation is empty or contains invalid evidence"):
        await publication.rebuild_procedure_taxonomy_signals(connection, source_schema, publication_schema)

    assert await _served_taxonomy_code(reader_connection, publication_schema) == "SECONDCODE"


@pytest.mark.asyncio
async def test_named_taxonomy_publication_preserves_incumbent_and_exact_rollback(monkeypatch):
    """Verify native publication across privilege, source race, reader, and rollback boundaries."""
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"{POSTGRES_DSN_ENV} is required")
    source_schema = f"taxonomy_source_{uuid.uuid4().hex[:12]}"
    publication_schema = f"taxonomy_publication_{uuid.uuid4().hex[:12]}"
    role_suffix = uuid.uuid4().hex[:12]
    writer_role = storage_identifier(f"taxonomy_writer_{role_suffix}")
    api_reader_role = storage_identifier(f"taxonomy_reader_{role_suffix}")
    publisher_role_name = f"taxonomy_publisher_{role_suffix}"
    publisher_role = storage_identifier(publisher_role_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", source_schema)
    monkeypatch.setenv("HLTHPRT_PRICING_TAXONOMY_PUBLICATION_SCHEMA", publication_schema)
    monkeypatch.setenv("HLTHPRT_PRICING_TAXONOMY_PUBLISHER_ROLE", publisher_role_name)
    connection = await asyncpg.connect(dsn)
    reader_connection = await asyncpg.connect(dsn)
    try:
        incumbent_table = await _seed_taxonomy_publication(connection, source_schema, publication_schema)
        await _assert_taxonomy_privileges(
            connection,
            reader_connection,
            source_schema,
            publication_schema,
            writer_role,
            api_reader_role,
            publisher_role,
            incumbent_table,
        )
        await _assert_stale_taxonomy_rejected(
            connection, reader_connection, source_schema, publication_schema, writer_role, publisher_role
        )
        await _assert_taxonomy_rollback(
            connection, reader_connection, source_schema, publication_schema, incumbent_table
        )
        await _assert_taxonomy_publication_with_missing_inputs(
            connection, reader_connection, source_schema, publication_schema
        )
    finally:
        await reader_connection.close()
        control_schema = taxonomy_signal_sql.procedure_taxonomy_control_schema(publication_schema)
        await connection.execute(f'DROP SCHEMA IF EXISTS "{control_schema}" CASCADE')
        await connection.execute(f'DROP SCHEMA IF EXISTS "{publication_schema}" CASCADE')
        await connection.execute(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE')
        await connection.execute(f"DROP ROLE IF EXISTS {writer_role}, {api_reader_role}, {publisher_role}")
        await connection.close()
