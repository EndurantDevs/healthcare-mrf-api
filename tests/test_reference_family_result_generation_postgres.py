# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
import importlib.util
import os
import re
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from tests.reference_family_generation_fixture import install_named_generation, install_source_generation_guards

generation = importlib.import_module("process.reference_family_result_generation")
_DSN_ENV = "HLTHPRT_REFERENCE_FAMILY_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE = re.compile(r"^hc_reference_family_[0-9a-f]{32}$")
_REFERENCE_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260914110000_reference_family_result_generation.py"
)
_NUCC_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20261006010000_nucc_reference_result_generation.py"
)
_MRF_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260914130000_mrf_result_generation.py"
_CMS_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260920100000_cms_doctors_result_generation.py"
)
_TIGER_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260920110000_tiger_result_generation.py"
)
_MRF_ADDRESS_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260920120000_mrf_address_result_generation.py"
)
_GEO_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260920130000_geo_result_generation.py"
_PHARMACY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260920140000_pharmacy_economics_result_generation.py"
)
_TERMINOLOGY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260920150000_terminology_result_generation.py"
)
_CENSUS_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260920160000_geo_census_result_generation.py"
)
_PROVIDER_QUALITY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260921000000_provider_quality_result_generation.py"
)
_FACILITY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260923000000_facility_address_contribution.py"
)
_CMS_GROUP_SITE_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260929000000_cms_doctor_group_site.py"
)
_NPI_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260914120000_npi_result_generation.py"
_NPI_TABLES = (
    "npi",
    "npi_address",
    "npi_taxonomy",
    "npi_taxonomy_group",
    "npi_other_identifier",
    "npi_phone_staffing",
)


def _database_url():
    raw = os.getenv(_DSN_ENV, "")
    if not raw:
        pytest.skip(f"{_DSN_ENV} is not set")
    url = make_url(raw)
    if (
        not url.drivername.startswith("postgresql")
        or url.host not in {"127.0.0.1", "localhost"}
        or url.port not in {5432, 5440}
        or not _LOCAL_DATABASE.fullmatch(str(url.database or ""))
    ):
        pytest.fail(f"{_DSN_ENV} must identify a UUID-owned local PostgreSQL 18 test database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


def _migration_module(path: Path):
    spec = importlib.util.spec_from_file_location("reference_family_generation_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _run_migration(connection, path: Path, action: str) -> None:
    module = _migration_module(path)

    def apply(sync_connection) -> None:
        module.op = Operations(MigrationContext.configure(sync_connection))
        getattr(module, action)()

    await connection.run_sync(apply)


async def _upgrade_reference_generation_chain(connection) -> None:
    for migration_path in (
        _REFERENCE_MIGRATION_PATH,
        _MRF_MIGRATION_PATH,
        _CMS_MIGRATION_PATH,
        _TIGER_MIGRATION_PATH,
        _MRF_ADDRESS_MIGRATION_PATH,
    ):
        await _run_migration(connection, migration_path, "upgrade")


@pytest.mark.asyncio
async def test_geo_migration_seeds_exact_generation_and_preserves_prior_rows(monkeypatch):
    schema = "reference_geo_upgrade_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _upgrade_reference_generation_chain(connection)
            before = (
                await connection.execute(
                    text(f'SELECT importer_id, local_lineage_id FROM "{schema}".reference_family_result_generation')
                )
            ).all()
            await _run_migration(connection, _GEO_MIGRATION_PATH, "upgrade")
            after = (
                await connection.execute(
                    text(f'SELECT importer_id, local_lineage_id FROM "{schema}".reference_family_result_generation')
                )
            ).all()
            assert {name: lineage for name, lineage in after if name != "geo"} == dict(before)
            assert [name for name, _ in after].count("geo") == 1
            await connection.execute(
                text(
                    f'UPDATE "{schema}".reference_family_result_generation '
                    "SET local_generation=1 WHERE importer_id='geo'"
                )
            )
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(connection, _GEO_MIGRATION_PATH, "downgrade")
            await connection.execute(
                text(
                    f'UPDATE "{schema}".reference_family_result_generation '
                    "SET local_generation=0 WHERE importer_id='geo'"
                )
            )
            await _run_migration(connection, _GEO_MIGRATION_PATH, "downgrade")
            assert (
                await connection.execute(
                    text(f'SELECT importer_id, local_lineage_id FROM "{schema}".reference_family_result_generation')
                )
            ).all() == before
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()


async def _npi_catalog_state(connection, schema: str):
    return (
        await connection.execute(
            text(
                "SELECT local_lineage_id, "
                "(SELECT array_agg(c.oid::bigint ORDER BY c.relname) "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname = ANY(:tables)), "
                "(SELECT array_agg(t.tgname ORDER BY t.tgrelid) "
                "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname = ANY(:tables) AND NOT t.tgisinternal) "
                f'FROM "{schema}".npi_result_generation WHERE singleton IS TRUE'
            ),
            {"schema": schema, "tables": list(_NPI_TABLES)},
        )
    ).one()


@pytest.mark.asyncio
async def test_mrf_upgrade_preserves_npi_catalog_and_reference_rows(monkeypatch):
    """The MRF branch extends reference authority without touching DEV's NPI family."""

    schema = "reference_mrf_upgrade_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            for table_name in _NPI_TABLES:
                await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
            table_names = {name for names in generation.RELATION_NAMES_BY_IMPORTER.values() for name in names}
            for table_name in table_names:
                await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
                await connection.execute(text(f'INSERT INTO "{schema}"."{table_name}" VALUES (1)'))
            await _run_migration(connection, _NPI_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _REFERENCE_MIGRATION_PATH, "upgrade")
            npi_before = await _npi_catalog_state(connection, schema)
            reference_rows_before = await connection.scalar(
                text(f'SELECT count(*) FROM "{schema}".reference_family_result_generation')
            )
            await _run_migration(connection, _MRF_MIGRATION_PATH, "upgrade")
            npi_after = await _npi_catalog_state(connection, schema)
            assert npi_after == npi_before
            assert (
                await connection.scalar(text(f'SELECT count(*) FROM "{schema}".reference_family_result_generation'))
                == reference_rows_before + 1
            )
            assert (
                await connection.scalar(
                    text(
                        f"SELECT count(*) FROM \"{schema}\".reference_family_result_generation WHERE importer_id='mrf'"
                    )
                )
                == 1
            )
            for relation_names in generation.RELATION_NAMES_BY_IMPORTER.values():
                for table_name in relation_names:
                    assert await connection.scalar(text(f'SELECT count(*) FROM "{schema}"."{table_name}"')) == 1
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()


async def _assert_native_publication_refused(connection, schema, importer_id):
    """Keep native-only families unchanged when the generic publisher refuses them."""

    authority_before = await generation.read_reference_family_result_generation_authority(
        connection, importer_id=importer_id, schema_name=schema
    )
    oids_before = await generation.current_reference_family_relation_oids(
        connection, importer_id=importer_id, schema_name=schema
    )
    failure_by_importer = {
        "geo": "geo_source_native_publication_required",
        "cms-doctors": "cms_doctors_native_publication_required",
    }
    with pytest.raises(RuntimeError, match=f"^{failure_by_importer[importer_id]}$"):
        await generation.publish_local_reference_family_generation(
            connection, importer_id=importer_id, schema_name=schema
        )
    assert (
        await generation.read_reference_family_result_generation_authority(
            connection, importer_id=importer_id, schema_name=schema
        )
        == authority_before
    )
    assert (
        await generation.current_reference_family_relation_oids(connection, importer_id=importer_id, schema_name=schema)
        == oids_before
    )


async def _publish_initial_generations(engine, schema):
    """Publish generic families and prove native-only families refuse this path."""

    authority_by_importer = {}
    for importer_id in generation.RELATION_NAMES_BY_IMPORTER:
        if importer_id in {"label", "tiger"}:
            continue
        async with engine.begin() as connection:
            if importer_id in {"geo", "cms-doctors"}:
                await _assert_native_publication_refused(connection, schema, importer_id)
                continue
            first = await generation.publish_local_reference_family_generation(
                connection,
                importer_id=importer_id,
                schema_name=schema,
            )
            assert first.local_generation == 1
            assert first.serving_generation.origin_lineage_id == first.local_lineage_id
            assert first.serving_generation.origin_generation == 1
            assert first.relations == generation.declared_reference_family_relations(importer_id, schema)
            authority_by_importer[importer_id] = first
    return authority_by_importer


async def _assert_adoption_rollback(engine, schema, incumbent_authority, source_generation_by_field):
    """Rotate one family and prove its authority rolls back with the tables."""

    importer_id = incumbent_authority.importer_id
    with pytest.raises(RuntimeError, match="force rollback"):
        async with engine.begin() as connection:
            table_name = generation.RELATION_NAMES_BY_IMPORTER[importer_id][0]
            await connection.execute(text(f'ALTER TABLE "{schema}"."{table_name}" RENAME TO "{table_name}_old"'))
            await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
            adopted = await generation.publish_adopted_reference_family_generation(
                connection,
                importer_id=importer_id,
                schema_name=schema,
                source_generation=source_generation_by_field,
                source_revision_tracked=True,
            )
            assert adopted.local_generation == 1
            assert adopted.serving_generation.origin_generation == 2
            # The same declared names now serve the replacement heap under a new local generation.
            assert adopted.relations == incumbent_authority.relations
            assert adopted.local_lineage_id == incumbent_authority.local_lineage_id
            raise RuntimeError("force rollback")
    async with engine.connect() as connection:
        rolled_back = await generation.read_reference_family_result_generation_authority(
            connection,
            importer_id=importer_id,
            schema_name=schema,
        )
        assert rolled_back == incumbent_authority
        assert (
            await generation.capture_reference_family_serving_generation(
                connection,
                importer_id=importer_id,
                schema_name=schema,
            )
        ) == incumbent_authority.serving_generation


@pytest.mark.asyncio
async def test_closed_family_generation_publication_adoption_and_rollback(monkeypatch):
    """Bind every family to exact OIDs and keep adoption transaction-local."""

    schema = "reference_generation_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _upgrade_reference_generation_chain(connection)
            await _run_migration(connection, _GEO_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _PHARMACY_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _TERMINOLOGY_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _CENSUS_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _PROVIDER_QUALITY_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _FACILITY_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _CMS_GROUP_SITE_MIGRATION_PATH, "upgrade")
            table_names = {name for names in generation.RELATION_NAMES_BY_IMPORTER.values() for name in names}
            for table_name in table_names - {"facility_address_contribution", "cms_doctor_group_site"}:
                await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
            # Keep migration order: the NUCC seed predates the named generation columns.
            await install_source_generation_guards(connection, schema, named=False)
            await _run_migration(connection, _NUCC_MIGRATION_PATH, "upgrade")
            await install_named_generation(connection, schema)

        authority_by_importer = await _publish_initial_generations(engine, schema)
        incumbent_authority = authority_by_importer["places-zcta"]
        source_generation_by_field = {
            "origin_lineage_id": incumbent_authority.local_lineage_id,
            "origin_generation": 2,
            "published_at": datetime.datetime(2026, 9, 14, 10, tzinfo=datetime.UTC),
        }
        await _assert_adoption_rollback(engine, schema, incumbent_authority, source_generation_by_field)

        async with engine.begin() as connection:
            with pytest.raises(RuntimeError, match="prevents downgrade"):
                await _run_migration(connection, _CENSUS_MIGRATION_PATH, "downgrade")
            with pytest.raises(RuntimeError, match="prevents downgrade"):
                await _run_migration(connection, _TERMINOLOGY_MIGRATION_PATH, "downgrade")
            with pytest.raises(RuntimeError, match="prevents downgrade"):
                await _run_migration(connection, _MRF_MIGRATION_PATH, "downgrade")
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()


async def _create_reader_compatibility_metadata(connection, schema):
    """Publish fixture origins through original metadata before the named migration."""
    for table in (*_NPI_TABLES, "doctor_clinician_address", "cms_doctor_education"):
        await connection.execute(text(f'CREATE TABLE "{schema}"."{table}" (value bigint)'))
        await connection.execute(text(f'INSERT INTO "{schema}"."{table}" VALUES (1)'))
    await _run_migration(connection, _NPI_MIGRATION_PATH, "upgrade")
    await _upgrade_reference_generation_chain(connection)
    await _run_migration(connection, _CMS_GROUP_SITE_MIGRATION_PATH, "upgrade")
    for table, names, predicate in (
        ("npi_result_generation", _NPI_TABLES, "singleton IS TRUE"),
        (
            "reference_family_result_generation",
            generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"],
            "importer_id='cms-doctors'",
        ),
    ):
        # The fixture's explicit publication reuses its migration-issued lineage and native tables.
        await connection.execute(
            text(
                f'UPDATE "{schema}"."{table}" SET local_generation=local_generation+1, '
                "origin_lineage_id=local_lineage_id,origin_generation=local_generation+1,published_at=transaction_timestamp(),"
                "relation_oids=(SELECT array_agg(to_regclass(:schema||'.'||name)::oid::bigint ORDER BY ordinal) "
                f"FROM unnest(CAST(:names AS text[])) WITH ORDINALITY declared(name,ordinal)) WHERE {predicate}"
            ),
            {"schema": schema, "names": list(names)},
        )
    await install_source_generation_guards(connection, schema, named=False)


async def _grant_compatibility_reader(connection, schema, reader, *, named):
    """Use the published CMS column contract without granting tracking or physical fields."""
    from process.provider_directory_cms_control_privileges import READ_COLUMN_GRANTS

    named_columns = {"serving_schema", "serving_relation_names"}
    reference_columns = tuple(
        column for column in READ_COLUMN_GRANTS[generation.TABLE_NAME] if named or column not in named_columns
    )
    assert len(reference_columns) == (8 if named else 6)
    assert "source_revision_tracked" not in reference_columns
    npi_columns = (
        "singleton",
        "local_lineage_id",
        "local_generation",
        "origin_lineage_id",
        "origin_generation",
        "published_at",
        "canonical_publication_ref",
        "canonical_publication_generation",
        "canonical_chain_ref",
        "canonical_import_date",
        *(("serving_schema", "serving_relation_names") if named else ()),
    )
    await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"'))
    for table, columns in ((generation.TABLE_NAME, reference_columns), ("npi_result_generation", npi_columns)):
        await connection.execute(text(f'GRANT SELECT({",".join(columns)}) ON "{schema}"."{table}" TO "{reader}"'))
    for table in generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]:
        await connection.execute(text(f'GRANT SELECT ON "{schema}"."{table}" TO "{reader}"'))


async def _read_compatibility_origins(engine, schema, reader):
    """Exercise the API native-family reader under real restricted PostgreSQL permissions."""
    from api import provider_profile_snapshot as snapshot
    from process import npi_result_generation as npi_generation

    async with engine.begin() as connection:
        await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await connection.execute(text(f'SET LOCAL ROLE "{reader}"'))
        for projection in ("source_revision_tracked", "to_jsonb(record)"):
            with pytest.raises(DBAPIError) as error:
                async with connection.begin_nested():
                    await connection.execute(
                        text(f'SELECT {projection} FROM "{schema}".{generation.TABLE_NAME} record')
                    )
            assert error.value.orig.sqlstate == "42501"
        npi = await npi_generation.read_npi_result_generation_authority(connection, schema_name=schema)
        reference = await generation.read_reference_family_result_generation_authority(
            connection, importer_id="cms-doctors", schema_name=schema
        )
        await connection.execute(text("RESET ROLE"))
        availability = await snapshot._inventory_relation_names(
            connection, schema, (generation.TABLE_NAME, *snapshot._DOCTORS_TABLES)
        )
        await connection.execute(text(f'SET LOCAL ROLE "{reader}"'))
        authorities = await snapshot._read_native_authorities(connection, schema, availability, include_address=False)
        assert authorities == {"cms-doctors": reference}
        assert npi.relations == tuple((schema, name) for name in _NPI_TABLES)
        assert reference.relations == generation.declared_reference_family_relations("cms-doctors", schema)
        return npi, reference


async def _read_raw_generation_projections(raw, schema, expected, *, named):
    """Keep native writer projections explicit, with the named edition as their default."""
    from process import npi_result_generation as npi_generation

    async with raw.transaction():
        row = await raw.fetchrow(npi_generation._state_projection(schema, lock=True, named=named))
        assert "relation_oids" not in row
        assert npi_generation.validate_npi_result_generation_authority(row, declared_schema=schema) == expected[0]
        row = await raw.fetchrow(
            generation._state_sql(schema, lock=True, named=named).replace(":importer_id", "$1"), "cms-doctors"
        )
        assert "source_revision_tracked" not in row
        assert (
            generation.validate_reference_family_result_generation_authority(row, declared_schema=schema) == expected[1]
        )
        if named:
            assert npi_generation._state_projection(schema, lock=True) == npi_generation._state_projection(
                schema, lock=True, named=True
            )
            assert generation._state_sql(schema, lock=True) == generation._state_sql(schema, lock=True, named=True)


async def _assert_partial_named_catalog_refused(engine, schema, reader):
    """A partial named edition must fail instead of falling back to legacy declared names."""
    from process import npi_result_generation as npi_generation

    for table, read in (
        (
            npi_generation.TABLE_NAME,
            lambda connection: npi_generation.read_npi_result_generation_authority(connection, schema_name=schema),
        ),
        (
            generation.TABLE_NAME,
            lambda connection: generation.read_reference_family_result_generation_authority(
                connection, importer_id="cms-doctors", schema_name=schema
            ),
        ),
    ):
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.execute(
                    text(f'ALTER TABLE "{schema}"."{table}" RENAME COLUMN serving_schema TO partial_serving_schema')
                )
                await connection.execute(text(f'SET LOCAL ROLE "{reader}"'))
                with pytest.raises(RuntimeError, match="invalid"):
                    await read(connection)
            finally:
                await transaction.rollback()


async def _assert_named_column_permission_required(engine, schema, reader):
    """An inaccessible named field remains required instead of appearing to be legacy."""
    async with engine.connect() as connection:
        transaction = await connection.begin()
        try:
            await connection.execute(
                text(f'REVOKE SELECT(serving_schema) ON "{schema}".{generation.TABLE_NAME} FROM "{reader}"')
            )
            await connection.execute(text(f'SET LOCAL ROLE "{reader}"'))
            with pytest.raises(DBAPIError) as error:
                await generation.read_reference_family_result_generation_authority(
                    connection, importer_id="cms-doctors", schema_name=schema
                )
            assert error.value.orig.sqlstate == "42501"
        finally:
            await transaction.rollback()


async def _create_compatibility_reader_fixture(engine, schema, reader):
    """Create an original-edition catalog and its exact column-only read recipient."""
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await _create_reader_compatibility_metadata(connection, schema)
        await connection.execute(text(f'CREATE ROLE "{reader}" NOLOGIN'))
        await _grant_compatibility_reader(connection, schema, reader, named=False)
        assert not await connection.scalar(
            text(
                "SELECT EXISTS(SELECT FROM information_schema.columns WHERE table_schema=:schema "
                "AND table_name IN ('npi_result_generation','reference_family_result_generation') AND column_name='serving_schema')"
            ),
            {"schema": schema},
        )


@pytest.mark.asyncio
async def test_legacy_and_named_metadata_keep_reader_first_origins(monkeypatch):
    """Original and migrated catalogs expose identical origins without rewriting them on read."""
    schema = "generation_reader_compat_" + uuid4().hex
    reader = "generation_reader_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    url = _database_url()
    engine = create_async_engine(url, poolclass=NullPool)
    raw = None
    try:
        await _create_compatibility_reader_fixture(engine, schema, reader)
        raw = await asyncpg.connect(
            make_url(url).set(drivername="postgresql").render_as_string(hide_password=False), timeout=5
        )
        before = await _read_compatibility_origins(engine, schema, reader)
        await _read_raw_generation_projections(raw, schema, before, named=False)
        async with engine.begin() as connection:
            await _run_migration(
                connection, _NPI_MIGRATION_PATH.with_name("20261114160000_npi_named_generation.py"), "upgrade"
            )
            await install_named_generation(connection, schema)
            await _grant_compatibility_reader(connection, schema, reader, named=True)
        assert await _read_compatibility_origins(engine, schema, reader) == before
        await _read_raw_generation_projections(raw, schema, before, named=True)
        await _assert_partial_named_catalog_refused(engine, schema, reader)
        await _assert_named_column_permission_required(engine, schema, reader)
        assert await _read_compatibility_origins(engine, schema, reader) == before
        assert before[0].serving_generation.origin_lineage_id == before[0].local_lineage_id
        assert before[1].serving_generation.origin_lineage_id == before[1].local_lineage_id
    finally:
        if raw is not None:
            await raw.close(timeout=5)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
                await connection.execute(text(f'DROP ROLE IF EXISTS "{reader}"'))
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": reader}
                )
        finally:
            await engine.dispose()
