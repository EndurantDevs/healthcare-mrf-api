# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib.util
import os
import re
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Database
from process import initial, plan_summary
from process import mrf_publication_receipt as publication_receipt
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.reference_family_generation_fixture import (
    generation_shape_check,
    install_source_generation_guards,
    native_reference_source,
)
from tests.test_mrf_publication_receipt_postgres import (
    _create_canonical_address_source,
    _drop_mrf_source_role,
    _native_mrf_source_sessions,
)

_DSN_ENV = "HLTHPRT_MRF_RESULT_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE = re.compile(r"hc_mrf_archive_[0-9a-f]{32}\Z")
_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260914130000_mrf_result_generation.py"
)
_TIGER_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260920110000_tiger_result_generation.py"
)
_MRF_ADDRESS_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260920120000_mrf_address_result_generation.py"
)


async def _prepare_synthetic_publication_schema(connection, schema):
    """Add the receipt migration and canonical-address coverage for a fixture."""

    migration = Path(__file__).resolve().parents[1] / "alembic/versions/20260920170000_mrf_publication_receipt.py"

    await _run_migration(connection, schema, "upgrade", migration)
    if not await connection.scalar(text("SELECT to_regclass(:table)"), {"table": f"{schema}.address_archive_v2"}):
        await _create_canonical_address_source(connection, archive, schema)
    if not await connection.scalar(text("SELECT to_regclass(:table)"), {"table": f"{schema}.plan_search_summary"}):
        model = plan_summary.PlanSearchSummary
        table = model.__table__.to_metadata(MetaData(), schema=schema)
        await connection.run_sync(lambda sync: table.create(sync))
        await archive._create_model_indexes(connection, archive.ReferenceFamilySpec("mrf", (model,)), schema)
    synthetic_key = uuid4()
    for name in ("mrf_address", "mrf_address_evidence"):
        await connection.execute(
            text(f'UPDATE "{schema}".{name} SET address_key=:key WHERE address_key IS NULL'),
            {"key": synthetic_key},
        )
    await connection.execute(
        text(f'''
        INSERT INTO "{schema}".address_archive_v2 (address_key, identity_key, source_bits)
        SELECT address_key, address_key::text, 16 FROM "{schema}".mrf_address
        UNION SELECT address_key, address_key::text, 16 FROM "{schema}".mrf_address_evidence
    ''')
    )
    assert await connection.scalar(text(f'SELECT count(*) FROM "{schema}".address_archive_v2')) > 0


@pytest.mark.asyncio
async def test_mrf_fixture_receipt_migration_targets_the_owned_source_schema(monkeypatch):
    """The real receipt belongs to the UUID source, not the default shared-type namespace."""
    migration = AsyncMock()
    monkeypatch.setattr(f"{__name__}._run_migration", migration)
    connection = SimpleNamespace(scalar=AsyncMock(side_effect=[True, True, 1]), execute=AsyncMock())
    await _prepare_synthetic_publication_schema(connection, "synthetic_source")
    migration.assert_awaited_once_with(
        connection,
        "synthetic_source",
        "upgrade",
        Path(__file__).resolve().parents[1] / "alembic/versions/20260920170000_mrf_publication_receipt.py",
    )


@pytest.mark.asyncio
async def test_mrf_unfinalized_fixture_provisions_the_real_summary_model_and_indexes(monkeypatch):
    """Catalog closure precedes SOURCE enrollment without pretending a finalizer completed."""
    monkeypatch.setattr(f"{__name__}._run_migration", AsyncMock())
    indexes = AsyncMock()
    monkeypatch.setattr(archive, "_create_model_indexes", indexes)
    connection = SimpleNamespace(
        scalar=AsyncMock(side_effect=[True, False, 1]), execute=AsyncMock(), run_sync=AsyncMock()
    )
    await _prepare_synthetic_publication_schema(connection, "synthetic_source")
    connection.run_sync.assert_awaited_once()
    assert indexes.call_args.args[0] is connection
    assert indexes.call_args.args[1].importer_id == "mrf"
    assert indexes.call_args.args[1].model_types == (plan_summary.PlanSearchSummary,)
    assert indexes.call_args.args[2] == "synthetic_source"


@pytest.mark.asyncio
async def test_mrf_fixture_publisher_gets_exact_guard_execute_without_code_membership():
    """Table ownership and the existing statement guard are independently enrolled."""
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=["issuer"]), execute=AsyncMock(), scalar=AsyncMock(return_value=False)
    )
    await _transfer_destination_owner(session, "synthetic_destination", "synthetic_owner", "synthetic_publisher")
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements == [
        'ALTER SCHEMA "synthetic_destination" OWNER TO "synthetic_owner"',
        'ALTER TABLE "synthetic_destination"."issuer" OWNER TO "synthetic_owner"',
        'GRANT EXECUTE ON FUNCTION "synthetic_destination".advance_reference_source_generation() TO "synthetic_publisher"',
    ]
    assert session.scalar.call_args.args[1] == {
        "role": "synthetic_publisher",
        "function": '"synthetic_destination".advance_reference_source_generation()',
    }


async def _provision_canonical_type(sessions):
    """Provision the declared shared type before any restricted fixture actor connects."""
    from db.models import AddressArchiveV2

    async with sessions.begin() as session:
        is_schema_new = not await session.scalar(text("SELECT to_regnamespace('mrf') IS NOT NULL"))
        is_type_new = not await session.scalar(text("SELECT to_regtype('mrf.address_archive_geo_source') IS NOT NULL"))
        await session.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await session.execute(text('CREATE SCHEMA IF NOT EXISTS "mrf"'))
        connection = await session.connection()
        await connection.run_sync(
            lambda sync: AddressArchiveV2.__table__.c.geo_source.type.create(sync, checkfirst=True)
        )
    return is_schema_new, is_type_new


async def _retire_canonical_type(sessions, created):
    async with sessions.begin() as session:
        if created[1]:
            await session.execute(text('DROP TYPE "mrf".address_archive_geo_source RESTRICT'))
        if created[0]:
            await session.execute(text('DROP SCHEMA "mrf" RESTRICT'))


async def _drop_owned_schemas(sessions, schemas):
    async with sessions.begin() as session:
        for schema in sorted(schemas):
            await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


@asynccontextmanager
async def _native_mrf_publisher(sessions, custody, destination_schema, owned_schemas):
    """Publish as a real non-code-owner LOGIN, distinct from the SELECT-only SOURCE actor."""
    role = "mrf_publisher_" + uuid4().hex
    password = uuid4().hex
    publisher_engine = create_async_engine(
        make_url(_database_url()).set(username=role, password=password), poolclass=NullPool, hide_parameters=True
    )
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_drop_mrf_source_role, sessions, role)
        cleanup.push_async_callback(_drop_owned_schemas, sessions, owned_schemas)
        cleanup.push_async_callback(publisher_engine.dispose)
        async with sessions.begin() as session:
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            try:
                await driver.execute(
                    f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOBYPASSRLS"
                )
            except Exception:
                raise RuntimeError("native publisher principal bootstrap failed") from None
            await session.execute(text(f'GRANT "{custody.owner}" TO "{role}"'))
            await session.execute(text(f'GRANT CREATE ON DATABASE "{make_url(_database_url()).database}" TO "{role}"'))
            await session.execute(text(f'GRANT USAGE ON SCHEMA "mrf" TO "{role}"'))
            if destination_schema:
                await _transfer_destination_owner(session, destination_schema, custody.owner, role)
        publisher = async_sessionmaker(publisher_engine, expire_on_commit=False)
        async with publisher.begin() as session:
            assert await session.scalar(text("SELECT current_user=session_user"))
            assert not await session.scalar(
                text("SELECT rolsuper OR rolcreaterole OR rolbypassrls FROM pg_roles WHERE rolname=current_user")
            )
            assert not await session.scalar(
                text(
                    "SELECT pg_has_role(current_user,typowner,'MEMBER') FROM pg_type WHERE oid=to_regtype('mrf.address_archive_geo_source')"
                )
            )
        yield publisher


async def _transfer_destination_owner(session, schema, owner, publisher_role):
    """Enroll the ordinary writer for the existing guard without granting its code ownership."""
    await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'))
    names = await session.scalars(
        text("SELECT tablename FROM pg_tables WHERE schemaname=:schema ORDER BY tablename"), {"schema": schema}
    )
    for name in names:
        await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{owner}"'))
    await session.execute(
        text(f'GRANT EXECUTE ON FUNCTION "{schema}".advance_reference_source_generation() TO "{publisher_role}"')
    )
    assert not await session.scalar(
        text("SELECT pg_has_role(:role,proowner,'MEMBER') FROM pg_proc WHERE oid=CAST(:function AS regprocedure)"),
        {"role": publisher_role, "function": f'"{schema}".advance_reference_source_generation()'},
    )


@asynccontextmanager
async def _native_mrf_archive(sessions, source_schema, destination_schema, owned_schemas):
    async with AsyncExitStack() as cleanup:
        source = await cleanup.enter_async_context(
            _native_mrf_source_sessions(sessions, source_schema, _database_url())
        )
        reader = source.kw["bind"].url.username
        custody = await cleanup.enter_async_context(native_reference_source(sessions, readers=(reader,)))
        publisher = await cleanup.enter_async_context(
            _native_mrf_publisher(sessions, custody, destination_schema, owned_schemas)
        )
        yield SimpleNamespace(admin=sessions, source=source, publisher=publisher, custody=custody)


async def _configure_synthetic_plan_summary_tables(connection, schema, patch):
    """Bind plan-summary tables to the synthetic schema and create missing ones."""

    metadata = MetaData()
    for attribute in (
        "plan_table",
        "plan_attributes_table",
        "plan_benefits_table",
        "plan_prices_table",
        "summary_table",
    ):
        table = getattr(plan_summary, attribute).to_metadata(metadata, schema=schema)
        patch.setattr(plan_summary, attribute, table)
        if table.name not in {"plan", "plan_search_summary"}:
            await connection.run_sync(lambda sync, table=table: table.create(sync, checkfirst=True))


async def _complete_synthetic_publication(monkeypatch, engine, sessions, schema, *, initialize=True):
    """Build the real summary and receipt after the fixture's family rotation."""

    database = Database(engine=engine, session_factory=sessions)

    async def ready(_test_mode):
        return None

    with monkeypatch.context() as patch:
        patch.setenv("HLTHPRT_DB_SCHEMA", schema)
        patch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2")
        patch.delenv("DB_SCHEMA", raising=False)
        patch.setattr(publication_receipt, "db", database)
        patch.setattr(plan_summary, "db", database)
        patch.setattr(plan_summary, "ensure_database", ready)
        async with engine.begin() as connection:
            if initialize:
                await _prepare_synthetic_publication_schema(connection, schema)
            await _configure_synthetic_plan_summary_tables(connection, schema, patch)
        patch.setattr(
            plan_summary,
            "PRICE_RATE_COLUMNS",
            tuple(plan_summary.plan_prices_table.c[column.name] for column in plan_summary.PRICE_RATE_COLUMNS),
        )
        attempt = await publication_receipt.begin_publication(schema, "synthetic")
        async with sessions() as session, session.begin():
            authority = await generation.read_reference_family_result_generation_authority(
                session,
                importer_id="mrf",
                schema_name=schema,
            )
        await plan_summary.rebuild_plan_search_summary(publication=(attempt, authority, False))


class _PublisherDatabase:
    """Run the normal importer publication against one disposable session."""

    def __init__(self, session, schema_name):
        self.session = session
        self.schema_name = schema_name

    def transaction(self):
        return self.session.begin()

    def in_transaction(self):
        return self.session.in_transaction()

    async def execute(self, statement, params=None):
        return await self.session.execute(statement, params or {})

    async def status(self, statement):
        return await self.session.execute(text(statement))

    async def create_table(self, table, *, checkfirst=False):
        if table.schema != self.schema_name:
            table = table.to_metadata(MetaData(), schema=self.schema_name)
        connection = await self.session.connection()
        await connection.run_sync(lambda sync: table.create(sync, checkfirst=checkfirst))


def _database_url() -> str:
    raw_dsn = os.getenv(_DSN_ENV, "")
    if not raw_dsn:
        pytest.skip(f"{_DSN_ENV} is not set")
    database_url = make_url(raw_dsn)
    if (
        not database_url.drivername.startswith("postgresql")
        or database_url.host not in {"127.0.0.1", "localhost", "postgres"}
        or _LOCAL_DATABASE.fullmatch(str(database_url.database or "")) is None
    ):
        pytest.fail(f"{_DSN_ENV} must identify a UUID-owned PostgreSQL test database")
    return database_url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _create_family(session, schema_name: str, *, revision_guards=True) -> None:
    await session.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
    await archive._create_model_family(session, archive.reference_family_spec("mrf"), schema_name)
    diagnostic_table = initial.ImportLog.__table__.to_metadata(MetaData(), schema=schema_name)
    connection = await session.connection()
    await connection.run_sync(lambda sync: diagnostic_table.create(sync))
    await session.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
            "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
            f"{generation_shape_check()}))"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES "
            "('mrf', :mrf_lineage_id, 0), ('mrf-address', :address_lineage_id, 0)"
        ),
        {"mrf_lineage_id": uuid4(), "address_lineage_id": uuid4()},
    )
    if revision_guards:
        await install_source_generation_guards(await session.connection(), schema_name)


async def _insert_family_rows(session, schema_name: str, marker: str) -> None:
    plan_id = "00000000000001"
    statements = (
        ("issuer", "(issuer_id, issuer_name) VALUES (1, :marker)"),
        ("plan", "(plan_id, year, marketing_name) VALUES (:plan_id, 2026, :marker)"),
        (
            "plan_formulary",
            "(plan_id, year, drug_tier, pharmacy_type) VALUES (:plan_id, 2026, 'generic', 'retail')",
        ),
        (
            "plan_benefits_marketplace",
            "(plan_id, year, checksum, benefit_name) VALUES (:plan_id, 2026, 1, :marker)",
        ),
        ("plan_transparency", "(plan_id, year, issuer_name) VALUES (:plan_id, 2026, :marker)"),
        (
            "plan_drug_raw",
            "(plan_id, rxnorm_id, drug_name) VALUES (:plan_id, '1', :marker)",
        ),
        (
            "plan_drug_stats",
            "(plan_id, total_drugs, auth_required, auth_not_required, step_required, "
            "step_not_required, quantity_limit, quantity_no_limit) "
            "VALUES (:plan_id, 1, 0, 1, 0, 1, 0, 1)",
        ),
        (
            "plan_drug_tier_stats",
            "(plan_id, drug_tier, drug_count) VALUES (:plan_id, 'generic', 1)",
        ),
        (
            "plan_npi_raw",
            "(npi, checksum_network, name_or_facility_name) VALUES (1000000001, 1, :marker)",
        ),
        (
            "plan_networktier",
            "(plan_id, checksum_network, network_tier) VALUES (:plan_id, 1, :marker)",
        ),
        (
            "mrf_address",
            "(checksum, npi, type, first_line) VALUES (1, 1000000001, 'practice', :marker)",
        ),
        (
            "mrf_address_evidence",
            "(evidence_checksum, npi, type, checksum, import_id, source_record_id, first_line) "
            "VALUES (1, 1000000001, 'practice', 1, 'synthetic-import', 'record-1', :marker)",
        ),
    )
    assert tuple(table_name for table_name, _ in statements) == generation.RELATION_NAMES_BY_IMPORTER["mrf"]
    for table_name, values_sql in statements:
        await session.execute(
            text(f'INSERT INTO "{schema_name}"."{table_name}" {values_sql}'),
            {"marker": marker, "plan_id": plan_id},
        )
    await session.execute(
        text(f'INSERT INTO "{schema_name}".log (issuer_id, checksum, text) VALUES (1, 1, :marker)'),
        {"marker": marker},
    )


async def _copy_prepared_stage(session, prepared, restored) -> None:
    for model in archive.reference_family_spec("mrf", canonical=True).model_types:
        table_name = model.__tablename__
        columns = tuple(column.name for column in model.__table__.columns)
        projection = ",".join(f'"{column}"' for column in columns)
        await archive.native_copy_projection(
            session,
            f'SELECT {projection} FROM "{prepared.ownership.schema_name}"."{table_name}"',
            schema_name=restored.schema_name,
            table_name=table_name,
            columns=columns,
            max_bytes=1_000_000,
            timeout=30,
        )
    await archive._rebase_owned_sequences(session, restored.schema_name, "mrf")


@pytest.mark.asyncio
async def test_mrf_fixture_restore_copies_every_compiled_column_with_bounded_native_copy(monkeypatch):
    """Typed canonical rows use the same bounded native copier as every serving relation."""
    copy = AsyncMock()
    rebase = AsyncMock()
    monkeypatch.setattr(archive, "native_copy_projection", copy)
    monkeypatch.setattr(archive, "_rebase_owned_sequences", rebase)
    session = object()
    prepared = SimpleNamespace(ownership=SimpleNamespace(schema_name="synthetic_source"))
    restored = SimpleNamespace(schema_name="synthetic_destination")
    await _copy_prepared_stage(session, prepared, restored)
    models = archive.reference_family_spec("mrf", canonical=True).model_types
    assert len(copy.await_args_list) == len(models) == 14
    for call, model in zip(copy.await_args_list, models, strict=True):
        assert call.args[0] is session
        assert call.kwargs == {
            "schema_name": restored.schema_name,
            "table_name": model.__tablename__,
            "columns": tuple(column.name for column in model.__table__.columns),
            "max_bytes": 1_000_000,
            "timeout": 30,
        }
        assert call.args[1].endswith(f'FROM "synthetic_source"."{model.__tablename__}"')
    rebase.assert_awaited_once_with(session, restored.schema_name, "mrf")


@pytest.mark.asyncio
async def test_mrf_source_reader_sequence_grant_is_select_only_before_custody_closes(monkeypatch):
    """Declared sequence state is readable for pg_dump, never advanced by the Reader."""
    from tests.reference_family_generation_fixture import ReferenceSourceCustody

    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    ownership = archive.ReferenceFamilyStageOwnership(
        "mrf-address", uuid4(), "synthetic_stage", 1, (("mrf_address", 2),)
    )
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=[True, False, False]),
        execute=AsyncMock(),
        get_transaction=lambda: "synthetic-transaction",
    )
    custody = ReferenceSourceCustody("synthetic_owner", ("synthetic_reader",))
    await custody.precreate(session, ownership)
    grants = [str(call.args[0]) for call in session.execute.await_args_list if str(call.args[0]).startswith("GRANT")]
    assert 'GRANT SELECT ON ALL SEQUENCES IN SCHEMA "synthetic_stage" TO "synthetic_reader"' in grants
    assert all("UPDATE" not in grant and "USAGE ON ALL SEQUENCES" not in grant for grant in grants)
    assert custody.precreated[ownership.dataset_id] == ("synthetic-transaction", ownership)
    assert custody.closed == {}


async def _prepare_restored_candidate(native, source_schema: str, prepared_dataset_id, restored_dataset_id):
    prepared_records = []

    async def retain_prepared(session, prepared) -> None:
        await native.custody.retain(session, prepared)
        prepared_records.append(prepared)

    async def dependencies(_session):
        return {"plan-attributes": "b" * 64}

    prepared = await archive.prepare_reference_family_archive_source(
        native.admin,
        importer_id="mrf",
        schema_name=source_schema,
        source_metadata={"release": "synthetic-mrf-2"},
        dataset_id=prepared_dataset_id,
        on_prepared=retain_prepared,
        dependency_factory=dependencies,
        source_copy=native.custody.source_copy,
        on_precreated=native.custody.precreate,
        source_sessions=native.source,
    )
    assert prepared_records == [prepared]
    async with native.admin() as session, session.begin():
        await session.execute(text(f"UPDATE \"{source_schema}\".issuer SET issuer_name = 'later'"))
    async with native.publisher() as session, session.begin():
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="mrf",
            dataset_id=restored_dataset_id,
            canonical=True,
        )
        await native.custody.precreate(session, restored)
        await _copy_prepared_stage(session, prepared, restored)
        await archive.complete_reference_family_restore(session, restored)
        await archive.validate_reference_family_stage(
            session,
            ownership=restored,
            manifest=prepared.manifest,
        )
        await native.custody.retire(session, prepared.ownership)
    return prepared.manifest, restored


async def _prepare_activation(sessions, destination_schema, manifest, ownership):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session,
            importer_id="mrf",
            schema_name=destination_schema,
            canonical=True,
        )
        owner_oid = await session.scalar(
            text("SELECT nspowner FROM pg_namespace WHERE oid=:oid"), {"oid": ownership.schema_oid}
        )
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=manifest,
            package_id="a" * 64,
            profile_contract=archive.TYPED_MRF_CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return incumbent, validation, owner_oid


async def _activate(session, ownership, manifest, incumbent, validation, owner_oid, source_generation):
    return await archive.activate_validated_reference_family_stage(
        session,
        ownership=ownership,
        manifest=manifest,
        expected_incumbent=incumbent,
        validation_receipt=validation,
        cutover=archive.ReferenceFamilyCutoverAuthority(
            "a" * 64,
            archive.TYPED_MRF_CONTRACT,
            owner_oid,
            owner_oid,
            "automatic",
            source_generation.as_dict(),
        ),
    )


async def _assert_replaced_sequence_rejected(sessions, ownership) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(text(f'DROP SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq CASCADE'))
        await session.execute(text(f'CREATE SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq'))
        owner = await session.scalar(
            text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=:oid"),
            {"oid": dict(ownership.relation_oids)["issuer"]},
        )
        await session.execute(text(f'ALTER SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq OWNER TO "{owner}"'))
        await session.execute(
            text(
                f'ALTER SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq '
                f'OWNED BY "{ownership.schema_name}".issuer.issuer_id'
            )
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership differs"):
            await archive.verify_reference_family_stage_ownership(session, ownership)
        await transaction.rollback()


async def _assert_wrong_sequence_column_rejected(sessions, ownership) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(
            text(
                f'ALTER SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq '
                f'OWNED BY "{ownership.schema_name}".issuer.issuer_name'
            )
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="sequence set is invalid"):
            await archive.verify_reference_family_stage_ownership(session, ownership)
        await transaction.rollback()


async def _assert_stale_incumbent_rejected(
    sessions,
    ownership,
    manifest,
    incumbent,
    validation,
    owner_oid,
    source_generation,
) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(text(f'ALTER TABLE "{incumbent.schema_name}".issuer RENAME TO issuer_stale'))
        await session.execute(
            text(
                f'CREATE TABLE "{incumbent.schema_name}".issuer '
                f'(LIKE "{incumbent.schema_name}".issuer_stale INCLUDING ALL)'
            )
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent changed"):
            await _activate(
                session,
                ownership,
                manifest,
                incumbent,
                validation,
                owner_oid,
                source_generation,
            )
        await transaction.rollback()


async def _seed_mrf_roundtrip(session, source_schema, destination_schema, unrelated_schema):
    await _create_family(session, source_schema)
    await _create_family(session, destination_schema)
    await _insert_family_rows(session, source_schema, "source-v1")
    await _insert_family_rows(session, destination_schema, "destination-v1")
    await session.execute(
        text(
            f'INSERT INTO "{destination_schema}".plan_search_summary '
            "(plan_id, year, marketing_name) VALUES ('00000000000001', 2026, 'destination-v1')"
        )
    )
    await _create_canonical_address_source(await session.connection(), archive, destination_schema)
    for table_name, marker in (("history", "retained-history"), ("account_state", "retained-account")):
        await session.execute(text(f'CREATE TABLE "{destination_schema}".{table_name} (marker text PRIMARY KEY)'))
        await session.execute(text(f"INSERT INTO \"{destination_schema}\".{table_name} VALUES ('{marker}')"))
    await session.execute(text(f'CREATE SCHEMA "{unrelated_schema}"'))
    await session.execute(text(f'CREATE TABLE "{unrelated_schema}".keep_me (marker text PRIMARY KEY)'))
    await session.execute(text(f"INSERT INTO \"{unrelated_schema}\".keep_me VALUES ('keep')"))
    first_source = await generation.publish_local_reference_family_generation(
        session, importer_id="mrf", schema_name=source_schema
    )
    await generation.publish_adopted_reference_family_generation(
        session,
        importer_id="mrf",
        schema_name=destination_schema,
        source_generation=first_source.serving_generation,
        source_revision_tracked=True,
    )
    await session.execute(text(f"UPDATE \"{source_schema}\".issuer SET issuer_name = 'source-v2'"))
    await session.execute(text(f"UPDATE \"{source_schema}\".plan SET marketing_name = 'source-v2'"))
    return await generation.publish_local_reference_family_generation(
        session, importer_id="mrf", schema_name=source_schema
    )


async def _assert_rolled_back_activation(
    sessions, destination_schema, ownership, manifest, incumbent, validation, owner_oid, source_generation
) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await _activate(session, ownership, manifest, incumbent, validation, owner_oid, source_generation)
        assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v2"
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "destination-v1"
        assert (
            await session.scalar(text(f'SELECT marketing_name FROM "{destination_schema}".plan_search_summary'))
            == "destination-v1"
        )
        assert (
            await session.scalar(
                text(f'SELECT count(*) FROM "{destination_schema}".address_archive_v2 WHERE source_bits=16')
            )
            == 0
        )


async def _assert_committed_activation(native, destination_schema, unrelated_schema, activation_by_field) -> None:
    sessions = native.publisher
    async with sessions() as session, session.begin():
        receipt = await _activate(session, **activation_by_field)
        assert receipt.predecessor_schema_name is not None
    async with sessions() as session, session.begin():
        adopted = await generation.read_reference_family_result_generation_authority(
            session, importer_id="mrf", schema_name=destination_schema
        )
        assert adopted.serving_generation == activation_by_field["source_generation"]
        assert len(adopted.relation_oids) == 12
        assert await session.scalar(text(f'SELECT text FROM "{destination_schema}".log')) == "destination-v1"
        assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v2"
        assert (
            await session.scalar(text(f'SELECT marketing_name FROM "{destination_schema}".plan_search_summary'))
            == "source-v2"
        )
        assert (
            await session.scalar(text(f'SELECT issuer_name FROM "{receipt.predecessor_schema_name}".issuer'))
            == "destination-v1"
        )
        assert (
            await session.scalar(
                text(f'SELECT marketing_name FROM "{receipt.predecessor_schema_name}".plan_search_summary')
            )
            == "destination-v1"
        )
        summary_indexes = set(
            await session.scalars(
                text(
                    "SELECT indexdef FROM pg_catalog.pg_indexes "
                    "WHERE schemaname=:schema AND tablename='plan_search_summary'"
                ),
                {"schema": destination_schema},
            )
        )
        assert any(" UNIQUE INDEX plan_search_summary_pkey " in index for index in summary_indexes)
        assert any(index.endswith(" (state, year)") for index in summary_indexes)
        assert any(index.endswith(" (issuer_id, year)") for index in summary_indexes)
        assert await session.scalar(text(f"SELECT nextval('\"{destination_schema}\".issuer_issuer_id_seq')")) == 2
        assert (
            await session.scalar(
                text(f"SELECT nextval('\"{destination_schema}\".mrf_address_evidence_evidence_checksum_seq')")
            )
            == 2
        )
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema)"), {"schema": activation_by_field["ownership"].schema_name}
            )
            is None
        )
    async with native.admin.begin() as observer:
        assert await observer.scalar(text(f'SELECT marker FROM "{unrelated_schema}".keep_me')) == "keep"
        for table_name, marker in (("history", "retained-history"), ("account_state", "retained-account")):
            assert await observer.scalar(text(f'SELECT marker FROM "{destination_schema}".{table_name}')) == marker


async def _prepare_destination_address_merge(sessions, destination_schema, ownership):
    """Insert one local address contribution and return its canonical key."""

    async with sessions() as session, session.begin():
        key = await session.scalar(
            text(
                f'SELECT address_key FROM "{ownership.schema_name}".mrf_canonical_address ORDER BY address_key LIMIT 1'
            )
        )
        await session.execute(
            text(
                f'INSERT INTO "{destination_schema}".address_archive_v2 '
                "(address_key, identity_key, identity_version, precision, premise_key, line1_norm, unit_norm, "
                "city_norm, state_code, zip5, zip4, country_code, source_bits, display_priority, formatted_address) "
                f"SELECT address_key,identity_key,identity_version,precision,premise_key,line1_norm,unit_norm, "
                f'city_norm,state_code,zip5,zip4,country_code,1,0,:note FROM "{ownership.schema_name}".mrf_canonical_address '
                "WHERE address_key=:key"
            ),
            {"key": key, "note": "keep-local"},
        )
    return key


async def _assert_destination_address_conflict_rejected(sessions, destination_schema, key, activation_by_field):
    """Reject activation when an incumbent local address conflicts with the stage."""

    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(
            text(f'UPDATE "{destination_schema}".address_archive_v2 SET merged_into=:other WHERE address_key=:key'),
            {"key": key, "other": key},
        )
        with pytest.raises(RuntimeError, match="canonical destination identity conflicts"):
            await _activate(session, **activation_by_field)
        await transaction.rollback()


async def _assert_destination_address_merge_retained(sessions, destination_schema, key):
    """Confirm activation preserves and combines the local address contribution."""

    async with sessions() as session, session.begin():
        assert (
            await session.scalar(
                text(f'SELECT formatted_address FROM "{destination_schema}".address_archive_v2 WHERE address_key=:key'),
                {"key": key},
            )
            == "keep-local"
        )
        assert (
            await session.scalar(
                text(f'SELECT source_bits FROM "{destination_schema}".address_archive_v2 WHERE address_key=:key'),
                {"key": key},
            )
            == 17
        )


async def _exercise_mrf_roundtrip(
    native,
    source_schema,
    destination_schema,
    unrelated_schema,
    prepared_dataset_id,
    restored_dataset_id,
) -> None:
    """Exercise address conflicts, rollbacks, and successful MRF archive activation."""

    manifest, ownership = await _prepare_restored_candidate(
        native, source_schema, prepared_dataset_id, restored_dataset_id
    )
    sessions = native.publisher
    key = await _prepare_destination_address_merge(sessions, destination_schema, ownership)
    incumbent, validation, owner_oid = await _prepare_activation(sessions, destination_schema, manifest, ownership)
    activation_by_field = {
        "ownership": ownership,
        "manifest": manifest,
        "incumbent": incumbent,
        "validation": validation,
        "owner_oid": owner_oid,
        "source_generation": manifest.source_serving_generation,
    }
    await _assert_destination_address_conflict_rejected(sessions, destination_schema, key, activation_by_field)
    await _assert_replaced_sequence_rejected(sessions, ownership)
    await _assert_wrong_sequence_column_rejected(sessions, ownership)
    await _assert_stale_incumbent_rejected(sessions, **activation_by_field)
    await _assert_rolled_back_activation(sessions, destination_schema, **activation_by_field)
    await _assert_committed_activation(native, destination_schema, unrelated_schema, activation_by_field)
    await _assert_destination_address_merge_retained(sessions, destination_schema, key)


@pytest.mark.asyncio
async def test_mrf_revision_migration_narrows_legacy_identity_without_trusting_it():
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = "mrf_legacy_diagnostics_" + uuid4().hex
    migration = _MIGRATION_PATH.with_name("20260929040000_reference_source_generation_guard.py")
    origin = uuid4()
    try:
        async with sessions.begin() as session:
            await _create_family(session, schema, revision_guards=False)
            result_oids = await generation.current_reference_family_relation_oids(
                session, importer_id="mrf", schema_name=schema
            )
            log_oid = await session.scalar(text(f"SELECT '\"{schema}\".log'::regclass::oid::bigint"))
            legacy_oids = [*result_oids[:8], log_oid, *result_oids[8:]]
            await session.execute(
                text(
                    f'UPDATE "{schema}".reference_family_result_generation SET local_generation=4, '
                    "origin_lineage_id=:origin, origin_generation=17, published_at='2026-09-01T00:00:00Z', "
                    "relation_oids=:oids WHERE importer_id='mrf'"
                ),
                {"origin": origin, "oids": legacy_oids},
            )
            await install_source_generation_guards(await session.connection(), schema)
            narrowed = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=schema
            )
            assert narrowed.relation_oids == result_oids
            assert narrowed.local_generation == 4
            assert narrowed.serving_generation.origin_lineage_id == str(origin)
            assert narrowed.serving_generation.origin_generation == 17
            assert not await session.scalar(
                text(
                    f'SELECT source_revision_tracked FROM "{schema}".reference_family_result_generation '
                    "WHERE importer_id='mrf'"
                )
            )
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(await session.connection(), schema, "downgrade", migration)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_shared_diagnostics_preserve_adopted_mrf_generation():
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = "mrf_diagnostics_" + uuid4().hex
    try:
        async with sessions.begin() as session:
            await _create_family(session, schema)
            await _insert_family_rows(session, schema, "result")
            adopted = await generation.publish_adopted_reference_family_generation(
                session,
                importer_id="mrf",
                schema_name=schema,
                source_generation={
                    "origin_lineage_id": str(uuid4()),
                    "origin_generation": 17,
                    "published_at": "2026-09-01T00:00:00Z",
                },
                source_revision_tracked=True,
            )
        async with sessions.begin() as session:
            await session.execute(
                text(f'''INSERT INTO "{schema}".log (issuer_id, checksum, text, source)
                         VALUES (1, 2, 'synthetic diagnostic', 'ptg')''')
            )
        async with sessions.begin() as session:
            assert (
                await generation.read_reference_family_result_generation_authority(
                    session, importer_id="mrf", schema_name=schema
                )
                == adopted
            )
            await session.execute(text(f"UPDATE \"{schema}\".issuer SET issuer_name='changed result'"))
        async with sessions.begin() as session:
            changed = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=schema
            )
            assert changed.local_generation == adopted.local_generation + 1
            assert changed.serving_generation.origin_lineage_id == adopted.local_lineage_id
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_mrf_model_family_roundtrip_retains_predecessor_and_rolls_back(monkeypatch):
    """Transfer 13 serving relations, preserving diagnostics, CAS, and atomic generation."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    source_schema = f"mrf_source_{token}"
    destination_schema = f"mrf_destination_{token}"
    unrelated_schema = f"mrf_unrelated_{token}"
    prepared_dataset_id, restored_dataset_id = uuid4(), uuid4()
    owned_schemas = {
        source_schema,
        destination_schema,
        unrelated_schema,
        archive.reference_family_stage_schema(prepared_dataset_id),
        archive.reference_family_stage_schema(restored_dataset_id),
        archive.reference_family_predecessor_schema(restored_dataset_id),
    }
    created = False, False
    try:
        created = await _provision_canonical_type(sessions)
        async with sessions() as session, session.begin():
            await _seed_mrf_roundtrip(session, source_schema, destination_schema, unrelated_schema)
        await _complete_synthetic_publication(monkeypatch, engine, sessions, source_schema)
        async with _native_mrf_archive(sessions, source_schema, destination_schema, owned_schemas) as native:
            await _exercise_mrf_roundtrip(
                native, source_schema, destination_schema, unrelated_schema, prepared_dataset_id, restored_dataset_id
            )
    finally:
        async with engine.begin() as connection:
            for schema_name in owned_schemas:
                if schema_name:
                    await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await _retire_canonical_type(sessions, created)
        await engine.dispose()


async def _initialize_published_mrf_schema(session, schema_name):
    """Create only the ordinary importer's generation authority prerequisites."""

    await session.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
    await session.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    await session.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
            "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
            f"{generation_shape_check()}))"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES "
            "('mrf', :mrf_lineage_id, 0), ('mrf-address', :address_lineage_id, 0)"
        ),
        {"mrf_lineage_id": uuid4(), "address_lineage_id": uuid4()},
    )
    await install_source_generation_guards(await session.connection(), schema_name)
    await session.commit()


async def _stage_normal_mrf_family(session, schema_name, import_date, address_key):
    """Build the ordinary importer's indexed synthetic family without publishing it."""

    await initial._prepare_import_tables(import_date, True)
    address_stage = initial.make_class(initial.MRFAddress, import_date, schema_override=schema_name)
    evidence_stage = initial.make_class(initial.MRFAddressEvidence, import_date, schema_override=schema_name)
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}"."{address_stage.__tablename__}" '
            "(checksum, npi, type, first_line, phone_number, address_key) "
            "VALUES (1, 1000000001, 'practice', 'Synthetic', '5550100', :address_key)"
        ),
        {"address_key": address_key},
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}"."{evidence_stage.__tablename__}" '
            "(npi, type, checksum, import_id, source_record_id, first_line) "
            "VALUES (1000000001, 'practice', 1, 'synthetic-import', 'record-1', 'Synthetic')"
        )
    )
    await initial._create_named_indexes(address_stage, schema_name)
    await initial._create_named_indexes(evidence_stage, schema_name)
    await session.commit()


async def _publish_normal_mrf_stage(session, schema_name, import_date, address_key):
    """Use the production staging and table-swap functions with synthetic rows."""

    await _stage_normal_mrf_family(session, schema_name, import_date, address_key)
    await initial._publish_mrf_table_generation(import_date, schema_name)


async def _activate_manual_archive(sessions, destination_schema, manifest, ownership):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session,
            importer_id="mrf",
            schema_name=destination_schema,
            canonical=True,
        )
        return await archive.activate_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            authority="manual",
        )


async def _run_interleaved_archive_cycle(
    engine,
    native,
    monkeypatch,
    source_schema,
    destination_schema,
    dataset_ids,
):
    first_prepared, first_restored, second_prepared, second_restored = dataset_ids
    manifest, ownership = await _prepare_restored_candidate(
        native,
        source_schema,
        first_prepared,
        first_restored,
    )
    await _activate_manual_archive(native.publisher, destination_schema, manifest, ownership)

    async with native.publisher() as session:
        monkeypatch.setattr(initial, "db", _PublisherDatabase(session, destination_schema))
        monkeypatch.setattr(initial, "get_import_schema", lambda *_args: destination_schema)
        await _publish_normal_mrf_stage(session, destination_schema, "20260921", uuid4())
    async with native.admin.begin() as session:
        await session.execute(text(f"UPDATE \"{source_schema}\".issuer SET issuer_name = 'source-v3'"))

    with pytest.raises(RuntimeError, match="completion generation differs"):
        await _prepare_restored_candidate(native, source_schema, second_prepared, second_restored)
    await _complete_synthetic_publication(monkeypatch, engine, native.admin, source_schema, initialize=False)
    manifest, ownership = await _prepare_restored_candidate(
        native,
        source_schema,
        second_prepared,
        second_restored,
    )
    await _activate_manual_archive(native.publisher, destination_schema, manifest, ownership)


async def _initialize_interleaved_archive_source(monkeypatch, engine, sessions, source_schema, destination_schema):
    """Create a completed source publication and the destination-local address archive."""

    async with sessions() as session, session.begin():
        await _create_family(session, source_schema)
        await _create_family(session, destination_schema)
        await _insert_family_rows(session, source_schema, "source-v1")
        await _insert_family_rows(session, destination_schema, "destination-v1")
        await generation.publish_local_reference_family_generation(
            session,
            importer_id="mrf",
            schema_name=source_schema,
        )

    await _complete_synthetic_publication(monkeypatch, engine, sessions, source_schema)
    async with sessions() as session, session.begin():
        await _create_canonical_address_source(await session.connection(), archive, destination_schema)


async def _command(*args):
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    assert process.returncode == 0, stderr.decode()
    return stdout.decode()


@pytest.mark.asyncio
async def test_second_address_generation_failure_rolls_back_normal_mrf_rotation(monkeypatch):
    """The address generation is part of the same real table-swap transaction."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema_name = f"mrf_atomic_address_{uuid4().hex[:10]}"
    real_generation_writer = initial.publish_local_reference_family_generation
    try:
        async with sessions() as session:
            monkeypatch.setattr(initial, "db", _PublisherDatabase(session, schema_name))
            monkeypatch.setattr(initial, "get_import_schema", lambda *_args: schema_name)
            await _initialize_published_mrf_schema(session, schema_name)
            await _publish_normal_mrf_stage(session, schema_name, "20260920", uuid4())
            before_mrf = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=schema_name
            )
            before_address = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf-address", schema_name=schema_name
            )
            await session.commit()
            await _stage_normal_mrf_family(session, schema_name, "20260921", uuid4())

            async def fail_after_address_write(database, *, importer_id, schema_name):
                result = await real_generation_writer(
                    database,
                    importer_id=importer_id,
                    schema_name=schema_name,
                )
                if importer_id == "mrf-address":
                    raise RuntimeError("synthetic address generation failure")
                return result

            monkeypatch.setattr(initial, "publish_local_reference_family_generation", fail_after_address_write)
            with pytest.raises(RuntimeError, match="address generation failure"):
                await initial._publish_mrf_table_generation("20260921", schema_name)

            after_mrf = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=schema_name
            )
            after_address = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf-address", schema_name=schema_name
            )
            assert after_mrf == before_mrf
            assert after_address == before_address
            assert (
                await generation.current_reference_family_relation_oids(
                    session, importer_id="mrf", schema_name=schema_name
                )
                == before_mrf.relation_oids
            )
            assert await session.scalar(text(f"SELECT to_regclass('{schema_name}.mrf_address_20260921')"))
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _synthetic_dependencies(_session):
    return {"plan-attributes": "b" * 64}


async def _assert_full_mrf_stage(native, schema_name, dataset_id, address_key):
    prepared = await archive.prepare_reference_family_archive_source(
        native.admin,
        importer_id="mrf",
        schema_name=schema_name,
        source_metadata={"release": "synthetic-published-mrf"},
        dataset_id=dataset_id,
        on_prepared=native.custody.retain,
        dependency_factory=_synthetic_dependencies,
        source_copy=native.custody.source_copy,
        on_precreated=native.custody.precreate,
        source_sessions=native.source,
    )
    async with native.publisher() as session, session.begin():
        await archive.validate_reference_family_stage(
            session,
            ownership=prepared.ownership,
            manifest=prepared.manifest,
        )
        assert (
            await session.scalar(text(f'SELECT address_key FROM "{prepared.ownership.schema_name}".mrf_address'))
            == address_key
        )
        assert (
            await session.scalar(text(f'SELECT phone_number FROM "{prepared.ownership.schema_name}".mrf_address'))
            == "5550100"
        )
        assert prepared.manifest.auxiliary["table_name"] == "mrf_canonical_address"
        assert prepared.manifest.auxiliary == archive._canonical_model_receipt()
        assert prepared.ownership.auxiliary_oid is None
        assert "mrf_canonical_address" in dict(prepared.ownership.relation_oids)
        assert (
            await session.scalar(
                text(
                    f'SELECT count(*) FROM "{prepared.ownership.schema_name}".mrf_canonical_address '
                    "WHERE address_key=:key AND source_bits=16"
                ),
                {"key": address_key},
            )
            == 1
        )
        await session.execute(
            text(f'UPDATE "{prepared.ownership.schema_name}".mrf_canonical_address SET source_bits=1')
        )
        with pytest.raises(RuntimeError, match="canonical contribution address scope differs"):
            await archive.validate_reference_family_stage(
                session,
                ownership=prepared.ownership,
                manifest=prepared.manifest,
            )
    async with native.publisher() as session, session.begin():
        await native.custody.retire(session, prepared.ownership)


async def _dump_prepared_archive(native, prepared, path):
    database_url = native.source.kw["bind"].url.set(drivername="postgresql")
    async with native.source.begin() as session:
        for _, oid, _, _ in prepared.ownership.sequence_oids:
            assert await session.scalar(
                text("SELECT has_sequence_privilege(current_user,CAST(:oid AS oid),'SELECT')"), {"oid": oid}
            )
            assert not await session.scalar(
                text("SELECT has_sequence_privilege(current_user,CAST(:oid AS oid),'USAGE,UPDATE')"), {"oid": oid}
            )

    async def dump(capture):
        await _command(
            "pg_dump",
            "--dbname",
            database_url.render_as_string(hide_password=False),
            "--format=custom",
            "--no-owner",
            "--no-acl",
            "--schema",
            capture.ownership.schema_name,
            "--snapshot",
            capture.postgres_snapshot,
            "--file",
            str(path),
        )

    await archive.export_prepared_reference_family_archive(
        native.source, prepared=prepared, archive_copy=dump, verify_custody=native.custody.verify
    )
    return await _command("pg_restore", "--list", str(path))


async def _restore_prepared_address_archive(native, prepared, dataset_id, path):
    async with native.publisher() as session, session.begin():
        await native.custody.retire(session, prepared.ownership)
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="mrf-address",
            dataset_id=dataset_id,
        )
        await session.execute(text(f'ALTER SCHEMA "{restored.schema_name}" OWNER TO "{native.custody.owner}"'))
        for name, _ in restored.relation_oids:
            await session.execute(
                text(f'ALTER TABLE "{restored.schema_name}"."{name}" OWNER TO "{native.custody.owner}"')
            )
    database_url = native.publisher.kw["bind"].url.set(drivername="postgresql")
    await _command(
        "pg_restore",
        "--dbname",
        database_url.render_as_string(hide_password=False),
        "--data-only",
        "--no-owner",
        "--no-acl",
        "--exit-on-error",
        "--single-transaction",
        str(path),
    )
    return restored


async def _assert_restored_address_archive(sessions, prepared, restored, address_key):
    async with sessions() as session, session.begin():
        await archive.complete_reference_family_restore(session, restored)
        await archive.validate_reference_family_stage(session, ownership=restored, manifest=prepared.manifest)
        assert tuple(table.table_name for table in prepared.manifest.tables) == (
            "mrf_address",
            "mrf_address_evidence",
        )
        assert restored.sequence_oids[0][0] == "mrf_address_evidence_evidence_checksum_seq"
        assert (
            await session.scalar(text(f'SELECT address_key FROM "{restored.schema_name}".mrf_address')) == address_key
        )
        assert (
            await session.scalar(text(f'SELECT first_line FROM "{restored.schema_name}".mrf_address_evidence'))
            == "Synthetic"
        )
        assert (
            await session.scalar(
                text(f"SELECT nextval('\"{restored.schema_name}\".mrf_address_evidence_evidence_checksum_seq')")
            )
            == 2
        )
        await archive.cleanup_reference_family_stage(session, restored)


async def _publish_synthetic_mrf_family(monkeypatch, sessions, schema_name, address_key):
    async with sessions() as session:
        monkeypatch.setattr(initial, "db", _PublisherDatabase(session, schema_name))
        monkeypatch.setattr(initial, "get_import_schema", lambda *_args: schema_name)
        await _initialize_published_mrf_schema(session, schema_name)
        await _publish_normal_mrf_stage(session, schema_name, "20260920", address_key)
        mrf = await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="mrf",
            schema_name=schema_name,
        )
        address = await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="mrf-address",
            schema_name=schema_name,
        )
        assert mrf.local_generation == address.local_generation == 1
        assert address.relation_oids == mrf.relation_oids[-2:]


async def _assert_unfinished_mrf_source_rejected(native, schema_name, dataset_id):
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        async with native.source() as session, session.begin():
            await archive.capture_reference_family_source(
                session,
                importer_id="mrf",
                schema_name=schema_name,
                source_metadata={"release": "synthetic-rotation-only"},
                dependencies={"plan-attributes": "b" * 64},
            )
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        await archive.prepare_reference_family_archive_source(
            native.admin,
            importer_id="mrf",
            schema_name=schema_name,
            source_metadata={"release": "synthetic-rotation-only"},
            dataset_id=dataset_id,
            on_prepared=native.custody.retain,
            dependency_factory=_synthetic_dependencies,
            source_copy=native.custody.source_copy,
            on_precreated=native.custody.precreate,
            source_sessions=native.source,
        )


async def _assert_address_archive_round_trip(native, schema_name, dataset_id, address_key, archive_path):
    prepared = await archive.prepare_reference_family_archive_source(
        native.admin,
        importer_id="mrf-address",
        schema_name=schema_name,
        source_metadata={"release": "synthetic-published-mrf-address"},
        dataset_id=dataset_id,
        on_prepared=native.custody.retain,
        source_copy=native.custody.source_copy,
        on_precreated=native.custody.precreate,
        source_sessions=native.source,
    )
    listing = await _dump_prepared_archive(native, prepared, archive_path)
    assert "mrf_address" in listing and "mrf_address_evidence" in listing
    assert f" {prepared.ownership.schema_name} issuer " not in listing
    assert f" {prepared.ownership.schema_name} plan_npi_raw " not in listing
    restored = await _restore_prepared_address_archive(native, prepared, dataset_id, archive_path)
    await _assert_restored_address_archive(native.publisher, prepared, restored, address_key)


@pytest.mark.asyncio
async def test_mrf_archive_accepts_normal_published_staging_tables(monkeypatch, tmp_path):
    """The archive must accept the importer's real table shape, not a model-only fixture."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema_name = f"mrf_published_{uuid4().hex[:10]}"
    dataset_id, address_dataset_id, address_key = uuid4(), uuid4(), uuid4()
    owned_schemas = (
        archive.reference_family_stage_schema(dataset_id),
        archive.reference_family_stage_schema(address_dataset_id),
        schema_name,
    )
    created = False, False
    try:
        created = await _provision_canonical_type(sessions)
        await _publish_synthetic_mrf_family(monkeypatch, sessions, schema_name, address_key)
        async with engine.begin() as connection:
            await _prepare_synthetic_publication_schema(connection, schema_name)
        async with _native_mrf_archive(sessions, schema_name, None, owned_schemas) as native:
            await _assert_unfinished_mrf_source_rejected(native, schema_name, dataset_id)
            await _complete_synthetic_publication(monkeypatch, engine, sessions, schema_name, initialize=False)
            await _assert_full_mrf_stage(native, schema_name, dataset_id, address_key)
            await _assert_address_archive_round_trip(
                native, schema_name, address_dataset_id, address_key, tmp_path / "mrf-address.dump"
            )
    finally:
        async with engine.begin() as connection:
            for owned_schema in owned_schemas:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{owned_schema}" CASCADE'))
        await _retire_canonical_type(sessions, created)
        await engine.dispose()


@pytest.mark.asyncio
async def test_mrf_archive_rotation_survives_an_interleaved_ordinary_import(monkeypatch):
    """An ordinary table swap must not leave names that block the next archive."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    source_schema = f"mrf_interleave_source_{token}"
    destination_schema = f"mrf_interleave_destination_{token}"
    first_prepared, first_restored = uuid4(), uuid4()
    second_prepared, second_restored = uuid4(), uuid4()
    dataset_ids = first_prepared, first_restored, second_prepared, second_restored
    owned_schemas = {
        source_schema,
        destination_schema,
        archive.reference_family_stage_schema(first_prepared),
        archive.reference_family_stage_schema(first_restored),
        archive.reference_family_predecessor_schema(first_restored),
        archive.reference_family_stage_schema(second_prepared),
        archive.reference_family_stage_schema(second_restored),
        archive.reference_family_predecessor_schema(second_restored),
    }
    created = False, False
    try:
        created = await _provision_canonical_type(sessions)
        await _initialize_interleaved_archive_source(
            monkeypatch,
            engine,
            sessions,
            source_schema,
            destination_schema,
        )

        async with _native_mrf_archive(sessions, source_schema, destination_schema, owned_schemas) as native:
            await _run_interleaved_archive_cycle(
                engine, native, monkeypatch, source_schema, destination_schema, dataset_ids
            )
            await _assert_interleaved_result(sessions, destination_schema)

    finally:
        async with engine.begin() as connection:
            for schema_name in owned_schemas:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await _retire_canonical_type(sessions, created)
        await engine.dispose()


async def _assert_interleaved_result(sessions, destination_schema):
    async with sessions.begin() as session:
        assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v3"
        assert list(
            await session.scalars(
                text(
                    "SELECT relation.relname FROM pg_catalog.pg_class AS relation "
                    "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
                    "WHERE namespace.nspname=:schema_name AND relation.relname LIKE '%\\_old' ESCAPE '\\'"
                ),
                {"schema_name": destination_schema},
            )
        ) == ["log_old"]


def _migration_module(path=_MIGRATION_PATH):
    module_spec = importlib.util.spec_from_file_location(
        f"{path.stem}_postgres_proof",
        path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_migration(connection, schema_name: str, operation: str, path=_MIGRATION_PATH) -> None:
    migration = _migration_module(path)

    def apply(sync_connection) -> None:
        migration.op = Operations(MigrationContext.configure(sync_connection))
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
            monkeypatch.delenv("DB_SCHEMA", raising=False)
            getattr(migration, operation)()

    await connection.run_sync(apply)


@pytest.mark.asyncio
async def test_mrf_generation_migration_adds_row_and_refuses_evidence_downgrade():
    """Extend the prior ledger without inventing or erasing MRF history."""

    engine = create_async_engine(_database_url())
    schema_name = f"mrf_migration_{uuid4().hex[:10]}"
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await connection.execute(
                text(
                    f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
                    "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
                    "local_generation bigint NOT NULL, origin_lineage_id uuid, "
                    "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
                    "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
                    "importer_id IN ('plan-attributes', 'places-zcta', 'lodes', 'medicare-enrollment')))"
                )
            )
            await _run_migration(connection, schema_name, "upgrade")
            assert (
                await connection.scalar(
                    text(
                        f'SELECT local_generation FROM "{schema_name}".'
                        "reference_family_result_generation WHERE importer_id='mrf'"
                    )
                )
                == 0
            )
            await connection.execute(
                text(
                    f'UPDATE "{schema_name}".reference_family_result_generation '
                    "SET local_generation=1 WHERE importer_id='mrf'"
                )
            )
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(connection, schema_name, "downgrade")
            await connection.execute(
                text(
                    f'UPDATE "{schema_name}".reference_family_result_generation '
                    "SET local_generation=0 WHERE importer_id='mrf'"
                )
            )
            await _run_migration(connection, schema_name, "downgrade")
            assert (
                await connection.scalar(
                    text(
                        f'SELECT count(*) FROM "{schema_name}".'
                        "reference_family_result_generation WHERE importer_id='mrf'"
                    )
                )
                == 0
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _install_prior_generation_ledger(connection, schema_name):
    tiger_migration = _migration_module(_TIGER_MIGRATION_PATH)
    prior_shape = tiger_migration._shape_check(
        {"tiger": tiger_migration._TIGER_CARDINALITY, **tiger_migration._REFERENCE_CARDINALITY}
    )
    prior_importers = tuple({"tiger": tiger_migration._TIGER_CARDINALITY, **tiger_migration._REFERENCE_CARDINALITY})
    await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    await connection.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
            "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
            f"{prior_shape}))"
        )
    )
    for importer_id in prior_importers:
        await connection.execute(
            text(
                f'INSERT INTO "{schema_name}".reference_family_result_generation '
                "(importer_id, local_lineage_id, local_generation) VALUES (:importer_id, :lineage_id, 0)"
            ),
            {"importer_id": importer_id, "lineage_id": uuid4()},
        )
    return (
        await connection.execute(
            text(f'SELECT * FROM "{schema_name}".reference_family_result_generation ORDER BY importer_id')
        )
    ).all()


async def _assert_address_generation_upgrade(connection, schema_name, before):
    await _run_migration(connection, schema_name, "upgrade", _MRF_ADDRESS_MIGRATION_PATH)
    after = (
        await connection.execute(
            text(
                f'SELECT * FROM "{schema_name}".reference_family_result_generation '
                "WHERE importer_id <> 'mrf-address' ORDER BY importer_id"
            )
        )
    ).all()
    assert after == before
    authority = await generation.read_reference_family_result_generation_authority(
        connection,
        importer_id="mrf-address",
        schema_name=schema_name,
    )
    assert authority.local_generation == 0 and authority.serving_generation is None
    with pytest.raises(RuntimeError, match="unavailable"):
        await generation.capture_reference_family_serving_generation(
            connection,
            importer_id="mrf-address",
            schema_name=schema_name,
        )


async def _assert_address_adoption_blocks_downgrade(connection, schema_name):
    await connection.execute(
        text(
            f'UPDATE "{schema_name}".reference_family_result_generation '
            "SET origin_lineage_id=:lineage_id, origin_generation=1, published_at=now(), "
            "relation_oids=ARRAY[1,2]::bigint[] WHERE importer_id='mrf-address'"
        ),
        {"lineage_id": uuid4()},
    )
    with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
        await _run_migration(connection, schema_name, "downgrade", _MRF_ADDRESS_MIGRATION_PATH)
    await connection.execute(
        text(
            f'UPDATE "{schema_name}".reference_family_result_generation '
            "SET origin_lineage_id=NULL, origin_generation=NULL, published_at=NULL, relation_oids=NULL "
            "WHERE importer_id='mrf-address'"
        )
    )


@pytest.mark.asyncio
async def test_mrf_address_generation_migration_requires_new_publication_before_export():
    """Seed generation zero and preserve every prior family while refusing evidence loss."""

    engine = create_async_engine(_database_url())
    schema_name = f"mrf_address_migration_{uuid4().hex[:10]}"
    try:
        async with engine.begin() as connection:
            before = await _install_prior_generation_ledger(connection, schema_name)
            await _assert_address_generation_upgrade(connection, schema_name, before)
            await _assert_address_adoption_blocks_downgrade(connection, schema_name)
            await _run_migration(connection, schema_name, "downgrade", _MRF_ADDRESS_MIGRATION_PATH)
            assert (
                await connection.scalar(
                    text(
                        f'SELECT count(*) FROM "{schema_name}".'
                        "reference_family_result_generation WHERE importer_id='mrf-address'"
                    )
                )
                == 0
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()
