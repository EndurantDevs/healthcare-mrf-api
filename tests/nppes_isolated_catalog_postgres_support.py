# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-role private NPPES preparation, retained freeze and resume proof."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from db.connection import Database
from process import nppes_isolated_catalog as isolated
from process import nppes_public_evidence_storage as storage
from process import provider_directory_generic_source_policy as source
from process.nppes_public_evidence_chain_writer import admit_nppes_public_evidence_chain
from process.nppes_public_evidence_writer import admit_nppes_registry_archive
from process.nppes_public_evidence_writer_contract import NppesPublicEvidenceWriterError
from process.plan_release_storage_catalog import CATALOG_SQL
from tests.public_evidence_nppes_admission_postgres_support import (
    finished_chain_receipt,
    prepared_replay,
    required_config,
)
from tests.public_evidence_storage_postgres_support import connect, database_url

PRODUCER = ("a" * 40, "sha256:" + "b" * 64)


class IsolatedSourceFactory:
    """Exercise genuine publisher sessions without importing private orchestration."""

    def __init__(self, database, url, policy, catalog):
        self.database, self.policy = database, policy
        self.producer_identity, self.storage_binding = PRODUCER, None
        self._manifest = SimpleNamespace(nppes_catalog=catalog)
        self.catalog, self.enabled = catalog, True
        self.url = url
        self.engine = create_async_engine(
            url.set(drivername="postgresql+asyncpg", username=policy.publisher_role), poolclass=NullPool
        )

    def _require_current_policy(self):
        assert self._manifest.nppes_catalog is self.catalog and self.policy.schema == self.catalog.schema

    @asynccontextmanager
    async def transaction(self, database, *, isolation):
        assert database is self.database and source._FACTORY.get() is self
        async with self.engine.connect() as connection, AsyncSession(bind=connection) as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation))
            driver = (await connection.get_raw_connection()).driver_connection

            async def verify(actual, schema, **roles):
                assert actual is driver and schema == self.policy.schema and roles == self.policy.role_arguments()
                await isolated.verify_isolated_nppes_catalog(actual, self.policy, self.catalog)

            async with (
                self.database.bind_existing_session(session),
                source.bind_generic_source_publisher(
                    driver,
                    self.policy,
                    verify_role_profile=verify,
                    transaction_isolation=isolation,
                ),
                source.source_owner_transaction(database),
            ):
                yield session

    @asynccontextmanager
    async def controller(self):
        assert source._PUBLISHER.get() is None and source._OWNER.get() is None
        connection = await connect(self.url.set(username=self.catalog.retained_publisher_role))
        try:
            async with connection.transaction():
                yield connection
        finally:
            await connection.close()

    async def freeze_nppes_candidate(self, candidate, prepared):
        if not self.enabled:
            raise NppesPublicEvidenceWriterError("isolated controller is unavailable")
        async with self.controller() as connection:
            return await isolated.commit_isolated_nppes_candidate(
                connection,
                self.policy,
                self.catalog,
                candidate,
                prepared,
            )

    async def seal_nppes_chain(self, receipt):
        async with self.controller() as connection:
            return await isolated.commit_isolated_nppes_chain(connection, self.policy, self.catalog, receipt)


@pytest.fixture
async def runtime():
    url = database_url()
    admin = await connect(url)
    suffix, password = uuid4().hex, uuid4().hex
    roles = tuple(
        "isolated_" + kind + "_" + suffix for kind in ("owner", "writer", "sealed", "controller", "namespace", "reader")
    )
    owner, writer, sealed, controller, namespace, reader = roles
    policy = source.GenericSourcePolicy("npi_source_" + suffix, owner, writer, (reader,))
    factory = None
    try:
        async with admin.transaction():
            for role in roles:
                login = f"LOGIN PASSWORD '{password}'" if role in {writer, controller, reader} else "NOLOGIN"
                await admin.execute(f'CREATE ROLE "{role}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS')
            await admin.execute(f'ALTER ROLE "{writer}" NOINHERIT')
            await admin.execute(f'GRANT "{owner}" TO "{writer}" WITH INHERIT FALSE,SET TRUE,ADMIN FALSE')
            await admin.execute(f'GRANT "{owner}","{sealed}" TO "{controller}" WITH INHERIT TRUE,SET TRUE,ADMIN FALSE')
            catalog = await isolated.provision_isolated_nppes_catalog(
                admin,
                policy,
                retained_owner_role=sealed,
                retained_publisher_role=controller,
                namespace_owner_role=namespace,
                producer_identity=PRODUCER,
            )
        database = Database()
        database._database_override = url.database
        factory = IsolatedSourceFactory(database, url.set(password=password), policy, catalog)
        async with source.bind_generic_source_transaction_factory(factory):
            yield SimpleNamespace(admin=admin, factory=factory, policy=policy, catalog=catalog, roles=roles)
    finally:
        if factory is not None:
            await factory.engine.dispose()
        async with admin.transaction():
            await admin.execute(f'DROP SCHEMA IF EXISTS "{policy.schema}" CASCADE')
            for role in reversed(roles):
                if await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
                    await admin.execute(f'DROP OWNED BY "{role}"')
                    await admin.execute(f'DROP ROLE "{role}"')
        await admin.close()


@asynccontextmanager
async def publisher(runtime):
    async with source.source_transaction(runtime.factory.database):
        yield source.bound_publisher().connection


async def _binding(runtime, admission_ref):
    async with publisher(runtime) as connection:
        return storage._json(
            await connection.fetchval(
                f'SELECT candidate_storage_json FROM "{runtime.policy.schema}".{storage.HEADER} WHERE admission_ref=$1',
                admission_ref,
            )
        )


@pytest.mark.asyncio
async def test_private_catalog_has_only_native_inert_relations(runtime):
    async with publisher(runtime) as connection:
        assert (
            await isolated.verified_isolated_nppes_catalog(
                connection,
                runtime.policy.schema,
                runtime.policy,
            )
            == runtime.catalog
        )
        assert len(runtime.catalog.tables) == 13
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE connamespace=$1 AND contype='f') "
            "OR EXISTS(SELECT FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace=$1) "
            "OR EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1)",
            runtime.catalog.schema_oid,
        )


async def _catalog_arguments(runtime, connection):
    _owner, retained_owner, _readers, retained_readers = await isolated._role_oids(
        connection, runtime.policy, runtime.catalog.retained_owner_role, runtime.catalog.retained_publisher_role
    )
    return (
        [f'"{runtime.policy.schema}"."{name}"' for name in isolated._table_definitions()],
        retained_owner,
        retained_readers,
    )


@pytest.mark.asyncio
async def test_fixed_catalog_matches_recursive_native_descriptors(runtime, tmp_path):
    _replay, prepared = await _prepare_without_controller(runtime, tmp_path)
    async with publisher(runtime) as connection:
        arguments = await _catalog_arguments(runtime, connection)
        source_owner, _retained, _readers, _retained_readers = await isolated._role_oids(
            connection, runtime.policy, runtime.catalog.retained_owner_role, runtime.catalog.retained_publisher_role
        )
        families = (
            arguments,
            ([f'"{runtime.policy.schema}"."{storage.HEADER}"'], *arguments[1:]),
            (
                [f'"{runtime.policy.schema}"."{value["name"]}"' for value in prepared["tables"].values()],
                source_owner,
                [],
            ),
            ([f'"{runtime.policy.schema}"."{prepared["chunks"]["table"]["name"]}"'], source_owner, []),
        )
        for arguments in families:
            recursive = await connection.fetch(CATALOG_SQL, *arguments)
            fixed = await connection.fetch(isolated._FIXED_CATALOG_SQL, *arguments)
            assert len(recursive) == len(fixed) == len(arguments[0])
            assert recursive == fixed


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ("child", "parent"))
async def test_fixed_catalog_rejects_inheritance_and_reopens_after_rollback(runtime, direction):
    schema = runtime.policy.schema
    table = f'"{schema}"."{storage.HEADER}"'
    relative = f'"{schema}"."unexpected_inheritance"'
    transaction = runtime.admin.transaction()
    await transaction.start()
    try:
        if direction == "child":
            await runtime.admin.execute(f"CREATE TABLE {relative} () INHERITS ({table})")
        else:
            await runtime.admin.execute(f"CREATE TABLE {relative} AS SELECT * FROM {table} WITH NO DATA")
            await runtime.admin.execute(f"ALTER TABLE {table} INHERIT {relative}")
        arguments = await _catalog_arguments(runtime, runtime.admin)
        recursive = await runtime.admin.fetch(CATALOG_SQL, *arguments)
        fixed = await runtime.admin.fetch(isolated._FIXED_CATALOG_SQL, *arguments)
        assert len(fixed) == 12
        if direction == "child":
            assert len(recursive) == 14
        else:
            assert any(catalog_row["parents"] for catalog_row in recursive)
        with pytest.raises(NppesPublicEvidenceWriterError):
            await isolated.capture_isolated_nppes_catalog(
                runtime.admin,
                runtime.policy,
                retained_owner_role=runtime.catalog.retained_owner_role,
                retained_publisher_role=runtime.catalog.retained_publisher_role,
                namespace_owner_role=runtime.catalog.namespace_owner_role,
                producer_identity=runtime.catalog.producer_identity,
            )
    finally:
        await transaction.rollback()
    async with publisher(runtime) as connection:
        assert await isolated.verified_isolated_nppes_catalog(connection, schema, runtime.policy) == runtime.catalog
        assert await connection.fetchval("SELECT to_regclass($1)", relative) is None


@pytest.mark.asyncio
async def test_candidate_inherited_child_refuses_freeze_then_rollback_resumes(runtime, tmp_path):
    replay, prepared = await _prepare_without_controller(runtime, tmp_path)
    tables_by_root = {root: value["name"] for root, value in prepared["tables"].items()}
    table = f'"{runtime.policy.schema}"."{tables_by_root["member"]}"'
    transaction = runtime.admin.transaction()
    await transaction.start()
    try:
        await runtime.admin.execute(f'CREATE TABLE "{runtime.policy.schema}"."unexpected_child" () INHERITS ({table})')
        with pytest.raises(NppesPublicEvidenceWriterError):
            await storage._capture(runtime.admin, runtime.policy, tables_by_root, indexed=True)
    finally:
        await transaction.rollback()
    admitted = await admit_nppes_registry_archive(
        replay, required_config(), schema=runtime.policy.schema, database=runtime.factory.database
    )
    assert admitted.write_state == "inserted"


async def _assert_source_cannot_mutate_retained(runtime, table):
    for statement in (
        f'UPDATE "{runtime.policy.schema}"."{table}" SET npi=npi',
        f'ALTER TABLE "{runtime.policy.schema}"."{table}" ADD COLUMN changed int',
        f'DROP TABLE "{runtime.policy.schema}"."{table}"',
        f'ALTER TABLE "{runtime.policy.schema}".{storage.HEADER} DISABLE ROW LEVEL SECURITY',
        f'DELETE FROM "{runtime.policy.schema}".{storage.HEADER}',
        f'TRUNCATE "{runtime.policy.schema}".{storage.HEADER}',
        f'UPDATE "{runtime.policy.schema}".public_evidence_nppes_registry_admission SET admission_ref=admission_ref',
        f'DELETE FROM "{runtime.policy.schema}".public_evidence_nppes_registry_admission_seal',
        f'ALTER TABLE "{runtime.policy.schema}".public_evidence_nppes_registry_admission_seal ADD COLUMN changed int',
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with publisher(runtime) as connection:
                await connection.execute(statement)


@pytest.mark.asyncio
async def test_prepared_source_resumes_only_through_independent_freeze(runtime, tmp_path, monkeypatch):
    """Resume sealed evidence and refuse foreign schemas before invoking the controller."""
    replay = await prepared_replay(tmp_path)
    runtime.factory.enabled = False
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(
            replay, required_config(), schema=runtime.policy.schema, database=runtime.factory.database
        )
    before = await _binding(runtime, replay.admission_row.admission_ref)
    assert before["phase"] == "prepared" and before["counts"]["member"] == 3
    table = before["tables"]["member"]["name"]
    async with publisher(runtime) as connection:
        assert await connection.execute(f'UPDATE "{runtime.policy.schema}"."{table}" SET npi=npi') == "UPDATE 3"
        assert (
            await connection.execute(
                f'UPDATE "{runtime.policy.schema}".{storage.HEADER} SET updated_at=clock_timestamp()',
            )
            == "UPDATE 0"
        )
    runtime.factory.enabled = True
    admitted = await admit_nppes_registry_archive(
        replay, required_config(), schema=runtime.policy.schema, database=runtime.factory.database
    )
    assert admitted.write_state == "inserted"
    frozen = await _binding(runtime, replay.admission_row.admission_ref)
    assert frozen["phase"] == "published" and frozen["chunks"]["table"] is None
    assert frozen["tables"]["member"]["owner"] != before["tables"]["member"]["owner"]
    assert frozen["tables"]["member"]["oid"] == before["tables"]["member"]["oid"]
    await _assert_source_cannot_mutate_retained(runtime, table)
    repeated = await admit_nppes_registry_archive(
        replay, required_config(), schema=runtime.policy.schema, database=runtime.factory.database
    )
    assert repeated.write_state == "already_present"
    receipt = finished_chain_receipt(replay, admitted)
    controller = AsyncMock(wraps=runtime.factory.seal_nppes_chain)
    monkeypatch.setattr(runtime.factory, "seal_nppes_chain", controller)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_public_evidence_chain(receipt, database=runtime.factory.database)
    controller.assert_not_awaited()
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_public_evidence_chain(receipt, schema="other_source", database=runtime.factory.database)
    controller.assert_not_awaited()
    for _attempt in range(2):
        assert (
            await admit_nppes_public_evidence_chain(
                receipt,
                schema=runtime.policy.schema,
                database=runtime.factory.database,
            )
            == receipt
        )
    assert controller.await_count == 2


async def _prepare_without_controller(runtime, tmp_path):
    replay = await prepared_replay(tmp_path)
    runtime.factory.enabled = False
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(
            replay,
            required_config(),
            schema=runtime.policy.schema,
            database=runtime.factory.database,
        )
    runtime.factory.enabled = True
    return replay, await _binding(runtime, replay.admission_row.admission_ref)


@pytest.mark.asyncio
@pytest.mark.parametrize("callback", (None, "dishonest"))
async def test_missing_or_dishonest_controller_cannot_publish(runtime, tmp_path, callback):
    replay, prepared = await _prepare_without_controller(runtime, tmp_path)

    async def dishonest(*_arguments):
        assert source._PUBLISHER.get() is None and source._OWNER.get() is None
        return "inserted"

    runtime.factory.freeze_nppes_candidate = dishonest if callback else None
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(
            replay,
            required_config(),
            schema=runtime.policy.schema,
            database=runtime.factory.database,
        )
    assert await _binding(runtime, replay.admission_row.admission_ref) == prepared
    async with publisher(runtime) as connection:
        assert not await connection.fetchval(
            f'SELECT EXISTS(SELECT FROM "{runtime.policy.schema}".public_evidence_nppes_registry_admission)',
        )


@pytest.mark.asyncio
async def test_freeze_rechecks_complete_rows_and_rolls_back_ownership(runtime, tmp_path):
    replay, prepared = await _prepare_without_controller(runtime, tmp_path)
    table = prepared["tables"]["member"]["name"]
    async with publisher(runtime) as connection:
        await connection.execute(
            f'UPDATE "{runtime.policy.schema}"."{table}" SET npi=$1 WHERE source_row_ordinal=1',
            "1111111111",
        )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(
            replay,
            required_config(),
            schema=runtime.policy.schema,
            database=runtime.factory.database,
        )
    assert await _binding(runtime, replay.admission_row.admission_ref) == prepared
    async with publisher(runtime) as connection:
        assert await connection.execute(f'UPDATE "{runtime.policy.schema}"."{table}" SET npi=npi') == "UPDATE 3"
        assert not await connection.fetchval(
            f'SELECT EXISTS(SELECT FROM "{runtime.policy.schema}".public_evidence_nppes_registry_admission_seal)',
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("policy", "grant", "namespace"))
async def test_original_native_receipt_rejects_control_authority_drift(runtime, mutation):
    schema = runtime.policy.schema
    if mutation == "policy":
        statement = f'ALTER POLICY nppes_loading_update ON "{schema}".{storage.HEADER} WITH CHECK(true)'
    elif mutation == "grant":
        statement = (
            f'GRANT UPDATE ON "{schema}".public_evidence_nppes_registry_admission TO "{runtime.policy.owner_role}"'
        )
    else:
        statement = f'GRANT "{runtime.catalog.namespace_owner_role}" TO "{runtime.policy.owner_role}"'
    await runtime.admin.execute(statement)
    with pytest.raises((NppesPublicEvidenceWriterError, ValueError)):
        async with publisher(runtime):
            pytest.fail("changed native authority must be rejected before Source access")


@pytest.mark.asyncio
async def test_busy_source_table_refuses_freeze_then_resumes(runtime, tmp_path):
    replay, prepared = await _prepare_without_controller(runtime, tmp_path)
    table = prepared["tables"]["member"]["name"]
    connection = await connect(runtime.factory.url.set(username=runtime.policy.publisher_role))
    try:
        async with connection.transaction():
            await connection.execute(f'SET LOCAL ROLE "{runtime.policy.owner_role}"')
            await connection.execute(f'UPDATE "{runtime.policy.schema}"."{table}" SET npi=npi')
            with pytest.raises(NppesPublicEvidenceWriterError):
                await admit_nppes_registry_archive(
                    replay,
                    required_config(),
                    schema=runtime.policy.schema,
                    database=runtime.factory.database,
                )
            assert await _binding(runtime, replay.admission_row.admission_ref) == prepared
    finally:
        await connection.close()
    admitted = await admit_nppes_registry_archive(
        replay,
        required_config(),
        schema=runtime.policy.schema,
        database=runtime.factory.database,
    )
    assert admitted.write_state == "inserted"


@pytest.mark.asyncio
async def test_controller_cancellation_rolls_back_seal_and_resumes(runtime, tmp_path):
    replay, prepared = await _prepare_without_controller(runtime, tmp_path)
    original = runtime.factory.freeze_nppes_candidate

    async def cancel_before_commit(candidate, fixed):
        async with runtime.factory.controller() as connection:
            await isolated.commit_isolated_nppes_candidate(
                connection,
                runtime.policy,
                runtime.catalog,
                candidate,
                fixed,
            )
            raise asyncio.CancelledError

    runtime.factory.freeze_nppes_candidate = cancel_before_commit
    with pytest.raises(asyncio.CancelledError):
        await admit_nppes_registry_archive(
            replay,
            required_config(),
            schema=runtime.policy.schema,
            database=runtime.factory.database,
        )
    assert await _binding(runtime, replay.admission_row.admission_ref) == prepared
    runtime.factory.freeze_nppes_candidate = original
    admitted = await admit_nppes_registry_archive(
        replay,
        required_config(),
        schema=runtime.policy.schema,
        database=runtime.factory.database,
    )
    assert admitted.write_state == "inserted"
