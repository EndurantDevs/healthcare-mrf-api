# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native transaction ownership with sealed scalar fixtures; component proof only."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import fields, replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, InvalidRequestError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import provider_directory_cms_publication as publication
from process.provider_directory_cms_preparation import PreparedServingArtifacts
from tests import test_cms_serving_publication_postgres as fixture
from tests import test_provider_directory_cms_preparation as preparation_fixture
from tests import test_provider_directory_cms_serving_receipt_postgres as native
from tests.cms_npd_admission_postgres_support import _database_url
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


@asynccontextmanager
async def _source_roles():
    """Register exact role cleanup before creating isolated login roles."""
    roles = ("cms_owner_" + uuid4().hex, "cms_reader_" + uuid4().hex)
    password = uuid4().hex
    admin = create_async_engine(_database_url())
    try:
        async with admin.begin() as session:
            for role in roles:
                await session.execute(text(f"CREATE ROLE \"{role}\" LOGIN NOINHERIT PASSWORD '{password}'"))
        yield roles, password
    finally:
        async with admin.begin() as session:
            for role in reversed(roles):
                if await session.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
                ):
                    await session.execute(text(f'DROP OWNED BY "{role}"'))
                    await session.execute(text(f'DROP ROLE "{role}"'))
                assert not await session.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
                )
        await admin.dispose()


async def _protect_tables(engine, schema, roles):
    async with engine.begin() as session:
        await session.execute(text(f'CREATE TABLE "{schema}".owner_transaction_probe (marker text PRIMARY KEY)'))
        names = (
            (
                await session.execute(
                    text(
                        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname=:schema AND c.relkind='r' ORDER BY c.relname"
                    ),
                    {"schema": schema},
                )
            )
            .scalars()
            .all()
        )
        for name in names:
            await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{roles[0]}"'))
        await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{roles[0]}"'))
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{roles[1]}"'))
        await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{roles[1]}"'))


@asynccontextmanager
async def _source_pools(monkeypatch):
    async with _source_roles() as (roles, password), native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await fixture._enable_candidate_checks(engine, schema)
        await _protect_tables(engine, schema, roles)
        url = _database_url()
        source_engine = create_async_engine(
            url.set(username=roles[0], password=password), pool_size=1, max_overflow=0, hide_parameters=True
        )
        database = Database()
        database.engine = create_async_engine(
            url.set(username=roles[1], password=password), pool_size=2, max_overflow=0, hide_parameters=True
        )
        database.session_factory = async_sessionmaker(database.engine, expire_on_commit=False)
        database._database_override = url.database
        try:
            yield SimpleNamespace(
                engine=engine,
                schema=schema,
                roles=roles,
                database=database,
                source_sessions=async_sessionmaker(source_engine, expire_on_commit=False),
                predecessor=predecessor,
            )
        finally:
            await database.disconnect()
            await source_engine.dispose()


async def _observe_owner(database, expected_role, observations):
    binding = database._transaction_binding()
    assert binding is not None and not binding.session.in_nested_transaction()
    transaction_coordinates = tuple(
        (
            await binding.session.execute(
                text(
                    "SELECT pg_backend_pid(),pg_current_xact_id()::text,current_user,current_setting('transaction_isolation')"
                )
            )
        ).one()
    )
    assert transaction_coordinates[2:] == (expected_role, "repeatable read")
    assert (
        tuple(
            await database.first(
                "SELECT pg_backend_pid(),pg_current_xact_id()::text,current_user,current_setting('transaction_isolation')"
            )
        )
        == transaction_coordinates
    )
    raw = await (await binding.session.connection()).get_raw_connection()
    observations.append((transaction_coordinates, raw.driver_connection))


async def _prepared_owner(monkeypatch, pools, outcome, observations):
    failure = {"rollback": "profile", "cancel": "cancel"}.get(outcome)
    fhir, prepared, address, proof = await fixture._prepare_publication(
        monkeypatch, pools.engine, pools.schema, failure
    )
    previous_db = fhir.db
    configure = fhir._configure_provider_directory_artifact_promotion
    assert_ready = prepared.assert_ready
    apply_results = publication._apply_prepared_results
    fhir.db = pools.database

    async def configure_owner(lock_timeout, statement_timeout):
        await _observe_owner(fhir.db, pools.roles[0], observations)
        previous_db.connection = fhir.db._transaction_binding().session
        await configure(lock_timeout, statement_timeout)

    async def ready_owner(**arguments):
        await _observe_owner(fhir.db, pools.roles[0], observations)
        await assert_ready(**arguments)

    async def observe_results(*arguments):
        await _observe_owner(fhir.db, pools.roles[0], observations)
        result = await apply_results(*arguments)
        await _observe_owner(fhir.db, pools.roles[0], observations)
        return result

    fhir._configure_provider_directory_artifact_promotion = configure_owner
    prepared.assert_ready = ready_owner
    monkeypatch.setattr(publication, "_apply_prepared_results", observe_results)
    execution = fixture._execution()
    if outcome == "guard":
        execution.attestation.proof_id = "d" * 64
    return fhir, prepared, address, proof, execution


@asynccontextmanager
async def _ack_factory(sessions, outcome):
    async with sessions() as session:
        assert not session.in_transaction()
        yield session
    if outcome == "lost_ack":
        raise OSError("synthetic acknowledgement loss")
    if outcome == "cancel_after":
        raise asyncio.CancelledError("synthetic cancellation after commit")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["commit", "rollback", "cancel", "guard", "lost_ack", "cancel_after"])
async def test_source_owner_publication_is_atomic(monkeypatch, outcome):
    """Real common guards and transaction boundaries retain commit/cancel recovery."""
    async with _source_pools(monkeypatch) as pools:
        before = await fixture._serving_state(pools.engine, pools.schema)
        observations = []
        fhir, prepared, address, proof, execution = await _prepared_owner(monkeypatch, pools, outcome, observations)
        prepared.source_session_factory = lambda: _ack_factory(pools.source_sessions, outcome)

        async def publish():
            return await publication.commit_prepared_serving_generation(
                fhir,
                execution,
                prepared,
                address=address,
                candidate_proof=proof,
                native_dependencies=before[0],
                predecessor=pools.predecessor,
            )

        await _publish_outcome(publish, outcome)
        is_committed = outcome in {"commit", "lost_ack", "cancel_after"}
        assert prepared.committed is is_committed and address.committed is is_committed
        assert len(observations) >= 2
        assert all(identity == observations[0][0] and driver is observations[0][1] for identity, driver in observations)
        assert pools.database._transaction_binding() is None
        after = await fixture._serving_state(pools.database.engine, pools.schema)
        assert "source_session_factory" not in json.dumps(prepared.metrics)
        if is_committed:
            assert after[1]["provider_directory_cms_serving_receipt"] == 2
            assert prepared.metrics["cms_serving"]["recovered_commit"] is (outcome != "commit")
        else:
            assert after == before


async def _publish_outcome(publish, outcome):
    if outcome in {"cancel", "cancel_after"}:
        with pytest.raises(asyncio.CancelledError):
            await publish()
    elif outcome == "rollback":
        with pytest.raises(RuntimeError, match="synthetic profile failure"):
            await publish()
    elif outcome == "guard":
        with pytest.raises(DBAPIError, match="cms_serving"):
            await publish()
    else:
        await publish()


def test_prepared_owner_handle_is_optional_and_private():
    prepared = PreparedServingArtifacts(None, None, None, None, None, None, {}, {}, None)
    assert prepared.source_session_factory is None
    factory_field = next(member for member in fields(prepared) if member.name == "source_session_factory")
    assert not factory_field.repr and not factory_field.compare
    with_handle = replace(prepared, source_session_factory=lambda: None)
    assert prepared == with_handle
    assert "source_session_factory" not in repr(with_handle)
    assert "source_session_factory" not in json.dumps(with_handle.metrics)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["initial", "delta"])
async def test_owner_handle_stays_out_of_prepared_manifest(monkeypatch, capsys, mode):
    case = preparation_fixture._manifest_fixture(monkeypatch, mode)

    def unused_factory():
        pytest.fail("Manifest serialization invoked the owner factory")

    case.prepared.source_session_factory = unused_factory
    async with preparation_fixture._manifest_boundary(case):
        encoded = capsys.readouterr().out
        assert "source_session_factory" not in encoded
        document_by_field = json.loads(encoded.split("\t", 1)[1])
        assert document_by_field["prepared_manifest"]["contract_id"] == "provider-directory-cms-prepared-layout-v1"
        assert case.prepared.source_session_factory is unused_factory


@pytest.mark.asyncio
async def test_owner_bridge_refuses_children_and_prebound_reentry(monkeypatch):
    async with _source_pools(monkeypatch) as pools:
        fhir = SimpleNamespace(db=pools.database)
        async with publication._publication_session(fhir, pools.source_sessions) as session:
            await session.execute(text(f"INSERT INTO \"{pools.schema}\".owner_transaction_probe VALUES ('owner')"))
            with pytest.raises(RuntimeError, match="child asyncio task"):
                await asyncio.create_task(pools.database.scalar("SELECT 1"))
            factory_calls = []

            def unopened_factory():
                factory_calls.append(True)
                return pools.source_sessions()

            with pytest.raises(RuntimeError, match="requires_own_transaction"):
                async with publication._publication_session(fhir, unopened_factory):
                    pytest.fail("Prebound owner was accepted")
            assert not factory_calls
            assert session.in_transaction() and not session.in_nested_transaction()
        assert pools.database._transaction_binding() is None
        assert await pools.database.scalar(f'SELECT marker FROM "{pools.schema}".owner_transaction_probe') == "owner"


@pytest.mark.asyncio
async def test_default_pool_and_reader_privileges_are_preserved(monkeypatch):
    async with _source_pools(monkeypatch) as pools:
        fhir = SimpleNamespace(db=pools.database)
        async with publication._publication_session(fhir, None) as session:
            assert await session.scalar(text("SELECT current_user")) == pools.roles[1]
            assert await session.scalar(text("SHOW transaction_isolation")) == "read committed"
            assert not await session.scalar(
                text("SELECT pg_has_role(session_user,:role,'SET')"), {"role": pools.roles[0]}
            )
        for sql in (
            f'SET ROLE "{pools.roles[0]}"',
            f"INSERT INTO \"{pools.schema}\".owner_transaction_probe VALUES ('forbidden')",
        ):
            with pytest.raises(DBAPIError) as refused:
                async with pools.database.transaction() as session:
                    await session.execute(text(sql))
            assert refused.value.orig.sqlstate == "42501"
        assert await pools.database.scalar(f'SELECT count(*) FROM "{pools.schema}".owner_transaction_probe') == 0


@pytest.mark.asyncio
async def test_source_factory_requires_a_fresh_transaction(monkeypatch):
    async with _source_pools(monkeypatch) as pools:
        fhir = SimpleNamespace(db=pools.database)
        async with pools.source_sessions() as existing, existing.begin():

            @asynccontextmanager
            async def existing_factory():
                yield existing

            with pytest.raises(InvalidRequestError, match="already begun"):
                async with publication._publication_session(fhir, existing_factory):
                    pytest.fail("Existing transaction was accepted")
            assert existing.in_transaction() and not existing.in_nested_transaction()
        with pytest.raises(RuntimeError, match="requires_own_transaction"):
            async with publication._publication_session(fhir, object()):
                pytest.fail("Noncallable factory was accepted")
        assert pools.database._transaction_binding() is None
