# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual accepted CMS notification through native reviewed composition and cutover."""

from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace
from uuid import uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_cms_registry_address_capture import native_driver
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_cms_source_consumer import (
    _canonical_session,
    _lock_key,
    consume_registry_cms_source_notification_once,
)
from process.registry_cms_source_notifications import plan_registry_cms_source_notification_grants
from tests import cms_registry_native_producer_postgres_support as native_producer
from tests import cms_registry_recipe_postgres_support as recipes
from tests import registry_candidate_custody_postgres_support as custody
from tests import test_network_custom_address_source_postgres as custom
from tests import test_network_registry_cms_prepared_pair_postgres as producer
from tests import test_network_serving_schema_postgres as registry
from tests.test_provider_directory_cms_resource_batch_postgres import cms_source_template as cms_source_template

pytestmark = pytest.mark.usefixtures(cms_source_template.__name__)


async def _remove_reader(database_url, reader):
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as setup:
            await setup.execute(text(f'DROP OWNED BY "{reader}"'))
            await setup.execute(text(f'DROP ROLE "{reader}"'))
    finally:
        await engine.dispose()


async def _direct_reader(database, pair, store, stack):
    reader = "cms_event_reader_" + uuid4().hex
    reader_password = uuid4().hex
    store = replace(store, reader_roles=(reader,))
    async with database.engine.begin() as setup:
        await setup.execute(text(f"CREATE ROLE \"{reader}\" LOGIN PASSWORD '{reader_password}'"))
    stack.push_async_callback(_remove_reader, database.engine.url, reader)
    async with database.engine.begin() as setup:
        await setup.execute(text(f'GRANT "{pair.runtime_roles[0]}" TO "{reader}" WITH INHERIT TRUE, SET FALSE'))
        await setup.execute(text(f'GRANT USAGE ON SCHEMA mrf TO "{reader}"'))
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        driver = await native_driver(session)
        for statement in await plan_registry_cms_source_notification_grants(
            driver,
            control_schema=store.control_schema,
            owner_role=store.owner_role,
            publisher_role=store.publisher_role,
            reader_roles=store.reader_roles,
        ):
            await session.execute(text(statement))
        await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA mrf TO "{reader}"'))
    engine = create_async_engine(
        database.engine.url.set(username=reader, password=reader_password), pool_size=1, max_overflow=0
    )
    stack.push_async_callback(engine.dispose)
    sessions = async_sessionmaker(engine)
    async with sessions() as session:
        assert (await session.execute(text("SELECT current_user,session_user"))).one() == (reader, reader)
    return store, sessions


async def _registry_fixture(database, monkeypatch, stack):
    monkeypatch.setenv(
        "NETWORK_REGISTRY_TEST_DSN",
        database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
    )
    monkeypatch.delenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", raising=False)
    schema = await stack.enter_async_context(aclosing(registry.serving_schema.__wrapped__()))
    serving = await anext(schema)
    fixture_gen = await stack.enter_async_context(aclosing(custom.custom_db.__wrapped__(serving)))
    fixture = await anext(fixture_gen)
    await custody.install_candidate_custody(fixture, stack)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "mrf")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setenv("DB_SCHEMA", "mrf")
    await _canonical_session(fixture.connection)
    database_name = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{fixture.roles["owner"]}"')
    return fixture


async def _initial_registry(fixture, old):
    _, _, _, approved = await recipes._reviewed_cms_binding(
        fixture.connection,
        fixture.control_schema,
        fixture.engine,
        old.recipe.source_pin,
        old.recipe.binding_coordinates,
    )
    identity = uuid4()
    fixture.composition_ids.append(uuid5(identity, "address:" + approved.generation_id))
    address_sources = await fixture.prepare_address_sources(
        request_id=identity,
        approved_revision=approved.approved_revision,
        source_manifest=None,
        expected_head=0,
        address_sources=RegistryCompositionAddressSources(old.address_source, initial_source_recipes=(old.recipe,)),
    )
    copy_target, address = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=identity,
        approved_revision=approved.approved_revision,
        expected_head=0,
        address_sources=address_sources,
        writer_roles={**fixture.writer_roles, "reader_roles": old.runtime_roles},
        control_schema=fixture.control_schema,
    )
    await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        copy_target,
        address,
        **{**fixture.writer_roles, "reader_roles": old.runtime_roles},
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)


async def _assert_incumbent(fixture, incumbent):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
            == incumbent
        )


async def _competing_lock(database, arguments_by_name):
    other = await asyncpg.connect(
        database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        key = _lock_key(arguments_by_name["config"])
        assert await other.fetchval("SELECT pg_try_advisory_lock($1::bigint)", key)
        assert (await consume_registry_cms_source_notification_once(**arguments_by_name))["state"] == "busy"
        assert await other.fetchval("SELECT pg_advisory_unlock($1::bigint)", key)
    finally:
        await other.close()


async def _assert_members(fixture, incumbent, manual):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        current = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        assert current.generation_id > incumbent.generation_id
        assert current.approved_custom_revision == manual.revision
        assert (
            await resolve_network_serving_manifest(
                fixture.connection, generation_id=incumbent.generation_id, control_schema=fixture.control_schema
            )
            == incumbent
        )
        members = await fixture.connection.fetch(
            f'SELECT provider_system,provider_id,location_id FROM "{current.schema_name}".network_membership'
        )
        assert any(
            row["provider_system"] == "manual" and row["provider_id"] == manual.provider["record_id"] for row in members
        )
        assert any(row["provider_system"] == "npi" and row["provider_id"] == "1000000491" for row in members)
        assert await fixture.connection.fetchval(
            f"SELECT bool_and(pg_typeof(canonical_network_ids)::text='integer[]') FROM \"{current.schema_name}\".entity_address_unified"
        )
        return current


async def _consume_pair(database, pair, notice, old, fixture, store, sessions):
    config = await custody.candidate_custody_consumer(fixture, pair, old, store)
    incumbent = await _initial_registry(fixture, old)
    arguments_by_name = dict(
        connection=fixture.execution_connection,
        notification_sessions=sessions,
        notification_store=store,
        source_schema="mrf",
        receipt_id=notice.pair.proof.serving_receipt_id,
        notification_sha256=notice.sha256,
        config=config,
    )
    assert (await consume_registry_cms_source_notification_once(**arguments_by_name))["state"] == "review_required"
    await _assert_incumbent(fixture, incumbent)
    with pytest.raises(ValueError, match="registry_cms_source_consumer_invalid"):
        await consume_registry_cms_source_notification_once(**{**arguments_by_name, "notification_sha256": "0" * 64})
    await recipes._reviewed_cms_binding(
        fixture.connection,
        fixture.control_schema,
        fixture.engine,
        pair.recipe.source_pin,
        pair.recipe.binding_coordinates,
    )
    manual = await custom._seed(fixture)
    await _competing_lock(database, arguments_by_name)
    completed = await consume_registry_cms_source_notification_once(**arguments_by_name)
    assert completed["state"] == "completed"
    replay = await consume_registry_cms_source_notification_once(**arguments_by_name)
    assert replay["state"] == "replayed" and replay["manifest"] == {**completed["manifest"], "replayed": True}
    current = await _assert_members(fixture, incumbent, manual)
    return dict(arguments_by_name=arguments_by_name, generation=current.generation_id)


@pytest.mark.asyncio
async def test_committed_notification_refresh_replay_review_and_owned_lock(monkeypatch, tmp_path):
    original_capture = producer.capture_registry_cms_source_pair
    original_verify = producer._verify_committed_notification
    original_publication = native_producer.genuine_prepared_publication
    original_resources = producer._retained_office_resources

    def membership_resources(patch):
        original_resources(patch)
        original_rows = producer.support._resource_rows

        def resources(revision):
            rows_by_kind = original_rows(revision)
            rows_by_kind["Organization"][0]["type"] = [{"text": "ntwk"}]
            rows_by_kind["OrganizationAffiliation"][0]["active"] = False
            for index, location_by_name in enumerate(rows_by_kind["Location"], 1):
                location_by_name["address"] = {
                    "line": [f"{index} Example Street"],
                    "city": "Example City",
                    "state": "CA",
                    "postalCode": "90210",
                    "country": "US",
                }
            return rows_by_kind

        patch.setattr(producer.support, "_resource_rows", resources)

    monkeypatch.setattr(producer, "_retained_office_resources", membership_resources)
    pairs, state = [], {}
    async with AsyncExitStack() as stack:

        @asynccontextmanager
        async def live_publication(*args, **kwargs):
            """Close consumers while the producer's database and roles remain live."""
            async with original_publication(*args, **kwargs) as published:
                try:
                    yield published
                finally:
                    await stack.aclose()

        async def capture(*args, **kwargs):
            pair = await original_capture(*args, **kwargs)
            pairs.append(pair)
            return pair

        async def verify(database, pair, store, notice):
            await original_verify(database, pair, state.get("store", store), notice)
            if state:
                replay = await consume_registry_cms_source_notification_once(**state["arguments_by_name"])
                assert replay["state"] == "replayed" and replay["manifest"]["generation_id"] == state["generation"]
                return
            store, sessions = await _direct_reader(database, pair, store, stack)
            fixture = await _registry_fixture(database, monkeypatch, stack)
            state.update(await _consume_pair(database, pair, notice, pairs[0], fixture, store, sessions))
            state["store"] = store

        monkeypatch.setattr(producer, "capture_registry_cms_source_pair", capture)
        monkeypatch.setattr(producer, "_verify_committed_notification", verify)
        monkeypatch.setattr(native_producer, "genuine_prepared_publication", live_publication)
        await producer.verify_committed_source_publication(monkeypatch, tmp_path)
