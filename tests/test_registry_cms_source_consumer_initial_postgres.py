# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine retained Source notification through initial native publication and replay."""

from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_cms_source_consumer import (
    consume_registry_cms_source_notification_once,
)
from tests import test_registry_cms_source_consumer_postgres as support
from tests.test_provider_directory_cms_resource_batch_postgres import cms_source_template as cms_source_template

pytestmark = pytest.mark.usefixtures(cms_source_template.__name__)


async def _publisher_sessions(database, _pair, store, stack):
    # Existing component publisher custody is retained; this is not ordinary LOGIN qualification.
    engine = create_async_engine(
        database.engine.url,
        isolation_level="REPEATABLE READ",
        connect_args={
            "server_settings": {"role": store.publisher_role, "default_transaction_read_only": "on"},
        },
        pool_size=1,
        max_overflow=0,
    )
    stack.push_async_callback(engine.dispose)
    return store, async_sessionmaker(engine)


async def _later_head(fixture, pair, current, revision):
    request_id = uuid4()
    sources = await fixture.prepare_address_sources(
        request_id=request_id,
        approved_revision=revision,
        expected_head=current.generation_id,
        source_manifest=current,
        address_sources=RegistryCompositionAddressSources(
            pair.address_source, replacement_source_recipes=(pair.recipe,)
        ),
    )
    target, addresses = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=request_id,
        approved_revision=revision,
        expected_head=current.generation_id,
        source_manifest=current,
        address_sources=sources,
        writer_roles={**fixture.writer_roles, "reader_roles": pair.runtime_roles},
        control_schema=fixture.control_schema,
    )
    return await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        target,
        addresses,
        **{**fixture.writer_roles, "reader_roles": pair.runtime_roles},
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )


async def _assert_initial_members(fixture, manual):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        current = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        assert current.approved_custom_revision == manual.revision
        members = await fixture.connection.fetch(
            f'SELECT provider_system,provider_id FROM "{current.schema_name}".network_membership'
        )
        assert any(member["provider_system"] == "npi" and member["provider_id"] == "1000000491" for member in members)
        assert any(
            member["provider_system"] == "manual" and member["provider_id"] == manual.provider["record_id"]
            for member in members
        )
        assert await fixture.connection.fetchval(
            f"SELECT bool_and(pg_typeof(canonical_network_ids)::text='integer[]') FROM \"{current.schema_name}\".entity_address_unified"
        )
    return current


async def _assert_source_offices(fixture, current, pair):
    fields = "location_key,entity_type,entity_id,npi,address_key::text,first_line,second_line,city_name,state_name,postal_code,country_code"
    source = pair.address_source
    expected = await fixture.connection.fetch(
        f'SELECT {fields} FROM "{source.schema_name}"."{source.table_name}" WHERE npi=1000000491 ORDER BY location_key'
    )
    actual = await fixture.connection.fetch(
        f'SELECT {fields} FROM "{current.schema_name}".entity_address_unified WHERE npi=1000000491 ORDER BY location_key'
    )
    assert expected and actual == expected
    assert await fixture.connection.fetchval(
        f'SELECT NOT EXISTS(SELECT 1 FROM "{current.schema_name}".provider_location_binding binding '
        f'JOIN "{current.schema_name}".entity_address_unified office USING(location_key) '
        "WHERE binding.provider_system='npi' AND (binding.entity_type, binding.entity_id) IS DISTINCT FROM (office.entity_type,office.entity_id))"
    )


async def _initial_flow(database, pair, notice, old, fixture, store, sessions):
    config = await support.custody.candidate_custody_consumer(fixture, pair, old, store)
    arguments_by_name = dict(
        connection=fixture.execution_connection,
        notification_sessions=sessions,
        notification_store=store,
        source_schema="mrf",
        receipt_id=pair.proof.serving_receipt_id,
        notification_sha256=notice.sha256,
        config=config,
        source_endpoint_id=pair.recipe.source_pin.endpoint_id,
    )
    assert (await consume_registry_cms_source_notification_once(**arguments_by_name))["state"] == "review_required"
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        is None
    )
    assert not await fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".network_membership_candidate)'
    )
    await support.recipes._reviewed_cms_binding(
        fixture.connection,
        fixture.control_schema,
        fixture.engine,
        pair.recipe.source_pin,
        pair.recipe.binding_coordinates,
    )
    manual = await support.custom._seed(fixture)
    completed = await consume_registry_cms_source_notification_once(**arguments_by_name)
    assert completed["state"] == "completed"
    current = await _assert_initial_members(fixture, manual)
    await _assert_source_offices(fixture, current, pair)
    later_manual = await support.custom._seed(fixture)
    later = await _later_head(fixture, pair, current, later_manual.revision)
    replay = await consume_registry_cms_source_notification_once(**arguments_by_name)
    assert replay["state"] == "replayed" and replay["manifest"]["generation_id"] == current.generation_id
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        ).generation_id == later["generation_id"]
    return dict(arguments_by_name=arguments_by_name, generation=current.generation_id)


@pytest.mark.asyncio
async def test_initial_source_approval_publication_and_replay_after_later_head(monkeypatch, tmp_path):
    """Use actual source preparation, native composition and a real later publication."""
    monkeypatch.setattr(support, "_direct_reader", _publisher_sessions)
    monkeypatch.setattr(support, "_consume_pair", _initial_flow)
    await support.test_committed_notification_refresh_replay_review_and_owned_lock(monkeypatch, tmp_path)
