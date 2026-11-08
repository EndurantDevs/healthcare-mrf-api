# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native accepted CMS anchors before any Network serving publication exists."""

from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace

import pytest
from sqlalchemy import text

from process import registry_cms_refresh_anchors as anchors
from process.network_cms_registry_address_capture import native_driver
from process.registry_cms_source_notifications import RegistryCMSSourceNotificationReader
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable
from process.registry_refresh_anchor_service import read_approved_refresh_bindings
from tests import cms_registry_native_producer_postgres_support as source
from tests import cms_registry_recipe_postgres_support as recipes
from tests import test_network_serving_schema_postgres as registry
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.test_registry_cms_source_consumer_postgres import _direct_reader

pytestmark = [pytest.mark.asyncio, pytest.mark.usefixtures(cms_recipe_template.__name__)]


@asynccontextmanager
async def _accepted_source(monkeypatch, tmp_path):
    async with source.genuine_first_prepared_publication(monkeypatch, tmp_path, (32, 64 * 1024)) as state:
        database, pair = state.database, state.pair
        async with AsyncExitStack() as stack:
            monkeypatch.setenv(
                "NETWORK_REGISTRY_TEST_DSN",
                database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
            )
            generator = await stack.enter_async_context(aclosing(registry.serving_schema.__wrapped__()))
            connection, schema, engine = await anext(generator)
            _, _, fields, approved = await recipes._reviewed_cms_binding(
                connection, schema, engine, pair.recipe.source_pin, pair.proof.binding_coordinates
            )
            assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_serving_manifest') == 0
            store, sessions = await _direct_reader(database, pair, state.store, stack)
            reader = RegistryCMSSourceNotificationReader(
                store.control_schema, store.owner_role, store.publisher_role, store.reader_roles
            )
            async with engine.begin() as setup:
                await setup.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{store.reader_roles[0]}"'))
                await setup.execute(
                    text(f'GRANT SELECT ON "{schema}".registry_approved_record TO "{store.reader_roles[0]}"')
                )
            async with sessions() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                enrollment_map = dict(
                    binding_id=fields["binding_id"],
                    binding_revision=1,
                    network_id=fields["network_id"],
                    continuity_policy="accepted_edition",
                    source_scope=dict(
                        source_system="fhir",
                        source_id=pair.proof.source_pin.source_id,
                        dataset_schema=pair.proof.source_pin.schema_name,
                        producer_id=pair.proof.binding_coordinates.producer_id,
                        endpoint_id=pair.proof.source_pin.endpoint_id,
                        alias_scope=pair.proof.source_pin.alias_scope,
                        resource_type="Organization",
                        resource_id=fields["source_key"],
                        legacy_network_id=fields["source_scope_json"]["legacy_uuid"],
                    ),
                )
                bindings = await read_approved_refresh_bindings(
                    await native_driver(session), approved, [enrollment_map], control_schema=schema
                )
            state.anchor_control_schema = schema
            state.anchor_approved = approved
            state.anchor_enrollment = enrollment_map
            yield state, reader, sessions, bindings


async def _resolve(sessions, reader, bindings, *, budget=None):
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        return await anchors.resolve_cms_refresh_anchors(
            session,
            bindings,
            reader=reader,
            read_budget=budget or RegistryPTGGraphReadBudget(16 * 1024 * 1024),
        )


async def _assert_scope_and_refusals(state, reader, sessions, bindings):
    binding = bindings[0]
    derived = await _resolve(sessions, reader, bindings)
    pin = state.pair.proof.source_pin
    expected_scope_map = dict(
        source_system="fhir",
        source_id=pin.source_id,
        dataset_schema=pin.schema_name,
        endpoint_id=pin.endpoint_id,
        producer_id=state.pair.proof.binding_coordinates.producer_id,
        alias_scope=pin.alias_scope,
        resource_type="Organization",
        resource_id="network-1",
        legacy_network_id=binding.record_json["source_scope_json"]["legacy_uuid"],
    )
    assert derived == {binding.binding_id: expected_scope_map}
    damaged_scope_map = {
        **binding.record_json["source_scope_json"],
        "legacy_uuid": "40000000-0000-4000-8000-000000000001",
    }
    damaged = replace(binding, record_json={**binding.record_json, "source_scope_json": damaged_scope_map})
    with pytest.raises(RegistryRefreshAnchorMismatch):
        await _resolve(sessions, reader, (damaged,))
    foreign = replace(binding, record_json={**binding.record_json, "producer_id": "other-producer"})
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await _resolve(sessions, reader, (foreign,))
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await _resolve(sessions, reader, bindings, budget=RegistryPTGGraphReadBudget(1))
    return expected_scope_map


async def _assert_native_tamper_and_permissions(state, reader, sessions, bindings):
    pin = state.pair.recipe.source_pin
    relation = f'"{pin.read_schema_name}".provider_directory_entity_release_evidence'
    async with state.database.engine.connect() as setup:
        transaction = await setup.begin()
        await setup.execute(
            text(
                f"UPDATE {relation} SET payload_sha256=repeat('f',64) WHERE resource_type='Organization' AND resource_id='network-1'"
            )
        )
        await transaction.commit()
    try:
        with pytest.raises(RegistryRefreshAnchorMismatch):
            await _resolve(sessions, reader, bindings)
    finally:
        async with state.database.engine.begin() as setup:
            await setup.execute(
                text(f"""UPDATE {relation} evidence SET payload_sha256=witness.raw_payload_sha256
                FROM "{pin.read_schema_name}".provider_directory_cms_npd_resource_witness witness
                WHERE evidence.source_id=witness.source_id AND evidence.release_id=witness.release_id
                AND evidence.resource_type=witness.resource_type AND evidence.resource_id=witness.resource_id
                AND evidence.resource_type='Organization' AND evidence.resource_id='network-1'""")
            )
    async with sessions() as session, session.begin():
        assert not await session.scalar(
            text("SELECT has_any_column_privilege(current_user,:relation,'INSERT,UPDATE')"),
            {"relation": f'"{reader.control_schema}".registry_cms_source_notification'},
        )
        assert not hasattr(reader, "notify_receipt") and not hasattr(reader, "source_attempt")


async def test_cms_anchor_uses_genuine_accepted_notification_and_original_native_evidence(monkeypatch, tmp_path):
    async with _accepted_source(monkeypatch, tmp_path) as (state, reader, sessions, bindings):
        expected_scope_map = await _assert_scope_and_refusals(state, reader, sessions, bindings)
        await _assert_native_tamper_and_permissions(state, reader, sessions, bindings)
        assert await _resolve(sessions, reader, bindings) == {bindings[0].binding_id: expected_scope_map}
