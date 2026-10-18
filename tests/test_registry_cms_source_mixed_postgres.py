# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mixed retained CMS/PTG component; upstream App decisions use existing fixtures.

Native stores, restricted PTG review/capture and composition are real. Registry
composition uses an original safe executor and independent candidate custodian.
The Source reader retains its existing identity; live App authority is separate.
"""

import json
import os
from contextlib import aclosing
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_approved_membership_source import pin_approved_membership_source
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_cms_source_consumer import (
    consume_registry_cms_source_notification_once,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_source_recipe_store import (
    canonical_registry_source_recipes,
    resolve_retained_registry_source_recipes,
)
from tests import test_registry_cms_source_consumer_initial_postgres as initial
from tests import test_registry_cms_source_consumer_postgres as cms
from tests import test_registry_ptg_office_coordinator_postgres as coordinator
from tests import test_registry_publication_ptg_runtime_postgres as ptg
from tests.test_provider_directory_cms_resource_batch_postgres import cms_source_template as cms_source_template

pytestmark = pytest.mark.usefixtures(cms_source_template.__name__)


async def _enter_fixture(stack, fixture):
    # Register aclose before acquisition so cancellation/setup failures also drain.
    await stack.enter_async_context(aclosing(fixture))
    return await anext(fixture)


async def _mixed_fixture(database, monkeypatch, stack, tmp_path, state_by_name, original):
    fixture = await original(database, monkeypatch, stack)
    dsn = database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    monkeypatch.setenv("HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN", dsn)
    assert os.environ.get("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") == "1"
    assert 180000 <= int(await fixture.connection.fetchval("SHOW server_version_num")) < 190000
    extensions = await fixture.connection.fetch("SELECT extname FROM pg_extension")
    assert {"intarray", "btree_gin", "postgis"} <= {extension["extname"] for extension in extensions}
    publisher = await _enter_fixture(stack, coordinator.office_publisher.__wrapped__(fixture, tmp_path))
    offices = await _enter_fixture(stack, coordinator.office_db.__wrapped__(fixture, tmp_path, monkeypatch, publisher))
    native = await _enter_fixture(
        stack, coordinator.coordinator_db.__wrapped__(offices, fixture, tmp_path, monkeypatch, publisher)
    )
    assert await fixture.connection.fetchval("SELECT current_database()") == database.engine.url.database
    assert native.custom is fixture
    service, recipes, _ = await ptg._review_offices(native)
    assert len(recipes) == 1
    await fixture.connection.execute(
        f'GRANT "{native.fixture.source.roles[2]}" TO "{fixture.executor_role}" '
        "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
    )
    engine = create_async_engine(fixture.execution_url, pool_size=1, max_overflow=0, hide_parameters=True)
    stack.push_async_callback(engine.dispose)
    runtime = RegistryPublicationPTGRuntime(
        async_sessionmaker(engine), service.store, RegistryPTGGraphReadBudget(16 * 1024 * 1024)
    )
    state_by_name.update(native=native, recipes=recipes, runtime=runtime)
    return fixture


async def _pin(fixture):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)


async def _publish(fixture, runtime, pair, source, recipes, revision, readers):
    request = uuid4()
    # Register the deterministic address artifact before native composition creates it.
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request, "address:" + approved.generation_id))
    target, address = await runtime.compose(
        fixture.execution_connection,
        request_id=request,
        approved_revision=revision,
        expected_head=source.generation_id,
        source_manifest=source,
        address_sources=RegistryCompositionAddressSources(pair.address_source, replacement_source_recipes=recipes),
        writer_roles={**fixture.writer_roles, "reader_roles": readers},
        control_schema=fixture.control_schema,
    )
    await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        target,
        address,
        **{**fixture.writer_roles, "reader_roles": readers},
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )
    return await _pin(fixture)


async def _site(fixture, native, pair):
    binding_id = UUID(native.request["selector"]["selections"][0]["binding_id"])
    site_document = await fixture.connection.fetchval(
        f'SELECT record_json::text FROM "{fixture.control_schema}".registry_approved_record '
        "WHERE record_kind='site_binding' AND record_key=$1 ORDER BY approved_revision DESC LIMIT 1",
        str(binding_id),
    )
    site = json.loads(site_document)
    assert site["provider_system"] == "npi"
    site["expected_network_id"] = native.request["source_command"]["network_id"]
    assert not await fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".registry_approved_record head '
        "CROSS JOIN LATERAL jsonb_to_recordset(CASE WHEN head.record_kind='membership' "
        "THEN head.record_json->'memberships_json' ELSE '[]'::jsonb END) "
        "AS member(provider_system text,provider_id text,location_id uuid) "
        "WHERE (member.provider_system,member.provider_id,member.location_id)=($1,$2,$3))",
        site["provider_system"],
        site["provider_id"],
        UUID(site["location_id"]),
    )
    base = pair.address_source
    assert not await fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{base.schema_name}"."{base.table_name}" WHERE location_key=$1)',
        site["location_key"],
    )
    retained_pin = native.fixture.serving
    address_witness = await fixture.connection.fetchrow(
        f"SELECT to_jsonb(address)::text AS document, "
        "encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS sha256 "
        f'FROM "{retained_pin.schema_name}".entity_address_unified address WHERE location_key=$1',
        site["location_key"],
    )
    assert address_witness["sha256"] == site["address_row_sha256"]
    return site, json.loads(address_witness["document"])


async def _snapshot(fixture, pin):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await resolve_network_serving_manifest(
                fixture.connection, generation_id=pin.generation_id, control_schema=fixture.control_schema
            )
            == pin
        )
        rows = []
        for table in ("network_membership", "entity_address_unified", "provider_location_binding"):
            rows.append(
                await fixture.connection.fetchval(
                    f"SELECT coalesce(jsonb_agg(to_jsonb(row) ORDER BY to_jsonb(row)::text),'[]'::jsonb)::text "
                    f'FROM "{pin.schema_name}"."{table}" row'
                )
            )
        return tuple(rows)


async def _assert_mixed(fixture, current, pair, ptg_recipes, site, source_row, manual):
    """Check both immutable recipes, exact carried office, and latest custom intent."""
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        recipes = await resolve_retained_registry_source_recipes(
            fixture.connection, current, control_schema=fixture.control_schema
        )
    assert canonical_registry_source_recipes(recipes) == canonical_registry_source_recipes((*ptg_recipes, pair.recipe))
    assert len(recipes) == 2 and current.approved_custom_revision == manual.revision
    namespace = f'"{current.schema_name}"'
    actual = json.loads(
        await fixture.connection.fetchval(
            f"SELECT to_jsonb(address)::text FROM {namespace}.entity_address_unified address WHERE location_key=$1",
            site["location_key"],
        )
    )
    # Projection intentionally recomputes only this array; every other field is exact.
    expected_by_field = dict(source_row, canonical_network_ids=actual["canonical_network_ids"])
    assert actual == expected_by_field and actual["canonical_network_ids"]
    assert await fixture.connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {namespace}.network_membership "
        "WHERE (provider_system,provider_id,location_id,network_id)=($1,$2,$3,$4))",
        site["provider_system"],
        site["provider_id"],
        UUID(site["location_id"]),
        site["expected_network_id"],
    )
    assert await fixture.connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {namespace}.network_membership "
        "WHERE provider_system='manual' AND provider_id=$1)",
        manual.provider["record_id"],
    )
    assert await fixture.connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {namespace}.network_membership WHERE provider_system='npi' AND provider_id='1000000491')"
    )
    assert await fixture.connection.fetchval(
        f"SELECT bool_and(pg_typeof(canonical_network_ids)::text='integer[]') FROM {namespace}.entity_address_unified"
    )
    await _assert_carried_hash(fixture, current, site)


async def _assert_carried_hash(fixture, current, site):
    candidate = await fixture.connection.fetchrow(
        f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(current.candidate_id),
    )
    checkpoint = json.loads(candidate["validation_json"])["cms_source_notification"]
    attempt = UUID(checkpoint["attempt_id"])
    approved_generation = current.source_generations["custom_membership"]
    address_schema = "network_composition_" + uuid5(attempt, "address:" + approved_generation).hex
    # This is the copied address artifact before canonical-network projection.
    assert (
        await fixture.connection.fetchval(
            "SELECT encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') "
            f'FROM "{address_schema}".entity_address_unified address WHERE location_key=$1',
            site["location_key"],
        )
        == site["address_row_sha256"]
    )
    receipt = json.loads(
        await fixture.connection.fetchval(
            "SELECT obj_description(to_regnamespace($1),'pg_namespace')",
            address_schema,
        )
    )
    assert receipt["revision"] == 3 and len(receipt["ptg_carry_sha256"]) == 64


async def _flow(database, pair, notice, old, fixture, store, sessions, state_by_name):
    native = state_by_name["native"]
    runtime = state_by_name["runtime"]
    config = await cms.custody.candidate_custody_consumer(fixture, pair, old, store, runtime)
    ptg_recipes = state_by_name["recipes"]
    site, source_row = await _site(fixture, native, pair)
    await cms.recipes._reviewed_cms_binding(
        fixture.connection,
        fixture.control_schema,
        fixture.engine,
        old.recipe.source_pin,
        old.recipe.binding_coordinates,
    )
    revision = await fixture.connection.fetchval(
        f'SELECT approved_revision FROM "{fixture.control_schema}".registry_revision_control WHERE id=1'
    )
    incumbent = await _publish(
        fixture, runtime, old, await _pin(fixture), (*ptg_recipes, old.recipe), revision, pair.runtime_roles
    )
    old_rows = await _snapshot(fixture, incumbent)
    await cms.recipes._reviewed_cms_binding(
        fixture.connection,
        fixture.control_schema,
        fixture.engine,
        pair.recipe.source_pin,
        pair.recipe.binding_coordinates,
    )
    manual = await cms.custom._seed(fixture)
    arguments_by_name = dict(
        connection=fixture.execution_connection,
        notification_sessions=sessions,
        notification_store=store,
        source_schema="mrf",
        receipt_id=notice.pair.proof.serving_receipt_id,
        notification_sha256=notice.sha256,
        config=config,
        source_endpoint_id=pair.recipe.source_pin.endpoint_id,
    )
    completed = await consume_registry_cms_source_notification_once(**arguments_by_name)
    assert completed["state"] == "completed"
    current = await _pin(fixture)
    await _assert_mixed(fixture, current, pair, ptg_recipes, site, source_row, manual)
    assert await _snapshot(fixture, incumbent) == old_rows
    later = await _publish(
        fixture, runtime, pair, current, (*ptg_recipes, pair.recipe), manual.revision, pair.runtime_roles
    )
    assert later.generation_id > current.generation_id
    replay = await consume_registry_cms_source_notification_once(**arguments_by_name)
    assert replay["state"] == "replayed" and replay["manifest"] == {**completed["manifest"], "replayed": True}
    assert await _pin(fixture) == later
    return dict(arguments_by_name=arguments_by_name, generation=current.generation_id)


@pytest.mark.asyncio
async def test_mixed_cms_refresh_retains_nonmanual_ptg_office_and_latest_custom(monkeypatch, tmp_path):
    """Same DB, real retained receipts/reviewed capture, no ordinary-role claim."""
    state_by_name = {}
    original = cms._registry_fixture

    async def fixture(database, patch, stack):
        return await _mixed_fixture(database, patch, stack, tmp_path, state_by_name, original)

    async def flow(*arguments):
        return await _flow(*arguments, state_by_name)

    monkeypatch.setattr(cms, "_registry_fixture", fixture)
    monkeypatch.setattr(cms, "_direct_reader", initial._publisher_sessions)
    monkeypatch.setattr(cms, "_consume_pair", flow)
    await cms.test_committed_notification_refresh_replay_review_and_owned_lock(monkeypatch, tmp_path)
