# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Upgrade a real retained bootstrap head through ordinary queued Source publication."""

import json
from contextlib import aclosing, asynccontextmanager
from dataclasses import replace
from uuid import UUID

import pytest

from api.network_compact_address import compact_address_native_sql
from db.models import EntityAddressUnified
from process.network_serving_read import resolve_network_serving_manifest
from tests.source_witness_capture_postgres_support import capture_retained_source_for_roles
from tests.test_network_custom_address_source_postgres import (
    _bootstrap_prepare,
    _bootstrap_specification,
    bootstrap_db,
)
from tests.test_network_fhir_retained_capture_postgres import _approved_capture_recipe
from tests.test_network_fhir_source_witness_publication_postgres import (
    _consume_expected_success,
    _consumer,
    _manifest,
    _membership_snapshot,
    _publication_callbacks,
    _publication_case,
)
from tests.test_registry_initial_inventory_postgres import _sources as _bootstrap_sources
from tests.test_registry_publication_consumer_postgres import _enqueue_ordinary, _retained_recipe_custody, _status
from tests.test_registry_publication_worker_permissions_postgres import _login

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _retained_bootstrap(context, monkeypatch):
    """Close actual native parent migrations with the existing Source office records."""
    async with aclosing(bootstrap_db.__wrapped__(context.serving, monkeypatch)) as generator:
        fixture = await anext(generator)
        connection = fixture.connection
        columns = ",".join('"' + name + '"' for name in EntityAddressUnified.__table__.columns.keys())
        await connection.execute(f'DELETE FROM "{fixture.source_schema}".entity_address_unified')
        await connection.execute(
            f'INSERT INTO "{fixture.source_schema}".entity_address_unified({columns}) '
            f'SELECT {columns} FROM "{context.address.schema_name}".entity_address_unified'
        )
        await connection.execute(f'INSERT INTO "{fixture.source_schema}".npi(npi) VALUES(1234567893)')
        receipt = await _bootstrap_prepare(
            fixture,
            await _bootstrap_specification(fixture),
            runtime_roles=(context.worker.fixture.roles["reader"],),
        )
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        yield receipt


async def _queued_publication(context, sources, *, head=0):
    async with _consumer(context.worker) as (arguments, calls):
        arguments["initial_address_sources"] = sources
        queued = await _enqueue_ordinary(context.worker, head)
        completed = await _consume_expected_success(context, arguments, queued)
        assert completed["request_id"] == queued["request_id"]
        assert (await _status(context.worker, queued))["state"] == "completed" and len(calls) >= 4
        assert await _consume_expected_success(context, arguments, queued) is None
    manifest = await _manifest(context)
    assert manifest.generation_id == completed["result"]["serving_generation"] > head
    return manifest


async def _address_snapshot(connection, manifest):
    if manifest.compact_layout is None:
        relation, parameters = f'"{manifest.schema_name}".entity_address_unified', ()
    else:
        relation, parameters = compact_address_native_sql(manifest.compact_layout, first_parameter=1)
    return tuple(
        tuple(row)
        for row in await connection.fetch(
            f"SELECT location_key,entity_id,second_line,canonical_network_ids FROM {relation} office ORDER BY location_key",
            *parameters,
        )
    )


async def _candidate_address_pin(context, manifest):
    validation = await context.worker.fixture.connection.fetchval(
        f'SELECT validation_json::text FROM "{context.worker.fixture.control_schema}".network_membership_candidate '
        "WHERE candidate_id=$1",
        UUID(manifest.candidate_id),
    )
    return json.loads(validation)["address_source"]


async def _published_memberships(connection, manifest):
    if manifest.compact_layout is not None:
        return await _membership_snapshot(connection, manifest)
    memberships = await connection.fetch(
        f"SELECT network_id,provider_system,provider_id,location_id::text,evidence_id "
        f'FROM "{manifest.schema_name}".network_membership '
        "ORDER BY network_id,provider_system,provider_id,location_id,evidence_id"
    )
    # Legacy serving has no provider-only evidence table.
    return tuple(map(tuple, memberships)), ()


async def _assert_compact_generation(context, manifest, bootstrap, retained_network, original):
    assert manifest.compact_layout is not None
    assert manifest.compact_layout.base.schema_name == bootstrap.address_source.schema_name
    assert manifest.compact_layout.base.table_oid == dict(bootstrap.relation_oids)["entity_address_unified"]
    async with _login(context.worker, "execution") as reader:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            memberships, provider_only = await _membership_snapshot(reader, manifest)
            assert tuple(row for row in memberships if row[0] == retained_network) == original[0]
            assert {row[0] for row in provider_only} == {retained_network, context.network_id}
            assert {row[0] for row in memberships} == {retained_network, context.network_id}
            assert len(memberships) == 2 and len(provider_only) == 2
            addresses = await _address_snapshot(reader, manifest)
            assert len(addresses) == 2
            assert {network for row in addresses for network in row[3]} == {retained_network, context.network_id}
    return memberships, provider_only, addresses


async def test_legacy_bootstrap_head_upgrades_and_reuses_compact_origin(monkeypatch, record_property):
    async with _publication_case(monkeypatch) as context, _retained_bootstrap(context, monkeypatch) as bootstrap:
        retained_capture = await capture_retained_source_for_roles(context.case, context.runtime.runtime_roles)
        retained, context.approved, retained_network = await _approved_capture_recipe(
            context.case.fixture, context.serving, retained_capture
        )
        context.worker.bootstrap = bootstrap
        initial = replace(
            _bootstrap_sources(context.worker),
            initial_source_recipes=(retained,),
            source_witness_runtime=context.runtime,
        )
        context.worker.publication_custody_by_name = _retained_recipe_custody(context.worker, context.operator)
        first = await _queued_publication(context, initial)
        assert first.compact_layout is None
        assert await _candidate_address_pin(context, first) == {
            "schema_name": bootstrap.address_source.schema_name,
            "table_name": bootstrap.address_source.table_name,
            "generation_id": bootstrap.generation_sha256,
        }
        replacement = replace(
            initial, compact=True, initial_source_recipes=(), replacement_source_recipes=(retained, context.recipe)
        )
        context.worker.publication_custody_by_name = _publication_callbacks(context)
        async with _login(context.worker, "execution") as pinned:
            async with pinned.transaction(isolation="repeatable_read", readonly=True):
                assert (
                    await resolve_network_serving_manifest(pinned, control_schema=context.worker.fixture.control_schema)
                    == first
                )
                original = await _published_memberships(pinned, first)
                original_addresses = await _address_snapshot(pinned, first)
                assert {membership[0] for membership in original[0]} == {retained_network}
                second = await _queued_publication(context, replacement, head=first.generation_id)
                assert await _published_memberships(pinned, first) == original
                assert await _address_snapshot(pinned, first) == original_addresses
                assert (
                    await resolve_network_serving_manifest(pinned, control_schema=context.worker.fixture.control_schema)
                    == first
                )
        second_rows = await _assert_compact_generation(context, second, bootstrap, retained_network, original)
        third = await _queued_publication(context, replacement, head=second.generation_id)
        assert await _assert_compact_generation(context, third, bootstrap, retained_network, original) == second_rows
        assert third.compact_layout.base == second.compact_layout.base
        record_property("component", "legacy_bootstrap_to_compact_ordinary_publication")
        record_property("generations", json.dumps([first.generation_id, second.generation_id, third.generation_id]))
        record_property(
            "memberships", "one retained; two after approved witness introduction; two on same-origin reuse"
        )
        record_property("legacy_pinned_reads", "membership and address results unchanged across publication")
        record_property("limits", "local real queue/custody/Source fixture; no deployed import or API timing claim")
