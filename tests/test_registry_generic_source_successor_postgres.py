# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Two genuinely published Source editions retain approval through atomic Network refresh."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from api.network_address_scope import network_address_read_scope
from api.network_compact_address import compact_address_native_sql
from api.network_provider_only_read import read_network_provider_only_page
from process.network_cms_registry_address_capture import native_driver
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_membership_refresh_authority import COMMIT_AUTHORITY_PATH
from process.registry_membership_refresh_queue import validated_membership_refresh_request_status
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes
from tests import test_network_core_layer_provider_postgres as transport
from tests.generic_source_successor_postgres_support import (
    generic_refresh_execution,
    native_generic_source,
    publish_edition,
    start_refresh_runtime,
)
from tests.test_registry_membership_refresh_execution_postgres import _counts, _heads, _storage_rows

pytestmark = pytest.mark.asyncio


async def _read(fixture, connection, generation=None):
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=fixture.control_schema
    )
    relation, arguments = compact_address_native_sql(manifest.compact_layout, first_parameter=1)
    offices = await connection.fetch(
        f"SELECT second_line,canonical_network_ids FROM {relation} ORDER BY second_line", *arguments
    )
    with network_address_read_scope(manifest, (fixture.network_id,)) as scope:
        providers = await read_network_provider_only_page(connection, scope, control_schema=fixture.control_schema)
    return manifest, [(row["second_line"], row["canonical_network_ids"]) for row in offices], providers


async def _consume(fixture):
    desired = await fixture.bundle.inputs_service()
    queued = await fixture.enqueue(fixture.command(desired))
    result = await fixture.consume(queued["request_id"])
    assert result["state"] == "completed", result
    async with fixture.api.transaction(isolation="repeatable_read", readonly=True):
        snapshot = await _read(fixture, fixture.api)
    await transport._inventory_metadata(fixture, snapshot[0], office_revision=5)
    return desired, queued, snapshot


async def _assert_successor_facts(fixture, manifest, original, current):
    descriptor, _derivation = await transport._inventory_metadata(fixture, manifest, office_revision=5)
    assert descriptor["descriptor_version"] == 2
    assert descriptor["fact_candidate_id"] is None and descriptor["fact_rows"] == 4
    async with fixture.api.transaction(isolation="repeatable_read", readonly=True):
        recipes = await resolve_retained_registry_source_recipes(
            fixture.api,
            manifest,
            control_schema=fixture.control_schema,
        )
        assert len(recipes) == 1
        pin = recipes[0].source_pin
        assert pin.retained_capture == original and pin.retained_successor.retained_capture == current
        assert recipes[0].binding_coordinates.edition_id == original.capture_id
        facts = await fixture.api.fetch(
            f'SELECT fact_json::text FROM "{manifest.schema_name}".network_source_contribution'
        )
    assert len(facts) == 4
    assert sum(json.loads(fact_record["fact_json"])["kind"] == "resource" for fact_record in facts) == 2
    for fact_record in facts:
        fact = json.loads(fact_record["fact_json"])
        assert fact["fact_version"] == 2
        if fact["kind"] == "resource":
            assert fact["omission_reason"] is None and fact["malformed"] is False
            continue
        assert fact["source_scope_json"] == {
            "organization_id": "network-one",
            "capture_sha256": original.capture_sha256,
        }
        assert fact["evidence_basis"][:3] == [current.origin.dataset_id, current.origin.source_id, current.release_id]


async def _assert_replay(fixture, queued, manifest, inputs):
    current_storage = await _storage_rows(fixture, manifest.schema_name)
    before = await _counts(fixture)
    command = fixture.command(inputs)
    request_query = (
        f'SELECT request.*,xmin::text AS row_xmin,ctid::text AS row_ctid FROM "{fixture.control_schema}".'
        "registry_publication_request request WHERE request_id=$1"
    )
    completed_record = await fixture.connection.fetchrow(request_query, UUID(queued["request_id"]))
    relation_oids = await _heap_oids(fixture, manifest)
    commits_before, cas_before = fixture.calls.count(COMMIT_AUTHORITY_PATH), list(fixture.cas_checks)
    assert completed_record["state"] == "completed"
    assert validated_membership_refresh_request_status(completed_record) == command
    assert await fixture.enqueue(command) == {**queued, "state": "completed", "replayed": True}
    assert await fixture.consume(queued["request_id"]) is None
    assert await fixture.connection.fetchrow(request_query, UUID(queued["request_id"])) == completed_record
    assert await _counts(fixture) == before
    assert await _storage_rows(fixture, manifest.schema_name) == current_storage
    assert await _heap_oids(fixture, manifest) == relation_oids
    assert fixture.calls.count(COMMIT_AUTHORITY_PATH) == commits_before == 2 and fixture.cas_checks == cas_before


async def _heap_oids(fixture, manifest):
    return await fixture.connection.fetch(
        "SELECT relname,oid::bigint,relfilenode::bigint FROM pg_class "
        "WHERE relnamespace=to_regnamespace($1) AND relkind='r' ORDER BY relname",
        manifest.schema_name,
    )


async def _transport_extension_receipt(fixture, record_property):
    """Reuse the existing native transport's PostGIS prerequisite in this owned database."""
    query = (
        "SELECT e.extname,e.extversion,n.nspname AS schema_name FROM pg_extension e "
        "JOIN pg_namespace n ON n.oid=e.extnamespace ORDER BY e.extname"
    )
    before_extensions = [dict(extension) for extension in await fixture.connection.fetch(query)]
    await fixture.connection.execute('CREATE EXTENSION IF NOT EXISTS "postgis"')
    after_extensions = [dict(extension) for extension in await fixture.connection.fetch(query)]
    assert all(extension in after_extensions for extension in before_extensions)
    assert any(extension["extname"] == "postgis" for extension in after_extensions)
    record_property(
        "source_database_extension_receipt",
        json.dumps(
            {
                "database": await fixture.connection.fetchval("SELECT current_database()"),
                "before": before_extensions,
                "after": after_extensions,
            },
            sort_keys=True,
        ),
    )


async def _assert_connected_transport(
    monkeypatch, source_fixture, fixture, original, current, layer_inputs, record_property
):
    """Read both actual Source-derived generations without changing their Reader."""
    reader = source_fixture.component.reader
    adapter = SimpleNamespace(
        connection=fixture.connection,
        engine=fixture.engine,
        control_schema=fixture.control_schema,
        roles={"reader": reader},
        writer_roles={"owner_role": fixture.roles_by_kind["physical"]},
    )
    reader_url = source_fixture.component.admin_engine.url.set(username=reader)
    await _transport_extension_receipt(fixture, record_property)
    await transport._assert_core_layer_reads(
        adapter,
        monkeypatch,
        layer_inputs=layer_inputs,
        network_id=fixture.network_id,
        generations=(original[0].generation_id, current[0].generation_id),
        reader_url=reader_url,
    )


async def _exercise_source_successor(monkeypatch, *, layer_inputs=None, record_property=None):
    async with native_generic_source(monkeypatch) as source_fixture:
        original_capture = await publish_edition(source_fixture, 1)
        async with generic_refresh_execution(source_fixture, monkeypatch) as (fixture, serving, stack, head_url):
            await transport._assert_inventory_migration(fixture)
            await start_refresh_runtime(fixture, source_fixture, serving, stack, head_url)
            approved_heads, enrollment = await _heads(fixture), deepcopy(fixture.enrollment)
            first_inputs, _, original = await _consume(fixture)
            assert original[1] == [("Suite 1", [fixture.network_id]), ("Suite 2", [])]
            assert {provider_row["provider_id"] for provider_row in original[2]["providers"]} == {"1000000004"}
            assert first_inputs["accepted_sources"][0]["capture_generation"] == original_capture.capture_sha256
            original_storage = await _storage_rows(fixture, original[0].schema_name)
            async with fixture.outer() as old_session, old_session.begin():
                await old_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                old_reader = await native_driver(old_session)
                assert await _read(fixture, old_reader) == original
                successor = await publish_edition(source_fixture, 2)
                assert successor.capture_id != original_capture.capture_id
                assert successor.capture_sha256 != original_capture.capture_sha256
                assert successor.content_sha256 != original_capture.content_sha256
                second_inputs, queued, current = await _consume(fixture)
                assert current[0].generation_id > original[0].generation_id
                assert current[0].compact_layout.base.table_oid == original[0].compact_layout.base.table_oid
                assert current[1] == [("Suite 1", []), ("Suite 2", [fixture.network_id])]
                assert {provider_row["provider_id"] for provider_row in current[2]["providers"]} == {"1000000491"}
                assert second_inputs["accepted_sources"][0]["capture_generation"] == successor.capture_sha256
                if layer_inputs is not None:
                    await _assert_connected_transport(
                        monkeypatch, source_fixture, fixture, original, current, layer_inputs, record_property
                    )
                assert await _read(fixture, old_reader) == original
                assert await _read(fixture, old_reader, original[0].generation_id) == original
            assert await _heads(fixture) == approved_heads and fixture.enrollment == enrollment
            assert source_fixture.commits == [candidate.dataset_id for candidate in source_fixture.candidates]
            assert source_fixture.source_writer_rejections == source_fixture.commits
            assert len(source_fixture.commits) == 2 and fixture.calls.count(COMMIT_AUTHORITY_PATH) == 2
            assert await _storage_rows(fixture, original[0].schema_name) == original_storage
            assert fixture.cas_checks == [original[0].generation_id, current[0].generation_id]
            await _assert_successor_facts(fixture, current[0], original_capture, successor)
            await _assert_replay(fixture, queued, current[0], second_inputs)


async def test_generic_source_successor_refresh_replay(monkeypatch, record_property):
    record_property("proof_mode", "source-successor")
    await _exercise_source_successor(monkeypatch)


async def test_generic_source_successor_core_layer_transport(monkeypatch, record_property):
    layer_inputs = transport._native_layer_inputs()
    record_property("proof_mode", "source-successor-core-layer")
    await _exercise_source_successor(monkeypatch, layer_inputs=layer_inputs, record_property=record_property)


async def test_existing_http_reader_preserves_credentials_and_grants():
    url = make_url("postgresql+asyncpg://synthetic_admin:discard@localhost/synthetic_source")
    supplied = url.set(username="synthetic_reader")
    fixture = SimpleNamespace(
        roles={"reader": supplied.username},
        engine=SimpleNamespace(url=url),
        connection=SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("existing Reader mutation"))),
    )
    assert await transport._http_reader_url(fixture, supplied) is supplied
    fixture.connection.execute.assert_not_awaited()


@pytest.mark.parametrize("change", [{"username": "other_reader"}, {"database": "other_database"}])
async def test_existing_http_reader_refuses_different_actor_or_database(change):
    url = make_url("postgresql+asyncpg://synthetic_reader:discard@localhost/synthetic_source")
    fixture = SimpleNamespace(
        roles={"reader": url.username},
        engine=SimpleNamespace(url=url),
        connection=SimpleNamespace(execute=AsyncMock()),
    )
    with pytest.raises(AssertionError):
        await transport._http_reader_url(fixture, url.set(**change))
    fixture.connection.execute.assert_not_awaited()


async def test_connected_transport_requires_binary_before_source_resources(monkeypatch):
    monkeypatch.delenv("NETWORK_PROVIDER_TEST_LAYER_BINARY", raising=False)
    monkeypatch.delenv("NETWORK_PROVIDER_TEST_LAYER_ROOT", raising=False)
    with pytest.raises(pytest.skip.Exception, match="NETWORK_PROVIDER_TEST_LAYER_BINARY"):
        await test_generic_source_successor_core_layer_transport(monkeypatch, lambda *arguments: None)
