# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Accepted CMS offices compose compactly and retain their exact address origin."""

from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from api.network_address_scope import network_address_read_scope
from api.network_compact_address import compact_address_native_sql
from api.network_provider_only_read import read_network_provider_only_page
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_source_office_origin import resolve_source_office_origin
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.cms_registry_recipe_postgres_support import cms_recipe_database as cms_recipe_database
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.cms_registry_recipe_postgres_support import serving_schema as serving_schema
from tests.registry_compact_source_office_postgres_support import compact_cms_offices
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_fhir_membership_source_postgres import _reviewed_binding
from tests.test_network_initial_source_office_bindings_postgres import cms_full_office_source as cms_full_office_source
from tests.test_network_provider_only_read_postgres import provider_only_source as provider_only_source
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_generic_fhir_reuse_postgres import _inventory_metadata
from tests.test_registry_source_office_origin_postgres import _rotate

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def compact_cms_source(cms_full_office_source, provider_only_source, custom_db, monkeypatch):
    async with compact_cms_offices(custom_db, provider_only_source, monkeypatch) as context:
        yield context


async def _compose(context, revision, manifest=None):
    fixture = context.fixture
    request = uuid4()
    context.requests.append(request)
    target, source = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=request,
        approved_revision=revision,
        expected_head=0 if manifest is None else manifest.generation_id,
        source_manifest=manifest,
        address_sources=RegistryCompositionAddressSources(
            context.source,
            compact=True,
            initial_source_recipes=(RegistrySourceMembershipRecipe(context.source_pin, context.reviewed[4]),)
            if manifest is None
            else (),
        ),
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    context.targets.append(target)
    assert source == context.source
    return target


async def _publish(context, target):
    fixture = context.fixture
    await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        target,
        context.source,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(
            fixture.execution_connection, control_schema=fixture.control_schema
        )


async def _assert_summary(context, target, revision):
    candidate_by_field = dict(
        await context.fixture.connection.fetchrow(
            f'SELECT * FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(target.candidate_id),
        )
    )
    summary = verify_initial_source_office_receipt(candidate_by_field)
    _contribution, checked_summary, descriptor, _derivation = await _inventory_metadata(
        context.fixture, target, witness=False, address_source=context.source
    )
    assert checked_summary == summary
    assert (summary["revision"], summary["recipe_count"], summary["office_rows"]) == (revision, 1, 1)
    namespace = '"' + target.schema_name + '"'
    assert await context.fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.provider_location_binding") == 1
    assert (
        await context.fixture.connection.fetchval(
            "SELECT to_regclass($1)", target.schema_name + ".entity_address_unified"
        )
        is None
    )
    contributions = candidate_by_field["validation_json"]
    import json

    contributions = json.loads(contributions) if type(contributions) is str else contributions
    assert contributions["registry_source_contributions"] == _contribution
    assert descriptor["provider_only_rows"] == 3 and descriptor["membership_rows"] == 1
    return candidate_by_field, summary


async def _rebind(context):
    fixture = context.fixture
    actor, networks, binding = context.reviewed[1:4]
    changed_binding_by_field = {
        **binding,
        "network_id": networks[1]["record_id"],
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": networks[0]["record_id"],
    }
    result = (await _reviewed_binding(fixture.connection, fixture.control_schema, actor, [changed_binding_by_field]))[
        "records"
    ][0]
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, result),
        actor,
    )
    return approved["approved_revision"]


async def _wrong_office_refusal(context, target):
    fixture = context.fixture
    await prepare_network_candidate(
        fixture.execution_connection,
        target,
        context.source,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    await fixture.connection.execute(
        f'UPDATE "{target.schema_name}".provider_location_binding SET location_key=$1', "b" * 64
    )
    try:
        with pytest.raises(ValueError, match="registry_source_office_bindings_differ"):
            await fixture.seal_candidate(target)
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
                UUID(target.candidate_id),
            )
            == 0
        )
    finally:
        await fixture.connection.execute(
            f'UPDATE "{target.schema_name}".provider_location_binding SET location_key=$1', "a" * 64
        )
    await fixture.seal_candidate(target)


async def _read_generation(context, manifest, network_id):
    fixture = context.fixture
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        current = await resolve_network_serving_manifest(
            fixture.execution_connection, generation_id=manifest.generation_id, control_schema=fixture.control_schema
        )
        relation, arguments = compact_address_native_sql(
            current.compact_layout, first_parameter=1, network_ids=(network_id,)
        )
        offices = await fixture.execution_connection.fetch(
            f"SELECT location_key,canonical_network_ids FROM {relation} office", *arguments
        )
        with network_address_read_scope(current, (network_id,)) as scope:
            providers = await read_network_provider_only_page(
                fixture.execution_connection, scope, control_schema=fixture.control_schema
            )
        assert providers["total_count"] == 2
        assert {provider["provider_id"] for provider in providers["providers"]} == {"1000000491", "1000000004"}
        assert len(offices) == 1 and offices[0]["location_key"] == "a" * 64
        assert offices[0]["canonical_network_ids"] == [network_id]
        return [dict(office) for office in offices], providers


async def _assert_rotated_rebind(context, manifest, monkeypatch):
    original_network = context.reviewed[2][0]["record_id"]
    expected = await _read_generation(context, manifest, original_network)
    await _rotate(context, monkeypatch, old_key="a" * 64, new_key="c" * 64)
    revision = await _rebind(context)
    second = await _compose(context, revision, manifest)
    candidate_by_field, summary = await _assert_summary(context, second, 4)
    _contribution, _office, _descriptor, derivation = await _inventory_metadata(
        context.fixture, second, witness=False, address_source=context.source
    )
    assert derivation["mode"] == "facts"
    assert summary["origin_manifest_sha256"] == manifest.manifest_sha256
    assert (
        await context.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{second.schema_name}".network_source_contribution'
        )
        == 0
    )
    current = await _publish(context, second)
    assert current.compact_layout.base.table_oid == manifest.compact_layout.base.table_oid
    assert "entity_address_unified_retained_" in current.compact_layout.base.table_name
    assert await _read_generation(context, manifest, original_network) == expected
    await _read_generation(context, current, context.reviewed[2][1]["record_id"])


async def test_first_compact_cms_source_offices_have_independent_correspondence(compact_cms_source, monkeypatch):
    context = compact_cms_source
    first = await _compose(context, context.reviewed[5].approved_revision)
    await _assert_summary(context, first, 4)
    await _wrong_office_refusal(context, first)
    manifest = await _publish(context, first)
    assert manifest.compact_layout.base.table_oid == context.address.descriptor["table"]["oid"]
    assert manifest.compact_layout.custom is None
    await _assert_rotated_rebind(context, manifest, monkeypatch)
