# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-only pages through actual source custody, publication and reader ACLs."""

import hashlib
import json
from contextlib import AsyncExitStack
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest

from api import network_provider_only_read as readers
from api.network_address_scope import network_address_read_scope
from api.network_provider_read import (
    NetworkProviderReadError,
    NetworkProviderReadUnavailable,
    read_network_provider_page,
)
from process.network_address_projection import PinnedAddressSource
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_membership_publication import NetworkPublicationError
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from tests.cms_registry_recipe_postgres_support import cms_recipe_database as cms_recipe_database
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.cms_registry_recipe_postgres_support import serving_schema as serving_schema
from tests.source_contribution_custody_postgres_support import install_source_contribution_custody
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_candidate_composition_postgres import _remove_candidates
from tests.test_registry_source_recipe_composition_postgres import (
    _complete_cms_office_source,
    _publish_raw_recipe,
    _review_source_operation,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def provider_only_source(cms_recipe_database, serving_schema, monkeypatch, tmp_path):
    """Admit two provider-only identities, including one without any office fact."""
    from tests import cms_registry_recipe_postgres_support as support

    original = support._membership_resource_rows

    def resources(revision_source, revision):
        documents = original(revision_source, revision)
        practitioner = documents["Practitioner"][0]
        documents["Practitioner"].append(
            {
                **practitioner,
                "id": "provider-only-person",
                "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000004"}],
            }
        )
        role = documents["PractitionerRole"][0]
        documents["PractitionerRole"].extend(
            [
                {**role, "id": "zz-provider-only", "location": []},
                {**role, "id": "zz-provider-only-again", "location": []},
                {
                    **role,
                    "id": "zz-provider-only-second",
                    "location": [],
                    "practitioner": {"reference": "Practitioner/provider-only-person"},
                },
            ]
        )
        return documents

    monkeypatch.setattr(support, "_membership_resource_rows", resources)
    async with _complete_cms_office_source(
        cms_recipe_database, serving_schema, monkeypatch, tmp_path
    ) as source_fixture:
        yield source_fixture


async def _assert_seal_required(fixture, copy_target, addresses, expected_head):
    """Preparation may complete, but missing custody cannot move the serving head."""
    with pytest.raises(NetworkPublicationError, match="independent physical custody"):
        await prepare_and_publish_network_candidate(
            fixture.connection, copy_target, addresses, **fixture.writer_roles, control_schema=fixture.control_schema
        )
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == expected_head
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == 0
    )


async def _compose(context, *, revision, source_manifest):
    fixture = context.fixture
    request_id = uuid4()
    context.request_ids.append(request_id)
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.connection,
        request_id=request_id,
        approved_revision=revision,
        expected_head=source_manifest.generation_id,
        address_sources=RegistryCompositionAddressSources(
            PinnedAddressSource(source_manifest.schema_name, "entity_address_unified", source_manifest.manifest_sha256)
        ),
        writer_roles=fixture.writer_roles,
        source_manifest=source_manifest,
        control_schema=fixture.control_schema,
    )
    context.copy_targets.append(copy_target)
    artifact_generation = await fixture.connection.fetchval(
        f"SELECT source_generations->>'custom_membership' FROM \"{fixture.control_schema}\".network_membership_candidate "
        "WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    if artifact_generation is not None:
        fixture.composition_ids.append(uuid5(request_id, "address:" + artifact_generation))
    await _assert_seal_required(fixture, copy_target, addresses, source_manifest.generation_id)
    await fixture.seal_candidate(copy_target)
    await prepare_and_publish_network_candidate(
        fixture.connection,
        copy_target,
        addresses,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
                UUID(copy_target.candidate_id),
            )
            == 1
        )
        return manifest


@pytest.fixture
async def provider_only_db(custom_db, provider_only_source):
    fixture = custom_db
    context = SimpleNamespace(
        fixture=fixture,
        source=provider_only_source,
        copy_targets=[],
        request_ids=[],
        network_id=provider_only_source[2][0]["record_id"],
        other_network_id=provider_only_source[2][1]["record_id"],
    )
    async with AsyncExitStack() as stack:
        await install_source_contribution_custody(fixture, context.source[0][2], stack)
        context.original = await _publish_raw_recipe(fixture, provider_only_source, context.copy_targets)
        try:
            context.manifest = await _compose(
                context, revision=provider_only_source[-1].approved_revision, source_manifest=context.original
            )
            await fixture.connection.execute(
                f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
            )
            await fixture.connection.execute(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
            )
            yield context
        finally:
            for request_id in context.request_ids:
                await _remove_candidates(fixture, context.copy_targets, request_id)


async def _read(context, *, manifest=None, network_ids=None, office=False, **page):
    fixture = context.fixture
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        with network_address_read_scope(manifest or context.manifest, network_ids or (context.network_id,)) as scope:
            reader = read_network_provider_page if office else readers.read_network_provider_only_page
            return await reader(fixture.connection, scope, control_schema=fixture.control_schema, **page)


async def test_explicit_provider_evidence_is_distinct_before_pagination(provider_only_db):
    context = provider_only_db
    page = await _read(context, limit=1)
    assert set(page) == {"membership_level", "generation_id", "total_count", "limit", "offset", "has_more", "providers"}
    assert page["membership_level"] == "provider" and page["total_count"] == 2 and page["has_more"] is True
    provider = page["providers"][0]
    assert set(provider) == {"provider_system", "provider_id", "display_name", "npi", "network_memberships"}
    assert (
        provider["provider_id"] == "1000000004" and provider["display_name"] is None and provider["npi"] == 1000000004
    )
    assert provider["network_memberships"][0]["network_id"] == context.network_id
    second = await _read(context, limit=1, offset=1)
    assert second["total_count"] == 2 and second["has_more"] is False
    assert second["providers"][0]["provider_id"] == "1000000491"
    evidence = second["providers"][0]["network_memberships"][0]["evidence_ids"]
    assert len(evidence) == 2 and evidence == sorted(set(evidence))
    bases = await context.fixture.connection.fetch(
        f'''SELECT jsonb_set(fact_json->'evidence_basis','{{6}}',to_jsonb($1::integer))::text AS basis
        FROM "{context.manifest.schema_name}".network_source_contribution
        WHERE fact_json->>'provider_id'='1000000491' AND fact_json->'location_id'='null'::jsonb''',
        context.network_id,
    )
    assert evidence == sorted(hashlib.sha256(row["basis"].encode()).hexdigest() for row in bases)
    assert (await _read(context, offset=2))["providers"] == []
    offices = await _read(context, office=True)
    assert offices["total_count"] == 1 and offices["providers"][0]["provider_id"] == "1000000491"
    assert "membership_level" not in offices
    assert "location" not in json.dumps(page) and "location" not in json.dumps(second)


async def _assert_inherited_fact_leak(connection, current, query, read_relations, network_id):
    inherited_query, arguments = query
    inherited = await connection.fetchrow(
        inherited_query.replace("FROM ONLY ", "FROM ").replace("JOIN ONLY ", "JOIN "),
        *arguments,
        [network_id],
        50,
        0,
        readers.MAX_PROVIDER_EVIDENCE,
        readers.MAX_RESPONSE_BYTES,
        current.generation_id,
    )
    await readers.verify_consumed_network_relations(
        connection, tuple(read_relations), expected_backend_pid=connection.get_server_pid()
    )
    assert "1234567893" in inherited["result"] and inherited["valid"] is True


async def test_provider_only_read_excludes_fact_child_attached_after_metadata(provider_only_db, monkeypatch):
    context = provider_only_db
    original_query = readers._page_query
    parent_pins = []

    async def observe_query(connection, current, control_schema, *, read_relations):
        query = await original_query(connection, current, control_schema, read_relations=read_relations)
        parent_pins.extend(pin for pin in read_relations if pin.table_name == "network_source_contribution")
        return query

    monkeypatch.setattr(readers, "_page_query", observe_query)
    baseline = await _read(context)
    assert baseline["total_count"] == 2 and len(parent_pins) == 1
    pin = parent_pins[0]
    parent = f'"{pin.schema_name}"."{pin.table_name}"'
    child = f'"{context.fixture.source_schema}".provider_only_fact_child'
    url = context.fixture.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    admin = await asyncpg.connect(url)
    has_created_child = False
    try:
        await admin.execute("SET lock_timeout='250ms'")
        await admin.execute(f"CREATE TABLE {child} (LIKE {parent} INCLUDING CONSTRAINTS)")
        has_created_child = True
        await admin.execute(
            f"INSERT INTO {child} SELECT recipe_sha256,ordinal,"
            "jsonb_set(fact_json,'{provider_id}','\"1234567893\"'::jsonb) "
            f"FROM ONLY {parent} WHERE fact_json->'location_id'='null'::jsonb "
            "AND fact_json->'omission_reason'='null'::jsonb LIMIT 1"
        )
        attached_relations = []

        async def attach_after_metadata(connection, current, control_schema, *, read_relations):
            query = await original_query(connection, current, control_schema, read_relations=read_relations)
            assert pin in read_relations
            await admin.execute(f"ALTER TABLE {child} INHERIT {parent}")
            attached_relations.append(child)
            await _assert_inherited_fact_leak(connection, current, query, read_relations, context.network_id)
            return query

        monkeypatch.setattr(readers, "_page_query", attach_after_metadata)
        assert await _read(context) == baseline
        assert attached_relations == [child]
        assert await admin.fetchval(f"SELECT count(*) FROM {parent} WHERE fact_json->>'provider_id'='1234567893'") == 1
        assert (
            await admin.fetchval(f"SELECT count(*) FROM ONLY {parent} WHERE fact_json->>'provider_id'='1234567893'")
            == 0
        )
    finally:
        if has_created_child:
            await admin.execute(f"DROP TABLE {child}")
        await admin.close()


async def test_provider_evidence_uses_retained_revision_after_approved_rebind(provider_only_db):
    context = provider_only_db
    source_fixture, actor, networks, binding, _, _ = context.source
    connection, schema, _, _ = source_fixture
    before = await _read(context)
    changed = await _review_source_operation(connection, schema, actor, binding, "rebind", networks)
    approved = await _approve(connection, schema, await _command(connection, schema, changed), actor)
    current = await _compose(context, revision=approved["approved_revision"], source_manifest=context.manifest)
    assert await _read(context) == before
    rebound = await _read(context, manifest=current, network_ids=(context.other_network_id,))
    assert rebound["total_count"] == 2 and rebound["generation_id"] == current.generation_id
    assert all(
        provider["network_memberships"][0]["network_id"] == context.other_network_id
        for provider in rebound["providers"]
    )
    assert (
        rebound["providers"][0]["network_memberships"][0]["evidence_ids"]
        != before["providers"][0]["network_memberships"][0]["evidence_ids"]
    )


async def test_unavailable_sources_uncovered_networks_and_native_bounds_fail_closed(provider_only_db, monkeypatch):
    context = provider_only_db
    for selection in ((context.other_network_id,), tuple(sorted((context.network_id, context.other_network_id)))):
        with pytest.raises(NetworkProviderReadUnavailable):
            await _read(context, network_ids=selection)
    with pytest.raises(NetworkProviderReadUnavailable):
        await _read(context, manifest=context.original)
    with monkeypatch.context() as bounds:
        bounds.setattr(readers, "MAX_PROVIDER_EVIDENCE", 1)
        with pytest.raises(NetworkProviderReadUnavailable):
            await _read(context)
    with monkeypatch.context() as bounds:
        bounds.setattr(readers, "MAX_RESPONSE_BYTES", 2048)
        with pytest.raises(NetworkProviderReadUnavailable):
            await _read(context)
    with pytest.raises(NetworkProviderReadUnavailable):
        await _read(context, manifest=replace(context.manifest, manifest_sha256="f" * 64))


async def test_transaction_and_network_count_boundary_precede_source_reads(provider_only_db):
    context = provider_only_db
    with network_address_read_scope(context.manifest, (context.network_id,)) as scope:
        with pytest.raises(NetworkProviderReadError, match="caller-owned"):
            await readers.read_network_provider_only_page(context.fixture.connection, scope)
    with network_address_read_scope(context.manifest, tuple(range(1, 52))) as scope:
        with pytest.raises(NetworkProviderReadError, match="network bound"):
            await readers.read_network_provider_only_page(context.fixture.connection, scope)


async def test_authenticated_provider_manifest_refuses_changed_approved_fingerprint(provider_only_db, monkeypatch):
    context = provider_only_db
    original_pin = readers.pin_api_retained_approved_membership_source

    async def changed_pin(*arguments, **options):
        approved, relation = await original_pin(*arguments, **options)
        assert approved.generation_id == context.manifest.source_generations["custom_membership"]
        return replace(approved, generation_id="f" * 64), relation

    async def forbidden_bindings(*_arguments, **_options):
        raise AssertionError("Changed approved fingerprint must be refused before source binding reads")

    monkeypatch.setattr(readers, "pin_api_retained_approved_membership_source", changed_pin)
    monkeypatch.setattr(readers, "require_retained_approved_network_source_bindings", forbidden_bindings)
    with pytest.raises(NetworkProviderReadUnavailable, match="approved mapping"):
        await _read(context)


async def test_provider_only_decoder_rejects_shape_and_evidence_corruption(monkeypatch):
    membership_by_field = {"network_id": 1, "evidence_ids": ["a" * 64]}
    provider_by_field = {
        "provider_system": "npi",
        "provider_id": "1000000004",
        "display_name": None,
        "npi": 1000000004,
        "network_memberships": [membership_by_field],
    }
    page_by_field = {
        "membership_level": "provider",
        "generation_id": 1,
        "total_count": 1,
        "limit": 50,
        "offset": 0,
        "has_more": False,
        "providers": [provider_by_field],
    }
    scope = SimpleNamespace(manifest=SimpleNamespace(generation_id=1), network_ids=(1,))
    receipt_by_name = {"valid": True, "bounded": True, "result": json.dumps(page_by_field)}
    assert readers._result(receipt_by_name, scope, 50, 0) == page_by_field
    full_evidence_ids = [f"{number:064x}" for number in range(1000)]
    full_page = page_by_field | {
        "providers": [
            provider_by_field | {"network_memberships": [membership_by_field | {"evidence_ids": full_evidence_ids}]}
        ]
    }
    assert readers._result(receipt_by_name | {"result": json.dumps(full_page)}, scope, 50, 0) == full_page
    invalid_providers = [
        provider_by_field | {"locations": []},
        provider_by_field | {"npi": True},
        provider_by_field | {"provider_id": "invalid"},
    ]
    for evidence_ids in (["A" * 64], ["a" * 64, "a" * 64], ["b" * 64, "a" * 64], full_evidence_ids + ["f" * 64], []):
        invalid_providers.append(
            provider_by_field | {"network_memberships": [membership_by_field | {"evidence_ids": evidence_ids}]}
        )
    invalid_pages = [
        page_by_field | {"extra": True},
        page_by_field | {"generation_id": True},
        page_by_field | {"total_count": True},
    ]
    invalid_pages.extend(page_by_field | {"providers": [invalid_provider]} for invalid_provider in invalid_providers)
    for invalid_page in invalid_pages:
        with pytest.raises(ValueError):
            readers._result(receipt_by_name | {"result": json.dumps(invalid_page)}, scope, 50, 0)

    def forbidden_decode(_document):
        raise AssertionError("Oversized response must be rejected before decoding")

    monkeypatch.setattr(readers.json, "loads", forbidden_decode)
    with pytest.raises(ValueError):
        readers._result(receipt_by_name | {"result": "x" * (readers.MAX_RESPONSE_BYTES + 1)}, scope, 50, 0)
