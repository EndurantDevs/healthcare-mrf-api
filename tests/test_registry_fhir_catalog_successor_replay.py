# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic replay checks do not establish native reader custody or serving."""

import hashlib
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_grant as grants
from process import registry_fhir_catalog_products as products
from process import registry_fhir_catalog_projection_validation as projection
from process import registry_fhir_catalog_retained as retained
from process import registry_fhir_catalog_sync as sync
from process.network_fhir_membership_source import with_fhir_successor
from process.registry_fhir_catalog_extraction import RESOURCE_TYPES
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant
from tests.test_registry_fhir_catalog_retained import recipe_fixture
from tests.test_registry_fhir_catalog_successor_retained import successor_recipe


@pytest.mark.asyncio
async def test_plan_and_reference_replay_use_same_physical_dataset_and_pin(monkeypatch):
    """Replay plans and references from one pinned physical dataset without descriptor changes."""
    recipe = successor_recipe()
    descriptor = retained._descriptor(recipe, None, 104, "https://example.test/fhir")
    before = deepcopy(descriptor)
    physical = retained.retained_fhir_catalog_read_source(descriptor)
    source_read = SimpleNamespace(capture=physical.retained_capture, verify=AsyncMock())
    monkeypatch.setattr(sync, "pin_retained_fhir_catalog_source", AsyncMock(return_value=source_read))
    monkeypatch.setattr(sync, "_published_read", AsyncMock(return_value=None))
    pages, reference_calls = [], []

    async def iter_pages(connection, **options):
        pages.append((connection, options))
        yield _catalog_replay_documents()

    async def no_references():
        if False:
            yield ()

    @asynccontextmanager
    async def references(connection, **options):
        reference_calls.append((connection, options))
        yield SimpleNamespace(iter_batches=no_references, accounting=SimpleNamespace(source_rows=1))

    monkeypatch.setattr(sync, "iter_fhir_catalog_pages", iter_pages)
    monkeypatch.setattr(sync, "staged_fhir_catalog_references", references)
    connection, workspace, observations = object(), object(), AsyncMock()
    assert (
        await sync.replay_fhir_catalog_observations(connection, descriptor, observations, reference_workspace=workspace)
        == 3
    )
    assert pages == [
        (
            connection,
            dict(
                schema_name=physical.read_schema_name,
                dataset_id=physical.dataset_id,
                resource_types=("Organization", "InsurancePlan"),
                source_read=source_read,
            ),
        )
    ]
    assert reference_calls == [
        (
            connection,
            dict(
                schema_name=physical.read_schema_name,
                dataset_id=physical.dataset_id,
                source_uri=descriptor["source_uri"],
                source_read=source_read,
                workspace=workspace,
            ),
        )
    ]
    generation = fhir_catalog_source_generation(descriptor)
    assert {observation_record[0] for call in observations.await_args_list for observation_record in call.args[0]} == {
        generation
    }
    source_read.verify.assert_awaited_once_with(connection, physical.retained_capture)
    assert descriptor == before and descriptor["coordinates"]["dataset_id"] != physical.dataset_id


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "admitted_role", "no_owner_path", "no_schema_create"])
async def test_reader_requires_original_and_physical_profiles(monkeypatch, failure):
    descriptor = retained._descriptor(successor_recipe(), None, 104, None)
    identity_by_field = {"database_oid": 104, "recovering": False}
    role_by_field = {
        "rolcanlogin": True,
        **dict.fromkeys(
            ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls", "elevated_membership"), False
        ),
    }
    observed_profiles = []

    async def permissions(sql, encoded):
        profiles = json.loads(encoded)
        observed_profiles.extend(profiles)
        rows = [dict(admitted_role=True, no_owner_path=True, no_schema_create=True) for _ in profiles]
        if failure:
            rows[-1][failure] = False
        return rows

    reader = SimpleNamespace(fetchrow=AsyncMock(side_effect=[identity_by_field, role_by_field]), fetch=permissions)
    writer = SimpleNamespace(fetchrow=AsyncMock(return_value=identity_by_field))
    if failure:
        with pytest.raises(grants.FHIRCatalogGrantError, match="reader_role_unsafe"):
            await grants._reader_identity(reader, writer, (descriptor,))
    else:
        await grants._reader_identity(reader, writer, (descriptor,))
    physical = retained.retained_fhir_catalog_read_source(descriptor).retained_capture
    assert {profile["schema_name"] for profile in observed_profiles} == {
        descriptor["native_read_profile"]["schema_name"],
        physical.schema_name,
    }
    assert len(observed_profiles) == 2


@pytest.mark.asyncio
async def test_reader_without_successor_checks_one_deduplicated_profile():
    descriptor = retained._descriptor(recipe_fixture(), None, 104, None)
    identity_by_field = {"database_oid": 104, "recovering": False}
    role_by_field = {
        "rolcanlogin": True,
        **dict.fromkeys(
            ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls", "elevated_membership"), False
        ),
    }
    reader = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=[identity_by_field, role_by_field]),
        fetch=AsyncMock(return_value=[dict(admitted_role=True, no_owner_path=True, no_schema_create=True)]),
    )
    writer = SimpleNamespace(fetchrow=AsyncMock(return_value=identity_by_field))
    await grants._reader_identity(reader, writer, (descriptor, descriptor))
    assert json.loads(reader.fetch.await_args.args[1]) == [descriptor["native_read_profile"]]


@pytest.mark.asyncio
async def test_published_successor_replaces_only_matching_bootstrap_anchor(monkeypatch):
    recipe = successor_recipe()
    original = retained._descriptor(recipe_fixture(recipe.source_pin.retained_capture), None, 104, None)
    published = retained._descriptor(recipe, None, 104, None)
    unrelated = retained._descriptor(recipe_fixture(), None, 104, None)
    explicit_sources = tuple(SimpleNamespace(descriptor=descriptor) for descriptor in (original, unrelated))
    published_source = SimpleNamespace(descriptor=published)
    monkeypatch.setattr(sync, "resolve_fhir_catalog_retained_sources", AsyncMock(return_value=(published_source,)))
    monkeypatch.setattr(sync, "resolve_granted_fhir_catalog_sources", AsyncMock(return_value=explicit_sources))
    grant = SimpleNamespace(
        retained_source_uris=(),
        retained_source_recipes=True,
        retained_sources=(recipe,),
        control_schema="registry_example",
    )
    selected = await sync._retained_catalog_sources(object(), grant, object())
    assert {fhir_catalog_source_generation(source.descriptor) for source in selected} == {
        fhir_catalog_source_generation(published),
        fhir_catalog_source_generation(unrelated),
    }
    assert original["coordinates"] == published["coordinates"]
    assert original["capture_sha256"] == published["capture_sha256"]


@pytest.mark.asyncio
@pytest.mark.parametrize("published_kind", ["original", "same_successor", "different_successor"])
async def test_published_recipe_cannot_silently_replace_explicit_successor(monkeypatch, published_kind):
    recipe = successor_recipe()
    original_source = replace(recipe.source_pin, retained_successor=None)
    published_recipe = recipe
    if published_kind == "original":
        published_recipe = replace(recipe, source_pin=original_source)
    elif published_kind == "different_successor":
        published_recipe = replace(
            recipe, source_pin=with_fhir_successor(original_source, successor_recipe().source_pin.retained_successor)
        )
    explicit = SimpleNamespace(
        descriptor=retained._descriptor(recipe, None, 104, None), endpoint_id=recipe.source_pin.endpoint_id
    )
    published = SimpleNamespace(
        descriptor=retained._descriptor(published_recipe, None, 104, None), endpoint_id=recipe.source_pin.endpoint_id
    )
    monkeypatch.setattr(sync, "resolve_fhir_catalog_retained_sources", AsyncMock(return_value=(published,)))
    monkeypatch.setattr(sync, "resolve_granted_fhir_catalog_sources", AsyncMock(return_value=(explicit,)))
    grant = SimpleNamespace(
        retained_source_uris=(),
        retained_source_recipes=True,
        retained_sources=(recipe,),
        control_schema="registry_example",
    )
    selected = await sync._retained_catalog_sources(object(), grant, object())
    assert fhir_catalog_source_generation(explicit.descriptor) in {
        fhir_catalog_source_generation(selected_source.descriptor) for selected_source in selected
    }
    if published_kind == "same_successor":
        assert len(selected) == 1
    else:
        assert len(selected) == 2
        with pytest.raises(ValueError, match="duplicate_source_profile"):
            sync._validate_catalog_sources(selected, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_count", [False, True])
async def test_product_verifier_selects_active_generation_and_counts_physical_rows(monkeypatch, wrong_count):
    recipe = successor_recipe()
    descriptor = retained._descriptor(recipe, None, 104, None)
    generation = fhir_catalog_source_generation(descriptor)
    descriptors_by_generation = {generation: descriptor}
    grant = standing_grant(retained_sources=(recipe,))
    selected = snapshot(grant, (descriptor,), status="last_good", network_generation=None, manifest_sha256=None)
    active = (generation,)
    resolver = AsyncMock(return_value=active)
    selector = AsyncMock(return_value=descriptors_by_generation)
    monkeypatch.setattr(sync, "resolve_fhir_catalog_projection_generations", resolver)
    monkeypatch.setattr(products, "selected_reviewed_fhir_catalog_product_sources", selector)
    monkeypatch.setattr(projection, "require_fhir_catalog_registry_reader", AsyncMock())
    monkeypatch.setattr(grants, "require_retained_fhir_catalog_sources", AsyncMock())
    physical = retained.retained_fhir_catalog_read_source(descriptor).retained_capture
    count = sum(
        resource_count for family, resource_count in physical.origin.resource_counts if family in RESOURCE_TYPES
    )
    original_count = sum(
        resource_count
        for family, resource_count in recipe.source_pin.retained_capture.origin.resource_counts
        if family in RESOURCE_TYPES
    )
    assert original_count != count
    monkeypatch.setattr(sync, "replay_fhir_catalog_observations", AsyncMock(return_value=count))
    monkeypatch.setattr(
        grants,
        "_observation_accounting",
        AsyncMock(return_value={"source_rows": original_count if wrong_count else count, "observation_rows": 0}),
    )
    monkeypatch.setattr(
        grants,
        "fhir_catalog_observation_digest",
        AsyncMock(return_value={"rows": 0, "sha256": hashlib.sha256().hexdigest()}),
    )
    connection, reader, workspace = object(), object(), object()
    if wrong_count:
        with pytest.raises(grants.FHIRCatalogGrantError, match="observation_facts_changed"):
            await grants._verify_retained_product_observations(
                connection, selected, grant, reader, workspace, descriptors_by_generation
            )
    else:
        await grants._verify_retained_product_observations(
            connection, selected, grant, reader, workspace, descriptors_by_generation
        )
    resolver.assert_awaited_once_with(reader, selected, grant=grant, descriptors=descriptors_by_generation)
    selector.assert_awaited_once_with(
        connection,
        observation_schema=selected["schema_name"],
        snapshot=selected,
        control_schema=grant.control_schema,
        source_generations=active,
    )


@pytest.mark.asyncio
async def test_product_verifier_checks_registry_reader_before_resolving_authority(monkeypatch):
    denied = AsyncMock(side_effect=ValueError("synthetic reader refusal"))
    resolver = AsyncMock()
    monkeypatch.setattr(projection, "require_fhir_catalog_registry_reader", denied)
    monkeypatch.setattr(sync, "resolve_fhir_catalog_projection_generations", resolver)
    connection, reader, selected, grant = object(), object(), object(), object()
    with pytest.raises(ValueError, match="synthetic reader refusal"):
        await grants._verify_retained_product_observations(connection, selected, grant, reader, object(), {})
    denied.assert_awaited_once_with(connection, reader, selected, grant)
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_product_writer_forwards_active_generation_selector(monkeypatch):
    active, seen = ("a" * 64,), []

    async def iterator(connection, **options):
        seen.append(options)
        if False:
            yield ()

    monkeypatch.setattr(products, "iter_reviewed_fhir_catalog_product_rows", iterator)
    monkeypatch.setattr(products, "clear_reviewed_fhir_catalog_product_rows", AsyncMock())
    monkeypatch.setattr(sync, "_approved_maps", AsyncMock(return_value=((), ())))
    grant = standing_grant()
    selected = snapshot(grant)
    result = await sync.project_reviewed_fhir_catalog_products(
        object(),
        SimpleNamespace(schema_name="catalog_example"),
        selected,
        grant=grant,
        loader_role="catalog_loader",
        source_generations=active,
    )
    assert result == {"binding": 0, "relationship": 0}
    assert len(seen) == 1 and seen[0]["source_generations"] == active


def _catalog_replay_documents():
    """Create fresh exact Organization and InsurancePlan rows for each replay."""
    return [
        {
            "resource_type": "Organization",
            "resource_id": "network-one",
            "payload_hash": "a" * 64,
            "payload_json": {"resource_id": "network-one", "name": "Network One"},
        },
        {
            "resource_type": "InsurancePlan",
            "resource_id": "plan-one",
            "payload_hash": "b" * 64,
            "payload_json": {"resource_id": "plan-one", "network_refs": ["Organization/network-one"]},
        },
    ]
