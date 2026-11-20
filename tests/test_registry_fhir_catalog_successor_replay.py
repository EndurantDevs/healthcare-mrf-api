# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic replay checks do not establish native reader custody or serving."""

import hashlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_retained_capture as captures
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
    """Replay plans and references from one validated named dataset without descriptor changes."""
    recipe = successor_recipe()
    descriptor = retained._descriptor(recipe, None, "https://example.test/fhir")
    before = deepcopy(descriptor)
    physical = retained.retained_fhir_catalog_read_source(descriptor)
    guard = AsyncMock()
    monkeypatch.setattr(captures, "require_retained_fhir_capture", guard)
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
                resource_relation=physical.read_resource_relation,
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
                resource_relation=physical.read_resource_relation,
                workspace=workspace,
            ),
        )
    ]
    generation = fhir_catalog_source_generation(descriptor)
    assert {observation_record[0] for call in observations.await_args_list for observation_record in call.args[0]} == {
        generation
    }
    guard.assert_awaited_once_with(connection, physical.retained_capture)
    assert descriptor == before and descriptor["coordinates"]["dataset_id"] != physical.dataset_id


@pytest.mark.asyncio
async def test_published_successor_replaces_only_matching_bootstrap_anchor(monkeypatch):
    recipe = successor_recipe()
    original = retained._descriptor(recipe_fixture(recipe.source_pin.retained_capture), None, None)
    published = retained._descriptor(recipe, None, None)
    unrelated = retained._descriptor(recipe_fixture(), None, None)
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
        descriptor=retained._descriptor(recipe, None, None), endpoint_id=recipe.source_pin.endpoint_id
    )
    published = SimpleNamespace(
        descriptor=retained._descriptor(published_recipe, None, None), endpoint_id=recipe.source_pin.endpoint_id
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
