# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained catalog successor wiring; metadata doubles establish no native custody."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_retained as retained
from process.network_fhir_membership_source import with_fhir_successor
from process.network_fhir_retained_capture_contract import (
    SEMANTIC_PROFILE,
    canonical,
    capture_digest,
)
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from process.registry_source_recipe_store import canonical_registry_source_recipes
from tests.test_network_fhir_retained_capture import pin
from tests.test_network_fhir_retained_capture import retained as capture_fixture
from tests.test_registry_fhir_catalog_retained import manifest_fixture, recipe_fixture

pytestmark = pytest.mark.asyncio


def _semantic_capture(edition):
    capture = capture_fixture()
    counts = tuple((name, count * edition) for name, count in capture.origin.resource_counts)
    origin = replace(
        capture.origin,
        dataset_id=f"dataset-{edition}",
        dataset_sha256=f"{edition:064x}",
        root_run_id=f"run-{edition}",
        resource_counts=counts,
        descriptor_counts=counts,
        admission_profile=SEMANTIC_PROFILE,
        original_admission_refusal=None,
        admission_metadata_sha256=f"{edition + 10:064x}",
        semantic_projection_as_of="2026-01-01",
    )
    capture = replace(
        capture,
        origin=origin,
        content_sha256=f"{edition + 20:064x}",
    )
    return replace(capture, capture_sha256=capture_digest(capture))


def successor_recipe():
    """Return valid synthetic original/physical captures under one original approval anchor."""
    recipe = recipe_fixture(_semantic_capture(1))
    return replace(recipe, source_pin=with_fhir_successor(recipe.source_pin, pin(_semantic_capture(2))))


@pytest.mark.parametrize("semantic", [False, True])
async def test_no_successor_preserves_the_original_descriptor_bytes(semantic):
    capture = _semantic_capture(1) if semantic else capture_fixture()
    recipe = recipe_fixture(capture)
    descriptor = retained._descriptor(recipe, None, "https://example.test/fhir")
    expected_by_field = {
        "coordinates": asdict(recipe.binding_coordinates),
        "profile": "retained_normalized",
        "endpoint_id": recipe.source_pin.endpoint_id,
        "dataset_sha256": recipe.source_pin.dataset_sha256,
        "tables": {
            "header": dict(capture.tables["provider_directory_endpoint_dataset"]),
            "resource": dict(capture.tables["provider_directory_dataset_resource"]),
        },
        "source_uri": "https://example.test/fhir/",
        "accepted_receipt_id": capture.release_id,
        "accepted_receipt_sha256": capture.capture_sha256,
        "complete": True,
        "previous_dataset_id": None,
        "source_aliases": [recipe.source_pin.source_id],
        "native_read_profile": {
            "schema_name": capture.schema_name,
            "catalog_schema_name": capture.schema_name,
        },
        "admission": None,
        "capture_sha256": capture.capture_sha256,
        "retained_recipe": json.loads(canonical_registry_source_recipes((recipe,))),
        "retained_manifest": None,
    }
    assert canonical(descriptor).encode() == canonical(expected_by_field).encode()
    assert "retained_successor" not in descriptor["retained_recipe"][0]["source_pin"]
    assert retained.retained_fhir_catalog_read_source(descriptor) == recipe.source_pin


async def test_successor_changes_generation_and_physical_read_without_changing_flat_anchor():
    recipe = successor_recipe()
    original_source = replace(recipe.source_pin, retained_successor=None)
    original_recipe = replace(recipe, source_pin=original_source)
    before = retained._descriptor(original_recipe, None, None)
    after = retained._descriptor(recipe, None, None)
    selected_fields = {"retained_recipe", "tables", "native_read_profile"}
    assert (
        canonical({key: value for key, value in before.items() if key not in selected_fields}).encode()
        == canonical({key: value for key, value in after.items() if key not in selected_fields}).encode()
    )
    assert fhir_catalog_source_generation(after) != fhir_catalog_source_generation(before)
    assert retained.retained_fhir_catalog_recipe(after) == recipe
    physical = retained.retained_fhir_catalog_read_source(after)
    assert physical == recipe.source_pin.retained_successor
    assert physical.read_schema_name != original_source.read_schema_name
    assert physical.read_resource_relation == after["tables"]["resource"]
    assert physical.read_resource_relation != original_source.read_resource_relation
    assert physical.dataset_id != after["coordinates"]["dataset_id"]
    assert physical.retained_capture.origin.resource_counts != original_source.retained_capture.origin.resource_counts
    assert after["capture_sha256"] == original_source.retained_capture.capture_sha256


@pytest.mark.parametrize(
    "field",
    [
        "coordinates",
        "dataset_sha256",
        "native_read_profile",
        "accepted_receipt_id",
        "accepted_receipt_sha256",
        "capture_sha256",
    ],
)
async def test_physical_fields_cannot_replace_the_original_descriptor_anchor(field):
    recipe = successor_recipe()
    descriptor = retained._descriptor(recipe, None, None)
    physical_recipe = recipe_fixture(recipe.source_pin.retained_successor.retained_capture)
    physical_descriptor = retained._descriptor(physical_recipe, None, None)
    if field == "native_read_profile":
        original_recipe = replace(recipe, source_pin=replace(recipe.source_pin, retained_successor=None))
        physical_descriptor = retained._descriptor(original_recipe, None, None)
    assert descriptor[field] != physical_descriptor[field]
    descriptor[field] = deepcopy(physical_descriptor[field])
    with pytest.raises(ValueError):
        retained.retained_fhir_catalog_read_source(descriptor)


@pytest.mark.parametrize("published", [False, True])
async def test_metadata_authority_rechecks_original_and_physical_captures(monkeypatch, published):
    recipe = successor_recipe()
    manifest = manifest_fixture(recipe)
    locator = retained._manifest_document(manifest) if published else None
    descriptor = retained._descriptor(recipe, locator, None)
    connection = SimpleNamespace(fetchval=AsyncMock())
    resolver = AsyncMock(return_value=manifest)
    recipes = AsyncMock(return_value=(recipe,))
    monkeypatch.setattr(retained, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(retained, "resolve_retained_registry_source_recipes", recipes)
    assert await retained.has_verified_retained_fhir_catalog_sources(
        connection, (descriptor,), control_schema="control_example"
    )
    if published:
        resolver.assert_awaited_once_with(
            connection, generation_id=manifest.generation_id, control_schema="control_example"
        )
        recipes.assert_awaited_once_with(connection, manifest, control_schema="control_example")
    else:
        (disposition,) = await retained.resolve_granted_fhir_catalog_sources(connection, (recipe,))
        assert disposition.descriptor == descriptor
        resolver.assert_not_awaited()
        recipes.assert_not_awaited()
    assert retained.retained_fhir_catalog_read_source(descriptor) == recipe.source_pin.retained_successor
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("damage", ["manifest", "recipe"])
async def test_published_successor_cannot_substitute_for_exact_manifest_authority(monkeypatch, damage):
    recipe = successor_recipe()
    manifest = manifest_fixture(recipe)
    descriptor = retained._descriptor(recipe, retained._manifest_document(manifest), None)
    selected = replace(manifest, manifest_sha256="f" * 64) if damage == "manifest" else manifest
    resolver = AsyncMock(return_value=selected)
    recipes = AsyncMock(return_value=() if damage == "recipe" else (recipe,))
    monkeypatch.setattr(retained, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(retained, "resolve_retained_registry_source_recipes", recipes)
    with pytest.raises(retained.FHIRCatalogRetainedError):
        await retained.has_verified_retained_fhir_catalog_sources(
            object(), (descriptor,), control_schema="control_example"
        )
    resolver.assert_awaited_once()
    if damage == "manifest":
        recipes.assert_not_awaited()
    else:
        recipes.assert_awaited_once()


@pytest.mark.parametrize("capture_field", ["retained_capture", "retained_successor"])
async def test_retained_capture_digest_refusal_precedes_manifest_lookup(monkeypatch, capture_field):
    recipe = successor_recipe()
    manifest = manifest_fixture(recipe)
    descriptor = retained._descriptor(recipe, retained._manifest_document(manifest), None)
    source = descriptor["retained_recipe"][0]["source_pin"]
    capture = (
        source["retained_capture"]
        if capture_field == "retained_capture"
        else source["retained_successor"]["retained_capture"]
    )
    capture["content_sha256"] = "f" * 64
    resolver, recipes = AsyncMock(), AsyncMock()
    monkeypatch.setattr(retained, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(retained, "resolve_retained_registry_source_recipes", recipes)
    with pytest.raises(ValueError):
        await retained.has_verified_retained_fhir_catalog_sources(
            object(), (descriptor,), control_schema="control_example"
        )
    resolver.assert_not_awaited()
    recipes.assert_not_awaited()
