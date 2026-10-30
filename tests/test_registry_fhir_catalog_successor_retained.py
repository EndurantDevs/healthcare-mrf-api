# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained catalog successor wiring; metadata doubles establish no native custody."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

from process import registry_fhir_catalog_retained as retained
from process.network_fhir_membership_source import with_fhir_successor
from process.network_fhir_retained_capture_contract import (
    SEMANTIC_PROFILE,
    RetainedFHIRCaptureError,
    canonical,
    capture_digest,
)
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from process.registry_source_recipe_store import canonical_registry_source_recipes
from tests.test_network_fhir_retained_capture import pin
from tests.test_network_fhir_retained_capture import retained as capture_fixture
from tests.test_registry_fhir_catalog_retained import manifest_fixture, recipe_fixture

pytestmark = pytest.mark.asyncio
_DATABASE_OID = 104


def _semantic_capture(edition):
    capture = capture_fixture()
    counts = tuple((name, count * edition) for name, count in capture.origin.resource_counts)
    origin = replace(
        capture.origin,
        dataset_id=f"dataset-{edition}",
        dataset_sha256=f"{edition:064x}",
        root_run_id=f"run-{edition}",
        header_oid=capture.origin.header_oid + 100 * edition,
        resource_parent_oid=capture.origin.resource_parent_oid + 100 * edition,
        resource_leaf_oid=capture.origin.resource_leaf_oid + 100 * edition,
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
        schema_oid=capture.schema_oid + 100 * edition,
        relation_oids=tuple((name, oid + 100 * edition) for name, oid in capture.relation_oids),
        catalog_sha256=f"{edition + 30:064x}",
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
    descriptor = retained._descriptor(recipe, None, _DATABASE_OID, "https://example.test/fhir")
    oids_by_name = dict(capture.relation_oids)
    expected_by_field = {
        "coordinates": asdict(recipe.binding_coordinates),
        "profile": "retained_normalized",
        "endpoint_id": recipe.source_pin.endpoint_id,
        "dataset_sha256": recipe.source_pin.dataset_sha256,
        "resource_table_oid": oids_by_name["provider_directory_dataset_resource"],
        "resource_leaf_oids": [oids_by_name["provider_directory_dataset_resource"]],
        "header_table_oid": oids_by_name["provider_directory_endpoint_dataset"],
        "database_oid": _DATABASE_OID,
        "source_uri": "https://example.test/fhir/",
        "accepted_receipt_id": capture.release_id,
        "accepted_receipt_sha256": capture.capture_sha256,
        "complete": True,
        "previous_dataset_id": None,
        "source_aliases": [recipe.source_pin.source_id],
        "native_read_profile": {
            "schema_name": capture.schema_name,
            "owner_role": capture.owner_role,
            "runtime_roles": list(capture.runtime_roles),
            "catalog_schema_name": capture.schema_name,
        },
        "native_catalog_sha256": capture.catalog_sha256,
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
    before = retained._descriptor(original_recipe, None, _DATABASE_OID, None)
    after = retained._descriptor(recipe, None, _DATABASE_OID, None)
    assert (
        canonical({key: value for key, value in before.items() if key != "retained_recipe"}).encode()
        == canonical({key: value for key, value in after.items() if key != "retained_recipe"}).encode()
    )
    assert fhir_catalog_source_generation(after) != fhir_catalog_source_generation(before)
    assert retained.retained_fhir_catalog_recipe(after) == recipe
    physical = retained.retained_fhir_catalog_read_source(after)
    assert physical == recipe.source_pin.retained_successor
    assert physical.read_schema_name != original_source.read_schema_name
    assert physical.read_resource_table_oid != after["resource_table_oid"]
    assert physical.dataset_id != after["coordinates"]["dataset_id"]
    assert physical.retained_capture.origin.resource_counts != original_source.retained_capture.origin.resource_counts
    assert after["capture_sha256"] == original_source.retained_capture.capture_sha256


@pytest.mark.parametrize(
    "field",
    [
        "coordinates",
        "dataset_sha256",
        "resource_table_oid",
        "resource_leaf_oids",
        "header_table_oid",
        "native_read_profile",
        "native_catalog_sha256",
        "accepted_receipt_id",
        "accepted_receipt_sha256",
        "capture_sha256",
    ],
)
async def test_physical_fields_cannot_replace_the_original_descriptor_anchor(field):
    recipe = successor_recipe()
    descriptor = retained._descriptor(recipe, None, _DATABASE_OID, None)
    physical_recipe = recipe_fixture(recipe.source_pin.retained_successor.retained_capture)
    physical_descriptor = retained._descriptor(physical_recipe, None, _DATABASE_OID, None)
    assert descriptor[field] != physical_descriptor[field]
    descriptor[field] = deepcopy(physical_descriptor[field])
    with pytest.raises(ValueError):
        retained.retained_fhir_catalog_read_source(descriptor)


@pytest.mark.parametrize("published", [False, True])
async def test_metadata_authority_rechecks_original_and_physical_captures(monkeypatch, published):
    recipe = successor_recipe()
    manifest = manifest_fixture(recipe)
    locator = retained._manifest_document(manifest) if published else None
    descriptor = retained._descriptor(recipe, locator, _DATABASE_OID, None)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=_DATABASE_OID))
    resolver = AsyncMock(return_value=manifest)
    recipes = AsyncMock(return_value=(recipe,))
    guard = AsyncMock()
    monkeypatch.setattr(retained, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(retained, "resolve_retained_registry_source_recipes", recipes)
    monkeypatch.setattr(retained, "require_retained_fhir_capture", guard)
    if published:
        assert await retained.has_verified_retained_fhir_catalog_sources(
            connection, (descriptor,), control_schema="control_example"
        )
        resolver.assert_awaited_once_with(
            connection, generation_id=manifest.generation_id, control_schema="control_example"
        )
        recipes.assert_awaited_once_with(connection, manifest, control_schema="control_example")
    else:
        (disposition,) = await retained.resolve_granted_fhir_catalog_sources(connection, (recipe,))
        assert disposition.descriptor == descriptor
        resolver.assert_not_awaited()
        recipes.assert_not_awaited()
    assert guard.await_args_list == [
        call(connection, recipe.source_pin.retained_capture, verify_content=False),
        call(connection, recipe.source_pin.retained_successor.retained_capture, verify_content=False),
    ]


@pytest.mark.parametrize("published", [False, True])
@pytest.mark.parametrize("failure_at", ["original", "physical"])
async def test_metadata_custody_refusal_is_terminal(monkeypatch, published, failure_at):
    recipe = successor_recipe()
    manifest = manifest_fixture(recipe)
    descriptor = retained._descriptor(
        recipe, retained._manifest_document(manifest) if published else None, _DATABASE_OID, None
    )
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=_DATABASE_OID))
    refusal = RetainedFHIRCaptureError("synthetic metadata refusal")
    guard = AsyncMock(side_effect=[refusal] if failure_at == "original" else [None, refusal])
    monkeypatch.setattr(retained, "require_retained_fhir_capture", guard)
    monkeypatch.setattr(retained, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    monkeypatch.setattr(retained, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,)))
    with pytest.raises(RetainedFHIRCaptureError) as error:
        if published:
            await retained.has_verified_retained_fhir_catalog_sources(
                connection, (descriptor,), control_schema="control_example"
            )
        else:
            await retained.resolve_granted_fhir_catalog_sources(connection, (recipe,))
    assert error.value is refusal
    assert guard.await_count == (1 if failure_at == "original" else 2)
    assert guard.await_args.args[1] == (
        recipe.source_pin.retained_capture
        if failure_at == "original"
        else recipe.source_pin.retained_successor.retained_capture
    )


@pytest.mark.parametrize("damage", ["manifest", "recipe"])
async def test_published_successor_cannot_substitute_for_exact_manifest_authority(monkeypatch, damage):
    recipe = successor_recipe()
    manifest = manifest_fixture(recipe)
    descriptor = retained._descriptor(recipe, retained._manifest_document(manifest), _DATABASE_OID, None)
    selected = replace(manifest, manifest_sha256="f" * 64) if damage == "manifest" else manifest
    resolver = AsyncMock(return_value=selected)
    recipes = AsyncMock(return_value=() if damage == "recipe" else (recipe,))
    guard = AsyncMock()
    monkeypatch.setattr(retained, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(retained, "resolve_retained_registry_source_recipes", recipes)
    monkeypatch.setattr(retained, "require_retained_fhir_capture", guard)
    with pytest.raises(retained.FHIRCatalogRetainedError):
        await retained.has_verified_retained_fhir_catalog_sources(
            object(), (descriptor,), control_schema="control_example"
        )
    guard.assert_not_awaited()
    if damage == "manifest":
        recipes.assert_not_awaited()


@pytest.mark.parametrize("successor", [False, True])
async def test_pin_checks_metadata_before_pinning_only_the_physical_capture(monkeypatch, successor):
    recipe = successor_recipe()
    if not successor:
        recipe = replace(recipe, source_pin=replace(recipe.source_pin, retained_successor=None))
    descriptor = retained._descriptor(recipe, None, _DATABASE_OID, None)
    original = recipe.source_pin.retained_capture
    physical = recipe.source_pin.retained_successor.retained_capture if successor else original
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=_DATABASE_OID))
    events = []

    async def check(actual, capture, *, verify_content):
        assert actual is connection and verify_content is False
        events.append(capture)

    token = object()

    async def pin_read(actual, capture):
        assert actual is connection and capture == physical
        assert events == ([original, physical] if successor else [original])
        return token

    guard, pinner = AsyncMock(side_effect=check), AsyncMock(side_effect=pin_read)
    monkeypatch.setattr(retained, "require_retained_fhir_capture", guard)
    monkeypatch.setattr(retained, "pin_retained_fhir_read", pinner)
    assert await retained.pin_retained_fhir_catalog_source(connection, descriptor) is token
    connection.fetchval.assert_awaited_once()
    assert guard.await_count == (2 if successor else 1)
    pinner.assert_awaited_once_with(connection, physical)


@pytest.mark.parametrize("failure_at", ["database", "original", "physical", "pin"])
async def test_pin_refusal_never_falls_back_to_the_original_capture(monkeypatch, failure_at):
    recipe = successor_recipe()
    descriptor = retained._descriptor(recipe, None, _DATABASE_OID, None)
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=_DATABASE_OID + 1 if failure_at == "database" else _DATABASE_OID)
    )
    refusal = RetainedFHIRCaptureError("synthetic physical refusal")
    guard = AsyncMock(
        side_effect=[refusal] if failure_at == "original" else [None, refusal] if failure_at == "physical" else None
    )
    pinner = AsyncMock(side_effect=refusal if failure_at == "pin" else AssertionError("unexpected physical pin"))
    monkeypatch.setattr(retained, "require_retained_fhir_capture", guard)
    monkeypatch.setattr(retained, "pin_retained_fhir_read", pinner)
    with pytest.raises(ValueError) as error:
        await retained.pin_retained_fhir_catalog_source(connection, descriptor)
    if failure_at == "database":
        assert isinstance(error.value, retained.FHIRCatalogRetainedError)
        guard.assert_not_awaited()
    else:
        assert error.value is refusal
        assert guard.await_count == (1 if failure_at == "original" else 2)
    if failure_at == "pin":
        pinner.assert_awaited_once_with(connection, recipe.source_pin.retained_successor.retained_capture)
    else:
        pinner.assert_not_awaited()
