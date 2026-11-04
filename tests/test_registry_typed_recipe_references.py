# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact worker references preserve the closed retained inventory and native hydration boundary."""

import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process.network_fhir_typed_recipe import TypedFHIRRecipeReference
from process.registry_publication_worker import MAX_CONFIG_BYTES, parse_registry_publication_worker_config
from process.registry_source_recipe_additions import (
    RegistryTypedSourceRecipeReplacement,
    decode_source_recipe_additions,
    resolve_source_recipe_additions,
)
from tests.test_registry_source_recipe_additions import _configured_additions
from tests.test_registry_source_witness_runtime import _document
from tests.test_registry_typed_source_recipe import _recipe


def _source_recipe(index):
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_source_witness_contract import digest
    from process.network_fhir_typed_capture import PREFIX, TypedGraphAnchor
    from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

    original = _recipe()
    catalog, capture = deepcopy(original.source_pin.catalog_descriptor), deepcopy(original.source_pin.capture)
    coordinates = catalog["coordinates"]
    old_dataset = coordinates["dataset_id"]
    coordinates.update(
        source_id=f"alias-a-{index}",
        dataset_id=f"dataset-{index}",
        edition_id=f"dataset-{index}",
        dataset_schema=f"source_example_{index}",
    )
    catalog["native_read_profile"]["schema_name"] = coordinates["dataset_schema"]
    catalog["source_aliases"] = [f"alias-a-{index}", f"alias-b-{index}"]
    catalog["endpoint_id"] = f"endpoint-{index}"
    catalog["accepted_receipt_id"] = (
        catalog["accepted_receipt_id"].removesuffix(old_dataset) + coordinates["dataset_id"]
    )
    origin = capture["graph"]["origin"]
    origin.update(
        schema_name=coordinates["dataset_schema"],
        source_id=coordinates["source_id"],
        dataset_id=coordinates["dataset_id"],
        endpoint_id=catalog["endpoint_id"],
        source_aliases=catalog["source_aliases"],
    )
    anchor = TypedGraphAnchor(str(UUID(int=index)), capture["observed_at"])
    capture.update(
        capture_id=anchor.capture_id,
        schema_name=PREFIX + UUID(anchor.capture_id).hex,
        owner_role="capture_owner",
        runtime_roles=_document()["source_witness"]["runtime_roles"],
    )
    if "catalog_descriptor" in capture:
        capture["catalog_descriptor"] = catalog
    capture["capture_sha256"] = digest({key: value for key, value in capture.items() if key != "capture_sha256"})
    return RegistrySourceMembershipRecipe(
        PinnedTypedFHIRMembershipSource(capture, anchor, catalog), RegistryNetworkSourceCoordinates(**coordinates)
    )


def _configuration():
    configured = _document()
    _old_config, additions = _configured_additions()
    sources = [_source_recipe(index) for index in range(1, 15)]
    additions["recipes"] = [
        {
            "source_kind": "fhir_typed_reference",
            "capture_id": recipe.source_pin.anchor.capture_id,
            "observed_at": recipe.source_pin.anchor.observed_at,
            "capture_sha256": recipe.source_pin.generation_id,
            "catalog_generation": recipe.source_pin.catalog_generation,
        }
        for recipe in sources
    ]
    configured["initial_address_sources"]["source_recipe_additions"] = additions
    configured["initial_address_sources"]["base_source"]["table_name"] = "entity_address_unified"
    original_profile = configured["source_witness"]["source_profiles"][0]
    configured["source_witness"]["source_profiles"] = [
        {**original_profile, "schema_name": "source_example_" + str(index)} for index in range(1, 15)
    ]
    return configured


def test_complete_fourteen_source_worker_configuration_fits_existing_wire_cap():
    configured = _configuration()
    encoded = json.dumps(configured, separators=(",", ":"), ensure_ascii=False)
    parsed = parse_registry_publication_worker_config(encoded)
    references = parsed.initial_address_sources.source_recipe_additions.recipes
    assert len(references) == 14 and all(type(reference) is TypedFHIRRecipeReference for reference in references)
    assert len({reference.capture_id for reference in references}) == 14
    assert len(parsed.source_witness.source_profiles) == 14
    assert len(encoded.encode()) < MAX_CONFIG_BYTES == 16384
    assert "root_run_id" not in encoded and "resource_hashes" not in encoded
    assert (
        parsed.initial_address_sources.source_recipe_additions.inventory.as_dict()
        == configured["initial_address_sources"]["source_recipe_additions"]["inventory"]
    )


@pytest.mark.parametrize("change", ["unknown", "duplicate", "hash", "anchor", "full_origin", "kind", "too_many"])
def test_reference_codec_refuses_malformed_or_ambiguous_batches(change):
    configured = _configuration()
    documents = configured["initial_address_sources"]["source_recipe_additions"]["recipes"]
    if change == "duplicate":
        documents[1] = deepcopy(documents[0])
    elif change == "hash":
        documents[0]["capture_sha256"] = "not-a-hash"
    elif change == "anchor":
        documents[0]["observed_at"] = "today"
    elif change == "kind":
        documents[0]["source_kind"] = "fhir"
    elif change == "too_many":
        documents *= 8
    else:
        documents[0]["raw_origin" if change == "full_origin" else "extra"] = {}
    with pytest.raises(ValueError):
        parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.asyncio
async def test_reference_resolves_after_exact_predecessor_and_keeps_complete_vector(monkeypatch):
    from process import registry_source_recipe_additions as additions_module
    from tests.test_registry_source_inventory_consumers import _inventory

    configured = _configuration()
    document = configured["initial_address_sources"]["source_recipe_additions"]
    manifest = _inventory().manifest
    document["manifest_sha256"] = manifest.manifest_sha256
    additions = decode_source_recipe_additions(document)
    retained = _inventory().selected_recipes
    new_recipes = tuple(_source_recipe(index) for index in range(1, 15))
    monkeypatch.setattr(additions_module, "resolve_retained_registry_source_recipes", AsyncMock(return_value=retained))
    hydrate = AsyncMock(side_effect=new_recipes)
    monkeypatch.setattr(TypedFHIRRecipeReference, "resolve", hydrate)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(additions.inventory.as_dict())))
    runtime = object()
    actual = await resolve_source_recipe_additions(
        connection, additions, manifest, 7, control_schema="registry", source_witness_runtime=runtime
    )
    assert len(actual) == len(retained) + 14
    assert set(recipe.source_pin.generation_id for recipe in retained) <= set(
        recipe.source_pin.generation_id for recipe in actual
    )
    assert all(recipe in actual for recipe in new_recipes)
    assert hydrate.await_count == 14
    assert all(call.args == (connection, runtime) for call in hydrate.await_args_list)
    connection.fetchval.return_value = json.dumps({**additions.inventory.as_dict(), "table_name": "other"})
    hydrate.reset_mock()
    with pytest.raises(ValueError):
        await resolve_source_recipe_additions(
            connection, additions, manifest, 7, control_schema="registry", source_witness_runtime=runtime
        )
    hydrate.assert_not_awaited()


def _old_capture_recipe(typed, *, second=False):
    from process.network_fhir_retained_capture_contract import capture_digest
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
    from tests.test_network_fhir_retained_capture import pin, retained

    capture = retained()
    coordinates = typed.binding_coordinates
    origin = replace(
        capture.origin,
        schema_name=coordinates.dataset_schema,
        source_id=coordinates.source_id,
        endpoint_id=typed.source_pin.capture["graph"]["origin"]["endpoint_id"],
        dataset_id="old-dataset-two" if second else "old-dataset-one",
    )
    capture = replace(capture, origin=origin)
    capture = replace(capture, capture_sha256=capture_digest(capture))
    source = pin(capture)
    return RegistrySourceMembershipRecipe(
        source,
        replace(
            coordinates,
            dataset_id=source.dataset_id,
            producer_id="registry-retained-normalized-fhir-v1",
            edition_id=capture.capture_id,
        ),
    )


@pytest.mark.parametrize("operation", ["replace_typed_source_vector", "replace", None])
def test_closed_replacement_wire_fits_complete_fourteen_source_config(operation):
    configured = _configuration()
    document = configured["initial_address_sources"]["source_recipe_additions"]
    document["operation"] = operation
    encoded = json.dumps(configured, separators=(",", ":"))
    if operation != "replace_typed_source_vector":
        with pytest.raises(ValueError):
            parse_registry_publication_worker_config(encoded)
        return
    parsed = parse_registry_publication_worker_config(encoded)
    assert type(parsed.initial_address_sources.source_recipe_additions) is RegistryTypedSourceRecipeReplacement
    assert len(parsed.initial_address_sources.source_recipe_additions.recipes) == 14
    assert len(encoded.encode()) < MAX_CONFIG_BYTES
    document["recipes"] = _configured_additions()[1]["recipes"]
    with pytest.raises(ValueError):
        parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.asyncio
async def test_replacement_hydrates_complete_vector_preserves_non_fhir_and_removes_old_captures(monkeypatch):
    from process import registry_source_recipe_additions as additions_module
    from tests.test_registry_publication_worker import _configured_recipe
    from tests.test_registry_source_inventory_consumers import _inventory

    document = _configuration()["initial_address_sources"]["source_recipe_additions"]
    document["operation"] = "replace_typed_source_vector"
    manifest = _inventory().manifest
    document["manifest_sha256"] = manifest.manifest_sha256
    replacement = decode_source_recipe_additions(document)
    current = tuple(_source_recipe(index) for index in range(1, 15))
    old = (_old_capture_recipe(current[0]), _old_capture_recipe(current[0], second=True))
    unrelated = _configured_recipe("ptg")
    monkeypatch.setattr(
        additions_module, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(*old, unrelated))
    )
    hydrate = AsyncMock(side_effect=current)
    monkeypatch.setattr(TypedFHIRRecipeReference, "resolve", hydrate)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(replacement.inventory.as_dict())))
    actual = await resolve_source_recipe_additions(
        connection, replacement, manifest, 7, control_schema="registry", source_witness_runtime=object()
    )
    assert len(actual) == 15 and unrelated in actual
    assert all(recipe in actual for recipe in current) and all(recipe not in actual for recipe in old)
    assert hydrate.await_count == 14
    connection.fetchval.return_value = json.dumps({**replacement.inventory.as_dict(), "table_name": "other"})
    hydrate.reset_mock()
    with pytest.raises(ValueError, match="inventory_reference_invalid"):
        await resolve_source_recipe_additions(connection, replacement, manifest, 7, control_schema="registry")
    hydrate.assert_not_awaited()


@pytest.mark.parametrize("damage", ["source", "schema", "endpoint", "dropped", "duplicate"])
def test_typed_successor_requires_one_exact_physical_source_and_preserves_old_slots(damage):
    from process.registry_fhir_serving_preservation import FHIRServingPreservationError, fhir_vector_succession

    typed = _source_recipe(1)
    old = _old_capture_recipe(typed)
    assert fhir_vector_succession((old,), (typed,)) == ((old, typed),)
    if damage == "dropped":
        current = ()
    elif damage == "duplicate":
        current = (typed, typed)
    else:
        source = old.source_pin
        capture = source.retained_capture
        from process.network_fhir_retained_capture_contract import capture_digest
        from tests.test_network_fhir_retained_capture import pin

        field = {"source": "source_id", "schema": "schema_name", "endpoint": "endpoint_id"}[damage]
        capture = replace(capture, origin=replace(capture.origin, **{field: "different"}))
        capture = replace(capture, capture_sha256=capture_digest(capture))
        source = pin(capture)
        old = replace(
            old,
            source_pin=source,
            binding_coordinates=replace(
                old.binding_coordinates, source_id=source.source_id, dataset_schema=source.schema_name
            ),
        )
        current = (typed,)
    with pytest.raises(FHIRServingPreservationError, match="vector_incomplete"):
        fhir_vector_succession((old,), current)
