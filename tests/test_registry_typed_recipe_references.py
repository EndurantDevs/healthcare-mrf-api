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
from tests.test_registry_fhir_catalog_sources import read_profile
from tests.test_registry_publication_worker import configuration
from tests.test_registry_source_recipe_additions import _configured_additions
from tests.test_registry_typed_source_recipe import _recipe


def _document():
    configured = configuration()
    configured["initial_address_sources"] = {
        "base_source": {"schema_name": "address_source", "table_name": "address_rows", "generation_id": "one"},
        "compact": True,
    }
    configured["source_witness"] = {
        "source_profiles": [read_profile().document()],
        "capture_owner_role": "capture_owner",
        "runtime_roles": ["source_publisher", "source_reader"],
    }
    return configured


def _source_recipe(index):
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_source_witness_contract import digest
    from process.network_fhir_typed_capture import PREFIX, TypedGraphAnchor
    from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

    original = _recipe()
    catalog, capture = deepcopy(original.source_pin.catalog_descriptor), deepcopy(original.source_pin.capture)
    coordinates = catalog["coordinates"]
    coordinates.update(
        source_id=f"alias-a-{index}",
        dataset_id=f"dataset-{index}",
        edition_id=f"dataset-{index}",
        dataset_schema=f"source_example_{index}",
    )
    catalog["native_read_profile"]["schema_name"] = coordinates["dataset_schema"]
    catalog["source_aliases"] = [f"alias-a-{index}", f"alias-b-{index}"]
    catalog["endpoint_id"] = f"endpoint-{index}"
    from process.registry_fhir_catalog_observation import (
        CATALOG_OBSERVATION_CONTRACT,
        catalog_observation_descriptor_digest,
    )

    catalog["tables"]["resource"]["schema"] = coordinates["dataset_schema"]
    content = catalog_observation_descriptor_digest(catalog)
    catalog["accepted_receipt_id"] = CATALOG_OBSERVATION_CONTRACT + ":" + content
    catalog["accepted_receipt_sha256"] = content
    origin = capture["graph"]["origin"]
    origin.update(
        schema_name=coordinates["dataset_schema"],
        source_id=coordinates["source_id"],
        dataset_id=coordinates["dataset_id"],
        endpoint_id=catalog["endpoint_id"],
        source_aliases=catalog["source_aliases"],
        header_relation=catalog["tables"]["header"],
        resource_relation=catalog["tables"]["resource"],
    )
    anchor = TypedGraphAnchor(str(UUID(int=index)), capture["observed_at"])
    capture.update(
        capture_id=anchor.capture_id,
        schema_name=PREFIX + UUID(anchor.capture_id).hex,
    )
    capture["tables"] = {kind: {**table, "schema": capture["schema_name"]} for kind, table in capture["tables"].items()}
    if "catalog_descriptor" in capture:
        capture["catalog_descriptor"] = catalog
    capture["capture_sha256"] = digest(
        {key: source_pin for key, source_pin in capture.items() if key != "capture_sha256"}
    )
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


def _mutate_reference_documents(documents, change):
    if change == "duplicate":
        documents[1] = deepcopy(documents[0])
        return
    if change == "hash":
        documents[0]["capture_sha256"] = "not-a-hash"
        return
    if change == "anchor":
        documents[0]["observed_at"] = "today"
        return
    if change == "kind":
        documents[0]["source_kind"] = "fhir"
        return
    if change == "too_many":
        documents *= 8
        return
    else:
        documents[0]["raw_origin" if change == "full_origin" else "extra"] = {}


@pytest.mark.parametrize("change", ["unknown", "duplicate", "hash", "anchor", "full_origin", "kind", "too_many"])
def test_reference_codec_refuses_malformed_or_ambiguous_batches(change):
    configured = _configuration()
    documents = configured["initial_address_sources"]["source_recipe_additions"]["recipes"]
    _mutate_reference_documents(documents, change)
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
        header_relation={**capture.origin.header_relation, "schema": coordinates.dataset_schema},
        resource_relation={**capture.origin.resource_relation, "schema": coordinates.dataset_schema},
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
    current_recipes = tuple(_source_recipe(index) for index in range(1, 15))
    old = (_old_capture_recipe(current_recipes[0]), _old_capture_recipe(current_recipes[0], second=True))
    unrelated = _configured_recipe("ptg")
    monkeypatch.setattr(
        additions_module, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(*old, unrelated))
    )
    hydrate = AsyncMock(side_effect=current_recipes)
    monkeypatch.setattr(TypedFHIRRecipeReference, "resolve", hydrate)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(replacement.inventory.as_dict())))
    actual = await resolve_source_recipe_additions(
        connection, replacement, manifest, 7, control_schema="registry", source_witness_runtime=object()
    )
    assert len(actual) == 15 and unrelated in actual
    assert all(recipe in actual for recipe in current_recipes) and all(recipe not in actual for recipe in old)
    assert hydrate.await_count == 14
    connection.fetchval.return_value = json.dumps({**replacement.inventory.as_dict(), "table_name": "other"})
    hydrate.reset_mock()
    with pytest.raises(ValueError, match="inventory_reference_invalid"):
        await resolve_source_recipe_additions(connection, replacement, manifest, 7, control_schema="registry")
    hydrate.assert_not_awaited()


@pytest.mark.parametrize("damage", ["source", "schema", "endpoint", "dropped", "duplicate"])
def test_typed_successor_preserves_exact_logical_source_scope_and_old_slots(damage):
    from process.registry_fhir_serving_preservation import FHIRServingPreservationError, fhir_vector_succession

    typed = _source_recipe(1)
    old = _old_capture_recipe(typed)
    assert fhir_vector_succession((old,), (typed,)) == ((old, typed),)
    if damage == "dropped":
        current = ()
    elif damage == "duplicate":
        current = (typed, typed)
    else:
        source_pin = old.source_pin
        capture = source_pin.retained_capture
        from process.network_fhir_retained_capture_contract import capture_digest
        from tests.test_network_fhir_retained_capture import pin

        field = {"source": "source_id", "schema": "schema_name", "endpoint": "endpoint_id"}[damage]
        origin_changes_by_field = {field: "different"}
        if field == "schema_name":
            origin_changes_by_field.update(
                header_relation={**capture.origin.header_relation, "schema": "different"},
                resource_relation={**capture.origin.resource_relation, "schema": "different"},
            )
        capture = replace(capture, origin=replace(capture.origin, **origin_changes_by_field))
        capture = replace(capture, capture_sha256=capture_digest(capture))
        source_pin = pin(capture)
        old = replace(
            old,
            source_pin=source_pin,
            binding_coordinates=replace(
                old.binding_coordinates, source_id=source_pin.source_id, dataset_schema=source_pin.schema_name
            ),
        )
        current = (typed,)
    with pytest.raises(FHIRServingPreservationError, match="vector_incomplete"):
        fhir_vector_succession((old,), current)


def _ordinary_replacement_configuration():
    configured = _configuration()
    sources = configured["initial_address_sources"]
    sources["replacement_source_recipes"] = sources.pop("source_recipe_additions")["recipes"]
    return configured


def test_ordinary_replacement_wire_accepts_complete_vector_without_inventory():
    configured = _ordinary_replacement_configuration()
    encoded = json.dumps(configured, separators=(",", ":"))
    parsed = parse_registry_publication_worker_config(encoded)
    sources = parsed.initial_address_sources
    assert sources.source_recipe_additions is None
    assert len(sources.replacement_source_recipes) == 14
    assert all(type(recipe) is TypedFHIRRecipeReference for recipe in sources.replacement_source_recipes)
    assert len(encoded.encode()) < MAX_CONFIG_BYTES
    assert "inventory" not in configured["initial_address_sources"]


@pytest.mark.parametrize("change", ["unknown", "duplicate", "hash", "anchor", "full_origin", "kind", "too_many"])
def test_ordinary_replacement_reference_codec_preserves_closed_bounds(change):
    configured = _ordinary_replacement_configuration()
    _mutate_reference_documents(configured["initial_address_sources"]["replacement_source_recipes"], change)
    with pytest.raises(ValueError):
        parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.parametrize("phase", ["initial", "noncompact", "conflicting"])
def test_transport_references_require_compact_replacement_phase(phase):
    configured = _ordinary_replacement_configuration()
    sources = configured["initial_address_sources"]
    if phase == "initial":
        sources["initial_source_recipes"] = sources.pop("replacement_source_recipes")
    elif phase == "noncompact":
        sources["compact"] = False
    else:
        sources["source_recipe_additions"] = _configuration()["initial_address_sources"]["source_recipe_additions"]
    with pytest.raises(ValueError):
        parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.asyncio
async def test_ordinary_replacement_hydrates_all_references_before_full_validation(monkeypatch):
    from process.registry_composition_addresses import _resolve_source_recipe_additions

    sources = parse_registry_publication_worker_config(
        json.dumps(_ordinary_replacement_configuration())
    ).initial_address_sources
    current_recipes = tuple(_source_recipe(index) for index in range(1, 15))
    hydrate = AsyncMock(side_effect=current_recipes)
    monkeypatch.setattr(TypedFHIRRecipeReference, "resolve", hydrate)
    connection = object()
    resolved = await _resolve_source_recipe_additions(connection, sources, None, 1, "registry")
    assert resolved.source_recipe_additions is None
    assert len(resolved.replacement_source_recipes) == 14
    assert all(recipe in resolved.replacement_source_recipes for recipe in current_recipes)
    assert hydrate.await_count == 14
    assert all(call.args == (connection, sources.source_witness_runtime) for call in hydrate.await_args_list)
    hydrate.side_effect = [current_recipes[0]] * 14
    with pytest.raises(ValueError):
        await _resolve_source_recipe_additions(connection, sources, None, 1, "registry")


@pytest.mark.asyncio
async def test_ordinary_replacement_cannot_hydrate_without_installed_runtime():
    from process.registry_composition_addresses import _resolve_source_recipe_additions

    sources = parse_registry_publication_worker_config(
        json.dumps(_ordinary_replacement_configuration())
    ).initial_address_sources
    with pytest.raises(ValueError, match="runtime_unavailable"):
        await _resolve_source_recipe_additions(object(), sources, None, 1, "registry")
