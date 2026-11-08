# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete metadata pairing uses existing codecs; fixtures confer no SOURCE admission or custody."""

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from uuid import UUID

import pytest

from process import reference_family_archive as archive
from process import registry_source_inventory_contract as contract
from process import registry_source_inventory_preparation as preparation
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, fhir_physical_source
from process.network_legacy_membership_source import PinnedACAMembershipSource
from process.registry_source_contribution_store import fhir_fact_transform, recipe_contribution_identity
from process.registry_source_inventory_contract import (
    RegistrySourceInventoryError,
    encode_source_inventory_entry,
    make_source_inventory_entry,
    source_inventory_root,
)
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)
from tests.test_registry_aca_source_recipe import _DATASET, _metadata
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_contributions import _descriptor as _legacy_descriptor
from tests.test_registry_source_inventory_store_postgres import _capture_recipe
from tests.test_registry_source_office_summary_v3 import _derivation as _office_example
from tests.test_registry_source_recipe_store_postgres import _example


def _aca_recipe():
    metadata = _metadata()
    spec = archive.reference_family_spec("mrf", canonical=True)
    tables = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 0) for model in spec.model_types
    )
    manifest = archive.ReferenceFamilyManifest(
        "mrf",
        tables,
        metadata,
        archive._source_metadata(metadata)[1],
        archive._schema_digest(tables),
        {"plan-attributes": "c" * 64},
        archive._canonical_model_receipt(),
    )
    relations = tuple((name, index + 100) for index, name in enumerate(sorted(spec.table_names)))
    ownership = archive.ReferenceFamilyStageOwnership(
        "mrf", _DATASET, archive.reference_family_stage_schema(_DATASET), 12, relations
    )
    validation_by_field = {
        "contract": archive.VALIDATION_CONTRACT,
        "importer_id": "mrf",
        "package_id": "b" * 64,
        "profile_contract": archive.TYPED_MRF_CONTRACT,
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": 12,
        "relation_oids": [list(pair) for pair in relations],
        "sealed_owner_oid": 13,
        "manifest_sha256": hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest(),
        "tables": [table.as_dict() for table in tables],
    }
    validation = archive.validate_reference_family_validation_receipt(
        {**validation_by_field, "validation_sha256": archive._validation_digest(validation_by_field)}
    )
    source_pin = PinnedACAMembershipSource(
        archive.ReferenceFamilyPreparedSource(manifest, ownership),
        validation,
        **{**metadata["network_membership"], "runtime_roles": tuple(metadata["network_membership"]["runtime_roles"])},
    )
    return RegistrySourceMembershipRecipe(
        source_pin,
        RegistryNetworkSourceCoordinates(
            **{key: coordinate for key, coordinate in metadata["network_bindings"].items() if key != "source_key_kind"}
        ),
    )


def _descriptor(recipe, anchored=False):
    identity, capture = recipe_contribution_identity(recipe)
    descriptor = _legacy_descriptor(recipe)
    descriptor.update(
        recipe_sha256=identity,
        capture_sha256=capture,
        transform_version=fhir_fact_transform(recipe.source_pin),
        fact_candidate_id=str(UUID(int=7)) if anchored else None,
        fact_table_oid=71 if anchored else None,
        fact_seal_sha256="d" * 64 if anchored else None,
    )
    descriptor["source_generation"] = hashlib.sha256(
        json.dumps([capture, descriptor["mapping_sha256"]], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return descriptor


def _derivation(recipe, descriptor=None, *, facts=False):
    identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    derivation = _office_example(identity, mode="facts" if facts else "raw")
    physical = (
        fhir_physical_source(recipe.source_pin)
        if type(recipe.source_pin) is PinnedFHIRMembershipSource
        else recipe.source_pin
    )
    derivation["physical_source_sha256"] = physical.generation_id
    if descriptor is not None:
        derivation["mapping_sha256"] = descriptor["mapping_sha256"]
    if facts:
        derivation["fact_artifact"] = {
            field: descriptor[field]
            for field in (
                "fact_candidate_id",
                "fact_table_oid",
                "fact_seal_sha256",
                "capture_sha256",
                "transform_version",
                "fact_sha256",
                "fact_rows",
            )
        }
    return derivation


def _metadata_rows(recipes):
    contributions, derivations = [], []
    for index, recipe in enumerate(recipes):
        descriptor = (
            _descriptor(recipe, anchored=bool(index % 2))
            if type(recipe.source_pin) is PinnedFHIRMembershipSource
            else None
        )
        if descriptor is not None:
            contributions.append(descriptor)
        derivations.append(
            _derivation(
                recipe, descriptor, facts=descriptor is not None and descriptor["fact_candidate_id"] is not None
            )
        )
    return contributions, derivations


@pytest.mark.parametrize("count", [1, 101, 1000])
def test_complete_mixed_source_preparation_preserves_order_singletons_and_legacy_digest(count):
    recipes = (
        (_example(),)
        if count == 1
        else tuple(_example(index) for index in range(count - 3)) + (_aca_recipe(), _ptg_recipe(), _capture_recipe())
    )
    contributions, derivations = _metadata_rows(recipes)
    reference, entries = preparation.prepare_source_inventory(
        reversed(recipes), reversed(contributions), reversed(derivations)
    )
    assert reference.root.entry_count == reference.root.derivation_count == count
    assert reference.root.contribution_count == sum(
        type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in recipes
    )
    assert tuple(entry.ordinal for entry in entries) == tuple(range(1, count + 1))
    assert [entry.recipe_json for entry in entries] == sorted(entry.recipe_json for entry in entries)
    recipe_array = "[" + ",".join(entry.recipe_json for entry in entries) + "]"
    assert reference.root.recipes_sha256 == hashlib.sha256(recipe_array.encode()).hexdigest()
    if count <= 100:
        assert recipe_array == canonical_registry_source_recipes(recipes)
        assert reference.root.recipes_sha256 == registry_source_recipes_sha256(recipe_array)
    assert reference.root == source_inventory_root(iter(entries))


@pytest.mark.asyncio
async def test_builder_properties_and_metadata_sink_work_across_source_page_awaits():
    recipes = tuple(_example(index) for index in range(101))
    contributions, derivations = _metadata_rows(recipes)
    builder = preparation.SourceInventoryPreparation(reversed(recipes))
    assert builder.recipe_root.entry_count == 101
    assert builder.recipe_root.contribution_count == builder.recipe_root.derivation_count == 0
    assert builder.recipes == tuple(sorted(recipes, key=lambda recipe: canonical_registry_source_recipes((recipe,))))
    for descriptor in contributions:
        builder.add_contribution(descriptor)
    for derivation in reversed(derivations):
        await asyncio.sleep(0)
        builder.add_derivation(derivation)
    result = builder.finish()
    assert result == preparation.prepare_source_inventory(recipes, contributions, derivations)
    assert builder.finish() is result
    with pytest.raises(FrozenInstanceError):
        result[1][0].ordinal = 100
    with pytest.raises(AttributeError):
        builder.recipes = ()
    with pytest.raises(RegistrySourceInventoryError):
        builder.add_derivation(derivations[0])
    assert builder.finish() is result


def test_aca_recipe_property_cannot_mutate_the_internal_canonical_metadata():
    recipe = _aca_recipe()
    builder = preparation.SourceInventoryPreparation((recipe,))
    exposed = builder.recipes[0]
    exposed.source_pin.prepared.manifest.source_metadata["network_membership"]["source_id"] = "changed"
    builder.add_derivation(_derivation(recipe))
    reference, entries = builder.finish()
    assert entries[0].recipe_json == canonical_registry_source_recipes((recipe,))[1:-1]
    assert reference.root.contribution_count == 0 and reference.root.derivation_count == 1


def test_empty_complete_inventory_and_premature_finish_contract():
    reference, entries = preparation.prepare_source_inventory((), (), ())
    assert entries == () and reference.root == source_inventory_root(())
    recipe = _example()
    builder = preparation.SourceInventoryPreparation((recipe,))
    with pytest.raises(RegistrySourceInventoryError):
        builder.finish()
    with pytest.raises(RegistrySourceInventoryError):
        builder.add_contribution(_descriptor(recipe))
    with pytest.raises(RegistrySourceInventoryError):
        builder.finish()


@pytest.mark.parametrize(
    "damage",
    [
        "missing_contribution",
        "missing_derivation",
        "duplicate_contribution",
        "duplicate_derivation",
        "orphan_contribution",
        "orphan_derivation",
        "rev1",
        "partial_anchor",
        "nonfhir_contribution",
        "nonfhir_facts",
        "types",
    ],
)
def test_complete_preparation_rejects_missing_or_unpaired_metadata(damage):
    recipes = (_example(), _ptg_recipe())
    contributions, derivations = _metadata_rows(recipes)
    match damage:
        case "missing_contribution":
            contributions.clear()
        case "missing_derivation":
            derivations.pop()
        case "duplicate_contribution":
            contributions.append(deepcopy(contributions[0]))
        case "duplicate_derivation":
            derivations.append(deepcopy(derivations[0]))
        case "orphan_contribution":
            contributions.append(_descriptor(_example(99)))
        case "orphan_derivation":
            derivations.append(_derivation(_example(99), _descriptor(_example(99))))
        case "rev1":
            for field in ("fact_candidate_id", "fact_table_oid", "fact_seal_sha256"):
                contributions[0].pop(field)
        case "partial_anchor":
            contributions[0]["fact_table_oid"] = 71
        case "nonfhir_contribution":
            contributions.append({**contributions[0], "recipe_sha256": derivations[1]["recipe_sha256"]})
        case "nonfhir_facts":
            derivations[1] = {
                **_office_example(derivations[1]["recipe_sha256"]),
                "physical_source_sha256": recipes[1].source_pin.generation_id,
            }
        case _:
            contributions[0] = "untrusted-json-text"
    with pytest.raises(RegistrySourceInventoryError):
        preparation.prepare_source_inventory(recipes, contributions, derivations)


@pytest.mark.parametrize(
    "field",
    [
        "mapping_sha256",
        "physical_source_sha256",
        "fact_candidate_id",
        "fact_table_oid",
        "fact_seal_sha256",
        "capture_sha256",
        "transform_version",
        "fact_sha256",
        "fact_rows",
    ],
)
def test_fhir_fact_derivation_must_match_the_complete_descriptor(field):
    recipe = _capture_recipe()
    descriptor = _descriptor(recipe, anchored=True)
    derivation = _derivation(recipe, descriptor, facts=True)
    if field in ("mapping_sha256", "physical_source_sha256"):
        derivation[field] = "0" * 64
    else:
        artifact = derivation["fact_artifact"]
        artifact[field] = (
            artifact[field] + 1
            if type(artifact[field]) is int
            else str(UUID(int=99))
            if field == "fact_candidate_id"
            else "fhir-source-facts-v1"
            if field == "transform_version"
            else "0" * 64
        )
    with pytest.raises(RegistrySourceInventoryError):
        preparation.prepare_source_inventory((recipe,), (descriptor,), (derivation,))


def test_local_null_anchor_refuses_fact_derivation_and_poisons_builder():
    recipe = _example()
    descriptor = _descriptor(recipe)
    derivation = _derivation(recipe, _descriptor(recipe, anchored=True), facts=True)
    builder = preparation.SourceInventoryPreparation((recipe,))
    builder.add_contribution(descriptor)
    with pytest.raises(RegistrySourceInventoryError):
        builder.add_derivation(derivation)
    with pytest.raises(RegistrySourceInventoryError):
        builder.add_derivation(_derivation(recipe, descriptor))
    with pytest.raises(RegistrySourceInventoryError):
        builder.finish()


def test_recipe_uniqueness_matches_existing_pin_generation_codec():
    recipe = _example()
    duplicate_pin = replace(recipe, binding_coordinates=replace(recipe.binding_coordinates, edition_id="other"))
    for recipes in ((recipe, recipe), (recipe, duplicate_pin)):
        with pytest.raises(RegistrySourceInventoryError):
            preparation.SourceInventoryPreparation(recipes)


def test_count_and_entry_page_bounds_are_enforced_by_the_existing_contract(monkeypatch):
    with pytest.raises(RegistrySourceInventoryError, match="unsupported_bound"):
        preparation.SourceInventoryPreparation(_example(index) for index in range(1001))
    recipe = _example()
    entry_bytes = len(encode_source_inventory_entry(make_source_inventory_entry(1, recipe)))
    monkeypatch.setattr(contract, "MAX_INVENTORY_PAGE_BYTES", entry_bytes + 1)
    with pytest.raises(RegistrySourceInventoryError, match="unsupported_bound"):
        preparation.SourceInventoryPreparation((recipe,))


def test_incremental_byte_budget_precedes_entry_replacement_and_further_metadata(monkeypatch):
    recipes = (_example(0), _example(1))
    builder = preparation.SourceInventoryPreparation(recipes)
    original_by_identity = dict(builder._entries_by_sha)
    descriptor = _descriptor(recipes[0])
    identity = descriptor["recipe_sha256"]
    current = original_by_identity[identity]
    replacement = make_source_inventory_entry(current.ordinal, recipes[0], contribution=descriptor)
    exact_bytes = (
        builder.recipe_root.encoded_bytes
        - len(encode_source_inventory_entry(current))
        + len(encode_source_inventory_entry(replacement))
    )
    monkeypatch.setattr(preparation, "MAX_INVENTORY_BYTES", exact_bytes - 1)
    with pytest.raises(RegistrySourceInventoryError, match="unsupported_bound"):
        builder.add_contribution(descriptor)
    assert builder._entries_by_sha == original_by_identity
    assert builder._encoded_bytes == builder.recipe_root.encoded_bytes
    with pytest.raises(RegistrySourceInventoryError):
        builder.finish()


def test_exact_complete_total_bytes_fit_and_one_byte_less_refuses(monkeypatch):
    recipes = (_example(),)
    contributions, derivations = _metadata_rows(recipes)
    expected = preparation.prepare_source_inventory(recipes, contributions, derivations)
    monkeypatch.setattr(preparation, "MAX_INVENTORY_BYTES", expected[0].root.encoded_bytes)
    assert preparation.prepare_source_inventory(recipes, contributions, derivations) == expected
    monkeypatch.setattr(preparation, "MAX_INVENTORY_BYTES", expected[0].root.encoded_bytes - 1)
    with pytest.raises(RegistrySourceInventoryError, match="unsupported_bound"):
        preparation.prepare_source_inventory(recipes, contributions, derivations)


def test_unicode_singletons_and_legacy_array_bytes_survive_complete_metadata_pairing():
    original = _example()
    recipe = RegistrySourceMembershipRecipe(
        replace(original.source_pin, source_id='source-é😀"\\id'),
        replace(original.binding_coordinates, source_id='source-é😀"\\id'),
    )
    contributions, derivations = _metadata_rows((recipe,))
    reference, entries = preparation.prepare_source_inventory((recipe,), contributions, derivations)
    canonical = canonical_registry_source_recipes((recipe,))
    assert entries[0].recipe_json.encode() == canonical[1:-1].encode()
    assert reference.root.recipes_sha256 == registry_source_recipes_sha256(canonical)


def test_self_consistent_descriptor_transform_must_still_match_its_recipe():
    recipe = _capture_recipe()
    descriptor = _descriptor(recipe)
    descriptor["transform_version"] = "fhir-source-facts-v1"
    descriptor["capture_sha256"] = hashlib.sha256(
        json.dumps([descriptor["recipe_sha256"], descriptor["transform_version"]], separators=(",", ":")).encode()
    ).hexdigest()
    descriptor["source_generation"] = hashlib.sha256(
        json.dumps([descriptor["capture_sha256"], descriptor["mapping_sha256"]], separators=(",", ":")).encode()
    ).hexdigest()
    with pytest.raises(RegistrySourceInventoryError):
        preparation.prepare_source_inventory((recipe,), (descriptor,), (_derivation(recipe, descriptor),))
