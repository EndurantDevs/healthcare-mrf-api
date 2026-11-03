# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed append inputs retain native inventory authority and the existing config budget."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_source_recipe_additions as additions
from process.registry_publication_worker import MAX_CONFIG_BYTES, parse_registry_publication_worker_config
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import SourceInventoryReference, encode_source_inventory_reference
from process.registry_source_recipe_store import canonical_registry_source_recipes
from tests.test_registry_publication_worker import _configured_recipe, configuration
from tests.test_registry_source_inventory_consumers import _inventory


def _configured_additions():
    recipe = _configured_recipe()
    reference = SourceInventoryReference(source_inventory_root((make_source_inventory_entry(1, recipe),)))
    additions_by_field = {
        "expected_serving_generation": 7,
        "manifest_sha256": "d" * 64,
        "inventory": reference.as_dict(),
        "recipes": json.loads(canonical_registry_source_recipes((recipe,))),
    }
    configured = configuration()
    configured["initial_address_sources"] = {
        "base_source": {"schema_name": "addresses", "table_name": "entity_address_unified", "generation_id": "a" * 64},
        "compact": True,
        "source_recipe_additions": additions_by_field,
    }
    return configured, additions_by_field


def test_worker_decodes_only_additions_without_inlining_predecessor_recipes():
    configured, additions_by_field = _configured_additions()
    parsed = parse_registry_publication_worker_config(json.dumps(configured)).initial_address_sources
    assert not parsed.initial_source_recipes and not parsed.replacement_source_recipes
    assert parsed.source_recipe_additions == additions.decode_source_recipe_additions(additions_by_field)
    assert parsed.source_recipe_additions.recipes == (_configured_recipe(),)
    assert parsed.source_recipe_additions.inventory.as_dict() == additions_by_field["inventory"]
    encoded = json.dumps(configured)
    assert len(encoded.encode()) < MAX_CONFIG_BYTES
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        parse_registry_publication_worker_config(encoded + " " * MAX_CONFIG_BYTES)


@pytest.mark.parametrize(
    "damage",
    [
        "unknown",
        "missing",
        "null",
        "generation_bool",
        "generation_zero",
        "digest",
        "reference",
        "reference_field",
        "recipes_null",
        "recipes_string",
        "recipes_empty",
        "recipe_unknown",
        "duplicate",
        "initial",
        "replacement",
        "legacy",
    ],
)
def test_worker_rejects_ambiguous_additions_and_mixed_modes(damage):
    configured, additions_by_field = _configured_additions()
    sources_by_phase = configured["initial_address_sources"]
    match damage:
        case "unknown":
            additions_by_field["unexpected"] = True
        case "missing":
            additions_by_field.pop("inventory")
        case "null":
            sources_by_phase["source_recipe_additions"] = None
        case "generation_bool":
            additions_by_field["expected_serving_generation"] = True
        case "generation_zero":
            additions_by_field["expected_serving_generation"] = 0
        case "digest":
            additions_by_field["manifest_sha256"] = "D" * 64
        case "reference":
            additions_by_field["inventory"] = []
        case "reference_field":
            additions_by_field["inventory"]["table_name"] = "another_inventory"
        case "recipes_null":
            additions_by_field["recipes"] = None
        case "recipes_string":
            additions_by_field["recipes"] = json.dumps(additions_by_field["recipes"])
        case "recipes_empty":
            additions_by_field["recipes"] = []
        case "recipe_unknown":
            additions_by_field["recipes"][0]["unexpected"] = True
        case "duplicate":
            additions_by_field["recipes"] *= 2
        case "initial" | "replacement":
            sources_by_phase[damage + "_source_recipes"] = additions_by_field["recipes"]
        case "legacy":
            sources_by_phase["compact"] = False
    with pytest.raises(ValueError, match="^registry_publication_worker_config_invalid$"):
        parse_registry_publication_worker_config(json.dumps(configured))


async def test_append_reference_never_substitutes_for_native_retained_custody(monkeypatch):
    _, additions_by_field = _configured_additions()
    manifest = _inventory().manifest
    parsed = replace(
        additions.decode_source_recipe_additions(additions_by_field), manifest_sha256=manifest.manifest_sha256
    )
    native_resolver = AsyncMock(side_effect=ValueError("native_custody_refused"))
    monkeypatch.setattr(additions, "resolve_retained_registry_source_recipes", native_resolver)
    connection = SimpleNamespace(fetchval=AsyncMock())
    with pytest.raises(ValueError, match="native_custody_refused"):
        await additions.resolve_source_recipe_additions(connection, parsed, manifest, 7, control_schema="registry")
    native_resolver.assert_awaited_once_with(connection, manifest, control_schema="registry")
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("damage", ["generation", "manifest", "missing_manifest"])
async def test_append_predecessor_is_checked_before_retained_reads(monkeypatch, damage):
    _, additions_by_field = _configured_additions()
    manifest = _inventory().manifest
    parsed = replace(
        additions.decode_source_recipe_additions(additions_by_field), manifest_sha256=manifest.manifest_sha256
    )
    resolver = AsyncMock()
    monkeypatch.setattr(additions, "resolve_retained_registry_source_recipes", resolver)
    if damage == "manifest":
        parsed = replace(parsed, manifest_sha256="f" * 64)
    with pytest.raises(ValueError, match="additions_predecessor_changed"):
        await additions.resolve_source_recipe_additions(
            None,
            parsed,
            None if damage == "missing_manifest" else manifest,
            8 if damage == "generation" else 7,
            control_schema="registry",
        )
    resolver.assert_not_awaited()


async def test_append_checks_complete_reference_including_aggregate_counts(monkeypatch):
    _, additions_by_field = _configured_additions()
    manifest = _inventory().manifest
    parsed = replace(
        additions.decode_source_recipe_additions(additions_by_field), manifest_sha256=manifest.manifest_sha256
    )
    actual = replace(
        parsed.inventory, root=replace(parsed.inventory.root, encoded_bytes=parsed.inventory.root.encoded_bytes + 1)
    )
    monkeypatch.setattr(additions, "resolve_retained_registry_source_recipes", AsyncMock(return_value=()))
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=encode_source_inventory_reference(actual)))
    with pytest.raises(ValueError, match="additions_inventory_changed"):
        await additions.resolve_source_recipe_additions(connection, parsed, manifest, 7, control_schema="registry")


async def test_append_cannot_replace_an_existing_fhir_endpoint(monkeypatch):
    _, additions_by_field = _configured_additions()
    manifest = _inventory().manifest
    parsed = replace(
        additions.decode_source_recipe_additions(additions_by_field), manifest_sha256=manifest.manifest_sha256
    )
    original = parsed.recipes[0]
    successor = replace(
        original,
        source_pin=replace(original.source_pin, dataset_id="dataset-two", dataset_sha256="f" * 64),
        binding_coordinates=replace(original.binding_coordinates, dataset_id="dataset-two", edition_id="two"),
    )
    parsed = replace(parsed, recipes=(successor,))
    monkeypatch.setattr(additions, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(original,)))
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=encode_source_inventory_reference(parsed.inventory)))
    with pytest.raises(ValueError, match="registry_fhir_serving_preservation_vector_incomplete"):
        await additions.resolve_source_recipe_additions(connection, parsed, manifest, 7, control_schema="registry")
