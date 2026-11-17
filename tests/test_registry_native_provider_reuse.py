# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Many native sources resolve their complete retained recipe vector once per independent pass."""

import hashlib
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_provider_only_projection as projection
from process import registry_native_provider_source as providers
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_source_witness_contract import digest
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes


def native_recipe(ordinal):
    coordinates = RegistryNetworkSourceCoordinates(
        "aca",
        "selected-source-" + str(ordinal),
        "source_plans",
        "edition-one",
        "registry-native-network-source-v1",
        "edition-one",
    )
    source_by_field = {
        "coordinates": asdict(coordinates),
        "policy": "native-provider-only-networks-v1",
        "selectors": {},
    }
    capture_by_field = {
        "contract": providers.CONTRACT,
        "schema_name": providers.PREFIX + providers.uuid5(providers._NAMESPACE, digest(source_by_field)).hex,
        "source_data": source_by_field,
        "rows": 1,
        "content_sha256": "b" * 64,
    }
    capture_by_field["capture_sha256"] = digest(capture_by_field)
    pin = providers.PinnedNativeProviderMembershipSource(capture_by_field, coordinates, "d" * 64)
    return RegistrySourceMembershipRecipe(pin, coordinates)


@pytest.mark.asyncio
async def test_many_sources_load_retained_inventory_once_per_independent_pass(monkeypatch):
    from process import registry_source_recipe_store

    recipes = tuple(native_recipe(ordinal) for ordinal in range(304))
    connection = SimpleNamespace(fetch=AsyncMock(return_value=recipes), execute=AsyncMock())
    manifest = SimpleNamespace(generation_id=9, manifest_sha256="a" * 64)

    async def resolve(driver, source_manifest, *, control_schema):
        assert source_manifest == manifest and control_schema == "registry"
        return await driver.fetch("complete retained recipe inventory")

    monkeypatch.setattr(registry_source_recipe_store, "resolve_retained_registry_source_recipes", resolve)
    observed_indexes = []

    async def rebuild(driver, recipe, approved, **options):
        retained = options["retained_captures"]
        assert providers._has_retained_native_capture(recipe.source_pin, retained, options["source_manifest"])
        observed_indexes.append(retained)
        identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
        return providers.NativeProviderStage(
            identity,
            recipe.source_pin.generation_id,
            recipe.source_pin.catalog_generation,
            "e" * 64,
            'pg_temp."native_provider_mapped_' + uuid4().hex + '"',
            1,
            options["catalog_scope"],
            manifest,
        )

    monkeypatch.setattr(providers, "rebuild_native_provider_stage", rebuild)
    approved = ApprovedMembershipSource(1, "c" * 64)
    context = SimpleNamespace(
        source_manifest=manifest, source_witness_runtime=SimpleNamespace(catalog_bindings=object())
    )
    base = RegistrySourceRecipeStage("base_stage", 0, 0, "f" * 64)
    stage = await providers.append_native_provider_stages(
        connection, recipes, approved, base, read_context=context, control_schema="registry", verify_source=False
    )
    assert connection.fetch.await_count == 1
    assert len(observed_indexes) == 304 and len({id(index) for index in observed_indexes}) == 1
    producer_index = observed_indexes[0]
    observed_indexes.clear()
    await projection._append_native_staged_sources(
        connection, "destination", recipes, stage, approved, "registry", verify_source=True
    )
    assert connection.fetch.await_count == 2
    assert len(observed_indexes) == 304 and len({id(index) for index in observed_indexes}) == 1
    assert observed_indexes[0] is not producer_index
