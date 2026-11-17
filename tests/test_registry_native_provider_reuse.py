# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Many native sources resolve their complete retained recipe vector once per independent pass."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_provider_only_projection as projection
from process import registry_imported_source_inventory as inventory
from process import registry_native_provider_source as providers
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_source_witness_contract import digest
from process.ptg_parts.ptg2_shared_source_set import shared_source_set_metadata
from process.registry_native_network_catalog import native_catalog_descriptor
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes
from tests.test_registry_imported_source_inventory import native_aca_source, native_ptg_source

_COPY_FRAME = b"synthetic copy frame"


def _retained_capture(source_by_field, *, row_count=1, content_sha256="b" * 64):
    capture_by_field = {
        "contract": providers.CONTRACT,
        "schema_name": providers.PREFIX + providers.uuid5(providers._NAMESPACE, digest(source_by_field)).hex,
        "source_data": source_by_field,
        "rows": row_count,
        "content_sha256": content_sha256,
    }
    capture_by_field["capture_sha256"] = digest(capture_by_field)
    return capture_by_field


def native_recipe(ordinal):
    coordinates = RegistryNetworkSourceCoordinates(
        "aca",
        "selected-source-" + str(ordinal),
        "source_plans",
        "edition-one",
        "registry-native-network-source-v1",
        "edition-one",
    )
    capture_by_field = _retained_capture(
        {
            "coordinates": asdict(coordinates),
            "policy": "native-provider-only-networks-v1",
            "selectors": {},
        }
    )
    pin = providers.PinnedNativeProviderMembershipSource(capture_by_field, coordinates, "d" * 64)
    return RegistrySourceMembershipRecipe(pin, coordinates)


@pytest.mark.parametrize(
    "capture_changes, coordinate_changes",
    [
        pytest.param({"rows": -1}, {}, id="negative_count"),
        pytest.param({"content_sha256": "g" * 64}, {}, id="bad_digest"),
        pytest.param({"schema_name": "unrelated_provider_facts"}, {}, id="wrong_name"),
        pytest.param({}, {"source_id": "different_source"}, id="different_source"),
        pytest.param({}, {"source_system": "fhir"}, id="different_family"),
    ],
)
def test_retained_native_pin_refuses_invalid_declarations(capture_changes, coordinate_changes):
    pin = native_recipe(1).source_pin
    with pytest.raises(ValueError, match="registry_native_provider_pin_invalid"):
        replace(
            pin,
            capture={**pin.capture, **capture_changes},
            coordinates=replace(pin.coordinates, **coordinate_changes),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("capture_changed", [False, True])
async def test_retained_recipe_refuses_wrong_capture_metadata(monkeypatch, capture_changed):
    descriptor = native_catalog_descriptor(native_aca_source())
    source_by_field = providers.native_provider_source_data(descriptor)
    requested = _retained_capture(source_by_field)
    foreign = _retained_capture({**source_by_field, "selectors": {"unrelated_network": 37}})
    note = providers.CONTRACT + ":" + json.dumps(foreign) if capture_changed else None
    writer = SimpleNamespace(fetchrow=AsyncMock(return_value={"metadata": note}), execute=AsyncMock())
    reader = AsyncMock(side_effect=AssertionError("existing recipe must not reacquire Source"))
    monkeypatch.setattr(inventory, "native_imported_source_reader", reader)

    with pytest.raises(ValueError, match="registry_native_provider_existing_invalid"):
        await providers.prepare_native_provider_recipe(
            writer, descriptor, owner_role="snapshot_owner", runtime_roles=()
        )

    assert writer.fetchrow.await_args.args[1:] == (requested["schema_name"],)
    writer.execute.assert_not_awaited()
    reader.assert_not_called()


async def _copy_provider_facts(_query, *_parameters, output, **options):
    assert options["format"] == "binary"
    await output(_COPY_FRAME)
    return "COPY 2"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["same", "missing_metadata", "different_rows"])
async def test_native_recipe_reuses_only_verified_provider_facts(monkeypatch, state):
    descriptor = native_catalog_descriptor(native_aca_source())
    source_by_field = providers.native_provider_source_data(descriptor)
    content = hashlib.sha256((providers.CONTRACT + ":binary:").encode() + _COPY_FRAME).hexdigest()
    capture_by_field = _retained_capture(
        source_by_field, row_count=1 if state == "different_rows" else 2, content_sha256=content
    )
    note = providers.CONTRACT + ":" + json.dumps(capture_by_field)
    writer = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"metadata": note}),
        fetchval=AsyncMock(return_value=None if state == "missing_metadata" else note),
        copy_from_query=AsyncMock(side_effect=_copy_provider_facts),
        execute=AsyncMock(),
    )
    reader = AsyncMock(side_effect=AssertionError("existing recipe must not reacquire Source"))
    monkeypatch.setattr(inventory, "native_imported_source_reader", reader)
    if state == "same":
        recipe, created = await providers.prepare_native_provider_recipe(
            writer, descriptor, owner_role="snapshot_owner", runtime_roles=()
        )
        pin = recipe.source_pin
        assert not created and pin.capture == capture_by_field
        assert pin.catalog_generation == digest(descriptor)
        assert pin.source_id == "aca:10001:2027"
        assert pin.schema_name == "source_plans" and pin.dataset_id == "edition-one"
    else:
        with pytest.raises(ValueError, match="registry_native_provider_capture_changed"):
            await providers.prepare_native_provider_recipe(
                writer, descriptor, owner_role="snapshot_owner", runtime_roles=()
            )
    assert writer.copy_from_query.await_count == (0 if state == "missing_metadata" else 1)
    writer.execute.assert_not_awaited()
    reader.assert_not_called()


def _provider_copy_source(source_by_field, *, current):
    raw_source = "a" * 64
    input_set = shared_source_set_metadata([raw_source])
    if source_by_field["source_system"] == "ptg":
        source_by_field["graph"]["source_set"] = input_set
    layout_by_field = {
        "state": "sealed",
        "generation": "shared_blocks_v3",
        "source_set": input_set,
        "source_inputs": {"source_count": 1, "source_set_digest": input_set["raw_container_sha256_digest"]},
    }

    return SimpleNamespace(
        fetchval=AsyncMock(side_effect=[current, False]),
        fetchrow=AsyncMock(return_value=layout_by_field),
        fetch=AsyncMock(return_value=[{"raw_container_sha256": raw_source}]),
        execute=AsyncMock(),
        copy_from_query=AsyncMock(side_effect=_copy_provider_facts),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_factory, current, copied_rows, error",
    [
        pytest.param(native_aca_source, False, 0, "source_changed", id="source_advanced"),
        pytest.param(native_aca_source, True, 1, "copy_changed", id="incomplete_copy"),
        pytest.param(native_ptg_source, True, 2, "graph_scope_changed", id="graph_count_changed"),
    ],
)
async def test_native_recipe_failure_never_marks_capture_complete(
    monkeypatch, source_factory, current, copied_rows, error
):
    source_by_field = source_factory()
    source_connection = _provider_copy_source(source_by_field, current=current)
    descriptor = native_catalog_descriptor(source_by_field)

    @asynccontextmanager
    async def reader():
        yield source_connection

    async def copy_to_table(name, *, schema_name, columns, source, **options):
        assert (name, columns, options["format"]) == ("provider_fact", ("source_key", "npi"), "binary")
        assert schema_name.startswith(providers.PREFIX) and source.read() == _COPY_FRAME
        return f"COPY {copied_rows}"

    writer = SimpleNamespace(fetchrow=AsyncMock(return_value=None), execute=AsyncMock(), copy_to_table=copy_to_table)
    freeze = AsyncMock()
    monkeypatch.setattr(inventory, "native_imported_source_reader", reader)
    monkeypatch.setattr(providers, "freeze_native_provider_snapshot", freeze)

    with pytest.raises(ValueError, match=f"registry_native_provider_{error}"):
        await providers.prepare_native_provider_recipe(
            writer, descriptor, owner_role="snapshot_owner", runtime_roles=()
        )

    assert writer.execute.await_count == (2 if current else 0)
    if current:
        source_connection.copy_from_query.assert_awaited_once()
    else:
        source_connection.copy_from_query.assert_not_awaited()
    freeze.assert_not_awaited()


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
