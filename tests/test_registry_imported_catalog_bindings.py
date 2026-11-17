# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Imported catalog scope preserves manual exclusions without manual admission writes."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_imported_catalog_bindings as imported
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_typed_mapping import TypedGraphApproval, _approved_graph_keys, typed_graph_bindings
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from tests.test_registry_fhir_catalog_sources import descriptor as source_descriptor


@pytest.fixture
def catalog_case(monkeypatch):
    descriptor = source_descriptor()
    coordinates = RegistryNetworkSourceCoordinates(**descriptor["coordinates"])
    approved = ApprovedMembershipSource(7, "c" * 64)
    generation = fhir_catalog_source_generation(descriptor)
    snapshot_by_field = {
        "schema_name": "fhir_catalog_" + "1" * 32,
        "metadata_approved_revision": 5,
        "source_vector": json.dumps(
            [{"source_generation": generation, "source_id": coordinates.source_id, "status": "current"}]
        ),
    }
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=snapshot_by_field),
        fetchval=AsyncMock(side_effect=[json.dumps(descriptor), False]),
    )
    pin = AsyncMock(return_value=approved)
    monkeypatch.setattr(imported, "pin_approved_membership_source", pin)
    return SimpleNamespace(
        connection=connection,
        scope=imported.PublishedFHIRCatalogBindingScope("d" * 64),
        approved=approved,
        coordinates=coordinates,
        descriptor=descriptor,
        snapshot=snapshot_by_field,
        pin=pin,
    )


async def bindings(case):
    return await imported.published_fhir_catalog_bindings_sql(
        case.connection, case.scope, case.approved, case.coordinates, case.descriptor, control_schema="registry"
    )


@pytest.mark.parametrize("generation", [None, True, "", "A" * 64, "a" * 63, "a" * 64 + "' OR true"])
def test_scope_accepts_only_a_complete_logical_generation(generation):
    with pytest.raises(ValueError, match="scope_invalid"):
        imported.PublishedFHIRCatalogBindingScope(generation)


@pytest.mark.asyncio
async def test_exact_published_source_uses_catalog_without_manual_admission(catalog_case):
    case = catalog_case
    original = deepcopy(case.descriptor)
    sql = await bindings(case)
    assert '"fhir_catalog_' in sql and "approved_network_bindings AS MATERIALIZED" in sql
    assert "INSERT" not in sql and "UPDATE" not in sql
    assert case.descriptor == original
    case.pin.assert_awaited_once_with(case.connection, approved_revision=7, control_schema="registry")
    assert case.connection.fetchval.await_args_list[-1].args[1:] == (7, *case.coordinates.sql_parameters)


def _damage_catalog_scope(case, damage):
    if damage in {"last_good", "missing", "duplicate"}:
        _damage_source_vector(case, damage)
        return
    mutations_by_name = {
        "unpublished": lambda: setattr(case.connection.fetchrow, "return_value", None),
        "schema": lambda: case.snapshot.update(schema_name="unrelated"),
        "newer_revision": lambda: case.snapshot.update(metadata_approved_revision=8),
        "descriptor": lambda: setattr(
            case.connection.fetchval, "side_effect", [json.dumps({**case.descriptor, "source_aliases": ["other"]})]
        ),
        "conflict": lambda: setattr(case.connection.fetchval, "side_effect", [json.dumps(case.descriptor), True]),
    }
    mutations_by_name[damage]()


def _damage_source_vector(case, damage):
    vector = json.loads(case.snapshot["source_vector"])
    if damage == "last_good":
        vector[0]["status"] = "last_good"
    case.snapshot["source_vector"] = json.dumps(
        [] if damage == "missing" else vector * (2 if damage == "duplicate" else 1)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["unpublished", "schema", "newer_revision", "last_good", "missing", "duplicate", "descriptor", "conflict"]
)
async def test_changed_or_incomplete_native_scope_never_falls_back(catalog_case, damage):
    case = catalog_case
    _damage_catalog_scope(case, damage)
    with pytest.raises(ValueError, match="registry_imported_catalog_"):
        await bindings(case)


@pytest.mark.asyncio
async def test_manual_pin_must_still_be_current(catalog_case):
    case = catalog_case
    case.pin.return_value = ApprovedMembershipSource(7, "b" * 64)
    with pytest.raises(ValueError, match="manual_map_changed"):
        await bindings(case)
    case.connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_typed_mapping_selects_imported_scope_explicitly(catalog_case, monkeypatch):
    from process import network_approved_source_bindings

    case = catalog_case
    manual = AsyncMock(side_effect=AssertionError("imported publication must not need manual binding approval"))
    monkeypatch.setattr(network_approved_source_bindings, "require_approved_network_source_bindings", manual)
    approval = TypedGraphApproval(case.approved, case.coordinates, case.descriptor, catalog_bindings=case.scope)
    assert "catalog_bindings AS MATERIALIZED" in await typed_graph_bindings(
        case.connection, approval, control_schema="registry"
    )
    manual.assert_not_awaited()


@pytest.mark.asyncio
async def test_all_manually_excluded_imported_networks_can_produce_an_empty_mapping():
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=[True, []]))
    arguments = (7, "fhir", "source", "source_schema", "dataset", "producer", "edition", "a" * 64)
    assert await _approved_graph_keys(connection, "bindings", arguments, allow_empty=True) == []
    assert connection.fetchval.await_args_list[-1].args[1:] == arguments
