# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared native catalogue accounting survives persistence and exact logical resolution."""

import json
from types import SimpleNamespace

import pytest

from db.registry_fhir_catalog import table_sql
from process import registry_fhir_catalog_validation as validation
from process.network_fhir_source_witness_contract import digest
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_store import (
    catalog_snapshot_preparation,
    checkpoint_fhir_catalog_snapshot,
    has_catalog_snapshot_validation,
)
from process.registry_fhir_catalog_sync import _reusable_catalog_snapshot, _reused_catalog_preparation_target
from process.registry_imported_catalog_preparation import resolve_imported_catalog_preparation
from process.registry_native_network_catalog import native_catalog_descriptor
from tests.test_registry_fhir_catalog_grant import standing_grant
from tests.test_registry_fhir_catalog_store_postgres import storage_database as storage_database
from tests.test_registry_fhir_catalog_store_postgres import target
from tests.test_registry_fhir_catalog_validation import snapshot
from tests.test_registry_imported_source_inventory import native_aca_source
from tests.test_registry_native_network_catalog_postgres import _stage_native_descriptor, native_control


async def prepared_snapshot(fixture, candidate):
    descriptor = native_catalog_descriptor(native_aca_source())
    selected = snapshot(standing_grant())
    fingerprints = await approved_fhir_catalog_fingerprints(fixture.connection, (6,), control_schema=fixture.control)
    marker_by_field = {
        "contract": "registry-imported-catalog-inputs-v1",
        "token": "e" * 64,
        "native_sources": [
            {"source_id": descriptor["coordinates"]["source_id"], "source_generation": digest(descriptor)}
        ],
    }
    selected.update(
        schema_name=candidate.schema_name,
        source_vector_json=[
            {
                "source_id": descriptor["coordinates"]["source_id"],
                "source_generation": digest(descriptor),
                "status": "current",
            }
        ],
        mapping_sha256=fingerprints[6]["metadata"],
        membership_mapping_sha256=fingerprints[6]["membership"],
        accounting_json={"imported_source_inventory": marker_by_field},
    )
    selected["metadata_generation"] = validation.metadata_generation(selected)
    await _stage_native_descriptor(fixture, candidate, selected, descriptor)
    await checkpoint_fhir_catalog_snapshot(
        fixture.connection, candidate, selected, None, {}, control_schema=fixture.control
    )
    assert not has_catalog_snapshot_validation(selected)
    assert validation.metadata_generation(selected) == selected["metadata_generation"]
    retained = await _reusable_catalog_snapshot(
        fixture.connection, SimpleNamespace(control_schema=fixture.control), selected["metadata_generation"]
    )
    staged_by_kind, phase_accounting = {}, {}
    assert _reused_catalog_preparation_target(retained, selected, None, staged_by_kind, phase_accounting) == candidate
    assert staged_by_kind == {"checkpointed": True}
    await freeze_fhir_source_snapshot(fixture.connection, candidate.schema_name, namespace_owner=fixture.loader)
    receipt = await validation.validate_fhir_catalog_candidate(
        fixture.connection, candidate, selected, control_schema=fixture.control
    )
    selected["custody_json"] = {**receipt, "preparation": catalog_snapshot_preparation(selected)}
    selected["accounting_json"] = {**selected["accounting_json"], **receipt["tables"]}
    assert has_catalog_snapshot_validation(selected)
    return selected


async def persist_snapshot(connection, control, selected):
    await connection.execute(
        'UPDATE "'
        + control
        + '".registry_fhir_catalog_snapshot SET custody_json=$2::jsonb,accounting_json=$3::jsonb WHERE metadata_generation=$1',
        selected["metadata_generation"],
        json.dumps(selected["custody_json"]),
        json.dumps(selected["accounting_json"]),
    )


@pytest.fixture
async def prepared_catalog(storage_database):
    fixture = storage_database
    await fixture.connection.execute(f'DROP TABLE "{fixture.control}".registry_fhir_catalog_snapshot')
    await fixture.connection.execute(table_sql('"' + fixture.control + '"', "registry_fhir_catalog_snapshot")[0])
    await native_control(fixture)
    candidate = await target(fixture)
    async with fixture.connection.transaction():
        selected = await prepared_snapshot(fixture, candidate)
        await persist_snapshot(fixture.connection, fixture.control, selected)
    return SimpleNamespace(fixture=fixture, selected=selected, candidate=candidate)


async def resolve(case):
    async with case.fixture.connection.transaction():
        return await resolve_imported_catalog_preparation(
            case.fixture.connection, "e" * 64, 6, control_schema=case.fixture.control
        )


@pytest.mark.asyncio
async def test_real_prepared_native_accounting_resolves_after_persistence(prepared_catalog):
    case = prepared_catalog
    result = await resolve(case)
    assert result.metadata_generation == case.selected["metadata_generation"]
    assert result.schema_name == case.candidate.schema_name
    assert result.accounting == case.selected["accounting_json"]
    assert set(result.accounting) == {*validation._KEYS, "imported_source_inventory"}


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["table_count", "missing_table", "unknown_key", "marker"])
async def test_prepared_resolution_rejects_changed_accounting_or_marker(prepared_catalog, damage):
    case = prepared_catalog
    accounting_by_key = json.loads(json.dumps(case.selected["accounting_json"]))
    if damage == "table_count":
        accounting_by_key["registry_fhir_catalog_network"]["rows"] += 1
    if damage == "missing_table":
        del accounting_by_key["registry_fhir_catalog_network"]
    if damage == "unknown_key":
        accounting_by_key["unrecognized"] = {}
    if damage == "marker":
        accounting_by_key["imported_source_inventory"]["native_sources"][0]["source_generation"] = "f" * 64
    await case.fixture.connection.execute(
        f'UPDATE "{case.fixture.control}".registry_fhir_catalog_snapshot SET accounting_json=$1::jsonb',
        json.dumps(accounting_by_key),
    )
    with pytest.raises(ValueError, match="inputs_changed"):
        await resolve(case)
