# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native candidate tables and independent native metadata projection checks."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from process import registry_fhir_catalog_validation as validation
from process.network_fhir_source_witness_contract import digest
from process.registry_imported_catalog_preparation import stage_native_catalog_source
from process.registry_native_network_catalog import native_catalog_descriptor
from tests.test_registry_fhir_catalog_grant import standing_grant
from tests.test_registry_fhir_catalog_store_postgres import storage_database as storage_database
from tests.test_registry_fhir_catalog_store_postgres import target
from tests.test_registry_fhir_catalog_validation import snapshot
from tests.test_registry_imported_source_inventory import native_aca_source, native_ptg_source


async def native_control(fixture):
    control = '"' + fixture.control + '"'
    await fixture.connection.execute(
        f"CREATE TABLE {control}.network_registry_identity(allocation_key uuid PRIMARY KEY,network_id integer GENERATED ALWAYS AS IDENTITY UNIQUE)"
    )
    await fixture.connection.execute(
        f"CREATE TABLE {control}.registry_approved_record(approved_revision bigint,record_kind text,record_key text,record_json jsonb,record_revision bigint DEFAULT 1,custom_revision bigint DEFAULT 1)"
    )


def _native_snapshot_vector(selected, descriptors):
    selected["source_vector_json"] = sorted(
        [
            {
                "source_generation": digest(descriptor),
                "source_id": descriptor["coordinates"]["source_id"],
                "status": "current",
            }
            for descriptor in descriptors
        ],
        key=lambda source: (source["source_id"], source["source_generation"]),
    )
    selected["metadata_generation"] = validation.metadata_generation(selected)


@pytest.mark.asyncio
@pytest.mark.parametrize("with_company", [False, True])
async def test_native_family_catalogue_uses_real_candidates_and_rejects_tampered_projection(
    storage_database, monkeypatch, with_company
):
    fixture = storage_database
    await native_control(fixture)
    candidate = await target(fixture)
    selected = snapshot(standing_grant())
    selected.update(schema_name=candidate.schema_name, network_generation=None, manifest_sha256=None)
    aca_source = native_aca_source()
    if with_company:
        from tests.test_registry_aca_company_catalog import enriched_source

        aca_source, _reader, _resolver = await enriched_source(monkeypatch)
    descriptors = [native_catalog_descriptor(aca_source), native_catalog_descriptor(native_ptg_source())]
    _native_snapshot_vector(selected, descriptors)
    accepted_at = datetime(2026, 11, 17, tzinfo=timezone.utc)
    async with fixture.connection.transaction():
        for descriptor in descriptors:
            stage_result = await stage_native_catalog_source(
                fixture.connection,
                candidate,
                SimpleNamespace(descriptor=descriptor, accepted_at=accepted_at),
                selected,
                loader_role=fixture.loader,
                control_schema=fixture.control,
            )
            assert stage_result["native"] and stage_result["networks"] == stage_result["bindings"] == 1
        await validation._validate_sets(
            fixture.connection,
            '"' + candidate.schema_name + '"',
            selected["metadata_generation"],
            validation.snapshot_inputs(selected),
            fixture.control,
        )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{candidate.schema_name}".registry_fhir_catalog_network'
        )
        == 2
    )


@pytest.mark.asyncio
async def test_native_allocation_survives_new_edition_and_keeps_manual_definition(storage_database):
    fixture = storage_database
    await native_control(fixture)
    candidate = await target(fixture)
    selected = snapshot(standing_grant())
    descriptor = native_catalog_descriptor(native_aca_source())
    accepted_at = datetime(2026, 11, 17, tzinfo=timezone.utc)
    async with fixture.connection.transaction():
        await stage_native_catalog_source(
            fixture.connection,
            candidate,
            SimpleNamespace(descriptor=descriptor, accepted_at=accepted_at),
            selected,
            loader_role=fixture.loader,
            control_schema=fixture.control,
        )
        network_id = await fixture.connection.fetchval(
            f'SELECT network_id FROM "{candidate.schema_name}".registry_fhir_catalog_network'
        )
        await fixture.connection.execute(
            f"INSERT INTO \"{fixture.control}\".registry_approved_record(approved_revision,record_kind,record_key,record_json) VALUES($1,'network',$2,$3::jsonb)",
            selected["metadata_approved_revision"],
            str(network_id),
            json.dumps({"display_name": "Manual title", "archived": True}),
        )
        await fixture.connection.execute(
            f"UPDATE \"{candidate.schema_name}\".registry_fhir_catalog_network SET display_name='Manual title',archived=true"
        )
        newer = native_aca_source()
        newer["edition_id"] = "edition-two"
        await stage_native_catalog_source(
            fixture.connection,
            candidate,
            SimpleNamespace(descriptor=native_catalog_descriptor(newer), accepted_at=accepted_at),
            selected,
            loader_role=fixture.loader,
            control_schema=fixture.control,
        )
        actual = await fixture.connection.fetchrow(
            f'SELECT network_id,display_name,archived FROM "{candidate.schema_name}".registry_fhir_catalog_network'
        )
        assert dict(actual) == {"network_id": network_id, "display_name": "Manual title", "archived": True}


async def _stage_native_descriptor(fixture, candidate, selected, descriptor):
    return await stage_native_catalog_source(
        fixture.connection,
        candidate,
        SimpleNamespace(descriptor=descriptor, accepted_at=datetime(2026, 11, 17, tzinfo=timezone.utc)),
        selected,
        loader_role=fixture.loader,
        control_schema=fixture.control,
    )


@pytest.mark.asyncio
async def test_native_replacement_retires_obsolete_rows_and_keeps_other_sources(storage_database):
    """Retire one accepted native replacement while keeping other Sources and the published predecessor."""
    from process.registry_imported_catalog_preparation import retire_replaced_catalog_sources

    fixture = storage_database
    await native_control(fixture)
    old_candidate, new_candidate = await target(fixture), await target(fixture)
    selected = snapshot(standing_grant())
    old_source = native_aca_source()
    old_source["definitions"].append(
        {**old_source["definitions"][0], "network_tier": "Obsolete", "checksum_network": 77}
    )
    old_descriptor = native_catalog_descriptor(old_source)
    retained_descriptor = native_catalog_descriptor(native_ptg_source())
    new_source = native_aca_source()
    new_source["edition_id"] = "edition-two"
    new_source["definitions"][0]["issuer_name"] = "Updated insurer"
    new_descriptor = native_catalog_descriptor(new_source)
    async with fixture.connection.transaction():
        for candidate in (old_candidate, new_candidate):
            for descriptor in (old_descriptor, retained_descriptor):
                await _stage_native_descriptor(fixture, candidate, selected, descriptor)
        old_id = await fixture.connection.fetchval(
            f"SELECT network_id FROM \"{old_candidate.schema_name}\".registry_fhir_catalog_network WHERE display_name LIKE '%Preferred'"
        )
        retained_by_field = dict(source_generation=digest(retained_descriptor))
        previous_by_field = dict(source_vector_json=[{"source_generation": digest(old_descriptor)}, retained_by_field])
        selected["source_vector_json"] = [{"source_generation": digest(new_descriptor)}, retained_by_field]
        await retire_replaced_catalog_sources(
            fixture.connection, new_candidate, selected, previous_by_field, fixture.control
        )
        await _stage_native_descriptor(fixture, new_candidate, selected, new_descriptor)
        catalog_rows = await fixture.connection.fetch(
            f'SELECT network_id,display_name FROM "{new_candidate.schema_name}".registry_fhir_catalog_network'
        )
        assert len(catalog_rows) == 2
        assert {catalog_row["display_name"] for catalog_row in catalog_rows} == {
            "Updated insurer 2027 Preferred",
            "Provider set source-graph",
        }
        assert (
            next(
                catalog_row["network_id"] for catalog_row in catalog_rows if "Preferred" in catalog_row["display_name"]
            )
            == old_id
        )
        generations = await fixture.connection.fetch(
            f'SELECT source_generation FROM "{new_candidate.schema_name}".registry_fhir_catalog_source'
        )
        assert {catalog_row["source_generation"] for catalog_row in generations} == {
            digest(new_descriptor),
            digest(retained_descriptor),
        }
        old_count = await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{old_candidate.schema_name}".registry_fhir_catalog_network'
        )
        assert old_count == 3
