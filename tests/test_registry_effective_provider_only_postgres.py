# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-only visibility uses exact native family coordinates and frozen evidence."""

import json

import pytest

from process import registry_network_catalog_read as catalog_read
from process.network_provider_only_projection import _create_evidence_heap, provider_only_page_sql
from process.registry_effective_network_catalog import effective_catalog_sql
from tests.effective_catalog_support import SOURCE_GENERATION
from tests.effective_catalog_support import effective_catalog_db as effective_catalog_db


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["aca", "ptg"])
async def test_native_provider_only_families_require_complete_source_coordinates(effective_catalog_db, family):
    db = effective_catalog_db
    control, catalog = '"' + db.control + '"', '"' + db.catalog + '"'
    binding_by_field = json.loads(
        await db.connection.fetchval(f"SELECT document_json FROM {catalog}.registry_fhir_catalog_binding")
    )
    binding_by_field.update(
        source_system=family, producer_id="registry-native-network-source-v1", source_key="plan-snapshot"
    )
    binding_by_field["source_scope"] = {
        "kind": "imported_provider_set_v1",
        "source_generation": SOURCE_GENERATION,
        "source_key": "plan-snapshot",
        "snapshot_id": "snapshot-one",
        "snapshot_key": 1,
    }
    if family == "aca":
        binding_by_field["source_key"] = "12345CA1234567"
        binding_by_field["source_scope"] = {
            "issuer_id": "12345",
            "plan_id": "12345CA1234567",
            "plan_year": 2026,
            "state": "CA",
            "checksum_network": 17,
            "source_generation": SOURCE_GENERATION,
        }
    await db.connection.execute(
        f"UPDATE {catalog}.registry_fhir_catalog_binding SET document_json=$1::jsonb", json.dumps(binding_by_field)
    )
    await db.connection.execute(
        f"CREATE TABLE {control}.network_source_inventory(source_system text,source_id text,dataset_schema text,dataset_id text,producer_id text,edition_id text,fhir_scope_key text,fhir_scope_value text)"
    )
    await db.connection.execute(
        f"INSERT INTO {control}.network_source_inventory VALUES($1,'source-one','source_data','dataset-one','registry-native-network-source-v1','dataset-one',NULL,NULL)",
        family,
    )
    await _create_evidence_heap(db.connection, control + ".network_provider_only_evidence")
    await db.connection.execute(
        f"INSERT INTO {control}.network_provider_only_evidence VALUES(7,'npi','1234567893',$1)", "f" * 64
    )
    catalog_records = await effective_catalog_sql(db.connection, db.manifest, db.control)
    catalogue = await catalog_read._page(
        db.connection, db.manifest, catalog_read.RegistryNetworkCatalogQuery(), (), db.control
    )
    assert catalogue["items"][0]["source_bindings"][0]["source_system"] == family
    statement = provider_only_page_sql(db.control, control_schema=db.control, catalog_records=catalog_records)
    parameters = (0, [7], 25, 0, 1000, 1048576, 9)
    page = await db.connection.fetchrow(statement, *parameters)
    assert page["valid"] is True and page["bounded"] is True
    document = json.loads(page["result"])
    assert document["total_count"] == 1 and document["providers"][0]["provider_id"] == "1234567893"
    assert document["providers"][0]["network_memberships"] == [{"network_id": 7, "evidence_ids": ["f" * 64]}]
    assert await db.connection.fetchval(f"SELECT count(*) FROM {control}.network_membership") == 0
    await db.connection.execute(f"UPDATE {control}.network_source_inventory SET edition_id='different-generation'")
    assert (await db.connection.fetchrow(statement, *parameters))["valid"] is False
