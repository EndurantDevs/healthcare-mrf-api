# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep relationship network validation exact and bounded by its existing key."""

import json

import pytest

from process import registry_fhir_catalog_validation as validation
from tests.test_registry_fhir_catalog_custody_postgres import (
    freeze_metadata,
    metadata_custody as metadata_custody,
    seed_source_projection,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("network_present", [False, True])
async def test_relationship_network_is_required_after_writer_closure(metadata_custody, network_present):
    fixture = metadata_custody
    snapshot = await seed_source_projection(fixture)
    namespace = '"' + fixture.target.schema_name + '"'
    if network_present:
        sealed = await freeze_metadata(fixture, snapshot=snapshot)
        assert sealed["accounting_json"]["registry_fhir_catalog_relationship"]["rows"] == 1
    else:
        await fixture.loader.execute(f"DELETE FROM {namespace}.registry_fhir_catalog_network WHERE network_id=7")
        with pytest.raises(ValueError, match="set_validation_failed"):
            await freeze_metadata(fixture, snapshot=snapshot)
        assert (
            await fixture.connection.fetchval(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=$1", fixture.target.schema_name
            )
            == fixture.roles["loader"]
        )


@pytest.mark.asyncio
async def test_relationship_network_check_uses_existing_primary_key(metadata_custody):
    fixture = metadata_custody
    snapshot = await seed_source_projection(fixture)
    namespace = '"' + fixture.target.schema_name + '"'
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_registry_identity '
        "SELECT network_id FROM generate_series(100,1399) network_id"
    )
    await fixture.loader.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_network "
        "SELECT metadata_generation,identifiers.network_id,display_name,aliases,archived,revision,benefit_codes,search_name "
        f"FROM {namespace}.registry_fhir_catalog_network original CROSS JOIN generate_series(100,1399) identifiers(network_id) "
        "WHERE original.network_id=7"
    )
    await fixture.connection.execute(f"ANALYZE {namespace}.registry_fhir_catalog_network")
    query = validation._SET_SQL.format(namespace=namespace, control='"' + fixture.control_schema + '"')
    explained = json.loads(
        await fixture.connection.fetchval(
            "EXPLAIN(ANALYZE,FORMAT JSON) " + query,
            snapshot["metadata_generation"],
            snapshot["metadata_approved_revision"],
            validation.canonical_json(snapshot["source_vector_json"]),
            snapshot["mapping_sha256"],
        )
    )
    nodes = [explained[0]["Plan"]]
    matching_probes = []
    while nodes:
        node = nodes.pop()
        nodes.extend(node.get("Plans", []))
        if node.get("Index Name") == "registry_fhir_catalog_network_pkey" and "r.network_id" in node.get(
            "Index Cond", ""
        ):
            matching_probes.append(node)
    assert len(matching_probes) == 1
    assert matching_probes[0]["Actual Loops"] == 1
    assert matching_probes[0]["Actual Rows"] == 1
    sealed = await freeze_metadata(fixture, snapshot=snapshot)
    assert sealed["accounting_json"]["registry_fhir_catalog_network"]["rows"] == 1301
