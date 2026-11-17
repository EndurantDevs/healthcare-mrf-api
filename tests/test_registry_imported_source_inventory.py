# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current Source metadata, native family definitions and cohort identity stay distinct."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_imported_membership_refresh as refresh
from process import registry_imported_source_inventory as inventory
from process import registry_native_network_catalog as native
from process.registry_fhir_catalog_validation import metadata_generation, snapshot_inputs
from tests.test_registry_fhir_catalog_grant import standing_grant
from tests.test_registry_fhir_catalog_validation import snapshot
from tests.test_registry_typed_source_recipe import _recipe


def native_aca_source():
    return {
        "contract": native.CONTRACT,
        "source_system": "aca",
        "source_id": "aca:10001:2027",
        "schema_name": "source_plans",
        "edition_id": "edition-one",
        "policy": inventory.NATIVE_POLICY,
        "definitions": [
            {
                "issuer_id": "10001",
                "plan_year": 2027,
                "network_tier": "Preferred",
                "checksum_network": 23,
                "plan_id": "10001NY0000001",
                "state": "NY",
                "marketing_name": "Example plan",
                "issuer_name": "Example insurer",
            }
        ],
    }


def native_ptg_source():
    return {
        "contract": native.CONTRACT,
        "source_system": "ptg",
        "source_id": "source-graph",
        "schema_name": "source_rates",
        "edition_id": "snapshot-one",
        "policy": inventory.NATIVE_POLICY,
        "graph": {
            "snapshot_key": 31,
            "import_run_id": "import-one",
            "source_set": {
                "contract": "sorted_raw_container_sha256_bytes_v1",
                "source_count": 1,
                "raw_container_sha256_digest": "e" * 64,
            },
            "provider_graph": {"npi_count": 4, "owner_count": 5, "provider_group_count": 2},
        },
        "definitions": [
            {
                "source_key": "source-graph",
                "network_names": [],
                "plans": [{"plan_id": "plan-one", "plan_market_type": "group"}],
            }
        ],
    }


def inventory_config(descriptor):
    return inventory.parse_imported_source_inventory_config(
        {
            "aca_source_schema": "source_plans",
            "ptg_source_schema": "source_rates",
            "fhir_source_profiles": [descriptor["native_read_profile"]],
        }
    )


@pytest.fixture
def inventory_case():
    descriptor = _recipe().source_pin.catalog_descriptor
    configured = inventory_config(descriptor)
    aca = native_aca_source()
    ptg = native_ptg_source()

    async def fetch(sql, *parameters):
        if "plan_networktier" in sql:
            return deepcopy(aca["definitions"])
        if "snapshot.manifest" in sql:
            graph = ptg["graph"]
            return [
                {
                    "source_key": ptg["source_id"],
                    "snapshot_id": ptg["edition_id"],
                    "snapshot_key": graph["snapshot_key"],
                    "import_run_id": graph["import_run_id"],
                    "serving_index": graph,
                    "allowed_amount_index": None,
                },
                {
                    "source_key": "allowed-only",
                    "snapshot_id": "allowed-snapshot",
                    "snapshot_key": None,
                    "import_run_id": "allowed-import",
                    "serving_index": None,
                    "allowed_amount_index": {"data_domain": "allowed_amounts"},
                },
            ]
        if "current_plan_source" in sql:
            return [
                {"source_key": ptg["source_id"], "snapshot_id": ptg["edition_id"], **ptg["definitions"][0]["plans"][0]}
            ]
        raise AssertionError(sql)

    async def pages():
        yield (SimpleNamespace(status="accepted", descriptor=descriptor),)

    @asynccontextmanager
    async def factory(*, source_profile):
        yield {
            "native_read_profile": source_profile.document(),
            "inventory": pages,
            "replay": AsyncMock(),
            "legacy_bindings": AsyncMock(),
        }

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetch=AsyncMock(side_effect=fetch),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetchrow=AsyncMock(return_value={"import_id": "edition-one", "state": "complete"}),
    )
    return SimpleNamespace(
        connection=connection, config=configured, factory=factory, descriptor=descriptor, aca=aca, ptg=ptg
    )


async def observe(case):
    return await inventory.observe_imported_source_inventory(
        case.connection, config=case.config, catalog_export=case.factory
    )


@pytest.mark.asyncio
async def test_inventory_reads_native_heads_and_excludes_allowed_amounts(inventory_case):
    case = inventory_case
    result = await observe(case)
    assert result.fhir_descriptors == (case.descriptor,)
    assert {source["source_system"] for source in result.native_sources} == {"aca", "ptg"}
    assert len(result.native_sources) == 2
    generation, sources, descriptors = await refresh.imported_catalog_inputs(object(), "registry", inventory=result)
    assert generation == result.token and descriptors == [case.descriptor]
    assert len(sources) == 3 and {source["capture_generation"] for source in sources} == {result.token}
    assert all("network_serving_control" not in call.args[0] for call in case.connection.fetch.call_args_list)


@pytest.mark.asyncio
async def test_source_token_tracks_named_data_and_policy(inventory_case, monkeypatch):
    case = inventory_case
    before = await observe(case)
    case.descriptor["tables"]["resource"] = {"schema": "edition_resources", "name": "resources"}
    assert (await observe(case)).token != before.token
    case.ptg["edition_id"] = "snapshot-two"
    assert (await observe(case)).token != before.token
    monkeypatch.setattr(inventory, "graph_policy", lambda source_id: {"office_policy": "next-policy"})
    assert (await observe(case)).token != before.token


@pytest.mark.asyncio
async def test_incomplete_native_publication_refuses_inventory(inventory_case):
    case = inventory_case
    case.connection.fetchrow.return_value["state"] = "pending"
    with pytest.raises(ValueError, match="aca_incomplete"):
        await observe(case)


def test_native_definition_identity_and_unnamed_labels_preserve_proven_scope():
    aca = native.native_catalog_descriptor(native_aca_source())
    definitions = native.native_network_definitions(aca)
    assert (
        len(definitions) == 1 and next(iter(definitions.values()))["display_name"] == "Example insurer 2027 Preferred"
    )
    changed = deepcopy(aca)
    changed["coordinates"].update(dataset_id="edition-two", edition_id="edition-two")
    changed["input"]["edition_id"] = "edition-two"
    assert list(native.native_network_definitions(changed)) == list(definitions)
    ptg = native.native_catalog_descriptor(native_ptg_source())
    network_ids_by_key = {key: 42 for key in native.native_network_definitions(ptg)}
    projection = native.native_catalog_rows(
        ptg,
        generation="a" * 64,
        accepted_at="2026-11-17T00:00:00Z",
        metadata_generation="b" * 64,
        networks=network_ids_by_key,
    )
    assert projection.network_rows[0][2] == "Provider set source-graph"
    binding = json.loads(projection.binding_rows[0][-1])
    assert binding["source_system"] == "ptg" and binding["source_scope"]["snapshot_key"] == 31
    assert "organization_id" not in binding["source_scope"]


def test_aca_conflicting_checksum_or_tier_identity_refuses_definition():
    source = native_aca_source()
    source["definitions"].append({**source["definitions"][0], "network_tier": "Different"})
    with pytest.raises(ValueError, match="aca_ambiguous"):
        native.native_catalog_descriptor(source)


def test_native_cohort_hash_excludes_membership_association_but_retains_inputs():
    selected = snapshot(standing_grant())
    selected["accounting_json"] = {
        "imported_source_inventory": {
            "contract": "registry-imported-catalog-inputs-v1",
            "token": "d" * 64,
            "native_sources": [{"source_id": "source-graph", "source_generation": "f" * 64}],
        }
    }
    before = metadata_generation(selected)
    selected.update(network_generation=10, manifest_sha256="c" * 64)
    assert metadata_generation(selected) == before
    assert metadata_generation(snapshot_inputs(selected)) == before
    selected["accounting_json"]["imported_source_inventory"]["token"] = "e" * 64
    assert metadata_generation(selected) != before


@pytest.mark.asyncio
async def test_missing_server_inventory_capability_never_falls_back_to_serving_pointer():
    with pytest.raises(ValueError, match="native_inventory_required"):
        await refresh.imported_catalog_inputs(AsyncMock(), "registry")


def test_large_plan_inventory_keeps_control_pins_compact(record_property):
    from time import perf_counter

    from process.network_fhir_source_witness_contract import canonical, digest
    from process.registry_imported_catalog_preparation import imported_catalog_accounting
    from process.registry_native_provider_source import native_provider_source_data

    started = perf_counter()
    native_sources = []
    for ordinal in range(304):
        source_document = native_ptg_source()
        source_document["source_id"] = "selected-source-" + str(ordinal)
        definition = source_document["definitions"][0]
        definition["source_key"] = source_document["source_id"]
        definition["plans"] = [
            {"plan_id": digest([ordinal, plan]), "plan_market_type": "group"}
            for plan in range(101261 // 304 + (ordinal < 101261 % 304))
        ]
        native_sources.append(source_document)
    document_by_field = {
        "contract": "registry-imported-catalog-inputs-v1",
        "fhir_semantics": [],
        "fhir_descriptors": [],
        "native_sources": native_sources,
        "unavailable": [],
        "effective_date": "2026-11-17",
        "search_policy": "native-network-search-v1",
        "catalog_projector_version": inventory.PROJECTOR_VERSION,
    }
    observed = inventory.ImportedSourceInventory(canonical(document_by_field))
    accounting_bytes = len(canonical(imported_catalog_accounting(observed)).encode())
    descriptor_bytes = [
        len(canonical(native.native_catalog_descriptor(source_document)).encode()) for source_document in native_sources
    ]
    recipe_input_bytes = [
        len(canonical(native_provider_source_data(native.native_catalog_descriptor(source_document))).encode())
        for source_document in native_sources
    ]
    evidence_by_field = {
        "scope": "metadata preparation component only",
        "sources": 304,
        "plan_pointers": 101261,
        "full_inventory_bytes": len(observed.document_json.encode()),
        "control_accounting_bytes": accounting_bytes,
        "max_source_descriptor_bytes": max(descriptor_bytes),
        "max_recipe_source_input_bytes": max(recipe_input_bytes),
        "seconds": perf_counter() - started,
    }
    record_property("metadata_scale", json.dumps(evidence_by_field, sort_keys=True))
    print(json.dumps(evidence_by_field, sort_keys=True))
    assert sum(len(source_document["definitions"][0]["plans"]) for source_document in native_sources) == 101261
    assert accounting_bytes < 8 * 1024 * 1024 and max(descriptor_bytes) < 8 * 1024 * 1024
    assert max(recipe_input_bytes) < 4096


@pytest.mark.asyncio
async def test_catalogue_policy_change_invalidates_inventory_token(inventory_case, monkeypatch):
    before = await observe(inventory_case)
    monkeypatch.setattr(inventory, "PROJECTOR_VERSION", "fhir-catalog/next-version")
    after = await observe(inventory_case)
    assert before.token != after.token
    assert before.fhir_descriptors == after.fhir_descriptors
    assert before.native_sources == after.native_sources
