# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""HIOS evidence links companies in batches without name matching or ownership-date inference."""

import json
from copy import deepcopy
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_aca_company_catalog as companies
from process.network_fhir_source_witness_contract import digest
from process.registry_imported_source_inventory import ImportedSourceInventory
from process.registry_native_network_catalog import (
    native_catalog_descriptor,
    native_catalog_rows,
    native_network_definitions,
    validate_native_catalog_descriptor,
)
from process.registry_native_provider_source import native_provider_source_data
from tests.test_registry_imported_source_inventory import native_aca_source, native_ptg_source

COMPANY = str(UUID(int=101))
GROUP = str(UUID(int=102))


def issuer_resolution():
    return {
        "hios_issuer_id": "10001",
        "business_state": "NY",
        "reporting_year": 2024,
        "relationship_scope": "source_reporting_period",
        "company_resolution_status": "resolved",
        "group_resolution_status": "resolved",
        "resolution_status": "resolved",
        "issuer_source_labels": ["Source issuer label"],
        "legal_company": {"company_id": COMPANY, "source_labels": ["Source legal company"]},
        "reported_group": {
            "group_id": GROUP,
            "source_labels": ["Source reported group"],
            "relationship_kind": "reported_affiliation",
        },
        "issues": [],
        "evidence": [
            {
                "snapshot_id": str(UUID(int=103)),
                "source_record_key": "source-row-one",
                "group_assertion": {"group_id": GROUP},
            }
        ],
    }


async def enriched_source(monkeypatch, resolution=None):
    chosen = issuer_resolution() if resolution is None else resolution
    reader = AsyncMock()
    reader.fetch.return_value = [{"group_id": GROUP, "group_kind": "naic_group"}]
    resolver = AsyncMock(return_value=(chosen,))
    monkeypatch.setattr(companies, "read_registry_issuer_resolutions", resolver)
    (source,) = await companies.enrich_aca_company_sources(reader, [native_aca_source()], control_schema="registry")
    return source, reader, resolver


@pytest.mark.asyncio
async def test_distinct_issuer_batch_preserves_reporting_period(monkeypatch):
    reader = AsyncMock()
    reader.fetch.return_value = [{"group_id": GROUP, "group_kind": "naic_group"}]
    resolver = AsyncMock(return_value=(issuer_resolution(),))
    monkeypatch.setattr(companies, "read_registry_issuer_resolutions", resolver)
    first = native_aca_source()
    second = deepcopy(first)
    second["definitions"][0]["plan_year"] = 2026
    ptg = native_ptg_source()
    enriched = await companies.enrich_aca_company_sources(reader, [first, second, ptg], control_schema="registry")
    resolver.assert_awaited_once_with(reader, ["10001"], control_schema="registry")
    assert enriched[2] is ptg and "company_catalog" not in first
    assert enriched[0]["company_catalog"] == enriched[1]["company_catalog"]
    assert enriched[0]["company_catalog"]["resolution"]["reporting_year"] == 2024
    query = reader.fetch.await_args.args[0]
    assert "registry_source_observation" in query and "company_group_registry" not in query


@pytest.mark.asyncio
async def test_company_metadata_changes_identity_but_reuses_provider_capture(monkeypatch):
    native_source, _reader, _resolver = await enriched_source(monkeypatch)
    previous = native_catalog_descriptor(native_aca_source())
    current = native_catalog_descriptor(native_source)
    assert digest(current) != digest(previous)
    assert native_provider_source_data(current) == native_provider_source_data(previous)
    inventory_by_field = {
        "contract": "registry-imported-native_source-inventory-v1",
        "fhir_semantics": [],
        "unavailable": [],
        "effective_date": "2026-11-17",
        "search_policy": "native-network-search-v1",
        "catalog_projector_version": "v1",
        "native_sources": [native_aca_source()],
    }
    before = ImportedSourceInventory(json.dumps(inventory_by_field)).token
    inventory_by_field["native_sources"] = [native_source]
    assert ImportedSourceInventory(json.dumps(inventory_by_field)).token != before
    projection = native_catalog_rows(
        current,
        generation=digest(current),
        accepted_at="2026-11-17T00:00:00Z",
        metadata_generation="a" * 64,
        networks={key: 7 for key in native_network_definitions(current)},
    )
    relationship_row = projection.relationship_rows[0]
    assert relationship_row[6:8] == (UUID(COMPANY), UUID(GROUP))
    relationship = json.loads(relationship_row[-1])
    assert relationship["company"]["display_name"] == "Source legal company"
    assert relationship["company_role"] == "insurer"
    assert relationship["resolution_reason"] == "aca_issuer_reporting_period_2024"
    assert relationship["group"]["relationship_kind"] == "reported_affiliation"
    assert relationship["group"]["valid_from"] is relationship["group"]["valid_to"] is None


@pytest.mark.parametrize(
    "source_factory,changes,error",
    [
        pytest.param(native_aca_source, {"network_tier": " "}, "aca_invalid", id="unidentified-aca-tier"),
        pytest.param(native_ptg_source, {"source_key": "unrelated-graph"}, "ptg_invalid", id="other-ptg-source"),
        pytest.param(
            native_ptg_source, {"network_names": [{"name": "Network"}]}, "ptg_invalid", id="invalid-ptg-label"
        ),
    ],
)
def test_native_catalog_refuses_malformed_definitions(source_factory, changes, error):
    source = source_factory()
    source["definitions"][0].update(changes)
    with pytest.raises(ValueError, match=f"registry_native_catalog_{error}"):
        native_catalog_descriptor(source)


def test_native_catalog_refuses_conflicting_tier_checksums():
    source = native_aca_source()
    (first_plan,) = source["definitions"]
    source["definitions"].append(first_plan | {"plan_id": "10001NY0000002", "checksum_network": 24})
    with pytest.raises(ValueError, match="registry_native_catalog_aca_ambiguous"):
        native_catalog_descriptor(source)


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"complete": False}, id="incomplete"),
        pytest.param({"input": native_ptg_source() | {"edition_id": "snapshot-two"}}, id="different-edition"),
    ],
)
def test_native_catalog_refuses_unusable_descriptor(changes):
    descriptor = native_catalog_descriptor(native_ptg_source())
    with pytest.raises(ValueError, match="registry_native_catalog_source_invalid"):
        validate_native_catalog_descriptor(descriptor | changes)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "conflicting", "state", "unknown_state", "labels"])
async def test_missing_conflicting_or_wrong_state_never_creates_company_link(monkeypatch, damage):
    resolution = issuer_resolution()
    if damage in {"missing", "conflicting"}:
        resolution.update(company_resolution_status=damage, legal_company=None, reported_group=None)
    elif damage in {"state", "unknown_state"}:
        resolution["business_state"] = "CA" if damage == "state" else None
    else:
        resolution["legal_company"]["source_labels"] = []
        resolution["issuer_source_labels"] = []
    source, _reader, _resolver = await enriched_source(monkeypatch, resolution)
    company, group, status, _reason = companies.aca_company_relationship(source, source["definitions"][0])
    assert company is group is None
    assert status == ("conflict" if damage in {"conflicting", "state"} else "unresolved")


@pytest.mark.asyncio
async def test_conflicting_group_keeps_legal_company_without_inventing_parent(monkeypatch):
    resolution = issuer_resolution()
    resolution.update(group_resolution_status="conflicting", resolution_status="conflicting", reported_group=None)
    source, _reader, _resolver = await enriched_source(monkeypatch, resolution)
    company, group, status, _reason = companies.aca_company_relationship(source, source["definitions"][0])
    assert company["company_id"] == COMPANY and group is None and status == "conflict"


@pytest.mark.asyncio
async def test_non_aca_sources_do_not_query_company_data():
    reader = AsyncMock()
    source = native_ptg_source()
    assert await companies.enrich_aca_company_sources(reader, [source], control_schema="registry") == [source]
    reader.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_company_lookup_batches_hundreds_of_distinct_issuers(monkeypatch):
    native_sources = []
    for issuer in range(10001, 10202):
        native_source = native_aca_source()
        native_source["definitions"][0]["issuer_id"] = str(issuer)
        native_sources.append(native_source)

    async def resolve(_connection, issuers, **_options):
        return tuple({**issuer_resolution(), "hios_issuer_id": issuer} for issuer in issuers)

    resolver = AsyncMock(side_effect=resolve)
    monkeypatch.setattr(companies, "read_registry_issuer_resolutions", resolver)
    reader = AsyncMock()
    reader.fetch.return_value = [{"group_id": GROUP, "group_kind": "naic_group"}]
    enriched = await companies.enrich_aca_company_sources(reader, native_sources, control_schema="registry")
    assert len(enriched) == 201 and [len(call.args[1]) for call in resolver.await_args_list] == [200, 1]
    reader.fetch.assert_awaited_once()
