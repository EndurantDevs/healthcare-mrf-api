# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog extraction retains source scope without allocating registry identities."""

import json
from dataclasses import asdict
from unittest.mock import AsyncMock, Mock

import pytest

from process.registry_fhir_catalog_extraction import (
    FHIRCatalogExtractionError,
    extract_fhir_catalog_batch,
    iter_fhir_catalog_pages,
)


def row(resource_type, resource_id, payload, *, raw=False):
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_hash": "a" * 64,
        "payload_json": {"resourceType": resource_type, "id": resource_id, **payload} if raw else payload,
    }


def test_raw_declared_and_ordinary_organizations_remain_source_observations():
    batch = extract_fhir_catalog_batch(
        [
            row(
                "Organization",
                "network",
                {"name": "Example Network", "type": [{"text": "ntwk"}], "active": False},
                raw=True,
            ),
            row("Organization", "owner", {"name": "Example Insurer", "alias": ["Example"]}, raw=True),
        ],
        profile="raw_fhir",
    )
    assert [
        (organization.resource_id, organization.declared_network, organization.status)
        for organization in batch.organizations
    ] == [("network", True, "inactive"), ("owner", False, "unknown")]
    assert batch.organizations[1].aliases == ("Example",)
    assert batch.edges == () and batch.source_rows == 2
    assert all("archived" not in asdict(organization) for organization in batch.organizations)


def test_raw_plan_edges_keep_exact_scope_and_distinct_owner_and_administrator():
    plan = row(
        "InsurancePlan",
        "plan",
        {
            "name": "Example Choice",
            "alias": ["Choice"],
            "status": "active",
            "identifier": [{"system": "urn:example:product", "value": "product"}],
            "ownedBy": {"reference": "Organization/insurer"},
            "administeredBy": {"reference": "Organization/administrator"},
            "period": {"start": "2026-01-01", "end": "2026-12-31"},
            "coverageArea": [{"reference": "Location/state"}],
            "network": [{"reference": "Organization/network"}],
            "plan": [{"identifier": [{"value": "plan-variant"}], "network": [{"reference": "Organization/network"}]}],
            "coverage": [
                {
                    "type": {"coding": [{"code": "medical"}]},
                    "network": [{"reference": "Organization/network-two"}],
                    "benefit": [{"type": {"text": "Medical"}, "requirement": "Referral"}],
                }
            ],
        },
        raw=True,
    )
    batch = extract_fhir_catalog_batch([plan], profile="raw_fhir")
    assert [edge.scope_path for edge in batch.edges] == ["/network/0", "/plan/0/network/0", "/coverage/0/network/0"]
    assert [edge.network_resource_id for edge in batch.edges] == ["network", "network", "network-two"]
    assert all(edge.plan_resource_id == "plan" and edge.plan_name == "Example Choice" for edge in batch.edges)
    assert all(
        edge.owner_ref == "Organization/insurer"
        and edge.administrator_ref == "Organization/administrator"
        and edge.valid_from == "2026-01-01"
        and edge.valid_to == "2026-12-31"
        for edge in batch.edges
    )
    assert batch.edges[1].scope_identifiers == ({"value": "plan-variant"},)
    assert batch.edges[2].benefits[0]["requirement"] == "Referral"
    assert batch.edges[2].geography == ("Location/state",)


def test_normalized_aggregate_does_not_invent_top_level_scope_or_merge_networks():
    batch = extract_fhir_catalog_batch(
        [
            row("Organization", "unreferenced", {"name": "Shared Name", "type_codes": [{"code": "ntwk"}]}),
            row("Organization", "network", {"name": "Shared Name", "type_codes": [{"text": "ntwk"}]}),
            row(
                "InsurancePlan",
                "plan",
                {
                    "name": "Choice",
                    "aliases": ["Option"],
                    "owned_by_ref": "Organization/owner",
                    "network_refs": ["Organization/network"],
                    "plan_backbones": [
                        {"network_refs": ["Organization/network"], "identifiers": [{"value": "variant"}]}
                    ],
                    "coverage": [
                        {"network_refs": ["Organization/network"], "benefits": [{"type_codes": [{"code": "medical"}]}]}
                    ],
                },
            ),
        ],
        profile="retained_normalized",
    )
    assert len(batch.organizations) == 2 and all(organization.declared_network for organization in batch.organizations)
    assert [edge.scope_path for edge in batch.edges] == [
        "/normalized_aggregate/0",
        "/plan/0/network/0",
        "/coverage/0/network/0",
    ]
    assert batch.edges[2].benefits == ({"type_codes": [{"code": "medical"}]},)
    assert all(edge.plan_aliases == ("Option",) for edge in batch.edges)


def test_published_normalized_payloads_keep_normalized_scope_without_claiming_raw_fhir():
    rows = [
        row("Organization", "network", {"name": "Example", "type_codes": [{"text": "ntwk"}]}),
        row(
            "InsurancePlan",
            "plan",
            {
                "name": "Choice",
                "owned_by_ref": "Organization/insurer",
                "network_refs": ["Organization/network"],
                "coverage": [{"network_refs": ["Organization/network"]}],
            },
        ),
    ]
    batch = extract_fhir_catalog_batch(rows, profile="published_normalized")
    assert batch.organizations[0].declared_network
    assert [edge.scope_path for edge in batch.edges] == ["/normalized_aggregate/0", "/coverage/0/network/0"]
    assert all(edge.owner_ref == "Organization/insurer" for edge in batch.edges)
    with pytest.raises(FHIRCatalogExtractionError):
        extract_fhir_catalog_batch(rows, profile="raw_fhir")


def test_foreign_missing_and_malformed_references_remain_unresolved():
    batch = extract_fhir_catalog_batch(
        [
            row(
                "InsurancePlan",
                "plan",
                {
                    "network": [
                        {"reference": "https://example.test/fhir/Organization/local"},
                        {"reference": "https://foreign.test/fhir/Organization/local"},
                        {"reference": "https://example.test/fhir-other/Organization/local"},
                        {"identifier": {"value": "unknown"}},
                        {"reference": " Organization/local "},
                        {"reference": "Organization/not-imported"},
                    ]
                },
                raw=True,
            )
        ],
        profile="raw_fhir",
        source_uri="https://example.test/fhir",
    )
    assert [(edge.network_resource_id, edge.resolution_reason) for edge in batch.edges] == [
        ("local", None),
        (None, "reference_outside_source"),
        (None, "reference_outside_source"),
        (None, "reference_missing"),
        (None, "reference_invalid"),
        ("not-imported", None),
    ]
    assert batch.edges[3].network_ref is None


def test_role_and_affiliation_references_include_recognized_raw_extensions():
    batch = extract_fhir_catalog_batch(
        [
            row(
                "PractitionerRole",
                "role",
                {
                    "network": [{"reference": "Organization/direct"}],
                    "extension": [
                        {
                            "url": "https://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference",
                            "valueReference": {"reference": "Organization/extended"},
                        }
                    ],
                },
                raw=True,
            ),
            row(
                "OrganizationAffiliation",
                "affiliation",
                {"network": [{"reference": "Organization/affiliated"}]},
                raw=True,
            ),
        ],
        profile="raw_fhir",
    )
    assert [edge.network_resource_id for edge in batch.edges] == ["direct", "extended", "affiliated"]
    assert all(edge.plan_resource_id is None and edge.plan_name is None for edge in batch.edges)


def test_replay_and_rename_never_allocate_or_change_source_identity():
    original_rows = [row("Organization", "network", {"name": "Original", "type_codes": [{"code": "ntwk"}]})]
    first = extract_fhir_catalog_batch(original_rows, profile="retained_normalized")
    assert first == extract_fhir_catalog_batch(original_rows, profile="retained_normalized")
    renamed = extract_fhir_catalog_batch(
        [row("Organization", "network", {"name": "Renamed", "type_codes": [{"code": "ntwk"}], "active": False})],
        profile="retained_normalized",
    )
    assert first.organizations[0].resource_id == renamed.organizations[0].resource_id
    assert set(asdict(renamed.organizations[0])) == {
        "resource_id",
        "name",
        "aliases",
        "declared_network",
        "status",
        "identifiers",
        "source_hash",
        "part_of_ref",
    }


def test_unreferenced_plan_and_nested_metadata_remain_unassigned_observations():
    batch = extract_fhir_catalog_batch(
        [
            row(
                "InsurancePlan",
                "unreferenced",
                {
                    "name": "Unassigned Choice",
                    "owned_by_ref": "Organization/insurer",
                    "plan_backbones": [{"identifiers": [{"value": "variant"}], "network_refs": []}],
                    "coverage": [{"benefits": [{"requirement": "Referral"}]}],
                },
            )
        ],
        profile="retained_normalized",
    )
    assert [edge.scope_path for edge in batch.edges] == [
        "/normalized_aggregate/0",
        "/plan/0/network/0",
        "/coverage/0/network/0",
    ]
    assert all(
        edge.network_ref is None and edge.network_resource_id is None and edge.resolution_reason == "reference_missing"
        for edge in batch.edges
    )
    assert all(
        edge.plan_name == "Unassigned Choice" and edge.owner_ref == "Organization/insurer" for edge in batch.edges
    )
    assert batch.edges[1].scope_identifiers == ({"value": "variant"},)
    assert batch.edges[2].benefits == ({"requirement": "Referral"},)


@pytest.mark.parametrize("mutation", ["hash", "duplicate", "profile", "identity", "name", "input", "output"])
def test_invalid_or_overlarge_batch_refuses_without_truncation(mutation):
    rows = [row("Organization", "network", {"name": "Example"})]
    options_by_name = {"profile": "retained_normalized"}
    if mutation == "hash":
        rows[0]["payload_hash"] = "A" * 64
    if mutation == "duplicate":
        rows *= 2
    if mutation == "profile":
        options_by_name["profile"] = "unknown"
    if mutation == "identity":
        rows[0]["payload_json"]["resource_id"] = "different"
    if mutation == "name":
        rows[0]["payload_json"]["name"] = "x" * 513
    if mutation in {"input", "output"}:
        options_by_name["max_" + mutation + "_bytes"] = 1
    with pytest.raises(FHIRCatalogExtractionError):
        extract_fhir_catalog_batch(rows, **options_by_name)


def page_record(resource_id, *, size=400):
    return {
        **row("Organization", resource_id, {"name": "Example"}),
        "row_bytes": size,
        "payload_json": json.dumps({"name": "Example"}),
    }


@pytest.mark.asyncio
async def test_page_iterator_advances_exact_keyset_and_never_writes_or_commits():
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(side_effect=[[page_record("one")], [page_record("two")], []]),
    )
    pages = [
        page
        async for page in iter_fhir_catalog_pages(
            connection, schema_name="source_schema", dataset_id="dataset", page_rows=2, page_bytes=1024
        )
    ]
    assert [page[0]["resource_id"] for page in pages] == ["one", "two"]
    assert connection.fetch.await_args_list[1].args[3:5] == ("Organization", "one")
    assert connection.fetch.await_args_list[2].args[3:5] == ("Organization", "two")
    query = connection.fetch.await_args_list[0].args[0]
    assert 'resource_type COLLATE "C"' in query and "sum(row_bytes)" in query
    assert not any(call[0] in {"execute", "executemany", "commit", "transaction"} for call in connection.mock_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["no-transaction", "read-committed", "oversize"])
async def test_page_iterator_refuses_unpinned_or_overlarge_pages(mode):
    connection = Mock(
        is_in_transaction=Mock(return_value=mode != "no-transaction"),
        fetchval=AsyncMock(return_value="read committed" if mode == "read-committed" else "repeatable read"),
        fetch=AsyncMock(return_value=[page_record("one", size=2048)]),
    )
    with pytest.raises(FHIRCatalogExtractionError):
        async for page in iter_fhir_catalog_pages(
            connection, schema_name="source_schema", dataset_id="dataset", page_bytes=1024
        ):
            pytest.fail(f"refused page unexpectedly yielded {len(page)} rows")
    if mode != "oversize":
        connection.fetch.assert_not_awaited()
