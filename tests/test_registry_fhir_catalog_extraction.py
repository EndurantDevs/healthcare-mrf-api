# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog extraction retains source scope without allocating registry identities."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.registry_fhir_catalog_extraction import (
    FHIRCatalogExtractionError,
    extract_fhir_catalog_batch,
    fhir_source_uri,
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


@pytest.fixture
def published_scope(monkeypatch):
    from process.registry_fhir_catalog_published_read import FHIRCatalogPublishedRead

    verify = AsyncMock()
    select = Mock(
        return_value='SELECT * FROM ONLY "source_schema"."leaf_one" '
        'UNION ALL SELECT * FROM ONLY "source_schema"."leaf_two"'
    )
    monkeypatch.setattr(FHIRCatalogPublishedRead, "verify", verify)
    monkeypatch.setattr(FHIRCatalogPublishedRead, "resource_sql", select)
    return SimpleNamespace(read=object.__new__(FHIRCatalogPublishedRead), verify=verify, select=select)


@pytest.mark.asyncio
async def test_published_pages_keep_one_global_keyset_and_bound(published_scope):
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(),
    )

    async def fetch(*args):
        assert published_scope.verify.await_count == connection.fetch.await_count + 1
        return [[page_record("one")], [page_record("two")], []][connection.fetch.await_count - 1]

    connection.fetch.side_effect = fetch
    pages = [
        page
        async for page in iter_fhir_catalog_pages(
            connection,
            schema_name="source_schema",
            dataset_id="dataset",
            source_read=published_scope.read,
            page_rows=2,
            page_bytes=1024,
        )
    ]
    assert [page[0]["resource_id"] for page in pages] == ["one", "two"]
    assert connection.fetch.await_args_list[1].args[3:5] == ("Organization", "one")
    assert connection.fetch.await_args_list[2].args[3:5] == ("Organization", "two")
    query = connection.fetch.await_args_list[0].args[0]
    assert "FROM (" + published_scope.select.return_value + ") AS catalog_resources" in query
    assert "provider_directory_dataset_resource" not in query
    assert query.count("LIMIT $5") == 1 and query.count("sum(row_bytes)") == 1
    assert 'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $5' in query
    assert connection.fetch.await_args_list[0].args[-2:] == (2, 1024)
    assert published_scope.verify.await_count == 4
    for verification in published_scope.verify.await_args_list:
        assert verification.args == (connection,)
        assert verification.kwargs == {"schema_name": "source_schema", "dataset_id": "dataset"}


@pytest.mark.asyncio
async def test_published_page_reverification_refuses_before_the_next_page(published_scope):
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(return_value=[page_record("one")]),
    )
    published_scope.verify.side_effect = [None, None, ValueError("published read changed")]
    pages = iter_fhir_catalog_pages(
        connection, schema_name="source_schema", dataset_id="dataset", source_read=published_scope.read
    )
    assert (await anext(pages))[0]["resource_id"] == "one"
    with pytest.raises(ValueError, match="published read changed"):
        await anext(pages)
    assert connection.fetch.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["duck", "coordinates"])
async def test_published_page_scope_refuses_unverified_input_before_resource_sql(published_scope, mode):
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(),
    )
    source_read = published_scope.read
    if mode == "duck":
        source_read = SimpleNamespace(verify=AsyncMock(), resource_sql=Mock())
    else:
        published_scope.verify.side_effect = ValueError("dataset changed")
    with pytest.raises(ValueError):
        await anext(
            iter_fhir_catalog_pages(
                connection, schema_name="source_schema", dataset_id="dataset", source_read=source_read
            )
        )
    connection.fetch.assert_not_awaited()
    published_scope.select.assert_not_called()


@pytest.mark.asyncio
async def test_retained_source_pages_preserve_heap_and_native_verification(monkeypatch):
    from process.network_fhir_retained_capture import RetainedFHIRRead

    capture = SimpleNamespace(
        schema_name="source_schema",
        origin=SimpleNamespace(dataset_id="dataset"),
        relation_oids=(("provider_directory_dataset_resource", 123),),
    )
    source_read = object.__new__(RetainedFHIRRead)
    object.__setattr__(source_read, "capture", capture)
    verify = AsyncMock()
    monkeypatch.setattr(RetainedFHIRRead, "verify", verify)
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(side_effect=["repeatable read", 123]),
        fetch=AsyncMock(side_effect=[[page_record("one")], []]),
    )
    pages = [
        page
        async for page in iter_fhir_catalog_pages(
            connection, schema_name="source_schema", dataset_id="dataset", source_read=source_read
        )
    ]
    assert [page[0]["resource_id"] for page in pages] == ["one"]
    assert 'FROM "source_schema".provider_directory_dataset_resource' in connection.fetch.await_args.args[0]
    assert verify.await_count == 3
    assert all(call.args == (connection, capture) for call in verify.await_args_list)
    assert connection.fetchval.await_args.args == (
        "SELECT to_regclass($1)::oid::bigint",
        "source_schema.provider_directory_dataset_resource",
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "\ud800"},
        {"aliases": [""]},
        {"aliases": ["alias"] * 101},
        {"aliases": "alias"},
        {"identifiers": ["not-an-object"]},
        {"active": 1},
        {"part_of_ref": {}},
    ],
)
def test_malformed_organization_metadata_never_becomes_an_observation(payload):
    with pytest.raises(FHIRCatalogExtractionError):
        extract_fhir_catalog_batch([row("Organization", "network", payload)], profile="retained_normalized")


@pytest.mark.parametrize(
    "source_uri",
    ["", "https://[invalid", "file:///source", "https://example.test/?query=1", "https://user@example.test/fhir"],
)
def test_source_uri_refuses_ambiguous_or_credentialed_reference_namespaces(source_uri):
    with pytest.raises(FHIRCatalogExtractionError):
        fhir_source_uri(source_uri)
    assert fhir_source_uri("https://example.test/fhir///") == "https://example.test/fhir/"


@pytest.mark.parametrize(
    "resource",
    [
        {**row("Organization", "network", {}), "unexpected": True},
        row("Patient", "patient", {}),
        row("Organization", "bad/id", {}),
        {**row("Organization", "network", {}), "payload_json": []},
        row("InsurancePlan", "plan", {"period": []}, raw=True),
        row("InsurancePlan", "plan", {"network": ["Organization/network"]}, raw=True),
    ],
)
def test_extraction_rejects_malformed_resource_identity_and_reference_containers(resource):
    with pytest.raises(FHIRCatalogExtractionError):
        extract_fhir_catalog_batch([resource], profile="raw_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"page_rows": True},
        {"schema_name": "bad.schema"},
        {"dataset_id": " dataset "},
        {"resource_types": []},
        {"resource_types": ("Patient",)},
        {"resource_types": ("Organization", "Organization")},
        {"after_resource": ["Organization", "one"]},
        {"after_resource": ("Patient", "one")},
        {"after_resource": ("Organization", "bad/id")},
    ],
)
async def test_invalid_page_coordinates_refuse_before_any_database_read(options):
    connection = Mock(fetch=AsyncMock(), fetchval=AsyncMock())
    arguments_by_name = {"schema_name": "source_schema", "dataset_id": "dataset", **options}
    with pytest.raises(FHIRCatalogExtractionError):
        await anext(iter_fhir_catalog_pages(connection, **arguments_by_name))
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload,page_bytes", [("{", 1024), ([], 1024), ('{"name":"' + "x" * 500 + '"}', 300)])
async def test_page_decode_refuses_invalid_or_underreported_json_before_yield(payload, page_bytes):
    record_by_name = {**page_record("one", size=1), "payload_json": payload}
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(return_value=[record_by_name]),
    )
    with pytest.raises(FHIRCatalogExtractionError):
        await anext(
            iter_fhir_catalog_pages(
                connection, schema_name="source_schema", dataset_id="dataset", page_bytes=page_bytes
            )
        )
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_retained_page_refuses_replaced_heap_before_fetch(monkeypatch):
    from process.network_fhir_retained_capture import RetainedFHIRRead

    source_read = object.__new__(RetainedFHIRRead)
    object.__setattr__(
        source_read,
        "capture",
        SimpleNamespace(
            schema_name="source_schema",
            origin=SimpleNamespace(dataset_id="dataset"),
            relation_oids=(("provider_directory_dataset_resource", 123),),
        ),
    )
    monkeypatch.setattr(RetainedFHIRRead, "verify", AsyncMock())
    connection = Mock(
        is_in_transaction=Mock(return_value=True),
        fetchval=AsyncMock(side_effect=["repeatable read", 456]),
        fetch=AsyncMock(),
    )
    with pytest.raises(FHIRCatalogExtractionError, match="retained_read_changed"):
        await anext(
            iter_fhir_catalog_pages(
                connection, schema_name="source_schema", dataset_id="dataset", source_read=source_read
            )
        )
    connection.fetch.assert_not_awaited()
