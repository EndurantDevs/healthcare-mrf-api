# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed enriched reads retain exact pins, literal filters and last-good data."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_read as catalog
from process import registry_fhir_catalog_validation as validation
from process.registry_catalog_policy import RegistryOrganizationVisibility

COMPANY = "11111111-1111-4111-8111-111111111111"
GROUP = "33333333-3333-4333-8333-333333333333"


def relationship_document():
    return {
        "relationship_id": "b" * 64,
        "source_id": "sample-fhir",
        "plan": {
            "resource_id": "plan-one",
            "scope_path": "InsurancePlan/plan-one/plan/0",
            "display_name": "Example Gold Plan",
            "valid_from": None,
            "valid_to": None,
        },
        "organization_reference": "Organization/insurer-one",
        "company_role": "insurer",
        "company": {"company_id": COMPANY, "display_name": "Example Insurance"},
        "group": {
            "group_id": GROUP,
            "display_name": "Example Parent",
            "group_kind": "corporate_parent",
            "relationship_kind": "verified_ownership",
            "valid_from": "2025-01-01",
            "valid_to": None,
        },
        "resolution_status": "resolved",
        "resolution_reason": None,
    }


def test_manual_company_link_does_not_invent_a_fhir_role():
    relationship_by_field = {
        **relationship_document(),
        "source_id": "registry-manual",
        "plan": None,
        "organization_reference": None,
        "company_role": None,
    }
    assert catalog._relationship_document(relationship_by_field) == relationship_by_field
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._relationship_document({**relationship_by_field, "source_id": "sample-fhir"})


def binding_document():
    return {
        "namespace": "source_binding",
        "binding_id": "44444444-4444-4444-8444-444444444444",
        "revision": 1,
        "source_system": "fhir",
        "source_id": "sample-fhir",
        "dataset_schema": "sample_source",
        "dataset_id": "capture-one",
        "producer_id": "registry-retained-normalized-fhir-v1",
        "edition_id": "edition-one",
        "source_key": "network-one",
        "source_scope": {"organization_id": "network-one", "capture_sha256": "c" * 64},
        "evidence_id": "sample-evidence",
        "evidence_sha256": "d" * 64,
        "metadata_sha256": "e" * 64,
        "accepted_at": "2026-01-01T00:00:00Z",
        "status": "current",
    }


def catalog_binding_document():
    binding = binding_document()
    binding["producer_id"] = "registry-fhir-catalog-v1"
    binding["source_scope"] = {"organization_id": "network-one", "source_generation": "c" * 64}
    binding["evidence_id"] = "c" * 64 + ":Organization/network-one"
    return binding


def test_catalog_binding_scope_is_enriched_only_and_sealer_uses_same_validator():
    binding = catalog_binding_document()
    assert catalog._fhir_binding_document(binding) == binding
    validation._validate_document(
        "registry_fhir_catalog_binding",
        {"binding_id": binding["binding_id"], "source_id": binding["source_id"], "document_json": binding},
    )
    legacy_by_field = {key: entry for key, entry in binding.items() if key not in catalog._BINDING_METADATA}
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.catalog_binding_document(legacy_by_field)


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("producer_id", "registry-retained-normalized-fhir-v1"),
        ("source_system", "ptg"),
        ("source_key", "another-network"),
        ("evidence_id", "c" * 64 + ":Organization/another-network"),
        ("source_scope", {"organization_id": "network-one", "source_generation": "C" * 64}),
        ("source_scope", {"organization_id": "network_one", "source_generation": "c" * 64}),
        ("source_scope", {"organization_id": "network-one", "source_generation": "c" * 63}),
        ("source_scope", {"organization_id": "network-one", "source_generation": "c" * 64, "extra": True}),
        ("source_scope", {"organization_id": "network-one", "capture_sha256": "c" * 64}),
        ("extra", True),
    ],
)
def test_catalog_binding_scope_rejects_wrong_authority_and_malformed_fields(field, replacement):
    binding = catalog_binding_document()
    binding[field] = replacement
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._fhir_binding_document(binding)


def nested_document(entries, total=None):
    total = len(entries) if total is None else total
    return {
        "total": total,
        "offset": 0,
        "limit": 5,
        "items": entries,
        "next_offset": len(entries) if len(entries) < total else None,
    }


def network_document():
    return {
        "network_id": 7,
        "display_name": "Example Network",
        "aliases": ["Example Alias"],
        "archived": False,
        "revision": 3,
        "directory_available": True,
        "priceable": None,
        "benefit_codes": None,
        "matching_relationships": nested_document([relationship_document()]),
        "source_bindings": nested_document([binding_document()]),
    }


@pytest.fixture
def page_boundary(monkeypatch):
    connection = SimpleNamespace(
        fetchrow=AsyncMock(), fetchval=AsyncMock(return_value=True), is_in_transaction=lambda: True
    )
    manifest = SimpleNamespace(
        generation_id=9, manifest_sha256="f" * 64, approved_custom_revision=4, schema_name="sample_memberships"
    )
    snapshot_by_field = {
        "metadata_generation": "a" * 64,
        "network_generation": 9,
        "manifest_sha256": "f" * 64,
        "metadata_approved_revision": 6,
        "schema_name": "sample_metadata",
        "custody_json": {},
    }
    monkeypatch.setattr(catalog, "catalog_pin", AsyncMock(return_value=manifest))
    monkeypatch.setattr(catalog, "_metadata_pin", AsyncMock(return_value=snapshot_by_field))
    connection.fetchrow.return_value = {"total": 1, "rows_json": json.dumps([network_document()])}
    return connection, manifest, snapshot_by_field


@pytest.mark.parametrize(
    "change",
    [
        {"limit": 101},
        {"company_search": "ab"},
        {"plan_search": "  "},
        {"company_id": "company"},
        {"group_id": "00000000-0000-0000-0000-000000000000"},
        {"metadata_generation": "A" * 64},
        {"offset": 1},
        {"binding_offset": 1},
        {"relationship_offset": -1},
    ],
)
def test_reject_invalid_queries(change):
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog.FHIRCatalogQuery(**change)


def test_literal_unicode_search_and_continuation():
    query = catalog.FHIRCatalogQuery(
        company_search="  Česká_100%  ",
        plan_search="Gold\\Plan",
        offset=3,
        generation_id=9,
        metadata_generation="a" * 64,
        continuation_scope_sha256="b" * 64,
    )
    assert query.company_search == "Česká_100%"
    assert catalog._literal_pattern(query.company_search) == "%česká\\_100\\%%"
    assert catalog._literal_pattern(query.plan_search) == "%gold\\\\plan%"


@pytest.mark.asyncio
async def test_unattached_metadata_never_pins_or_reads_directory_tables(page_boundary):
    connection, _, snapshot = page_boundary
    snapshot.update(network_generation=None, manifest_sha256=None)
    network = network_document()
    network["directory_available"] = False
    connection.fetchrow.return_value["rows_json"] = json.dumps([network])
    page = await catalog.read_fhir_catalog(
        connection, catalog.FHIRCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
    )
    assert page["generation"] is None and page["approved_custom_revision"] is None
    assert page["metadata_generation"] == "a" * 64 and page["metadata_approved_revision"] == "6"
    assert page["items"][0]["directory_available"] is False
    catalog.catalog_pin.assert_not_awaited()
    assert "network_membership" not in connection.fetchrow.call_args.args[0]


@pytest.mark.asyncio
async def test_unattached_read_keeps_the_readonly_repeatable_snapshot_guard(page_boundary):
    connection, _, snapshot = page_boundary
    snapshot.update(network_generation=None, manifest_sha256=None)
    connection.fetchval.return_value = False
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="transaction_required"):
        await catalog.read_fhir_catalog(connection, catalog.FHIRCatalogQuery(), excluded_network_ids=())
    connection.fetchrow.assert_not_awaited()
    catalog.catalog_pin.assert_not_awaited()


@pytest.mark.parametrize("attachment", [(None, "f" * 64), (9, None), (None, None), (10, "f" * 64)])
@pytest.mark.asyncio
async def test_nullable_attachment_and_explicit_generation_must_agree(page_boundary, attachment):
    connection, _, snapshot = page_boundary
    snapshot.update(zip(("network_generation", "manifest_sha256"), attachment, strict=True))
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await catalog.read_fhir_catalog(
            connection,
            catalog.FHIRCatalogQuery(generation_id=9),
            excluded_network_ids=(),
            control_schema="sample_control",
        )
    catalog.catalog_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_only_continuation_binds_optional_directory_attachment(page_boundary):
    connection, _, snapshot = page_boundary
    page = await catalog.read_fhir_catalog(
        connection, catalog.FHIRCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
    )
    pinned = catalog.FHIRCatalogQuery(
        metadata_generation="a" * 64, continuation_scope_sha256=page["continuation_scope_sha256"]
    )
    assert (await catalog.read_fhir_catalog(connection, pinned, excluded_network_ids=()))["generation"] == "9"
    snapshot.update(network_generation=None, manifest_sha256=None)
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await catalog.read_fhir_catalog(connection, pinned, excluded_network_ids=())


@pytest.mark.asyncio
async def test_last_good_keeps_identity_and_serving(page_boundary):
    connection, manifest, snapshot = page_boundary
    query = catalog.FHIRCatalogQuery()
    current = await catalog.read_fhir_catalog(
        connection, query, excluded_network_ids=(), control_schema="sample_control"
    )
    network = network_document()
    network["source_bindings"]["items"][0]["status"] = "last_good"
    connection.fetchrow.return_value["rows_json"] = json.dumps([network])
    retained = await catalog.read_fhir_catalog(
        connection, query, excluded_network_ids=(), control_schema="sample_control"
    )
    assert retained["items"][0]["network_id"] == current["items"][0]["network_id"] == 7
    assert retained["items"][0]["directory_available"] is True
    assert retained["generation"] == current["generation"] == str(manifest.generation_id)
    assert retained["metadata_approved_revision"] == str(snapshot["metadata_approved_revision"])
    assert retained["approved_custom_revision"] == "4"
    sql = connection.fetchrow.call_args.args[0]
    assert '"sample_metadata".registry_fhir_catalog_relationship' in sql
    assert '"sample_memberships".network_membership' in sql


@pytest.mark.asyncio
async def test_policy_and_same_edge_parameters(page_boundary):
    connection, _, _ = page_boundary
    policy = RegistryOrganizationVisibility((COMPANY,), (GROUP,))
    connection.fetchrow.return_value = {"total": 0, "rows_json": "[]"}
    await catalog.read_fhir_catalog(
        connection,
        catalog.FHIRCatalogQuery(company_id=COMPANY, plan_search="Gold"),
        excluded_network_ids=(8,),
        organization_policy=policy,
        control_schema="sample_control",
    )
    sql, *parameters = connection.fetchrow.call_args.args
    assert parameters[1] == (8,)
    assert str(parameters[2][0]) == COMPANY and str(parameters[3][0]) == GROUP
    assert parameters[7] == "%gold%"
    assert "edge.company_id=$5" in sql and "edge.plan_search LIKE $8" in sql
    assert "DISTINCT ON (network_id,visible_document)" in sql


@pytest.mark.asyncio
async def test_byte_short_page_has_explicit_continuation(page_boundary):
    connection, _, _ = page_boundary
    connection.fetchrow.return_value["total"] = 20
    page = await catalog.read_fhir_catalog(
        connection, catalog.FHIRCatalogQuery(limit=10), excluded_network_ids=(), control_schema="sample_control"
    )
    assert len(page["items"]) == 1 and page["next_offset"] == 1 and page["limit"] == 10


@pytest.mark.asyncio
async def test_missing_or_hidden_network_refuses_detail(page_boundary):
    connection, _, _ = page_boundary
    connection.fetchrow.return_value = {"total": 0, "rows_json": "[]"}
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="detail_denied"):
        await catalog.read_fhir_catalog(
            connection,
            catalog.FHIRCatalogQuery(limit=1),
            excluded_network_ids=(),
            control_schema="sample_control",
            network_id=7,
        )


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("source_bindings", "items", 0, "extra"), True),
        (("source_bindings", "items", 0, "source_scope", "capture_sha256"), "A" * 64),
        (("source_bindings", "items", 0, "producer_id"), "unadmitted-producer"),
        (("source_bindings", "items", 0, "accepted_at"), "2026-01-01T00:00:00+01:00"),
        (("source_bindings", "items", 0, "status"), "removed"),
        (("matching_relationships", "items", 0, "group", "group_id"), None),
    ],
)
def test_reject_malformed_nested_documents(path, replacement):
    network = network_document()
    parent = network
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = replacement
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._network_item(network, catalog.FHIRCatalogQuery())


def test_nested_continuation_is_exact_and_distinct():
    network = network_document()
    network["matching_relationships"]["total"] = 6
    network["matching_relationships"]["next_offset"] = 1
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._network_item(network, catalog.FHIRCatalogQuery())
    network = network_document()
    network["matching_relationships"] = nested_document([relationship_document(), relationship_document()])
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._network_item(network, catalog.FHIRCatalogQuery())


def test_legacy_companies_and_boolean_offsets_refuse():
    network = network_document()
    network["companies"] = []
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._network_item(network, catalog.FHIRCatalogQuery())
    network = network_document()
    network["matching_relationships"]["offset"] = False
    with pytest.raises(catalog.RegistryNetworkCatalogError):
        catalog._network_item(network, catalog.FHIRCatalogQuery())


@pytest.mark.asyncio
async def test_continuation_binds_policy_and_filters(page_boundary):
    connection, _, _ = page_boundary
    query = catalog.FHIRCatalogQuery(plan_search="Gold")
    page = await catalog.read_fhir_catalog(
        connection, query, excluded_network_ids=(), control_schema="sample_control", access_scope_sha256="1" * 64
    )
    pinned = catalog.FHIRCatalogQuery(
        generation_id=9,
        metadata_generation="a" * 64,
        plan_search="Gold",
        continuation_scope_sha256=page["continuation_scope_sha256"],
    )
    for changes in ({"access_scope_sha256": "2" * 64}, {"query": replace(pinned, plan_search="Silver")}):
        parameters = {"access_scope_sha256": "1" * 64, "query": pinned} | changes
        with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
            await catalog.read_fhir_catalog(
                connection, excluded_network_ids=(), control_schema="sample_control", **parameters
            )
    changed_policy = catalog.RegistryOrganizationVisibility((COMPANY,), ())
    initial = await catalog.read_fhir_catalog(
        connection, catalog.FHIRCatalogQuery(), excluded_network_ids=(), control_schema="sample_control"
    )
    pinned = catalog.FHIRCatalogQuery(
        generation_id=9, metadata_generation="a" * 64, continuation_scope_sha256=initial["continuation_scope_sha256"]
    )
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await catalog.read_fhir_catalog(
            connection,
            pinned,
            excluded_network_ids=(),
            organization_policy=changed_policy,
            control_schema="sample_control",
        )


@pytest.mark.asyncio
async def test_scope_allows_list_to_detail(page_boundary):
    connection, _, _ = page_boundary
    page = await catalog.read_fhir_catalog(
        connection,
        catalog.FHIRCatalogQuery(company_search="Example"),
        excluded_network_ids=(),
        control_schema="sample_control",
    )
    query = catalog.FHIRCatalogQuery(
        limit=1,
        company_search=" Example ",
        generation_id=9,
        metadata_generation="a" * 64,
        continuation_scope_sha256=page["continuation_scope_sha256"],
    )
    detail = await catalog.read_fhir_catalog(
        connection, query, excluded_network_ids=(), control_schema="sample_control", network_id=7
    )
    assert detail["continuation_scope_sha256"] == page["continuation_scope_sha256"]
    assert detail["limit"] == 1 and detail["items"][0]["network_id"] == 7
