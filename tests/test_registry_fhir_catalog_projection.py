# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Projection COPY rows preserve allocated identities and exact source evidence."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
from uuid import UUID

import pytest

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_enrichment import FHIRCatalogEnrichmentContext, enrich_fhir_catalog_edges
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_projection import (
    BINDING_COLUMNS,
    NETWORK_COLUMNS,
    RELATIONSHIP_COLUMNS,
    FHIRCatalogProjectionError,
    SourceCatalogContext,
    format_fhir_catalog_projection,
    format_reviewed_fhir_product_projection,
)
from process.registry_fhir_catalog_read import _fhir_binding_document, _relationship_document

COMPANY = "11111111-1111-4111-8111-111111111111"
LEGACY_ALIAS = "22222222-2222-4222-8222-222222222222"


def source_batch():
    resources = [
        (
            "Organization",
            "network",
            {"name": "Source Network", "aliases": ["Source Alias"], "active": False, "type_codes": [{"code": "ntwk"}]},
        ),
        ("Organization", "insurer", {"name": "Source Insurer"}),
        (
            "InsurancePlan",
            "plan",
            {"name": "Gold Plan", "network_refs": ["Organization/network"], "owned_by_ref": "Organization/insurer"},
        ),
    ]
    batch = extract_fhir_catalog_batch(
        tuple(
            {
                "resource_type": resource_type,
                "resource_id": resource_id,
                "payload_hash": "a" * 64,
                "payload_json": resource_payload,
            }
            for resource_type, resource_id, resource_payload in resources
        ),
        profile="retained_normalized",
    )
    context = FHIRCatalogEnrichmentContext(
        "example-fhir",
        datetime(2026, 1, 1).date(),
        ({"company_id": COMPANY, "display_name": "Reviewed Insurer"},),
        (),
        "sample-endpoint",
        (("insurer", COMPANY),),
    )
    relationships = enrich_fhir_catalog_edges(batch.organizations, batch.edges, context)
    return batch.organizations, relationships


def source_context():
    return SourceCatalogContext(
        coordinates=RegistryNetworkSourceCoordinates(
            "fhir",
            "example-fhir",
            "sample_source",
            "dataset-one",
            "registry-retained-normalized-fhir-v1",
            "edition-one",
        ),
        source_generation="b" * 64,
        metadata_generation="c" * 64,
        accepted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        profile="retained_normalized",
        capture_sha256="d" * 64,
    )


def project(*, organizations=None, relationships=None, context=None, canonical_networks=()):
    source_organizations, source_relationships = source_batch()
    return format_fhir_catalog_projection(
        source_organizations if organizations is None else organizations,
        source_relationships if relationships is None else relationships,
        (("network", 7),),
        source_context() if context is None else context,
        canonical_networks=canonical_networks,
    )


def test_copy_rows_share_exact_binding_identity_and_producer_qualified_capture_scope():
    batch = project()
    assert all(len(row) == len(NETWORK_COLUMNS) for row in batch.network_rows)
    assert all(len(row) == len(RELATIONSHIP_COLUMNS) for row in batch.relationship_rows)
    assert all(len(row) == len(BINDING_COLUMNS) for row in batch.binding_rows)
    network_by_field = dict(zip(NETWORK_COLUMNS, batch.network_rows[0], strict=True))
    binding_by_field = dict(zip(BINDING_COLUMNS, batch.binding_rows[0], strict=True))
    document = json.loads(binding_by_field["document_json"])
    assert network_by_field["network_id"] == 7 and network_by_field["display_name"] == "Source Network"
    assert network_by_field["archived"] is False
    assert type(binding_by_field["binding_id"]) is UUID and binding_by_field["binding_id"].version == 5
    assert document["binding_id"] == str(binding_by_field["binding_id"])
    assert document["source_scope"] == {"organization_id": "network", "capture_sha256": "d" * 64}
    assert document["accepted_at"] == "2026-01-01T00:00:00Z" and document["status"] == "current"
    assert document["evidence_sha256"] == "a" * 64
    for row in batch.relationship_rows:
        relationship_by_field = dict(zip(RELATIONSHIP_COLUMNS, row, strict=True))
        assert relationship_by_field["network_id"] == 7 and relationship_by_field["source_generation"] == "b" * 64
        assert relationship_by_field["binding_id"] == binding_by_field["binding_id"]
        assert relationship_by_field["company_id"] == UUID(COMPANY)
        _relationship_document(json.loads(relationship_by_field["document_json"]))
    _fhir_binding_document(document)


def test_canonical_manual_fields_and_explicit_archive_are_preserved_exactly():
    head_by_field = {
        "network_id": 7,
        "display_name": "Reviewed Name",
        "aliases": ["Reviewed Alias"],
        "archived": True,
        "revision": 9,
        "benefit_codes": ["medical", "vision"],
    }
    batch = project(canonical_networks=(head_by_field,))
    network_by_field = dict(zip(NETWORK_COLUMNS, batch.network_rows[0], strict=True))
    assert network_by_field["display_name"] == head_by_field["display_name"]
    assert json.loads(network_by_field["aliases"]) == head_by_field["aliases"]
    assert network_by_field["archived"] is True and network_by_field["revision"] == 9
    assert network_by_field["benefit_codes"] == ["medical", "vision"]
    assert network_by_field["search_name"] == "reviewed name\nreviewed alias"
    assert head_by_field["display_name"] == "Reviewed Name"


def test_source_disappearance_changes_freshness_without_changing_network_or_binding_identity():
    first = project()
    last_good = project(context=replace(source_context(), status="last_good", metadata_generation="e" * 64))
    assert first.network_rows[0][1:] == last_good.network_rows[0][1:]
    assert first.binding_rows[0][1:4] == last_good.binding_rows[0][1:4]
    assert json.loads(last_good.binding_rows[0][-1])["status"] == "last_good"
    assert project() == first


def test_legacy_profile_requires_genuine_alias_and_never_synthesizes_one_from_hashes():
    retained = source_context()
    legacy = replace(
        retained,
        coordinates=replace(retained.coordinates, producer_id="legacy-fhir-v1"),
        profile="raw_fhir",
        capture_sha256=None,
        legacy_aliases=(("network", LEGACY_ALIAS, "reviewed-source"),),
    )
    binding_by_field = json.loads(project(context=legacy).binding_rows[0][-1])
    assert binding_by_field["source_scope"] == {
        "organization_id": "network",
        "legacy_uuid": LEGACY_ALIAS,
        "alias_scope": "reviewed-source",
    }
    with pytest.raises(FHIRCatalogProjectionError):
        project(context=replace(legacy, legacy_aliases=()))
    with pytest.raises(FHIRCatalogProjectionError):
        project(context=replace(retained, legacy_aliases=legacy.legacy_aliases))


def test_published_normalized_requires_metadata_scope_without_legacy_or_capture_claims():
    retained = source_context()
    published = replace(
        retained,
        profile="published_normalized",
        capture_sha256=None,
        coordinates=replace(retained.coordinates, producer_id="registry-fhir-catalog-v1"),
    )
    binding_by_field = json.loads(project(context=published).binding_rows[0][-1])
    assert binding_by_field["producer_id"] == "registry-fhir-catalog-v1"
    assert binding_by_field["source_key"] == "network"
    assert binding_by_field["source_scope"] == {"organization_id": "network", "source_generation": "b" * 64}
    assert binding_by_field["evidence_id"] == "b" * 64 + ":Organization/network"
    reviewed = replace(published, legacy_aliases=(("network", LEGACY_ALIAS, "reviewed-source"),))
    assert project(context=reviewed).binding_rows == project(context=published).binding_rows
    for context in (
        replace(published, capture_sha256="d" * 64),
        replace(published, legacy_aliases=(("network", "a" * 64, "reviewed-source"),)),
        replace(published, coordinates=retained.coordinates),
        replace(published, coordinates=replace(published.coordinates, producer_id="other-fhir-producer")),
        replace(published, source_generation="invalid"),
        replace(published, source_generation="B" * 64),
    ):
        with pytest.raises(FHIRCatalogProjectionError):
            project(context=context)


def test_source_fingerprints_do_not_become_network_identity():
    organizations, relationships = source_batch()
    first = project()
    changed = (replace(relationships[0], plan_search="corrected alias"), *relationships[1:])
    second = project(
        organizations=organizations,
        relationships=changed,
        context=replace(source_context(), source_generation="f" * 64),
    )
    first_binding, second_binding = json.loads(first.binding_rows[0][-1]), json.loads(second.binding_rows[0][-1])
    assert first.network_rows == second.network_rows
    assert first_binding["binding_id"] == second_binding["binding_id"]
    assert first_binding["metadata_sha256"] != second_binding["metadata_sha256"]
    assert first_binding["evidence_sha256"] == second_binding["evidence_sha256"]


def test_binding_fingerprints_are_independent_of_relationship_page_boundaries():
    organizations, relationships = source_batch()
    first = project(organizations=organizations, relationships=relationships[:1])
    second = project(organizations=organizations, relationships=relationships[1:])
    combined = project(organizations=organizations, relationships=relationships)
    assert first.binding_rows == second.binding_rows == combined.binding_rows
    assert first.network_rows == second.network_rows == combined.network_rows


def test_several_source_organizations_can_share_one_reviewed_canonical_network():
    organizations, relationships = source_batch()
    other = replace(organizations[0], resource_id="network-two", name="Other source name")
    batch = format_fhir_catalog_projection(
        (*organizations, other), relationships, (("network-two", 7), ("network", 7)), source_context()
    )
    assert len(batch.network_rows) == 1 and batch.network_rows[0][1] == 7
    assert len(batch.binding_rows) == 2 and len({row[2] for row in batch.binding_rows}) == 2


def test_unmapped_and_unassigned_relationships_are_counted_without_fabricated_bindings():
    organizations, relationships = source_batch()
    unassigned = replace(relationships[0], network_resource_id=None)
    batch = format_fhir_catalog_projection(organizations, (unassigned,), (), source_context())
    assert batch.network_rows == batch.binding_rows == batch.relationship_rows == ()
    assert batch.unresolved_relationships == 1
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(organizations, relationships, (("missing-organization", 8),), source_context())


@pytest.mark.parametrize("mutation", ["producer", "capture", "naive-time", "status", "network-id", "output-size"])
def test_invalid_context_or_overlarge_copy_batch_is_rejected(mutation):
    organizations, relationships = source_batch()
    context = source_context()
    network_ids = (("network", 7),)
    options_by_name = {}
    if mutation == "producer":
        context = replace(context, coordinates=replace(context.coordinates, producer_id="unadmitted-profile"))
    if mutation == "capture":
        context = replace(context, capture_sha256="A" * 64)
    if mutation == "naive-time":
        context = replace(context, accepted_at=datetime(2026, 1, 1))
    if mutation == "status":
        context = replace(context, status="archived")
    if mutation == "network-id":
        network_ids = (("network", True),)
    if mutation == "output-size":
        options_by_name["max_output_bytes"] = 1
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(organizations, relationships, network_ids, context, **options_by_name)


def product_projection_inputs():
    organizations, relationships = source_batch()
    context = source_context()
    return {
        "organizations": organizations,
        "relationships": relationships,
        "context": context,
        "binding": {
            "namespace": "source_binding",
            "binding_id": "44444444-4444-4444-8444-444444444444",
            "revision": 3,
            **asdict(context.coordinates),
            "source_key": "InsurancePlan/plan",
            "source_scope": {
                "insurance_plan_id": "plan",
                "capture_sha256": context.capture_sha256,
                "product_network_id": 7,
            },
            "evidence_id": "reviewed-plan",
            "evidence_sha256": "e" * 64,
        },
        "canonical_network": {
            "network_id": 7,
            "display_name": "Reviewed Product",
            "aliases": ["Manual Alias"],
            "archived": False,
            "revision": 9,
        },
        "plan_source_hash": "a" * 64,
        "organization_witness_sha256": "b" * 64,
    }


def test_reviewed_product_keeps_actual_binding_and_manual_network_untouched():
    arguments_by_name = product_projection_inputs()
    original = deepcopy(arguments_by_name)
    batch = format_reviewed_fhir_product_projection(**arguments_by_name)
    assert arguments_by_name == original and batch.network_rows == ()
    assert len(batch.binding_rows) == 1 and batch.relationship_rows
    binding = arguments_by_name["binding"]
    binding_by_field = dict(zip(BINDING_COLUMNS, batch.binding_rows[0], strict=True))
    document = json.loads(binding_by_field["document_json"])
    assert binding_by_field["binding_id"] == UUID(binding["binding_id"])
    assert {key: document[key] for key in binding} == binding
    assert document["accepted_at"] == "2026-01-01T00:00:00Z" and document["status"] == "current"
    assert len(document["metadata_sha256"]) == 64
    relationship_by_field = dict(zip(RELATIONSHIP_COLUMNS, batch.relationship_rows[0], strict=True))
    original_relationship = arguments_by_name["relationships"][0]
    assert relationship_by_field["binding_id"] == UUID(binding["binding_id"])
    assert relationship_by_field["network_id"] == 7
    assert relationship_by_field["company_search"] == original_relationship.company_search
    assert relationship_by_field["plan_search"] == original_relationship.plan_search
    relationship = json.loads(relationship_by_field["document_json"])
    assert relationship["relationship_id"] != original_relationship.document["relationship_id"]
    assert {key: value for key, value in relationship.items() if key != "relationship_id"} in [
        {key: value for key, value in candidate.document.items() if key != "relationship_id"}
        for candidate in arguments_by_name["relationships"]
    ]


def test_shared_organization_products_have_separate_reviewed_relationship_ids():
    first = product_projection_inputs()
    second = deepcopy(first)
    second["canonical_network"]["network_id"] = 8
    second["binding"]["source_scope"]["product_network_id"] = 8
    second["binding"]["binding_id"] = "55555555-5555-4555-8555-555555555555"
    one = format_reviewed_fhir_product_projection(**first)
    two = format_reviewed_fhir_product_projection(**second)
    automatic = project()
    assert one.network_rows == two.network_rows == ()
    assert len({one.relationship_rows[0][1], two.relationship_rows[0][1], automatic.relationship_rows[0][1]}) == 3
    assert {one.relationship_rows[0][2], two.relationship_rows[0][2]} == {7, 8}
    assert {one.relationship_rows[0][5], two.relationship_rows[0][5]} == {
        UUID(first["binding"]["binding_id"]),
        UUID(second["binding"]["binding_id"]),
    }


def test_reviewed_binding_fingerprint_is_page_independent_and_preserves_freshness():
    arguments_by_name = product_projection_inputs()
    relationships = arguments_by_name["relationships"]
    full = format_reviewed_fhir_product_projection(**arguments_by_name)
    for page in (relationships[:1], relationships[1:]):
        assert (
            format_reviewed_fhir_product_projection(**(arguments_by_name | {"relationships": page})).binding_rows
            == full.binding_rows
        )
    context = replace(arguments_by_name["context"], status="last_good", metadata_generation="f" * 64)
    retained = format_reviewed_fhir_product_projection(**(arguments_by_name | {"context": context}))
    original_document = json.loads(full.binding_rows[0][-1])
    retained_document = json.loads(retained.binding_rows[0][-1])
    assert retained_document["status"] == "last_good"
    assert retained_document["metadata_sha256"] == original_document["metadata_sha256"]
    changed = format_reviewed_fhir_product_projection(**(arguments_by_name | {"plan_source_hash": "9" * 64}))
    assert json.loads(changed.binding_rows[0][-1])["metadata_sha256"] != original_document["metadata_sha256"]
    changed_witness = format_reviewed_fhir_product_projection(
        **(arguments_by_name | {"organization_witness_sha256": "8" * 64})
    )
    assert json.loads(changed_witness.binding_rows[0][-1])["metadata_sha256"] != original_document["metadata_sha256"]
    changed_generation = format_reviewed_fhir_product_projection(
        **(arguments_by_name | {"context": replace(arguments_by_name["context"], source_generation="7" * 64)})
    )
    assert changed_generation.relationship_rows[0][1] != full.relationship_rows[0][1]


@pytest.mark.parametrize(
    "mutation",
    [
        "coordinate",
        "capture",
        "target",
        "archived",
        "profile",
        "plan",
        "nested",
        "source",
        "metadata",
        "organization",
        "plan-hash",
        "output-size",
        "row-bytes",
        "row-limit",
    ],
)
def test_reviewed_product_refuses_wrong_scope_or_incomplete_bounded_input(mutation):
    arguments_by_name = product_projection_inputs()
    relationship = arguments_by_name["relationships"][0]
    if mutation == "coordinate":
        arguments_by_name["binding"]["edition_id"] = "another-capture"
    if mutation == "capture":
        arguments_by_name["binding"]["source_scope"]["capture_sha256"] = "f" * 64
    if mutation == "target":
        arguments_by_name["canonical_network"]["network_id"] = 8
    if mutation == "archived":
        arguments_by_name["canonical_network"]["archived"] = True
    if mutation == "profile":
        arguments_by_name["context"] = replace(arguments_by_name["context"], profile="raw_fhir")
    if mutation in {"plan", "nested", "source", "row-bytes"}:
        document = deepcopy(relationship.document)
        if mutation == "plan":
            document["plan"]["resource_id"] = "another-plan"
        if mutation == "nested":
            document["plan"]["scope_path"] = "InsurancePlan/plan/plan/0/network/0"
        if mutation == "source":
            document["source_id"] = "another-source"
        if mutation == "row-bytes":
            document["plan"]["display_name"] = "x" * 32768
        arguments_by_name["relationships"] = (replace(relationship, document=document),)
    if mutation == "metadata":
        arguments_by_name["binding"]["metadata_sha256"] = "f" * 64
    if mutation == "organization":
        arguments_by_name["relationships"] = (replace(relationship, network_resource_id="missing"),)
    if mutation == "plan-hash":
        arguments_by_name["plan_source_hash"] = "A" * 64
    if mutation == "output-size":
        arguments_by_name["max_output_bytes"] = 1
    if mutation == "row-limit":
        arguments_by_name["relationships"] = (relationship,) * 1001
    with pytest.raises(FHIRCatalogProjectionError):
        format_reviewed_fhir_product_projection(**arguments_by_name)


def test_reviewed_product_keeps_unresolved_company_accounting():
    arguments_by_name = product_projection_inputs()
    relationship = arguments_by_name["relationships"][0]
    document = relationship.document | {"resolution_status": "unresolved", "resolution_reason": "company_missing"}
    arguments_by_name["relationships"] = (replace(relationship, document=document),)
    batch = format_reviewed_fhir_product_projection(**arguments_by_name)
    assert batch.unresolved_relationships == len(batch.relationship_rows) == 1


@pytest.mark.parametrize("conflicting", [False, True])
def test_replayed_relationship_is_idempotent_but_same_id_with_changed_content_refuses(conflicting):
    organizations, relationships = source_batch()
    repeated = replace(relationships[0], plan_search="changed search") if conflicting else relationships[0]
    if conflicting:
        with pytest.raises(FHIRCatalogProjectionError):
            project(organizations=organizations, relationships=relationships + (repeated,))
    else:
        assert project(organizations=organizations, relationships=relationships + (repeated,)) == project()


@pytest.mark.parametrize(
    "changes",
    [
        {"display_name": " padded network "},
        {"aliases": ["Same", "Same"]},
        {"archived": None},
        {"revision": 0},
        {"benefit_codes": ["vision", "medical"]},
        {"benefit_codes": ["unknown"]},
    ],
)
def test_projection_never_repairs_malformed_approved_network_records(changes):
    network_by_field = {
        "network_id": 7,
        "display_name": "Reviewed Network",
        "aliases": [],
        "archived": False,
        "revision": 1,
        **changes,
    }
    with pytest.raises(FHIRCatalogProjectionError):
        project(canonical_networks=(network_by_field,))


@pytest.mark.parametrize("bindings", [(("network", 7), ("network", 8)), (("absent", 7),)])
def test_projection_refuses_ambiguous_or_absent_source_organization_allocations(bindings):
    organizations, relationships = source_batch()
    with pytest.raises(FHIRCatalogProjectionError):
        format_fhir_catalog_projection(organizations, relationships, bindings, source_context())
