# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Projection COPY rows preserve allocated identities and exact source evidence."""

import json
from dataclasses import replace
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
