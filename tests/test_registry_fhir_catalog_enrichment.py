# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Approved organization evidence enriches sources without changing canonical records."""

from copy import deepcopy
from dataclasses import asdict, replace
from datetime import date

import pytest

from process.registry_fhir_catalog_enrichment import (
    FHIRCatalogEnrichmentContext,
    FHIRCatalogEnrichmentError,
    enrich_fhir_catalog_edges,
    fhir_catalog_organization_id,
    fhir_company_scope,
)
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_validation import (
    validate_fhir_catalog_organization_batch,
)

COMPANY = "11111111-1111-4111-8111-111111111111"
ADMINISTRATOR = "22222222-2222-4222-8222-222222222222"
GROUP = "33333333-3333-4333-8333-333333333333"
OTHER_GROUP = "44444444-4444-4444-8444-444444444444"
ASSERTION = "55555555-5555-4555-8555-555555555555"
OTHER_ASSERTION = "66666666-6666-4666-8666-666666666666"


def source_row(resource_type, resource_id, fields):
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_hash": "a" * 64,
        "payload_json": fields,
    }


def source_batch(*, organization_fields=None, plan_fields=None, include_owner=True):
    rows = [
        source_row(
            "Organization", "network", {"name": "Example Network", "type_codes": [{"code": "ntwk"}], "active": False}
        )
    ]
    if include_owner:
        rows.append(source_row("Organization", "insurer", {"name": "Example Insurance", **(organization_fields or {})}))
    rows.append(
        source_row(
            "InsurancePlan",
            "plan",
            {
                "name": "Example Gold",
                "aliases": ["Premium Choice"],
                "owned_by_ref": "Organization/insurer",
                "network_refs": ["Organization/network"],
                **(plan_fields or {}),
            },
        )
    )
    return extract_fhir_catalog_batch(rows, profile="retained_normalized")


def group_assertion(
    *,
    group_id=GROUP,
    assertion_id=ASSERTION,
    kind="corporate_parent",
    relationship="verified_ownership",
    provenance="manual_reference",
    start="2025-01-01",
    end=None,
):
    return {
        "assertion_id": assertion_id,
        "company_id": COMPANY,
        "group_id": group_id,
        "group_kind": kind,
        "relationship_kind": relationship,
        "valid_from": start,
        "valid_to": end,
        "provenance": {"kind": provenance, "evidence_ref": "reviewed-example"},
    }


def context(*, companies=None, groups=None, crosswalk=None, as_of=date(2026, 6, 1)):
    return FHIRCatalogEnrichmentContext(
        source_id="example-fhir",
        endpoint_id="example-endpoint",
        as_of=as_of,
        approved_companies=tuple(
            companies
            if companies is not None
            else [
                {
                    "company_id": COMPANY,
                    "display_name": "Example Insurance",
                    "aliases": ["Example Carrier"],
                    "group_assertions": [group_assertion()],
                }
            ]
        ),
        approved_groups=tuple(
            groups
            if groups is not None
            else [{"group_id": GROUP, "display_name": "Example Parent", "group_kind": "corporate_parent"}]
        ),
        company_crosswalk=tuple(crosswalk if crosswalk is not None else [("insurer", COMPANY)]),
    )


def project(batch, pinned_context):
    return enrich_fhir_catalog_edges(batch.organizations, batch.edges, pinned_context)


def test_explicit_crosswalk_preserves_roles_aliases_dates_and_source_inactive_inventory():
    batch = source_batch(
        plan_fields={
            "administered_by_ref": "Organization/administrator",
            "period_start": "2026-01-01",
            "period_end": "2026-12-31",
        }
    )
    administrator = source_row("Organization", "administrator", {"name": "Example Administrator"})
    admin_observation = extract_fhir_catalog_batch([administrator], profile="retained_normalized").organizations[0]
    batch = replace(batch, organizations=(*batch.organizations, admin_observation))
    pinned = context(
        companies=[
            {
                "company_id": COMPANY,
                "display_name": "Example Insurance",
                "aliases": ["Example Carrier"],
                "group_assertions": [group_assertion()],
            },
            {"company_id": ADMINISTRATOR, "display_name": "Example Administrator"},
        ],
        crosswalk=[("insurer", COMPANY), ("administrator", ADMINISTRATOR)],
    )
    before = deepcopy(pinned)
    relationships = project(batch, pinned)
    assert {relationship.document["company_role"] for relationship in relationships} == {"insurer", "administrator"}
    assert {relationship.network_resource_id for relationship in relationships} == {"network"}
    insurer = next(
        relationship
        for relationship in relationships
        if relationship.document["group"] is not None and relationship.document["group"]["group_id"] == GROUP
    )
    assert insurer.document["company"] == {"company_id": COMPANY, "display_name": "Example Insurance"}
    assert insurer.document["plan"] == {
        "resource_id": "plan",
        "scope_path": "InsurancePlan/plan/normalized_aggregate/0",
        "display_name": "Example Gold",
        "valid_from": "2026-01-01",
        "valid_to": "2026-12-31",
    }
    assert insurer.document["resolution_status"] == "resolved"
    assert "example carrier" in insurer.company_search and "premium choice" in insurer.plan_search
    assert pinned == before and batch.organizations[0].status == "inactive"
    assert all("archived" not in relationship.document for relationship in relationships)


@pytest.mark.parametrize(
    "owner_ref,include_owner,reason",
    [
        ("Organization/insurer", False, "source_organization_missing"),
        ("https://foreign.test/Organization/insurer", True, "company_reference_outside_source"),
        (None, True, "company_reference_missing"),
    ],
)
def test_names_missing_organizations_and_foreign_references_never_create_mappings(owner_ref, include_owner, reason):
    batch = source_batch(plan_fields={"owned_by_ref": owner_ref}, include_owner=include_owner)
    relationships = project(batch, context(crosswalk=[]))
    assert len(relationships) == 1
    assert relationships[0].document["company"] is None
    assert relationships[0].document["group"] is None
    assert relationships[0].document["resolution_status"] == "unresolved"
    assert relationships[0].document["resolution_reason"] == reason


@pytest.mark.parametrize(
    "system,value,kind,approved_value",
    [
        ("urn:oid:2.16.840.1.113883.4.4", "12-3456789", "ein", "123456789"),
        ("urn:oid:2.16.840.1.113883.6.300", "01234", "naic_company", "01234"),
    ],
)
def test_exact_standard_identifiers_join_only_approved_current_assertions(system, value, kind, approved_value):
    batch = source_batch(organization_fields={"identifiers": [{"system": system, "value": value}]})
    company_by_field = {
        "company_id": COMPANY,
        "display_name": "Reviewed Company",
        "identifier_assertions": [
            {
                "identifier_system": kind,
                "identifier_scope": "US",
                "identifier_value": approved_value,
                "valid_from": "2025-01-01",
                "valid_to": None,
            }
        ],
    }
    relationships = project(batch, context(companies=[company_by_field], crosswalk=[]))
    assert all(relationship.document["company"]["company_id"] == COMPANY for relationship in relationships)
    expired = deepcopy(company_by_field)
    expired["identifier_assertions"][0]["valid_to"] = "2025-12-31"
    assert project(batch, context(companies=[expired], crosswalk=[]))[0].document["company"]["company_id"] != COMPANY


@pytest.mark.parametrize(
    "system,value",
    [
        ("https://example.test/naic", "01234"),
        ("https://example.test/us-pseudo-ein", "123456789"),
        ("urn:oid:2.16.840.1.113883.6.300", "1234"),
        ("urn:oid:2.16.840.1.113883.6.300", "00000"),
        ("urn:oid:2.16.840.1.113883.4.4", "123 45 6789"),
    ],
)
def test_identifier_substrings_and_malformed_values_do_not_resolve_companies(system, value):
    batch = source_batch(organization_fields={"identifiers": [{"system": system, "value": value}]})
    company_by_field = {
        "company_id": COMPANY,
        "display_name": "Reviewed Company",
        "identifier_assertions": [
            {
                "identifier_system": "naic_company",
                "identifier_scope": "US",
                "identifier_value": "01234",
                "valid_from": "2025-01-01",
                "valid_to": None,
            }
        ],
    }
    assert (
        project(batch, context(companies=[company_by_field], crosswalk=[]))[0].document["company"]["company_id"]
        != COMPANY
    )


def test_archived_identifier_mapping_cannot_reappear_as_an_imported_company():
    batch = source_batch(
        organization_fields={
            "identifiers": [
                {
                    "system": "urn:oid:2.16.840.1.113883.6.300",
                    "value": "01234",
                }
            ]
        }
    )
    approved_by_field = {
        "company_id": COMPANY,
        "display_name": "Reviewed Company",
        "archived": False,
        "identifier_assertions": [
            {
                "identifier_system": "naic_company",
                "identifier_scope": "US",
                "identifier_value": "01234",
                "valid_from": "2025-01-01",
                "valid_to": None,
            }
        ],
    }
    assert (
        project(batch, context(companies=[approved_by_field], crosswalk=[]))[0].document["company"]["company_id"]
        == COMPANY
    )
    archived_by_field = {**approved_by_field, "archived": True}
    suppressed_by_field = project(batch, context(companies=[archived_by_field], crosswalk=[]))[0].document
    assert suppressed_by_field["company"] is None
    assert suppressed_by_field["resolution_reason"] == "approved_company_unavailable"
    replacement_by_field = {"company_id": ADMINISTRATOR, "display_name": "Replacement"}
    explicit_by_field = project(
        batch, context(companies=[archived_by_field, replacement_by_field], crosswalk=[("insurer", ADMINISTRATOR)])
    )[0].document
    assert explicit_by_field["company"]["company_id"] == ADMINISTRATOR


def test_manual_crosswalk_target_survives_conflicting_source_identifier():
    batch = source_batch(
        organization_fields={"identifiers": [{"system": "urn:oid:2.16.840.1.113883.6.300", "value": "01234"}]}
    )
    companies = [
        {"company_id": COMPANY, "display_name": "Reviewed Company"},
        {
            "company_id": ADMINISTRATOR,
            "display_name": "Other Company",
            "identifier_assertions": [
                {
                    "identifier_system": "naic_company",
                    "identifier_value": "01234",
                    "identifier_scope": "US",
                    "valid_from": "2025-01-01",
                    "valid_to": None,
                }
            ],
        },
    ]
    relationships = project(batch, context(companies=companies))
    assert all(relationship.document["company"]["company_id"] == COMPANY for relationship in relationships)
    assert all(
        relationship.document["resolution_status"] == "conflict"
        and relationship.document["resolution_reason"] == "company_identifier_conflict"
        for relationship in relationships
    )
    assert (
        project(batch, context(companies=companies, crosswalk=[("insurer", COMPANY), ("insurer", ADMINISTRATOR)]))[
            0
        ].document["company"]
        is None
    )


def test_group_projection_uses_frozen_date_plan_overlap_and_manual_precedence():
    manual = group_assertion(group_id=None, relationship="verified_no_affiliation")
    source_assertion = group_assertion(assertion_id=OTHER_ASSERTION, provenance="source_reference")
    company_by_field = {
        "company_id": COMPANY,
        "display_name": "Reviewed Company",
        "group_assertions": [source_assertion, manual],
    }
    batch = source_batch(plan_fields={"period_start": "2026-01-01", "period_end": "2026-12-31"})
    relationships = project(batch, context(companies=[company_by_field]))
    parent = next(
        relationship.document
        for relationship in relationships
        if relationship.document["group"]["group_kind"] == "corporate_parent"
    )
    assert parent["group"]["group_id"] is None and parent["group"]["relationship_kind"] == "verified_no_affiliation"
    assert parent["resolution_status"] == "resolved"
    expired_company = deepcopy(company_by_field)
    for assertion in expired_company["group_assertions"]:
        assertion["valid_to"] = "2025-12-31"
    assert all(
        relationship.document["resolution_reason"] == "group_affiliation_unknown"
        for relationship in project(batch, context(companies=[expired_company]))
    )
    assert any(
        relationship.document["resolution_status"] == "resolved"
        for relationship in project(batch, context(companies=[company_by_field], as_of=date(2026, 7, 1)))
    )
    future_plan = source_batch(plan_fields={"period_start": "2027-01-01", "period_end": "2027-12-31"})
    ending_company = deepcopy(company_by_field)
    for assertion in ending_company["group_assertions"]:
        assertion["valid_to"] = "2026-12-31"
    assert all(
        relationship.document["resolution_status"] == "unresolved"
        for relationship in project(future_plan, context(companies=[ending_company]))
    )


def test_distinct_group_kinds_and_conflicting_assertions_remain_explicit():
    company_by_field = {
        "company_id": COMPANY,
        "display_name": "Reviewed Company",
        "group_assertions": [group_assertion(), group_assertion(group_id=OTHER_GROUP, assertion_id=OTHER_ASSERTION)],
    }
    groups = [
        {"group_id": GROUP, "display_name": "Parent One", "group_kind": "corporate_parent"},
        {"group_id": OTHER_GROUP, "display_name": "Parent Two", "group_kind": "corporate_parent"},
    ]
    relationships = project(source_batch(), context(companies=[company_by_field], groups=groups))
    conflicted_documents = [
        relationship.document
        for relationship in relationships
        if relationship.document["resolution_status"] == "conflict"
    ]
    assert {document["group"]["group_id"] for document in conflicted_documents} == {GROUP, OTHER_GROUP}
    assert any(
        relationship.document["group"]["group_kind"] == "naic_group"
        and relationship.document["resolution_status"] == "unresolved"
        for relationship in relationships
    )


def test_mapping_only_refresh_changes_context_without_changing_source_network():
    batch = source_batch()
    first = project(batch, context())
    corrected_company_by_field = {"company_id": ADMINISTRATOR, "display_name": "Corrected Company"}
    second = project(batch, context(companies=[corrected_company_by_field], crosswalk=[("insurer", ADMINISTRATOR)]))
    assert {relationship.network_resource_id for relationship in first + second} == {"network"}
    assert {relationship.document["company"]["company_id"] for relationship in second} == {ADMINISTRATOR}
    assert first == project(batch, context())


def imported_parent_case():
    batch = source_batch(organization_fields={"part_of_ref": "Organization/parent"})
    parent = extract_fhir_catalog_batch(
        [source_row("Organization", "parent", {"name": "Source Parent"})], profile="retained_normalized"
    ).organizations[0]
    return replace(batch, organizations=batch.organizations + (parent,))


def test_imported_company_and_parent_are_immediately_usable_with_stable_source_identity():
    batch = imported_parent_case()
    source_context = context(companies=[], groups=[], crosswalk=[])
    relationship = next(row.document for row in project(batch, source_context) if row.document["group"]["group_id"])
    assert relationship["company"]["display_name"] == "Example Insurance"
    assert relationship["group"]["display_name"] == "Source Parent"
    assert relationship["resolution_status"] == "resolved"
    renamed = replace(
        batch, organizations=tuple(replace(org, name=org.name + " Renamed") for org in batch.organizations)
    )
    updated = next(
        row.document
        for row in project(renamed, replace(source_context, source_id="renamed-alias"))
        if row.document["group"]["group_id"]
    )
    assert updated["company"]["company_id"] == relationship["company"]["company_id"]
    assert updated["group"]["group_id"] == relationship["group"]["group_id"]
    assert updated["company"]["display_name"].endswith("Renamed")
    assert fhir_catalog_organization_id("different-endpoint", "insurer", "company") != updated["company"]["company_id"]
    inactive = replace(renamed, organizations=tuple(replace(org, status="inactive") for org in renamed.organizations))
    retained = next(row.document for row in project(inactive, source_context) if row.document["group"]["group_id"])
    assert retained["company"] == updated["company"] and retained["group"] == updated["group"]


@pytest.mark.parametrize("tamper", [None, "company_id", "parent_id", "foreign_reference", "parent_name"])
def test_imported_organization_proof_rejects_forged_identity_or_parent(tamper):
    batch = imported_parent_case()
    source_context = context(companies=[], groups=[], crosswalk=[])
    relationship = next(row.document for row in project(batch, source_context) if row.document["group"]["group_id"])
    proof_by_field = {
        "relationship": relationship,
        "edges": [asdict(batch.edges[0])],
        "source": {"endpoint_id": "example-endpoint", "source_uri": None},
        "organization": asdict(batch.organizations[1]),
        "parent": asdict(batch.organizations[-1]),
        "approved_company": None,
        "approved_group": None,
    }
    if tamper == "company_id":
        relationship["company"]["company_id"] = COMPANY
    elif tamper == "parent_id":
        relationship["group"]["group_id"] = GROUP
    elif tamper == "foreign_reference":
        relationship["organization_reference"] = "https://foreign.test/Organization/insurer"
    elif tamper == "parent_name":
        relationship["group"]["display_name"] = "Invented Parent"
    if tamper is None:
        validate_fhir_catalog_organization_batch([proof_by_field], source_context)
    else:
        with pytest.raises(ValueError, match="organization_precedence_invalid"):
            validate_fhir_catalog_organization_batch([proof_by_field], source_context)


@pytest.mark.parametrize("override", ["company", "parent"])
def test_imported_identity_cannot_bypass_approved_company_or_manual_parent(override):
    batch = imported_parent_case()
    source_context = context(companies=[], groups=[], crosswalk=[])
    relationship = next(
        projected.document for projected in project(batch, source_context) if projected.document["group"]["group_id"]
    )
    proof_by_field = {
        "relationship": relationship,
        "edges": [asdict(batch.edges[0])],
        "organization": asdict(batch.organizations[1]),
        "parent": asdict(batch.organizations[-1]),
    }
    if override == "company":
        authoritative = context()
    else:
        company_id = relationship["company"]["company_id"]
        assertion_by_field = {
            **group_assertion(group_id=None, relationship="verified_no_affiliation"),
            "company_id": company_id,
        }
        authoritative = context(
            companies=[
                {
                    "company_id": company_id,
                    "display_name": "Example Insurance",
                    "group_assertions": [assertion_by_field],
                }
            ],
            groups=[],
            crosswalk=[("insurer", company_id)],
        )
    with pytest.raises(ValueError, match="organization_precedence_invalid"):
        validate_fhir_catalog_organization_batch([proof_by_field], authoritative)


def test_equivalent_reference_candidates_survive_proof_chunk_boundaries():
    batch = imported_parent_case()
    source_context = replace(context(companies=[], groups=[], crosswalk=[]), source_uri="https://example.test/fhir/")
    proofs = []
    for number in range(51):
        relative = replace(batch.edges[0], owner_resource_id=f"plan-{number}", plan_resource_id=f"plan-{number}")
        absolute = replace(relative, network_ref="https://example.test/fhir/Organization/network")
        pair = replace(batch, edges=(relative, absolute))
        for projected in project(pair, source_context):
            if projected.document["group"]["group_id"] is not None:
                proofs.append(
                    {
                        "relationship": projected.document,
                        "edges": [asdict(relative), asdict(absolute)],
                        "organization": asdict(batch.organizations[1]),
                        "parent": asdict(batch.organizations[-1]),
                    }
                )
    assert len(proofs) == 102
    for size in (1, 100):
        for offset in range(0, len(proofs), size):
            validate_fhir_catalog_organization_batch(proofs[offset : offset + size], source_context)


def test_unverifiable_source_dates_stay_reviewable_without_inventing_affiliation():
    partial = source_batch(plan_fields={"period_start": "2026"})
    invalid = source_batch(plan_fields={"period_start": "2026-99-99"})
    for batch in (partial, invalid):
        assert all(
            relationship.document["resolution_reason"] == "plan_period_unverified"
            for relationship in project(batch, context())
        )
    identifiers = source_batch(
        organization_fields={
            "identifiers": [{"system": "urn:oid:2.16.840.1.113883.6.300", "value": "01234", "period_start": "2026"}]
        }
    )
    projected = project(identifiers, context(crosswalk=[]))[0].document
    assert projected["company"]["company_id"] == fhir_catalog_organization_id("example-endpoint", "insurer", "company")
    assert projected["resolution_reason"] == "group_affiliation_unknown"


def test_identifier_timestamps_use_frozen_midnight_utc_without_rejecting_other_rows():
    batch = source_batch(
        organization_fields={
            "identifiers": [
                {
                    "system": "urn:oid:2.16.840.1.113883.6.300",
                    "value": "01234",
                    "period_start": "2026-06-01T12:00:00Z",
                    "period_end": "2026-12-31T23:59:59Z",
                }
            ]
        }
    )
    company_by_field = {
        "company_id": COMPANY,
        "display_name": "Reviewed Company",
        "identifier_assertions": [
            {
                "identifier_system": "naic_company",
                "identifier_scope": "US",
                "identifier_value": "01234",
                "valid_from": "2025-01-01",
                "valid_to": None,
            }
        ],
    }
    first_day = project(batch, context(companies=[company_by_field], crosswalk=[], as_of=date(2026, 6, 1)))
    next_day = project(batch, context(companies=[company_by_field], crosswalk=[], as_of=date(2026, 6, 2)))
    assert first_day[0].document["company"]["company_id"] != COMPANY
    assert all(relationship.document["company"]["company_id"] == COMPANY for relationship in next_day)


def test_bounds_and_invalid_context_refuse_without_partial_results():
    batch = source_batch()
    with pytest.raises(FHIRCatalogEnrichmentError):
        enrich_fhir_catalog_edges(batch.organizations, batch.edges, context(), max_output_bytes=1)
    with pytest.raises(FHIRCatalogEnrichmentError):
        enrich_fhir_catalog_edges(batch.organizations, batch.edges * 1001, context())
    with pytest.raises(FHIRCatalogEnrichmentError):
        project(batch, replace(context(), as_of="2026-06-01"))
    assert len(fhir_company_scope("source-" + "x" * 120)) == 69
    assert fhir_company_scope("one") != fhir_company_scope("two")
