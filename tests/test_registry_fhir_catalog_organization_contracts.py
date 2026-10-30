# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source dates and retained affiliations never imply a current company mapping."""

from copy import deepcopy
from datetime import date

import pytest

from tests.test_registry_fhir_catalog_enrichment import (
    COMPANY,
    GROUP,
    context,
    group_assertion,
    project,
    source_batch,
)


def identified_company():
    return {
        "company_id": COMPANY,
        "display_name": "Reviewed Insurer",
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


@pytest.mark.parametrize(
    "period,expected_company,reason",
    [
        ({"period_start": "2026-06-01", "period_end": "2026-06-01"}, COMPANY, None),
        ({"period_start": "2026-06-02"}, None, "company_mapping_missing"),
        ({"period_end": "2026-05-31"}, None, "company_mapping_missing"),
        ({"period_start": "2026-06-01T00:00:00"}, None, "company_identifier_period_unverified"),
        ({"period_start": "2026-02-30"}, None, "company_identifier_period_unverified"),
        ({"period_start": "2026"}, None, "company_identifier_period_unverified"),
    ],
)
def test_source_identifier_periods_resolve_only_at_the_frozen_date(period, expected_company, reason):
    batch = source_batch(
        organization_fields={
            "name": None,
            "identifiers": [{"system": "urn:oid:2.16.840.1.113883.6.300", "value": "01234", **period}],
        }
    )
    before = deepcopy(batch)
    relationships = project(batch, context(companies=[identified_company()], crosswalk=[], as_of=date(2026, 6, 1)))
    assert relationships
    for relationship in relationships:
        company = relationship.document["company"]
        assert (company["company_id"] if company else None) == expected_company
        if reason is not None:
            assert relationship.document["resolution_reason"] == reason
    assert batch == before


def test_reversed_source_plan_period_stays_visible_without_creating_affiliation():
    batch = source_batch(plan_fields={"period_start": "2026-12-31", "period_end": "2026-01-01"})
    relationships = project(batch, context())
    assert relationships
    assert all(relationship.document["resolution_reason"] == "plan_period_unverified" for relationship in relationships)
    assert all(relationship.document["group"]["group_id"] is None for relationship in relationships)
    assert all(relationship.document["plan"]["valid_from"] == "2026-12-31" for relationship in relationships)


@pytest.mark.parametrize("group_state", ["missing", "archived", "different_kind"])
def test_effective_company_assertion_does_not_restore_an_unavailable_group(group_state):
    company_by_field = {
        "company_id": COMPANY,
        "display_name": "Reviewed Insurer",
        "group_assertions": [group_assertion()],
    }
    groups = (
        []
        if group_state == "missing"
        else [
            {
                "group_id": GROUP,
                "display_name": "Retained Parent",
                "group_kind": "association" if group_state == "different_kind" else "corporate_parent",
                "archived": group_state == "archived",
            }
        ]
    )
    relationships = project(source_batch(), context(companies=[company_by_field], groups=groups))
    selected_relationships = [
        relationship.document
        for relationship in relationships
        if relationship.document["group"]["group_kind"] == "corporate_parent"
    ]
    assert selected_relationships and all(
        relationship["company"]["company_id"] == COMPANY for relationship in selected_relationships
    )
    assert all(relationship["group"]["group_id"] is None for relationship in selected_relationships)
    assert all(
        relationship["resolution_reason"] == "approved_group_unavailable" for relationship in selected_relationships
    )
