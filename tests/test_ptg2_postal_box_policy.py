# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Postal boxes remain mailing evidence and never become priced locations."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_address_policy import (
    PTG_ADDRESS_KIND_PHYSICAL,
    PTG_ADDRESS_KIND_POSTAL_BOX,
    PTG_ADDRESS_KIND_UNKNOWN,
    PTG_POSTAL_BOX_GEO_FIELDS,
    PTG_POSTAL_BOX_LOCATION_LABEL_FIELDS,
    address_display_rank_sql,
    classify_ptg_address_kind,
    postal_box_address_sql,
)
from api.ptg2_geo_policy import provider_address_location_filter_sql
from process.ptg_parts.address_assurance import (
    summarize_ptg_price_address_payload,
)


@pytest.mark.parametrize(
    "address_line",
    [
        "PO Box 123",
        "P.O. Box 123",
        "P O Box 123",
        "Post Office Box 123",
        "POB 123",
    ],
)
def test_postal_box_classifier_accepts_only_explicit_forms(address_line):
    assert classify_ptg_address_kind({"first_line": address_line}) == (
        PTG_ADDRESS_KIND_POSTAL_BOX
    )


@pytest.mark.parametrize(
    ("address_payload", "expected_kind"),
    [
        ({"second_line": "P.O. Box 123"}, PTG_ADDRESS_KIND_POSTAL_BOX),
        (
            {"first_line": "EXAMPLE CLINIC", "second_line": "P.O. Box 123"},
            PTG_ADDRESS_KIND_POSTAL_BOX,
        ),
        ({"first_line": "123 Boxwood Avenue"}, PTG_ADDRESS_KIND_PHYSICAL),
        ({"first_line": "RR 2 Box 152"}, PTG_ADDRESS_KIND_PHYSICAL),
        ({"city": "Example City", "state": "MI"}, PTG_ADDRESS_KIND_UNKNOWN),
        ({}, PTG_ADDRESS_KIND_UNKNOWN),
        (None, PTG_ADDRESS_KIND_UNKNOWN),
    ],
)
def test_postal_box_classifier_stays_conservative(
    address_payload,
    expected_kind,
):
    assert classify_ptg_address_kind(address_payload) == expected_kind


def test_postal_box_sql_matches_both_address_lines_and_rejects_unsafe_aliases():
    predicate = postal_box_address_sql("addr")

    assert "addr.first_line" in predicate
    assert "addr.second_line" in predicate
    assert "POST OFFICE BOX" in predicate
    with pytest.raises(ValueError, match="simple PostgreSQL identifier"):
        postal_box_address_sql("addr;drop")


def test_address_display_rank_orders_physical_postal_then_blank():
    rank_sql = address_display_rank_sql("addr")

    assert "THEN 2" in rank_sql
    assert "THEN 1 ELSE 0" in rank_sql
    assert "addr.first_line" in rank_sql
    assert "addr.second_line" in rank_sql


def test_spatial_and_text_location_filters_exclude_postal_boxes():
    spatial_filter = provider_address_location_filter_sql(
        "addr",
        schema_name="tenant_data",
        exact_zip_predicate="addr.zip5 = :zip5",
        radius_predicates=["addr.lat IS NOT NULL"],
    )
    parameters_by_name = {}
    text_filter, _ = serving._membership_filter_sql(
        {"state": "MI", "city": "Detroit"},
        candidate_npis=None,
        uses_unified_addresses=True,
        address_zip5_sql="addr.zip5",
        parameter_map=parameters_by_name,
    )
    spatial_parameters_by_name = {}
    membership_spatial_filter, _ = serving._membership_filter_sql(
        {"zip5": "48278"},
        candidate_npis=None,
        uses_unified_addresses=True,
        address_zip5_sql="addr.zip5",
        parameter_map=spatial_parameters_by_name,
    )

    assert f"NOT {postal_box_address_sql('addr')}" in spatial_filter
    assert f"NOT {postal_box_address_sql('addr')}" in text_filter
    assert membership_spatial_filter.count(postal_box_address_sql("addr")) == 1
    assert parameters_by_name["state_value"] == "MI"
    assert parameters_by_name["city_value"] == "DETROIT"


@pytest.mark.asyncio
async def test_provider_enrichment_prefers_physical_over_primary_postal_box(
    monkeypatch,
):
    monkeypatch.setattr(
        serving,
        "_ptg2_table_columns",
        AsyncMock(return_value=frozenset()),
    )

    statement = str(
        await serving._provider_enrichment_statement(
            object(),
            "mrf.npi",
            "mrf.entity_address_unified",
        )
    )

    main_rank = statement.index(address_display_rank_sql("addr"))
    primary_rank = statement.index("(addr.type = 'primary') DESC")
    fallback_rank = statement.index(address_display_rank_sql("na"))
    fallback_type_rank = statement.index("CASE na.type WHEN 'primary'")
    assert main_rank < primary_rank
    assert fallback_rank < fallback_type_rank
    assert "OR (addr.display_rank = 1 AND na.display_rank = 0)" in statement
    assert "THEN NULL::text" in statement
    assert "'npi_address:', na.npi" in statement


def _provider_item(
    address_line: str,
    request_args: dict | None = None,
) -> dict:
    return serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Clinician",
            "location_source": "payer_confirmed_location",
            "source_trace": [{"source_file_version_id": "synthetic-version"}],
            "state": "MI",
            "city": "Example City",
            "zip5": "48000",
            "distance_miles": 1.2,
            "zip_match_type": "radius",
            "anchor_zip5": "48001",
            "zip_radius_miles": 25,
            "address_payload": {
                "first_line": address_line,
                "city": "Example City",
                "state": "MI",
                "postal_code": "48000",
                "lat": 42.0,
                "long": -83.0,
                "coordinates": {"lat": 42.0, "long": -83.0},
                "google_maps_url": "https://maps.example.invalid/location",
                "location_key": "synthetic-postal-location",
                "premise_key": "synthetic-postal-premise",
                "address_site_key": "synthetic-postal-site",
                "location_confidence_id": 3,
                "telephone_number": "313-555-0100",
                "address_sources": ["nppes"],
                "address_verification_evidence": {
                    "source_record_id": "synthetic-record"
                },
            },
            "prices": [],
        },
        request_args or {"plan_id": "synthetic-plan"},
    )


def test_postal_box_is_mailing_evidence_without_geographic_fields():
    item = _provider_item("P.O. Box 123")
    verification = item["address_verification"]

    assert item["address_kind"] == PTG_ADDRESS_KIND_POSTAL_BOX
    assert item["address"]["address_kind"] == PTG_ADDRESS_KIND_POSTAL_BOX
    assert item["address"]["first_line"] == "P.O. Box 123"
    assert item["city"] == "Example City"
    assert item["state"] == "MI"
    assert item["zip5"] == "48000"
    assert item["phone_number"] == "3135550100"
    assert verification["address_kind"] == PTG_ADDRESS_KIND_POSTAL_BOX
    assert verification["address_evidence_level"] == "postal_box_provider_address"
    assert verification["address_network_binding"] == "not_applicable_postal_box"
    assert verification["network_bound_address"] is False
    assert verification["requires_location_confirmation"] is True
    assert verification["displayed_address_present"] is True
    assert item["confidence"]["location"] == "postal_box_provider_address"
    assert PTG_POSTAL_BOX_GEO_FIELDS.isdisjoint(item)
    assert PTG_POSTAL_BOX_GEO_FIELDS.isdisjoint(item["address"])
    assert PTG_POSTAL_BOX_GEO_FIELDS.isdisjoint(verification)
    assert PTG_POSTAL_BOX_LOCATION_LABEL_FIELDS.isdisjoint(item)
    assert PTG_POSTAL_BOX_LOCATION_LABEL_FIELDS.isdisjoint(item["address"])
    assert PTG_POSTAL_BOX_LOCATION_LABEL_FIELDS.isdisjoint(verification)


def test_second_line_only_postal_box_remains_displayable_mailing_evidence():
    item = serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Clinician",
            "address_payload": {
                "second_line": "P.O. Box 123",
                "telephone_number": "313-555-0100",
            },
            "prices": [],
        },
        {"plan_id": "synthetic-plan"},
    )

    assert item["address_kind"] == PTG_ADDRESS_KIND_POSTAL_BOX
    assert item["address"]["second_line"] == "P.O. Box 123"
    assert item["address_verification"]["displayed_address_present"] is True
    assert item["address_verification"]["address_evidence_level"] == (
        "postal_box_provider_address"
    )


def test_physical_address_retains_existing_location_semantics():
    item = _provider_item("100 Example Street")

    assert item["address_kind"] == PTG_ADDRESS_KIND_PHYSICAL
    assert item["address"]["lat"] == 42.0
    assert item["address"]["long"] == -83.0
    assert item["distance_miles"] == 1.2
    assert item["address_verification"]["address_evidence_level"] == (
        "payer_confirmed_location"
    )
    assert item["address_verification"]["network_bound_address"] is True


def test_suppressed_postal_box_normalizes_to_no_address_contract():
    item = _provider_item(
        "P.O. Box 123",
        {
            "plan_id": "synthetic-plan",
            "include_unverified_addresses": False,
        },
    )
    verification = item["address_verification"]

    assert verification["displayed_address_present"] is False
    assert verification["address_network_binding"] == (
        "inferred_from_provider_identity"
    )
    assert verification["address_evidence_level"] == "unknown"
    assert verification["network_bound_address"] is False
    assert item["confidence"]["location"] == "unknown"
    assert "address_kind" not in item
    assert "address_kind" not in verification
    assert "address" not in item
    summary = summarize_ptg_price_address_payload(
        {"data": {"items": [item]}},
        require_displayed_address=False,
    )
    assert summary["ok"] is True, summary["issues"]
