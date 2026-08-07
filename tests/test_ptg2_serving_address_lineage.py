# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


def _location_query():
    return serving._MembershipLocationQuery(
        address_table="mrf.entity_address_unified",
        npi_scope_table="mrf.ptg2_v3_npi_scope",
        filter_sql="npi_scope.snapshot_key = :shared_snapshot_key",
        parameter_map={"limit": 2},
        distance_sql="NULL::double precision",
        knn_order_sql=None,
    )


def test_address_provenance_preserves_numeric_run_id_and_derives_valid_date():
    entry = serving._address_provenance_entry(
        {
            "source_id": 8,
            "source_record_key": "provider_directory_fhir:fixture",
            "source_run_id": "20260731",
            "observed_at": None,
            "last_seen_at": None,
        }
    )

    assert entry == {
        "dataset_id": "payer_provider_directory_fhir",
        "source_id": 8,
        "source_record_id": "provider_directory_fhir:fixture",
        "record_version_id": "20260731",
        "record_version_ids": ["20260731"],
        "retrieved_at": "2026-07-31",
    }
    assert serving._provenance_run_retrieved_at("opaque-run-id") is None
    assert serving._is_complete_address_provenance_entry(entry) is True


def test_address_provenance_index_omits_incomplete_or_source_zero_entries():
    rows = [
        {
            "location_key": "invalid-date",
            "source_id": 8,
            "source_record_key": "provider_directory_fhir:invalid-date",
            "source_run_id": "20261399",
        },
        {
            "location_key": "materialization-only",
            "source_id": 0,
            "source_record_key": "materialization-only",
            "source_run_id": "20260731",
        },
    ]

    assert serving._index_address_provenance(rows) == {}


def test_address_provenance_query_selects_coherent_active_source_lineage():
    sql = serving._ADDRESS_PROVENANCE_SQL

    assert "UNION ALL" in sql
    assert "SELECT unified.*" not in sql
    assert "unified.address_sources" in sql
    assert "stored_evidence AS MATERIALIZED" in sql
    assert "entity_address_unified AS unified" in sql
    assert "stored.retired_at IS NULL" in sql
    assert "stored_specific AS MATERIALIZED" in sql
    assert "live_mrf AS MATERIALIZED" in sql
    assert "live_nppes AS MATERIALIZED" in sql
    assert "live_cms_doctors AS MATERIALIZED" in sql
    assert "specific_candidates AS MATERIALIZED" in sql
    assert "STARTS_WITH(stored.source_record_key, source.source_record_prefix)" in sql
    assert "stored.npi IS NULL OR stored.npi = source.npi" in sql
    assert "stored.address_key = source.address_key" in sql
    assert "stored.premise_key = source.premise_key" in sql
    assert "STARTS_WITH(sources.source_name, 'facility_anchor:')" in sql
    assert "NULLIF(BTRIM(stored.source_run_id), '')" in sql
    assert "nppes.date_added::text AS source_run_id" in sql
    assert "doctor.updated_at::text AS source_run_id" in sql
    assert "generic_materialization" not in sql
    assert "FROM specific_evidence" in sql
    assert serving._ptg2_mrf_lineage_complete_sql("candidate") in sql
    assert "COALESCE(NULLIF(stored.source_record_key" not in sql


def test_membership_location_queries_retain_the_public_site_key() -> None:
    for sql in (
        serving._MEMBERSHIP_LOCATION_SQL,
        serving._MEMBERSHIP_UNIFIED_ASSURED_LOCATION_SQL,
        serving._MEMBERSHIP_LOCATION_KNN_SQL,
    ):
        assert "'address_site_key'" in sql


def test_address_provenance_uses_sql_admission_label_without_reclassification():
    location_rows = [
        {
            "_geo_evidence_level": "multi_issuer_marketplace_address",
            "_geo_evidence_source_id": 2,
            "address_payload": json.dumps(
                {
                    "location_key": "location-fixture",
                    "address_sources": ["mrf"],
                }
            ),
        }
    ]

    assert serving._selected_location_requests(location_rows) == [
        ("location-fixture", 2)
    ]
    serving._apply_address_provenance(
        location_rows,
        {
            "location-fixture": [
                {
                    "dataset_id": "marketplace_provider_directory",
                    "source_id": 2,
                    "source_record_id": "mrf:1000000005:practice:17",
                    "record_version_id": "20260731",
                    "retrieved_at": "2026-07-31",
                }
            ]
        },
    )

    address_payload = json.loads(location_rows[0]["address_payload"])
    assert address_payload["geo_evidence_level"] == ("multi_issuer_marketplace_address")
    assert len(address_payload["address_provenance"]) == 1
    assert "_geo_evidence_level" not in location_rows[0]
    assert "_geo_evidence_source_id" not in location_rows[0]


def test_address_provenance_drops_geo_row_without_complete_admitted_lineage():
    location_rows = [
        {
            "_geo_evidence_level": "multi_issuer_marketplace_address",
            "_geo_evidence_source_id": 2,
            "address_payload": json.dumps({"location_key": "missing-lineage"}),
        }
    ]

    serving._apply_address_provenance(
        location_rows,
        {
            "missing-lineage": [
                {
                    "dataset_id": "marketplace_provider_directory",
                    "source_id": 2,
                    "source_record_id": "mrf:1000000005:practice:17",
                    "record_version_id": "20260731",
                }
            ]
        },
    )

    assert location_rows == []


def _unproven_location_rows(expected_prices, expected_rate_summaries):
    return [
        {
            "npi": 1990000122,
            "provider_set_key": 71,
            "state": "MI",
            "city": "TEST CITY",
            "zip5": "48201",
            "telephone_number": "555-0100",
            "location_hash": "test-location-hash",
            "prices": expected_prices,
            "price_summary": expected_rate_summaries,
            "address_payload": json.dumps(
                {
                    "location_key": "source-zero-only",
                    "first_line": "1 TEST STREET",
                    "city": "TEST CITY",
                    "state": "MI",
                    "postal_code": "48201",
                    "telephone_number": "555-0100",
                    "address_sources": ["entity_address_unified"],
                }
            ),
        }
    ]


def _enriched_provider_context():
    return {
        "npi": 1990000122,
        "provider_name": "TEST PROVIDER",
        "state": "CA",
        "city": "FALLBACK TEST CITY",
        "zip5": "90001",
        "telephone_number": "555-0199",
        "address_payload": json.dumps(
            {
                "first_line": "99 FALLBACK TEST STREET",
                "city": "FALLBACK TEST CITY",
                "state": "CA",
                "postal_code": "90001",
            }
        ),
    }


def _assert_redacted_location(location_row, expected_prices, expected_rate_summaries):
    assert location_row["npi"] == 1990000122
    assert location_row["provider_set_key"] == 71
    assert location_row["prices"] == expected_prices
    assert location_row["price_summary"] == expected_rate_summaries
    assert location_row[serving._PTG_UNPROVEN_ADDRESS_MARKER] is True
    assert json.loads(location_row["address_payload"]) == {}
    for field_name in ("state", "city", "zip5", "telephone_number"):
        assert field_name not in location_row


def _assert_compacted_provider_redaction(
    compacted_provider,
    expected_prices,
    expected_rate_summaries,
):
    assert compacted_provider["prices"] == expected_prices
    assert compacted_provider["price_summary"] == expected_rate_summaries
    address_verification = compacted_provider["address_verification"]
    assert address_verification["displayed_address_present"] is False
    assert address_verification["address_evidence_level"] == "unknown"
    assert "address_provenance" not in address_verification
    for field_name in (
        "address",
        "state",
        "city",
        "zip5",
        "telephone_number",
        "phone_number",
    ):
        assert field_name not in compacted_provider


def test_unproven_non_geo_address_is_redacted_without_changing_rate_payload():
    expected_prices = [{"negotiated_rate": "125.00", "billing_class": "professional"}]
    expected_rate_summaries = [{"min": "125.00", "max": "125.00"}]
    location_rows = _unproven_location_rows(
        expected_prices,
        expected_rate_summaries,
    )

    serving._apply_address_provenance(
        location_rows,
        {},
        include_response_evidence=True,
    )

    assert len(location_rows) == 1
    location_row = location_rows[0]
    _assert_redacted_location(location_row, expected_prices, expected_rate_summaries)
    provider_data = serving._graph_provider_data(
        location_row,
        _enriched_provider_context(),
        "entity_address_unified",
    )
    provider_data.update(
        {
            "prices": expected_prices,
            "tic_prices": expected_prices,
            "price_summary": expected_rate_summaries,
        }
    )
    compacted_provider = serving._compact_item_from_row(
        provider_data,
        {"include_evidence": "true"},
    )
    _assert_compacted_provider_redaction(
        compacted_provider,
        expected_prices,
        expected_rate_summaries,
    )


def test_complete_provenance_validates_default_and_is_only_exposed_with_evidence():
    source_lineage_dict = {
        "dataset_id": "payer_provider_directory_fhir",
        "source_id": 8,
        "source_record_id": "provider_directory_fhir:test-record",
        "record_version_id": "test-version",
        "retrieved_at": "2026-07-30T00:00:00+00:00",
    }
    items_by_evidence_flag = {}
    for include_response_evidence in (False, True):
        location_rows = [
            {
                "npi": 1990000122,
                "provider_name": "TEST PROVIDER",
                "state": "MI",
                "city": "TEST CITY",
                "zip5": "48201",
                "prices": [{"negotiated_rate": "125.00"}],
                "tic_prices": [{"negotiated_rate": "125.00"}],
                "price_summary": [{"min": "125.00", "max": "125.00"}],
                "address_payload": json.dumps(
                    {
                        "location_key": "proven-location",
                        "first_line": "1 TEST STREET",
                        "city": "TEST CITY",
                        "state": "MI",
                        "postal_code": "48201",
                        "address_sources": ["provider_directory_fhir"],
                    }
                ),
            }
        ]
        serving._apply_address_provenance(
            location_rows,
            {"proven-location": [source_lineage_dict]},
            include_response_evidence=include_response_evidence,
        )
        items_by_evidence_flag[include_response_evidence] = (
            serving._compact_item_from_row(
                location_rows[0],
                {"include_evidence": include_response_evidence},
            )
        )

    default_item = items_by_evidence_flag[False]
    evidence_item = items_by_evidence_flag[True]
    assert default_item["prices"] == evidence_item["prices"]
    assert default_item["price_summary"] == evidence_item["price_summary"]
    assert default_item["address"] == {
        field_name: field_value
        for field_name, field_value in evidence_item["address"].items()
        if field_name != "address_provenance"
    }
    assert "address_provenance" not in default_item["address"]
    assert "address_provenance" not in default_item["address_verification"]
    assert evidence_item["address_verification"]["address_provenance"] == [
        source_lineage_dict
    ]


@pytest.mark.asyncio
async def test_membership_location_rows_preserves_probe_state_after_lineage_drop(
    monkeypatch,
):
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=_location_query()),
    )

    async def drop_unproven_rows(
        _session,
        location_rows,
        *,
        include_response_evidence,
        use_stored_only,
    ):
        assert include_response_evidence
        assert not use_stored_only
        location_rows.clear()
        return "available"

    monkeypatch.setattr(serving, "_hydrate_address_provenance", drop_unproven_rows)
    session = FakeSession(
        [
            FakeResult(
                [
                    {
                        "npi": 1234567890,
                        "_geo_evidence_level": "nppes_registry_address",
                        "_geo_evidence_source_id": 1,
                        "_ptg_source_exhausted": False,
                        "address_payload": json.dumps(
                            {"location_key": "race-dropped-location"}
                        ),
                    }
                ]
            )
        ]
    )

    location_rows = await serving._membership_location_rows(
        session,
        strict_v3_tables(),
        {"include_evidence": True},
        candidate_npis=None,
        limit=2,
    )

    assert location_rows == [{"_ptg_probe_empty": True, "_ptg_source_exhausted": False}]


@pytest.mark.asyncio
async def test_default_lineage_validation_is_one_bounded_set_query(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_is_relation_available",
        AsyncMock(return_value=True),
    )
    location_rows = [
        {
            "npi": 1990000122,
            "address_payload": json.dumps(
                {
                    "location_key": f"bounded-location-{index}",
                    "first_line": f"{index} TEST STREET",
                    "city": "TEST CITY",
                    "state": "MI",
                    "postal_code": "48201",
                }
            ),
        }
        for index in range(64)
    ]
    session = FakeSession([FakeResult([])])

    await serving._hydrate_address_provenance(
        session,
        location_rows,
        include_response_evidence=False,
    )

    assert len(session.calls) == 1
    query_parameters = session.calls[0][0][1]
    assert len(query_parameters["location_keys"]) == 64
    assert len(query_parameters["admitted_source_ids"]) == 64
    assert query_parameters["stored_only"] is False
    assert len(location_rows) == 64
    assert all(
        location_row[serving._PTG_UNPROVEN_ADDRESS_MARKER] is True
        and json.loads(location_row["address_payload"]) == {}
        for location_row in location_rows
    )


@pytest.mark.asyncio
async def test_billing_lineage_requires_materialized_evidence(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_is_relation_available",
        AsyncMock(return_value=False),
    )
    location_rows = [
        {
            "npi": 1990000122,
            "address_payload": json.dumps(
                {"location_key": "generation-bound-location"}
            ),
        }
    ]
    session = FakeSession([])

    provenance_status = await serving._hydrate_address_provenance(
        session,
        location_rows,
        use_stored_only=True,
    )

    assert provenance_status == "unavailable"
    assert session.calls == []
    assert location_rows[0]["npi"] == 1990000122
