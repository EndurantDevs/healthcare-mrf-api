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


def _location_query(*, knn_order_sql=None):
    return serving._MembershipLocationQuery(
        address_table="mrf.entity_address_unified",
        npi_scope_table="mrf.ptg2_v3_npi_scope",
        filter_sql="npi_scope.snapshot_key = :shared_snapshot_key",
        parameter_map={"limit": 2},
        distance_sql="NULL::double precision",
        knn_order_sql=knn_order_sql,
    )


def test_membership_filter_rejects_empty_scope_and_invalid_values():
    assert (
        serving._membership_filter_sql(
            {},
            candidate_npis=(),
            uses_unified_addresses=False,
            address_zip5_sql="LEFT(addr.postal_code, 5)",
            parameter_map={},
        )
        is None
    )
    assert (
        serving._membership_filter_sql(
            {"radius_miles": "5"},
            candidate_npis=None,
            uses_unified_addresses=False,
            address_zip5_sql="LEFT(addr.postal_code, 5)",
            parameter_map={},
        )
        is None
    )
    assert (
        serving._membership_filter_sql(
            {"npi": "not-an-npi"},
            candidate_npis=None,
            uses_unified_addresses=False,
            address_zip5_sql="LEFT(addr.postal_code, 5)",
            parameter_map={},
        )
        is None
    )


def test_membership_filter_supports_literal_address_and_text_location_filters():
    parameter_map = {}

    filter_sql, distance_sql = serving._membership_filter_sql(
        {
            "state": " il ",
            "city": " chicago ",
            "zip": "60601-1234",
            "npi": "1234567890",
        },
        candidate_npis=None,
        uses_unified_addresses=False,
        address_zip5_sql="LEFT(addr.postal_code, 5)",
        parameter_map=parameter_map,
        literal_service_address_types=True,
        include_taxonomy_filters=False,
    )

    assert "addr.type IN ('primary', 'secondary', 'practice', 'site')" in filter_sql
    assert "state_value" in filter_sql
    assert "city_value" in filter_sql
    assert "LEFT(addr.postal_code, 5) = :zip5" in filter_sql
    assert "addr.npi = :provider_npi" in filter_sql
    assert distance_sql == "NULL::double precision"
    assert parameter_map == {
        "state_value": "IL",
        "city_value": "CHICAGO",
        "zip5": "60601",
        "provider_npi": 1234567890,
    }


def test_membership_filter_appends_geo_clauses_without_zip(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_taxonomy_filters",
        lambda _args, _parameters: ["taxonomy_matches"],
    )
    monkeypatch.setattr(
        serving,
        "_membership_geo_sql",
        lambda *_args, **_kwargs: ("distance_expression", ["geo_matches"]),
    )
    parameter_map = {}

    filter_sql, distance_sql = serving._membership_filter_sql(
        {},
        candidate_npis=(1234567890,),
        uses_unified_addresses=True,
        address_zip5_sql="addr.zip5",
        parameter_map=parameter_map,
    )

    assert "taxonomy_matches" in filter_sql
    assert "geo_matches" in filter_sql
    assert parameter_map["candidate_npis"] == [1234567890]
    assert distance_sql == "distance_expression"


def test_geo_evidence_case_preserves_stable_precedence():
    sql = serving._ptg2_geo_evidence_level_case_sql(
        nppes_condition_sql="nppes_is_valid",
        mrf_condition_sql="mrf_is_valid",
        cms_condition_sql="cms_is_valid",
    )

    assert sql.index("nppes_is_valid") < sql.index("mrf_is_valid")
    assert sql.index("mrf_is_valid") < sql.index("cms_is_valid")
    assert "nppes_registry_address" in sql
    assert "multi_issuer_marketplace_address" in sql
    assert "cms_doctors_source_with_nppes_identity_anchor" in sql


def test_unified_geo_sql_requires_record_level_evidence():
    sql = serving._ptg2_geo_assured_address_sql("addr")

    assert "(addr.address_source_mask & 1) <> 0" in sql
    assert "mrf_address AS geo_mrf" in sql
    assert "source_issuer_names" in sql
    assert "COUNT(DISTINCT LOWER(BTRIM(issuer_name)))" in sql
    assert "UNNEST(geo_mrf.source_import_ids)" in sql
    assert "npi_address AS geo_nppes" in sql
    assert "geo_nppes.date_added IS NOT NULL" in sql
    assert "doctor_clinician_address AS geo_doctor" in sql
    assert "geo_doctor.updated_at IS NOT NULL" in sql
    assert "entity_address_unified AS geo_nppes_anchor" in sql
    assert "npi_address AS geo_nppes_anchor_source" in sql
    assert "geo_nppes_anchor.premise_key = addr.premise_key" in sql
    assert "geo_nppes_anchor.type IN" in sql
    assert "geo_doctor_anchor" not in sql


def test_unified_location_identity_uses_collision_resistant_location_key():
    assert "premise_key" in serving._PTG2_UNIFIED_ADDRESS_COLUMNS
    assert serving._ptg2_address_location_hash_sql(
        "addr", "mrf.entity_address_unified"
    ) == "CONCAT('entity_address_unified:', addr.location_key)"
    assert "checksum" in serving._ptg2_address_location_hash_sql(
        "addr", "mrf.npi_address"
    )


def test_address_provenance_exposes_dataset_version_and_retrieval_time():
    entry = serving._address_provenance_entry(
        {
            "source_id": 2,
            "source_record_key": "mrf:1000000005:fixture",
            "source_import_ids": ["20260710"],
            "source_import_dates": ["2026-07-10"],
            "source_issuer_names": ["Issuer A", "Issuer B"],
            "source_urls": ["https://example.test/providers.json"],
        }
    )

    assert entry == {
        "dataset_id": "marketplace_provider_directory",
        "source_id": 2,
        "source_record_id": "mrf:1000000005:fixture",
        "record_version_id": "20260710",
        "record_version_ids": ["20260710"],
        "retrieved_at": "2026-07-10",
        "issuer_names": ["Issuer A", "Issuer B"],
        "source_urls": ["https://example.test/providers.json"],
    }


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
    assert "STARTS_WITH(sources.source_name, 'facility_anchor:')" in sql
    assert "NULLIF(BTRIM(stored.source_run_id), '')" in sql
    assert "nppes.date_added::text AS source_run_id" in sql
    assert "doctor.updated_at::text AS source_run_id" in sql
    assert "generic_materialization" not in sql
    assert "FROM specific_evidence" in sql
    assert serving._ptg2_mrf_lineage_complete_sql("candidate") in sql
    assert "COALESCE(NULLIF(stored.source_record_key" not in sql


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
    assert address_payload["geo_evidence_level"] == "multi_issuer_marketplace_address"
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


def test_unproven_non_geo_address_is_redacted_without_changing_rate_payload():
    prices = [{"negotiated_rate": "125.00", "billing_class": "professional"}]
    price_summary = [{"min": "125.00", "max": "125.00"}]
    location_rows = [
        {
            "npi": 1990000122,
            "provider_set_key": 71,
            "state": "MI",
            "city": "TEST CITY",
            "zip5": "48201",
            "telephone_number": "555-0100",
            "location_hash": "test-location-hash",
            "prices": prices,
            "price_summary": price_summary,
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

    serving._apply_address_provenance(
        location_rows,
        {},
        include_response_evidence=True,
    )

    assert len(location_rows) == 1
    location_row = location_rows[0]
    assert location_row["npi"] == 1990000122
    assert location_row["provider_set_key"] == 71
    assert location_row["prices"] == prices
    assert location_row["price_summary"] == price_summary
    assert location_row[serving._PTG_UNPROVEN_ADDRESS_MARKER] is True
    assert json.loads(location_row["address_payload"]) == {}
    for field_name in (
        "state",
        "city",
        "zip5",
        "telephone_number",
    ):
        assert field_name not in location_row

    provider_row = serving._graph_provider_data(
        location_row,
        {
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
        },
        "entity_address_unified",
    )
    provider_row.update(
        {
            "prices": prices,
            "tic_prices": prices,
            "price_summary": price_summary,
        }
    )
    item = serving._compact_item_from_row(
        provider_row,
        {"include_evidence": "true"},
    )

    assert item["prices"] == prices
    assert item["price_summary"] == price_summary
    assert item["address_verification"]["displayed_address_present"] is False
    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert "address_provenance" not in item["address_verification"]
    for field_name in (
        "address",
        "state",
        "city",
        "zip5",
        "telephone_number",
        "phone_number",
    ):
        assert field_name not in item


def test_complete_provenance_validates_default_and_is_only_exposed_with_evidence():
    provenance = {
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
            {"proven-location": [provenance]},
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
        provenance
    ]


def test_nullish_contact_values_become_json_null_without_changing_rates():
    address_by_field = {
        "telephone_number": "null",
        "phone_number": "None",
        "fax_number": "undefined",
    }
    prices = [{"negotiated_rate": "405.60"}]

    serving._sanitize_address_contact_payload(address_by_field)

    assert address_by_field == {
        "telephone_number": None,
        "phone_number": None,
        "fax_number": None,
    }
    assert prices == [{"negotiated_rate": "405.60"}]


def test_include_evidence_exposes_truthful_location_confidence():
    payload = {
        "items": [
            {
                "npi": 1000000003,
                "confidence": {
                    "network": "tic_rate_npi_tin",
                    "location": "nppes_provider_address",
                },
            }
        ],
        "query": {},
    }

    default_response = serving._shape_ptg2_response(payload, {})
    evidence_response = serving._shape_ptg2_response(
        payload, {"include_evidence": True}
    )

    assert "confidence" not in default_response["items"][0]
    assert evidence_response["items"][0]["confidence"]["location"] == (
        "nppes_provider_address"
    )


@pytest.mark.asyncio
async def test_membership_location_rows_short_circuits_empty_and_unavailable_queries(
    monkeypatch,
):
    query_builder = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_membership_location_query", query_builder)

    assert (
        await serving._membership_location_rows(
            object(),
            strict_v3_tables(),
            {},
            candidate_npis=(),
            limit=2,
        )
        == []
    )
    query_builder.assert_not_awaited()

    assert (
        await serving._membership_location_rows(
            object(),
            strict_v3_tables(),
            {},
            candidate_npis=None,
            limit=2,
        )
        is None
    )
    query_builder.assert_awaited_once()


@pytest.mark.asyncio
async def test_membership_location_rows_executes_standard_query(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=_location_query()),
    )

    async def validate_default_response(
        _session,
        location_rows,
        *,
        include_response_evidence,
    ):
        assert include_response_evidence is False
        for location_row in location_rows:
            location_row.pop("_geo_evidence_level", None)
            location_row.pop("_geo_evidence_source_id", None)

    monkeypatch.setattr(
        serving,
        "_hydrate_address_provenance",
        validate_default_response,
    )
    session = FakeSession(
        [
            FakeResult(
                [
                    {
                        "npi": 1234567890,
                        "_geo_evidence_level": "nppes_registry_address",
                        "_geo_evidence_source_id": 1,
                    }
                ]
            )
        ]
    )

    location_rows = await serving._membership_location_rows(
        session,
        strict_v3_tables(),
        {},
        candidate_npis=None,
        limit=2,
        offset=3,
    )

    assert location_rows == [{"npi": 1234567890}]
    assert "raw_probe_limit" not in session.calls[0][0][1]


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
    ):
        assert include_response_evidence is True
        location_rows.clear()

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

    assert location_rows == [
        {"_ptg_probe_empty": True, "_ptg_source_exhausted": False}
    ]


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
    assert len(location_rows) == 64
    assert all(
        location_row[serving._PTG_UNPROVEN_ADDRESS_MARKER] is True
        and json.loads(location_row["address_payload"]) == {}
        for location_row in location_rows
    )


@pytest.mark.asyncio
async def test_membership_location_rows_bounds_knn_and_restores_planner(monkeypatch):
    query = _location_query(knn_order_sql="addr.location <-> :request_location")
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=query),
    )
    enable = AsyncMock(return_value=("auto", "2"))
    restore = AsyncMock()
    monkeypatch.setattr(serving, "_enable_serial_knn_planning", enable)
    monkeypatch.setattr(serving, "_restore_knn_planning", restore)
    session = FakeSession([FakeResult([{"npi": 1234567890}])])

    location_rows = await serving._membership_location_rows(
        session,
        strict_v3_tables(),
        {},
        candidate_npis=None,
        limit=2,
    )

    assert location_rows == [
        {
            "npi": 1234567890,
            serving._PTG_UNPROVEN_ADDRESS_MARKER: True,
            "address_payload": "{}",
        }
    ]
    assert query.parameter_map["raw_probe_limit"] == 67
    enable.assert_awaited_once_with(session)
    restore.assert_awaited_once_with(session, ("auto", "2"))


@pytest.mark.asyncio
async def test_membership_location_rows_preserves_knn_query_failure(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_membership_location_query",
        AsyncMock(return_value=_location_query(knn_order_sql="knn_order")),
    )
    monkeypatch.setattr(
        serving,
        "_enable_serial_knn_planning",
        AsyncMock(return_value=("auto", "2")),
    )
    restore = AsyncMock()
    monkeypatch.setattr(serving, "_restore_knn_planning", restore)
    session = FakeSession([RuntimeError("query failed")])

    with pytest.raises(RuntimeError, match="query failed"):
        await serving._membership_location_rows(
            session,
            strict_v3_tables(),
            {},
            candidate_npis=None,
            limit=1,
        )

    restore.assert_not_awaited()
