# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

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
    sql = serving._geo_evidence_level_case_sql(
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
        use_stored_only,
    ):
        assert not include_response_evidence
        assert not use_stored_only
        for location_row in location_rows:
            location_row.pop("_geo_evidence_level", None)
            location_row.pop("_geo_evidence_source_id", None)
        return "available"

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
