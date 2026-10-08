# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.endpoint import npi as npi_module
from api.endpoint import pricing
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
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


@pytest.mark.parametrize(
    ("knn_order_sql", "address_assurance_sql"),
    [
        (None, "TRUE"),
        (None, "addr.geo_evidence_level IS NOT NULL"),
        ("addr.location <-> :requested_location", "addr.geo_evidence_level IS NOT NULL"),
    ],
)
def test_membership_location_sql_keeps_snapshot_scope_join_planner_visible(
    knn_order_sql,
    address_assurance_sql,
):
    query = _location_query(knn_order_sql=knn_order_sql)
    query = replace(
        query,
        address_assurance_sql=address_assurance_sql,
    )

    statement = serving._membership_location_sql(query, limit=2, offset=0)

    assert ("JOIN mrf.ptg2_v3_npi_scope npi_scope ON npi_scope.npi = addr.npi") in statement
    assert "npi_scope.snapshot_key = :shared_snapshot_key" in statement
    assert "SELECT npi_scope.snapshot_key, npi_scope.npi" not in statement


@pytest.mark.parametrize("is_assured", [False, True])
def test_unpaged_location_keeps_complete_scope(is_assured):
    query = replace(
        _location_query(knn_order_sql="addr.location <-> :requested_location"),
        address_assurance_sql="addr.geo_evidence_level IS NOT NULL" if is_assured else "TRUE",
    )
    original_parameter_map = dict(query.parameter_map)
    statement = serving._unpaged_membership_location_sql(query)
    assert "LIMIT :limit" not in statement and "OFFSET :offset" not in statement
    assert "<->" not in statement
    assert "npi_scope.snapshot_key = :shared_snapshot_key" in statement
    assert ("WHERE classified.geo_evidence_level IS NOT NULL" in statement) is is_assured
    assert query.knn_order_sql == "addr.location <-> :requested_location"
    assert query.parameter_map == original_parameter_map


@pytest.mark.asyncio
async def test_native_sources_keep_valid_selected_address(monkeypatch):
    address_key = "00000000-0000-0000-0000-000000000001"
    addresses = [
        dict(npi=1000000001, address_key=address_key, address_sources=["mrf"]),
        dict(npi="invalid", address_key=address_key, address_sources=["mrf"]),
    ]
    source_rows = [
        dict(npi="invalid", address_key=address_key, issuer_name="Synthetic Issuer"),
        dict(
            npi=1000000001,
            address_key=address_key,
            issuer_name="Synthetic Issuer",
            issuer_ids=[None, "invalid", 7, "7"],
            source_urls=[],
        ),
    ]
    monkeypatch.setattr(npi_module, "_is_table_available", AsyncMock(return_value=True))
    reader = AsyncMock(return_value=SimpleNamespace(all=lambda: source_rows))
    monkeypatch.setattr(npi_module, "_execute_stmt", reader)
    await npi_module._attach_mrf_source_details(addresses, session=object())
    assert reader.await_args.kwargs["params"]["npis"] == [1000000001]
    assert addresses[0][npi_module.MRF_SOURCE_DETAIL_KEY][0]["issuer_ids"] == [7]
    assert addresses[0][npi_module.MRF_SOURCE_COUNT_KEY] == 1
    assert npi_module.MRF_SOURCE_DETAIL_KEY not in addresses[1]


@pytest.mark.parametrize("network_status", [pricing.ALLOWED_AMOUNT_NETWORK_STATUS_MIXED, "unknown"])
def test_payment_evidence_keeps_unverified_fields_separate(network_status):
    provider_by_field = dict(
        network_statuses=[network_status],
        address_payload="invalid JSON",
        allowed_amount_min="invalid",
        evidence_count="invalid",
    )
    provider_item = pricing._allowed_amount_provider_item(
        npi=1000000001,
        payment_rows=[{"network_status": network_status}],
        provider_by_field=provider_by_field,
        code="27447",
        code_system="CPT",
    )
    assert provider_item["npi"] == 1000000001
    expected_disclaimer = (
        "Historical allowed amount with mixed network status; not a contracted negotiated rate."
        if network_status == pricing.ALLOWED_AMOUNT_NETWORK_STATUS_MIXED
        else "Historical out-of-network or not-confirmed-in-network allowed amount; not a negotiated rate."
    )
    assert provider_item["prices"][0]["disclaimer"] == expected_disclaimer
    assert provider_item["prices"][0]["price_type"] == "historical_allowed_amount"
    assert pricing._allowed_amount_sources("invalid JSON") == []
    assert "address" not in provider_item
    assert provider_item["allowed_amount_min"] is None
    assert provider_item["evidence_count"] == 0


def test_provider_totals_keep_native_legacy_aliases():
    provider_by_field = dict(
        total_services=3,
        total_reported_service_codes=2,
        total_submitted_charges=9,
        total_beneficiaries=1,
        total_allowed_amount="2.000000000001",
    )
    canonical_by_field = dict(provider_by_field)
    pricing._add_legacy_provider_totals(provider_by_field)
    assert all(provider_by_field[key] == value for key, value in canonical_by_field.items())
    assert provider_by_field["total_claims"] == 3
    assert provider_by_field["total_30day_fills"] == 2
    assert provider_by_field["total_day_supply"] == 9
    assert provider_by_field["total_benes"] == 1
    assert provider_by_field["total_drug_cost"] == "2.000000000001"


def test_unified_geo_retains_service_location_types(monkeypatch):
    monkeypatch.setattr(npi_module, "_should_include_geo_service_locations", lambda: True)
    clause = npi_module._nearby_geo_type_clause("mrf.entity_address_unified")
    assert all(f"'{address_type}'" in clause for address_type in npi_module.GEO_SERVICE_LOCATION_TYPES)
    assert npi_module._nearby_geo_type_clause("mrf.npi_address") == "AND (a.type = 'primary' OR a.type = 'secondary')"


@pytest.mark.parametrize("window_count", [0, 2])
def test_unpaged_location_rejects_ambiguous_window(monkeypatch, window_count):
    monkeypatch.setattr(
        serving,
        "_membership_location_sql",
        lambda *_args, **_kwargs: "SELECT 1 " + "LIMIT :limit OFFSET :offset " * window_count,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="invalid page window"):
        serving._unpaged_membership_location_sql(_location_query())


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
    assert (
        serving._ptg2_address_location_hash_sql("addr", "mrf.entity_address_unified")
        == "CONCAT('entity_address_unified:', addr.location_key)"
    )
    assert "checksum" in serving._ptg2_address_location_hash_sql("addr", "mrf.npi_address")


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
    evidence_response = serving._shape_ptg2_response(payload, {"include_evidence": True})

    assert "confidence" not in default_response["items"][0]
    assert evidence_response["items"][0]["confidence"]["location"] == ("nppes_provider_address")


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
    enable = AsyncMock(return_value=("auto", "2", "on"))
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
    restore.assert_awaited_once_with(session, ("auto", "2", "on"))


@pytest.mark.asyncio
async def test_large_exact_npi_scope_disables_location_jit(monkeypatch):
    query = _location_query()
    query.parameter_map["candidate_npis"] = list(range(4097))
    enable = AsyncMock(return_value=("auto", "2", "on"))
    restore = AsyncMock()
    monkeypatch.setattr(serving, "_enable_serial_knn_planning", enable)
    monkeypatch.setattr(serving, "_restore_knn_planning", restore)

    location_rows = await serving._execute_membership_location_sql(
        FakeSession([FakeResult([])]), query, "SELECT 1 WHERE FALSE", offset=0
    )

    assert location_rows == []
    enable.assert_awaited_once()
    restore.assert_awaited_once()


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
        AsyncMock(return_value=("auto", "2", "on")),
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
