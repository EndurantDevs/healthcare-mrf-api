# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


def _overlay_provider_rows():
    address_key = "00000000-0000-0000-0000-000000000011"
    original_row_map = {
        "npi": 11,
        "address_key": address_key,
        "address_payload": ["malformed shape"],
        "provider_name": "Eleven",
    }
    unmatched_row_map = {
        "npi": 12,
        "address_key": "00000000-0000-0000-0000-000000000012",
    }
    return address_key, original_row_map, unmatched_row_map


def _provider_directory_corroboration_row(address_key):
    return {
        "npi": 11,
        "address_key": address_key,
        "address_network_binding": "payer_directory_corroborated_location",
        "provider_directory_plan_context_matched": False,
        "provider_directory_source_id": "directory-a",
        "provider_directory_org_name": "Coverage Health",
        "provider_directory_plan_name": "Coverage Plan",
        "provider_directory_provider_resource_id": "provider-a",
        "provider_directory_provider_name": "Coverage Provider",
        "provider_directory_role_resource_id": "role-a",
        "provider_directory_location_resource_id": "location-a",
        "provider_directory_location_name": "Coverage Clinic",
        "provider_directory_network_refs": ["Organization/network-a"],
        "provider_directory_insurance_plan_refs": ["InsurancePlan/plan-a"],
        "provider_directory_network_names": ["Coverage Network"],
        "provider_directory_network_matches": [{"name": "Coverage Network"}],
        "provider_directory_network_context_present": True,
        "provider_directory_insurance_plan_matches": ["plan-a"],
        "provider_directory_match_type": "npi_address",
        "address_verification_evidence": {"matched_on": "npi_address_key"},
        "provider_directory_telephone_number": "312-555-0100",
        "provider_directory_phone_number": "3125550100",
        "provider_directory_phone_extension": "7",
        "provider_directory_fax_number": "312-555-0199",
        "provider_directory_fax_number_digits": "3125550199",
        "provider_directory_fax_extension": "8",
    }


@pytest.mark.asyncio
async def test_table_columns_handles_invalid_rows_and_optional_query_failure():
    invalid_session = FakeSession()
    assert (
        await serving._ptg2_table_columns(invalid_session, "unsafe table")
        == frozenset()
    )
    assert invalid_session.calls == []

    catalog_rows = [
        SimpleNamespace(_mapping={"column_name": "npi"}),
        ("address_key",),
        (None,),
    ]
    session = FakeSession([FakeResult(catalog_rows)])
    assert await serving._ptg2_table_columns(session, "mrf.npi_address") == frozenset(
        {"npi", "address_key"}
    )

    failing_session = FakeSession([RuntimeError("catalog unavailable")])
    assert (
        await serving._ptg2_table_columns(failing_session, "mrf.npi_address")
        == frozenset()
    )
    assert failing_session.rollback_count == 1


@pytest.mark.asyncio
async def test_taxonomy_rows_skip_missing_npi_and_normalize_optional_fields():
    session = FakeSession(
        [
            FakeResult(scalar="mrf.npi_taxonomy"),
            FakeResult(scalar="mrf.nucc_taxonomy"),
            FakeResult(
                [
                    {"npi": None, "taxonomy_codes": ["ignored"]},
                    {
                        "npi": 11,
                        "taxonomy_codes": None,
                        "specialties": ["Family Medicine"],
                        "classifications": None,
                        "specializations": ["Adult Medicine"],
                        "primary_specialty": "Family Medicine",
                        "primary_specialization": None,
                    },
                ]
            ),
        ]
    )

    taxonomy_by_npi = await serving._taxonomy_rows_for_npis(session, (11, 12))

    assert taxonomy_by_npi == {
        11: {
            "taxonomy_codes": [],
            "specialties": ["Family Medicine"],
            "classifications": [],
            "specializations": ["Adult Medicine"],
            "primary_specialty": "Family Medicine",
            "primary_specialization": None,
        }
    }


@pytest.mark.asyncio
async def test_membership_location_query_rejects_unavailable_or_invalid_locations(
    monkeypatch,
):
    address_table = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_ptg2_address_serving_table", address_table)

    assert (
        await serving._membership_location_query(
            object(),
            strict_v3_tables(),
            {},
            candidate_npis=None,
            limit=10,
        )
        is None
    )

    address_table.return_value = "mrf.npi_address"
    assert (
        await serving._membership_location_query(
            object(),
            strict_v3_tables(),
            {"radius_miles": "5"},
            candidate_npis=None,
            limit=10,
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("unified", [False, True])
async def test_membership_location_query_builds_bounded_context(monkeypatch, unified):
    address_table = "mrf.entity_address_unified" if unified else "mrf.npi_address"
    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value=address_table),
    )
    args = {"lat": "41.9", "long": "-87.6"} if unified else {"state": "IL"}

    query = await serving._membership_location_query(
        object(),
        strict_v3_tables(),
        args,
        candidate_npis=None,
        limit=0,
        offset=0 if unified else -1,
    )

    assert query is not None
    assert query.address_table == address_table
    assert query.parameter_map["limit"] == 1
    assert query.parameter_map["offset"] == 0
    assert query.parameter_map["shared_snapshot_key"] == 41
    assert "npi_scope.snapshot_key = :shared_snapshot_key" in query.filter_sql
    if unified:
        assert query.knn_order_sql is not None
        assert "ST_DWithin" in query.filter_sql
    else:
        assert query.knn_order_sql is None
        assert query.parameter_map["state_value"] == "IL"


@pytest.mark.asyncio
async def test_overlay_corroboration_passthrough_paths(monkeypatch):
    provider_rows = [{"npi": 11, "provider_name": "Eleven"}]

    assert (
        await serving._overlay_provider_directory_corroboration(
            object(), [], plan_id="plan-a"
        )
        == []
    )
    assert (
        await serving._overlay_provider_directory_corroboration(object(), provider_rows)
        is provider_rows
    )

    table_lookup = AsyncMock(
        return_value="mrf.provider_directory_address_corroboration"
    )
    monkeypatch.setattr(
        serving, "_ptg2_provider_directory_corroboration_table", table_lookup
    )
    invalid_rows = [
        {"npi": "invalid", "address_key": "address-a"},
        {"npi": 12},
    ]
    assert (
        await serving._overlay_provider_directory_corroboration(
            object(), invalid_rows, plan_id="plan-a"
        )
        is invalid_rows
    )
    table_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_overlay_corroboration_handles_missing_table_and_query_failure(
    monkeypatch,
):
    provider_rows = [{"npi": 11, "address_key": "00000000-0000-0000-0000-000000000011"}]
    table_lookup = AsyncMock(return_value=None)
    monkeypatch.setattr(
        serving, "_ptg2_provider_directory_corroboration_table", table_lookup
    )
    assert (
        await serving._overlay_provider_directory_corroboration(
            object(), provider_rows, source_key="source-a"
        )
        is provider_rows
    )

    table_lookup.return_value = "mrf.provider_directory_address_corroboration"
    session = FakeSession([RuntimeError("corroboration unavailable")])
    assert (
        await serving._overlay_provider_directory_corroboration(
            session, provider_rows, source_key="source-a"
        )
        is provider_rows
    )
    assert session.rollback_count == 1

    session = FakeSession([FakeResult()])
    assert (
        await serving._overlay_provider_directory_corroboration(
            session, provider_rows, source_key="source-a"
        )
        is provider_rows
    )


@pytest.mark.asyncio
async def test_overlay_corroboration_updates_contact_and_directory_evidence(
    monkeypatch,
):
    address_key, original_row_map, unmatched_row_map = _overlay_provider_rows()
    corroboration_row_map = _provider_directory_corroboration_row(address_key)
    monkeypatch.setattr(
        serving,
        "_ptg2_provider_directory_corroboration_table",
        AsyncMock(return_value="mrf.provider_directory_address_corroboration"),
    )
    session = FakeSession(
        [
            FakeResult(
                [
                    {"npi": None, "address_key": address_key},
                    {"npi": 11, "address_key": None},
                    corroboration_row_map,
                ]
            )
        ]
    )

    overlaid_rows = await serving._overlay_provider_directory_corroboration(
        session,
        [original_row_map, unmatched_row_map],
        plan_id="plan-a",
        snapshot_id="snapshot-a",
        source_key="source-a",
    )

    assert original_row_map["address_payload"] == ["malformed shape"]
    assert overlaid_rows[1] is unmatched_row_map
    updated_row_map = overlaid_rows[0]
    assert updated_row_map["location_source"] == "provider_directory_fhir"
    assert updated_row_map["location_confidence_code"] == "provider_directory_address"
    assert updated_row_map["telephone_number"] == "312-555-0100"
    assert updated_row_map["phone_number"] == "3125550100"
    assert updated_row_map["phone_extension"] == "7"
    assert updated_row_map["fax_number"] == "312-555-0199"
    assert updated_row_map["fax_number_digits"] == "3125550199"
    assert updated_row_map["fax_extension"] == "8"
    assert updated_row_map["address_payload"]["address_sources"] == [
        "provider_directory_fhir"
    ]
    assert updated_row_map["address_payload"]["phone_extension"] == "7"
    assert updated_row_map["address_payload"]["fax_extension"] == "8"


@pytest.mark.asyncio
async def test_direct_group_ids_by_npi_respects_bounded_scope(monkeypatch):
    empty_scope = serving._ManifestRateScope((), frozenset(), 0)
    assert (
        await serving._direct_group_ids_by_npi(
            object(), strict_v3_tables(), empty_scope
        )
        is None
    )

    oversized_scope = serving._ManifestRateScope(
        tuple(f"{index:032x}" for index in range(1025)), frozenset(), 1025
    )
    assert (
        await serving._direct_group_ids_by_npi(
            object(), strict_v3_tables(), oversized_scope
        )
        is None
    )

    group_a = "01" * 16
    group_b = "02" * 16
    graph_lookup = AsyncMock(
        return_value={
            group_a: (
                serving._ptg2_npi_member_id(11),
                "not-hex",
                "00" * 16,
            ),
            group_b: (serving._ptg2_npi_member_id(11),),
        }
    )
    monkeypatch.setattr(serving, "_shared_graph_members_by_id", graph_lookup)

    group_ids_by_npi = await serving._direct_group_ids_by_npi(
        object(),
        strict_v3_tables(),
        serving._ptg2_build_rate_scope((group_a, group_b)),
    )

    assert group_ids_by_npi == {11: {group_a, group_b}}
