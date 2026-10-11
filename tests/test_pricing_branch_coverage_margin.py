"""Branch-focused public pricing boundary coverage."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage, NotFound

from tests.test_pricing_api import (
    FakeResult,
    FakeSession,
    _canonical_release_selection,
    _patch_group_plan_snapshot_tables,
    make_request,
    pricing_module,
)


def _estimated_score_row(peer_count: int = 12) -> dict[str, object]:
    return {
        "peer_count": peer_count,
        "risk_ratio_point": 0.8,
        "ci75_low": 0.7,
        "ci75_high": 0.9,
        "ci90_low": 0.6,
        "ci90_high": 1.0,
        "score_0_100": 82.0,
    }


@pytest.mark.asyncio
async def test_estimated_quality_modes_skip_empty_and_unknown_cohorts():
    domain_rows = [
        {"domain": "unknown", "peer_count": 12},
        {
            "domain": "cost",
            "peer_count": 12,
            "risk_ratio": 0.75,
            "score_0_100": 85.0,
            "ci75_low": 0.7,
            "ci75_high": 0.8,
            "ci90_low": 0.6,
            "ci90_high": 0.9,
        },
    ]
    session = FakeSession(
        [
            FakeResult(rows=[]),
            FakeResult(rows=[_estimated_score_row(peer_count=0)]),
            FakeResult(rows=[_estimated_score_row()]),
            FakeResult(rows=domain_rows),
        ]
    )
    profile_by_field = {
        "taxonomy_code": "207Q00000X",
        "specialty_key": "family_medicine",
        "provider_class": "individual",
        "zip5": "60654",
        "state_key": "IL",
        "location_source": "synthetic",
    }

    scores_by_mode = await pricing_module._load_estimated_quality_modes(
        session,
        profile=profile_by_field,
        year=2024,
    )

    assert scores_by_mode["zip"] is None
    assert scores_by_mode["state"] is None
    assert scores_by_mode["national"]["score_method"] == "estimated"
    assert scores_by_mode["national"]["domains"]["cost"]["score_0_100"] == 85.0
    parameter_maps = [execution_args[1] for execution_args, _execution_kwargs in session.executions]
    assert [parameters["benchmark_mode"] for parameters in parameter_maps] == [
        "zip",
        "state",
        "national",
        "national",
    ]
    assert parameter_maps[0]["zip5"] == "60654"
    assert "state_key" not in parameter_maps[0]
    assert parameter_maps[1]["state_key"] == "IL"
    assert "zip5" not in parameter_maps[1]


@pytest.mark.asyncio
async def test_estimated_quality_modes_use_specialty_or_skip_missing_identity():
    specialty_session = FakeSession(
        [
            FakeResult(rows=[_estimated_score_row()]),
            FakeResult(rows=[]),
        ]
    )
    specialty_profile_by_field = {
        "taxonomy_code": None,
        "specialty_key": "family_medicine",
        "provider_class": "unknown",
        "zip5": None,
        "state_key": None,
    }
    scores_by_mode = await pricing_module._load_estimated_quality_modes(
        specialty_session,
        profile=specialty_profile_by_field,
        year=2024,
    )
    assert scores_by_mode["national"]["score_method"] == "estimated"

    missing_identity_session = FakeSession()
    missing_identity_profile_by_field = {
        "taxonomy_code": None,
        "specialty_key": None,
        "provider_class": "unknown",
        "zip5": None,
        "state_key": None,
    }
    empty_scores = await pricing_module._load_estimated_quality_modes(
        missing_identity_session,
        profile=missing_identity_profile_by_field,
        year=2024,
    )
    assert all(score is None for score in empty_scores.values())
    assert missing_identity_session.executions == []

    no_zip_scores = await pricing_module._load_estimated_quality_modes(
        FakeSession(),
        profile=missing_identity_profile_by_field,
        year=2024,
        benchmark_mode="zip",
    )
    assert all(score is None for score in no_zip_scores.values())


@pytest.mark.asyncio
async def test_cost_index_enrichment_stops_at_missing_inputs_or_tables(monkeypatch):
    availability = AsyncMock(return_value=True)
    monkeypatch.setattr(pricing_module, "_is_table_available", availability)
    await pricing_module._enrich_provider_service_cost_indices(FakeSession(), [], year=2024, internal_codes=[123])
    await pricing_module._enrich_provider_service_cost_indices(FakeSession(), [{}], year=2024, internal_codes=[])
    assert availability.await_count == 0

    monkeypatch.setattr(
        pricing_module,
        "_is_table_available",
        AsyncMock(return_value=False),
    )
    await pricing_module._enrich_provider_service_cost_indices(
        FakeSession(), [{"npi": 123}], year=2024, internal_codes=[123]
    )

    second_table_missing = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(
        pricing_module,
        "_is_table_available",
        second_table_missing,
    )
    await pricing_module._enrich_provider_service_cost_indices(
        FakeSession(), [{"npi": 123}], year=2024, internal_codes=[123]
    )
    assert second_table_missing.await_count == 2


@pytest.mark.asyncio
async def test_cost_index_enrichment_filters_rows_and_uses_all_specialty_peer(monkeypatch):
    """Filter invalid profiles and use the all-specialty peer fallback."""
    monkeypatch.setattr(
        pricing_module,
        "_is_table_available",
        AsyncMock(return_value=True),
    )
    provider_service_rows = _provider_service_cost_rows()
    profile_rows = _provider_cost_profile_rows()
    peer_rows = _provider_cost_peer_rows()
    session = FakeSession([FakeResult(rows=profile_rows), FakeResult(rows=peer_rows)])

    await pricing_module._enrich_provider_service_cost_indices(
        session,
        provider_service_rows,
        year=2024,
        internal_codes=[123],
    )

    assert provider_service_rows[3]["cost_index"] == "$$$$$"
    assert "cost_index" not in provider_service_rows[4]


def _provider_service_cost_rows():
    return [
        {"npi": None},
        {"npi": "bad"},
        {"npi": "-1"},
        {"npi": "123", "state": "IL", "city": "Chicago", "zip5": "60654"},
        {"npi": "124", "state": "IL", "city": "Chicago", "zip5": "60654"},
    ]


def _provider_cost_profile_rows():
    return [
        {"npi": None},
        {"npi": "bad"},
        {"npi": 123, "geography_scope": "", "geography_value": ""},
        {
            "npi": 123,
            "procedure_code": 123,
            "year": 2024,
            "geography_scope": "zip5",
            "geography_value": "60654",
            "specialty_key": "",
            "setting_key": "all",
            "claim_count": 20,
            "avg_submitted_charge": 50.0,
        },
        {
            "npi": 124,
            "procedure_code": 123,
            "year": 2024,
            "geography_scope": "zip5",
            "geography_value": "60654",
            "specialty_key": "",
            "setting_key": "all",
            "claim_count": 10,
            "avg_submitted_charge": None,
        },
    ]


def _provider_cost_peer_rows():
    return [
        {"procedure_code": "bad", "year": 2024},
        {
            "procedure_code": 123,
            "year": 2024,
            "geography_scope": "zip5",
            "geography_value": "60654",
            "specialty_key": "__all__",
            "setting_key": "all",
            "p20": 10.0,
            "p40": 20.0,
            "p60": 30.0,
            "p80": 40.0,
        },
    ]


@pytest.mark.asyncio
async def test_provider_procedure_list_rejects_invalid_path_and_plan_type():
    with pytest.raises(InvalidUsage, match="npi"):
        await pricing_module.list_provider_procedures(make_request([]), "")
    with pytest.raises(InvalidUsage, match="plan_id_type"):
        await pricing_module.list_provider_procedures(
            make_request([], args={"plan_id_type": "other"}),
            "123",
        )


@pytest.mark.asyncio
async def test_provider_procedure_list_distinguishes_plan_route_states(monkeypatch):
    monkeypatch.setattr(
        pricing_module,
        "search_ptg2_provider_procedures",
        AsyncMock(return_value=None),
    )
    no_match_response = await pricing_module.list_provider_procedures(
        make_request([], args={"source_key": "source-a", "year": "2024"}),
        "123",
    )
    no_match_by_field = json.loads(no_match_response.body)
    assert no_match_by_field["query"]["status"] == "no_match"
    assert no_match_by_field["query"]["ignored_params"] == ["year"]

    no_route_response = await pricing_module.list_provider_procedures(
        make_request([], args={"plan_id": "plan-a"}),
        "123",
    )
    no_route_by_field = json.loads(no_route_response.body)
    assert no_route_by_field["query"]["status"] == "no_route"
    assert no_route_by_field["resolved"] is False


@pytest.mark.asyncio
async def test_provider_procedure_list_applies_every_legacy_filter(monkeypatch):
    code_context_by_field = {
        "input_code": {"code_system": "CPT", "code": "12345"},
        "resolved_codes": [{"code_system": "HP_PROCEDURE_CODE", "code": "123"}],
        "matched_via": "exact",
    }
    monkeypatch.setattr(
        pricing_module,
        "_resolve_internal_codes_for_request",
        AsyncMock(return_value=([123], code_context_by_field)),
    )
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 123,
                        "procedure_code": 123,
                        "service_description": "Synthetic service",
                        "reported_code": "12345",
                        "total_services": 5,
                        "total_allowed_amount": 100.0,
                    }
                ]
            ),
        ],
        args={
            "year": "2024",
            "service_name": "service",
            "reported_code": "123",
            "q": "synthetic",
            "code": "12345",
            "min_claims": "1",
            "min_total_cost": "10",
        },
    )

    response_payload = json.loads((await pricing_module.list_provider_procedures(request, "123")).body)

    assert response_payload["pagination"]["total"] == 1
    assert response_payload["query"]["input_code"] == code_context_by_field["input_code"]
    assert response_payload["query"]["min_claims"] == 1.0
    assert response_payload["query"]["min_total_cost"] == 10.0
    assert len(request.ctx.sa_session.executions) == 2


@pytest.mark.asyncio
async def test_provider_procedure_detail_rejects_empty_npi_and_returns_match(monkeypatch):
    with pytest.raises(InvalidUsage, match="npi"):
        await pricing_module.get_provider_procedure(make_request([]), "", "123")

    code_context_by_field = {
        "input_code": {"code_system": "HP_PROCEDURE_CODE", "code": "123"},
        "resolved_codes": [{"code_system": "HP_PROCEDURE_CODE", "code": "123"}],
        "matched_via": "internal",
    }
    monkeypatch.setattr(
        pricing_module,
        "_resolve_internal_codes_for_request",
        AsyncMock(return_value=([123], code_context_by_field)),
    )
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 123,
                        "procedure_code": 123,
                        "service_description": "Synthetic service",
                        "total_services": 5,
                    }
                ]
            )
        ],
        args={"year": "2024"},
    )

    response_payload = json.loads((await pricing_module.get_provider_procedure(request, "123", "123")).body)
    assert response_payload["service_code"] == "123"
    assert response_payload["year_used"] == 2024
    assert response_payload["matched_via"] == "internal"


@pytest.mark.asyncio
async def test_provider_score_rejects_missing_path_and_quality_tables(monkeypatch):
    with pytest.raises(InvalidUsage, match="npi"):
        await pricing_module.get_pricing_provider_score(make_request([]), "")

    monkeypatch.setattr(
        pricing_module,
        "_is_table_available",
        AsyncMock(return_value=False),
    )
    with pytest.raises(NotFound, match="score table"):
        await pricing_module.get_pricing_provider_score(make_request([]), "123")

    monkeypatch.setattr(
        pricing_module,
        "_is_table_available",
        AsyncMock(side_effect=[True, False]),
    )
    with pytest.raises(NotFound, match="domain table"):
        await pricing_module.get_pricing_provider_score(make_request([]), "123")


@pytest.mark.asyncio
async def test_provider_score_rejects_unavailable_requested_mode(monkeypatch):
    monkeypatch.setattr(
        pricing_module,
        "_is_table_available",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        pricing_module,
        "_resolve_quality_year",
        AsyncMock(return_value=(2024, "request")),
    )
    request = make_request(
        [
            FakeResult(rows=[{"benchmark_mode": "state", "score_0_100": 80.0}]),
        ],
        args={"benchmark_mode": "zip", "year": "2024"},
    )

    with pytest.raises(NotFound, match="benchmark_mode='zip'"):
        await pricing_module.get_pricing_provider_score(request, "123")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit", "cursor", "bound_limit", "bound_cursor"),
    [("invalid", "invalid", 200, 0), ("0", "-1", 1, 0), ("1001", "1000000000", 1000, 1000000000)],
)
async def test_group_provider_bounds_are_normalized(monkeypatch, limit, cursor, bound_limit, bound_cursor):
    _patch_group_plan_snapshot_tables(monkeypatch)
    request = make_request(
        [FakeResult(rows=[SimpleNamespace(npi=1000000001)])],
        args={"plan_id": "SYNTHETIC-PLAN", "limit": limit, "cursor": cursor},
    )

    document = json.loads((await pricing_module.group_plan_providers(request)).body)

    parameters = request.ctx.sa_session.executions[0][0][1]
    assert parameters["limit"] == bound_limit
    assert parameters["cursor_npi"] == bound_cursor
    assert document["providers"]["items"] == [{"npi": 1000000001}]
    assert document["providers"]["next_cursor"] == ("1000000001" if bound_limit == 1 else None)
    assert document["exhausted"] is (bound_limit != 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("unified", [False, True])
async def test_group_provider_mail_address_enrichment(monkeypatch, unified):
    _patch_group_plan_snapshot_tables(monkeypatch)
    monkeypatch.setattr(
        pricing_module,
        "_group_plan_provider_address_source",
        AsyncMock(return_value=("mrf.entity_address_unified" if unified else "mrf.npi_address", unified, False, False)),
    )
    address_by_field = {
        "npi": 1000000001,
        "type": "mail",
        "first_line": "1 Example Street",
        "city_name": "EXAMPLE",
        "state_name": "IL",
        "postal_code": "60601-1234",
        "plan_coverage_match": False,
        "address_precision": "street",
    }
    enrichment_rows = [
        SimpleNamespace(
            npi=1000000001,
            provider_first_name=None,
            provider_last_name=None,
            provider_organization_name="Synthetic Clinic",
            provider_credential_text=None,
            entity_type_code=2,
            provider_sex_code=None,
        ),
        SimpleNamespace(
            npi=1000000002,
            provider_first_name="Example",
            provider_last_name="Person",
            provider_organization_name=None,
            provider_credential_text="MD",
            entity_type_code=1,
            provider_sex_code="F",
        ),
    ]
    request = make_request(
        [
            FakeResult(rows=[SimpleNamespace(npi=npi) for npi in (1000000001, 1000000002, 1000000003)]),
            *([] if unified else [FakeResult(scalar=3)]),
            FakeResult(rows=[address_by_field]),
            FakeResult(rows=enrichment_rows),
        ],
        args={
            "plan_id": "SYNTHETIC-PLAN",
            "city": " Example ",
            "state": " il ",
            "enrich": "yes",
            "include_mail_addresses": "true",
            "count": "true",
        },
    )

    document = json.loads((await pricing_module.group_plan_providers(request)).body)

    _assert_mail_enrichment(request, document, unified)


def _assert_mail_enrichment(request, document, unified):
    providers = document["providers"]["items"]
    assert providers[0]["name"] == "Synthetic Clinic"
    assert providers[1]["name"] == "Example Person"
    assert providers[1]["credential"] == "MD"
    assert providers[1]["provider_sex_code"] == "F"
    assert providers[2] == {"npi": 1000000003}
    assert providers[0]["address"] == providers[0]["addresses"][0]
    assert providers[0]["address"]["zip5"] == "60601"
    assert ("plan_coverage_match" in providers[0]["address"]) is unified
    if unified:
        assert providers[0]["address"]["plan_coverage_match"] is False
    assert document["location_filter"]["address_types"] is None
    assert document["location_filter"]["count_exact"] is (not unified)
    assert document["providers"]["total"] == (None if unified else 3)
    page_sql, parameters = request.ctx.sa_session.executions[0][0]
    assert "addr.type IN" not in str(page_sql)
    assert parameters["location_city"] == "example"
    assert parameters["location_state"] == "IL"
    address_sql, address_parameters = request.ctx.sa_session.executions[-2][0]
    assert "addr.type IN" not in str(address_sql)
    assert address_parameters["npis"] == [1000000001, 1000000002, 1000000003]


@pytest.mark.asyncio
async def test_group_provider_requires_plan_scope():
    request = make_request([])

    with pytest.raises(InvalidUsage, match="plan_id .* or plan_release_id is required"):
        await pricing_module.group_plan_providers(request)

    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_group_provider_rejects_unbound_storage(monkeypatch):
    _patch_group_plan_snapshot_tables(monkeypatch)
    monkeypatch.setattr(
        pricing_module,
        "snapshot_serving_tables",
        AsyncMock(return_value=SimpleNamespace(uses_shared_blocks=False)),
    )
    request = make_request([], args={"plan_id": "SYNTHETIC-PLAN"})

    with pytest.raises(RuntimeError, match="not bound to strict shared-block V3 storage"):
        await pricing_module.group_plan_providers(request)

    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_group_release_without_network_stays_resolved(monkeypatch):
    selection = _canonical_release_selection([])
    monkeypatch.setattr(pricing_module, "resolve_plan_release_serving", AsyncMock(return_value=selection))
    request = make_request([], args={"plan_release_id": selection.plan_release_id})

    document = json.loads((await pricing_module.group_plan_providers(request)).body)

    assert document["resolved"] is True
    assert document["query"]["status"] == "no_match"
    assert document["result_state"] == "no_matching_providers"
    assert document["reason"] == "published plan release has no in-network provider binding"
    assert document["providers"] == {"count": 0, "items": [], "next_cursor": None}
    assert document["healthporta_plan_id"] == selection.healthporta_plan_id
    assert document["query"]["plan_release_id"] == selection.plan_release_id
    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_local_group_candidates_keep_taxonomy_and_sex(monkeypatch):
    _patch_group_plan_snapshot_tables(monkeypatch)
    monkeypatch.setattr(
        pricing_module,
        "_group_plan_provider_address_source",
        AsyncMock(return_value=("mrf.entity_address_unified", True, True, True)),
    )
    request = make_request(
        [FakeResult(rows=[SimpleNamespace(npi=1000000001)]), FakeResult(scalar=2), FakeResult(rows=[])],
        args={
            "plan_id": "SYNTHETIC-PLAN",
            "taxonomy_codes": "207Q00000X",
            "provider_sex_code": "f",
            "zip5": "60601",
            "zip_radius_miles": "0",
            "count": "true",
            "limit": "1",
        },
    )

    document = json.loads((await pricing_module.group_plan_providers(request)).body)

    assert document["providers"]["total"] == 2
    assert document["providers"]["next_cursor"] == "1000000001"
    assert document["location_filter"]["count_exact"] is True
    assert document["provider_sex_code"] == "F"
    assert document["taxonomy_filter"]["taxonomy_code_set"] == ["207Q00000X"]
    page_sql, parameters = request.ctx.sa_session.executions[0][0]
    count_sql = request.ctx.sa_session.executions[1][0][0]
    assert "addr.taxonomy_array &&" in str(page_sql)
    assert "cand_provider_specialty_nt.healthcare_provider_taxonomy_code IN" in str(page_sql)
    assert "group_candidate_sex_npi.provider_sex_code" in str(page_sql)
    assert parameters["group_candidate_sex_provider_sex_code"] == "F"
    assert "local_specialty_npis" in str(count_sql)
    assert "COUNT(DISTINCT gm.npi)" in str(count_sql)
    assert "cursor_npi" not in str(count_sql)
    assert parameters["cand_array_taxonomy_code_0"] == "207Q00000X"
    assert parameters["location_zips"] == ["60601"]
