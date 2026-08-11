"""Branch-focused public pricing boundary coverage."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage, NotFound

from tests.test_pricing_api import (
    FakeResult,
    FakeSession,
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
    await pricing_module._enrich_provider_service_cost_indices(
        FakeSession(), [], year=2024, internal_codes=[123]
    )
    await pricing_module._enrich_provider_service_cost_indices(
        FakeSession(), [{}], year=2024, internal_codes=[]
    )
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
    session = FakeSession(
        [FakeResult(rows=profile_rows), FakeResult(rows=peer_rows)]
    )

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

    response_payload = json.loads(
        (await pricing_module.list_provider_procedures(request, "123")).body
    )

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

    response_payload = json.loads(
        (await pricing_module.get_provider_procedure(request, "123", "123")).body
    )
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
