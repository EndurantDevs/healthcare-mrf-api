# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from collections import OrderedDict

import pytest

from tests.test_pricing_api import (
    FakeResult,
    FakeSession,
    list_providers_by_procedure,
    make_request,
    pricing_module,
)


async def _resolve_single_internal_code(
    _session,
    _code,
    _args,
    default_system=None,
):
    return [1099885900], {
        "input_code": {"code_system": default_system, "code": "99213"},
        "resolved_codes": [
            {
                "code_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM,
                "code": "1099885900",
            }
        ],
        "matched_via": [],
    }


async def _skip_cost_enrichment(*_args, **_kwargs):
    return None


@pytest.mark.parametrize(
    ("order", "expected_order"),
    (
        ("desc", ("total_allowed_amount DESC", "npi ASC")),
        ("asc", ("total_allowed_amount ASC", "npi DESC")),
    ),
)
def test_single_code_consecutive_pages_break_amount_ties_by_npi(
    order,
    expected_order,
):
    queries = (
        pricing_module._single_procedure_provider_page_query(
            year=2023,
            procedure_code=1099885900,
            order=order,
            limit=1,
            offset=offset,
        )
        for offset in (0, 1)
    )

    for query in queries:
        assert tuple(str(clause).rsplit(".", 1)[-1] for clause in query._order_by_clauses) == expected_order


@pytest.mark.asyncio
async def test_single_code_taxonomy_uses_precomputed_signal():
    """Avoid rebuilding taxonomy evidence from provider-level claims."""

    evidence_by_field = {
        "taxonomy_code": "207Q00000X",
        "classification": "Family Medicine",
        "specialization": None,
        "display_name": "Family Medicine",
        "distinct_npis": 80,
        "total_services": 500.0,
        "total_beneficiaries": 400.0,
        "provider_types": ["Family Practice"],
    }
    session = FakeSession(
        [
            FakeResult(scalar="mrf.procedure_taxonomy_signal"),
            FakeResult(rows=[evidence_by_field]),
        ]
    )
    pricing_module._PROCEDURE_TAXONOMY_EVIDENCE_CACHE.clear()

    evidence = await pricing_module._load_procedure_taxonomy_evidence(
        session,
        year=2023,
        internal_codes=[1099885900],
        limit=10,
    )

    assert evidence == [pricing_module._taxonomy_evidence_item(evidence_by_field)]
    statements = [str(execution_args[0]) for execution_args, _execution_kwargs in session.executions]
    assert any("procedure_taxonomy_signal" in statement for statement in statements)
    assert any("source_relation_fingerprint" in statement and "to_regclass" in statement for statement in statements)
    assert not any("FROM mrf.pricing_provider_procedure" in statement for statement in statements)


@pytest.mark.asyncio
async def test_taxonomy_signal_unavailable_keeps_existing_query():
    """Fall back unchanged while the new table is absent or mid-rollout."""

    evidence_by_field = {
        "taxonomy_code": "207Q00000X",
        "classification": "Family Medicine",
        "specialization": None,
        "display_name": "Family Medicine",
        "distinct_npis": 2,
        "total_services": 12.0,
        "total_beneficiaries": 10.0,
        "provider_types": ["Family Practice"],
    }
    session = FakeSession(
        [
            FakeResult(scalar=None),
            FakeResult(scalar=None),
            FakeResult(rows=[evidence_by_field]),
        ]
    )
    pricing_module._PROCEDURE_TAXONOMY_EVIDENCE_CACHE.clear()

    evidence = await pricing_module._load_procedure_taxonomy_evidence(
        session,
        year=2023,
        internal_codes=[1099885900],
        limit=10,
    )

    assert evidence == [pricing_module._taxonomy_evidence_item(evidence_by_field)]
    statements = [str(args[0]) for args, _kwargs in session.executions]
    assert any("pricing_provider_procedure" in statement for statement in statements)


@pytest.mark.asyncio
async def test_single_code_default_search_avoids_repeated_grouping(monkeypatch):
    """Use precomputed totals and one ordered provider-page query."""

    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", _resolve_single_internal_code)
    monkeypatch.setattr(pricing_module, "_enrich_provider_service_cost_indices", _skip_cost_enrichment)
    provider_by_field = {
        "npi": 1000000001,
        "provider_name": "Synthetic Provider",
        "provider_type": "Synthetic Specialty",
        "city": "Example City",
        "state": "EX",
        "zip5": "00000",
        "total_services": 12.0,
        "total_submitted_charges": 240.0,
        "total_allowed_amount": 120.0,
        "total_beneficiaries": 10.0,
        "matched_service_codes": 1,
    }
    request = make_request(
        [FakeResult(scalar=3), FakeResult(rows=[provider_by_field])],
        args={
            "code": "99213",
            "code_system": "CPT",
            "year": "2023",
            "limit": "1",
        },
    )

    response = await list_providers_by_procedure(request)

    response_by_field = json.loads(response.body)
    assert response_by_field["pagination"]["total"] == 3
    assert response_by_field["items"] == [
        pricing_module._normalize_provider_service_aggregate(
            provider_by_field,
            False,
        )
    ]
    statements = [str(execution_args[0]) for execution_args, _execution_kwargs in request.ctx.sa_session.executions]
    provider_statements = [statement for statement in statements if "pricing_provider_procedure" in statement]
    assert len(provider_statements) == 1
    assert "GROUP BY" not in provider_statements[0]
    assert any("pricing_procedure.provider_count" in statement for statement in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "thresholds",
    ({"min_claims": "12"}, {"min_total_cost": "120"}, {"min_claims": "12", "min_total_cost": "120"}),
)
async def test_native_claims_thresholds_filter_rows_before_count_and_page(monkeypatch, thresholds):
    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", _resolve_single_internal_code)
    request = make_request(
        [FakeResult(scalar=0), FakeResult(rows=[])],
        args={
            "code": "99213",
            "code_system": "CPT",
            "year": "2023",
            "limit": "2",
            "offset": "4",
            "order": "asc",
            **thresholds,
        },
    )

    response = await list_providers_by_procedure(request)

    body = json.loads(response.body)
    assert set(body) == {"items", "pagination", "query"}
    assert body["items"] == []
    assert body["pagination"] == {"total": 0, "limit": 2, "offset": 4, "page": 3}
    statements = [
        str(args[0].compile(compile_kwargs={"literal_binds": True}))
        for args, _kwargs in request.ctx.sa_session.executions
    ]
    assert len(statements) == 2
    for statement in statements:
        assert "GROUP BY" in statement
        for name, column in (("min_claims", "total_services"), ("min_total_cost", "total_allowed_amount")):
            if name in thresholds:
                predicate = f"mrf.pricing_provider_procedure.{column} >= {float(thresholds[name])}"
                assert statement.index(predicate) < statement.index("GROUP BY")
                assert body["query"][name] == float(thresholds[name])
        assert "custom_import" not in statement
    assert "LIMIT" not in statements[0] and "OFFSET" not in statements[0]
    page_query = request.ctx.sa_session.executions[-1][0][0]
    assert tuple(str(clause).rsplit(".", 1)[-1] for clause in page_query._order_by_clauses) == (
        "total_allowed_amount ASC",
        "npi ASC",
    )
    assert "LIMIT 2 OFFSET 4" in statements[1]


@pytest.mark.asyncio
async def test_coordinate_search_preserves_zip_distances(monkeypatch):
    monkeypatch.setattr(pricing_module, "_ZIP_RADIUS_ROWS_CACHE", OrderedDict())
    request = make_request(
        [
            FakeResult(rows=[{"zip5": "10001", "state": "NY", "latitude": 40.75, "longitude": -73.99}]),
            FakeResult(
                rows=[
                    {"zip5": "10002", "state": "NY", "distance_miles": 7.0},
                ]
            ),
            FakeResult(scalar=None),
            FakeResult(scalar=3),
            FakeResult(
                rows=[
                    {"npi": 1000000001, "zip5": "10001"},
                    {"npi": 1000000002, "zip5": "10002"},
                    {"npi": 1000000003, "zip5": "10003"},
                ]
            ),
        ],
        args={
            "q": "synthetic",
            "year": "2025",
            "zip5": "10001",
            "zip_radius_miles": "30",
            "lat": "40.75",
            "long": "-73.99",
            "radius": "50",
        },
    )

    reply = await list_providers_by_procedure(request)

    response_by_field = json.loads(reply.body)
    assert response_by_field["query"]["zip_candidate_count"] == 2
    assert response_by_field["pagination"]["total"] == 3
    provider_items = response_by_field["items"]
    assert [provider_item["npi"] for provider_item in provider_items] == [1000000001, 1000000002, 1000000003]
    assert provider_items[0]["distance_miles"] == 0.0
    assert provider_items[0]["anchor_zip5"] == "10001"
    assert provider_items[1]["distance_miles"] == 7.0
    assert provider_items[1]["distance_bucket"] == "within_10mi"
    assert "distance_miles" not in provider_items[2]
    assert "anchor_zip5" not in provider_items[2]
    for execution_args, _execution_kwargs in request.ctx.sa_session.executions[-2:]:
        statement = str(execution_args[0].compile(compile_kwargs={"literal_binds": True}))
        assert "mrf.pricing_provider.zip5 IN (SELECT mrf.geo_zip_lookup.zip_code" in statement
        assert "mrf.geo_zip_lookup.zip_code = '10001' OR" in statement
        assert "<= 50.0" in statement
