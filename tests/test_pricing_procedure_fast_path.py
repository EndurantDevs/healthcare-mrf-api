# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

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
        assert tuple(
            str(clause).rsplit(".", 1)[-1]
            for clause in query._order_by_clauses
        ) == expected_order


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
    statements = [
        str(execution_args[0])
        for execution_args, _execution_kwargs in session.executions
    ]
    assert any("procedure_taxonomy_signal" in statement for statement in statements)
    assert any(
        "source_relation_fingerprint" in statement
        and "to_regclass" in statement
        for statement in statements
    )
    assert not any(
        "FROM mrf.pricing_provider_procedure" in statement
        for statement in statements
    )


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

    monkeypatch.setattr(
        pricing_module, "_resolve_internal_codes_for_request", _resolve_single_internal_code
    )
    monkeypatch.setattr(
        pricing_module, "_enrich_provider_service_cost_indices", _skip_cost_enrichment
    )
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
    statements = [
        str(execution_args[0])
        for execution_args, _execution_kwargs in request.ctx.sa_session.executions
    ]
    provider_statements = [
        statement
        for statement in statements
        if "pricing_provider_procedure" in statement
    ]
    assert len(provider_statements) == 1
    assert "GROUP BY" not in provider_statements[0]
    assert any(
        "pricing_procedure.provider_count" in statement
        for statement in statements
    )
