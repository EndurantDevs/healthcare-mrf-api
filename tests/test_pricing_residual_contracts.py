# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from collections import OrderedDict

import pytest
from sanic.exceptions import InvalidUsage, NotFound

from tests.test_pricing_api import FakeResult, FakeSession, make_request, pricing_module


@pytest.mark.parametrize(
    ("risk_ratio", "expected_level"),
    [
        (None, "$$$"),
        (0.80, "$"),
        (0.81, "$$"),
        (0.90, "$$"),
        (0.91, "$$$"),
        (1.10, "$$$"),
        (1.11, "$$$$"),
        (1.25, "$$$$"),
        (1.26, "$$$$$"),
    ],
)
def test_cost_levels_preserve_inclusive_boundaries(risk_ratio, expected_level):
    assert pricing_module._estimated_cost_level_from_risk_ratio(risk_ratio) == expected_level


@pytest.mark.parametrize(
    ("value", "expected_level"),
    [(None, None), ("", None), (" l1 ", "L1"), ("0", "L0"), ("3", "L3"), ("unknown", None)],
)
def test_cohort_levels_normalize_supported_aliases(value, expected_level):
    assert pricing_module._normalize_cohort_level(value) == expected_level


@pytest.mark.parametrize(
    ("row_scope", "row_value", "target_scope", "target_value", "expected_match"),
    [
        ("state", "USA", "national", "US", True),
        ("state", "IL", "national", "US", False),
        ("county", "Cook", "county", "", True),
        ("county", "Cook", "county", "COOK", True),
        ("county", "Cook", "county", "OTHER", False),
        ("zip5", "60601-1234", "zip", "60601", True),
        ("state", "IL|COOK", "state", "IL", True),
        ("state", "ILLINOIS", "state", "IL", False),
    ],
)
def test_peer_geographies_match_exact_scopes(row_scope, row_value, target_scope, target_value, expected_match):
    assert pricing_module._is_row_geography_match(row_scope, row_value, target_scope, target_value) is expected_match


@pytest.mark.parametrize(
    ("values", "percentile", "expected_value"),
    [
        ([], 0.5, None),
        ([10.0], 0.5, 10.0),
        ([10.0, 20.0, 30.0], 0.5, 20.0),
        ([10.0, 20.0, 30.0], -1, 10.0),
        ([10.0, 20.0, 30.0], 2, 30.0),
        ([10.0, 20.0], 0.5, 15.0),
    ],
)
def test_percentiles_handle_small_clamped_samples(values, percentile, expected_value):
    assert pricing_module._percentile_cont(values, percentile) == expected_value


@pytest.mark.parametrize("charges", [[100, 100, 100, 100], [100, 200, 300]])
def test_charge_trimming_preserves_sparse_constant_cohorts(charges):
    charge_rows = [{"avg_submitted_charge": charge} for charge in charges]

    assert pricing_module._trim_charge_rows_log_iqr(charge_rows, iqr_factor=1.5) == charge_rows


@pytest.mark.parametrize(
    ("charges", "retained_charges"),
    [([None, 0, 100, 110, 120], [None, 0, 100, 110, 120]), ([None, 0, 100, 110, 120, 130], [100, 110, 120, 130])],
)
def test_charge_trimming_requires_positive_sample(charges, retained_charges):
    charge_rows = [{"avg_submitted_charge": charge} for charge in charges]

    retained_rows = pricing_module._trim_charge_rows_log_iqr(charge_rows, iqr_factor=1.5)

    assert [row["avg_submitted_charge"] for row in retained_rows] == retained_charges


def _taxonomy_rows(provider_counts):
    taxonomy_codes = ["2085R0202X", "207Q00000X", "207R00000X", "207P00000X", "207V00000X"]
    return [
        {
            "taxonomy_code": taxonomy_code,
            "classification": "Example specialty",
            "display_name": "Example specialty",
            "distinct_npis": count,
            "total_services": count * 10,
            "total_beneficiaries": count * 5,
            "provider_types": ["Example specialist"],
        }
        for taxonomy_code, count in zip(taxonomy_codes, provider_counts, strict=False)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "intent", "provider_counts", "confidence", "conflict_reasons"),
    [
        (
            "29888",
            "primary care",
            [100],
            "low",
            [
                "intent_conflicts_with_curated_code_range",
                "utilization_top_taxonomy_conflicts_with_selected_taxonomy",
            ],
        ),
        ("70551", "orthopedic", [100], "high", ["intent_conflicts_with_curated_code_range"]),
        ("29888", "orthopedic", [25, 22, 20, 18, 15], "low", []),
        ("99999", "unspecified", [100, 80, 70], "medium", []),
        ("99999", "unspecified", [25, 22, 20, 18, 15], "low", []),
        ("99999", "unspecified", [], "none", []),
    ],
)
async def test_taxonomy_resolution_explains_confidence_conflicts(
    monkeypatch, code, intent, provider_counts, confidence, conflict_reasons
):
    monkeypatch.setattr(pricing_module, "_PROCEDURE_TAXONOMY_EVIDENCE_CACHE", OrderedDict())
    request = make_request(
        [FakeResult(scalar=None), FakeResult(scalar=None), FakeResult(rows=_taxonomy_rows(provider_counts))],
        args={"year": "2024", "code_system": "HP_PROCEDURE_CODE", "code": code, "clinical_intent": intent},
    )

    result = json.loads((await pricing_module.resolve_procedure_taxonomy(request)).body)

    resolution_by_field = result["resolution"]
    assert resolution_by_field["confidence"] == confidence
    assert resolution_by_field["conflict_reasons"] == conflict_reasons
    assert resolution_by_field["needs_review"] is bool(conflict_reasons)
    assert resolution_by_field["safe_for_hard_filter"] is False
    assert resolution_by_field["provider_filter"] is None
    if provider_counts:
        assert resolution_by_field["recommended_mode"] == "soft_boost"
        assert resolution_by_field["provider_boost"]["taxonomy_codes"] == resolution_by_field["taxonomy_codes"]
        assert result["evidence"]["top_taxonomies"][0]["distinct_npi_share"] == round(
            provider_counts[0] / sum(provider_counts), 6
        )
    else:
        assert resolution_by_field["status"] == "insufficient_evidence"
        assert resolution_by_field["recommended_mode"] == "validate_only"
        assert resolution_by_field["provider_boost"] is None
        assert result["evidence"]["top_taxonomies"] == []
    assert request.ctx.sa_session.executions[-1][0][1] == {"year": 2024, "internal_codes": [int(code)], "limit": 10}


@pytest.mark.asyncio
async def test_unmapped_taxonomy_returns_validation_context():
    request = make_request([FakeResult(rows=[])], args={"year": "2024", "code_system": "HCPCS", "code": "A1234"})

    result = json.loads((await pricing_module.resolve_procedure_taxonomy(request)).body)

    assert result["code_context"]["resolution_status"] == "unmapped"
    assert result["code_context"]["resolved_codes"] == []
    assert result["resolution"]["recommended_mode"] == "validate_only"
    assert result["resolution"]["provider_filter"] is None
    assert result["evidence"]["top_taxonomies"] == []
    assert len(request.ctx.sa_session.executions) == 1
    bound_values = request.ctx.sa_session.executions[0][0][0].compile().params.values()
    assert "HCPCS" in bound_values
    assert "A1234" in bound_values


def _taxonomy_fallback_results(fallback_source):
    sparse_rows = _taxonomy_rows([1])
    representative_rows = _taxonomy_rows([100])
    results = [FakeResult(scalar="procedure_taxonomy_signal"), FakeResult(rows=sparse_rows)]
    if fallback_source == "precomputed":
        return [*results, FakeResult(rows=representative_rows)]
    results.extend([FakeResult(rows=[]), FakeResult(scalar="provider_quality_feature")])
    if fallback_source == "quality":
        return [*results, FakeResult(rows=representative_rows)]
    return [*results, FakeResult(rows=sparse_rows), FakeResult(rows=representative_rows)]


@pytest.mark.asyncio
@pytest.mark.parametrize("fallback_source", ["precomputed", "quality", "nppes"])
async def test_taxonomy_evidence_uses_representative_fallback(monkeypatch, fallback_source):
    monkeypatch.setattr(pricing_module, "_PROCEDURE_TAXONOMY_EVIDENCE_CACHE", OrderedDict())
    session = FakeSession(_taxonomy_fallback_results(fallback_source))

    evidence_items = await pricing_module._load_procedure_taxonomy_evidence(
        session, year=2024, internal_codes=[123], limit=10
    )

    assert evidence_items[0]["distinct_npis"] == 100
    assert evidence_items[0]["total_beneficiaries"] == 500.0
    assert evidence_items[0]["taxonomy_code"] == "2085R0202X"
    quality_params_by_name = session.executions[1][0][0].compile().params
    fallback_params_by_name = session.executions[2][0][0].compile().params
    assert quality_params_by_name["evidence_source_1"] == "quality_feature"
    assert fallback_params_by_name["evidence_source_1"] == "quality_or_nppes"
    assert fallback_params_by_name["year_1"] == 2024
    assert fallback_params_by_name["procedure_code_1"] == 123
    assert "source_relation_fingerprint" in str(session.executions[2][0][0])
    if fallback_source != "precomputed":
        assert session.executions[-1][0][1] == {"year": 2024, "internal_codes": [123], "limit": 10}
    if fallback_source == "nppes":
        assert "LEFT JOIN LATERAL" in str(session.executions[-1][0][0])
        assert "quality_taxonomy_code" in str(session.executions[-1][0][0])
    execution_count = len(session.executions)
    evidence_items[0]["distinct_npis"] = 0
    cached_items = await pricing_module._load_procedure_taxonomy_evidence(
        session, year=2024, internal_codes=[123], limit=10
    )
    assert cached_items[0]["distinct_npis"] == 100
    assert len(session.executions) == execution_count


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_name", ["list_provider_prescriptions", "get_provider_prescription"])
async def test_prescriptions_reject_missing_provider(handler_name):
    request = make_request([], args={"year": "2024"})
    handler = getattr(pricing_module, handler_name)
    path_values = ("null",) if handler_name == "list_provider_prescriptions" else ("null", "HP_RX_CODE", "RX1")

    with pytest.raises(InvalidUsage, match="npi.*must be provided"):
        await handler(request, *path_values)

    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_prescription_list_reports_unavailable_data():
    request = make_request([FakeResult(scalar=None)], args={"year": "2024", "limit": "2"})

    result = json.loads((await pricing_module.list_provider_prescriptions(request, "1000000001")).body)

    assert result["items"] == []
    assert result["pagination"] == {"total": 0, "limit": 2, "offset": 0, "page": 1}
    assert result["query"]["data_status"] == "unavailable"
    assert result["query"]["year_used"] == 2024
    assert len(request.ctx.sa_session.executions) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("results", "error_message", "expected_executions"),
    [
        ([FakeResult(scalar=None)], "Prescription data is not available", 1),
        ([FakeResult(scalar="provider_prescription"), FakeResult(rows=[])], "Provider prescription not found", 2),
    ],
)
async def test_prescription_detail_reports_absent_records(results, error_message, expected_executions):
    request = make_request(results, args={"year": "2024"})

    with pytest.raises(NotFound, match=error_message):
        await pricing_module.get_provider_prescription(request, "1000000001", "HP_RX_CODE", "RX1")

    assert len(request.ctx.sa_session.executions) == expected_executions
    if expected_executions == 2:
        bound_values = request.ctx.sa_session.executions[-1][0][0].compile().params.values()
        assert 1000000001 in bound_values
        assert ["RX1"] in bound_values
        assert 2024 in bound_values


@pytest.mark.asyncio
async def test_prescription_list_preserves_past_page_context():
    request = make_request(
        [FakeResult(scalar="provider_prescription"), FakeResult(scalar=3), FakeResult(rows=[])],
        args={"year": "2024", "code": "RX1", "limit": "2", "offset": "6"},
    )

    result = json.loads((await pricing_module.list_provider_prescriptions(request, "1000000001")).body)

    assert result["items"] == []
    assert result["pagination"] == {"total": 3, "limit": 2, "offset": 6, "page": 4}
    assert result["query"]["input_code"] == {"code_system": "HP_RX_CODE", "code": "RX1"}
    assert result["query"]["resolved_codes"] == [{"code_system": "HP_RX_CODE", "code": "RX1"}]
    assert len(request.ctx.sa_session.executions) == 3
    page_params_by_name = request.ctx.sa_session.executions[-1][0][0].compile().params
    assert page_params_by_name["param_1"] == 2
    assert page_params_by_name["param_2"] == 6
    assert page_params_by_name["rx_code_1"] == ["RX1"]


@pytest.mark.asyncio
async def test_prescription_expansion_rejects_unmapped_codes():
    request = make_request(
        [FakeResult(scalar="provider_prescription"), FakeResult(rows=[])],
        args={"year": "2024", "expand_codes": "true"},
    )

    with pytest.raises(NotFound, match="No mapped internal prescription code found"):
        await pricing_module.get_provider_prescription(request, "1000000001", "NDC", "0001")

    crosswalk_query = request.ctx.sa_session.executions[-1][0][0]
    assert " OR " in str(crosswalk_query)
    assert set(crosswalk_query.compile().params.values()) == {"NDC", "0001", "HP_RX_CODE"}
    assert len(request.ctx.sa_session.executions) == 2


@pytest.mark.asyncio
async def test_prescription_benchmarks_report_unavailable_data():
    request = make_request([FakeResult(scalar=None)], args={"year": "2024", "state": " il "})

    result = json.loads((await pricing_module.get_prescription_benchmarks(request, "HP_RX_CODE", "RX1")).body)

    assert result["query"]["data_status"] == "unavailable"
    assert result["query"]["state"] == "IL"
    assert result["benchmark"]["matched_rows"] == 0
    assert result["benchmark"]["total_allowed_amount"] == 0.0
    assert result["benchmark"]["estimated_cost_level_thresholds"] == {}
    assert len(request.ctx.sa_session.executions) == 1


@pytest.mark.asyncio
async def test_prescription_benchmarks_bind_geography_filters():
    aggregate_by_field = {"matched_rows": 0, "provider_count": 0, "total_claims": None, "total_drug_cost": None}
    thresholds_by_percentile = {"p20": None, "p40": None, "p60": None, "p80": None}
    request = make_request(
        [
            FakeResult(scalar="provider_prescription"),
            FakeResult(rows=[aggregate_by_field]),
            FakeResult(rows=[thresholds_by_percentile]),
        ],
        args={"year": "2024", "state": " il ", "city": " Example City "},
    )

    result = json.loads((await pricing_module.get_prescription_benchmarks(request, "HP_RX_CODE", "RX1")).body)

    assert result["query"]["state"] == "IL"
    assert result["query"]["city"] == "example city"
    assert result["benchmark"]["provider_count"] == 0
    assert result["benchmark"]["total_prescriptions"] == 0.0
    assert result["benchmark"]["estimated_cost_level_thresholds"]["$"]["max_total_allowed_amount"] is None
    for execution in request.ctx.sa_session.executions[1:]:
        bound_values = execution[0][0].compile().params.values()
        assert "IL" in bound_values
        assert "%example city%" in bound_values
        assert ["RX1"] in bound_values
        assert 2024 in bound_values


@pytest.mark.asyncio
async def test_procedure_locations_bind_all_geography_filters():
    request = make_request(
        [FakeResult(scalar=3), FakeResult(rows=[])],
        args={"year": "2024", "state": " il ", "city": " Example City ", "zip5": "60601", "limit": "2", "offset": "4"},
    )

    result = json.loads((await pricing_module.list_provider_procedure_locations(request, "1000000001", "123")).body)

    assert result["items"] == []
    assert result["pagination"] == {"total": 3, "limit": 2, "offset": 4, "page": 3}
    assert result["query"]["state"] == "IL"
    assert result["query"]["city"] == "example city"
    assert result["query"]["zip5"] == "60601"
    for execution in request.ctx.sa_session.executions:
        bound_values = execution[0][0].compile().params.values()
        assert "IL" in bound_values
        assert "%example city%" in bound_values
        assert "60601" in bound_values
        assert [123] in bound_values
    page_params_by_name = request.ctx.sa_session.executions[-1][0][0].compile().params
    assert page_params_by_name["param_1"] == 2
    assert page_params_by_name["param_2"] == 4


@pytest.mark.asyncio
async def test_procedure_benchmarks_support_city_only_filters():
    aggregate_by_field = {"matched_rows": 0, "provider_count": 0, "total_services": None, "total_allowed_amount": None}
    thresholds_by_percentile = {"p20": None, "p40": None, "p60": None, "p80": None}
    request = make_request(
        [FakeResult(rows=[aggregate_by_field]), FakeResult(rows=[thresholds_by_percentile])],
        args={"year": "2024", "city": " Example City "},
    )

    result = json.loads((await pricing_module.get_procedure_benchmarks(request, "HP_PROCEDURE_CODE", "123")).body)

    assert result["query"]["state"] is None
    assert result["query"]["city"] == "example city"
    assert result["query"]["include_legacy_fields"] is False
    assert result["benchmark"]["total_services"] == 0.0
    assert result["benchmark"]["total_allowed_amount"] == 0.0
    assert "total_drug_cost" not in result["benchmark"]
    for execution in request.ctx.sa_session.executions:
        query = execution[0][0]
        assert "state_1" not in query.compile().params
        assert "%example city%" in query.compile().params.values()
        assert [123] in query.compile().params.values()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_edge", [False, True])
async def test_crosswalk_expansion_stops_empty_cycles(has_edge):
    edge_by_field = {"from_system": "CPT", "from_code": "12345", "to_system": "HP_PROCEDURE_CODE", "to_code": "123"}
    session = FakeSession(
        [FakeResult(rows=[edge_by_field]), FakeResult(rows=[edge_by_field])] if has_edge else [FakeResult(rows=[])]
    )

    visited_pairs, matched_edges = await pricing_module._expand_code_crosswalk(session, ("CPT", "12345"), hops=4)

    assert ("CPT", "12345") in visited_pairs
    assert len(session.executions) == (2 if has_edge else 1)
    if has_edge:
        assert visited_pairs == {("CPT", "12345"), ("HP_PROCEDURE_CODE", "123")}
        assert len(matched_edges) == 1
        assert matched_edges[0]["to_code"] == "123"
        assert "HP_PROCEDURE_CODE" in session.executions[1][0][0].compile().params.values()
    else:
        assert visited_pairs == {("CPT", "12345")}
        assert matched_edges == []
    assert "12345" in session.executions[0][0][0].compile().params.values()
