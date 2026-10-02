# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pricing resolution, schema cache and query-filter boundaries."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import NotFound
from sqlalchemy import true

from tests.test_pricing_api import FakeResult, FakeSession
from tests.test_pricing_api import pricing_module as pricing


@pytest.mark.asyncio
async def test_provider_terms_retain_matches_without_duplicate_types(monkeypatch):
    rows = [
        {"target_system": "OTHER", "target_code": "ignored"},
        {"target_system": "NUCC", "target_code": "1", "canonical_term": "Family"},
        {"target_system": "NUCC", "target_code": "2", "target_display": "Family"},
        {"target_system": "PROVIDER_TYPE", "target_code": " family "},
        {"target_system": "PROVIDER_TYPE", "target_code": ""},
    ]
    lookup = AsyncMock(return_value=rows)
    monkeypatch.setattr(pricing, "_query_terminology", lookup)
    result = await pricing._resolve_provider_type_terms(FakeSession(), ["first", "second"])
    assert result == {
        "input_terms": ["first", "second"],
        "provider_types": ["Family"],
        "matches": rows,
        "matched": True,
    }
    assert lookup.await_count == 2
    assert [call.kwargs["term"] for call in lookup.await_args_list] == ["first", "second"]


@pytest.mark.asyncio
async def test_procedure_terminology_normalizes_deduplicates_and_bounds_resolution(monkeypatch):
    rows = [
        {},
        {"target_system": "CPT"},
        {"target_system": pricing.INTERNAL_CODE_SYSTEM, "target_code": "002"},
        {"target_system": "CPT", "target_code": "001"},
        {"target_system": "cpt", "target_code": "001"},
    ]
    resolve = AsyncMock(return_value={"internal_codes": ["3", "invalid", 2]})
    monkeypatch.setattr(pricing, "_resolve_code_context", resolve)
    rows.extend([rows[2]] * 35)
    rows.append({"target_system": pricing.INTERNAL_CODE_SYSTEM, "target_code": "999"})
    session = FakeSession()
    assert await pricing._internal_procedure_codes_from_terminology(session, rows) == [2, 3]
    resolve.assert_awaited_once_with(session, "CPT", "001", expand_codes=False)


@pytest.mark.parametrize(
    "codes,system,resolved,expected",
    [
        ([], "CPT", [], set()),
        ([None, " "], "CPT", [], set()),
        (["001", "bad"], pricing.INTERNAL_CODE_SYSTEM, [], {"1"}),
        ([" lower "], "CPT", [2, 3], {"2", "3"}),
        ([" lower "], "CPT", [], {"LOWER"}),
    ],
)
@pytest.mark.asyncio
async def test_procedure_overrides_preserve_normalized_fallbacks(monkeypatch, codes, system, resolved, expected):
    resolve = AsyncMock(return_value={"internal_codes": resolved})
    monkeypatch.setattr(pricing, "_resolve_code_context", resolve)
    assert await pricing._resolve_procedure_override_codes(FakeSession(), codes, system) == expected
    assert resolve.await_count == (1 if system == "CPT" and codes == [" lower "] else 0)


@pytest.mark.asyncio
async def test_procedure_override_resolution_failure_keeps_requested_code(monkeypatch):
    monkeypatch.setattr(pricing, "_resolve_code_context", AsyncMock(side_effect=RuntimeError("unavailable")))
    assert await pricing._resolve_procedure_override_codes(FakeSession(), [" a "], "CPT") == {"A"}


@pytest.mark.parametrize(
    "pairs,results,expected,queries",
    [
        (set(), [], set(), 0),
        ({("CPT", "001")}, [FakeResult(scalar=None)], set(), 1),
        (
            {("CPT", "001")},
            [FakeResult(scalar=" Shared "), FakeResult(rows=[("hcpcs", "a"), ("CPT", "001")])],
            {("CPT", "001"), ("HCPCS", "A")},
            2,
        ),
    ],
)
@pytest.mark.asyncio
async def test_catalog_neighbors_require_a_name_and_normalize_codes(pairs, results, expected, queries):
    session = FakeSession(results)
    assert await pricing._query_catalog_neighbors(session, pairs) == expected
    assert len(session.executions) == queries


@pytest.mark.parametrize("name", ["example", "mrf.example"])
@pytest.mark.asyncio
async def test_schema_cache_retains_negative_results_until_expiry(monkeypatch, name):
    monkeypatch.setattr(pricing, "ENABLE_PRICING_SCHEMA_CACHE", True)
    monkeypatch.setattr(pricing, "_PRICING_TABLE_EXISTS_CACHE", {})
    monkeypatch.setattr(pricing, "_PRICING_SCHEMA_CACHE_TTL_SECONDS", 3)
    clock_values = [10]
    monkeypatch.setattr(pricing.time, "monotonic", lambda: clock_values[0])
    session = FakeSession([FakeResult(scalar=None), FakeResult(scalar="mrf.example")])
    assert await pricing._is_table_available(session, name) is False
    assert await pricing._is_table_available(session, name) is False
    assert len(session.executions) == 1
    clock_values[0] = 14
    assert await pricing._is_table_available(session, name) is True
    assert len(session.executions) == 2
    assert session.executions[0][0][1] == {"name": "mrf.example"}


@pytest.mark.asyncio
async def test_column_cache_returns_independent_sets_and_refreshes(monkeypatch):
    monkeypatch.setattr(pricing, "ENABLE_PRICING_SCHEMA_CACHE", True)
    monkeypatch.setattr(pricing, "_PRICING_TABLE_COLUMNS_CACHE", {})
    monkeypatch.setattr(pricing, "_PRICING_SCHEMA_CACHE_TTL_SECONDS", 3)
    clock_values = [10]
    monkeypatch.setattr(pricing.time, "monotonic", lambda: clock_values[0])
    session = FakeSession([FakeResult(rows=[(), (None,), (" old ",)]), FakeResult(rows=[("new",)])])
    first = await pricing._table_columns(session, "example")
    assert first == {"old"}
    first.add("local")
    assert await pricing._table_columns(session, "mrf.example") == {"old"}
    assert len(session.executions) == 1
    clock_values[0] = 14
    assert await pricing._table_columns(session, "mrf.example") == {"new"}
    assert len(session.executions) == 2


@pytest.mark.parametrize(
    "requested,configured,stored,expected",
    [
        (2023, 2024, None, (2023, "request")),
        (None, 2024, None, (2024, "env")),
        (None, None, 2022, (2022, "data_max")),
    ],
)
@pytest.mark.asyncio
async def test_quality_year_precedence_avoids_unnecessary_queries(monkeypatch, requested, configured, stored, expected):
    monkeypatch.setattr(pricing, "PRICING_DEFAULT_YEAR", configured)
    session = FakeSession([FakeResult(scalar=stored)])
    assert await pricing._resolve_quality_year(session, requested) == expected
    assert len(session.executions) == (1 if expected[1] == "data_max" else 0)


@pytest.mark.asyncio
async def test_quality_year_without_data_reports_unavailable(monkeypatch):
    monkeypatch.setattr(pricing, "PRICING_DEFAULT_YEAR", None)
    with pytest.raises(NotFound, match="No provider quality score data"):
        await pricing._resolve_quality_year(FakeSession([FakeResult(scalar=None)]), None)


@pytest.mark.parametrize(
    "value,expected", [(None, None), (True, True), (False, False), (" ON ", True), ("off", False), ("other", None)]
)
def test_optional_booleans_preserve_unknown_values(value, expected):
    assert pricing._as_bool(value) is expected


def _provider_filters(enabled):
    return {
        "npi": 123 if enabled else None,
        "state": "IL" if enabled else None,
        "city": "synthetic" if enabled else None,
        "specialty": None,
        "query_text": "needle" if enabled else None,
        "min_claims": 0 if enabled else None,
        "min_total_cost": 0 if enabled else None,
    }


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.asyncio
async def test_provider_query_filters_preserve_zero_thresholds(monkeypatch, enabled):
    resolution_by_field = {"matched": enabled}
    monkeypatch.setattr(
        pricing,
        "_provider_type_filter_clause",
        AsyncMock(return_value=(true() if enabled else None, resolution_by_field)),
    )
    filter_values_by_name = _provider_filters(enabled)
    first, context = await pricing._pricing_provider_list_where(FakeSession(), {}, 2024, filter_values_by_name)
    second, other = await pricing._procedure_provider_list_where(FakeSession(), {}, 2024, [1], filter_values_by_name)
    assert context == other == resolution_by_field
    for expression in (first, second):
        parameter_map = expression.compile().params
        assert 2024 in parameter_map.values()
        assert ("%needle%" in parameter_map.values()) is enabled
        assert ("%synthetic%" in parameter_map.values()) is enabled
        for column in ("total_services", "total_allowed_amount"):
            assert parameter_map.get(f"{column}_1", "absent") == (0 if enabled else "absent")


def _crosswalk(source, source_code, target, target_code):
    return {"from_system": source, "from_code": source_code, "to_system": target, "to_code": target_code}


@pytest.mark.asyncio
async def test_external_rx_resolution_keeps_direction_and_deduplicates():
    system = pricing.INTERNAL_RX_CODE_SYSTEM
    rows = [
        _crosswalk(system, "A", "NDC", "001"),
        _crosswalk(system, "A", "NDC", "001"),
        _crosswalk("RXNORM", "123", system, "A"),
        _crosswalk("OTHER", "x", "OTHER", "y"),
        _crosswalk(system, "", "NDC", "002"),
    ]
    session = FakeSession([FakeResult(rows=rows)])
    assert await pricing._resolve_external_rx_codes_for_internal(session, [" a ", "A", None]) == {
        "A": {"NDC": ["001"], "RXNORM": ["123"]},
    }
    empty_session = FakeSession()
    assert await pricing._resolve_external_rx_codes_for_internal(empty_session, [None, " "]) == {}
    assert empty_session.executions == []


@pytest.mark.parametrize(
    "ndc,rxnorm,fallback,expected",
    [
        ([None, " 001 "], ["123"], ("other", "a"), ("NDC", "001")),
        ([None, " "], [None, " 123 "], ("other", "a"), ("RXNORM", "123")),
        ([], [], (" other ", " a "), ("OTHER", "A")),
        ([], [], (None, None), (None, None)),
    ],
)
def test_external_rx_preference_ignores_empty_codes(ndc, rxnorm, fallback, expected):
    assert pricing._select_preferred_external_rx_code(*fallback, ndc_codes=ndc, rxnorm_codes=rxnorm) == expected


@pytest.mark.parametrize("expand,expected", [(False, ["A"]), (True, ["A", "B"])])
def test_rx_crosswalk_reverse_matches_require_explicit_expansion(expand, expected):
    system = pricing.INTERNAL_RX_CODE_SYSTEM
    rows = [
        _crosswalk("NDC", "001", system, "A"),
        _crosswalk(system, "B", "NDC", "002"),
        _crosswalk("X", "1", "Y", "2"),
    ]
    codes, matches = pricing._resolved_rx_crosswalk_matches(rows, expand_codes=expand)
    assert codes == expected
    assert matches == [{**row, "match_type": None, "confidence": None, "source": None} for row in rows[: len(expected)]]


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.asyncio
async def test_prescription_filters_keep_search_and_zero_thresholds(monkeypatch, enabled):
    filter_values_by_name = {
        key: f"example_{key}" if enabled else None
        for key in ("generic_name", "brand_name", "rx_name", "query_text", "code")
    }
    filter_values_by_name.update(
        rx_code_system="NDC", min_claims=0 if enabled else None, min_total_cost=0 if enabled else None
    )
    code_context_by_field = {"matched_via": "crosswalk"}
    resolver = AsyncMock(return_value=(["A"], code_context_by_field))
    monkeypatch.setattr(pricing, "_resolve_internal_rx_codes_for_request", resolver)
    expression, resolved_context_by_field = await pricing._provider_prescription_list_where(
        FakeSession(), {}, 123, 2024, filter_values_by_name
    )
    parameter_map = expression.compile().params
    assert resolved_context_by_field == (code_context_by_field if enabled else None)
    assert 123 in parameter_map.values() and 2024 in parameter_map.values()
    for field in ("generic_name", "brand_name", "rx_name", "query_text"):
        assert (f"%example_{field}%" in parameter_map.values()) is enabled
    assert ("%EXAMPLE_QUERY_TEXT%" in parameter_map.values()) is enabled
    for column in ("total_claims", "total_drug_cost"):
        assert parameter_map.get(f"{column}_1", "absent") == (0 if enabled else "absent")
    assert resolver.await_count == int(enabled)
    filters = SimpleNamespace(
        state="IL" if enabled else None,
        city="city" if enabled else None,
        specialty="family" if enabled else None,
        search_query="needle" if enabled else None,
        min_claims=0 if enabled else None,
        min_total_cost=0 if enabled else None,
    )
    other = pricing._prescription_provider_where_clause(["A"], 2024, filters).compile().params
    assert ("%family%" in other.values()) is enabled
    assert ("%city%" in other.values()) is enabled
    assert ("%needle%" in other.values()) is enabled
    for column in ("total_claims", "total_drug_cost"):
        assert other.get(f"{column}_1", "absent") == (0 if enabled else "absent")


@pytest.mark.parametrize("primary_only", ["true", "false"])
def test_allowed_amount_taxonomy_filters_bind_values_and_primary_policy(primary_only):
    parameter_map = {}
    sql = pricing._allowed_amount_provider_filter_sql(
        {
            "taxonomy_classification": "Class",
            "taxonomy_specialization": "Specialty",
            "taxonomy_section": "Section",
            "primary_only": primary_only,
        },
        parameter_map,
    )
    assert parameter_map == {
        "allowed_taxonomy_classification": "Class",
        "allowed_taxonomy_specialization": "Specialty",
        "allowed_taxonomy_section": "Section",
    }
    assert "allowed_exact_taxonomy.npi = provider_rollup.npi" in sql
    assert ("healthcare_provider_primary_taxonomy_switch" in sql) is (primary_only == "true")
    assert all(":" + name in sql for name in parameter_map)


@pytest.mark.parametrize(
    "payload,expected",
    [
        (None, ("family", "ABC", {"1"})),
        ({"cohort_context": []}, ("family", "ABC", {"1"})),
        ({"cohort_context": {}}, ("family", "ABC", {"1"})),
        (
            {"cohort_context": {"specialty_key": "other", "taxonomy_code": "xyz", "procedure_bucket": "2"}},
            ("other", "XYZ", {"2"}),
        ),
    ],
)
def test_variant_scope_uses_complete_context_or_independent_fallback(payload, expected):
    codes = {"1"}
    result = pricing._variant_scope_inputs_from_mode_payload(
        payload, fallback_specialty_key="family", fallback_taxonomy_code="ABC", fallback_procedure_codes=codes
    )
    assert result == expected
    result[2].add("local")
    assert codes == {"1"}


def test_provider_variant_candidates_filter_mismatch_and_keep_fallback():
    def candidate(specialty="family", taxonomy="ABC", ratio=1):
        return {"payload_lower": {"specialty": specialty, "taxonomy": taxonomy}, "procedure_match_ratio": ratio}

    accepted = candidate()
    candidates = [
        {"payload_lower": None},
        candidate("other"),
        candidate(taxonomy="OTHER"),
        candidate(ratio=0),
        accepted,
    ]
    filter_by_name = {
        "provider_specialty_key": "family",
        "provider_taxonomy_code": "ABC",
        "provider_procedure_codes": {"1"},
    }
    assert pricing._collect_provider_scope_variant_candidates(candidates, **filter_by_name) == [accepted]
    assert pricing._collect_provider_scope_variant_candidates(candidates[:-1], **filter_by_name) == [candidates[0]]
    assert pricing._collect_provider_scope_variant_candidates([], **filter_by_name) == []


@pytest.mark.parametrize("confidence,expected", [(80, "high"), (55, "medium"), (1, "low"), (0, "none")])
def test_confidence_bands_preserve_boundary_values(confidence, expected):
    assert pricing._live_confidence_band(confidence) == expected


@pytest.mark.parametrize(
    "claims,qpp,rx,expected",
    [
        (True, True, False, "direct"),
        (True, False, False, "mixed"),
        (False, True, False, "mixed"),
        (False, False, True, "mixed"),
        (False, False, False, "unavailable"),
    ],
)
def test_score_method_requires_both_claims_and_quality_for_direct(claims, qpp, rx, expected):
    assert pricing._live_score_method(claims, qpp, rx) == expected
