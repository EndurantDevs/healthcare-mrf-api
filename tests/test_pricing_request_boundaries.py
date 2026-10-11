# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from collections import OrderedDict
from types import SimpleNamespace

import pytest
from sanic.exceptions import InvalidUsage, NotFound
from sanic.request import RequestParameters

from tests.test_pricing_api import FakeResult, FakeSession, make_request, pricing_module


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_name", "path_values", "query_by_name", "message"),
    [
        ("get_pricing_provider", ("null",), {}, "npi.*must be provided"),
        ("list_provider_procedures", ("null",), {}, "npi.*must be provided"),
        ("get_provider_procedure_cost_level", ("null", "123"), {}, "npi.*must be provided"),
        ("list_provider_procedure_locations", ("null", "123"), {}, "npi.*must be provided"),
        ("resolve_procedure_taxonomy", (), {"code": " "}, "code.*required"),
        ("autocomplete_provider_types", (), {"q": "a"}, "q.*at least 2"),
    ],
)
async def test_request_validation_precedes_database(handler_name, path_values, query_by_name, message):
    request = make_request([], args={"year": "2024", **query_by_name})

    with pytest.raises(InvalidUsage, match=message):
        await getattr(pricing_module, handler_name)(request, *path_values)

    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_requests_require_database_session():
    request = SimpleNamespace(args={"year": "2024"}, ctx=SimpleNamespace())

    with pytest.raises(RuntimeError, match="session not available"):
        await pricing_module.get_pricing_provider(request, "1000000001")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_requested_year", [False, True])
async def test_missing_provider_reports_not_found(monkeypatch, use_requested_year):
    monkeypatch.setattr(pricing_module, "PRICING_DEFAULT_YEAR", None)
    request = make_request([FakeResult(rows=[], scalar=None)], args={"year": "2024"} if use_requested_year else {})

    with pytest.raises(NotFound, match="Provider not found" if use_requested_year else "No pricing data available"):
        await pricing_module.get_pricing_provider(request, "1000000001")

    assert len(request.ctx.sa_session.executions) == 1
    query = request.ctx.sa_session.executions[0][0][0]
    if use_requested_year:
        assert query.compile().params == {"npi_1": 1000000001, "year_1": 2024}
    else:
        assert "max(" in str(query)


@pytest.mark.asyncio
async def test_provider_details_keep_requested_legacy_aliases():
    provider_by_field = {"npi": 1000000001, "year": 2024, "total_services": 0, "total_allowed_amount": 0}
    request = make_request(
        [FakeResult(rows=[provider_by_field]), FakeResult(scalar=0), FakeResult(scalar=0)],
        args={"year": "2024", "include_legacy_fields": "true"},
    )

    result = json.loads((await pricing_module.get_pricing_provider(request, "1000000001")).body)

    assert result["total_services"] == result["total_claims"] == 0
    assert result["total_allowed_amount"] == result["total_drug_cost"] == 0
    assert result["legacy_field_aliases"]["total_claims"] == "total_services"
    assert result["year_used"] == 2024
    assert len(request.ctx.sa_session.executions) == 3


@pytest.mark.asyncio
async def test_location_ordering_rejects_unknown_fields():
    request = make_request([FakeResult(scalar=0)], args={"year": "2024", "order_by": "unsupported"})

    with pytest.raises(InvalidUsage, match="Unsupported order_by.*Allowed: city, npi, state, zip5"):
        await pricing_module.list_provider_procedure_locations(request, "1000000001", "123")

    assert len(request.ctx.sa_session.executions) == 1
    assert "count(" in str(request.ctx.sa_session.executions[0][0][0])


@pytest.mark.parametrize("value", ["2012", "invalid"])
def test_default_year_rejects_invalid_configuration(monkeypatch, value):
    monkeypatch.setenv("HLTHPRT_PRICING_DEFAULT_YEAR", value)

    with pytest.raises(RuntimeError, match="HLTHPRT_PRICING_DEFAULT_YEAR"):
        pricing_module._parse_pricing_default_year()


@pytest.mark.parametrize(
    ("first_value", "second_value", "default", "expected_enabled"),
    [(None, None, True, True), (" ", "yes", False, True), ("off", "yes", True, False)],
)
def test_environment_flags_select_first_value(monkeypatch, first_value, second_value, default, expected_enabled):
    for name, value in (("EXAMPLE_FIRST_FLAG", first_value), ("EXAMPLE_SECOND_FLAG", second_value)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    assert (
        pricing_module._is_env_flag_enabled("EXAMPLE_FIRST_FLAG", "EXAMPLE_SECOND_FLAG", default=default)
        is expected_enabled
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("search_query", ["", "unlisted specialty"])
async def test_specialty_search_binds_exact_geography(search_query):
    query_by_name = {
        "year": "2024",
        "state": " il ",
        "city": " Example City ",
        "zip5": "60601",
        "zip_radius_miles": "0",
        "q": search_query,
    }
    results = [FakeResult(rows=[]), FakeResult(scalar=0)]
    if search_query:
        results.insert(0, FakeResult(scalar=None))
    request = make_request(results, args=query_by_name)

    result = json.loads((await pricing_module.list_provider_specialties(request)).body)

    assert result["items"] == []
    assert result["pagination"]["total"] == 0
    assert result["query"]["state"] == "IL"
    assert result["query"]["city"] == "example city"
    assert result["query"]["zip_candidate_count"] == 1
    for execution in request.ctx.sa_session.executions[-2:]:
        bound_values = execution[0][0].compile().params.values()
        assert "IL" in bound_values
        assert "%example city%" in bound_values
        assert "60601" in bound_values
        if search_query:
            assert "%unlisted specialty%" in bound_values


@pytest.mark.asyncio
async def test_unresolved_taxonomy_preserves_resolution_evidence():
    request = make_request(
        [FakeResult(scalar=None), FakeResult(rows=[]), FakeResult(scalar=0)],
        args={"year": "2024", "taxonomy_classification": "Example classification"},
    )

    result = json.loads((await pricing_module.list_provider_specialties(request)).body)

    assert result["items"] == []
    resolution_by_field = result["query"]["provider_type_resolution"]
    assert resolution_by_field["input_terms"] == ["Example classification"]
    assert resolution_by_field["matched"] is False
    assert resolution_by_field["provider_types"] == []
    assert len(request.ctx.sa_session.executions) == 3


@pytest.mark.asyncio
async def test_geographic_benchmarks_report_missing_table():
    request = make_request([FakeResult(scalar=None)], args={"year": "2024", "state": " il "})

    result = json.loads((await pricing_module.get_procedure_geo_benchmarks(request, "HP_PROCEDURE_CODE", "123")).body)

    assert result["benchmarks"] == {"national": None, "state": None}
    assert result["query"]["data_status"] == "unavailable"
    assert result["query"]["state"] == "IL"
    assert result["query"]["resolved_codes"] == []
    assert len(request.ctx.sa_session.executions) == 1


@pytest.mark.asyncio
async def test_geographic_benchmarks_preserve_empty_aggregates():
    aggregate_by_field = {
        "rows": 0,
        "total_services": None,
        "avg_submitted_charge": None,
        "avg_payment_amount": None,
        "avg_standardized_amount": None,
    }
    session = FakeSession([FakeResult(rows=[aggregate_by_field])])

    result = await pricing_module._procedure_geo_scope_benchmark(
        session, year=2024, internal_codes=[123], scope="national", geography_value=None
    )

    assert result is None
    params_by_name = session.executions[0][0][0].compile().params
    assert params_by_name["year_1"] == 2024
    assert params_by_name["procedure_code_1"] == [123]
    assert "geography_value_1" not in params_by_name


@pytest.mark.parametrize(
    ("context_by_field", "threshold", "expected_threshold"),
    [
        ({}, None, None),
        ({"peer_count": "25", "selected_cohort_level": "1"}, None, None),
        ({"peer_count": "25"}, 1.5, 1.0),
    ],
)
def test_score_cohorts_normalize_explicit_context(context_by_field, threshold, expected_threshold):
    result = pricing_module._cohort_context_from_score_row(
        {"cohort_context": context_by_field}, computed_live=False, procedure_match_threshold=threshold
    )

    assert result is not context_by_field
    assert result["computed_live"] is False
    assert result.get("procedure_match_threshold") == expected_threshold
    assert result["peer_count"] == (25 if context_by_field else None)
    assert result["selected_cohort_level"] == ("L1" if "selected_cohort_level" in context_by_field else None)
    assert "computed_live" not in context_by_field


@pytest.mark.parametrize("benchmark_mode", ["state", "national"])
def test_peer_selection_excludes_other_geographies(benchmark_mode):
    peer_rows = [
        {
            "year": 2024,
            "benchmark_mode": benchmark_mode,
            "geography_scope": "state",
            "geography_value": "CA",
            "cohort_level": "L1",
            "peer_n": 30,
            "specialty_key": "__all__",
            "taxonomy_code": "*",
            "procedure_bucket": "",
        },
        {
            "year": 2024,
            "benchmark_mode": benchmark_mode,
            "geography_scope": "national",
            "geography_value": "US",
            "cohort_level": "L2",
            "peer_n": 50,
            "specialty_key": "*",
            "taxonomy_code": "ANY",
            "procedure_bucket": "",
        },
    ]

    candidate = pricing_module._select_peer_target_candidate(
        peer_rows,
        benchmark_mode=benchmark_mode,
        state_key="IL",
        zip5=None,
        specialty_key=None,
        taxonomy_code="207Q00000X",
        requested_procedure_codes=set(),
        procedure_match_threshold=0.8,
    )

    assert candidate["selected_geography"] == "national"
    assert candidate["peer_count"] == 50
    assert candidate["procedure_match_ratio"] == 1.0
    assert candidate["taxonomy_match"] is True


def test_peer_selection_returns_best_available_match():
    peer_rows = [
        {
            "geography_scope": "county",
            "geography_value": "COOK",
            "cohort_level": "L1",
            "peer_n": 30,
            "specialty_key": "other",
            "year": 2024,
            "benchmark_mode": "national",
            "taxonomy_code": "*",
            "procedure_bucket": "",
        }
    ]
    candidate = pricing_module._select_peer_target_candidate(
        peer_rows,
        benchmark_mode="national",
        state_key=None,
        zip5=None,
        specialty_key="example",
        taxonomy_code=None,
        requested_procedure_codes={"123"},
        procedure_match_threshold=0.8,
    )

    assert candidate is None
    peer_rows = [
        {
            "geography_scope": "national",
            "geography_value": "US",
            "procedure_bucket": "999",
            "specialty_key": "other",
            "peer_n": 30,
            "year": 2024,
            "benchmark_mode": "national",
            "cohort_level": "L2",
            "taxonomy_code": "*",
        }
    ]
    candidate = pricing_module._select_peer_target_candidate(
        peer_rows,
        benchmark_mode="national",
        state_key=None,
        zip5=None,
        specialty_key="example",
        taxonomy_code=None,
        requested_procedure_codes={"123"},
        procedure_match_threshold=0.8,
    )
    assert candidate["selected_geography"] == "national"
    assert candidate["specialty_match"] is False
    assert candidate["procedure_match_ratio"] == 0.0


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_mode", ["state", None])
async def test_quality_modes_preserve_request_defaults(requested_mode):
    session = FakeSession([FakeResult(scalar=None)])

    mode, source = await pricing_module._resolve_quality_benchmark_mode(session, 2024, requested_mode)

    assert (mode, source) == (("state", "request") if requested_mode else ("national", "default"))
    assert len(session.executions) == (0 if requested_mode else 1)
    if requested_mode is None:
        assert session.executions[0][0][1] == {"year": 2024}


@pytest.mark.parametrize(
    ("ratios_by_name", "expected_tier", "expected_borderline"),
    [
        ({"risk_ratio_point": 1.2, "ci90_low": 1.1, "ci75_high": 1.3}, "low", False),
        ({"risk_ratio_point": 0.8, "ci90_low": 0.7, "ci75_high": 0.9}, "high", False),
        ({"risk_ratio_point": 1.2, "ci90_low": 1.0, "ci75_high": 1.3}, "acceptable", True),
    ],
)
def test_live_tiers_require_matching_confidence(ratios_by_name, expected_tier, expected_borderline):
    evidence_by_field = pricing_module._live_tier_evidence(ratios_by_name)

    assert evidence_by_field["tier"] == expected_tier
    assert evidence_by_field["borderline_status"] is expected_borderline


def test_empty_measure_domains_preserve_missing_evidence():
    assert pricing_module._geometric_mean([]) == 1.0
    domain_by_field = pricing_module._aggregate_domain([])

    assert domain_by_field["risk_ratio_point"] is None
    assert domain_by_field["score_0_100"] is None
    assert domain_by_field["evidence_n"] is None
    assert domain_by_field["ci_90"] == {"low": None, "high": None}


@pytest.mark.asyncio
async def test_missing_svi_uses_neutral_default():
    session = FakeSession([FakeResult(scalar=None)])

    assert (
        await pricing_module._load_provider_svi(session, zip5=None, year=2024)
        == pricing_module.PROVIDER_QUALITY_DEFAULT_SVI
    )
    assert session.executions == []
    assert (
        await pricing_module._load_provider_svi(session, zip5="60601", year=2024)
        == pricing_module.PROVIDER_QUALITY_DEFAULT_SVI
    )
    assert session.executions[0][0][1] == {"zcta": "60601", "year": 2024}


@pytest.mark.asyncio
async def test_observed_quality_preserves_absent_provider():
    session = FakeSession([FakeResult(rows=[])])

    assert await pricing_module._load_provider_quality_observed(session, npi=1000000001, year=2024) is None
    assert session.executions[0][0][0].compile().params == {"npi_1": 1000000001, "year_1": 2024}


@pytest.mark.asyncio
async def test_catalog_expansion_includes_same_name_codes():
    session = FakeSession(
        [
            FakeResult(rows=[]),
            FakeResult(scalar="Example service"),
            FakeResult(rows=[("HP_PROCEDURE_CODE", "123"), ("CPT", "12345")]),
        ]
    )

    context_by_field = await pricing_module._resolve_code_context(session, "CPT", "12345", expand_codes=True)

    assert context_by_field["internal_codes"] == [123]
    assert context_by_field["expanded"] is True
    assert context_by_field["matched_via"] == []
    assert context_by_field["resolved_codes"] == [
        {"code_system": "CPT", "code": "12345"},
        {"code_system": "HP_PROCEDURE_CODE", "code": "123"},
    ]
    assert "example service" in session.executions[-1][0][0].compile().params.values()


@pytest.mark.parametrize("raw_values", [[], ["123,456", "123"]])
def test_code_lists_accept_sanic_query_values(raw_values):
    args = RequestParameters({"procedure_codes": raw_values} if raw_values else {})

    assert pricing_module._parse_code_list_query_param(args, "procedure_codes") == (
        ["123", "456"] if raw_values else []
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("has_peers_table", [False, True])
async def test_cost_enrichment_preserves_unavailable_profiles(has_peers_table):
    session = FakeSession([FakeResult(scalar=True), FakeResult(scalar=has_peers_table), FakeResult(rows=[])])
    service_rows = [{"npi": 1000000001, "state": "IL", "cost_index": None}]

    await pricing_module._enrich_provider_service_cost_indices(session, service_rows, year=2024, internal_codes=[123])

    assert service_rows == [{"npi": 1000000001, "state": "IL", "cost_index": None}]
    assert len(session.executions) == (3 if has_peers_table else 2)
    if has_peers_table:
        bound_values = session.executions[-1][0][0].compile().params.values()
        assert [1000000001] in bound_values
        assert [123] in bound_values
        assert 2024 in bound_values


@pytest.mark.parametrize("is_enabled", [False, True])
def test_taxonomy_cache_supports_configured_limits(monkeypatch, is_enabled):
    monkeypatch.setattr(pricing_module, "_PROCEDURE_TAXONOMY_EVIDENCE_CACHE", OrderedDict())
    monkeypatch.setattr(pricing_module, "_PROCEDURE_TAXONOMY_EVIDENCE_CACHE_TTL_SECONDS", 10 if is_enabled else 0)
    monkeypatch.setattr(pricing_module, "_PROCEDURE_TAXONOMY_EVIDENCE_CACHE_MAX_KEYS", 1)
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(pricing_module.time, "monotonic", lambda: clock.now)
    first_key = (2024, (123,), 10)
    second_key = (2024, (456,), 10)
    evidence_items = [{"taxonomy_code": "207Q00000X", "distinct_npis": 25}]

    assert pricing_module._procedure_taxonomy_evidence_cache_set(first_key, evidence_items) == evidence_items
    assert pricing_module._procedure_taxonomy_evidence_cache_get(first_key) == (evidence_items if is_enabled else None)
    pricing_module._procedure_taxonomy_evidence_cache_set(second_key, evidence_items)
    assert pricing_module._procedure_taxonomy_evidence_cache_get(first_key) is None
    clock.now = 111.0
    assert pricing_module._procedure_taxonomy_evidence_cache_get(second_key) is None
    assert not pricing_module._PROCEDURE_TAXONOMY_EVIDENCE_CACHE


@pytest.mark.parametrize(
    ("status", "semantics", "warning_code"),
    [
        ("in_network", "in_network_historical_allowed_amounts", "allowed_amounts_not_negotiated_rates"),
        ("mixed", "mixed_historical_allowed_amounts", "allowed_amounts_mixed_network_status"),
        (
            "not_confirmed_in_network",
            "out_of_network_historical_allowed_amounts",
            "allowed_amounts_not_in_network_rates",
        ),
    ],
)
def test_allowed_amounts_explain_network_evidence(status, semantics, warning_code):
    network_by_field = pricing_module._allowed_amount_network_context(
        [{"network_status": status, "network_semantics": semantics}]
    )
    warning_by_field = pricing_module._allowed_amount_warning(network_by_field)

    assert network_by_field["network_semantics"] == semantics
    assert warning_by_field["code"] == warning_code
    assert "historical" in warning_by_field["message"].lower()


@pytest.mark.parametrize("serialized_sources", [None, {}, "{}", "invalid"])
def test_allowed_amount_sources_ignore_invalid_shapes(serialized_sources):
    assert pricing_module._allowed_amount_sources(serialized_sources) == []


@pytest.mark.parametrize("is_plan_scoped", [False, True])
def test_plan_annotations_preserve_optional_metadata(is_plan_scoped):
    payload_by_field = {"items": [], "pagination": {"total": 0}}

    pricing_module._annotate_ptg2_query_payload(
        payload_by_field, plan_id_type="ein", year=None, has_plan_scope=is_plan_scoped
    )
    pricing_module._annotate_ptg2_result_state(
        payload_by_field, has_plan_scope=is_plan_scoped, has_location_filter=False
    )

    assert payload_by_field["query"]["plan_id_type"] == "ein"
    assert "ignored_params" not in payload_by_field["query"]
    if is_plan_scoped:
        assert payload_by_field["result_state"] == "no_matching_rates"
    else:
        assert "result_state" not in payload_by_field


@pytest.mark.parametrize("raw_code", [" 99214 ", " d0120 ", " a1234 ", ""])
def test_reported_codes_preserve_system_inference(raw_code):
    expected_system = {"99214": "CPT", "D0120": "CDT", "A1234": "HCPCS", "": None}[raw_code.strip().upper()]

    assert pricing_module._reported_procedure_code_system(raw_code) == expected_system


def test_service_normalization_preserves_unrelated_payloads():
    payload_by_field = {"npi": 1000000001, "total_services": 0}

    assert pricing_module._normalize_service_payload(payload_by_field) is payload_by_field
    assert payload_by_field == {"npi": 1000000001, "total_services": 0}
