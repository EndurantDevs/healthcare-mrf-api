# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from unittest.mock import AsyncMock

import pytest

from api.endpoint import pricing
from tests.test_pricing_api import FakeResult, make_request, pricing_module


@pytest.mark.parametrize(
    ("percentile", "expected"),
    [
        (0.20, "$"),
        (0.21, "$$"),
        (0.40, "$$"),
        (0.41, "$$$"),
        (0.60, "$$$"),
        (0.61, "$$$$"),
        (0.80, "$$$$"),
        (0.81, "$$$$$"),
    ],
)
def test_cost_level_percentile_boundaries(percentile, expected):
    assert pricing._cost_level_from_percentile(percentile) == expected


@pytest.mark.parametrize(
    ("value", "thresholds", "expected"),
    [
        (None, (1, 2, 3, 4), None),
        (1, (1, 2, 3, 4), "$"),
        (2, (None, 2, 3, 4), "$$"),
        (3, (None, None, 3, 4), "$$$"),
        (4, (None, None, None, 4), "$$$$"),
        (5, (None, None, None, None), "$$$$$"),
    ],
)
def test_cost_level_threshold_boundaries(value, thresholds, expected):
    assert pricing._cost_level_from_thresholds(value, *thresholds) == expected


@pytest.mark.parametrize(
    ("risk_ratio", "ci75_high", "ci90_low", "confidence", "expected"),
    [
        (None, None, None, None, None),
        (1.20, 1.10, 1.10, 54, "acceptable"),
        (1.12, 1.10, 1.08, 55, "low"),
        (1.12, 1.10, 1.07, 55, "acceptable"),
        (0.88, 0.99, 0.70, 55, "high"),
        (0.88, None, 0.70, 55, "acceptable"),
    ],
)
def test_quality_tier_requires_confident_interval_support(risk_ratio, ci75_high, ci90_low, confidence, expected):
    assert (
        pricing._tier_from_quality_summary(
            risk_ratio_point=risk_ratio,
            ci75_high=ci75_high,
            ci90_low=ci90_low,
            confidence_0_100=confidence,
        )
        == expected
    )


def test_estimated_data_coverage_scores_available_evidence_and_caps():
    assert pricing._estimated_data_coverage_score({}) == 10.0
    assert (
        pricing._estimated_data_coverage_score(
            {
                "taxonomy_code": "207Q00000X",
                "specialty_key": "family-medicine",
                "zip5": "60601",
                "state_key": "IL",
                "provider_class": "individual",
                "has_enrollment": True,
                "has_medicare_claims": True,
                "location_source": "npi_address",
            }
        )
        == 60.0
    )


@pytest.mark.parametrize(
    ("profile", "peer_count", "expected"),
    [
        ({}, 0, 15.0),
        (
            {
                "state_key": "IL",
                "provider_class": "individual",
                "location_source": "npi_address",
            },
            29,
            28.0,
        ),
        (
            {
                "taxonomy_code": "207Q00000X",
                "specialty_key": "family-medicine",
                "zip5": "60601",
                "provider_class": "individual",
                "location_source": "npi_address",
            },
            30,
            52.0,
        ),
        (
            {
                "taxonomy_code": "207Q00000X",
                "specialty_key": "family-medicine",
                "zip5": "60601",
                "provider_class": "individual",
                "location_source": "npi_address",
            },
            100,
            54.0,
        ),
    ],
)
def test_estimated_confidence_uses_geography_and_peer_thresholds(profile, peer_count, expected):
    assert pricing._estimated_confidence_score(profile, peer_count) == expected


@pytest.mark.parametrize(
    ("profile", "requested_mode", "expected"),
    [
        ({"zip5": "60601"}, "zip", ["zip"]),
        ({}, "zip", []),
        ({"state_key": "IL"}, "state", ["state"]),
        ({}, "state", []),
        ({}, "national", ["national"]),
        ({"zip5": "60601", "state_key": "IL"}, None, ["zip", "state", "national"]),
        ({"state_key": "IL"}, None, ["state", "national"]),
        ({}, None, ["national"]),
    ],
)
def test_estimated_benchmark_modes_follow_available_geography(profile, requested_mode, expected):
    assert pricing._estimated_benchmark_modes_for_profile(profile, benchmark_mode=requested_mode) == expected


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (None, False),
        ({}, False),
        ({"cohort_context": []}, False),
        ({"cohort_context": {}}, False),
        ({"cohort_context": {"selected_geography": "zip:60601"}}, True),
        ({"cohort_context": {"selected_cohort_level": "specialty"}}, True),
        ({"cohort_context": {"peer_count": "31"}}, True),
        ({"cohort_context": {"peer_count": None}}, False),
    ],
)
def test_live_mode_availability_requires_concrete_cohort_evidence(payload, expected):
    assert pricing._is_live_mode_payload_available(payload) is expected


@pytest.mark.parametrize(
    ("raw_tokens", "expected"),
    [
        (None, []),
        ("   ", []),
        ([" 00123 ", None, "abc"], ["123", "ABC"]),
        (("1", "2"), ["1", "2"]),
        ({"3", "4"}, ["3", "4"]),
        ('[" 005 ", "hcpcs"]', ["5", "HCPCS"]),
        ("[invalid,json]", ["[INVALID", "JSON]"]),
        ("a,b | c;d", ["A", "B", "C", "D"]),
        (123, ["123"]),
    ],
)
def test_parse_token_list_normalizes_supported_input_shapes(raw_tokens, expected):
    actual = pricing._parse_token_list(raw_tokens)
    if isinstance(raw_tokens, set):
        assert sorted(actual) == sorted(expected)
    else:
        assert actual == expected


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"selected_geography": "national:anything"}, ("national", "US", "national")),
        ({"selected_geography": "zip:60601"}, ("zip", "60601", "zip:60601")),
        ({"selected_geography": "county:Cook"}, ("county", "Cook", "county:Cook")),
        ({"selected_geography": "national"}, ("national", "US", "national")),
        ({"selected_geography": "zip"}, (None, None, "zip")),
        ({"geography_scope": "country"}, ("national", "US", "national")),
        ({"geography_value": "USA"}, ("national", "US", "national")),
        ({"geography_value": "IL"}, ("state", "IL", "state:IL")),
        ({"geography_value": "60601-1234"}, ("zip", "60601-1234", "zip:60601-1234")),
        ({"geography_value": "Cook"}, (None, "Cook", None)),
        ({"scope": "state", "state": "IL"}, ("state", "IL", "state:IL")),
    ],
)
def test_extract_peer_target_geography_normalizes_explicit_and_inferred_shapes(payload, expected):
    assert pricing._extract_peer_target_geography(payload) == expected


@pytest.mark.parametrize(
    ("profile", "benchmark_mode", "expected"),
    [
        (None, None, ["provider_profile_not_found"]),
        ({}, None, ["missing_specialty_or_taxonomy", "missing_geography"]),
        (
            {"taxonomy_code": "207Q00000X", "state_key": "IL"},
            "zip",
            ["missing_zip5_for_zip_benchmark"],
        ),
        (
            {"specialty_key": "family-medicine", "zip5": "60601"},
            "state",
            ["missing_state_for_state_benchmark"],
        ),
        (
            {"specialty_key": "family-medicine", "state_key": "IL"},
            "state",
            [],
        ),
    ],
)
def test_quality_unavailable_reasons_are_specific(profile, benchmark_mode, expected):
    assert pricing._provider_quality_unavailable_reasons(profile, benchmark_mode=benchmark_mode) == expected


@pytest.mark.parametrize(
    ("scope", "value", "expected"),
    [
        ("national", "US", "national"),
        ("zip5", "60601", "zip:60601"),
        ("state", "IL", "state:IL"),
        ("state", None, None),
        ("county", "Cook", "Cook"),
    ],
)
def test_selected_geography_label_is_stable(scope, value, expected):
    assert pricing._selected_geography_label(scope, value) == expected


@pytest.mark.parametrize(
    ("raw", "default", "expected"),
    [
        (None, 0.4, 0.4),
        ("null", 0.4, 0.4),
        ("-1", 0.4, 0.0),
        ("1.5", 0.4, 1.0),
        ("0.25", 0.4, 0.25),
    ],
)
def test_probability_parser_defaults_and_clamps(raw, default, expected):
    assert pricing._parse_probability_clamped(raw, "threshold", default) == expected


def test_probability_parser_rejects_non_numeric_input():
    with pytest.raises(pricing.InvalidUsage, match="must be numeric"):
        pricing._parse_probability_clamped("not-a-number", "threshold", 0.4)


class MultiValueArgs(dict):
    def getlist(self, key):
        return ["001, 002", "002", ""] if key == "codes" else []


def test_code_list_parser_handles_multivalue_and_scalar_inputs():
    assert pricing._parse_code_list_query_param(MultiValueArgs(), "codes") == [
        "001",
        "002",
    ]
    assert pricing._parse_code_list_query_param({"codes": ("a", "b")}, "codes") == [
        "A",
        "B",
    ]
    assert pricing._parse_code_list_query_param({"codes": None}, "codes") == []


def test_geography_candidates_preserve_specific_to_national_order():
    assert pricing._geography_candidates(state_raw=" il ", city_raw=" Chicago ", zip5_raw="60601-1234") == [
        ("zip5", "60601"),
        ("state_city", "IL|chicago"),
        ("state", "IL"),
        ("national", "US"),
    ]
    assert pricing._geography_candidates(state_raw=None, city_raw=None, zip5_raw=None) == [("national", "US")]


def _quality_profile(**overrides):
    profile_by_field = {
        "specialty_key": "family_medicine",
        "taxonomy_code": "207Q00000X",
        "zip5": "60601",
        "state_key": "IL",
        "provider_class": "clinician",
        "location_source": "npi_address",
        "has_enrollment": True,
    }
    profile_by_field.update(overrides)
    if profile_by_field["zip5"] is None and profile_by_field["state_key"] is None:
        profile_by_field["location_source"] = "unknown"
    return profile_by_field


def _patch_quality_tables(monkeypatch):
    async def is_table_available(_session, table_name):
        return table_name in {
            pricing_module.QUALITY_SCORE_TABLE_NAME,
            pricing_module.QUALITY_DOMAIN_TABLE_NAME,
            pricing_module.NPIDataTaxonomy.__tablename__,
            pricing_module.ProviderEnrichmentSummary.__tablename__,
            pricing_module.NPIAddress.__tablename__,
        }

    monkeypatch.setattr(pricing_module, "_is_table_available", is_table_available)


def _empty_quality_cohort():
    return FakeResult(
        rows=[
            {
                "peer_count": 0,
                "risk_ratio_point": None,
                "ci75_low": None,
                "ci75_high": None,
                "ci90_low": None,
                "ci90_high": None,
                "score_0_100": None,
            }
        ]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("benchmark_mode", ["zip", "state", "national"])
@pytest.mark.parametrize("override", [False, True])
async def test_score_estimates_requested_cohort(monkeypatch, benchmark_mode, override):
    _patch_quality_tables(monkeypatch)
    request_args_by_name = {"year": "2024", "benchmark_mode": benchmark_mode}
    if override:
        request_args_by_name["taxonomy_code"] = " 207r00000x "
    request = make_request(
        [
            FakeResult(rows=[]),
            FakeResult(rows=[_quality_profile()]),
            FakeResult(rows=[{"peer_count": 40, "risk_ratio_point": 0.8, "score_0_100": 82}]),
            FakeResult(rows=[{"domain": "cost", "risk_ratio": 0.75, "peer_count": 35}]),
        ],
        args=request_args_by_name,
    )

    document = json.loads((await pricing_module.get_pricing_provider_score(request, "1000000001")).body)

    assert document["score_method"] == "estimated"
    assert document["benchmark_mode"] == benchmark_mode
    assert document["available_benchmark_modes"] == [benchmark_mode]
    assert document["score_0_100"] == 82.0
    assert document["domains"]["cost"]["evidence_n"] == 35.0
    assert document["evidence_profile"]["has_enrollment"] is True
    assert document["scores_by_benchmark_mode"][benchmark_mode]["score_0_100"] == 82.0
    assert (
        document["cohort_context"]["selected_geography"]
        == {"zip": "zip:60601", "state": "state:IL", "national": "national"}[benchmark_mode]
    )
    estimate_sql, parameters = request.ctx.sa_session.executions[-2][0]
    assert "s.score_method" in str(estimate_sql)
    assert parameters["year"] == 2024
    assert parameters["benchmark_mode"] == benchmark_mode
    assert parameters["taxonomy_code"] == ("207R00000X" if override else "207Q00000X")
    assert parameters.get("zip5") == ("60601" if benchmark_mode == "zip" else None)
    assert parameters.get("state_key") == ("IL" if benchmark_mode == "state" else None)


@pytest.mark.asyncio
async def test_score_fallback_selects_next_available_cohort(monkeypatch):
    _patch_quality_tables(monkeypatch)
    request = make_request(
        [
            FakeResult(rows=[]),
            FakeResult(rows=[_quality_profile()]),
            _empty_quality_cohort(),
            FakeResult(rows=[{"peer_count": 30, "score_0_100": 70}]),
            FakeResult(rows=[]),
            _empty_quality_cohort(),
        ],
        args={"year": "2024"},
    )

    document = json.loads((await pricing_module.get_pricing_provider_score(request, "1000000001")).body)

    assert document["score_method"] == "estimated"
    assert document["benchmark_mode"] == "state"
    assert document["available_benchmark_modes"] == ["state"]
    assert document["scores_by_benchmark_mode"]["zip"] is None
    assert document["scores_by_benchmark_mode"]["national"] is None
    assert document["cohort_context"]["selected_geography"] == "state:IL"
    assert [execution[0][1]["benchmark_mode"] for execution in request.ctx.sa_session.executions[2:]] == [
        "zip",
        "state",
        "state",
        "national",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("profile", "benchmark_mode", "selected_mode", "geography", "reason", "empty_modes"),
    [
        (None, None, "national", None, "provider_profile_not_found", ()),
        (_quality_profile(), "zip", "zip", "zip:60601", "benchmark_mode_zip_estimate_unavailable", ("zip",)),
        (_quality_profile(zip5=None), None, "state", "state:IL", "no_matching_peer_cohort", ("state", "national")),
        (_quality_profile(zip5=None, state_key=None), None, "national", None, "missing_geography", ()),
        (_quality_profile(state_key=None), "state", "state", "zip:60601", "missing_state_for_state_benchmark", ()),
    ],
)
async def test_score_fallback_explains_unavailability(
    monkeypatch, profile, benchmark_mode, selected_mode, geography, reason, empty_modes
):
    _patch_quality_tables(monkeypatch)
    request_args_by_name = {"year": "2024"}
    if benchmark_mode:
        request_args_by_name["benchmark_mode"] = benchmark_mode
    request = make_request(
        [
            FakeResult(rows=[]),
            FakeResult(rows=[] if profile is None else [profile]),
            *[_empty_quality_cohort() for _ in empty_modes],
        ],
        args=request_args_by_name,
    )

    document = json.loads((await pricing_module.get_pricing_provider_score(request, "1000000001")).body)

    assert document["score_method"] == "unavailable"
    assert document["benchmark_mode"] == selected_mode
    assert document["score_0_100"] is None
    assert document["available_benchmark_modes"] == []
    assert reason in document["unavailable_reasons"]
    assert all(score is None for score in document["scores_by_benchmark_mode"].values())
    assert document.get("cohort_context", {}).get("selected_geography") == geography
    assert request.ctx.sa_session.executions[1][0][1] == {"npi": 1000000001, "year": 2024}
    assert (
        tuple(execution[0][1]["benchmark_mode"] for execution in request.ctx.sa_session.executions[2:]) == empty_modes
    )


@pytest.mark.asyncio
async def test_score_fallback_handles_estimate_query_failure(monkeypatch):
    _patch_quality_tables(monkeypatch)
    request = make_request([], args={"year": "2024", "benchmark_mode": "state"})
    request.ctx.sa_session.execute = AsyncMock(
        side_effect=[
            FakeResult(rows=[]),
            FakeResult(rows=[_quality_profile()]),
            RuntimeError("Synthetic peer lookup unavailable"),
        ]
    )

    document = json.loads((await pricing_module.get_pricing_provider_score(request, "1000000001")).body)

    assert document["score_method"] == "unavailable"
    assert document["unavailable_reasons"] == ["benchmark_mode_state_estimate_unavailable"]
    assert document["cohort_context"]["selected_geography"] == "state:IL"
    assert request.ctx.sa_session.execute.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("benchmark_mode", [None, "zip"])
async def test_live_score_without_peer_evidence_falls_back(monkeypatch, benchmark_mode):
    _patch_quality_tables(monkeypatch)
    request_args_by_name = {"year": "2024", "taxonomy_code": "207Q00000X"}
    if benchmark_mode:
        request_args_by_name["benchmark_mode"] = benchmark_mode
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1000000001,
                        "total_services": 10,
                        "total_beneficiaries": 5,
                        "total_allowed_amount": 500,
                        "state": "IL",
                        "zip5": "60601",
                    }
                ]
            ),
            FakeResult(rows=[]),
            FakeResult(rows=[{"total_rx_claims": 0.0, "total_rx_beneficiaries": 0.0}]),
            FakeResult(scalar=0.5),
            FakeResult(rows=[]),
            FakeResult(rows=[]),
            FakeResult(rows=[]),
            FakeResult(rows=[_quality_profile()]),
            *[_empty_quality_cohort() for _ in range(1 if benchmark_mode else 3)],
        ],
        args=request_args_by_name,
    )

    document = json.loads((await pricing_module.get_pricing_provider_score(request, "1000000001")).body)

    assert document["score_method"] == "unavailable"
    assert document["benchmark_mode"] == "zip"
    assert document["available_benchmark_modes"] == []
    assert document["unavailable_reasons"] == [
        "benchmark_mode_zip_estimate_unavailable" if benchmark_mode else "no_matching_peer_cohort"
    ]
    target_parameters = request.ctx.sa_session.executions[4][0][1]
    assert target_parameters["zip5"] == "60601"
    assert target_parameters["state_key"] == "IL"
    assert target_parameters["mode_0"] == "zip"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("score_rows", "selected_mode", "selected_score", "available_modes"),
    [
        ([{"benchmark_mode": "historic", "score_0_100": 55}], "historic", 55.0, []),
        (
            [
                {"benchmark_mode": "zip", "score_0_100": 82},
                {"benchmark_mode": "national", "score_0_100": 70},
            ],
            "zip",
            82.0,
            ["zip", "national"],
        ),
    ],
)
async def test_stored_score_selects_available_modes_and_ignores_unknown_domains(
    monkeypatch, score_rows, selected_mode, selected_score, available_modes
):
    _patch_quality_tables(monkeypatch)
    request = make_request(
        [
            FakeResult(rows=score_rows),
            FakeResult(
                rows=[
                    {"benchmark_mode": "historic", "domain": "cost", "score_0_100": 1},
                    {"benchmark_mode": "zip", "domain": "obsolete", "score_0_100": 1},
                ]
            ),
        ],
        args={"year": "2024", "procedure_codes": " , , ", "benchmark_mode": "   "},
    )

    document = json.loads((await pricing_module.get_pricing_provider_score(request, "1000000001")).body)

    assert document["benchmark_mode"] == selected_mode
    assert document["score_0_100"] == selected_score
    assert document["available_benchmark_modes"] == available_modes
    assert document["domains"]["cost"]["score_0_100"] is None
    assert "obsolete" not in document["domains"]
    assert len(request.ctx.sa_session.executions) == 2
    assert all(execution[0][1] == {"npi": 1000000001, "year": 2024} for execution in request.ctx.sa_session.executions)


@pytest.mark.asyncio
async def test_score_rejects_unknown_benchmark():
    request = make_request([], args={"benchmark_mode": "county"})

    with pytest.raises(pricing_module.InvalidUsage, match="benchmark_mode.*zip, state, national"):
        await pricing_module.get_pricing_provider_score(request, "1000000001")

    assert request.ctx.sa_session.executions == []
