# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from api.endpoint import pricing


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
def test_quality_tier_requires_confident_interval_support(
    risk_ratio, ci75_high, ci90_low, confidence, expected
):
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
def test_estimated_confidence_uses_geography_and_peer_thresholds(
    profile, peer_count, expected
):
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
def test_estimated_benchmark_modes_follow_available_geography(
    profile, requested_mode, expected
):
    assert (
        pricing._estimated_benchmark_modes_for_profile(
            profile, benchmark_mode=requested_mode
        )
        == expected
    )


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
def test_extract_peer_target_geography_normalizes_explicit_and_inferred_shapes(
    payload, expected
):
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
    assert (
        pricing._provider_quality_unavailable_reasons(
            profile, benchmark_mode=benchmark_mode
        )
        == expected
    )


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
    assert pricing._geography_candidates(
        state_raw=" il ", city_raw=" Chicago ", zip5_raw="60601-1234"
    ) == [
        ("zip5", "60601"),
        ("state_city", "IL|chicago"),
        ("state", "IL"),
        ("national", "US"),
    ]
    assert pricing._geography_candidates(
        state_raw=None, city_raw=None, zip5_raw=None
    ) == [("national", "US")]
