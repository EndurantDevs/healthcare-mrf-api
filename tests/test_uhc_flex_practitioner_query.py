# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure tests for exact Flex Practitioner query validation."""

from dataclasses import replace
import urllib.parse

import pytest

from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_QUERY_COUNT,
)
from process.uhc_flex_practitioner_query import (
    UHCFlexPractitionerQueryError,
    UHC_FLEX_PRACTITIONER_MATCHED,
    UHC_FLEX_PRACTITIONER_UNMATCHED,
    classify_uhc_flex_practitioner_exception,
    classify_uhc_flex_practitioner_http_status,
    uhc_flex_practitioner_query_url,
    validate_uhc_flex_practitioner_search_bundle,
)


REQUESTED_NPI = 1234567893
OTHER_NPI = 1588616783


def _practitioner(
    resource_id: str,
    npi: int = REQUESTED_NPI,
    **additional_fields,
) -> dict:
    return {
        "resourceType": "Practitioner",
        "id": resource_id,
        "identifier": [
            {
                "system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
                "value": str(npi),
            }
        ],
        "name": [{"family": "Example", "given": ["Avery"]}],
        **additional_fields,
    }


def _search_bundle(resource_rows: list[dict], *, total: int | None = None) -> dict:
    bundle_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": resource} for resource in resource_rows],
    }
    if total is not None:
        bundle_by_field["total"] = total
    return bundle_by_field


def _validation_error_code(response_payload: object) -> str:
    with pytest.raises(UHCFlexPractitionerQueryError) as error_info:
        validate_uhc_flex_practitioner_search_bundle(
            REQUESTED_NPI,
            response_payload,
        )
    return error_info.value.code


def test_query_url_has_one_exact_system_qualified_token_and_fixed_count():
    query_url = uhc_flex_practitioner_query_url(REQUESTED_NPI)
    parsed_url = urllib.parse.urlsplit(query_url)
    query_pairs = urllib.parse.parse_qsl(parsed_url.query)

    assert query_url == (
        "https://flex.optum.com/fhirpublic/R4/Practitioner?"
        "identifier=http%3A%2F%2Fhl7.org%2Ffhir%2Fsid%2Fus-npi%7C"
        "1234567893&_count=16"
    )
    assert parsed_url.path == "/fhirpublic/R4/Practitioner"
    assert query_pairs == [
        (
            "identifier",
            f"{UHC_FLEX_OFFICIAL_NPI_SYSTEM}|{REQUESTED_NPI}",
        ),
        ("_count", str(UHC_FLEX_PRACTITIONER_QUERY_COUNT)),
    ]
    assert "," not in query_pairs[0][1]


@pytest.mark.parametrize(
    "candidate",
    [True, "1234567893", [REQUESTED_NPI], 1234567890],
)
def test_query_url_rejects_noncanonical_or_multi_value_input(candidate):
    with pytest.raises(
        UHCFlexPractitionerQueryError,
        match="requested NPI is invalid",
    ):
        uhc_flex_practitioner_query_url(candidate)


def test_empty_searchset_is_an_explicit_unmatched_result():
    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([], total=0),
    )

    assert result.requested_npi == REQUESTED_NPI
    assert result.outcome == UHC_FLEX_PRACTITIONER_UNMATCHED
    assert result.is_unmatched is True
    assert result.resource_count == 0
    assert result.resource_ids == ()
    assert result.resource_payloads() == ()
    assert len(result.result_sha256) == 64


def test_matched_result_is_sorted_deterministic_and_returns_fresh_payloads():
    first_bundle = _search_bundle(
        [_practitioner("practitioner-b"), _practitioner("practitioner-a")],
        total=2,
    )
    second_bundle = _search_bundle(
        [_practitioner("practitioner-a"), _practitioner("practitioner-b")],
        total=2,
    )

    first_result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        first_bundle,
    )
    second_result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        second_bundle,
    )

    assert first_result.outcome == UHC_FLEX_PRACTITIONER_MATCHED
    assert first_result.is_unmatched is False
    assert first_result.resource_ids == ("practitioner-a", "practitioner-b")
    assert first_result.resource_count == 2
    assert first_result.result_sha256 == second_result.result_sha256
    assert first_result.resource_sha256_by_id == (
        second_result.resource_sha256_by_id
    )
    returned_payloads = first_result.resource_payloads()
    returned_payloads[0]["active"] = False
    assert "active" not in first_result.resource_payloads()[0]


def test_identical_duplicate_resource_ids_are_deduplicated():
    resource = _practitioner("practitioner-a")
    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([resource, dict(resource)], total=2),
    )

    assert result.resource_count == 1
    assert result.resource_ids == ("practitioner-a",)


def test_conflicting_duplicate_resource_ids_fail_closed():
    first = _practitioner("practitioner-a", active=True)
    second = _practitioner("practitioner-a", active=False)

    assert _validation_error_code(_search_bundle([first, second])) == (
        "duplicate_resource_conflict"
    )
    malformed = _practitioner("practitioner-a")
    malformed["identifier"][0]["value"] = None
    for resource_rows in ([first, malformed], [malformed, first]):
        result = validate_uhc_flex_practitioner_search_bundle(
            REQUESTED_NPI,
            _search_bundle(resource_rows),
        )
        assert result.resource_payloads() == (first,)


@pytest.mark.parametrize(
    ("response_payload", "expected_code"),
    [
        ({"resourceType": "OperationOutcome"}, "operation_outcome"),
        (
            {"resourceType": "Bundle", "type": "collection", "entry": []},
            "searchset_required",
        ),
        (
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [],
                "link": [{"relation": "next", "url": "https://example.test"}],
            },
            "next_link_forbidden",
        ),
        (
            _search_bundle([{"resourceType": "OperationOutcome", "id": "error"}]),
            "operation_outcome",
        ),
        (
            _search_bundle(
                [
                    {
                        "resourceType": "Organization",
                        "id": "organization-a",
                        "identifier": [],
                    }
                ]
            ),
            "practitioner_required",
        ),
        (
            _search_bundle([_practitioner("not/valid")]),
            "resource_id_invalid",
        ),
        (
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [None],
            },
            "entry_invalid",
        ),
        (_search_bundle([], total=1), "total_mismatch"),
        (
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [],
                "total": True,
            },
            "total_invalid",
        ),
    ],
)
def test_search_bundle_rejects_non_exact_or_unsafe_responses(
    response_payload,
    expected_code,
):
    assert _validation_error_code(response_payload) == expected_code


def test_search_bundle_requires_the_exact_us_npi_identifier_system():
    resource = _practitioner("practitioner-a")
    resource["identifier"][0]["system"] = "https://example.test/npi"

    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([resource], total=1),
    )
    assert result.outcome == UHC_FLEX_PRACTITIONER_UNMATCHED
    assert result.resource_count == 0


@pytest.mark.parametrize("identifiers", [[], [None]])
def test_search_bundle_quarantines_resources_without_a_valid_npi(identifiers):
    resource = _practitioner("practitioner-a")
    resource["identifier"] = identifiers

    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([resource], total=1),
    )

    assert result.outcome == UHC_FLEX_PRACTITIONER_UNMATCHED
    assert result.resource_count == 0


def test_search_bundle_quarantines_malformed_npi_with_or_without_exact_sibling():
    exact_resource = _practitioner("practitioner-a")
    malformed_resource = _practitioner("practitioner-b")
    malformed_resource["identifier"][0]["value"] = None
    expected_result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([exact_resource], total=1),
    )

    for resource_rows in (
        [exact_resource, malformed_resource],
        [malformed_resource, exact_resource],
    ):
        result = validate_uhc_flex_practitioner_search_bundle(
            REQUESTED_NPI,
            _search_bundle(resource_rows, total=2),
        )

        assert result.resource_count == 1
        assert result.resource_ids == ("practitioner-a",)
        assert result.resource_payloads() == (exact_resource,)
        assert result.resource_sha256_by_id == expected_result.resource_sha256_by_id
        assert result.result_sha256 == expected_result.result_sha256

    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([malformed_resource], total=1),
    )
    assert result.outcome == UHC_FLEX_PRACTITIONER_UNMATCHED
    assert result.resource_count == 0


@pytest.mark.parametrize("foreign_identifier_position", [0, 1])
@pytest.mark.parametrize("is_malformed_entry_first", [False, True])
def test_search_bundle_quarantines_foreign_npi_in_malformed_sibling(
    foreign_identifier_position,
    is_malformed_entry_first,
):
    malformed_resource = _practitioner("practitioner-b")
    malformed_resource["identifier"][0]["value"] = None
    foreign_identifier_by_field = {
        "system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
        "value": str(OTHER_NPI),
    }
    malformed_resource["identifier"].insert(
        foreign_identifier_position,
        foreign_identifier_by_field,
    )

    exact_resource = _practitioner("practitioner-a")
    entries = [malformed_resource, exact_resource]
    if not is_malformed_entry_first:
        entries.reverse()

    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle(entries, total=2),
    )
    assert result.outcome == UHC_FLEX_PRACTITIONER_MATCHED
    assert result.resource_ids == ("practitioner-a",)


def test_search_bundle_quarantines_an_ambiguous_only_result():
    resource = _practitioner("practitioner-a")
    resource["identifier"].append(
        {
            "system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
            "value": str(OTHER_NPI),
        }
    )

    response_payload = _search_bundle([resource], total=1)
    response_payload["entry"][0]["search"] = {"mode": "match"}
    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        response_payload,
    )
    empty_result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([], total=0),
    )

    assert result.outcome == UHC_FLEX_PRACTITIONER_UNMATCHED
    assert result.is_unmatched is True
    assert result.resource_count == 0
    assert result.resource_ids == ()
    assert result.resource_payloads() == ()
    assert result.resource_sha256_by_id == ()
    assert result.result_sha256 == empty_result.result_sha256


def test_search_bundle_quarantines_a_foreign_only_sibling():
    exact_resource = _practitioner("practitioner-a")
    foreign_resource = _practitioner("practitioner-b", OTHER_NPI)

    for resource_rows in (
        [exact_resource, foreign_resource],
        [foreign_resource, exact_resource],
    ):
        result = validate_uhc_flex_practitioner_search_bundle(
            REQUESTED_NPI,
            _search_bundle(resource_rows, total=2),
        )
        assert result.outcome == UHC_FLEX_PRACTITIONER_MATCHED
        assert result.resource_ids == ("practitioner-a",)

    foreign_only = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([foreign_resource], total=1),
    )
    assert foreign_only.outcome == UHC_FLEX_PRACTITIONER_UNMATCHED
    assert foreign_only.resource_count == 0


def test_search_bundle_keeps_exact_resource_and_omits_ambiguous_sibling():
    exact_resource = _practitioner("practitioner-a")
    ambiguous_resource = _practitioner("practitioner-b")
    ambiguous_resource["identifier"].append(
        {
            "system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
            "value": str(OTHER_NPI),
        }
    )

    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([ambiguous_resource, exact_resource], total=2),
    )

    assert result.outcome == UHC_FLEX_PRACTITIONER_MATCHED
    assert result.resource_ids == ("practitioner-a",)

    conflicting_resource_map = dict(ambiguous_resource, id="practitioner-a")
    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([exact_resource, conflicting_resource_map], total=2),
    )
    assert result.resource_payloads() == (exact_resource,)


def test_search_bundle_rejects_entry_or_total_above_the_fixed_cap():
    over_cap_resources = [
        _practitioner(f"practitioner-{index}")
        for index in range(UHC_FLEX_PRACTITIONER_QUERY_COUNT + 1)
    ]

    assert _validation_error_code(_search_bundle(over_cap_resources)) == (
        "result_cap_exceeded"
    )
    assert _validation_error_code(
        _search_bundle([], total=UHC_FLEX_PRACTITIONER_QUERY_COUNT + 1)
    ) == "result_cap_exceeded"


def test_query_error_does_not_echo_npi_or_response_payload():
    response_payload = _search_bundle(
        [
            _practitioner("practitioner-a", active=True),
            _practitioner("practitioner-a", active=False),
        ]
    )

    with pytest.raises(UHCFlexPractitionerQueryError) as error_info:
        validate_uhc_flex_practitioner_search_bundle(
            REQUESTED_NPI,
            response_payload,
        )

    message = str(error_info.value)
    assert str(REQUESTED_NPI) not in message
    assert str(OTHER_NPI) not in message
    assert "practitioner-a" not in message


def test_result_rejects_outcome_or_hash_drift():
    result = validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        _search_bundle([_practitioner("practitioner-a")]),
    )

    with pytest.raises(UHCFlexPractitionerQueryError, match="result is invalid"):
        replace(result, outcome=UHC_FLEX_PRACTITIONER_UNMATCHED)
    with pytest.raises(UHCFlexPractitionerQueryError, match="result is invalid"):
        replace(result, result_sha256="0" * 64)


@pytest.mark.parametrize(
    ("http_status", "category", "is_retryable"),
    [
        (200, "success", False),
        (408, "retryable", True),
        (423, "retryable", True),
        (425, "retryable", True),
        (429, "retryable", True),
        (500, "retryable", True),
        (599, "retryable", True),
        (404, "terminal", False),
        (201, "terminal", False),
        (True, "invalid", False),
        (99, "invalid", False),
    ],
)
def test_http_retry_classification_is_bounded(
    http_status,
    category,
    is_retryable,
):
    decision = classify_uhc_flex_practitioner_http_status(http_status)

    assert decision.category == category
    assert decision.is_retryable is is_retryable


@pytest.mark.parametrize(
    ("error", "category", "is_retryable"),
    [
        (TimeoutError("secret"), "retryable", True),
        (ConnectionRefusedError("secret"), "retryable", True),
        (
            UHCFlexPractitionerQueryError("cross_npi"),
            "terminal",
            False,
        ),
        (
            UHCFlexPractitionerQueryError("total_mismatch"),
            "retryable",
            True,
        ),
        (RuntimeError("secret"), "terminal", False),
        ("not-an-exception", "invalid", False),
    ],
)
def test_exception_retry_classification_retains_no_error_text(
    error,
    category,
    is_retryable,
):
    decision = classify_uhc_flex_practitioner_exception(error)

    assert decision.category == category
    assert decision.is_retryable is is_retryable
    assert "secret" not in decision.reason_code
