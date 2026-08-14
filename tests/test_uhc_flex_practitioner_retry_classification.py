# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retry classification for exact Flex Practitioner queries."""

import pytest

from process.uhc_flex_practitioner_query import UHCFlexPractitionerQueryError
from process.uhc_flex_practitioner_query import (
    classify_uhc_flex_practitioner_exception,
)
from process.uhc_flex_practitioner_query import (
    classify_uhc_flex_practitioner_http_status,
)


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
) -> None:
    decision = classify_uhc_flex_practitioner_http_status(http_status)

    assert decision.category == category
    assert decision.is_retryable is is_retryable


@pytest.mark.parametrize(
    ("error", "category", "is_retryable"),
    [
        (TimeoutError("secret"), "retryable", True),
        (ConnectionRefusedError("secret"), "retryable", True),
        (UHCFlexPractitionerQueryError("cross_npi"), "terminal", False),
        (UHCFlexPractitionerQueryError("total_mismatch"), "retryable", True),
        (RuntimeError("secret"), "terminal", False),
        ("not-an-exception", "invalid", False),
    ],
)
def test_exception_retry_classification_retains_no_error_text(
    error,
    category,
    is_retryable,
) -> None:
    decision = classify_uhc_flex_practitioner_exception(error)

    assert decision.category == category
    assert decision.is_retryable is is_retryable
    assert "secret" not in decision.reason_code
