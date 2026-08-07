# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pseudonymous state-digest tests for sealed billing-search access."""

from api import billing_search_endpoint_access as endpoint
from tests.test_billing_search_endpoint_access import (
    BILLING_ENTITY_REF,
    TRUSTED_NOW,
    _authorize,
)


def test_endpoint_state_digest_is_stable_only_for_same_sealed_access() -> None:
    first_access = _authorize()
    second_access = _authorize()

    validated, first_digest = endpoint.validate_billing_search_endpoint_access_state(
        first_access,
        trusted_now=TRUSTED_NOW,
    )
    _, repeated_digest = endpoint.validate_billing_search_endpoint_access_state(
        first_access,
        trusted_now=TRUSTED_NOW,
    )
    _, second_digest = endpoint.validate_billing_search_endpoint_access_state(
        second_access,
        trusted_now=TRUSTED_NOW,
    )

    assert validated is first_access
    assert first_digest == repeated_digest
    assert len(first_digest) == 64
    assert first_digest != second_digest
    assert BILLING_ENTITY_REF not in first_digest
