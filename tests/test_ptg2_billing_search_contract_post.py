# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolved-query and selector-scope contract tests."""

from __future__ import annotations

import pytest

from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_NO_MATCH,
    BillingSearchResolvedQuery,
    BillingSearchSelectorBindingScope,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_post_support import (
    SNAPSHOT_ID,
    billing_entity_ref,
    query,
    source_scope,
)


def test_resolved_query_supports_exact_zip_and_central_radius_coordinates() -> None:
    exact_zip = query(limit=200)
    radius = query(
        zip5=None,
        latitude=38.0,
        longitude=-82.0,
        radius_miles=25.0,
    )

    assert exact_zip.geo_args == {"zip5": "25000"}
    assert radius.geo_args == {
        "lat": 38.0,
        "long": -82.0,
        "radius_miles": 25.0,
    }


@pytest.mark.parametrize(
    "updates",
    (
        {"limit": 201},
        {"zip5": "25000", "latitude": 38.0, "longitude": -82.0},
        {"zip5": None, "latitude": 38.0, "longitude": None, "radius_miles": 5.0},
        {"selector_kind": "tax_identity", "tax_identity_type": None},
        {"selector_kind": "billing_entity_ref", "tax_identity_type": "ein"},
    ),
)
def test_resolved_query_rejects_ambiguous_or_out_of_contract_fields(updates) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        query(**updates)


def test_billing_ref_query_carries_no_tax_identity_type() -> None:
    ref_query = query(
        selector_kind="billing_entity_ref",
        tax_identity_type=None,
    )

    assert isinstance(ref_query, BillingSearchResolvedQuery)
    assert ref_query.tax_identity_type is None
    assert "redacted" in repr(ref_query)


def test_nonmatched_selector_binding_cannot_carry_identity_material() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchSelectorBindingScope(
            binding_ordinal=0,
            snapshot_id=SNAPSHOT_ID,
            state=BILLING_SELECTOR_NO_MATCH,
            source_scope=source_scope(),
            billing_entity_ref=billing_entity_ref(),
        )
