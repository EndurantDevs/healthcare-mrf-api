# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy

import pytest

from api.billing_search_post_query import build_billing_search_resolved_query
from api.billing_search_post_request import parse_billing_search_post_request
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError

PLAN_RELEASE_ID = "hprelease_" + "0" * 26


def _service_query(*, radius_miles: float = 0.0):
    return parse_billing_search_post_request(
        {
            "healthporta_plan_id": "hpplan_" + "0" * 26,
            "billing_identity": {"billing_entity_ref": "be1_" + "a" * 48},
            "procedure": {
                "code_system": "CPT",
                "code": "00000",
                "modifiers": [],
                "place_of_service": [],
            },
            "geo": {"zip5": "00000", "radius_miles": radius_miles},
        }
    ).service_query


def test_exact_zip_query_needs_no_centroid() -> None:
    query = build_billing_search_resolved_query(
        _service_query(),
        plan_release_id=PLAN_RELEASE_ID,
        radius_zip_context=None,
        after_sort_key=None,
    )

    assert query.geo_args == {"zip5": "00000"}
    assert query.selector_kind == "billing_entity_ref"


def test_radius_query_uses_only_server_resolved_centroid() -> None:
    query = build_billing_search_resolved_query(
        _service_query(radius_miles=25.0),
        plan_release_id=PLAN_RELEASE_ID,
        radius_zip_context={
            "zip5": "00000",
            "latitude": 38.0,
            "longitude": -82.0,
        },
        after_sort_key=None,
    )

    assert query.geo_args == {
        "lat": 38.0,
        "long": -82.0,
        "radius_miles": 25.0,
    }


@pytest.mark.parametrize(
    "context",
    (
        None,
        {"zip5": "00001", "latitude": 38.0, "longitude": -82.0},
        {"zip5": "00000", "latitude": float("nan"), "longitude": -82.0},
        {"zip5": "00000", "latitude": 38.0, "longitude": None},
    ),
)
def test_radius_query_fails_closed_without_exact_centroid(context) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        build_billing_search_resolved_query(
            _service_query(radius_miles=25.0),
            plan_release_id=PLAN_RELEASE_ID,
            radius_zip_context=copy.deepcopy(context),
            after_sort_key=None,
        )


def test_exact_zip_rejects_unrequested_centroid() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        build_billing_search_resolved_query(
            _service_query(),
            plan_release_id=PLAN_RELEASE_ID,
            radius_zip_context={
                "zip5": "00000",
                "latitude": 38.0,
                "longitude": -82.0,
            },
            after_sort_key=None,
        )
