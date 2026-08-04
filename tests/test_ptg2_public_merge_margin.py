# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from decimal import Decimal
from types import SimpleNamespace

import pytest

from api import ptg2_serving as serving


@pytest.mark.parametrize(
    ("raw_rate", "expected"),
    [
        (None, None),
        (True, None),
        (Decimal("12.50"), Decimal("12.50")),
        ("", None),
        ("not-a-rate", None),
        ("NaN", None),
    ],
)
def test_decimal_rate_sorting_rejects_non_prices(raw_rate, expected):
    """Accept finite numeric rates and reject empty, boolean, or invalid values."""

    assert serving._ptg2_decimal_rate_sort_value(raw_rate) == expected


def test_provider_price_sorting_ignores_invalid_price_payload_members():
    """Ignore non-record and empty-rate members in both public price fields."""

    sort_value = serving._ptg2_provider_price_sort_value(
        {
            "prices": ["invalid", {"negotiated_rate": None}],
            "price_summary": [7, {"rate": ""}],
        }
    )
    assert sort_value == Decimal("Infinity")


def test_unique_payload_merge_normalizes_target_and_deduplicates_values():
    """Normalize scalar payloads while preserving unique structured evidence."""

    target_by_field = {"evidence": "existing"}
    serving._merge_unique_payload_list(target_by_field, "evidence", "null")
    serving._merge_unique_payload_list(target_by_field, "evidence", {"id": 1})
    serving._merge_unique_payload_list(
        target_by_field,
        "evidence",
        [None, "", {"id": 1}, "additional"],
    )

    assert target_by_field["evidence"] == ["existing", {"id": 1}, "additional"]


def _mergeable_provider_rate(*, price_key, rate):
    return {
        "npi": 1234567890,
        "address": "[]",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "source_artifact_key": 0,
        "provider_set_hash": f"{1:032x}",
        "price_set_hash": f"{100 + price_key:032x}",
        "rate_pack_hash": f"{200 + price_key:032x}",
        "_ptg_price_key": price_key,
        "prices": [{"negotiated_rate": rate}],
        "price_summary": [],
    }


def test_provider_rate_merge_keeps_the_lowest_dense_price_key():
    """Merge equivalent rows and retain their earliest dense price identity."""

    merged = serving._merge_ptg2_provider_rate_items(
        [
            _mergeable_provider_rate(price_key=9, rate="125.00"),
            _mergeable_provider_rate(price_key=3, rate="115.00"),
        ]
    )

    assert len(merged) == 1
    assert merged[0]["_ptg_price_key"] == 3
    assert {price["negotiated_rate"] for price in merged[0]["prices"]} == {
        115,
        125,
    }


def test_invalid_multi_network_concurrency_uses_the_bounded_default(monkeypatch):
    """Use the bounded default when deployment configuration is malformed."""

    monkeypatch.setenv(serving._PTG2_MULTI_NETWORK_CONCURRENCY_ENV, "invalid")
    assert serving._ptg2_multi_network_concurrency() == (
        serving._PTG2_MULTI_NETWORK_CONCURRENCY_DEFAULT
    )


def test_multi_network_shaping_distinguishes_no_response_from_empty_match():
    """Return unavailable for no responses and exact empty for a real network response."""

    pagination = SimpleNamespace(limit=10, offset=0)
    network_snapshots = [("network-a", "snapshot-a")]
    assert serving._shape_multi_provider_procedure_response(
        [("network-a", "snapshot-a", None)],
        network_snapshots,
        {},
        pagination,
    ) is None

    response = serving._shape_multi_provider_procedure_response(
        [
            (
                "network-a",
                "snapshot-a",
                {
                    "items": [],
                    "pagination": {
                        "total": 0,
                        "total_lower_bound": 0,
                        "total_is_exact": True,
                    },
                    "query": {"plan_id": "synthetic-plan"},
                },
            )
        ],
        network_snapshots,
        {},
        pagination,
    )

    assert response["items"] == []
    assert response["query"]["combined"] is True
    assert response["query"]["networks"] == []
