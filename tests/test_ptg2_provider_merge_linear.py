# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Linear deduplication contracts for provider-rate aggregation."""

from copy import deepcopy
from unittest.mock import Mock

from api import ptg2_serving as serving


def _provider_ref(index: int) -> str:
    return f"{index % 20 + 1:032x}"


def _price_ref(index: int) -> str:
    return f"{index % 40 + 101:032x}"


def _rate_ref(index: int) -> str:
    rate_index = (index % 30 + index // 120) % 30
    return f"{rate_index + 201:032x}"


def _rate_item(index: int) -> dict[str, object]:
    identity_index = index if index == 50 else index % 50
    return {
        "npi": 1234567890,
        "location_hash": "synthetic-location",
        "reported_code_system": "CPT",
        "reported_code": "00001",
        "negotiation_arrangement": "FFS",
        "source_artifact_key": 7,
        "price_set_hash": _price_ref(index),
        "price_set_hashes": [_price_ref(index)],
        "rate_pack_hash": _rate_ref(index),
        "provider_set_hash": _provider_ref(index),
        "source_trace": [
            {
                "source_file_version_id": f"source-{index % 50}",
                "identity_sha256": f"{identity_index:064x}",
            }
        ],
        "_ptg_price_key": 200 - index,
        "prices": [{"negotiated_rate": index}],
        "price_summary": [],
    }


def test_provider_rate_merge_deduplicates_in_linear_first_seen_order(monkeypatch):
    """Reuse per-group seen sets without dropping negotiated-price occurrences."""

    original_key = serving._price_row_key
    counted_key = Mock(wraps=original_key)
    monkeypatch.setattr(serving, "_price_row_key", counted_key)
    rate_items = [_rate_item(index) for index in range(200)]
    original_rate_items = deepcopy(rate_items)
    merged = serving._merge_ptg2_provider_rate_items(rate_items)

    assert len(merged) == 1
    assert counted_key.call_count <= 450
    assert merged[0]["source_trace"] == [
        {
            "source_file_version_id": f"source-{index}",
            "identity_sha256": f"{index:064x}",
        }
        for index in range(50)
    ] + [
        {
            "source_file_version_id": "source-0",
            "identity_sha256": f"{50:064x}",
        }
    ]
    assert merged[0]["price_set_hashes"] == [
        f"{index + 101:032x}" for index in range(40)
    ]
    assert merged[0]["rate_pack_hashes"] == [
        f"{index + 201:032x}" for index in range(30)
    ]
    assert merged[0]["provider_set_hashes"] == [
        f"{index + 1:032x}" for index in range(20)
    ]
    assert merged[0]["rate_option_count"] == 200
    assert merged[0]["provider_set_count"] == 20
    assert merged[0]["price_set_count"] == 40
    assert merged[0]["rate_pack_count"] == 30
    assert [
        (
            option["provider_set_ref"],
            option["price_set_ref"],
            option["rate_pack_ref"],
            option["prices"][0]["negotiated_rate"],
        )
        for option in merged[0]["rate_options"]
    ] == [
        (
            _provider_ref(index),
            _price_ref(index),
            _rate_ref(index),
            index,
        )
        for index in range(200)
    ]
    assert merged[0]["_ptg_price_key"] == 1
    assert [price["negotiated_rate"] for price in merged[0]["prices"]] == list(
        range(200)
    )
    assert rate_items == original_rate_items
