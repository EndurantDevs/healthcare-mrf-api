# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed TOC classification margin for release-review evidence."""

from process.ptg_parts import toc_entries
from process.ptg_parts.domain import (
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2_DOMAIN_DRUG,
    PTG2_DOMAIN_IN_NETWORK,
)


def test_toc_query_suffix_and_source_fallbacks_are_deterministic() -> None:
    assert toc_entries._is_toc_body_file_location(
        "https://payer.example/download?file=network.json"
    ) is True
    assert toc_entries._toc_body_source_type(
        "in-network",
        "https://payer.example/pharmacy-rx-feed",
    ) == ("payer-drug", PTG2_DOMAIN_DRUG)
    assert toc_entries._toc_body_source_type(
        "allowed-amounts",
        "https://payer.example/opaque-feed",
    ) == ("allowed-amounts", PTG2_DOMAIN_ALLOWED_AMOUNT)
    assert toc_entries._toc_body_source_type(
        "payer-drug",
        "https://payer.example/opaque-feed",
    ) == ("payer-drug", PTG2_DOMAIN_DRUG)
    assert toc_entries._toc_body_source_type(
        "in-network",
        "https://payer.example/in-network.json.gz?Signature=random-rx-bytes",
    ) == ("in-network", PTG2_DOMAIN_IN_NETWORK)
    assert toc_entries._toc_body_source_type(
        "in-network",
        "https://payer.example/in-network.json.gz?Signature=random-oon-bytes",
    ) == ("in-network", PTG2_DOMAIN_IN_NETWORK)
    assert toc_entries._toc_body_source_type(
        "in-network",
        "https://payer.example/download?file=allowed-amounts.json",
    ) == ("allowed-amounts", PTG2_DOMAIN_ALLOWED_AMOUNT)


def test_flat_toc_ignores_invalid_items_and_keeps_valid_files() -> None:
    metadata = {
        "reporting_entity_name": "Synthetic Payer",
        "reporting_entity_type": "health insurance issuer",
    }
    assert toc_entries._build_flat_toc_catalog_entry(
        {"location": "not-a-url"},
        ("in-network", "in-network"),
        "https://payer.example/12-3456789.json",
        metadata,
        (),
    ) is None

    catalog_entries = toc_entries.flat_toc_catalog_entries(
        {
            "in-network files": [
                "not-an-object",
                {"location": "https://payer.example/network.json"},
            ],
            "ignored": {"location": "https://payer.example/ignored.json"},
        },
        "https://payer.example/12-3456789.json",
        metadata,
    )

    assert len(catalog_entries) == 1
    assert catalog_entries[0].plan_info[0]["plan_id"] == "123456789"
