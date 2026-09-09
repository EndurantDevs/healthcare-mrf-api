# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed TOC classification margin for release-review evidence."""

import pytest

from process.ptg_parts import toc_entries
from process.ptg_parts.domain import (
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2_DOMAIN_DRUG,
    PTG2_DOMAIN_IN_NETWORK,
)
from process.ptg_parts.source_jobs import parse_toc_catalog_entries


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


@pytest.mark.parametrize(
    "file_name",
    (
        "synthetic-second-choice-rates.zip",
        "SyntheticNonRLRX-rates.zip",
        "synthetic-balloon-rates.zip",
    ),
)
def test_toc_short_acronyms_do_not_match_inside_words(file_name: str) -> None:
    assert toc_entries._toc_body_source_type(
        "in-network",
        f"https://payer.example/{file_name}",
    ) == ("in-network", PTG2_DOMAIN_IN_NETWORK)


@pytest.mark.parametrize(
    "url_template",
    (
        "https://payer.example/synthetic-{acronym}-rates.zip",
        "https://payer.example/synthetic-{acronym}2026-rates.zip",
        "https://payer.example/download?kind={acronym}",
    ),
)
@pytest.mark.parametrize(
    "acronym, expected_source_type, expected_domain",
    (
        ("rx", "payer-drug", PTG2_DOMAIN_DRUG),
        ("NDC", "payer-drug", PTG2_DOMAIN_DRUG),
        ("OON", "allowed-amounts", PTG2_DOMAIN_ALLOWED_AMOUNT),
    ),
)
def test_toc_short_acronyms_keep_explicit_boundaries(
    url_template: str,
    acronym: str,
    expected_source_type: str,
    expected_domain: str,
) -> None:
    url = url_template.format(acronym=acronym)
    assert toc_entries._toc_body_source_type("in-network", url) == (
        expected_source_type,
        expected_domain,
    )


def test_toc_parser_keeps_incidental_acronym_files_in_network() -> None:
    body_file_names = (
        "synthetic-second-choice-rates.zip",
        "SyntheticNonRLRX-rates.zip",
        "synthetic-balloon-rates.zip",
        "synthetic-allowed-amounts.json",
        "synthetic-pharmacy-rates.json",
    )
    catalog_entries = parse_toc_catalog_entries(
        {
            "reporting_entity_name": "Synthetic Payer",
            "reporting_entity_type": "health insurance issuer",
            "reporting_structure": [
                {
                    "reporting_plans": [
                        {
                            "plan_name": "Synthetic Plan",
                            "plan_id": "synthetic-plan",
                            "plan_market_type": "group",
                        }
                    ],
                    "in_network_files": [
                        {"location": f"https://payer.example/{file_name}"}
                        for file_name in body_file_names
                    ],
                }
            ],
        },
        "https://payer.example/synthetic-index.json",
    )

    assert [entry.source_type for entry in catalog_entries] == [
        "table-of-contents",
        "in-network",
        "in-network",
        "in-network",
        "allowed-amounts",
        "payer-drug",
    ]


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
