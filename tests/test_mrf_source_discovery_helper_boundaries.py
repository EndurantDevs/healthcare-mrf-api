# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import re

import pytest

discovery = importlib.import_module("process.mrf_source_discovery")


def test_sockjs_url_uses_transport_scheme_without_an_event_loop():
    websocket_url = discovery._mymedicalshopper_sockjs_ws_url(
        "https://catalog.example.test/search"
    )

    assert re.fullmatch(
        r"wss://catalog\.example\.test/sockjs/\d{3}/[0-9a-f]{8}/websocket",
        websocket_url,
    )


@pytest.mark.asyncio
async def test_sockjs_url_uses_the_current_task_for_plain_http():
    websocket_url = discovery._mymedicalshopper_sockjs_ws_url(
        "http://catalog.example.test/search"
    )

    assert websocket_url.startswith("ws://catalog.example.test/sockjs/")
    assert websocket_url.endswith("/websocket")


def test_sapphire_hash_parser_accepts_nested_and_embedded_payloads():
    nested_payload = '{"result":{"staticQueryHashes":["nested","nested","next"]}}'
    embedded_payload = (
        'not-json "staticQueryHashes": [" embedded ", "embedded", "other"]'
    )

    assert discovery._sapphire_static_query_hashes(nested_payload) == [
        "nested",
        "next",
    ]
    assert discovery._sapphire_static_query_hashes(embedded_payload) == [
        "embedded",
        "other",
    ]
    assert discovery._sapphire_static_query_hashes("not-json") == []


@pytest.mark.parametrize(
    ("source_row", "expected_file_name"),
    [
        (
            {
                "link": (
                    "https://files.example.test/download"
                    "?fileName=allowed-amounts.json.gz"
                )
            },
            "allowed-amounts.json.gz",
        ),
        (
            ["https://files.example.test/rates/in-network.zip"],
            "in-network.zip",
        ),
        ("artifact.csv.gz", "artifact.csv.gz"),
        ("no importable artifact", None),
    ],
)
def test_humana_file_name_parser_covers_supported_row_shapes(
    source_row,
    expected_file_name,
):
    assert discovery._humana_pct_file_name_from_row(source_row) == expected_file_name


@pytest.mark.parametrize(
    ("url", "resolver", "expected_brands"),
    [
        (
            "https://configured.example.test/search",
            {"default_brands_by_host": {"configured.example.test": "brand-a"}},
            ["brand-a"],
        ),
        (
            "https://www.configured.example.test/search",
            {
                "default_brands_by_host": {
                    "configured.example.test": ["brand-a", "brand-a", "brand-b"]
                }
            },
            ["brand-a", "brand-b"],
        ),
        ("https://amerihealthnj.com/search", {}, ["ahnj", "ahnjhmo"]),
        ("https://amerihealth.com/search", {}, ["ahpa"]),
        ("https://ibx.com/search", {}, ["qcc"]),
        ("https://unknown.example.test/search", {}, []),
    ],
)
def test_cmstic_brand_candidates_cover_configured_and_known_hosts(
    url,
    resolver,
    expected_brands,
):
    assert discovery._cmstic_brand_candidates_from_url(url, resolver) == expected_brands


def test_anthem_employer_queries_are_ordered_and_case_insensitive():
    source_by_field = {
        "metadata_json": {
            "target_payer_query": "Example Holdings LLC",
            "query_context_employer_name": "example holdings llc",
            "query_context_employer_aliases": [
                "Example Employer",
                "example employer",
                "",
            ],
        }
    }

    assert discovery._anthem_s3_employer_name_queries(source_by_field) == (
        "Example Holdings LLC",
        "Example Employer",
    )


@pytest.mark.parametrize(
    ("url", "expected_format"),
    [
        ("https://files.example.test/rates.csv", "csv"),
        ("https://files.example.test/rates.7z", "7z"),
        ("https://files.example.test/rates.zip", "zip"),
        ("https://mrf.healthcarebluebook.com/12345", "zip"),
        ("https://files.example.test/rates.json.gz", "gzip"),
        ("https://files.example.test/rates.json", "json"),
        ("https://files.example.test/rates.txt", None),
    ],
)
def test_healthcarebluebook_source_format_classifies_supported_containers(
    url,
    expected_format,
):
    assert discovery._healthcarebluebook_source_format(url) == expected_format


def test_triples_period_parser_ignores_malformed_rows_and_selects_latest():
    malformed_payload_by_field = {
        "selects": {
            "year": [None, {"year": "bad"}],
            "month": ["bad", {"month": ""}],
        },
        "list": ["bad", {"year": "bad", "month": "13"}],
    }
    valid_payload_by_field = {
        "selects": {
            "year": [{"year": "2025"}],
            "month": [{"month": "11"}],
        },
        "list": [
            {"year": "2026", "month": "7"},
            None,
            {"year": "invalid", "month": "8"},
        ],
    }

    assert discovery._triples_mtt_latest_year_month(None) == (None, None)
    assert discovery._triples_mtt_latest_year_month(malformed_payload_by_field) == (
        None,
        None,
    )
    assert discovery._triples_mtt_latest_year_month(valid_payload_by_field) == (
        "2026",
        "11",
    )


def test_triples_latest_entries_preserve_fallback_and_filter_latest_period():
    file_entries = [
        {"url": "https://files.example.test/old.json", "year": 2025, "month": 12},
        {"url": "https://files.example.test/new.json", "year": 2026, "month": 7},
        {"year": 2027, "month": 1},
    ]

    assert (
        discovery._latest_triples_file_entries(
            file_entries,
            latest_month_only=False,
        )
        is file_entries
    )
    assert discovery._latest_triples_file_entries(
        [{"year": 2026, "month": 7}],
        latest_month_only=True,
    ) == [{"year": 2026, "month": 7}]
    assert discovery._latest_triples_file_entries(
        file_entries,
        latest_month_only=True,
    ) == [file_entries[1]]


def test_triples_target_requires_an_importable_file_reference():
    source_by_field = {"source_id": "mrfsource_test"}
    assert (
        discovery._triples_mtt_crawl_target(
            source_by_field,
            {"marketing": "Missing URL"},
            resolved_from_url="https://catalog.example.test",
            resolver_type="triples_mtt",
        )
        is None
    )

    crawl_target = discovery._triples_mtt_crawl_target(
        source_by_field,
        {
            "id": "file-1",
            "url": "https://files.example.test/in-network-rates.json.gz",
            "plan": "Example Plan",
            "marketing": "Example Network",
            "network": "Network A",
            "year": "2026",
            "month": "07",
        },
        resolved_from_url="https://catalog.example.test",
        resolver_type="triples_mtt",
    )

    assert crawl_target is not None
    assert crawl_target.metadata["target_kind"] == "file_reference"
    assert crawl_target.metadata["target_file_type"] == "in-network"
    assert crawl_target.metadata["plan_info"][0]["plan_name"] == (
        "Example Plan - Example Network"
    )
