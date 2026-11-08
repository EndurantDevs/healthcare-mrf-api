# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import re
from unittest.mock import AsyncMock, call

import pytest

discovery = importlib.import_module("process.mrf_source_discovery")


@pytest.mark.parametrize(
    ("url", "message"),
    [
        (None, "unsupported GitHub MRF URL"),
        ("https://example.test/owner/repo", "unsupported GitHub MRF URL"),
        ("https://github.com/owner", "must include owner and repo"),
    ],
)
def test_github_repository_url_rejects_missing_identity(url, message):
    with pytest.raises(ValueError, match=message):
        discovery._github_repo_parts_from_url(url)


@pytest.mark.parametrize(
    ("suffix", "branch", "prefix"),
    [
        ("", None, None),
        ("/tree/release", "release", None),
        ("/blob/release/rates/nested", "release", "rates/nested"),
    ],
)
def test_github_repository_url_preserves_branch_and_path(suffix, branch, prefix):
    assert discovery._github_repo_parts_from_url(f" https://GITHUB.COM/example/mrf-fixture{suffix} ") == (
        "example",
        "mrf-fixture",
        branch,
        prefix,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("max_targets", [None, 1])
async def test_github_tree_filters_paths_before_limiting_targets(monkeypatch, max_targets):
    tree_entries = [
        None,
        {"type": "blob"},
        {"type": "blob", "path": "rates-other/allowed-amounts.json"},
        {"type": "tree", "path": "rates/nested"},
        {"type": "blob", "path": "rates"},
        {"type": "blob", "path": "rates/README.md"},
        {"type": "blob", "path": "rates/plan_index.json", "sha": "toc", "size": 12},
        {"type": "blob", "path": "rates/plan_in-network.json.gz", "sha": "rates"},
    ]
    fetch_json = AsyncMock(return_value={"tree": tree_entries})
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)
    session = object()
    tree_url = "https://api.github.com/repos/example/mrf-fixture/git/trees/release?recursive=1"
    crawl_targets = await discovery._resolve_github_repo_mrf(
        {"source_id": "source_fixture"},
        "https://github.com/example/mrf-fixture/tree/release/rates",
        {"tree_max_bytes": 8192, "max_targets": max_targets},
        session,
    )

    expected_paths = ["rates/plan_index.json", "rates/plan_in-network.json.gz"]
    assert [crawl_target.metadata["github_path"] for crawl_target in crawl_targets] == expected_paths[:max_targets]
    assert crawl_targets[0].resolved_from_url == tree_url
    assert crawl_targets[0].metadata["target_kind"] == "toc_json"
    assert crawl_targets[0].metadata["target_file_type"] == "table-of-contents"
    assert crawl_targets[0].metadata["github_sha"] == "toc"
    assert crawl_targets[0].metadata["blob_size"] == 12
    fetch_json.assert_awaited_once_with(tree_url, max_bytes=8192, session=session)


def test_github_tree_ignores_an_empty_blob_path():
    assert (
        discovery._github_tree_mrf_target(
            {"source_id": "source_fixture"},
            owner="example",
            repo="mrf-fixture",
            branch="main",
            tree_entry={"type": "blob", "path": " "},
            tree_url="https://api.github.com/repos/example/mrf-fixture/git/trees/main",
            resolver_type="github_repo_mrf_tree",
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("tree", [None, {}, [], [{"type": "blob", "path": "README.md"}]])
async def test_github_tree_rejects_missing_file_lists_and_empty_results(monkeypatch, tree):
    fetch_json = AsyncMock(side_effect=[{"default_branch": " "}, {"tree": tree}])
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)
    session = object()
    message = "no GitHub MRF files" if isinstance(tree, list) else "did not include file list"

    with pytest.raises(ValueError, match=message):
        await discovery._resolve_github_repo_mrf(
            {},
            "https://github.com/example/mrf-fixture",
            {"repo_max_bytes": 2048, "tree_max_bytes": 4096},
            session,
        )

    assert fetch_json.await_args_list == [
        call("https://api.github.com/repos/example/mrf-fixture", max_bytes=2048, session=session),
        call(
            "https://api.github.com/repos/example/mrf-fixture/git/trees/main?recursive=1",
            max_bytes=4096,
            session=session,
        ),
    ]


def test_sockjs_url_uses_transport_scheme_without_an_event_loop():
    websocket_url = discovery._mymedicalshopper_sockjs_ws_url("https://catalog.example.test/search")

    assert re.fullmatch(
        r"wss://catalog\.example\.test/sockjs/\d{3}/[0-9a-f]{8}/websocket",
        websocket_url,
    )


@pytest.mark.asyncio
async def test_sockjs_url_uses_the_current_task_for_plain_http():
    websocket_url = discovery._mymedicalshopper_sockjs_ws_url("http://catalog.example.test/search")

    assert websocket_url.startswith("ws://catalog.example.test/sockjs/")
    assert websocket_url.endswith("/websocket")


def test_sapphire_hash_parser_accepts_nested_and_embedded_payloads():
    nested_payload = '{"result":{"staticQueryHashes":["nested","nested","next"]}}'
    embedded_payload = 'not-json "staticQueryHashes": [" embedded ", "embedded", "other"]'

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
            {"link": ("https://files.example.test/download?fileName=allowed-amounts.json.gz")},
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
            {"default_brands_by_host": {"configured.example.test": ["brand-a", "brand-a", "brand-b"]}},
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
    assert crawl_target.metadata["plan_info"][0]["plan_name"] == ("Example Plan - Example Network")
