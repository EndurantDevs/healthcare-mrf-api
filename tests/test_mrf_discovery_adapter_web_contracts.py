# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from process import mrf_source_discovery as discovery


def _webtpa_contract_payloads(base_url):
    return {
        f"{base_url}plans": [
            "ignored",
            {"name": "Missing Identifier"},
            {"mrfBenefitplanId": "plan 1", "benefitplanNm": "Synthetic Plan"},
        ],
        f"{base_url}plans/plan%201/in-network": [
            "ignored",
            {
                "mrfInNetworkRatesId": "direct",
                "fileName": "direct.json.gz",
                "location": "https://files.example.invalid/direct.json.gz",
            },
            {
                "mrfInNetworkRatesId": "file 2",
                "fileName": "located.json.gz",
            },
            {
                "mrfInNetworkRatesId": "file-error",
                "fileName": "unavailable.json.gz",
            },
            {"location": "ftp://files.example.invalid/ignored.json"},
        ],
        f"{base_url}plans/plan%201/allowed-amounts": [
            {
                "mrfAllowedAmountsId": "allowed",
                "name": "allowed.json",
                "url": "https://files.example.invalid/allowed.json",
            },
            {"mrfAllowedAmountsId": "missing-location"},
        ],
    }


@pytest.mark.asyncio
async def test_web_api_resolver_preserves_direct_and_location_lookup_targets(
    monkeypatch,
):
    """Resolve direct and indirection records without losing plan provenance."""

    base_url = "https://adapter.example.invalid/"
    payloads_by_url = _webtpa_contract_payloads(base_url)

    async def fetch_value(url, **_kwargs):
        return payloads_by_url[url]

    async def fetch_location(url, **_kwargs):
        if url.endswith("file%202/location"):
            return {"location": "https://files.example.invalid/located.json.gz"}
        raise OSError("synthetic location failure")

    monkeypatch.setattr(discovery, "_fetch_json_value", fetch_value)
    monkeypatch.setattr(discovery, "_fetch_json", fetch_location)

    crawl_targets = await discovery._resolve_webtpa_mrf_api(
        {
            "source_id": "mrfsource_adapter_contract",
            "display_name": "Synthetic Source",
        },
        f"{base_url}landing",
        {
            "plans_path": "/plans",
            "in_network_path_template": "/plans/{plan_id}/in-network",
            "in_network_location_path_template": "/files/{file_id}/location",
            "allowed_amounts_path_template": "/plans/{plan_id}/allowed-amounts",
            "max_plans": 4,
            "max_targets": 10,
        },
        object(),
    )

    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://files.example.invalid/direct.json.gz",
        "https://files.example.invalid/located.json.gz",
        "https://files.example.invalid/allowed.json",
    ]
    assert [crawl_target.metadata["target_file_type"] for crawl_target in crawl_targets] == [
        "in-network",
        "in-network",
        "allowed-amounts",
    ]
    assert crawl_targets[1].resolved_from_url.endswith("file%202/location")
    assert crawl_targets[0].metadata["plan_info"] == [
        {
            "plan_id": "plan 1",
            "plan_id_type": "webtpa_mrf_benefitplan_id",
            "plan_name": "Synthetic Plan",
        }
    ]


@pytest.mark.parametrize(
    ("plans_payload", "list_payload", "max_targets", "expected_message"),
    [
        ({"not": "a list"}, [], None, "plans endpoint did not return a list"),
        ([{"id": "plan"}], {"not": "a list"}, None, "no WebTPA MRF file targets"),
        ([{"id": "plan"}], [], None, "no WebTPA MRF file targets"),
    ],
)
@pytest.mark.asyncio
async def test_web_api_resolver_rejects_malformed_or_empty_contracts(
    monkeypatch,
    plans_payload,
    list_payload,
    max_targets,
    expected_message,
):
    response_payloads = iter([plans_payload, list_payload, list_payload])

    async def fetch_value(_url, **_kwargs):
        return next(response_payloads)

    monkeypatch.setattr(discovery, "_fetch_json_value", fetch_value)

    with pytest.raises(ValueError, match=expected_message):
        await discovery._resolve_webtpa_mrf_api(
            {"display_name": "Synthetic Source"},
            "https://adapter.example.invalid/landing",
            {"plans_path": "/plans", "max_targets": max_targets},
            object(),
        )


@pytest.mark.asyncio
async def test_web_api_resolver_stops_at_configured_target_limit(monkeypatch):
    response_payloads = iter(
        [
            [{"id": "plan", "name": "Synthetic Plan"}],
            [
                {
                    "id": "first",
                    "name": "first.json",
                    "location": "https://files.example.invalid/first.json",
                },
                {
                    "id": "second",
                    "name": "second.json",
                    "location": "https://files.example.invalid/second.json",
                },
            ],
        ]
    )

    async def fetch_value(_url, **_kwargs):
        return next(response_payloads)

    monkeypatch.setattr(discovery, "_fetch_json_value", fetch_value)

    crawl_targets = await discovery._resolve_webtpa_mrf_api(
        {"display_name": "Synthetic Source"},
        "https://adapter.example.invalid/landing",
        {"plans_path": "/plans", "max_targets": 1},
        object(),
    )

    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://files.example.invalid/first.json"
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ({"mrfBenefitplanId": 0}, "0"),
        ({"benefitplanId": "benefit"}, "benefit"),
        ({"planId": "plan"}, "plan"),
        ({"id": "fallback"}, "fallback"),
        ({}, None),
    ],
)
def test_web_api_plan_identifier_precedence(value, expected):
    assert discovery._webtpa_plan_id(value) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://adapter.example.invalid/path", "https://adapter.example.invalid/"),
        ("not-a-url", None),
        ("", None),
    ],
)
def test_web_api_base_url_requires_absolute_http_context(url, expected):
    if expected is None:
        with pytest.raises(ValueError, match="unsupported"):
            discovery._webtpa_api_base_url(url)
    else:
        assert discovery._webtpa_api_base_url(url) == expected


def _auxiant_contract_fixture():
    external_url = "https://external.example.invalid/landing"
    return {
        "source": {
            "source_id": "mrfsource_directory_contract",
            "display_name": "Synthetic Source",
        },
        "directory_url": "https://directory.example.invalid/networks",
        "page_url": "https://directory.example.invalid/network-a",
        "external_url": external_url,
        "directory_link": {
            "target_kind": "file_reference",
            "target_file_type": "allowed-amounts",
            "container_format": "json",
            "url": "https://files.example.invalid/historical.json",
            "label": "Historical",
        },
        "page_link": {
            "target_kind": "file_reference",
            "target_file_type": "in-network",
            "container_format": "gzip",
            "url": "https://files.example.invalid/current.json.gz",
            "label": "Current",
        },
        "external_link": {
            "target_kind": "source_landing_page",
            "url": external_url,
            "label": "External",
        },
    }


def _install_auxiant_contract_mocks(monkeypatch, fixture):
    async def fetch_text(url, **_kwargs):
        return "directory" if url == fixture["directory_url"] else "network-page"

    def parse_links(text, **_kwargs):
        if text == "directory":
            return [fixture["directory_link"]]
        return [
            fixture["page_link"],
            fixture["external_link"],
            {"target_kind": "source_landing_page", "url": ""},
        ]

    nested_target = discovery.CrawlTarget(
        source={"source_id": "nested"},
        url="https://files.example.invalid/nested.json",
        label="Nested label",
        resolved_from_url=fixture["external_url"],
        metadata={"resolver": "nested-adapter"},
    )
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    monkeypatch.setattr(
        discovery,
        "_parse_auxiant_directory_networks",
        lambda *_args, **_kwargs: [
            {"url": fixture["page_url"], "label": "Synthetic Network"},
            {"url": "https://directory.example.invalid/ignored", "label": "Ignored"},
        ],
    )
    monkeypatch.setattr(discovery, "_parse_auxiant_page_links", parse_links)
    monkeypatch.setattr(
        discovery,
        "classify_hosting_platform",
        lambda _url: "nested-platform",
    )
    monkeypatch.setattr(
        discovery,
        "_crawl_targets_for_source",
        AsyncMock(return_value=[nested_target]),
    )


@pytest.mark.asyncio
async def test_directory_resolver_combines_direct_nested_and_landing_targets(
    monkeypatch,
):
    """Keep direct downloads and nested adapter targets in directory order."""

    fixture = _auxiant_contract_fixture()
    _install_auxiant_contract_mocks(monkeypatch, fixture)

    crawl_targets = await discovery._resolve_auxiant_wordpress_directory(
        fixture["source"],
        fixture["directory_url"],
        {
            "type": "auxiant_wordpress_directory",
            "directory_path": "/networks",
            "max_networks": 1,
            "data_available_only": False,
        },
        object(),
    )

    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://files.example.invalid/historical.json",
        "https://files.example.invalid/current.json.gz",
        "https://files.example.invalid/nested.json",
    ]
    assert crawl_targets[0].metadata["auxiant_network_name"] == (
        "Historical Out of Network Allowed Amounts"
    )
    assert crawl_targets[2].metadata["nested_resolver"] == "nested-adapter"
    assert crawl_targets[2].metadata["external_hosting_platform"] == "nested-platform"


@pytest.mark.parametrize(("nested_failure", "has_page_links"), [(True, True), (False, False)])
@pytest.mark.asyncio
async def test_directory_resolver_retains_actionable_landing_when_files_are_absent(
    monkeypatch,
    nested_failure,
    has_page_links,
):
    directory_url = "https://directory.example.invalid/networks"
    page_url = "https://directory.example.invalid/network-a"
    external_url = "https://external.example.invalid/landing"
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value="html"))
    monkeypatch.setattr(
        discovery,
        "_parse_auxiant_directory_networks",
        lambda *_args, **_kwargs: [{"url": page_url, "label": "Synthetic Network"}],
    )

    def page_links(_text, *, base_url):
        if base_url == directory_url or not has_page_links:
            return []
        return [{"target_kind": "source_landing_page", "url": external_url, "label": "External"}]

    monkeypatch.setattr(discovery, "_parse_auxiant_page_links", page_links)
    monkeypatch.setattr(discovery, "classify_hosting_platform", lambda _url: None)
    if nested_failure:
        monkeypatch.setattr(
            discovery,
            "_crawl_targets_for_source",
            AsyncMock(side_effect=ValueError("synthetic nested failure")),
        )

    crawl_targets = await discovery._resolve_auxiant_wordpress_directory(
        {"source_id": "mrfsource_directory_contract", "display_name": "Synthetic Source"},
        directory_url,
        {"directory_path": "/networks"},
        object(),
    )

    assert len(crawl_targets) == 1
    assert crawl_targets[0].metadata["target_kind"] == "source_landing_page"
    if has_page_links:
        assert crawl_targets[0].url == external_url
        assert "synthetic nested failure" in crawl_targets[0].metadata["nested_error"]
    else:
        assert crawl_targets[0].url == page_url
        assert crawl_targets[0].metadata["landing_reason"] == "auxiant_page_without_file_links"


@pytest.mark.asyncio
async def test_directory_resolver_rejects_directory_without_networks_or_files(
    monkeypatch,
):
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value="empty"))
    monkeypatch.setattr(discovery, "_parse_auxiant_directory_networks", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(discovery, "_parse_auxiant_page_links", lambda *_args, **_kwargs: [])

    with pytest.raises(ValueError, match="no Auxiant network MRF links"):
        await discovery._resolve_auxiant_wordpress_directory(
            {"display_name": "Synthetic Source"},
            "https://directory.example.invalid/networks",
            {"directory_path": "/networks"},
            object(),
        )


@pytest.mark.asyncio
async def test_directory_resolver_ignores_unused_page_limit(monkeypatch):
    directory_url = "https://directory.example.invalid/networks"
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value="directory"))
    monkeypatch.setattr(
        discovery, "_parse_auxiant_directory_networks", lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr(
        discovery,
        "_parse_auxiant_page_links",
        lambda *_args, **_kwargs: [
            {
                "target_kind": "file_reference",
                "url": "https://files.example.invalid/historical.json",
                "label": "Historical",
            }
        ],
    )

    [target] = await discovery._resolve_auxiant_wordpress_directory(
        {"display_name": "Synthetic Source"},
        directory_url,
        {"directory_path": "/networks", "page_max_bytes": "invalid"},
        object(),
    )

    assert target.url == "https://files.example.invalid/historical.json"
