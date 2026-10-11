# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from unittest.mock import AsyncMock, call

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

    assert [crawl_target.url for crawl_target in crawl_targets] == ["https://files.example.invalid/first.json"]


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
    assert crawl_targets[0].metadata["auxiant_network_name"] == ("Historical Out of Network Allowed Amounts")
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
    monkeypatch.setattr(discovery, "_parse_auxiant_directory_networks", lambda *_args, **_kwargs: [])
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


@pytest.mark.parametrize("known_file", [True, False])
@pytest.mark.asyncio
async def test_viva_direct_file_selection_does_not_fetch_employer_pages(monkeypatch, known_file):
    fetch_text = AsyncMock(side_effect=AssertionError("unexpected page request"))
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    file_info = discovery._VIVA_HEALTH_COMMERCIAL_MRF_FILES[0]
    url = file_info["url"] if known_file else "https://adapter.example.invalid/files/mrf/unknown"

    if known_file:
        [target] = await discovery._resolve_viva_health_mrf({}, url, {}, object())
        assert target.url == file_info["url"]
        assert target.resolved_from_url == url
        assert target.metadata["target_file_type"] == "in-network"
        assert target.metadata["content_disposition_filename"] == (file_info["content_disposition_filename"])
    else:
        with pytest.raises(ValueError, match="no VIVA Health commercial MRF target"):
            await discovery._resolve_viva_health_mrf({}, url, {}, object())
    fetch_text.assert_not_awaited()


@pytest.mark.parametrize("include_commercial", [True, False])
@pytest.mark.asyncio
async def test_viva_employer_fetch_failure_preserves_only_available_static_files(monkeypatch, include_commercial):
    fetch_text = AsyncMock(side_effect=OSError("synthetic employer page failure"))
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    session = object()
    resolver_by_field = {"include_commercial_static": include_commercial, "max_bytes": 4096}
    url = "https://adapter.example.invalid/mrf/"

    if include_commercial:
        targets = await discovery._resolve_viva_health_mrf({}, url, resolver_by_field, session)
        assert [target.url for target in targets] == [
            file_info["url"] for file_info in discovery._VIVA_HEALTH_COMMERCIAL_MRF_FILES
        ]
    else:
        with pytest.raises(ValueError, match="no VIVA Health MRF targets"):
            await discovery._resolve_viva_health_mrf({}, url, resolver_by_field, session)
    fetch_text.assert_awaited_once_with(
        "https://adapter.example.invalid/mrf/employers/", max_bytes=4096, session=session
    )


@pytest.mark.parametrize(("max_links", "max_targets", "expected_count"), [(1, 3, 1), (3, 1, 1), (3, 3, 2)])
@pytest.mark.asyncio
async def test_viva_employer_html_filters_duplicates_and_respects_both_limits(
    monkeypatch, max_links, max_targets, expected_count
):
    employer_urls = [
        "https://www.mymedicalshopper.com/mrf/synthetic-first-viva-health-x00001",
        "https://www.mymedicalshopper.com/mrf/synthetic-second-viva-health-x00002",
    ]
    page_html = (
        f'<a href="{employer_urls[0]}">First</a>'
        f'<a href="{employer_urls[0]}">Duplicate</a>'
        '<a href="https://unrecognized.example.invalid/mrf/">Unsupported</a>'
        '<a href="https://transparency.auxiant.com/directory-of-data-sources/">Other adapter</a>'
        f'<a href="{employer_urls[1]}">Second</a>'
    )
    fetch_text = AsyncMock(return_value=page_html)
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)

    employer_targets = await discovery._resolve_viva_health_mrf(
        {"source_id": "mrfsource_synthetic"},
        "https://adapter.example.invalid/mrf/",
        {
            "include_commercial_static": False,
            "max_employer_links": max_links,
            "max_targets": max_targets,
        },
        object(),
    )

    assert [employer_target.url for employer_target in employer_targets] == employer_urls[:expected_count]
    assert employer_targets[0].metadata["group_number"] == "x00001"
    assert employer_targets[0].metadata["plan_info"][0]["plan_id"] == "x00001"
    assert all(employer_target.metadata["target_kind"] == "source_landing_page" for employer_target in employer_targets)
    fetch_text.assert_awaited_once()


@pytest.mark.asyncio
async def test_viva_disabled_sources_fail_without_network_requests(monkeypatch):
    fetch_text = AsyncMock(side_effect=AssertionError("unexpected page request"))
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)

    with pytest.raises(ValueError, match="no VIVA Health MRF targets"):
        await discovery._resolve_viva_health_mrf(
            {},
            "https://adapter.example.invalid/mrf/",
            {"include_commercial_static": False, "include_employer_mrf_links": False},
            object(),
        )
    fetch_text.assert_not_awaited()


@pytest.mark.parametrize(
    ("url", "group_numbers"),
    [
        ("https://adapter.example.invalid/MRF?groupNumber=12", []),
        ("https://adapter.example.invalid/MRF", ["1234", "12x4"]),
        ("https://adapter.example.invalid/MRF?g=1234", [" 1234 ", "12345"]),
    ],
)
def test_asr_group_resolver_rejects_invalid_identifiers_before_producing_targets(url, group_numbers):
    with pytest.raises(ValueError, match="4 digits"):
        discovery._resolve_asr_health_benefits_mrf({}, url, {"group_numbers": group_numbers})


def test_asr_group_resolver_requires_at_least_one_group():
    with pytest.raises(ValueError, match="no ASR Health Benefits group numbers configured"):
        discovery._resolve_asr_health_benefits_mrf(
            {}, "https://adapter.example.invalid/MRF", {"group_numbers": [None, ""]}
        )


def test_asr_duplicate_seed_context_merges_nonempty_fields_without_losing_order(tmp_path, monkeypatch):
    seed_path = tmp_path / "groups.csv"
    seed_path.write_text(
        "group_number,status,company_name,employer_name,client_name,plan_name\n"
        "1234,active,Synthetic Company,First Employer,,Initial Plan\n"
        "1234,active,,Updated Employer,,Updated Plan\n"
        "2345,active,,,Synthetic Client,Client Plan\n"
        "3456,retired,Retired Company,,,\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "sources.json"
    config_path.write_text(
        json.dumps({"seed_lists": {"synthetic": {"schema": "group_number_seed_v1", "path": str(seed_path)}}}),
        encoding="utf-8",
    )
    monkeypatch.setenv(discovery.SOURCE_CONFIG_ENV, str(config_path))
    monkeypatch.delenv(discovery.PRIVATE_SEED_CONTEXT_PATHS_ENV, raising=False)

    group_targets = discovery._resolve_asr_health_benefits_mrf(
        {},
        "https://adapter.example.invalid/MRF?g=1234",
        {"seed_list": "synthetic", "group_numbers": ["2345", "", None, "4567"], "toc_path": "/download"},
    )

    assert [group_target.metadata["group_number"] for group_target in group_targets] == ["1234", "2345", "4567"]
    assert group_targets[0].metadata["company_name"] == "Synthetic Company"
    assert group_targets[0].metadata["employer_name"] == "Updated Employer"
    assert group_targets[0].metadata["plan_name"] == "Updated Plan"
    assert group_targets[0].label == "ASR Health Benefits group 1234 - Synthetic Company"
    assert group_targets[1].label == "ASR Health Benefits group 2345 - Synthetic Client"
    assert group_targets[2].label == "ASR Health Benefits group 4567"
    assert group_targets[0].url == (
        "https://adapter.example.invalid/download?fileType=TableOfContents&groupNumber=1234"
    )


def test_auxiant_directory_html_rejects_navigation_and_keeps_available_duplicate():
    page_html = """
        <a href="/outside/">*Outside content</a>
        <div class="wide entry-content">
          <a>No href</a><a href="">Empty href</a><a href="#top">*Top</a>
          <a href="mailto:synthetic@example.invalid">*Email</a>
          <a href="tel:000">*Phone</a><a href="javascript:void(0)">*Script</a>
          <a href="/blank/"> </a><a href="/stars/">***</a>
          <a href="https://external.example.invalid/network/">*External</a>
          <a href="/">*Home</a><a href="/directory-of-data-sources/">*Directory</a>
          <a href="/implementation-in-process/">*Pending</a>
          <a href="/wp-admin/">*Admin</a><a href="/index/example/">*Index</a>
          <a href="/network-a/">Earlier name</a>
          <a href="/network-a/">*Synthetic &amp; Network</a>
          <a href="/network-a/">Later name</a>
          <a href="/network-b/">Synthetic Unavailable</a>
        </div><!-- .entry-content -->
    """
    base_url = "https://directory.example.invalid/directory-of-data-sources/"

    all_networks = discovery._parse_auxiant_directory_networks(page_html, base_url=base_url, data_available_only=False)
    available_networks = discovery._parse_auxiant_directory_networks(page_html, base_url=base_url)

    assert all_networks == [
        {"url": "https://directory.example.invalid/network-a/", "label": "Synthetic & Network", "data_available": True},
        {
            "url": "https://directory.example.invalid/network-b/",
            "label": "Synthetic Unavailable",
            "data_available": False,
        },
    ]
    assert available_networks == all_networks[:1]


def test_auxiant_page_html_filters_assets_and_retains_labelled_downloads():
    base_url = "https://directory.example.invalid/network/"
    links = discovery._parse_auxiant_page_links(
        """
        <img src="/image.json"><a href="/wp-content/asset.zip">Asset</a>
        <a href="/wp-includes/asset.json">Asset</a>
        <a href="/wp-admin/admin-ajax.php?cmd=other">Ignored archive.zip</a>
        <a href="/local/">Local navigation</a>
        <a href="/directory/">Return to list of networks</a>
        <a href="ftp://external.example.invalid/landing">Unsupported</a>
        <a href="/download?id=1">Synthetic out of network.json.zip</a>
        <a href="/download?id=2">Synthetic pharmacy.json</a>
        <a href="/in-network.json"></a>
        <a href="https://external.example.invalid/landing">Synthetic external</a>
        """,
        base_url=base_url,
    )

    assert [link["url"] for link in links] == [
        "https://directory.example.invalid/download?id=1",
        "https://directory.example.invalid/download?id=2",
        "https://directory.example.invalid/in-network.json",
        "https://external.example.invalid/landing",
    ]
    assert [link.get("target_file_type") for link in links] == ["allowed-amounts", "payer-drug", "in-network", None]
    assert [link.get("container_format") for link in links] == ["zip", None, None, None]
    assert links[2]["label"] == "in-network.json"
    assert links[3]["target_kind"] == "external_landing"


def _auxiant_contract_pages(fixture):
    return {
        fixture["directory_url"]: (
            f'<a href="{fixture["page_url"]}">*Synthetic Network</a>'
            '<a href="/ignored/">*Ignored Network</a>'
            f'<a href="{fixture["directory_link"]["url"]}">Historical out of network.json</a>'
        ),
        fixture["page_url"]: (
            f'<a href="{fixture["page_link"]["url"]}">Current in-network</a>'
            f'<a href="{fixture["external_url"]}">Synthetic external</a>'
        ),
    }


@pytest.mark.parametrize("nested_result", ["empty", "failure", "file"])
@pytest.mark.asyncio
async def test_auxiant_html_resolution_keeps_nested_provenance_and_bounded_page_requests(monkeypatch, nested_result):
    """Preserve nested provenance and enforce page request limits for each outcome."""
    fixture = _auxiant_contract_fixture()
    session = object()
    page_html_by_url = _auxiant_contract_pages(fixture)
    fetch_text = AsyncMock(side_effect=lambda url, **_kwargs: page_html_by_url[url])
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    original_nested = discovery.CrawlTarget(
        source={"source_id": "nested"},
        url="https://files.example.invalid/nested.json",
        label="Nested label",
        resolved_from_url=None,
        metadata={},
    )
    nested = AsyncMock(return_value=[original_nested] if nested_result == "file" else [])
    if nested_result == "failure":
        nested.side_effect = ValueError("x" * 600)
    monkeypatch.setattr(discovery, "_crawl_targets_for_source", nested)

    directory_targets = await discovery._resolve_auxiant_wordpress_directory(
        fixture["source"],
        fixture["directory_url"],
        {"directory_path": "/networks", "max_networks": 1, "max_bytes": 2048, "page_max_bytes": 4096},
        session,
    )

    assert [directory_target.url for directory_target in directory_targets[:2]] == [
        fixture["directory_link"]["url"],
        fixture["page_link"]["url"],
    ]
    fetch_text.assert_has_awaits(
        [
            call(fixture["directory_url"], max_bytes=2048, session=session),
            call(fixture["page_url"], max_bytes=4096, session=session),
        ]
    )
    assert fetch_text.await_count == 2
    nested.assert_awaited_once_with(
        {**fixture["source"], "hosting_platform": "custom"}, fixture["external_url"], session
    )
    assert directory_targets[2].source == fixture["source"]
    assert directory_targets[2].metadata["auxiant_network_name"] == "Synthetic Network"
    assert directory_targets[2].metadata["external_source_url"] == fixture["external_url"]
    if nested_result == "file":
        assert directory_targets[2].url == original_nested.url
        assert directory_targets[2].resolved_from_url == fixture["external_url"]
        assert directory_targets[2].metadata["nested_target_label"] == "Nested label"
        assert original_nested.metadata == {}
    else:
        assert directory_targets[2].url == fixture["external_url"]
        assert directory_targets[2].metadata["landing_reason"] == "external_landing_no_concrete_targets"
        error = directory_targets[2].metadata["nested_error"]
        if nested_result == "failure":
            assert error.startswith("ValueError: ")
            assert len(error) == 500
        else:
            assert error is None


@pytest.mark.asyncio
async def test_auxiant_network_fetch_failure_does_not_return_partial_directory_results(monkeypatch):
    fixture = _auxiant_contract_fixture()
    failure = OSError("synthetic network page failure")
    directory_html = (
        f'<a href="{fixture["directory_link"]["url"]}">Historical.json</a>'
        f'<a href="{fixture["page_url"]}">*Synthetic Network</a>'
    )
    fetch_text = AsyncMock(side_effect=[directory_html, failure])
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)

    with pytest.raises(OSError, match="synthetic network page failure") as error:
        await discovery._resolve_auxiant_wordpress_directory(
            fixture["source"], fixture["directory_url"], {"directory_path": "/networks"}, object()
        )
    assert error.value is failure
    assert fetch_text.await_count == 2
