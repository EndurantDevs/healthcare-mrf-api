# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import gzip
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import mrf_source_discovery as discovery


class _ChunkedBody:
    def __init__(self, *chunks: bytes):
        self._chunks = chunks

    async def iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk


class _Response:
    def __init__(
        self,
        *chunks: bytes,
        content_type: str = "application/json",
        status: int = 200,
    ):
        self.content = _ChunkedBody(*chunks)
        self.headers = {"Content-Type": content_type}
        self.charset = "utf-8"
        self.status = status
        self.url = "https://adapter.example.invalid/final"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _Session:
    def __init__(self, response: _Response):
        self.response = response
        self.timeout = object()
        self.get_calls = []
        self.post_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        return self.response

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        return self.response


_ASYNC_RESOLVER_ROUTES = [
    ("kaiser_monthly_inventory", "_resolve_kaiser_monthly_inventory"),
    ("azure_mrf_listing", "_resolve_azure_mrf_listing"),
    ("triples_mtt_api", "_resolve_triples_mtt_api"),
    ("s3_xml_listing", "_resolve_s3_xml_listing"),
    ("cigna_static_mrf_lookup", "_resolve_cigna_static_mrf_lookup"),
    ("bcbs_global_solutions_mrf", "_resolve_bcbs_global_solutions_mrf"),
    ("bcbs_asomrf_filelist", "_resolve_bcbs_asomrf_filelist"),
    ("meritain_mrf_search", "_resolve_meritain_mrf_search"),
    ("healthcarebluebook_mrf", "_resolve_healthcarebluebook_mrf"),
    ("ebms_caa_directory", "_resolve_ebms_caa_directory"),
    (
        "html_mrf_with_healthcarebluebook",
        "_resolve_html_mrf_with_healthcarebluebook",
    ),
    ("healthgram_network_index", "_resolve_healthgram_network_index"),
    ("anthem_s3_mrf", "_resolve_anthem_s3_mrf"),
    ("hcsc_asomrf_landing", "_resolve_hcsc_asomrf_landing"),
    ("point32_azure_mrf_directory", "_resolve_point32_azure_mrf_directory"),
    ("html_delegated_mrf_links", "_resolve_html_delegated_mrf_links"),
    ("midlandschoice_mrf", "_resolve_midlandschoice_mrf"),
    ("wordpress_elfinder_mrf_links", "_resolve_wordpress_elfinder_mrf_links"),
    ("html_mrf_links", "_resolve_html_mrf_links"),
    ("socrata_data_json_mrf_catalog", "_resolve_socrata_data_json_mrf_catalog"),
    ("json_mrf_directory_links", "_resolve_json_mrf_directory_links"),
    (
        "healthspace_machine_readable_files",
        "_resolve_healthspace_machine_readable_files",
    ),
    ("humana_pct_file_list", "_resolve_humana_pct_file_list"),
    ("fchn_payor_search", "_resolve_fchn_payor_search"),
    ("viva_health_mrf", "_resolve_viva_health_mrf"),
    ("healthez_benefits_mrf", "_resolve_healthez_benefits_mrf"),
    ("payercompass_mrf", "_resolve_payercompass_mrf"),
    ("webtpa_mrf_api", "_resolve_webtpa_mrf_api"),
    ("cmstic_file_info", "_resolve_cmstic_file_info"),
    ("cmstic_keyed_toc_redirect", "_resolve_cmstic_keyed_toc_redirect"),
    ("github_repo_mrf_tree", "_resolve_github_repo_mrf"),
    ("auxiant_wordpress_directory", "_resolve_auxiant_wordpress_directory"),
    ("healthsparq_direct_metadata", "_resolve_healthsparq_direct_metadata"),
    ("healthsparq_public_mrf", "_resolve_healthsparq_public_mrf"),
    ("providence_mrf_api", "_resolve_providence_mrf_api"),
    ("magnacare_transparency_mrf", "_resolve_magnacare_transparency_mrf"),
    ("mymedicalshopper_talon_mrf", "_resolve_mymedicalshopper_talon_mrf"),
]


@pytest.mark.parametrize(("resolver_type", "adapter_name"), _ASYNC_RESOLVER_ROUTES)
@pytest.mark.asyncio
async def test_platform_resolver_routes_to_exact_async_adapter(
    monkeypatch,
    resolver_type,
    adapter_name,
):
    source_record_by_field = {
        "source_id": "mrfsource_route_contract",
        "display_name": "Synthetic Source",
        "hosting_platform": "synthetic-platform",
    }
    expected_crawl_targets = [SimpleNamespace(url="https://data.example.invalid/toc.json")]
    adapter = AsyncMock(return_value=expected_crawl_targets)
    monkeypatch.setattr(
        discovery,
        "_platform_resolver_config",
        lambda _platform: {"type": resolver_type, "max_targets": 25},
    )
    monkeypatch.setattr(discovery, adapter_name, adapter)
    session = object()

    crawl_targets = await discovery._crawl_targets_for_source(
        source_record_by_field,
        "https://data.example.invalid/source",
        session,
        target_limit=7,
    )

    assert crawl_targets is expected_crawl_targets
    adapter.assert_awaited_once()
    assert adapter.await_args.args[0:2] == (
        source_record_by_field,
        "https://data.example.invalid/source",
    )
    assert adapter.await_args.args[2] == {"type": resolver_type, "max_targets": 7}
    assert adapter.await_args.args[3] is session


@pytest.mark.parametrize(
    ("resolver_type", "adapter_name"),
    [
        ("bcbsma_monthly_tocs", "_bcbsma_monthly_toc_targets"),
        ("monthly_toc_templates", "_monthly_toc_targets"),
        ("asr_health_benefits_mrf", "_resolve_asr_health_benefits_mrf"),
    ],
)
@pytest.mark.asyncio
async def test_platform_resolver_routes_to_exact_synchronous_adapter(
    monkeypatch,
    resolver_type,
    adapter_name,
):
    expected_targets = [SimpleNamespace(url="https://data.example.invalid/toc.json")]
    adapter = Mock(return_value=expected_targets)
    monkeypatch.setattr(
        discovery,
        "_platform_resolver_config",
        lambda _platform: {"type": resolver_type},
    )
    monkeypatch.setattr(discovery, adapter_name, adapter)
    source_record_by_field = {"hosting_platform": "synthetic-platform"}

    crawl_targets = await discovery._crawl_targets_for_source(
        source_record_by_field,
        "https://data.example.invalid/source",
        object(),
    )

    assert crawl_targets is expected_targets
    adapter.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_bytes_owns_session_and_enforces_byte_limit(monkeypatch):
    allowed = AsyncMock()
    session = _Session(_Response(b"ab", b"cd", content_type="application/octet-stream"))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allowed)
    monkeypatch.setattr(discovery, "_tcp_connector", lambda **_kwargs: object())
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", lambda **_kwargs: session)

    body = await discovery._fetch_bytes(
        "https://data.example.invalid/archive.zip",
        max_bytes=4,
    )

    assert body == b"abcd"
    assert [call.args[0] for call in allowed.await_args_list] == [
        "https://data.example.invalid/archive.zip",
        "https://data.example.invalid/archive.zip",
        "https://adapter.example.invalid/final",
    ]
    with pytest.raises(ValueError, match="exceeds 3 byte"):
        await discovery._fetch_bytes(
            "https://data.example.invalid/archive.zip",
            max_bytes=3,
            session=session,
        )


@pytest.mark.asyncio
async def test_post_adapters_preserve_payload_shape_headers_and_compression(
    monkeypatch,
):
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    form_session = _Session(_Response(gzip.compress(b'{"items": [1, 2]}')))
    json_session = _Session(_Response(b'{"ok": true}'))
    text_session = _Session(
        _Response(gzip.compress("synthetic text".encode()), content_type="text/plain")
    )

    form_value = await discovery._post_form_json_value(
        "https://adapter.example.invalid/form",
        {"query": "synthetic"},
        session=form_session,
    )
    json_value = await discovery._post_json_value(
        "https://adapter.example.invalid/json",
        {"query": "synthetic"},
        session=json_session,
    )
    text_value = await discovery._post_text(
        "https://adapter.example.invalid/text",
        "query=synthetic",
        headers={"X-Contract": "adapter"},
        session=text_session,
    )

    assert form_value == {"items": [1, 2]}
    assert json_value == {"ok": True}
    assert text_value == "synthetic text"
    _, form_kwargs = form_session.post_calls[0]
    assert form_kwargs["data"] == {"query": "synthetic"}
    assert form_kwargs["headers"]["X-Requested-With"] == "XMLHttpRequest"
    _, json_kwargs = json_session.post_calls[0]
    assert json_kwargs["json"] == {"query": "synthetic"}
    _, text_kwargs = text_session.post_calls[0]
    assert text_kwargs["data"] == "query=synthetic"
    assert text_kwargs["headers"] == {"X-Contract": "adapter"}


@pytest.mark.parametrize(
    ("response", "max_bytes", "message"),
    [
        (_Response(b"{}", content_type="text/html"), 5, "content-type is not JSON"),
        (_Response(b"  <!doctype html>"), 64, "response body is not JSON"),
        (_Response(b"123", b"456"), 5, "exceeds 5 byte"),
    ],
)
@pytest.mark.asyncio
async def test_json_post_rejects_non_json_and_oversized_responses(
    monkeypatch,
    response,
    max_bytes,
    message,
):
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())

    with pytest.raises(ValueError, match=message):
        await discovery._post_json_value(
            "https://adapter.example.invalid/json",
            {"query": "synthetic"},
            max_bytes=max_bytes,
            session=_Session(response),
        )


@pytest.mark.parametrize(
    ("response", "max_bytes", "allow_browser_fallback", "error_type", "message"),
    [
        (
            _Response(b"{}", content_type="text/html"),
            64,
            False,
            ValueError,
            "content-type is not JSON",
        ),
        (
            _Response(b"{}", content_type="text/html"),
            64,
            True,
            discovery._BrowserFallbackRequired,
            None,
        ),
        (
            _Response(b"123", b"456"),
            5,
            False,
            ValueError,
            "exceeds 5 byte",
        ),
        (
            _Response(b"  <!doctype html>"),
            64,
            False,
            ValueError,
            "response body is not JSON",
        ),
        (
            _Response(b"  <!doctype html>"),
            64,
            True,
            discovery._BrowserFallbackRequired,
            None,
        ),
    ],
)
@pytest.mark.asyncio
async def test_read_text_response_preserves_json_validation_and_fallback_contract(
    monkeypatch,
    response,
    max_bytes,
    allow_browser_fallback,
    error_type,
    message,
):
    """The extracted response reader retains the fetcher's error semantics."""
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())

    with pytest.raises(error_type, match=message):
        await discovery._read_text_response(
            response,
            max_bytes=max_bytes,
            expect_json=True,
            allow_browser_fallback=allow_browser_fallback,
        )


@pytest.mark.asyncio
async def test_fetch_text_uses_supplied_session_for_bounded_json(monkeypatch):
    allowed = AsyncMock()
    session = _Session(_Response(b'{"ok": true}'))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allowed)

    text = await discovery._fetch_text(
        "https://adapter.example.invalid/toc.json",
        max_bytes=64,
        session=session,
        expect_json=True,
    )

    assert text == '{"ok": true}'
    assert [call.args[0] for call in allowed.await_args_list] == [
        "https://adapter.example.invalid/toc.json",
        "https://adapter.example.invalid/final",
    ]
    assert session.get_calls[0][0] == "https://adapter.example.invalid/toc.json"


@pytest.mark.asyncio
async def test_fetch_text_owns_session_when_one_is_not_supplied(monkeypatch):
    allowed = AsyncMock()
    session = _Session(_Response(b"directory html", content_type="text/html"))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allowed)
    monkeypatch.setattr(discovery, "_tcp_connector", lambda **_kwargs: object())
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", lambda **_kwargs: session)

    text = await discovery._fetch_text(
        "https://adapter.example.invalid/directory",
        max_bytes=64,
    )

    assert text == "directory html"
    assert [call.args[0] for call in allowed.await_args_list] == [
        "https://adapter.example.invalid/directory",
        "https://adapter.example.invalid/directory",
        "https://adapter.example.invalid/final",
    ]
    assert session.get_calls[0][0] == "https://adapter.example.invalid/directory"


@pytest.mark.parametrize(
    ("helper_name", "value", "expected_message"),
    [
        ("_fetch_json", {"ok": True}, None),
        ("_fetch_json", ["not", "an", "object"], "expected JSON object"),
        ("_post_json", {"ok": True}, None),
        ("_post_json", ["not", "an", "object"], "expected JSON object"),
    ],
)
@pytest.mark.asyncio
async def test_object_json_helpers_reject_array_payloads(
    monkeypatch,
    helper_name,
    value,
    expected_message,
):
    dependency_name = "_fetch_json_value" if helper_name == "_fetch_json" else "_post_json_value"
    monkeypatch.setattr(discovery, dependency_name, AsyncMock(return_value=value))
    helper = getattr(discovery, helper_name)

    if expected_message:
        with pytest.raises(ValueError, match=expected_message):
            if helper_name == "_fetch_json":
                await helper("https://adapter.example.invalid/json")
            else:
                await helper("https://adapter.example.invalid/json", {})
    elif helper_name == "_fetch_json":
        assert await helper("https://adapter.example.invalid/json") == value
    else:
        assert await helper("https://adapter.example.invalid/json", {}) == value


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
