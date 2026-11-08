# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import base64
import json
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call
from xml.etree.ElementTree import ParseError

import pytest

from process import mrf_source_discovery as discovery
from tests.test_mrf_source_discovery import _ELFINDER_PAGE_HTML, _FakeChunkedContent


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("ftp://catalog.example.invalid/rates.json", "only http"),
        ("https:///rates.json", "URL host is required"),
        ("http://localhost/rates.json", "local hosts are not allowed"),
        ("http://0.0.0.0/rates.json", "local hosts are not allowed"),
        ("https://catalog.local/rates.json", "local hosts are not allowed"),
    ],
)
@pytest.mark.asyncio
async def test_fetch_url_rejects_unsupported_hosts_before_dns(monkeypatch, url, message):
    dns_lookup = AsyncMock(side_effect=AssertionError("unexpected DNS lookup"))
    monkeypatch.setattr(discovery.asyncio, "to_thread", dns_lookup)
    with pytest.raises(ValueError, match=message):
        await discovery._assert_fetch_url_allowed(url)
    dns_lookup.assert_not_awaited()


@pytest.mark.parametrize("host", ["1.1.1.1", "[2606:4700:4700::1111]"])
@pytest.mark.asyncio
async def test_fetch_url_accepts_public_ip_literals_without_dns(monkeypatch, host):
    dns_lookup = AsyncMock(side_effect=AssertionError("unexpected DNS lookup"))
    monkeypatch.setattr(discovery.asyncio, "to_thread", dns_lookup)
    assert await discovery._assert_fetch_url_allowed(f"https://{host}/rates.json") is None
    dns_lookup.assert_not_awaited()


@pytest.mark.parametrize(
    ("scheme", "configured_port", "expected_port"), [("https", "", 443), ("http", "", 80), ("https", ":8443", 8443)]
)
@pytest.mark.asyncio
async def test_fetch_url_checks_every_dns_address_and_uses_requested_port(
    monkeypatch, scheme, configured_port, expected_port
):
    dns_addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", expected_port))]
    dns_lookup = AsyncMock(return_value=dns_addresses)
    monkeypatch.setattr(discovery.asyncio, "to_thread", dns_lookup)
    url = f"{scheme}://CATALOG.example.invalid{configured_port}/rates.json"
    assert await discovery._assert_fetch_url_allowed(url) is None
    dns_lookup.assert_awaited_once_with(
        discovery.socket.getaddrinfo, "catalog.example.invalid", expected_port, type=socket.SOCK_STREAM
    )

    dns_addresses.append((socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", expected_port)))
    with pytest.raises(ValueError, match="resolves to a non-public address: 127.0.0.1") as error:
        await discovery._assert_fetch_url_allowed(url)
    assert isinstance(error.value.__cause__, ValueError)


@pytest.mark.parametrize(
    ("status", "headers", "message"), [(503, {}, "HTTP 503"), (200, {"Content-Length": "9"}, "exceeds 8 byte")]
)
@pytest.mark.asyncio
async def test_streamed_toc_rejects_http_and_declared_size_before_read(status, headers, message):
    body = _FakeChunkedContent(b"{}")
    response = SimpleNamespace(status=status, headers=headers, content=body)
    with pytest.raises(ValueError, match=message):
        await discovery._read_query_matching_structures(response, target_queries=["Example"], max_bytes=8)
    assert body._offset == 0


@pytest.mark.asyncio
async def test_streamed_toc_retains_matching_plan_and_yields_during_large_inventory(monkeypatch):
    matching_plan_by_field = {"plan_name": "Example Health", "plan_id": "synthetic-plan"}
    matching_structure_by_field = {
        "reporting_plans": [None, {"plan_name": "Other Plan"}, matching_plan_by_field],
        "in_network_files": [{"location": "https://files.example.invalid/in-network.json"}],
    }
    encoded_body = json.dumps(
        {"reporting_structure": [None, *({"reporting_plans": []} for _ in range(999)), matching_structure_by_field]}
    ).encode()
    response = SimpleNamespace(status=200, headers={}, content=_FakeChunkedContent(encoded_body))
    yield_control = AsyncMock()
    monkeypatch.setattr(discovery.asyncio, "sleep", yield_control)

    assert await discovery._read_query_matching_structures(
        response, target_queries=["Example Health"], max_bytes=len(encoded_body)
    ) == [{**matching_structure_by_field, "reporting_plans": [matching_plan_by_field]}]
    yield_control.assert_awaited_once_with(0)


@pytest.mark.parametrize(
    ("url", "platform"),
    [
        ("https://www.emihealth.com/machinereadables/current", "html_mrf_links"),
        ("https://peakhealth.org/transparency/current", "html_mrf_links"),
        ("https://peakhealth.org/unrelated", "custom"),
        ("https://www.boonchapman.com/machine-readable-files/", "html_mrf_links"),
        ("https://catalog.example.invalid/tcr_directory.json", "json_mrf_directory_links"),
        ("https://catalog.changehealthcare.com/current", "change_healthcare"),
        ("https://catalog.amazonaws.com/current", "s3"),
        ("https://catalog.s3.example.invalid/current", "s3"),
    ],
)
def test_hosting_classification_preserves_specialized_fallbacks(url, platform):
    assert discovery.classify_hosting_platform(url) == platform


@pytest.mark.parametrize(
    ("file_schema", "expected_type"),
    [
        ("in network rates", "in-network"),
        ("allowed amount", "allowed-amounts"),
        ("Table of Contents", "table-of-contents"),
        ("Prescription Drug", "payer-drug"),
        ("Custom Format", "custom-format"),
        (None, "unknown"),
    ],
)
def test_healthsparq_file_schema_normalizes_supported_and_unknown_types(file_schema, expected_type):
    assert discovery._healthsparq_file_type(file_schema) == expected_type


@pytest.mark.parametrize(
    ("metadata_by_field", "expected_files"),
    [
        (None, []),
        ({"data": [None, {"file_path": "rates.json"}]}, [{"file_path": "rates.json"}]),
        ({"data": {"results": [{"file_path": "rates.json"}]}}, [{"file_path": "rates.json"}]),
        ({"files": [], "data": {"files": [{"file_path": "rates.json"}]}}, []),
        (
            {"files": "invalid", "data": {"items": ["invalid", {"file_path": "rates.json"}]}},
            [{"file_path": "rates.json"}],
        ),
        ({"data": "invalid"}, []),
    ],
)
def test_healthsparq_metadata_filters_malformed_rows_and_preserves_top_level_precedence(
    metadata_by_field, expected_files
):
    assert discovery._healthsparq_metadata_files(metadata_by_field) == expected_files


@pytest.mark.parametrize(
    ("file_path", "expected_url"),
    [
        (None, ""),
        (" ", ""),
        ("../rates.json", "https://catalog.example.invalid/rates.json"),
        ("/rates.json", "https://catalog.example.invalid/rates.json"),
        ("https://files.example.invalid/rates.json", "https://files.example.invalid/rates.json"),
    ],
)
def test_healthsparq_file_reference_resolves_relative_paths_without_changing_absolute_urls(file_path, expected_url):
    assert (
        discovery._healthsparq_file_url("https://catalog.example.invalid/current/metadata.json", file_path)
        == expected_url
    )


def test_plan_metadata_filters_unidentified_rows_and_preserves_explicit_market():
    original_plan_by_field = {"plan_name": "Example Plan", "market_type": "group"}
    explicit_plan_by_field = {"plan_id": "synthetic-plan", "market_type": "group", "plan_market_type": "individual"}
    metadata_by_field = {
        "plan_info": [None, {}, {"market_type": "group"}, original_plan_by_field, explicit_plan_by_field]
    }

    assert discovery._metadata_plan_info(metadata_by_field) == [
        {**original_plan_by_field, "plan_market_type": "group"},
        explicit_plan_by_field,
    ]
    assert "plan_market_type" not in original_plan_by_field
    assert discovery._metadata_plan_info({"plan_info": "invalid"}) == []


CATALOG_SOURCE = {"source_id": "source_catalog_contract", "display_name": "Example Plan"}


CATALOG_URL = "https://catalog.example.invalid/"


@pytest.mark.parametrize("include_body_files,max_targets", [(False, None), (True, None), (True, 1)])
def test_humana_payload_filters_invalid_rows_and_retains_selected_file_metadata(include_body_files, max_targets):
    """Alternate row envelopes keep valid TOCs and optional bodies in publication order."""
    toc_name = "2026-01-01_Example_index.json"
    body_name = "2026-01-01_Example_in-network-rates.json.gz"
    toc_link = f"https://files.example.invalid/download?filename={toc_name}"
    file_list_by_field = {
        "aaData": {},
        "data": [None, {"download": toc_link}, {"download": toc_link}, body_name, "notes.zip"],
    }
    crawl_targets = discovery._humana_pct_targets_from_payload(
        CATALOG_SOURCE,
        file_list_by_field,
        api_url=CATALOG_URL + "api/files",
        resolver={"download_path": "/download"},
        resolver_type="humana_pct_file_list",
        include_body_files=include_body_files,
        max_targets=max_targets,
    )
    expected_names = [toc_name, body_name] if include_body_files else [toc_name]
    assert [crawl_target.metadata["humana_file_name"] for crawl_target in crawl_targets] == expected_names[:max_targets]
    assert crawl_targets[0].url == CATALOG_URL + "download?fileName=" + toc_name
    assert crawl_targets[0].metadata["target_kind"] == "toc_json"
    assert "plan_info" not in crawl_targets[0].metadata
    if include_body_files and max_targets is None:
        assert crawl_targets[1].metadata["target_file_type"] == "in-network"
        assert crawl_targets[1].metadata["container_format"] == "gzip"
        assert crawl_targets[1].metadata["plan_info"][0]["plan_name"] == body_name


@pytest.mark.parametrize("file_list_by_field", [{}, {"aaData": None, "data": {}, "rows": "invalid"}])
def test_humana_payload_requires_a_row_collection(file_list_by_field):
    """Malformed row envelopes and record counts do not create publication targets."""
    assert (
        discovery._humana_pct_targets_from_payload(
            CATALOG_SOURCE,
            file_list_by_field,
            api_url=CATALOG_URL,
            resolver={},
            resolver_type="humana_pct_file_list",
        )
        == []
    )
    assert discovery._humana_pct_total_records({**file_list_by_field, "recordsTotal": "unknown"}) is None


@pytest.mark.asyncio
async def test_humana_pagination_stops_on_short_pages(monkeypatch):
    """A short page is terminal without fetching a second page."""
    file_name = "2026-01-01_Example_index.json"
    fetch_json = AsyncMock(return_value={"rows": [[file_name]], "recordsTotal": "unknown"})
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)
    crawl_targets = await discovery._humana_pct_file_type_targets(
        CATALOG_SOURCE,
        CATALOG_URL + "api/files",
        "innetwork",
        {"page_size": 2, "max_pages": 3},
        "humana_pct_file_list",
        1,
        0,
        None,
    )
    assert [crawl_target.metadata["humana_file_name"] for crawl_target in crawl_targets] == [file_name]
    fetch_json.assert_awaited_once_with(
        CATALOG_URL + "api/files?fileType=innetwork&iDisplayStart=0&iDisplayLength=2&sEcho=1",
        max_bytes=10 * 1024 * 1024,
        session=None,
    )


@pytest.mark.asyncio
async def test_humana_resolver_stops_before_the_next_file_type_at_capacity(monkeypatch):
    """A full page cannot exceed the target cap or trigger another file type."""
    first_file = "2026-01-01_Example_index.json"
    fetch_json = AsyncMock(
        return_value={"rows": [[first_file], ["2026-01-01_Other_index.json"]], "recordsTotal": "unknown"}
    )
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)

    crawl_targets = await discovery._resolve_humana_pct_file_list(
        CATALOG_SOURCE,
        CATALOG_URL,
        {
            "api_url": CATALOG_URL + "api/files",
            "file_types": ["innetwork", "allowedamount"],
            "page_size": 2,
            "max_pages": 3,
            "max_targets": 1,
        },
        None,
    )

    assert [crawl_target.metadata["humana_file_name"] for crawl_target in crawl_targets] == [first_file]
    assert fetch_json.await_count >= 1
    assert all("fileType=innetwork&" in request.args[0] for request in fetch_json.await_args_list)


@pytest.mark.parametrize(
    "toc_by_field,expected_name",
    [
        ([], None),
        ({"reporting_structure": [None, {"reporting_plans": [None, {"reporting_entity_name": " "}]}]}, None),
        (
            {
                "reporting_structure": [
                    None,
                    {"reporting_plans": [None, {}, {"reporting_entity_name": " Example First "}]},
                    {"reporting_plans": [{"reporting_entity_name": "Example Second"}]},
                ]
            },
            "Example First",
        ),
    ],
)
def test_bcbs_global_first_plan_skips_malformed_records_and_preserves_first_identity(toc_by_field, expected_name):
    """Use the first nonempty plan identity without promoting malformed preceding records."""
    assert discovery._bcbs_global_solutions_first_plan_name(toc_by_field) == expected_name


@pytest.mark.parametrize("max_targets", [None, 1])
def test_triples_payload_preserves_order_and_deduplicates_valid_publications(max_targets):
    """Unusable rows and duplicate URLs cannot displace selected TOCs or rate files."""
    toc_by_field = {"id": "toc-example", "url": "https://files.example.invalid/example_index.json"}
    body_by_field = {"id": "body-example", "url": "https://files.example.invalid/example_in-network-rates.json.gz"}
    crawl_targets = discovery._triples_mtt_targets_from_payload(
        CATALOG_SOURCE,
        {
            "list": [
                None,
                {},
                {"url": "https://files.example.invalid/readme.txt"},
                toc_by_field,
                toc_by_field,
                body_by_field,
            ]
        },
        resolved_from_url=CATALOG_URL,
        resolver={"latest_month_only": False, "max_targets": max_targets},
    )
    assert [crawl_target.metadata["triples_id"] for crawl_target in crawl_targets] == ["toc-example", "body-example"][
        :max_targets
    ]
    assert crawl_targets[0].metadata["target_kind"] == "toc_json"
    assert crawl_targets[0].metadata["plan_info"][0]["plan_name"] is None
    if max_targets is None:
        assert crawl_targets[1].metadata["target_kind"] == "file_reference"
        assert crawl_targets[1].metadata["container_format"] == "gzip"
    assert all(crawl_target.resolved_from_url == CATALOG_URL for crawl_target in crawl_targets)


@pytest.mark.parametrize("latest_month_only", [False, True])
@pytest.mark.asyncio
async def test_triples_resolver_keeps_valid_undated_inventory_without_a_second_request(monkeypatch, latest_month_only):
    """A usable undated inventory remains available when month selection cannot be inferred."""
    file_url = "https://files.example.invalid/example_index.json"
    fetch_json = AsyncMock(return_value={"selects": [], "list": [{"url": file_url}]})
    monkeypatch.setattr(discovery, "_fetch_json_value", fetch_json)
    [crawl_target] = await discovery._resolve_triples_mtt_api(
        CATALOG_SOURCE, CATALOG_URL, {"latest_month_only": latest_month_only}, None
    )
    assert crawl_target.url == file_url
    assert crawl_target.metadata["target_file_type"] == "table-of-contents"
    assert "month=&year=" in crawl_target.resolved_from_url
    fetch_json.assert_awaited_once()


@pytest.mark.parametrize("api_payload", [None, [], {"list": [None, {}]}])
@pytest.mark.asyncio
async def test_triples_resolver_rejects_empty_or_malformed_inventories(monkeypatch, api_payload):
    """Missing file records cannot be mistaken for a successful discovery result."""
    monkeypatch.setattr(discovery, "_fetch_json_value", AsyncMock(return_value=api_payload))
    with pytest.raises(ValueError, match="no Triple-S MTT targets"):
        await discovery._resolve_triples_mtt_api(CATALOG_SOURCE, CATALOG_URL, {}, None)


@pytest.mark.parametrize("file_payload", [None, "invalid", {}, {"files": None, "data": {"unexpected": "value"}}])
def test_payercompass_file_list_ignores_unrecognized_envelopes(file_payload):
    """Only recognized record envelopes can contribute downloadable file identities."""
    assert discovery._payercompass_file_list_items(file_payload) == []


def test_payercompass_target_requires_identity_and_preserves_frame_fallback():
    """Missing file IDs fail closed while a valid frame identity supplies supported defaults."""
    target_kwargs_by_field = {"base_url": CATALOG_URL, "resolver": {}, "file_item": {}}
    assert discovery._payercompass_target_for_file(CATALOG_SOURCE, frame={}, **target_kwargs_by_field) is None
    crawl_target = discovery._payercompass_target_for_file(
        CATALOG_SOURCE,
        frame={"id": "frame-example", "name": "Example_allowed-amounts.json.gz"},
        **target_kwargs_by_field,
    )
    assert crawl_target.url == CATALOG_URL + "api/File/Download?Id=frame-example"
    assert crawl_target.metadata["payercompass_file_id"] == "frame-example"
    assert crawl_target.metadata["payercompass_file_name"] == "Example_allowed-amounts.json.gz"
    assert crawl_target.metadata["target_file_type"] == "allowed-amounts"


def test_payercompass_plan_index_normalizes_nested_records_without_losing_metadata():
    """JSON arrays and singleton envelopes retain plans while unusable structures and files are skipped."""
    toc_by_field = {
        "reporting_entity_name": " Example Carrier ",
        "reporting_structure": [
            {"reporting_plans": None},
            {"reporting_plans": [{}, None]},
            {
                "reporting_plans": {
                    "planId": "plan-example",
                    "planName": "Example PPO",
                    "plan_sponser_name": "Example Employer",
                },
                "in_network_files": [
                    None,
                    {"location": "https://files.example.invalid/get?token=ignored&fileName=Example.json.gz"},
                ],
            },
        ],
    }
    plan_info_by_key, toc_metadata_by_field = discovery._payercompass_plan_info_by_file_key(toc_by_field)
    assert plan_info_by_key["example.json.gz"] == [
        {
            "plan_id": "plan-example",
            "plan_id_type": None,
            "plan_market_type": "group",
            "plan_name": "Example PPO",
            "plan_sponsor_name": "Example Employer",
        }
    ]
    assert plan_info_by_key["example.json"] == plan_info_by_key["example.json.gz"]
    assert toc_metadata_by_field == {"reporting_entity_name": "Example Carrier", "reporting_entity_type": ""}


@pytest.mark.parametrize(
    "structure_by_field",
    [
        {},
        {"mrfConfig": []},
        {"mrfConfig": {"timeFrames": {}}},
        {"mrfConfig": {"timeFrames": [None, {"id": " "}]}},
    ],
)
@pytest.mark.asyncio
async def test_payercompass_resolver_rejects_unusable_timeframes_without_file_requests(monkeypatch, structure_by_field):
    """Invalid catalog structure cannot trigger file-list requests or yield targets."""
    monkeypatch.setattr(discovery, "_post_json", AsyncMock(return_value=structure_by_field))
    post_file_list = AsyncMock()
    monkeypatch.setattr(discovery, "_post_json_value", post_file_list)
    with pytest.raises(ValueError, match="no PayerCompass MRF files"):
        await discovery._resolve_payercompass_mrf(CATALOG_SOURCE, CATALOG_URL, {}, None)
    post_file_list.assert_not_awaited()


@pytest.mark.asyncio
async def test_payercompass_resolver_bounds_timeframes_and_retains_file_provenance(monkeypatch):
    """The timeframe cap bounds requests before normalized file metadata becomes a target."""
    structure_by_field = {
        "mrfConfig": {
            "timeFrames": [None, {}, {"id": "frame-first", "name": "January", "fileType": 1}, {"id": "frame-later"}]
        }
    }
    monkeypatch.setattr(discovery, "_post_json", AsyncMock(return_value=structure_by_field))
    post_file_list = AsyncMock(
        return_value={
            "files": [
                None,
                {"downloadId": "file-first", "file_name": "Example_in-network-rates.json.gz", "sizeBytes": "1 KiB"},
            ]
        }
    )
    monkeypatch.setattr(discovery, "_post_json_value", post_file_list)
    [crawl_target] = await discovery._resolve_payercompass_mrf(
        CATALOG_SOURCE, CATALOG_URL, {"max_timeframes": 3, "max_bytes": 2048}, None
    )
    assert crawl_target.url == CATALOG_URL + "api/File/Download?Id=file-first"
    assert crawl_target.metadata["payercompass_timeframe_id"] == "frame-first"
    assert crawl_target.metadata["payercompass_timeframe_name"] == "January"
    assert crawl_target.metadata["size_bytes"] == 1024
    assert crawl_target.metadata["target_file_type"] == "in-network"
    post_file_list.assert_awaited_once_with(
        CATALOG_URL + "api/File/List", {"timeFrameId": "frame-first"}, max_bytes=2048, session=None
    )


def test_inventory_azure_xml_infers_urls_and_filters_duplicate_non_mrf_blobs():
    xml_text = """
    <EnumerationResults ContainerName="https://storage.example.invalid/files/">
      <Blobs>
        <Blob><Name>synthetic_index.json</Name></Blob>
        <Blob><Name>duplicate.json</Name><Url>https://storage.example.invalid/files/synthetic_index.json</Url></Blob>
        <Blob><Name>notes.txt</Name></Blob><Blob />
        <Blob><Name>synthetic_in-network-rates.json.gz</Name></Blob>
      </Blobs>
    </EnumerationResults>
    """
    crawl_targets = discovery._azure_mrf_listing_targets_from_xml(
        {"source_id": "synthetic"},
        xml_text,
        listing_url="https://adapter.example.invalid/list",
        resolver={"toc_max_bytes": "2 MiB"},
    )
    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://storage.example.invalid/files/synthetic_index.json",
        "https://storage.example.invalid/files/synthetic_in-network-rates.json.gz",
    ]
    assert crawl_targets[0].metadata["blob_name"] == "synthetic_index.json"
    assert crawl_targets[0].metadata["target_max_bytes"] == 2 * 1024 * 1024
    assert crawl_targets[1].metadata["target_max_bytes"] is None
    assert crawl_targets[1].metadata["container_format"] == "gzip"


@pytest.mark.asyncio
async def test_inventory_azure_rejects_malformed_xml_at_transport_boundary(monkeypatch):
    session = object()
    fetch_text = AsyncMock(return_value="<EnumerationResults><Blobs>")
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    with pytest.raises(ParseError):
        await discovery._resolve_azure_mrf_listing(
            {}, "https://adapter.example.invalid/list", {"max_bytes": 1234}, session
        )
    fetch_text.assert_awaited_once_with("https://adapter.example.invalid/list", max_bytes=1234, session=session)


@pytest.mark.asyncio
async def test_inventory_point32_direct_azure_directory_preserves_context_and_cap(monkeypatch):
    directory_url = "https://synthetic-mrf.z13.web.core.windows.net/plans"
    fetch_text = AsyncMock(
        return_value=(
            '<a href="/synthetic_in-network-rates.json.gz">Synthetic rates</a>'
            '<a href="/synthetic_allowed-amounts.json">Synthetic allowed amounts</a>'
        )
    )
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    session = object()
    [crawl_target] = await discovery._resolve_point32_azure_mrf_directory(
        {}, directory_url, {"max_targets": 1, "directory_max_bytes": 2345}, session
    )
    assert crawl_target.url == "https://synthetic-mrf.z13.web.core.windows.net/synthetic_in-network-rates.json.gz"
    assert crawl_target.resolved_from_url == directory_url
    assert crawl_target.metadata["point32_directory_url"] == directory_url
    assert crawl_target.metadata["point32_landing_url"] == directory_url
    fetch_text.assert_awaited_once_with(directory_url, max_bytes=2345, session=session)


@pytest.mark.asyncio
async def test_inventory_point32_landing_keeps_direct_files_and_limits_directory_walk(monkeypatch):
    landing_url = "https://adapter.example.invalid/transparency"
    directory_url = "https://synthetic-mrf.z13.web.core.windows.net/first"
    page_html_by_url = {
        landing_url: (
            '<a href="/synthetic_in-network-rates.json">Synthetic rates</a>'
            f'<a href="{directory_url}">MRF directory</a>'
            '<a href="https://synthetic-mrf.z13.web.core.windows.net/second">Other MRF directory</a>'
        ),
        directory_url: '<a href="/synthetic_allowed-amounts.json">Synthetic allowed amounts</a>',
    }
    fetch_text = AsyncMock(side_effect=lambda url, **_kwargs: page_html_by_url[url])
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    session = object()
    crawl_targets = await discovery._resolve_point32_azure_mrf_directory(
        {}, landing_url, {"max_directories": 1, "max_bytes": 1111, "directory_max_bytes": 2222}, session
    )
    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://adapter.example.invalid/synthetic_in-network-rates.json",
        "https://synthetic-mrf.z13.web.core.windows.net/synthetic_allowed-amounts.json",
    ]
    assert "point32_directory_url" not in crawl_targets[0].metadata
    assert crawl_targets[1].metadata["point32_directory_url"] == directory_url
    assert crawl_targets[1].metadata["point32_landing_url"] == landing_url
    assert fetch_text.await_args_list == [
        call(landing_url, max_bytes=1111, session=session),
        call(directory_url, max_bytes=2222, session=session),
    ]


@pytest.mark.asyncio
async def test_inventory_point32_rejects_landing_without_files_or_directory(monkeypatch):
    fetch_text = AsyncMock(return_value='<a href="/contact">Contact</a>')
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    with pytest.raises(ValueError, match="no Point32 Azure MRF directory targets"):
        await discovery._resolve_point32_azure_mrf_directory(
            {}, "https://adapter.example.invalid/transparency", {}, object()
        )
    fetch_text.assert_awaited_once()


def _inventory_manager_html(data_key="synthetic-key", nonce="synthetic-nonce"):
    return f'<script>url:"/ajax",_wpnonce:"{nonce}",data_key:"{data_key}"</script>'


@pytest.mark.parametrize(
    "page_html",
    [
        '<script>data_key:"synthetic-key"</script>',
        _inventory_manager_html(data_key=" "),
        _inventory_manager_html(nonce=" "),
    ],
)
def test_inventory_elfinder_config_rejects_incomplete_credentials(page_html):
    assert (
        discovery._wordpress_elfinder_configs_from_html(page_html, base_url="https://adapter.example.invalid/landing")
        == []
    )


def test_inventory_elfinder_config_deduplicates_manager_without_dom_identifier():
    manager_configs = discovery._wordpress_elfinder_configs_from_html(
        _inventory_manager_html() * 2, base_url="https://adapter.example.invalid/landing"
    )
    assert manager_configs == [
        {
            "url": "https://adapter.example.invalid/ajax",
            "data_key": "synthetic-key",
            "nonce": "synthetic-nonce",
        }
    ]


@pytest.mark.parametrize("encoded_hash", [None, "invalid", "l1_", "l1_é"])
def test_inventory_elfinder_invalid_hashes_do_not_become_file_paths(encoded_hash):
    assert discovery._wordpress_elfinder_hash_path(encoded_hash) is None


@pytest.mark.parametrize(
    ("root_url", "file_by_field", "expected_url"),
    [
        (
            None,
            {"url": "https://files.example.invalid/in-network.json"},
            "https://files.example.invalid/in-network.json",
        ),
        (None, {"url": "ftp://files.example.invalid/in-network.json"}, None),
        ("https://files.example.invalid/root/", {"hash": "l1_"}, None),
        ("https://files.example.invalid/root/", {"hash": "l1_Lw"}, None),
    ],
)
def test_inventory_elfinder_file_urls_require_direct_http_or_decodable_file_path(root_url, file_by_field, expected_url):
    assert discovery._wordpress_elfinder_file_url(root_url, file_by_field) == expected_url


def test_inventory_elfinder_derived_url_quotes_path_and_ignores_non_http_override():
    file_path = "synthetic group/allowed-amounts ü.json"
    encoded_hash = "l1_" + base64.urlsafe_b64encode(file_path.encode()).decode().rstrip("=")
    assert (
        discovery._wordpress_elfinder_file_url(
            "https://files.example.invalid/root", {"hash": encoded_hash, "url": "ftp://unsupported.invalid/file"}
        )
        == "https://files.example.invalid/root/synthetic%20group/allowed-amounts%20%C3%BC.json"
    )


@pytest.mark.parametrize("directory_payload", [{}, {"files": "invalid"}, ["invalid"]])
def test_inventory_elfinder_malformed_file_collections_are_empty(directory_payload):
    assert discovery._wordpress_elfinder_files(directory_payload) == []


def test_inventory_elfinder_payload_ignores_unresolvable_files_and_non_mrf_entries():
    directory_payload_by_field = {
        "files": [
            "invalid",
            {"mime": "directory", "hash": "root", "phash": ""},
            {"name": "Missing URL", "hash": "l1_"},
            {"name": "Notes", "url": "https://files.example.invalid/readme.txt"},
            {
                "name": "Synthetic rates",
                "url": "https://files.example.invalid/in-network.json",
                "mime": "application/json",
            },
        ]
    }
    crawl_targets = discovery._wordpress_elfinder_targets_from_payload(
        {},
        directory_payload_by_field,
        root_url=None,
        resolved_from_url="https://adapter.example.invalid/ajax",
        resolver_type="wordpress_elfinder_mrf_links",
        file_manager_id=None,
    )
    assert [crawl_target.url for crawl_target in crawl_targets] == ["https://files.example.invalid/in-network.json"]
    assert crawl_targets[0].metadata["target_file_type"] == "in-network"
    assert "wordpress_elfinder_file_manager_id" not in crawl_targets[0].metadata
    assert discovery._wordpress_elfinder_directory_hashes(directory_payload_by_field) == []


@pytest.mark.parametrize("root_result", ["failure", "malformed"])
@pytest.mark.asyncio
async def test_inventory_elfinder_manager_reports_root_failures_without_walking(monkeypatch, root_result):
    failure = OSError("synthetic root failure")
    post_form = AsyncMock(return_value=["invalid"])
    if root_result == "failure":
        post_form.side_effect = failure
    monkeypatch.setattr(discovery, "_post_form_json_value", post_form)
    manager_by_field = {"url": "https://adapter.example.invalid/ajax", "nonce": "synthetic", "data_key": "key"}
    session = object()
    crawl_targets, opened_count, last_error = await discovery._wordpress_elfinder_manager_targets(
        {}, manager_by_field, "wordpress_elfinder_mrf_links", 2048, 10, 3, session
    )
    assert crawl_targets == []
    assert opened_count == 3
    assert last_error is (failure if root_result == "failure" else None)
    post_form.assert_awaited_once_with(
        manager_by_field["url"],
        {"_wpnonce": "synthetic", "data_key": "key", "cmd": "open", "init": "1", "tree": "1"},
        max_bytes=2048,
        session=session,
    )


@pytest.mark.asyncio
async def test_inventory_elfinder_walk_survives_bad_children_and_enforces_global_cap(monkeypatch):
    root_payload_by_field = {
        "files": [
            {"mime": "directory", "hash": folder_name, "phash": "root"}
            for folder_name in ("failed", "malformed", "good", "not-opened")
        ]
    }
    failure = OSError("synthetic child failure")
    good_payload_by_field = {
        "files": [
            {
                "name": "Synthetic rates",
                "url": "https://files.example.invalid/in-network.json",
            }
        ]
    }
    post_form = AsyncMock(side_effect=[root_payload_by_field, failure, [], good_payload_by_field])
    monkeypatch.setattr(discovery, "_post_form_json_value", post_form)
    manager_by_field = {"url": "https://adapter.example.invalid/ajax", "nonce": "synthetic", "data_key": "key"}
    crawl_targets, opened_count, last_error = await discovery._wordpress_elfinder_manager_targets(
        {}, manager_by_field, "wordpress_elfinder_mrf_links", 2048, 5, 2, object()
    )
    assert [crawl_target.url for crawl_target in crawl_targets] == ["https://files.example.invalid/in-network.json"]
    assert opened_count == 5
    assert last_error is failure
    assert [request.args[1].get("target") for request in post_form.await_args_list] == [
        None,
        "failed",
        "malformed",
        "good",
    ]
    assert all(request.kwargs["max_bytes"] == 2048 for request in post_form.await_args_list)


@pytest.mark.parametrize("root_result", ["file", "failure", "malformed"])
@pytest.mark.asyncio
async def test_inventory_elfinder_resolver_closes_owned_browser_for_all_outcomes(monkeypatch, root_result):
    browser_session = object()
    browser_context = AsyncMock()
    browser_context.__aenter__.return_value = browser_session
    session_factory = Mock(return_value=browser_context)
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", session_factory)
    connector = object()
    monkeypatch.setattr(discovery, "_tcp_connector", Mock(return_value=connector))
    fetch_text = AsyncMock(return_value=_ELFINDER_PAGE_HTML)
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    post_form = AsyncMock(
        return_value={
            "files": [
                {
                    "name": "Synthetic rates",
                    "url": "https://files.example.invalid/in-network.json",
                }
            ]
        }
    )
    if root_result == "failure":
        post_form.side_effect = OSError("synthetic root failure")
    elif root_result == "malformed":
        post_form.return_value = []
    monkeypatch.setattr(discovery, "_post_form_json_value", post_form)
    timeout = object()
    session = SimpleNamespace(timeout=timeout)
    if root_result == "file":
        [crawl_target] = await discovery._resolve_wordpress_elfinder_mrf_links(
            {}, "https://example.test/machine-readable-files/", {}, session
        )
        assert crawl_target.url == "https://files.example.invalid/in-network.json"
        assert crawl_target.metadata["wordpress_elfinder_file_manager_id"] == "123"
        browser_context.__aexit__.assert_awaited_once_with(None, None, None)
    else:
        error_pattern = "synthetic root failure" if root_result == "failure" else "no WordPress elFinder MRF links"
        with pytest.raises(ValueError, match=error_pattern) as failure:
            await discovery._resolve_wordpress_elfinder_mrf_links(
                {}, "https://example.test/machine-readable-files/", {}, session
            )
        assert browser_context.__aexit__.await_args.args[1] is failure.value
    assert session_factory.call_args.kwargs["timeout"] is timeout
    assert session_factory.call_args.kwargs["connector"] is connector
    assert session_factory.call_args.kwargs["trust_env"] is False
    fetch_text.assert_awaited_once_with(
        "https://example.test/machine-readable-files/", max_bytes=5 * 1024 * 1024, session=browser_session
    )
    post_form.assert_awaited_once()
