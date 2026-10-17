# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree

import pytest

from process import mrf_source_discovery as discovery
from tests.test_mrf_discovery_adapter_contracts import _Response, _Session
from tests.test_mrf_source_discovery import _FakeFetchResponse
from tests.test_mrf_source_discovery_retained_paths import _TrackedResponse


@pytest.mark.parametrize("operation", ["_post_form_json_value", "_post_json_value"])
@pytest.mark.parametrize("oversized", [False, True])
@pytest.mark.asyncio
async def test_json_request_owned_session__closes_after_success_or_byte_rejection(monkeypatch, operation, oversized):
    response = _Response(b'{"items":', b"[1]}")
    response.charset = None
    session = _Session(response)
    session_factory = Mock(return_value=session)
    connector = object()
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    monkeypatch.setattr(discovery, "_tcp_connector", lambda **_kwargs: connector)
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", session_factory)
    if oversized:
        with pytest.raises(ValueError, match="exceeds 8 byte"):
            await getattr(discovery, operation)(
                "https://catalog.example.invalid/query", {"query": "Example"}, max_bytes=8
            )
    else:
        assert await getattr(discovery, operation)(
            "https://catalog.example.invalid/query", {"query": "Example"}, max_bytes=13
        ) == {"items": [1]}
    assert session.entered and session.exited
    options_by_name = session_factory.call_args.kwargs
    assert options_by_name["trust_env"] is False
    assert options_by_name["connector"] is connector
    assert options_by_name["timeout"].connect == 15
    request_options = session.post_calls[0][1]
    if operation == "_post_form_json_value":
        assert request_options["data"] == {"query": "Example"}
        assert request_options["headers"]["X-Requested-With"] == "XMLHttpRequest"
    else:
        assert request_options["json"] == {"query": "Example"}


@pytest.mark.asyncio
async def test_filtered_toc_owned_session__streams_matching_plans_and_closes(monkeypatch):
    target = discovery.CrawlTarget(
        source={"metadata_json": {"target_payer_query": "Example Employer"}},
        url="https://catalog.example.invalid/index.json",
        label="Example Employer",
        metadata={"reporting_entity_name": "Example Payer"},
    )
    matching_structure_by_field = {
        "reporting_plans": [{"plan_name": "Example Employer"}],
        "in_network_files": [{"location": "https://files.example.invalid/rates.json"}],
    }
    response = _FakeFetchResponse(
        status=200,
        body=json.dumps({"reporting_structure": [matching_structure_by_field]}).encode(),
        content_type="application/json",
        url=target.url,
    )
    session = _Session(response)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    monkeypatch.setattr(discovery, "_tcp_connector", lambda **_kwargs: object())
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", lambda **_kwargs: session)
    assert await discovery._fetch_query_filtered_toc(target, max_bytes=4096) == {
        "reporting_entity_name": "Example Payer",
        "reporting_structure": [matching_structure_by_field],
    }
    assert session.entered and session.exited


@pytest.mark.asyncio
async def test_filtered_toc__requires_query_before_opening_transport(monkeypatch):
    transport = Mock(side_effect=AssertionError("unexpected transport"))
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", transport)
    target = discovery.CrawlTarget(source={}, url="https://catalog.example.invalid/index.json", label="Index")
    with pytest.raises(ValueError, match="target payer query is required"):
        await discovery._fetch_query_filtered_toc(target, max_bytes=64)
    transport.assert_not_called()


def test_toc_json_repair__preserves_escaped_quotes_backslashes_and_object_text():
    payload_by_field = {
        "reporting_structure": [
            {
                "reporting_plans": [{"plan_name": 'Quoted "Plan" C:\\Temp } {'}],
                "in_network_files": [{"location": "a.json"}, {"location": "b.json"}],
            }
        ]
    }
    malformed_text = json.dumps(payload_by_field).replace('}, {"location"', '} {"location"', 1)
    assert discovery._loads_mrf_json_value(malformed_text) == payload_by_field


@pytest.mark.parametrize(
    "malformed_text",
    ['{"items":[{}{}]}', '{"reporting_structure":[{"reporting_plans":[],"in_network_files":[]}'],
)
def test_toc_json_repair__rejects_other_syntax_errors(malformed_text):
    with pytest.raises(json.JSONDecodeError):
        discovery._loads_mrf_json_value(malformed_text)


def _json_contract_archive(member_by_name):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in member_by_name.items():
            archive.writestr(name, value)
    return buffer.getvalue()


@pytest.mark.parametrize(
    ("archive_bytes", "max_bytes", "message"),
    [
        (b"not a zip", 32, "not a readable ZIP archive"),
        (_json_contract_archive({"directory/": "", "readme.txt": "no data"}), 512, "does not contain JSON"),
        (
            _json_contract_archive({"index.json": json.dumps({"padding": "x" * 4096})}),
            256,
            "zip member index.json exceeds 256 byte",
        ),
    ],
    ids=["invalid-zip", "no-json", "oversized-member"],
)
def test_zip_json_inventory__rejects_invalid_empty_or_oversized_members(archive_bytes, max_bytes, message):
    assert len(archive_bytes) <= max_bytes
    with pytest.raises(ValueError, match=message) as error:
        discovery._json_values_from_zip_bytes(archive_bytes, max_bytes=max_bytes)
    if archive_bytes == b"not a zip":
        assert isinstance(error.value.__cause__, zipfile.BadZipFile)


@pytest.mark.asyncio
async def test_fetch_zip_json_inventory__forwards_limit_and_decodes_member(monkeypatch):
    archive_bytes = _json_contract_archive({"index.json": '{"version":"1"}'})
    fetch_bytes = AsyncMock(return_value=archive_bytes)
    monkeypatch.setattr(discovery, "_fetch_bytes", fetch_bytes)
    session = object()
    assert await discovery._fetch_zip_json_values(
        "https://catalog.example.invalid/inventory.zip", max_bytes=len(archive_bytes), session=session
    ) == [("index.json", {"version": "1"})]
    fetch_bytes.assert_awaited_once_with(
        "https://catalog.example.invalid/inventory.zip", max_bytes=len(archive_bytes), session=session
    )


@pytest.mark.parametrize(
    ("payload_text", "expected_hashes"),
    [
        ("[]", []),
        ('{"staticQueryHashes":[null," ","one","one"]}', ["one"]),
        ('not-json "staticQueryHashes":[" ","one"]', ["one"]),
    ],
)
def test_sapphire_query_hashes__skip_empty_or_non_mapping_payloads(payload_text, expected_hashes):
    assert discovery._sapphire_static_query_hashes(payload_text) == expected_hashes


@pytest.mark.parametrize(("query", "expected_slugs"), [(None, []), ("###", []), ("Example", ["example"])])
def test_sapphire_query_slugs__handle_absent_punctuation_and_single_words(query, expected_slugs):
    assert discovery._sapphire_query_slug_variants(query) == expected_slugs


@pytest.mark.parametrize("invalid_url", [" ", "/tocs/current/provider-data.json"])
def test_sapphire_query_payload__rejects_blank_and_provider_only_toc_links(invalid_url):
    assert (
        discovery._parse_sapphire_static_query_toc_links(
            json.dumps({"data": [{"url": invalid_url, "label": "Example Employer"}]}),
            base_url="https://catalog.sapphiremrfhub.com/",
        )
        == []
    )


@pytest.mark.parametrize("inline", [True, False])
@pytest.mark.asyncio
async def test_sapphire_static_queries__honor_target_cap_before_later_requests(monkeypatch, inline):
    base_url = "https://catalog.sapphiremrfhub.com/"
    target_records = [{"url": f"/tocs/current/{name}", "label": name} for name in ["Example", "Other"]]
    page_text = json.dumps({"data": target_records} if inline else {"staticQueryHashes": ["one", "two"]})
    requests = []

    async def fetch_text(url, **_kwargs):
        requests.append(url)
        if url.endswith("page-data/index/page-data.json"):
            return page_text
        assert url.endswith("page-data/sq/d/one.json")
        return json.dumps({"data": target_records})

    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    assert await discovery._resolve_sapphire_static_query_toc_links(base_url, {"max_targets": 1}, object()) == [
        {"url": base_url + "tocs/current/Example", "label": "Example", "file_name": "Example"}
    ]
    expected_urls = [base_url + "page-data/index/page-data.json"]
    if not inline:
        expected_urls.append(base_url + "page-data/sq/d/one.json")
    assert requests == expected_urls


@pytest.mark.asyncio
async def test_sapphire_static_queries__discard_malformed_query_json(monkeypatch):
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value="not-json"))
    assert (
        await discovery._resolve_sapphire_static_query_toc_links("https://catalog.sapphiremrfhub.com/", {}, object())
        == []
    )


@pytest.mark.parametrize("has_toc", [True, False])
@pytest.mark.asyncio
async def test_healthgram_landing_fallback__keeps_only_toc_and_reports_empty_index(monkeypatch, has_toc):
    landing_url = "https://mrf.healthgram.com/"
    landing_html = '<a href="https://other.example.invalid/network/example.cfm">Other network</a>'
    landing_html += '<a href="/rates_in-network-rates.json.gz">In network rates</a>'
    if has_toc:
        landing_html += '<a href="/example_index.json">Table of contents</a>'
    fetch_text = AsyncMock(return_value=landing_html)
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    if has_toc:
        [target] = await discovery._resolve_healthgram_network_index(
            {"display_name": "Example Network"}, landing_url, {}, object()
        )
        assert target.url == landing_url + "example_index.json"
        assert target.label == "Example Network"
        assert target.metadata["healthgram_network_page_url"] == landing_url
    else:
        with pytest.raises(ValueError, match="no Healthgram network index links"):
            await discovery._resolve_healthgram_network_index({}, landing_url, {}, object())
    fetch_text.assert_awaited_once()


DIRECTORY_SOURCE = {"source_id": "source_directory_contract", "display_name": "Example Plan"}


DIRECTORY_URL = "https://directory.example.invalid/index.html"


def test_mymedicalshopper_feed_retains_configuration_and_employer_updates__json_request_contract():
    """Ignore unrelated feed records and retain the latest identified employer update."""
    messages = [
        {"collection": "unrelated", "fields": {"slug": "example"}},
        {"collection": "thirdPartyAdministrators", "fields": []},
        {"collection": "thirdPartyAdministrators", "fields": {"slug": "other"}},
        {"collection": "thirdPartyAdministrators", "fields": {"slug": "example", "displayName": "Example TPA"}},
        {"collection": "employers", "msg": "added", "fields": {"name": "Missing slug"}},
        {
            "collection": "employers",
            "msg": "added",
            "id": {"$type": "oid", "$value": "employer-first"},
            "fields": {"slug": "example-10001", "name": "Old name"},
        },
        {
            "collection": "employers",
            "msg": "changed",
            "id": {"$type": "oid", "$value": "employer-first"},
            "fields": {"slug": "example-10001", "name": "Example Employer"},
        },
    ]
    config_by_field = discovery._mymedicalshopper_entity_config_from_messages(messages, "example")
    assert config_by_field == {"slug": "example", "displayName": "Example TPA"}
    assert discovery._mymedicalshopper_entity_config_from_messages(messages, "absent") == {}
    assert discovery._mymedicalshopper_tpa_name_from_config(config_by_field, "example") == "Example TPA"
    assert discovery._mymedicalshopper_tpa_name_from_config({}, "example-tpa") == "Example Tpa"
    assert discovery._mymedicalshopper_employer_docs_from_messages(messages) == [
        {"_id": "employer-first", "slug": "example-10001", "name": "Example Employer"}
    ]


@pytest.mark.parametrize("include_valid", [False, True])
def test_mymedicalshopper_generated_feed_filters_unpublished_and_unusable_history__json_request_contract(include_valid):
    """Only generated HTTP publications survive incomplete plan and history records."""
    generated_entries = [{"plan": {"unrelated": "value"}}, {"history": [None, {"mrfGenerated": False}]}]
    if include_valid:
        generated_entries.append(
            {
                "history": [
                    {
                        "mrfGenerated": True,
                        "month": "2026-01",
                        "link": "https://files.example.invalid/example_index.json",
                    }
                ]
            }
        )
    crawl_targets = discovery._mymedicalshopper_targets_from_generated(
        DIRECTORY_SOURCE,
        entity_slug="example",
        employer={"slug": "example-10001", "name": "Example Employer"},
        generated=generated_entries,
        resolver_type="mymedicalshopper_talon_mrf",
        resolved_from_url=DIRECTORY_URL,
    )
    if not include_valid:
        assert crawl_targets == []
    else:
        [crawl_target] = crawl_targets
        assert crawl_target.url == "https://files.example.invalid/example_index.json"
        assert crawl_target.metadata["plan_id"] is None
        assert crawl_target.metadata["plan_name"] is None
        assert crawl_target.metadata["group_number"] == "10001"
        assert crawl_target.label == "Example Employer - 2026-01"


def _mms_frame__json_request_contract(message_by_field):
    return SimpleNamespace(type=discovery.aiohttp.WSMsgType.TEXT, data="a" + json.dumps([json.dumps(message_by_field)]))


@pytest.mark.parametrize("has_generated_file", [False, True])
@pytest.mark.asyncio
async def test_mymedicalshopper_direct_employer_closes_transport_after_discovery__json_request_contract(
    monkeypatch, has_generated_file
):
    """Real protocol parsing closes its transport after both valid and empty generated inventories."""
    employer_slug = "example-tpa-10001"
    generated_entries = (
        [
            {
                "planId": "plan-first",
                "history": [{"mrfGenerated": True, "link": "https://files.example.invalid/example_index.json"}],
            }
        ]
        if has_generated_file
        else []
    )
    websocket = SimpleNamespace(
        receive=AsyncMock(
            side_effect=[
                SimpleNamespace(type=discovery.aiohttp.WSMsgType.TEXT, data="o"),
                _mms_frame__json_request_contract({"msg": "connected", "session": "example"}),
                _mms_frame__json_request_contract(
                    {"msg": "result", "id": f"mms-employer-name-{employer_slug}", "result": "Example Employer"}
                ),
                _mms_frame__json_request_contract(
                    {"msg": "result", "id": f"mms-plans-{employer_slug}", "result": ["plan-first"]}
                ),
                _mms_frame__json_request_contract(
                    {"msg": "result", "id": f"mms-generated-{employer_slug}", "result": generated_entries}
                ),
            ]
        ),
        send_str=AsyncMock(),
        close=AsyncMock(),
    )
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    session = SimpleNamespace(ws_connect=AsyncMock(return_value=websocket))
    request = discovery._resolve_mymedicalshopper_talon_mrf(
        DIRECTORY_SOURCE, f"https://directory.example.invalid/mrf/{employer_slug}", {}, session
    )
    if has_generated_file:
        [crawl_target] = await request
        assert crawl_target.metadata["plan_id"] == "plan-first"
        assert crawl_target.metadata["employer_slug"] == employer_slug
    else:
        with pytest.raises(ValueError, match="no generated MyMedicalShopper MRF links"):
            await request
    websocket.close.assert_awaited_once()
    sent_messages = [
        json.loads(encoded_message)
        for call in websocket.send_str.await_args_list
        for encoded_message in json.loads(call.args[0])
    ]
    assert [message_by_field.get("method") for message_by_field in sent_messages[1:]] == [
        "getEmployerName",
        "getBenefitPlans",
        "getGeneratedMRFInfoByPlan",
    ]
    assert sent_messages[-1]["params"] == [["plan-first"]]


@pytest.mark.asyncio
async def test_mymedicalshopper_rejects_unsupported_url_before_transport__json_request_contract():
    """An unsupported URL cannot open a feed connection."""
    session = SimpleNamespace(ws_connect=AsyncMock())
    with pytest.raises(ValueError, match="unsupported MyMedicalShopper MRF URL"):
        await discovery._resolve_mymedicalshopper_talon_mrf(DIRECTORY_SOURCE, DIRECTORY_URL, {}, session)
    session.ws_connect.assert_not_awaited()


def _magnacare_row_html__json_request_contract(
    *,
    run_id="run-first",
    file_name="Example_in-network.zip",
    plan_name="Example PPO",
    plan_id="plan-first",
    file_type="In-Network",
):
    return (
        '<tr class="default"><td>Group ID</td><td>Group</td>'
        f"<td>{plan_id}</td><td>{plan_name}</td><td>Example Network</td><td>{file_type}</td><td>1 MiB</td>"
        f'<td><a data-rhid="{run_id}" data-fname="{file_name}">Download</a></td></tr>'
    )


def test_magnacare_search_aliases_discard_blank_duplicates__json_request_contract():
    """Search aliases remain ordered while blanks and repeated spellings are removed."""
    source_by_field = {"metadata_json": {"plan_names": [" ", "Example-DBA-Alternate"]}}
    assert discovery._magnacare_search_terms(source_by_field, {"search_terms": [" ", "Alternate", "alternate"]}) == [
        "Example DBA Alternate",
        "Alternate",
        "Example-DBA-Alternate",
    ]


@pytest.mark.parametrize(
    "html_text",
    [
        '<tr class="heading"><td>Not a result</td></tr>',
        '<tr class="default"><td>Incomplete</td><a data-rhid="first">Download</a></tr>',
        '<tr class="default">' + "<td>value</td>" * 7 + "</tr>",
        _magnacare_row_html__json_request_contract(file_name=""),
        _magnacare_row_html__json_request_contract(file_name="readme.txt", file_type="Notes"),
    ],
)
@pytest.mark.asyncio
async def test_magnacare_rejects_unusable_results_before_download__json_request_contract(monkeypatch, html_text):
    """Malformed rows and untyped files cannot trigger download requests."""
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value=html_text))
    fetch_json = AsyncMock()
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)
    with pytest.raises(ValueError, match="no MagnaCare transparency MRF rows"):
        await discovery._resolve_magnacare_transparency_mrf(
            DIRECTORY_SOURCE, DIRECTORY_URL, {"search_terms": ["example"]}, None
        )
    fetch_json.assert_not_awaited()


@pytest.mark.parametrize("anonymous_plan", [False, True])
@pytest.mark.asyncio
async def test_magnacare_deduplicates_plans_and_bounds_search_requests__json_request_contract(
    monkeypatch, anonymous_plan
):
    """One file retains distinct plan metadata without repeating a bounded search."""
    result_html = _magnacare_row_html__json_request_contract(
        plan_id="" if anonymous_plan else "plan-first",
        plan_name="" if anonymous_plan else "Example PPO",
        file_type="Out-Network",
    )
    fetch_text = AsyncMock(return_value=result_html * 2)
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    monkeypatch.setattr(
        discovery,
        "_fetch_json",
        AsyncMock(return_value={"Data": "https://files.example.invalid/example_allowed-amounts.zip"}),
    )
    [crawl_target] = await discovery._resolve_magnacare_transparency_mrf(
        DIRECTORY_SOURCE, DIRECTORY_URL, {"search_terms": ["example", "later"], "max_targets": 1}, None
    )
    assert crawl_target.metadata["target_file_type"] == "allowed-amounts"
    assert crawl_target.metadata["search_terms"] == ["example"]
    assert crawl_target.metadata["size_bytes"] == 1024 * 1024
    if anonymous_plan:
        assert crawl_target.metadata["plan_info"] == []
    else:
        assert crawl_target.metadata["plan_info"] == [
            {
                "plan_id": "plan-first",
                "plan_id_type": "Group ID",
                "plan_market_type": "group",
                "plan_name": "Example PPO",
            }
        ]
    fetch_text.assert_awaited_once()


@pytest.mark.parametrize("download_by_field", [{}, {"Data": "relative.zip"}])
@pytest.mark.asyncio
async def test_magnacare_rejects_non_http_download_locations__json_request_contract(monkeypatch, download_by_field):
    """A catalog row without an absolute HTTP download fails explicitly."""
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value=_magnacare_row_html__json_request_contract()))
    monkeypatch.setattr(discovery, "_fetch_json", AsyncMock(return_value=download_by_field))
    with pytest.raises(ValueError, match="no downloadable MagnaCare transparency MRF files"):
        await discovery._resolve_magnacare_transparency_mrf(
            DIRECTORY_SOURCE, DIRECTORY_URL, {"search_terms": ["example"]}, None
        )


@pytest.mark.asyncio
async def test_magnacare_keeps_available_files_after_unusable_download__json_request_contract(monkeypatch):
    """An unusable first download cannot suppress a later valid catalog file."""
    html_text = _magnacare_row_html__json_request_contract() + _magnacare_row_html__json_request_contract(
        run_id="run-second", file_name="Second_in-network.zip"
    )
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value=html_text))
    fetch_json = AsyncMock(side_effect=[{}, {"Data": "https://files.example.invalid/second_in-network.zip"}])
    monkeypatch.setattr(discovery, "_fetch_json", fetch_json)
    [crawl_target] = await discovery._resolve_magnacare_transparency_mrf(
        DIRECTORY_SOURCE, DIRECTORY_URL, {"search_terms": ["example"]}, None
    )
    assert crawl_target.url == "https://files.example.invalid/second_in-network.zip"
    assert crawl_target.metadata["run_history_id"] == "run-second"
    assert "runHistoryID=run-second" in fetch_json.await_args.args[0]


@pytest.mark.parametrize("max_targets", [None, 1])
@pytest.mark.asyncio
async def test_ebms_nested_pages_recover_and_bound_targets__json_request_contract(monkeypatch, max_targets):
    """Failed nested pages are skipped while valid targets retain client provenance."""
    client_url = "https://directory.example.invalid/client/index.html"
    failed_url = "https://directory.example.invalid/client/failed/index.html"
    first_url = "https://directory.example.invalid/client/first/index.html"
    later_url = "https://directory.example.invalid/client/later/index.html"
    pages_by_url = {
        DIRECTORY_URL: '<a href="/client/index.html">Example Client</a>',
        client_url: '<a href="failed/index.html">Failed</a><a href="first/index.html">First</a><a href="later/index.html">Later</a>',
        first_url: '<a href="https://files.example.invalid/example_index.json">Table of contents</a>',
        later_url: '<a href="https://files.example.invalid/example_in-network-rates.json.gz">In-network rates</a>',
    }
    requested_urls = []

    async def fetch_text(page_url, **kwargs):
        requested_urls.append(page_url)
        if page_url == failed_url:
            raise OSError("unavailable page")
        return pages_by_url[page_url]

    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    crawl_targets = await discovery._resolve_ebms_caa_directory(
        DIRECTORY_SOURCE, DIRECTORY_URL, {"max_targets": max_targets, "toc_max_bytes": 2048}, None
    )
    assert [crawl_target.url for crawl_target in crawl_targets] == [
        "https://files.example.invalid/example_index.json",
        "https://files.example.invalid/example_in-network-rates.json.gz",
    ][:max_targets]
    assert crawl_targets[0].metadata["ebms_client_url"] == client_url
    assert crawl_targets[0].metadata["ebms_client_label"] == "Example Client"
    assert crawl_targets[0].metadata["ebms_nested_url"] == first_url
    assert crawl_targets[0].metadata["target_max_bytes"] == 2048
    assert requested_urls == [DIRECTORY_URL, client_url, failed_url, first_url] + (
        [later_url] if max_targets is None else []
    )


@pytest.mark.asyncio
async def test_ebms_directory_reports_no_results_after_unavailable_client__json_request_contract(monkeypatch):
    """A failed client page yields an explicit empty-directory result."""
    fetch_text = AsyncMock(side_effect=['<a href="/client/index.html">Client</a>', OSError("unavailable client")])
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    with pytest.raises(ValueError, match="no EBMS CAA directory MRF links"):
        await discovery._resolve_ebms_caa_directory(DIRECTORY_SOURCE, DIRECTORY_URL, {}, None)
    assert [call.args[0] for call in fetch_text.await_args_list] == [
        DIRECTORY_URL,
        "https://directory.example.invalid/client/index.html",
    ]


@pytest.mark.parametrize(
    "file_name,expected_type",
    [
        ("example_index.json", "table-of-contents"),
        ("example_in-network-rates.json.gz", "in-network"),
        ("example_mrf.zip", None),
        ("readme.txt", None),
    ],
)
def test_json_directory_target_requires_recognizable_file_semantics__json_request_contract(file_name, expected_type):
    """Recognized TOCs and typed bodies retain provenance; ambiguous files remain rejected."""
    file_url = "https://files.example.invalid/" + file_name
    crawl_target = discovery._json_mrf_directory_crawl_target(
        DIRECTORY_SOURCE, file_url, directory_url=DIRECTORY_URL, resolver_type="json_mrf_directory"
    )
    if expected_type is None:
        assert crawl_target is None
    else:
        assert crawl_target.url == file_url
        assert crawl_target.resolved_from_url == DIRECTORY_URL
        assert crawl_target.metadata["target_file_type"] == expected_type
        if expected_type == "in-network":
            assert crawl_target.metadata["plan_info"][0]["plan_name"] == file_name


def test_healthcarebluebook_grid_items__fallback_deduplicates_context_links():
    html_text = """
    <section><a href="file:///help">Help</a></section>
    <section>
      <a href="https://files.example.test/employer-in-network.json.gz">Download</a>
      <button data-href="https://files.example.test/employer-in-network.json.gz">Open</button>
      <p>Example Employer in-network</p>
    </section>
    """

    listing_items = discovery._healthcarebluebook_grid_items(
        html_text, base_url="https://mrf.healthcarebluebook.com/Example"
    )

    assert len(listing_items) == 2
    assert listing_items[0]["url"] == ("https://files.example.test/employer-in-network.json.gz")
    assert "Example Employer in-network" in listing_items[0]["label"]
    assert listing_items[1]["url"] is None
    assert listing_items[1]["text"] == listing_items[0]["label"]


@pytest.mark.asyncio
async def test_healthcarebluebook_mrf__filename_identifies_untyped_grid_file(monkeypatch):
    listing_url = "https://mrf.healthcarebluebook.com/Example"
    file_url = "https://files.example.test/employer-in-network.json.gz"
    fetch_text = AsyncMock(
        return_value=f'<div class="grid-item"><a href="{file_url}">Rates</a></div>'
        '<div class="grid-item">Published file</div>'
    )
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    source_by_field = {"source_id": "source-catalog", "display_name": "Example Plan"}

    [crawl_target] = await discovery._resolve_healthcarebluebook_mrf(source_by_field, listing_url, {}, None)

    assert crawl_target.source == source_by_field
    assert crawl_target.url == file_url
    assert crawl_target.resolved_from_url == listing_url
    assert crawl_target.metadata["target_file_type"] == "in-network"
    assert crawl_target.metadata["source_format"] == "gzip"
    assert crawl_target.metadata["healthcarebluebook_file_type"] == "Published file"
    fetch_text.assert_awaited_once_with(listing_url, max_bytes=10 * 1024 * 1024, session=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("nested_url", ["file:///catalog", "https://mrfdata.hmhs.com/"])
async def test_healthcarebluebook_mrf__unresolvable_catalog_is_not_a_file(monkeypatch, nested_url):
    listing_url = "https://mrf.healthcarebluebook.com/Example"
    html_text = (
        f'<div class="grid-item"><a href="{nested_url}">Example Plan</a></div>'
        '<div class="grid-item">Table of Contents</div>'
    )

    requested_urls = []

    async def fetch_text(url, **_kwargs):
        requested_urls.append(url)
        if url == listing_url:
            return html_text
        raise OSError("catalog unavailable")

    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)

    with pytest.raises(ValueError, match="no Healthcare Bluebook MRF links"):
        await discovery._resolve_healthcarebluebook_mrf({"source_id": "source-catalog"}, listing_url, {}, None)


@pytest.mark.asyncio
async def test_html_mrf_with_healthcarebluebook__empty_landing_rejects_no_targets(
    monkeypatch,
):
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value='<a href="/help">Help</a>'))

    with pytest.raises(ValueError, match="no HTML or Healthcare Bluebook MRF links"):
        await discovery._resolve_html_mrf_with_healthcarebluebook(
            {"source_id": "source-catalog"}, "https://catalog.example.test/", {}, None
        )


def test_kaiser_inventory_targets_from_text__bad_sizes_preserve_valid_file_metadata():
    inventory_url = "https://catalog.example.test/innetwork/2026-10_List.txt"
    inventory_text = "\n".join(
        [
            "/state/hi/employer_index.json unknown",
            "/externaldata/vendor/employer_in-network.json.gz 42",
            "/single.json 10",
        ]
    )

    crawl_targets = discovery._kaiser_inventory_targets_from_text(
        {"source_id": "source-catalog"},
        inventory_text,
        inventory_url=inventory_url,
        inventory_month="2026-10",
        category="innetwork",
        resolver={"include_external_data": True},
    )

    assert [crawl_target.metadata["target_file_type"] for crawl_target in crawl_targets] == [
        "table-of-contents",
        "in-network",
    ]
    assert crawl_targets[0].url == "https://catalog.example.test/innetwork/state/hi/employer_index.json"
    assert crawl_targets[0].metadata["kaiser_state_file"] is True
    assert crawl_targets[0].metadata["kaiser_region_code"] == "hi"
    assert "size_bytes" not in crawl_targets[0].metadata
    assert crawl_targets[0].metadata["target_max_bytes"] == 5 * 1024 * 1024
    assert crawl_targets[1].metadata["kaiser_external_data"] is True
    assert crawl_targets[1].metadata["size_bytes"] == 42
    assert crawl_targets[1].metadata["inventory_url"] == inventory_url


def test_kaiser_inventory_months__defaults_and_duplicate_offsets_keep_order():
    current = discovery.dt.datetime(2026, 1, 16, 12, 0, 0)

    assert discovery._kaiser_inventory_months({}, now=current) == [
        "2026-01",
        "2025-12",
    ]
    assert discovery._kaiser_inventory_months({"month_offsets": [0, 0, -1, -1]}, now=current) == ["2026-01", "2025-12"]


@pytest.mark.asyncio
async def test_kaiser_monthly_inventory__empty_categories_exhaust_month_fallbacks(
    monkeypatch,
):
    fetch_text = AsyncMock(return_value="No files published")
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    monkeypatch.setattr(discovery, "_utc_now", lambda: discovery.dt.datetime(2026, 1, 16, 12, 0, 0))

    with pytest.raises(ValueError, match="no Kaiser MRF inventory targets"):
        await discovery._resolve_kaiser_monthly_inventory(
            {"source_id": "source-catalog"},
            "https://catalog.example.test/transparency",
            {"base_url": "https://catalog.example.test/pricing"},
            None,
        )

    assert [call.args[0] for call in fetch_text.await_args_list] == [
        "https://catalog.example.test/pricing/innetwork/2026-01_List.txt",
        "https://catalog.example.test/pricing/innetwork/2025-12_List.txt",
        "https://catalog.example.test/pricing/outofnetwork/2026-01_List.txt",
        "https://catalog.example.test/pricing/outofnetwork/2025-12_List.txt",
    ]


@pytest.mark.asyncio
async def test_highmark_script_crawl_targets__real_script_preserves_rendered_provenance(
    monkeypatch,
):
    fetch_text = AsyncMock(
        return_value="""
        {regName: 'Ignored', dl: '   ', dt: 'No path'}
        {regName: 'East', dl: '/?FIRST_DAY_CUR_MONTH_employer_index.json?x=1&amp;y=2', dt: 'Employer index'}
        {regName: 'West', dl: '/FIRST_DAY_CUR_MONTH_other_index.json', dt: ''}
        """
    )
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    monkeypatch.setattr(discovery, "_utc_now", lambda: discovery.dt.datetime(2026, 10, 16, 12, 0, 0))
    source_by_field = {"source_id": "source-catalog", "display_name": "Example Plan"}

    crawl_targets = await discovery._highmark_script_crawl_targets(
        source_by_field,
        "https://mrfdata.hmhs.com/",
        None,
        {"script_path": "/assets/catalog.js", "max_bytes": 2048},
    )

    assert [crawl_target.label for crawl_target in crawl_targets] == ["Employer index", "Example Plan"]
    assert crawl_targets[0].url == ("https://mrfdata.hmhs.com/2026-10-01_employer_index.json?x=1&y=2")
    assert crawl_targets[0].metadata["raw_path"] == ("/?FIRST_DAY_CUR_MONTH_employer_index.json?x=1&y=2")
    assert crawl_targets[0].metadata["rendered_path"] == ("/2026-10-01_employer_index.json?x=1&y=2")
    assert [crawl_target.metadata["region"] for crawl_target in crawl_targets] == ["East", "West"]
    assert all(crawl_target.source == source_by_field for crawl_target in crawl_targets)
    assert all(
        crawl_target.resolved_from_url == "https://mrfdata.hmhs.com/assets/catalog.js" for crawl_target in crawl_targets
    )
    fetch_text.assert_awaited_once_with("https://mrfdata.hmhs.com/assets/catalog.js", max_bytes=2048, session=None)


@pytest.mark.asyncio
async def test_highmark_script_crawl_targets__malformed_script_reports_script_url(
    monkeypatch,
):
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value="var records = [];"))

    with pytest.raises(
        ValueError, match=r"no Highmark HMHS index links found in https://mrfdata.hmhs.com/js/script.js"
    ):
        await discovery._highmark_script_crawl_targets(
            {"source_id": "source-catalog"}, "https://mrfdata.hmhs.com/", None, {}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("resolver_type", ["direct_toc", "direct_mrf_body"])
async def test_configured_crawl_targets__direct_resolver_rejects_landing_pages(
    resolver_type,
):
    with pytest.raises(ValueError, match="no direct MRF"):
        await discovery._configured_crawl_targets(
            {"source_id": "source-catalog"},
            "https://catalog.example.test/transparency",
            None,
            {"type": resolver_type},
        )


@pytest.mark.asyncio
async def test_healthspace__missing_session_releases_page_without_posting(monkeypatch):
    page_response = _TrackedResponse(b"<html>No public session</html>", content_type="text/html")
    session = _Session(page_response)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    with pytest.raises(ValueError, match="did not expose a session id"):
        await discovery._resolve_healthspace_machine_readable_files(
            {}, "https://portal.example.invalid/files", {"page_max_bytes": 1234}, session
        )
    assert page_response.released
    assert session.post_calls == []
    assert session.get_calls == [("https://portal.example.invalid/files", {"allow_redirects": True})]


def _provider_healthspace_soap():
    return """
    <MachineReadableFiles>
      <MachineReadableFile FileName="Missing URL" />
      <MachineReadableFile FilePathURL="https://files.example.invalid/download" FileName="Synthetic Document" />
      <MachineReadableFile FilePathURL="https://files.example.invalid/in-network.json" FileName="Synthetic rates" />
      <MachineReadableFile FilePathURL="https://files.example.invalid/in-network.json" FileName="Duplicate rates" />
    </MachineReadableFiles>
    """


@pytest.mark.asyncio
async def test_healthspace__soap_filters_incomplete_and_duplicate_files(monkeypatch):
    page_response = _TrackedResponse(
        b"<script>sessionStorage['HealthspaceSessionId'] = 'synthetic&session';</script>", content_type="text/html"
    )
    soap_response = _TrackedResponse(_provider_healthspace_soap().encode(), content_type="text/xml")
    session = SimpleNamespace(get=Mock(return_value=page_response), post=Mock(return_value=soap_response))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    [crawl_target] = await discovery._resolve_healthspace_machine_readable_files(
        {},
        "https://portal.example.invalid/files",
        {"service_path": "/soap", "operation_id": "Synthetic.Operation"},
        session,
    )
    assert crawl_target.url == "https://files.example.invalid/in-network.json"
    assert crawl_target.resolved_from_url == "https://portal.example.invalid/soap"
    assert crawl_target.metadata["plan_info"] == []
    assert page_response.released and soap_response.released
    post_request = session.post.call_args
    assert post_request.args == ("https://portal.example.invalid/soap",)
    envelope = ElementTree.fromstring(post_request.kwargs["data"])
    fields_by_name = {element.tag.rsplit("}", 1)[-1]: element.text for element in envelope.iter()}
    assert fields_by_name["sessionId"] == "synthetic&session"
    assert fields_by_name["operationId"] == "Synthetic.Operation"
    assert post_request.kwargs["headers"]["Content-Type"] == "text/xml"
    assert post_request.kwargs["allow_redirects"] is True


@pytest.mark.parametrize("soap_text", ["<MachineReadableFiles />", "<MachineReadableFiles>"])
@pytest.mark.asyncio
async def test_healthspace__empty_or_malformed_soap_releases_both_responses(monkeypatch, soap_text):
    page_response = _TrackedResponse(b"HealthspaceSessionId', 'synthetic'", content_type="text/html")
    soap_response = _TrackedResponse(soap_text.encode(), content_type="text/xml")
    session = SimpleNamespace(get=Mock(return_value=page_response), post=Mock(return_value=soap_response))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    expected_error = ElementTree.ParseError if soap_text.endswith(">") and not soap_text.endswith("/>") else ValueError
    with pytest.raises(expected_error):
        await discovery._resolve_healthspace_machine_readable_files(
            {}, "https://portal.example.invalid/files", {}, session
        )
    assert page_response.released and soap_response.released
    session.post.assert_called_once()


def _provider_healthez_html():
    return """
    <a href="/contact">Contact</a>
    <a href="/api/outbound/latest?fileType=unknown">Other document</a>
    <a href="/api/outbound/latest?fileType=inNetwork&amp;groupName=Synthetic=AP">Legacy AP</a>
    <a href="/api/outbound/latest?fileType=inNetwork&amp;groupName=Synthetic&amp;network=AP">Duplicate AP</a>
    <a href="/api/outbound/latest?fileType=inNetwork&amp;groupName=Synthetic=AP&amp;network=AE">Explicit AE</a>
    <a href="/api/outbound/latest?fileType=outOfNetwork&amp;groupName=Synthetic=XX">Synthetic allowed amounts</a>
    """


@pytest.mark.parametrize("max_targets", [None, 1])
@pytest.mark.asyncio
async def test_healthez__normalization_preserves_explicit_network_and_deduplicates(monkeypatch, max_targets):
    response = _TrackedResponse(_provider_healthez_html().encode(), content_type="text/html")
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    crawl_targets = await discovery._resolve_healthez_benefits_mrf(
        {}, "https://adapter.example.invalid/plans", {"max_targets": max_targets}, _Session(response)
    )
    assert [crawl_target.label for crawl_target in crawl_targets] == (
        ["HealthEZ AP"] if max_targets else ["HealthEZ AP", "HealthEZ AE", "Synthetic allowed amounts"]
    )
    assert parse_qs(urlsplit(crawl_targets[0].url).query) == {
        "fileType": ["inNetwork"],
        "groupName": ["Synthetic"],
        "network": ["AP"],
    }
    if not max_targets:
        assert parse_qs(urlsplit(crawl_targets[1].url).query)["network"] == ["AE"]
        assert parse_qs(urlsplit(crawl_targets[2].url).query)["groupName"] == ["Synthetic=XX"]
        assert crawl_targets[2].metadata["target_file_type"] == "allowed-amounts"
    assert response.released


@pytest.mark.asyncio
async def test_healthez__no_supported_files_releases_response_and_fails(monkeypatch):
    response = _TrackedResponse(b'<a href="/contact">Contact</a>', content_type="text/html")
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    with pytest.raises(ValueError, match="no HealthEZ MRF targets"):
        await discovery._resolve_healthez_benefits_mrf(
            {}, "https://adapter.example.invalid/plans", {}, _Session(response)
        )
    assert response.released


def _provider_sparq_metadata():
    return {
        "files": [
            {"fileSchema": "IN_NETWORK_RATES", "fileName": "Missing path"},
            {"filePath": "unclassified.json", "fileName": "Missing schema"},
            {"filePath": "rates.json.gz", "fileSchema": "IN_NETWORK_RATES", "fileName": "Synthetic rates"},
        ]
    }


@pytest.mark.parametrize("metadata_payload_by_field", [{"files": []}, _provider_sparq_metadata()])
@pytest.mark.asyncio
async def test_healthsparq__direct_metadata_keeps_fallback_or_valid_files(monkeypatch, metadata_payload_by_field):
    metadata_url = "https://files.example.invalid/prd/mrf/SYNTH_I/SYNTH/latest_metadata.json"
    response = _TrackedResponse(json.dumps(metadata_payload_by_field).encode())
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    [crawl_target] = await discovery._resolve_healthsparq_direct_metadata({}, metadata_url, {}, _Session(response))
    if metadata_payload_by_field["files"]:
        assert crawl_target.url == "https://files.example.invalid/prd/mrf/SYNTH_I/SYNTH/rates.json.gz"
        assert crawl_target.metadata["target_file_type"] == "in-network"
    else:
        assert crawl_target.url == metadata_url
        assert crawl_target.metadata["landing_reason"] == "healthsparq_metadata_unexpanded"
    assert response.released


@pytest.mark.parametrize(
    "url",
    [
        "https://files.example.invalid/latest_metadata.json",
        "https://files.example.invalid/prd/mrf/%20/SYNTH/latest_metadata.json",
    ],
)
@pytest.mark.asyncio
async def test_healthsparq__invalid_metadata_route_fails_before_http_request(url):
    session = SimpleNamespace(get=Mock(side_effect=AssertionError("unexpected request")))
    with pytest.raises(ValueError, match="metadata URL must include"):
        await discovery._resolve_healthsparq_direct_metadata({}, url, {}, session)
    session.get.assert_not_called()


@pytest.mark.parametrize(
    ("plan_name", "query", "expected_name", "expected_sponsor"),
    [
        ("", "Synthetic", None, None),
        ("医院 Gold PPO", "医院", "医院 Gold PPO", "医院"),
        ("Orchard Synthetic PPO", "Synthetic Orchard", "Orchard Synthetic PPO", "Synthetic Orchard"),
    ],
)
def test_healthsparq__plan_labels_preserve_empty_unicode_and_reordered_names(
    plan_name, query, expected_name, expected_sponsor
):
    [plan_by_field] = discovery._healthsparq_plan_info(
        {"reportingPlans": [{"planName": plan_name}]}, target_query=query
    )
    assert plan_by_field["plan_name"] == expected_name
    assert plan_by_field.get("plan_sponsor_name") == expected_sponsor
    assert plan_by_field.get("company_name") == expected_sponsor


@pytest.mark.parametrize("metadata_case", ["files", "empty", "empty_tenant"])
@pytest.mark.asyncio
async def test_healthsparq__service_route_uses_real_json_requests_and_fallback(monkeypatch, metadata_case):
    insurer_code = "---" if metadata_case == "empty_tenant" else "SYNTH_I"
    public_url = f"https://adapter.example.invalid/public?insurerCode={insurer_code}&brandCode=SYNTH"
    metadata_url = f"https://files.example.invalid/prd/mrf/{insurer_code}/SYNTH/latest_metadata.json"
    login_response = _TrackedResponse(b"{}")
    api_response = _TrackedResponse(json.dumps({"url": metadata_url}).encode())
    metadata_response = _TrackedResponse(
        json.dumps({"files": []} if metadata_case == "empty" else _provider_sparq_metadata()).encode()
    )
    session = SimpleNamespace(
        get=Mock(side_effect=[login_response, metadata_response]), post=Mock(return_value=api_response)
    )
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    resolver_by_field = {"login_path": "login", "mrf_all_path": "mrf-all"}
    if metadata_case == "empty_tenant":
        resolver_by_field["metadata_url_template"] = "https://files.example.invalid/{tenant}/latest_metadata.json"
    [crawl_target] = await discovery._resolve_healthsparq_public_mrf({}, public_url, resolver_by_field, session)
    service_url = "https://adapter.example.invalid/healthsparq/public/service/mrf-all"
    if metadata_case == "empty":
        assert crawl_target.url == service_url
        assert crawl_target.metadata["metadata_url"] == metadata_url
        assert crawl_target.metadata["landing_reason"] == "healthsparq_metadata_unexpanded"
    else:
        assert crawl_target.url == metadata_url.rsplit("/", 1)[0] + "/rates.json.gz"
        assert crawl_target.metadata["healthsparq_public_url"] == service_url
    session.post.assert_called_once_with(
        service_url, json={"insurerCode": insurer_code, "brandCode": "SYNTH"}, allow_redirects=True
    )
    login_url = session.get.call_args_list[0].args[0]
    assert urlsplit(login_url).path == "/healthsparq/public/service/login"
    login_query_by_field = parse_qs(urlsplit(login_url).query)
    assert login_query_by_field["insurerCode"] == [insurer_code]
    assert login_query_by_field["brandCode"] == ["SYNTH"]
    assert login_query_by_field["_"][0].isdigit()
    assert all(response.released for response in (login_response, api_response, metadata_response))


@pytest.mark.asyncio
async def test_healthsparq__missing_service_path_fails_before_http_request():
    session = SimpleNamespace(get=Mock(side_effect=AssertionError("unexpected request")))
    with pytest.raises(ValueError, match="resolver missing login_path"):
        await discovery._resolve_healthsparq_public_mrf(
            {}, "https://adapter.example.invalid/public?insurerCode=SYNTH_I&brandCode=SYNTH", {}, session
        )
    session.get.assert_not_called()


@pytest.mark.parametrize(
    "config_by_field",
    [
        {"API_ENDPOINT": "https://api.example.invalid/"},
        {"X_API_KEY": "public"},
    ],
)
@pytest.mark.asyncio
async def test_providence__incomplete_config_releases_response_before_group_requests(monkeypatch, config_by_field):
    response = _TrackedResponse(json.dumps(config_by_field).encode())
    session = _Session(response)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    with pytest.raises(ValueError, match="config did not include API endpoint/key"):
        await discovery._resolve_providence_mrf_api({}, "https://adapter.example.invalid/", {}, session)
    assert session.get_calls == [("https://adapter.example.invalid/config.json", {"allow_redirects": True})]
    assert response.released


def _provider_providence_responses():
    return [
        {"API_ENDPOINT": "https://api.example.invalid/", "X_API_KEY": "public"},
        {"groups": ["invalid", {"group-id": ""}, {"group-id": "one"}, {"group-id": "two"}]},
        {"groups": [{"group-id": "one"}, {"group-id": "three"}]},
        {
            "groups": [
                "invalid",
                {
                    "group-id": "one",
                    "group-name": "Synthetic Group",
                    "tocs": [
                        "invalid",
                        {"TOC_URL": "https://files.example.invalid/help"},
                        {"TOC_URL": "https://files.example.invalid/a_index.json"},
                        {"TOC_URL": "https://files.example.invalid/b_index.json"},
                    ],
                },
            ]
        },
    ]


@pytest.mark.asyncio
async def test_providence__mixed_records_keep_group_and_target_limits(monkeypatch):
    responses = [
        _TrackedResponse(json.dumps(payload_by_field).encode()) for payload_by_field in _provider_providence_responses()
    ]
    session = SimpleNamespace(get=Mock(side_effect=responses))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    [crawl_target] = await discovery._resolve_providence_mrf_api(
        {},
        "https://adapter.example.invalid/",
        {"group_queries": [" ", "Synthetic & Family", "Other"], "max_groups": 1, "max_targets": 1},
        session,
    )
    assert crawl_target.url == "https://files.example.invalid/a_index.json"
    assert crawl_target.metadata["group_id"] == "one"
    assert crawl_target.metadata["plan_info"][0]["plan_name"] == "Synthetic Group"
    assert [request.args[0] for request in session.get.call_args_list] == [
        "https://adapter.example.invalid/config.json",
        "https://api.example.invalid/group/?groupname=Synthetic+%26+Family",
        "https://api.example.invalid/group/?groupname=Other",
        "https://api.example.invalid/toc/?groupid=one",
    ]
    assert all(request.kwargs["headers"] == {"X-API-Key": "public"} for request in session.get.call_args_list[1:])
    assert all(response.released for response in responses)


@pytest.mark.parametrize("has_group", [False, True])
@pytest.mark.asyncio
async def test_providence__no_group_or_toc_fails_after_releasing_responses(monkeypatch, has_group):
    payloads = [
        {"API_ENDPOINT": "https://api.example.invalid/", "X_API_KEY": "public"},
        {"groups": [{"group-id": "one"}] if has_group else []},
    ]
    if has_group:
        payloads.append({"groups": [{"group-id": "one", "tocs": []}]})
    responses = [_TrackedResponse(json.dumps(payload_by_field).encode()) for payload_by_field in payloads]
    session = SimpleNamespace(get=Mock(side_effect=responses))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    with pytest.raises(ValueError, match="no Providence MRF API targets"):
        await discovery._resolve_providence_mrf_api({}, "https://adapter.example.invalid/", {}, session)
    assert all(response.released for response in responses)
    assert session.get.call_args_list[-1].args[0] == (
        "https://api.example.invalid/toc/?groupid=one"
        if has_group
        else "https://api.example.invalid/group/?groupname=Individual+and+Family+Plans"
    )
