# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public directory registration, issued cursors and durable acquisition boundaries."""

import asyncio
import dataclasses
import importlib
import urllib.parse

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_christus as seeds
from tests.test_process_provider_directory_fhir_unit import (
    _assert_no_rest_page_prefetch_task,
    _parallel_resource_source,
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _RestPageHarness,
    _source_http_test_session,
    unit_checkpoint_catalog,
    unit_source_factory,
)

importer = importlib.import_module("process.provider_directory_fhir")
BASE = seeds.API_BASE
SOURCE_ID = "pdfhir_b674847fc79d2b2ea1f4e986"


def test_christus_seed_matches_runnable_catalog_and_profile(monkeypatch):
    """Keep seed discovery, stable identity and executable collection scope aligned."""
    monkeypatch.setattr(importer, "_remote_supplemental_catalogs", lambda **_kwargs: ([], {}))
    rows, metrics = importer._seed_rows_from_supplemental_catalogs(source_query="CHRISTUS")
    assert len(rows) == 1
    source = importer._source_row_from_seed(rows[0])
    entry = next(
        item for item in provider_directory_source_catalog()["items"] if item["entry_id"] == "christus-health-plan"
    )
    assert source["source_id"] == SOURCE_ID
    assert source["canonical_api_base"] == entry["canonical_base"] == BASE
    assert entry["source_ids"] == [SOURCE_ID]
    assert entry["runnable"] and entry["profile_enabled"]
    assert entry["resources"] == list(seeds.RESOURCES)
    assert metrics["catalogs"]["christus_health_plan"]["rows"] == 1
    for resource in seeds.RESOURCES:
        assert importer._resource_start_url(source, resource, page_count=1000) == f"{BASE}/{resource}?_count=100"
    assert importer._resource_start_url(source, "Endpoint", page_count=100) is None
    assert not importer._is_seed_row_query_match(rows[0], "unrelated payer")


def test_christus_uses_the_existing_durable_checkpoint_scope():
    """Actual seed eligibility must enable retry lineage and bounded resource overlap."""
    source = importer._source_row_from_seed(seeds.christus_seed_rows()[0])
    context = importer._pagination_checkpoint_context(source, [SOURCE_ID], run_id="run-first", retry_of_run_id=None)
    assert context is not None
    assert context.canonical_api_base == BASE and context.source_ids == (SOURCE_ID,)
    assert context.owner_run_id == context.acquisition_root_run_id == "run-first"
    assert importer._pagination_checkpoint_scope_identity(source, [SOURCE_ID]) == (BASE, context.source_scope_hash)


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [({}, 7), ({"source_concurrency": 2}, 1), ({"checkpointing_enabled": False}, 1)],
)
def test_christus_eight_cursor_request_uses_shared_isolated_scan_gates(monkeypatch, overrides, expected):
    source = _parallel_resource_source()
    source.update(importer._source_row_from_seed(seeds.christus_seed_rows()[0]))
    source["_partition_checkpoint_source_ids"] = (SOURCE_ID,)
    source["_pagination_checkpoint_context"] = dataclasses.replace(
        source["_pagination_checkpoint_context"], canonical_api_base=BASE, source_ids=(SOURCE_ID,)
    )
    _patch_resource_scan_storage(monkeypatch, True)
    options = _resource_concurrency_options(resource_types=seeds.RESOURCES, requested_concurrency=8, **overrides)
    with _source_http_test_session():
        assert importer._effective_resource_scan_concurrency(source, options) == expected


def test_christus_seed_metadata_is_not_shared_between_attempts():
    first = seeds.christus_seed_rows()[0]
    first["metadata_json"]["provider_directory_supported_resources"].clear()
    assert seeds.christus_seed_rows()[0]["metadata_json"]["provider_directory_supported_resources"] == list(
        seeds.RESOURCES
    )


@pytest.mark.parametrize("resource_type", ["PractitionerRole", "OrganizationAffiliation"])
def test_christus_membership_keeps_source_locations_and_networks_without_inferred_npi(resource_type):
    payload = {
        "resourceType": resource_type,
        "id": "membership-a",
        "location": [{"reference": "Location/office-a"}],
    }
    if resource_type == "PractitionerRole":
        payload["practitioner"] = {"reference": "Practitioner/practitioner-a"}
        payload["extension"] = [
            {
                "url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference",
                "valueReference": {"reference": "Organization/network-a"},
            }
        ]
    else:
        payload["participatingOrganization"] = {"reference": "Organization/facility-a"}
        payload["network"] = [{"reference": "Organization/network-a"}]
    _, row = importer.parse_fhir_resource(SOURCE_ID, payload, run_id="run_1")
    assert row["source_id"] == SOURCE_ID
    assert row["location_refs"] == ["Location/office-a"]
    assert row["network_refs"] == ["Organization/network-a"]
    assert row.get("npi") is None


@pytest.mark.parametrize(
    ("resource", "query"),
    [
        ("Practitioner", "_count=100&_cursor=opaque%2Bafter-10000+%2F%3D"),
        ("Organization", "_count=100&_offset=100"),
        ("Practitioner", "_resume=opaque%2B%2F%3D&version=next"),
        ("Practitioner", "_count=100&_resume=opaque&family=synthetic"),
    ],
)
def test_christus_continuations_preserve_issued_url_without_sort_or_offset_inference(resource, query):
    current = f"{BASE}/{resource}?_count=100"
    following = f"{BASE}/{resource}?{query}"
    source = importer._source_row_from_seed(seeds.christus_seed_rows()[0])
    assert importer._resolved_fhir_next_url(source, current, following) == following
    assert "_sort" not in importer._resource_start_url(source, resource, page_count=100)


@pytest.mark.parametrize(
    "following",
    [
        "https://outside.example/fhir/Practitioner?_count=100&_cursor=opaque",
        f"{BASE}/Location?_count=100&_cursor=opaque",
        f"{BASE}/Practitioner?_count=100&_cursor=opaque#fragment",
        BASE.replace("https://", "https://user:password@") + "/Practitioner?_count=100&_cursor=opaque",
        BASE.replace("https://", "http://") + "/Practitioner?_count=100&_cursor=opaque",
    ],
)
def test_christus_rejects_continuation_scope_changes(following):
    with pytest.raises(ValueError, match="untrusted_christus_pagination_link"):
        seeds.resolved_christus_next_url(f"{BASE}/Practitioner?_count=100", following)


@pytest.mark.parametrize("port", ["invalid", "65536", "-1"])
def test_christus_rejects_malformed_authority_ports(port):
    """An invalid authority must never enter either byte-preserving request path."""
    parsed = urllib.parse.urlsplit(BASE)
    following = parsed._replace(netloc=f"{parsed.hostname}:{port}").geturl() + "/Practitioner?_count=100&_cursor=opaque"
    assert seeds.is_request_url(following) is False
    with pytest.raises(ValueError, match="untrusted_christus_pagination_link"):
        seeds.resolved_christus_next_url(f"{BASE}/Practitioner?_count=100", following)


def _christus_page_scan(monkeypatch, *, prefetch=False, pages=3):
    """Reuse the checkpoint harness with this source's opaque URL family."""
    scan = _RestPageHarness(monkeypatch, pages=pages, prefetch=prefetch)
    scan.urls = [f"{BASE}/Practitioner?_count=100"] + [
        f"{BASE}/Practitioner?_count=100&_cursor=opaque-{page}%2B%2F%3D" for page in range(1, pages)
    ]
    scan.source = importer._source_row_from_seed(seeds.christus_seed_rows()[0])
    scan.checkpoint_context = dataclasses.replace(
        scan.checkpoint_context, canonical_api_base=BASE, source_ids=(SOURCE_ID,)
    )
    scan.responses = {}
    for page, url in enumerate(scan.urls):
        payload = {"resourceType": "Bundle", "type": "searchset", "entry": []}
        if page < pages - 1:
            payload["entry"] = [{"resource": {"resourceType": "Practitioner", "id": f"practitioner-{page}"}}]
            payload["link"] = [{"relation": "next", "url": scan.urls[page + 1]}]
        scan.responses[url] = (200, payload, None, 5)
    return scan


@pytest.mark.usefixtures("unit_checkpoint_catalog")
@pytest.mark.parametrize("prefetch", [False, True])
async def test_christus_full_pages_accept_missing_later_totals_and_final_empty_page(monkeypatch, prefetch):
    scan = _christus_page_scan(monkeypatch, prefetch=prefetch)
    for page, url in enumerate(scan.urls[:2]):
        payload = scan.responses[url][1]
        payload["entry"] = [
            {"resource": {"resourceType": "Practitioner", "id": f"practitioner-{page}-{number}"}}
            for number in range(100)
        ]
    scan.responses[scan.urls[0]][1]["total"] = 200
    result = await scan.run()
    assert result.complete and result.rows_written == 200 and result.pages_fetched == 3
    assert scan.requests == scan.urls
    assert scan.checkpoint_urls == [scan.urls[1], scan.urls[2], None]
    assert result.page_prefetch_eligible is prefetch


@pytest.mark.usefixtures("unit_checkpoint_catalog")
async def test_christus_short_page_with_next_is_not_a_terminal_page(monkeypatch):
    scan = _christus_page_scan(monkeypatch)
    result = await scan.run()
    assert result.complete and result.rows_written == 2
    assert scan.requests == scan.urls


@pytest.mark.usefixtures("unit_checkpoint_catalog")
async def test_christus_resumes_opaque_cursor_after_ten_thousand_records(monkeypatch):
    scan = _christus_page_scan(monkeypatch, pages=2)
    scan.resume_state = importer.PaginationResumeState(scan.urls[1], 100, 10000, (), resumed=True)
    result = await scan.run()
    assert result.complete
    assert scan.requests == [scan.urls[1]]
    assert scan.checkpoints[-1]["rows_processed"] == 10000
    assert scan.checkpoints[-1]["pages_processed"] == 101


@pytest.mark.usefixtures("unit_checkpoint_catalog")
async def test_christus_repeated_opaque_cursor_does_not_complete(monkeypatch):
    scan = _christus_page_scan(monkeypatch)
    scan.responses[scan.urls[1]][1]["link"][0]["url"] = scan.urls[1]
    result = await scan.run()
    assert not result.complete and result.error == "pagination loop detected"
    assert scan.requests == scan.urls[:2]


@pytest.mark.usefixtures("unit_checkpoint_catalog")
async def test_christus_failed_checkpoint_cancels_prefetch_before_consumption(monkeypatch):
    scan = _christus_page_scan(monkeypatch, prefetch=True, pages=2)

    async def hold_prefetch(page):
        if page == 1:
            await asyncio.Event().wait()

    async def fail_checkpoint(_checkpoint):
        await asyncio.wait_for(scan.fetch_started[1].wait(), timeout=1)
        raise RuntimeError("synthetic checkpoint failure")

    scan.fetch_hook = hold_prefetch
    scan.checkpoint_hook = fail_checkpoint
    with pytest.raises(RuntimeError, match="synthetic checkpoint failure"):
        await asyncio.wait_for(scan.run(), timeout=2)
    assert scan.fetch_cancelled[1].is_set()
    assert len(scan.write_attempts) == 1
    _assert_no_rest_page_prefetch_task()


async def _fetch_christus_transport(local_base, request_url, transport):
    """Exercise the same public fetch entry points used by the three transport modes."""
    if transport == "sync":
        session_token = importer._SOURCE_HTTP_SESSION.set(None)
        try:
            return await importer._fetch_json(request_url, timeout=2)
        finally:
            importer._SOURCE_HTTP_SESSION.reset(session_token)
    async with aiohttp.ClientSession() as source_session:
        source_context = (
            importer._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION if transport == "pinned" else importer._SOURCE_HTTP_SESSION
        )
        session_token = source_context.set(source_session)
        try:
            if transport == "pinned":
                return await importer._fetch_source_json_once({"api_base": local_base}, request_url, timeout=2)
            return await importer._fetch_json(request_url, timeout=2)
        finally:
            source_context.reset(session_token)


@pytest.mark.parametrize("transport", ["ordinary", "pinned", "sync"])
async def test_christus_http_transport_preserves_encoded_cursor_query(monkeypatch, transport):
    observed_queries = []

    async def page(request):
        observed_queries.append(request.raw_path.split("?", 1)[1])
        return web.json_response({"resourceType": "Bundle", "entry": []})

    app = web.Application()
    app.router.add_get("/fhir/Practitioner", page)
    server = TestServer(app)
    await server.start_server()
    local_base = str(server.make_url("/fhir"))
    monkeypatch.setattr(seeds, "API_BASE", local_base)
    query = "_count=100&_cursor=opaque%2Bplus+slash%2Fpad%3D"
    try:
        fetch_result = await _fetch_christus_transport(local_base, f"{local_base}/Practitioner?{query}", transport)
    finally:
        await server.close()
    assert fetch_result[0] == 200
    assert observed_queries == [query]


@pytest.mark.parametrize("transport", ["ordinary", "pinned", "sync"])
@pytest.mark.parametrize("destination_kind", ["origin", "collection"])
async def test_christus_transport_refuses_redirect_before_contacting_destination(
    monkeypatch, transport, destination_kind
):
    """An issued collection URL cannot redirect a fetch into a different trust scope."""
    destination_requests = []
    redirect_location_by_kind = {}

    async def destination(request):
        destination_requests.append(request.path)
        return web.json_response({"resourceType": "Bundle", "entry": []})

    async def redirect(_request):
        return web.json_response(
            {"resourceType": "OperationOutcome"},
            status=302,
            headers={"Location": redirect_location_by_kind[destination_kind]},
        )

    source_app = web.Application()
    source_app.router.add_get("/fhir/Practitioner", redirect)
    source_app.router.add_get("/fhir/Location", destination)
    destination_app = web.Application()
    destination_app.router.add_get("/landing", destination)
    async with TestServer(destination_app) as outside_server, TestServer(source_app) as source_server:
        local_base = str(source_server.make_url("/fhir"))
        monkeypatch.setattr(seeds, "API_BASE", local_base)
        redirect_location_by_kind.update(
            origin=str(outside_server.make_url("/landing")), collection=str(source_server.make_url("/fhir/Location"))
        )
        fetch_result = await _fetch_christus_transport(local_base, f"{local_base}/Practitioner?_count=100", transport)
    assert fetch_result[0] == 302
    assert fetch_result[1]["resourceType"] != "Bundle"
    assert destination_requests == []


@pytest.mark.parametrize("query", ["_count=100&_cursor=opaque%2B%2F%3D", "_count=100&_resume=opaque%2B%2F%3D"])
def test_christus_rejected_cursor_never_retries_a_rewritten_count(query):
    issued = f"{BASE}/Practitioner?{query}"
    assert importer._source_fetch_candidate_urls(issued) == [issued]
