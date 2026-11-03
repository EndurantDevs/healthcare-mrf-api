# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed authenticated collections retain bounded shared transport."""

import asyncio
import base64
import dataclasses
import http.client
import importlib
import io
import ssl
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from tests.test_process_provider_directory_fhir_unit import (
    _parallel_resource_source,
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _stub_limited_process_data,
)

directory = importlib.import_module("process.provider_directory_fhir")
selection = directory.source_selection


def _reviewed_source(monkeypatch, base, token_url):
    monkeypatch.setattr(directory.anthem, "API_BASE", base)
    monkeypatch.setattr(directory.anthem, "TOKEN_URL", token_url)
    credential_spec_by_field = {
        "oauth2": {"token_url": token_url, "client_id": "synthetic", "client_secret": "synthetic"}
    }
    monkeypatch.setattr(directory, "_credential_spec_for_source", lambda _source: credential_spec_by_field)
    monkeypatch.setattr(directory, "_OAUTH_TOKEN_CACHE", {})
    source_by_field = {"api_base": base, "requires_registration": True, "_provider_directory_request_pacing_state": {}}
    return source_by_field, credential_spec_by_field


def _concurrent_auth_app():
    """Expose real token and collection routes with observable in-flight bounds."""
    admitted_event, release_event = asyncio.Event(), asyncio.Event()
    counters_by_name = {"tokens": 0, "requests": 0, "active": 0, "maximum": 0}
    observed_paths = []

    async def token(request):
        counters_by_name["tokens"] += 1
        assert request.headers["Authorization"] == "Basic " + base64.b64encode(b"synthetic:synthetic").decode()
        assert (await request.post())["grant_type"] == "client_credentials"
        await asyncio.sleep(0.01)
        return web.json_response({"access_token": "synthetic-token", "expires_in": 3600})

    async def page(request):
        assert request.headers["Authorization"] == "Bearer synthetic-token"
        observed_paths.append(request.raw_path)
        counters_by_name["active"] += 1
        counters_by_name["requests"] += 1
        counters_by_name["maximum"] = max(counters_by_name["maximum"], counters_by_name["active"])
        if counters_by_name["active"] == 8:
            admitted_event.set()
        try:
            await release_event.wait()
            return web.json_response({"resourceType": "Bundle", "type": "searchset", "entry": []})
        finally:
            counters_by_name["active"] -= 1

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    http_app.router.add_get("/fhir/{resource}", page)
    return http_app, admitted_event, release_event, counters_by_name, observed_paths


async def test_eight_authenticated_sends_share_token_refresh_and_bounded_connections(monkeypatch):
    """Share token refresh and keep the eight-request connection ceiling."""
    http_app, admitted_event, release_event, counters_by_name, observed_paths = _concurrent_auth_app()
    async with TestServer(http_app) as server:
        base = str(server.make_url("/fhir"))
        source_by_field, _spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        async with directory._source_http_session_scope(force=True, request_concurrency=8):
            tasks = [
                asyncio.create_task(
                    directory._fetch_source_json_once(
                        dict(source_by_field), f"{base}/{index}?_count=1&_offset=0002", timeout=2
                    )
                )
                for index in range(9)
            ]
            try:
                await asyncio.wait_for(admitted_event.wait(), 3)
                await asyncio.sleep(0)
                assert counters_by_name["requests"] == 8
                assert counters_by_name["tokens"] == 1
                release_event.set()
                assert all(fetch_result[0] == 200 for fetch_result in await asyncio.gather(*tasks))
                for key, (access_token, _expiry) in directory._OAUTH_TOKEN_CACHE.items():
                    directory._OAUTH_TOKEN_CACHE[key] = (access_token, 0)
                fetch_results = await asyncio.gather(
                    *(
                        directory._fetch_source_json_once(source_by_field, f"{base}/{index}", timeout=2)
                        for index in range(8)
                    )
                )
                assert all(fetch_result[0] == 200 for fetch_result in fetch_results)
                assert counters_by_name["tokens"] == 2
            finally:
                release_event.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    assert counters_by_name["maximum"] == 8
    assert counters_by_name["active"] == 0
    assert all(path.endswith("?_count=1&_offset=0002") for path in observed_paths[:9])
    assert directory._SOURCE_HTTP_SESSION.get() is None
    assert selection.CREDENTIAL_REQUEST_LOCKS.get() is None


@pytest.mark.parametrize("token_redirect", [False, True])
@pytest.mark.parametrize("keepalive", [False, True])
async def test_authenticated_token_and_data_redirects_never_receive_credentials(monkeypatch, token_redirect, keepalive):
    leaked_headers = []

    async def redirected(request):
        leaked_headers.append(dict(request.headers))
        return web.json_response({"access_token": "untrusted"})

    async def token(_request):
        if token_redirect:
            raise web.HTTPFound("/redirected")
        return web.json_response({"access_token": "synthetic-token", "expires_in": 3600})

    async def page(_request):
        raise web.HTTPFound("/redirected")

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    http_app.router.add_route("*", "/redirected", redirected)
    http_app.router.add_get("/fhir/Practitioner", page)
    monkeypatch.delenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, raising=False)
    async with TestServer(http_app) as server:
        base = str(server.make_url("/fhir"))
        source_by_field, _spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        async with directory._source_http_session_scope(force=keepalive):
            fetch_result = await directory._fetch_source_json_unpaced(
                source_by_field, base + "/Practitioner", timeout=2
            )
    if token_redirect:
        assert fetch_result[:3] == (None, None, "provider_directory_credentials_unavailable")
        assert not directory._is_transient_source_fetch_failure(fetch_result[0], fetch_result[2])
    else:
        assert fetch_result[:3] == (302, None, None)
    assert leaked_headers == []


@pytest.mark.parametrize(
    "suffix",
    [
        "/../sibling/Practitioner",
        "/%2e%2e/sibling/Practitioner",
        "/%252e%252e/sibling/Practitioner",
        "/%2f..%2fsibling/Practitioner",
        "/Practitioner%255cother",
        "/Practitioner#fragment",
        "/Practitioner\\other",
    ],
)
async def test_credential_scope_rejects_path_changes_before_token_lookup(monkeypatch, suffix):
    base = "https://payer.example/fhir"
    source_by_field, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    resolve = Mock(side_effect=AssertionError("credentials must not be resolved"))
    monkeypatch.setattr(directory, "_credential_request_options_for_source", resolve)
    for url in (base + suffix, "https://other.example/fhir/Practitioner", base.replace("/fhir", "/fhir-other")):
        with pytest.raises(selection.QualificationURLRejected, match="credential_url_outside_scope"):
            await directory._fetch_source_json_unpaced(source_by_field, url, timeout=1)
        assert not directory._is_credential_allowed_for_url(source_by_field, url)
    resolve.assert_not_called()


async def test_unapproved_token_url_and_query_credentials_fail_before_network_io(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    resolve = Mock(side_effect=AssertionError("credentials must not be resolved"))
    monkeypatch.setattr(directory, "_credential_request_options_for_source", resolve)
    credential_spec_by_field["oauth2"]["token_url"] = "https://other.example/token"
    with pytest.raises(ValueError, match="credential_token_url_unapproved"):
        await directory._fetch_source_json_unpaced(source_by_field, base + "/Location", timeout=1)
    credential_spec_by_field["oauth2"]["token_url"] = "https://payer.example/token"
    credential_spec_by_field["query_params"] = {"access_token": "synthetic"}
    with pytest.raises(ValueError, match="credential_query_parameters_forbidden"):
        await directory._fetch_source_json_unpaced(source_by_field, base + "/Location", timeout=1)
    resolve.assert_not_called()


async def test_missing_credentials_stop_before_anonymous_network_fallback(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    credential_spec_by_field.clear()
    fetch = AsyncMock(side_effect=AssertionError("anonymous fallback forbidden"))
    monkeypatch.setattr(directory, "_fetch_json", fetch)
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    fetch_result = await directory._fetch_source_json_unpaced(source_by_field, base + "/Location", timeout=1)
    assert fetch_result[:3] == (None, None, "provider_directory_credentials_unavailable")
    fetch.assert_not_called()


async def test_authenticated_throttle_keeps_retry_after_and_reduces_shared_slots(monkeypatch):
    received_times = []

    async def page(request):
        assert request.headers["Authorization"] == "Bearer synthetic-token"
        received_times.append(directory.time.monotonic())
        if len(received_times) == 1:
            return web.Response(status=429, text="busy", headers={"Retry-After": "0.02"})
        return web.json_response({"resourceType": "Bundle", "type": "searchset", "entry": []})

    http_app = web.Application()
    http_app.router.add_get("/fhir/Location", page)
    async with TestServer(http_app) as server:
        base = str(server.make_url("/fhir"))
        source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        credential_spec_by_field.clear()
        credential_spec_by_field["bearer_token"] = "synthetic-token"
        async with directory._source_http_session_scope(force=True, request_concurrency=8):
            first = await directory._fetch_source_json_once(source_by_field, base + "/Location", timeout=2)
            assert first[0] == 429 and directory._source_retry_after_seconds(first[1]) == 0.02
            deadline = source_by_field["_provider_directory_request_pacing_state"]["retry_after_monotonic"]
            second = await directory._fetch_source_json_once(source_by_field, base + "/Location", timeout=2)
            assert second[0] == 200 and received_times[1] >= deadline
            limits = [
                slot.limit for slot in directory.SOURCE_HTTP_REQUEST_SLOTS.get().values() if hasattr(slot, "limit")
            ]
            assert limits == [4]


async def test_reviewed_authenticated_scans_use_scheduler_without_enabling_page_prefetch(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    checkpointed = _parallel_resource_source()
    source_by_field.update(checkpointed, api_base=base, canonical_api_base=base)
    source_by_field["_pagination_checkpoint_context"] = dataclasses.replace(
        checkpointed["_pagination_checkpoint_context"], canonical_api_base=base
    )
    _patch_resource_scan_storage(monkeypatch, True)
    options = _resource_concurrency_options(requested_concurrency=8, resource_types=directory.anthem.RESOURCES)
    async with directory._source_http_session_scope(force=True, request_concurrency=8):
        assert directory._effective_resource_scan_concurrency(source_by_field, options) == 7
        assert directory._rest_page_prefetch_session(source_by_field, base + "/Location") is None
        assert directory._partition_window_concurrency(source_by_field, base + "/Location") == 1
        assert (
            directory._effective_resource_scan_concurrency(
                {**source_by_field, "canonical_api_base": "https://other.example/fhir"}, options
            )
            == 1
        )


async def test_reviewed_source_forces_verified_session_and_closes_it(monkeypatch):
    source_by_field, _spec = _reviewed_source(monkeypatch, "https://payer.example/fhir", "https://payer.example/token")
    monkeypatch.delenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, raising=False)
    create = directory._source_http_client_session
    sessions = []

    def capture(**options):
        assert options["require_verified_tls"] is True
        session = create(**options)
        sessions.append(session)
        return session

    async def acquire(_sources, **_options):
        assert directory._SOURCE_HTTP_SESSION.get().connector.limit_per_host == 8
        return {}

    monkeypatch.setattr(directory, "_source_http_client_session", capture)
    monkeypatch.setattr(directory, "_import_resources", acquire)
    await directory._import_resources_with_source_http_session([source_by_field], resource_scan_concurrency=8)
    assert len(sessions) == 1 and sessions[0].closed


async def test_ordinary_probe_keeps_tls_verified_before_resolving_credentials(monkeypatch):
    _stub_limited_process_data(monkeypatch)
    base = "https://payer.example/fhir"
    source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    source_by_field["source_id"] = "reviewed-source"
    credential_spec_by_field.clear()
    credential_spec_by_field["bearer_token"] = "synthetic-token"
    monkeypatch.setattr(directory, "_source_row_from_seed", lambda _row: dict(source_by_field))
    monkeypatch.setenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    calls = []

    def opener(*handlers):
        context = next(handler._context for handler in handlers if hasattr(handler, "_context"))
        assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED

        def open_request(request, **_options):
            calls.append(request.get_header("Authorization"))
            return nullcontext(
                SimpleNamespace(status=200, headers={}, read=io.BytesIO(b'{"resourceType":"CapabilityStatement"}').read)
            )

        return SimpleNamespace(open=open_request)

    monkeypatch.setattr(directory.urllib.request, "build_opener", opener)
    async_fetch = AsyncMock(side_effect=AssertionError("unverified async transport forbidden"))
    monkeypatch.setattr(directory, "_fetch_json_with_source_session", async_fetch)

    async def probe(_sources, **_options):
        context = directory._SOURCE_HTTP_SESSION.get().connector._ssl
        assert context.verify_mode == ssl.CERT_NONE
        fetch_result = await directory._fetch_source_json_unpaced(source_by_field, base + "/metadata", timeout=1)
        assert fetch_result[0] == 200
        return 1, 1, {source_by_field["source_id"]}

    monkeypatch.setattr(directory, "_run_source_probe_batch", probe)
    await directory.process_data(
        {"context": {}},
        {"probe": True, "import_resources": False, "publish_artifacts": False, "publish_corroboration": False},
    )
    assert calls == ["Bearer synthetic-token"]
    async_fetch.assert_not_called()


async def test_probe_rejects_unapproved_token_before_any_oauth_network_call(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    credential_spec_by_field["oauth2"]["token_url"] = "https://other.example/token"
    fetch_token = Mock(side_effect=AssertionError("token transport forbidden"))
    monkeypatch.setattr(directory, "_fetch_oauth2_client_token_sync", fetch_token)
    with pytest.raises(ValueError, match="credential_token_url_unapproved"):
        await directory._probe_source(source_by_field, timeout=1, run_id=None)
    with pytest.raises(ValueError, match="credential_token_url_unapproved"):
        directory._resource_import_group_key(source_by_field)
    fetch_token.assert_not_called()


async def test_pinned_anonymous_session_cannot_preempt_authenticated_transport(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    credential_spec_by_field.clear()
    credential_spec_by_field["bearer_token"] = "synthetic-token"
    fetch = AsyncMock(return_value=(200, {}, None, 0))
    anonymous = AsyncMock(side_effect=AssertionError("anonymous session forbidden"))
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    monkeypatch.setattr(directory, "_fetch_json_with_source_session", anonymous)
    token = directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.set(object())
    try:
        assert (await directory._fetch_source_json_unpaced(source_by_field, base + "/Location", timeout=1))[0] == 200
    finally:
        directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.reset(token)
    assert fetch.await_args.kwargs["extra_headers"]["Authorization"] == "Bearer synthetic-token"
    anonymous.assert_not_called()


async def test_reviewed_retry_after_above_legacy_cap_reaches_pacing_and_deadline(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    response = (429, {directory.SOURCE_RETRY_AFTER_FIELD: "1200"}, None, 0)
    monkeypatch.setattr(directory, "_fetch_source_json_unpaced", AsyncMock(return_value=response))
    async with directory._source_http_session_scope(force=True, request_concurrency=8):
        before = directory.time.monotonic()
        await directory._fetch_source_json_once(source_by_field, base + "/Location", timeout=1)
        assert source_by_field["_provider_directory_request_pacing_state"]["retry_after_monotonic"] >= before + 1200
    fetch = AsyncMock(side_effect=AssertionError("early retry forbidden"))
    monkeypatch.setattr(directory, "_fetch_cooldown_checkpoint_page", fetch)
    outcome = await directory._retry_rest_pagination_after_cooldown(
        source_by_field, base + "/Location", response, timeout=1, deadline_at=directory.time.monotonic() + 700
    )
    assert outcome.deadline_blocked and outcome.retries == 0
    fetch.assert_not_called()


async def test_reviewed_quick_retry_does_not_truncate_provider_delay(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    response = (429, {directory.SOURCE_RETRY_AFTER_FIELD: "1200"}, None, 0)
    success = (200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 0)
    attempt = AsyncMock(side_effect=[(response, False, True, False), (success, False, False, False)])
    waits = []

    async def sleep(delay):
        waits.append(delay)

    monkeypatch.setattr(directory, "_fetch_source_json_attempt", attempt)
    monkeypatch.setattr(directory.asyncio, "sleep", sleep)
    fetch_result, _resize, retries = await directory._fetch_source_json_candidate(
        source_by_field, base + "/Location", timeout=1, is_last_candidate=True
    )
    assert fetch_result[0] == 200 and retries == 1 and waits == [1200]


@pytest.mark.parametrize("header", ["Authorization", "X-Api-Key"])
def test_reviewed_fallback_never_reports_invalid_secret_header_contents(monkeypatch, header):
    marker = "synthetic-private-marker"

    def validate_headers(_directory, request, *, timeout):
        connection = http.client.HTTPConnection("directory.example.test")
        connection.putrequest("GET", "/fhir/Location")
        for name, access_token in request.header_items():
            connection.putheader(name, access_token)
        raise AssertionError("invalid header should have failed before any network I/O")

    monkeypatch.setattr(selection, "open_source_request", validate_headers)
    fetch_result = directory._fetch_json_sync(
        "https://directory.example.test/fhir/Location",
        timeout=1,
        extra_headers={header: "Bearer " + marker + "\ninvalid"},
        allow_gzip=True,
    )
    assert fetch_result[:3] == (None, None, "ValueError")
    assert marker not in str(fetch_result)
