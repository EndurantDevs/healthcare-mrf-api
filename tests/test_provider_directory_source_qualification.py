# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh isolated source qualification preserves source and transport admission."""

import asyncio
import importlib
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from process import provider_directory_generic_source_policy as source_policy
from process.provider_directory_source_selection import qualify_stale_isolated_source

directory = importlib.import_module("process.provider_directory_fhir")
_REAL_SOURCE_FETCH = directory._fetch_source_json
CAPABILITY = {"resourceType": "CapabilityStatement", "rest": [{"resource": [{"type": "Practitioner"}]}]}


def _source():
    return {
        "source_id": "synthetic-source",
        "api_base": "https://directory.example.test/fhir",
        "canonical_api_base": "https://directory.example.test/fhir",
        "last_validated_status": "valid_non_fhir",
        "auth_type": "none",
        "metadata_json": {},
    }


def _task(**overrides):
    return {
        "source_ids": ["synthetic-source"],
        "provider_directory_endpoint_scope": "https://directory.example.test/fhir",
        "import_resources": True,
        "full_refresh": True,
        "defer_typed_materialization": True,
        "probe": False,
        "bulk_export": False,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "resources": ["Practitioner"],
        "timeout": 3,
        **overrides,
    }


@pytest.fixture(autouse=True)
def _isolate_transport_and_forbid_probe_writes(monkeypatch):
    monkeypatch.setattr(directory, "is_source_storage_required", lambda: True)
    token = source_policy._FACTORY.set(SimpleNamespace(storage_binding=object()))
    monkeypatch.setattr(directory, "_credential_spec_for_source", lambda _source: None)
    writers = [AsyncMock(side_effect=AssertionError("unexpected catalog probe writer")) for _ in range(2)]
    monkeypatch.setattr(directory, "_run_source_probe_batch", writers[0])
    monkeypatch.setattr(directory, "_upsert_rows", writers[1])
    monkeypatch.setattr(directory, "_fetch_source_json", AsyncMock(side_effect=AssertionError("unexpected network")))
    try:
        yield
    finally:
        source_policy._FACTORY.reset(token)
    assert directory.source_selection.QUALIFICATION_API_BASE.get() is None
    for writer in writers:
        writer.assert_not_awaited()


def _mock_transport(monkeypatch, *, resource_status=200, resource_payload=None, metadata_error=None, resource_url=None):
    expected_base = _source()["api_base"]

    async def fetch(_source_record, url, *, timeout):
        assert timeout == 3
        assert _source_record["api_base"] == _source_record["canonical_api_base"] == expected_base
        if url in (f"{expected_base}/metadata?_format=json", f"{expected_base}/metadata"):
            return (None, None, metadata_error, 0) if metadata_error else (200, CAPABILITY, None, 1)
        assert url == (resource_url or f"{expected_base}/Practitioner?_count=1")
        payload = {"resourceType": "Bundle", "type": "searchset"} if resource_payload is None else resource_payload
        return resource_status, payload, None, 1

    transport = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(directory, "_fetch_source_json", transport)
    return transport


def _selected(source_rows):
    return directory._select_resource_import_sources(
        source_rows, valid_source_ids=None, open_only=True, include_auth_required=False
    )[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("entries", [[], [{"resource": {"resourceType": "Practitioner", "id": "practitioner-1"}}]])
async def test_stale_seed_requires_real_capability_and_resource_access(monkeypatch, entries):
    source_record = _source()
    transport = _mock_transport(
        monkeypatch, resource_payload={"resourceType": "Bundle", "type": "searchset", "entry": entries}
    )
    assert _selected([source_record]) == []
    qualified = await qualify_stale_isolated_source(directory, [source_record], _task(), timeout=3, run_id="run-test")
    assert qualified == [{**source_record, "last_validated_status": "valid"}]
    assert _selected(qualified) == qualified
    assert source_record["last_validated_status"] == "valid_non_fhir"
    assert transport.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical_valid", [True, False])
@pytest.mark.parametrize("has_credentials", [False, True])
async def test_canonical_metadata_precedes_unused_out_of_scope_fallback(monkeypatch, canonical_valid, has_credentials):
    api_base = "https://directory.example.test/providerdirectory"
    source_map = {
        **_source(),
        "api_base": api_base,
        "canonical_api_base": api_base,
        "metadata_json": {"provider_directory_supported_resources": ["Practitioner"]},
    }
    source_rows = [source_map]
    metadata_url_pairs = directory._candidate_metadata_urls(source_map)
    assert ("https://directory.example.test", "https://directory.example.test/metadata") in metadata_url_pairs
    if has_credentials:
        monkeypatch.setattr(
            directory, "_credential_spec_for_source", lambda _source: {"bearer_token": "synthetic-token"}
        )

    async def fetch(_source_record, url, *, timeout):
        assert directory._is_url_within_api_base(url, api_base)
        if "/metadata" in url:
            return (200, CAPABILITY, None, 1) if canonical_valid else (503, None, None, 1)
        assert url == f"{api_base}/Practitioner?_count=1"
        return 200, {"resourceType": "Bundle", "type": "searchset"}, None, 1

    transport = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(directory, "_fetch_source_json", transport)
    qualified = await qualify_stale_isolated_source(
        directory, source_rows, _task(provider_directory_endpoint_scope=api_base), timeout=3, run_id=None
    )
    assert (qualified[0]["last_validated_status"] == "valid") is canonical_valid
    assert source_rows == [source_map]
    requested_urls = [call.args[1] for call in transport.await_args_list]
    assert requested_urls[0] == f"{api_base}/metadata?_format=json"
    assert len(requested_urls) == 2
    if not canonical_valid:
        assert qualified is source_rows
        assert requested_urls[1] == f"{api_base}/metadata"


@pytest.mark.asyncio
async def test_malformed_qualification_url_refuses_before_credentials(monkeypatch):
    credentials = Mock(side_effect=AssertionError("unexpected credential resolution"))
    monkeypatch.setattr(directory, "_credential_spec_for_source", credentials)

    async def probe(source_record, *, timeout, run_id):
        return await directory._fetch_source_json_unpaced(source_record, "https://[", timeout=timeout)

    monkeypatch.setattr(directory, "_probe_source", probe)
    source_rows = [_source()]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    credentials.assert_not_called()
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_qualification_base_refuses_before_probe(monkeypatch):
    probe = AsyncMock(side_effect=AssertionError("unexpected probe"))
    monkeypatch.setattr(directory, "_probe_source", probe)
    source_map = {**_source(), "api_base": None, "canonical_api_base": None}
    with pytest.raises(directory.source_selection.QualificationURLRejected, match="url_outside_scope"):
        await directory.source_selection._scoped_qualification_probe(directory, source_map, timeout=3, run_id=None)
    probe.assert_not_awaited()
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {"resource_status": 401},
        {"resource_status": 403},
        {"resource_status": 503},
        {"metadata_error": "TimeoutError: upstream unavailable"},
        {"resource_payload": {"resourceType": "OperationOutcome", "issue": [{"severity": "fatal"}]}},
        {"resource_payload": {}},
        {"resource_payload": {"ok": True}},
        {"resource_payload": {"resourceType": "Practitioner", "id": "practitioner-1"}},
        {"resource_payload": {"resourceType": "Bundle"}},
        {"resource_payload": {"resourceType": "Bundle", "type": "collection"}},
        {"resource_payload": {"resourceType": "Bundle", "type": "searchset", "entry": {}}},
        {"resource_payload": {"resourceType": "Bundle", "type": "searchset", "entry": [{}]}},
        {
            "resource_payload": {
                "resourceType": "Bundle",
                "type": "searchset",
                "entry": [{"resource": {"resourceType": "Organization", "id": "organization-1"}}],
            }
        },
    ],
)
async def test_failed_live_qualification_cannot_admit_stale_seed(monkeypatch, response):
    transport = _mock_transport(monkeypatch, **response)
    source_rows = [_source()]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    assert _selected(source_rows) == []
    requested_urls = [request.args[1] for request in transport.await_args_list]
    assert requested_urls[0] == f"{source_rows[0]['api_base']}/metadata?_format=json"
    if response.get("metadata_error"):
        assert f"{source_rows[0]['api_base']}/Practitioner?_count=1" not in requested_urls
    else:
        assert f"{source_rows[0]['api_base']}/Practitioner?_count=1" in requested_urls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint,is_scoped",
    [
        ("https://other.example.test/fhir/Practitioner", False),
        ("https://directory.example.test/other/Practitioner", False),
        ("https://directory.example.test/fhir-other/Practitioner", False),
        ("https://directory.example.test:8443/fhir/Practitioner", False),
        ("http://directory.example.test/fhir/Practitioner", False),
        ("https://directory.example.test/fhir/../other/Practitioner", False),
        ("https://directory.example.test/fhir/%2e%2e/other/Practitioner", False),
        ("https://user@directory.example.test/fhir/Practitioner", False),
        ("https://directory.example.test/fhir/Practitioner", True),
        ("https://DIRECTORY.example.test:443/fhir/Practitioner", True),
    ],
)
async def test_resource_bundle_must_come_from_scoped_base(monkeypatch, endpoint, is_scoped):
    source_rows = [{**_source(), "endpoint_practitioner": endpoint}]
    resource_url = f"{endpoint}?_count=1"
    transport = _mock_transport(monkeypatch, resource_url=resource_url)
    credentials = Mock(return_value=None)
    monkeypatch.setattr(directory, "_credential_spec_for_source", credentials)
    qualified = await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None)
    assert [request.args[1] for request in transport.await_args_list] == (
        [
            f"{source_rows[0]['api_base']}/metadata?_format=json",
            resource_url,
        ]
        if is_scoped
        else []
    )
    assert source_rows[0]["last_validated_status"] == "valid_non_fhir"
    if is_scoped:
        assert qualified == [{**source_rows[0], "last_validated_status": "valid"}]
        assert _selected(qualified) == qualified
    else:
        credentials.assert_not_called()
        assert qualified is source_rows
        assert _selected(qualified) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("has_match", [False, True])
@pytest.mark.parametrize(
    "severity,is_valid",
    [("warning", True), ("information", True), ("error", False), ("fatal", False), ("unknown", False)],
)
async def test_search_outcomes_qualify_only_successful_searches(monkeypatch, has_match, severity, is_valid):
    entries = [{"resource": {"resourceType": "Practitioner", "id": "p1"}}] if has_match else []
    entries.append(
        {
            "search": {"mode": "outcome"},
            "resource": {
                "resourceType": "OperationOutcome",
                "issue": [{"severity": severity, "code": "informational"}],
            },
        }
    )
    transport = _mock_transport(
        monkeypatch, resource_payload={"resourceType": "Bundle", "type": "searchset", "entry": entries}
    )
    monkeypatch.setattr(directory, "_fetch_source_json_unpaced", transport)
    monkeypatch.setattr(directory, "_fetch_source_json", _REAL_SOURCE_FETCH)
    source_rows = [_source()]
    qualified = await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None)
    assert (qualified[0]["last_validated_status"] == "valid") is (has_match and is_valid)
    assert source_rows == [_source()]
    assert directory._current_version_census_bundle_error(
        {"resourceType": "Bundle", "type": "searchset", "entry": entries}, "Practitioner"
    )
    ordinary = directory._source_fetch_result_with_payload_error(
        source_rows[0],
        f"{source_rows[0]['api_base']}/Practitioner?_count=1",
        (200, {"resourceType": "Bundle", "type": "searchset", "entry": entries}, None, 1),
    )
    assert (ordinary[2] is None) is (has_match and is_valid)
    probe = directory._resource_access_probe_result(
        source_rows[0],
        f"{source_rows[0]['api_base']}/Practitioner?_count=1",
        "Practitioner",
        200,
        {"resourceType": "Bundle", "type": "searchset", "entry": entries},
        None,
        1,
    )
    assert probe["search_bundle_valid"] is (has_match and is_valid)


@pytest.mark.parametrize(
    "outcome",
    [
        {"resource": {"resourceType": "OperationOutcome", "issue": [{"severity": "warning", "code": "informational"}]}},
        {"search": {"mode": "outcome"}, "resource": {"resourceType": "OperationOutcome", "issue": []}},
        {
            "search": {"mode": "outcome"},
            "resource": {"resourceType": "OperationOutcome", "issue": [{"severity": "warning"}]},
        },
        {
            "search": {"mode": "outcome"},
            "resource": {
                "resourceType": "OperationOutcome",
                "issue": [{"severity": "warning", "code": "informational"}, "invalid"],
            },
        },
    ],
)
def test_malformed_outcome_entries_are_not_search_evidence(outcome):
    result = directory._resource_access_probe_result(
        _source(),
        f"{_source()['api_base']}/Practitioner?_count=1",
        "Practitioner",
        200,
        {"resourceType": "Bundle", "type": "searchset", "entry": [outcome]},
        None,
        1,
    )
    assert result["search_bundle_valid"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["metadata_url", "provider_directory_confirmed_metadata_url"])
async def test_metadata_scope_precedes_credentials_and_transport(monkeypatch, field):
    source_rows = [{**_source(), "metadata_json": {field: "https://other.example.test/fhir/metadata"}}]
    credentials = Mock(side_effect=AssertionError("credentials resolved before URL admission"))
    monkeypatch.setattr(directory, "_credential_spec_for_source", credentials)
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    credentials.assert_not_called()
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_qualification_scope_is_private_to_each_task(monkeypatch):
    entered_events = [asyncio.Event(), asyncio.Event()]

    async def probe(source_record, *, timeout, run_id):
        entered_events[int(run_id)].set()
        await entered_events[1 - int(run_id)].wait()
        assert directory.source_selection.QUALIFICATION_API_BASE.get() == source_record["api_base"]
        return {"status": "unreachable"}, None

    monkeypatch.setattr(directory, "_probe_source", probe)
    source_records = [
        _source(),
        {
            **_source(),
            "api_base": "https://second.example.test/fhir",
            "canonical_api_base": "https://second.example.test/fhir",
        },
    ]
    operations = [
        asyncio.create_task(
            qualify_stale_isolated_source(
                directory,
                [source_record],
                _task(provider_directory_endpoint_scope=source_record["api_base"]),
                timeout=3,
                run_id=str(index),
            )
        )
        for index, source_record in enumerate(source_records)
    ]
    try:
        async with asyncio.timeout(3):
            await asyncio.gather(*operations)
        assert directory.source_selection.QUALIFICATION_API_BASE.get() is None
    finally:
        for operation in operations:
            operation.cancel()
        await asyncio.gather(*operations, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_kind", ["session", "pinned", "sync", "credentialed"])
@pytest.mark.parametrize("redirect_resource", ["metadata", "Practitioner"])
@pytest.mark.parametrize("destination", ["/other/result", "/fhir/result", "cross-origin"])
async def test_qualification_never_follows_redirects(monkeypatch, transport_kind, redirect_resource, destination):
    requests = []

    async def respond(request):
        requests.append((request.path, request.headers.get("Authorization")))
        if request.path == f"/fhir/{redirect_resource}":
            location = (
                destination
                if destination != "cross-origin"
                else str(server.make_url("/other/result")).replace("127.0.0.1", "localhost")
            )
            raise web.HTTPFound(location=location)
        response = (
            CAPABILITY
            if redirect_resource == "metadata" or request.path.endswith("/metadata")
            else {"resourceType": "Bundle", "type": "searchset"}
        )
        return web.json_response(response)

    application = web.Application()
    application.router.add_get("/{path:.*}", respond)
    async with TestServer(application) as server, aiohttp.ClientSession() as session:
        base = str(server.make_url("/fhir"))
        source_rows = [{**_source(), "api_base": base, "canonical_api_base": base}]
        monkeypatch.setattr(directory, "_fetch_source_json", _REAL_SOURCE_FETCH)
        if transport_kind == "credentialed":
            monkeypatch.setattr(
                directory, "_credential_spec_for_source", lambda _source: {"bearer_token": "synthetic-token"}
            )
        token = directory._SOURCE_HTTP_SESSION.set(session if transport_kind == "session" else None)
        pinned = directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.set(session if transport_kind == "pinned" else None)
        try:
            qualified = await qualify_stale_isolated_source(
                directory, source_rows, _task(provider_directory_endpoint_scope=base), timeout=3, run_id=None
            )
        finally:
            directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.reset(pinned)
            directory._SOURCE_HTTP_SESSION.reset(token)
    assert qualified is source_rows
    assert requests and all(path in {"/fhir/metadata", "/fhir/Practitioner"} for path, _authorization in requests)
    assert all(
        (authorization == "Bearer synthetic-token") is (transport_kind == "credentialed")
        for _path, authorization in requests
    )


def _configure_oauth(monkeypatch, token_url, auth_mode):
    monkeypatch.setattr(directory, "_OAUTH_TOKEN_CACHE", {})
    monkeypatch.setattr(
        directory,
        "_credential_spec_for_source",
        lambda _source: {
            "oauth2": {
                "token_url": token_url,
                "client_id": "synthetic-client",
                "client_secret": "synthetic-secret",
                "auth": auth_mode,
            }
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_mode", ["basic", "body"])
async def test_qualification_oauth_separate_origin(monkeypatch, auth_mode):
    token_requests, resource_requests = [], []

    async def token_response(request):
        token_requests.append(request.path)
        form = await request.post()
        assert form["grant_type"] == "client_credentials"
        assert bool(request.headers.get("Authorization")) is (auth_mode == "basic")
        assert (form.get("client_secret") == "synthetic-secret") is (auth_mode == "body")
        return web.json_response({"access_token": "synthetic-token", "expires_in": 3600})

    async def resource_response(request):
        resource_requests.append(request.path)
        assert request.headers.get("Authorization") == "Bearer synthetic-token"
        payload = CAPABILITY if request.path.endswith("/metadata") else {"resourceType": "Bundle", "type": "searchset"}
        return web.json_response(payload)

    token_app, resource_app = web.Application(), web.Application()
    token_app.router.add_post("/token", token_response)
    resource_app.router.add_get("/fhir/{resource}", resource_response)
    async with TestServer(token_app) as token_server, TestServer(resource_app) as resource_server:
        base = str(resource_server.make_url("/fhir"))
        assert token_server.port != resource_server.port
        _configure_oauth(monkeypatch, str(token_server.make_url("/token")), auth_mode)
        monkeypatch.setattr(directory, "_fetch_source_json", _REAL_SOURCE_FETCH)
        source_rows = [{**_source(), "api_base": base, "canonical_api_base": base, "auth_type": "OAuth2"}]
        qualified = await qualify_stale_isolated_source(
            directory,
            source_rows,
            _task(provider_directory_endpoint_scope=base, open_only=False, include_auth_required=True),
            timeout=3,
            run_id=None,
        )
    assert qualified == [{**source_rows[0], "last_validated_status": "valid"}]
    assert source_rows[0]["last_validated_status"] == "valid_non_fhir"
    assert token_requests == ["/token"]
    assert resource_requests == ["/fhir/metadata", "/fhir/Practitioner"]


@pytest.mark.asyncio
@pytest.mark.parametrize("auth_mode", ["basic", "body"])
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_qualification_oauth_redirect_refusal(monkeypatch, auth_mode, status):
    token_requests, destination_requests = [], []

    async def destination_response(request):
        destination_requests.append(request.method)
        return web.json_response({"access_token": "redirected-token"})

    async def token_response(request):
        token_requests.append(request.path)
        return web.Response(status=status, headers={"Location": str(destination_server.make_url("/token"))})

    token_app, destination_app = web.Application(), web.Application()
    token_app.router.add_post("/token", token_response)
    destination_app.router.add_route("*", "/token", destination_response)
    async with TestServer(destination_app) as destination_server, TestServer(token_app) as token_server:
        _configure_oauth(monkeypatch, str(token_server.make_url("/token")), auth_mode)
        source_rows = [{**_source(), "auth_type": "OAuth2"}]
        qualified = await qualify_stale_isolated_source(
            directory, source_rows, _task(open_only=False, include_auth_required=True), timeout=3, run_id=None
        )
    assert qualified is source_rows
    assert source_rows[0]["last_validated_status"] == "valid_non_fhir"
    assert token_requests == ["/token"]
    assert destination_requests == []
    directory._fetch_source_json.assert_not_awaited()


def _blocking_oauth_token(monkeypatch):
    loop = asyncio.get_running_loop()
    state = SimpleNamespace(
        entered=asyncio.Event(), finished=asyncio.Event(), release=threading.Event(), scopes=[], threads=[]
    )
    _configure_oauth(monkeypatch, "https://auth.example.test/token", "basic")

    def blocking_token(_oauth2):
        state.threads.append(threading.get_ident())
        state.scopes.append(directory.source_selection.QUALIFICATION_API_BASE.get())
        loop.call_soon_threadsafe(state.entered.set)
        try:
            assert state.release.wait(3), "token fixture was not released"
            return "synthetic-token"
        finally:
            loop.call_soon_threadsafe(state.finished.set)

    monkeypatch.setattr(directory, "_fetch_oauth2_client_token_sync", blocking_token)
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["short_circuit", "metadata", "fetch"])
@pytest.mark.parametrize("completion", ["release", "timeout", "cancel"])
async def test_qualification_oauth_wait_cleanup(monkeypatch, boundary, completion):
    state, scopes_after = _blocking_oauth_token(monkeypatch), []
    main_thread = threading.get_ident()
    transport = _mock_transport(monkeypatch)
    if boundary != "short_circuit":
        monkeypatch.setattr(directory, "_credential_probe_short_circuit", lambda *_args, **_kwargs: None)
    if boundary == "fetch":

        async def fetch_options(url, *, timeout, **_options):
            return await transport(source_rows[0], url, timeout=timeout)

        monkeypatch.setattr(directory, "_fetch_source_json", _REAL_SOURCE_FETCH)
        monkeypatch.setattr(directory, "_fetch_json_with_options", fetch_options)
    if completion == "timeout":
        monkeypatch.setattr(
            directory.source_selection, "_qualification_probe_timeout_seconds", lambda *_args, **_kwargs: 0.25
        )
    source_rows = [{**_source(), "auth_type": "OAuth2"}]

    async def qualify():
        try:
            return await qualify_stale_isolated_source(
                directory, source_rows, _task(open_only=False, include_auth_required=True), timeout=3, run_id=None
            )
        finally:
            scopes_after.append(directory.source_selection.QUALIFICATION_API_BASE.get())

    operation = asyncio.create_task(qualify())
    try:
        async with asyncio.timeout(3):
            await state.entered.wait()
            assert not operation.done()
            assert state.threads and all(thread != main_thread for thread in state.threads)
            assert not state.release.is_set() and not state.finished.is_set()
            if completion == "cancel":
                operation.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await operation
            else:
                if completion == "release":
                    state.release.set()
                qualified = await operation
                assert (qualified is source_rows) is (completion == "timeout")
                if completion == "release":
                    assert qualified == [{**source_rows[0], "last_validated_status": "valid"}]
            assert scopes_after == [None]
            assert directory.source_selection.QUALIFICATION_API_BASE.get() is None
            assert source_rows[0]["last_validated_status"] == "valid_non_fhir"
    finally:
        state.release.set()
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
        await asyncio.wait_for(state.finished.wait(), timeout=3)
    assert state.scopes and set(state.scopes) == {source_rows[0]["api_base"]}


@pytest.mark.parametrize(
    "payload,is_valid", [({}, False), ({"resourceType": "Bundle", "type": "searchset", "entry": []}, True)]
)
def test_resource_probe_preserves_reachability_while_reporting_bundle_evidence(payload, is_valid):
    result = directory._resource_access_probe_result(
        _source(), "https://directory.example.test/fhir/Practitioner?_count=1", "Practitioner", 200, payload, None, 1
    )
    assert result["status"] == "reachable"
    assert result["error"] is None
    assert result["search_bundle_valid"] is is_valid


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["valid", "auth_required", "probe_only", "unreachable"])
async def test_other_seed_statuses_do_not_probe(monkeypatch, status):
    source_rows = [{**_source(), "last_validated_status": status}]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [{"provider_directory_manual_only": True}, {"provider_directory_coverage_mode": "probe_only"}],
)
async def test_manual_and_probe_only_sources_stay_dormant(metadata):
    source_rows = [{**_source(), "metadata_json": metadata}]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()
    assert _selected(source_rows) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"retry_of_run_id": "run-prior"},
        {"provider_directory_pagination_root_run_id": "run-root"},
        {"probe": True},
        {"defer_typed_materialization": False},
        {"import_resources": False},
        {"full_refresh": False},
        {"bulk_export": True},
        {"publish_artifacts": True},
        {"publish_source_dataset_only": True},
        {"stale_cleanup": True},
        {"seed_only": True},
        {"test": True},
        {"test_mode": True},
        {"publish_after_acquisition": True},
        {"publish_corroboration": True},
        {"source_ids": ["different-source"]},
    ],
)
async def test_retries_and_other_operations_do_not_change(overrides):
    source_rows = [_source()]
    assert (
        await qualify_stale_isolated_source(directory, source_rows, _task(**overrides), timeout=3, run_id=None)
        is source_rows
    )
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbound_storage_does_not_qualify(monkeypatch):
    monkeypatch.setattr(directory, "is_source_storage_required", lambda: False)
    source_rows = [_source()]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_source_factory_does_not_qualify():
    source_policy._FACTORY.get().storage_binding = None
    source_rows = [_source()]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_credentials_still_refuse_before_network():
    source_rows = [{**_source(), "auth_type": "OAuth2/SMART"}]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    assert _selected(source_rows) == []
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_resource_probe", [False, True])
async def test_capability_alone_and_changed_endpoint_do_not_qualify(monkeypatch, has_resource_probe):
    source_rows = [_source()]
    probe_map = {"status": "valid", "http_status": 200, "api_base": source_rows[0]["api_base"]}
    if has_resource_probe:
        probe_map.update(
            api_base="https://other.example.test/fhir",
            resource_probe={
                "status": "reachable",
                "http_status": 200,
                "search_bundle_valid": True,
                "url": f"{source_rows[0]['api_base']}/Practitioner?_count=1",
            },
        )
    probe = AsyncMock(return_value=(probe_map, CAPABILITY))
    monkeypatch.setattr(directory, "_probe_source", probe)
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    assert _selected(source_rows) == []
    probe.assert_awaited_once_with(source_rows[0], timeout=3, run_id=None)


@pytest.mark.asyncio
async def test_endpoint_scope_is_checked_before_transport():
    with pytest.raises(RuntimeError, match="endpoint_scope_mismatch"):
        await qualify_stale_isolated_source(
            directory,
            [_source()],
            _task(provider_directory_endpoint_scope="https://other.example.test/fhir"),
            timeout=3,
            run_id=None,
        )
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError, asyncio.CancelledError])
async def test_probe_timeout_refuses_and_cancellation_propagates(monkeypatch, failure):
    monkeypatch.setattr(directory, "_probe_source", AsyncMock(side_effect=failure))
    source_rows = [_source()]
    if failure is asyncio.CancelledError:
        with pytest.raises(asyncio.CancelledError):
            await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None)
    else:
        assert (
            await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
        )
        assert _selected(source_rows) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("override,expected_timeout", [("1", 14), ("30", 30)])
async def test_qualification_deadline_keeps_full_request_budget(monkeypatch, override, expected_timeout):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_SOURCE_PROBE_HARD_TIMEOUT", override)
    transport = _mock_transport(monkeypatch)
    wait_for = AsyncMock(wraps=asyncio.wait_for)
    monkeypatch.setattr(asyncio, "wait_for", wait_for)
    source_rows = [_source()]
    qualified = await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None)
    assert qualified == [{**source_rows[0], "last_validated_status": "valid"}]
    assert transport.await_count == 2
    assert wait_for.await_count == 1
    assert wait_for.await_args.kwargs == {"timeout": expected_timeout}
    assert directory._source_probe_hard_timeout_seconds(source_rows[0], timeout=3) == int(override)


@pytest.mark.asyncio
async def test_pending_probe_is_cancelled_at_hard_timeout_without_changing_source(monkeypatch):
    pending = asyncio.Future()
    cleanup_completed = asyncio.Event()

    async def pending_probe(*_args, **_kwargs):
        try:
            await pending
        finally:
            cleanup_completed.set()

    probe = AsyncMock(side_effect=pending_probe)
    hard_timeout = Mock(return_value=0.01)
    monkeypatch.setattr(directory, "_probe_source", probe)
    monkeypatch.setattr(directory, "_source_probe_hard_timeout_seconds", hard_timeout)
    source_rows = [_source()]
    async with asyncio.timeout(3):
        qualified = await qualify_stale_isolated_source(
            directory, source_rows, _task(), timeout=0.001, run_id="run-test"
        )
    assert qualified is source_rows
    assert source_rows == [_source()]
    assert _selected(source_rows) == []
    assert pending.cancelled()
    assert cleanup_completed.is_set()
    probe.assert_awaited_once_with(source_rows[0], timeout=0.001, run_id="run-test")
    hard_timeout.assert_called_once_with(source_rows[0], timeout=0.001)


@pytest.mark.asyncio
async def test_real_import_entry_qualifies_scoped_seed_before_existing_storage(monkeypatch):
    for name in (
        "ensure_database",
        "_prepare_provider_directory_job_tables",
        "_raise_if_resource_import_cancelled",
        "_clear_resource_rows_seen",
        "_mark_provider_directory_progress",
    ):
        monkeypatch.setattr(directory, name, AsyncMock())
    seed_by_field = {
        "id": 17,
        "org_name": "Synthetic Health",
        "api_base": _source()["api_base"],
        "auth_type": "none",
        "last_validated_status": "valid_non_fhir",
    }
    source_record = directory._source_row_from_seed(seed_by_field)
    monkeypatch.setattr(directory, "_resolve_seed_db", Mock(return_value=("unused-local-seed", None)))
    monkeypatch.setattr(directory, "_seed_rows_from_sqlite", Mock(return_value=[seed_by_field]))
    monkeypatch.setattr(directory, "_resolve_retest_results", Mock(return_value=(None, None)))
    _mock_transport(monkeypatch)
    source_upsert = AsyncMock(side_effect=RuntimeError("qualified source reached storage"))
    monkeypatch.setattr(directory, "_upsert_provider_directory_source_rows", source_upsert)
    with pytest.raises(RuntimeError, match="qualified source reached storage"):
        await directory.process_provider_directory_fhir_data(
            {"context": {}}, _task(source_ids=[source_record["source_id"]])
        )
    qualified = source_upsert.await_args.args[0]
    assert len(qualified) == 1
    assert qualified[0]["source_id"] == source_record["source_id"]
    assert qualified[0]["api_base"] == source_record["api_base"]
    assert qualified[0]["last_validated_status"] == "valid"
    assert seed_by_field["last_validated_status"] == "valid_non_fhir"
    assert _selected(qualified) == qualified
