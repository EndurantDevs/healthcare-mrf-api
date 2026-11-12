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


def _mock_fresh_token(monkeypatch):
    """Replace only token I/O while retaining real cache selection and authorization assembly."""

    async def token(directory, source_record, url, oauth, timeout):
        key = directory._oauth2_cache_key(oauth)
        cached = directory._OAUTH_TOKEN_CACHE.get(key)
        if cached and cached[1] > directory.time.time():
            return cached
        directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-fresh", directory.time.time() + 3600)
        return directory._OAUTH_TOKEN_CACHE[key]

    request = AsyncMock(side_effect=token)
    monkeypatch.setattr(selection, "_credentialed_oauth_token", request)
    return request


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


async def test_concurrent_unauthorized_pages_share_one_replacement_token(monkeypatch):
    rejected, release = asyncio.Event(), asyncio.Event()
    counters_by_name = {"tokens": 0, "rejected": 0}
    paths_by_token = {"synthetic-old": [], "synthetic-fresh": []}

    async def token(_request):
        counters_by_name["tokens"] += 1
        await asyncio.sleep(0.01)
        return web.json_response({"access_token": "synthetic-fresh", "expires_in": 3600})

    async def page(request):
        access_token = request.headers["Authorization"].removeprefix("Bearer ")
        paths_by_token[access_token].append(request.raw_path)
        if access_token == "synthetic-old":
            counters_by_name["rejected"] += 1
            if counters_by_name["rejected"] == 2:
                rejected.set()
            await release.wait()
            return web.Response(status=401)
        return web.json_response({"resourceType": "Bundle", "type": "searchset", "entry": []})

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    http_app.router.add_get("/fhir/{resource}", page)
    async with TestServer(http_app) as server:
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
        async with directory._source_http_session_scope(force=True, request_concurrency=2):
            tasks = [
                asyncio.create_task(
                    directory._fetch_source_json_once(
                        source_by_field, f"{base}/{resource}?_count=1000&_offset=0002", timeout=2
                    )
                )
                for resource in ("Location", "PractitionerRole")
            ]
            try:
                await asyncio.wait_for(rejected.wait(), 3)
                release.set()
                fetch_results = await asyncio.wait_for(asyncio.gather(*tasks), 3)
                assert [fetch_result[0] for fetch_result in fetch_results] == [200, 200]
                assert counters_by_name == {"tokens": 1, "rejected": 2}
                assert sorted(paths_by_token["synthetic-fresh"]) == sorted(paths_by_token["synthetic-old"])
                assert all(path.endswith("?_count=1000&_offset=0002") for path in paths_by_token["synthetic-fresh"])
            finally:
                release.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    assert directory._SOURCE_HTTP_SESSION.get() is None
    assert selection.CREDENTIAL_REQUEST_LOCKS.get() is None


def _stalled_renewal_app():
    """Let a canceled upstream reply finish after a sibling has obtained its token."""
    renewal_started, release_renewal, renewal_finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    counters_by_name = {"tokens": 0, "pages": 0}

    async def token(_request):
        counters_by_name["tokens"] += 1
        if counters_by_name["tokens"] == 1:
            renewal_started.set()
            try:
                await release_renewal.wait()
            except asyncio.CancelledError:
                await release_renewal.wait()
            renewal_finished.set()
            return web.json_response({"access_token": "synthetic-late", "expires_in": 3600})
        return web.json_response({"access_token": "synthetic-fresh", "expires_in": 3600})

    async def page(_request):
        counters_by_name["pages"] += 1
        return web.Response(status=401)

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    http_app.router.add_get("/fhir/Location", page)
    return http_app, renewal_started, release_renewal, renewal_finished, counters_by_name


def _hold_first_token_cleanup(monkeypatch):
    """Expose the owned HTTP cleanup boundary through repeated parent cancellation."""
    cleanup_started, release_cleanup = asyncio.Event(), asyncio.Event()
    original = selection._credentialed_oauth_token

    async def token(*args):
        try:
            return await original(*args)
        finally:
            if not cleanup_started.is_set():
                cleanup_started.set()
                await release_cleanup.wait()

    monkeypatch.setattr(selection, "_credentialed_oauth_token", token)
    return cleanup_started, release_cleanup


@pytest.mark.parametrize("cancellations", [1, 3])
async def test_cancelled_token_renewal_drains_before_a_sibling_uses_the_cache(monkeypatch, cancellations):
    http_app, renewal_started, release_renewal, renewal_finished, counters_by_name = _stalled_renewal_app()
    cleanup_started, release_cleanup = _hold_first_token_cleanup(monkeypatch)
    async with (
        TestServer(http_app) as server,
        directory._source_http_session_scope(force=True, request_concurrency=2),
    ):
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)

        async def renew_credentials():
            """Resolve the rejected token under the shared credential lock."""
            return await selection.credentialed_rest_options(
                directory, source_by_field, base + "/Location", rejected_authorization="Bearer synthetic-old"
            )

        renewal_tasks = [asyncio.create_task(renew_credentials())]
        try:
            await asyncio.wait_for(renewal_started.wait(), 3)
            renewal_tasks[0].cancel("cancel-0")
            await asyncio.wait_for(cleanup_started.wait(), 3)
            for _ in range(cancellations - 1):
                renewal_tasks[0].cancel("cancel-again")
                await asyncio.sleep(0)
            assert not renewal_tasks[0].done() and selection.CREDENTIAL_REQUEST_LOCKS.get()[key].locked()
            renewal_tasks.append(asyncio.create_task(renew_credentials()))
            await asyncio.sleep(0)
            assert counters_by_name["tokens"] == 1 and not renewal_tasks[1].done()
            release_cleanup.set()
            with pytest.raises(asyncio.CancelledError, match="cancel-0"):
                await asyncio.wait_for(renewal_tasks[0], 3)
            options = await asyncio.wait_for(renewal_tasks[1], 3)
            assert options["headers"] == {"Authorization": "Bearer synthetic-fresh"}
            release_renewal.set()
            await asyncio.wait_for(renewal_finished.wait(), 3)
            assert directory._OAUTH_TOKEN_CACHE[key][0] == "synthetic-fresh" and counters_by_name["tokens"] == 2
        finally:
            release_cleanup.set()
            release_renewal.set()
            for task in renewal_tasks:
                task.cancel()
            await asyncio.gather(*renewal_tasks, return_exceptions=True)


async def test_page_deadline_cancels_stalled_token_renewal_without_a_late_cache_write(monkeypatch):
    http_app, renewal_started, release_renewal, renewal_finished, counters_by_name = _stalled_renewal_app()
    async with (
        TestServer(http_app) as server,
        directory._source_http_session_scope(force=True, request_concurrency=1),
    ):
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
        deadline = asyncio.timeout(None)

        async def request():
            """Use the same ambient page deadline as the shared resource reader."""
            async with deadline:
                return await directory._fetch_source_json_once(source_by_field, base + "/Location", timeout=2)

        task = asyncio.create_task(request())
        try:
            await asyncio.wait_for(renewal_started.wait(), 3)
            deadline.reschedule(asyncio.get_running_loop().time() + 0.02)
            done, _pending = await asyncio.wait({task}, timeout=0.5)
            assert done == {task}, "a page deadline must not wait for the upstream token response"
            with pytest.raises(TimeoutError):
                await task
            assert deadline.expired() and not selection.CREDENTIAL_REQUEST_LOCKS.get()[key].locked()
            release_renewal.set()
            await asyncio.wait_for(renewal_finished.wait(), 3)
            assert key not in directory._OAUTH_TOKEN_CACHE and counters_by_name == {"tokens": 1, "pages": 1}
        finally:
            release_renewal.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def _unauthorized_throttle_app(
    status, retry_after, *, initial_requests=2, hold_authenticated=None, short_first_token=False
):
    """Expose renewal blocked behind a sibling's real Retry-After response."""
    renewal_started, release_renewal = asyncio.Event(), asyncio.Event()
    initial_admitted, fresh_started = asyncio.Event(), asyncio.Event()
    observations_by_name = {"tokens": 0, "role_requests": [], "initial": 0, "active": 0, "maximum": 0}

    async def token(_request):
        observations_by_name["tokens"] += 1
        renewal_started.set()
        await release_renewal.wait()
        return web.json_response(
            {
                "access_token": f"synthetic-{observations_by_name['tokens']}"
                if short_first_token
                else "synthetic-fresh",
                "expires_in": 1 if short_first_token and observations_by_name["tokens"] == 1 else 3600,
            }
        )

    async def page(request):
        authorization = request.headers["Authorization"]
        if authorization == "Bearer synthetic-old":
            observations_by_name["initial"] += 1
            if observations_by_name["initial"] == initial_requests:
                initial_admitted.set()
        if request.match_info["resource"] == "Location":
            await renewal_started.wait()
            return web.Response(status=status, headers={"Retry-After": str(retry_after)})
        observations_by_name["role_requests"].append((request.raw_path, directory.time.monotonic(), authorization))
        if authorization == "Bearer synthetic-old":
            await initial_admitted.wait()
            return web.Response(status=401)
        observations_by_name["active"] += 1
        observations_by_name["maximum"] = max(observations_by_name["maximum"], observations_by_name["active"])
        fresh_started.set()
        try:
            if hold_authenticated is not None:
                await hold_authenticated.wait()
            return web.json_response({"resourceType": "Bundle", "type": "searchset", "entry": []})
        finally:
            observations_by_name["active"] -= 1

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    http_app.router.add_get("/fhir/{resource}", page)
    return http_app, release_renewal, fresh_started, observations_by_name


@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize("expires_during_cooldown", [False, True])
async def test_unauthorized_retry_obeys_concurrent_retry_after_and_deadline(
    monkeypatch, status, expires_during_cooldown
):
    http_app, release_renewal, _fresh_started, observations_by_name = _unauthorized_throttle_app(
        status, 2 if expires_during_cooldown else 0.03
    )
    async with (
        TestServer(http_app) as server,
        directory._source_http_session_scope(force=True, request_concurrency=2),
    ):
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
        request_deadline = asyncio.timeout(None)
        role_url = base + "/PractitionerRole?_count=1000&_offset=0002"

        async def role_request():
            """Apply the caller's deadline to renewal and its exact-page retry."""
            async with request_deadline:
                return await directory._fetch_source_json_once(source_by_field, role_url, timeout=2)

        tasks = [
            asyncio.create_task(role_request()),
            asyncio.create_task(directory._fetch_source_json_once(source_by_field, base + "/Location", timeout=2)),
        ]
        try:
            assert (await asyncio.wait_for(tasks[1], 3))[0] == status
            deadline = source_by_field["_provider_directory_request_pacing_state"]["retry_after_monotonic"]
            if expires_during_cooldown:
                request_deadline.reschedule(asyncio.get_running_loop().time() + 0.02)
            release_renewal.set()
            if expires_during_cooldown:
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(tasks[0], 3)
                assert request_deadline.expired() and deadline > directory.time.monotonic()
                assert len(observations_by_name["role_requests"]) == 1
            else:
                assert (await asyncio.wait_for(tasks[0], 3))[0] == 200
                sent = observations_by_name["role_requests"]
                assert sent[0][0] == sent[1][0] == role_url.removeprefix(base.rsplit("/", 1)[0])
                assert sent[1][1] >= deadline and sent[1][2] == "Bearer synthetic-fresh"
            assert observations_by_name["tokens"] == 1
        finally:
            release_renewal.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize(("retry_unauthorized", "unpaced"), [(False, False), (True, False), (True, True)])
async def test_token_expiry_during_minimum_interval_preserves_unsent_pacing(monkeypatch, retry_unauthorized, unpaced):
    source_by_field, spec = _reviewed_source(monkeypatch, "https://payer.example/fhir", "https://payer.example/token")
    source_by_field["metadata_json"] = {"provider_directory_request_interval_seconds": 30}
    state = source_by_field["_provider_directory_request_pacing_state"]
    state["_provider_directory_last_request_monotonic"] = 1000
    clock = SimpleNamespace(now=1000)
    waits, renewals, requests = [], [], []
    expected_waits = 2 if retry_unauthorized and not unpaced else 1
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", 4600 if retry_unauthorized else 1010)

    async def sleep(seconds):
        waits.append(seconds)
        assert len(waits) <= expected_waits, "an unsent request must not consume another pacing interval"
        clock.now += seconds

    async def token(_directory, _source, _url, _oauth, _timeout):
        cached = directory._OAUTH_TOKEN_CACHE.get(key)
        if cached and cached[1] > clock.now:
            return cached
        renewals.append(clock.now)
        cached = (f"synthetic-renewed-{len(renewals)}", clock.now + 10)
        directory._OAUTH_TOKEN_CACHE[key] = cached
        return cached

    async def fetch(_url, **options):
        requests.append((clock.now, options["extra_headers"]["Authorization"]))
        assert directory._OAUTH_TOKEN_CACHE[key][1] > clock.now
        return (401 if retry_unauthorized and len(requests) == 1 else 200), {}, None, 1

    monkeypatch.setattr(directory, "time", SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.now))
    monkeypatch.setattr(directory, "asyncio", SimpleNamespace(**(vars(asyncio) | {"sleep": sleep})))
    monkeypatch.setattr(selection, "_credentialed_oauth_token", token)
    transport = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(directory, "_fetch_json_with_options", transport)
    url = "https://payer.example/fhir/PractitionerRole?_count=1000&_offset=0002"
    fetch_page = directory._fetch_source_json_unpaced if unpaced else directory._fetch_source_json_once
    assert (await fetch_page(source_by_field, url, timeout=2))[0] == 200
    assert waits == [30] * expected_waits
    assert len(requests) == (2 if retry_unauthorized else 1)
    assert renewals == ([1000 if unpaced else 1030, 1030 if unpaced else 1060] if retry_unauthorized else [1030])
    assert requests[-1][1] == f"Bearer synthetic-renewed-{len(renewals)}"
    assert state["_provider_directory_last_request_monotonic"] == requests[-1][0]
    assert all(call.args == (url,) for call in transport.await_args_list)


async def test_expired_token_with_empty_renewal_never_sends_and_releases_admission(monkeypatch):
    clock = SimpleNamespace(now=1000)
    calls_by_name = {"tokens": 0, "pages": 0}
    replies = [{}, {"access_token": "synthetic-fresh", "expires_in": 3600}]

    async def token(_request):
        calls_by_name["tokens"] += 1
        return web.json_response(replies.pop(0))

    async def page(request):
        calls_by_name["pages"] += 1
        assert request.headers["Authorization"] == "Bearer synthetic-fresh"
        return web.json_response({"resourceType": "Bundle", "type": "searchset", "entry": []})

    async def sleep(seconds):
        clock.now += seconds

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    http_app.router.add_get("/fhir/Location", page)
    async with TestServer(http_app) as server, directory._source_http_session_scope(force=True, request_concurrency=1):
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        source_by_field["metadata_json"] = {"provider_directory_request_interval_seconds": 30}
        pacing = source_by_field["_provider_directory_request_pacing_state"]
        pacing["_provider_directory_last_request_monotonic"] = clock.now
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        cached = ("synthetic-old", clock.now + 10)
        directory._OAUTH_TOKEN_CACHE[key] = cached
        monkeypatch.setattr(directory, "time", SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.now))
        monkeypatch.setattr(directory, "asyncio", SimpleNamespace(**(vars(asyncio) | {"sleep": sleep})))
        url = base + "/Location"

        fetch_result = await directory._fetch_source_json_once(source_by_field, url, timeout=2)
        assert fetch_result[:3] == (None, None, "provider_directory_credentials_unavailable")
        assert calls_by_name == {"tokens": 1, "pages": 0}
        assert directory._OAUTH_TOKEN_CACHE[key] == cached
        assert pacing["_provider_directory_last_request_monotonic"] == 1000
        locks = selection.CREDENTIAL_REQUEST_LOCKS.get()
        assert locks and all(not lock.locked() for lock in locks.values())

        fetch_result = await asyncio.wait_for(directory._fetch_source_json_once(source_by_field, url, timeout=2), 3)
        assert fetch_result[0] == 200
        assert calls_by_name == {"tokens": 2, "pages": 1}
        assert not replies
    assert directory._SOURCE_HTTP_SESSION.get() is None
    assert selection.CREDENTIAL_REQUEST_LOCKS.get() is None


async def test_replacement_expiring_during_cooldown_is_renewed_before_the_only_retry(monkeypatch):
    http_app, release_renewal, _fresh_started, observations_by_name = _unauthorized_throttle_app(
        429, 0.03, short_first_token=True
    )
    clock_by_field = {"now": 1000}
    pace = directory._pace_source_request

    async def paced(source_record, **options):
        if observations_by_name["tokens"] == 1:
            clock_by_field["now"] = 1002
        await pace(source_record, **options)

    monkeypatch.setattr(directory, "_pace_source_request", paced)
    monkeypatch.setattr(
        directory, "time", SimpleNamespace(time=lambda: clock_by_field["now"], monotonic=directory.time.monotonic)
    )
    async with (
        TestServer(http_app) as server,
        directory._source_http_session_scope(force=True, request_concurrency=2),
    ):
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", 4600)
        tasks = [
            asyncio.create_task(
                directory._fetch_source_json_once(
                    source_by_field, f"{base}/{resource}?_count=1000&_offset=0002", timeout=2
                )
            )
            for resource in ("PractitionerRole", "Location")
        ]
        try:
            assert (await asyncio.wait_for(tasks[1], 3))[0] == 429
            release_renewal.set()
            assert (await asyncio.wait_for(tasks[0], 3))[0] == 200
            assert observations_by_name["tokens"] == 2
            requests = observations_by_name["role_requests"]
            assert [request[2] for request in requests] == ["Bearer synthetic-old", "Bearer synthetic-2"]
            assert requests[0][0] == requests[1][0]
            assert directory._OAUTH_TOKEN_CACHE[key][0] == "synthetic-2"
        finally:
            release_renewal.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_token_request_failure_keeps_its_original_exception_class(monkeypatch):
    source_by_field, _spec = _reviewed_source(monkeypatch, "https://payer.example/fhir", "https://payer.example/token")
    failure = ValueError("synthetic-token-request-invalid")
    monkeypatch.setattr(directory, "_oauth2_token_request", Mock(side_effect=failure))
    with pytest.raises(ValueError) as captured:
        await selection.credentialed_rest_options(directory, source_by_field, "https://payer.example/fhir/Location")
    assert captured.value is failure


@pytest.mark.parametrize("reuse", [False, True])
async def test_async_token_request_uses_only_verified_owned_or_shared_sessions(monkeypatch, reuse):
    monkeypatch.delenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, raising=False)

    async def token(_request):
        return web.json_response({"access_token": "synthetic-token", "expires_in": 3600})

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    original, sessions = directory._source_http_client_session, []

    def client(**options):
        session = original(**options)
        sessions.append(session)
        assert options == {"require_verified_tls": True}
        assert session.connector._ssl.check_hostname and session.connector._ssl.verify_mode == ssl.CERT_REQUIRED
        return session

    async with (
        TestServer(http_app) as server,
        directory._source_http_session_scope(force=reuse, require_verified_tls=True),
    ):
        base = str(server.make_url("/fhir"))
        source_by_field, _spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        monkeypatch.setattr(directory, "_source_http_client_session", client)
        options = await selection.credentialed_rest_options(directory, source_by_field, base + "/Location", timeout=2)
        assert options["headers"] == {"Authorization": "Bearer synthetic-token"}
        assert len(sessions) == (0 if reuse else 1)
        assert all(session.closed for session in sessions)
        if reuse:
            assert not directory._SOURCE_HTTP_SESSION.get().closed


async def test_cached_token_does_not_replace_missing_client_credentials(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    cached = ("synthetic-old", directory.time.time() + 3600)
    directory._OAUTH_TOKEN_CACHE[key] = cached
    del spec["oauth2"]["client_secret"]
    client = Mock(side_effect=AssertionError("an incomplete credential rule must not send a token request"))
    monkeypatch.setattr(directory, "_source_http_client_session", client)
    options = await selection.credentialed_rest_options(directory, source_by_field, base + "/Location")
    assert options["headers"] == {} and directory._OAUTH_TOKEN_CACHE[key] == cached
    client.assert_not_called()


@pytest.mark.parametrize("expires_in,expected", [(3600, 3600), (0, 3600), ("invalid", 3600), (-1, None)])
async def test_async_token_cache_preserves_expiry_defaults_and_refuses_expired_replies(
    monkeypatch, expires_in, expected
):
    async def token(_request):
        return web.json_response({"access_token": "synthetic-token", "expires_in": expires_in})

    http_app = web.Application()
    http_app.router.add_post("/token", token)
    async with TestServer(http_app) as server, directory._source_http_session_scope(force=True):
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        before = directory.time.time()
        options = await selection.credentialed_rest_options(directory, source_by_field, base + "/Location", timeout=2)
        if expected is None:
            assert options["headers"] == {} and key not in directory._OAUTH_TOKEN_CACHE
        else:
            cached_token, cached_expiry = directory._OAUTH_TOKEN_CACHE[key]
            assert cached_token == "synthetic-token"
            assert before + expected - directory.OAUTH_TOKEN_EXPIRY_SKEW_SECONDS <= cached_expiry
            assert cached_expiry <= directory.time.time() + expected - directory.OAUTH_TOKEN_EXPIRY_SKEW_SECONDS
            assert selection.has_current_credentialed_rest_options(directory, options)


@pytest.mark.parametrize("status", [429, 503])
async def test_unauthorized_retries_reenter_the_reduced_one_request_allowance(monkeypatch, status):
    release_authenticated = asyncio.Event()
    http_app, release_renewal, fresh_started, observations_by_name = _unauthorized_throttle_app(
        status, 0.03, initial_requests=3, hold_authenticated=release_authenticated
    )
    async with (
        TestServer(http_app) as server,
        directory._source_http_session_scope(force=True, request_concurrency=3),
    ):
        base = str(server.make_url("/fhir"))
        source_by_field, spec = _reviewed_source(monkeypatch, base, str(server.make_url("/token")))
        key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
        directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
        tasks = [
            asyncio.create_task(directory._fetch_source_json_once(source_by_field, f"{base}/{resource}", timeout=2))
            for resource in ("PractitionerRole", "HealthcareService", "Location")
        ]
        try:
            assert (await asyncio.wait_for(tasks[2], 3))[0] == status
            slots = [slot for slot in directory.SOURCE_HTTP_REQUEST_SLOTS.get().values() if hasattr(slot, "limit")]
            assert len(slots) == 1 and slots[0].limit == 1
            release_renewal.set()
            await asyncio.wait_for(fresh_started.wait(), 3)
            await asyncio.sleep(0.02)
            assert observations_by_name["maximum"] == observations_by_name["active"] == 1
            assert slots[0].active == 1
            release_authenticated.set()
            fetch_results = await asyncio.wait_for(asyncio.gather(*tasks[:2]), 3)
            assert [fetch_result[0] for fetch_result in fetch_results] == [200, 200]
            assert observations_by_name["tokens"] == 1 and observations_by_name["maximum"] == 1
            assert observations_by_name["initial"] == 3 and slots[0].active == 0
        finally:
            release_renewal.set()
            release_authenticated.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("priority", [None, "proxy", "census", "capped", "same_collection"])
def test_credentialed_transport_preserves_prior_transport_contracts(monkeypatch, priority):
    source_by_field, _spec = _reviewed_source(monkeypatch, "https://payer.example/fhir", "https://payer.example/token")
    monkeypatch.setattr(
        directory.source_proxy, "selected_run", lambda *_args: object() if priority == "proxy" else None
    )
    monkeypatch.setattr(
        directory, "current_version_census_contract", lambda *_args: object() if priority == "census" else None
    )
    monkeypatch.setattr(directory.capped_partition, "enabled", lambda *_args: priority == "capped")
    monkeypatch.setattr(
        directory,
        "_source_metadata",
        lambda *_args: {"provider_directory_pagination_same_collection": priority == "same_collection"},
    )
    assert selection.uses_credentialed_rest_transport(directory, source_by_field) is (priority is None)


async def test_unpaced_unauthorized_retry_preserves_census_deferral(monkeypatch):
    base = "https://payer.example/fhir"
    source_by_field, spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
    _mock_fresh_token(monkeypatch)
    fetch = AsyncMock(return_value=(401, None, None, 3))
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    pace = AsyncMock(side_effect=selection.CensusRequestDeferred)
    monkeypatch.setattr(directory, "_pace_source_request", pace)
    assert await directory._fetch_source_json_unpaced(source_by_field, base + "/Location", timeout=2) == (
        None,
        None,
        selection.CENSUS_REQUEST_DEFERRED_ERROR,
        3,
    )
    fetch.assert_awaited_once()
    pace.assert_awaited_once()
    assert pace.await_args.args == (source_by_field,)
    assert pace.await_args.kwargs["allow_census_defer"] is True
    assert pace.await_args.kwargs["ready"]() is True


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("extra_headers", [{}, {"Authorization": ""}])
async def test_oauth_refresh_is_once_only_and_never_retries_forbidden(monkeypatch, status, extra_headers):
    base = "https://payer.example/fhir"
    source, spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    spec["headers"] = extra_headers
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
    token_request = _mock_fresh_token(monkeypatch)
    fetch = AsyncMock(side_effect=[(status, None, None, 3), (401, None, None, 7)])
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    url = base + "/Location?_count=1000&_offset=0002"
    result = await directory._fetch_source_json_unpaced(source, url, timeout=2)
    assert result == (status, None, None, 10 if status == 401 else 3)
    assert fetch.await_count == (2 if status == 401 else 1)
    assert token_request.await_count == (2 if status == 401 else 1)
    assert all(call.args == (url,) and call.kwargs["timeout"] == 2 for call in fetch.await_args_list)
    assert fetch.await_args_list[0].kwargs["extra_headers"] == {"Authorization": "Bearer synthetic-old"}
    if status == 401:
        assert fetch.await_args_list[1].kwargs["extra_headers"] == {"Authorization": "Bearer synthetic-fresh"}


@pytest.mark.parametrize("override", ["bearer", "header", "mixed_case_header", "api_key", "oauth_none"])
async def test_unauthorized_static_or_overridden_authorization_does_not_invalidate_oauth(monkeypatch, override):
    base = "https://payer.example/fhir"
    source, spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    cached = ("synthetic-old", directory.time.time() + 3600)
    directory._OAUTH_TOKEN_CACHE[key] = cached
    if override in {"bearer", "oauth_none"}:
        spec["bearer_token"] = "synthetic-old"
    if override == "oauth_none":
        spec["oauth2"] = None
    elif override == "api_key":
        spec["api_key"] = {"header": "Authorization", "value": "Bearer synthetic-old"}
    elif override in {"header", "mixed_case_header"}:
        spec["headers"] = {"Authorization" if override == "header" else "authorization": "Bearer synthetic-old"}
    token_request = Mock(side_effect=AssertionError("static authorization must not refresh OAuth"))
    monkeypatch.setattr(selection, "_credentialed_oauth_token", token_request)
    fetch = AsyncMock(return_value=(401, None, None, 3))
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    assert await directory._fetch_source_json_unpaced(source, base + "/Location", timeout=2) == (401, None, None, 3)
    fetch.assert_awaited_once()
    token_request.assert_not_called()
    assert directory._OAUTH_TOKEN_CACHE == {key: cached}


async def test_unauthorized_page_reuses_a_newer_cached_token_without_invalidating_it(monkeypatch):
    base = "https://payer.example/fhir"
    source, spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
    fresh = ("synthetic-fresh", directory.time.time() + 3600)
    calls = []

    async def fetch(url, **options):
        calls.append((url, options))
        if len(calls) == 1:
            directory._OAUTH_TOKEN_CACHE[key] = fresh
            return 401, None, None, 3
        return 200, {}, None, 7

    token_request = Mock(side_effect=AssertionError("a newer cached token must be reused"))
    monkeypatch.setattr(directory, "_source_http_client_session", token_request)
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    url = base + "/Location?_count=1000&_offset=0002"
    assert await directory._fetch_source_json_unpaced(source, url, timeout=2) == (200, {}, None, 10)
    assert len(calls) == 2 and all(call[0] == url for call in calls)
    assert calls[1][1]["extra_headers"] == {"Authorization": "Bearer synthetic-fresh"}
    assert directory._OAUTH_TOKEN_CACHE[key] == fresh
    token_request.assert_not_called()


async def test_failed_oauth_refresh_preserves_the_authentication_failure_without_anonymous_fallback(monkeypatch):
    base = "https://payer.example/fhir"
    source, spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
    token_request = Mock(side_effect=OSError("synthetic-provider-detail"))
    monkeypatch.setattr(directory, "_source_http_client_session", token_request)
    fetch = AsyncMock(return_value=(401, None, None, 3))
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    assert await directory._fetch_source_json_unpaced(source, base + "/Location", timeout=2) == (401, None, None, 3)
    fetch.assert_awaited_once()
    token_request.assert_called_once()
    assert key not in directory._OAUTH_TOKEN_CACHE


async def test_cancellation_during_the_unauthorized_retry_propagates(monkeypatch):
    base = "https://payer.example/fhir"
    source, spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    key = directory._oauth2_cache_key(directory._oauth2_spec(spec))
    directory._OAUTH_TOKEN_CACHE[key] = ("synthetic-old", directory.time.time() + 3600)
    _mock_fresh_token(monkeypatch)
    fetch = AsyncMock(side_effect=[(401, None, None, 3), asyncio.CancelledError])
    monkeypatch.setattr(directory, "_fetch_json_with_options", fetch)
    with pytest.raises(asyncio.CancelledError):
        await directory._fetch_source_json_unpaced(source, base + "/Location", timeout=2)
    assert fetch.await_count == 2


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


@pytest.mark.parametrize("use_default_resources", (False, True))
async def test_reviewed_authenticated_scans_use_scheduler_without_enabling_page_prefetch(
    monkeypatch, use_default_resources
):
    base = "https://payer.example/fhir"
    source_by_field, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    checkpointed = _parallel_resource_source()
    source_by_field.update(checkpointed, api_base=base, canonical_api_base=base)
    source_by_field["_pagination_checkpoint_context"] = dataclasses.replace(
        checkpointed["_pagination_checkpoint_context"], canonical_api_base=base
    )
    source_by_field["metadata_json"] = {
        "provider_directory_supported_resources": list(directory.anthem.RESOURCES),
    }
    _patch_resource_scan_storage(monkeypatch, True)
    resources = tuple(directory.DEFAULT_RESOURCES) if use_default_resources else directory.anthem.RESOURCES
    options = _resource_concurrency_options(requested_concurrency=8, resource_types=resources)
    async with directory._source_http_session_scope(force=True, request_concurrency=8):
        assert directory._effective_resource_scan_concurrency(source_by_field, options) == 7
        assert (
            directory.resource_scan_types(directory, source_by_field, resources, options) == directory.anthem.RESOURCES
        )
        assert options.resource_types == resources
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
    source_by_field, credential_spec_by_field = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    credential_spec_by_field.clear()
    credential_spec_by_field["bearer_token"] = "synthetic-token"
    response = (429, {directory.SOURCE_RETRY_AFTER_FIELD: "1200"}, None, 0)
    monkeypatch.setattr(selection, "send_authenticated_page", AsyncMock(return_value=response))
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


@pytest.mark.parametrize("recovers", (True, False))
async def test_reviewed_server_error_cooldown_retries_the_unchanged_cursor(monkeypatch, recovers):
    base = "https://payer.example/fhir"
    source, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    url = base + "/Location?_count=1000&_page=opaque"
    failure = (500, {"resourceType": "OperationOutcome"}, None, 0)
    success = (200, {"resourceType": "Bundle", "entry": []}, None, 0)
    fetch = AsyncMock(side_effect=[failure, success] if recovers else [failure, failure])
    sleep = AsyncMock()
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory.asyncio, "sleep", sleep)
    outcome = await directory._retry_rest_pagination_after_cooldown(source, url, failure, timeout=60, deadline_at=None)
    assert outcome.retries == directory.REST_PAGINATION_COOLDOWN_RETRY_CAP == 2
    assert outcome.recovered is recovers and outcome.exhausted is not recovers
    assert not outcome.deadline_blocked
    assert all(call.args == (source, url) and call.kwargs == {"timeout": 60} for call in fetch.await_args_list)
    assert fetch.await_count == sleep.await_count == 2
    assert outcome.fetch_result == (success if recovers else failure)


@pytest.mark.parametrize(
    ("status", "fetch_error", "source_base", "eligible"),
    (
        (500, None, "https://payer.example/fhir", True),
        (500, "invalid_response", "https://payer.example/fhir", False),
        (500, None, "https://other.example/fhir", False),
        (503, None, "https://payer.example/fhir", False),
    ),
)
def test_reviewed_server_error_cooldown_keeps_its_source_scope(monkeypatch, status, fetch_error, source_base, eligible):
    _reviewed_source(monkeypatch, "https://payer.example/fhir", "https://payer.example/token")
    assert (
        directory._is_rest_pagination_cooldown_failure(status, fetch_error, source_record={"api_base": source_base})
        is eligible
    )
    assert not directory._is_rest_pagination_cooldown_failure(500, None)


async def test_reviewed_server_error_retry_after_cannot_overrun_the_resource_deadline(monkeypatch):
    base = "https://payer.example/fhir"
    source, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    failure = (500, {directory.SOURCE_RETRY_AFTER_FIELD: "1200"}, None, 0)
    fetch = AsyncMock(side_effect=AssertionError("retry before provider delay forbidden"))
    sleep = AsyncMock(side_effect=AssertionError("sleep beyond deadline forbidden"))
    monkeypatch.setattr(directory, "_fetch_cooldown_checkpoint_page", fetch)
    monkeypatch.setattr(directory.asyncio, "sleep", sleep)
    outcome = await directory._retry_rest_pagination_after_cooldown(
        source, base + "/Location", failure, timeout=60, deadline_at=directory.time.monotonic() + 700
    )
    assert outcome.deadline_blocked and outcome.retries == 0 and not outcome.exhausted
    assert outcome.fetch_result == failure
    fetch.assert_not_awaited()
    sleep.assert_not_awaited()


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


def test_unreviewed_or_malformed_collection_never_resolves_credentials(monkeypatch):
    """Reject both unrelated sources and malformed destinations before secret lookup."""
    base = "https://payer.example/fhir"
    source, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    resolve = Mock(side_effect=AssertionError("credentials must not be resolved"))
    monkeypatch.setattr(directory, "_credential_spec_for_source", resolve)
    assert not selection.credentialed_rest_request_allowed(directory, {"api_base": base + "-other"}, base)
    assert not selection.credentialed_rest_request_allowed(directory, source, "https://[invalid")
    assert not selection.is_anonymous_source_url("https://[invalid")
    resolve.assert_not_called()


def test_reviewed_session_requires_verified_tls_context(monkeypatch):
    """Reuse only the reviewed credentialed session while TLS verification remains enabled."""
    base = "https://payer.example/fhir"
    source, _spec = _reviewed_source(monkeypatch, base, "https://payer.example/token")
    context = ssl.create_default_context()
    session = SimpleNamespace(connector=SimpleNamespace(_ssl=context), closed=False)
    token = directory._SOURCE_HTTP_SESSION.set(session)
    try:
        assert selection.credentialed_rest_session(directory, source, base + "/Location") is session
        context.check_hostname = False
        assert selection.credentialed_rest_session(directory, source, base + "/Location") is None
        context.check_hostname = True
        session.closed = True
        assert selection.credentialed_rest_session(directory, source, base + "/Location") is None
    finally:
        directory._SOURCE_HTTP_SESSION.reset(token)
